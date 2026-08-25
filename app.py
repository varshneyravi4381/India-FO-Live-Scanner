from __future__ import annotations
import json, time
import asyncio
import sqlite3
import io
import csv
import re
import requests
import threading
from pathlib import Path
from typing import Dict, List
import pandas as pd
import yfinance as yf
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

BASE = Path(__file__).resolve().parent
SECTORS: Dict[str,List[str]] = json.loads((BASE/"sectors.json").read_text(encoding="utf-8"))
NIFTY50: List[str] = json.loads((BASE/"nifty50.json").read_text(encoding="utf-8"))

_NIFTY100_DF = pd.read_csv(BASE / "ind_nifty100list.csv")
_NIFTY500_DF = pd.read_csv(BASE / "ind_nifty500list.csv")

NIFTY100: List[str] = (
    _NIFTY100_DF["Symbol"].astype(str).str.strip().str.upper().tolist()
)
NIFTY500: List[str] = (
    _NIFTY500_DF["Symbol"].astype(str).str.strip().str.upper().tolist()
)

INDEX_INDUSTRY_MAP = {
    str(row["Symbol"]).strip().upper(): str(row["Industry"]).strip()
    for _, row in _NIFTY500_DF.iterrows()
}


# ============================================================================
# CENTRAL YAHOO SYMBOL GATE — V17
# ============================================================================
# Every call to yf.download() is filtered here, regardless of which scanner tab
# or backend function originated the request.

INVALID_YAHOO_FILE = BASE / "invalid_yahoo_symbols.json"

YAHOO_EXCLUDED_SYMBOLS = {
    "NIFTY",
    "BANKNIFTY",
    "FINNIFTY",
    "MIDCPNIFTY",
    "NIFTYNXT50",
    "NIFTYFPI",
    "NIFTYIT",
    "NIFTYBANK",
}

def _load_permanent_yahoo_exclusions():
    excluded = set(YAHOO_EXCLUDED_SYMBOLS)
    try:
        payload = json.loads(
            INVALID_YAHOO_FILE.read_text(encoding="utf-8")
        )
        for symbol in payload.get("permanent_excluded", []):
            symbol = str(symbol).strip().upper()
            if symbol:
                excluded.add(symbol)
    except Exception:
        pass
    return excluded

_yahoo_permanent_excluded = _load_permanent_yahoo_exclusions()
_yahoo_invalid_symbols = set()
_yahoo_skipped_once = set()

def _base_nse_symbol(value):
    s = str(value or "").strip().upper()
    if s.endswith(".NS"):
        s = s[:-3]
    return s

def _is_stock_symbol(symbol: str) -> bool:
    s = _base_nse_symbol(symbol)
    if not s:
        return False

    if s in _yahoo_permanent_excluded:
        return False

    # Index-family identifiers are not cash-equity symbols for these batches.
    if s.startswith("NIFTY"):
        return False

    if s in _yahoo_invalid_symbols:
        return False

    return True

def _sanitize_stock_symbols(symbols):
    result = []
    seen = set()

    for symbol in symbols or []:
        s = _base_nse_symbol(symbol)
        if not s or s in seen:
            continue
        seen.add(s)

        if _is_stock_symbol(s):
            result.append(s)

    return result

def _mark_invalid_symbol(symbol):
    s = _base_nse_symbol(symbol)
    if s and s not in _yahoo_permanent_excluded:
        _yahoo_invalid_symbols.add(s)

def _normalize_yahoo_ticker_input(tickers):
    if tickers is None:
        return [], "none"

    if isinstance(tickers, str):
        # yfinance accepts comma/space separated strings.
        raw = re.split(r"[\s,]+", tickers.strip())
        return [x for x in raw if x], "string"

    try:
        return list(tickers), "list"
    except Exception:
        return [str(tickers)], "single"

def _sanitize_yahoo_tickers(tickers):
    raw, input_kind = _normalize_yahoo_ticker_input(tickers)

    kept = []
    skipped = []

    for ticker in raw:
        t = str(ticker or "").strip().upper()
        if not t:
            continue

        base = _base_nse_symbol(t)

        # Only apply NSE stock filtering to .NS-style / bare NSE symbols.
        if t.endswith(".NS") or "." not in t:
            if not _is_stock_symbol(base):
                skipped.append(t)
                continue

        kept.append(t)

    if skipped:
        new_skips = [
            s for s in skipped
            if s not in _yahoo_skipped_once
        ]
        if new_skips:
            print(
                "[YahooFilter] Permanently skipped unsupported symbols:",
                ", ".join(new_skips)
            )
            _yahoo_skipped_once.update(new_skips)

    return kept, input_kind

# Store the genuine yfinance function once, then put one global gate in front.
_YF_DOWNLOAD_RAW = yf.download

def _safe_yf_download(*args, **kwargs):
    positional = list(args)

    if "tickers" in kwargs:
        original = kwargs.get("tickers")
        filtered, _ = _sanitize_yahoo_tickers(original)

        if not filtered:
            return pd.DataFrame()

        kwargs["tickers"] = filtered
        return _YF_DOWNLOAD_RAW(*positional, **kwargs)

    if positional:
        original = positional[0]
        filtered, _ = _sanitize_yahoo_tickers(original)

        if not filtered:
            return pd.DataFrame()

        positional[0] = filtered
        return _YF_DOWNLOAD_RAW(*positional, **kwargs)

    # Defensive fallback for an unusual yfinance invocation.
    return _YF_DOWNLOAD_RAW(*args, **kwargs)

# IMPORTANT: all existing code below still calls yf.download(), but now every
# one of those calls is automatically sanitized by this single wrapper.
yf.download = _safe_yf_download



DATA_DIR = BASE/"data"
DATA_DIR.mkdir(exist_ok=True)
HISTORY_FILE = DATA_DIR/"breadth_history.json"

app = FastAPI(title="India F&O Live Sector Scanner v27 Cloud Edition")

# Pre-market module import self-check.
_PREMARKET_IMPORTS_OK = all([
    re is not None,
    asyncio is not None,
    sqlite3 is not None,
    io is not None,
    requests is not None,
])

app.mount("/static", StaticFiles(directory=BASE/"static"), name="static")

CACHE_SECONDS = 5
_cache = {}

def _sym(s:str)->str: return f"{s}.NS"

def _single(data:pd.DataFrame, ys:str)->pd.DataFrame:
    if data is None or data.empty: return pd.DataFrame()
    if isinstance(data.columns,pd.MultiIndex):
        lv0=list(data.columns.get_level_values(0).unique())
        lv1=list(data.columns.get_level_values(1).unique())
        if ys in lv0: return data[ys].copy()
        if ys in lv1: return data.xs(ys,axis=1,level=1).copy()
    return data.copy()

def _load_history():
    if not HISTORY_FILE.exists(): return {}
    try: return json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception: return {}

def _save_history(h):
    HISTORY_FILE.write_text(json.dumps(h,indent=2),encoding="utf-8")

def _record_breadth(sector, rows, updated_at):
    adv=sum(1 for r in rows if r.get("move_pct") is not None and r["move_pct"]>0)
    dec=sum(1 for r in rows if r.get("move_pct") is not None and r["move_pct"]<0)
    flat=sum(1 for r in rows if r.get("move_pct") == 0)
    hist=_load_history()
    today=pd.Timestamp.now(tz="Asia/Kolkata").date().isoformat()
    sector_hist=hist.setdefault(sector,{})
    points=sector_hist.setdefault(today,[])
    stamp=pd.Timestamp(updated_at).strftime("%H:%M")
    point={"time":stamp,"advances":adv,"declines":dec,"flat":flat}
    if points and points[-1]["time"]==stamp:
        points[-1]=point
    else:
        points.append(point)
    if len(points)>450: points[:] = points[-450:]
    # retain only latest 10 dates per sector
    dates=sorted(sector_hist.keys())
    for d in dates[:-10]: sector_hist.pop(d,None)
    _save_history(hist)
    return point


def _sector_first_candle_flags(frame, pdc, pdh, pdl):
    result = {
        "first5_status": "No data",
        "first5_bull_reclaim_pdh": False,
        "first5_bear_reclaim_pdl": False,
        "first5_close_above_pdh": False,
        "first5_close_below_pdl": False,
        "first15_status": "No data",
        "first15_bull_reclaim_pdh": False,
        "first15_bear_reclaim_pdl": False,
        "first15_close_above_pdh": False,
        "first15_close_below_pdl": False,
    }

    if frame is None or frame.empty:
        return result

    f = frame.dropna(subset=["Close"]).copy()
    if f.empty:
        return result

    if getattr(f.index, "tz", None) is None:
        f.index = f.index.tz_localize("UTC").tz_convert("Asia/Kolkata")
    else:
        f.index = f.index.tz_convert("Asia/Kolkata")

    now = pd.Timestamp.now(tz="Asia/Kolkata")
    today = now.date()
    now_minutes = now.hour * 60 + now.minute

    day = f[f.index.date == today]
    regular = day[
        (day.index.hour > 9) |
        ((day.index.hour == 9) & (day.index.minute >= 15))
    ]

    def apply(prefix, o, h, l, c):
        result[f"{prefix}_bull_reclaim_pdh"] = (
            pdc is not None and pdh is not None and
            o > pdc and l < pdc and c > pdh
        )
        result[f"{prefix}_bear_reclaim_pdl"] = (
            pdc is not None and pdl is not None and
            o < pdc and h > pdc and c < pdl
        )
        result[f"{prefix}_close_above_pdh"] = pdh is not None and c > pdh
        result[f"{prefix}_close_below_pdl"] = pdl is not None and c < pdl

    if now_minutes < 9 * 60 + 20:
        result["first5_status"] = "Candle in formation"
    elif len(regular) >= 1:
        b = regular.iloc[0]
        o, h, l, c = [float(b[x]) for x in ["Open", "High", "Low", "Close"]]
        apply("first5", o, h, l, c)
        result["first5_status"] = "Complete"

    if now_minutes < 9 * 60 + 30:
        result["first15_status"] = "Candle in formation"
    elif len(regular) >= 15:
        # 1-minute source: first 15 rows form the 09:15-09:30 candle.
        bars = regular.iloc[:15]
        o = float(bars.iloc[0]["Open"])
        h = float(bars["High"].max())
        l = float(bars["Low"].min())
        c = float(bars.iloc[14]["Close"])
        apply("first15", o, h, l, c)
        result["first15_status"] = "Complete"

    return result



# ============================================================================
# V18 PERSISTENT FAST SECTOR ENGINE
# ============================================================================
# Goals:
# 1) never block page startup on Yahoo;
# 2) load last good sector snapshot from disk immediately;
# 3) previous-day levels are fetched only once per trading date;
# 4) recurring refresh downloads only today's 1-minute bars;
# 5) all sector changes are served from RAM.

SECTOR_SNAPSHOT_REFRESH_SECONDS = 10
SECTOR_SNAPSHOT_FILE = DATA_DIR / "sector_live_snapshot.json"
SECTOR_LEVELS_FILE = DATA_DIR / "sector_previous_levels.json"

_sector_snapshot_lock = threading.Lock()
_sector_snapshot = {
    "ts": 0.0,
    "rows_by_symbol": {},
    "updated_at": None,
    "refreshing": False,
}

_sector_levels_lock = threading.Lock()
_sector_levels = {
    "trade_date": None,
    "rows_by_symbol": {},
}

def _all_sector_symbols():
    return _sanitize_stock_symbols(
        sorted({
            s
            for symbols in SECTORS.values()
            for s in symbols
        })
    )

def _load_json_file(path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def _atomic_json_write(path, payload):
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8"
        )
        tmp.replace(path)
    except Exception:
        pass

def _restore_sector_cache_from_disk():
    snap = _load_json_file(
        SECTOR_SNAPSHOT_FILE,
        {}
    )

    rows = snap.get("rows_by_symbol", {})
    if isinstance(rows, dict) and rows:
        with _sector_snapshot_lock:
            _sector_snapshot["rows_by_symbol"] = rows
            _sector_snapshot["updated_at"] = snap.get("updated_at")
            _sector_snapshot["ts"] = 0.0

    levels = _load_json_file(
        SECTOR_LEVELS_FILE,
        {}
    )
    level_rows = levels.get("rows_by_symbol", {})
    if isinstance(level_rows, dict):
        with _sector_levels_lock:
            _sector_levels["trade_date"] = levels.get("trade_date")
            _sector_levels["rows_by_symbol"] = level_rows

_restore_sector_cache_from_disk()

def _build_previous_levels(symbols):
    today = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).date()

    with _sector_levels_lock:
        if (
            _sector_levels["trade_date"] == today.isoformat()
            and _sector_levels["rows_by_symbol"]
        ):
            return dict(_sector_levels["rows_by_symbol"])

    ys = [_sym(s) for s in symbols]

    # Lightweight daily bars: previous-day OHLC changes only once per session.
    daily = yf.download(
        tickers=ys,
        period="10d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
        prepost=False,
    )

    rows = {}

    for symbol, y in zip(symbols, ys):
        f = _single(daily, y)

        if f.empty or "Close" not in f:
            continue

        f = f.dropna(subset=["Close"]).copy()
        if f.empty:
            continue

        try:
            if getattr(f.index, "tz", None) is not None:
                idx_dates = [
                    x.tz_convert("Asia/Kolkata").date()
                    for x in f.index
                ]
            else:
                idx_dates = [
                    pd.Timestamp(x).date()
                    for x in f.index
                ]

            completed_positions = [
                i for i, d in enumerate(idx_dates)
                if d < today
            ]

            if not completed_positions:
                continue

            p = f.iloc[completed_positions[-1]]

            rows[symbol] = {
                "pdc": float(p["Close"]),
                "pdh": float(p["High"]),
                "pdl": float(p["Low"]),
                "pdo": float(p["Open"]),
            }
        except Exception:
            continue

    if rows:
        payload = {
            "trade_date": today.isoformat(),
            "rows_by_symbol": rows,
        }

        with _sector_levels_lock:
            _sector_levels["trade_date"] = today.isoformat()
            _sector_levels["rows_by_symbol"] = rows

        _atomic_json_write(
            SECTOR_LEVELS_FILE,
            payload
        )

    return rows

def _build_sector_snapshot():
    symbols = _all_sector_symbols()
    if not symbols:
        return

    levels = _build_previous_levels(symbols)

    # Recurring refresh is now TODAY ONLY, not 5 days of 1-minute data.
    ys = [_sym(s) for s in symbols]

    intraday = yf.download(
        tickers=ys,
        period="1d",
        interval="1m",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
        prepost=False,
    )

    rows_by_symbol = {}
    today = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).date()

    for symbol, y in zip(symbols, ys):
        prev = levels.get(symbol)
        if not prev:
            continue

        f = _single(intraday, y)

        if (
            f.empty
            or "Close" not in f
            or "Open" not in f
            or "High" not in f
            or "Low" not in f
        ):
            # Keep last good disk/RAM row instead of deleting the stock.
            with _sector_snapshot_lock:
                old = _sector_snapshot["rows_by_symbol"].get(symbol)
            if old:
                rows_by_symbol[symbol] = dict(old)
            continue

        f = f.dropna(subset=["Close"]).copy()
        if f.empty:
            continue

        if getattr(f.index, "tz", None) is None:
            f.index = f.index.tz_localize(
                "UTC"
            ).tz_convert("Asia/Kolkata")
        else:
            f.index = f.index.tz_convert(
                "Asia/Kolkata"
            )

        day = f[f.index.date == today]
        if day.empty:
            with _sector_snapshot_lock:
                old = _sector_snapshot["rows_by_symbol"].get(symbol)
            if old:
                rows_by_symbol[symbol] = dict(old)
            continue

        try:
            today_open = float(
                day["Open"].dropna().iloc[0]
            )
            current = float(
                day["Close"].dropna().iloc[-1]
            )
            pdc = float(prev["pdc"])
            pdh = float(prev["pdh"])
            pdl = float(prev["pdl"])
            pdo = float(prev["pdo"])
        except Exception:
            continue

        gap = (
            (today_open - pdc) / pdc * 100.0
            if pdc else None
        )
        move = (
            (current - pdc) / pdc * 100.0
            if pdc else None
        )

        # Helper only needs today's intraday frame + previous levels.
        flags = _sector_first_candle_flags(
            day,
            pdc,
            pdh,
            pdl
        )

        row = {
            "symbol": symbol,
            "gap_pct": gap,
            "move_pct": move,
            "current_price": current,
            "today_open": today_open,
            "pdh": pdh,
            "pdo": pdo,
            "pdc": pdc,
            "pdl": pdl,
            "above_pdh": current > pdh,
            "below_pdh": current < pdh,
            "above_pdo": current > pdo,
            "below_pdo": current < pdo,
            "above_pdc": current > pdc,
            "below_pdc": current < pdc,
            "above_pdl": current > pdl,
            "below_pdl": current < pdl,
        }
        row.update(flags)
        rows_by_symbol[symbol] = row

    if not rows_by_symbol:
        with _sector_snapshot_lock:
            _sector_snapshot["refreshing"] = False
        return

    now = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).isoformat()

    with _sector_snapshot_lock:
        # Merge instead of replacing, so a temporary Yahoo miss does not
        # blank out previously cached stocks.
        merged = dict(
            _sector_snapshot["rows_by_symbol"]
        )
        merged.update(rows_by_symbol)

        _sector_snapshot["rows_by_symbol"] = merged
        _sector_snapshot["updated_at"] = now
        _sector_snapshot["ts"] = time.time()
        _sector_snapshot["refreshing"] = False

        disk_rows = dict(merged)

    _atomic_json_write(
        SECTOR_SNAPSHOT_FILE,
        {
            "updated_at": now,
            "rows_by_symbol": disk_rows,
        }
    )

def _safe_sector_snapshot_refresh():
    try:
        _build_sector_snapshot()
    except Exception as exc:
        print(
            "[SectorCache] background refresh failed:",
            exc
        )
        with _sector_snapshot_lock:
            _sector_snapshot["refreshing"] = False

def _trigger_sector_snapshot_refresh(force=False):
    with _sector_snapshot_lock:
        has_data = bool(
            _sector_snapshot["rows_by_symbol"]
        )
        age = (
            time.time() - _sector_snapshot["ts"]
            if _sector_snapshot["ts"]
            else 999999
        )
        refreshing = bool(
            _sector_snapshot["refreshing"]
        )

        needed = (
            force
            or not has_data
            or age >= SECTOR_SNAPSHOT_REFRESH_SECONDS
        )

        if needed and not refreshing:
            _sector_snapshot["refreshing"] = True

            threading.Thread(
                target=_safe_sector_snapshot_refresh,
                daemon=True
            ).start()

def _get_sector_snapshot(sector):
    # V18 NEVER waits for Yahoo here.
    # Return disk/RAM cache immediately and refresh stale data in background.
    _trigger_sector_snapshot_refresh(
        force=False
    )

    symbols = _sanitize_stock_symbols(
        SECTORS.get(sector, [])
    )

    with _sector_snapshot_lock:
        source = _sector_snapshot[
            "rows_by_symbol"
        ]
        updated_at = _sector_snapshot[
            "updated_at"
        ]

        rows = [
            dict(source[s])
            for s in symbols
            if s in source
        ]

    return rows, updated_at


def fetch_sector(sector:str):
    symbols = _sanitize_stock_symbols(SECTORS.get(sector, [])) if sector in SECTORS else None

    if symbols is None:
        raise HTTPException(404, "Unknown sector")

    if not symbols:
        return {
            "sector": sector,
            "rows": [],
            "updated_at": pd.Timestamp.now(tz="Asia/Kolkata").isoformat(),
            "source": "Yahoo Finance / RAM cache",
            "breadth": {"advances": 0, "declines": 0, "flat": 0}
        }

    rows, updated_at = _get_sector_snapshot(sector)

    advances = sum(
        1 for r in rows
        if r.get("move_pct") is not None and r["move_pct"] > 0
    )
    declines = sum(
        1 for r in rows
        if r.get("move_pct") is not None and r["move_pct"] < 0
    )
    flat = sum(
        1 for r in rows
        if r.get("move_pct") == 0
    )

    return {
        "sector": sector,
        "rows": rows,
        "updated_at": updated_at or pd.Timestamp.now(tz="Asia/Kolkata").isoformat(),
        "source": "Yahoo Finance / RAM cache",
        "breadth": {
            "advances": advances,
            "declines": declines,
            "flat": flat
        }
    }


# ============================================================================
# V23 CACHE-FIRST MARKET BREADTH ENGINE + SAVED LIVE CHART HISTORY
# ============================================================================
BREADTH_CACHE_SECONDS = 1
BREADTH_HISTORY_DAYS = 30
_breadth_cache = {"ts": 0.0, "value": None}
_breadth_lock = threading.Lock()
BREADTH_FILE = DATA_DIR / "combined_breadth_history.json"

def _load_combined_breadth_history():
    if not BREADTH_FILE.exists():
        return {}
    try:
        return json.loads(BREADTH_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _save_combined_breadth_history(data):
    try:
        tmp = BREADTH_FILE.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(data, indent=2), encoding='utf-8')
        tmp.replace(BREADTH_FILE)
    except Exception:
        pass

def _counts(move_map):
    values = [v for v in move_map.values() if v is not None]
    advances = sum(1 for v in values if v > 0)
    declines = sum(1 for v in values if v < 0)
    flat = sum(1 for v in values if v == 0)
    return {
        "advances": advances,
        "declines": declines,
        "flat": flat,
        "total": len(values),
        "breadth_score": advances - declines,
        "advance_pct": (advances / len(values) * 100.0) if values else None,
    }

def _breadth_cached_moves():
    # F&O is served from the same fast sector snapshot used by Sectorial.
    _trigger_sector_snapshot_refresh(force=False)
    with _sector_snapshot_lock:
        sector_rows = dict(_sector_snapshot["rows_by_symbol"])

    # Nifty50 is served from the Top Movers browser cache. Refresh happens in background.
    try:
        _tm_trigger_background_refresh(force=False)
    except Exception:
        pass
    with _top_movers_lock:
        tm_rows = dict(_top_movers_cache["rows"])

    fo_symbols = _sanitize_stock_symbols(sorted({s for items in SECTORS.values() for s in items}))
    fo_moves = {
        s: sector_rows[s].get("move_pct")
        for s in fo_symbols
        if s in sector_rows and sector_rows[s].get("move_pct") is not None
    }
    n50_moves = {
        s: tm_rows[s].get("move_pct")
        for s in _sanitize_stock_symbols(NIFTY50)
        if s in tm_rows and tm_rows[s].get("move_pct") is not None
    }
    return fo_moves, n50_moves

def _record_breadth_point(fo, n50, now):
    date_key = now.date().isoformat()
    # One point per minute is enough because underlying Yahoo stock data is 1-minute.
    time_label = now.strftime("%H:%M")
    history = _load_combined_breadth_history()
    day = history.setdefault(date_key, [])
    point = {
        "time": time_label,
        "fo_advances": fo["advances"],
        "fo_declines": fo["declines"],
        "fo_score": fo["breadth_score"],
        "n50_advances": n50["advances"],
        "n50_declines": n50["declines"],
        "n50_score": n50["breadth_score"],
    }
    if day and day[-1].get("time") == time_label:
        day[-1] = point
    else:
        day.append(point)
    for old_date in sorted(history.keys())[:-BREADTH_HISTORY_DAYS]:
        history.pop(old_date, None)
    _save_combined_breadth_history(history)

def fetch_combined_breadth(force=False):
    with _breadth_lock:
        if not force and _breadth_cache["value"] is not None:
            if time.time() - _breadth_cache["ts"] < BREADTH_CACHE_SECONDS:
                return _breadth_cache["value"]

    fo_moves, n50_moves = _breadth_cached_moves()
    fo = _counts(fo_moves)
    n50 = _counts(n50_moves)
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    _record_breadth_point(fo, n50, now)

    value = {
        "fo": fo,
        "nifty50": n50,
        "updated_at": now.isoformat(),
        "source": "Shared RAM/disk caches — no Yahoo wait in breadth endpoint",
        "effective_refresh_seconds": BREADTH_CACHE_SECONDS,
    }
    with _breadth_lock:
        _breadth_cache["ts"] = time.time()
        _breadth_cache["value"] = value
    return value

@app.get("/api/combined-breadth")
def combined_breadth(force: bool = False):
    return fetch_combined_breadth(force=force)

@app.get("/api/combined-breadth-history")
def combined_breadth_history(trade_date: str | None = None):
    history = _load_combined_breadth_history()
    today = pd.Timestamp.now(tz="Asia/Kolkata").date().isoformat()
    dates = sorted(history.keys())
    chosen = trade_date if trade_date in history else (today if today in history else (dates[-1] if dates else None))
    return {
        "date": chosen,
        "dates": dates,
        "points": history.get(chosen, []) if chosen else [],
        "basis": "live/current session" if chosen == today else "saved historical session",
    }

async def _breadth_history_scheduler():
    while True:
        try:
            now = pd.Timestamp.now(tz="Asia/Kolkata")
            minute = now.hour * 60 + now.minute
            if now.weekday() < 5 and 9*60+15 <= minute <= 15*60+35:
                await asyncio.to_thread(fetch_combined_breadth, True)
        except Exception:
            pass
        await asyncio.sleep(60)

@app.on_event("startup")
async def _start_breadth_history_scheduler():
    asyncio.create_task(_breadth_history_scheduler())

# ============================================================================
# NSE LIVE FUTURES OI
# ============================================================================

FO_UNIVERSE = _sanitize_stock_symbols(sorted({s for items in SECTORS.values() for s in items}))
SYMBOL_TO_SECTOR = {}
for _sector_name, _symbols in SECTORS.items():
    for _symbol in _symbols:
        SYMBOL_TO_SECTOR[_symbol] = _sector_name

NSE_BASE = "https://www.nseindia.com"
NSE_OI_CONTRACTS = NSE_BASE + "/api/live-analysis-oi-spurts-contracts"
NSE_OI_UNDERLYINGS = NSE_BASE + "/api/live-analysis-oi-spurts-underlyings"

_oi_cache = {"ts": 0.0, "value": None}

def _pick(row, *names):
    for name in names:
        if name in row and row.get(name) not in (None, ""):
            return row.get(name)
    return None

def _num(value):
    try:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = value.replace(",", "").replace("%", "").strip()
        return float(value)
    except Exception:
        return None

def _nse_json(url: str):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/oi-spurts",
        "Connection": "keep-alive",
    }
    session = requests.Session()

    # NSE commonly requires cookies from a normal page visit first.
    warm_urls = [
        NSE_BASE + "/market-data/oi-spurts",
        NSE_BASE,
    ]
    last_error = None
    for warm_url in warm_urls:
        try:
            session.get(warm_url, headers=headers, timeout=12)
            response = session.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                return response.json()
            last_error = f"NSE HTTP {response.status_code}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.5)

    raise RuntimeError(last_error or "NSE OI request failed")

def _normalize_oi_rows(payload, source_name: str):
    raw = payload.get("data", []) if isinstance(payload, dict) else []
    rows = []

    for item in raw:
        if not isinstance(item, dict):
            continue

        symbol = _pick(
            item,
            "symbol",
            "underlying",
            "underlyingSymbol",
            "identifier"
        )
        if symbol is None:
            continue

        symbol = str(symbol).strip().upper()

        possible = _pick(item, "underlying", "underlyingSymbol")
        if possible is not None:
            possible = str(possible).strip().upper()
            if possible:
                symbol = possible

        # Keep every NSE underlying returned by OI Spurts.
        # Do not discard newly admitted F&O symbols missing from local sector mapping.

        instrument = _pick(
            item,
            "instrumentType",
            "instrument",
            "instrumentTypeName",
            "instrumentName",
        )
        instrument_text = "" if instrument is None else str(instrument).upper()

        if source_name == "contracts" and instrument_text:
            if "FUTSTK" not in instrument_text and "FUTURE" not in instrument_text:
                continue

        expiry = _pick(item, "expiryDate", "expiry", "expiryDt")

        # NSE "By Underlying" currently exposes fields such as:
        # latestOI, prevOI, changeInOI and avgInOI.
        latest_oi = _num(_pick(
            item,
            "latestOI",
            "openInterest",
            "openInterestContracts",
            "oi"
        ))

        prev_oi = _num(_pick(
            item,
            "prevOI",
            "previousOI",
            "previousOpenInterest"
        ))

        change_oi = _num(_pick(
            item,
            "changeInOI",
            "changeInOi",
            "changeOI",
            "chgInOI",
            "change"
        ))

        pct_oi = _num(_pick(
            item,
            "pChangeInOI",
            "percentChangeInOI",
            "percentChangeInOi",
            "pChangeOI",
            "changeInOIPercent"
        ))

        # Important fallback:
        # NSE underlying feed may not return the percentage field directly.
        # The UI value shown by NSE is:
        # Change in OI / Previous OI * 100.
        if pct_oi is None and change_oi is not None and prev_oi not in (None, 0):
            pct_oi = change_oi / prev_oi * 100.0

        # If change itself is missing but latest/previous exist, calculate it.
        if change_oi is None and latest_oi is not None and prev_oi is not None:
            change_oi = latest_oi - prev_oi

        if pct_oi is None and latest_oi is not None and prev_oi not in (None, 0):
            pct_oi = (latest_oi - prev_oi) / prev_oi * 100.0

        ltp = _num(_pick(
            item,
            "underlyingValue",
            "lastPrice",
            "ltp",
            "last"
        ))

        rows.append({
            "symbol": symbol,
            "sector": SYMBOL_TO_SECTOR.get(symbol, "Other / Unmapped"),
            "oi": latest_oi,
            "latest_oi": latest_oi,
            "prev_oi": prev_oi,
            "change_oi": change_oi,
            "oi_change_pct": pct_oi,
            "ltp": ltp,
            "expiry": "" if expiry is None else str(expiry),
            "source_mode": source_name,
        })

    return rows

def _collapse_contract_rows(rows):
    # Contract endpoint can return multiple expiries. Keep one row per stock.
    # Prefer a row with an OI-change percentage and then the first returned
    # contract (NSE typically orders active/near contracts prominently).
    result = {}
    for row in rows:
        sym = row["symbol"]
        if sym not in result:
            result[sym] = row
            continue

        old = result[sym]
        old_has = old.get("oi_change_pct") is not None
        new_has = row.get("oi_change_pct") is not None
        if new_has and not old_has:
            result[sym] = row

    return list(result.values())

def fetch_live_oi(force: bool = False):
    if not force and _oi_cache["value"] is not None:
        if time.time() - _oi_cache["ts"] < 5:
            return _oi_cache["value"]

    errors = []
    rows = []
    source_mode = "NSE OI Spurts - By Underlying"

    # Primary source for the scanner/pre-market table:
    # NSE Change in Open Interest -> By Underlying.
    try:
        payload = _nse_json(NSE_OI_UNDERLYINGS)
        rows = _normalize_oi_rows(payload, "underlyings")
    except Exception as exc:
        errors.append(f"underlyings: {exc}")

    # Fallback to contract-level data if the underlying endpoint is unavailable.
    if not rows:
        source_mode = "NSE OI Spurts - Contracts fallback"
        try:
            payload = _nse_json(NSE_OI_CONTRACTS)
            rows = _normalize_oi_rows(payload, "contracts")
            rows = _collapse_contract_rows(rows)
        except Exception as exc:
            errors.append(f"contracts: {exc}")

    rows.sort(
        key=lambda r: (
            r.get("oi_change_pct") is None,
            -(r.get("oi_change_pct") or 0.0),
            r["symbol"],
        )
    )

    now = pd.Timestamp.now(tz="Asia/Kolkata").isoformat()
    value = {
        "rows": rows,
        "updated_at": now,
        "source": source_mode,
        "error": "; ".join(errors) if not rows and errors else None,
    }

    _oi_cache["ts"] = time.time()
    _oi_cache["value"] = value
    return value


@app.get("/api/oi")
def live_oi(sector: str = "All F&O", force: bool = False):
    data = fetch_live_oi(force=force)
    rows = data["rows"]

    if sector != "All F&O":
        if sector not in SECTORS:
            raise HTTPException(status_code=404, detail="Unknown sector")
        allowed = set(SECTORS[sector])
        rows = [r for r in rows if r["symbol"] in allowed]

    return {
        **data,
        "rows": rows,
        "sector": sector,
    }




# ============================================================================
# OI SPURT SELECTION
# ============================================================================
# This module is additive. Existing Sectorial / Breadth / Pre-Market / OI
# functionality is left intact.

OI_SPURT_DB = DATA_DIR / "oi_spurt_selection.sqlite3"
OI_SPURT_DEFAULT_TOP_N = 30
OI_SPURT_DEFAULT_KEEP_SESSIONS = 14
OI_SPURT_PRICE_CACHE_SECONDS = 12

_oi_spurt_price_cache = {
    "ts": 0.0,
    "symbols_key": (),
    "rows": {},
}

def _ois_db():
    conn = sqlite3.connect(OI_SPURT_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oi_spurt_sessions (
            trade_date TEXT PRIMARY KEY,
            captured_0922_at TEXT,
            opening_json TEXT,
            eod_at TEXT,
            eod_json TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oi_spurt_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.execute(
        "INSERT OR IGNORE INTO oi_spurt_settings(key,value) VALUES('top_n',?)",
        (str(OI_SPURT_DEFAULT_TOP_N),)
    )
    conn.execute(
        "INSERT OR IGNORE INTO oi_spurt_settings(key,value) VALUES('keep_sessions',?)",
        (str(OI_SPURT_DEFAULT_KEEP_SESSIONS),)
    )
    conn.commit()
    return conn

def _ois_setting_int(key, default_value, minimum, maximum):
    conn = _ois_db()
    row = conn.execute(
        "SELECT value FROM oi_spurt_settings WHERE key=?",
        (key,)
    ).fetchone()
    conn.close()
    try:
        value = int(row[0]) if row else int(default_value)
    except Exception:
        value = int(default_value)
    return max(minimum, min(maximum, value))

def _ois_set_setting(key, value, minimum, maximum):
    value = max(minimum, min(maximum, int(value)))
    conn = _ois_db()
    conn.execute(
        "INSERT INTO oi_spurt_settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value))
    )
    conn.commit()
    conn.close()
    if key == "keep_sessions":
        _ois_prune()
    return value

def _ois_keep_sessions():
    return _ois_setting_int(
        "keep_sessions",
        OI_SPURT_DEFAULT_KEEP_SESSIONS,
        1,
        60
    )

def _ois_top_n():
    return _ois_setting_int(
        "top_n",
        OI_SPURT_DEFAULT_TOP_N,
        5,
        100
    )

def _ois_prune():
    conn = _ois_db()
    dates = [
        r[0] for r in conn.execute(
            "SELECT trade_date FROM oi_spurt_sessions ORDER BY trade_date DESC"
        ).fetchall()
    ]
    keep = _ois_keep_sessions()
    for d in dates[keep:]:
        conn.execute(
            "DELETE FROM oi_spurt_sessions WHERE trade_date=?",
            (d,)
        )
    conn.commit()
    conn.close()

def _ois_dates():
    conn = _ois_db()
    rows = conn.execute("""
        SELECT trade_date,captured_0922_at,eod_at
        FROM oi_spurt_sessions
        ORDER BY trade_date DESC
        LIMIT ?
    """, (_ois_keep_sessions(),)).fetchall()
    conn.close()
    return [
        {
            "trade_date": r[0],
            "captured_0922_at": r[1],
            "eod_at": r[2],
            "eod_saved": bool(r[2]) and bool(_tm_load_history(r[0]).get("rows")),
        }
        for r in rows
    ]

def _ois_load(trade_date):
    conn = _ois_db()
    row = conn.execute("""
        SELECT trade_date,captured_0922_at,opening_json,eod_at,eod_json
        FROM oi_spurt_sessions
        WHERE trade_date=?
    """, (trade_date,)).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "trade_date": row[0],
        "captured_0922_at": row[1],
        "opening_rows": json.loads(row[2]) if row[2] else [],
        "eod_at": row[3],
        "eod_rows": json.loads(row[4]) if row[4] else [],
        "eod_saved": bool(row[3]) and bool(json.loads(row[4]) if row[4] else []),
    }

def _ois_save_opening(trade_date, rows, captured_at):
    conn = _ois_db()
    conn.execute("""
        INSERT INTO oi_spurt_sessions(
            trade_date,captured_0922_at,opening_json
        )
        VALUES(?,?,?)
        ON CONFLICT(trade_date) DO UPDATE SET
            captured_0922_at=COALESCE(
                oi_spurt_sessions.captured_0922_at,
                excluded.captured_0922_at
            ),
            opening_json=CASE
                WHEN oi_spurt_sessions.opening_json IS NULL
                THEN excluded.opening_json
                ELSE oi_spurt_sessions.opening_json
            END
    """, (
        trade_date,
        captured_at,
        json.dumps(rows)
    ))
    conn.commit()
    conn.close()
    _ois_prune()

def _ois_save_eod(trade_date, rows, eod_at):
    conn = _ois_db()
    conn.execute("""
        INSERT INTO oi_spurt_sessions(
            trade_date,eod_at,eod_json
        )
        VALUES(?,?,?)
        ON CONFLICT(trade_date) DO UPDATE SET
            eod_at=excluded.eod_at,
            eod_json=excluded.eod_json
    """, (
        trade_date,
        eod_at,
        json.dumps(rows)
    ))
    conn.commit()
    conn.close()
    _ois_prune()

_oi_spurt_price_cache_lock = threading.Lock()
_ois_price_refresh_lock = threading.Lock()
_ois_price_refreshing = False

def _ois_price_refresh_worker(symbols):
    global _ois_price_refreshing
    try:
        clean = _sanitize_stock_symbols(symbols)
        if not clean:
            return

        yahoo_symbols = [_sym(s) for s in clean]

        data = yf.download(
            tickers=yahoo_symbols,
            period="5d",
            interval="1m",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
            progress=False,
            prepost=False,
        )

        result = {}

        for symbol, yahoo_symbol in zip(clean, yahoo_symbols):
            frame = _single(data, yahoo_symbol)

            if frame.empty or "Close" not in frame or "Open" not in frame:
                continue

            frame = frame.dropna(subset=["Close"]).copy()
            if frame.empty:
                continue

            if getattr(frame.index, "tz", None) is None:
                frame.index = frame.index.tz_localize("UTC").tz_convert("Asia/Kolkata")
            else:
                frame.index = frame.index.tz_convert("Asia/Kolkata")

            frame["_date"] = frame.index.date
            dates = list(pd.unique(frame["_date"]))
            if len(dates) < 2:
                continue

            cur, prev = dates[-1], dates[-2]
            current_day = frame[frame["_date"] == cur]
            previous_day = frame[frame["_date"] == prev]

            if current_day.empty or previous_day.empty:
                continue

            try:
                today_open = float(current_day["Open"].dropna().iloc[0])
                current_price = float(current_day["Close"].dropna().iloc[-1])
                pdc = float(previous_day["Close"].dropna().iloc[-1])
            except Exception:
                continue

            result[symbol] = {
                "gap_pct": ((today_open - pdc) / pdc * 100.0) if pdc else None,
                "move_pct": ((current_price - pdc) / pdc * 100.0) if pdc else None,
                "current_price": current_price,
                "pdc": pdc,
            }

        if result:
            with _oi_spurt_price_cache_lock:
                merged = dict(_oi_spurt_price_cache["rows"])
                merged.update(result)
                _oi_spurt_price_cache["rows"] = merged
                _oi_spurt_price_cache["ts"] = time.time()
    except Exception as exc:
        print("[OISpurtPriceCache] refresh failed:", exc)
    finally:
        with _ois_price_refresh_lock:
            _ois_price_refreshing = False

def _ois_price_map(symbols, force=False):
    global _ois_price_refreshing

    clean = _sanitize_stock_symbols(
        sorted({str(s).upper() for s in symbols if s})
    )
    if not clean:
        return {}

    with _oi_spurt_price_cache_lock:
        age = (
            time.time() - _oi_spurt_price_cache["ts"]
            if _oi_spurt_price_cache["ts"] else 999999
        )
        cached = {
            s: dict(_oi_spurt_price_cache["rows"][s])
            for s in clean
            if s in _oi_spurt_price_cache["rows"]
        }

    needed = (
        force
        or age >= OI_SPURT_PRICE_CACHE_SECONDS
        or len(cached) < len(clean)
    )

    if needed:
        with _ois_price_refresh_lock:
            if not _ois_price_refreshing:
                _ois_price_refreshing = True
                threading.Thread(
                    target=_ois_price_refresh_worker,
                    args=(clean,),
                    daemon=True
                ).start()

    return cached


def _ois_build_rows(top_n=None, force_oi=False, all_rows=False):
    data = fetch_live_oi(force=force_oi)
    oi_rows = list(data.get("rows", []))

    # Only rows with a valid percentage can participate in the ranked table.
    oi_rows = [
        r for r in oi_rows
        if r.get("symbol") and r.get("oi_change_pct") is not None
    ]

    oi_rows.sort(
        key=lambda r: (
            -(r.get("oi_change_pct") or 0.0),
            r.get("symbol", "")
        )
    )

    if all_rows:
        selected = oi_rows
    else:
        n = int(top_n or _ois_top_n())
        n = max(5, min(100, n))
        selected = oi_rows[:n]

    symbols = [r["symbol"] for r in selected]
    prices = _ois_price_map(symbols)

    rows = []
    for rank, oi_row in enumerate(selected, start=1):
        symbol = oi_row["symbol"]
        price = prices.get(symbol, {})
        rows.append({
            "rank": rank,
            "symbol": symbol,
            "sector": SYMBOL_TO_SECTOR.get(
                symbol,
                oi_row.get("sector") or "Other / Unmapped"
            ),
            "oi_change_pct": oi_row.get("oi_change_pct"),
            "change_oi": oi_row.get("change_oi"),
            "oi": oi_row.get("oi"),
            "gap_pct": price.get("gap_pct"),
            "move_pct": price.get("move_pct"),
            "current_price": price.get("current_price"),
            "pdc": price.get("pdc"),
        })

    return {
        "rows": rows,
        "updated_at": data.get(
            "updated_at",
            pd.Timestamp.now(tz="Asia/Kolkata").isoformat()
        ),
        "source": data.get("source", "NSE OI Spurts - By Underlying"),
        "error": data.get("error"),
    }

def _ois_capture_0922_if_needed():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    trade_date = now.date().isoformat()

    snap = _ois_load(trade_date)
    if snap and snap.get("captured_0922_at"):
        return snap

    data = _ois_build_rows(
        top_n=100,
        force_oi=True,
        all_rows=True
    )
    _ois_save_opening(
        trade_date,
        data["rows"],
        now.isoformat()
    )
    return _ois_load(trade_date)

def _ois_save_today_eod():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    trade_date = now.date().isoformat()

    data = _ois_build_rows(
        top_n=100,
        force_oi=True,
        all_rows=True
    )
    _ois_save_eod(
        trade_date,
        data["rows"],
        now.isoformat()
    )
    return _ois_load(trade_date)

async def _oi_spurt_scheduler():
    # 09:22 IST: store first OI-spurt snapshot once.
    # 15:32 IST: store final EOD table once.
    while True:
        sleep_seconds = 20
        try:
            now = pd.Timestamp.now(tz="Asia/Kolkata")
            weekday = now.weekday() < 5
            minutes = now.hour * 60 + now.minute

            if weekday:
                trade_date = now.date().isoformat()
                snap = _ois_load(trade_date)

                if minutes >= 9 * 60 + 22 and minutes < 15 * 60 + 30:
                    if not snap or not snap.get("captured_0922_at"):
                        try:
                            await asyncio.to_thread(
                                _ois_capture_0922_if_needed
                            )
                        except Exception:
                            pass

                snap = _ois_load(trade_date)
                if minutes >= 15 * 60 + 32:
                    if not snap or not snap.get("eod_saved"):
                        try:
                            await asyncio.to_thread(
                                _ois_save_today_eod
                            )
                        except Exception:
                            pass

        except Exception:
            pass

        await asyncio.sleep(sleep_seconds)



@app.on_event("startup")
async def _warm_top_movers_v19():
    _tm_trigger_background_refresh(force=False)

@app.on_event("startup")
async def _warm_sector_snapshot_v18():
    # Do not await network. Browser can use the persistent cache immediately.
    _trigger_sector_snapshot_refresh(force=False)

@app.on_event("startup")
async def _start_oi_spurt_scheduler():
    asyncio.create_task(_oi_spurt_scheduler())

@app.get("/api/oi-spurt/settings")
def oi_spurt_settings():
    return {
        "top_n": _ois_top_n(),
        "keep_sessions": _ois_keep_sessions(),
        "live_refresh_seconds": 5,
        "price_cache_seconds": OI_SPURT_PRICE_CACHE_SECONDS,
    }

@app.post("/api/oi-spurt/settings")
def update_oi_spurt_settings(
    top_n: int = OI_SPURT_DEFAULT_TOP_N,
    keep_sessions: int = OI_SPURT_DEFAULT_KEEP_SESSIONS
):
    return {
        "top_n": _ois_set_setting(
            "top_n",
            top_n,
            5,
            100
        ),
        "keep_sessions": _ois_set_setting(
            "keep_sessions",
            keep_sessions,
            1,
            60
        ),
    }

@app.get("/api/oi-spurt/live")
def oi_spurt_live(
    top_n: int = OI_SPURT_DEFAULT_TOP_N,
    force: bool = False
):
    return _ois_build_rows(
        top_n=top_n,
        force_oi=force,
        all_rows=False
    )

@app.get("/api/oi-spurt/dates")
def oi_spurt_dates():
    return {"dates": _ois_dates()}

@app.get("/api/oi-spurt/history/{trade_date}")
def oi_spurt_history(trade_date: str):
    snap = _ois_load(trade_date)
    if not snap:
        raise HTTPException(
            status_code=404,
            detail="No OI-spurt snapshot saved for this date"
        )

    rows = snap["eod_rows"] if snap["eod_rows"] else snap["opening_rows"]

    return {
        "trade_date": trade_date,
        "rows": rows,
        "captured_0922_at": snap["captured_0922_at"],
        "eod_at": snap["eod_at"],
        "eod_saved": snap["eod_saved"],
        "basis": "EOD" if snap["eod_rows"] else "09:22 snapshot",
    }

@app.post("/api/oi-spurt/capture")
def oi_spurt_capture():
    return _ois_capture_0922_if_needed()

@app.post("/api/oi-spurt/eod")
def oi_spurt_eod_save():
    return _ois_save_today_eod()


# ============================================================================
# V13 OI-SPURT COMMON SNAPSHOT + PREVIOUS-DATA STOCK SELECTION
# ============================================================================

def _ois_ensure_v13_schema():
    conn = _ois_db()

    existing = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info(oi_spurt_sessions)"
        ).fetchall()
    }

    additions = {
        "common_capture_json": "TEXT",
        "common_eod_json": "TEXT",
    }

    for col, sql_type in additions.items():
        if col not in existing:
            conn.execute(
                f"ALTER TABLE oi_spurt_sessions ADD COLUMN {col} {sql_type}"
            )

    defaults = [
        ("capture_hour", "9"),
        ("capture_minute", "22"),
        ("preopen_top_n", "30"),
    ]

    for key, value in defaults:
        conn.execute(
            "INSERT OR IGNORE INTO oi_spurt_settings(key,value) VALUES(?,?)",
            (key, value)
        )

    conn.commit()
    conn.close()

_ois_ensure_v13_schema()

def _ois_capture_hour():
    return _ois_setting_int("capture_hour", 9, 0, 23)

def _ois_capture_minute():
    return _ois_setting_int("capture_minute", 22, 0, 59)

def _ois_preopen_top_n():
    return _ois_setting_int("preopen_top_n", 30, 5, 100)

def _ois_get_common_columns(trade_date):
    conn = _ois_db()
    row = conn.execute("""
        SELECT common_capture_json, common_eod_json
        FROM oi_spurt_sessions
        WHERE trade_date=?
    """, (trade_date,)).fetchone()
    conn.close()

    if not row:
        return {
            "common_capture_rows": [],
            "common_eod_rows": [],
        }

    return {
        "common_capture_rows": json.loads(row[0]) if row[0] else [],
        "common_eod_rows": json.loads(row[1]) if row[1] else [],
    }

def _ois_save_common_rows(trade_date, rows, eod=False):
    conn = _ois_db()
    column = "common_eod_json" if eod else "common_capture_json"

    conn.execute(
        f"""
        INSERT INTO oi_spurt_sessions(trade_date,{column})
        VALUES(?,?)
        ON CONFLICT(trade_date) DO UPDATE SET
            {column}=excluded.{column}
        """,
        (trade_date, json.dumps(rows))
    )
    conn.commit()
    conn.close()

def _ois_ranked_preopen_rows(trade_date, preopen_top_n=None):
    snap = _pm_load(trade_date)
    if not snap:
        return []

    limit = int(preopen_top_n or _ois_preopen_top_n())
    limit = max(5, min(100, limit))

    rows = list(snap.get("rows", []))

    # Pre-market scanner rows already represent the Value Top-N ∩
    # Futures-Volume Top-N set. Use combined rank as the adjustable limit.
    rows = [
        r for r in rows
        if r.get("symbol")
        and (r.get("combined_rank") is None or int(r.get("combined_rank")) <= limit)
    ]

    return rows

def _ois_ranked_opening_rows(trade_date, oi_top_n=None):
    snap = _ois_load(trade_date)
    if not snap:
        return []

    limit = int(oi_top_n or _ois_top_n())
    limit = max(5, min(100, limit))

    rows = list(snap.get("opening_rows", []))
    rows.sort(
        key=lambda r: (
            -(r.get("oi_change_pct") or -999999),
            r.get("symbol", "")
        )
    )

    return rows[:limit]

def _ois_build_preopen_oi_common(
    trade_date,
    oi_rows=None,
    preopen_rows=None,
    oi_top_n=None,
    preopen_top_n=None
):
    if oi_rows is None:
        oi_rows = _ois_ranked_opening_rows(
            trade_date,
            oi_top_n=oi_top_n
        )

    if preopen_rows is None:
        preopen_rows = _ois_ranked_preopen_rows(
            trade_date,
            preopen_top_n=preopen_top_n
        )

    oi_map = {
        str(r.get("symbol", "")).upper(): r
        for r in oi_rows
        if r.get("symbol")
    }

    pre_map = {
        str(r.get("symbol", "")).upper(): r
        for r in preopen_rows
        if r.get("symbol")
    }

    common_symbols = sorted(
        set(oi_map.keys()) & set(pre_map.keys())
    )

    result = []

    for symbol in common_symbols:
        oi = oi_map[symbol]
        pre = pre_map[symbol]

        result.append({
            "symbol": symbol,
            "sector": SYMBOL_TO_SECTOR.get(
                symbol,
                oi.get("sector") or "Other / Unmapped"
            ),
            "oi_rank": oi.get("rank"),
            "oi_change_pct": oi.get("oi_change_pct"),
            "gap_pct": oi.get("gap_pct", pre.get("gap_pct")),
            "move_pct": oi.get("move_pct", pre.get("live_pct")),
            "current_price": oi.get("current_price"),
            "pdc": oi.get("pdc"),
            "value_rank": pre.get("value_rank"),
            "volume_rank": pre.get("volume_rank"),
            "combined_rank": pre.get("combined_rank"),
        })

    result.sort(
        key=lambda r: (
            r.get("combined_rank") if r.get("combined_rank") is not None else 999,
            r.get("oi_rank") if r.get("oi_rank") is not None else 999,
            r.get("symbol", "")
        )
    )

    return result

def _ois_capture_with_common_if_needed():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    trade_date = now.date().isoformat()

    snap = _ois_capture_0922_if_needed()

    # Use today's saved opening rows + today's saved pre-market snapshot.
    oi_rows = _ois_ranked_opening_rows(
        trade_date,
        oi_top_n=_ois_top_n()
    )
    pre_rows = _ois_ranked_preopen_rows(
        trade_date,
        preopen_top_n=_ois_preopen_top_n()
    )

    common = _ois_build_preopen_oi_common(
        trade_date,
        oi_rows=oi_rows,
        preopen_rows=pre_rows
    )

    _ois_save_common_rows(
        trade_date,
        common,
        eod=False
    )

    result = _ois_load(trade_date) or {}
    result.update(_ois_get_common_columns(trade_date))
    return result

def _ois_save_eod_with_common():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    trade_date = now.date().isoformat()

    snap = _ois_save_today_eod()

    eod_rows = list(
        (snap or {}).get("eod_rows", [])
    )

    # Preserve the capture-time Top-N OI/pre-market membership, but enrich
    # the saved common list with EOD OI/price data.
    capture_common = _ois_get_common_columns(
        trade_date
    ).get("common_capture_rows", [])

    if not capture_common:
        capture_common = _ois_build_preopen_oi_common(
            trade_date
        )

    eod_map = {
        str(r.get("symbol", "")).upper(): r
        for r in eod_rows
        if r.get("symbol")
    }

    final_common = []

    for row in capture_common:
        r = dict(row)
        symbol = str(r.get("symbol", "")).upper()
        eod = eod_map.get(symbol, {})

        if eod:
            r["oi_change_pct"] = eod.get(
                "oi_change_pct",
                r.get("oi_change_pct")
            )
            r["gap_pct"] = eod.get(
                "gap_pct",
                r.get("gap_pct")
            )
            r["move_pct"] = eod.get(
                "move_pct",
                r.get("move_pct")
            )
            r["current_price"] = eod.get(
                "current_price",
                r.get("current_price")
            )
            r["pdc"] = eod.get(
                "pdc",
                r.get("pdc")
            )

        final_common.append(r)

    _ois_save_common_rows(
        trade_date,
        final_common,
        eod=True
    )

    result = _ois_load(trade_date) or {}
    result.update(_ois_get_common_columns(trade_date))
    return result

def _ois_historical_row_map(trade_date):
    snap = _ois_load(trade_date)
    if not snap:
        return {}

    rows = snap.get("eod_rows") or snap.get("opening_rows") or []

    return {
        str(r.get("symbol", "")).upper(): r
        for r in rows
        if r.get("symbol")
    }

def _pm_historical_row_map(trade_date):
    snap = _pm_load(trade_date)
    if not snap:
        return {}

    return {
        str(r.get("symbol", "")).upper(): r
        for r in snap.get("rows", [])
        if r.get("symbol")
    }

def _historical_net_move(symbol, start_date, end_date):
    start_oi = _ois_historical_row_map(start_date).get(symbol)
    end_oi = _ois_historical_row_map(end_date).get(symbol)

    start_pm = _pm_historical_row_map(start_date).get(symbol)
    end_pm = _pm_historical_row_map(end_date).get(symbol)

    start_pdc = None
    end_close = None

    if start_oi:
        start_pdc = start_oi.get("pdc")

    if start_pdc is None and start_pm:
        start_pdc = start_pm.get("pdc")

    if end_oi:
        end_close = end_oi.get("current_price")

    if end_close is None and end_pm:
        # EOD pre-market enrichment stores current/closing move, but can also
        # carry explicit current_price when available.
        end_close = end_pm.get("current_price")

    # Same-day fallback: the saved daily move is exactly the move from PDC.
    if start_date == end_date:
        row = end_oi or end_pm
        if row:
            value = row.get("move_pct")
            if value is None:
                value = row.get("live_pct")
            if value is not None:
                return float(value)

    if (
        start_pdc is None
        or end_close is None
        or float(start_pdc) == 0
    ):
        return None

    return (
        (float(end_close) - float(start_pdc))
        / float(start_pdc)
        * 100.0
    )

def _previous_data_selection(
    start_date,
    end_date,
    oi_top_n,
    preopen_top_n,
    min_common_sessions,
    max_abs_net_move
):
    dates = [
        d["trade_date"]
        for d in _ois_dates()
        if d["trade_date"] >= start_date
        and d["trade_date"] <= end_date
    ]

    dates = sorted(dates)

    if not dates:
        return {
            "dates": [],
            "rows": [],
        }

    appearances = {}
    latest_detail = {}

    for trade_date in dates:
        oi_rows = _ois_ranked_opening_rows(
            trade_date,
            oi_top_n=oi_top_n
        )
        pre_rows = _ois_ranked_preopen_rows(
            trade_date,
            preopen_top_n=preopen_top_n
        )

        common = _ois_build_preopen_oi_common(
            trade_date,
            oi_rows=oi_rows,
            preopen_rows=pre_rows
        )

        for row in common:
            symbol = row["symbol"]
            appearances.setdefault(
                symbol,
                []
            ).append(trade_date)

            latest_detail[symbol] = dict(row)

    result = []

    required = max(
        1,
        min(
            int(min_common_sessions),
            len(dates)
        )
    )

    for symbol, common_dates in appearances.items():
        if len(common_dates) < required:
            continue

        detail = latest_detail.get(symbol, {})

        net_move = _historical_net_move(
            symbol,
            start_date,
            end_date
        )

        if (
            max_abs_net_move is not None
            and net_move is not None
            and abs(float(net_move)) > float(max_abs_net_move)
        ):
            continue

        result.append({
            "symbol": symbol,
            "sector": SYMBOL_TO_SECTOR.get(
                symbol,
                detail.get("sector") or "Other / Unmapped"
            ),
            "common_sessions": len(common_dates),
            "common_dates": common_dates,
            "latest_oi_change_pct": detail.get(
                "oi_change_pct"
            ),
            "latest_gap_pct": detail.get(
                "gap_pct"
            ),
            "latest_close_move_pct": detail.get(
                "move_pct"
            ),
            "period_net_move_pct": net_move,
            "latest_combined_rank": detail.get(
                "combined_rank"
            ),
            "latest_oi_rank": detail.get(
                "oi_rank"
            ),
        })

    result.sort(
        key=lambda r: (
            -r["common_sessions"],
            abs(r["period_net_move_pct"])
            if r["period_net_move_pct"] is not None
            else 999999,
            r["symbol"]
        )
    )

    return {
        "dates": dates,
        "rows": result,
        "start_date": start_date,
        "end_date": end_date,
        "oi_top_n": oi_top_n,
        "preopen_top_n": preopen_top_n,
        "min_common_sessions": required,
        "max_abs_net_move": max_abs_net_move,
    }

# Replace the v12 OI scheduler with a configurable capture-time scheduler.
async def _oi_spurt_scheduler_v13():
    while True:
        sleep_seconds = 15
        try:
            now = pd.Timestamp.now(tz="Asia/Kolkata")
            weekday = now.weekday() < 5
            minutes = now.hour * 60 + now.minute

            capture_minutes = (
                _ois_capture_hour() * 60
                + _ois_capture_minute()
            )

            if weekday:
                trade_date = now.date().isoformat()
                snap = _ois_load(trade_date)

                if (
                    minutes >= capture_minutes
                    and minutes < 15 * 60 + 30
                ):
                    common_state = _ois_get_common_columns(
                        trade_date
                    )

                    if (
                        not snap
                        or not snap.get("captured_0922_at")
                        or not common_state.get(
                            "common_capture_rows"
                        )
                    ):
                        try:
                            await asyncio.to_thread(
                                _ois_capture_with_common_if_needed
                            )
                        except Exception:
                            pass

                snap = _ois_load(trade_date)
                common_state = _ois_get_common_columns(
                    trade_date
                )

                if minutes >= 15 * 60 + 32:
                    if (
                        not snap
                        or not snap.get("eod_saved")
                        or not common_state.get(
                            "common_eod_rows"
                        )
                    ):
                        try:
                            await asyncio.to_thread(
                                _ois_save_eod_with_common
                            )
                        except Exception:
                            pass

        except Exception:
            pass

        await asyncio.sleep(sleep_seconds)

@app.on_event("startup")
async def _start_oi_spurt_scheduler_v13():
    asyncio.create_task(
        _oi_spurt_scheduler_v13()
    )

@app.get("/api/oi-spurt/v13-settings")
def oi_spurt_v13_settings():
    return {
        "capture_hour": _ois_capture_hour(),
        "capture_minute": _ois_capture_minute(),
        "top_n": _ois_top_n(),
        "preopen_top_n": _ois_preopen_top_n(),
        "keep_sessions": _ois_keep_sessions(),
    }

@app.post("/api/oi-spurt/v13-settings")
def update_oi_spurt_v13_settings(
    capture_hour: int,
    capture_minute: int,
    top_n: int,
    preopen_top_n: int,
    keep_sessions: int
):
    return {
        "capture_hour": _ois_set_setting(
            "capture_hour",
            capture_hour,
            0,
            23
        ),
        "capture_minute": _ois_set_setting(
            "capture_minute",
            capture_minute,
            0,
            59
        ),
        "top_n": _ois_set_setting(
            "top_n",
            top_n,
            5,
            100
        ),
        "preopen_top_n": _ois_set_setting(
            "preopen_top_n",
            preopen_top_n,
            5,
            100
        ),
        "keep_sessions": _ois_set_setting(
            "keep_sessions",
            keep_sessions,
            1,
            60
        ),
    }

@app.get("/api/oi-spurt/common-history/{trade_date}")
def oi_spurt_common_history(trade_date: str):
    snap = _ois_load(trade_date)
    if not snap:
        raise HTTPException(
            status_code=404,
            detail="No OI-spurt session for this date"
        )

    common = _ois_get_common_columns(
        trade_date
    )

    return {
        "trade_date": trade_date,
        "capture_rows": common.get(
            "common_capture_rows",
            []
        ),
        "eod_rows": common.get(
            "common_eod_rows",
            []
        ),
        "basis": (
            "EOD"
            if common.get("common_eod_rows")
            else "Capture"
        ),
    }

@app.post("/api/oi-spurt/capture-with-common")
def oi_spurt_capture_with_common():
    return _ois_capture_with_common_if_needed()

@app.post("/api/oi-spurt/eod-with-common")
def oi_spurt_eod_with_common():
    return _ois_save_eod_with_common()

@app.get("/api/previous-data-selection")
def previous_data_selection(
    start_date: str,
    end_date: str,
    oi_top_n: int = 30,
    preopen_top_n: int = 30,
    min_common_sessions: int = 1,
    max_abs_net_move: float | None = None
):
    if start_date > end_date:
        raise HTTPException(
            status_code=400,
            detail="Start date must be before or equal to end date"
        )

    return _previous_data_selection(
        start_date=start_date,
        end_date=end_date,
        oi_top_n=max(5, min(100, oi_top_n)),
        preopen_top_n=max(5, min(100, preopen_top_n)),
        min_common_sessions=max(1, min_common_sessions),
        max_abs_net_move=max_abs_net_move,
    )


# ============================================================================
# TOP GAINERS / LOSERS SELECTION (V14)
# ============================================================================
# Independent module. It does not change existing scanner logic.

TOP_MOVERS_DB = DATA_DIR / "top_movers_history.sqlite3"
TOP_MOVERS_LIVE_CACHE_FILE = DATA_DIR / "top_movers_live_cache.json"
TOP_MOVERS_DEFAULT_TOP_N = 10
TOP_MOVERS_DEFAULT_KEEP = 14

_top_movers_cache = {
    "ts": 0.0,
    "rows": {},
    "updated_at": None,
    "refreshing": False,
}
_top_movers_lock = threading.Lock()

_tm_details_lock = threading.Lock()
_tm_details_cache = {}
_tm_details_refreshing = set()

def _tm_details_worker(symbols):
    clean = _sanitize_stock_symbols(symbols)
    try:
        with _top_movers_lock:
            base = {
                s: dict(_top_movers_cache["rows"][s])
                for s in clean
                if s in _top_movers_cache["rows"]
            }

        if not base:
            return

        rows = _tm_intraday_flags(
            list(base.keys()),
            base
        )

        now_ts = time.time()
        with _tm_details_lock:
            for row in rows:
                _tm_details_cache[row["symbol"]] = {
                    "ts": now_ts,
                    "row": row,
                }
    except Exception as exc:
        print("[TopMoversDetails] refresh failed:", exc)
    finally:
        with _tm_details_lock:
            for s in clean:
                _tm_details_refreshing.discard(s)

def _tm_cached_details(symbols, force=False):
    clean = _sanitize_stock_symbols(symbols)
    now_ts = time.time()
    result = []
    refresh = []

    with _tm_details_lock:
        for s in clean:
            item = _tm_details_cache.get(s)

            if item:
                result.append(dict(item["row"]))

            stale = (
                item is None
                or now_ts - item["ts"] >= 10
            )

            if (force or stale) and s not in _tm_details_refreshing:
                _tm_details_refreshing.add(s)
                refresh.append(s)

    if refresh:
        threading.Thread(
            target=_tm_details_worker,
            args=(refresh,),
            daemon=True
        ).start()

    return result



def _tm_restore_live_cache():
    try:
        if not TOP_MOVERS_LIVE_CACHE_FILE.exists():
            return
        payload = json.loads(
            TOP_MOVERS_LIVE_CACHE_FILE.read_text(encoding="utf-8")
        )
        rows = payload.get("rows", {})
        if isinstance(rows, dict) and rows:
            with _top_movers_lock:
                _top_movers_cache["rows"] = rows
                _top_movers_cache["updated_at"] = payload.get("updated_at")
                _top_movers_cache["ts"] = 0.0
    except Exception:
        pass

def _tm_write_live_cache():
    try:
        with _top_movers_lock:
            payload = {
                "updated_at": _top_movers_cache["updated_at"],
                "rows": dict(_top_movers_cache["rows"]),
            }
        tmp = TOP_MOVERS_LIVE_CACHE_FILE.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8"
        )
        tmp.replace(TOP_MOVERS_LIVE_CACHE_FILE)
    except Exception:
        pass

_tm_restore_live_cache()

_tm_refresh_lock = threading.Lock()
_tm_refreshing = False

def _tm_background_refresh_worker():
    global _tm_refreshing
    try:
        _tm_build_daily_snapshot(force=True)
    except Exception as exc:
        print("[TopMoversCache] refresh failed:", exc)
    finally:
        with _tm_refresh_lock:
            _tm_refreshing = False

def _tm_trigger_background_refresh(force=False):
    global _tm_refreshing

    refresh_seconds = _tm_settings()["refresh_seconds"]

    with _top_movers_lock:
        has_data = bool(_top_movers_cache["rows"])
        age = (
            time.time() - _top_movers_cache["ts"]
            if _top_movers_cache["ts"]
            else 999999
        )

    if not (force or not has_data or age >= refresh_seconds):
        return

    with _tm_refresh_lock:
        if _tm_refreshing:
            return
        _tm_refreshing = True

    threading.Thread(
        target=_tm_background_refresh_worker,
        daemon=True
    ).start()


def _tm_db():
    conn = sqlite3.connect(TOP_MOVERS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS top_movers_sessions (
            trade_date TEXT PRIMARY KEY,
            eod_at TEXT NOT NULL,
            rows_json TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS top_movers_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    defaults = [
        ("top_n", str(TOP_MOVERS_DEFAULT_TOP_N)),
        ("keep_sessions", str(TOP_MOVERS_DEFAULT_KEEP)),
        ("start_hour", "9"),
        ("start_minute", "15"),
        ("end_hour", "15"),
        ("end_minute", "15"),
        ("refresh_seconds", "15"),
        ("max_gainer_pct", "100"),
        ("max_loser_abs_pct", "100"),
    ]
    for key, value in defaults:
        conn.execute(
            "INSERT OR IGNORE INTO top_movers_settings(key,value) VALUES(?,?)",
            (key, value)
        )
    conn.commit()
    return conn


def _tm_ensure_v15_schema():
    conn = _tm_db()

    columns = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info(top_movers_sessions)"
        ).fetchall()
    }

    additions = {
        "snapshot_at": "TEXT",
        "snapshot_rows_json": "TEXT",
    }

    for col, sql_type in additions.items():
        if col not in columns:
            conn.execute(
                f"ALTER TABLE top_movers_sessions ADD COLUMN {col} {sql_type}"
            )

    defaults = [
        ("snapshot_hour", "10"),
        ("snapshot_minute", "0"),
    ]

    for key, value in defaults:
        conn.execute(
            "INSERT OR IGNORE INTO top_movers_settings(key,value) VALUES(?,?)",
            (key, value)
        )

    conn.commit()
    conn.close()

_tm_ensure_v15_schema()

def _tm_setting_int(key, default, minimum, maximum):
    conn = _tm_db()
    row = conn.execute(
        "SELECT value FROM top_movers_settings WHERE key=?",
        (key,)
    ).fetchone()
    conn.close()
    try:
        value = int(float(row[0])) if row else int(default)
    except Exception:
        value = int(default)
    return max(minimum, min(maximum, value))

def _tm_setting_float(key, default, minimum, maximum):
    conn = _tm_db()
    row = conn.execute(
        "SELECT value FROM top_movers_settings WHERE key=?",
        (key,)
    ).fetchone()
    conn.close()
    try:
        value = float(row[0]) if row else float(default)
    except Exception:
        value = float(default)
    return max(minimum, min(maximum, value))

def _tm_set(key, value):
    conn = _tm_db()
    conn.execute(
        "INSERT INTO top_movers_settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value))
    )
    conn.commit()
    conn.close()

def _tm_settings():
    return {
        "top_n": _tm_setting_int("top_n", 10, 1, 50),
        "keep_sessions": _tm_setting_int("keep_sessions", 14, 1, 60),
        "start_hour": _tm_setting_int("start_hour", 9, 0, 23),
        "start_minute": _tm_setting_int("start_minute", 15, 0, 59),
        "end_hour": _tm_setting_int("end_hour", 15, 0, 23),
        "end_minute": _tm_setting_int("end_minute", 15, 0, 59),
        "refresh_seconds": _tm_setting_int("refresh_seconds", 15, 5, 120),
        "max_gainer_pct": _tm_setting_float("max_gainer_pct", 100, 0, 1000),
        "max_loser_abs_pct": _tm_setting_float("max_loser_abs_pct", 100, 0, 1000),
        "snapshot_hour": _tm_setting_int("snapshot_hour", 10, 0, 23),
        "snapshot_minute": _tm_setting_int("snapshot_minute", 0, 0, 59),
    }

def _tm_save_settings(
    top_n,
    keep_sessions,
    start_hour,
    start_minute,
    end_hour,
    end_minute,
    refresh_seconds,
    max_gainer_pct,
    max_loser_abs_pct,
    snapshot_hour,
    snapshot_minute,
):
    values = {
        "top_n": max(1, min(50, int(top_n))),
        "keep_sessions": max(1, min(60, int(keep_sessions))),
        "start_hour": max(0, min(23, int(start_hour))),
        "start_minute": max(0, min(59, int(start_minute))),
        "end_hour": max(0, min(23, int(end_hour))),
        "end_minute": max(0, min(59, int(end_minute))),
        "refresh_seconds": max(5, min(120, int(refresh_seconds))),
        "max_gainer_pct": max(0.0, min(1000.0, float(max_gainer_pct))),
        "max_loser_abs_pct": max(0.0, min(1000.0, float(max_loser_abs_pct))),
        "snapshot_hour": max(0, min(23, int(snapshot_hour))),
        "snapshot_minute": max(0, min(59, int(snapshot_minute))),
    }
    for k, v in values.items():
        _tm_set(k, v)
    _tm_prune()
    return values

def _tm_prune():
    conn = _tm_db()
    dates = [
        r[0] for r in conn.execute(
            "SELECT trade_date FROM top_movers_sessions ORDER BY trade_date DESC"
        ).fetchall()
    ]
    keep = _tm_setting_int("keep_sessions", 14, 1, 60)
    for d in dates[keep:]:
        conn.execute(
            "DELETE FROM top_movers_sessions WHERE trade_date=?",
            (d,)
        )
    conn.commit()
    conn.close()

def _tm_dates():
    conn = _tm_db()
    rows = conn.execute("""
        SELECT trade_date,snapshot_at,eod_at
        FROM top_movers_sessions
        ORDER BY trade_date DESC
        LIMIT ?
    """, (_tm_setting_int("keep_sessions", 14, 1, 60),)).fetchall()
    conn.close()

    return [
        {
            "trade_date": r[0],
            "snapshot_at": r[1],
            "eod_at": r[2],
            "snapshot_saved": bool(r[1]),
            "eod_saved": bool(r[2]) and bool(_tm_load_history(r[0]).get("rows")),
        }
        for r in rows
    ]


def _tm_load_history(trade_date):
    conn = _tm_db()
    row = conn.execute("""
        SELECT trade_date,snapshot_at,snapshot_rows_json,eod_at,rows_json
        FROM top_movers_sessions
        WHERE trade_date=?
    """, (trade_date,)).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "trade_date": row[0],
        "snapshot_at": row[1],
        "snapshot_rows": json.loads(row[2]) if row[2] else [],
        "eod_at": row[3],
        "rows": json.loads(row[4]) if row[4] else [],
        "snapshot_saved": bool(row[1]),
        "eod_saved": bool(row[3]) and bool(json.loads(row[4]) if row[4] else []),
    }


def _tm_save_history(trade_date, rows):
    now = pd.Timestamp.now(tz="Asia/Kolkata").isoformat()
    conn = _tm_db()

    existing = conn.execute(
        "SELECT trade_date FROM top_movers_sessions WHERE trade_date=?",
        (trade_date,)
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE top_movers_sessions
            SET eod_at=?, rows_json=?
            WHERE trade_date=?
        """, (now, json.dumps(rows), trade_date))
    else:
        # Legacy schema has eod_at/rows_json NOT NULL, so for a brand-new EOD
        # record both are populated normally.
        conn.execute("""
            INSERT INTO top_movers_sessions(
                trade_date,eod_at,rows_json
            )
            VALUES(?,?,?)
        """, (trade_date, now, json.dumps(rows)))

    conn.commit()
    conn.close()
    _tm_prune()

def _tm_save_timed_snapshot(trade_date, rows):
    now = pd.Timestamp.now(tz="Asia/Kolkata").isoformat()
    conn = _tm_db()

    existing = conn.execute(
        "SELECT trade_date,eod_at,rows_json FROM top_movers_sessions WHERE trade_date=?",
        (trade_date,)
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE top_movers_sessions
            SET snapshot_at=?, snapshot_rows_json=?
            WHERE trade_date=?
        """, (now, json.dumps(rows), trade_date))
    else:
        # Backward-compatible insert for the old NOT NULL EOD columns:
        # placeholder EOD fields are inserted, then treated as unsaved because
        # we immediately null them after insertion is impossible under old
        # constraint. Therefore we use snapshot rows as placeholders but keep
        # a separate snapshot_at flag; later EOD overwrite replaces them.
        conn.execute("""
            INSERT INTO top_movers_sessions(
                trade_date,eod_at,rows_json,snapshot_at,snapshot_rows_json
            )
            VALUES(?,?,?,?,?)
        """, (
            trade_date,
            now,
            json.dumps([]),
            now,
            json.dumps(rows)
        ))

    conn.commit()
    conn.close()
    _tm_prune()


def _tm_all_symbols():
    return _sanitize_stock_symbols(
        sorted(
            set(NIFTY500)
            | set(FO_UNIVERSE)
            | set(NIFTY100)
            | set(NIFTY50)
        )
    )

def _tm_sector_for_symbol(symbol):
    if symbol in SYMBOL_TO_SECTOR:
        return SYMBOL_TO_SECTOR[symbol]
    return INDEX_INDUSTRY_MAP.get(symbol, "Other / Unmapped")

def _tm_universe_symbols(universe, sector=None):
    if universe == "Nifty 50":
        symbols = _sanitize_stock_symbols(list(NIFTY50))
    elif universe == "F&O":
        symbols = _sanitize_stock_symbols(list(FO_UNIVERSE))
    elif universe == "Nifty 100":
        symbols = _sanitize_stock_symbols(list(NIFTY100))
    elif universe == "Nifty 500":
        symbols = _sanitize_stock_symbols(list(NIFTY500))
    elif universe == "Sector":
        symbols = _tm_all_symbols()
    else:
        symbols = _sanitize_stock_symbols(list(NIFTY50))

    if sector and sector != "All Sectors":
        symbols = [
            s for s in symbols
            if _tm_sector_for_symbol(s) == sector
        ]
    return sorted(set(symbols))

def _tm_window_active():
    s = _tm_settings()
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    if now.weekday() >= 5:
        return False
    minute = now.hour * 60 + now.minute
    start = s["start_hour"] * 60 + s["start_minute"]
    end = s["end_hour"] * 60 + s["end_minute"]
    return start <= minute <= end

def _tm_build_daily_snapshot(force=False):
    with _top_movers_lock:
        has_data = bool(_top_movers_cache["rows"])
        age = time.time() - _top_movers_cache["ts"]

    refresh_seconds = _tm_settings()["refresh_seconds"]

    if has_data and not force and age < refresh_seconds:
        return

    symbols = _tm_all_symbols()
    ys = [_sym(s) for s in symbols]

    # Daily bars are much lighter than pulling 1-minute data for the full Nifty 500.
    # Today's daily Close acts as the current/latest Yahoo price.
    data = yf.download(
        tickers=ys,
        period="7d",
        interval="1d",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
        prepost=False,
    )

    rows = {}

    for symbol, y in zip(symbols, ys):
        frame = _single(data, y)
        if frame.empty or "Close" not in frame or "Open" not in frame:
            _mark_invalid_symbol(symbol)
            continue

        frame = frame.dropna(subset=["Close"]).copy()
        if len(frame) < 2:
            continue

        cur = frame.iloc[-1]
        prev = frame.iloc[-2]

        try:
            current = float(cur["Close"])
            today_open = float(cur["Open"])
            pdc = float(prev["Close"])
        except Exception:
            continue

        if not pdc:
            continue

        rows[symbol] = {
            "symbol": symbol,
            "sector": _tm_sector_for_symbol(symbol),
            "current_price": current,
            "pdc": pdc,
            "gap_pct": (today_open - pdc) / pdc * 100.0,
            "move_pct": (current - pdc) / pdc * 100.0,
        }

    with _top_movers_lock:
        _top_movers_cache["rows"] = rows
        _top_movers_cache["ts"] = time.time()
        _top_movers_cache["updated_at"] = pd.Timestamp.now(
            tz="Asia/Kolkata"
        ).isoformat()

    _tm_write_live_cache()

def _tm_intraday_flags(symbols, base_rows):
    symbols = _sanitize_stock_symbols(symbols)
    if not symbols:
        return base_rows

    ys = [_sym(s) for s in symbols]

    data = yf.download(
        tickers=ys,
        period="5d",
        interval="1m",
        group_by="ticker",
        auto_adjust=False,
        threads=True,
        progress=False,
        prepost=False,
    )

    result = []

    for symbol, y in zip(symbols, ys):
        row = dict(base_rows[symbol])
        frame = _single(data, y)

        if frame.empty or "Close" not in frame:
            row.update({
                "first5_status": "No data",
                "first15_status": "No data",
            })
            result.append(row)
            continue

        frame = frame.dropna(subset=["Close"]).copy()
        if getattr(frame.index, "tz", None) is None:
            frame.index = frame.index.tz_localize("UTC").tz_convert("Asia/Kolkata")
        else:
            frame.index = frame.index.tz_convert("Asia/Kolkata")

        frame["_date"] = frame.index.date
        dates = list(pd.unique(frame["_date"]))
        if len(dates) < 2:
            row.update({
                "first5_status": "No data",
                "first15_status": "No data",
            })
            result.append(row)
            continue

        cur_date, prev_date = dates[-1], dates[-2]
        cur = frame[frame["_date"] == cur_date]
        prev = frame[frame["_date"] == prev_date]

        try:
            current = float(cur["Close"].dropna().iloc[-1])
            today_open = float(cur["Open"].dropna().iloc[0])
            pdc = float(prev["Close"].dropna().iloc[-1])
            pdh = float(prev["High"].max())
            pdl = float(prev["Low"].min())

            row["current_price"] = current
            row["pdc"] = pdc
            row["gap_pct"] = (today_open - pdc) / pdc * 100.0 if pdc else None
            row["move_pct"] = (current - pdc) / pdc * 100.0 if pdc else None

            flags = _sector_first_candle_flags(
                frame,
                pdc,
                pdh,
                pdl
            )
            row.update(flags)
        except Exception:
            row.update({
                "first5_status": "No data",
                "first15_status": "No data",
            })

        result.append(row)

    return result

def _tm_live(universe, sector, top_n, max_gainer_pct, max_loser_abs_pct, force=False):
    # Outside the configured live window, normal auto refresh does not force Yahoo.
    # Manual Fetch Now passes force=True.
    if force or _tm_window_active():
        _tm_build_daily_snapshot(force=force)
    else:
        _tm_build_daily_snapshot(force=False)

    with _top_movers_lock:
        base = dict(_top_movers_cache["rows"])
        updated_at = _top_movers_cache["updated_at"]

    symbols = _tm_universe_symbols(universe, sector)
    rows = [
        dict(base[s])
        for s in symbols
        if s in base
    ]

    gainers = [
        r for r in rows
        if r.get("move_pct") is not None
        and r["move_pct"] > 0
        and r["move_pct"] <= max_gainer_pct
    ]
    losers = [
        r for r in rows
        if r.get("move_pct") is not None
        and r["move_pct"] < 0
        and abs(r["move_pct"]) <= max_loser_abs_pct
    ]

    gainers.sort(
        key=lambda r: -r["move_pct"]
    )
    losers.sort(
        key=lambda r: r["move_pct"]
    )

    gainers = gainers[:top_n]
    losers = losers[:top_n]

    candidate_symbols = [
        r["symbol"]
        for r in gainers + losers
    ]

    candidate_map = {
        r["symbol"]: r
        for r in gainers + losers
    }

    detailed = _tm_intraday_flags(
        candidate_symbols,
        candidate_map
    )
    detailed_map = {
        r["symbol"]: r
        for r in detailed
    }

    gainers = [
        detailed_map.get(r["symbol"], r)
        for r in gainers
    ]
    losers = [
        detailed_map.get(r["symbol"], r)
        for r in losers
    ]

    return {
        "universe": universe,
        "sector": sector,
        "gainers": gainers,
        "losers": losers,
        "updated_at": updated_at,
        "window_active": _tm_window_active(),
        "settings": _tm_settings(),
    }


def _tm_save_snapshot_today():
    _tm_build_daily_snapshot(force=True)

    with _top_movers_lock:
        rows = list(_top_movers_cache["rows"].values())

    trade_date = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).date().isoformat()

    _tm_save_timed_snapshot(
        trade_date,
        rows
    )

    return _tm_load_history(trade_date)

def _tm_save_eod_today():
    _tm_build_daily_snapshot(force=True)
    with _top_movers_lock:
        rows = list(_top_movers_cache["rows"].values())

    trade_date = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).date().isoformat()

    _tm_save_history(
        trade_date,
        rows
    )

    return _tm_load_history(trade_date)

async def _top_movers_scheduler():
    while True:
        try:
            now = pd.Timestamp.now(
                tz="Asia/Kolkata"
            )
            weekday = now.weekday() < 5
            minute = now.hour * 60 + now.minute
            settings = _tm_settings()

            if weekday:
                trade_date = now.date().isoformat()
                saved = _tm_load_history(trade_date)

                snapshot_minute = (
                    settings["snapshot_hour"] * 60
                    + settings["snapshot_minute"]
                )

                # Once current time reaches the configured snapshot time,
                # save today's timed snapshot if it has not already been saved.
                if minute >= snapshot_minute and minute < 15 * 60 + 30:
                    if not saved or not saved.get("snapshot_saved"):
                        try:
                            await asyncio.to_thread(
                                _tm_save_snapshot_today
                            )
                        except Exception:
                            pass

                saved = _tm_load_history(trade_date)

                # EOD save remains automatic and separate.
                if minute >= 15 * 60 + 32:
                    if not saved or not saved.get("eod_saved"):
                        try:
                            await asyncio.to_thread(
                                _tm_save_eod_today
                            )
                        except Exception:
                            pass

        except Exception:
            pass

        await asyncio.sleep(20)


@app.on_event("startup")
async def _start_top_movers_scheduler():
    asyncio.create_task(
        _top_movers_scheduler()
    )

@app.get("/api/top-movers/settings")
def top_movers_settings():
    return _tm_settings()

@app.post("/api/top-movers/settings")
def update_top_movers_settings(
    top_n: int,
    keep_sessions: int,
    start_hour: int,
    start_minute: int,
    end_hour: int,
    end_minute: int,
    refresh_seconds: int,
    max_gainer_pct: float,
    max_loser_abs_pct: float,
    snapshot_hour: int,
    snapshot_minute: int,
):
    return _tm_save_settings(
        top_n,
        keep_sessions,
        start_hour,
        start_minute,
        end_hour,
        end_minute,
        refresh_seconds,
        max_gainer_pct,
        max_loser_abs_pct,
        snapshot_hour,
        snapshot_minute,
    )

@app.get("/api/top-movers/universe")
def top_movers_universe(
    universe: str = "Nifty 50"
):
    symbols = _tm_universe_symbols(
        universe,
        None
    )
    sectors = sorted({
        _tm_sector_for_symbol(s)
        for s in symbols
    })
    return {
        "universe": universe,
        "sectors": sectors,
        "stocks": symbols,
    }


@app.get("/api/top-movers/browser-cache")
def top_movers_browser_cache():
    _tm_trigger_background_refresh(force=False)

    with _top_movers_lock:
        rows = list(_top_movers_cache["rows"].values())
        updated_at = _top_movers_cache["updated_at"]

    return {
        "rows": rows,
        "updated_at": updated_at,
        "refreshing": _tm_refreshing,
        "universes": {
            "Nifty 50": _sanitize_stock_symbols(NIFTY50),
            "F&O": _sanitize_stock_symbols(FO_UNIVERSE),
            "Nifty 100": _sanitize_stock_symbols(NIFTY100),
            "Nifty 500": _sanitize_stock_symbols(NIFTY500),
            "Sector": _tm_all_symbols(),
        },
    }

@app.get("/api/top-movers/details")
def top_movers_details(
    symbols: str = "",
    force: bool = False
):
    requested = _sanitize_stock_symbols(
        [x.strip().upper() for x in symbols.split(",") if x.strip()]
    )

    rows = _tm_cached_details(
        requested,
        force=force
    )

    return {
        "rows": rows,
        "requested": requested,
        "cached_count": len(rows),
        "refreshing": len(rows) < len(requested),
        "updated_at": pd.Timestamp.now(
            tz="Asia/Kolkata"
        ).isoformat(),
    }


@app.get("/api/top-movers/live")
def top_movers_live(
    universe: str = "Nifty 50",
    sector: str = "All Sectors",
    top_n: int = 10,
    max_gainer_pct: float = 100,
    max_loser_abs_pct: float = 100,
    force: bool = False,
):
    return _tm_live(
        universe=universe,
        sector=sector,
        top_n=max(1, min(50, top_n)),
        max_gainer_pct=max(0.0, max_gainer_pct),
        max_loser_abs_pct=max(0.0, max_loser_abs_pct),
        force=force,
    )

@app.get("/api/top-movers/dates")
def top_movers_dates():
    return {"dates": _tm_dates()}

@app.get("/api/top-movers/history/{trade_date}")
def top_movers_history(
    trade_date: str,
    universe: str = "Nifty 50",
    sector: str = "All Sectors",
    basis: str = "eod",
):
    snap = _tm_load_history(
        trade_date
    )
    if not snap:
        raise HTTPException(
            status_code=404,
            detail="No saved Top Movers history for this date"
        )

    if basis == "snapshot":
        rows = snap.get("snapshot_rows", [])
        saved_at = snap.get("snapshot_at")
        label = "Timed Snapshot"
    else:
        rows = snap.get("rows", [])
        saved_at = snap.get("eod_at")
        label = "EOD"

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No {label} data saved for this date"
        )

    allowed = set(
        _tm_universe_symbols(
            universe,
            sector
        )
    )

    rows = [
        r for r in rows
        if r.get("symbol") in allowed
    ]

    return {
        "trade_date": trade_date,
        "saved_at": saved_at,
        "rows": rows,
        "universe": universe,
        "sector": sector,
        "basis": label,
    }

@app.post("/api/top-movers/snapshot")
def top_movers_save_snapshot():
    return _tm_save_snapshot_today()


@app.post("/api/top-movers/eod")
def top_movers_save_eod():
    return _tm_save_eod_today()


# ============================================================================
# V21 PREVIOUS DAYS STOCK IMPACT ANALYSIS
# ============================================================================
IMPACT_DB = DATA_DIR / "previous_days_impact.sqlite3"
IMPACT_DEFAULT_PRE_TOP_N = 30
IMPACT_DEFAULT_OI_TOP_N = 30
IMPACT_DEFAULT_LOOKBACK = 5
IMPACT_DEFAULT_KEEP = 30

def _impact_db():
    conn = sqlite3.connect(IMPACT_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS impact_sessions (
            analysis_date TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            rows_json TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS impact_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    defaults = [
        ("pre_top_n", str(IMPACT_DEFAULT_PRE_TOP_N)),
        ("oi_top_n", str(IMPACT_DEFAULT_OI_TOP_N)),
        ("lookback_days", str(IMPACT_DEFAULT_LOOKBACK)),
        ("keep_sessions", str(IMPACT_DEFAULT_KEEP)),
        ("min_abs_move", "0"),
        ("max_abs_move", "100"),
    ]
    for key, value in defaults:
        conn.execute(
            "INSERT OR IGNORE INTO impact_settings(key,value) VALUES(?,?)",
            (key, value)
        )
    conn.commit()
    return conn

def _impact_setting_int(key, default, minimum, maximum):
    conn = _impact_db()
    row = conn.execute(
        "SELECT value FROM impact_settings WHERE key=?",
        (key,)
    ).fetchone()
    conn.close()
    try:
        value = int(float(row[0])) if row else int(default)
    except Exception:
        value = int(default)
    return max(minimum, min(maximum, value))

def _impact_setting_float(key, default, minimum, maximum):
    conn = _impact_db()
    row = conn.execute(
        "SELECT value FROM impact_settings WHERE key=?",
        (key,)
    ).fetchone()
    conn.close()
    try:
        value = float(row[0]) if row else float(default)
    except Exception:
        value = float(default)
    return max(minimum, min(maximum, value))

def _impact_set(key, value):
    conn = _impact_db()
    conn.execute(
        "INSERT INTO impact_settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value))
    )
    conn.commit()
    conn.close()

def _impact_settings():
    return {
        "pre_top_n": _impact_setting_int("pre_top_n", 30, 5, 100),
        "oi_top_n": _impact_setting_int("oi_top_n", 30, 5, 100),
        "lookback_days": _impact_setting_int("lookback_days", 5, 1, 5),
        "keep_sessions": _impact_setting_int("keep_sessions", 30, 1, 120),
        "min_abs_move": _impact_setting_float("min_abs_move", 0, 0, 1000),
        "max_abs_move": _impact_setting_float("max_abs_move", 100, 0, 1000),
    }

def _impact_save_settings(
    pre_top_n,
    oi_top_n,
    lookback_days,
    keep_sessions,
    min_abs_move,
    max_abs_move,
):
    values = {
        "pre_top_n": max(5, min(100, int(pre_top_n))),
        "oi_top_n": max(5, min(100, int(oi_top_n))),
        "lookback_days": max(1, min(5, int(lookback_days))),
        "keep_sessions": max(1, min(120, int(keep_sessions))),
        "min_abs_move": max(0.0, float(min_abs_move)),
        "max_abs_move": max(0.0, float(max_abs_move)),
    }
    for k, v in values.items():
        _impact_set(k, v)
    _impact_prune()
    return values

def _impact_prune():
    conn = _impact_db()
    dates = [
        r[0] for r in conn.execute(
            "SELECT analysis_date FROM impact_sessions ORDER BY analysis_date DESC"
        ).fetchall()
    ]
    keep = _impact_setting_int("keep_sessions", 30, 1, 120)
    for d in dates[keep:]:
        conn.execute(
            "DELETE FROM impact_sessions WHERE analysis_date=?",
            (d,)
        )
    conn.commit()
    conn.close()

def _impact_dates():
    conn = _impact_db()
    rows = conn.execute("""
        SELECT analysis_date,created_at
        FROM impact_sessions
        ORDER BY analysis_date DESC
        LIMIT ?
    """, (_impact_setting_int("keep_sessions", 30, 1, 120),)).fetchall()
    conn.close()
    return [
        {"analysis_date": r[0], "created_at": r[1]}
        for r in rows
    ]

def _impact_load(analysis_date):
    conn = _impact_db()
    row = conn.execute("""
        SELECT analysis_date,created_at,rows_json
        FROM impact_sessions
        WHERE analysis_date=?
    """, (analysis_date,)).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "analysis_date": row[0],
        "created_at": row[1],
        "rows": json.loads(row[2]),
    }

def _impact_save(analysis_date, rows):
    now = pd.Timestamp.now(tz="Asia/Kolkata").isoformat()
    conn = _impact_db()
    conn.execute("""
        INSERT INTO impact_sessions(analysis_date,created_at,rows_json)
        VALUES(?,?,?)
        ON CONFLICT(analysis_date) DO UPDATE SET
            created_at=excluded.created_at,
            rows_json=excluded.rows_json
    """, (
        analysis_date,
        now,
        json.dumps(rows)
    ))
    conn.commit()
    conn.close()
    _impact_prune()
    return _impact_load(analysis_date)

def _impact_saved_market_dates():
    dates = set()
    try:
        dates.update(x["trade_date"] for x in _ois_dates())
    except Exception:
        pass
    try:
        conn = _pm_db()
        rows = conn.execute(
            "SELECT trade_date FROM snapshots ORDER BY trade_date"
        ).fetchall()
        conn.close()
        dates.update(r[0] for r in rows)
    except Exception:
        pass
    return sorted(dates)

def _impact_pre_rows(trade_date, top_n):
    snap = _pm_load(trade_date)
    if not snap:
        return []

    rows = [
        dict(r)
        for r in snap.get("rows", [])
        if r.get("symbol")
    ]

    rows.sort(
        key=lambda r: (
            r.get("combined_rank")
            if r.get("combined_rank") is not None
            else 9999,
            r.get("symbol", "")
        )
    )
    return rows[:top_n]

def _impact_oi_rows(trade_date, top_n):
    snap = _ois_load(trade_date)
    if not snap:
        return []

    rows = (
        snap.get("eod_rows")
        or snap.get("opening_rows")
        or []
    )

    rows = [
        dict(r)
        for r in rows
        if r.get("symbol")
        and r.get("oi_change_pct") is not None
    ]

    rows.sort(
        key=lambda r: (
            -(r.get("oi_change_pct") or -999999),
            r.get("symbol", "")
        )
    )
    return rows[:top_n]

def _impact_move_for_date(symbol, trade_date):
    oi = _ois_historical_row_map(trade_date).get(symbol)
    pm = _pm_historical_row_map(trade_date).get(symbol)

    for row in [oi, pm]:
        if not row:
            continue
        for key in ["move_pct", "live_pct"]:
            value = row.get(key)
            if value is not None:
                try:
                    return float(value)
                except Exception:
                    pass

    # Fallback to saved top movers EOD if present.
    try:
        tm = _tm_load_history(trade_date)
        if tm:
            for row in tm.get("rows", []):
                if row.get("symbol") == symbol and row.get("move_pct") is not None:
                    return float(row["move_pct"])
    except Exception:
        pass

    return None

def _impact_observation(row):
    prev = row.get("previous_days", [])
    current = row.get("analysis_day_move_pct")
    next_move = row.get("next_day_move_pct")

    pre_hits = sum(1 for x in prev if x.get("in_preopen"))
    oi_hits = sum(1 for x in prev if x.get("in_oi_spurt"))
    both_hits = sum(
        1 for x in prev
        if x.get("in_preopen") and x.get("in_oi_spurt")
    )

    prev_moves = [
        x.get("move_pct")
        for x in prev
        if x.get("move_pct") is not None
    ]

    parts = []

    if both_hits >= 2:
        parts.append(f"Repeated in both methods {both_hits} prior sessions")
    elif both_hits == 1:
        parts.append("Present in both methods once previously")
    elif pre_hits or oi_hits:
        parts.append(
            f"Prior presence: Pre {pre_hits}, OI {oi_hits}"
        )
    else:
        parts.append("No prior presence in selected lookback")

    if prev_moves:
        avg_prev = sum(prev_moves) / len(prev_moves)
        if avg_prev > 0.5:
            parts.append("prior-day bias positive")
        elif avg_prev < -0.5:
            parts.append("prior-day bias negative")
        else:
            parts.append("prior-day moves mixed/flat")

    if current is not None:
        if current > 1:
            parts.append("analysis day strong positive")
        elif current < -1:
            parts.append("analysis day strong negative")
        else:
            parts.append("analysis day moderate/flat")

    if next_move is not None:
        if current is not None and current * next_move > 0:
            parts.append("next day continued direction")
        elif current is not None and current * next_move < 0:
            parts.append("next day reversed direction")
        else:
            parts.append("next day available")

    return "; ".join(parts)

def _impact_build(analysis_date, settings=None):
    settings = settings or _impact_settings()

    pre_n = settings["pre_top_n"]
    oi_n = settings["oi_top_n"]
    lookback = settings["lookback_days"]

    market_dates = _impact_saved_market_dates()
    if analysis_date not in market_dates:
        raise HTTPException(
            status_code=404,
            detail="Analysis date is not available in saved Pre-Market/OI history"
        )

    idx = market_dates.index(analysis_date)
    previous_dates = market_dates[max(0, idx-lookback):idx]
    previous_dates = list(reversed(previous_dates))

    next_date = (
        market_dates[idx+1]
        if idx+1 < len(market_dates)
        else None
    )

    current_pre = _impact_pre_rows(analysis_date, pre_n)
    current_oi = _impact_oi_rows(analysis_date, oi_n)

    pre_map = {
        r["symbol"]: r
        for r in current_pre
    }
    oi_map = {
        r["symbol"]: r
        for r in current_oi
    }

    symbols = sorted(
        set(pre_map.keys()) | set(oi_map.keys())
    )

    prior_pre_maps = {}
    prior_oi_maps = {}

    for d in previous_dates:
        prior_pre_maps[d] = {
            r["symbol"]: r
            for r in _impact_pre_rows(d, pre_n)
        }
        prior_oi_maps[d] = {
            r["symbol"]: r
            for r in _impact_oi_rows(d, oi_n)
        }

    rows = []

    for symbol in symbols:
        current_move = _impact_move_for_date(
            symbol,
            analysis_date
        )

        if current_move is not None:
            abs_move = abs(current_move)
            if abs_move < settings["min_abs_move"]:
                continue
            if abs_move > settings["max_abs_move"]:
                continue

        history = []

        for d in previous_dates:
            history.append({
                "date": d,
                "in_preopen": symbol in prior_pre_maps[d],
                "in_oi_spurt": symbol in prior_oi_maps[d],
                "move_pct": _impact_move_for_date(symbol, d),
            })

        row = {
            "symbol": symbol,
            "sector": SYMBOL_TO_SECTOR.get(
                symbol,
                (
                    oi_map.get(symbol, {}).get("sector")
                    or "Other / Unmapped"
                )
            ),
            "analysis_date": analysis_date,
            "in_analysis_preopen": symbol in pre_map,
            "in_analysis_oi_spurt": symbol in oi_map,
            "analysis_pre_rank": pre_map.get(symbol, {}).get("combined_rank"),
            "analysis_oi_rank": (
                current_oi.index(oi_map[symbol]) + 1
                if symbol in oi_map
                else None
            ),
            "analysis_oi_change_pct": oi_map.get(symbol, {}).get("oi_change_pct"),
            "analysis_gap_pct": (
                oi_map.get(symbol, {}).get("gap_pct")
                if symbol in oi_map
                else pre_map.get(symbol, {}).get("gap_pct")
            ),
            "analysis_day_move_pct": current_move,
            "previous_days": history,
            "next_date": next_date,
            "next_day_move_pct": (
                _impact_move_for_date(symbol, next_date)
                if next_date
                else None
            ),
        }

        row["observation"] = _impact_observation(row)
        rows.append(row)

    rows.sort(
        key=lambda r: (
            -sum(
                1 for x in r["previous_days"]
                if x["in_preopen"] and x["in_oi_spurt"]
            ),
            -abs(r["analysis_day_move_pct"] or 0),
            r["symbol"],
        )
    )

    return {
        "analysis_date": analysis_date,
        "previous_dates": previous_dates,
        "next_date": next_date,
        "rows": rows,
        "settings": settings,
    }

def _impact_create_or_update(analysis_date=None):
    if analysis_date is None:
        analysis_date = pd.Timestamp.now(
            tz="Asia/Kolkata"
        ).date().isoformat()

    result = _impact_build(
        analysis_date,
        _impact_settings()
    )

    return _impact_save(
        analysis_date,
        result["rows"]
    )

def _impact_refresh_next_day_links():
    # Rebuild recent analyses so yesterday's "next day move" becomes available.
    dates = [
        x["analysis_date"]
        for x in _impact_dates()
    ]

    for d in dates[:7]:
        try:
            _impact_create_or_update(d)
        except Exception:
            pass

async def _impact_scheduler():
    while True:
        try:
            now = pd.Timestamp.now(
                tz="Asia/Kolkata"
            )
            minute = now.hour * 60 + now.minute

            if now.weekday() < 5 and minute >= 15 * 60 + 35:
                today = now.date().isoformat()

                if not _impact_load(today):
                    try:
                        await asyncio.to_thread(
                            _impact_create_or_update,
                            today
                        )
                    except Exception:
                        pass

                # Update older rows with newly available next-day move.
                try:
                    await asyncio.to_thread(
                        _impact_refresh_next_day_links
                    )
                except Exception:
                    pass

        except Exception:
            pass

        await asyncio.sleep(30)

@app.on_event("startup")
async def _start_impact_scheduler():
    asyncio.create_task(
        _impact_scheduler()
    )

@app.get("/api/impact/settings")
def impact_settings():
    return _impact_settings()

@app.post("/api/impact/settings")
def update_impact_settings(
    pre_top_n: int,
    oi_top_n: int,
    lookback_days: int,
    keep_sessions: int,
    min_abs_move: float = 0,
    max_abs_move: float = 100,
):
    return _impact_save_settings(
        pre_top_n,
        oi_top_n,
        lookback_days,
        keep_sessions,
        min_abs_move,
        max_abs_move,
    )

@app.get("/api/impact/dates")
def impact_dates():
    return {
        "dates": _impact_dates(),
        "market_dates": _impact_saved_market_dates(),
    }

@app.get("/api/impact/{analysis_date}")
def impact_view(
    analysis_date: str,
    rebuild: bool = False
):
    if rebuild:
        snap = _impact_create_or_update(
            analysis_date
        )
    else:
        snap = _impact_load(
            analysis_date
        )

        if not snap:
            built = _impact_build(
                analysis_date,
                _impact_settings()
            )
            return built

    if not snap:
        raise HTTPException(
            status_code=404,
            detail="No impact analysis available"
        )

    # Rehydrate metadata for display.
    built = _impact_build(
        analysis_date,
        _impact_settings()
    )
    built["rows"] = snap["rows"]
    built["created_at"] = snap["created_at"]
    return built

@app.post("/api/impact/build/{analysis_date}")
def impact_build_now(
    analysis_date: str
):
    return _impact_create_or_update(
        analysis_date
    )

@app.get("/api/impact/download/{analysis_date}.csv")
def impact_download_csv(
    analysis_date: str
):
    snap = _impact_load(analysis_date)
    if not snap:
        raise HTTPException(
            status_code=404,
            detail="No saved impact analysis for this date"
        )

    rows = snap["rows"]
    flat = []

    for row in rows:
        base = {
            "Analysis Date": analysis_date,
            "Stock": row.get("symbol"),
            "Sector": row.get("sector"),
            "In Analysis Pre-Market": row.get("in_analysis_preopen"),
            "In Analysis OI Spurt": row.get("in_analysis_oi_spurt"),
            "Pre Rank": row.get("analysis_pre_rank"),
            "OI Rank": row.get("analysis_oi_rank"),
            "OI Change %": row.get("analysis_oi_change_pct"),
            "Gap %": row.get("analysis_gap_pct"),
            "Analysis Day Move %": row.get("analysis_day_move_pct"),
            "Next Date": row.get("next_date"),
            "Next Day Move %": row.get("next_day_move_pct"),
            "Observation": row.get("observation"),
        }

        for i, hist in enumerate(row.get("previous_days", []), start=1):
            base[f"Prev{i} Date"] = hist.get("date")
            base[f"Prev{i} Pre-Market"] = hist.get("in_preopen")
            base[f"Prev{i} OI Spurt"] = hist.get("in_oi_spurt")
            base[f"Prev{i} Move %"] = hist.get("move_pct")

        flat.append(base)

    df = pd.DataFrame(flat)

    from fastapi.responses import StreamingResponse
    import io as _io

    buf = _io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition":
                f'attachment; filename="Previous_Days_Impact_{analysis_date}.csv"'
        },
    )

@app.get("/api/impact/download/{analysis_date}.xlsx")
def impact_download_xlsx(
    analysis_date: str
):
    snap = _impact_load(analysis_date)
    if not snap:
        raise HTTPException(
            status_code=404,
            detail="No saved impact analysis for this date"
        )

    rows = snap["rows"]
    flat = []

    for row in rows:
        base = {
            "Analysis Date": analysis_date,
            "Stock": row.get("symbol"),
            "Sector": row.get("sector"),
            "In Analysis Pre-Market": row.get("in_analysis_preopen"),
            "In Analysis OI Spurt": row.get("in_analysis_oi_spurt"),
            "Pre Rank": row.get("analysis_pre_rank"),
            "OI Rank": row.get("analysis_oi_rank"),
            "OI Change %": row.get("analysis_oi_change_pct"),
            "Gap %": row.get("analysis_gap_pct"),
            "Analysis Day Move %": row.get("analysis_day_move_pct"),
            "Next Date": row.get("next_date"),
            "Next Day Move %": row.get("next_day_move_pct"),
            "Observation": row.get("observation"),
        }

        for i, hist in enumerate(row.get("previous_days", []), start=1):
            base[f"Prev{i} Date"] = hist.get("date")
            base[f"Prev{i} Pre-Market"] = hist.get("in_preopen")
            base[f"Prev{i} OI Spurt"] = hist.get("in_oi_spurt")
            base[f"Prev{i} Move %"] = hist.get("move_pct")

        flat.append(base)

    df = pd.DataFrame(flat)

    from fastapi.responses import StreamingResponse
    import io as _io

    output = _io.BytesIO()

    with pd.ExcelWriter(
        output,
        engine="openpyxl"
    ) as writer:
        df.to_excel(
            writer,
            sheet_name="Impact Analysis",
            index=False
        )

    output.seek(0)

    return StreamingResponse(
        output,
        media_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition":
                f'attachment; filename="Previous_Days_Impact_{analysis_date}.xlsx"'
        },
    )

# ============================================================================
# V21 TOP MOVERS 09:22 SNAPSHOT
# ============================================================================
TM_0922_DEFAULT_TOP_N = 5

def _tm_0922_schema():
    conn = _tm_db()
    cols = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info(top_movers_sessions)"
        ).fetchall()
    }

    if "snapshot_0922_at" not in cols:
        conn.execute(
            "ALTER TABLE top_movers_sessions ADD COLUMN snapshot_0922_at TEXT"
        )

    if "snapshot_0922_rows_json" not in cols:
        conn.execute(
            "ALTER TABLE top_movers_sessions ADD COLUMN snapshot_0922_rows_json TEXT"
        )

    conn.execute(
        "INSERT OR IGNORE INTO top_movers_settings(key,value) VALUES('snapshot_0922_top_n','5')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO top_movers_settings(key,value) VALUES('snapshot_capture_time','09:22')"
    )

    conn.commit()
    conn.close()

_tm_0922_schema()

def _tm_0922_top_n():
    return _tm_setting_int(
        "snapshot_0922_top_n",
        TM_0922_DEFAULT_TOP_N,
        1,
        50
    )

def _tm_0922_capture_time():
    conn = _tm_db()
    row = conn.execute(
        "SELECT value FROM top_movers_settings WHERE key='snapshot_capture_time'"
    ).fetchone()
    conn.close()
    value = str(row[0] if row else "09:22").strip()
    if not re.match(r"^(?:[01]\d|2[0-3]):[0-5]\d$", value):
        value = "09:22"
    return value

def _tm_0922_live_payload():
    # V26: exactly the Futures-OI pattern — return RAM/disk cache immediately
    # and refresh the heavy Yahoo base in the background.
    try:
        _tm_trigger_background_refresh(force=False)
    except Exception:
        pass

    with _top_movers_lock:
        rows = [dict(r) for r in _top_movers_cache["rows"].values()]
        base_updated_at = _top_movers_cache.get("updated_at")

    gainers = sorted(
        [r for r in rows if r.get("move_pct") is not None and r["move_pct"] > 0],
        key=lambda r: -r["move_pct"]
    )[:_tm_0922_top_n()]

    losers = sorted(
        [r for r in rows if r.get("move_pct") is not None and r["move_pct"] < 0],
        key=lambda r: r["move_pct"]
    )[:_tm_0922_top_n()]

    selected_symbols = [r["symbol"] for r in gainers + losers]

    # Candle details are also cache-first. Missing/stale details refresh in a
    # background thread and appear on a later UI cycle.
    try:
        detail_rows = _tm_cached_details(selected_symbols, force=False)
    except Exception:
        detail_rows = []

    dmap = {r.get("symbol"): r for r in detail_rows if r.get("symbol")}

    def merge_detail(row):
        merged = dict(row)
        detail = dmap.get(row.get("symbol"))
        if detail:
            merged.update(detail)
        else:
            merged.setdefault("first5_status", "Loading candle…")
            merged.setdefault("first15_status", "Loading candle…")
        return merged

    now = pd.Timestamp.now(tz="Asia/Kolkata")
    trade_date = now.date().isoformat()
    saved = _tm_load_0922_snapshot(trade_date)

    return {
        "trade_date": trade_date,
        "live_at": now.isoformat(),
        "base_updated_at": base_updated_at,
        "capture_time": _tm_0922_capture_time(),
        "is_live": True,
        "snapshot_saved": bool(saved),
        "snapshot_saved_at": saved.get("snapshot_0922_at") if saved else None,
        "refreshing": bool(_tm_refreshing),
        "gainers": [merge_detail(r) for r in gainers],
        "losers": [merge_detail(r) for r in losers],
    }


def _tm_save_0922_snapshot():
    _tm_build_daily_snapshot(force=True)

    with _top_movers_lock:
        rows = list(
            _top_movers_cache["rows"].values()
        )

    gainers = sorted(
        [r for r in rows if r.get("move_pct") is not None and r["move_pct"] > 0],
        key=lambda r: -r["move_pct"]
    )[:_tm_0922_top_n()]

    losers = sorted(
        [r for r in rows if r.get("move_pct") is not None and r["move_pct"] < 0],
        key=lambda r: r["move_pct"]
    )[:_tm_0922_top_n()]

    selected = gainers + losers

    base = {
        r["symbol"]: r
        for r in selected
    }

    detailed = _tm_intraday_flags(
        list(base.keys()),
        base
    )

    detailed_map = {
        r["symbol"]: r
        for r in detailed
    }

    payload = {
        "gainers": [
            detailed_map.get(r["symbol"], r)
            for r in gainers
        ],
        "losers": [
            detailed_map.get(r["symbol"], r)
            for r in losers
        ],
    }

    trade_date = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).date().isoformat()

    now = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).isoformat()

    conn = _tm_db()

    existing = conn.execute(
        "SELECT trade_date FROM top_movers_sessions WHERE trade_date=?",
        (trade_date,)
    ).fetchone()

    if existing:
        conn.execute("""
            UPDATE top_movers_sessions
            SET snapshot_0922_at=?,
                snapshot_0922_rows_json=?
            WHERE trade_date=?
        """, (
            now,
            json.dumps(payload),
            trade_date
        ))
    else:
        # satisfy legacy NOT NULL eod columns with placeholders
        conn.execute("""
            INSERT INTO top_movers_sessions(
                trade_date,
                eod_at,
                rows_json,
                snapshot_0922_at,
                snapshot_0922_rows_json
            )
            VALUES(?,?,?,?,?)
        """, (
            trade_date,
            now,
            json.dumps([]),
            now,
            json.dumps(payload)
        ))

    conn.commit()
    conn.close()

    return {
        "trade_date": trade_date,
        "snapshot_0922_at": now,
        **payload,
    }

def _tm_load_0922_snapshot(trade_date):
    conn = _tm_db()
    row = conn.execute("""
        SELECT snapshot_0922_at,snapshot_0922_rows_json
        FROM top_movers_sessions
        WHERE trade_date=?
    """, (trade_date,)).fetchone()
    conn.close()

    if not row or not row[1]:
        return None

    payload = json.loads(row[1])

    return {
        "trade_date": trade_date,
        "snapshot_0922_at": row[0],
        "gainers": payload.get("gainers", []),
        "losers": payload.get("losers", []),
    }

async def _tm_0922_scheduler():
    while True:
        try:
            now = pd.Timestamp.now(
                tz="Asia/Kolkata"
            )
            minute = now.hour * 60 + now.minute

            capture_h, capture_m = [int(x) for x in _tm_0922_capture_time().split(":")]
            capture_minute = capture_h * 60 + capture_m

            if (
                now.weekday() < 5
                and minute >= capture_minute
                and minute < 15 * 60 + 30
            ):
                trade_date = now.date().isoformat()

                if not _tm_load_0922_snapshot(trade_date):
                    try:
                        await asyncio.to_thread(
                            _tm_save_0922_snapshot
                        )
                    except Exception:
                        pass

        except Exception:
            pass

        await asyncio.sleep(20)

@app.on_event("startup")
async def _start_tm_0922_scheduler():
    asyncio.create_task(
        _tm_0922_scheduler()
    )

@app.get("/api/top-movers/live-today")
def top_movers_live_today():
    return _tm_0922_live_payload()

@app.get("/api/top-movers/0922/{trade_date}")
def top_movers_0922_history(
    trade_date: str
):
    snap = _tm_load_0922_snapshot(
        trade_date
    )
    if not snap:
        raise HTTPException(
            status_code=404,
            detail="No 09:22 Top Movers snapshot for this date"
        )
    return snap

@app.post("/api/top-movers/0922")
def top_movers_0922_now():
    return _tm_save_0922_snapshot()

@app.post("/api/top-movers/0922/settings")
def top_movers_0922_settings(
    top_n: int,
    capture_time: str = "09:22"
):
    _tm_set("snapshot_0922_top_n", max(1, min(50, int(top_n))))
    if not re.match(r"^(?:[01]\d|2[0-3]):[0-5]\d$", str(capture_time)):
        raise HTTPException(400, "Capture time must be HH:MM")
    _tm_set("snapshot_capture_time", capture_time)
    return {"top_n": _tm_0922_top_n(), "capture_time": _tm_0922_capture_time()}

@app.get("/api/top-movers/0922/settings")
def top_movers_0922_get_settings():
    return {"top_n": _tm_0922_top_n(), "capture_time": _tm_0922_capture_time()}

@app.get("/api/top-movers/0922/live")
def top_movers_0922_live():
    return _tm_0922_live_payload()


# ============================================================================
# V22 — EXPORTS + FULL-UNIVERSE TOP5 HISTORY + 5M MARKET CHARTS
# ============================================================================

def _v22_xlsx_response(df, filename, sheet_name="Data"):
    from fastapi.responses import StreamingResponse
    import io as _io

    output = _io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    output.seek(0)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        },
    )

def _v22_flatten(rows):
    out = []
    for row in rows or []:
        item = {}
        for key, value in row.items():
            if isinstance(value, (dict, list)):
                item[key] = json.dumps(value, ensure_ascii=False)
            else:
                item[key] = value
        out.append(item)
    return out

@app.get("/api/export/{dataset}.xlsx")
def v22_export_excel(
    dataset: str,
    sector: str = "Nifty Bank",
    trade_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    oi_top_n: int = 30,
    preopen_top_n: int = 30,
    min_common_sessions: int = 1,
    max_abs_net_move: float | None = None,
):
    today = pd.Timestamp.now(tz="Asia/Kolkata").date().isoformat()
    date = trade_date or today
    rows = []
    filename = f"{dataset}_{date}.xlsx"
    sheet = dataset[:31]

    if dataset == "sectorial":
        rows = fetch_sector(sector).get("rows", [])
        filename = f"Sectorial_{sector.replace(' ', '_')}_{today}.xlsx"
        sheet = "Sectorial"

    elif dataset == "breadth":
        data = combined_breadth(force=False)
        if isinstance(data, dict):
            rows = data.get("rows") or data.get("history") or []
        filename = f"Market_Breadth_{today}.xlsx"
        sheet = "Market Breadth"

    elif dataset == "premarket":
        snap = _pm_load(date)
        if not snap:
            raise HTTPException(404, "No pre-market data for selected date")
        rows = snap.get("rows", [])
        filename = f"NSE_PreMarket_{date}.xlsx"
        sheet = "PreMarket"

    elif dataset == "futures_oi":
        rows = fetch_live_oi(force=False).get("rows", [])
        filename = f"Futures_OI_{today}.xlsx"
        sheet = "Futures OI"

    elif dataset == "oi_spurt":
        if trade_date:
            snap = _ois_load(date)
            if not snap:
                raise HTTPException(404, "No OI-spurt history for selected date")
            rows = snap.get("eod_rows") or snap.get("opening_rows") or []
        else:
            rows = _ois_build_rows(
                top_n=100,
                force_oi=False,
                all_rows=True
            ).get("rows", [])
        filename = f"OI_Spurt_{date}.xlsx"
        sheet = "OI Spurt"

    elif dataset == "previous_selection":
        if not start_date or not end_date:
            raise HTTPException(400, "Start and end dates are required")
        data = _previous_data_selection(
            start_date=start_date,
            end_date=end_date,
            oi_top_n=max(5, min(100, oi_top_n)),
            preopen_top_n=max(5, min(100, preopen_top_n)),
            min_common_sessions=max(1, min_common_sessions),
            max_abs_net_move=max_abs_net_move,
        )
        rows = data.get("rows", [])
        filename = f"Previous_Data_Selection_{start_date}_to_{end_date}.xlsx"
        sheet = "Previous Selection"

    elif dataset == "top_movers":
        snap = _tm_load_history(date)
        if snap and snap.get("rows"):
            rows = snap["rows"]
        else:
            with _top_movers_lock:
                rows = list(_top_movers_cache["rows"].values())
        filename = f"Top_Movers_{date}.xlsx"
        sheet = "Top Movers"

    elif dataset == "impact":
        snap = _impact_load(date)
        if not snap:
            raise HTTPException(404, "No impact analysis for selected date")
        rows = snap.get("rows", [])
        filename = f"Impact_Analysis_{date}.xlsx"
        sheet = "Impact Analysis"

    else:
        raise HTTPException(400, "Unknown export dataset")

    df = pd.DataFrame(_v22_flatten(rows))
    return _v22_xlsx_response(df, filename, sheet)


# ---------------------------------------------------------------------------
# FULL-UNIVERSE TOP 5 GAINERS / LOSERS — 09:22 AND EOD
# ---------------------------------------------------------------------------

def _tm_v22_schema():
    conn = _tm_db()
    cols = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info(top_movers_sessions)"
        ).fetchall()
    }

    if "top5_eod_at" not in cols:
        conn.execute(
            "ALTER TABLE top_movers_sessions ADD COLUMN top5_eod_at TEXT"
        )

    if "top5_eod_json" not in cols:
        conn.execute(
            "ALTER TABLE top_movers_sessions ADD COLUMN top5_eod_json TEXT"
        )

    conn.commit()
    conn.close()

_tm_v22_schema()

def _tm_top5_from_rows(rows):
    clean = [
        dict(r)
        for r in rows or []
        if r.get("symbol") and r.get("move_pct") is not None
    ]

    gainers = sorted(
        [r for r in clean if r["move_pct"] > 0],
        key=lambda r: -r["move_pct"]
    )[:5]

    losers = sorted(
        [r for r in clean if r["move_pct"] < 0],
        key=lambda r: r["move_pct"]
    )[:5]

    return {
        "gainers": gainers,
        "losers": losers,
    }

def _tm_save_top5_eod():
    _tm_build_daily_snapshot(force=True)

    with _top_movers_lock:
        rows = list(_top_movers_cache["rows"].values())

    top = _tm_top5_from_rows(rows)
    trade_date = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).date().isoformat()
    now = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).isoformat()

    conn = _tm_db()
    existing = conn.execute(
        "SELECT trade_date FROM top_movers_sessions WHERE trade_date=?",
        (trade_date,)
    ).fetchone()

    payload = json.dumps(top, ensure_ascii=False)

    if existing:
        conn.execute("""
            UPDATE top_movers_sessions
            SET top5_eod_at=?, top5_eod_json=?
            WHERE trade_date=?
        """, (now, payload, trade_date))
    else:
        conn.execute("""
            INSERT INTO top_movers_sessions(
                trade_date, eod_at, rows_json,
                top5_eod_at, top5_eod_json
            )
            VALUES(?,?,?,?,?)
        """, (
            trade_date,
            now,
            json.dumps([]),
            now,
            payload
        ))

    conn.commit()
    conn.close()

    return {
        "trade_date": trade_date,
        "basis": "EOD",
        "saved_at": now,
        **top,
    }

def _tm_load_top5_history(trade_date, basis="eod"):
    conn = _tm_db()
    row = conn.execute("""
        SELECT
            snapshot_0922_at,
            snapshot_0922_rows_json,
            top5_eod_at,
            top5_eod_json
        FROM top_movers_sessions
        WHERE trade_date=?
    """, (trade_date,)).fetchone()
    conn.close()

    if not row:
        return None

    if basis == "0922":
        if not row[1]:
            return None
        payload = json.loads(row[1])
        return {
            "trade_date": trade_date,
            "basis": "09:22",
            "saved_at": row[0],
            "gainers": (payload.get("gainers", []) if isinstance(payload, dict) else [])[:5],
            "losers": (payload.get("losers", []) if isinstance(payload, dict) else [])[:5],
        }

    if not row[3]:
        return None

    payload = json.loads(row[3])
    return {
        "trade_date": trade_date,
        "basis": "EOD",
        "saved_at": row[2],
        "gainers": payload.get("gainers", []),
        "losers": payload.get("losers", []),
    }

@app.get("/api/top-movers/top5/{trade_date}")
def top_movers_top5_history(
    trade_date: str,
    basis: str = "eod",
):
    snap = _tm_load_top5_history(
        trade_date,
        basis=basis
    )
    if not snap:
        raise HTTPException(
            404,
            "No saved Top-5 history for this date/basis"
        )
    return snap

@app.post("/api/top-movers/top5/eod")
def top_movers_top5_eod_now():
    return _tm_save_top5_eod()

async def _tm_top5_eod_scheduler():
    while True:
        try:
            now = pd.Timestamp.now(tz="Asia/Kolkata")
            minute = now.hour * 60 + now.minute

            if (
                now.weekday() < 5
                and minute >= 15 * 60 + 35
            ):
                date = now.date().isoformat()
                if not _tm_load_top5_history(
                    date,
                    basis="eod"
                ):
                    try:
                        await asyncio.to_thread(
                            _tm_save_top5_eod
                        )
                    except Exception:
                        pass
        except Exception:
            pass

        await asyncio.sleep(30)

@app.on_event("startup")
async def _start_tm_top5_eod_scheduler():
    asyncio.create_task(
        _tm_top5_eod_scheduler()
    )


# ---------------------------------------------------------------------------
# V23 LIVE 5-MINUTE PRICE CHARTS + KEY LEVELS + HISTORY
# ---------------------------------------------------------------------------
MARKET_CHART_CACHE_SECONDS = 20
MARKET_CHART_HISTORY_DAYS = 30
MARKET_CHART_HISTORY_FILE = DATA_DIR / "market_chart_history.json"

# Known Yahoo index tickers. Any unavailable ticker automatically falls back
# to a synthetic equal-weight basket price built from that sector's stocks.
MARKET_INDEX_TICKERS = {
    "Nifty 50": "^NSEI",
    "Nifty Bank": "^NSEBANK",
    "Nifty Auto": "^CNXAUTO",
    "Nifty Energy": "^CNXENERGY",
    "Nifty Fin Services": "^CNXFINANCE",
    "Nifty FMCG": "^CNXFMCG",
    "Nifty IT": "^CNXIT",
    "Nifty Metal": "^CNXMETAL",
    "Nifty Pharma": "^CNXPHARMA",
    "Nifty PSU Bank": "^CNXPSUBANK",
    "Nifty Realty": "^CNXREALTY",
    # Telecom / Consumer use basket fallback because a stable Yahoo index
    # ticker is not assumed here.
}

_market_chart_lock = threading.Lock()
_market_chart_cache = {"ts": 0.0, "value": None, "refreshing": False}

def _load_market_chart_history():
    if not MARKET_CHART_HISTORY_FILE.exists():
        return {}
    try:
        return json.loads(MARKET_CHART_HISTORY_FILE.read_text(encoding='utf-8'))
    except Exception:
        return {}

def _save_market_chart_history(data):
    try:
        tmp = MARKET_CHART_HISTORY_FILE.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
        tmp.replace(MARKET_CHART_HISTORY_FILE)
    except Exception:
        pass

def _to_ist_frame(frame):
    if frame is None or frame.empty:
        return pd.DataFrame()
    f = frame.copy()
    if getattr(f.index, 'tz', None) is None:
        f.index = f.index.tz_localize('UTC').tz_convert('Asia/Kolkata')
    else:
        f.index = f.index.tz_convert('Asia/Kolkata')
    return f

def _price_5m_points(frame, trade_date=None):
    f = _to_ist_frame(frame)
    if f.empty or 'Close' not in f:
        return []
    date = pd.Timestamp(trade_date).date() if trade_date else pd.Timestamp.now(tz='Asia/Kolkata').date()
    f = f[f.index.date == date]
    if f.empty:
        return []
    close = f['Close'].dropna()
    if close.empty:
        return []
    # Data may already be 5m; resampling is harmless and ensures a uniform grid.
    five = close.resample('5min').last().dropna()
    return [{"time": ts.strftime('%H:%M'), "price": float(v)} for ts,v in five.items()]

def _daily_rows(frame):
    if frame is None or frame.empty:
        return pd.DataFrame()
    f = frame.dropna(subset=['Close']).copy()
    if f.empty:
        return f
    # Daily index from yfinance is normally timezone-naive; keep date only.
    f['_date'] = [pd.Timestamp(x).date() for x in f.index]
    return f

def _key_levels_from_daily(daily_frame, current_date=None, intraday_open=None):
    f = _daily_rows(daily_frame)
    current_date = current_date or pd.Timestamp.now(tz='Asia/Kolkata').date()
    completed = f[f['_date'] < current_date].copy() if not f.empty else f
    levels = {}
    if completed is None or completed.empty:
        return levels

    completed = completed.sort_values('_date')
    last = completed.iloc[-1]
    levels.update({
        'PDH': float(last['High']),
        'PDL': float(last['Low']),
        'PDC': float(last['Close']),
    })

    # User terminology: PDH/PDL is the immediately previous session. 1D Ago
    # begins with the session before PDH/PDL, then continues to 4D Ago.
    for ago in range(1,5):
        idx = -(ago+1)
        if len(completed) >= ago+1:
            r = completed.iloc[idx]
            levels[f'{ago}D_Ago_H'] = float(r['High'])
            levels[f'{ago}D_Ago_L'] = float(r['Low'])

    # Weekly OHLC from completed daily bars.
    temp = completed.copy()
    temp['_week'] = [pd.Timestamp(d).isocalendar()[:2] for d in temp['_date']]
    weeks=[]
    for wk,g in temp.groupby('_week',sort=True):
        g=g.sort_values('_date')
        weeks.append({
            'week': wk,
            'open': float(g.iloc[0]['Open']),
            'high': float(g['High'].max()),
            'low': float(g['Low'].min()),
            'close': float(g.iloc[-1]['Close']),
            'last_date': g.iloc[-1]['_date'],
        })
    current_iso = pd.Timestamp(current_date).isocalendar()[:2]
    previous_weeks=[w for w in weeks if w['week'] < current_iso]
    if previous_weeks:
        pw=previous_weeks[-1]
        levels.update({
            'PWO': pw['open'],
            'PWH': pw['high'],
            'PWL': pw['low'],
            'PWC': pw['close'],
        })

    # Current week open: current day's open if Monday/current week has no daily
    # completed row yet; otherwise first daily open of current ISO week.
    current_week_rows = f[[pd.Timestamp(d).isocalendar()[:2] == current_iso for d in f['_date']]] if not f.empty else f
    if current_week_rows is not None and not current_week_rows.empty:
        levels['CWO'] = float(current_week_rows.sort_values('_date').iloc[0]['Open'])
    elif intraday_open is not None:
        levels['CWO'] = float(intraday_open)
    return levels

def _basket_intraday_and_daily(symbols):
    clean = _sanitize_stock_symbols(symbols)
    if not clean:
        return pd.DataFrame(), pd.DataFrame()
    ys=[_sym(s) for s in clean]
    intra = yf.download(
        tickers=ys, period='5d', interval='5m', group_by='ticker',
        auto_adjust=False, threads=True, progress=False, prepost=False,
    )
    daily = yf.download(
        tickers=ys, period='30d', interval='1d', group_by='ticker',
        auto_adjust=False, threads=True, progress=False, prepost=False,
    )
    # Synthetic basket price = equal-weight arithmetic average of constituent prices.
    intra_series=[]
    daily_rows=[]
    for s,y in zip(clean,ys):
        fi=_single(intra,y)
        if not fi.empty and 'Close' in fi:
            fi=_to_ist_frame(fi)
            intra_series.append(fi['Close'].rename(s))
        fd=_single(daily,y)
        if not fd.empty and all(c in fd for c in ['Open','High','Low','Close']):
            d=fd[['Open','High','Low','Close']].copy()
            d.columns=pd.MultiIndex.from_product([[s],d.columns])
            daily_rows.append(d)
    intra_out=pd.DataFrame()
    if intra_series:
        x=pd.concat(intra_series,axis=1)
        intra_out=pd.DataFrame({'Close': x.mean(axis=1,skipna=True)})
    daily_out=pd.DataFrame()
    if daily_rows:
        x=pd.concat(daily_rows,axis=1)
        daily_out=pd.DataFrame(index=x.index)
        for col in ['Open','High','Low','Close']:
            daily_out[col]=x.xs(col,axis=1,level=1).mean(axis=1,skipna=True)
    return intra_out,daily_out

def _build_one_market_series(name, ticker=None, symbols=None):
    basis='Actual Yahoo index'
    intraday=pd.DataFrame(); daily=pd.DataFrame()
    if ticker:
        try:
            raw_i=yf.download(
                tickers=[ticker], period='5d', interval='5m', group_by='ticker',
                auto_adjust=False, threads=True, progress=False, prepost=False,
            )
            raw_d=yf.download(
                tickers=[ticker], period='40d', interval='1d', group_by='ticker',
                auto_adjust=False, threads=True, progress=False, prepost=False,
            )
            intraday=_single(raw_i,ticker)
            daily=_single(raw_d,ticker)
        except Exception:
            intraday=pd.DataFrame(); daily=pd.DataFrame()
    if intraday.empty or 'Close' not in intraday:
        intraday,daily=_basket_intraday_and_daily(symbols or [])
        basis='Synthetic equal-weight basket price'
    points=_price_5m_points(intraday)
    today=pd.Timestamp.now(tz='Asia/Kolkata').date()
    dayf=_to_ist_frame(intraday)
    dayf=dayf[dayf.index.date==today] if not dayf.empty else dayf
    intraday_open=None
    if not dayf.empty:
        if 'Open' in dayf and not dayf['Open'].dropna().empty:
            intraday_open=float(dayf['Open'].dropna().iloc[0])
        elif 'Close' in dayf and not dayf['Close'].dropna().empty:
            intraday_open=float(dayf['Close'].dropna().iloc[0])
    levels=_key_levels_from_daily(daily,today,intraday_open)
    return {
        'name': name,
        'ticker': ticker,
        'basis': basis,
        'points': points,
        'levels': levels,
    }

def _market_chart_build():
    result={}
    result['Nifty 50']=_build_one_market_series('Nifty 50',MARKET_INDEX_TICKERS.get('Nifty 50'),NIFTY50)
    for sector_name,symbols in SECTORS.items():
        result[sector_name]=_build_one_market_series(
            sector_name,
            MARKET_INDEX_TICKERS.get(sector_name),
            symbols,
        )
    now=pd.Timestamp.now(tz='Asia/Kolkata')
    payload={
        'date': now.date().isoformat(),
        'updated_at': now.isoformat(),
        'series': result,
    }
    history=_load_market_chart_history()
    history[payload['date']]=payload
    for d in sorted(history.keys())[:-MARKET_CHART_HISTORY_DAYS]:
        history.pop(d,None)
    _save_market_chart_history(history)
    return payload

def _market_chart_refresh_worker():
    try:
        value=_market_chart_build()
        with _market_chart_lock:
            _market_chart_cache['value']=value
            _market_chart_cache['ts']=time.time()
    except Exception as exc:
        print('[MarketCharts] refresh failed:',exc)
    finally:
        with _market_chart_lock:
            _market_chart_cache['refreshing']=False

def _market_chart_get(trade_date=None):
    today=pd.Timestamp.now(tz='Asia/Kolkata').date().isoformat()
    if trade_date and trade_date != today:
        history=_load_market_chart_history()
        return history.get(trade_date, {'date':trade_date,'updated_at':None,'series':{}})
    with _market_chart_lock:
        age=time.time()-_market_chart_cache['ts'] if _market_chart_cache['ts'] else 999999
        if (_market_chart_cache['value'] is None or age>=MARKET_CHART_CACHE_SECONDS) and not _market_chart_cache['refreshing']:
            _market_chart_cache['refreshing']=True
            threading.Thread(target=_market_chart_refresh_worker,daemon=True).start()
        value=dict(_market_chart_cache['value']) if _market_chart_cache['value'] else {'date':today,'updated_at':None,'series':{}}
    return value

@app.get('/api/market-charts/5m')
def market_charts_5m(trade_date: str | None = None):
    return _market_chart_get(trade_date)

@app.get('/api/market-charts/dates')
def market_chart_dates():
    history=_load_market_chart_history()
    today=pd.Timestamp.now(tz='Asia/Kolkata').date().isoformat()
    dates=sorted(set(history.keys()) | {today})
    return {'dates': dates}

async def _market_chart_history_scheduler():
    while True:
        try:
            now=pd.Timestamp.now(tz='Asia/Kolkata')
            minute=now.hour*60+now.minute
            if now.weekday()<5 and 9*60+15 <= minute <= 15*60+35:
                with _market_chart_lock:
                    stale=(time.time()-_market_chart_cache['ts']>=240) if _market_chart_cache['ts'] else True
                    busy=_market_chart_cache['refreshing']
                    if stale and not busy:
                        _market_chart_cache['refreshing']=True
                        threading.Thread(target=_market_chart_refresh_worker,daemon=True).start()
        except Exception:
            pass
        await asyncio.sleep(60)

@app.on_event('startup')
async def _start_market_chart_history_scheduler():
    asyncio.create_task(_market_chart_history_scheduler())

# ============================================================================
# NSE PRE-MARKET SELECTION
# ============================================================================

PREMARKET_DB = DATA_DIR / "premarket_scanner.sqlite3"
PREMARKET_DEFAULT_KEEP_SESSIONS = 14
PREMARKET_TOP_N = 30

def _pm_db():
    conn = sqlite3.connect(PREMARKET_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            trade_date TEXT PRIMARY KEY,
            captured_at TEXT NOT NULL,
            data1_json TEXT NOT NULL,
            data2_json TEXT NOT NULL,
            rows_json TEXT NOT NULL,
            eod_saved INTEGER NOT NULL DEFAULT 0,
            source_note TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.execute(
        "INSERT OR IGNORE INTO app_settings(key,value) VALUES('premarket_keep_sessions',?)",
        (str(PREMARKET_DEFAULT_KEEP_SESSIONS),)
    )
    conn.commit()
    return conn


def _pm_get_keep_sessions():
    conn = _pm_db()
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key='premarket_keep_sessions'"
    ).fetchone()
    conn.close()
    try:
        value = int(row[0]) if row else PREMARKET_DEFAULT_KEEP_SESSIONS
    except Exception:
        value = PREMARKET_DEFAULT_KEEP_SESSIONS
    return max(1, min(value, 60))

def _pm_set_keep_sessions(value):
    value = max(1, min(int(value), 60))
    conn = _pm_db()
    conn.execute(
        "INSERT INTO app_settings(key,value) VALUES('premarket_keep_sessions',?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(value),)
    )
    conn.commit()
    conn.close()
    _pm_prune()
    return value

def _pm_prune():
    conn = _pm_db()
    dates = [r[0] for r in conn.execute(
        "SELECT trade_date FROM snapshots ORDER BY trade_date DESC"
    ).fetchall()]
    keep_sessions = _pm_get_keep_sessions()
    for d in dates[keep_sessions:]:
        conn.execute("DELETE FROM snapshots WHERE trade_date=?", (d,))
    conn.commit()
    conn.close()

def _pm_save(trade_date, data1, data2, rows, captured_at=None, eod_saved=False, source_note=""):
    captured_at = captured_at or pd.Timestamp.now(tz="Asia/Kolkata").isoformat()
    conn = _pm_db()
    conn.execute("""
        INSERT INTO snapshots(trade_date,captured_at,data1_json,data2_json,rows_json,eod_saved,source_note)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(trade_date) DO UPDATE SET
          captured_at=excluded.captured_at,
          data1_json=excluded.data1_json,
          data2_json=excluded.data2_json,
          rows_json=excluded.rows_json,
          eod_saved=excluded.eod_saved,
          source_note=excluded.source_note
    """, (
        trade_date, captured_at, json.dumps(data1), json.dumps(data2),
        json.dumps(rows), 1 if eod_saved else 0, source_note
    ))
    conn.commit()
    conn.close()
    _pm_prune()

def _pm_load(trade_date):
    conn = _pm_db()
    row = conn.execute("""
        SELECT trade_date,captured_at,data1_json,data2_json,rows_json,eod_saved,source_note
        FROM snapshots WHERE trade_date=?
    """, (trade_date,)).fetchone()
    conn.close()
    if not row:
        return None
    return {
        "trade_date": row[0], "captured_at": row[1],
        "data1": json.loads(row[2]), "data2": json.loads(row[3]),
        "rows": json.loads(row[4]), "eod_saved": bool(row[5]),
        "source_note": row[6] or ""
    }

def _pm_dates():
    conn = _pm_db()
    rows = conn.execute("""
        SELECT trade_date,captured_at,eod_saved,source_note
        FROM snapshots ORDER BY trade_date DESC LIMIT ?
    """, (_pm_get_keep_sessions(),)).fetchall()
    conn.close()
    return [
        {"trade_date": r[0], "captured_at": r[1], "eod_saved": bool(r[2]) and bool(_tm_load_history(r[0]).get("rows")), "source_note": r[3] or ""}
        for r in rows
    ]

def _canon(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())

def _df(obj):
    if isinstance(obj, pd.DataFrame):
        df = obj.copy()
    elif isinstance(obj, list):
        df = pd.DataFrame(obj)
    elif isinstance(obj, dict):
        if isinstance(obj.get("data"), list):
            df = pd.DataFrame(obj["data"])
        else:
            df = pd.DataFrame(obj)
    else:
        df = pd.DataFrame()

    if not df.empty:
        try:
            df.index.name = None
        except Exception:
            pass
        df = df.reset_index(drop=True)

        # Remove duplicate column names, keeping the first.
        df = df.loc[:, ~df.columns.duplicated()].copy()

    return df

def _find_col(df, names):
    # Columns are normalized to strings here to avoid pandas ambiguity.
    cmap = {}
    for c in list(df.columns):
        key = _canon(c)
        if key not in cmap:
            cmap[key] = c
    for name in names:
        key = _canon(name)
        if key in cmap:
            return cmap[key]
    # fuzzy containment fallback
    for name in names:
        key = _canon(name)
        for k, original in cmap.items():
            if key and (key in k or k in key):
                return original
    return None

def _to_num_series(series):
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False),
        errors="coerce"
    )

def _records_safe(df):
    clean = df.copy()
    clean = clean.where(pd.notna(clean), None)
    return json.loads(clean.to_json(orient="records"))

def _normalize_data1(df):
    if df.empty:
        return pd.DataFrame(columns=["symbol","value_crore","pdc","iep","preopen_pct"])

    # NseKit may return Symbol as both index name and normal column.
    # Reset/drop the index so pandas does not treat "symbol" as ambiguous.
    df = df.copy()
    try:
        df.index.name = None
    except Exception:
        pass
    df = df.reset_index(drop=True)

    sym = _find_col(df, ["symbol"])
    value = _find_col(df, ["value crores","value crore","value","totalTurnover","turnover"])
    pdc = _find_col(df, ["prev close","previous close","previousClose","prevClose"])
    iep = _find_col(df, ["iep","final price","finalPrice","lastPrice"])
    pct = _find_col(df, ["%chng","%chg","pChange","percentChange","pchange"])

    out = pd.DataFrame()
    out["symbol"] = df[sym].astype(str).str.strip().str.upper() if sym else ""
    out["value_crore"] = _to_num_series(df[value]) if value else None

    # NSE API totalTurnover is commonly raw rupees; UI/CSV "VALUE (₹ Crores)" is already crores.
    if value and _canon(value) in ("totalturnover","turnover"):
        median = out["value_crore"].dropna().median()
        if pd.notna(median) and median > 10000:
            out["value_crore"] = out["value_crore"] / 1e7

    out["pdc"] = _to_num_series(df[pdc]) if pdc else None
    out["iep"] = _to_num_series(df[iep]) if iep else None
    out["preopen_pct"] = _to_num_series(df[pct]) if pct else None
    out = out[out["symbol"].ne("")].drop_duplicates("symbol", keep="first")
    return out

def _normalize_data2(df):
    if df.empty:
        return pd.DataFrame(columns=["symbol","expiry","final_volume","final_price","fut_preopen_pct"])

    # Avoid ambiguity when NseKit places symbol/expiry in the dataframe index.
    df = df.copy()
    try:
        df.index.name = None
    except Exception:
        pass
    df = df.reset_index(drop=True)

    instrument = _find_col(df, ["instrument type","instrumentType","instrument"])
    if instrument:
        mask = df[instrument].astype(str).str.upper().str.contains("FUTSTK|STOCK FUT", regex=True, na=False)
        if mask.any():
            df = df[mask].copy()

    sym = _find_col(df, ["symbol","underlying"])
    expiry = _find_col(df, ["expiry date","expiryDate","expiry"])
    volume = _find_col(df, ["final volume contracts","final volume","finalVolume","final quantity","finalQuantity","volume contracts"])
    final_price = _find_col(df, ["final price","finalPrice","iep","lastPrice"])
    pct = _find_col(df, ["%chng","%chg","pChange","percentChange","pchange"])

    out = pd.DataFrame()
    out["symbol"] = df[sym].astype(str).str.strip().str.upper() if sym else ""
    out["expiry"] = df[expiry].astype(str).str.strip() if expiry else ""
    out["final_volume"] = _to_num_series(df[volume]) if volume else None
    out["final_price"] = _to_num_series(df[final_price]) if final_price else None
    out["fut_preopen_pct"] = _to_num_series(df[pct]) if pct else None
    out = out[out["symbol"].ne("")].copy()

    # Remove next-month / duplicate expiries: keep nearest parsed expiry per symbol.
    parsed = pd.to_datetime(out["expiry"], errors="coerce", dayfirst=True)
    out["_expiry_dt"] = parsed
    out["_expiry_sort"] = out["_expiry_dt"].fillna(pd.Timestamp.max)
    out = out.sort_values(["symbol","_expiry_sort","final_volume"], ascending=[True,True,False])
    out = out.drop_duplicates("symbol", keep="first")
    return out.drop(columns=["_expiry_dt","_expiry_sort"], errors="ignore")

def _build_common(data1_df, data2_df):
    d1 = _normalize_data1(data1_df)
    d2 = _normalize_data2(data2_df)

    d1 = d1.sort_values("value_crore", ascending=False, na_position="last").head(PREMARKET_TOP_N).copy()
    d2 = d2.sort_values("final_volume", ascending=False, na_position="last").head(PREMARKET_TOP_N).copy()

    d1["value_rank"] = range(1, len(d1)+1)
    d2["volume_rank"] = range(1, len(d2)+1)

    common = d1.merge(d2, on="symbol", how="inner", suffixes=("_eq","_fut"))
    common["combined_score"] = common["value_rank"] + common["volume_rank"]
    common = common.sort_values(["combined_score","value_rank","volume_rank","symbol"]).reset_index(drop=True)
    common["combined_rank"] = range(1, len(common)+1)

    rows = _records_safe(common)
    return _records_safe(d1), _records_safe(d2), rows

def _nse_session_json(url):
    import requests
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/pre-open-market-fno",
    }
    s = requests.Session()
    for warm in [
        "https://www.nseindia.com/market-data/pre-open-market-fno",
        "https://www.nseindia.com/"
    ]:
        try:
            s.get(warm, headers=headers, timeout=12)
            r = s.get(url, headers=headers, timeout=15)
            if r.ok:
                return r.json()
        except Exception:
            pass
    raise RuntimeError("NSE pre-open request failed / was blocked")

def _fetch_data1_direct():
    payload = _nse_session_json("https://www.nseindia.com/api/market-data-pre-open?key=FO")
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    flat = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        md = item.get("metadata", item)
        if isinstance(md, dict):
            flat.append(md)
    return pd.DataFrame(flat)

def _fetch_via_nsekit():
    try:
        import NseKit
    except Exception as exc:
        raise RuntimeError(f"NseKit import failed: {exc}")

    get = NseKit.Nse()
    d1 = get.pre_market_info("Securities in F&O")
    d2 = get.pre_market_derivatives_info("Stock Futures")
    return _df(d1), _df(d2)

def capture_premarket_snapshot():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    trade_date = now.date().isoformat()

    source_note = ""
    try:
        d1, d2 = _fetch_via_nsekit()
        source_note = "NSE via NseKit: Securities in F&O + Stock Futures"
    except Exception as first_error:
        # Direct NSE endpoint can still capture Data 1. Data 2 needs NseKit or CSV import.
        d1 = _fetch_data1_direct()
        raise RuntimeError(
            f"Data 1 fetched directly, but Stock Futures pre-open fetch failed. "
            f"Use CSV import fallback. NseKit error: {first_error}"
        )

    top1, top2, rows = _build_common(d1, d2)
    _pm_save(trade_date, top1, top2, rows, captured_at=now.isoformat(), source_note=source_note)
    return _pm_load(trade_date)

def _live_price_enrichment(symbols, trade_date=None):
    symbols = _sanitize_stock_symbols(symbols)
    if not symbols:
        return {}

    trade_date = trade_date or pd.Timestamp.now(tz="Asia/Kolkata").date().isoformat()
    ysymbols = [f"{s}.NS" for s in symbols]

    # 5-minute data is enough for both first-5m and first-15m analysis.
    data5 = yf.download(
        tickers=ysymbols, period="30d", interval="5m", group_by="ticker",
        auto_adjust=False, threads=True, progress=False, prepost=False
    )
    daily = yf.download(
        tickers=ysymbols, period="35d", interval="1d", group_by="ticker",
        auto_adjust=False, threads=True, progress=False, prepost=False
    )

    result = {}
    target_date = pd.Timestamp(trade_date).date()
    now_ist = pd.Timestamp.now(tz="Asia/Kolkata")
    is_today = target_date == now_ist.date()
    now_minutes = now_ist.hour * 60 + now_ist.minute
    first5_complete_now = (not is_today) or now_minutes >= (9 * 60 + 20)
    first15_complete_now = (not is_today) or now_minutes >= (9 * 60 + 30)

    def apply_patterns(item, prefix, o, h, l, c):
        pdc, pdh, pdl = item["pdc"], item["pdh"], item["pdl"]
        item[f"{prefix}_open"] = o
        item[f"{prefix}_high"] = h
        item[f"{prefix}_low"] = l
        item[f"{prefix}_close"] = c
        if pdc is not None and pdh is not None and pdl is not None:
            item[f"{prefix}_bull_reclaim_pdh"] = o > pdc and l < pdc and c > pdh
            item[f"{prefix}_bear_reclaim_pdl"] = o < pdc and h > pdc and c < pdl
            item[f"{prefix}_close_above_pdh"] = c > pdh
            item[f"{prefix}_close_below_pdl"] = c < pdl

    for sym, ys in zip(symbols, ysymbols):
        f5 = _single(data5, ys)
        fd = _single(daily, ys)

        item = {
            "live_pct": None, "gap_pct": None, "current_price": None,
            "pdc": None, "pdh": None, "pdl": None,

            "first5_status": "No data",
            "first5_open": None, "first5_high": None, "first5_low": None, "first5_close": None,
            "first5_bull_reclaim_pdh": False,
            "first5_bear_reclaim_pdl": False,
            "first5_close_above_pdh": False,
            "first5_close_below_pdl": False,

            "first15_status": "No data",
            "first15_open": None, "first15_high": None, "first15_low": None, "first15_close": None,
            "first15_bull_reclaim_pdh": False,
            "first15_bear_reclaim_pdl": False,
            "first15_close_above_pdh": False,
            "first15_close_below_pdl": False,
        }

        # Previous-day reference levels + current-day gap/live move.
        if not fd.empty and "Close" in fd:
            fd = fd.dropna(subset=["Close"]).copy()
            if getattr(fd.index, "tz", None) is not None:
                fd.index = fd.index.tz_convert("Asia/Kolkata")
            date_rows = [(idx.date(), row) for idx, row in fd.iterrows()]
            previous_rows = [r for d, r in date_rows if d < target_date]
            current_rows = [r for d, r in date_rows if d == target_date]

            if previous_rows:
                prev = previous_rows[-1]
                item["pdc"] = float(prev["Close"])
                item["pdh"] = float(prev["High"]) if "High" in prev else None
                item["pdl"] = float(prev["Low"]) if "Low" in prev else None

            if current_rows:
                cur = current_rows[-1]
                current_price = float(cur["Close"])
                today_open = float(cur["Open"]) if "Open" in cur else None
                item["current_price"] = current_price
                if item["pdc"]:
                    item["live_pct"] = (current_price - item["pdc"]) / item["pdc"] * 100.0
                    if today_open is not None:
                        item["gap_pct"] = (today_open - item["pdc"]) / item["pdc"] * 100.0

        # Intraday first 5m / 15m.
        if not f5.empty and "Close" in f5:
            f5 = f5.dropna(subset=["Close"]).copy()
            if getattr(f5.index, "tz", None) is None:
                f5.index = f5.index.tz_localize("UTC").tz_convert("Asia/Kolkata")
            else:
                f5.index = f5.index.tz_convert("Asia/Kolkata")

            day = f5[f5.index.date == target_date]
            regular = day[
                (day.index.hour > 9) |
                ((day.index.hour == 9) & (day.index.minute >= 15))
            ]

            # First 5-minute candle = 09:15-09:20.
            if not first5_complete_now:
                item["first5_status"] = "Candle in formation"
            elif len(regular) >= 1:
                bar = regular.iloc[0]
                o, h, l, c = [float(bar[x]) for x in ["Open", "High", "Low", "Close"]]
                apply_patterns(item, "first5", o, h, l, c)
                item["first5_status"] = "Complete"
            else:
                item["first5_status"] = "No data"

            # First 15-minute candle = 09:15-09:30, built from the first three 5m bars.
            if not first15_complete_now:
                item["first15_status"] = "Candle in formation"
            elif len(regular) >= 3:
                bars = regular.iloc[:3]
                o = float(bars.iloc[0]["Open"])
                h = float(bars["High"].max())
                l = float(bars["Low"].min())
                c = float(bars.iloc[2]["Close"])
                apply_patterns(item, "first15", o, h, l, c)
                item["first15_status"] = "Complete"
            else:
                item["first15_status"] = "No data"
        else:
            if not first5_complete_now:
                item["first5_status"] = "Candle in formation"
            if not first15_complete_now:
                item["first15_status"] = "Candle in formation"

        result[sym] = item

    return result


# ============================================================================
# V20 PRE-MARKET CACHE-FIRST LIVE ENRICHMENT
# ============================================================================
PREMARKET_LIVE_CACHE_SECONDS = 8
PREMARKET_LIVE_CACHE_FILE = DATA_DIR / "premarket_live_cache.json"

_premarket_live_lock = threading.Lock()
_premarket_live_cache = {
    "ts": 0.0,
    "trade_date": None,
    "snapshot": None,
    "refreshing": False,
}

def _pm_restore_live_cache():
    try:
        if not PREMARKET_LIVE_CACHE_FILE.exists():
            return
        payload = json.loads(
            PREMARKET_LIVE_CACHE_FILE.read_text(encoding="utf-8")
        )
        snap = payload.get("snapshot")
        if isinstance(snap, dict):
            with _premarket_live_lock:
                _premarket_live_cache["snapshot"] = snap
                _premarket_live_cache["trade_date"] = payload.get("trade_date")
                _premarket_live_cache["ts"] = 0.0
    except Exception:
        pass

def _pm_write_live_cache():
    try:
        with _premarket_live_lock:
            snap = _premarket_live_cache["snapshot"]
            trade_date = _premarket_live_cache["trade_date"]
        if not snap:
            return
        tmp = PREMARKET_LIVE_CACHE_FILE.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(
                {"trade_date": trade_date, "snapshot": snap},
                ensure_ascii=False
            ),
            encoding="utf-8"
        )
        tmp.replace(PREMARKET_LIVE_CACHE_FILE)
    except Exception:
        pass

_pm_restore_live_cache()

def _pm_live_refresh_worker(trade_date):
    try:
        snap = enrich_premarket_snapshot(
            trade_date,
            persist=False,
            eod=False
        )
        if snap:
            with _premarket_live_lock:
                _premarket_live_cache["snapshot"] = snap
                _premarket_live_cache["trade_date"] = trade_date
                _premarket_live_cache["ts"] = time.time()
            _pm_write_live_cache()
    except Exception as exc:
        print("[PreMarketCache] refresh failed:", exc)
    finally:
        with _premarket_live_lock:
            _premarket_live_cache["refreshing"] = False

def _pm_trigger_live_refresh(trade_date, force=False):
    with _premarket_live_lock:
        same_date = _premarket_live_cache["trade_date"] == trade_date
        has_data = same_date and _premarket_live_cache["snapshot"] is not None
        age = (
            time.time() - _premarket_live_cache["ts"]
            if _premarket_live_cache["ts"] else 999999
        )
        refreshing = _premarket_live_cache["refreshing"]

        if (force or not has_data or age >= PREMARKET_LIVE_CACHE_SECONDS) and not refreshing:
            _premarket_live_cache["refreshing"] = True
            threading.Thread(
                target=_pm_live_refresh_worker,
                args=(trade_date,),
                daemon=True
            ).start()

def _pm_cached_live_snapshot(trade_date):
    _pm_trigger_live_refresh(trade_date, force=False)

    with _premarket_live_lock:
        if (
            _premarket_live_cache["trade_date"] == trade_date
            and _premarket_live_cache["snapshot"] is not None
        ):
            return dict(_premarket_live_cache["snapshot"])

    return _pm_load(trade_date)

def _oi_map():
    try:
        d = fetch_live_oi(force=False)
        return {r["symbol"]: r for r in d.get("rows", [])}
    except Exception:
        return {}

def enrich_premarket_snapshot(trade_date, persist=False, eod=False):
    snap = _pm_load(trade_date)
    if not snap:
        return None

    rows = snap["rows"]
    symbols = [str(r.get("symbol","")).upper() for r in rows if r.get("symbol")]
    prices = _live_price_enrichment(symbols, trade_date)
    oi = _oi_map()

    enriched = []
    for row in rows:
        r = dict(row)
        sym = str(r.get("symbol","")).upper()
        r.update(prices.get(sym, {}))
        oirow = oi.get(sym, {})
        r["oi_change_pct"] = oirow.get("oi_change_pct")
        r["oi_change"] = oirow.get("change_oi")
        enriched.append(r)

    if persist:
        _pm_save(
            trade_date, snap["data1"], snap["data2"], enriched,
            captured_at=snap["captured_at"], eod_saved=eod or snap["eod_saved"],
            source_note=snap["source_note"]
        )
    snap["rows"] = enriched
    snap["eod_saved"] = eod or snap["eod_saved"]
    return snap

async def _premarket_scheduler():
    # Fast capture loop:
    # - from 09:08 IST, if today's snapshot is missing, retry every 3 seconds.
    # - once captured, stop re-downloading and only enrich live fields on demand.
    # - EOD freeze after 15:32.
    while True:
        sleep_seconds = 20
        try:
            now = pd.Timestamp.now(tz="Asia/Kolkata")
            d = now.date().isoformat()
            weekday = now.weekday() < 5
            minutes = now.hour * 60 + now.minute

            if weekday:
                snap = _pm_load(d)

                if minutes >= (9 * 60 + 8) and minutes < (15 * 60 + 30) and snap is None:
                    sleep_seconds = 3
                    try:
                        await asyncio.to_thread(capture_premarket_snapshot)
                        sleep_seconds = 20
                    except Exception:
                        # Keep retrying quickly until NSE publishes/responds successfully.
                        sleep_seconds = 3

                snap = _pm_load(d)
                if minutes >= (15 * 60 + 32) and snap and not snap["eod_saved"]:
                    try:
                        await asyncio.to_thread(enrich_premarket_snapshot, d, True, True)
                    except Exception:
                        pass

        except Exception:
            pass

        await asyncio.sleep(sleep_seconds)


@app.on_event("startup")
async def _warm_sector_snapshot_v11():
    _trigger_sector_snapshot_refresh(force=True)

@app.on_event("startup")
async def _start_premarket_scheduler():
    asyncio.create_task(_premarket_scheduler())


@app.get("/api/premarket/settings")
def premarket_settings():
    return {"keep_sessions": _pm_get_keep_sessions()}

@app.post("/api/premarket/settings")
def update_premarket_settings(keep_sessions: int):
    return {"keep_sessions": _pm_set_keep_sessions(keep_sessions)}

@app.get("/api/premarket/dates")
def premarket_dates():
    return {"dates": _pm_dates()}

@app.post("/api/premarket/capture")
async def premarket_capture():
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(capture_premarket_snapshot),
            timeout=35
        )
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail=(
                "NSE pre-market capture timed out after 35 seconds. "
                "Retry once or use the Equity + Stock Futures CSV upload fallback."
            )
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=(
                "Pre-market capture failed: "
                + str(exc)
                + ". If NSE/NseKit format changed, use the two CSV upload fields as fallback."
            )
        )

@app.get("/api/premarket/{trade_date}")
def premarket_view(trade_date: str, live: bool = True):
    snap = _pm_load(trade_date)
    if not snap:
        raise HTTPException(
            status_code=404,
            detail="No saved pre-market snapshot for this date"
        )

    today = pd.Timestamp.now(
        tz="Asia/Kolkata"
    ).date().isoformat()

    if (
        live
        and trade_date == today
        and not snap["eod_saved"]
    ):
        cached = _pm_cached_live_snapshot(trade_date)
        if cached:
            return cached

    return snap


@app.post("/api/premarket/import")
async def premarket_import(
    trade_date: str,
    data1: UploadFile = File(...),
    data2: UploadFile = File(...)
):
    b1 = await data1.read()
    b2 = await data2.read()

    def read_csv_bytes(b):
        # NSE CSVs can occasionally contain BOM / mixed encodings.
        for enc in ("utf-8-sig","utf-8","latin1"):
            try:
                return pd.read_csv(io.BytesIO(b), encoding=enc)
            except Exception:
                continue
        raise HTTPException(status_code=400, detail="Unable to parse CSV")

    d1 = read_csv_bytes(b1)
    d2 = read_csv_bytes(b2)
    top1, top2, rows = _build_common(d1, d2)
    _pm_save(
        trade_date, top1, top2, rows,
        source_note="Manual NSE CSV import (Securities in F&O + Stock Futures)"
    )
    try:
        enrich_premarket_snapshot(trade_date, persist=True, eod=(trade_date < pd.Timestamp.now(tz="Asia/Kolkata").date().isoformat()))
    except Exception:
        pass
    return _pm_load(trade_date)

@app.post("/api/premarket/eod/{trade_date}")
def premarket_save_eod(trade_date: str):
    snap = enrich_premarket_snapshot(trade_date, persist=True, eod=True)
    if not snap:
        raise HTTPException(status_code=404, detail="No snapshot")
    return snap

@app.get("/api/premarket/download/{trade_date}")
def premarket_download(trade_date: str):
    snap = _pm_load(trade_date)
    if not snap:
        raise HTTPException(status_code=404, detail="No snapshot")
    df = pd.DataFrame(snap["rows"])
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="NSE_PreMarket_Selection_{trade_date}.csv"'}
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv", headers=headers)

@app.middleware("http")
async def no_cache_middleware(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@app.get("/api/version")
def version():
    return {"version": "2.1", "ui": "sorts + PDH/PDO/PDC/PDL filter + advances/declines"}

@app.get("/favicon.ico")
def favicon():
    from fastapi.responses import Response
    return Response(status_code=204, headers={"Cache-Control":"no-store"})

@app.get("/")
def home(): return FileResponse(BASE/"static"/"index.html")

@app.get("/manifest.webmanifest")
def manifest(): return FileResponse(BASE/"static"/"manifest.webmanifest")

@app.get("/service-worker.js")
def sw(): return FileResponse(BASE/"static"/"service-worker.js",media_type="application/javascript")


@app.get("/api/yahoo-symbol-status")
def yahoo_symbol_status():
    return {
        "permanent_excluded": sorted(_yahoo_permanent_excluded),
        "invalid_runtime": sorted(_yahoo_invalid_symbols),
        "invalid_runtime_count": len(_yahoo_invalid_symbols),
        "filter_mode": "central_yf_download_wrapper",
        "file": str(INVALID_YAHOO_FILE.name),
    }



@app.get("/api/cache-status")
def cache_status():
    with _sector_snapshot_lock:
        rows_count = len(
            _sector_snapshot["rows_by_symbol"]
        )
        updated_at = _sector_snapshot[
            "updated_at"
        ]
        refreshing = _sector_snapshot[
            "refreshing"
        ]

    with _sector_levels_lock:
        level_count = len(
            _sector_levels["rows_by_symbol"]
        )
        level_date = _sector_levels[
            "trade_date"
        ]

    return {
        "sector_rows_cached": rows_count,
        "sector_updated_at": updated_at,
        "sector_refreshing": refreshing,
        "previous_levels_cached": level_count,
        "previous_levels_date": level_date,
        "persistent_cache": True,
        "sector_refresh_target_seconds": SECTOR_SNAPSHOT_REFRESH_SECONDS,
    }


@app.get("/api/sector-browser-cache")
def sector_browser_cache():
    _trigger_sector_snapshot_refresh(force=False)

    with _sector_snapshot_lock:
        source = dict(_sector_snapshot["rows_by_symbol"])
        updated_at = _sector_snapshot["updated_at"]
        refreshing = bool(_sector_snapshot["refreshing"])

    payload = {}
    for sector_name, symbols in SECTORS.items():
        clean = _sanitize_stock_symbols(symbols)
        payload[sector_name] = [
            dict(source[s])
            for s in clean
            if s in source
        ]

    return {
        "sectors": payload,
        "updated_at": updated_at,
        "refreshing": refreshing,
    }

@app.get("/api/sectors")
def sectors(): return {"sectors":list(SECTORS.keys())}


@app.post("/api/sector-refresh")
def sector_refresh():
    _trigger_sector_snapshot_refresh(force=True)
    return {"status": "refresh_started"}

@app.get("/api/sector/{sector}")
def sector(sector:str): return fetch_sector(sector)

@app.get("/api/breadth/{sector}")
def breadth(sector:str):
    fetch_sector(sector)
    hist=_load_history()
    today=pd.Timestamp.now(tz="Asia/Kolkata").date().isoformat()
    sector_hist=hist.get(sector,{})
    dates=sorted(sector_hist.keys())
    chosen=today if today in sector_hist else (dates[-1] if dates else None)
    points=sector_hist.get(chosen,[]) if chosen else []
    return {"sector":sector,"date":chosen,"points":points,
            "market_basis":"live" if chosen==today else "last available closed session"}


_summary_cache_lock = threading.Lock()
_summary_cache = {
    "ts": 0.0,
    "value": None,
}

@app.get("/api/summary")
def summary():
    with _summary_cache_lock:
        if (
            _summary_cache["value"] is not None
            and time.time() - _summary_cache["ts"] < 3
        ):
            return _summary_cache["value"]

    out = []

    for sector_name in SECTORS:
        d = fetch_sector(sector_name)
        vals = [
            r["move_pct"]
            for r in d["rows"]
            if r.get("move_pct") is not None
        ]

        avg = sum(vals) / len(vals) if vals else None
        adv = sum(1 for v in vals if v > 0)
        dec = sum(1 for v in vals if v < 0)

        out.append({
            "sector": sector_name,
            "avg_move_pct": avg,
            "advances": adv,
            "declines": dec,
            "stocks": len(vals),
        })

    value = {
        "rows": out,
        "updated_at": pd.Timestamp.now(
            tz="Asia/Kolkata"
        ).isoformat(),
    }

    with _summary_cache_lock:
        _summary_cache["value"] = value
        _summary_cache["ts"] = time.time()

    return value

# ============================================================================
# V27 CLOUD EDITION — OPTIONAL REMOTE HISTORY BACKUP / RESTORE
# ============================================================================
# Free web hosts can have an ephemeral local filesystem.  When the following
# environment variables are configured, v27 mirrors /data to a private
# Supabase Storage object and restores it after a cloud restart.
#
#   SUPABASE_URL=https://<project>.supabase.co
#   SUPABASE_SERVICE_KEY=<service-role key>
#   SUPABASE_BUCKET=scanner-backups
#   SUPABASE_OBJECT=v27/data-backup.zip
#
# Local Windows/Docker use remains unchanged when these variables are absent.

import os as _os
import shutil as _shutil
import tempfile as _tempfile
import zipfile as _zipfile

_CLOUD_BACKUP_LOCK = threading.Lock()
_CLOUD_LAST_FINGERPRINT = None


def _cloud_cfg():
    url = _os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    key = _os.getenv("SUPABASE_SERVICE_KEY", "").strip()
    bucket = _os.getenv("SUPABASE_BUCKET", "scanner-backups").strip()
    obj = _os.getenv("SUPABASE_OBJECT", "v27/data-backup.zip").strip().lstrip("/")
    return url, key, bucket, obj


def _cloud_enabled():
    url, key, bucket, obj = _cloud_cfg()
    return bool(url and key and bucket and obj)


def _data_fingerprint():
    parts = []
    try:
        for p in sorted(DATA_DIR.rglob("*")):
            if p.is_file() and not p.name.startswith(".cloud-"):
                st = p.stat()
                parts.append((str(p.relative_to(DATA_DIR)), st.st_size, st.st_mtime_ns))
    except Exception:
        pass
    return repr(parts)


def _safe_snapshot_data(snapshot_dir: Path):
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    for src in DATA_DIR.rglob("*"):
        if not src.is_file() or src.name.startswith(".cloud-"):
            continue
        rel = src.relative_to(DATA_DIR)
        dst = snapshot_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            if src.suffix.lower() in {".sqlite", ".sqlite3", ".db"}:
                sconn = sqlite3.connect(str(src), timeout=10)
                dconn = sqlite3.connect(str(dst), timeout=10)
                try:
                    sconn.backup(dconn)
                finally:
                    dconn.close(); sconn.close()
            else:
                _shutil.copy2(src, dst)
        except Exception:
            # A single transient cache file must not prevent history backup.
            continue


def _cloud_backup_now(force: bool = False):
    global _CLOUD_LAST_FINGERPRINT
    if not _cloud_enabled():
        return {"enabled": False, "saved": False, "reason": "cloud backup not configured"}

    with _CLOUD_BACKUP_LOCK:
        fp = _data_fingerprint()
        if not force and fp == _CLOUD_LAST_FINGERPRINT:
            return {"enabled": True, "saved": False, "reason": "no data changes"}

        url, key, bucket, obj = _cloud_cfg()
        with _tempfile.TemporaryDirectory(prefix="scanner-v27-") as td:
            td = Path(td)
            snap = td / "data"
            _safe_snapshot_data(snap)
            zip_path = td / "data-backup.zip"
            with _zipfile.ZipFile(zip_path, "w", _zipfile.ZIP_DEFLATED) as zf:
                for p in snap.rglob("*"):
                    if p.is_file():
                        zf.write(p, p.relative_to(snap).as_posix())

            endpoint = f"{url}/storage/v1/object/{bucket}/{obj}"
            headers = {
                "Authorization": f"Bearer {key}",
                "apikey": key,
                "Content-Type": "application/zip",
                "x-upsert": "true",
            }
            with zip_path.open("rb") as fh:
                r = requests.post(endpoint, headers=headers, data=fh, timeout=60)
            if r.status_code not in (200, 201):
                raise RuntimeError(f"cloud backup failed: HTTP {r.status_code}: {r.text[:300]}")

        _CLOUD_LAST_FINGERPRINT = fp
        return {"enabled": True, "saved": True, "object": obj}


def _cloud_restore_now():
    global _CLOUD_LAST_FINGERPRINT
    if not _cloud_enabled():
        return {"enabled": False, "restored": False, "reason": "cloud backup not configured"}

    with _CLOUD_BACKUP_LOCK:
        url, key, bucket, obj = _cloud_cfg()
        endpoint = f"{url}/storage/v1/object/{bucket}/{obj}"
        headers = {"Authorization": f"Bearer {key}", "apikey": key}
        r = requests.get(endpoint, headers=headers, timeout=60)
        if r.status_code == 404:
            return {"enabled": True, "restored": False, "reason": "no remote backup yet"}
        if r.status_code != 200:
            raise RuntimeError(f"cloud restore failed: HTTP {r.status_code}: {r.text[:300]}")

        with _tempfile.TemporaryDirectory(prefix="scanner-v27-restore-") as td:
            zip_path = Path(td) / "backup.zip"
            zip_path.write_bytes(r.content)
            extract = Path(td) / "extract"
            extract.mkdir()
            with _zipfile.ZipFile(zip_path, "r") as zf:
                # Reject path traversal in a damaged/untrusted remote archive.
                for member in zf.infolist():
                    target = (extract / member.filename).resolve()
                    if extract.resolve() not in target.parents and target != extract.resolve():
                        raise RuntimeError("unsafe backup archive path")
                zf.extractall(extract)
            for src in extract.rglob("*"):
                if src.is_file():
                    dst = DATA_DIR / src.relative_to(extract)
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    _shutil.copy2(src, dst)

        _CLOUD_LAST_FINGERPRINT = _data_fingerprint()
        return {"enabled": True, "restored": True, "object": obj}


async def _cloud_backup_loop():
    # Frequent enough to protect captures, but uploads only when /data changed.
    while True:
        try:
            await asyncio.to_thread(_cloud_backup_now, False)
        except Exception as exc:
            print(f"[v27 cloud backup] {exc}")
        await asyncio.sleep(60)


@app.on_event("startup")
async def _v27_cloud_startup():
    if _cloud_enabled():
        try:
            result = await asyncio.to_thread(_cloud_restore_now)
            print(f"[v27 cloud restore] {result}")
        except Exception as exc:
            print(f"[v27 cloud restore] {exc}")
        asyncio.create_task(_cloud_backup_loop())


@app.get("/api/cloud/health")
def v27_cloud_health():
    url, key, bucket, obj = _cloud_cfg()
    return {
        "version": "v27-cloud",
        "status": "ok",
        "ist_time": pd.Timestamp.now(tz="Asia/Kolkata").isoformat(),
        "remote_history_configured": bool(url and key),
        "bucket": bucket if url and key else None,
        "object": obj if url and key else None,
    }


@app.post("/api/cloud/backup-now")
def v27_cloud_backup_now():
    try:
        return _cloud_backup_now(True)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))
