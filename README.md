# India F&O Intraday Scanner v4

## Tabs

1. Sectorial Stock Selection
2. Market Breadth
3. NSE Pre-Market Selection
4. Futures OI

## NSE Pre-Market Selection

Automatic scheduler:
- Starts attempting the one-time pre-open capture after 09:08 IST.
- Data 1: NSE Pre-Open -> Securities in F&O
- Data 2: NSE Pre-Open Equity Derivatives -> Stock Futures
- Stock Futures are de-duplicated by symbol by keeping the nearest expiry.
- Data 1 is sorted by Value (₹ Crores), top 30.
- Data 2 is sorted by Final Volume (Contracts), top 30.
- Only common symbols are kept.
- Value Rank, Volume Rank, Combined Score and Combined Rank are calculated.
- EOD snapshot is frozen automatically after 15:32 IST.
- Latest 14 stored sessions are retained.

Live enrichment:
- OI Change % from NSE OI Spurts
- Live / closing % versus PDC
- Gap % = current-day open versus PDC
- First 5-minute conditions:
  1. Open > PDC, Low < PDC, Close > PDH
  2. Open < PDC, High > PDC, Close < PDL
  3. Open anywhere, Close > PDH
  4. Open anywhere, Close < PDL

Historical data:
- Date selector reads saved snapshots.
- CSV download is available for every saved date.
- Exact old pre-open rankings cannot be reconstructed from the current-session NSE public API.
- To backfill older dates exactly, use the two CSV import fields with the NSE files you downloaded on those dates.

## Install / run

```powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --host 0.0.0.0 --port 8000
```

Then open http://127.0.0.1:8000

## Important

Keep the server running before 09:08 IST for automatic capture. If NSE blocks automated access,
use the CSV import fallback for that date. NSE public webpages/APIs can apply cookie/session controls.


## v4.1 changes

### Instant pre-market processing
- Starting at 09:08 IST, if today's snapshot is not yet saved, the backend retries every 3 seconds.
- The moment both datasets are available, it immediately:
  1. normalizes Securities in F&O data,
  2. sorts by Value ₹ Crores and keeps Top 30,
  3. normalizes Stock Futures,
  4. removes duplicate/next-month contracts by keeping the nearest expiry,
  5. sorts by Final Volume and keeps Top 30,
  6. intersects the two Top-30 lists,
  7. calculates Value Rank, Volume Rank, Combined Score and Combined Rank,
  8. saves the result for the trading date.

### Editable history retention
- Pre-Market tab now has "Saved trading sessions".
- Choose 1-60 sessions and click "Save history setting".
- The database automatically prunes older sessions beyond this limit.

### Manual CSV upload
- Historical/backup workflow still accepts both:
  - Securities in F&O CSV
  - Stock Futures CSV
- The imported files go through the exact same Top-30/common-stock ranking process.

### 5-minute and 15-minute candle analysis
Both timeframes use the same four conditions:
1. Open > PDC, Low < PDC, Close > PDH
2. Open < PDC, High > PDC, Close < PDL
3. Open anywhere, Close > PDH
4. Open anywhere, Close < PDL

Completion:
- First 5m candle is complete after 09:20 IST.
- First 15m candle is complete after 09:30 IST.
- Before completion, the corresponding table cell explicitly shows:
  "Candle in formation"

The 15-minute candle is constructed from the 09:15, 09:20 and 09:25 five-minute bars.


## v4.1.1 fix
- Added missing `import re` used by pre-market column normalization.
- Added readable capture API errors instead of raw tracebacks.
- Added 35-second capture timeout.


## v5 fixes
- Fixed pandas error: `symbol is both an index level and a column label`.
- Normalizes NseKit dataframes with `reset_index(drop=True)` before ranking.
- Removes duplicate dataframe columns before processing.
- Fixed remaining frontend `classList` null accesses with a safe `isVisible()` helper.
- Stock symbols in Sectorial, Pre-Market and Futures OI tables are clickable and open NSE charts on TradingView.

## v6
- Fixed v5 JavaScript startup syntax issue causing permanent Loading state and dead tabs.
- Fixed remaining null classList access.
- Clears Cache Storage and unregisters old service workers on page load.
- Strengthened no-cache response headers.
- Removed favicon 404 noise.

## v7
- Restored the missing init() function.
- Rebound all tab/dropdown/button handlers safely.
- Startup now continues even if an optional API fails.
- Keeps automatic cache/service-worker cleanup.


## v8
- Pre-market `OI Chg %` now uses NSE OI Spurts **By Underlying** as the primary source.
- Supports NSE fields `latestOI`, `prevOI`, `changeInOI`.
- If NSE does not send percentage directly, v8 calculates:
  `OI Change % = Change in OI / Previous OI * 100`.
- OI cache reduced to 15 seconds.
- First 5m and First 15m filters are now independent and can be applied together.
- Added "Candle in formation" as an explicit filter option for each timeframe.


## v10
- Rebuilt from intact v8 backend to restore all OI and pre-market routes.
- Kept NSE By Underlying OI parsing with unmapped/new F&O symbols allowed.
- Removed Sectorial Levels column.
- Added First 5m and First 15m columns and independent filters to Sectorial scanner.
- Verified `/api/oi`, `/api/premarket/dates`, capture/import and breadth routes are present.


## v11 speed-only optimization

- Existing working tabs/features retained.
- Sector dropdown no longer downloads Yahoo data sector-by-sector.
- A single background Yahoo batch refreshes the full sector universe.
- Sector switching is served from RAM.
- Shared sector snapshot target refresh: ~12 seconds.
- UI refresh reduced to 3 seconds.
- NSE OI cache reduced to 10 seconds.
- Stale sector snapshots refresh in the background instead of blocking the dropdown.
- Yahoo/yfinance remains based on 1-minute bars; this reduces application lag but cannot provide true millisecond exchange ticks.


## v12 — OI Spurt Selection

New additive tab only. Existing v11 tabs are unchanged.

- Automatic first OI-spurt snapshot from 09:22 IST.
- Live NSE OI-spurt ranking by % Change in OI.
- Editable Top N: 5-100, default 30.
- Main columns:
  - Stock
  - Sector
  - % Change in OI
  - Gap %
  - Current % from PDC / final Close % after market close
- Live OI refresh target: 5 seconds.
- Yahoo price batch cache: 12 seconds (Yahoo source itself remains 1-minute data).
- EOD full OI-spurt universe is saved automatically after 15:32 IST.
- History retention is editable from 1-60 sessions.
- All rows are saved at EOD; Min/Max Current/Close % filters can be applied later.
- Common-stock tables:
  - Common in all three: Sectorial + Pre-Market + OI Spurt
  - Sectorial + Pre-Market
  - Sectorial + OI Spurt
  - Pre-Market + OI Spurt
- Common tables show sector, OI %, Gap %, and Current/Close %.


## v13 — automatic common-history capture + previous-data selection

### OI Spurt automatic saving
- Auto capture time is editable from the OI Spurt tab; default 09:22 IST.
- At/after the configured capture time, the app automatically saves:
  - OI Spurt opening snapshot
  - Pre-Market + OI common-stock snapshot
- OI Top N and Pre-Market Top N are both editable.
- At/after 15:32 IST, the app automatically saves:
  - Full EOD OI Spurt universe
  - EOD-enriched Pre-Market + OI common-stock table
- Retention remains editable from 1-60 trading sessions.
- Historical OI Spurt dates can display their saved Pre-Market + OI common table.

### Stock Selection Based on Previous Data
New tab:
- Select saved Start Date and End Date; they may be the same.
- Select OI Top N (e.g. 30/40).
- Select Pre-Market Top N / Combined Rank limit.
- Select minimum number of sessions in which a stock must appear in the daily
  Pre-Market ∩ OI-Spurt common list.
- Optional Max |Net Movement| % filter.
- Same-date Period Net % = that day's saved Close/Current % vs PDC.
- Multi-date Period Net % = first selected session PDC to last selected session
  saved close when those prices are available.
- Output includes:
  Stock, Sector, Common Days, latest OI %, latest Gap %, latest Close %,
  period Net %, latest Pre-Market rank and OI rank.


## v14 — Top Gainers & Losers

New independent tab only.

Universes:
- Nifty 50
- F&O Stocks
- Nifty 100
- Nifty 500
- Sector / Industry

Controls:
- Universe dropdown
- Sector / Industry dropdown
- Stock dropdown
- Top N gainers/losers, default 10
- Editable live window, default 09:15-15:15 IST
- Editable auto-refresh seconds
- Editable 14-session history retention
- Max Gainer %
- Max Loser absolute %
- First 5m candle filter
- First 15m candle filter
- Manual Fetch button
- Manual EOD Save button
- Saved history date selector

At EOD after 15:32 IST the full all-universe mover snapshot is automatically saved.
History is stored independently from every other scanner module.

Performance:
- Full universe ranking uses lightweight daily Yahoo bars.
- Only shortlisted top gainers/losers fetch 1-minute data for exact current move
  and 5m/15m candle classification.


## v15 — Timed Snapshot + EOD History

Top Gainers & Losers now saves two independent daily historical states:

1. Timed Snapshot
   - Snapshot Time is editable from the UI.
   - Default 10:00 IST.
   - Automatically saved once the configured time is reached.
   - Manual "Save Snapshot Now" button is also available.

2. EOD Snapshot
   - Automatically saved after 15:32 IST.
   - Manual EOD save remains available.

History retention:
- Both Timed Snapshot and EOD data use the same editable History Sessions value.
- Default 14 trading sessions.
- Historical View includes a "Historical basis" dropdown:
  - Timed Snapshot
  - EOD

No other scanner tab or selection logic is changed.


## v16 — Yahoo invalid-symbol speed fix

No scanner logic changed.

The following non-stock/index symbols are excluded from Yahoo equity batches:
- NIFTY
- BANKNIFTY
- FINNIFTY
- MIDCPNIFTY
- NIFTYNXT50
- NIFTYFPI
- NIFTYIT
- NIFTYBANK

Additionally, if a symbol returns no usable Yahoo data during the current server
session, it is added to an in-memory invalid-symbol cache and skipped in later
large batch requests.

This avoids repeatedly retrying delisted/unsupported/index tickers and reduces
batch latency for Sectorial, Breadth, OI-spurt price enrichment and Top Movers.

Diagnostic endpoint:
`/api/yahoo-symbol-status`


## v17 — Central Yahoo Filter

This version fixes the remaining repeated unsupported-index downloads by
filtering at the `yf.download()` boundary itself.

Permanent blocked symbols confirmed from the CMD log:
- BANKNIFTY
- FINNIFTY
- MIDCPNIFTY
- NIFTY
- NIFTYFPI
- NIFTYNXT50

Additional protections:
- all NIFTY* index-style identifiers are rejected from stock-only Yahoo batches;
- every existing/future `yf.download()` call passes through the same central gate;
- pre-market and OI-spurt price enrichment sanitize their symbols before cache keys;
- runtime no-data stocks remain skipped for the rest of the running server session;
- permanent exclusions are stored in `invalid_yahoo_symbols.json`.

Diagnostic URL:
`http://127.0.0.1:8000/api/yahoo-symbol-status`

Expected console behavior:
You may see one `[YahooFilter] Permanently skipped...` line per newly encountered
blocked ticker, but yfinance should no longer send those symbols to Yahoo and
therefore should no longer print the repeated 404 / Failed downloads messages.


## v18 — Instant startup / persistent shared cache

Performance-only architecture update. Scanner rules are unchanged.

### Startup
- Browser first paint loads only:
  - sector list
  - currently selected Sectorial data
  - sector summary
- Pre-Market, Futures OI, OI Spurt, Previous Data and Top Movers are lazy-loaded
  only when their tab is opened.
- OI for Sectorial is loaded asynchronously after the first paint and does not
  block the interface.

### Persistent cache
- `data/sector_live_snapshot.json`
  stores the latest valid Sectorial rows and survives app restart.
- `data/sector_previous_levels.json`
  stores PDC/PDH/PDL/PDO for the current trading session.
- After restart, the app serves the last cached rows immediately while Yahoo
  refreshes in the background.

### Lighter Yahoo refresh
Old shared sector refresh:
- 5 days × 1-minute bars for the full sector universe.

v18:
- previous-day OHLC = one lightweight daily-bar download, normally once/day;
- recurring live refresh = today-only 1-minute bars;
- temporary Yahoo misses retain the last good cached stock row.

### Refresh timing
- UI/visible Sectorial refresh: 1 second.
- Sector summary: every 5 seconds.
- Sector OI enrichment: every 5 seconds.
- Shared Yahoo sector snapshot target: 10 seconds in background.
- OI Spurt visible-tab refresh: 3 seconds.

Important:
The 1-second UI refresh does NOT mean Yahoo provides 1-second market ticks.
Yahoo remains a 1-minute source. The purpose of the 1-second loop is to make
tab/filter/cache responses immediate while background data sources refresh at
their appropriate rate.

Diagnostic:
`http://127.0.0.1:8000/api/cache-status`


## v19 — Instant local dropdown switching

No selection rules changed.

### Sectorial
- Entire Sectorial snapshot is downloaded to browser memory once.
- Changing sector makes ZERO network requests.
- Stock list is rendered locally immediately.
- 1-second loop refreshes the full browser sector cache in background.

### Top Gainers & Losers
- Full mover universe is stored in browser memory.
- Changing Universe, Sector, Stock, Top N, Max Gainer or Max Loser is local-only.
- Gainer/loser ranking is recalculated immediately in JavaScript.
- 5m/15m details are refreshed asynchronously for only the shortlisted symbols.
- The stock list therefore appears first; candle details can update afterward.
- Live mover base cache survives restart in `data/top_movers_live_cache.json`.

### Important
The UI interaction can be effectively immediate because it uses local memory.
Underlying Yahoo prices are still 1-minute data. A 1-second UI refresh does not
create 1-second exchange ticks.


## v20 — Futures-OI style cache-first architecture

Why Futures OI is fast:
- one small NSE JSON request;
- 5-second RAM cache;
- no Yahoo request before returning rows;
- sorting/filtering uses cached memory.

v20 applies that same response pattern to the slower modules.

### NSE Pre-Market
- page request never waits for Yahoo live enrichment;
- returns latest cached enriched snapshot immediately;
- stale/missing live enrichment refreshes in background;
- enriched cache persists in `data/premarket_live_cache.json`.

### OI Spurt
- OI ranking returns immediately from NSE/cache;
- Gap % / Current % are attached from available price cache;
- stale/missing Yahoo price enrichment runs in background;
- table does not wait for Yahoo.

### Top Movers
- ranking/universe/sector already local in v19;
- 5m/15m detail endpoint now returns cached candle details immediately;
- stale details refresh in background;
- next 1-second UI cycle displays them.

### Sector Summary
- 3-second RAM cache avoids recalculating all sectors on every request.

### UI
- stays at 1-second refresh;
- slow source calls are decoupled from table response;
- a Yahoo/NSE delay should no longer freeze dropdowns or table rendering.

Important:
This makes application response cache-first like Futures OI. Yahoo itself remains
a 1-minute market-data source; this does not create true 1-second exchange ticks.


## v21 — Previous Days Impact Analysis + 09:22 Movers

### Previous Days Impact Analysis
Automatically built after EOD (~15:35 IST) from:
- Top N Pre-Market stocks
- Top N OI Spurt stocks

Configurable:
- Pre-Market Top N
- OI Spurt Top N
- Lookback 1-5 saved sessions
- History retention
- Min / Max absolute analysis-day move %

For each analysis-day stock:
- analysis-day Pre-Market presence
- analysis-day OI presence
- OI change %
- Gap %
- analysis-day move %
- for each previous saved session:
  - present in Pre-Market
  - present in OI Spurt
  - that day's move %
- next saved session date
- next-day move %
- automatic observation

Yesterday's analysis is rebuilt automatically when the next session becomes available,
so Next Day Move % is filled in later.

Downloads:
- CSV
- Excel (.xlsx)

### Top Movers 09:22 Snapshot
Independent from selected sector/universe.
- Automatically captured at/after 09:22 IST once per day
- full stock mover cache
- default Top 5 gainers + Top 5 losers, editable
- 5m filter
- 15m filter
- manual Capture Now
- saved in Top Movers history database


## v22 — Top-5 history + Excel exports + 5-minute charts

### Full-universe Top 5 history
- 09:22 snapshot remains automatic and independent of sector selection.
- Dedicated history table always shows the first Top 5 gainers and Top 5 losers from that 09:22 snapshot.
- EOD Top 5 gainers and Top 5 losers are saved automatically after ~15:35 IST.
- EOD Top 5 is also independent of selected sector/universe.
- Historical date and basis selector: 09:22 / EOD.

### Excel exports
Excel export added to:
- Sectorial Stock Selection
- Market Breadth
- NSE Pre-Market
- Futures OI
- OI Spurt Selection
- Previous Data Selection
- Top Gainers & Losers
- Previous Days Impact Analysis already includes Excel export

### Live 5-Minute Market Charts
New tab:
- Nifty
- all sectors together
- individual sector dropdown
- current-day 5-minute % movement
- chart UI refresh every 5 seconds
- backend chart cache refresh target ~20 seconds
- Nifty uses Yahoo ^NSEI when available
- sectors are equal-weight constituent-stock lines, avoiding unsupported Yahoo sector-index symbols


## v23 — User Futures Sectors + Price/Level Charts + Breadth History

- Imported futures-sector assignments from the supplied `Market Analyzer` workbook.
- Supplied symbols override their old sector assignment; old symbols not present in the workbook remain available.
- Added Nifty Telecom and Nifty Consumer sectors.
- F&O breadth automatically uses the expanded futures universe.
- Breadth is now cache-first (no blocking Yahoo call): All F&O / Nifty 50 selector, green Advances line, red Declines line, current counts at top, mouse hover values, and saved daily history.
- Live market chart is now PRICE, not percent.
- Nifty/sector chart is individually selectable.
- Key levels: PDH, PDL, PDC, 1D–4D Ago H/L, PWH, PWL, PWC, PWO, Current Week Open.
- Actual Yahoo index ticker is used where available; unsupported sector index falls back to a clearly-labelled equal-weight basket price.
- Market charts are persisted to `data/market_chart_history.json` and historical dates are selectable.
- Breadth history persists in `data/combined_breadth_history.json` for 30 sessions.


## v24 — Today/live 09:22 correction
- Upper 09:22 panel is locked to today by default and never silently falls back to the previous trading day.
- Before today snapshot exists, upper table shows today live movers.
- Editable automatic capture time (default 09:22 IST).
- Date selector + Fetch Selected Date for explicit historical viewing.
- Live IST clock.
- Saved snapshot and live/not-saved state are clearly labelled.
- Existing Full-Universe Top 5 History remains unchanged.


## v25 — Live Top Movers separated from Snapshot History

- The upper Top Gainers / Top Losers tables are always TODAY'S LIVE data.
- Saving the configured 09:22 snapshot never freezes or replaces the upper live tables.
- The lower history section is the only place that displays saved 09:22/EOD snapshots.
- Capture time remains editable.
- Live IST clock remains independent.
- Added Refresh Live Now.

## Cloud deployment files

This package now includes Dockerfile and docker-compose.yml. The `./data` directory is mounted as a persistent volume, so SQLite/JSON history survives container restarts and redeploys on a persistent VM.
\n\n## v26 — Live Top Movers route fix\n\n- Fixed FastAPI route shadowing where `/api/top-movers/0922/live` was being\n  interpreted as `/api/top-movers/0922/{trade_date}` with `trade_date=live`.\n- New unambiguous endpoint: `/api/top-movers/live-today`.\n- Upper Top Gainers/Losers section is today's live full-universe ranking only.\n- Lower Full-Universe Top 5 History remains saved 09:22/EOD history only.\n- Live endpoint is cache-first: it immediately returns the existing Top Movers\n  RAM/disk cache and triggers heavy Yahoo refresh in the background.\n- 5m/15m candle details are also cache-first and fill on later UI refreshes.\n