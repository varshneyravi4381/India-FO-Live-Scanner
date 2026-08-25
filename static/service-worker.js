const CACHE_NAME = "india-fo-scanner-pwa-v27-1";
const APP_SHELL = [
  "/",
  "/manifest.webmanifest",
  "/static/icons/icon-192.png",
  "/static/icons/icon-512.png"
];

self.addEventListener("install", event => {
  event.waitUntil(caches.open(CACHE_NAME).then(cache => cache.addAll(APP_SHELL)).catch(() => null));
  self.skipWaiting();
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys().then(keys => Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", event => {
  const req = event.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  // Never cache live/API data.
  if (url.pathname.startsWith("/api/")) {
    event.respondWith(fetch(req));
    return;
  }
  // Network-first for navigation so new scanner versions appear immediately.
  if (req.mode === "navigate") {
    event.respondWith(fetch(req).catch(() => caches.match("/")));
    return;
  }
  // Cache-first only for PWA assets/icons.
  event.respondWith(caches.match(req).then(hit => hit || fetch(req).then(resp => {
    if (resp && resp.ok && url.origin === self.location.origin) {
      const copy = resp.clone();
      caches.open(CACHE_NAME).then(cache => cache.put(req, copy));
    }
    return resp;
  })));
});
