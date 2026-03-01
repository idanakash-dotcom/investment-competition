// Investment Competition PWA - Service Worker
const CACHE_NAME = 'invest-competition-v1';
const ASSETS = [
  '/',
  '/index.html',
  '/manifest.json'
];

self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(ASSETS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', event => {
  // Network-first for API calls, cache-first for assets
  const url = new URL(event.request.url);
  
  if (url.pathname.startsWith('/api/') || url.hostname.includes('supabase')) {
    // Network first for data
    event.respondWith(
      fetch(event.request).catch(() => caches.match(event.request))
    );
  } else {
    // Cache first for assets
    event.respondWith(
      caches.match(event.request).then(cached => {
        return cached || fetch(event.request).then(response => {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
          return response;
        });
      })
    );
  }
});

// Background sync for price updates
self.addEventListener('periodicsync', event => {
  if (event.tag === 'price-update') {
    event.waitUntil(updatePrices());
  }
});

async function updatePrices() {
  try {
    // This would call your market data endpoint
    const response = await fetch('/api/refresh-prices');
    const data = await response.json();
    
    // Notify clients
    const clients = await self.clients.matchAll();
    clients.forEach(client => client.postMessage({ type: 'PRICES_UPDATED', data }));
  } catch (err) {
    console.error('Background price update failed:', err);
  }
}
