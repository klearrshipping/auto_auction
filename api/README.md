# Japan Auction Valuation API — Integration Guide

**For external sites:** Use this API to stream live auction vehicle data (images, specs, valuations) into your site. Supports **paginated REST with ETag caching** for fast initial load and refresh, and **Server-Sent Events (SSE)** for chunked streaming.

---

## Base URL

**Base URL:** `http://klearr.me:8000`

| Resource | URL |
|----------|-----|
| **Base** | `http://klearr.me:8000` |
| **Interactive docs** | `http://klearr.me:8000/docs` |
| **Paginated listings** | `http://klearr.me:8000/api/auction-listings` |
| **Live count (no cache)** | `http://klearr.me:8000/api/auction-listings/count` |
| **Stream auction listings** | `http://klearr.me:8000/api/stream-auction-listings` |
| **Live bid/status updates** | `http://klearr.me:8000/api/auction-updates` |

---

## Real-Time Sync (Website = Database)

**All listing endpoints use `auction_data.valuations`** — website count must match DB count.

- **Do not use** `/api/stream-auction-vehicles` for listings — it returns ALL vehicles (no valuation filter) and will show a different count.
- **Use** `/api/auction-listings` or `/api/stream-auction-listings` — both stream from valuations.
- **Cache:** `max-age=0, must-revalidate` — no stale data; every request revalidates with the server.
- **Verify count:** Poll `GET /api/auction-listings/count` (no caching) to confirm website matches DB.

---

## Recommended Architecture (Jamaican Auction Platform)

For 1000+ vehicles with near-instant perceived load and minimal bandwidth on refresh:

- **Paginated REST** — Initial load. Real-time sync: no stale cache.
- **SSE stream** — Chunked delivery when you want all vehicles: first 50 records immediately, then the rest in chunks of 100. No blank screen for 2–3 seconds.
- **SSE for live updates** — `GET /api/auction-updates` streams only deltas (price changed, lot sold). No full catalogue reload.
- **TanStack Virtual** — Client-side virtualized scroll for large lists.

---

## Quick Start for External Sites

**Paginated (best for initial load and refresh):**

```javascript
// Fetch first page (real-time sync: no stale cache)
const res = await fetch('http://klearr.me:8000/api/auction-listings?page=1&limit=50');
const { items, total } = await res.json();
// Verify count matches DB: fetch('/api/auction-listings/count')
```

**Stream (best when you need all vehicles as they arrive):**

```javascript
const eventSource = new EventSource('http://klearr.me:8000/api/stream-auction-listings');
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.vehicle_name) {
    console.log(data.vehicle_name, data.min_price, data.max_price);
  }
};
```

---

## Auction Data Endpoints

### 1. Paginated Auction Listings (recommended for initial load)

**`GET /api/auction-listings?page=1&limit=50`**

Returns a JSON page of auction vehicles with pre-computed valuations. Uses **ETag + Cache-Control: stale-while-revalidate** so on refresh the browser serves cached data immediately while re-fetching in the background.

**Query params:** `page` (default 1), `limit` (default 50, max 100)

**Response:** `application/json`

```json
{
  "items": [...],
  "total": 3709,
  "page": 1,
  "limit": 50
}
```

**Caching:** `Cache-Control: max-age=0, must-revalidate` — real-time sync; no stale data. ETag still used for 304 when unchanged.

---

### 2. Stream Auction Listings (chunked delivery)

**`GET /api/stream-auction-listings`**

Streams auction vehicles via SSE. **Chunked delivery:** first 50 records sent immediately, then the rest in batches of 100. Users see cars appearing almost instantly instead of a blank screen for 2–3 seconds.

**Response:** Server-Sent Events (SSE), `text/event-stream`

**Payload per vehicle:**

| Field | Type | Description |
|-------|------|-------------|
| `image_links` | `string[]` | Image URLs (proxied via `/api/proxy-image` so users can view aleado images on your site) |
| `vehicle_name` | `string` | "year make model" (e.g. "2024 AUDI A3") |
| `year` | `int \| null` | Model year |
| `make` | `string` | Manufacturer |
| `model` | `string` | Model name |
| `model_type` | `string \| null` | Model type (e.g. GYDLA) |
| `grade` | `string \| null` | Grade/trim |
| `color` | `string \| null` | Color |
| `mileage` | `int \| null` | Mileage (km) |
| `score` | `string \| null` | Auction score |
| `auction_house` | `string \| null` | Auction house name |
| `min_price` | `int \| null` | Min valuation (yen) |
| `max_price` | `int \| null` | Max valuation (yen) |
| `trimmed_mean` | `int \| null` | Trimmed mean (yen) |
| `fair_value` | `int \| null` | Fair value (yen) — primary valuation |

**Source:** Streams from `auction_data.valuations` (fair values) joined with `vehicles` for details. Only vehicles with valuations are included.

**Image proxy:** Aleado blocks direct embeds (hotlink protection). `image_links` point to our proxy (`/api/proxy-image?url=...`), which fetches with the correct Referer and streams the image to your users.

---

### 3. Live Count (no cache)

**`GET /api/auction-listings/count`**

Returns `{ total, source }` from `auction_data.valuations`. No caching — use to verify website matches DB. Poll to detect when a full refresh is needed.

---

### 4. Live Auction Updates (deltas only)

**`GET /api/auction-updates?poll_interval=5&since=2026-03-13T12:00:00Z`**

Streams only deltas for live bid/status updates. No full catalogue reload — only vehicles that changed (price, result) since `last_seen`. Polls DB every `poll_interval` seconds.

**Query params:** `poll_interval` (default 5, 1–60 seconds), `since` (optional ISO8601 — use after reconnect to get missed updates)

**Response:** SSE, `text/event-stream`

**Payload per delta:**

| Field | Type | Description |
|-------|------|-------------|
| `type` | `string` | `"lot_sold"` or `"lot_updated"` |
| `vehicle_id` | `int` | Vehicle ID |
| `vehicle_name` | `string` | "year make model" |
| `start_price` | `int \| null` | Start price (yen) |
| `end_price` | `int \| null` | End/final price (yen) |
| `result` | `string \| null` | e.g. "available", "sold" |
| `fair_value` | `int \| null` | Fair value (yen) |
| `updated_at` | `string` | ISO8601 timestamp |
| `image_links` | `string[]` | First 3 images (proxied) |

**Source:** Polls `auction_data.valuations` for changes (updated_at).

---

### 5. Stream All Auction Vehicles (raw)

**`GET /api/stream-auction-vehicles`**

Streams full vehicle records from `auction_data.vehicles` (no valuation join).

**Response:** SSE, `text/event-stream`

---

### 6. Stream Vehicles with On-Demand Valuation

**`GET /api/get-valuation`** or **`GET /api/stream-auction-vehicles-with-valuation`**

Streams vehicles with bucket pairing and valuation computed at request time. Includes full vehicle, bucket, and valuation objects.

**Response:** SSE, `text/event-stream`

---

## Connecting to the API

### cURL

```bash
# Paginated (with ETag caching)
curl -i "http://klearr.me:8000/api/auction-listings?page=1&limit=50"
# On refresh, send If-None-Match from previous response:
curl -i -H 'If-None-Match: W/"abc123"' "http://klearr.me:8000/api/auction-listings?page=1&limit=50"

# Stream auction listings (chunked: 50 first, then 100)
curl -N "http://klearr.me:8000/api/stream-auction-listings"

# Stream raw vehicles
curl -N "http://klearr.me:8000/api/stream-auction-vehicles"

# Live auction updates (deltas only)
curl -N "http://klearr.me:8000/api/auction-updates?poll_interval=5"
# After reconnect, pass missed updates:
curl -N "http://klearr.me:8000/api/auction-updates?since=2026-03-13T12:00:00Z"
```

`-N` disables buffering so events appear as they arrive.

---

### JavaScript (Paginated with cache)

```javascript
async function fetchListings(page = 1, limit = 50, etag = null) {
  const headers = etag ? { 'If-None-Match': etag } : {};
  const res = await fetch(`http://klearr.me:8000/api/auction-listings?page=${page}&limit=${limit}`, { headers });
  if (res.status === 304) {
    return { cached: true, etag: res.headers.get('ETag') };
  }
  const data = await res.json();
  return { ...data, etag: res.headers.get('ETag') };
}
// Verify count matches DB (no cache):
// const { total } = await fetch('/api/auction-listings/count').then(r => r.json());
```

### JavaScript (Live auction updates)

```javascript
let lastSeen = null;
const es = new EventSource('http://klearr.me:8000/api/auction-updates?poll_interval=5');
es.onmessage = (e) => {
  const d = JSON.parse(e.data);
  if (d.meta) return;
  if (d.error) { console.warn(d.error); return; }
  lastSeen = d.updated_at;
  if (d.type === 'lot_sold') {
    console.log('Sold:', d.vehicle_name, d.end_price);
  } else {
    console.log('Updated:', d.vehicle_name, d.end_price);
  }
};
// On reconnect, use lastSeen: EventSource('...?since=' + encodeURIComponent(lastSeen))
```

### JavaScript (EventSource)

```javascript
const eventSource = new EventSource('http://klearr.me:8000/api/stream-auction-listings');

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.meta) {
    console.log('Total vehicles:', data.meta.total);
  } else if (data.status === 'complete') {
    console.log('Done. Total:', data.total);
    eventSource.close();
  } else {
    // Vehicle listing - image_links are proxied; use directly in <img src="...">
    console.log(data.vehicle_name, data.min_price, data.max_price);
    if (data.image_links?.length) {
      // e.g. <img src="${data.image_links[0]}" />
    }
  }
};

eventSource.onerror = (err) => {
  console.error('SSE error:', err);
  eventSource.close();
};
```

---

### JavaScript (fetch + ReadableStream)

```javascript
async function streamAuctionListings() {
  const response = await fetch('http://klearr.me:8000/api/stream-auction-listings');
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const json = line.slice(6);
        if (json === '[DONE]' || json === '') continue;
        try {
          const data = JSON.parse(json);
          console.log(data);
        } catch (e) {}
      }
    }
  }
}
```

---

### PowerShell

```powershell
# Stream to file
Invoke-WebRequest -Uri "http://klearr.me:8000/api/stream-auction-listings" -OutFile "auction_listings.jsonl"

# Or process line by line (requires custom handling for SSE)
$response = Invoke-WebRequest -Uri "http://klearr.me:8000/api/stream-auction-listings" -UseBasicParsing
$response.Content
```

---

### Python

```python
import requests
import json

url = "http://klearr.me:8000/api/stream-auction-listings"
with requests.get(url, stream=True) as r:
    r.raise_for_status()
    for line in r.iter_lines():
        if line and line.startswith(b"data: "):
            data = json.loads(line[6:].decode())
            if "vehicle_name" in data:
                print(data["vehicle_name"], data.get("min_price"), data.get("max_price"))
            elif data.get("status") == "complete":
                print("Done:", data["total"])
```

---

## SSE Event Format

Each event is a line starting with `data: ` followed by JSON:

```
data: {"meta":{"total":3709}}

data: {"image_links":["https://..."],"vehicle_name":"2024 AUDI A3","year":2024,"make":"AUDI","model":"A3","grade":"SPORTBACK 30TFSI","color":"BLACK","mileage":7000,"score":"4","auction_house":"TAA Kyushu","min_price":850000,"max_price":1200000,"trimmed_mean":980000,"fair_value":980000}

: keep-alive

data: {"status":"complete","total":3709}
```

- **First event:** `meta` with total count
- **Middle events:** One vehicle per event
- **`: keep-alive`:** Heartbeat (ignore)
- **Last event:** `status: "complete"` with total emitted

**Auction updates stream** (`/api/auction-updates`):

```
data: {"meta":{"type":"auction_updates","poll_interval":5}}

data: {"type":"lot_updated","vehicle_id":12345,"vehicle_name":"2024 AUDI A3","start_price":500000,"end_price":850000,"result":"available","fair_value":980000,"updated_at":"2026-03-13T14:30:00Z","image_links":["..."]}

: keep-alive

data: {"type":"lot_sold","vehicle_id":12346,"vehicle_name":"2023 HONDA FIT","end_price":920000,"result":"sold","fair_value":920000,"updated_at":"2026-03-13T14:31:00Z","image_links":["..."]}
```

---

## CORS

The API does not restrict CORS by default. External sites can call it from the browser. For production, configure CORS in `api/app.py` if you need to restrict origins.

---

## For API Operators (Self-Hosting)

**Prerequisites:** Python 3.10+, `.env` with `SUPABASE_URL` and `SUPABASE_KEY`

**Environment (optional):**
- `API_BASE_URL` — Base URL for proxy image links (default: `http://klearr.me:8000`). Set to your public URL so `image_links` point to your proxy.

**Start the API:**

```powershell
uvicorn api.app:app --host 0.0.0.0 --port 8000
```

**Data pipeline:** Auction listing data is populated by `operations.auction.run_japan_auction_pipeline` (`run_japan_auction_pipeline.py`). Run it before expecting fresh data.

**Live updates:** `GET /api/auction-updates` polls `auction_data.valuations` for changes (updated_at).
