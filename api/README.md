# Japan Auction Valuation API

FastAPI server for auction vehicle data and valuations. Connects to Supabase (`auction_data` and `sales_data` schemas).

---

## Prerequisites

- Python 3.10+
- `.env` with `SUPABASE_URL` and `SUPABASE_KEY` (in project root or `tools/aggregate_auction/.env`)

---

## Starting the API

From the project root:

```powershell
# Windows
.\.venv\Scripts\python.exe -m uvicorn api.app:app --reload --port 8000

# Or with uvicorn on PATH
uvicorn api.app:app --reload --port 8000
```

**Base URL:** `http://localhost:8000`

**Interactive docs:** `http://localhost:8000/docs`

---

## Auction Data Endpoints

### 1. Stream Auction Listings (recommended)

**`GET /api/stream-auction-listings`**

Streams auction vehicles with pre-computed valuations. Best for listing UIs.

**Response:** Server-Sent Events (SSE), `text/event-stream`

**Payload per vehicle:**

| Field | Type | Description |
|-------|------|-------------|
| `image_links` | `string[]` | Image URLs |
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

Vehicles without a valuation have `null` for `min_price`, `max_price`, `trimmed_mean`.

---

### 2. Stream All Auction Vehicles (raw)

**`GET /api/stream-auction-vehicles`**

Streams full vehicle records from `auction_data.vehicles` (no valuation join).

**Response:** SSE, `text/event-stream`

---

### 3. Stream Vehicles with On-Demand Valuation

**`GET /api/get-valuation`** or **`GET /api/stream-auction-vehicles-with-valuation`**

Streams vehicles with bucket pairing and valuation computed at request time. Includes full vehicle, bucket, and valuation objects.

**Response:** SSE, `text/event-stream`

---

## Connecting to the API

### cURL

```bash
# Stream auction listings
curl -N "http://localhost:8000/api/stream-auction-listings"

# Stream raw vehicles
curl -N "http://localhost:8000/api/stream-auction-vehicles"
```

`-N` disables buffering so events appear as they arrive.

---

### JavaScript (EventSource)

```javascript
const eventSource = new EventSource('http://localhost:8000/api/stream-auction-listings');

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  if (data.meta) {
    console.log('Total vehicles:', data.meta.total);
  } else if (data.status === 'complete') {
    console.log('Done. Total:', data.total);
    eventSource.close();
  } else {
    // Vehicle listing
    console.log(data.vehicle_name, data.min_price, data.max_price);
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
  const response = await fetch('http://localhost:8000/api/stream-auction-listings');
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
Invoke-WebRequest -Uri "http://localhost:8000/api/stream-auction-listings" -OutFile "auction_listings.jsonl"

# Or process line by line (requires custom handling for SSE)
$response = Invoke-WebRequest -Uri "http://localhost:8000/api/stream-auction-listings" -UseBasicParsing
$response.Content
```

---

### Python

```python
import requests
import json

url = "http://localhost:8000/api/stream-auction-listings"
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

data: {"image_links":["https://..."],"vehicle_name":"2024 AUDI A3","year":2024,"make":"AUDI","model":"A3","grade":"SPORTBACK 30TFSI","color":"BLACK","mileage":7000,"score":"4","auction_house":"TAA Kyushu","min_price":850000,"max_price":1200000,"trimmed_mean":980000}

: keep-alive

data: {"status":"complete","total":3709}
```

- **First event:** `meta` with total count
- **Middle events:** One vehicle per event
- **`: keep-alive`:** Heartbeat (ignore)
- **Last event:** `status: "complete"` with total emitted

---

## CORS

The API does not restrict CORS by default. For production, configure CORS in `api/app.py` if needed.

---

## Data Pipeline

Auction data is populated by the auction pipeline:

1. **Extract** (listings → details → compile)
2. **Sync** to `auction_data.vehicles`
3. **Get valuation** populates `auction_data.valuations`

Run the pipeline before expecting fresh data:

```powershell
python -m operations.auction.auction_manager
```
