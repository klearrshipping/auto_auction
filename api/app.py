"""
Japan Auction Valuation — FastAPI Web Server
Run with: uvicorn api.app:app --reload --port 8000
"""

import hashlib
import json
import os
from urllib.parse import urlparse

import requests
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel
from api.services.valuation_service import ValuationService
from api.services.auction_service import AuctionService

app = FastAPI(title="Japan Auction Valuation API", version="1.0.0")

# Base URL for proxy links (env or default). Used when transforming image_links.
API_BASE_URL = os.getenv("API_BASE_URL", "http://klearr.me:8000").rstrip("/")

# Referers for p3.aleado.com (hotlink protection). Try in order if first fails.
ALEADO_REFERERS = [
    "https://auction.zenautoworks.ca/",
    "https://auc.japancarauc.com/",
    "https://www.aleado.com/",
]
service = ValuationService()
auction_service = AuctionService()

# --- Request/Response Models ---
class ValuationRequest(BaseModel):
    year:       int
    make:       str
    model:      str
    model_type: str
    score:      str
    mileage:    int

# --- Routes ---
@app.get("/")
def index():
    return {"message": "Japan Auction Valuation API", "docs": "/docs"}

@app.post("/api/valuate")
async def valuate(req: ValuationRequest):
    """Legacy/Single-shot endpoint"""
    score_band = service._get_score_band(req.score)
    mileage_band = service._get_mileage_band(req.mileage)

    res = service.supabase.table('japan_sales_buckets').select('*').eq(
        'year', req.year
    ).eq('make', req.make.upper()).eq('model', req.model.upper()).eq(
        'model_type', req.model_type.upper()
    ).eq('score_band', score_band).eq('mileage_band', mileage_band).limit(1).execute()

    if not res.data:
        raise HTTPException(status_code=404, detail="No data found")

    return await service._calculate_valuation(res.data[0])

@app.post("/api/stream-valuate")
async def stream_valuate(req: ValuationRequest):
    """Real-time streaming endpoint for a specific vehicle"""
    return StreamingResponse(
        service.get_valuation_stream(req),
        media_type="text/event-stream"
    )

@app.get("/api/buckets")
async def get_buckets(page: int = 1, limit: int = 50):
    """Paginated endpoint for all buckets"""
    return await service.get_buckets_page(page, limit)

@app.get("/api/stream-all-buckets")
async def stream_all_buckets():
    """Global real-time streaming endpoint for all buckets"""
    return StreamingResponse(
        service.get_global_stream(),
        media_type="text/event-stream"
    )

@app.get("/api/stream-auction-vehicles")
async def stream_auction_vehicles():
    """
    Stream ALL vehicles from auction_data.vehicles (no valuation filter).
    For listings with fair values, use /api/auction-listings or /api/stream-auction-listings instead.
    """
    return StreamingResponse(
        auction_service.stream_vehicles(),
        media_type="text/event-stream"
    )

@app.get("/api/get-valuation")
@app.get("/api/stream-auction-vehicles-with-valuation")
async def get_valuation():
    """Stream auction vehicles with sales bucket pairing and valuations."""
    return StreamingResponse(
        auction_service.stream_vehicles_with_valuation(),
        media_type="text/event-stream"
    )


@app.get("/api/proxy-image")
async def proxy_image(url: str = Query(..., description="Image URL to proxy (e.g. p3.aleado.com)")):
    """
    Proxy images from p3.aleado.com so users can view them on your site.
    Aleado blocks direct embeds (hotlink protection); this endpoint fetches with
    the correct Referer and streams the image to the client.
    """
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Invalid URL")
    if parsed.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="URL must be http or https")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "image/*,*/*;q=0.8",
    }
    r = None
    for referer in ALEADO_REFERERS:
        headers["Referer"] = referer
        try:
            r = requests.get(url, headers=headers, timeout=15, stream=True)
            if r.status_code == 200:
                break
        except requests.RequestException as e:
            raise HTTPException(status_code=502, detail=f"Upstream error: {e}")

    if not r or r.status_code != 200:
        raise HTTPException(
            status_code=r.status_code if r else 502,
            detail=f"Upstream returned {r.status_code}" if r else "Upstream unreachable",
        )

    content_type = r.headers.get("Content-Type", "image/jpeg")
    return StreamingResponse(
        r.iter_content(chunk_size=8192),
        media_type=content_type,
    )


# Real-time sync: no stale cache — website count must match DB count
# max-age=0, must-revalidate = always revalidate; no stale-while-revalidate
LISTINGS_CACHE_CONTROL = "max-age=0, must-revalidate, no-cache"


@app.get("/api/auction-listings/count")
async def get_auction_listings_count():
    """
    Live count from auction_data.valuations. No caching — use to verify website matches DB.
    Poll this to detect when a full refresh is needed.
    """
    total = await auction_service.get_listings_count()
    return Response(
        content=json.dumps({"total": total, "source": "auction_data.valuations"}),
        media_type="application/json",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/auction-listings")
async def get_auction_listings(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    if_none_match: str | None = Header(None, alias="If-None-Match"),
):
    """
    Paginated auction listings with ETag caching. Best for initial load and refresh:
    browser serves cached data immediately while re-fetching in background.
    Returns same payload shape as stream-auction-listings (image_links, vehicle_name, etc.).
    """
    listings, total = await auction_service.get_auction_listings_page(
        page=page, limit=limit, proxy_base=API_BASE_URL
    )
    payload = {"items": listings, "total": total, "page": page, "limit": limit}
    body = json.dumps(payload, sort_keys=True)
    etag = f'W/"{hashlib.md5(body.encode()).hexdigest()}"'

    if if_none_match and if_none_match.strip() == etag:
        return Response(
            status_code=304,
            headers={
                "ETag": etag,
                "Cache-Control": LISTINGS_CACHE_CONTROL,
                "X-Data-Source": "auction_data.valuations",
            },
        )

    return Response(
        content=body,
        media_type="application/json",
        headers={
            "ETag": etag,
            "Cache-Control": LISTINGS_CACHE_CONTROL,
            "X-Data-Source": "auction_data.valuations",
        },
    )


@app.get("/api/stream-auction-listings")
async def stream_auction_listings():
    """Stream auction listings: first 50 immediately, then chunks of 100. SSE.
    image_links, year, make, model, grade, color, mileage, score, auction_house,
    min_price, max_price, trimmed_mean. Uses pre-computed valuations.
    image_links are proxied so users can view aleado images on your site."""
    return StreamingResponse(
        auction_service.stream_auction_listings(proxy_base=API_BASE_URL),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Data-Source": "auction_data.valuations",
        },
    )


@app.get("/api/auction-updates")
async def stream_auction_updates(
    poll_interval: float = Query(5.0, ge=1.0, le=60.0, description="Seconds between polls"),
    since: str | None = Query(None, description="ISO8601 timestamp; only changes after this time"),
):
    """
    Live bid/status updates via SSE. Streams only deltas (price changed, lot sold, etc.)
    instead of full catalogue reloads. Polls DB for vehicles with updated_at > last_seen.
    Requires migration 015_vehicles_updated_at.sql. Use ?since=ISO8601 after reconnect.
    """
    return StreamingResponse(
        auction_service.stream_auction_updates(
            poll_interval=poll_interval,
            proxy_base=API_BASE_URL,
            since=since,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )
