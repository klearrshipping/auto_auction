"""
Japan Auction Valuation — FastAPI Web Server
Run with: uvicorn api.app:app --reload --port 8000
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from api.services.valuation_service import ValuationService

# --- Paths ---
SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(SERVER_DIR, 'static')

app = FastAPI(title="Japan Auction Valuation API", version="1.0.0")
service = ValuationService()

# Serve static frontend
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

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
    return FileResponse(os.path.join(STATIC_DIR, 'index.html'))

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
    
    return service._calculate_valuation(res.data[0])

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
