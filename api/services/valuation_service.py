import os
import json
import asyncio
from supabase import create_client, ClientOptions
from dotenv import load_dotenv

# --- Paths ---
SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR    = os.path.abspath(os.path.join(SERVICE_DIR, '..', '..'))
ENV_PATH    = os.path.join(ROOT_DIR, 'tools', 'aggregate_sales', '.env')

load_dotenv(ENV_PATH)

class ValuationService:
    def __init__(self):
        options = ClientOptions(schema='sales_data')
        self.supabase = create_client(
            os.getenv('SUPABASE_URL'), 
            os.getenv('SUPABASE_KEY'), 
            options=options
        )
        self._last_sold_cache = {} # Cache for repetitive lookups during a stream

    def _get_score_band(self, score: str):
        s = str(score).strip().upper()
        if s == 'R': return 'R'
        if s == 'S': return 'S'
        if s == '5': return '5'
        try:
            n = float(s)
            if 3.0 <= n <= 3.5: return '3-3.5'
            if 4.0 <= n <= 4.5: return '4-4.5'
        except ValueError:
            pass
        return None

    def _get_mileage_band(self, mileage: int):
        if mileage <= 30000:    return '0-30k'
        elif mileage <= 60000:  return '30k-60k'
        elif mileage <= 90000:  return '60k-90k'
        elif mileage <= 120000: return '90k-120k'
        elif mileage <= 150000: return '120k-150k'
        elif mileage <= 200000: return '150k-200k'
        else:                   return '200k+'

    async def _get_last_sold_price(self, make, model, year, model_type):
        """Looks up the most recent hammer price for this configuration (with caching)"""
        cache_key = f"{make}|{model}|{year}|{model_type}".upper()
        if cache_key in self._last_sold_cache:
            return self._last_sold_cache[cache_key]

        try:
            # Type and casing safety
            make_val = str(make).strip().upper()
            model_val = str(model).strip().upper()
            type_val = str(model_type).strip().upper()
            year_val = int(year)

            res = self.supabase.table('japan_sales').select('end_price').eq(
                'make', make_val
            ).eq('model', model_val).eq('year', year_val).eq(
                'model_type', type_val
            ).order('sale_date', desc=True).limit(1).execute()
            
            price = None
            if res.data and len(res.data) > 0:
                price = res.data[0].get('end_price')
            
            self._last_sold_cache[cache_key] = price
            return price
        except Exception as e:
            print(f"Error in last_sold_lookup: {e}")
        return None

    async def _calculate_valuation(self, row, req_info=None):
        tier    = row.get('confidence_tier', 'thin')
        iqr     = row.get('iqr', 0)
        trimmed = row.get('trimmed_mean')
        median  = row.get('median_price')
        min_p   = row.get('min_price', 0)
        max_p   = row.get('max_price', 0)

        fv = trimmed if (tier == 'strong' and trimmed) else median
        method = 'trimmed mean' if (tier == 'strong' and trimmed) else 'median'

        if not fv and median:
            fv = median
        elif not fv:
            fv = row.get('mean_price', 0)

        range_low   = fv - (iqr // 2) if iqr else min_p
        range_high  = fv + (iqr // 2) if iqr else max_p
        
        last_sold = await self._get_last_sold_price(
            row['make'], row['model'], row['year'], row['model_type']
        )

        vehicle_name = req_info if req_info else f"{row.get('year')} {row.get('make')} {row.get('model')} {row.get('model_type')}"

        return {
            "vehicle":          vehicle_name,
            "score_band":       row.get('score_band', 'N/A'),
            "mileage_band":     row.get('mileage_band', 'N/A'),
            "fv_price":         fv,
            "fv_method":        method,
            "range_low":        range_low,
            "range_high":       range_high,
            "price_range_str":  f"{min_p/1e6:.1f}M - {max_p/1e6:.1f}M",
            "mean_price":       row.get('mean_price'),
            "median_price":     median,
            "last_sold_price":  last_sold,
            "market_spread_pct": row.get('price_spread_pct'),
            "confidence_tier":  tier.upper(),
            "comparable_count": row.get('comparable_count', 0),
            "min_price":        min_p,
            "max_price":        max_p,
            "last_updated":     row.get('updated_at', 'Just now')
        }

    async def get_buckets_page(self, page: int = 1, limit: int = 50):
        """Fetches a specific page of valuation buckets with calculated data"""
        offset = (page - 1) * limit
        
        # Fetch count and data
        res = self.supabase.table('japan_sales_buckets').select('*', count='exact').order('year', desc=True).range(offset, offset + limit - 1).execute()
        
        if not res.data:
            return {"data": [], "total": res.count or 0}

        # Process lookups concurrently for the page
        tasks = [self._calculate_valuation(row) for row in res.data]
        processed_data = await asyncio.gather(*tasks)

        return {
            "data": processed_data,
            "total": res.count or 0,
            "page": page,
            "limit": limit
        }

    async def get_valuation_stream(self, req):
        score_band = self._get_score_band(req.score)
        mileage_band = self._get_mileage_band(req.mileage)

        res = self.supabase.table('japan_sales_buckets').select('*').eq(
            'year', req.year
        ).eq('make', req.make.upper()).eq('model', req.model.upper()).eq(
            'model_type', req.model_type.upper()
        ).eq('score_band', score_band).eq('mileage_band', mileage_band).limit(1).execute()

        if res.data:
            req_name = f"{req.year} {req.make.upper()} {req.model.upper()} {req.model_type.upper()}"
            computed = await self._calculate_valuation(res.data[0], req_name)
            yield f"data: {json.dumps(computed)}\n\n"
        else:
            yield f"data: {json.dumps({'error': 'No data found', 'status': 'waiting'})}\n\n"

        while True:
            await asyncio.sleep(15)
            yield ": keep-alive\n\n"

    async def get_global_stream(self):
        """Streams all bucket data using paged fetching and concurrent lookups"""
        self._last_sold_cache.clear() # Reset cache for new stream session
        page_size = 1000
        offset = 0
        total_pushed = 0

        # Optional: Get total count first for progress reporting (sent via data: { "count": X })
        try:
            count_res = self.supabase.table('japan_sales_buckets').select('*', count='exact').limit(0).execute()
            yield f"data: {json.dumps({'meta': {'total': count_res.count}})}\n\n"
        except:
            pass

        while True:
            # Fetch a page of buckets
            res = self.supabase.table('japan_sales_buckets').select('*').order('year', desc=True).range(offset, offset + page_size - 1).execute()
            
            if not res.data:
                break

            # Process lookups in smaller concurrent batches to avoid slamming DB
            batch_size = 50
            for i in range(0, len(res.data), batch_size):
                batch = res.data[i : i + batch_size]
                
                # Create tasks for this batch
                tasks = [self._calculate_valuation(row) for row in batch]
                results = await asyncio.gather(*tasks)

                for computed in results:
                    yield f"data: {json.dumps(computed)}\n\n"
                    total_pushed += 1
                
                # Yield keep-alive after each batch to keep connection warm
                yield ": keep-alive\n\n"
                await asyncio.sleep(0.01) # Yield control

            offset += page_size
            if len(res.data) < page_size:
                break

        yield f"data: {json.dumps({'status': 'complete', 'total': total_pushed})}\n\n"

        while True:
            await asyncio.sleep(15)
            yield ": keep-alive\n\n"
