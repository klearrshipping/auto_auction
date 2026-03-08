"""Service for auction_data.vehicles from Supabase."""

import json
import asyncio
import os
from supabase import create_client, ClientOptions
from dotenv import load_dotenv

SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SERVICE_DIR, "..", ".."))
for p in [
    os.path.join(ROOT_DIR, "tools", "aggregate_sales", ".env"),
    os.path.join(ROOT_DIR, "tools", "aggregate_auction", ".env"),
    os.path.join(ROOT_DIR, ".env"),
]:
    if os.path.exists(p):
        load_dotenv(p)
        break


class AuctionService:
    def __init__(self):
        options = ClientOptions(schema="auction_data")
        self.supabase = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_KEY"),
            options=options,
        )

    async def stream_vehicles(self, page_size: int = 500):
        """Stream all auction vehicles from Supabase as Server-Sent Events."""
        offset = 0
        total_pushed = 0

        try:
            count_res = (
                self.supabase.table("vehicles")
                .select("*", count="exact")
                .limit(0)
                .execute()
            )
            yield f"data: {json.dumps({'meta': {'total': count_res.count or 0}})}\n\n"
        except Exception:
            pass

        while True:
            res = (
                self.supabase.table("vehicles")
                .select("*")
                .order("id")
                .range(offset, offset + page_size - 1)
                .execute()
            )

            if not res.data:
                break

            for row in res.data:
                yield f"data: {json.dumps(row)}\n\n"
                total_pushed += 1

            yield ": keep-alive\n\n"
            await asyncio.sleep(0.01)

            offset += page_size
            if len(res.data) < page_size:
                break

        yield f"data: {json.dumps({'status': 'complete', 'total': total_pushed})}\n\n"

        while True:
            await asyncio.sleep(15)
            yield ": keep-alive\n\n"

    async def stream_vehicles_with_valuation(self, page_size: int = 200, batch_concurrent: int = 20):
        """Stream auction vehicles paired with their sales bucket and valuation.
        Hides vehicles with insufficient data for matching (e.g. model_type, model, grade all missing).
        Unmatched vehicles (no bucket) are still included.
        """
        from api.services.get_valuation import GetValuation

        pairing = GetValuation()
        offset = 0
        total_emitted = 0
        matched = 0
        hidden_incomplete = 0

        try:
            count_res = (
                self.supabase.table("vehicles")
                .select("*", count="exact")
                .limit(0)
                .execute()
            )
            yield f"data: {json.dumps({'meta': {'total': count_res.count or 0}})}\n\n"
        except Exception:
            pass

        while True:
            res = (
                self.supabase.table("vehicles")
                .select("*")
                .order("id")
                .range(offset, offset + page_size - 1)
                .execute()
            )

            if not res.data:
                break

            for i in range(0, len(res.data), batch_concurrent):
                batch = res.data[i : i + batch_concurrent]
                # Filter out vehicles with insufficient data
                complete = [row for row in batch if pairing.has_sufficient_data_for_matching(row)]
                hidden_incomplete += len(batch) - len(complete)

                tasks = [pairing.pair_vehicle(row) for row in complete]
                results = await asyncio.gather(*tasks)

                for vehicle, bucket, valuation in results:
                    payload = {
                        "vehicle": vehicle,
                        "bucket": bucket,
                        "valuation": valuation,
                    }
                    yield f"data: {json.dumps(payload)}\n\n"
                    total_emitted += 1
                    if bucket:
                        matched += 1

            yield ": keep-alive\n\n"
            await asyncio.sleep(0.01)

            offset += page_size
            if len(res.data) < page_size:
                break

        yield f"data: {json.dumps({'status': 'complete', 'emitted': total_emitted, 'matched': matched, 'hidden_incomplete': hidden_incomplete})}\n\n"

        while True:
            await asyncio.sleep(15)
            yield ": keep-alive\n\n"

    async def stream_auction_listings(
        self,
        page_size: int = 200,
    ):
        """Stream auction listings with valuation: image_links, year, make, model, grade, color,
        mileage, score, auction_house, min_price, max_price, trimmed_mean.
        Uses pre-computed valuations table. Vehicles without valuations have null for price fields.
        """
        offset = 0
        total_emitted = 0

        try:
            count_res = (
                self.supabase.table("vehicles")
                .select("id", count="exact")
                .limit(0)
                .execute()
            )
            yield f"data: {json.dumps({'meta': {'total': count_res.count or 0}})}\n\n"
        except Exception:
            pass

        while True:
            res = (
                self.supabase.table("vehicles")
                .select("id, image_urls, year, make, model, model_type, grade, color, mileage, scores, auction")
                .order("id")
                .range(offset, offset + page_size - 1)
                .execute()
            )

            if not res.data:
                break

            ids = [r["id"] for r in res.data]
            valuations_by_id = {}
            try:
                vres = (
                    self.supabase.table("valuations")
                    .select("vehicle_id, min_value, max_value, trimmed_mean")
                    .in_("vehicle_id", ids)
                    .execute()
                )
                for v in vres.data or []:
                    valuations_by_id[v["vehicle_id"]] = v
            except Exception:
                pass

            for row in res.data:
                vid = row.get("id")
                v = valuations_by_id.get(vid) if vid else None
                score_val = None
                if isinstance(row.get("scores"), dict):
                    score_val = row["scores"].get("score")
                elif row.get("scores") is not None:
                    score_val = str(row["scores"])

                year = row.get("year")
                make = (row.get("make") or "").strip()
                model = (row.get("model") or "").strip()
                parts = [str(y) for y in [year, make, model] if y]
                vehicle_name = " ".join(parts) if parts else None

                payload = {
                    "image_links": row.get("image_urls") or [],
                    "vehicle_name": vehicle_name,
                    "year": year,
                    "make": make,
                    "model": model,
                    "model_type": row.get("model_type"),
                    "grade": row.get("grade"),
                    "color": row.get("color"),
                    "mileage": row.get("mileage"),
                    "score": score_val,
                    "auction_house": row.get("auction"),
                    "min_price": v.get("min_value") if v else None,
                    "max_price": v.get("max_value") if v else None,
                    "trimmed_mean": v.get("trimmed_mean") if v else None,
                }
                yield f"data: {json.dumps(payload)}\n\n"
                total_emitted += 1

            yield ": keep-alive\n\n"
            await asyncio.sleep(0.01)

            offset += page_size
            if len(res.data) < page_size:
                break

        yield f"data: {json.dumps({'status': 'complete', 'total': total_emitted})}\n\n"

        while True:
            await asyncio.sleep(15)
            yield ": keep-alive\n\n"
