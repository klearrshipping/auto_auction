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
        from api.services.vehicle_bucket_pairing import VehicleBucketPairing

        pairing = VehicleBucketPairing()
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
