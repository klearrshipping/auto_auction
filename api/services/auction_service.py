"""Service for auction_data.vehicles and valuations from Supabase.
Listings stream from valuations (fair values) joined with vehicles for details."""

import hashlib
import json
import asyncio
import os
from datetime import datetime, timezone
from urllib.parse import quote
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

    def _proxy_url(self, url: str, proxy_base: str | None) -> str:
        if not proxy_base or not url or "aleado.com" not in url:
            return url
        return f"{proxy_base.rstrip('/')}/api/proxy-image?url={quote(url, safe='')}"

    def _row_to_listing(self, vehicle: dict, valuation: dict, proxy_base: str | None) -> dict:
        """Convert vehicle + valuation to a listing payload. Streams from valuations (fair values)."""
        score_val = None
        if isinstance(vehicle.get("scores"), dict):
            score_val = vehicle["scores"].get("score")
        elif vehicle.get("scores") is not None:
            score_val = str(vehicle["scores"])

        year = vehicle.get("year")
        make = (vehicle.get("make") or "").strip()
        model = (vehicle.get("model") or "").strip()
        parts = [str(y) for y in [year, make, model] if y]
        vehicle_name = " ".join(parts) if parts else None

        image_urls = vehicle.get("image_urls") or []
        image_links = [self._proxy_url(u, proxy_base) for u in image_urls] if image_urls else []
        return {
            "image_links": image_links,
            "vehicle_name": vehicle_name,
            "year": year,
            "make": make,
            "model": model,
            "model_type": vehicle.get("model_type"),
            "grade": vehicle.get("grade"),
            "color": vehicle.get("color"),
            "mileage": vehicle.get("mileage"),
            "score": score_val,
            "auction_house": vehicle.get("auction"),
            "min_price": valuation.get("min_value") if valuation else None,
            "max_price": valuation.get("max_value") if valuation else None,
            "trimmed_mean": valuation.get("trimmed_mean") if valuation else None,
            "fair_value": valuation.get("fair_value") if valuation else None,
        }

    def _fetch_listings_batch(self, offset: int, limit: int, proxy_base: str | None) -> tuple[list[dict], int]:
        """Fetch a batch of listings from valuations (joined with vehicles). Returns (listings, fetched_count)."""
        vres = (
            self.supabase.table("valuations")
            .select("vehicle_id, min_value, max_value, trimmed_mean, fair_value")
            .order("id")
            .range(offset, offset + limit - 1)
            .execute()
        )
        if not vres.data:
            return [], 0

        vehicle_ids = [v["vehicle_id"] for v in vres.data]
        valuations_by_vid = {v["vehicle_id"]: v for v in vres.data}

        vehicles_res = (
            self.supabase.table("vehicles")
            .select("id, image_urls, year, make, model, model_type, grade, color, mileage, scores, auction")
            .in_("id", vehicle_ids)
            .execute()
        )
        vehicles_by_id = {r["id"]: r for r in (vehicles_res.data or [])}

        # Preserve order from valuations
        listings = []
        for vid in vehicle_ids:
            vehicle = vehicles_by_id.get(vid)
            valuation = valuations_by_vid.get(vid)
            if vehicle:
                listings.append(self._row_to_listing(vehicle, valuation, proxy_base))

        return listings, len(vres.data)

    async def get_listings_count(self) -> int:
        """Live count from auction_data.valuations. No caching."""
        try:
            res = (
                self.supabase.table("valuations")
                .select("id", count="exact")
                .limit(0)
                .execute()
            )
            return res.count or 0
        except Exception:
            return 0

    async def get_auction_listings_page(
        self,
        page: int = 1,
        limit: int = 50,
        proxy_base: str | None = None,
    ) -> tuple[list[dict], int]:
        """
        Paginated auction listings for REST. Streams from valuations (fair values).
        Returns (listings, total_count). Use for initial load with ETag caching.
        """
        try:
            count_res = (
                self.supabase.table("valuations")
                .select("id", count="exact")
                .limit(0)
                .execute()
            )
            total = count_res.count or 0
        except Exception:
            total = 0

        if page < 1 or limit < 1:
            return [], total

        offset = (page - 1) * limit
        listings, fetched = self._fetch_listings_batch(offset, limit, proxy_base)
        return listings, total

    async def stream_auction_listings(
        self,
        initial_page_size: int = 50,
        subsequent_page_size: int = 100,
        proxy_base: str | None = None,
    ):
        """Stream auction listings with valuation. Chunked delivery: first 50 records immediately,
        then the rest in batches of 100. Users see cars appearing almost instantly.
        Uses pre-computed valuations table. If proxy_base is set, aleado image URLs use proxy.
        """
        offset = 0
        total_emitted = 0
        first_batch = True

        try:
            count_res = (
                self.supabase.table("valuations")
                .select("id", count="exact")
                .limit(0)
                .execute()
            )
            yield f"data: {json.dumps({'meta': {'total': count_res.count or 0}})}\n\n"
        except Exception:
            pass

        while True:
            page_size = initial_page_size if first_batch else subsequent_page_size
            listings, fetched = self._fetch_listings_batch(offset, page_size, proxy_base)

            if not listings:
                break

            for payload in listings:
                yield f"data: {json.dumps(payload)}\n\n"
                total_emitted += 1

            yield ": keep-alive\n\n"
            await asyncio.sleep(0.01)

            offset += page_size
            first_batch = False
            if fetched < page_size:
                break

        yield f"data: {json.dumps({'status': 'complete', 'total': total_emitted})}\n\n"

        while True:
            await asyncio.sleep(15)
            yield ": keep-alive\n\n"

    def _row_to_delta(self, vehicle: dict, valuation: dict, proxy_base: str | None) -> dict:
        """Convert vehicle + valuation to a minimal delta payload for live updates."""
        vid = vehicle.get("id")
        result_val = (vehicle.get("result") or "").strip().lower()
        delta_type = "lot_sold" if result_val in ("sold", "1", "yes") else "lot_updated"

        year = vehicle.get("year")
        make = (vehicle.get("make") or "").strip()
        model = (vehicle.get("model") or "").strip()
        parts = [str(y) for y in [year, make, model] if y]
        vehicle_name = " ".join(parts) if parts else None

        image_urls = vehicle.get("image_urls") or []
        image_links = [self._proxy_url(u, proxy_base) for u in image_urls] if image_urls else []

        return {
            "type": delta_type,
            "vehicle_id": vid,
            "vehicle_name": vehicle_name,
            "start_price": vehicle.get("start_price"),
            "end_price": vehicle.get("end_price"),
            "result": vehicle.get("result"),
            "fair_value": valuation.get("fair_value") if valuation else None,
            "min_price": valuation.get("min_value") if valuation else None,
            "max_price": valuation.get("max_value") if valuation else None,
            "updated_at": valuation.get("updated_at") if valuation else vehicle.get("updated_at"),
            "image_links": image_links[:3],
        }

    async def stream_auction_updates(
        self,
        poll_interval: float = 5.0,
        proxy_base: str | None = None,
        since: str | None = None,
    ):
        """
        Stream only deltas (valuation changes) via SSE. Polls valuations table.
        Use for live bid/status updates. Client can pass ?since=ISO8601 after reconnect.
        """
        last_seen = since
        if not last_seen:
            last_seen = datetime.now(timezone.utc).isoformat()

        yield f"data: {json.dumps({'meta': {'type': 'auction_updates', 'poll_interval': poll_interval}})}\n\n"

        while True:
            try:
                vres = (
                    self.supabase.table("valuations")
                    .select("vehicle_id, min_value, max_value, fair_value, updated_at")
                    .order("updated_at", desc=False)
                    .gt("updated_at", last_seen)
                    .limit(100)
                    .execute()
                )

                if vres.data:
                    vehicle_ids = [v["vehicle_id"] for v in vres.data]
                    valuations_by_vid = {v["vehicle_id"]: v for v in vres.data}

                    vehicles_res = (
                        self.supabase.table("vehicles")
                        .select("id, start_price, end_price, result, updated_at, year, make, model, image_urls")
                        .in_("id", vehicle_ids)
                        .execute()
                    )
                    vehicles_by_id = {r["id"]: r for r in (vehicles_res.data or [])}

                    latest = last_seen
                    for vid in vehicle_ids:
                        vehicle = vehicles_by_id.get(vid)
                        valuation = valuations_by_vid.get(vid)
                        if vehicle:
                            delta = self._row_to_delta(vehicle, valuation, proxy_base)
                            yield f"data: {json.dumps(delta)}\n\n"
                            u = (valuation or {}).get("updated_at")
                            if u and (not latest or str(u) > latest):
                                latest = str(u)
                    last_seen = latest

            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)})}\n\n"
                break

            yield ": keep-alive\n\n"
            await asyncio.sleep(poll_interval)
