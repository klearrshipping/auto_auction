"""
DatabaseHandler for auction data - Supabase client for vehicles, processed_urls, vehicle_details.
Uses SUPABASE_URL and SUPABASE_KEY from .env (project root or tools/aggregate_sales).
"""
import os
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Any

logger = logging.getLogger(__name__)
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class DatabaseHandler:
    """Supabase client for auction tables: vehicles, processed_urls, vehicle_details."""

    def __init__(self):
        self.supabase_client = None
        self._connected = False

    def connect(self):
        from dotenv import load_dotenv
        from supabase import create_client
        for p in [os.path.join(_ROOT, ".env"), os.path.join(_ROOT, "tools", "aggregate_sales", ".env")]:
            if os.path.exists(p):
                load_dotenv(p)
                break
        url = os.environ.get("SUPABASE_URL", "")
        key = os.environ.get("SUPABASE_KEY", "")
        if not url or not key:
            raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")
        self.supabase_client = create_client(url, key)
        self._connected = True

    def close(self):
        self._connected = False
        self.supabase_client = None

    async def bulk_insert_staging_concurrent(self, site_name: str, listings: List[Dict]) -> Tuple[int, int, int]:
        if not listings or not self.supabase_client:
            return (0, 0, 0)
        try:
            recs = [{k: L.get(k) for k in ["site_name", "lot_number", "make", "model", "year", "mileage", "start_price", "end_price", "grade", "color", "result", "scores", "lot_link", "auction", "search_date"]} for L in listings]
            try:
                self.supabase_client.table("vehicles_staging").insert(recs).execute()
                return (len(recs), 0, 0)
            except Exception:
                self.supabase_client.table("vehicles").upsert(recs, on_conflict="site_name,lot_number").execute()
                return (len(recs), len(recs), 0)
        except Exception as e:
            logger.error(f"bulk_insert_staging_concurrent: {e}")
            return (0, 0, len(listings))

    def process_staging_to_main(self) -> Tuple[int, int]:
        if not self.supabase_client:
            return (0, 0)
        try:
            r = self.supabase_client.table("vehicles_staging").select("*").execute()
            rows = r.data or []
            if not rows:
                return (0, 0)
            recs = [{k: x.get(k) for k in ["site_name", "lot_number", "make", "model", "year", "mileage", "start_price", "end_price", "grade", "color", "result", "scores", "lot_link", "auction", "search_date"]} for x in rows]
            self.supabase_client.table("vehicles").upsert(recs, on_conflict="site_name,lot_number").execute()
            for x in rows:
                self.supabase_client.table("vehicles_staging").delete().match({"id": x["id"]}).execute()
            return (len(recs), 0)
        except Exception:
            return (0, 0)

    def populate_processed_urls(self) -> Tuple[int, int]:
        if not self.supabase_client:
            return (0, 0)
        try:
            v = self.supabase_client.table("vehicles").select("id, site_name, lot_link").not_.is_("lot_link", "null").execute()
            ins, sk = 0, 0
            for x in (v.data or []):
                if not x.get("lot_link"):
                    continue
                ex = self.supabase_client.table("processed_urls").select("id").eq("vehicle_id", x["id"]).execute()
                if ex.data:
                    sk += 1
                    continue
                self.supabase_client.table("processed_urls").upsert([{"site_name": x["site_name"], "url": x["lot_link"], "vehicle_id": x["id"], "processed": False}], on_conflict="site_name,url").execute()
                ins += 1
            return (ins, sk)
        except Exception as e:
            logger.error(f"populate_processed_urls: {e}")
            return (0, 0)

    def cleanup_staging(self) -> int:
        if not self.supabase_client:
            return 0
        try:
            r = self.supabase_client.table("vehicles_staging").select("id").execute()
            for x in (r.data or []):
                self.supabase_client.table("vehicles_staging").delete().eq("id", x["id"]).execute()
            return len(r.data or [])
        except Exception:
            return 0

    def verify_data_movement(self) -> Dict[str, Any]:
        if not self.supabase_client:
            return {}
        try:
            s = self.supabase_client.table("vehicles_staging").select("*", count="exact").execute()
            m = self.supabase_client.table("vehicles").select("*", count="exact").execute()
            u = self.supabase_client.table("processed_urls").select("*", count="exact").execute()
            return {"processed_count": getattr(s, "count", len(s.data or [])), "duplicate_count": 0, "main_count": getattr(m, "count", len(m.data or [])), "urls_count": getattr(u, "count", len(u.data or []))}
        except Exception:
            return {}

    def verify_url_processing(self) -> Dict[str, Any]:
        if not self.supabase_client:
            return {}
        try:
            t = self.supabase_client.table("vehicles").select("*", count="exact").limit(0).execute()
            u = self.supabase_client.table("processed_urls").select("*").execute()
            d = u.data or []
            p = sum(1 for x in d if x.get("processed"))
            return {"total_vehicles": getattr(t, "count", 0), "processed_urls": p, "unprocessed_urls": len(d) - p}
        except Exception:
            return {}
