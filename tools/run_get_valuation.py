#!/usr/bin/env python3
"""
Standalone script to run get-valuation on all auction vehicles.
Prints summary: emitted, matched, unmatched, hidden (insufficient data).

Usage:
  python tools/run_get_valuation.py
  python tools/run_get_valuation.py --limit 100   # test with first 100 vehicles
"""

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, timezone

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

from dotenv import load_dotenv
from supabase import create_client, ClientOptions

for p in [
    os.path.join(ROOT, "tools", "aggregate_auction", ".env"),
    os.path.join(ROOT, "tools", "aggregate_sales", ".env"),
    os.path.join(ROOT, ".env"),
]:
    if os.path.exists(p):
        load_dotenv(p)
        break


async def main():
    parser = argparse.ArgumentParser(description="Run get-valuation on auction vehicles")
    parser.add_argument("--limit", type=int, default=0, help="Max vehicles to process (0 = all)")
    args = parser.parse_args()

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        print("Error: SUPABASE_URL and SUPABASE_KEY required in .env")
        sys.exit(1)

    supabase = create_client(url, key, options=ClientOptions(schema="auction_data"))
    from api.services.get_valuation import GetValuation

    pairing = GetValuation()
    start_time = time.perf_counter()
    start_iso = datetime.now(timezone.utc).isoformat()
    now = start_iso
    page_size = 200
    batch_concurrent = 20
    offset = 0
    total_emitted = 0
    matched = 0
    hidden_incomplete = 0

    print(f"Started: {start_iso}", flush=True)
    print("Running get-valuation on auction vehicles...", flush=True)
    print(flush=True)

    while True:
        fetch_size = page_size if not args.limit else min(page_size, args.limit - offset)
        if args.limit and fetch_size <= 0:
            break

        res = (
            supabase.table("vehicles")
            .select("*")
            .order("id")
            .range(offset, offset + fetch_size - 1)
            .execute()
        )

        if not res.data:
            break

        for i in range(0, len(res.data), batch_concurrent):
            batch = res.data[i : i + batch_concurrent]
            complete = [row for row in batch if pairing.has_sufficient_data_for_matching(row)]
            hidden_incomplete += len(batch) - len(complete)

            tasks = [pairing.pair_vehicle(row) for row in complete]
            results = await asyncio.gather(*tasks)

            valuation_rows = []
            for vehicle, bucket, valuation in results:
                total_emitted += 1
                if bucket:
                    matched += 1
                    vid = vehicle.get("id")
                    if vid:
                        # FV hierarchy: trimmed_mean → median_price → mean_price
                        trimmed = bucket.get("trimmed_mean")
                        median = bucket.get("median_price")
                        mean_val = bucket.get("mean_price")
                        fair_value = trimmed if trimmed else (median if median else mean_val)

                        valuation_rows.append({
                            "vehicle_id": vid,
                            "min_value": bucket.get("min_price"),
                            "max_value": bucket.get("max_price"),
                            "trimmed_mean": bucket.get("trimmed_mean"),
                            "price_spread_pct": bucket.get("price_spread_pct"),
                            "confidence_tier": bucket.get("confidence_tier", "thin"),
                            "fair_value": fair_value,
                            "updated_at": now,
                        })

            if valuation_rows:
                try:
                    supabase.table("valuations").upsert(valuation_rows, on_conflict="vehicle_id").execute()
                except Exception as e:
                    print(f"  Warning: failed to save valuations: {e}", flush=True)

        offset += len(res.data)
        print(f"  Processed {offset} vehicles... (matched: {matched})", flush=True)
        if len(res.data) < fetch_size:
            break
        if args.limit and offset >= args.limit:
            break

    elapsed = time.perf_counter() - start_time
    end_iso = datetime.now(timezone.utc).isoformat()
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)

    print("=" * 50, flush=True)
    print("GET VALUATION RESULTS", flush=True)
    print("=" * 50, flush=True)
    print(f"  Started:                 {start_iso}", flush=True)
    print(f"  Completed:               {end_iso}", flush=True)
    print(f"  Duration:                {mins}m {secs}s", flush=True)
    print(f"  Emitted (complete data): {total_emitted}", flush=True)
    print(f"  Matched (with valuation): {matched}", flush=True)
    print(f"  Unmatched:               {total_emitted - matched}", flush=True)
    print(f"  Hidden (insufficient):   {hidden_incomplete}", flush=True)
    print("=" * 50, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
