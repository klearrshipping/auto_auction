"""
Two-phase bucket pipeline after sales upload:
  Phase 1 (Structure): Discover bucket keys from japan_sales.
  Phase 2 (Stats):     Compute analysis and upsert into single japan_sales_buckets table.

Run after cloud_sync. Triggered automatically by cloud_sync when data is uploaded.
"""

import os
import sys
import statistics
from collections import defaultdict
from datetime import datetime

from supabase import create_client, ClientOptions
from dotenv import load_dotenv
from scipy.stats import iqr, trim_mean

# Add project root for imports
_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.abspath(os.path.join(_script_dir, "..", ".."))
sys.path.insert(0, _root)

from config.sales_bands import get_score_band, get_mileage_band, SOLD_RESULTS

load_dotenv(os.path.join(_root, "tools", "aggregate_sales", ".env"))
options = ClientOptions(schema="sales_data")
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"), options=options)

# --- Fetch records ---
print("Fetching records from japan_sales...", flush=True)
all_records = []
offset = 0
while True:
    res = supabase.table("japan_sales").select(
        "year,make,model,model_type,scores,mileage,end_price,result,sale_date"
    ).range(offset, offset + 999).execute()
    if not res.data:
        break
    all_records.extend(res.data)
    offset += 1000
    if offset % 10000 == 0:
        print(f"  Fetched {len(all_records)}...", flush=True)

print(f"Total records fetched: {len(all_records)}", flush=True)

# --- Group into buckets: key -> [(price, sale_date), ...] ---
groups = defaultdict(list)
skipped = 0

for row in all_records:
    result_val = (row.get("result") or "").strip().lower()
    if result_val not in SOLD_RESULTS:
        skipped += 1
        continue
    price = row.get("end_price")
    if not price or price <= 0:
        skipped += 1
        continue

    score_band = get_score_band(row.get("scores"))
    mileage_band = get_mileage_band(row.get("mileage"))
    if not score_band or not mileage_band:
        skipped += 1
        continue

    key = (
        row.get("year") or 0,
        (row.get("make") or "").strip(),
        (row.get("model") or "").strip(),
        (row.get("model_type") or "").strip(),
        score_band,
        mileage_band,
    )
    sale_date = (row.get("sale_date") or "").strip() or None
    groups[key].append((price, sale_date))

records_in_buckets = sum(len(entries) for entries in groups.values())
assert records_in_buckets == len(all_records) - skipped, (
    f"Invariant violated: {records_in_buckets} records in buckets vs {len(all_records) - skipped} expected"
)
print(f"Skipped: {skipped}. Buckets: {len(groups)} ({records_in_buckets} sales, each in exactly one band)", flush=True)

# --- Upsert into single japan_sales_buckets table ---
print("\nUpdating buckets...", flush=True)
now = datetime.utcnow().isoformat()
batch = []
total_pushed = 0
BATCH_SIZE = 200

for (year, make, model, mtype, score_band, mileage_band), entries in groups.items():
    prices = [e[0] for e in entries]
    dated = [(p, d) for p, d in entries if d]
    dated.sort(key=lambda x: x[1] or "", reverse=True)
    last_sold_price = dated[0][0] if dated else None
    last_sold_date = dated[0][1] if dated else None

    prices_sorted = sorted(prices)
    count = len(prices)
    mean_val = statistics.mean(prices)
    median_val = int(statistics.median(prices))
    min_val = min(prices)
    max_val = max(prices)

    iqr_val = int(iqr(prices_sorted)) if count >= 4 else None
    trimmed = int(trim_mean(prices_sorted, 0.1)) if count >= 5 else None
    spread_pct = round((iqr_val / trimmed) * 100, 2) if iqr_val and trimmed else None

    std_val = int(statistics.stdev(prices)) if count >= 2 else None
    cv_val = round((std_val / mean_val) * 100, 2) if std_val and mean_val else None

    tier = "strong" if count >= 15 else ("light" if count >= 5 else "thin")

    batch.append({
        "year": year,
        "make": make,
        "model": model,
        "model_type": mtype,
        "score_band": score_band,
        "mileage_band": mileage_band,
        "comparable_count": count,
        "median_price": median_val,
        "mean_price": int(mean_val),
        "min_price": min_val,
        "max_price": max_val,
        "iqr": iqr_val,
        "trimmed_mean": trimmed,
        "price_spread_pct": spread_pct,
        "std_dev": std_val,
        "cv_pct": cv_val,
        "confidence_tier": tier,
        "last_sold_price": last_sold_price,
        "last_sold_date": last_sold_date,
        "updated_at": now,
    })

    if len(batch) >= BATCH_SIZE:
        supabase.table("japan_sales_buckets").upsert(
            batch,
            on_conflict="year,make,model,model_type,score_band,mileage_band",
        ).execute()
        total_pushed += len(batch)
        print(f"  {total_pushed} / {len(groups)}...", flush=True)
        batch = []

if batch:
    supabase.table("japan_sales_buckets").upsert(
        batch,
        on_conflict="year,make,model,model_type,score_band,mileage_band",
    ).execute()
    total_pushed += len(batch)

print(f"\nDone! {total_pushed} buckets updated")
