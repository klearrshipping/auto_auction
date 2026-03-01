import os
import statistics
from supabase import create_client, ClientOptions
from dotenv import load_dotenv
from scipy.stats import iqr, trim_mean
from collections import defaultdict
from datetime import datetime

load_dotenv('tools/aggregate_sales/.env')
options = ClientOptions(schema='sales_data')
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'), options=options)

# --- Band Definitions ---
def get_score_band(score):
    if not score:
        return None
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

def get_mileage_band(mileage):
    if mileage is None:
        return None
    try:
        m = int(mileage)
        if m <= 30000:      return '0-30k'
        elif m <= 60000:    return '30k-60k'
        elif m <= 90000:    return '60k-90k'
        elif m <= 120000:   return '90k-120k'
        elif m <= 150000:   return '120k-150k'
        elif m <= 200000:   return '150k-200k'
        else:               return '200k+'
    except (ValueError, TypeError):
        return None

# --- Fetch All Records ---
print("Fetching all records from Supabase...", flush=True)
all_records = []
offset = 0
while True:
    res = supabase.table('japan_sales').select(
        'year,make,model,model_type,scores,mileage,end_price,result'
    ).range(offset, offset + 999).execute()
    if not res.data:
        break
    all_records.extend(res.data)
    offset += 1000
    if offset % 10000 == 0:
        print(f"  Fetched {len(all_records)}...", flush=True)

print(f"Total records fetched: {len(all_records)}", flush=True)

# --- Group Into Buckets ---
groups = defaultdict(list)
skipped = 0

for row in all_records:
    if row.get('result') not in ('sold', 'negotiate sold'):
        skipped += 1
        continue
    price = row.get('end_price')
    if not price or price <= 0:
        skipped += 1
        continue

    score_band = get_score_band(row.get('scores'))
    mileage_band = get_mileage_band(row.get('mileage'))

    if not score_band or not mileage_band:
        skipped += 1
        continue

    key = (
        row.get('year') or 0,
        row.get('make', '') or '',
        row.get('model', '') or '',
        row.get('model_type', '') or '',
        score_band,
        mileage_band
    )
    groups[key].append(price)

print(f"Skipped records (no valid price/score/mileage): {skipped}")
print(f"Total buckets to push: {len(groups)}", flush=True)

# --- Delete existing rows and re-insert fresh ---
print("Clearing existing bucket data...", flush=True)
supabase.table('japan_sales_buckets').delete().neq('id', 0).execute()
print("Cleared. Pushing fresh data...", flush=True)

now = datetime.utcnow().isoformat()
batch = []
total_pushed = 0
BATCH_SIZE = 200

for (year, make, model, mtype, score_band, mileage_band), prices in groups.items():
    prices_sorted = sorted(prices)
    iqr_val = int(iqr(prices_sorted)) if len(prices) >= 4 else None
    trimmed = int(trim_mean(prices_sorted, 0.1)) if len(prices) >= 5 else None

    # Derived confidence metrics
    spread_pct = round((iqr_val / trimmed) * 100, 2) if iqr_val and trimmed else None
    count = len(prices)
    if count >= 15:
        tier = 'strong'
    elif count >= 5:
        tier = 'light'
    else:
        tier = 'thin'

    batch.append({
        'year':             year,
        'make':             make,
        'model':            model,
        'model_type':       mtype,
        'score_band':       score_band,
        'mileage_band':     mileage_band,
        'comparable_count': count,
        'median_price':     int(statistics.median(prices)),
        'mean_price':       int(statistics.mean(prices)),
        'min_price':        min(prices),
        'max_price':        max(prices),
        'iqr':              iqr_val,
        'trimmed_mean':     trimmed,
        'price_spread_pct': spread_pct,
        'confidence_tier':  tier,
        'updated_at':       now
    })

    if len(batch) >= BATCH_SIZE:
        supabase.table('japan_sales_buckets').insert(batch).execute()
        total_pushed += len(batch)
        print(f"  Pushed {total_pushed} / 12401 buckets...", flush=True)
        batch = []

# Final flush
if batch:
    supabase.table('japan_sales_buckets').insert(batch).execute()
    total_pushed += len(batch)

print(f"\nDone! Total buckets pushed to Supabase: {total_pushed}")
