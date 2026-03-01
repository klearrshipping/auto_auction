import os
from supabase import create_client, ClientOptions
from dotenv import load_dotenv
from collections import Counter

load_dotenv('tools/aggregate_sales/.env')
options = ClientOptions(schema='sales_data')
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'), options=options)

print("Fetching data...", flush=True)

# Fetch in batches
all_records = []
offset = 0
while True:
    res = supabase.table('japan_sales').select('make,model_type,result').range(offset, offset + 999).execute()
    if not res.data:
        break
    all_records.extend(res.data)
    offset += 1000
    print(f"  Fetched {len(all_records)}...", flush=True)

print(f"\nTotal records: {len(all_records)}\n")

# 1. By Make
make_counts = Counter(r.get('make','') for r in all_records)
print("=== TOP 20 MAKES (potential model-per-make segmentation) ===")
for make, count in make_counts.most_common(20):
    pct = count / len(all_records) * 100
    print(f"  {make:<25} {count:>6} records ({pct:.1f}%)")

# 2. By Model Type
mtype_counts = Counter(r.get('model_type','') for r in all_records)
print("\n=== TOP 20 MODEL TYPES (potential type-based segmentation) ===")
for mtype, count in mtype_counts.most_common(20):
    pct = count / len(all_records) * 100
    print(f"  {mtype:<30} {count:>6} records ({pct:.1f}%)")

# 3. By Result
result_counts = Counter(r.get('result','') for r in all_records)
print("\n=== RESULT DISTRIBUTION ===")
for result, count in result_counts.most_common():
    pct = count / len(all_records) * 100
    print(f"  {result:<20} {count:>6} records ({pct:.1f}%)")
