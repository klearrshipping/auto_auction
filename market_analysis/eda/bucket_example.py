import os
from supabase import create_client, ClientOptions
from dotenv import load_dotenv

load_dotenv('tools/aggregate_sales/.env')
options = ClientOptions(schema='sales_data')
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'), options=options)

# Example bucket: 2021 Toyota Aqua, score 4-4.5, mileage 30k-60k
res = supabase.table('japan_sales').select(
    'year,make,model,model_type,scores,mileage,end_price,color,transmission,sale_date'
).eq('year', 2021).eq('make', 'TOYOTA').eq('model', 'AQUA').execute()

data = [r for r in res.data if r.get('end_price') and r.get('end_price') > 0]

def score_band(s):
    if not s: return None
    s = str(s).strip().upper()
    if s in ('R',): return 'R'
    if s == 'S': return 'S'
    if s == '5': return '5'
    try:
        n = float(s)
        if 3.0 <= n <= 3.5: return '3-3.5'
        if 4.0 <= n <= 4.5: return '4-4.5'
    except: pass
    return None

def mileage_band(m):
    if m is None: return None
    m = int(m)
    if m <= 30000: return '0-30k'
    if m <= 60000: return '30k-60k'
    if m <= 90000: return '60k-90k'
    if m <= 120000: return '90k-120k'
    if m <= 150000: return '120k-150k'
    if m <= 200000: return '150k-200k'
    return '200k+'

bucket = [r for r in data if score_band(r.get('scores')) == '4-4.5' and mileage_band(r.get('mileage')) == '30k-60k']

print(f"Bucket: 2021 Toyota AQUA | Score: 4-4.5 | Mileage: 30k-60k km")
print(f"Comparables found: {len(bucket)}")
print("=" * 70)
for r in bucket:
    print(f"  {r['sale_date']} | {r['mileage']:>7} km | Score: {r['scores']:<5} | ¥{r['end_price']:>10,} | {r['color']}")
