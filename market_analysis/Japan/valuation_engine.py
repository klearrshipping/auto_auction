"""
Japan Auction Vehicle Valuation Engine
Looks up the Fair Value (FV) of a vehicle based on historical auction data.
Uses bucket-based comparable sales methodology.

Usage:
    python valuation_engine.py --year 2026 --make HONDA --model VEZEL --model-type RV5 --score S --mileage 15000
"""

import os
import argparse
from supabase import create_client, ClientOptions
from dotenv import load_dotenv

# --- Load credentials ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(SCRIPT_DIR, '..', '..', 'tools', 'aggregate_sales', '.env')
load_dotenv(ENV_PATH)
options = ClientOptions(schema='sales_data')
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'), options=options)

# --- Band Definitions ---
def get_score_band(score):
    if not score: return None
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
    try:
        m = int(mileage)
        if m <= 30000:    return '0-30k'
        elif m <= 60000:  return '30k-60k'
        elif m <= 90000:  return '60k-90k'
        elif m <= 120000: return '90k-120k'
        elif m <= 150000: return '120k-150k'
        elif m <= 200000: return '150k-200k'
        else:             return '200k+'
    except (ValueError, TypeError):
        return None

def format_yen(value):
    return f"¥{value:,.0f}"

def lookup(year, make, model, model_type, score, mileage):
    score_band = get_score_band(score)
    mileage_band = get_mileage_band(mileage)

    if not score_band:
        print(f"Invalid score: '{score}'. Valid scores: R, 3, 3.5, 4, 4.5, 5, S")
        return
    if not mileage_band:
        print(f"Invalid mileage: '{mileage}'.")
        return

    # Query the bucket
    res = supabase.table('japan_sales_buckets').select('*').eq(
        'year', year
    ).eq('make', make.upper()).eq('model', model.upper()).eq(
        'model_type', model_type.upper()
    ).eq('score_band', score_band).eq('mileage_band', mileage_band).limit(1).execute()

    if not res.data:
        print(f"\nNo comparable data found for:")
        print(f"  {year} {make.upper()} {model.upper()} {model_type.upper()} | Score: {score_band} | Mileage: {mileage_band}")
        print("\nTry a broader score band or check the year/make/model spelling.")
        return

    row = res.data[0]
    count = row['comparable_count']
    tier = row['confidence_tier']
    spread = row['price_spread_pct']
    iqr = row['iqr']
    trimmed = row['trimmed_mean']
    median = row['median_price']
    
    # Determine FV price
    if tier == 'strong' and trimmed:
        fv = trimmed
        method = 'trimmed mean'
    elif median:
        fv = median
        method = 'median'
    else:
        fv = row['mean_price']
        method = 'mean'

    # Range: FV ± IQR/2 (approximation of Q1-Q3 band)
    if iqr:
        range_low = fv - (iqr // 2)
        range_high = fv + (iqr // 2)
        range_str = f"{format_yen(range_low)} – {format_yen(range_high)}"
    else:
        range_str = f"{format_yen(row['min_price'])} – {format_yen(row['max_price'])}"

    # Output
    print()
    print("=" * 52)
    print(f"  VEHICLE VALUATION — JAPAN AUCTION MARKET")
    print("=" * 52)
    print(f"  {year} {make.upper()} {model.upper()} {model_type.upper()}")
    print(f"  Score: {score_band}   |   Mileage Band: {mileage_band} km")
    print("-" * 52)
    print(f"  FV Price:       {format_yen(fv)}  ({method})")
    print(f"  Range (Q1–Q3):  {range_str}")
    if spread:
        print(f"  Market Spread:  {spread:.2f}%")
    print(f"  Confidence:     {tier.upper()} ({count} comparables)")
    print("=" * 52)
    print()

def main():
    parser = argparse.ArgumentParser(description='Japan Auction Vehicle Valuation Engine')
    parser.add_argument('--year',       type=int,   required=True,  help='Vehicle year (e.g. 2026)')
    parser.add_argument('--make',       type=str,   required=True,  help='Make (e.g. HONDA)')
    parser.add_argument('--model',      type=str,   required=True,  help='Model (e.g. VEZEL)')
    parser.add_argument('--model-type', type=str,   required=True,  help='Model type/chassis (e.g. RV5)')
    parser.add_argument('--score',      type=str,   required=True,  help='Auction score (R, 3, 3.5, 4, 4.5, 5, S)')
    parser.add_argument('--mileage',    type=int,   required=True,  help='Mileage in km (e.g. 15000)')
    args = parser.parse_args()

    lookup(
        year=args.year,
        make=args.make,
        model=args.model,
        model_type=args.model_type,
        score=args.score,
        mileage=args.mileage
    )

if __name__ == '__main__':
    main()
