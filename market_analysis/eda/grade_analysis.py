import os, sys
from supabase import create_client, ClientOptions
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv('tools/aggregate_sales/.env')
options = ClientOptions(schema='sales_data')
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'), options=options)

print("Fetching sample data...", flush=True)
res = supabase.table('japan_sales').select('make,model,model_type,grade').limit(5000).execute()
data = res.data

groups = defaultdict(set)
for row in data:
    key = (row.get('make',''), row.get('model',''), row.get('model_type',''))
    grade = row.get('grade', '')
    if grade:
        groups[key].add(grade)

sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)[:15]

output_lines = ["\nTop 15 Make/Model/Type combos by Grade Variation:", "=" * 70]
for (make, model, mtype), grades in sorted_groups:
    output_lines.append(f"\n{make} | {model} | {mtype}")
    output_lines.append(f"  Unique grades: {len(grades)}")
    for g in sorted(grades):
        output_lines.append(f"    - {g}")

with open('grade_results.txt', 'w', encoding='utf-8') as f:
    f.write('\n'.join(output_lines))

print('\n'.join(output_lines), flush=True)
