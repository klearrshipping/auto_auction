import json, glob, os

# Count all local JSON records on the hard drive
data_dir = 'data/sales_data'
total_local = 0
total_files = 0
for f in glob.glob(f'{data_dir}/**/*.json', recursive=True):
    if '_progress' in f or '_urls' in f:
        continue
    try:
        records = json.load(open(f, encoding='utf-8'))
        if isinstance(records, list):
            total_local += len(records)
            total_files += 1
    except:
        pass

# Count processed files tracked by cloud_sync
tracker = 'tools/aggregate_sales/processed_files.json'
tracked = 0
if os.path.exists(tracker):
    tracked = len(json.load(open(tracker, encoding='utf-8')).get('processed', []))

print(f'Local JSON files found:  {total_files}')
print(f'Local records total:     {total_local}')
print(f'Files marked as synced:  {tracked}')
print(f'Files not yet synced:    {total_files - tracked}')
