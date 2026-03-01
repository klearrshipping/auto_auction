import json
import glob
import os

def check_status():
    progress_files = glob.glob('data/sales_data/_progress/extraction_*.json')
    if not progress_files:
        print("[Status] No extraction files found.")
        return

    # Grab the most recently modified progress tracker
    latest = max(progress_files, key=os.path.getmtime)
    
    with open(latest, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    date = data.get('date', 'Unknown')
    t = data.get('totals', {})
    
    pct = (t.get('jobs_completed', 0) / t.get('jobs_total', 1)) * 100
    
    print("=" * 45)
    print(f"LIVE SCRAPE: {date} (In Progress)")
    print("=" * 45)
    print(f"Models:  {t.get('jobs_completed', 0)} / {t.get('jobs_total', 0)} ({pct:.1f}%)")
    print(f"Records: {t.get('records_extracted', 0)} scraped")
    print("=" * 45 + "\n")

if __name__ == "__main__":
    check_status()
