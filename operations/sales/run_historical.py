import os
import sys
import subprocess
from datetime import date, timedelta
import calendar

# Target months in order: November backwards to January
# But within each month, process days forwards (1st to 30/31st)
TARGET_MONTHS = [
    (2025, 11),
    (2025, 10),
    (2025, 9),
    (2025, 8),
    (2025, 7),
    (2025, 6),
    (2025, 5),
    (2025, 4),
    (2025, 3),
    (2025, 2),
    (2025, 1)
]

def get_japan_working_days(year, month):
    """Generate a chronological list of Japan working days for a specific month."""
    try:
        import holidays
        jp_holidays = holidays.Japan(years=[year])
    except ImportError:
        print("Warning: 'holidays' package missing.")
        jp_holidays = set()
        
    _, last_day = calendar.monthrange(year, month)
    
    days = []
    for day in range(1, last_day + 1):
        current = date(year, month, day)
        if current.weekday() < 5 and current not in jp_holidays:
            days.append(current.isoformat())
            
    return days

def main():
    print("=" * 50)
    print("STARTING 2025 HISTORICAL EXTRACTION")
    print("=" * 50)
    
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _root = os.path.dirname(os.path.dirname(_script_dir))
    extractor_script = os.path.join(_script_dir, "run_all.py")
    sync_script = os.path.join(_root, "tools", "aggregate_sales", "cloud_sync.py")
    
    for year, month in TARGET_MONTHS:
        working_days = get_japan_working_days(year, month)
        print(f"\n[{year}-{month:02d}] Fetching {len(working_days)} days sequentially forwards...")
        
        for day in working_days:
            print(f"\n[>>>] Initiating Extraction for: {day}")
            
            # 1. Extract Data
            extract_cmd = [sys.executable, "-u", extractor_script, "--date", day]
            ext_result = subprocess.run(extract_cmd, cwd=_root)
            
            if ext_result.returncode != 0:
                print(f"Extraction failed for {day}. Skipping to next.")
                continue
                
            # 2. Sync to Supabase
            print(f"[>>>] Syncing {day} to Database...")
            sync_cmd = [sys.executable, "-u", sync_script, "--batch-size", "50"]
            subprocess.run(sync_cmd, cwd=_root)
            
            print(f"[✓] Successfully completed {day}")

if __name__ == "__main__":
    main()
