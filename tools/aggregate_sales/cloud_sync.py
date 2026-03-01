import os
import json
import re
import argparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set
from supabase import create_client, Client, ClientOptions

# --- Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data', 'sales_data')
TRACKER_PATH = os.path.join(SCRIPT_DIR, 'processed_files.json')

# --- Tracked Files Cache Management ---
def load_processed_files() -> Set[str]:
    """Load the set of relative file paths that have already been pushed to Supabase."""
    if os.path.exists(TRACKER_PATH):
        try:
            with open(TRACKER_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.get("processed", []))
        except json.JSONDecodeError:
            print("Warning: corrupted tracker file. Re-syncing all files.")
            return set()
    return set()

def save_processed_files(processed_set: Set[str]):
    """Save the set of processed files to JSON."""
    with open(TRACKER_PATH, 'w', encoding='utf-8') as f:
        json.dump({"processed": list(processed_set)}, f, indent=2)

# --- Data Cleaning Functions (Identical to previous logic) ---
def clean_string(val):
    if not val:
        return ""
    return str(val).strip()

def clean_color(val):
    if not val:
        return "NO-COLOR"
    color_val = str(val).strip().upper()
    non_colors = {"ACTUAL VEHICLE", "UNSPECIFIED", "UNKNOWN", "NONE", "-", "N/A", "NO COLOR", "NOT SPECIFIED"}
    if not color_val or color_val in non_colors:
        return "NO-COLOR"
    return color_val

def clean_displacement(disp_str):
    if not disp_str:
        return None
    digits = re.sub(r'[^\d]', '', str(disp_str))
    return int(digits) if digits else None

def clean_mileage(mileage_str):
    if not mileage_str:
        return None
    digits = re.sub(r'[^\d]', '', str(mileage_str))
    return int(digits) if digits else None

def clean_price(price_str):
    if not price_str:
        return None
    try:
        digits = re.sub(r'[^\d]', '', str(price_str))
        if digits:
            return int(digits) * 1000  # Raw yen
        return None
    except ValueError:
        return None

# --- Pipeline Logic ---
def get_supabase_client() -> Client:
    """Initialize the Supabase client securely using local .env variables."""
    from dotenv import load_dotenv
    
    # Load the keys from the secure .env file in the same directory
    env_path = os.path.join(SCRIPT_DIR, '.env')
    load_dotenv(dotenv_path=env_path)
    
    url = str(os.environ.get("SUPABASE_URL", ""))
    key = str(os.environ.get("SUPABASE_KEY", ""))
    
    if not url or not key:
        raise ValueError(f"Missing 'SUPABASE_URL' or 'SUPABASE_KEY' from {env_path}")
                         
    options = ClientOptions(schema="sales_data")
    return create_client(url, key, options=options)

def scan_for_unsynced_files(processed_cache: Set[str]) -> List[Tuple[str, str]]:
    """Scan data directory and return list of (absolute_path, relative_path) for completely new files."""
    unsynced = []
    for root, _, files in os.walk(DATA_DIR):
        if '_progress' in root:
            continue
            
        for file in files:
            if file.endswith('.json') and not file.endswith('_urls.json'):
                abs_path = os.path.join(root, file)
                rel_path = os.path.relpath(abs_path, PROJECT_ROOT)
                
                if rel_path not in processed_cache:
                    unsynced.append((abs_path, rel_path))
    return unsynced

def clean_file_data(abs_path: str, rel_path: str) -> List[Dict]:
    """Read a raw JSON file, apply cleaning rules, and return the Supabase formatted dicts."""
    try:
        with open(abs_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if not data:
            return []
            
        cleaned_records = []
        for item in data:
            mileage = clean_mileage(item.get('mileage'))
            end_price = clean_price(item.get('end_price'))
            displacement = clean_displacement(item.get('displacement'))
            
            year = None
            if item.get('year') and str(item.get('year')).isdigit():
                parsed_year = int(item['year'])
                if 1900 <= parsed_year <= 2030:
                    year = parsed_year
                    
            record = {
                "site_name": clean_string(item.get('site_name')),
                "lot_number": clean_string(item.get('lot_number')),
                "make": clean_string(item.get('make')),
                "model": clean_string(item.get('model')),
                "year": year,
                "grade": clean_string(item.get('grade')),
                "model_type": clean_string(item.get('model_type')),
                "mileage": mileage,
                "displacement": displacement,
                "transmission": clean_string(item.get('transmission')),
                "color": clean_color(item.get('color')),
                "auction": clean_string(item.get('auction')),
                "sale_date": clean_string(item.get('sale_date')),
                "end_price": end_price,
                "result": clean_string(item.get('result')),
                "scores": clean_string(item.get('scores')),
                "url": clean_string(item.get('url')),
                "file_source": rel_path
            }
            cleaned_records.append(record)
            
        return cleaned_records
    except Exception as e:
        print(f"Error reading {rel_path}: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description='Clean raw JSONs and push to Supabase API directly.')
    parser.add_argument('--batch-size', type=int, default=1000, 
                        help='Number of records to push per API request.')
    args = parser.parse_args()

    try:
        # Initialize API
        supabase = get_supabase_client()
        processed_cache = load_processed_files()
        
        # Scan hard drive for new untracked files
        print("Scanning for new data files...")
        unsynced_files = scan_for_unsynced_files(processed_cache)
        
        if not unsynced_files:
            print("No new files to process.")
            return
            
        print(f"Found {len(unsynced_files)} new files to clean and upload.")
        
        # We will hold all cleaned records spanning across multiple files in a single flat batch
        master_records_batch: List[Dict] = []
        files_in_current_batch: List[str] = []
        total_pushed = 0
        
        for i, (abs_path, rel_path) in enumerate(unsynced_files):
            file_records = clean_file_data(abs_path, rel_path)
            if file_records:
                for rec in file_records:
                    master_records_batch.append(rec)
                files_in_current_batch.append(rel_path)
            
            # Flush batch to Supabase when it reaches optimal size
            if len(master_records_batch) >= args.batch_size or i == len(unsynced_files) - 1:
                # Deduplicate the batch based on the unique index constraint to prevent 
                # PostgreSQL "ON CONFLICT DO UPDATE cannot affect row a second time" errors.
                seen = set()
                deduped_batch = []
                for r in master_records_batch:
                    # Create a deterministic unique key for the tuple
                    uid = (r.get('site_name'), r.get('lot_number'), r.get('sale_date'))
                    if uid not in seen:
                        seen.add(uid)
                        deduped_batch.append(r)
                
                # Overwrite master with the deduped version for processing
                master_records_batch = deduped_batch
                
                # Chunk it exactly if it slightly overshot
                while len(master_records_batch) > 0:
                    chunk = master_records_batch[:args.batch_size]
                    
                    try:
                        # Upload to Supabase PostgreSQL Database (Updates conflicts automatically based on the unique index)
                        supabase.table('japan_sales').upsert(
                            chunk, 
                            on_conflict='site_name,lot_number,sale_date'
                        ).execute()
                        total_pushed += len(chunk)
                    except Exception as e:
                        import traceback
                        print(f"Critical error pushing to Supabase:\n{e}\nTraceback:\n{traceback.format_exc()}")
                        # Safe stop: Don't mark these files as processed since they failed upload
                        return
                    
                    # Pop the successfully uploaded chunk
                    master_records_batch = master_records_batch[args.batch_size:]
                
                # Success! Only track the files as processed AFTER Supabase confirms upload
                processed_cache.update(files_in_current_batch)
                save_processed_files(processed_cache)
                files_in_current_batch = []
                
                print(f"Synchronized {total_pushed} records to Supabase...")
                
        print(f"Finished successfully. Total records inserted/updated: {total_pushed}")
                
    except ValueError as ve:
        print(ve)
    except Exception as e:
        print(f"Fatal script error: {e}")

if __name__ == '__main__':
    main()
