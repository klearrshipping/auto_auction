"""
Sync compiled auction JSON files to Supabase auction_data.vehicles.

Scans data/auction_data for *_compiled.json files, cleans records, and upserts
to auction_data.vehicles. Tracks processed files to avoid re-uploading.

Usage:
  python cloud_sync.py
  python cloud_sync.py --batch-size 500
  python cloud_sync.py --full        # re-sync all files (ignore processed cache)
  python cloud_sync.py --truncate    # delete all vehicles before sync (clean slate)
  python cloud_sync.py --full --truncate   # full replace: wipe DB, then sync all
"""

import os
import json
import re
import argparse
from typing import List, Dict, Tuple, Set
from supabase import create_client, Client, ClientOptions

# --- Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "auction_data")
TRACKER_PATH = os.path.join(SCRIPT_DIR, "processed_files.json")


def load_processed_files() -> Set[str]:
    """Load the set of relative file paths already pushed to Supabase."""
    if os.path.exists(TRACKER_PATH):
        try:
            with open(TRACKER_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data.get("processed", []))
        except json.JSONDecodeError:
            print("Warning: corrupted tracker file. Re-syncing all files.")
            return set()
    return set()


def save_processed_files(processed_set: Set[str]) -> None:
    """Save the set of processed files to JSON."""
    with open(TRACKER_PATH, "w", encoding="utf-8") as f:
        json.dump({"processed": list(processed_set)}, f, indent=2)


def clean_string(val) -> str:
    if not val:
        return ""
    return str(val).strip()


def clean_mileage(mileage_val):
    """Normalize mileage to int or None."""
    if mileage_val is None:
        return None
    if isinstance(mileage_val, int):
        return mileage_val if mileage_val >= 0 else None
    digits = re.sub(r"[^\d]", "", str(mileage_val))
    return int(digits) if digits else None


def clean_price(price_val):
    """Normalize price to int (yen) or None."""
    if price_val is None:
        return None
    if isinstance(price_val, int):
        return price_val if price_val >= 0 else None
    digits = re.sub(r"[^\d]", "", str(price_val))
    return int(digits) if digits else None


def get_supabase_client() -> Client:
    """Initialize Supabase client with auction_data schema."""
    from dotenv import load_dotenv

    for env_path in [
        os.path.join(SCRIPT_DIR, ".env"),
        os.path.join(os.path.dirname(SCRIPT_DIR), "aggregate_sales", ".env"),
        os.path.join(PROJECT_ROOT, ".env"),
    ]:
        if os.path.exists(env_path):
            load_dotenv(dotenv_path=env_path)
            break

    url = str(os.environ.get("SUPABASE_URL", ""))
    key = str(os.environ.get("SUPABASE_KEY", ""))

    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key, options=ClientOptions(schema="auction_data"))


def derive_site_from_path(rel_path: str) -> str:
    """Derive site_name from path when missing in record. e.g. Honda_Vezel_Zen_Autoworks_compiled.json -> Zen Autoworks."""
    basename = os.path.basename(rel_path)
    if not basename.endswith("_compiled.json"):
        return ""
    stem = basename[:- len("_compiled.json")]
    parts = stem.split("_")
    if len(parts) >= 3:
        site_parts = parts[2:]
        return " ".join(site_parts)
    return stem


def scan_for_compiled_files(processed_cache: Set[str], full_sync: bool) -> List[Tuple[str, str]]:
    """Scan auction_data for *_compiled.json files. Returns (abs_path, rel_path)."""
    files = []
    for root, _, filenames in os.walk(DATA_DIR):
        for f in filenames:
            if f.endswith("_compiled.json"):
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, PROJECT_ROOT)
                if full_sync or rel_path not in processed_cache:
                    files.append((abs_path, rel_path))
    return sorted(files)


def clean_record(item: Dict, rel_path: str) -> Dict | None:
    """Convert compiled record to Supabase vehicles row."""
    site_name = clean_string(item.get("site_name")) or derive_site_from_path(rel_path)
    lot_number = clean_string(item.get("lot_number"))
    if not site_name or not lot_number:
        return None

    year = item.get("year")
    if year is not None:
        try:
            y = int(year)
            year = y if 1900 <= y <= 2030 else None
        except (ValueError, TypeError):
            year = None

    return {
        "site_name": site_name,
        "lot_number": lot_number,
        "make": clean_string(item.get("make")),
        "model": clean_string(item.get("model")),
        "year": year,
        "mileage": clean_mileage(item.get("mileage")),
        "start_price": clean_price(item.get("start_price")),
        "end_price": clean_price(item.get("end_price")),
        "grade": clean_string(item.get("grade")),
        "model_type": clean_string(item.get("model_type")),
        "color": clean_string(item.get("color")),
        "result": clean_string(item.get("result")),
        "scores": item.get("scores") if isinstance(item.get("scores"), dict) else None,
        "lot_link": clean_string(item.get("lot_link")),
        "auction": clean_string(item.get("auction")),
        "search_date": clean_string(item.get("search_date")),
        "auction_time": clean_string(item.get("auction_time")),
        "image_urls": item.get("image_urls") if isinstance(item.get("image_urls"), list) else None,
    }


def clean_file_data(abs_path: str, rel_path: str) -> List[Dict]:
    """Read compiled JSON and return cleaned records for Supabase."""
    try:
        with open(abs_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {rel_path}: {e}")
        return []

    if not isinstance(data, list):
        return []

    records = []
    for item in data:
        rec = clean_record(item, rel_path)
        if rec:
            records.append(rec)
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync compiled auction JSON to Supabase auction_data.vehicles")
    parser.add_argument("--batch-size", type=int, default=1000, help="Records per upsert batch")
    parser.add_argument("--full", action="store_true", help="Re-sync all files (ignore processed cache)")
    parser.add_argument("--truncate", action="store_true", help="Delete all vehicles before sync (clean slate)")
    args = parser.parse_args()

    supabase = get_supabase_client()

    if args.truncate:
        print("Truncating auction_data.vehicles...")
        try:
            supabase.table("vehicles").delete().gte("id", 0).execute()
            print("Truncated.")
        except Exception as e:
            import traceback
            print(f"Truncate failed: {e}\n{traceback.format_exc()}")
            return
        processed_cache = set()
    else:
        processed_cache = load_processed_files() if not args.full else set()

    print("Scanning for compiled auction files...")
    files = scan_for_compiled_files(processed_cache, args.full)

    if not files:
        print("No files to sync.")
        return

    print(f"Found {len(files)} file(s) to process.")
    total_pushed = 0
    master_batch: List[Dict] = []
    files_in_batch: List[str] = []

    for i, (abs_path, rel_path) in enumerate(files):
        records = clean_file_data(abs_path, rel_path)
        if records:
            for rec in records:
                master_batch.append(rec)
            files_in_batch.append(rel_path)

        if len(master_batch) >= args.batch_size or i == len(files) - 1:
            # Deduplicate by (site_name, lot_number)
            seen = set()
            deduped = []
            for r in master_batch:
                key = (r.get("site_name"), r.get("lot_number"))
                if key not in seen:
                    seen.add(key)
                    deduped.append(r)

            while deduped:
                chunk = deduped[: args.batch_size]
                try:
                    supabase.table("vehicles").upsert(chunk, on_conflict="site_name,lot_number").execute()
                    total_pushed += len(chunk)
                except Exception as e:
                    import traceback
                    print(f"Error pushing to Supabase:\n{e}\n{traceback.format_exc()}")
                    return

                deduped = deduped[args.batch_size :]

            processed_cache.update(files_in_batch)
            save_processed_files(processed_cache)
            files_in_batch = []
            master_batch = []
            print(f"Synced {total_pushed} records so far...")

    print(f"Done. Total records upserted: {total_pushed}")


if __name__ == "__main__":
    main()
