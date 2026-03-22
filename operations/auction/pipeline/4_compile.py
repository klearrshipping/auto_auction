#!/usr/bin/env python3
"""
Compile auction data from UID-keyed state into array format for consumers.

For each state file (e.g. Audi_A3_Zervtek.json), iterates completed entries
and outputs: lot_number, make, model, grade, color, mileage, score, auction, auction_time, image_urls

Output: {stem}_compiled.json in the same folder.

Usage:
  python 4_compile.py                    # process all
  python 4_compile.py --limit 2          # first 2 files
  python 4_compile.py --file path.json   # single file
"""

import argparse
import json
import re
import sys
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

_script_dir = Path(__file__).resolve().parent

JST = timezone(timedelta(hours=9))
AUCTION_TIME_FMT = "%Y-%m-%d %H:%M:%S"


def _is_auction_at_least_24h_ahead(auction_time_str) -> bool:
    """True if auction_time is at least 24 hours ahead of now (JST)."""
    if not auction_time_str or not isinstance(auction_time_str, str):
        return True
    s = str(auction_time_str).strip()
    if not s or len(s) < 10:
        return True
    try:
        dt = datetime.strptime(s[:19], AUCTION_TIME_FMT).replace(tzinfo=JST)
        now = datetime.now(timezone.utc).astimezone(JST)
        return dt >= now + timedelta(hours=24)
    except ValueError:
        return True
_root = _script_dir.parent.parent.parent
sys.path.insert(0, str(_root))

try:
    from config.manufacturer_config_JM import manufacturer_configs
except ImportError:
    manufacturer_configs = {}

AUCTION_DATA_ROOT = _root / "data" / "auction_data"


def _min_year_for_vehicle(make: str, model: str) -> int:
    """Minimum year allowed (Jamaica age limit). Default 6 years."""
    try:
        if make and model and make in manufacturer_configs and model in manufacturer_configs[make]:
            age_limit = manufacturer_configs[make][model].get("age_limit", 6)
        else:
            age_limit = 6
        return date.today().year - age_limit
    except Exception:
        return date.today().year - 6

OLD_DATE_RE = re.compile(r"_\d{4}-\d{2}-\d{2}\.json$")


def find_state_files(root: Path, limit: int = 0) -> list[Path]:
    """Find UID state files. Exclude _compiled.json and old date-based format."""
    files = [
        p for p in root.glob("**/*.json")
        if not p.name.endswith("_compiled.json")
        and not p.name.endswith("_details.json")
        and not OLD_DATE_RE.search(p.name)
    ]
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    if limit > 0:
        files = files[:limit]
    return files


def compile_record_from_state_entry(entry: dict, site_name: str) -> dict:
    """Build one compiled record from state entry (listing + details)."""
    listing = entry.get("listing") or {}
    details = entry.get("details") or {}
    ld_details = details.get("details") or {}

    score_val = listing.get("scores") or ld_details.get("scores")
    scores_json = {"score": str(score_val)} if score_val is not None else None

    year_val = listing.get("year")
    if year_val is not None and isinstance(year_val, (int, str)):
        try:
            y = int(year_val)
            year_val = y if 1900 <= y <= 2030 else None
        except (ValueError, TypeError):
            year_val = None
    else:
        year_val = None

    return {
        "site_name": site_name,
        "lot_number": listing.get("lot_number") or details.get("lot_number"),
        "make": listing.get("make"),
        "model": listing.get("model"),
        "year": year_val,
        "grade": listing.get("grade"),
        "model_type": listing.get("model_type"),
        "color": listing.get("color") or ld_details.get("color"),
        "mileage": listing.get("mileage") or ld_details.get("mileage"),
        "score": score_val,
        "scores": scores_json,
        "start_price": listing.get("start_price"),
        "end_price": listing.get("end_price"),
        "result": listing.get("result"),
        "lot_link": listing.get("lot_link") or listing.get("url"),
        "auction": listing.get("auction"),
        "search_date": listing.get("search_date"),
        "auction_time": ld_details.get("auction_time"),
        "image_urls": details.get("image_urls") or [],
    }


def process_file(state_path: Path) -> bool:
    """Compile state to array. Returns True if saved."""
    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)

    if not isinstance(state, dict):
        print(f"  Skipping: not a state object")
        return False

    listings = state.get("listings") or {}
    completed = [
        entry for entry in listings.values()
        if isinstance(entry, dict) and entry.get("status") == "completed"
    ]

    site_name = state.get("site_name") or ""
    compiled = []
    for e in completed:
        rec = compile_record_from_state_entry(e, site_name)
        auction_time = rec.get("auction_time")
        if not _is_auction_at_least_24h_ahead(auction_time):
            continue
        # Enforce age limit: exclude vehicles older than Jamaica import rules allow
        year_val = rec.get("year")
        if year_val is not None:
            min_year = _min_year_for_vehicle(rec.get("make") or "", rec.get("model") or "")
            if year_val < min_year:
                continue
        compiled.append(rec)

    out_path = state_path.parent / f"{state_path.stem}_compiled.json"
    if not compiled:
        print(f"  No entries with auction >= 24h ahead; writing empty compiled file")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        return True

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(compiled, f, ensure_ascii=False, indent=2)
    print(f"  Saved {len(compiled)} records to {out_path.name}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Compile state into structured auction data.")
    parser.add_argument("--limit", type=int, default=0, help="Max files to process (0 = all)")
    parser.add_argument("--file", type=str, help="Process single file path")
    args = parser.parse_args()

    if args.file:
        files = [Path(args.file).resolve()]
        if not files[0].is_file():
            print(f"File not found: {args.file}")
            return
    else:
        if not AUCTION_DATA_ROOT.is_dir():
            print(f"Directory not found: {AUCTION_DATA_ROOT}")
            return
        files = find_state_files(AUCTION_DATA_ROOT, args.limit)

    if not files:
        print("No state files found.")
        return

    print(f"Processing {len(files)} file(s):")
    for fp in files:
        try:
            rel = fp.relative_to(_root)
        except ValueError:
            rel = fp
        print(f"\n{rel}")
        process_file(fp)
    print("\nDone.")


if __name__ == "__main__":
    import traceback
    try:
        main()
    except Exception as e:
        print(f"\nCompile failed: {e}\n{traceback.format_exc()}", file=sys.stderr)
        sys.exit(1)
