#!/usr/bin/env python3
"""
Migrate old date-based listing + details files to new UID-keyed state format.

Old: Audi_A3_Zervtek_2026-03-03.json + Audi_A3_Zervtek_2026-03-03_details.json
New: Audi_A3_Zervtek.json (single keyed state)

Handles multiple date files for same make/model/site by merging (later wins for last_seen).

Usage:
  python migrate_to_uid_state.py --dry-run
  python migrate_to_uid_state.py --backup
  python migrate_to_uid_state.py --delete-old
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_root = _script_dir.parent.parent.parent
sys.path.insert(0, str(_root))

AUCTION_DATA_ROOT = _root / "data" / "auction_data"

from operations.auction.utils.uid_utils import listing_uid

DATE_SUFFIX_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})$")


def is_old_format(path: Path) -> bool:
    """True if filename has date suffix (old format)."""
    stem = path.stem
    return bool(DATE_SUFFIX_RE.search(stem))


def parse_stem(path: Path) -> tuple[str, str, str, str | None]:
    """Extract make, model, site, date from path. Uses path for make/model."""
    make = path.parent.parent.parent.name
    model = path.parent.parent.name
    stem = path.stem
    prefix = f"{make}_{model}_"
    if not stem.startswith(prefix):
        return (make, model, "", None)
    rest = stem[len(prefix) :]
    m = DATE_SUFFIX_RE.search(rest)
    if m:
        site = rest[: -len(m.group(0))].rstrip("_")
        date = m.group(1)
        return (make, model, site, date)
    return (make, model, rest, None)


def new_stem(make: str, model: str, site: str) -> str:
    return f"{make}_{model}_{site}"


def listing_to_record(listing: dict, site_name: str) -> dict:
    """Build listing dict for state entry (normalize keys)."""
    return {
        "lot_number": listing.get("lot_number"),
        "make": listing.get("make"),
        "model": listing.get("model"),
        "year": listing.get("year"),
        "grade": listing.get("grade"),
        "color": listing.get("color"),
        "mileage": listing.get("mileage"),
        "auction": listing.get("auction"),
        "lot_link": listing.get("lot_link") or listing.get("url"),
        "site_name": listing.get("site_name") or site_name,
    }


def migrate_file(
    listing_path: Path,
    new_path: Path,
    existing_state: dict | None,
    dry_run: bool,
    backup: bool,
) -> bool:
    """Migrate one listing file (and its details if present) into state."""
    with open(listing_path, "r", encoding="utf-8") as f:
        listings = json.load(f)

    if not isinstance(listings, list):
        print(f"  Skipping: not a list")
        return False

    make, model, site, date_from_stem = parse_stem(listing_path)
    if not site:
        print(f"  Skipping: could not parse site from stem")
        return False

    site_name = site.replace("_", " ")

    details_path = listing_path.parent / f"{listing_path.stem}_details.json"
    details_list = []
    if details_path.is_file():
        with open(details_path, "r", encoding="utf-8") as f:
            details_list = json.load(f)
    details_by_idx = {d["record_index"]: d for d in details_list if isinstance(d, dict) and "record_index" in d}

    state = existing_state or {
        "schema_version": 1,
        "make": make,
        "model": model,
        "site_name": site_name,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "listings": {},
    }

    last_seen = date_from_stem or datetime.now().strftime("%Y-%m-%d")
    added = 0

    for i, listing in enumerate(listings):
        if not isinstance(listing, dict):
            continue
        lot_link = listing.get("lot_link") or listing.get("url")
        site_name_val = listing.get("site_name") or site_name
        uid = listing_uid(
            site_name_val,
            lot_link,
            fallback_lot_number=str(listing.get("lot_number", "")),
            fallback_auction=str(listing.get("auction", "")),
        )

        details_entry = details_by_idx.get(i)
        details_data = details_entry.get("lot_details") if details_entry else None
        status = "completed" if details_data else "pending"

        entry = state["listings"].get(uid)
        if entry and entry.get("status") == "completed" and status == "pending":
            continue
        if entry and status == "completed":
            entry["status"] = "completed"
            entry["last_seen"] = last_seen
            entry["details"] = details_data or {}
        else:
            state["listings"][uid] = {
                "status": status,
                "last_seen": last_seen,
                "listing": listing_to_record(listing, site_name_val),
                "details": details_data if details_data else {},
            }
            added += 1

    if dry_run:
        print(f"  Would write {len(state['listings'])} entries to {new_path.name}")
        return True

    new_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = new_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp_path.replace(new_path)

    if backup:
        if listing_path.exists():
            listing_path.rename(listing_path.with_suffix(".json.bak"))
        if details_path.exists():
            details_path.rename(details_path.with_suffix(".json.bak"))

    return True


def main():
    parser = argparse.ArgumentParser(description="Migrate to UID-keyed state format.")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no writes")
    parser.add_argument("--backup", action="store_true", help="Rename old files to .json.bak")
    parser.add_argument("--delete-old", action="store_true", help="Delete old files after migration (use with --backup for safety)")
    args = parser.parse_args()

    print("Migrating to UID state format...")
    print(f"  Root: {AUCTION_DATA_ROOT}")
    print(f"  Dry-run: {args.dry_run}")
    print()

    listing_files = [
        p for p in AUCTION_DATA_ROOT.glob("**/*.json")
        if not p.name.endswith("_details.json")
        and not p.name.endswith("_compiled.json")
        and is_old_format(p)
    ]

    if not listing_files:
        print("No old-format listing files found.")
        return

    by_new_path = {}
    for p in listing_files:
        make, model, site, _ = parse_stem(p)
        new_stem_val = new_stem(make, model, site)
        new_path = p.parent / f"{new_stem_val}.json"
        if new_path not in by_new_path:
            by_new_path[new_path] = []
        by_new_path[new_path].append(p)

    for new_path in sorted(by_new_path.keys()):
        sources = sorted(by_new_path[new_path], key=lambda x: x.stem)
        state = None
        for listing_path in sources:
            rel = listing_path.relative_to(AUCTION_DATA_ROOT)
            print(f"  {rel}")
            ok = migrate_file(listing_path, new_path, state, args.dry_run, args.backup)
            if not ok:
                continue
            if not args.dry_run and new_path.is_file():
                with open(new_path, "r", encoding="utf-8") as f:
                    state = json.load(f)

        if args.delete_old and not args.dry_run:
            for listing_path in sources:
                details_path = listing_path.parent / f"{listing_path.stem}_details.json"
                if listing_path.exists():
                    listing_path.unlink()
                if details_path.exists():
                    details_path.unlink()

    print("\nDone.")


if __name__ == "__main__":
    main()
