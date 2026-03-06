#!/usr/bin/env python3
"""
Remove expired lots (auction time passed) from UID-keyed state files.
Uses current Japanese time (JST) to determine which lots have gone.
Deletes corresponding _compiled.json for regenerated output.
"""

import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_root = _script_dir.parent.parent.parent

sys.path.insert(0, str(_root))

AUCTION_DATA_ROOT = _root / "data" / "auction_data"
AUCTION_TIME_FMT = "%Y-%m-%d %H:%M:%S"
JST = timezone(timedelta(hours=9))
OLD_DATE_RE = re.compile(r"_\d{4}-\d{2}-\d{2}\.json$")


def now_jst() -> datetime:
    """Current time in Japan (JST)."""
    return datetime.now(timezone.utc).astimezone(JST)


def parse_auction_time(s: str | None) -> datetime | None:
    """Parse auction_time string as JST. Returns None if invalid/missing."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    try:
        dt = datetime.strptime(s, AUCTION_TIME_FMT)
        return dt.replace(tzinfo=JST)
    except ValueError:
        return None


def is_expired(auction_time_str: str | None, now: datetime) -> bool:
    """True if auction_time has passed. Treats missing/invalid as not expired (keep)."""
    parsed = parse_auction_time(auction_time_str)
    if parsed is None:
        return False
    return parsed <= now


def find_state_files(root: Path) -> list[Path]:
    """Find UID state files. Exclude _compiled.json and old date-based format."""
    return [
        p for p in root.glob("**/*.json")
        if not p.name.endswith("_compiled.json")
        and not p.name.endswith("_details.json")
        and not OLD_DATE_RE.search(p.name)
    ]


def prune_state_file(state_path: Path, now: datetime, dry_run: bool = False) -> tuple[int, int]:
    """Remove expired entries from state. Returns (removed_count, remaining_count)."""
    with open(state_path, "r", encoding="utf-8") as f:
        state = json.load(f)

    if not isinstance(state, dict):
        return (0, 0)

    listings = state.get("listings") or {}
    if not isinstance(listings, dict):
        return (0, 0)

    to_remove = []
    for uid, entry in listings.items():
        if not isinstance(entry, dict):
            continue
        details = entry.get("details") or {}
        details_inner = details.get("details") or {}
        auction_time = details_inner.get("auction_time")
        if is_expired(auction_time, now):
            to_remove.append(uid)

    if not to_remove:
        return (0, len(listings))

    for uid in to_remove:
        del listings[uid]

    if not dry_run:
        state["last_updated"] = datetime.now(timezone.utc).isoformat()
        tmp_path = state_path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        tmp_path.replace(state_path)

    return (len(to_remove), len(listings))


def main(dry_run: bool = False):
    now = now_jst()
    print(f"Removing expired lots (JST: {now.strftime(AUCTION_TIME_FMT)})")
    print(f"Mode: {'DRY-RUN (no writes)' if dry_run else 'LIVE'}")
    print()

    total_removed = 0
    pruned_paths = []

    for state_path in sorted(find_state_files(AUCTION_DATA_ROOT)):
        try:
            removed, remaining = prune_state_file(state_path, now, dry_run=dry_run)
            if removed > 0:
                rel = state_path.relative_to(AUCTION_DATA_ROOT)
                print(f"  {rel}: removed {removed}, kept {remaining}")
                total_removed += removed
                pruned_paths.append(state_path)
        except Exception as e:
            print(f"  ERROR {state_path}: {e}")

    if pruned_paths:
        print(f"\nRemoved {total_removed} expired lot(s) from {len(pruned_paths)} file(s)")
        if not dry_run:
            for state_path in pruned_paths:
                compiled_path = state_path.parent / f"{state_path.stem}_compiled.json"
                if compiled_path.is_file():
                    compiled_path.unlink()
                    print(f"  Deleted {compiled_path.relative_to(AUCTION_DATA_ROOT)} (will be regenerated)")
    else:
        print("No expired lots to remove.")

    return total_removed


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Remove expired lots from state JSON.")
    parser.add_argument("--dry-run", action="store_true", help="Report only, do not modify files")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
