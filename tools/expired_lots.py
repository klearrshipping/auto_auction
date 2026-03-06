#!/usr/bin/env python3
"""
Scan compiled JSON files and identify which lots have gone (auction time passed)
based on current Japanese time (JST = UTC+9).
"""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

AUCTION_DATA_ROOT = Path(__file__).resolve().parent.parent / "data" / "auction_data"
AUCTION_TIME_FMT = "%Y-%m-%d %H:%M:%S"
JST = timezone(timedelta(hours=9))


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


def main():
    now = now_jst()
    print(f"Current Japanese time (JST): {now.strftime(AUCTION_TIME_FMT)}")
    print()

    compiled_files = list(AUCTION_DATA_ROOT.glob("**/*_compiled.json"))
    gone = []
    upcoming = []
    no_time = []

    for path in sorted(compiled_files):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  Error reading {path}: {e}")
            continue

        if not isinstance(data, list):
            continue

        for lot in data:
            if not isinstance(lot, dict):
                continue
            lot_num = lot.get("lot_number", "?")
            auction = lot.get("auction", "?")
            make = lot.get("make", "")
            model = lot.get("model", "")
            auction_time_str = lot.get("auction_time")
            parsed = parse_auction_time(auction_time_str)

            if parsed is None:
                no_time.append({
                    "file": str(path.relative_to(AUCTION_DATA_ROOT)),
                    "lot_number": lot_num,
                    "auction": auction,
                    "make": make,
                    "model": model,
                })
            elif parsed <= now:
                gone.append({
                    "file": str(path.relative_to(AUCTION_DATA_ROOT)),
                    "lot_number": lot_num,
                    "auction": auction,
                    "auction_time": auction_time_str,
                    "make": make,
                    "model": model,
                })
            else:
                upcoming.append({
                    "file": str(path.relative_to(AUCTION_DATA_ROOT)),
                    "lot_number": lot_num,
                    "auction": auction,
                    "auction_time": auction_time_str,
                    "make": make,
                    "model": model,
                })

    print("=" * 70)
    print("LOTS THAT HAVE GONE (auction time passed)")
    print("=" * 70)
    if gone:
        for g in sorted(gone, key=lambda x: (x["auction_time"] or "", x["auction"], x["lot_number"])):
            print(f"  Lot {g['lot_number']} | {g['make']} {g['model']} | {g['auction']} | {g['auction_time']}")
        print(f"\n  Total: {len(gone)} lot(s)")
    else:
        print("  (none)")

    print()
    print("=" * 70)
    print("LOTS WITHOUT VALID AUCTION TIME")
    print("=" * 70)
    if no_time:
        for n in no_time[:20]:  # limit output
            print(f"  Lot {n['lot_number']} | {n['make']} {n['model']} | {n['auction']} | {n['file']}")
        if len(no_time) > 20:
            print(f"  ... and {len(no_time) - 20} more")
        print(f"\n  Total: {len(no_time)} lot(s)")
    else:
        print("  (none)")

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Gone (passed):     {len(gone)}")
    print(f"  Upcoming:          {len(upcoming)}")
    print(f"  No auction time:   {len(no_time)}")
    print(f"  Total lots:        {len(gone) + len(upcoming) + len(no_time)}")


if __name__ == "__main__":
    main()
