#!/usr/bin/env python3
"""
Run auction details extraction for UID-keyed state files in data/auction_data.
Fetches details only for entries with status == "pending"; merges into state.

Usage:
  python 3_extract_details.py                    # process all state files with pending
  python 3_extract_details.py --limit 2           # process first 2 files
  python 3_extract_details.py --file path.json    # process single file
"""

import argparse
import asyncio
import json
import re
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_root = _script_dir.parent.parent.parent
sys.path.insert(0, str(_root))

AUCTION_DATA_ROOT = _root / "data" / "auction_data"


def log(msg):
    print(msg, flush=True)


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


def get_site_from_state(state: dict) -> str | None:
    """Get site_name from state."""
    if not isinstance(state, dict):
        return None
    sn = (state.get("site_name") or "").strip()
    if sn:
        return sn
    listings = state.get("listings") or {}
    for entry in listings.values():
        if isinstance(entry, dict):
            listing = entry.get("listing") or {}
            sn = (listing.get("site_name") or "").strip()
            if sn:
                return sn
    return None


def count_pending(state: dict) -> int:
    """Count entries with status == pending."""
    if not isinstance(state, dict):
        return 0
    listings = state.get("listings") or {}
    return sum(1 for e in listings.values() if isinstance(e, dict) and e.get("status") == "pending")


async def main():
    from playwright.async_api import async_playwright
    from get_market_data.Japan.auction_site_config_JP import auction_sites
    from get_market_data.Japan.auction_data.get_details import (
        fetch_pending_details,
        ExtractionAborted,
    )
    from config.config import runtime_settings, browser_settings

    parser = argparse.ArgumentParser(description="Fetch lot details for pending entries in state files.")
    parser.add_argument("--limit", type=int, default=0, help="Max files to process (0 = all)")
    parser.add_argument("--file", type=str, help="Process single file path")
    args = parser.parse_args()

    if args.file:
        files = [Path(args.file).resolve()]
        if not files[0].is_file():
            log(f"File not found: {args.file}")
            return
    else:
        if not AUCTION_DATA_ROOT.is_dir():
            log(f"Directory not found: {AUCTION_DATA_ROOT}")
            return
        files = find_state_files(AUCTION_DATA_ROOT, args.limit)
        files = [f for f in files if count_pending(json.loads(f.read_text(encoding="utf-8"))) > 0]

    if not files:
        log("No state files with pending entries found.")
        return

    log(f"Found {len(files)} file(s) with pending:")
    for f in files:
        try:
            log(f"  {f.relative_to(_root)}")
        except ValueError:
            log(f"  {f}")

    settings = runtime_settings
    log(f"\nSettings: concurrency={settings.get('concurrent_lot_details', 2)}, "
        f"batch_delay={settings.get('detail_batch_delay', 3)}s\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=browser_settings.get("headless", False))
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        last_site = None
        should_stop = False

        for file_path in files:
            if should_stop:
                log(f"Skipping {file_path.name} (previous aborted).")
                continue

            with open(file_path, "r", encoding="utf-8") as f:
                state = json.load(f)

            site_name = get_site_from_state(state)
            if not site_name or site_name not in auction_sites:
                log(f"Skipping {file_path.name}: could not determine site")
                continue

            pending = count_pending(state)
            if pending == 0:
                log(f"Skipping {file_path.name}: no pending")
                continue

            if site_name != last_site:
                if site_name == "Zen Autoworks" and last_site is not None:
                    log("Zen Autoworks: creating fresh browser context...")
                    await context.close()
                    context = await browser.new_context(
                        viewport={"width": 1920, "height": 1080},
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    )
                    page = await context.new_page()

                log(f"\nLogging into {site_name}...")
                site = auction_sites[site_name]
                login_timeout = 60000
                await page.goto(site["scraping"]["auction_url"], timeout=login_timeout)
                await page.fill("#usr_name", site.get("username", ""), timeout=login_timeout)
                await page.fill("#usr_pwd", site.get("password", ""), timeout=login_timeout)
                await page.click('input[name="Submit"][value="Sign in"]', timeout=login_timeout)
                await page.wait_for_load_state("networkidle", timeout=login_timeout)
                log("Logged in.")
                last_site = site_name

            log(f"\nProcessing: {file_path.relative_to(AUCTION_DATA_ROOT)} ({pending} pending)")
            base_url = auction_sites[site_name]["scraping"].get("url") or auction_sites[site_name]["scraping"].get("auction_url", "")

            try:
                state, meaningful = await fetch_pending_details(
                    context, state, base_url,
                    max_concurrent=settings.get("concurrent_lot_details", 2),
                    batch_delay=settings.get("detail_batch_delay", 3),
                    max_consecutive_empty=settings.get("max_consecutive_empty", 5),
                )
                log(f"  Result: {meaningful} meaningful this run")

                tmp_path = file_path.with_suffix(".json.tmp")
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(state, f, ensure_ascii=False, indent=2)
                tmp_path.replace(file_path)
                log(f"  Saved to {file_path.name}")
            except ExtractionAborted as e:
                log(f"  ABORTED: {e}")
                should_stop = True
            except Exception as e:
                import traceback
                log(f"  ERROR: {e}")
                log(traceback.format_exc())
                should_stop = True

        await browser.close()

    log("\nDone.")


if __name__ == "__main__":
    import traceback
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\nDetails extraction failed: {e}\n{traceback.format_exc()}", file=sys.stderr)
        sys.exit(1)
