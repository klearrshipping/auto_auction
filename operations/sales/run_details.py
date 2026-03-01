#!/usr/bin/env python3
"""
Run sales details extraction for all pending lot_urls files.
Scans data/sales_data/ for *_lot_urls.json files, logs in once, fetches lot details
in small batches with early termination, merges into the listing JSON,
then renames _lot_urls.json to _lot_urls_done.json.

Usage:
  python -u run_sales_details.py              (process all pending lot_urls files)
  python -u run_sales_details.py --reprocess  (also re-process _lot_urls_done.json files)
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(os.path.dirname(_script_dir))
sys.path.insert(0, _root)
SALES_DATA_ROOT = Path(_root) / "data" / "sales_data"

from playwright.async_api import async_playwright
from get_market_data.Japan.auction_site_config_JP import auction_sites
from get_market_data.Japan.sales_data.get_sales_details import (
    fetch_all_lot_details_from_lot_urls_file,
    detail_has_data,
    ExtractionAborted,
)
from config.config import runtime_settings, browser_settings


def log(msg):
    print(msg, flush=True)


def find_lot_url_files(root: Path, include_done: bool = False) -> list:
    """Find all *_lot_urls.json (and optionally *_lot_urls_done.json) under root."""
    patterns = ["**/*_lot_urls.json"]
    if include_done:
        patterns.append("**/*_lot_urls_done.json")
    files = []
    for pat in patterns:
        files.extend(root.glob(pat))
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


async def process_lot_urls_file(context, lot_urls_path: Path, settings: dict) -> bool:
    """Process one lot_urls file: load listing, fetch details, save, rename. Returns True on success."""
    with open(lot_urls_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    entries = data.get("entries", [])
    listing_name = data.get("listing_file", "")
    listing_path = lot_urls_path.parent / listing_name

    if not listing_path.is_file():
        log(f"  Listing not found: {listing_path.name}, skipping.")
        return False
    if not entries:
        log(f"  No entries in {lot_urls_path.name}, skipping.")
        return False

    with open(listing_path, "r", encoding="utf-8") as f:
        results = json.load(f)
    log(f"  {len(entries)} URLs, listing has {len(results)} records.")

    aborted = False
    try:
        results, meaningful = await fetch_all_lot_details_from_lot_urls_file(
            context, results, entries,
            max_concurrent=settings.get("concurrent_lot_details", 2),
            batch_delay=settings.get("detail_batch_delay", 3),
            max_consecutive_empty=settings.get("max_consecutive_empty", 5),
            early_check_after=settings.get("early_check_after", 3),
        )
    except ExtractionAborted as e:
        log(f"  EXTRACTION ABORTED: {e}")
        meaningful = sum(1 for r in results if detail_has_data(r.get("lot_details")))
        aborted = True

    total_with_details = sum(1 for r in results if detail_has_data(r.get("lot_details")))
    log(f"  Result: {meaningful} meaningful this run, {total_with_details} total with details.")

    if total_with_details > 0:
        with open(listing_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        log(f"  Saved partial results to {listing_path.name}")

    if aborted:
        log(f"  NOT marking as done (aborted). File stays as {lot_urls_path.name} for retry.")
        return False

    done_path = lot_urls_path.with_name(
        lot_urls_path.name.replace("_lot_urls.json", "_lot_urls_done.json")
    )
    lot_urls_path.rename(done_path)
    log(f"  Renamed to {done_path.name}")
    return True


async def main():
    parser = argparse.ArgumentParser(description="Fetch lot details for all pending lot_urls files.")
    parser.add_argument("--reprocess", action="store_true", help="Also re-process _lot_urls_done.json files.")
    args = parser.parse_args()

    files = find_lot_url_files(SALES_DATA_ROOT, include_done=args.reprocess)
    if not files:
        log("No lot_urls files found to process.")
        return

    log(f"Found {len(files)} lot_urls file(s) to process:")
    for f in files:
        log(f"  {f.relative_to(SALES_DATA_ROOT)}")

    site_name = list(auction_sites.keys())[0] if auction_sites else None
    if not site_name:
        log("No auction sites configured.")
        return
    site = auction_sites[site_name]
    settings = runtime_settings

    log(f"\nSettings: concurrency={settings.get('concurrent_lot_details', 2)}, "
        f"batch_delay={settings.get('detail_batch_delay', 3)}s, "
        f"abort after {settings.get('max_consecutive_empty', 5)} consecutive empty, "
        f"early check after {settings.get('early_check_after', 3)} lots.\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=browser_settings.get("headless", False))
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        log(f"Logging into {site_name}...")
        await page.goto(site["scraping"]["auction_url"])
        await page.fill("#usr_name", site.get("username", ""))
        await page.fill("#usr_pwd", site.get("password", ""))
        await page.click('input[name="Submit"][value="Sign in"]')
        await page.wait_for_load_state("networkidle")
        log("Logged in.\n")

        should_stop = False
        for lot_urls_path in files:
            if should_stop:
                log(f"Skipping {lot_urls_path.relative_to(SALES_DATA_ROOT)} (previous file aborted).")
                continue
            log(f"Processing: {lot_urls_path.relative_to(SALES_DATA_ROOT)}")
            try:
                ok = await process_lot_urls_file(context, lot_urls_path, settings)
                if not ok:
                    should_stop = True
                    log("  Stopping all further processing to protect the account.")
            except Exception as e:
                log(f"  ERROR: {e}")
                should_stop = True
            log("")

        await browser.close()

    log("Done.")


if __name__ == "__main__":
    asyncio.run(main())
