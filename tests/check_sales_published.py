#!/usr/bin/env python3
"""
Check when sales data are published for a given auction date.

Logs into one auction site, searches for Honda Vezel (March 6, 2026, Sold),
and checks if any results appear on the page. Runs every hour until results
are displayed, then exits. Use this to determine when sales data go live.

Usage:
  python tests/check_sales_published.py
  python tests/check_sales_published.py --site Zervtek
  python tests/check_sales_published.py --interval 30   # check every 30 min
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime

_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_script_dir)
sys.path.insert(0, _root)

from playwright.async_api import async_playwright
from get_market_data.Japan.auction_site_config_JP import auction_sites
from get_market_data.Japan.sales_data.get_sales_results import (
    run_search_from_config,
    RESULT_VALUE_SOLD,
)
from config.config import browser_settings


AUCTION_DATE = "2026-03-06"  # Date to check for published sales data
MAKER = "HONDA"
MODEL = "VEZEL"
DEFAULT_INTERVAL_MINUTES = 60


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


async def check_has_results(page) -> tuple[bool, int]:
    """
    Check if #mainTable has any data rows. Returns (has_results, row_count).
    """
    try:
        await page.wait_for_selector("#mainTable", timeout=20000)
    except Exception:
        return (False, 0)

    table_debug = await page.evaluate("""
        () => {
            const table = document.getElementById('mainTable');
            if (!table) return { error: 'No mainTable found' };
            const cellRows = table.querySelectorAll('tr[id^="cell_"]');
            return {
                cellRows: cellRows.length,
                tableExists: true
            };
        }
    """)

    if table_debug.get("error"):
        return (False, 0)

    count = table_debug.get("cellRows", 0)
    return (count > 0, count)


async def run_check(site_name: str, site: dict) -> tuple[bool, int]:
    """Login, search Honda Vezel March 6 2026, check for results. Returns (has_results, row_count)."""
    sales_url = site["scraping"].get("sales_data_url")
    if not sales_url:
        log(f"[{site_name}] No sales_data_url, skipping.")
        return (False, 0)

    search_config = {
        "maker": MAKER,
        "model": MODEL,
        "year_from": "2020",
        "year_to": "2026",
        "auction_since_date": AUCTION_DATE,
        "auction_till_date": AUCTION_DATE,
        "result_value": RESULT_VALUE_SOLD,
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=browser_settings.get("headless", True))
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        try:
            log(f"[{site_name}] Logging in...")
            await page.goto(site["scraping"]["auction_url"], timeout=60000)
            await page.fill("#usr_name", site.get("username", ""))
            await page.fill("#usr_pwd", site.get("password", ""))
            await page.click('input[name="Submit"][value="Sign in"]')
            await page.wait_for_load_state("networkidle", timeout=60000)
            log(f"[{site_name}] Logged in.")

            await page.goto(sales_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)

            ok, _, _ = await run_search_from_config(page, site, search_config)
            if not ok:
                log(f"[{site_name}] Search form failed (maker/model not found).")
                await browser.close()
                return (False, 0)

            has_results, count = await check_has_results(page)
            await browser.close()
            return (has_results, count)

        except Exception as e:
            log(f"[{site_name}] Error: {e}")
            await browser.close()
            return (False, 0)


async def main():
    parser = argparse.ArgumentParser(
        description="Check when sales data are published for an auction date."
    )
    parser.add_argument(
        "--site",
        help="Site to use (default: first available with sales_data_url)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_MINUTES,
        help=f"Minutes between checks (default: {DEFAULT_INTERVAL_MINUTES})",
    )
    parser.add_argument(
        "--visible",
        action="store_true",
        help="Show browser window",
    )
    args = parser.parse_args()

    if args.visible:
        import config.config as config_module
        config_module.browser_settings["headless"] = False

    active_sites = {k: v for k, v in auction_sites.items() if v.get("scraping", {}).get("sales_data_url")}
    if not active_sites:
        log("No auction sites with sales_data_url configured.")
        return

    site_name = args.site if args.site and args.site in active_sites else list(active_sites.keys())[0]
    site = active_sites[site_name]

    log(f"Checking {AUCTION_DATE} - {MAKER} {MODEL} on {site_name}")
    log(f"Interval: {args.interval} minute(s)")
    log("")

    interval_sec = args.interval * 60

    while True:
        has_results, count = await run_check(site_name, site)

        if has_results:
            first_available = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log("=" * 60)
            log(f"RESULTS FOUND: {count} row(s) for {MAKER} {MODEL} on {AUCTION_DATE}")
            log(f"SALES DATA FIRST AVAILABLE AT: {first_available}")
            log("Sales data are now published. Terminating.")
            log("=" * 60)
            return

        log(f"No results yet. Next check in {args.interval} minute(s).")
        await asyncio.sleep(interval_sec)


if __name__ == "__main__":
    asyncio.run(main())
