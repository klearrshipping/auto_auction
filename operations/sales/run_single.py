#!/usr/bin/env python3
"""
Run sales data extraction from auction results pages.
Orchestrates: search form (get_sales_results) -> table extract (get_sales_data) -> save listing + lot_urls file.
Details extraction is decoupled: use the _lot_urls.json file and get_sales_details separately.
Requires: get_market_data.Japan.auction_site_config_JP (credentials from Secret Manager).
"""

import asyncio
import os
import sys
from pathlib import Path

_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(os.path.dirname(_script_dir))
sys.path.insert(0, _root)
SALES_DATA_ROOT = Path(_root) / "data" / "sales_data"

from playwright.async_api import async_playwright
from get_market_data.Japan.auction_site_config_JP import auction_sites
from get_market_data.Japan.sales_data.get_sales_results import run_search_from_config, RESULT_VALUE_SOLD
from config.config import browser_settings
from get_market_data.Japan.sales_data.get_sales_data import (
    extract_sales_data_from_results,
    save_sales_results,
    save_lot_urls_file,
)

# --- Test override: same shape as manufacturer_config_JM; one make/model only. Set to None for normal runs. ---
TEST_MANUFACTURER_CONFIG = {
    "SUZUKI": {
        "SWIFT": {"category": "cars", "age_limit": 6},
    }
}
# -------------------------------------------------------------------------------------------------

# --- Auction date filter override (Date of Auction on search page) ---
# Set both values as YYYY-MM-DD to force a specific auction-date window.
# Leave as None to skip auction-date filtering.
AUCTION_SINCE_DATE = None
AUCTION_TILL_DATE = None
# Example:
# AUCTION_SINCE_DATE = "2025-11-22"
# AUCTION_TILL_DATE = "2026-02-20"
# ---------------------------------------------------------------------


def _year_range_from_age_limit(age_limit: int):
    from datetime import date
    y = date.today().year
    return (str(y - age_limit), str(y))


def _search_config_from_site_and_test(site: dict, use_test: bool):
    if use_test and TEST_MANUFACTURER_CONFIG:
        for make, models in TEST_MANUFACTURER_CONFIG.items():
            for model, info in models.items():
                age_limit = info.get("age_limit", 6)
                y_from, y_to = _year_range_from_age_limit(age_limit)
                return {"maker": make, "model": model, "year_from": y_from, "year_to": y_to, "result_value": RESULT_VALUE_SOLD}
    return site.get("scraping", {}).get("sales_search") or {
        "maker": "SUZUKI", "model": "SWIFT",
        "year_from": str(__import__("datetime").date.today().year - 6),
        "year_to": str(__import__("datetime").date.today().year),
        "result_value": RESULT_VALUE_SOLD,
    }


async def main():
    site_name = list(auction_sites.keys())[0] if auction_sites else None
    if not site_name:
        print("No auction sites configured.")
        return
    site = auction_sites[site_name]
    sales_url = site["scraping"].get("sales_data_url")
    if not sales_url:
        print(f"No sales_data_url for {site_name}")
        return

    use_test = TEST_MANUFACTURER_CONFIG is not None
    search_config = _search_config_from_site_and_test(site, use_test)
    if AUCTION_SINCE_DATE and AUCTION_TILL_DATE:
        # Keep auction-date filter controlled by this run script, not site config.
        search_config["auction_since_date"] = AUCTION_SINCE_DATE
        search_config["auction_till_date"] = AUCTION_TILL_DATE

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=browser_settings.get("headless", False))
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        print(f"Logging into {site_name}...")
        await page.goto(site["scraping"]["auction_url"])
        await page.fill("#usr_name", site.get("username", ""))
        await page.fill("#usr_pwd", site.get("password", ""))
        await page.click('input[name="Submit"][value="Sign in"]')
        await page.wait_for_load_state("networkidle")

        print("Navigating to sales data URL...")
        await page.goto(sales_url, wait_until="domcontentloaded")
        await asyncio.sleep(2)

        ok, current_make, current_model = await run_search_from_config(page, site, search_config)
        if not ok:
            print("Search form submit failed.")
            await browser.close()
            return
        await page.wait_for_selector("#mainTable", timeout=20000)
        await asyncio.sleep(1)

        base_url = site["scraping"].get("url") or sales_url
        make_name = current_make or "Unknown"
        model_name = current_model or "Unknown"

        def _batch_save(results_so_far):
            out_inprog = save_sales_results(
                results_so_far, make_name, model_name, SALES_DATA_ROOT, in_progress=True
            )
            if out_inprog:
                save_lot_urls_file(results_so_far, out_inprog, base_url)

        results = await extract_sales_data_from_results(
            page,
            session_name=site_name,
            batch_size=25,
            batch_callback=_batch_save,
        )
        await browser.close()

    print(f"\nExtracted {len(results)} sales records.")
    if results and (current_make or current_model):
        out = save_sales_results(results, current_make or "Unknown", current_model or "Unknown", SALES_DATA_ROOT)
        if out:
            print(f"Saved to {out}")
            base_url = site["scraping"].get("url") or sales_url
            lot_urls_path = save_lot_urls_file(results, out, base_url)
            if lot_urls_path:
                print(f"Lot URLs (for decoupled details): {lot_urls_path}")
            # Remove in-progress batch files now that final save is done
            make_title = (current_make or "Unknown").strip().title()
            model_title = (current_model or "Unknown").strip().title()
            inprog_dir = SALES_DATA_ROOT / make_title / model_title / "Japan"
            for stem in (f"{make_title}_{model_title}_in_progress",):
                for ext in (".json", "_lot_urls.json"):
                    p = inprog_dir / (stem + ext)
                    if p.exists():
                        try:
                            p.unlink()
                            print(f"Removed {p.name}")
                        except OSError:
                            pass
        else:
            print("No parseable sale_date in results, not saved.")


if __name__ == "__main__":
    asyncio.run(main())
