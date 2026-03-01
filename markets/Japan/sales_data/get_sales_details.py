#!/usr/bin/env python3
"""
get_sales_details.py - Sales Details Extraction Script
Extracts detailed information from auction lot pages.
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from playwright.async_api import BrowserContext, Page

async def extract_page_data(page: Page, url_data: Dict) -> Optional[Dict]:
    """
    Extract data from an auction lot page.
    
    Args:
        page: Playwright Page object
        url_data: Dictionary containing url_id, site_name, lot_number, etc.
    
    Returns:
        Dictionary containing extracted page data
    """
    try:
        # Wait for page to load (domcontentloaded is more reliable than networkidle on stats pages)
        await page.wait_for_load_state('domcontentloaded', timeout=15000)
        await asyncio.sleep(1.5)
        
        # Extract basic page information
        page_data = {
            'url_id': url_data.get('id'),
            'site_name': url_data.get('site_name'),
            'lot_number': url_data.get('lot_number'),
            'extracted_at': datetime.now().isoformat(),
            'image_urls': [],
            'auction_sheet_url': '',
            'final_price': '',
            'details': {}
        }
        
        # Extract all image URLs from the page
        try:
            image_urls = await page.evaluate("""
                () => Array.from(document.querySelectorAll('img'))
                    .map(img => img.src || img.getAttribute('data-src'))
                    .filter(Boolean)
            """)
            page_data['image_urls'] = image_urls if isinstance(image_urls, list) else []
        except Exception:
            pass
        
        # Extract auction sheet image URL
        try:
            auction_sheet_url = await page.evaluate("""
                () => {
                    // Look for highslide-image class (auction sheet image)
                    const auctionSheetImg = document.querySelector('img.highslide-image');
                    if (auctionSheetImg && auctionSheetImg.src) {
                        return auctionSheetImg.src;
                    }
                    
                    // Fallback: look for any image with 'aleado.com' in src (auction sheet domain)
                    const aleadoImages = document.querySelectorAll('img[src*="aleado.com"]');
                    if (aleadoImages.length > 0) {
                        return aleadoImages[0].src;
                    }
                    
                    // Fallback: look for images with 'auction' or 'sheet' in src
                    const auctionImages = document.querySelectorAll('img[src*="auction"], img[src*="sheet"]');
                    if (auctionImages.length > 0) {
                        return auctionImages[0].src;
                    }
                    
                    return '';
                }
            """)
            page_data['auction_sheet_url'] = auction_sheet_url
        except:
            pass
        
        # Extract final price from table row (stats pages may use "Final price", "End price", or "Price")
        try:
            final_price = await page.evaluate("""
                () => {
                    const rows = document.querySelectorAll('tr');
                    const labels = ['Final price', 'End price', 'Sold price', 'Price', 'Final Price'];
                    for (const row of rows) {
                        const cells = row.querySelectorAll('td');
                        for (let i = 0; i < cells.length; i++) {
                            const cellText = (cells[i].textContent || '').trim();
                            if (labels.some(l => cellText === l || cellText.indexOf(l) === 0) && cells[i + 1]) {
                                const v = (cells[i + 1].textContent || '').trim();
                                if (v) return v;
                            }
                        }
                    }
                    const allText = document.body.innerText;
                    const m = allText.match(/(?:Final price|End price|Sold price)[\\s\\n]*([^\\n]+)/i) || allText.match(/([0-9,]+\\s*JPY)/i);
                    if (m) return m[1].trim();
                    return '';
                }
            """)
            page_data['final_price'] = final_price
        except:
            pass
        
        # Extract specific auction details
        try:
            details = await page.evaluate("""
                () => {
                    const details = {};
                    
                    // Try to extract common auction details
                    const priceElements = document.querySelectorAll('[class*="price"], [id*="price"]');
                    if (priceElements.length > 0) {
                        details.prices = Array.from(priceElements).map(el => el.textContent.trim()).slice(0, 5);
                    }
                    
                    const infoElements = document.querySelectorAll('[class*="info"], [class*="detail"]');
                    if (infoElements.length > 0) {
                        details.info = Array.from(infoElements).map(el => el.textContent.trim()).slice(0, 10);
                    }
                    
                    // Extract color and year from table
                    const rows = document.querySelectorAll('tr');
                    for (const row of rows) {
                        const cells = row.querySelectorAll('td');
                        for (let i = 0; i < cells.length; i++) {
                            const cellText = cells[i].textContent.trim();
                            if (cellText === 'Color' && cells[i + 1]) {
                                details.color = cells[i + 1].textContent.trim();
                            }
                            if (cellText === 'Year' && cells[i + 1]) {
                                details.year = cells[i + 1].textContent.trim();
                            }
                        }
                    }
                    
                    return details;
                }
            """)
            page_data['details'] = details
        except:
            pass
        
        # Fallback: use end_price from results table if page had no final_price (stats pages vary)
        if not (page_data.get('final_price') or "").strip() and url_data.get('end_price'):
            page_data['final_price'] = str(url_data['end_price']).strip() + " JPY"
        
        return page_data
        
    except Exception as e:
        print(f"❌ Error extracting page data: {e}")
        return None


def detail_has_data(detail: Optional[Dict]) -> bool:
    """True if lot_details contains meaningful extracted content (not just defaults)."""
    if not detail or not isinstance(detail, dict):
        return False
    if detail.get("image_urls"):
        return True
    if (detail.get("auction_sheet_url") or "").strip():
        return True
    if (detail.get("final_price") or "").strip():
        return True
    d = detail.get("details") or {}
    if d and isinstance(d, dict) and any(v for v in d.values() if v):
        return True
    return False


class ExtractionAborted(Exception):
    """Raised when extraction is aborted due to too many consecutive empty results."""
    pass


async def _fetch_single_url(
    context: BrowserContext,
    url: str,
    url_data: Dict,
) -> Optional[Dict]:
    """Open one URL in a new tab, extract, close. Returns detail dict or None."""
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(2)
        detail = await extract_page_data(page, url_data)
        return detail
    except Exception as e:
        print(f"    Lot {url_data.get('lot_number', '?')} failed: {e}", flush=True)
        return None
    finally:
        await page.close()


async def fetch_lot_details_batched(
    context: BrowserContext,
    results: List[Dict],
    items: List[Dict],
    batch_size: int = 3,
    batch_delay: float = 2.0,
    max_consecutive_empty: int = 10,
    early_check_after: int = 5,
) -> Tuple[List[Dict], int]:
    """
    Fetch lot details in small batches with early termination.
    Each item must have 'url' and 'index' keys (plus optional 'lot_number', 'site_name', etc).

    Safeguards:
      - Processes batch_size URLs at a time, then waits batch_delay seconds.
      - After early_check_after lots, if 0 meaningful data extracted: ABORT.
      - If max_consecutive_empty lots in a row return no meaningful data: ABORT.

    Returns (results with lot_details attached, meaningful_count).
    """
    if not items:
        return (results, 0)

    meaningful = 0
    processed = 0
    consecutive_empty = 0

    for batch_start in range(0, len(items), batch_size):
        batch = items[batch_start:batch_start + batch_size]
        tasks = []
        for entry in batch:
            idx = entry.get("index", -1)
            url = entry.get("url", "").strip()
            if idx < 0 or idx >= len(results) or not url:
                continue
            record = results[idx]
            url_data = {
                "id": idx,
                "site_name": entry.get("site_name") or record.get("site_name"),
                "lot_number": entry.get("lot_number") or record.get("lot_number"),
                "end_price": record.get("end_price"),
            }
            tasks.append((idx, url, url_data))

        batch_results = await asyncio.gather(
            *[_fetch_single_url(context, url, ud) for _, url, ud in tasks],
            return_exceptions=True,
        )

        for (idx, url, url_data), outcome in zip(tasks, batch_results):
            processed += 1
            if isinstance(outcome, Exception) or outcome is None:
                consecutive_empty += 1
            else:
                results[idx]["lot_details"] = outcome
                if detail_has_data(outcome):
                    meaningful += 1
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1

            if consecutive_empty >= max_consecutive_empty:
                msg = f"ABORT: {consecutive_empty} consecutive lots with no meaningful data. Stopping to protect the account."
                print(f"  {msg}", flush=True)
                raise ExtractionAborted(
                    f"Aborted after {consecutive_empty} consecutive empty results ({processed} processed, {meaningful} meaningful)."
                )

        if processed >= early_check_after and meaningful == 0:
            msg = f"ABORT: First {processed} lots yielded 0 meaningful data. Stopping to protect the account."
            print(f"  {msg}", flush=True)
            raise ExtractionAborted(
                f"Aborted after early check: {processed} lots processed, 0 meaningful."
            )

        print(f"  Batch done: {processed}/{len(items)} processed, {meaningful} meaningful so far.", flush=True)

        if batch_start + batch_size < len(items):
            await asyncio.sleep(batch_delay)

    return (results, meaningful)


def _build_items_from_results_with_links(results: List[Dict], base_url: str) -> List[Dict]:
    """Build items list (with 'url' and 'index') from results that have lot_link."""
    items = []
    for i, r in enumerate(results):
        link = (r.get("lot_link") or "").strip()
        if not link:
            continue
        url = link if link.startswith("http") else urljoin(base_url, link)
        items.append({
            "url": url, "index": i,
            "lot_number": r.get("lot_number"), "site_name": r.get("site_name"),
        })
    return items


async def fetch_all_lot_details(
    context: BrowserContext,
    results: List[Dict],
    base_url: str,
    max_concurrent: int = 3,
    batch_delay: float = 2.0,
    max_consecutive_empty: int = 10,
    early_check_after: int = 5,
) -> Tuple[List[Dict], int]:
    """
    Fetch lot details for all records that have lot_link; attach to results in place.
    Uses batched fetching with early termination. Returns (results, meaningful_count).
    """
    items = _build_items_from_results_with_links(results, base_url)
    return await fetch_lot_details_batched(
        context, results, items,
        batch_size=max_concurrent, batch_delay=batch_delay,
        max_consecutive_empty=max_consecutive_empty, early_check_after=early_check_after,
    )


async def fetch_all_lot_details_from_lot_urls_file(
    context: BrowserContext,
    results: List[Dict],
    entries: List[Dict],
    max_concurrent: int = 3,
    batch_delay: float = 2.0,
    max_consecutive_empty: int = 10,
    early_check_after: int = 5,
) -> Tuple[List[Dict], int]:
    """
    Fetch lot details using entries from a _lot_urls.json file (full URL per entry).
    Uses batched fetching with early termination. Returns (results, meaningful_count).
    """
    return await fetch_lot_details_batched(
        context, results, entries,
        batch_size=max_concurrent, batch_delay=batch_delay,
        max_consecutive_empty=max_consecutive_empty, early_check_after=early_check_after,
    )


if __name__ == "__main__":
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    sys.path.insert(0, _root)
    import argparse
    from playwright.async_api import async_playwright
    from get_market_data.Japan.auction_site_config_JP import auction_sites
    from config.config import runtime_settings

    async def _main():
        parser = argparse.ArgumentParser(
            description="Fetch lot details. Pass either the listing JSON or the _lot_urls.json from run_sales_data."
        )
        parser.add_argument(
            "json_path",
            help="Listing JSON (data/sales_data/.../Make_Model_dates.json) or lot URLs file (..._lot_urls.json)",
        )
        args = parser.parse_args()
        path = Path(args.json_path)
        if not path.is_file():
            print(f"File not found: {path}")
            return
        use_lot_urls = path.name.endswith("_lot_urls.json")
        if use_lot_urls:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            entries = data.get("entries", [])
            listing_name = data.get("listing_file", "")
            listing_path = path.parent / listing_name
            if not listing_path.is_file():
                print(f"Listing file not found: {listing_path}")
                return
            with open(listing_path, "r", encoding="utf-8") as f:
                results = json.load(f)
            print(f"Loaded {len(entries)} URLs from {path.name}, listing {listing_path.name} ({len(results)} records).")
        else:
            with open(path, "r", encoding="utf-8") as f:
                results = json.load(f)
            entries = []
            base_url = ""
        site_name = list(auction_sites.keys())[0] if auction_sites else None
        if not site_name:
            print("No auction sites configured.")
            return
        site = auction_sites[site_name]
        base_url = site["scraping"].get("url") or site["scraping"].get("sales_data_url", "")
        from config.config import browser_settings
        settings = runtime_settings
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=browser_settings.get("headless", False))
            context = await browser.new_context(viewport={"width": 1920, "height": 1080})
            page = await context.new_page()
            await page.goto(site["scraping"]["auction_url"])
            await page.fill("#usr_name", site.get("username", ""))
            await page.fill("#usr_pwd", site.get("password", ""))
            await page.click('input[name="Submit"][value="Sign in"]')
            await page.wait_for_load_state("networkidle")
            try:
                if use_lot_urls:
                    results, meaningful = await fetch_all_lot_details_from_lot_urls_file(
                        context, results, entries,
                        max_concurrent=settings.get("concurrent_lot_details", 3),
                        batch_delay=settings.get("detail_batch_delay", 2),
                        max_consecutive_empty=settings.get("max_consecutive_empty", 10),
                        early_check_after=settings.get("early_check_after", 5),
                    )
                    save_path = listing_path
                else:
                    results, meaningful = await fetch_all_lot_details(
                        context, results, base_url,
                        max_concurrent=settings.get("concurrent_lot_details", 3),
                        batch_delay=settings.get("detail_batch_delay", 2),
                        max_consecutive_empty=settings.get("max_consecutive_empty", 10),
                        early_check_after=settings.get("early_check_after", 5),
                    )
                    save_path = path
            except ExtractionAborted as e:
                print(f"ABORTED: {e}")
                meaningful = sum(1 for r in results if detail_has_data(r.get("lot_details")))
                save_path = listing_path if use_lot_urls else path
            await browser.close()
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"Updated {save_path}: {meaningful} lots with extracted content.")

    asyncio.run(_main())
