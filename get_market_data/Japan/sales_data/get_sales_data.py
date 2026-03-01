#!/usr/bin/env python3
"""
get_sales_data.py - Sales Data Extraction Script
Extracts sales data from the sales results page (the table with #mainTable shown
after submitting the search form on the stats/search page). Not for the auction
listing page; use this after get_sales_results.fill_and_submit_search() has run.
"""

import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from playwright.async_api import Page

# Helper functions for safe extraction
async def get_text_safe(cells, idx, debug_msgs, row_num, field_name):
    """Safely extract text from a table cell."""
    try:
        if idx >= len(cells):
            if debug_msgs is not None:
                debug_msgs.append(f"Row {row_num}: Cell {idx} ({field_name}) not found - only {len(cells)} cells")
            return ''
        el = cells[idx]
        return (await el.inner_text()).strip()
    except Exception as e:
        if debug_msgs is not None:
            debug_msgs.append(f"Row {row_num}: Failed to extract {field_name} from cell {idx}: {e}")
        return ''

async def get_img_url_safe(cells, idx, debug_msgs, row_num):
    """Safely extract image URL from a table cell."""
    try:
        if idx >= len(cells):
            return ''
        el = cells[idx]
        a_tag = await el.query_selector('a')
        if a_tag:
            href = await a_tag.get_attribute('href')
            return href if href else ''
        return ''
    except Exception as e:
        if debug_msgs is not None:
            debug_msgs.append(f"Row {row_num}: Failed to extract image HREF from cell {idx}: {e}")
        return ''

async def get_lot_link_safe(cells, idx, debug_msgs, row_num):
    """Safely extract lot link from a table cell."""
    try:
        if idx >= len(cells):
            return ''
        el = cells[idx]
        a_tag = await el.query_selector('a.red')  # Look for the red link class
        if a_tag:
            href = await a_tag.get_attribute('href')
            return href if href else ''
        return ''
    except Exception as e:
        if debug_msgs is not None:
            debug_msgs.append(f"Row {row_num}: Failed to extract lot link HREF from cell {idx}: {e}")
        return ''

async def get_price_safe(cells, idx, prefix, debug_msgs, row_num):
    """Safely extract price from a table cell."""
    try:
        if idx >= len(cells):
            if debug_msgs is not None:
                debug_msgs.append(f"Row {row_num}: Price cell {idx} not found - only {len(cells)} cells")
            return ''
        el = cells[idx]
        price_div = await el.query_selector(f'div[id^="{prefix}"]')
        if price_div:
            return (await price_div.inner_text()).strip()
        return (await el.inner_text()).strip()
    except Exception as e:
        if debug_msgs is not None:
            debug_msgs.append(f"Row {row_num}: Failed to extract price from cell {idx}: {e}")
        return ''

async def extract_sales_data_from_results(
    page: Page,
    debug_msgs=None,
    session_name: str = None,
    batch_size: int = 0,
    batch_callback=None,
) -> list:
    """
    Extract sales data from the sales results page (#mainTable) with pagination support.
    Expects the page to already be on the results view (e.g. after submitting the
    stats search form via get_sales_results.fill_and_submit_search).

    Args:
        page: Playwright Page object
        debug_msgs: Optional list to collect debug messages
        session_name: Optional session identifier for logging
        batch_size: If > 0 and batch_callback is set, call batch_callback(results_so_far) every this many records.
        batch_callback: Callable(results_list) invoked when accumulated results hit each batch_size threshold.

    Returns:
        List of sales records as dictionaries
    """
    def log(msg):
        print(msg, flush=True)
        if session_name and ("Starting pagination" in msg or "Processing page" in msg or 
                           "Total data rows" in msg or "No data rows" in msg or 
                           "Pagination complete" in msg):
            print(f"{session_name} - {msg}", flush=True)

    all_results = []
    next_batch_at = batch_size if (batch_size > 0 and batch_callback) else None
    page_num = 1
    MAX_PAGES = 50
    MIN_CELLS = 15
    
    log("Starting pagination extraction...")
    
    try:
        # Set results per page to 100 (site defaults to 20)
        try:
            await page.evaluate("setvs(100)")
            await page.wait_for_load_state("networkidle", timeout=15000)
            await asyncio.sleep(1)
            await page.wait_for_selector("#mainTable", timeout=15000)
            log("Set results per page to 100.")
        except Exception as e:
            log(f"Could not set results per page: {e}")

        while True:
            log(f"Processing page {page_num}...")
            
            # Show all hidden columns
            await page.evaluate("""
                const hiddenElements = document.querySelectorAll('[style*="display: none"]');
                hiddenElements.forEach(el => el.style.display = '');
                const mainTable = document.getElementById('mainTable');
                if (mainTable) mainTable.style.display = '';
            """)
            
            await asyncio.sleep(0.5)
            
            # Count table rows
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
                log(f"Page {page_num}: {table_debug['error']} (wrong page or different site structure?)")
                break
            total_rows = table_debug.get('cellRows', 0)
            log(f"Page {page_num}: Found {total_rows} data rows")
            
            if total_rows == 0:
                log(f"Page {page_num}: No data rows found, stopping pagination")
                break
            
            # Extract rows
            rows = await page.query_selector_all('#mainTable tr[id^="cell_"]')
            page_results = []
            
            for i, row in enumerate(rows, start=1):
                try:
                    cells = await row.query_selector_all('td')
                    if not cells or len(cells) < MIN_CELLS:
                        continue
                    
                    result = {}
                    try:
                        result['date'] = await get_text_safe(cells, 0, debug_msgs, i, 'date')
                        result['lot_number'] = await get_text_safe(cells, 1, debug_msgs, i, 'lot_number')
                        result['lot_link'] = await get_lot_link_safe(cells, 1, debug_msgs, i)
                        result['auction'] = await get_text_safe(cells, 2, debug_msgs, i, 'auction')
                        result['photo_url'] = await get_img_url_safe(cells, 3, debug_msgs, i)
                        result['maker'] = await get_text_safe(cells, 4, debug_msgs, i, 'maker')
                        result['model'] = await get_text_safe(cells, 5, debug_msgs, i, 'model')
                        result['grade'] = await get_text_safe(cells, 6, debug_msgs, i, 'grade')
                        result['year'] = await get_text_safe(cells, 7, debug_msgs, i, 'year')
                        result['mileage'] = await get_text_safe(cells, 8, debug_msgs, i, 'mileage')
                        result['displacement'] = await get_text_safe(cells, 9, debug_msgs, i, 'displacement')
                        result['transmission'] = await get_text_safe(cells, 10, debug_msgs, i, 'transmission')
                        result['color'] = await get_text_safe(cells, 12, debug_msgs, i, 'color')
                        result['model_type'] = await get_text_safe(cells, 13, debug_msgs, i, 'model_type')
                        result['end_price'] = await get_price_safe(cells, 16, 'priceE', debug_msgs, i)
                        result['result'] = await get_text_safe(cells, 17, debug_msgs, i, 'result')
                        result['scores'] = await get_text_safe(cells, 18, debug_msgs, i, 'scores')
                        
                        # Map fields to match database schema
                        sales_record = {
                            'site_name': result.get('auction'),
                            'lot_number': result.get('lot_number'),
                            'make': result.get('maker'),
                            'model': result.get('model'),
                            'year': result.get('year'),
                            'grade': result.get('grade'),
                            'model_type': result.get('model_type'),
                            'mileage': result.get('mileage'),
                            'displacement': result.get('displacement'),
                            'transmission': result.get('transmission'),
                            'color': result.get('color'),
                            'auction': result.get('auction'),
                            'sale_date': result.get('date'),
                            'end_price': result.get('end_price'),
                            'result': result.get('result'),
                            'scores': result.get('scores'),
                            'url': result.get('photo_url'),
                            'lot_link': result.get('lot_link'),
                        }
                        page_results.append(sales_record)
                    except Exception:
                        pass
                except Exception:
                    pass
            
            all_results.extend(page_results)
            log(f"Page {page_num}: Extracted {len(page_results)} records")
            log(f"Running total extracted: {len(all_results)} vehicles")
            # Batch save: every batch_size records call batch_callback with current results
            if next_batch_at is not None and len(all_results) >= next_batch_at:
                while next_batch_at is not None and len(all_results) >= next_batch_at:
                    try:
                        batch_callback(list(all_results))
                    except Exception as e:
                        log(f"Batch callback error: {e}")
                    next_batch_at += batch_size
                log(f"Batch save: {len(all_results)} records written so far.")
            
            # Check for next page
            pagination_debug = await page.evaluate(
                f"""
                () => {{
                    const currentPage = {page_num};
                    const allLinks = document.querySelectorAll('a');
                    const paginationInfo = {{
                        nextPageExists: false,
                        nextPageNumber: null,
                        sequentialNextExists: false,
                        hasNextArrow: false
                    }};
                    
                    for (const link of allLinks) {{
                        const text = (link.textContent || '').trim();
                        const pageNum = parseInt(text, 10);
                        
                        if (pageNum === currentPage + 1) {{
                            paginationInfo.sequentialNextExists = true;
                            paginationInfo.nextPageExists = true;
                            paginationInfo.nextPageNumber = pageNum;
                        }}
                        
                        if (text === '>>' || text === '»') {{
                            paginationInfo.hasNextArrow = true;
                            paginationInfo.nextPageExists = true;
                            paginationInfo.nextPageNumber = currentPage + 1;
                        }}
                    }}
                    
                    return paginationInfo;
                }}
                """
            )
            
            next_page_exists = pagination_debug['nextPageExists']
            
            if not next_page_exists:
                log(f"No next page found, stopping pagination at page {page_num}")
                break
            
            # Click next page
            click_debug = await page.evaluate(
                f"""
                () => {{
                    const currentPage = {page_num};
                    const targetPageNum = currentPage + 1;
                    const allButtons = document.querySelectorAll('a');
                    const clickInfo = {{
                        targetPageNumber: targetPageNum,
                        clicked: false
                    }};
                    
                    // Look for sequential next page
                    for (const btn of allButtons) {{
                        const text = btn.textContent && btn.textContent.trim();
                        const pageNum = parseInt(text, 10);
                        if (pageNum === targetPageNum) {{
                            btn.click();
                            clickInfo.clicked = true;
                            break;
                        }}
                    }}
                    
                    // If no sequential found, look for next arrow
                    if (!clickInfo.clicked) {{
                        for (const btn of allButtons) {{
                            const text = btn.textContent && btn.textContent.trim();
                            if (text === '>>' || text === '»') {{
                                btn.click();
                                clickInfo.clicked = true;
                                break;
                            }}
                        }}
                    }}
                    
                    return clickInfo;
                }}
                """
            )
            
            if click_debug['clicked']:
                log(f"Page {page_num}: Clicked page {click_debug['targetPageNumber']}")
            else:
                log(f"Page {page_num}: Failed to click page {click_debug['targetPageNumber']}")
                break
            
            # Wait for page to load; on failure wait 5s, reload, then retry the same process
            page_loaded = False
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    if attempt > 0:
                        log(f"Page {page_num}: Retry {attempt}, waiting 5s then reloading...")
                        await asyncio.sleep(5)
                        await page.reload()
                    await page.wait_for_load_state('networkidle', timeout=15000)
                    await asyncio.sleep(1)
                    await page.wait_for_selector('#mainTable', timeout=15000)
                    table_check = await page.evaluate("""
                        () => {
                            const table = document.getElementById('mainTable');
                            if (!table) return 0;
                            const rows = table.querySelectorAll('tr[id^="cell_"]');
                            return rows.length;
                        }
                    """)
                    page_loaded = True
                    log(f"Page {page_num}: Loaded page {page_num + 1} ({table_check} records)")
                    break
                except Exception:
                    if attempt + 1 >= max_retries:
                        log(f"Page {page_num}: Failed to load page {page_num + 1} after {max_retries} attempts")
                    pass
            if not page_loaded:
                break
            
            page_num += 1
            
            # Safety limit
            if page_num > MAX_PAGES:
                log(f"Reached maximum page limit ({MAX_PAGES}), stopping pagination")
                break
        
        log(f"Pagination complete: Processed {page_num} pages, extracted {len(all_results)} total records")
        return all_results
        
    except Exception as e:
        log(f"Failed to extract sales data: {e}")
        return all_results


def parse_sale_date(s: str) -> Optional[datetime]:
    """Try to parse sale_date string to date; return None if unparseable."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y.%m.%d"):
        try:
            return datetime.strptime(s[:10] if len(s) >= 10 else s, fmt)
        except ValueError:
            continue
    m = re.match(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", s)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def sales_date_range(results: list) -> Tuple[Optional[str], Optional[str]]:
    """Return (start_date_str, end_date_str) as YYYY-MM-DD from results' sale_date, or (None, None)."""
    dates = []
    for r in results:
        d = parse_sale_date(r.get("sale_date") or "")
        if d:
            dates.append(d)
    if not dates:
        return (None, None)
    start = min(dates).strftime("%Y-%m-%d")
    end = max(dates).strftime("%Y-%m-%d")
    return (start, end)


def save_sales_results(
    results: list,
    make: str,
    model: str,
    root_path: Path,
    in_progress: bool = False,
) -> Optional[Path]:
    """
    Save results to root_path / {Make} / {Model} / Japan / {Make}_{Model}_{start}_to_{end}.json.
    If in_progress=True, save to {Make}_{Model}_in_progress.json (no date range needed).
    Returns path written, or None if not saved.
    """
    if not results or not make or not model:
        return None
    make_title = (make or "").strip().title()
    model_title = (model or "").strip().title()
    out_dir = root_path / make_title / model_title / "Japan"
    out_dir.mkdir(parents=True, exist_ok=True)
    if in_progress:
        out_name = f"{make_title}_{model_title}_in_progress.json"
    else:
        start_date, end_date = sales_date_range(results)
        if not start_date or not end_date:
            return None
        out_name = f"{make_title}_{model_title}_{start_date}_to_{end_date}.json"
    out_file = out_dir / out_name
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    return out_file


def save_lot_urls_file(
    results: list,
    listing_path: Path,
    base_url: str,
) -> Optional[Path]:
    """
    Save a second JSON file next to the listing file with complete lot URLs and keys
    to match each URL back to the listing (for decoupled details extraction).
    Filename: {listing_stem}_lot_urls.json
    """
    from urllib.parse import urljoin

    entries = []
    for i, r in enumerate(results):
        link = (r.get("lot_link") or "").strip()
        if not link:
            continue
        url = link if link.startswith("http") else urljoin(base_url, link)
        entries.append({
            "url": url,
            "index": i,
            "lot_number": r.get("lot_number"),
            "site_name": r.get("site_name"),
            "lot_link": link,
            "make": r.get("make"),
            "model": r.get("model"),
            "sale_date": r.get("sale_date"),
            "listing_file": listing_path.name,
        })
    if not entries:
        return None
    out_file = listing_path.parent / f"{listing_path.stem}_lot_urls.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"listing_file": listing_path.name, "entries": entries}, f, ensure_ascii=False, indent=2)
    return out_file


if __name__ == "__main__":
    _root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    sys.path.insert(0, _root)
    from playwright.async_api import async_playwright
    from get_market_data.Japan.auction_site_config_JP import auction_sites
    from get_market_data.Japan.sales_data.get_sales_results import run_search_from_config, RESULT_VALUE_SOLD

    async def _main():
        site_name = list(auction_sites.keys())[0] if auction_sites else None
        if not site_name:
            print("No auction sites configured.")
            return
        site = auction_sites[site_name]
        sales_url = site["scraping"].get("sales_data_url")
        if not sales_url:
            print("No sales_data_url.")
            return
        search_config = site.get("scraping", {}).get("sales_search") or {
            "maker": "SUZUKI", "model": "SWIFT",
            "year_from": str(datetime.now().year - 6), "year_to": str(datetime.now().year),
            "result_value": RESULT_VALUE_SOLD,
        }
        from config.config import browser_settings
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=browser_settings.get("headless", False))
            context = await browser.new_context(viewport={"width": 1920, "height": 1080})
            page = await context.new_page()
            await page.goto(site["scraping"]["auction_url"])
            await page.fill("#usr_name", site.get("username", ""))
            await page.fill("#usr_pwd", site.get("password", ""))
            await page.click('input[name="Submit"][value="Sign in"]')
            await page.wait_for_load_state("networkidle")
            await page.goto(sales_url, wait_until="domcontentloaded")
            await asyncio.sleep(2)
            ok, make, model = await run_search_from_config(page, site, search_config)
            if not ok:
                print("Search failed.")
                await browser.close()
                return
            await page.wait_for_selector("#mainTable", timeout=20000)
            await asyncio.sleep(1)
            results = await extract_sales_data_from_results(page, session_name=site_name)
            await browser.close()
        print(f"Extracted {len(results)} records.")
        root = Path(_root) / "data" / "sales_data"
        out = save_sales_results(results, make or "Unknown", model or "Unknown", root)
        if out:
            print(f"Saved to {out}")
        else:
            print("No parseable sale_date in results, not saved.")

    asyncio.run(_main())
