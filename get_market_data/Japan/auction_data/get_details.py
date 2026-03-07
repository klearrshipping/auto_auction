# get_details.py
"""
Extract detailed data from auction lot pages (JSON-based, no database).

Provides: extract_auction_page_data, fetch_auction_lot_details, fetch_pending_details.
Used by operations/auction/pipeline/3_extract_details.py for UID-keyed state files.
"""

import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin
from playwright.async_api import BrowserContext, Page

# Setup logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import asyncio


# --- Auction extraction (JSON-based, no DB) ---

class ExtractionAborted(Exception):
    """Raised when extraction is aborted due to too many consecutive empty results."""
    pass


def _detail_has_data(detail: Optional[Dict]) -> bool:
    """True if lot_details contains meaningful extracted content."""
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


async def extract_auction_page_data(page: Page, url_data: Dict) -> Optional[Dict]:
    """
    Extract data from an auction lot page (aleado/Zervtek structure).
    Uses auction-specific selectors: a[href*="pic/?system=auto"], table[bgcolor="#D8D8D8"].
    Returns dict compatible with lot_details format.
    """
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
        await asyncio.sleep(1.5)

        data = await page.evaluate("""() => {
            const getImageUrls = () => {
                const imageUrls = [];
                const imageLinks = document.querySelectorAll('a[href*="pic/?system=auto"]');
                imageLinks.forEach(link => {
                    const href = link.href;
                    if (href && href.includes('pic/?system=auto')) {
                        const cleanUrl = href.split('&h=')[0];
                        if (!imageUrls.includes(cleanUrl)) imageUrls.push(cleanUrl);
                    }
                });
                if (imageUrls.length === 0) {
                    document.querySelectorAll('img[src*="pic/?system=auto"]').forEach(img => {
                        const src = img.src;
                        if (src && src.includes('pic/?system=auto')) {
                            const cleanUrl = src.split('&h=')[0];
                            if (!imageUrls.includes(cleanUrl)) imageUrls.push(cleanUrl);
                        }
                    });
                }
                return imageUrls;
            };

            const getTableData = () => {
                const data = {};
                const table = document.querySelector('table[bgcolor="#D8D8D8"]');
                if (table) {
                    table.querySelectorAll('tr').forEach(row => {
                        const cells = row.querySelectorAll('td');
                        for (let i = 0; i < cells.length; i += 2) {
                            const labelCell = cells[i];
                            const valueCell = cells[i + 1];
                            if (labelCell && valueCell && labelCell.classList.contains('ColorCell_1')) {
                                const label = labelCell.textContent.trim().toLowerCase();
                                const value = valueCell.textContent.trim();
                                if (label.includes('type')) data.type_code = value;
                                else if (label.includes('year')) data.year = value;
                                else if (label.includes('scores')) data.scores = value;
                                else if (label.includes('start price')) data.start_price = value;
                                else if (label.includes('mileage')) data.mileage = value;
                                else if (label.includes('interior score')) data.interior_score = value;
                                else if (label.includes('final price')) data.final_price = value;
                                else if (label.includes('transmission')) data.transmission = value;
                                else if (label.includes('displacement')) data.displacement = value;
                                else if (label.includes('exterior score')) data.exterior_score = value;
                                else if (label.includes('result')) data.result = value;
                                else if (label.includes('color')) data.color = value;
                                else if (label.includes('equipment')) data.equipment = value;
                                else if (label.includes('time')) data.auction_time = value;
                            }
                        }
                    });
                }
                return data;
            };

            const imageUrls = getImageUrls();
            const tableData = getTableData();
            return {
                image_urls: imageUrls,
                auction_sheet_url: imageUrls[0] || '',
                final_price: tableData.final_price || '',
                details: tableData
            };
        }""")

        return {
            "url_id": url_data.get("id"),
            "site_name": url_data.get("site_name"),
            "lot_number": url_data.get("lot_number"),
            "extracted_at": datetime.now().isoformat(),
            "image_urls": data.get("image_urls", []),
            "auction_sheet_url": data.get("auction_sheet_url", ""),
            "final_price": data.get("final_price", ""),
            "details": data.get("details", {}),
        }
    except Exception as e:
        logger.warning(f"Extract failed: {e}")
        return None


async def _fetch_single_auction_url(context: BrowserContext, url: str, url_data: Dict) -> Optional[Dict]:
    """Open one auction URL, extract, close."""
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(2)
        return await extract_auction_page_data(page, url_data)
    except Exception as e:
        print(f"    Lot {url_data.get('lot_number', '?')} failed: {e}", flush=True)
        return None
    finally:
        await page.close()


def _build_items_from_results(results: List[Dict], base_url: str) -> List[Dict]:
    """Build items list from results that have lot_link."""
    items = []
    for i, r in enumerate(results):
        link = (r.get("lot_link") or r.get("url") or "").strip()
        if not link:
            continue
        url = link if link.startswith("http") else urljoin(base_url, link)
        items.append({
            "url": url, "index": i,
            "lot_number": r.get("lot_number"), "site_name": r.get("site_name"),
        })
    return items


async def fetch_auction_lot_details(
    context: BrowserContext,
    results: List[Dict],
    base_url: str,
    max_concurrent: int = 3,
    batch_delay: float = 2.0,
    max_consecutive_empty: int = 10,
    early_check_after: int = 5,
) -> Tuple[List[Dict], int]:
    """
    Fetch auction lot details for records with lot_link; attach to results in place.
    Uses auction-specific extraction (aleado/Zervtek page structure).
    Returns (results, meaningful_count).
    """
    items = _build_items_from_results(results, base_url)
    if not items:
        return (results, 0)

    meaningful = 0
    processed = 0
    consecutive_empty = 0

    for batch_start in range(0, len(items), max_concurrent):
        batch = items[batch_start : batch_start + max_concurrent]
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
            }
            tasks.append((idx, url, url_data))

        batch_results = await asyncio.gather(
            *[_fetch_single_auction_url(context, url, ud) for _, url, ud in tasks],
            return_exceptions=True,
        )

        for (idx, url, url_data), outcome in zip(tasks, batch_results):
            processed += 1
            if isinstance(outcome, Exception) or outcome is None:
                consecutive_empty += 1
            else:
                results[idx]["lot_details"] = outcome
                if _detail_has_data(outcome):
                    meaningful += 1
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1

            if consecutive_empty >= max_consecutive_empty:
                raise ExtractionAborted(
                    f"Aborted after {consecutive_empty} consecutive empty results ({processed} processed, {meaningful} meaningful)."
                )

        if processed >= early_check_after and meaningful == 0:
            raise ExtractionAborted(
                f"Aborted after early check: {processed} lots processed, 0 meaningful."
            )

        print(f"  Batch done: {processed}/{len(items)} processed, {meaningful} meaningful so far.", flush=True)
        if batch_start + max_concurrent < len(items):
            await asyncio.sleep(batch_delay)

    return (results, meaningful)


async def fetch_pending_details(
    context: BrowserContext,
    state: Dict,
    base_url: str,
    max_concurrent: int = 3,
    batch_delay: float = 2.0,
    max_consecutive_empty: int = 10,
) -> Tuple[Dict, int]:
    """
    Fetch details for pending entries in UID-keyed state.
    Updates state in place; returns (state, meaningful_count).
    """
    listings = state.get("listings") or {}
    pendings = [
        (uid, entry)
        for uid, entry in listings.items()
        if isinstance(entry, dict) and entry.get("status") == "pending"
    ]
    if not pendings:
        return (state, 0)

    items = []
    for uid, entry in pendings:
        listing = entry.get("listing") or {}
        link = (listing.get("lot_link") or listing.get("url") or "").strip()
        if not link:
            continue
        url = link if link.startswith("http") else urljoin(base_url, link)
        items.append({
            "uid": uid,
            "url": url,
            "lot_number": listing.get("lot_number"),
            "site_name": listing.get("site_name"),
        })

    if not items:
        return (state, 0)

    meaningful = 0
    processed = 0
    consecutive_empty = 0

    for batch_start in range(0, len(items), max_concurrent):
        batch = items[batch_start : batch_start + max_concurrent]
        tasks = []
        for entry in batch:
            url_data = {
                "id": entry["uid"],
                "site_name": entry.get("site_name"),
                "lot_number": entry.get("lot_number"),
            }
            tasks.append((entry["uid"], entry["url"], url_data))

        batch_results = await asyncio.gather(
            *[_fetch_single_auction_url(context, url, ud) for _, url, ud in tasks],
            return_exceptions=True,
        )

        for (uid, _, url_data), outcome in zip(tasks, batch_results):
            processed += 1
            if isinstance(outcome, Exception) or outcome is None:
                consecutive_empty += 1
            else:
                if uid in listings:
                    listings[uid]["details"] = outcome
                    listings[uid]["status"] = "completed"
                if _detail_has_data(outcome):
                    meaningful += 1
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1

            if consecutive_empty >= max_consecutive_empty:
                raise ExtractionAborted(
                    f"Aborted after {consecutive_empty} consecutive empty results."
                )

        print(f"  Batch done: {processed}/{len(items)} processed, {meaningful} meaningful.", flush=True)
        if batch_start + max_concurrent < len(items):
            await asyncio.sleep(batch_delay)

    state["last_updated"] = datetime.now().isoformat()
    return (state, meaningful)


if __name__ == "__main__":
    print("Use via: operations/auction/pipeline/3_extract_details.py")
