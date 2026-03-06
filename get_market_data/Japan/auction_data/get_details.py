# get_details.py
"""
Script to analyze and extract detailed data from auction URLs.
DISABLED: Requires database (processed_urls, vehicle_details). Auction DB has been removed.

Standalone extraction (no DB): extract_auction_page_data, fetch_auction_lot_details.
Used by operations/auction/run_details.py for JSON-based auction lot details.
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

# Auction database removed - get_details cannot run without it
def _disabled():
    print("get_details is disabled: auction database has been removed.")
    print("This script required processed_urls and vehicle_details tables.")
    sys.exit(1)

import asyncio
import logging
import time
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from get_market_data.Japan.auction_site_config_JP import auction_sites

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Standalone auction extraction (no DB) ---

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


class AuctionDataAnalyzer:
    def __init__(self):
        self.db_handler = DatabaseHandler()
        self.browsers = {}  # site_name -> browser
        self.contexts = {}  # site_name -> context
        self.page_pools = {}  # Pool of pages per site
        self.playwright = None
        
        # Performance settings - more realistic
        self.PAGES_PER_SITE = 2        # Reduced for debugging
        self.MAX_TOTAL_PAGES = 10      # Total across all sites (2×5)
        self.BATCH_SIZE = 5            # Reduced for debugging
        self.PAGE_TIMEOUT = 20000      # Increased for better reliability
        self.SELECTOR_TIMEOUT = 8000   # Increased for better reliability
        self.BATCH_DELAY = 1.0         # Increased delay between batches
        self.RATE_LIMIT_PER_SITE = 1   # Reduced to 1 concurrent request per site
        
    async def connect_database(self):
        """Connect to Supabase database"""
        try:
            self.db_handler.connect()
            logger.info("✅ Connected to Supabase")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def analyze_processed_urls_by_site(self):
        """Analyze the processed_urls table and compute statistics per auction site"""
        try:
            logger.info("📊 Analyzing processed_urls table...")
            
            # First, get the total count
            count_result = self.db_handler.supabase_client.table("processed_urls").select("count").execute()
            total_count = count_result.data[0]['count'] if count_result.data else 0
            logger.info(f"📊 Total records in processed_urls: {total_count:,}")
            
            # Get all records from processed_urls in batches
            all_records = []
            offset = 0
            batch_size = 1000  # Supabase default limit
            
            while len(all_records) < total_count:
                result = self.db_handler.supabase_client.table("processed_urls").select("*").range(offset, offset + batch_size - 1).execute()
                
                if not result.data:
                    break
                    
                all_records.extend(result.data)
                offset += batch_size
                logger.info(f"📊 Fetched {len(all_records):,} records so far...")
                
                # Safety check to prevent infinite loop
                if len(result.data) < batch_size:
                    break
            
            logger.info(f"📊 Successfully fetched {len(all_records):,} records out of {total_count:,} total")
            
            # Use all_records instead of result.data
            if not all_records:
                logger.warning("⚠️ No data found in processed_urls table")
                return {}
            
            # Initialize counters
            site_stats = {}
            total_urls = len(all_records)
            
            # Count URLs by site and processing status
            for record in all_records:
                site_name = record.get('site_name', 'Unknown')
                processed = record.get('processed', False)
                
                if site_name not in site_stats:
                    site_stats[site_name] = {
                        'total_urls': 0,
                        'processed_urls': 0,
                        'unprocessed_urls': 0,
                        'processing_started': 0,
                        'processing_completed': 0,
                        'error_count': 0,
                        'retry_count': 0
                    }
                
                site_stats[site_name]['total_urls'] += 1
                
                if processed:
                    site_stats[site_name]['processed_urls'] += 1
                else:
                    site_stats[site_name]['unprocessed_urls'] += 1
                
                # Count processing status
                if record.get('processing_started'):
                    site_stats[site_name]['processing_started'] += 1
                
                if record.get('processing_completed'):
                    site_stats[site_name]['processing_completed'] += 1
                
                # Count errors
                if record.get('error_message'):
                    site_stats[site_name]['error_count'] += 1
                
                # Count retries
                retry_count = record.get('retry_count', 0)
                site_stats[site_name]['retry_count'] += retry_count
            
            # Display results
            self.display_site_analysis(site_stats, total_urls)
            
            return site_stats
            
        except Exception as e:
            logger.error(f"❌ Error analyzing processed_urls: {e}")
            return {}

    def display_site_analysis(self, site_stats: Dict, total_urls: int):
        """Display the analysis results in a formatted table"""
        print(f"\n{'='*80}")
        print(f"📊 PROCESSED_URLS ANALYSIS")
        print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}")
        
        if not site_stats:
            print("❌ No data to analyze")
            return
        
        # Print header
        print(f"{'Site Name':<20} {'Total':<8} {'Processed':<10} {'Unprocessed':<12} {'Started':<8} {'Completed':<10} {'Errors':<7} {'Retries':<8}")
        print("-" * 80)
        
        # Print data for each site
        for site_name, stats in sorted(site_stats.items()):
            print(f"{site_name:<20} {stats['total_urls']:<8} {stats['processed_urls']:<10} {stats['unprocessed_urls']:<12} "
                  f"{stats['processing_started']:<8} {stats['processing_completed']:<10} {stats['error_count']:<7} {stats['retry_count']:<8}")
        
        print("-" * 80)
        
        # Calculate totals
        total_processed = sum(stats['processed_urls'] for stats in site_stats.values())
        total_unprocessed = sum(stats['unprocessed_urls'] for stats in site_stats.values())
        total_started = sum(stats['processing_started'] for stats in site_stats.values())
        total_completed = sum(stats['processing_completed'] for stats in site_stats.values())
        total_errors = sum(stats['error_count'] for stats in site_stats.values())
        total_retries = sum(stats['retry_count'] for stats in site_stats.values())
        
        print(f"{'TOTALS':<20} {total_urls:<8} {total_processed:<10} {total_unprocessed:<12} "
              f"{total_started:<8} {total_completed:<10} {total_errors:<7} {total_retries:<8}")
        
        # Calculate percentages
        if total_urls > 0:
            processed_pct = (total_processed / total_urls) * 100
            unprocessed_pct = (total_unprocessed / total_urls) * 100
            error_pct = (total_errors / total_urls) * 100
            
            print(f"\n📈 SUMMARY:")
            print(f"   • Total URLs: {total_urls:,}")
            print(f"   • Processed: {total_processed:,} ({processed_pct:.1f}%)")
            print(f"   • Unprocessed: {total_unprocessed:,} ({unprocessed_pct:.1f}%)")
            print(f"   • Errors: {total_errors:,} ({error_pct:.1f}%)")
            print(f"   • Total Retries: {total_retries:,}")
        
        print(f"{'='*80}\n")

    def get_url_pool(self, batch_size: int = 50, max_workers: int = 5):
        """Create a pool of URLs for processing, distributed by site"""
        try:
            logger.info(f"🔄 Creating URL pool with batch_size={batch_size}, max_workers={max_workers}")
            
            # Get ALL unprocessed URLs using pagination to overcome 1000 record limit
            all_urls = []
            offset = 0
            limit = 1000  # Supabase default limit
            
            while True:
                result = self.db_handler.supabase_client.table("processed_urls").select(
                    "id, site_name, url, vehicle_id"
                ).eq("processed", False).order("id").range(offset, offset + limit - 1).execute()
                
                if not result.data:
                    break
                
                all_urls.extend(result.data)
                offset += limit
                
                # If we got less than the limit, we've reached the end
                if len(result.data) < limit:
                    break
            
            logger.info(f"📊 Fetched {len(all_urls)} total unprocessed URLs")
            
            if not all_urls:
                logger.warning("⚠️ No unprocessed URLs found")
                return []
            
            # Group URLs by site
            urls_by_site = {}
            for record in all_urls:
                site_name = record.get('site_name', 'Unknown')
                if site_name not in urls_by_site:
                    urls_by_site[site_name] = []
                urls_by_site[site_name].append(record)
            
            # Create balanced batches across sites
            url_pool = []
            site_names = list(urls_by_site.keys())
            site_index = 0
            
            while any(urls_by_site.values()):  # While any site has URLs
                batch = []
                
                # Distribute URLs across sites in round-robin fashion
                for _ in range(batch_size):
                    # Find next site with URLs
                    attempts = 0
                    while attempts < len(site_names):
                        current_site = site_names[site_index % len(site_names)]
                        if urls_by_site[current_site]:
                            batch.append(urls_by_site[current_site].pop(0))
                            break
                        site_index += 1
                        attempts += 1
                    
                    if not any(urls_by_site.values()):
                        break
                
                if batch:
                    url_pool.append(batch)
                    logger.info(f"📦 Created batch {len(url_pool)} with {len(batch)} URLs")
                
                site_index += 1
            
            logger.info(f"✅ Created {len(url_pool)} batches with total {sum(len(batch) for batch in url_pool)} URLs")
            return url_pool
            
        except Exception as e:
            logger.error(f"❌ Error creating URL pool: {e}")
            return []

    def get_site_specific_pool(self, site_name: str, batch_size: int = 50):
        """Get URLs for a specific site"""
        try:
            logger.info(f"🔄 Creating URL pool for {site_name} with batch_size={batch_size}")
            
            result = self.db_handler.supabase_client.table("processed_urls").select(
                "id, site_name, url, vehicle_id"
            ).eq("processed", False).eq("site_name", site_name).order("id").limit(batch_size).execute()
            
            if not result.data:
                logger.warning(f"⚠️ No unprocessed URLs found for {site_name}")
                return []
            
            logger.info(f"✅ Found {len(result.data)} URLs for {site_name}")
            return result.data
            
        except Exception as e:
            logger.error(f"❌ Error creating site-specific URL pool: {e}")
            return []

    def mark_urls_processing(self, url_ids: List[int]):
        """Mark URLs as processing started"""
        try:
            if not url_ids:
                return
            
            current_time = datetime.now().isoformat()
            
            # Update processing_started for the URLs
            self.db_handler.supabase_client.table("processed_urls").update({
                "processing_started": current_time
            }).in_("id", url_ids).execute()
            
            logger.info(f"✅ Marked {len(url_ids)} URLs as processing started")
            
        except Exception as e:
            logger.error(f"❌ Error marking URLs as processing: {e}")

    def get_processing_stats(self):
        """Get current processing statistics"""
        try:
            result = self.db_handler.supabase_client.table("processed_urls").select(
                "processed, processing_started, processing_completed, error_message"
            ).execute()
            
            if not result.data:
                return {}
            
            stats = {
                'total': len(result.data),
                'processed': 0,
                'unprocessed': 0,
                'processing': 0,
                'completed': 0,
                'errors': 0
            }
            
            for record in result.data:
                if record.get('processed'):
                    stats['processed'] += 1
                else:
                    stats['unprocessed'] += 1
                
                if record.get('processing_started') and not record.get('processing_completed'):
                    stats['processing'] += 1
                
                if record.get('processing_completed'):
                    stats['completed'] += 1
                
                if record.get('error_message'):
                    stats['errors'] += 1
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error getting processing stats: {e}")
            return {}

    async def initialize_browsers(self):
        """Initialize browsers with more pages per site"""
        self.playwright = await async_playwright().start()
        
        logger.info("🚀 Initializing browsers for auction sites...")
        
        for site_name in auction_sites.keys():
            try:
                logger.info(f"  📱 Setting up browser for {site_name}...")
                
                # Optimized browser settings
                browser = await self.playwright.chromium.launch(
                    headless=False,  # Show browser to debug what's happening
                    args=[
                        '--disable-gpu', 
                        '--no-sandbox', 
                        '--disable-dev-shm-usage',
                        '--disable-images',        # Skip images for faster loading
                        '--disable-extensions',
                        '--disable-plugins'
                    ]
                )
                
                context = await browser.new_context(
                    viewport={'width': 1280, 'height': 800},
                    ignore_https_errors=True,
                    java_script_enabled=True  # Enable JavaScript for site functionality
                )
                
                # Create page pool
                self.browsers[site_name] = browser
                self.contexts[site_name] = context
                self.page_pools[site_name] = []
                
                # Create reasonable number of pages per site
                for i in range(self.PAGES_PER_SITE):
                    page = await context.new_page()
                    self.page_pools[site_name].append(page)
                
                logger.info(f"  ✅ Browser ready for {site_name} with {self.PAGES_PER_SITE} pages")
                
            except Exception as e:
                logger.error(f"  ❌ Failed to initialize browser for {site_name}: {e}")
                raise
        
        logger.info("✅ All browsers initialized successfully")

    async def login_to_sites(self):
        """Login to each auction site"""
        logger.info("🔐 Logging into auction sites...")
        
        login_tasks = []
        for site_name in auction_sites.keys():
            task = asyncio.create_task(self._login_single_site(site_name))
            login_tasks.append(task)
        
        # Execute all logins concurrently
        results = await asyncio.gather(*login_tasks, return_exceptions=True)
        
        successful_logins = 0
        for i, result in enumerate(results):
            site_name = list(auction_sites.keys())[i]
            if isinstance(result, Exception):
                logger.error(f"  ❌ Login failed for {site_name}: {result}")
            else:
                successful_logins += 1
                logger.info(f"  ✅ Login successful for {site_name}")
        
        logger.info(f"🔐 Login complete: {successful_logins}/{len(auction_sites)} sites")
        return successful_logins

    async def _login_single_site(self, site_name: str):
        """Login to a single auction site"""
        try:
            # Get site config
            site_config = auction_sites[site_name]
            
            # Create a page for login
            page = await self.contexts[site_name].new_page()
            
            # Navigate to login page
            await page.goto(site_config['scraping']['auction_url'])
            
            # Check if already logged in
            try:
                await page.wait_for_selector('input[type="password"]', timeout=3000)
                # Login form found, perform login
                await page.fill('input[name="username"]#usr_name', site_config["username"])
                await page.fill('input[name="password"]#usr_pwd', site_config["password"])
                await page.click('input[name="Submit"][value="Sign in"]')
                await page.wait_for_load_state('networkidle', timeout=30000)
            except:
                # No login form, already logged in
                pass
            
            # Keep this page for future use
            self.page_pools[site_name].append(page)
            
            return True
            
        except Exception as e:
            logger.error(f"Login error for {site_name}: {e}")
            raise

    async def extract_data_from_url(self, page: Page, site_name: str, url_record: Dict) -> Optional[Dict]:
        """Optimized data extraction with shorter timeouts"""
        try:
            # Mark processing as started
            await self.mark_processing_started([url_record['id']])
            
            # Construct full URL
            base_urls = {
                'AutoPacific': 'https://auction.pacificcoastjdm.com',
                'Zervtek': 'https://auctions.zervtek.com',
                'Manga Auto Import': 'https://auc.mangaautoimport.ca',
                'Japan Car Auc': 'https://auc.japancarauc.com',
                'Zen Autoworks': 'https://auction.zenautoworks.ca'
            }
            
            base_url = base_urls.get(site_name, '')
            url = url_record.get('url', '')
            
            if not url:
                await self.mark_processing_failed(url_record['id'], "No URL provided")
                return None
            
            # Clean up malformed URLs (remove trailing &s or incomplete parameters)
            if url.endswith('&s'):
                url = url[:-2]  # Remove trailing &s
            elif url.endswith('&'):
                url = url[:-1]  # Remove trailing &
                
            full_url = url if url.startswith('http') else f"{base_url}{url}"
            
            logger.info(f"🔗 [{site_name}] Processing: {full_url}")
            logger.info(f"  📍 Original URL: {url}")
            logger.info(f"  🌐 Base URL: {base_url}")
            logger.info(f"  🔗 Full URL: {full_url}")
            
            # Extract lot number from URL
            lot_number = self.extract_lot_number_from_url(url)
            
            # Try navigation with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Faster navigation with shorter timeout
                    response = await page.goto(full_url, 
                                               wait_until="domcontentloaded", 
                                               timeout=self.PAGE_TIMEOUT)
                    
                    # Check if page loaded successfully
                    if response and response.status == 200:
                        current_url = page.url
                        logger.info(f"  ✅ Page loaded (attempt {attempt + 1})")
                        logger.info(f"  📍 Current page URL: {current_url}")
                        break
                    else:
                        logger.warning(f"  ⚠️ Status {response.status if response else 'None'} (attempt {attempt + 1})")
                        
                except Exception as e:
                    logger.warning(f"  ⚠️ Navigation failed (attempt {attempt + 1}): {e}")
                    if attempt == max_retries - 1:
                        raise e
                    await asyncio.sleep(1)  # Wait before retry
            
            # Quick selector check with shorter timeout
            try:
                await page.wait_for_selector('table[bgcolor="#D8D8D8"], div.Verdana16px', 
                                           timeout=self.SELECTOR_TIMEOUT)
                logger.info("  ✅ Selectors found")
                
                # Check if we're on the right page by looking for auction content
                page_title = await page.title()
                logger.info(f"  📄 Page title: {page_title}")
                
                # Check for auction-specific content
                auction_content = await page.evaluate("""() => {
                    const hasAuctionTable = !!document.querySelector('table[bgcolor="#D8D8D8"]');
                    const hasVerdanaDiv = !!document.querySelector('div.Verdana16px');
                    const hasImageLinks = !!document.querySelector('a[href*="pic/?system=auto"]');
                    return { hasAuctionTable, hasVerdanaDiv, hasImageLinks };
                }""")
                logger.info(f"  🔍 Page content check: {auction_content}")
                
            except Exception as e:
                logger.warning(f"  ⚠️ Selectors not found: {e}")
                pass  # Continue extraction even if specific selectors not found
            
            # Optimized extraction - same logic but with error handling
            data = await page.evaluate("""() => {
                // Debug: Log what we find
                console.log('Starting extraction...');
                
                const getModelName = () => {
                    // Look for the model name in the table data instead of the header
                    const table = document.querySelector('table[bgcolor="#D8D8D8"]');
                    if (table) {
                        const rows = table.querySelectorAll('tr');
                        for (const row of rows) {
                            const cells = row.querySelectorAll('td');
                            for (let i = 0; i < cells.length; i += 2) {
                                const labelCell = cells[i];
                                const valueCell = cells[i + 1];
                                
                                if (labelCell && valueCell && labelCell.classList.contains('ColorCell_1')) {
                                    const label = labelCell.textContent.trim().toLowerCase();
                                    if (label.includes('type')) {
                                        // This is the vehicle type, not the model name
                                        continue;
                                    }
                                    // Look for the model name in the table structure
                                    if (label.includes('model') || label.includes('name')) {
                                        return valueCell.textContent.trim().replace(/&nbsp;/g, ' ');
                                    }
                                }
                            }
                        }
                    }
                    
                    // Fallback: try to find any div with vehicle information
                    const modelDivs = document.querySelectorAll('div.Verdana16px');
                    for (const div of modelDivs) {
                        const text = div.textContent.trim().replace(/&nbsp;/g, ' ');
                        // Skip auction info (contains dates and auction names)
                        if (text.includes('>') || text.includes('TAA') || text.includes('AUCNET') || text.includes('JU')) {
                            continue;
                        }
                        // Look for vehicle model patterns
                        if (text.includes('TOYOTA') || text.includes('HONDA') || text.includes('NISSAN') || 
                            text.includes('MAZDA') || text.includes('SUBARU') || text.includes('MITSUBISHI')) {
                            console.log('Model name found:', text);
                            return text;
                        }
                    }
                    
                    console.log('No model name found');
                    return '';
                };
                
                const getTableData = () => {
                    const data = {};
                    const table = document.querySelector('table[bgcolor="#D8D8D8"]');
                    console.log('Table found:', table);
                    
                    if (table) {
                        const rows = table.querySelectorAll('tr');
                        rows.forEach(row => {
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
                    
                    // Also try alternative table selector
                    if (Object.keys(data).length === 0) {
                        const altTable = document.querySelector('table.Verdana12px[bgcolor="#D8D8D8"]');
                        if (altTable) {
                            const rows = altTable.querySelectorAll('tr');
                            rows.forEach(row => {
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
                    }
                    
                    return data;
                };
                
                const getImageUrls = () => {
                    const imageUrls = [];
                    // Look for image links in the table structure
                    const imageLinks = document.querySelectorAll('a[href*="pic/?system=auto"]');
                    console.log('Image links found:', imageLinks.length);
                    
                    imageLinks.forEach(link => {
                        const href = link.href;
                        // Get the full image URL without the height parameter
                        if (href && href.includes('pic/?system=auto')) {
                            // Remove height parameter if present
                            const cleanUrl = href.split('&h=')[0];
                            if (!imageUrls.includes(cleanUrl)) {
                                imageUrls.push(cleanUrl);
                            }
                        }
                    });
                    
                    // Also try alternative selectors
                    if (imageUrls.length === 0) {
                        const imgElements = document.querySelectorAll('img[src*="pic/?system=auto"]');
                        imgElements.forEach(img => {
                            const src = img.src;
                            if (src && src.includes('pic/?system=auto')) {
                                const cleanUrl = src.split('&h=')[0];
                                if (!imageUrls.includes(cleanUrl)) {
                                    imageUrls.push(cleanUrl);
                                }
                            }
                        });
                    }
                    
                    return imageUrls;
                };
                
                const modelName = getModelName();
                const tableData = getTableData();
                const imageUrls = getImageUrls();
                
                console.log('Final extracted data:', { modelName, tableData, imageUrls });
                
                return {
                    model_name: modelName,
                    ...tableData,
                    image_urls: imageUrls,
                    total_images: imageUrls.length,
                    auction_sheet_url: imageUrls[0] || ''
                };
            }""")
            
            # Process extracted data
            make, model = self.parse_make_model(data.get('model_name', ''))
            
            # Debug: Log what we extracted
            logger.info(f"  📊 Extracted data: model_name='{data.get('model_name', '')}', type_code='{data.get('type_code', '')}', images={len(data.get('image_urls', []))}")
            
            return {
                'url_record_id': url_record['id'],
                'vehicle_id': url_record.get('vehicle_id'),
                'site_name': site_name,
                'lot_number': lot_number,
                'url': url,
                'make': make,
                'model': model,
                'year': self._parse_numeric(data.get('year', '')),
                'model_name': data.get('model_name', ''),
                'type_code': data.get('type_code', ''),
                'scores': data.get('scores', ''),
                'start_price': data.get('start_price', ''),
                'final_price': data.get('final_price', ''),
                'mileage': data.get('mileage', ''),
                'interior_score': data.get('interior_score', ''),
                'exterior_score': data.get('exterior_score', ''),
                'transmission': data.get('transmission', ''),
                'displacement': data.get('displacement', ''),
                'result': data.get('result', ''),
                'color': data.get('color', ''),
                'equipment': data.get('equipment', ''),
                'auction_time': data.get('auction_time', ''),
                'auction_sheet_url': data.get('auction_sheet_url', ''),
                'image_urls': data.get('image_urls', []),
                'total_images': data.get('total_images', 0)
            }
            
        except Exception as e:
            logger.error(f"❌ Extraction failed for {url}: {e}")
            
            # Take a screenshot for debugging
            try:
                screenshot_path = f"logs/screenshots/error_{site_name}_{url_record['id']}.png"
                os.makedirs(os.path.dirname(screenshot_path), exist_ok=True)
                await page.screenshot(path=screenshot_path)
                logger.info(f"  📸 Screenshot saved: {screenshot_path}")
            except:
                pass
                
            await self.mark_processing_failed(url_record['id'], str(e))
            return None

    def extract_lot_number_from_url(self, url: str) -> str:
        """Extract lot number from URL"""
        try:
            # Extract lot number from URL patterns
            patterns = [
                r'lot&id=(\d+)',  # Standard pattern
                r'lot/(\d+)',     # Alternative pattern
                r'id=(\d+)',      # Generic pattern
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    return match.group(1)
            
            return "unknown"
        except:
            return "unknown"

    def parse_make_model(self, model_name: str) -> tuple:
        """Parse make and model from model name"""
        try:
            if not model_name:
                return "Unknown", "Unknown"
            
            # Common makes to look for
            makes = [
                'Toyota', 'Honda', 'Nissan', 'Mazda', 'Subaru', 'Mitsubishi',
                'Lexus', 'Acura', 'Infiniti', 'Suzuki', 'Daihatsu', 'Isuzu',
                'BMW', 'Mercedes', 'Audi', 'Volkswagen', 'Volvo', 'Saab',
                'Ford', 'Chevrolet', 'Dodge', 'Chrysler', 'Jeep', 'GMC',
                'Hyundai', 'Kia', 'Genesis'
            ]
            
            model_name_upper = model_name.upper()
            
            for make in makes:
                if make.upper() in model_name_upper:
                    # Extract model (everything after the make)
                    model_part = model_name.replace(make, '').strip()
                    return make, model_part if model_part else "Unknown"
            
            # If no make found, try to split on common patterns
            parts = model_name.split()
            if len(parts) >= 2:
                return parts[0], ' '.join(parts[1:])
            
            return "Unknown", model_name
            
        except:
            return "Unknown", "Unknown"

    def _parse_numeric(self, value) -> int:
        """Parse numeric values safely"""
        if not value:
            return 0
        try:
            # Remove all non-digit characters including spaces, newlines, and special chars
            cleaned = ''.join(c for c in str(value) if c.isdigit())
            return int(cleaned) if cleaned else 0
        except:
            return 0

    def _clean_price(self, value) -> str:
        """Clean price values for database storage"""
        if not value:
            return ''
        try:
            # Remove extra whitespace, newlines, and special characters
            cleaned = str(value).replace('\n', ' ').replace('\xa0', ' ').strip()
            # Remove multiple spaces
            cleaned = ' '.join(cleaned.split())
            return cleaned
        except:
            return ''

    async def mark_processing_started(self, url_ids: List[int]):
        """Mark URLs as processing started"""
        try:
            if not url_ids:
                return
            
            current_time = datetime.now().isoformat()
            
            # Update processing_started for the URLs
            self.db_handler.supabase_client.table("processed_urls").update({
                "processing_started": current_time
            }).in_("id", url_ids).execute()
            
            # Don't log this - too verbose
            pass
            
        except Exception as e:
            logger.error(f"❌ Error marking URLs as processing: {e}")

    async def mark_processing_failed(self, url_id: int, error_message: str):
        """Mark URL as processing failed"""
        try:
            self.db_handler.supabase_client.table("processed_urls").update({
                "processed": False,
                "processing_completed": datetime.now().isoformat(),
                "error_message": error_message
            }).eq("id", url_id).execute()
        except Exception as e:
            logger.error(f"❌ Error marking URL {url_id} as failed: {e}")

    def display_url_pool_status(self):
        """Display URL pooling status and statistics"""
        try:
            logger.info("📊 Displaying URL pooling status...")
            
            # Get balanced URL pool
            print(f"\n{'='*60}")
            print("📊 URL POOLING STATUS")
            print(f"{'='*60}")
            
            url_pool = self.get_url_pool(batch_size=20, max_workers=3)
            
            if url_pool:
                print(f"📦 Created {len(url_pool)} batches")
                for i, batch in enumerate(url_pool[:5], 1):  # Show first 5 batches
                    site_counts = {}
                    for url_record in batch:
                        site = url_record.get('site_name', 'Unknown')
                        site_counts[site] = site_counts.get(site, 0) + 1
                    
                    print(f"  Batch {i}: {len(batch)} URLs")
                    for site, count in sorted(site_counts.items()):
                        print(f"    • {site}: {count} URLs")
                print()
            
            # Get site-specific pool status
            print("🔗 Site-specific pools:")
            for site_name in ['AutoPacific', 'Zen Autoworks', 'Zervtek']:
                site_urls = self.get_site_specific_pool(site_name, batch_size=10)
                print(f"  • {site_name}: {len(site_urls)} URLs")
            
            # Show processing stats
            print(f"\n📊 Current Processing Stats:")
            stats = self.get_processing_stats()
            if stats:
                print(f"  • Total: {stats.get('total', 0):,}")
                print(f"  • Unprocessed: {stats.get('unprocessed', 0):,}")
                print(f"  • Processing: {stats.get('processing', 0):,}")
                print(f"  • Completed: {stats.get('completed', 0):,}")
                print(f"  • Errors: {stats.get('errors', 0):,}")
            
            print(f"{'='*60}\n")
            
        except Exception as e:
            logger.error(f"❌ Error displaying URL pooling status: {e}")

    async def run_analysis(self):
        """Main analysis function"""
        logger.info("🚀 Starting auction data analysis...")
        
        # Connect to database
        await self.connect_database()
        
        # Analyze processed_urls table
        site_stats = self.analyze_processed_urls_by_site()
        
        # Display URL pooling status
        self.display_url_pool_status()
        
        logger.info("✅ Analysis complete!")

    async def run_extraction(self, batch_size: int = 20, max_workers: int = 3):
        """Run the actual data extraction process"""
        logger.info("🚀 Starting data extraction process...")
        
        try:
            # Initialize browsers
            await self.initialize_browsers()
            
            # Login to sites
            successful_logins = await self.login_to_sites()
            if successful_logins == 0:
                logger.error("❌ No successful logins. Cannot proceed with extraction.")
                return
            
            # Get URL pool
            url_pool = self.get_url_pool(batch_size=batch_size, max_workers=max_workers)
            if not url_pool:
                logger.warning("⚠️ No URLs to process")
                return
            
            logger.info(f"📦 Processing {len(url_pool)} batches with {sum(len(batch) for batch in url_pool)} total URLs")
            
            # Process batches
            for batch_index, batch in enumerate(url_pool, 1):
                print(f"\n{'='*50}")
                print(f"🔄 BATCH {batch_index}/{len(url_pool)} - {len(batch)} URLs")
                print(f"{'='*50}")
                logger.info(f"Processing batch {batch_index}/{len(url_pool)} with {len(batch)} URLs")
                
                # Group URLs by site for efficient processing
                urls_by_site = {}
                for url_record in batch:
                    site_name = url_record.get('site_name')
                    if site_name not in urls_by_site:
                        urls_by_site[site_name] = []
                    urls_by_site[site_name].append(url_record)
                
                # Process each site's URLs concurrently
                site_tasks = []
                for site_name, site_urls in urls_by_site.items():
                    if site_name in self.page_pools and self.page_pools[site_name]:
                        task = asyncio.create_task(
                            self._process_site_urls(site_name, site_urls)
                        )
                        site_tasks.append(task)
                
                # Wait for all sites in this batch to complete
                if site_tasks:
                    await asyncio.gather(*site_tasks, return_exceptions=True)
                
                # Small delay between batches
                await asyncio.sleep(self.BATCH_DELAY)
            
            logger.info("✅ Data extraction process completed!")
            
        except Exception as e:
            logger.error(f"❌ Error during extraction: {e}")
            raise

    async def _process_site_urls(self, site_name: str, url_records: List[Dict]):
        """Process URLs for a specific site using available pages"""
        logger.info(f"🔄 Processing {len(url_records)} URLs for {site_name}")
        
        # Get available pages for this site
        pages = self.page_pools.get(site_name, [])
        if not pages:
            logger.error(f"❌ No pages available for {site_name}")
            return
        
        # Create semaphore to limit concurrent requests per site
        semaphore = asyncio.Semaphore(self.RATE_LIMIT_PER_SITE)
        
        async def process_single_url(url_record: Dict):
            async with semaphore:
                # Get next available page (round-robin)
                page = pages[len(self.page_pools[site_name]) % len(pages)]
                
                try:
                    # Extract data
                    extracted_data = await self.extract_data_from_url(page, site_name, url_record)
                    
                    if extracted_data:
                        # Save to database
                        await self._save_extracted_data(extracted_data)
                        logger.info(f"  ✅ Data extracted and saved")
                    else:
                        logger.warning(f"  ⚠️ No data extracted")
                        
                except Exception as e:
                    logger.error(f"  ❌ Processing failed: {e}")
                    await self.mark_processing_failed(url_record['id'], str(e))
        
        # Process URLs concurrently with rate limiting
        tasks = [process_single_url(url_record) for url_record in url_records]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _save_extracted_data(self, data: Dict):
        """Save extracted data to the database"""
        try:
            # Insert into vehicle_details table - only store additional details not in vehicles table
            vehicle_details_data = {
                'vehicle_id': data['vehicle_id'],
                'url': data['url'],
                'final_price': self._parse_numeric(data['final_price']),      # Final auction price as integer
                'auction_date': data['auction_time'],      # Auction date/time
                'engine_size': self._parse_numeric(data['displacement']),      # Engine displacement as integer
                'transmission': data['transmission'],      # Transmission type (e.g., "FAT")
                'additional_info': data['model_name'],     # Full model name as additional info
                'extraction_date': datetime.now().isoformat(),
                'type_code': data['type_code'],            # Vehicle type code (e.g., "NZT260")
                'chassis_number': '',                      # Not extracted yet
                'interior_score': data['interior_score'],  # Interior grade (e.g., "D")
                'exterior_score': data['exterior_score'],  # Exterior grade (e.g., "C")
                'equipment': data['equipment'],            # Equipment codes (e.g., "AAC")
                'auction_time': data['auction_time'],      # Specific auction time
                'displacement': self._parse_numeric(data['displacement']),     # Engine displacement as integer
                'image_urls': data['image_urls'],          # All image URLs
                'total_images': data['total_images'],      # Number of images
                'auction_sheet_url': data['auction_sheet_url'],  # First image URL
                'start_price': self._parse_numeric(data['start_price'])       # Start price as integer
            }
            
            # Debug: Log the data being sent to database
            logger.info(f"  📊 Database data: engine_size={vehicle_details_data['engine_size']}, displacement={vehicle_details_data['displacement']}")
            
            # Remove any None values that might cause issues
            vehicle_details_data = {k: v for k, v in vehicle_details_data.items() if v is not None}
            
            # Insert the data
            try:
                result = self.db_handler.supabase_client.table("vehicle_details").insert(vehicle_details_data).execute()
                
                # Mark URL as processed
                self.db_handler.supabase_client.table("processed_urls").update({
                    "processed": True,
                    "processing_completed": datetime.now().isoformat()
                }).eq("id", data['url_record_id']).execute()
                
                logger.info(f"  ✅ Data saved to database")
                
            except Exception as db_error:
                logger.error(f"  ❌ Database insert failed: {db_error}")
                # Log the problematic data for debugging
                for key, value in vehicle_details_data.items():
                    logger.error(f"    {key}: {value} (type: {type(value).__name__})")
                raise db_error
            
        except Exception as e:
            logger.error(f"  ❌ Database error: {e}")
            await self.mark_processing_failed(data['url_record_id'], f"Database error: {e}")

    async def cleanup(self):
        """Clean up resources"""
        logger.info("🧹 Cleaning up...")
        
        # Close all pages
        for site_name, pages in self.page_pools.items():
            for page in pages:
                try:
                    await page.close()
                except:
                    pass
        
        # Close all contexts
        for context in self.contexts.values():
            try:
                await context.close()
            except:
                pass
        
        # Close all browsers
        for browser in self.browsers.values():
            try:
                await browser.close()
            except:
                pass
        
        # Stop playwright
        if self.playwright:
            await self.playwright.stop()
        
        # Close database
        try:
            self.db_handler.close()
        except:
            pass
        
        logger.info("✅ Cleanup complete")

async def main():
    """Main function"""
    _disabled()
    analyzer = AuctionDataAnalyzer()
    
    try:
        # First run analysis to show current status
        await analyzer.run_analysis()
        
        # Start extraction automatically
        print(f"\n{'='*60}")
        print("🚀 STARTING EXTRACTION")
        print(f"{'='*60}")
        await analyzer.run_extraction(batch_size=5, max_workers=2)
            
    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        await analyzer.cleanup()

if __name__ == "__main__":
    asyncio.run(main()) 