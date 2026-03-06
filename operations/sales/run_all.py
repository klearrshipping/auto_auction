#!/usr/bin/env python3
"""
Extract sales data for ALL makes/models from manufacturer_config_JM across all active auction sites.
Runs for a single auction date to keep load low.

Usage:
  python -u run_sales_data_all.py                          # today's date, all sites round-robin
  python -u run_sales_data_all.py --date 2026-01-15        # specific date
  python -u run_sales_data_all.py --site Zervtek           # one site only
  python -u run_sales_data_all.py --limit 5                # first 5 make/models (test run)
  python -u run_sales_data_all.py --maker TOYOTA            # one maker only, all its models
  python -u run_sales_data_all.py --resume                 # resume a previously interrupted run
  python -u run_sales_data_all.py --auto                   # find next pending date from working_days.json

Progress tracker saved to:  data/sales_data/_progress/extraction_YYYY-MM-DD.json
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime
from itertools import cycle
from pathlib import Path
import subprocess

_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(os.path.dirname(_script_dir))
sys.path.insert(0, _root)
# ==================== TEST MODE ====================
# Set to True to use a separate test folder + single make/model.
# Set to False for production runs.
TEST_MODE = False
# ====================================================

if TEST_MODE:
    SALES_DATA_ROOT = Path(_root) / "data" / "sales_data_test"
else:
    SALES_DATA_ROOT = Path(_root) / "data" / "sales_data"
PROGRESS_DIR = SALES_DATA_ROOT / "_progress"

from playwright.async_api import async_playwright
from get_market_data.Japan.auction_site_config_JP import auction_sites
from get_market_data.Japan.sales_data.get_sales_results import (
    run_search_from_config,
    RESULT_VALUE_SOLD,
)
from get_market_data.Japan.sales_data.get_sales_data import (
    extract_sales_data_from_results,
    save_sales_results,
    save_lot_urls_file,
)
from config.manufacturer_config_JM import manufacturer_configs as _real_configs
from config.config import browser_settings

# In TEST_MODE, override to a single cheap make/model for fast, safe testing.
if TEST_MODE:
    manufacturer_configs = {
        "SUZUKI": {
            "SWIFT": {"category": "cars", "age_limit": 6},
        }
    }
else:
    manufacturer_configs = _real_configs


def log(msg):
    print(msg, flush=True)


def year_range_from_age_limit(age_limit: int):
    y = date.today().year
    return (str(y - age_limit), str(y))


# ---------------------------------------------------------------------------
# Progress tracker
# ---------------------------------------------------------------------------

def _progress_path(auction_date: str) -> Path:
    return PROGRESS_DIR / f"extraction_{auction_date}.json"


def _job_key(site_name: str, make: str, model: str) -> str:
    return f"{site_name}|{make}|{model}"


def load_progress(auction_date: str) -> dict:
    p = _progress_path(auction_date)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "date": auction_date,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "updated_at": None,
        "status": "in_progress",
        "totals": {
            "jobs_total": 0,
            "jobs_completed": 0,
            "jobs_remaining": 0,
            "records_extracted": 0,
            "files_saved": 0,
            "not_found": 0,
            "skipped": 0,
            "errors": 0,
        },
        "jobs": [],
    }


def save_progress(progress: dict):
    progress["updated_at"] = datetime.now().isoformat(timespec="seconds")
    PROGRESS_DIR.mkdir(parents=True, exist_ok=True)
    p = _progress_path(progress["date"])
    with open(p, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2, ensure_ascii=False)


def _recalc_totals(progress: dict):
    jobs = progress["jobs"]
    t = progress["totals"]
    t["jobs_completed"] = sum(1 for j in jobs if j["status"] in ("done", "skipped", "not_found", "no_results"))
    t["jobs_remaining"] = t["jobs_total"] - t["jobs_completed"]
    t["records_extracted"] = sum(j.get("records", 0) for j in jobs)
    t["files_saved"] = sum(1 for j in jobs if j["status"] == "done" and j.get("records", 0) > 0)
    t["not_found"] = sum(1 for j in jobs if j["status"] == "not_found")
    t["skipped"] = sum(1 for j in jobs if j["status"] in ("skipped", "no_results"))
    t["errors"] = sum(1 for j in jobs if j["status"] == "error")


def record_job(progress: dict, site_name: str, make: str, model: str,
               status: str, records: int = 0, file: str = None, error: str = None):
    key = _job_key(site_name, make, model)
    entry = {
        "key": key,
        "site": site_name,
        "make": make,
        "model": model,
        "status": status,
        "records": records,
        "file": file,
        "error": error,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    existing = {j["key"]: idx for idx, j in enumerate(progress["jobs"])}
    if key in existing:
        progress["jobs"][existing[key]] = entry
    else:
        progress["jobs"].append(entry)
    _recalc_totals(progress)
    save_progress(progress)


def get_completed_keys(progress: dict) -> set:
    return {j["key"] for j in progress["jobs"] if j["status"] in ("done", "skipped", "not_found", "no_results")}


def print_progress_bar(progress: dict):
    t = progress["totals"]
    done = t["jobs_completed"]
    total = t["jobs_total"]
    pct = (done / total * 100) if total else 0
    bar_len = 40
    filled = int(bar_len * done / total) if total else 0
    bar = "#" * filled + "-" * (bar_len - filled)
    log(f"  Progress: [{bar}] {done}/{total} ({pct:.0f}%)  |  "
        f"Records: {t['records_extracted']}  Saved: {t['files_saved']}  "
        f"NotFound: {t['not_found']}  Errors: {t['errors']}")


def build_job_list(auction_date: str, maker_filter: str = None):
    """Build list of (make, model, search_config) from manufacturer_configs."""
    jobs = []
    for make, models in manufacturer_configs.items():
        if maker_filter and make.upper() != maker_filter.upper():
            continue
        for model, info in models.items():
            age_limit = info.get("age_limit", 6)
            y_from, y_to = year_range_from_age_limit(age_limit)
            search_config = {
                "maker": make,
                "model": model,
                "year_from": y_from,
                "year_to": y_to,
                "auction_since_date": auction_date,
                "auction_till_date": auction_date,
                "result_value": RESULT_VALUE_SOLD,
            }
            jobs.append((make, model, search_config))
    return jobs


# ---------------------------------------------------------------------------
# Working Days Orchestration
# ---------------------------------------------------------------------------

CONFIG_DIR = Path(_root) / "config"
WORKING_DAYS_PATH = CONFIG_DIR / "working_days.json"

def get_next_pending_date():
    if not WORKING_DAYS_PATH.exists():
        return None
    with open(WORKING_DAYS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    for day in data.get("days", []):
        if day.get("status") == "pending":
            return day["date"]
    return None

def mark_date_completed(auction_date: str):
    if not WORKING_DAYS_PATH.exists():
        return
    with open(WORKING_DAYS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    found = False
    for day in data.get("days", []):
        if day.get("date") == auction_date:
            day["status"] = "completed"
            day["completed_at"] = datetime.now().isoformat()
            found = True
            break
    if found:
        data["updated_at"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        with open(WORKING_DAYS_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        log(f"Updated {WORKING_DAYS_PATH} for {auction_date}.")

def trigger_supabase_sync():
    sync_script = Path(_root) / "tools" / "aggregate_sales" / "cloud_sync.py"
    cmd = [sys.executable, str(sync_script)]
    log(f"Triggering Supabase sync: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        log(f"Sync failed: {e}")
        return False


async def run_jobs_on_site(site_name: str, site: dict, jobs: list,
                          progress: dict, delay_between: float = 3.0):
    """Login to one site, run all assigned jobs sequentially, update progress tracker."""
    sales_url = site["scraping"].get("sales_data_url")
    if not sales_url:
        log(f"  [{site_name}] No sales_data_url, skipping.")
        for make, model, _ in jobs:
            record_job(progress, site_name, make, model, "skipped")
        return

    base_url = site["scraping"].get("url") or sales_url
    completed_keys = get_completed_keys(progress)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=browser_settings.get("headless", False))
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        log(f"  [{site_name}] Logging in...")
        try:
            await page.goto(site["scraping"]["auction_url"], timeout=60000)
            await page.fill("#usr_name", site.get("username", ""))
            await page.fill("#usr_pwd", site.get("password", ""))
            await page.click('input[name="Submit"][value="Sign in"]')
            await page.wait_for_load_state("networkidle")
        except Exception as e:
            log(f"  [{site_name}] Login failed: {e}")
            log(f"  [{site_name}] Skipping all {len(jobs)} jobs for this site.")
            for make, model, _ in jobs:
                key = _job_key(site_name, make, model)
                if key not in completed_keys:
                    record_job(progress, site_name, make, model, "error", error=f"Login failed: {e}")
            await browser.close()
            return
        log(f"  [{site_name}] Logged in. Processing {len(jobs)} jobs.\n")

        for job_idx, (make, model, search_config) in enumerate(jobs, 1):
            key = _job_key(site_name, make, model)
            if key in completed_keys:
                log(f"  [{site_name}] Job {job_idx}/{len(jobs)}: {make} {model} — already done, skipping.")
                continue

            log(f"  [{site_name}] Job {job_idx}/{len(jobs)}: {make} {model}")
            try:
                await page.goto(sales_url, wait_until="domcontentloaded")
                await asyncio.sleep(2)

                ok, current_make, current_model = await run_search_from_config(page, site, search_config)
                if not ok:
                    log(f"    Maker/model not found on site, skipping.")
                    record_job(progress, site_name, make, model, "not_found")
                    print_progress_bar(progress)
                    await asyncio.sleep(1)
                    continue

                try:
                    await page.wait_for_selector("#mainTable", timeout=20000)
                except Exception:
                    log(f"    No results table (0 results for this date), skipping.")
                    record_job(progress, site_name, make, model, "no_results")
                    print_progress_bar(progress)
                    await asyncio.sleep(1)
                    continue

                await asyncio.sleep(1)
                results = await extract_sales_data_from_results(page, session_name=f"{site_name}/{make}/{model}")

                if not results:
                    log(f"    0 records extracted, skipping save.")
                    record_job(progress, site_name, make, model, "no_results")
                else:
                    out = save_sales_results(results, current_make or make, current_model or model, SALES_DATA_ROOT)
                    if out:
                        save_lot_urls_file(results, out, base_url)
                        log(f"    {len(results)} records -> {out.name}")
                        record_job(progress, site_name, make, model, "done",
                                   records=len(results), file=out.name)
                    else:
                        log(f"    {len(results)} records but no parseable sale_date, not saved.")
                        record_job(progress, site_name, make, model, "skipped",
                                   records=len(results))

            except Exception as e:
                log(f"    ERROR: {e}")
                record_job(progress, site_name, make, model, "error", error=str(e))

            print_progress_bar(progress)

            if job_idx < len(jobs):
                await asyncio.sleep(delay_between)

        await browser.close()


async def main():
    parser = argparse.ArgumentParser(description="Extract sales data for all makes/models, one date at a time.")
    parser.add_argument("--date", help="Auction date YYYY-MM-DD")
    parser.add_argument("--auto", action="store_true", help="Find next pending date from working_days.json")
    parser.add_argument("--site", help="Run on one site only (e.g. Zervtek)")
    parser.add_argument("--maker", help="One maker only (e.g. TOYOTA)")
    parser.add_argument("--limit", type=int, default=0, help="Max jobs to run (0 = all)")
    parser.add_argument("--delay", type=float, default=3.0, help="Seconds between jobs (default 3)")
    parser.add_argument("--resume", action="store_true", help="Resume a previously interrupted run for the same date")
    args = parser.parse_args()

    auction_date = args.date
    auto_mode = args.auto

    if not auction_date and auto_mode:
        auction_date = get_next_pending_date()
        if not auction_date:
            log("No pending days found in working_days.json. Everything is up to date!")
            return
        log(f"Auto-selected next pending date: {auction_date}")
    
    if not auction_date:
        auction_date = date.today().isoformat()
        log(f"Using default date (today): {auction_date}")

    try:
        datetime.strptime(auction_date, "%Y-%m-%d")
    except ValueError:
        log(f"Invalid date: {auction_date}. Use YYYY-MM-DD.")
        return

    active_sites = {k: v for k, v in auction_sites.items()}
    if args.site:
        if args.site not in active_sites:
            log(f"Site '{args.site}' not found. Available: {list(active_sites.keys())}")
            return
        active_sites = {args.site: active_sites[args.site]}

    if not active_sites:
        log("No active auction sites.")
        return

    jobs = build_job_list(auction_date, maker_filter=args.maker)
    if args.limit > 0:
        jobs = jobs[:args.limit]

    if not jobs:
        log("No jobs to run (check --maker filter or manufacturer_configs).")
        return

    # Load or create progress tracker
    if args.resume:
        progress = load_progress(auction_date)
        already = len(get_completed_keys(progress))
        if already:
            log(f"Resuming from previous run: {already} jobs already completed.")
        else:
            log("No previous progress found, starting fresh.")
    else:
        progress = load_progress(auction_date)
        progress["jobs"] = []

    site_names = list(active_sites.keys())

    # Distribute jobs round-robin across sites
    site_jobs = {name: [] for name in site_names}
    site_cycle = cycle(site_names)
    for job in jobs:
        sn = next(site_cycle)
        site_jobs[sn].append(job)

    total_job_count = sum(len(v) for v in site_jobs.values())
    progress["totals"]["jobs_total"] = total_job_count

    log(f"Date: {auction_date}")
    log(f"Sites: {', '.join(site_names)}")
    log(f"Jobs: {total_job_count} make/model combinations")
    log(f"Delay: {args.delay}s between jobs")
    log(f"Progress file: {_progress_path(auction_date)}\n")

    for name in site_names:
        log(f"  {name}: {len(site_jobs[name])} jobs")
    log("")

    save_progress(progress)

    for site_name in site_names:
        assigned = site_jobs[site_name]
        if not assigned:
            continue
        log(f"--- {site_name} ({len(assigned)} jobs) ---")
        await run_jobs_on_site(site_name, active_sites[site_name], assigned,
                               progress=progress, delay_between=args.delay)
        t = progress["totals"]
        log(f"--- {site_name} done ---\n")

    progress["status"] = "completed"
    save_progress(progress)

    log("=" * 60)

    # Trigger sync (cloud_sync runs bucket analysis after every upload)
    if t["jobs_completed"] > 0:
        if trigger_supabase_sync():
            if auto_mode:
                mark_date_completed(auction_date)
    else:
        log("No jobs were completed. Skipping sync.")


if __name__ == "__main__":
    asyncio.run(main())
