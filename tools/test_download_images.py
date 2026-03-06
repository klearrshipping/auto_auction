#!/usr/bin/env python3
"""
Test script to download auction images.

Flow:
  1. Scan dataset: find *_compiled.json files, extract records with image_urls
  2. Build batch job: list of (url, dest_path) for each image
  3. Launch browser (visible)
  4. For each URL in sequence: load URL, wait for image to load, save to folder

Usage:
  python tools/test_download_images.py [--limit N]

Run from project root.
"""
import argparse
import asyncio
import hashlib
import json
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

AUCTION_DATA_ROOT = _root / "data" / "auction_data"
IMAGES_ROOT = _root / "data" / "images" / "auction"


def lot_id_from_url(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:12]


def date_from_auction_time(auction_time: str) -> str | None:
    if not auction_time or len(auction_time) < 10:
        return None
    return auction_time[:10]


def scan_dataset(limit: int = 0) -> list[tuple[str, Path]]:
    """
    Scan compiled JSON files, extract image URLs.
    Returns list of (url, dest_path) for batch job.
    """
    batch = []
    compiled_files = sorted(AUCTION_DATA_ROOT.glob("**/*_compiled.json"))
    if limit > 0:
        compiled_files = compiled_files[:limit]

    for compiled_path in compiled_files:
        try:
            with open(compiled_path, "r", encoding="utf-8") as f:
                records = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        if not isinstance(records, list):
            continue

        for rec in records:
            if not isinstance(rec, dict):
                continue
            image_urls = rec.get("image_urls") or []
            if not image_urls:
                continue

            auction_time = rec.get("auction_time") or ""
            date_str = date_from_auction_time(auction_time) or "2026-03-06"
            lot_id = lot_id_from_url(image_urls[0])
            lot_dir = IMAGES_ROOT / date_str / lot_id

            for i, url in enumerate(image_urls):
                dest = lot_dir / f"{i}.jpg"
                batch.append((url, dest))

    return batch


async def login_zen_autoworks(page, auction_sites: dict) -> bool:
    """Log in to Zen Autoworks so image requests have session cookies."""
    site_name = "Zen Autoworks"
    if site_name not in auction_sites:
        print("  Zen Autoworks not in config, skipping login.")
        return False
    site = auction_sites[site_name]
    username = site.get("username", "")
    password = site.get("password", "")
    if not username or not password:
        print("  Zen Autoworks credentials missing, skipping login.")
        return False
    url = site["scraping"]["auction_url"]
    timeout = 60000
    try:
        await page.goto(url, wait_until="networkidle", timeout=timeout)
        await page.wait_for_selector("#usr_name", timeout=timeout)
        await page.fill("#usr_name", username)
        await page.fill("#usr_pwd", password)
        await page.click('input[name="Submit"][value="Sign in"]')
        await page.wait_for_load_state("networkidle", timeout=timeout)
        return True
    except Exception as e:
        print(f"  Login failed: {e}")
        return False


async def run_batch(batch: list[tuple[str, Path]], headless: bool = False, login: bool = True):
    """Launch browser, optionally log in, load each URL in sequence, wait for image, save."""
    from playwright.async_api import async_playwright
    from get_market_data.Japan.auction_site_config_JP import auction_sites

    if not batch:
        print("Batch is empty.")
        return 0

    print(f"Batch: {len(batch)} image(s)")
    print(f"Browser: {'headless' if headless else 'visible'}\n")

    async with async_playwright() as p:
        # Use system Chrome (same as desktop) so images load like in your browser
        try:
            browser = await p.chromium.launch(channel="chrome", headless=headless)
        except Exception:
            browser = await p.chromium.launch(headless=headless)
        # Referer from auction site may be required for p3.aleado.com images
        extra_headers = {"Referer": "https://auction.zenautoworks.ca/"}
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            extra_http_headers=extra_headers,
        )
        page = await context.new_page()

        if login:
            print("Logging into Zen Autoworks (session required for images)...")
            if await login_zen_autoworks(page, auction_sites):
                print("Logged in.\n")
            else:
                print("Login skipped or failed. Images may return 404.\n")

        success = 0
        for idx, (url, dest) in enumerate(batch):
            dest.parent.mkdir(parents=True, exist_ok=True)
            print(f"[{idx + 1}/{len(batch)}] Loading {url[:70]}...")
            try:
                response = await page.goto(url, wait_until="load", timeout=15000)
                if response and response.status == 200:
                    body = await response.body()
                    dest.write_bytes(body)
                    success += 1
                    print(f"  Saved to {dest.relative_to(_root)}")
                else:
                    print(f"  Failed: HTTP {response.status if response else 'no response'}")
            except Exception as e:
                print(f"  Failed: {e}")
            await asyncio.sleep(0.5)

        await browser.close()

    return success


def main():
    parser = argparse.ArgumentParser(description="Test download auction images.")
    parser.add_argument("--limit", type=int, default=1, help="Max compiled files to scan (0=all)")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--no-login", action="store_true", help="Skip Zen Autoworks login (images may 404)")
    args = parser.parse_args()

    print("Step 1: Scanning dataset for files with image URLs...")
    batch = scan_dataset(limit=args.limit if args.limit > 0 else 0)
    print(f"Found {len(batch)} image URL(s)\n")

    if not batch:
        print("No image URLs found.")
        return 1

    print("Step 2: Batch job built\n")
    print("Step 3: Launching browser...")
    print("Step 4: Loading URLs in sequence, waiting for image, saving...\n")

    success = asyncio.run(run_batch(batch, headless=args.headless, login=not args.no_login))

    print(f"\nDone: {success}/{len(batch)} images saved")
    return 0 if success == len(batch) else 1


if __name__ == "__main__":
    sys.exit(main())
