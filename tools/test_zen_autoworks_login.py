#!/usr/bin/env python3
"""
Test script for Zen Autoworks login.
Launches a visible browser, navigates to the login page, and attempts to log in.
Useful for debugging login failures (e.g. #usr_name timeout).

Usage:
  python tools/test_zen_autoworks_login.py

Run from project root. Uses credentials from Secret Manager (same as pipeline).
"""
import asyncio
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))


async def main():
    from playwright.async_api import async_playwright
    from get_market_data.Japan.auction_site_config_JP import auction_sites

    site_name = "Zen Autoworks"
    if site_name not in auction_sites:
        print(f"Site '{site_name}' not found in config.")
        return 1

    site = auction_sites[site_name]
    username = site.get("username", "")
    password = site.get("password", "")

    if not username or not password:
        print("Credentials missing. Check Secret Manager / get_auction_credentials().")
        return 1

    url = site["scraping"]["auction_url"]
    print(f"Testing Zen Autoworks login")
    print(f"URL: {url}")
    print(f"Username: {username[:3]}***")
    print()

    async with async_playwright() as p:
        # Launch visible browser for debugging (set headless=True to test headless mode)
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        page = await context.new_page()

        try:
            print("Navigating to auction URL...")
            await page.goto(url, wait_until="networkidle", timeout=60000)
            print("Page loaded.")

            # Save screenshot before login attempt
            screenshot_path = _root / "logs" / "zen_login_before.png"
            screenshot_path.parent.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=str(screenshot_path))
            print(f"Screenshot saved: {screenshot_path}")

            # Try to find login elements (with longer timeout)
            print("Looking for #usr_name (timeout 60s)...")
            try:
                await page.wait_for_selector("#usr_name", timeout=60000)
                print("Found #usr_name")
            except Exception as e:
                print(f"#usr_name not found: {e}")
                # List all input elements for debugging
                inputs = await page.query_selector_all("input")
                print(f"Page has {len(inputs)} input elements:")
                for i, inp in enumerate(inputs[:15]):
                    id_attr = await inp.get_attribute("id")
                    name_attr = await inp.get_attribute("name")
                    type_attr = await inp.get_attribute("type")
                    print(f"  {i+1}: id={id_attr!r} name={name_attr!r} type={type_attr!r}")
                await page.screenshot(path=str(_root / "logs" / "zen_login_no_usr_name.png"))
                return 1

            print("Filling username...")
            await page.fill("#usr_name", username)
            print("Filling password...")
            await page.fill("#usr_pwd", password)
            print("Clicking Sign in...")
            await page.click('input[name="Submit"][value="Sign in"]')
            await page.wait_for_load_state("networkidle", timeout=30000)

            await page.screenshot(path=str(_root / "logs" / "zen_login_after.png"))
            print(f"Post-login screenshot: logs/zen_login_after.png")
            print("Login attempt complete. Check browser window and screenshots.")
            await asyncio.sleep(5)  # Keep browser open briefly

        finally:
            await browser.close()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
