#!/usr/bin/env python3
"""
Test proxy list against the auction image URL.
Reports which proxies return 200 vs 404.

Usage:
  python tools/test_proxies.py "C:\path\to\proxies.txt"
  python tools/test_proxies.py "C:\path\to\proxies.txt" --url "https://..."
"""
import argparse
import asyncio
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

TEST_URL = "https://p3.aleado.com/pic/?system=auto&date=2026-03-07&auct=79&bid=5791&number=0"


def parse_proxy_line(line: str) -> dict | None:
    """Parse ip:port:user:pass into Playwright proxy config."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(":")
    if len(parts) != 4:
        return None
    ip, port, user, password = parts
    return {
        "server": f"http://{ip}:{port}",
        "username": user,
        "password": password,
    }


def load_proxies(path: Path) -> list[dict]:
    """Load proxy list from file."""
    proxies = []
    for line in path.read_text(encoding="utf-8").splitlines():
        p = parse_proxy_line(line)
        if p:
            proxies.append(p)
    return proxies


async def test_proxy(proxy: dict, url: str, timeout: int = 30000) -> tuple[str, int, str | None]:
    """Test one proxy. Returns (display, status_code, error_message)."""
    from playwright.async_api import async_playwright

    display = proxy["server"].replace("http://", "").replace("https://", "")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                proxy=proxy,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                extra_http_headers={"Referer": "https://auction.zenautoworks.ca/"},
            )
            page = await context.new_page()
            response = await page.goto(url, wait_until="load", timeout=timeout)
            status = response.status if response else 0
            return (display, status, None)
        except Exception as e:
            return (display, -1, str(e))
        finally:
            await browser.close()


async def run_tests(proxy_list_path: Path, url: str):
    """Test all proxies and report results."""
    proxies = load_proxies(proxy_list_path)
    if not proxies:
        print(f"No valid proxies found in {proxy_list_path}")
        return

    print(f"Testing {len(proxies)} proxies against:\n  {url}\n")

    results = []
    for i, proxy in enumerate(proxies):
        display = proxy["server"].replace("http://", "").replace("https://", "")
        print(f"[{i + 1}/{len(proxies)}] {display}...", end=" ", flush=True)
        display, status, err = await test_proxy(proxy, url)
        results.append((display, status, err))
        if status == 200:
            print("OK (200)")
        elif status == 404:
            print("404")
        elif status > 0:
            print(f"HTTP {status}")
        else:
            print(f"FAIL: {err[:60]}" if err else "FAIL")

    print("\n--- Summary ---")
    ok = [r for r in results if r[1] == 200]
    fail = [r for r in results if r[1] != 200]
    print(f"Working (200): {len(ok)}")
    for r in ok:
        print(f"  {r[0]}")
    print(f"Failed: {len(fail)}")
    for r in fail:
        msg = r[2] if r[2] else str(r[1])
        print(f"  {r[0]} -> {msg}")


def main():
    parser = argparse.ArgumentParser(description="Test proxies against auction image URL")
    parser.add_argument(
        "proxy_file",
        type=Path,
        default=Path(r"C:\Users\Administrator\Desktop\projects\proxy_list\Webshare 10 proxies.txt"),
        nargs="?",
        help="Path to proxy list (ip:port:user:pass per line)",
    )
    parser.add_argument("--url", "-u", default=TEST_URL, help="URL to test")
    args = parser.parse_args()

    if not args.proxy_file.exists():
        print(f"File not found: {args.proxy_file}")
        return 1

    asyncio.run(run_tests(args.proxy_file, args.url))
    return 0


if __name__ == "__main__":
    sys.exit(main())
