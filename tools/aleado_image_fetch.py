"""
Shared utilities for fetching p3.aleado.com images.

p3.aleado.com uses hotlink protection and may require:
  1. Referer header from an auction partner (e.g. https://auction.zenautoworks.ca/)
  2. Session cookies from a logged-in auction site (browser + login required)

Usage:
  - requests_fetch(): Fast, try first; works when Referer alone is sufficient
  - test_download_images.py (browser mode): Use when session cookies are required
"""

import requests

# Referrers that p3.aleado.com typically accepts (auction partner sites)
ALEADO_REFERERS = [
    "https://auction.zenautoworks.ca/",
    "https://auc.japancarauc.com/",
    "https://auc.mangaautoimport.ca/",
    "https://auctions.zervtek.com/",
    "https://www.aleado.com/",
]

DEFAULT_REFERER = "https://auction.zenautoworks.ca/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": DEFAULT_REFERER,
}


def requests_fetch(url: str, referer: str | None = None, timeout: int = 15) -> tuple[bytes | None, int, str | None]:
    """
    Fetch image via requests with Referer header (bypasses hotlink protection).

    Returns:
        (body, status_code, error_message)
        body is None on failure.
    """
    headers = {**HEADERS}
    if referer:
        headers["Referer"] = referer.rstrip("/") + "/"
    else:
        headers["Referer"] = DEFAULT_REFERER

    try:
        r = requests.get(url, headers=headers, timeout=timeout, stream=True)
        if r.status_code == 200:
            return (r.content, 200, None)
        return (None, r.status_code, f"HTTP {r.status_code}")
    except requests.RequestException as e:
        return (None, -1, str(e))


def try_fetch_with_referers(url: str, timeout: int = 15) -> tuple[bytes | None, str | None]:
    """
    Try fetching with each known referer until one works.
    Returns (body, used_referer) or (None, None).
    """
    for referer in ALEADO_REFERERS:
        body, status, err = requests_fetch(url, referer=referer, timeout=timeout)
        if body is not None:
            return (body, referer)
    return (None, None)
