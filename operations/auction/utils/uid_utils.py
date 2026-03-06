"""
UID utilities for auction pipeline.
Extract lot_id from URL and generate deterministic hashes for deduplication.
"""

import hashlib
import re


def extract_lot_id_from_url(lot_link: str | None) -> str | None:
    """
    Extract id= parameter from lot_link.
    Example: ...?p=project/lot&id=972055623&s -> 972055623
    Fallback: return full URL if no id found (for other site formats).
    """
    if not lot_link or not isinstance(lot_link, str):
        return None
    lot_link = lot_link.strip()
    if not lot_link:
        return None
    m = re.search(r"[?&]id=(\d+)", lot_link, re.I)
    return m.group(1) if m else lot_link


def listing_uid(site_name: str, lot_link: str | None, fallback_lot_number: str = "", fallback_auction: str = "") -> str:
    """
    16-char hash for O(1) lookup. Resistant to URL param changes.
    Uses lot_id from URL when available; falls back to lot_number+auction if URL missing.
    """
    lot_id = extract_lot_id_from_url(lot_link) if lot_link else None
    if lot_id:
        raw = f"{site_name}|{lot_id}"
    else:
        raw = f"{site_name}|{fallback_lot_number}|{fallback_auction}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
