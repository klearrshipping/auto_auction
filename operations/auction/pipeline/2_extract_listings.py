#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backward-compatible entry point. Prefer: extract_auction_listings.py
(Japan auction listing extraction step).
"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "extract_auction_listings.py"
    print(
        "NOTE: 2_extract_listings.py is deprecated; use extract_auction_listings.py",
        file=sys.stderr,
        flush=True,
    )
    runpy.run_path(str(target), run_name="__main__")
