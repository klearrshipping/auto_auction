#!/usr/bin/env python3
"""
Backward-compatible entry point. Prefer: run_japan_auction_pipeline.py
(Japan auction listings pipeline: prune → listings → details → compile → sync).
"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

if __name__ == "__main__":
    target = Path(__file__).resolve().parent / "run_japan_auction_pipeline.py"
    print(
        "NOTE: auction_manager.py is deprecated; use run_japan_auction_pipeline.py",
        file=sys.stderr,
        flush=True,
    )
    runpy.run_path(str(target), run_name="__main__")
