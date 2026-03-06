#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract auction listings for all makes/models from manufacturer_config_JM across auction sites.
Uses auction_url (not sales_data_url). Saves to JSON (data/auction_data/).
Requires: get_market_data.Japan.auction_site_config_JP (credentials from Secret Manager).

Usage:
  python 2_extract_listings.py                          # all sites, all make/models
  python 2_extract_listings.py --site Zervtek            # one site only
  python 2_extract_listings.py --maker TOYOTA            # one maker only
  python 2_extract_listings.py --limit 5                 # first 5 make/model jobs (test run)
  python 2_extract_listings.py --dry-run                 # extract only (no JSON save)
"""

import argparse
import asyncio
import os
import sys

_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(os.path.dirname(os.path.dirname(_script_dir)))
sys.path.insert(0, _root)

from get_market_data.Japan.auction_data.get_data import truly_optimized_main


def main():
    parser = argparse.ArgumentParser(
        description="Extract auction listings for all makes/models."
    )
    parser.add_argument("--site", help="Run on one site only (e.g. Zervtek)")
    parser.add_argument("--maker", help="One maker only (e.g. TOYOTA)")
    parser.add_argument("--limit", type=int, default=0, help="Max jobs to run (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Extract only, do not save to JSON")
    args = parser.parse_args()

    asyncio.run(
        truly_optimized_main(
            dry_run=args.dry_run,
            site_filter=args.site,
            maker_filter=args.maker,
            model_filter=None,
            limit=args.limit if args.limit > 0 else 0,
            output_file=None if args.dry_run else "data/auction_data",
        )
    )


if __name__ == "__main__":
    main()
