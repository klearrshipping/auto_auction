#!/usr/bin/env python3
"""
Run auction listing extraction for a single site and single make/model.
Uses auction_url (not sales_data_url). Extracts listings and saves to Supabase.
Requires: get_market_data.Japan.auction_site_config_JP (credentials from Secret Manager).

Usage:
  python run_single.py --site Zervtek --maker SUZUKI --model SWIFT
  python run_single.py --site Zervtek --maker SUZUKI --model SWIFT --dry-run
  python run_single.py --site Zervtek --maker SUZUKI --model SWIFT --dry-run --visible --output
  (--output saves to data/auction_data/{Make}/{Model}/Japan/ matching sales structure)
"""

import argparse
import asyncio
import os
import sys

_script_dir = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(os.path.dirname(_script_dir))
sys.path.insert(0, _root)

from get_market_data.Japan.auction_site_config_JP import auction_sites
from config.manufacturer_config_JM import manufacturer_configs
from config import config as config_module
from get_market_data.Japan.auction_data.get_data import truly_optimized_main


def main():
    parser = argparse.ArgumentParser(
        description="Extract auction listings for a single site and make/model."
    )
    parser.add_argument("--site", required=True, help="Site name (e.g. Zervtek)")
    parser.add_argument("--maker", required=True, help="Maker (e.g. SUZUKI)")
    parser.add_argument("--model", required=True, help="Model (e.g. SWIFT)")
    parser.add_argument("--dry-run", action="store_true", help="Extract only, do not save to DB")
    parser.add_argument("--visible", action="store_true", help="Show browser window (non-headless)")
    parser.add_argument("--output", nargs="?", const="data/auction_data", default=None,
                       metavar="DIR", help="Save to data folder (default: data/auction_data, same structure as sales)")
    args = parser.parse_args()

    site = args.site
    maker = args.maker.upper()
    model = args.model.upper()

    if site not in auction_sites:
        print(f"Site '{site}' not found. Available: {list(auction_sites.keys())}")
        sys.exit(1)

    if maker not in manufacturer_configs:
        print(f"Maker '{maker}' not in manufacturer_configs.")
        sys.exit(1)

    if model not in manufacturer_configs[maker]:
        print(f"Model '{model}' not in manufacturer_configs['{maker}'].")
        sys.exit(1)

    # Make browser visible for testing
    if args.visible:
        config_module.browser_settings["headless"] = False
        print("Browser will be visible (headless=False)")

    # Resolve output dir (root for data/auction_data structure, same as sales)
    output_file = args.output
    if output_file and not os.path.isabs(output_file):
        output_file = os.path.join(_root, output_file)

    asyncio.run(
        truly_optimized_main(
            dry_run=args.dry_run,
            site_filter=site,
            maker_filter=maker,
            model_filter=model,
            limit=1,
            output_file=output_file,
        )
    )


if __name__ == "__main__":
    main()
