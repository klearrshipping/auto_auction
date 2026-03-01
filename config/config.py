# config.py
"""
Runtime Configuration for Auction Data Collection System
"""

import logging

# Logging configuration (used by get_market_data.Japan.auction_data.get_data)
logging_config = {
    "level": logging.INFO,
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "files": {
        "main": "auction_data_main.log",
        "listing": "auction_data_listing.log",
    }
}

# Browser settings
browser_settings = {
    "headless": True,
    # Auction listing extraction: number of browsers in pool. Use 1 when another script
    # (e.g. sales details with 15 browsers) is running; increase when running alone.
    "browser_pool_size": 1,
}

# Runtime settings
runtime_settings = {
    # Sales data: max concurrent lot-detail page fetches (get_sales_details)
    "concurrent_lot_details": 2,
    # Seconds to wait between each batch of concurrent requests
    "detail_batch_delay": 3,
    # Abort if this many consecutive lots return no meaningful data
    "max_consecutive_empty": 5,
    # Abort if first N lots sampled yield 0 meaningful data (early termination check)
    "early_check_after": 3,
}

# Export configurations
__all__ = [
    "logging_config",
    "browser_settings",
    "runtime_settings",
]
