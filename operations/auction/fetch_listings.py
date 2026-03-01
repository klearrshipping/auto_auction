import os
import sys
import asyncio

# Setup pathing to allow imports from project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

try:
    from get_market_data.Japan.auction_data.get_data import truly_optimized_main
except ImportError as e:
    print(f"Error: Could not import auction engine. {e}")
    sys.exit(1)

async def run():
    print("=== Auction Listing Extraction Starting ===")
    try:
        await truly_optimized_main()
    except Exception as e:
        print(f"Fatal error during listing extraction: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(run())
