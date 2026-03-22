# Auto Auction

Extracts sales data from Japanese auction sites for configured makes/models.

## Setup

1. `pip install -r requirements.txt`
2. `playwright install chromium`
3. Configure GCP Secret Manager with auction site credentials (see `config/secrets_manager.py`)
4. Set `GOOGLE_APPLICATION_CREDENTIALS` to your service account key path

## Quick run (simplest)

From the **project root** (double-click or run in a terminal):

| What you want | Run |
|----------------|-----|
| **Sales** (sold results → `data/sales_data/`) | `run_sales.bat` |
| **Auction** (listings pipeline → `data/auction_data/` + Supabase) | `run_auction.bat` |

Logs go under `logs\`. For **Task Scheduler** with no “press a key” prompt, use `operations\sales\run_sales_scheduled.bat` and `operations\auction\run_auction_scheduled.bat` instead.

## Usage

- **Single date, all sites (Japan sold-results / sales):** `python -u operations/sales/extract_japan_sales_results.py --date 2026-01-05`
- **Auction listings pipeline (inventory, not sold-results):** `python -u operations/auction/run_japan_auction_pipeline.py` — prune → listings → details → compile → Supabase (see `operations/auction/pipeline/extract_auction_listings.py` for the listings step only)
- **Japan working days (auto):** `python -u operations/sales/run_workdays.py` — processes next pending date from `config/working_days.json`
- **Daily schedule:** Run `schedule_workdays_daily.bat` as Administrator to run extraction automatically each day at 2 AM
- **Lot details (Sales):** `python -u operations/sales/run_details.py` — fetches details for pending `_lot_urls.json` files

## Operations

The system is organized into major operational modules:
- `operations/sales/`: Japan **sold-results** extraction (`extract_japan_sales_results.py`), optional lot details (`run_details.py`), and helpers. Legacy backfill scripts were removed; old progress files named `backfill_7_10yr_*.json` may still exist under `data/sales_data/_progress/` for reference only.
- `operations/auction/`: Scripts for live/upcoming auction **listings** data.

## Config

- `config/manufacturer_config_JM.py` — makes/models to extract
- `config/config.py` — browser (headless) and extraction settings
- `config/working_days.json` — checklist of Japan working days
