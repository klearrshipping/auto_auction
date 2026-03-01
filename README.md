# Auto Auction

Extracts sales data from Japanese auction sites for configured makes/models.

## Setup

1. `pip install -r requirements.txt`
2. `playwright install chromium`
3. Configure GCP Secret Manager with auction site credentials (see `config/secrets_manager.py`)
4. Set `GOOGLE_APPLICATION_CREDENTIALS` to your service account key path

## Usage

- **Single date, all sites (Sales):** `python -u operations/sales/run_all.py --date 2026-01-05`
- **Japan working days (auto):** `python -u operations/sales/run_workdays.py` — processes next pending date from `config/working_days.json`
- **Daily schedule:** Run `schedule_workdays_daily.bat` as Administrator to run extraction automatically each day at 2 AM
- **Lot details (Sales):** `python -u operations/sales/run_details.py` — fetches details for pending `_lot_urls.json` files

## Operations

The system is organized into major operational modules:
- `operations/sales/`: Scripts for Japanese auction sales data (historical and current).
- `operations/auction/`: Scripts for live/upcoming auction data.

## Config

- `config/manufacturer_config_JM.py` — makes/models to extract
- `config/config.py` — browser (headless) and extraction settings
- `config/working_days.json` — checklist of Japan working days
