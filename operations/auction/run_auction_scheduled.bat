@echo off
REM Auction extraction - scheduled run (11 AM daily).
REM Runs full pipeline: prune -> listings -> details -> compile -> sync -> valuation.
cd /d "%~dp0..\.."

set LOGDIR=logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
for /f "tokens=*" %%t in ('powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"') do set TIMESTAMP=%%t
set LOGFILE=%LOGDIR%\auction_scheduled_%TIMESTAMP%.log

".venv\Scripts\python.exe" -u operations\auction\run_japan_auction_pipeline.py --replace 1> "%LOGFILE%" 2>&1
