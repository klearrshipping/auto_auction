@echo off
REM Sales extraction - scheduled run (1 AM daily).
REM Auction Mon-Fri; results available ~9h after close = 1 AM next day.
cd /d "%~dp0..\.."

set LOGDIR=logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
for /f "tokens=*" %%t in ('powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"') do set TIMESTAMP=%%t
set LOGFILE=%LOGDIR%\sales_scheduled_%TIMESTAMP%.log

".venv\Scripts\python.exe" -u operations\sales\run_all.py --scheduled 1> "%LOGFILE%" 2>&1
