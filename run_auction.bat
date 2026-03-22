@echo off
REM Extract AUCTION listings (inventory pipeline) -> data\auction_data\ + Supabase
REM Double-click or: run_auction
cd /d "%~dp0"

set LOGDIR=logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
for /f "tokens=*" %%t in ('powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"') do set TIMESTAMP=%%t
set LOGFILE=%LOGDIR%\auction_%TIMESTAMP%.log

echo Running auction pipeline... Log: %LOGFILE%
".venv\Scripts\python.exe" -u operations\auction\run_japan_auction_pipeline.py --replace 1> "%LOGFILE%" 2>&1
echo Done. Exit code: %ERRORLEVEL%
pause
