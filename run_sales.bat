@echo off
REM Extract SALES data (Japan sold auction results) -> data\sales_data\
REM Double-click or: run_sales
cd /d "%~dp0"

set LOGDIR=logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"
for /f "tokens=*" %%t in ('powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"') do set TIMESTAMP=%%t
set LOGFILE=%LOGDIR%\sales_%TIMESTAMP%.log

echo Running sales extraction... Log: %LOGFILE%
".venv\Scripts\python.exe" -u operations\sales\extract_japan_sales_results.py --scheduled 1> "%LOGFILE%" 2>&1
echo Done. Exit code: %ERRORLEVEL%
pause
