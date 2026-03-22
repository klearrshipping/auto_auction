# Run sales extraction on schedule (8 PM daily).
# Auction Mon-Fri; results available ~1 AM JST. Run at 8 PM Europe = 4 AM JST, data ready before 9 AM market open.
# Schedule via Windows Task Scheduler to run at 8:00 PM Mon-Sat.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$logDir = Join-Path $root "logs"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $logDir "sales_scheduled_$timestamp.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$venvPy = Join-Path $root ".venv\Scripts\python.exe"
$scriptPath = Join-Path $root "operations\sales\extract_japan_sales_results.py"

& $venvPy -u $scriptPath --scheduled *> $logFile
