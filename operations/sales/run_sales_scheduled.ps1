# Run sales extraction on schedule (1 AM daily).
# Auction Mon-Fri; results available ~9h after close = 1 AM next day. Extracts (today - 1 day).
# Schedule via Windows Task Scheduler to run at 1:00 AM daily.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$logDir = Join-Path $root "logs"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $logDir "sales_scheduled_$timestamp.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$venvPy = Join-Path $root ".venv\Scripts\python.exe"
$scriptPath = Join-Path $root "operations\sales\run_all.py"

& $venvPy -u $scriptPath --scheduled *> $logFile
