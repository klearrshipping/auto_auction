# Run check_sales_published in background. Checks every hour until results found.
# When results are detected, script terminates and logs: SALES DATA FIRST AVAILABLE AT: <timestamp>
# Monitor: Get-Content logs\check_sales_published_*.log -Wait -Tail 20

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $root "logs"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $logDir "check_sales_published_$timestamp.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

Write-Host "Starting check_sales_published in background."
Write-Host "Log: $logFile"
Write-Host "Monitor: Get-Content '$logFile' -Wait -Tail 20"
Write-Host ""

$venvPy = Join-Path $root ".venv\Scripts\python.exe"
$args = @("-u", (Join-Path $root "tests\check_sales_published.py"), "--interval", "60")
$psi = @{
    FilePath = $venvPy
    ArgumentList = $args
    WorkingDirectory = $root
    RedirectStandardOutput = $logFile
    RedirectStandardError = (Join-Path $logDir "check_sales_err.log")
    NoNewWindow = $false
    PassThru = $true
}

$proc = Start-Process @psi
Write-Host "Started (PID: $($proc.Id)). When results are found, check log for SALES DATA FIRST AVAILABLE AT."
