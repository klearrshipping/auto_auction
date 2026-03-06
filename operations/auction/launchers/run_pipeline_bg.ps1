# Run auction pipeline in background with progress logs
# Usage: .\run_pipeline_bg.ps1
#        .\run_pipeline_bg.ps1 -Resume   # Skip prune+listings, resume from details

param([switch]$Resume)

$ErrorActionPreference = "Stop"
$auctionDir = Split-Path -Parent $PSScriptRoot
$root = Split-Path -Parent (Split-Path -Parent $auctionDir)
$logDir = Join-Path $root "logs"
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = Join-Path $logDir "auction_pipeline_$timestamp.log"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

Write-Host "Starting auction pipeline in background..."
Write-Host "Progress log: $logFile"
Write-Host "Monitor with: Get-Content '$logFile' -Wait -Tail 50"
Write-Host ""

$venvPy = Join-Path $root ".venv\Scripts\python.exe"
if (Test-Path $venvPy) {
    $py = $venvPy
} else {
    $py = (Get-Command py -ErrorAction SilentlyContinue).Source
    if (-not $py) { $py = (Get-Command python -ErrorAction SilentlyContinue).Source }
    if (-not $py) { $py = "python" }
}

$errFile = Join-Path $logDir "auction_pipeline_${timestamp}_err.log"
$args = @("-u", (Join-Path $auctionDir "auction_manager.py"))
if ($Resume) { $args += "--resume"; Write-Host "Resume mode: skipping prune and listings" }
$psi = @{
    FilePath = $py
    ArgumentList = $args
    WorkingDirectory = $root
    RedirectStandardOutput = $logFile
    RedirectStandardError = $errFile
    NoNewWindow = $false
    PassThru = $true
}

$proc = Start-Process @psi
Write-Host "Pipeline started (PID: $($proc.Id))."
Write-Host "Progress: $logFile"
Write-Host "Errors:  $errFile"
