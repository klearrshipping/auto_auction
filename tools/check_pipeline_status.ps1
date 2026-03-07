# Check auction pipeline status using the run marker (reliable).
# Usage: .\check_pipeline_status.ps1

$root = Split-Path -Parent $PSScriptRoot
$markerPath = Join-Path $root "logs\auction_pipeline_run.json"

if (-not (Test-Path $markerPath)) {
    Write-Host "No pipeline run marker found. Pipeline may not have been run yet."
    exit 0
}

$marker = Get-Content $markerPath -Raw | ConvertFrom-Json
Write-Host "Status: $($marker.status)"
Write-Host "Started: $($marker.started_at)"
if ($marker.completed_at) { Write-Host "Completed: $($marker.completed_at)" }
if ($marker.failed_at) { Write-Host "Failed: $($marker.failed_at) - $($marker.error)" }
if ($marker.log_file) { Write-Host "Log: $($marker.log_file)" }
