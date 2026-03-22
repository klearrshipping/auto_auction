# Create Windows Task Scheduler entry for auction extraction at 11:00 AM daily.
# Run this script as Administrator to register the task.
#
# Runs full pipeline: prune expired lots -> extract listings -> fetch details -> compile -> Supabase sync -> valuation.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$batPath = Join-Path $root "operations\auction\run_auction_scheduled.bat"

$taskName = "AutoAuction_AuctionExtraction"
$taskDescription = "Auction pipeline: prune, listings, details, compile, sync, valuation. Runs daily at 11:00 AM."
$trigger = New-ScheduledTaskTrigger -Daily -At "11:00AM"
$action = New-ScheduledTaskAction -Execute $batPath -WorkingDirectory $root
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd

Register-ScheduledTask -TaskName $taskName -Description $taskDescription `
    -Action $action -Trigger $trigger -Settings $settings -Force

Write-Host "Task '$taskName' registered. Runs daily at 11:00 AM."
Write-Host "To run manually: Start-ScheduledTask -TaskName '$taskName'"
Write-Host "To remove: Unregister-ScheduledTask -TaskName '$taskName'"
