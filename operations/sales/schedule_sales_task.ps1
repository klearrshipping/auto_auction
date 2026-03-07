# Create Windows Task Scheduler entry for sales extraction at 1:00 AM daily.
# Run this script as Administrator to register the task.
#
# Rule: Auction Mon-Fri; results available ~9h after close = 1 AM next day.
# The task runs at 1:00 AM; --scheduled extracts (today - 1 day).

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$batPath = Join-Path $root "operations\sales\run_sales_scheduled.bat"

$taskName = "AutoAuction_SalesExtraction"
$taskDescription = "Extract Japan auction sales data. Runs at 1 AM Tue-Sat; extracts yesterday's auction (Mon-Fri auctions only)."
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Tuesday, Wednesday, Thursday, Friday, Saturday -At "1:00AM"
$action = New-ScheduledTaskAction -Execute $batPath -WorkingDirectory $root
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd

Register-ScheduledTask -TaskName $taskName -Description $taskDescription `
    -Action $action -Trigger $trigger -Settings $settings -Force

Write-Host "Task '$taskName' registered. Runs daily at 1:00 AM."
Write-Host "To run manually: Start-ScheduledTask -TaskName '$taskName'"
Write-Host "To remove: Unregister-ScheduledTask -TaskName '$taskName'"
