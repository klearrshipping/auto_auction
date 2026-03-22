# Create Windows Task Scheduler entry for sales extraction at 8:00 PM daily.
# Run this script as Administrator to register the task.
#
# Rule: Auction Mon-Fri; results available ~1 AM JST next day.
# Run at 8 PM Europe = 4 AM JST next day, so data is ready ~5h before 9 AM market open.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$batPath = Join-Path $root "operations\sales\run_sales_scheduled.bat"

$taskName = "AutoAuction_SalesExtraction"
$taskDescription = "Extract Japan auction sales data. Runs at 8 PM Mon-Sat (4 AM JST); data ready before 9 AM market open."
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday, Tuesday, Wednesday, Thursday, Friday, Saturday -At "8:00PM"
$action = New-ScheduledTaskAction -Execute $batPath -WorkingDirectory $root
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopOnIdleEnd

Register-ScheduledTask -TaskName $taskName -Description $taskDescription `
    -Action $action -Trigger $trigger -Settings $settings -Force

Write-Host "Task '$taskName' registered. Runs daily at 8:00 PM (4 AM JST)."
Write-Host "To run manually: Start-ScheduledTask -TaskName '$taskName'"
Write-Host "To remove: Unregister-ScheduledTask -TaskName '$taskName'"
