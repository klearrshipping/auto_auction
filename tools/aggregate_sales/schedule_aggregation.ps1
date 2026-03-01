$batPath = "C:\Users\Administrator\Desktop\projects\auto_auction\tools\aggregate_sales\run_aggregation.bat"
schtasks /create /sc daily /st 06:00 /tn "AutoAuctionDailyCleaner" /tr $batPath /f

