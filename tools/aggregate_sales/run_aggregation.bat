@echo off
cd /d "C:\Users\Administrator\Desktop\projects\auto_auction\tools\aggregate_sales"
"..\..\.venv\Scripts\python.exe" -u cloud_sync.py > aggregation_log.txt 2>&1
