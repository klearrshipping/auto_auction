@echo off
cd /d "%~dp0..\.."
".venv\Scripts\python.exe" -u operations\sales\extract_japan_sales_results.py %*
