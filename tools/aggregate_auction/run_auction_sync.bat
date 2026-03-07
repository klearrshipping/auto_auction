@echo off
cd /d "%~dp0"
"..\..\.venv\Scripts\python.exe" -u cloud_sync.py %*
