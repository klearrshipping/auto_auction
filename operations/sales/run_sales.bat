@echo off
cd /d "%~dp0..\.."
".venv\Scripts\python.exe" -u operations\sales\run_all.py %*
