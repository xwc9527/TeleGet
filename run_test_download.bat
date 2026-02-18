@echo off
chcp 65001 >nul 2>&1
cd /d "%~dp0"
python test_real_download.py
pause
