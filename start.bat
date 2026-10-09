@echo off
REM CMD/Batch Wrapper to run the start.ps1 PowerShell script automatically.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1"
pause
