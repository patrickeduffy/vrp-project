@echo off
setlocal
cd /d "%~dp0"
echo.
echo VRP Corsi Control Repair + Quarantine v2 - DRY RUN
echo No files will be copied or moved.
echo.
py "repair_corsi_control_and_quarantine_v2.py"
echo.
pause
endlocal
