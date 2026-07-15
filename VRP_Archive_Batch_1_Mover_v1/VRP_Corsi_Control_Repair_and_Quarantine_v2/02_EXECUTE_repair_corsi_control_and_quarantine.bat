@echo off
setlocal
cd /d "%~dp0"
echo.
echo VRP Corsi Control Repair + Quarantine v2 - EXECUTE
echo This recreates the missing staging control panel from its verified in-project duplicate.
echo It then quarantines only 2026-07-12 Corsi panels missing both required SPX columns.
echo.
py "repair_corsi_control_and_quarantine_v2.py" --execute
echo.
pause
endlocal
