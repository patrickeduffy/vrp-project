@echo off
setlocal
cd /d "%~dp0"
echo.
echo VRP Archive Batch 1 - RESTORE DRY RUN
echo No files will move.
echo.
py "restore_vrp_archive_batch_1_v1.py"
echo.
pause
endlocal
