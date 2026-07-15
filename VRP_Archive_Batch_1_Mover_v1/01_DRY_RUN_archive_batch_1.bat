@echo off
setlocal
cd /d "%~dp0"
echo.
echo VRP Archive Batch 1 - DRY RUN
echo No files will move.
echo.
py "archive_vrp_batch_1_v1.py" --dry-run
echo.
pause
endlocal
