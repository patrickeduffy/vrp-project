@echo off
setlocal
cd /d "%~dp0"
echo.
echo VRP Archive Batch 1 - RESTORE EXECUTE
echo The script will require the phrase: RESTORE BATCH 1
echo.
py "restore_vrp_archive_batch_1_v1.py" --execute
echo.
pause
endlocal
