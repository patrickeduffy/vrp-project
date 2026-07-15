@echo off
setlocal
cd /d "%~dp0"
echo.
echo VRP Archive Batch 1 - EXECUTE
echo Close Streamlit and make sure no VRP pipeline is running.
echo The script will require the phrase: ARCHIVE BATCH 1
echo.
py "archive_vrp_batch_1_v1.py" --execute
echo.
pause
endlocal
