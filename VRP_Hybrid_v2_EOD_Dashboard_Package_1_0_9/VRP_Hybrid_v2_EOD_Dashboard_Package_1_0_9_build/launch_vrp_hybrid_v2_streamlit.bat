@echo off
setlocal
set "PROJECT_ROOT=C:\Users\patri\vrp_project"
title VRP Hybrid v2 EOD Dashboard
cd /d "%PROJECT_ROOT%\notebooks"
py -m streamlit run ".\streamlit_vrp_hybrid_v2_eod.py"
set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo Streamlit exited with code %EXIT_CODE%.
pause
exit /b %EXIT_CODE%
