@echo off
setlocal
cd /d "%~dp0"
echo Starting VRP Production Control Center v3...
echo.
echo This window must stay open while the dashboard is running.
echo Close this window to stop the dashboard.
echo.
py -m pip install -r requirements.txt
py -m streamlit run app.py --server.port 8503 -- --project-root "C:\Users\patri\vrp_project"
pause
