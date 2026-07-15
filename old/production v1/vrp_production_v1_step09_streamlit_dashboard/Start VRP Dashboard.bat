@echo off
setlocal
cd /d "%~dp0"
echo Installing/updating dashboard requirements...
py -m pip install -r requirements.txt
if errorlevel 1 (
  echo.
  echo Requirement installation failed. Press any key to close.
  pause >nul
  exit /b 1
)
echo.
echo Starting VRP Production Control Center...
echo A browser window should open automatically.
py -m streamlit run app.py -- --project-root "C:\Users\patri\vrp_project"
pause
