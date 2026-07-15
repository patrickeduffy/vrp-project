@echo off
setlocal
cd /d "%~dp0"
echo Starting VRP Production v1 Dashboard v4...
echo.
py -m pip install -r requirements.txt
py -m streamlit run app.py --server.port 8504
pause
