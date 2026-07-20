@echo off
setlocal

set "PROJECT_ROOT=C:\Users\patri\vrp_project"
set "PYTHON_EXE=C:\Users\patri\AppData\Local\Programs\Python\Python313\python.exe"
set "DASHBOARD=%PROJECT_ROOT%\notebooks\streamlit_vrp_hybrid_v2_eod.py"

title VRP Hybrid v2 EOD Dashboard

if not exist "%PYTHON_EXE%" (
    echo ERROR: Python was not found at:
    echo %PYTHON_EXE%
    echo.
    pause
    exit /b 1
)

if not exist "%DASHBOARD%" (
    echo ERROR: The Hybrid v2 dashboard was not found at:
    echo %DASHBOARD%
    echo.
    pause
    exit /b 1
)

cd /d "%PROJECT_ROOT%\notebooks"
"%PYTHON_EXE%" -m streamlit run "%DASHBOARD%"

set "EXIT_CODE=%ERRORLEVEL%"
echo.
echo VRP Hybrid v2 dashboard exited with code %EXIT_CODE%.
pause
exit /b %EXIT_CODE%
