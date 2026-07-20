@echo off
setlocal
title VRP Hybrid v2 EOD Dashboard

set "PROJECT_ROOT=%~dp0"
set "PYTHON_EXE=C:\Users\patri\AppData\Local\Programs\Python\Python313\python.exe"
set "STREAMLIT_APP=%PROJECT_ROOT%notebooks\streamlit_vrp_hybrid_v2_eod.py"

if not exist "%PYTHON_EXE%" (
    echo Python was not found at:
    echo %PYTHON_EXE%
    echo.
    echo Update PYTHON_EXE in this launcher if Python has moved.
    pause
    exit /b 1
)

if not exist "%STREAMLIT_APP%" (
    echo The VRP Streamlit application was not found at:
    echo %STREAMLIT_APP%
    pause
    exit /b 1
)

cd /d "%PROJECT_ROOT%notebooks"
"%PYTHON_EXE%" -m streamlit run "%STREAMLIT_APP%"

if errorlevel 1 (
    echo.
    echo The VRP dashboard stopped with an error.
    pause
)

endlocal
