@echo off
setlocal enabledelayedexpansion
set "PACKAGE_ROOT=%~dp0"
set "PROJECT_ROOT=C:\Users\patri\vrp_project"

if not exist "%PROJECT_ROOT%" (
  echo ERROR: Project root does not exist: %PROJECT_ROOT%
  pause
  exit /b 1
)

if not exist "%PROJECT_ROOT%\notebooks" mkdir "%PROJECT_ROOT%\notebooks"
if not exist "%PROJECT_ROOT%\config" mkdir "%PROJECT_ROOT%\config"
if not exist "%PROJECT_ROOT%\docs" mkdir "%PROJECT_ROOT%\docs"

for %%F in (vrp_hybrid_v2_common.py vrp_hybrid_v2_health_check.py vrp_hybrid_v2_wilder_rsi_update.py vrp_hybrid_v2_signal_publish.py vrp_hybrid_v2_eod_pipeline.py streamlit_vrp_hybrid_v2_eod.py) do (
  copy /Y "%PACKAGE_ROOT%notebooks\%%F" "%PROJECT_ROOT%\notebooks\%%F" >nul
  if errorlevel 1 (
    echo ERROR copying %%F
    pause
    exit /b 1
  )
)

copy /Y "%PACKAGE_ROOT%config\vrp_hybrid_v2_eod_runtime_config.json" "%PROJECT_ROOT%\config\vrp_hybrid_v2_eod_runtime_config.json" >nul
copy /Y "%PACKAGE_ROOT%config\vrp_corsi_intraday_hybrid_v2_production_config.json" "%PROJECT_ROOT%\config\vrp_corsi_intraday_hybrid_v2_production_config.json" >nul
copy /Y "%PACKAGE_ROOT%config\vrp_corsi_intraday_hybrid_v2_lock.json" "%PROJECT_ROOT%\config\vrp_corsi_intraday_hybrid_v2_lock.json" >nul
copy /Y "%PACKAGE_ROOT%docs\VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx" "%PROJECT_ROOT%\docs\VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx" >nul
copy /Y "%PACKAGE_ROOT%docs\VRP_Corsi_Intraday_Hybrid_v2_Production_Runbook.docx" "%PROJECT_ROOT%\docs\VRP_Corsi_Intraday_Hybrid_v2_Production_Runbook.docx" >nul
copy /Y "%PACKAGE_ROOT%EOD_DASHBOARD_INTEGRATION_ADDENDUM.txt" "%PROJECT_ROOT%\docs\EOD_DASHBOARD_INTEGRATION_ADDENDUM.txt" >nul
copy /Y "%PACKAGE_ROOT%README.txt" "%PROJECT_ROOT%\docs\VRP_Hybrid_v2_EOD_Dashboard_README.txt" >nul
copy /Y "%PACKAGE_ROOT%launch_vrp_hybrid_v2_streamlit.bat" "%PROJECT_ROOT%\launch_vrp_hybrid_v2_streamlit.bat" >nul
copy /Y "%PACKAGE_ROOT%run_vrp_hybrid_v2_eod_once.bat" "%PROJECT_ROOT%\run_vrp_hybrid_v2_eod_once.bat" >nul
copy /Y "%PACKAGE_ROOT%requirements_vrp_hybrid_v2_eod.txt" "%PROJECT_ROOT%\requirements_vrp_hybrid_v2_eod.txt" >nul

if errorlevel 1 (
  echo ERROR copying configuration, documentation, or launcher files.
  pause
  exit /b 1
)

echo.
echo Hybrid v2 EOD dashboard files installed under:
echo   %PROJECT_ROOT%\notebooks
echo   %PROJECT_ROOT%\config
echo   %PROJECT_ROOT%\docs
echo.
echo Install or refresh Python dependencies with:
echo   py -m pip install -r "%PROJECT_ROOT%\requirements_vrp_hybrid_v2_eod.txt"
echo.
echo Then launch:
echo   %PROJECT_ROOT%\launch_vrp_hybrid_v2_streamlit.bat
echo.
pause
exit /b 0
