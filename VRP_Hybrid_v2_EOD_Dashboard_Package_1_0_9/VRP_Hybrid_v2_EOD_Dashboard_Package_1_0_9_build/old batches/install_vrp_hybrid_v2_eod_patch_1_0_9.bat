@echo off
setlocal
set "PACKAGE_ROOT=%~dp0"
set "PROJECT_ROOT=C:\Users\patri\vrp_project"

if not exist "%PACKAGE_ROOT%notebooks\vrp_hybrid_v2_health_check.py" (
  echo ERROR: Patch source not found: %PACKAGE_ROOT%notebooks\vrp_hybrid_v2_health_check.py
  pause
  exit /b 1
)
if not exist "%PROJECT_ROOT%\notebooks" (
  echo ERROR: Project notebooks directory not found: %PROJECT_ROOT%\notebooks
  pause
  exit /b 1
)

if exist "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_health_check.py" copy /Y "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_health_check.py" "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_health_check.py.pre_patch_1_0_9.bak" >nul

copy /Y "%PACKAGE_ROOT%notebooks\vrp_hybrid_v2_health_check.py" "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_health_check.py" >nul
if errorlevel 1 (
  echo ERROR: Could not install vrp_hybrid_v2_health_check.py
  pause
  exit /b 1
)

echo.
echo Hybrid v2 EOD patch 1.0.9 installed.
echo.
echo This patch changes only the final semantic health gate:
echo   - treats the forecast benchmark as a multi-spec historical date-grid anchor;
echo   - validates distinct date x tenor keys instead of rejecting expected raw duplicates;
echo   - accepts canonical SPY close aliases including eod_close;
echo   - still requires a finite, strictly positive SPY close on the target date;
echo   - leaves model, signal, sizing, selection, and published data unchanged.
echo.
echo Close and relaunch Streamlit, then run the normal refresh with Force recalculation OFF.
echo.
pause
exit /b 0
