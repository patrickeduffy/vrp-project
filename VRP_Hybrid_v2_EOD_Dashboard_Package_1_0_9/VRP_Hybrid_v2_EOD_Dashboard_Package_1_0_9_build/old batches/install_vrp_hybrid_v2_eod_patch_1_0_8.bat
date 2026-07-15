@echo off
setlocal
set "PACKAGE_ROOT=%~dp0"
set "PROJECT_ROOT=C:\Users\patri\vrp_project"

if not exist "%PACKAGE_ROOT%notebooks\vrp_hybrid_v2_signal_publish.py" (
  echo ERROR: Patch source not found: %PACKAGE_ROOT%notebooks\vrp_hybrid_v2_signal_publish.py
  pause
  exit /b 1
)
if not exist "%PROJECT_ROOT%\notebooks" (
  echo ERROR: Project notebooks directory not found: %PROJECT_ROOT%\notebooks
  pause
  exit /b 1
)

if exist "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_signal_publish.py" copy /Y "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_signal_publish.py" "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_signal_publish.py.pre_patch_1_0_8.bak" >nul

copy /Y "%PACKAGE_ROOT%notebooks\vrp_hybrid_v2_signal_publish.py" "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_signal_publish.py" >nul
if errorlevel 1 (
  echo ERROR: Could not install vrp_hybrid_v2_signal_publish.py
  pause
  exit /b 1
)

echo.
echo Hybrid v2 EOD patch 1.0.8 installed.
echo.
echo This patch:
echo   - validates RV21D only on dates consumed by the locked forecast/signal history;
echo   - audits unused rolling warm-up and pre-signal anomalies instead of blocking publication;
echo   - still fails hard for missing, infinite, zero, or negative RV21D on any required signal date;
echo   - prefers a valid duplicate row over an invalid duplicate copy;
echo   - writes vrp_hybrid_v2_rv21d_audit.csv in the staging output.
echo.
echo Close and relaunch Streamlit, then run the normal refresh with Force recalculation OFF.
echo.
pause
exit /b 0
