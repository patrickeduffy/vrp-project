@echo off
setlocal
set "PACKAGE_ROOT=%~dp0"
set "PROJECT_ROOT=C:\Users\patri\vrp_project"

if not exist "%PACKAGE_ROOT%notebooks\vrp_hybrid_v2_eod_pipeline.py" (
  echo ERROR: Patch source not found: %PACKAGE_ROOT%notebooks\vrp_hybrid_v2_eod_pipeline.py
  pause
  exit /b 1
)
if not exist "%PACKAGE_ROOT%config\vrp_hybrid_v2_eod_runtime_config.json" (
  echo ERROR: Patch config not found: %PACKAGE_ROOT%config\vrp_hybrid_v2_eod_runtime_config.json
  pause
  exit /b 1
)
if not exist "%PROJECT_ROOT%\notebooks" (
  echo ERROR: Project notebooks directory not found: %PROJECT_ROOT%\notebooks
  pause
  exit /b 1
)
if not exist "%PROJECT_ROOT%\config" (
  echo ERROR: Project config directory not found: %PROJECT_ROOT%\config
  pause
  exit /b 1
)

if exist "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_eod_pipeline.py" copy /Y "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_eod_pipeline.py" "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_eod_pipeline.py.pre_patch_1_0_7.bak" >nul
if exist "%PROJECT_ROOT%\config\vrp_hybrid_v2_eod_runtime_config.json" copy /Y "%PROJECT_ROOT%\config\vrp_hybrid_v2_eod_runtime_config.json" "%PROJECT_ROOT%\config\vrp_hybrid_v2_eod_runtime_config.json.pre_patch_1_0_7.bak" >nul

copy /Y "%PACKAGE_ROOT%notebooks\vrp_hybrid_v2_eod_pipeline.py" "%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_eod_pipeline.py" >nul
if errorlevel 1 (
  echo ERROR: Could not install vrp_hybrid_v2_eod_pipeline.py
  pause
  exit /b 1
)
copy /Y "%PACKAGE_ROOT%config\vrp_hybrid_v2_eod_runtime_config.json" "%PROJECT_ROOT%\config\vrp_hybrid_v2_eod_runtime_config.json" >nul
if errorlevel 1 (
  echo ERROR: Could not install vrp_hybrid_v2_eod_runtime_config.json
  pause
  exit /b 1
)

echo.
echo Hybrid v2 EOD patch 1.0.7 installed.
echo.
echo This patch:
echo   - validates the updater's stable implied-variance surface before publication;
echo   - synchronizes the validated target-date grid to the canonical legacy alias;
echo   - passes that exact path explicitly to the Hybrid v2 publisher;
echo   - writes implied-variance handoff audit files in every run directory;
echo   - adds the stable updater surface to transactional backup targets.
echo.
echo Close and relaunch Streamlit, then run the normal refresh with Force recalculation OFF.
echo.
pause
exit /b 0
