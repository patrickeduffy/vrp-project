@echo off
setlocal
set "PACKAGE_ROOT=%~dp0"
set "PROJECT_ROOT=C:\Users\patri\vrp_project"

for %%F in (vrp_hybrid_v2_signal_publish.py vrp_hybrid_v2_health_check.py) do (
  if not exist "%PACKAGE_ROOT%notebooks\%%F" (
    echo ERROR: Patch source not found: %PACKAGE_ROOT%notebooks\%%F
    pause
    exit /b 1
  )
)

if not exist "%PROJECT_ROOT%\notebooks" (
  echo ERROR: Project notebooks directory not found: %PROJECT_ROOT%\notebooks
  pause
  exit /b 1
)

for %%F in (vrp_hybrid_v2_signal_publish.py vrp_hybrid_v2_health_check.py) do (
  if exist "%PROJECT_ROOT%\notebooks\%%F" copy /Y "%PROJECT_ROOT%\notebooks\%%F" "%PROJECT_ROOT%\notebooks\%%F.pre_patch_1_0_5.bak" >nul
  copy /Y "%PACKAGE_ROOT%notebooks\%%F" "%PROJECT_ROOT%\notebooks\%%F" >nul
  if errorlevel 1 (
    echo ERROR: Could not install %%F
    pause
    exit /b 1
  )
)

echo.
echo Hybrid v2 EOD patch 1.0.5 installed.
echo.
echo This patch:
echo   - restores the locked missing-alpha fallback used by the historical benchmark reconstruction;
echo   - reselects alpha only when selected_alpha is blank, using train-only yearly walk-forward CV;
echo   - uses the exact grid 1, 10, 100, 300, 1000 and fallback alpha 100;
echo   - preserves the exact train_rows_used and last_forward_rv_date leakage contract;
echo   - updates the final health gate to accept a blank fit-log alpha only under that locked fallback policy.
echo.
echo Close and relaunch Streamlit, then run the normal refresh with Force recalculation OFF.
echo.
pause
exit /b 0
