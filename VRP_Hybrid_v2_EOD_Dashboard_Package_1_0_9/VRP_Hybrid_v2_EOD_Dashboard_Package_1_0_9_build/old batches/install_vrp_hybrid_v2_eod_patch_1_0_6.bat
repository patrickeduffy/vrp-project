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
  if exist "%PROJECT_ROOT%\notebooks\%%F" copy /Y "%PROJECT_ROOT%\notebooks\%%F" "%PROJECT_ROOT%\notebooks\%%F.pre_patch_1_0_6.bak" >nul
  copy /Y "%PACKAGE_ROOT%notebooks\%%F" "%PROJECT_ROOT%\notebooks\%%F" >nul
  if errorlevel 1 (
    echo ERROR: Could not install %%F
    pause
    exit /b 1
  )
)

echo.
echo Hybrid v2 EOD patch 1.0.6 installed.
echo.
echo This patch:
echo   - ignores only pre-model fit-log placeholder rows with train_rows_used equal to zero;
echo   - requires every active tenor to retain positive train_rows_used contracts;
echo   - fails if a zero-row contract appears inside or after active fit history;
echo   - preserves the locked missing-alpha walk-forward fallback from patch 1.0.5;
echo   - updates the health gate to report ignored placeholder contracts explicitly.
echo.
echo Close and relaunch Streamlit, then run the normal refresh with Force recalculation OFF.
echo.
pause
exit /b 0
