@echo off
setlocal
set "PACKAGE_ROOT=%~dp0"
set "PROJECT_ROOT=C:\Users\patri\vrp_project"

for %%F in (vrp_hybrid_v2_common.py vrp_hybrid_v2_wilder_rsi_update.py vrp_hybrid_v2_signal_publish.py vrp_hybrid_v2_eod_pipeline.py) do (
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

for %%F in (vrp_hybrid_v2_common.py vrp_hybrid_v2_wilder_rsi_update.py vrp_hybrid_v2_signal_publish.py vrp_hybrid_v2_eod_pipeline.py) do (
  if exist "%PROJECT_ROOT%\notebooks\%%F" copy /Y "%PROJECT_ROOT%\notebooks\%%F" "%PROJECT_ROOT%\notebooks\%%F.pre_patch_1_0_4.bak" >nul
  copy /Y "%PACKAGE_ROOT%notebooks\%%F" "%PROJECT_ROOT%\notebooks\%%F" >nul
  if errorlevel 1 (
    echo ERROR: Could not install %%F
    pause
    exit /b 1
  )
)

echo.
echo Hybrid v2 EOD cumulative patch 1.0.4 installed.
echo.
echo This patch includes all prior repairs and additionally:
echo   - builds the locked 5D, 21D, and 63D intraday predictors from the canonical Corsi component source;
echo   - joins those date-level predictors to the baseline feature panel before Ridge scoring;
echo   - passes the exact refreshed component source from the EOD runner to the publisher;
echo   - writes a staged intraday-feature audit and still enforces the locked forecast-reference check.
echo.
echo Close and relaunch Streamlit, then run the normal refresh with Force recalculation OFF.
echo.
pause
exit /b 0
