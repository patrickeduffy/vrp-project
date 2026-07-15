@echo off
setlocal
set "PACKAGE_ROOT=%~dp0"
set "PROJECT_ROOT=C:\Users\patri\vrp_project"
set "COMMON_SOURCE=%PACKAGE_ROOT%notebooks\vrp_hybrid_v2_common.py"
set "RSI_SOURCE=%PACKAGE_ROOT%notebooks\vrp_hybrid_v2_wilder_rsi_update.py"
set "COMMON_TARGET=%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_common.py"
set "RSI_TARGET=%PROJECT_ROOT%\notebooks\vrp_hybrid_v2_wilder_rsi_update.py"

if not exist "%COMMON_SOURCE%" (
  echo ERROR: Patch source not found: %COMMON_SOURCE%
  pause
  exit /b 1
)
if not exist "%RSI_SOURCE%" (
  echo ERROR: Patch source not found: %RSI_SOURCE%
  pause
  exit /b 1
)
if not exist "%PROJECT_ROOT%\notebooks" (
  echo ERROR: Project notebooks directory not found: %PROJECT_ROOT%\notebooks
  pause
  exit /b 1
)

if exist "%COMMON_TARGET%" copy /Y "%COMMON_TARGET%" "%COMMON_TARGET%.pre_patch_1_0_3.bak" >nul
if exist "%RSI_TARGET%" copy /Y "%RSI_TARGET%" "%RSI_TARGET%.pre_patch_1_0_3.bak" >nul

copy /Y "%COMMON_SOURCE%" "%COMMON_TARGET%" >nul
if errorlevel 1 (
  echo ERROR: Could not install shared date-normalization patch.
  pause
  exit /b 1
)
copy /Y "%RSI_SOURCE%" "%RSI_TARGET%" >nul
if errorlevel 1 (
  echo ERROR: Could not install cumulative RSI updater patch.
  pause
  exit /b 1
)

echo.
echo Hybrid v2 EOD patch 1.0.3 installed:
echo   %COMMON_TARGET%
echo   %RSI_TARGET%
echo.
echo This cumulative patch:
echo   - disables the blocked historical ThetaData SPY request;
echo   - safely audits equivalent duplicate RSI states;
echo   - parses integer YYYYMMDD dates as calendar dates instead of 1970 timestamps.
echo.
echo Close and relaunch Streamlit, then run the normal refresh with Force recalculation OFF.
echo.
pause
exit /b 0
