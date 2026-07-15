@echo off
setlocal
set "PROJECT_ROOT=C:\Users\patri\vrp_project"
title VRP Hybrid v2 EOD Refresh
cd /d "%PROJECT_ROOT%\notebooks"
py -u ".\vrp_hybrid_v2_eod_pipeline.py" --project-root "%PROJECT_ROOT%" --approved-nav 1000000
set "EXIT_CODE=%ERRORLEVEL%"
echo.
if "%EXIT_CODE%"=="0" (
  echo PASS - Hybrid v2 EOD refresh completed and published.
) else (
  echo FAILED - canonical dashboard outputs were not retained. Review data\audit\vrp_hybrid_v2_eod.
)
pause
exit /b %EXIT_CODE%
