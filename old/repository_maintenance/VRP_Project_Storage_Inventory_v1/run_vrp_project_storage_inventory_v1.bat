@echo off
setlocal
set SCRIPT_DIR=%~dp0

echo.
echo VRP Project Storage Inventory v1
echo READ-ONLY: this tool will not move or delete files.
echo.

py "%SCRIPT_DIR%vrp_project_storage_inventory_v1.py" ^
  --scan-root "C:\Users\patri\vrp_project" ^
  --archive-root "C:\Users\patri\VRP_Archive"

echo.
echo Inventory finished. Review the output path printed above.
pause
endlocal
