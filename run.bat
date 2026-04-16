@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".venv\\Scripts\\python.exe" (
  echo [ERROR] Chua setup moi truong.
  echo Hay chay setup.bat truoc.
  pause
  exit /b 1
)

echo ============================================================
echo   TVPL CAMOUFOX CUSTOM BATCH - RUN
echo ============================================================
echo Config: config\\custom_batch.json
echo.

call ".venv\\Scripts\\python.exe" run_custom_batch.py --config config\\custom_batch.json
set RC=%ERRORLEVEL%

echo.
if "%RC%"=="0" (
  echo [OK] Run xong.
) else (
  echo [ERROR] Run that bai. Exit code: %RC%
)
echo.
echo Logs: logs\\camoufox_custom_batch\\w1.log / w2.log / w3.log
pause
exit /b %RC%

