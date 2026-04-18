@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================================
echo   CAMOUFOX CUSTOM BATCH (UP TO 8 WORKERS + RESUME)
echo ============================================================
echo   Config file: config\custom_batch.json
echo   Sua docs/url/proxy trong file config roi chay script nay.
echo ============================================================
echo.

if not exist ".venv\\Scripts\\python.exe" (
  echo [ERROR] Chua setup moi truong.
  echo Hay chay setup.bat truoc.
  pause
  exit /b 1
)

call ".venv\\Scripts\\python.exe" run_custom_batch.py --config config\\custom_batch.json
set RC=%ERRORLEVEL%

echo.
if "%RC%"=="0" (
  echo [OK] Batch da dung.
) else (
  echo [ERROR] Batch loi, exit code: %RC%
)
echo Logs: logs\\camoufox_custom_batch\\w1.log ... w8.log
echo Resume: state\\custom_batch_resume\\w1.json ... w8.json
echo.
pause
exit /b %RC%

