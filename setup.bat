@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================================
echo   TVPL CAMOUFOX CUSTOM BATCH - SETUP
echo ============================================================
echo.

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Khong tim thay python trong PATH.
  exit /b 1
)

if not exist ".venv\\Scripts\\python.exe" (
  echo [STEP] Tao virtual environment .venv ...
  python -m venv .venv
  if errorlevel 1 (
    echo [ERROR] Tao .venv that bai.
    exit /b 1
  )
)

echo [STEP] Cai dependencies ...
call ".venv\\Scripts\\python.exe" -m pip install --upgrade pip
if errorlevel 1 exit /b 1
call ".venv\\Scripts\\python.exe" -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

echo [STEP] Tai Camoufox browser profile...
call ".venv\\Scripts\\python.exe" -m camoufox fetch
if errorlevel 1 (
  echo [WARN] camoufox fetch loi. Thu chay lai setup.bat sau.
)

echo.
echo [OK] Setup xong.
echo [NEXT] Sua file config\\custom_batch.json roi chay run.bat
echo.
pause

