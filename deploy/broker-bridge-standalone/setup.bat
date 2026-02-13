@echo off
setlocal EnableDelayedExpansion

echo.
echo =========================================
echo   Trading Dashboard — Broker Bridge Setup
echo =========================================
echo.

REM ---- Check Python ----
python --version >nul 2>&1
if errorlevel 1 (
    echo   ERROR: Python is not installed.
    echo.
    echo   Download it from: https://www.python.org/downloads/
    echo   IMPORTANT: Check "Add to PATH" during install.
    echo.
    pause
    exit /b 1
)
for /f "tokens=*" %%v in ('python --version 2^>^&1') do echo   %%v found.
echo.

REM ---- Virtual environment ----
if not exist .venv (
    echo   Creating virtual environment...
    python -m venv .venv
)
call .venv\Scripts\activate

echo   Installing dependencies (this takes ~30 seconds)...
pip install -q -r requirements.txt
echo   Done.
echo.

REM ---- Interactive .env creation ----
if exist .env goto :start_bridge

echo =========================================
echo   First-time setup — 2 quick questions
echo =========================================
echo.

REM Question 1: Database URL
echo   1) Paste the database URL you were given.
echo      (It starts with: postgresql+asyncpg://...)
echo.
set /p DB_URL="     URL: "
echo.

if "!DB_URL!"=="" (
    echo   ERROR: Database URL cannot be empty.
    pause
    exit /b 1
)

REM Question 2: Paper or live
echo   2) Are you using paper trading or live?
echo      1 = Paper trading (recommended to test first)
echo      2 = Live trading
echo.
set /p MODE="     Enter 1 or 2 [1]: "

if "!MODE!"=="" set MODE=1

if "!MODE!"=="2" (
    set IB_PORT=4001
) else (
    set IB_PORT=4002
)

(
    echo IB_HOST=127.0.0.1
    echo IB_PORT=!IB_PORT!
    echo IB_CLIENT_ID=1
    echo POSTGRES_URL=!DB_URL!
    echo DB_SSL=true
) > .env

echo.
echo   Config saved.

:start_bridge
echo.
echo =========================================
echo   Starting broker-bridge...
echo   (Press Ctrl+C to stop)
echo =========================================
echo.
python -m broker_bridge.main
