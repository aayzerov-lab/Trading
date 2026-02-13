@echo off

if not exist .venv\Scripts\activate (
    echo   First time? Run setup.bat instead.
    pause
    exit /b 1
)

if not exist .env (
    echo   No .env file found. Run setup.bat instead.
    pause
    exit /b 1
)

call .venv\Scripts\activate

echo.
echo   Starting broker-bridge... (Ctrl+C to stop)
echo.
python -m broker_bridge.main
