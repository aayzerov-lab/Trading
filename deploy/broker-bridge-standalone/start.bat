@echo off
if not exist .venv\Scripts\activate (
  echo .venv not found. Run setup.bat first.
  exit /b 1
)
call .venv\Scripts\activate
python -m broker_bridge.main
