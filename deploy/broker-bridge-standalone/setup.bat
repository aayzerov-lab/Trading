@echo off
python -m venv .venv
call .venv\Scripts\activate
python -m pip install -r requirements.txt
if not exist .env copy .env.example .env

echo Edit .env with your IB Gateway port if needed (default: 4001)
echo Starting broker-bridge...
python -m broker_bridge.main
