#!/bin/bash
set -euo pipefail

if [ ! -d ".venv" ]; then
  python3 -m venv .venv
fi

source .venv/bin/activate
python -m pip install -r requirements.txt

if [ ! -f ".env" ]; then
  cp .env.example .env
fi

echo "Edit .env with your IB Gateway port if needed (default: 4001)"
echo "Starting broker-bridge..."
python -m broker_bridge.main
