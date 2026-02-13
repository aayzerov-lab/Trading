#!/bin/bash
set -euo pipefail

if [ ! -d ".venv" ]; then
    echo "First time? Run ./setup.sh instead."
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "No .env file found. Run ./setup.sh instead."
    exit 1
fi

source .venv/bin/activate

echo ""
echo "  Starting broker-bridge... (Ctrl+C to stop)"
echo ""
python -m broker_bridge.main
