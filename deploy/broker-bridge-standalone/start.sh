#!/bin/bash
set -euo pipefail

if [ ! -d ".venv" ]; then
  echo ".venv not found. Run ./setup.sh first."
  exit 1
fi

source .venv/bin/activate
python -m broker_bridge.main
