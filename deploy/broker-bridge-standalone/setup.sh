#!/bin/bash
set -euo pipefail

echo ""
echo "========================================="
echo "  Trading Dashboard — Broker Bridge Setup"
echo "========================================="
echo ""

# ---- Check Python ----
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed."
    echo ""
    echo "Install it from: https://www.python.org/downloads/"
    echo ""
    read -p "Press Enter to exit..." _
    exit 1
fi
echo "  Python found: $(python3 --version)"
echo ""

# ---- Virtual environment ----
if [ ! -d ".venv" ]; then
    echo "  Creating virtual environment..."
    python3 -m venv .venv
fi
source .venv/bin/activate

echo "  Installing dependencies (this takes ~30 seconds)..."
pip install -q -r requirements.txt
echo "  Done."
echo ""

# ---- Interactive .env creation ----
if [ ! -f ".env" ]; then
    echo "========================================="
    echo "  First-time setup — 2 quick questions"
    echo "========================================="
    echo ""

    # Question 1: Database URL
    echo "  1) Paste the database URL you were given."
    echo "     (It starts with: postgresql+asyncpg://...)"
    echo ""
    read -p "     URL: " DB_URL
    echo ""

    # Trim whitespace
    DB_URL=$(echo "$DB_URL" | xargs)

    if [ -z "$DB_URL" ]; then
        echo "  ERROR: Database URL cannot be empty."
        read -p "  Press Enter to exit..." _
        exit 1
    fi

    # Question 2: Paper or live
    echo "  2) Are you using paper trading or live?"
    echo "     1 = Paper trading (recommended to test first)"
    echo "     2 = Live trading"
    echo ""
    read -p "     Enter 1 or 2 [1]: " MODE
    MODE=${MODE:-1}

    if [ "$MODE" = "2" ]; then
        IB_PORT=4001
    else
        IB_PORT=4002
    fi

    cat > .env << ENVEOF
IB_HOST=127.0.0.1
IB_PORT=$IB_PORT
IB_CLIENT_ID=1
POSTGRES_URL=$DB_URL
DB_SSL=true
ENVEOF

    echo ""
    echo "  Config saved."
fi

echo ""
echo "========================================="
echo "  Starting broker-bridge..."
echo "  (Press Ctrl+C to stop)"
echo "========================================="
echo ""
python -m broker_bridge.main
