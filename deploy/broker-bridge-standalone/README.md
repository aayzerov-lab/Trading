# Broker-Bridge Standalone

This package runs the broker-bridge against a cloud Postgres + Redis so a friend can publish their IBKR data without Docker.

## Prerequisites
- Python 3.10+ installed
- IB Gateway installed and running (log in to your IBKR account)

## First-time setup

Mac/Linux:
```bash
./setup.sh
```

Windows:
```bat
setup.bat
```

The script will create a virtual environment, install dependencies, and start the bridge.

## Daily use

Mac/Linux:
```bash
./start.sh
```

Windows:
```bat
start.bat
```

## Configuration
Edit `.env` after the first run:
- `IB_HOST`, `IB_PORT`, `IB_CLIENT_ID` for your IB Gateway session.
  - Default `IB_PORT` is `4001` (live). Use `4002` for paper trading.
- `POSTGRES_URL` and `REDIS_URL` should point to the cloud database services.

## Notes
- This package only runs the broker-bridge. The web UI and API run in the cloud.
