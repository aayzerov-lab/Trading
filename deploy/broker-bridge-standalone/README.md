# Broker-Bridge Standalone

Connect your Interactive Brokers account to the cloud trading dashboard — no Docker needed.

---

## What this does

You run this small Python program on your computer. It connects to your IB Gateway, reads your positions/executions/account data, and writes it to a shared cloud database. The web dashboard at **https://trading-beta-six.vercel.app** reads that database and shows your portfolio.

---

## Step 1 — Install Python

You need Python 3.10 or newer.

**Mac:** Python 3 comes pre-installed on modern macOS. Open Terminal and check:
```bash
python3 --version
```

**Windows:** Download from https://www.python.org/downloads/ — check "Add to PATH" during install.

---

## Step 2 — Configure IB Gateway

1. Open **IB Gateway** (or TWS) and log in to your IBKR account.

2. Go to **Configure** (gear icon top-left) → **Settings** → **API** → **Settings**:
   - **Enable ActiveX and Socket Clients** → check this ON
   - **Socket port** → note the number:
     - `4001` = live trading
     - `4002` = paper trading (use this to test first)
   - **Allow connections from localhost only** → keep checked (default)
   - **Read-Only API** → your choice (the bridge only reads data, never places orders)

3. Click **Apply** / **OK**.

4. Leave IB Gateway running — the bridge needs it open to connect.

---

## Step 3 — Download this folder

Download just the `deploy/broker-bridge-standalone` folder from GitHub. Easiest way:

```bash
git clone https://github.com/aayzerov/Trading.git
cd Trading/deploy/broker-bridge-standalone
```

Or download the ZIP from GitHub and extract just the `deploy/broker-bridge-standalone` folder.

---

## Step 4 — Create your .env file

Copy the example and fill in your values:

**Mac/Linux:**
```bash
cp .env.example .env
```

**Windows:**
```bat
copy .env.example .env
```

Now edit `.env` with any text editor. It should look like this:

```
IB_HOST=127.0.0.1
IB_PORT=4002
POSTGRES_URL=postgresql+asyncpg://neondb_owner:npg_XXXXX@ep-XXXXX.us-east-2.aws.neon.tech/neondb
DB_SSL=true
```

**What to change:**
- `IB_PORT` — use `4002` for paper trading, `4001` for live
- `POSTGRES_URL` — you'll receive this from whoever set up the cloud. Paste it exactly as given.
- Leave `DB_SSL=true` as-is (required for the cloud database)
- Leave `IB_HOST=127.0.0.1` as-is (connects to IB Gateway on your own machine)

---

## Step 5 — Run setup (first time only)

**Mac/Linux:**
```bash
chmod +x setup.sh
./setup.sh
```

**Windows:**
```bat
setup.bat
```

This creates a virtual environment and installs dependencies. Takes about 30 seconds.

---

## Step 6 — Start the bridge

After setup finishes, the bridge starts automatically. You should see output like:

```
broker_bridge_starting
database_initialised
redis_skipped  reason=REDIS_URL not configured
ib_connecting  host=127.0.0.1  port=4002
ib_connected
positions_synced  count=12
account_summary_synced  tags=15
```

If you see `ib_connected` and `positions_synced`, it's working. Open the dashboard and your data will appear within 30 seconds.

---

## Daily use (after first setup)

1. Open IB Gateway and log in
2. Run the bridge:

**Mac/Linux:**
```bash
./start.sh
```

**Windows:**
```bat
start.bat
```

3. Leave it running while you want live data on the dashboard
4. Press `Ctrl+C` to stop

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Connection refused` on port 4002 | IB Gateway isn't running, or API isn't enabled (see Step 2) |
| `password authentication failed` | Check `POSTGRES_URL` in `.env` — typo in the connection string? |
| `No module named 'broker_bridge'` | Run from inside the `broker-bridge-standalone` folder |
| Dashboard shows no data | Wait 30 seconds for the next refresh cycle. Check bridge terminal for errors. |
| `SSL connection is required` | Make sure `DB_SSL=true` is in your `.env` |
