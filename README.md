# RSI Pi Bot

A Discord bot that monitors stock RSI (Relative Strength Index) levels and sends alerts when stocks cross configured thresholds. Designed for Norwegian stocks with TradingView integration.

## Features

- **TradingView-only RSI (RSI14)**: Uses TradingView Screener's pre-calculated RSI14 for fast batch queries
- **RSI Alerts**: Get notified when stocks cross oversold (RSI < 30) or overbought (RSI > 70) thresholds
- **Crossing Detection**: Smart alert system that only triggers when RSI crosses a threshold (not every day it stays beyond)
- **Hourly Auto-Scans**: Automatic RSI scanning during market hours for Europe (09:30-17:30) and US/Canada (15:30-22:30)
- **Daily Change Detection**: Only posts to alert channels when the set of qualifying tickers changes
- **Fixed Alert Channels**: Alerts automatically route to `#rsi-oversold` and `#rsi-overbought`
- **Slash Commands**: Modern Discord slash command interface
- **Server-wide Alerts**: All alerts are visible to everyone in the server
- **Persistent Storage**: SQLite database survives bot restarts
- **Cooldown System**: Prevents alert spam with configurable cooldown periods
- **Batch Processing**: Efficiently handles 300-500 tickers with batched API calls
- **TradingView Links**: Alert messages include clickable TradingView chart links
- **Auto-Add Tickers**: Request new tickers in `#request` - bot auto-derives exchange codes
- **Message Chunking**: Automatically splits long messages to stay under Discord's 2000-character limit

## Quick Start

### 1. Prerequisites

- Python 3.11+ (recommended for Raspberry Pi OS 64-bit)
- A Discord Bot Token ([Create one here](https://discord.com/developers/applications))
- `tickers.csv` file with your stock list
- **Two channels in your Discord server:**
  - `#rsi-oversold` — for UNDER alerts (oversold signals)
  - `#rsi-overbought` — for OVER alerts (overbought signals)

### 2. Installation

```bash
# Clone from GitHub
git clone https://github.com/<YOUR_USER_OR_ORG>/rsi-pi-bot.git
cd rsi-pi-bot

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration

Set your Discord bot token:

```bash
export DISCORD_TOKEN=your_bot_token_here
```

### 4. Run the Bot

Linux/macOS:
```bash
PYTHONPATH=src python -m bot.main
```

Windows (PowerShell):
```powershell
$env:PYTHONPATH="src"
python -m bot.main
```

### Raspberry Pi (systemd service)

For 24/7 operation on Raspberry Pi OS 64-bit Lite, use the files in `deploy/`:

1. Copy `deploy/rsi-pi-bot.env.example` to `/etc/rsi-pi-bot.env` and set `DISCORD_TOKEN`.
2. Copy `deploy/rsi-pi-bot.service` to `/etc/systemd/system/rsi-pi-bot.service`.
3. Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now rsi-pi-bot.service
```

Logs:
```bash
journalctl -u rsi-pi-bot.service -f --no-pager
```


## Slash Commands

| Command | Description | Required Permissions |
|---------|-------------|---------------------|
| `/subscribe` | Create an RSI alert subscription | None |
| `/subscribe-bands` | Create both oversold and overbought alerts | None |
| `/unsubscribe` | Remove your own subscription by ID | None |
| `/unsubscribe-all` | Remove all your subscriptions | None |
| `/admin-unsubscribe` | Remove any subscription (logged) | Administrator |
| `/list` | List all subscriptions (with optional ticker filter) | None |
| `/run-now` | Manually trigger RSI check | Manage Server |
| `/set-defaults` | Configure server defaults | Manage Server |
| `/ticker-info` | Look up a ticker (shows RSI, subscriptions) | None |
| `/catalog-stats` | Show catalog and subscription statistics | None |
| `/reload-catalog` | Reload tickers.csv | Administrator |


### Command Examples

**Create a subscription:**
```
/subscribe ticker:EQNR.OL condition:under threshold:30
```

**Create both oversold and overbought alerts:**
```
/subscribe-bands ticker:YAR.OL oversold:30 overbought:70
```

**List all subscriptions:**
```
/list
```

**List subscriptions for a specific ticker:**
```
/list ticker:EQNR.OL
```

**Get detailed info about a ticker (including RSI and subscriptions):**
```
/ticker-info ticker:EQNR.OL
```

**Remove a subscription:**
```
/unsubscribe id:5
```

**Remove all your subscriptions:**
```
/unsubscribe-all
```

**View statistics:**
```
/catalog-stats
```

## Alert Channels

The bot uses **two fixed channels** for alerts (no channel selection needed):

| Channel | Alert Type | Sorting |
|---------|------------|---------|
| `#rsi-oversold` | UNDER alerts | Lowest RSI first |
| `#rsi-overbought` | OVER alerts | Highest RSI first |

**Important:** Create these channels before using the bot. The bot will show an error if they don't exist.

## Alert Message Format

Alerts use a numbered list format with clickable TradingView chart links:

```
📈 **RSI Overbought Alerts**

1) **AUSS.OL** — [Austevoll Seafood](https://www.tradingview.com/chart/?symbol=OSL:AUSS&interval=1D) — RSI14: **79.6** | Rule: **> 70.0** | ⏱️ **day 4**
2) **NHY.OL** — [Norsk Hydro](https://www.tradingview.com/chart/?symbol=OSL:NHY&interval=1D) — RSI14: **78.3** | Rule: **> 70.0** | 🆕 **just crossed**
```

- **🆕 just crossed** — First day the condition is met
- **⏱️ day N** — Consecutive trading days the condition has been met

## Configuration

### Automatic Hourly Scans

The bot automatically scans all tickers in the catalog during market hours:

- **European markets**: 09:30 - 17:30 Europe/Oslo (hourly at :30)
- **US/Canada markets**: 15:30 - 22:30 Europe/Oslo (hourly at :30)
- Only runs on weekdays (Mon-Fri)

**Daily Change Detection**: To reduce noise, the bot only posts to `#rsi-oversold` and `#rsi-overbought` when:
1. First scan of the day (always posts if tickers qualify)
2. The set of qualifying tickers has changed from the previous scan

Status updates are always posted to `#server-changelog` for every scan.

### Auto-Scan Thresholds

Admins can configure auto-scan thresholds per server using `/set-defaults`:

- **auto_oversold**: Oversold threshold for auto-scans (default: 34)
- **auto_overbought**: Overbought threshold for auto-scans (default: 70)

These are separate from user subscription thresholds and apply only to the hourly automatic scans.

### tickers.csv

The `tickers.csv` file is the source of truth for valid tickers. Format:

```csv
ticker,name,tradingview_slug
YAR.OL,Yara International ASA,OSL:YAR
EQNR.OL,Equinor ASA,OSL:EQNR
AAPL,Apple Inc.,NASDAQ:AAPL
```

- `ticker`: Yahoo Finance ticker symbol (e.g., `*.OL` for Oslo)
- `name`: Company display name for alerts
- `tradingview_slug`: TradingView symbol (EXCHANGE:SYMBOL format) for chart links

### Ticker Format Limits

**There are no artificial limits** on ticker symbol format beyond:
- Must exist in `tickers.csv` (the instrument catalog)
- Must include a valid `tradingview_slug` (format: `EXCHANGE:SYMBOL`)

### Server Defaults

Admins can configure server defaults with `/set-defaults`:

- **default_period**: Fixed to RSI14 in this build (TradingView Screener provides RSI14 only)
- **default_cooldown**: Hours between repeated alerts (default: 24)
- **schedule_time**: Daily check time in HH:MM (default: 18:30, Europe/Oslo)
- **alert_mode**: `CROSSING` (default) or `LEVEL`
- **hysteresis**: Buffer to prevent threshold bouncing (default: 2.0)
- **auto_oversold**: Auto-scan oversold threshold (default: 34)
- **auto_overbought**: Auto-scan overbought threshold (default: 70)

## Alert System

### Alert Modes

**CROSSING (default)**
- Only alerts when RSI *crosses* a threshold
- UNDER 30: Triggers when RSI goes from ≥30 to <30
- OVER 70: Triggers when RSI goes from ≤70 to >70
- Prevents daily repeated alerts when RSI stays beyond threshold

**LEVEL**
- Alerts whenever the condition is met
- Will alert every day RSI is beyond threshold (subject to cooldown)

### Cooldown

After an alert fires, it won't fire again for the same subscription until the cooldown period passes (default 24 hours).

### Persistence Counter

The bot tracks **consecutive trading days** that a stock meets the condition:
- `🆕 just crossed` — First day
- `⏱️ day N` — Number of consecutive trading days

## Database Schema

The bot uses SQLite with three tables:

### guild_config
Server-level settings including defaults for RSI period, cooldown, schedule time, and alert mode.

### subscriptions
Each alert rule with ticker, condition (UNDER/OVER), threshold, period, cooldown, and `created_by_user_id`.

### subscription_state
Tracks last RSI value, crossing status, cooldown, and consecutive days in zone for each subscription.

## File Structure

```
rsi-pi-bot/
├── main.py              # Main entry point with slash commands
├── config.py            # Configuration settings
├── database.py          # SQLite database operations
├── rsi_calculator.py    # RSI calculation logic
├── ticker_catalog.py    # Ticker catalog management
├── alert_engine.py      # Alert trigger logic and formatting
├── scheduler.py         # Scheduled job handling
├── ticker_request.py    # Auto-add tickers from #request channel
├── server_monitor.py    # Server health monitoring and control
├── tickers.csv          # Instrument catalog
├── requirements.txt     # Python dependencies
├── rsi_bot.db          # SQLite database (created on first run)
├── rsi_bot.log         # Log file
└── refdata/            # Exchange lookup reference data
    ├── exchange_code_yahoo_suffix.csv  # Yahoo suffix → exchange mapping
    ├── nasdaqlisted.txt                # NASDAQ symbols
    └── otherlisted.txt                 # Other US exchange symbols
```

### tickers.csv Format

```csv
ticker,name,tradingview_slug
YAR.OL,Yara International ASA,OSL:YAR
EQNR.OL,Equinor ASA,OSL:EQNR
AAPL,Apple Inc.,NASDAQ:AAPL
```

- `ticker`: Yahoo Finance ticker (used for data fetching)
- `name`: Company display name
- `tradingview_slug`: `EXCHANGE:SYMBOL` format for TradingView chart links

## Auto-Add Tickers (#request channel)

Users can request new tickers by posting in `#request` with this simple 2-line format:

```
https://finance.yahoo.com/quote/CINT.ST/
Cint Group AB
```

The bot will:
1. Parse the Yahoo Finance URL for the ticker symbol (`CINT.ST`)
2. Use line 2 as the company name (`Cint Group AB`)
3. **Auto-derive** the TradingView slug from reference data (`STO:CINT`)
4. Add to `tickers.csv` if not already present
5. Reply with confirmation including the TradingView link

**No manual exchange selection needed!** The bot uses reference data files in `refdata/` to automatically map:
- Yahoo suffixes (`.OL`, `.ST`, `.TO`, etc.) to exchange codes
- US stocks (no suffix) to NASDAQ, NYSE, etc.

### Reference Data Files

Located in `refdata/`:
- `exchange_code_yahoo_suffix.csv` - Maps Yahoo suffixes to TradingView exchange codes
- `nasdaqlisted.txt` - NASDAQ-listed US symbols
- `otherlisted.txt` - Other US exchange symbols (NYSE, AMEX, etc.)

## Subscription Ownership

- Users can only remove their **own** subscriptions with `/unsubscribe`
- Admins can remove any subscription with `/admin-unsubscribe`
- Admin actions are logged to `#server-changelog`

## Server Monitoring

Optional server health monitoring and control features.

### Environment Variables

```bash
# Health check endpoint (enables monitoring)
SERVER_HEALTH_URL=http://localhost:8080/health

# Server control scripts (admin commands)
SERVER_RESTART_SCRIPT=/opt/scripts/restart_server.sh
SERVER_SHUTDOWN_SCRIPT=/opt/scripts/shutdown_server.sh
```

### Features

- Automatic status announcements when server goes online/offline
- Scheduled restart/shutdown with warnings (10 min, 1 min before)
- All actions logged to `#server-changelog`

## Discord Bot Setup

1. Go to [Discord Developer Portal](https://discord.com/developers/applications)
2. Create a new application
3. Go to "Bot" section and create a bot
4. Copy the bot token
5. Enable the following intents:
   - **Message Content Intent** (required for #request channel)
6. Go to OAuth2 > URL Generator
7. Select scopes: `bot`, `applications.commands`
8. Select permissions: `Send Messages`, `Embed Links`, `Read Message History`
9. Use the generated URL to invite the bot to your server
10. **Create the required channels:**
    - `#rsi-oversold` — UNDER alerts
    - `#rsi-overbought` — OVER alerts
    - `#request` — Ticker add requests
    - `#server-changelog` — Admin logs and server status

## Scheduling

The bot runs a daily RSI check at a configurable time (default 18:30 Europe/Oslo timezone). This is after European markets close, ensuring the day's data is complete.

The job:
1. Loads all active subscriptions
2. Determines required tickers and RSI periods
3. Fetches price data in batches (100 tickers per batch)
4. Calculates RSI for each ticker/period combination
5. Evaluates crossing conditions
6. Sends sorted alerts to the fixed channels
7. Updates state for next run

## Troubleshooting

### Commands not appearing
- Wait 1 hour for Discord to sync globally
- Or use `/run-now` to test immediately

### No alerts triggering
- Check `/list` to verify subscriptions exist
- Use `/run-now` to trigger a check manually
- Verify ticker exists in `tickers.csv`

### "Channel not found" error
- Ensure `#rsi-oversold` and `#rsi-overbought` channels exist
- Ensure the bot has permission to send messages in these channels

### RSI calculation issues
- Ensure ticker format matches Yahoo Finance (e.g., `EQNR.OL`)
- Check logs for data fetch errors

## Logs

The bot logs to both console and `rsi_bot.log`. Check logs for:
- Startup status
- Data fetch success/failures
- Alert triggers
- Errors

## License

MIT License - See LICENSE file for details.


## Run as a systemd service (Raspberry Pi / Linux)

This avoids relying on `export DISCORD_TOKEN=...` in your shell (systemd does not inherit that).

### 1) Create a dedicated user and runtime directories

```bash
sudo useradd -r -m -s /usr/sbin/nologin rsi-pi-bot || true

sudo mkdir -p /opt/rsi-pi-bot
sudo mkdir -p /var/lib/rsi-pi-bot
sudo mkdir -p /var/log/rsi-pi-bot
sudo chown -R rsi-pi-bot:rsi-pi-bot /var/lib/rsi-pi-bot /var/log/rsi-pi-bot
```

### 2) Copy the provided templates

Templates are included in `deploy/`:

- `deploy/rsi-pi-bot.env.example`
- `deploy/rsi-pi-bot.service`

Copy and edit them:

```bash
# Environment file
sudo cp deploy/rsi-pi-bot.env.example /etc/rsi-pi-bot.env
sudo nano /etc/rsi-pi-bot.env

# Service file
sudo cp deploy/rsi-pi-bot.service /etc/systemd/system/rsi-pi-bot.service
```

### 3) Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now rsi-pi-bot.service

# Follow logs
journalctl -u rsi-pi-bot.service -f --no-pager
```

### Notes

- The code supports overriding paths via environment variables: `TICKERS_FILE`, `DB_PATH`, `LOG_PATH`.
- If you keep `tickers.csv` outside the repo (recommended), set `TICKERS_FILE=/var/lib/rsi-pi-bot/tickers.csv` and copy your existing file there.
