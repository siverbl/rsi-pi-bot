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
- **TradingView Links**: Alert messages include clickable TradingView chart links (embed-free)
- **Auto-Add Tickers**: Request new tickers in `#request` - bot auto-derives exchange codes
- **Message Chunking**: Automatically splits long messages to stay under Discord's 2000-character limit
- **Schedule Toggle**: Enable/disable automatic scheduled scans per server

## Quick Start

### 1. Prerequisites

- Python 3.11+ (recommended for Raspberry Pi OS 64-bit)
- A Discord Bot Token ([Create one here](https://discord.com/developers/applications))
- `tickers.csv` file with your stock list
- **Channels in your Discord server:**
  - `#rsi-oversold` — for UNDER alerts (oversold signals)
  - `#rsi-overbought` — for OVER alerts (overbought signals)
  - `#request` — for ticker add requests
  - `#server-changelog` — for admin logs and server status

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
| `/remove-ticker` | Remove a ticker from the catalog | Administrator |
| `/list` | List all subscriptions (with optional ticker filter) | None |
| `/run-now` | Manually trigger full RSI check | Manage Server |
| `/set-defaults` | Configure server defaults (including schedule toggle) | Manage Server |
| `/scheduler-status` | Show scheduling health: jobs, next runs, last scan/error | Manage Server |
| `/ticker-info` | Look up a ticker (shows RSI, subscriptions) | None |
| `/catalog-stats` | Show catalog and subscription statistics | None |
| `/reload-catalog` | Reload tickers.csv | Administrator |

All commands are server-only (they cannot be used in DMs).


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

**Remove a ticker from the catalog (Admin):**
```
/remove-ticker ticker:EQNR.OL
```

**Disable scheduled scans:**
```
/set-defaults schedule_enabled:Disabled
```

**Re-enable scheduled scans:**
```
/set-defaults schedule_enabled:Enabled
```

## Admin Commands

### `/remove-ticker`

**Purpose:** Remove a ticker from `tickers.csv` catalog.

**Permissions:** Administrator only

**Behavior:**
- Case-insensitive ticker matching
- Atomic CSV write (temp file + replace) to prevent corruption
- Shows removed ticker details (ticker, name, TradingView slug)
- **Disables all subscriptions for the removed ticker** (across servers) and
  reports the count — removal never leaves silently broken subscriptions
- Logs removal to `#server-changelog`
- Reloads catalog automatically after removal

**Example:**
```
/remove-ticker ticker:EQNR.OL
```

Response:
```
✅ Ticker removed: EQNR.OL
Name: Equinor ASA
TradingView: OSL:EQNR
```

### `/run-now`

**Purpose:** Manually trigger a complete RSI check.

**Permissions:** Manage Server

**Behavior:**
1. Scans **all tickers** in the catalog (not just subscriptions)
2. Posts auto-scan results to `#rsi-oversold` (sorted RSI ascending) and `#rsi-overbought` (sorted RSI descending)
3. Evaluates user subscriptions separately
4. Posts subscription alerts with "🔔 Subscription Alerts" label if triggered
5. Logs comprehensive summary to `#server-changelog` including:
   - Who triggered it
   - Scan statistics (batches, success/fail counts)
   - Data timestamp
   - Alert counts

### `/set-defaults`

**Purpose:** Configure server-level defaults and schedule settings.

**Permissions:** Manage Server

**Parameters:**
| Parameter | Description | Default |
|-----------|-------------|---------|
| `default_period` | RSI period (must be 14) | 14 |
| `default_cooldown` | Hours between alerts | 24 |
| `schedule_time` | Daily check time (HH:MM, Europe/Oslo) | 18:30 |
| `alert_mode` | CROSSING or LEVEL | CROSSING |
| `hysteresis` | Buffer for threshold bouncing | 2.0 |
| `auto_oversold` | Auto-scan oversold threshold | 34 |
| `auto_overbought` | Auto-scan overbought threshold | 70 |
| `schedule_enabled` | Enable/disable scheduled scans | Enabled |

**Schedule Toggle Behavior:**
- When `schedule_enabled = Disabled`, hourly and daily scheduled scans stop
- When `schedule_enabled = Enabled`, scheduling resumes immediately
- Changes persist across bot restarts
- Changes are logged to `#server-changelog`


## Alert Channels

The bot uses **fixed channels** for alerts (no channel selection needed):

| Channel | Alert Type | Sorting |
|---------|------------|---------|
| `#rsi-oversold` | UNDER alerts | Lowest RSI first |
| `#rsi-overbought` | OVER alerts | Highest RSI first |
| `#server-changelog` | Admin logs, scan status | Chronological |

**Important:** Create these channels before using the bot. The bot will show an error if they don't exist.

## Alert Message Format

Alerts use a numbered list format with clickable TradingView chart links (no embeds):

```
📈 **RSI Overbought Alerts**

1) **AUSS.OL** — [Austevoll Seafood](https://www.tradingview.com/chart/?symbol=OSL:AUSS&interval=1D) — RSI14: **79.6** | Rule: **> 70.0** | ⏱️ **day 4**
2) **NHY.OL** — [Norsk Hydro](https://www.tradingview.com/chart/?symbol=OSL:NHY&interval=1D) — RSI14: **78.3** | Rule: **> 70.0** | 🆕 **just crossed**
```

- **🆕 just crossed** — First day the condition is met
- **⏱️ day N** — Consecutive trading days the condition has been met
- Links are clickable but **no preview embeds** are shown

## Configuration

### Automatic Hourly Scans

The bot automatically scans all tickers in the catalog during market hours (when `schedule_enabled = true`):

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

These are separate from user subscription thresholds and apply only to the automatic scans.

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

### Server Defaults

Admins can configure server defaults with `/set-defaults`:

- **default_period**: Fixed to RSI14 in this build (TradingView Screener provides RSI14 only)
- **default_cooldown**: Hours between repeated alerts (default: 24)
- **schedule_time**: Daily check time in HH:MM (default: 18:30, Europe/Oslo)
- **alert_mode**: `CROSSING` (default) or `LEVEL`
- **hysteresis**: Buffer to prevent threshold bouncing (default: 2.0)
- **auto_oversold**: Auto-scan oversold threshold (default: 34)
- **auto_overbought**: Auto-scan overbought threshold (default: 70)
- **schedule_enabled**: Enable/disable scheduled scans (default: Enabled)

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

The bot uses SQLite with the following tables:

### guild_config
Server-level settings including:
- defaults for RSI period, cooldown, schedule time, alert mode
- auto-scan thresholds (oversold, overbought)
- **schedule_enabled** (boolean) - controls whether scheduled scans run

### subscriptions
Each alert rule with ticker, condition (UNDER/OVER), threshold, period, cooldown, and `created_by_user_id`.

### subscription_state
Tracks last RSI value, crossing status, cooldown, and consecutive days in zone for each subscription.

### auto_scan_state
Tracks daily auto-scan state per guild/condition for change detection.

## File Structure

```
rsi-pi-bot/
├── src/bot/
│   ├── main.py              # Main entry point with slash commands
│   ├── config.py            # Configuration settings
│   ├── repositories/
│   │   ├── database.py      # SQLite database operations
│   │   └── ticker_catalog.py # Ticker catalog management (including remove_ticker)
│   ├── services/
│   │   ├── scheduler.py     # Scheduled job handling (with schedule_enabled checks)
│   │   └── market_data/
│   │       ├── rsi_calculator.py    # RSI calculation logic
│   │       └── providers/
│   │           └── tradingview_provider.py  # TradingView data provider
│   ├── cogs/
│   │   ├── alert_engine.py  # Alert trigger logic and formatting
│   │   └── ticker_request.py # Auto-add tickers from #request channel
│   └── utils/
│       └── message_utils.py # Message chunking utilities
├── data/
│   ├── tickers.csv          # Instrument catalog
│   └── refdata/             # Exchange lookup reference data
├── deploy/
│   ├── rsi-pi-bot.env.example
│   └── rsi-pi-bot.service
├── tests/
│   ├── conftest.py              # Fake Discord/TradingView objects
│   ├── test_scheduler_jobs.py   # Job registration, windows, rescheduling
│   ├── test_scan_pipeline.py    # Unified scan pipeline + change detection
│   ├── test_alert_engine_scope.py
│   ├── test_commands.py         # Slash-command permissions/validation
│   ├── test_config_toggle.py
│   ├── test_deployment.py       # systemd unit / import-path assumptions
│   └── test_ticker_removal.py
├── pytest.ini
├── requirements.txt
└── README.md
```

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

**No manual exchange selection needed!** The bot uses reference data files in `data/refdata/` to automatically map:
- Yahoo suffixes (`.OL`, `.ST`, `.TO`, etc.) to exchange codes
- US stocks (no suffix) to NASDAQ, NYSE, etc.

## Subscription Ownership

- Users can only remove their **own** subscriptions with `/unsubscribe`
- Admins can remove any subscription with `/admin-unsubscribe`
- Admins can remove tickers from catalog with `/remove-ticker`
- Admin actions are logged to `#server-changelog`

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

The bot runs scheduled RSI checks based on the `schedule_enabled` setting:

**When enabled:**
- Hourly auto-scans during market hours, weekdays at `:30` Europe/Oslo:
  - Europe: 09:30–17:30 (one cron job)
  - US/Canada: 15:30–22:30 (one cron job)
- A **per-server** daily subscription check at that server's configured
  `schedule_time` (default 18:30 Europe/Oslo). Changing the time with
  `/set-defaults schedule_time:HH:MM` reschedules the job immediately —
  no restart needed.

**When disabled:**
- No automatic scans run for that server (other servers are unaffected)
- Manual `/run-now` still works (it bypasses the toggle)
- Status shown in `/catalog-stats` and `/scheduler-status`

**Reliability design:**
- Every server gets a default configuration automatically (on startup and
  when the bot joins) — no slash command needed before auto-scans work.
- All scans run through one shared pipeline (`/run-now` uses the same code
  path as scheduled scans) and are serialized with a lock, so two scans never
  fetch TradingView data or write SQLite concurrently.
- RSI results are cached for 10 minutes, so back-to-back jobs (e.g. the 18:30
  US scan and an 18:30 daily check) reuse data instead of re-querying.
- Scheduled jobs wait for the Discord connection to be ready before running.

### Diagnosing "no automatic messages"

Use `/scheduler-status` (Manage Server) and `#server-changelog`:

| Symptom | Meaning |
|---------|---------|
| `/scheduler-status` shows no jobs / not running | Scheduler never started — check `journalctl` for startup errors |
| Jobs listed with next runs, but schedule shows ❌ Disabled | Scheduler runs, but this server opted out (`/set-defaults schedule_enabled:Enabled`) |
| Next run is on a future weekday morning | You're outside market hours / on a weekend — nothing is due yet |
| Changelog says "⏭️ No new hits" | The scan ran fine; nothing newly entered the oversold/overbought zone |
| Changelog says "🚨 TradingView data fetch FAILED" | The scan ran but TradingView returned no data — check logs |
| Changelog lists "⚠️ Channel Issues" | The scan ran but couldn't post to a channel (missing channel or Send Messages permission) |
| No changelog message at all at `:30` | Bot can't post to `#server-changelog` (check it exists + permissions), or see `/scheduler-status` → Latest Error |

## Troubleshooting

### Commands not appearing
- Wait 1 hour for Discord to sync globally
- Or use `/run-now` to test immediately

### No alerts triggering
- Check `/list` to verify subscriptions exist
- Use `/run-now` to trigger a check manually
- Verify ticker exists in `tickers.csv`
- Check if `schedule_enabled` is disabled (use `/set-defaults`)

### "Channel not found" error
- Ensure `#rsi-oversold` and `#rsi-overbought` channels exist
- Ensure the bot has permission to send messages in these channels

### RSI calculation issues
- Ensure ticker format matches Yahoo Finance (e.g., `EQNR.OL`)
- Check logs for data fetch errors

### Scheduled scans not running
- Run `/scheduler-status` — it shows whether the scheduler is running, every
  job's next run time, the last completed scan, the last error, and channel
  health (see the table under **Scheduling** above)
- Check `/catalog-stats` for schedule status
- Use `/set-defaults schedule_enabled:Enabled` to re-enable
- On the Pi: `journalctl -u rsi-pi-bot.service --since today | grep -E "SCAN|scheduler"`

## Logs

The bot logs to both console and `runtime/rsi_bot.log`. Check logs for:
- Startup status
- Data fetch success/failures
- Alert triggers
- Schedule enable/disable changes
- Ticker removals
- Errors

## License

MIT License - See LICENSE file for details.


## Run as a systemd service (Raspberry Pi / Linux)

This avoids relying on `export DISCORD_TOKEN=...` or `PYTHONPATH=src` in your
shell — systemd inherits neither. The provided unit sets
`PYTHONPATH=/opt/rsi-pi-bot/src` itself, so imports work without any
interactive-shell setup.

### 1) Install the bot and create a dedicated user

```bash
sudo useradd -r -m -s /usr/sbin/nologin rsi-pi-bot || true

# Clone (or copy) the repo to /opt/rsi-pi-bot and create the venv
sudo git clone https://github.com/<YOUR_USER_OR_ORG>/rsi-pi-bot.git /opt/rsi-pi-bot
cd /opt/rsi-pi-bot
sudo python3 -m venv venv
sudo ./venv/bin/pip install -r requirements.txt

# The service user must be able to write runtime/ (DB+log fallback paths)
# and data/ (the #request channel appends to tickers.csv)
sudo chown -R rsi-pi-bot:rsi-pi-bot /opt/rsi-pi-bot
```

You do NOT need to create `/var/lib/rsi-pi-bot` or `/var/log/rsi-pi-bot`
manually — the unit's `StateDirectory=`/`LogsDirectory=` make systemd create
them with the correct ownership.

### 2) Copy the provided templates

Templates are included in `deploy/`:

- `deploy/rsi-pi-bot.env.example`
- `deploy/rsi-pi-bot.service`

Copy and edit them:

```bash
# Environment file (set DISCORD_TOKEN; keep it private)
sudo cp deploy/rsi-pi-bot.env.example /etc/rsi-pi-bot.env
sudo chmod 600 /etc/rsi-pi-bot.env
sudo nano /etc/rsi-pi-bot.env

# If you keep tickers.csv outside the repo (recommended, the env example
# points TICKERS_FILE at /var/lib/rsi-pi-bot), copy your file there after
# the first start, e.g.:
#   sudo cp data/tickers.csv /var/lib/rsi-pi-bot/tickers.csv
#   sudo chown rsi-pi-bot:rsi-pi-bot /var/lib/rsi-pi-bot/tickers.csv

# Service file
sudo cp deploy/rsi-pi-bot.service /etc/systemd/system/rsi-pi-bot.service
```

### 3) Enable and start

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now rsi-pi-bot.service
```

### 4) Operate

```bash
# Reload unit definition after editing the .service file
sudo systemctl daemon-reload

# Restart the bot (e.g. after a git pull)
sudo systemctl restart rsi-pi-bot.service

# Status (is it running? recent log lines)
systemctl status rsi-pi-bot.service --no-pager

# Follow logs live
journalctl -u rsi-pi-bot.service -f --no-pager

# Logs since the last boot / today
journalctl -u rsi-pi-bot.service -b --no-pager
journalctl -u rsi-pi-bot.service --since today --no-pager
```

On startup you should see log lines like:

```
Starting RSI scheduler (timezone: Europe/Oslo)
Registered auto-scan jobs: Europe 9:30-17:30, US/Canada 15:30-22:30 (Europe/Oslo, weekdays)
Scheduler has 3 registered jobs:
  - europe_autoscan: next run at 2026-07-06 09:30:00+02:00
  ...
Synced daily subscription jobs for 1 guilds
```

### Notes

- The code supports overriding paths via environment variables: `TICKERS_FILE`, `DB_PATH`, `LOG_PATH`.
- If you keep `tickers.csv` outside the repo (recommended), set `TICKERS_FILE=/var/lib/rsi-pi-bot/tickers.csv` and copy your existing file there.
- The unit uses `ProtectSystem=full` (keeps `/usr`, `/boot`, `/etc` read-only) while `/opt/rsi-pi-bot` stays writable for `runtime/` and `data/tickers.csv`.

## Running the tests

The test suite uses temporary SQLite databases and fake Discord/TradingView
objects — no token or network access required.

```bash
pip install pytest pytest-asyncio
pytest
```
