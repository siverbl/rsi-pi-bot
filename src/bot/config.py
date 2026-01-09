"""Configuration settings for the RSI Discord Bot (TradingView-only).

This build is designed for reliable 24/7 operation on low-power hosts
(e.g., Raspberry Pi 3, Raspberry Pi OS 64-bit Lite).

Data source
-----------
RSI values come from TradingView Screener via the `tradingview_screener`
package. TradingView Screener provides **RSI14** (14-period RSI) and does not
expose other RSI periods.

Runtime paths
-------------
By default, the bot stores its SQLite DB and log file under `runtime/` inside
the repo. For systemd deployments, override via environment variables:
- TICKERS_FILE
- DB_PATH
- LOG_PATH
"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
REFDATA_DIR = DATA_DIR / "refdata"
RUNTIME_DIR = PROJECT_ROOT / "runtime"

# Create runtime dir automatically (DB/log need this). Data dir should already exist.
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

# Core file paths (can be overridden via environment variables)
# This is useful for running as a systemd service and keeping runtime data outside the git repo.
TICKERS_FILE = Path(os.getenv("TICKERS_FILE", str(DATA_DIR / "tickers.csv")))
DB_PATH = Path(os.getenv("DB_PATH", str(RUNTIME_DIR / "rsi_bot.db")))
LOG_PATH = Path(os.getenv("LOG_PATH", str(RUNTIME_DIR / "rsi_bot.log")))

# Ensure parent directories exist for runtime paths
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# =============================================================================
# Environment
# =============================================================================

# Bot token (set via environment variable or .env loader)
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()

# =============================================================================
# TradingView Screener (RSI14) settings
# =============================================================================

# Only RSI14 is supported in this build.
SUPPORTED_RSI_PERIODS = {14}
DEFAULT_RSI_PERIOD = 14

# Batch settings (TradingView scanner is happiest at <= 50 tickers per call)
TV_BATCH_SIZE = 50
TV_BATCH_DELAY_SECONDS = 3.0

# Retry settings for failed tickers
RETRY_MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 5.0
RETRY_BATCH_SIZE = 10

# =============================================================================
# RSI defaults
# =============================================================================
DEFAULT_OVERSOLD_THRESHOLD = 30
DEFAULT_OVERBOUGHT_THRESHOLD = 70

# =============================================================================
# Auto-Scan Default Thresholds (can be changed by admin via Discord)
# =============================================================================
DEFAULT_AUTO_OVERSOLD_THRESHOLD = 34
DEFAULT_AUTO_OVERBOUGHT_THRESHOLD = 70

# =============================================================================
# Scheduling defaults
# =============================================================================
DEFAULT_TIMEZONE = "Europe/Oslo"
DEFAULT_SCHEDULE_TIME = "18:30"
DEFAULT_SCHEDULE_ENABLED = True  # NEW: Default for schedule toggle

# Market Hours (Europe/Oslo timezone)
# Norway/Europe market hours: 09:30 - 17:30
EUROPE_MARKET_START_HOUR = 9
EUROPE_MARKET_START_MINUTE = 30
EUROPE_MARKET_END_HOUR = 17
EUROPE_MARKET_END_MINUTE = 30

# US/Canada market hours (in Europe/Oslo time): 15:30 - 22:30
US_MARKET_START_HOUR = 15
US_MARKET_START_MINUTE = 30
US_MARKET_END_HOUR = 22
US_MARKET_END_MINUTE = 30

# =============================================================================
# Anti-spam / alert behavior
# =============================================================================
DEFAULT_COOLDOWN_HOURS = 24
DEFAULT_HYSTERESIS = 2.0
DEFAULT_ALERT_MODE = "CROSSING"  # CROSSING or LEVEL

# =============================================================================
# Discord rate limits / formatting
# =============================================================================
MAX_ALERTS_PER_MESSAGE = 25
DISCORD_MESSAGE_LIMIT = 2000
DISCORD_SAFE_LIMIT = 1900

# =============================================================================
# Links
# =============================================================================
TRADINGVIEW_URL_TEMPLATE = "https://www.tradingview.com/chart/?symbol={tradingview_slug}&interval=1D"

# =============================================================================
# Discord channels
# =============================================================================

# Fixed alert channel names (automatic routing)
OVERSOLD_CHANNEL_NAME = "rsi-oversold"       # For UNDER alerts
OVERBOUGHT_CHANNEL_NAME = "rsi-overbought"   # For OVER alerts

# Feature channels
REQUEST_CHANNEL_NAME = "request"             # For ticker add requests
CHANGELOG_CHANNEL_NAME = "server-changelog"  # For server status and admin logs

# =============================================================================
# Market Region Detection
# =============================================================================
# Yahoo Finance suffixes that indicate European markets
EUROPEAN_SUFFIXES = {
    '.OL',   # Oslo Stock Exchange (Norway)
    '.ST',   # Stockholm (Sweden)
    '.CO',   # Copenhagen (Denmark)
    '.HE',   # Helsinki (Finland)
    '.AS',   # Amsterdam (Netherlands)
    '.PA',   # Paris (France)
    '.DE',   # Frankfurt (Germany)
    '.L',    # London (UK)
    '.MI',   # Milan (Italy)
    '.MC',   # Madrid (Spain)
    '.SW',   # Zurich (Switzerland)
    '.VI',   # Vienna (Austria)
    '.BR',   # Brussels (Belgium)
    '.LS',   # Lisbon (Portugal)
    '.AT',   # Athens (Greece)
    '.WA',   # Warsaw (Poland)
    '.PR',   # Prague (Czech Republic)
}

# Yahoo Finance suffixes that indicate US/Canada markets
US_CANADA_SUFFIXES = {
    '.TO',   # Toronto Stock Exchange (Canada)
    '.V',    # TSX Venture (Canada)
    '.NE',   # NEO Exchange (Canada)
    '.CN',   # Canadian Securities Exchange
}
