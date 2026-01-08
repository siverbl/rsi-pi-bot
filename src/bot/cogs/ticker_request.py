"""
Ticker Request Handler for RSI Discord Bot.
Handles automatic ticker addition from #request channel messages.

Expected message format (2 lines):
    https://finance.yahoo.com/quote/CINT.ST/
    Cint Group AB

The bot automatically derives the TradingView slug from reference data.
"""
import asyncio
import csv
import re
import logging
from pathlib import Path
from typing import Optional, Tuple, Dict

import discord

from bot.config import TICKERS_FILE, REQUEST_CHANNEL_NAME, REFDATA_DIR

logger = logging.getLogger(__name__)

# Async lock for thread-safe file access
_csv_lock = asyncio.Lock()

# Regex pattern for Yahoo URL parsing
YAHOO_URL_PATTERN = re.compile(
    r'https?://(?:www\.)?finance\.yahoo\.com/quote/([A-Za-z0-9\.\-\^]+)/?',
    re.IGNORECASE
)


class ExchangeLookup:
    """
    Handles exchange code lookups from reference data files.
    Maps Yahoo suffixes and US symbols to TradingView exchange codes.
    """
    
    def __init__(self, refdata_dir: Path = REFDATA_DIR):
        self.refdata_dir = refdata_dir
        self._yahoo_suffix_map: Dict[str, str] = {}  # suffix -> exchange_code
        self._nasdaq_symbols: set = set()
        self._other_listed: Dict[str, str] = {}  # symbol -> exchange_code
        self._loaded = False
        
        # US exchange letter to TradingView code mapping
        self._us_exchange_map = {
            'N': 'NYSE',
            'A': 'AMEX',
            'P': 'NYSEARCA',
            'Z': 'BATS',
            'V': 'IEX'
        }
    
    def load(self) -> bool:
        """Load all reference data files."""
        try:
            self._load_yahoo_suffix_map()
            self._load_nasdaq_listed()
            self._load_other_listed()
            self._loaded = True
            logger.info(
                f"Exchange lookup loaded: {len(self._yahoo_suffix_map)} suffixes, "
                f"{len(self._nasdaq_symbols)} NASDAQ symbols, "
                f"{len(self._other_listed)} other US symbols"
            )
            return True
        except Exception as e:
            logger.error(f"Error loading exchange lookup data: {e}")
            return False
    
    def _load_yahoo_suffix_map(self):
        """Load exchange_code_yahoo_suffix.csv"""
        filepath = self.refdata_dir / "exchange_code_yahoo_suffix.csv"
        if not filepath.exists():
            logger.warning(f"Yahoo suffix map not found: {filepath}")
            return
        
        with open(filepath, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                suffix = row.get('yahoo_suffix', '').strip().upper()
                exchange = row.get('exchange_code', '').strip().upper()
                if suffix and exchange:
                    self._yahoo_suffix_map[suffix] = exchange
    
    def _load_nasdaq_listed(self):
        """Load nasdaqlisted.txt"""
        filepath = self.refdata_dir / "nasdaqlisted.txt"
        if not filepath.exists():
            logger.warning(f"NASDAQ listed file not found: {filepath}")
            return
        
        with open(filepath, encoding='utf-8') as f:
            # Skip header
            header = f.readline()
            for line in f:
                parts = line.strip().split('|')
                if parts and parts[0]:
                    symbol = parts[0].strip().upper()
                    if symbol and not symbol.startswith('Symbol'):
                        self._nasdaq_symbols.add(symbol)
    
    def _load_other_listed(self):
        """Load otherlisted.txt"""
        filepath = self.refdata_dir / "otherlisted.txt"
        if not filepath.exists():
            logger.warning(f"Other listed file not found: {filepath}")
            return
        
        with open(filepath, encoding='utf-8') as f:
            # Skip header
            header = f.readline()
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 3:
                    symbol = parts[0].strip().upper()
                    exchange_letter = parts[2].strip().upper()
                    if symbol and exchange_letter:
                        exchange_code = self._us_exchange_map.get(exchange_letter, 'NYSE')
                        self._other_listed[symbol] = exchange_code
    
    def get_tradingview_slug(self, yahoo_ticker: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Derive the TradingView slug from a Yahoo ticker.
        
        Args:
            yahoo_ticker: Yahoo Finance ticker (e.g., EQNR.OL, AAPL)
            
        Returns:
            Tuple of (tradingview_slug, error_message)
            tradingview_slug is in format EXCHANGE:SYMBOL (e.g., OSL:EQNR)
        """
        if not self._loaded:
            self.load()
        
        yahoo_ticker = yahoo_ticker.upper().strip()
        
        # Check if ticker has a suffix (non-US stock)
        if '.' in yahoo_ticker:
            parts = yahoo_ticker.rsplit('.', 1)
            base_symbol = parts[0]
            suffix = parts[1]
            
            # Look up the exchange code from suffix
            exchange_code = self._yahoo_suffix_map.get(suffix)
            if exchange_code:
                return f"{exchange_code}:{base_symbol}", None
            else:
                return None, f"Unknown Yahoo suffix `.{suffix}`. Please add it to `refdata/exchange_code_yahoo_suffix.csv`"
        
        # No suffix - this is a US stock
        base_symbol = yahoo_ticker
        
        # Check NASDAQ first
        if base_symbol in self._nasdaq_symbols:
            return f"NASDAQ:{base_symbol}", None
        
        # Check other US exchanges
        if base_symbol in self._other_listed:
            exchange_code = self._other_listed[base_symbol]
            return f"{exchange_code}:{base_symbol}", None
        
        # Symbol not found in US listings - default to NASDAQ for common tickers
        # but warn the user
        return None, (
            f"Symbol `{base_symbol}` not found in US exchange listings. "
            f"Update `refdata/nasdaqlisted.txt` or `refdata/otherlisted.txt`, "
            f"or use a ticker with exchange suffix (e.g., `{base_symbol}.OL`)"
        )


# Global exchange lookup instance
_exchange_lookup: Optional[ExchangeLookup] = None


def get_exchange_lookup() -> ExchangeLookup:
    """Get the global exchange lookup instance."""
    global _exchange_lookup
    if _exchange_lookup is None:
        _exchange_lookup = ExchangeLookup()
        _exchange_lookup.load()
    return _exchange_lookup


def parse_ticker_request(content: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Parse a ticker request message.
    
    Args:
        content: Message content with 2 lines:
            Line 1: Yahoo Finance URL
            Line 2: Company name
        
    Returns:
        Tuple of (ticker, name, tradingview_slug, error_message)
        If parsing fails, first 3 values are None and error_message explains why.
    """
    # Split into lines and clean up
    lines = [line.strip() for line in content.strip().split('\n') if line.strip()]
    
    if len(lines) != 2:
        return None, None, None, (
            f"Expected 2 lines, got {len(lines)}. Format:\n"
            f"```\nhttps://finance.yahoo.com/quote/TICKER/\nCompany Name\n```"
        )
    
    yahoo_line = lines[0]
    name_line = lines[1]
    
    # Parse Yahoo URL for ticker
    yahoo_match = YAHOO_URL_PATTERN.search(yahoo_line)
    if not yahoo_match:
        return None, None, None, (
            f"Could not parse Yahoo Finance URL: `{yahoo_line}`\n"
            f"Expected format: `https://finance.yahoo.com/quote/TICKER/`"
        )
    
    ticker = yahoo_match.group(1).upper()
    
    # Validate name (should not be a URL)
    if name_line.startswith('http'):
        return None, None, None, f"Line 2 should be the company name, not a URL: `{name_line}`"
    
    name = name_line.strip()
    if not name:
        return None, None, None, "Company name (line 2) cannot be empty"
    
    # Auto-derive TradingView slug
    lookup = get_exchange_lookup()
    tradingview_slug, error = lookup.get_tradingview_slug(ticker)
    
    if error:
        return None, None, None, error
    
    return ticker, name, tradingview_slug, None


async def ticker_exists(ticker: str) -> bool:
    """
    Check if a ticker already exists in tickers.csv (case-insensitive).
    """
    async with _csv_lock:
        if not TICKERS_FILE.exists():
            return False
        
        try:
            with open(TICKERS_FILE, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('ticker', '').upper() == ticker.upper():
                        return True
        except Exception as e:
            logger.error(f"Error checking ticker existence: {e}")
            return False
    
    return False


async def add_ticker(ticker: str, name: str, tradingview_slug: str) -> Tuple[bool, str]:
    """
    Add a ticker to tickers.csv.
    
    Args:
        ticker: Yahoo Finance ticker symbol
        name: Company name
        tradingview_slug: TradingView slug (EXCHANGE:SYMBOL)
        
    Returns:
        Tuple of (success, message)
    """
    async with _csv_lock:
        try:
            # Check if file exists and has header
            file_exists = TICKERS_FILE.exists()
            needs_header = not file_exists
            
            if file_exists:
                # Check if file is empty or has no header
                with open(TICKERS_FILE, 'r', newline='', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if not first_line or first_line != 'ticker,name,tradingview_slug':
                        needs_header = True
            
            # Check for duplicate (case-insensitive)
            if file_exists:
                with open(TICKERS_FILE, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get('ticker', '').upper() == ticker.upper():
                            return False, f"Ticker `{ticker}` already exists in catalog"
            
            # Append to file
            with open(TICKERS_FILE, 'a', newline='', encoding='utf-8') as f:
                if needs_header:
                    f.write('ticker,name,tradingview_slug\n')
                
                # Write the new row
                writer = csv.writer(f)
                writer.writerow([ticker, name, tradingview_slug])
            
            logger.info(f"Added ticker: {ticker} ({name}) -> {tradingview_slug}")
            return True, f"✅ Added `{ticker}` — {name}\n📊 TradingView: `{tradingview_slug}`"
            
        except Exception as e:
            logger.error(f"Error adding ticker: {e}")
            return False, f"❌ Error adding ticker: {str(e)}"


async def handle_request_message(message: discord.Message) -> Optional[str]:
    """
    Handle a message in the #request channel.
    
    Args:
        message: Discord message
        
    Returns:
        Response message to send, or None if message should be ignored
    """
    # Ignore bot messages
    if message.author.bot:
        return None
    
    # Only process in #request channel
    if message.channel.name != REQUEST_CHANNEL_NAME:
        return None
    
    # Parse the request
    ticker, name, tradingview_slug, error = parse_ticker_request(message.content)
    
    if error:
        return f"❌ **Parse Error**\n{error}"
    
    # Check if already exists
    if await ticker_exists(ticker):
        return f"ℹ️ Ticker `{ticker}` already exists in catalog"
    
    # Add the ticker
    success, response = await add_ticker(ticker, name, tradingview_slug)
    
    return response


class TickerRequestCog:
    """
    Handles ticker request messages in #request channel.
    Integrated into the main bot.
    """
    
    def __init__(self, bot):
        self.bot = bot
    
    async def on_message(self, message: discord.Message):
        """Process messages in #request channel."""
        # Only process in #request channel
        if not hasattr(message.channel, 'name') or message.channel.name != REQUEST_CHANNEL_NAME:
            return
        
        response = await handle_request_message(message)
        
        if response:
            try:
                await message.reply(response, mention_author=False)
            except discord.HTTPException as e:
                logger.error(f"Failed to reply to request message: {e}")
