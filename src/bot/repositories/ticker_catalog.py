"""
Ticker Catalog module for RSI Discord Bot.
Manages the instrument catalog (tickers.csv) as the single source of truth.
"""
import csv
import logging
from typing import Dict, Optional, List
from dataclasses import dataclass
from pathlib import Path

from bot.config import TICKERS_FILE, TRADINGVIEW_URL_TEMPLATE

logger = logging.getLogger(__name__)


@dataclass
class Instrument:
    """Represents an instrument from the ticker catalog."""
    ticker: str
    name: str
    tradingview_slug: str  # Format: EXCHANGE:TICKER (e.g., OSL:EQNR)

    @property
    def tradingview_url(self) -> str:
        """Generate the TradingView chart URL for this instrument."""
        return TRADINGVIEW_URL_TEMPLATE.format(tradingview_slug=self.tradingview_slug)


class TickerCatalog:
    """
    Manages the instrument catalog loaded from tickers.csv.
    
    The CSV file must have a header row with columns:
    ticker,name,tradingview_slug
    """
    
    def __init__(self, csv_path: Path = TICKERS_FILE):
        self.csv_path = csv_path
        self._instruments: Dict[str, Instrument] = {}
        self._loaded = False

    def load(self) -> bool:
        """
        Load the ticker catalog from CSV file.
        
        Returns:
            True if loaded successfully, False otherwise
        """
        self._instruments.clear()

        if not self.csv_path.exists():
            logger.error(f"Ticker catalog not found: {self.csv_path}")
            return False

        try:
            with open(self.csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Validate required columns
                required_columns = {'ticker', 'name', 'tradingview_slug'}
                if not reader.fieldnames:
                    logger.error("Ticker catalog has no header row")
                    return False
                
                missing_columns = required_columns - set(reader.fieldnames)
                if missing_columns:
                    logger.error(f"Ticker catalog missing columns: {missing_columns}")
                    return False

                # Load instruments
                line_num = 1
                for row in reader:
                    line_num += 1
                    ticker = row.get('ticker', '').strip().upper()
                    name = row.get('name', '').strip()
                    tradingview_slug = row.get('tradingview_slug', '').strip()

                    if not ticker or not name:
                        logger.warning(f"Skipping line {line_num}: missing ticker or name")
                        continue

                    if not tradingview_slug:
                        logger.warning(f"Ticker {ticker} has no tradingview_slug")

                    self._instruments[ticker] = Instrument(
                        ticker=ticker,
                        name=name,
                        tradingview_slug=tradingview_slug
                    )

            self._loaded = True
            logger.info(f"Loaded {len(self._instruments)} instruments from catalog")
            return True

        except Exception as e:
            logger.error(f"Error loading ticker catalog: {e}")
            return False

    def reload(self) -> bool:
        """Reload the catalog from disk."""
        return self.load()

    def is_valid_ticker(self, ticker: str) -> bool:
        """Check if a ticker exists in the catalog."""
        if not self._loaded:
            self.load()
        return ticker.upper() in self._instruments

    def get_instrument(self, ticker: str) -> Optional[Instrument]:
        """Get instrument details for a ticker."""
        if not self._loaded:
            self.load()
        return self._instruments.get(ticker.upper())

    def get_name(self, ticker: str) -> str:
        """Get the display name for a ticker."""
        instrument = self.get_instrument(ticker)
        return instrument.name if instrument else ticker

    def get_tradingview_url(self, ticker: str) -> str:
        """Get the TradingView URL for a ticker."""
        instrument = self.get_instrument(ticker)
        if instrument and instrument.tradingview_slug:
            return instrument.tradingview_url
        return ""

    def get_all_tickers(self) -> List[str]:
        """Get list of all valid tickers."""
        if not self._loaded:
            self.load()
        return list(self._instruments.keys())

    def search_tickers(self, query: str, limit: int = 25) -> List[Instrument]:
        """
        Search for tickers matching a query.
        Searches both ticker symbol and company name.
        """
        if not self._loaded:
            self.load()

        query = query.upper()
        results = []

        for ticker, instrument in self._instruments.items():
            if (query in ticker or 
                query in instrument.name.upper()):
                results.append(instrument)
                if len(results) >= limit:
                    break

        return results

    def __len__(self) -> int:
        """Return number of instruments in catalog."""
        if not self._loaded:
            self.load()
        return len(self._instruments)

    def __contains__(self, ticker: str) -> bool:
        """Check if ticker exists in catalog."""
        return self.is_valid_ticker(ticker)


# Global singleton instance
_catalog: Optional[TickerCatalog] = None


def get_catalog() -> TickerCatalog:
    """Get the global ticker catalog instance."""
    global _catalog
    if _catalog is None:
        _catalog = TickerCatalog()
        _catalog.load()
    return _catalog


def validate_ticker(ticker: str) -> tuple[bool, str]:
    """
    Validate a ticker symbol.
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    catalog = get_catalog()
    
    if not ticker:
        return False, "Ticker cannot be empty"
    
    ticker = ticker.upper().strip()
    
    if not catalog.is_valid_ticker(ticker):
        return False, (
            f"Ticker `{ticker}` is not in the instrument catalog. "
            f"Please add it to `tickers.csv` first."
        )
    
    return True, ""
