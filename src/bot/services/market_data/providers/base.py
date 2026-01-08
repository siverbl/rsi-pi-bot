"""
RSI Provider Base Interface.

Defines the common interface that all RSI data providers must implement.
This allows the bot to switch between different data sources seamlessly.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class RSIData:
    """
    Unified RSI data result from any provider.
    
    All providers must return data in this format
    to ensure the rest of the bot logic works consistently.
    """
    ticker: str
    name: Optional[str]  # Company name if available
    rsi_14: Optional[float]  # RSI with 14-period (None if calculation failed)
    close: Optional[float]  # Last close price
    data_timestamp: datetime  # When the data was retrieved/last candle time
    success: bool
    error: Optional[str] = None
    
    # Additional RSI periods if needed (for subscription-based checks)
    rsi_values: Optional[Dict[int, float]] = None  # period -> RSI value


class RSIProviderBase(ABC):
    """
    Abstract base class for RSI data providers.
    
    All providers must implement the `get_rsi_for_tickers` method
    which fetches RSI data for a list of tickers.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this provider (for logging/display)."""
        pass
    
    @abstractmethod
    async def get_rsi_for_tickers(
        self,
        tickers: List[str],
        periods: Optional[List[int]] = None
    ) -> Dict[str, RSIData]:
        """
        Fetch RSI data for a list of tickers.
        
        Args:
            tickers: List of ticker symbols (Yahoo Finance format, e.g., "EQNR.OL", "AAPL")
            periods: Optional list of RSI periods to calculate (default: [14])
        
        Returns:
            Dict mapping ticker -> RSIData
        """
        pass
    
    @abstractmethod
    async def get_rsi_single(
        self,
        ticker: str,
        periods: Optional[List[int]] = None
    ) -> RSIData:
        """
        Fetch RSI data for a single ticker.
        
        Args:
            ticker: Ticker symbol
            periods: Optional list of RSI periods (default: [14])
        
        Returns:
            RSIData for the ticker
        """
        pass
