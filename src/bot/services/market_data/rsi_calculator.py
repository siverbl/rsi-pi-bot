"""bot.services.market_data.rsi_calculator

TradingView-only RSI calculator wrapper.

In this build, RSI data comes from TradingView Screener (pre-calculated RSI14).
The RSICalculator class remains as a small compatibility layer so the rest of the
codebase can keep using the same interface.

Key constraints:
- Only RSI14 is supported.
- No local RSI calculation from historical prices is included.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from bot.services.market_data.providers import RSIData, get_provider

logger = logging.getLogger(__name__)

SUPPORTED_RSI_PERIODS = {14}


@dataclass
class RSIResult:
    """Result of RSI lookup for a single ticker."""

    ticker: str
    rsi_values: Dict[int, float]  # period -> RSI value
    last_date: str
    last_close: float
    success: bool
    error: Optional[str] = None
    data_timestamp: Optional[datetime] = None  # When data was fetched
    name: Optional[str] = None  # Company name if available

    @classmethod
    def from_rsi_data(cls, data: RSIData) -> "RSIResult":
        """Create RSIResult from provider RSIData."""
        rsi_values = data.rsi_values or {}
        if data.rsi_14 is not None and 14 not in rsi_values:
            rsi_values[14] = data.rsi_14

        last_date = ""
        if data.data_timestamp:
            last_date = data.data_timestamp.strftime("%Y-%m-%d")

        return cls(
            ticker=data.ticker,
            rsi_values=rsi_values,
            last_date=last_date,
            last_close=data.close or 0.0,
            success=data.success,
            error=data.error,
            data_timestamp=data.data_timestamp,
            name=data.name,
        )


class RSICalculator:
    """Fetch RSI values for tickers using TradingView Screener."""

    def __init__(self):
        self._provider = None

    @property
    def provider(self):
        """Lazy-load provider."""
        if self._provider is None:
            self._provider = get_provider("tradingview")
        return self._provider

    async def calculate_rsi_for_tickers(self, ticker_periods: Dict[str, List[int]]) -> Dict[str, RSIResult]:
        """Fetch RSI14 for multiple tickers.

        Args:
            ticker_periods: Dict mapping ticker -> list of RSI periods needed.
                           In this build, only period 14 is supported.

        Returns:
            Dict mapping ticker -> RSIResult
        """
        results: Dict[str, RSIResult] = {}
        tickers = list(ticker_periods.keys())
        if not tickers:
            return results

        # Warn if callers request unsupported periods
        requested_periods = {p for periods in ticker_periods.values() for p in periods}
        unsupported = requested_periods - SUPPORTED_RSI_PERIODS
        if unsupported:
            logger.warning(
                "Unsupported RSI periods requested (%s). This build only supports RSI14.",
                sorted(unsupported),
            )

        logger.info("Fetching RSI14 for %d tickers using %s", len(tickers), self.provider.name)

        provider_results = await self.provider.get_rsi_for_tickers(tickers=tickers, periods=[14])

        for ticker, rsi_data in provider_results.items():
            results[ticker] = RSIResult.from_rsi_data(rsi_data)

        # Ensure all requested tickers have a result
        for ticker in tickers:
            if ticker not in results:
                results[ticker] = RSIResult(
                    ticker=ticker,
                    rsi_values={},
                    last_date="",
                    last_close=0.0,
                    success=False,
                    error="No result from provider",
                )

        successful = sum(1 for r in results.values() if r.success)
        failed = len(results) - successful
        logger.info("RSI fetch complete: %d successful, %d failed", successful, failed)

        return results
