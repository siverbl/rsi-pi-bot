"""bot.services.market_data.providers

TradingView-only RSI provider layer.

This build intentionally supports a single RSI data source:
- TradingView Screener via the `tradingview_screener` package.

Important:
- TradingView Screener exposes **RSI14** (14-period RSI). Other RSI periods are
  not available in this build.

The public API is kept stable:
    from bot.services.market_data.providers import get_provider

    provider = get_provider()
    results = await provider.get_rsi_for_tickers(["EQNR.OL", "AAPL"])
"""

import logging
from typing import Optional

from bot.services.market_data.providers.base import RSIProviderBase, RSIData

logger = logging.getLogger(__name__)

# Cached provider instance (single provider in this build)
_provider_instance: Optional[RSIProviderBase] = None


def get_provider(provider_name: Optional[str] = None) -> RSIProviderBase:
    """Return the (single) TradingView provider instance.

    Args:
        provider_name: Optional provider name override. In this build, only
            "tradingview" is supported.

    Raises:
        ValueError: If provider_name is provided and is not "tradingview".
    """
    global _provider_instance

    if provider_name:
        name = provider_name.lower().strip()
        if name not in {"tradingview", "tv", "trading_view", "tradingview_screener"}:
            raise ValueError(
                "This build is TradingView-only. "
                "This build supports only TradingView Screener (RSI14)."
            )

    if _provider_instance is None:
        from bot.services.market_data.providers.tradingview_provider import TradingViewProvider

        _provider_instance = TradingViewProvider()
        logger.info("Using RSI provider: %s", _provider_instance.name)

    return _provider_instance


def reset_provider() -> None:
    """Reset the cached provider instance (primarily for tests)."""
    global _provider_instance
    _provider_instance = None


__all__ = [
    "RSIProviderBase",
    "RSIData",
    "get_provider",
    "reset_provider",
]
