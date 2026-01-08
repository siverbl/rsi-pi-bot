"""
TradingView Screener RSI Provider.

Uses the tradingview_screener package to fetch RSI data from TradingView's screener API.
This is the default provider as it provides pre-calculated RSI values efficiently.

Ticker mapping uses tradingview_slug from tickers.csv (already in EXCHANGE:SYMBOL format).

Documentation: https://shner-elmo.github.io/TradingView-Screener/
"""
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from bot.config import (
    TV_BATCH_SIZE, TV_BATCH_DELAY_SECONDS,
    RETRY_MAX_ATTEMPTS, RETRY_DELAY_SECONDS, RETRY_BATCH_SIZE
)
from bot.services.market_data.providers.base import RSIProviderBase, RSIData
from bot.repositories.ticker_catalog import get_catalog

logger = logging.getLogger(__name__)


class TradingViewProvider(RSIProviderBase):
    """
    RSI provider using TradingView Screener API.
    
    Features:
    - Batch queries for efficiency (up to 50 tickers per request)
    - Pre-calculated RSI values from TradingView
    - Uses tradingview_slug from tickers.csv for accurate ticker mapping
    """
    
    def __init__(self, batch_size: int = TV_BATCH_SIZE):
        self.batch_size = batch_size
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._catalog = get_catalog()
    
    @property
    def name(self) -> str:
        return "TradingView Screener"
    
    def _get_tradingview_ticker(self, yahoo_ticker: str) -> Optional[str]:
        """
        Get TradingView ticker from catalog's tradingview_slug.
        
        Args:
            yahoo_ticker: Yahoo Finance ticker (e.g., "EQNR.OL", "AAPL")
        
        Returns:
            TradingView ticker (e.g., "OSL:EQNR") or None if not found
        """
        instrument = self._catalog.get_instrument(yahoo_ticker)
        if instrument and instrument.tradingview_slug:
            return instrument.tradingview_slug
        return None
    
    def _fetch_batch_sync(
        self,
        tv_tickers: List[str],
        yahoo_tickers: List[str]
    ) -> Dict[str, RSIData]:
        """
        Synchronously fetch RSI data for a batch of tickers.
        
        Args:
            tv_tickers: List of TradingView-formatted tickers (e.g., "OSL:EQNR")
            yahoo_tickers: Corresponding Yahoo Finance tickers for result mapping
        
        Returns:
            Dict mapping Yahoo ticker -> RSIData
        """
        from tradingview_screener import Query
        
        results = {}
        fetch_time = datetime.utcnow()
        
        try:
            # Build query for specific tickers
            # Request: name, close, RSI, and update_mode for timestamp info
            query = (
                Query()
                .select('name', 'close', 'RSI', 'RSI[1]', 'update_mode')
                .set_tickers(*tv_tickers)
                .limit(len(tv_tickers))
            )
            
            # Execute query
            count, df = query.get_scanner_data()
            
            if df is None or df.empty:
                # No data returned
                for yf_ticker in yahoo_tickers:
                    results[yf_ticker] = RSIData(
                        ticker=yf_ticker,
                        name=None,
                        rsi_14=None,
                        close=None,
                        data_timestamp=fetch_time,
                        success=False,
                        error="No data from TradingView"
                    )
                return results
            
            # Map TradingView tickers back to Yahoo tickers
            tv_to_yahoo = dict(zip(tv_tickers, yahoo_tickers))
            
            # Process results
            for _, row in df.iterrows():
                tv_ticker = row.get('ticker', '')
                if tv_ticker not in tv_to_yahoo:
                    continue
                
                yf_ticker = tv_to_yahoo[tv_ticker]
                
                try:
                    rsi_value = row.get('RSI')
                    if rsi_value is not None:
                        rsi_value = float(rsi_value)
                    
                    close_value = row.get('close')
                    if close_value is not None:
                        close_value = float(close_value)
                    
                    # Get name from catalog (more reliable than TradingView)
                    instrument = self._catalog.get_instrument(yf_ticker)
                    name = instrument.name if instrument else row.get('name', yf_ticker)
                    
                    results[yf_ticker] = RSIData(
                        ticker=yf_ticker,
                        name=str(name) if name else None,
                        rsi_14=rsi_value,
                        close=close_value,
                        data_timestamp=fetch_time,
                        success=rsi_value is not None,
                        error=None if rsi_value is not None else "RSI value not available",
                        rsi_values={14: rsi_value} if rsi_value is not None else None
                    )
                except Exception as e:
                    results[yf_ticker] = RSIData(
                        ticker=yf_ticker,
                        name=None,
                        rsi_14=None,
                        close=None,
                        data_timestamp=fetch_time,
                        success=False,
                        error=str(e)
                    )
            
            # Mark any missing tickers as failed
            for yf_ticker in yahoo_tickers:
                if yf_ticker not in results:
                    results[yf_ticker] = RSIData(
                        ticker=yf_ticker,
                        name=None,
                        rsi_14=None,
                        close=None,
                        data_timestamp=fetch_time,
                        success=False,
                        error="Ticker not found in TradingView results"
                    )
            
            return results
            
        except Exception as e:
            logger.error(f"TradingView batch fetch error: {e}")
            # Return error results for all tickers in batch
            for yf_ticker in yahoo_tickers:
                results[yf_ticker] = RSIData(
                    ticker=yf_ticker,
                    name=None,
                    rsi_14=None,
                    close=None,
                    data_timestamp=fetch_time,
                    success=False,
                    error=str(e)
                )
            return results
    
    async def get_rsi_for_tickers(
        self,
        tickers: List[str],
        periods: Optional[List[int]] = None
    ) -> Dict[str, RSIData]:
        """
        Fetch RSI data for multiple tickers.
        
        Note: TradingView Screener only provides RSI14 (14-period RSI).
        Other periods are not available through this API.
        
        Failed tickers are automatically retried up to RETRY_MAX_ATTEMPTS times.
        """
        if not tickers:
            return {}
        
        results: Dict[str, RSIData] = {}
        
        # Convert Yahoo tickers to TradingView format using catalog
        ticker_mapping = []  # List of (tv_ticker, yahoo_ticker) pairs
        for yf_ticker in tickers:
            tv_ticker = self._get_tradingview_ticker(yf_ticker)
            if tv_ticker:
                ticker_mapping.append((tv_ticker, yf_ticker))
            else:
                # No tradingview_slug in catalog - mark as failed
                results[yf_ticker] = RSIData(
                    ticker=yf_ticker,
                    name=None,
                    rsi_14=None,
                    close=None,
                    data_timestamp=datetime.utcnow(),
                    success=False,
                    error="No tradingview_slug in catalog"
                )
        
        if not ticker_mapping:
            return results
        
        logger.info(f"Fetching RSI for {len(ticker_mapping)} tickers via TradingView Screener")
        
        # Process in batches
        loop = asyncio.get_event_loop()
        
        for i in range(0, len(ticker_mapping), self.batch_size):
            batch = ticker_mapping[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1
            total_batches = (len(ticker_mapping) + self.batch_size - 1) // self.batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickers)")
            
            tv_tickers = [t[0] for t in batch]
            yf_tickers = [t[1] for t in batch]
            
            # Run sync function in executor
            batch_results = await loop.run_in_executor(
                self._executor,
                self._fetch_batch_sync,
                tv_tickers,
                yf_tickers
            )
            
            results.update(batch_results)
            
            # Delay between batches
            if i + self.batch_size < len(ticker_mapping):
                await asyncio.sleep(TV_BATCH_DELAY_SECONDS)
        
        # Collect failed tickers for retry
        failed_tickers = [
            (tv, yf) for tv, yf in ticker_mapping 
            if yf in results and not results[yf].success
        ]
        
        # Retry failed tickers
        if failed_tickers:
            results = await self._retry_failed_tickers(results, failed_tickers, loop)
        
        # Log final summary
        successful = sum(1 for r in results.values() if r.success)
        failed = len(results) - successful
        logger.info(f"TradingView fetch complete: {successful} successful, {failed} failed")
        
        return results
    
    async def _retry_failed_tickers(
        self,
        results: Dict[str, RSIData],
        failed_tickers: List[Tuple[str, str]],
        loop
    ) -> Dict[str, RSIData]:
        """
        Retry failed tickers with smaller batches and delays.
        
        Args:
            results: Current results dict to update
            failed_tickers: List of (tv_ticker, yahoo_ticker) pairs that failed
            loop: Event loop for executor
        
        Returns:
            Updated results dict
        """
        logger.info(f"Retrying {len(failed_tickers)} failed tickers (up to {RETRY_MAX_ATTEMPTS} attempts)")
        
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            if not failed_tickers:
                break
            
            logger.info(f"Retry attempt {attempt}/{RETRY_MAX_ATTEMPTS} for {len(failed_tickers)} tickers")
            
            # Wait before retry
            await asyncio.sleep(RETRY_DELAY_SECONDS)
            
            still_failed = []
            
            # Process in smaller batches for retries
            for i in range(0, len(failed_tickers), RETRY_BATCH_SIZE):
                batch = failed_tickers[i:i + RETRY_BATCH_SIZE]
                
                tv_tickers = [t[0] for t in batch]
                yf_tickers = [t[1] for t in batch]
                
                logger.info(f"Retry batch: {len(batch)} tickers")
                
                # Run sync function in executor
                batch_results = await loop.run_in_executor(
                    self._executor,
                    self._fetch_batch_sync,
                    tv_tickers,
                    yf_tickers
                )
                
                # Check results and update
                for tv_ticker, yf_ticker in batch:
                    if yf_ticker in batch_results:
                        result = batch_results[yf_ticker]
                        if result.success:
                            results[yf_ticker] = result
                            logger.info(f"Retry successful for {yf_ticker}")
                        else:
                            still_failed.append((tv_ticker, yf_ticker))
                
                # Small delay between retry batches
                if i + RETRY_BATCH_SIZE < len(failed_tickers):
                    await asyncio.sleep(RETRY_DELAY_SECONDS / 2)
            
            failed_tickers = still_failed
            
            if not failed_tickers:
                logger.info(f"All retries successful after attempt {attempt}")
                break
        
        if failed_tickers:
            failed_symbols = [yf for _, yf in failed_tickers]
            logger.warning(f"Still failed after {RETRY_MAX_ATTEMPTS} retries: {failed_symbols}")
        
        return results
    
    async def get_rsi_single(
        self,
        ticker: str,
        periods: Optional[List[int]] = None
    ) -> RSIData:
        """Fetch RSI for a single ticker."""
        results = await self.get_rsi_for_tickers([ticker], periods)
        return results.get(ticker, RSIData(
            ticker=ticker,
            name=None,
            rsi_14=None,
            close=None,
            data_timestamp=datetime.utcnow(),
            success=False,
            error="Ticker not found in results"
        ))
