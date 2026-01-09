"""
Tests for ticker catalog removal functionality.

Run with: pytest tests/test_ticker_removal.py -v
"""
import asyncio
import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def temp_csv():
    """Create a temporary CSV file for testing."""
    fd, path = tempfile.mkstemp(suffix='.csv')
    os.close(fd)
    
    # Write test data
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ticker', 'name', 'tradingview_slug'])
        writer.writerow(['EQNR.OL', 'Equinor ASA', 'OSL:EQNR'])
        writer.writerow(['YAR.OL', 'Yara International ASA', 'OSL:YAR'])
        writer.writerow(['AAPL', 'Apple Inc.', 'NASDAQ:AAPL'])
    
    yield path
    
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def patched_tickers_file(temp_csv):
    """Patch TICKERS_FILE to use temp file."""
    with patch('bot.config.TICKERS_FILE', Path(temp_csv)):
        with patch('bot.repositories.ticker_catalog.TICKERS_FILE', Path(temp_csv)):
            yield temp_csv


class TestTickerRemoval:
    """Tests for remove_ticker function."""
    
    @pytest.mark.asyncio
    async def test_remove_existing_ticker(self, patched_tickers_file):
        """Test removing an existing ticker."""
        from bot.repositories.ticker_catalog import remove_ticker
        
        # Remove EQNR.OL
        success, message, removed = await remove_ticker('EQNR.OL')
        
        assert success is True
        assert 'EQNR.OL' in message
        assert removed is not None
        assert removed.ticker == 'EQNR.OL'
        assert removed.name == 'Equinor ASA'
        assert removed.tradingview_slug == 'OSL:EQNR'
        
        # Verify CSV no longer contains EQNR.OL
        with open(patched_tickers_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            tickers = [row['ticker'] for row in reader]
        
        assert 'EQNR.OL' not in tickers
        assert 'YAR.OL' in tickers  # Other tickers still exist
        assert 'AAPL' in tickers
    
    @pytest.mark.asyncio
    async def test_remove_ticker_case_insensitive(self, patched_tickers_file):
        """Test removing ticker with different case."""
        from bot.repositories.ticker_catalog import remove_ticker
        
        # Remove with lowercase
        success, message, removed = await remove_ticker('eqnr.ol')
        
        assert success is True
        assert removed is not None
        assert removed.ticker == 'EQNR.OL'
    
    @pytest.mark.asyncio
    async def test_remove_nonexistent_ticker(self, patched_tickers_file):
        """Test removing a ticker that doesn't exist."""
        from bot.repositories.ticker_catalog import remove_ticker
        
        success, message, removed = await remove_ticker('NOTREAL.OL')
        
        assert success is False
        assert 'not found' in message.lower()
        assert removed is None
    
    @pytest.mark.asyncio
    async def test_csv_integrity_after_removal(self, patched_tickers_file):
        """Test that CSV maintains proper format after removal."""
        from bot.repositories.ticker_catalog import remove_ticker
        
        await remove_ticker('YAR.OL')
        
        # Verify CSV structure is correct
        with open(patched_tickers_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Check header exists
            assert reader.fieldnames == ['ticker', 'name', 'tradingview_slug']
            
            # Check remaining rows
            rows = list(reader)
            assert len(rows) == 2  # EQNR.OL and AAPL remain
            
            # Verify data integrity
            for row in rows:
                assert row['ticker'] in ['EQNR.OL', 'AAPL']
                assert row['name'] != ''
                assert row['tradingview_slug'] != ''
    
    @pytest.mark.asyncio
    async def test_remove_all_tickers(self, patched_tickers_file):
        """Test removing all tickers leaves header intact."""
        from bot.repositories.ticker_catalog import remove_ticker
        
        await remove_ticker('EQNR.OL')
        await remove_ticker('YAR.OL')
        await remove_ticker('AAPL')
        
        # Verify header still exists
        with open(patched_tickers_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == ['ticker', 'name', 'tradingview_slug']
            rows = list(reader)
            assert len(rows) == 0


class TestAtomicWrite:
    """Tests for atomic file write operations."""
    
    @pytest.mark.asyncio
    async def test_atomic_write_no_corruption(self, patched_tickers_file):
        """Test that atomic write prevents file corruption."""
        from bot.repositories.ticker_catalog import remove_ticker
        
        original_content = Path(patched_tickers_file).read_text()
        
        # Perform removal
        await remove_ticker('EQNR.OL')
        
        # File should be valid CSV
        with open(patched_tickers_file, 'r', newline='', encoding='utf-8') as f:
            try:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert isinstance(rows, list)  # Should parse without error
            except csv.Error:
                pytest.fail("CSV file corrupted after removal")


class TestConcurrentAccess:
    """Tests for concurrent access safety."""
    
    @pytest.mark.asyncio
    async def test_concurrent_removals(self, patched_tickers_file):
        """Test that concurrent removals don't corrupt the file."""
        from bot.repositories.ticker_catalog import remove_ticker
        
        # Attempt concurrent removals
        results = await asyncio.gather(
            remove_ticker('EQNR.OL'),
            remove_ticker('YAR.OL'),
            return_exceptions=True
        )
        
        # Both should complete without exceptions
        for result in results:
            assert not isinstance(result, Exception)
        
        # Verify file is still valid
        with open(patched_tickers_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # At least one ticker should remain (AAPL)
        tickers = [row['ticker'] for row in rows]
        assert 'AAPL' in tickers
