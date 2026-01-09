"""
Tests for guild config and schedule toggle functionality.

Run with: pytest tests/test_config_toggle.py -v
"""
import asyncio
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

import pytest


@pytest.fixture
def temp_db():
    """Create a temporary database file for testing."""
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)
    
    yield path
    
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
async def db(temp_db):
    """Create a database instance with temp file."""
    with patch('bot.repositories.database.DB_PATH', Path(temp_db)):
        from bot.repositories.database import Database
        
        database = Database(temp_db)
        await database.initialize()
        yield database


class TestScheduleToggle:
    """Tests for schedule_enabled toggle functionality."""
    
    @pytest.mark.asyncio
    async def test_default_schedule_enabled(self, db):
        """Test that schedule_enabled defaults to True for new guilds."""
        config = await db.get_or_create_guild_config(123456789)
        
        assert config.schedule_enabled is True
    
    @pytest.mark.asyncio
    async def test_disable_schedule(self, db):
        """Test disabling the schedule."""
        guild_id = 123456789
        
        # Create initial config
        await db.get_or_create_guild_config(guild_id)
        
        # Disable schedule
        config = await db.update_guild_config(
            guild_id=guild_id,
            schedule_enabled=False
        )
        
        assert config.schedule_enabled is False
    
    @pytest.mark.asyncio
    async def test_enable_schedule(self, db):
        """Test enabling the schedule after it was disabled."""
        guild_id = 123456789
        
        # Create and disable
        await db.get_or_create_guild_config(guild_id)
        await db.update_guild_config(guild_id=guild_id, schedule_enabled=False)
        
        # Re-enable
        config = await db.update_guild_config(
            guild_id=guild_id,
            schedule_enabled=True
        )
        
        assert config.schedule_enabled is True
    
    @pytest.mark.asyncio
    async def test_schedule_persists(self, db):
        """Test that schedule_enabled persists after re-fetching."""
        guild_id = 123456789
        
        # Create and disable
        await db.get_or_create_guild_config(guild_id)
        await db.update_guild_config(guild_id=guild_id, schedule_enabled=False)
        
        # Fetch fresh
        config = await db.get_guild_config(guild_id)
        
        assert config.schedule_enabled is False
    
    @pytest.mark.asyncio
    async def test_schedule_toggle_independent(self, db):
        """Test that schedule toggle doesn't affect other settings."""
        guild_id = 123456789
        
        # Create with custom settings
        await db.get_or_create_guild_config(guild_id)
        await db.update_guild_config(
            guild_id=guild_id,
            default_cooldown_hours=48,
            auto_oversold_threshold=25
        )
        
        # Toggle schedule
        config = await db.update_guild_config(
            guild_id=guild_id,
            schedule_enabled=False
        )
        
        # Other settings should remain
        assert config.default_cooldown_hours == 48
        assert config.auto_oversold_threshold == 25
        assert config.schedule_enabled is False


class TestGuildConfigUpdate:
    """Tests for guild config update functionality."""
    
    @pytest.mark.asyncio
    async def test_update_multiple_settings(self, db):
        """Test updating multiple settings at once."""
        guild_id = 123456789
        
        await db.get_or_create_guild_config(guild_id)
        
        config = await db.update_guild_config(
            guild_id=guild_id,
            default_cooldown_hours=12,
            schedule_time="09:00",
            auto_oversold_threshold=28,
            auto_overbought_threshold=72,
            schedule_enabled=False
        )
        
        assert config.default_cooldown_hours == 12
        assert config.default_schedule_time == "09:00"
        assert config.auto_oversold_threshold == 28
        assert config.auto_overbought_threshold == 72
        assert config.schedule_enabled is False
    
    @pytest.mark.asyncio
    async def test_partial_update(self, db):
        """Test updating only some settings."""
        guild_id = 123456789
        
        # Create with defaults
        initial = await db.get_or_create_guild_config(guild_id)
        initial_cooldown = initial.default_cooldown_hours
        
        # Update only oversold threshold
        config = await db.update_guild_config(
            guild_id=guild_id,
            auto_oversold_threshold=20
        )
        
        # Oversold changed, cooldown unchanged
        assert config.auto_oversold_threshold == 20
        assert config.default_cooldown_hours == initial_cooldown


class TestMultiGuildConfig:
    """Tests for multiple guild configurations."""
    
    @pytest.mark.asyncio
    async def test_independent_guild_configs(self, db):
        """Test that different guilds have independent configs."""
        guild_1 = 111111111
        guild_2 = 222222222
        
        await db.get_or_create_guild_config(guild_1)
        await db.get_or_create_guild_config(guild_2)
        
        # Disable schedule for guild 1 only
        await db.update_guild_config(guild_id=guild_1, schedule_enabled=False)
        
        config_1 = await db.get_guild_config(guild_1)
        config_2 = await db.get_guild_config(guild_2)
        
        assert config_1.schedule_enabled is False
        assert config_2.schedule_enabled is True
    
    @pytest.mark.asyncio
    async def test_get_all_guild_ids(self, db):
        """Test retrieving all configured guild IDs."""
        guilds = [111, 222, 333]
        
        for guild_id in guilds:
            await db.get_or_create_guild_config(guild_id)
        
        all_ids = await db.get_all_guild_ids()
        
        for guild_id in guilds:
            assert guild_id in all_ids


class TestMigration:
    """Tests for database migration of schedule_enabled column."""
    
    @pytest.mark.asyncio
    async def test_existing_db_migration(self, temp_db):
        """Test that existing databases get the new column."""
        import aiosqlite
        
        # Create old-style database without schedule_enabled
        async with aiosqlite.connect(temp_db) as conn:
            await conn.execute("""
                CREATE TABLE guild_config (
                    guild_id INTEGER PRIMARY KEY,
                    default_channel_id INTEGER,
                    default_rsi_period INTEGER DEFAULT 14,
                    default_schedule_time TEXT DEFAULT '18:30',
                    default_cooldown_hours INTEGER DEFAULT 24,
                    alert_mode TEXT DEFAULT 'CROSSING',
                    hysteresis REAL DEFAULT 2.0,
                    auto_oversold_threshold REAL DEFAULT 34,
                    auto_overbought_threshold REAL DEFAULT 70
                )
            """)
            await conn.execute(
                "INSERT INTO guild_config (guild_id) VALUES (?)",
                (123456789,)
            )
            await conn.commit()
        
        # Initialize database (should migrate)
        from bot.repositories.database import Database
        db = Database(temp_db)
        await db.initialize()
        
        # Should be able to read schedule_enabled
        config = await db.get_guild_config(123456789)
        assert config.schedule_enabled is True  # Default value


class TestSchedulerIntegration:
    """Tests for scheduler integration with schedule_enabled."""
    
    @pytest.mark.asyncio
    async def test_scheduler_checks_enabled(self, db):
        """Test that scheduler respects schedule_enabled flag."""
        # This tests the flow, not the actual scheduler
        guild_id = 123456789
        
        # Get config with schedule disabled
        await db.get_or_create_guild_config(guild_id)
        await db.update_guild_config(guild_id=guild_id, schedule_enabled=False)
        
        config = await db.get_guild_config(guild_id)
        
        # Scheduler should check this before running
        should_run = config.schedule_enabled
        assert should_run is False


# Pytest configuration
@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
