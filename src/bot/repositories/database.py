"""
Database module for RSI Discord Bot.
Uses SQLite for persistent storage of subscriptions, state, RSI values, and auto-scan settings.

Key tables:
- guild_config: Per-guild configuration (thresholds, schedule enabled, etc.)
- subscriptions: User-created alert subscriptions
- subscription_state: State tracking for subscriptions (crossing detection, cooldown)
- auto_scan_state: Daily state for change detection in auto-scans
- ticker_rsi: Persistent RSI storage for all tickers (spec section 4)
"""
import aiosqlite
import json
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from contextlib import asynccontextmanager
import logging

from bot.config import (
    DEFAULT_RSI_PERIOD, DEFAULT_COOLDOWN_HOURS, DEFAULT_SCHEDULE_TIME, 
    DEFAULT_ALERT_MODE, DEFAULT_HYSTERESIS, DB_PATH,
    DEFAULT_AUTO_OVERSOLD_THRESHOLD, DEFAULT_AUTO_OVERBOUGHT_THRESHOLD,
    DEFAULT_SCHEDULE_ENABLED
)

logger = logging.getLogger(__name__)


class Condition(Enum):
    UNDER = "UNDER"
    OVER = "OVER"


class AlertMode(Enum):
    CROSSING = "CROSSING"
    LEVEL = "LEVEL"


class Status(Enum):
    ABOVE = "ABOVE"
    BELOW = "BELOW"
    UNKNOWN = "UNKNOWN"


@dataclass
class GuildConfig:
    guild_id: int
    default_channel_id: Optional[int]
    default_rsi_period: int
    default_schedule_time: str
    default_cooldown_hours: int
    alert_mode: str
    hysteresis: float
    auto_oversold_threshold: float = DEFAULT_AUTO_OVERSOLD_THRESHOLD
    auto_overbought_threshold: float = DEFAULT_AUTO_OVERBOUGHT_THRESHOLD
    schedule_enabled: bool = DEFAULT_SCHEDULE_ENABLED


@dataclass
class Subscription:
    id: int
    guild_id: int
    channel_id: Optional[int]
    ticker: str
    condition: str
    threshold: float
    period: int
    cooldown_hours: int
    enabled: bool
    created_by_user_id: Optional[int]
    created_at: datetime
    updated_at: datetime


@dataclass
class SubscriptionState:
    subscription_id: int
    last_rsi: Optional[float]
    last_close: Optional[float]
    last_date: Optional[str]
    last_status: str
    last_alert_at: Optional[datetime]
    days_in_zone: int


@dataclass
class AutoScanState:
    """Tracks the daily state of automatic RSI scans for change detection."""
    guild_id: int
    scan_date: str  # YYYY-MM-DD format
    condition: str  # "UNDER" or "OVER"
    last_tickers: Set[str] = field(default_factory=set)
    last_scan_time: Optional[datetime] = None
    post_count: int = 0


@dataclass
class TickerRSI:
    """Persistent RSI storage for a ticker (spec section 4)."""
    ticker: str
    tradingview_slug: Optional[str]
    rsi_14: float
    last_close: Optional[float]
    data_date: str  # Date of the RSI data (YYYY-MM-DD)
    data_timestamp: Optional[datetime]  # When data was fetched
    updated_at: datetime


class Database:
    def __init__(self, db_path=DB_PATH):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    @asynccontextmanager
    async def connect(self):
        """Open a SQLite connection with recommended pragmas (Pi-friendly)."""
        db = await aiosqlite.connect(self.db_path)
        try:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA synchronous=NORMAL;")
            await db.execute("PRAGMA foreign_keys=ON;")
            yield db
        finally:
            await db.close()

    async def initialize(self):
        """Create database tables if they don't exist."""
        async with self.connect() as db:

            # Guild configuration table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS guild_config (
                    guild_id INTEGER PRIMARY KEY,
                    default_channel_id INTEGER,
                    default_rsi_period INTEGER DEFAULT 14,
                    default_schedule_time TEXT DEFAULT '18:30',
                    default_cooldown_hours INTEGER DEFAULT 24,
                    alert_mode TEXT DEFAULT 'CROSSING',
                    hysteresis REAL DEFAULT 2.0,
                    auto_oversold_threshold REAL DEFAULT 34,
                    auto_overbought_threshold REAL DEFAULT 70,
                    schedule_enabled INTEGER DEFAULT 1
                )
            """)

            # Subscriptions table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    channel_id INTEGER,
                    ticker TEXT NOT NULL,
                    condition TEXT NOT NULL CHECK (condition IN ('UNDER', 'OVER')),
                    threshold REAL NOT NULL,
                    period INTEGER NOT NULL DEFAULT 14,
                    cooldown_hours INTEGER NOT NULL DEFAULT 24,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_by_user_id INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            # Subscription state table for anti-spam and crossing detection
            await db.execute("""
                CREATE TABLE IF NOT EXISTS subscription_state (
                    subscription_id INTEGER PRIMARY KEY,
                    last_rsi REAL,
                    last_close REAL,
                    last_date TEXT,
                    last_status TEXT DEFAULT 'UNKNOWN',
                    last_alert_at TEXT,
                    days_in_zone INTEGER DEFAULT 0,
                    FOREIGN KEY (subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
                )
            """)
            
            # Auto-scan daily state table (for change detection)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS auto_scan_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    guild_id INTEGER NOT NULL,
                    scan_date TEXT NOT NULL,
                    condition TEXT NOT NULL CHECK (condition IN ('UNDER', 'OVER')),
                    tickers_json TEXT NOT NULL DEFAULT '[]',
                    last_scan_time TEXT,
                    post_count INTEGER DEFAULT 0,
                    UNIQUE(guild_id, scan_date, condition)
                )
            """)
            
            # NEW: Ticker RSI persistence table (spec section 4)
            # Stores RSI values for ALL tickers evaluated during scans
            await db.execute("""
                CREATE TABLE IF NOT EXISTS ticker_rsi (
                    ticker TEXT PRIMARY KEY,
                    tradingview_slug TEXT,
                    rsi_14 REAL NOT NULL,
                    last_close REAL,
                    data_date TEXT NOT NULL,
                    data_timestamp TEXT,
                    updated_at TEXT NOT NULL
                )
            """)

            # Create indexes
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_subscriptions_guild 
                ON subscriptions(guild_id)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_subscriptions_ticker 
                ON subscriptions(ticker)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_subscriptions_enabled 
                ON subscriptions(enabled)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_auto_scan_state_guild_date 
                ON auto_scan_state(guild_id, scan_date)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticker_rsi_updated
                ON ticker_rsi(updated_at)
            """)
            
            # Migrations for existing databases
            migrations = [
                "ALTER TABLE guild_config ADD COLUMN auto_oversold_threshold REAL DEFAULT 34",
                "ALTER TABLE guild_config ADD COLUMN auto_overbought_threshold REAL DEFAULT 70",
                "ALTER TABLE guild_config ADD COLUMN schedule_enabled INTEGER DEFAULT 1",
            ]
            for migration in migrations:
                try:
                    await db.execute(migration)
                except Exception:
                    pass  # Column already exists

            # TradingView-only: normalize RSI periods to 14
            try:
                await db.execute("UPDATE guild_config SET default_rsi_period = 14 WHERE default_rsi_period IS NULL OR default_rsi_period != 14")
            except Exception:
                pass
            try:
                await db.execute("UPDATE subscriptions SET period = 14 WHERE period IS NULL OR period != 14")
            except Exception:
                pass

            await db.commit()
            logger.info("Database initialized successfully")

    # ==================== Guild Config Operations ====================

    async def get_guild_config(self, guild_id: int) -> Optional[GuildConfig]:
        """Get guild configuration, returns None if not configured."""
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                    "SELECT * FROM guild_config WHERE guild_id = ?",
                    (guild_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None

                keys = row.keys()
                oversold = row['auto_oversold_threshold'] if 'auto_oversold_threshold' in keys else None
                overbought = row['auto_overbought_threshold'] if 'auto_overbought_threshold' in keys else None
                schedule_enabled = row['schedule_enabled'] if 'schedule_enabled' in keys else 1

                return GuildConfig(
                    guild_id=row['guild_id'],
                    default_channel_id=row['default_channel_id'],
                    default_rsi_period=row['default_rsi_period'],
                    default_schedule_time=row['default_schedule_time'],
                    default_cooldown_hours=row['default_cooldown_hours'],
                    alert_mode=row['alert_mode'],
                    hysteresis=row['hysteresis'],
                    auto_oversold_threshold=DEFAULT_AUTO_OVERSOLD_THRESHOLD if oversold is None else oversold,
                    auto_overbought_threshold=DEFAULT_AUTO_OVERBOUGHT_THRESHOLD if overbought is None else overbought,
                    schedule_enabled=bool(schedule_enabled),
                )

    async def get_or_create_guild_config(self, guild_id: int) -> GuildConfig:
        """Get guild config, creating default if it doesn't exist."""
        config = await self.get_guild_config(guild_id)
        if config:
            return config

        async with self.connect() as db:
            await db.execute(
                """INSERT INTO guild_config (guild_id, default_rsi_period, 
                   default_schedule_time, default_cooldown_hours, alert_mode, hysteresis,
                   auto_oversold_threshold, auto_overbought_threshold, schedule_enabled)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (guild_id, DEFAULT_RSI_PERIOD, DEFAULT_SCHEDULE_TIME,
                 DEFAULT_COOLDOWN_HOURS, DEFAULT_ALERT_MODE, DEFAULT_HYSTERESIS,
                 DEFAULT_AUTO_OVERSOLD_THRESHOLD, DEFAULT_AUTO_OVERBOUGHT_THRESHOLD,
                 1 if DEFAULT_SCHEDULE_ENABLED else 0)
            )
            await db.commit()

        return GuildConfig(
            guild_id=guild_id,
            default_channel_id=None,
            default_rsi_period=DEFAULT_RSI_PERIOD,
            default_schedule_time=DEFAULT_SCHEDULE_TIME,
            default_cooldown_hours=DEFAULT_COOLDOWN_HOURS,
            alert_mode=DEFAULT_ALERT_MODE,
            hysteresis=DEFAULT_HYSTERESIS,
            auto_oversold_threshold=DEFAULT_AUTO_OVERSOLD_THRESHOLD,
            auto_overbought_threshold=DEFAULT_AUTO_OVERBOUGHT_THRESHOLD,
            schedule_enabled=DEFAULT_SCHEDULE_ENABLED
        )

    async def update_guild_config(
        self,
        guild_id: int,
        default_channel_id: Optional[int] = None,
        default_rsi_period: Optional[int] = None,
        default_schedule_time: Optional[str] = None,
        default_cooldown_hours: Optional[int] = None,
        alert_mode: Optional[str] = None,
        hysteresis: Optional[float] = None,
        auto_oversold_threshold: Optional[float] = None,
        auto_overbought_threshold: Optional[float] = None,
        schedule_enabled: Optional[bool] = None
    ) -> GuildConfig:
        """Update guild configuration with provided values."""
        await self.get_or_create_guild_config(guild_id)

        updates = []
        params = []

        if default_channel_id is not None:
            updates.append("default_channel_id = ?")
            params.append(default_channel_id)
        if default_rsi_period is not None:
            updates.append("default_rsi_period = ?")
            params.append(default_rsi_period)
        if default_schedule_time is not None:
            updates.append("default_schedule_time = ?")
            params.append(default_schedule_time)
        if default_cooldown_hours is not None:
            updates.append("default_cooldown_hours = ?")
            params.append(default_cooldown_hours)
        if alert_mode is not None:
            updates.append("alert_mode = ?")
            params.append(alert_mode)
        if hysteresis is not None:
            updates.append("hysteresis = ?")
            params.append(hysteresis)
        if auto_oversold_threshold is not None:
            updates.append("auto_oversold_threshold = ?")
            params.append(auto_oversold_threshold)
        if auto_overbought_threshold is not None:
            updates.append("auto_overbought_threshold = ?")
            params.append(auto_overbought_threshold)
        if schedule_enabled is not None:
            updates.append("schedule_enabled = ?")
            params.append(1 if schedule_enabled else 0)

        if updates:
            params.append(guild_id)
            async with self.connect() as db:
                await db.execute(
                    f"UPDATE guild_config SET {', '.join(updates)} WHERE guild_id = ?",
                    params
                )
                await db.commit()

        return await self.get_guild_config(guild_id)

    # ==================== Subscription Operations ====================

    async def create_subscription(
        self,
        guild_id: int,
        ticker: str,
        condition: str,
        threshold: float,
        period: int,
        cooldown_hours: int,
        created_by_user_id: Optional[int] = None,
        channel_id: Optional[int] = None,
        enabled: bool = True
    ) -> Subscription:
        """Create a new subscription."""
        now = datetime.utcnow().isoformat()

        async with self.connect() as db:
            cursor = await db.execute(
                """INSERT INTO subscriptions 
                   (guild_id, channel_id, ticker, condition, threshold, period, 
                    cooldown_hours, enabled, created_by_user_id, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (guild_id, channel_id, ticker.upper(), condition.upper(),
                 threshold, period, cooldown_hours, int(enabled), created_by_user_id, now, now)
            )
            subscription_id = cursor.lastrowid

            # Create initial state record
            await db.execute(
                """INSERT INTO subscription_state 
                   (subscription_id, last_status, days_in_zone)
                   VALUES (?, 'UNKNOWN', 0)""",
                (subscription_id,)
            )

            await db.commit()

            return Subscription(
                id=subscription_id,
                guild_id=guild_id,
                channel_id=channel_id,
                ticker=ticker.upper(),
                condition=condition.upper(),
                threshold=threshold,
                period=period,
                cooldown_hours=cooldown_hours,
                enabled=enabled,
                created_by_user_id=created_by_user_id,
                created_at=datetime.fromisoformat(now),
                updated_at=datetime.fromisoformat(now)
            )

    async def get_subscription(self, subscription_id: int) -> Optional[Subscription]:
        """Get a subscription by ID."""
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM subscriptions WHERE id = ?",
                (subscription_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return self._row_to_subscription(row)
                return None

    async def get_subscriptions_by_guild(
        self,
        guild_id: int,
        channel_id: Optional[int] = None,
        ticker: Optional[str] = None,
        enabled_only: bool = False
    ) -> List[Subscription]:
        """Get subscriptions for a guild with optional filters."""
        query = "SELECT * FROM subscriptions WHERE guild_id = ?"
        params = [guild_id]

        if channel_id is not None:
            query += " AND channel_id = ?"
            params.append(channel_id)
        if ticker is not None:
            query += " AND ticker = ?"
            params.append(ticker.upper())
        if enabled_only:
            query += " AND enabled = 1"

        query += " ORDER BY ticker, condition, threshold"

        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_subscription(row) for row in rows]

    async def get_all_enabled_subscriptions(self) -> List[Subscription]:
        """Get all enabled subscriptions across all guilds."""
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM subscriptions WHERE enabled = 1"
            ) as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_subscription(row) for row in rows]

    async def delete_subscription(self, subscription_id: int, guild_id: int) -> bool:
        """Delete a subscription by ID (must match guild_id for security)."""
        async with self.connect() as db:
            cursor = await db.execute(
                "DELETE FROM subscriptions WHERE id = ? AND guild_id = ?",
                (subscription_id, guild_id)
            )
            await db.commit()
            return cursor.rowcount > 0

    async def subscription_exists(
        self,
        guild_id: int,
        ticker: str,
        condition: str,
        threshold: float,
        period: int
    ) -> bool:
        """Check if a subscription with these exact parameters already exists."""
        async with self.connect() as db:
            async with db.execute(
                """SELECT 1 FROM subscriptions 
                   WHERE guild_id = ? AND ticker = ? 
                   AND condition = ? AND threshold = ? AND period = ?""",
                (guild_id, ticker.upper(), condition.upper(), threshold, period)
            ) as cursor:
                return await cursor.fetchone() is not None

    async def delete_user_subscriptions(self, guild_id: int, user_id: int) -> int:
        """Delete all subscriptions created by a specific user in a guild."""
        async with self.connect() as db:
            async with db.execute(
                """SELECT id FROM subscriptions 
                   WHERE guild_id = ? AND created_by_user_id = ?""",
                (guild_id, user_id)
            ) as cursor:
                rows = await cursor.fetchall()
                sub_ids = [row[0] for row in rows]
            
            if not sub_ids:
                return 0
            
            placeholders = ','.join('?' * len(sub_ids))
            await db.execute(
                f"DELETE FROM subscription_state WHERE subscription_id IN ({placeholders})",
                sub_ids
            )
            
            cursor = await db.execute(
                "DELETE FROM subscriptions WHERE guild_id = ? AND created_by_user_id = ?",
                (guild_id, user_id)
            )
            await db.commit()
            return cursor.rowcount

    async def get_user_subscriptions(self, guild_id: int, user_id: int) -> List[Subscription]:
        """Get all subscriptions created by a specific user in a guild."""
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT * FROM subscriptions 
                   WHERE guild_id = ? AND created_by_user_id = ?
                   ORDER BY ticker, condition, threshold""",
                (guild_id, user_id)
            ) as cursor:
                rows = await cursor.fetchall()
                return [self._row_to_subscription(row) for row in rows]

    # ==================== Subscription State Operations ====================

    async def get_subscription_state(self, subscription_id: int) -> Optional[SubscriptionState]:
        """Get state for a subscription."""
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM subscription_state WHERE subscription_id = ?",
                (subscription_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return SubscriptionState(
                        subscription_id=row['subscription_id'],
                        last_rsi=row['last_rsi'],
                        last_close=row['last_close'],
                        last_date=row['last_date'],
                        last_status=row['last_status'],
                        last_alert_at=datetime.fromisoformat(row['last_alert_at']) if row['last_alert_at'] else None,
                        days_in_zone=row['days_in_zone'] or 0
                    )
                return None

    async def update_subscription_state(
        self,
        subscription_id: int,
        last_rsi: Optional[float] = None,
        last_close: Optional[float] = None,
        last_date: Optional[str] = None,
        last_status: Optional[str] = None,
        last_alert_at: Optional[datetime] = None,
        days_in_zone: Optional[int] = None
    ):
        """Update subscription state."""
        updates = []
        params = []

        if last_rsi is not None:
            updates.append("last_rsi = ?")
            params.append(last_rsi)
        if last_close is not None:
            updates.append("last_close = ?")
            params.append(last_close)
        if last_date is not None:
            updates.append("last_date = ?")
            params.append(last_date)
        if last_status is not None:
            updates.append("last_status = ?")
            params.append(last_status)
        if last_alert_at is not None:
            updates.append("last_alert_at = ?")
            params.append(last_alert_at.isoformat())
        if days_in_zone is not None:
            updates.append("days_in_zone = ?")
            params.append(days_in_zone)

        if updates:
            params.append(subscription_id)
            async with self.connect() as db:
                await db.execute(
                    f"UPDATE subscription_state SET {', '.join(updates)} WHERE subscription_id = ?",
                    params
                )
                await db.commit()

    async def get_subscriptions_with_state(self, guild_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get subscriptions joined with their state."""
        query = """
            SELECT s.*, st.last_rsi, st.last_close, st.last_date, 
                   st.last_status, st.last_alert_at, st.days_in_zone
            FROM subscriptions s
            LEFT JOIN subscription_state st ON s.id = st.subscription_id
            WHERE s.enabled = 1
        """
        params = []

        if guild_id is not None:
            query += " AND s.guild_id = ?"
            params.append(guild_id)

        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    # ==================== Auto-Scan State Operations ====================
    
    async def get_auto_scan_state(
        self,
        guild_id: int,
        scan_date: str,
        condition: str
    ) -> Optional[AutoScanState]:
        """Get the auto-scan state for a guild/date/condition."""
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT * FROM auto_scan_state 
                   WHERE guild_id = ? AND scan_date = ? AND condition = ?""",
                (guild_id, scan_date, condition)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    tickers_json = row['tickers_json'] or '[]'
                    tickers = set(json.loads(tickers_json))
                    last_scan_time = None
                    if row['last_scan_time']:
                        try:
                            last_scan_time = datetime.fromisoformat(row['last_scan_time'])
                        except Exception:
                            pass
                    return AutoScanState(
                        guild_id=row['guild_id'],
                        scan_date=row['scan_date'],
                        condition=row['condition'],
                        last_tickers=tickers,
                        last_scan_time=last_scan_time,
                        post_count=row['post_count'] or 0
                    )
                return None
    
    async def update_auto_scan_state(
        self,
        guild_id: int,
        scan_date: str,
        condition: str,
        tickers: Set[str],
        increment_post_count: bool = False
    ) -> AutoScanState:
        """Update or create auto-scan state."""
        tickers_json = json.dumps(sorted(list(tickers)))
        now = datetime.utcnow().isoformat()
        
        async with self.connect() as db:
            existing = await self.get_auto_scan_state(guild_id, scan_date, condition)
            
            if existing:
                if increment_post_count:
                    await db.execute(
                        """UPDATE auto_scan_state 
                           SET tickers_json = ?, last_scan_time = ?, post_count = post_count + 1
                           WHERE guild_id = ? AND scan_date = ? AND condition = ?""",
                        (tickers_json, now, guild_id, scan_date, condition)
                    )
                else:
                    await db.execute(
                        """UPDATE auto_scan_state 
                           SET tickers_json = ?, last_scan_time = ?
                           WHERE guild_id = ? AND scan_date = ? AND condition = ?""",
                        (tickers_json, now, guild_id, scan_date, condition)
                    )
            else:
                post_count = 1 if increment_post_count else 0
                await db.execute(
                    """INSERT INTO auto_scan_state 
                       (guild_id, scan_date, condition, tickers_json, last_scan_time, post_count)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (guild_id, scan_date, condition, tickers_json, now, post_count)
                )
            await db.commit()
        
        return await self.get_auto_scan_state(guild_id, scan_date, condition)
    
    async def cleanup_old_auto_scan_states(self, days_to_keep: int = 7):
        """Clean up old auto-scan state records."""
        cutoff_date = (datetime.utcnow() - timedelta(days=days_to_keep)).strftime("%Y-%m-%d")
        
        async with self.connect() as db:
            await db.execute(
                "DELETE FROM auto_scan_state WHERE scan_date < ?",
                (cutoff_date,)
            )
            await db.commit()

    # ==================== Ticker RSI Persistence (Spec Section 4) ====================
    
    async def upsert_ticker_rsi(
        self,
        ticker: str,
        rsi_14: float,
        data_date: str,
        tradingview_slug: Optional[str] = None,
        last_close: Optional[float] = None,
        data_timestamp: Optional[datetime] = None
    ) -> TickerRSI:
        """
        Insert or update RSI value for a ticker.
        This stores RSI values for ALL evaluated tickers (catalog + subscriptions).
        """
        now = datetime.utcnow()
        data_ts_str = data_timestamp.isoformat() if data_timestamp else None
        
        async with self.connect() as db:
            await db.execute(
                """INSERT INTO ticker_rsi 
                   (ticker, tradingview_slug, rsi_14, last_close, data_date, data_timestamp, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(ticker) DO UPDATE SET
                       tradingview_slug = COALESCE(excluded.tradingview_slug, ticker_rsi.tradingview_slug),
                       rsi_14 = excluded.rsi_14,
                       last_close = excluded.last_close,
                       data_date = excluded.data_date,
                       data_timestamp = excluded.data_timestamp,
                       updated_at = excluded.updated_at
                """,
                (ticker.upper(), tradingview_slug, rsi_14, last_close, data_date, data_ts_str, now.isoformat())
            )
            await db.commit()
        
        return TickerRSI(
            ticker=ticker.upper(),
            tradingview_slug=tradingview_slug,
            rsi_14=rsi_14,
            last_close=last_close,
            data_date=data_date,
            data_timestamp=data_timestamp,
            updated_at=now
        )
    
    async def upsert_ticker_rsi_batch(
        self,
        rsi_data: List[Dict[str, Any]]
    ) -> int:
        """
        Batch insert/update RSI values for multiple tickers.
        
        Args:
            rsi_data: List of dicts with keys: ticker, rsi_14, data_date, 
                      and optional: tradingview_slug, last_close, data_timestamp
        
        Returns:
            Number of tickers updated
        """
        if not rsi_data:
            return 0
        
        now = datetime.utcnow().isoformat()
        count = 0
        
        async with self.connect() as db:
            for item in rsi_data:
                data_ts = item.get('data_timestamp')
                data_ts_str = data_ts.isoformat() if isinstance(data_ts, datetime) else data_ts
                
                await db.execute(
                    """INSERT INTO ticker_rsi 
                       (ticker, tradingview_slug, rsi_14, last_close, data_date, data_timestamp, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(ticker) DO UPDATE SET
                           tradingview_slug = COALESCE(excluded.tradingview_slug, ticker_rsi.tradingview_slug),
                           rsi_14 = excluded.rsi_14,
                           last_close = excluded.last_close,
                           data_date = excluded.data_date,
                           data_timestamp = excluded.data_timestamp,
                           updated_at = excluded.updated_at
                    """,
                    (
                        item['ticker'].upper(),
                        item.get('tradingview_slug'),
                        item['rsi_14'],
                        item.get('last_close'),
                        item['data_date'],
                        data_ts_str,
                        now
                    )
                )
                count += 1
            
            await db.commit()
        
        logger.debug(f"Batch upserted {count} ticker RSI records")
        return count
    
    async def get_ticker_rsi(self, ticker: str) -> Optional[TickerRSI]:
        """Get stored RSI data for a ticker."""
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM ticker_rsi WHERE ticker = ?",
                (ticker.upper(),)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    data_timestamp = None
                    if row['data_timestamp']:
                        try:
                            data_timestamp = datetime.fromisoformat(row['data_timestamp'])
                        except Exception:
                            pass
                    
                    return TickerRSI(
                        ticker=row['ticker'],
                        tradingview_slug=row['tradingview_slug'],
                        rsi_14=row['rsi_14'],
                        last_close=row['last_close'],
                        data_date=row['data_date'],
                        data_timestamp=data_timestamp,
                        updated_at=datetime.fromisoformat(row['updated_at'])
                    )
                return None
    
    async def get_all_ticker_rsi(self) -> List[TickerRSI]:
        """Get all stored ticker RSI values."""
        async with self.connect() as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT * FROM ticker_rsi ORDER BY ticker") as cursor:
                rows = await cursor.fetchall()
                results = []
                for row in rows:
                    data_timestamp = None
                    if row['data_timestamp']:
                        try:
                            data_timestamp = datetime.fromisoformat(row['data_timestamp'])
                        except Exception:
                            pass
                    
                    results.append(TickerRSI(
                        ticker=row['ticker'],
                        tradingview_slug=row['tradingview_slug'],
                        rsi_14=row['rsi_14'],
                        last_close=row['last_close'],
                        data_date=row['data_date'],
                        data_timestamp=data_timestamp,
                        updated_at=datetime.fromisoformat(row['updated_at'])
                    ))
                return results
    
    async def cleanup_old_ticker_rsi(self, days_to_keep: int = 30):
        """Remove ticker RSI records older than specified days."""
        cutoff = (datetime.utcnow() - timedelta(days=days_to_keep)).isoformat()
        
        async with self.connect() as db:
            cursor = await db.execute(
                "DELETE FROM ticker_rsi WHERE updated_at < ?",
                (cutoff,)
            )
            await db.commit()
            return cursor.rowcount

    # ==================== Helper Methods ====================

    def _row_to_subscription(self, row) -> Subscription:
        """Convert a database row to a Subscription object."""
        return Subscription(
            id=row['id'],
            guild_id=row['guild_id'],
            channel_id=row['channel_id'],
            ticker=row['ticker'],
            condition=row['condition'],
            threshold=row['threshold'],
            period=row['period'],
            cooldown_hours=row['cooldown_hours'],
            enabled=bool(row['enabled']),
            created_by_user_id=row['created_by_user_id'] if 'created_by_user_id' in row.keys() else None,
            created_at=datetime.fromisoformat(row['created_at']),
            updated_at=datetime.fromisoformat(row['updated_at'])
        )

    async def get_unique_tickers(self) -> List[str]:
        """Get list of unique tickers with active subscriptions."""
        async with self.connect() as db:
            async with db.execute(
                "SELECT DISTINCT ticker FROM subscriptions WHERE enabled = 1"
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def get_unique_periods_for_ticker(self, ticker: str) -> List[int]:
        """Get unique RSI periods needed for a ticker."""
        async with self.connect() as db:
            async with db.execute(
                """SELECT DISTINCT period FROM subscriptions 
                   WHERE ticker = ? AND enabled = 1""",
                (ticker.upper(),)
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
    
    async def get_all_guild_ids(self) -> List[int]:
        """Get all guild IDs that have configurations."""
        async with self.connect() as db:
            async with db.execute(
                "SELECT DISTINCT guild_id FROM guild_config"
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
