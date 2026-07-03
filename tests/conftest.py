"""
Shared fixtures and fake Discord objects for the test suite.

No real Discord connection, no live TradingView calls, no real token:
- Database uses temporary SQLite files.
- RSI data comes from FakeRSICalculator.
- Guilds/channels are lightweight fakes with the attributes the bot uses.
"""
import os
import tempfile

# Point runtime paths at a temp dir BEFORE any bot.* import so the test run
# does not write a DB/log into the repository.
_TEST_RUNTIME = tempfile.mkdtemp(prefix="rsi-bot-tests-")
os.environ.setdefault("DB_PATH", os.path.join(_TEST_RUNTIME, "runtime.db"))
os.environ.setdefault("LOG_PATH", os.path.join(_TEST_RUNTIME, "runtime.log"))

from datetime import datetime
from types import SimpleNamespace
from typing import Dict, List, Optional

import pytest

from bot.config import (
    OVERSOLD_CHANNEL_NAME, OVERBOUGHT_CHANNEL_NAME,
    CHANGELOG_CHANNEL_NAME,
)
from bot.repositories.database import Database
from bot.repositories.ticker_catalog import Instrument
from bot.services.market_data.rsi_calculator import RSIResult


# ==================== Fake Discord objects ====================

class FakeChannel:
    """Text channel double that records sent messages."""

    def __init__(self, name: str, can_send: bool = True):
        self.name = name
        self._can_send = can_send
        self.sent: List[str] = []

    @property
    def mention(self) -> str:
        return f"#{self.name}"

    def permissions_for(self, member) -> SimpleNamespace:
        return SimpleNamespace(send_messages=self._can_send)

    async def send(self, content=None, **kwargs):
        self.sent.append(content)
        return SimpleNamespace(id=len(self.sent))


class FakeGuild:
    def __init__(self, guild_id: int, name: str = "test-guild",
                 channels: Optional[List[FakeChannel]] = None):
        self.id = guild_id
        self.name = name
        self.me = SimpleNamespace(id=999)
        self.text_channels = channels if channels is not None else []

    def channel(self, name: str) -> Optional[FakeChannel]:
        for ch in self.text_channels:
            if ch.name == name:
                return ch
        return None


class FakeBot:
    """Just enough of discord.ext.commands.Bot for RSIScheduler."""

    def __init__(self, db: Database, guilds: Optional[List[FakeGuild]] = None):
        self.db = db
        self._guilds = guilds or []

    @property
    def guilds(self) -> List[FakeGuild]:
        return self._guilds

    def get_guild(self, guild_id: int) -> Optional[FakeGuild]:
        for g in self._guilds:
            if g.id == guild_id:
                return g
        return None

    async def wait_until_ready(self):
        return


class FakeUser:
    def __init__(self, user_id: int = 42, name: str = "tester", permissions=None):
        import discord
        self.id = user_id
        self.name = name
        self.mention = f"<@{user_id}>"
        self.guild_permissions = permissions if permissions is not None else discord.Permissions.none()

    def __str__(self):
        return self.name


class _Recorder:
    """Records async calls with their args/kwargs."""

    def __init__(self):
        self.calls = []

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))


class FakeInteraction:
    """Interaction double for exercising slash-command callbacks."""

    def __init__(self, guild: Optional[FakeGuild], user: FakeUser):
        self.guild = guild
        self.guild_id = guild.id if guild else None
        self.user = user
        self.response = SimpleNamespace(defer=_Recorder())
        self.followup = SimpleNamespace(send=_Recorder())
        self._edits = _Recorder()

    async def edit_original_response(self, **kwargs):
        await self._edits(**kwargs)

    @property
    def messages(self) -> List[str]:
        """All texts sent via followup.send (positional or content kwarg)."""
        out = []
        for args, kwargs in self.followup.send.calls:
            out.append(args[0] if args else kwargs.get("content", ""))
        return out

    @property
    def edited(self) -> List[str]:
        return [kwargs.get("content", "") for _, kwargs in self._edits.calls]


# ==================== Fake market data / catalog ====================

class FakeRSICalculator:
    """Serves canned RSI values; records every fetch call."""

    def __init__(self, rsi_map: Optional[Dict[str, float]] = None, fail_all: bool = False):
        self.rsi_map = dict(rsi_map or {})
        self.fail_all = fail_all
        self.calls: List[List[str]] = []

    async def calculate_rsi_for_tickers(self, ticker_periods) -> Dict[str, RSIResult]:
        tickers = list(ticker_periods.keys())
        self.calls.append(sorted(tickers))
        results = {}
        for ticker in tickers:
            if self.fail_all or ticker not in self.rsi_map:
                results[ticker] = RSIResult(
                    ticker=ticker, rsi_values={}, last_date="", last_close=0.0,
                    success=False, error="simulated fetch failure",
                )
            else:
                results[ticker] = RSIResult(
                    ticker=ticker,
                    rsi_values={14: self.rsi_map[ticker]},
                    last_date="2026-07-03",
                    last_close=100.0,
                    success=True,
                    data_timestamp=datetime.utcnow(),
                )
        return results


class FakeCatalog:
    def __init__(self, tickers: Dict[str, str]):
        self._tickers = {t.upper(): name for t, name in tickers.items()}

    def get_all_tickers(self) -> List[str]:
        return list(self._tickers.keys())

    def get_instrument(self, ticker: str) -> Optional[Instrument]:
        ticker = ticker.upper()
        if ticker in self._tickers:
            base = ticker.split('.')[0]
            return Instrument(ticker=ticker, name=self._tickers[ticker],
                              tradingview_slug=f"TEST:{base}")
        return None

    def __len__(self):
        return len(self._tickers)


# ==================== Fixtures ====================

def make_standard_channels(can_send: bool = True) -> List[FakeChannel]:
    return [
        FakeChannel(OVERSOLD_CHANNEL_NAME, can_send=can_send),
        FakeChannel(OVERBOUGHT_CHANNEL_NAME, can_send=can_send),
        FakeChannel(CHANGELOG_CHANNEL_NAME, can_send=can_send),
    ]


@pytest.fixture
async def temp_db(tmp_path):
    """Initialized Database backed by a temporary file."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    return db


@pytest.fixture
async def scan_env(temp_db):
    """
    A complete fake scan environment:
    one guild with all standard channels, a two-region catalog, and a fake
    RSI calculator (AAA.OL oversold at 25, BBB neutral at 50, CCC.TO overbought at 80).
    """
    from bot.services.scheduler import RSIScheduler

    channels = make_standard_channels()
    guild = FakeGuild(1111, channels=channels)
    bot = FakeBot(temp_db, [guild])

    scheduler = RSIScheduler(bot)
    scheduler.catalog = FakeCatalog({
        "AAA.OL": "Alpha ASA",       # europe
        "BBB": "Beta Inc",           # us (no suffix)
        "CCC.TO": "Gamma Corp",      # us_canada
    })
    scheduler.rsi_calculator = FakeRSICalculator({
        "AAA.OL": 25.0,
        "BBB": 50.0,
        "CCC.TO": 80.0,
    })

    yield SimpleNamespace(
        scheduler=scheduler, db=temp_db, bot=bot, guild=guild,
        oversold=guild.channel(OVERSOLD_CHANNEL_NAME),
        overbought=guild.channel(OVERBOUGHT_CHANNEL_NAME),
        changelog=guild.channel(CHANGELOG_CHANNEL_NAME),
    )

    scheduler.stop()
