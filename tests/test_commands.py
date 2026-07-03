"""
Slash-command permission and validation tests.

Command callbacks are exercised directly with fake interactions - no Discord
connection or token involved. The module-level bot instance in bot.main gets
its db/scheduler swapped per test.
"""
from datetime import datetime
from types import SimpleNamespace

import discord
from discord import app_commands
import pytest
import pytz

import bot.main as main_mod
from bot.repositories.database import Database
from tests.conftest import FakeGuild, FakeUser, FakeInteraction, make_standard_channels

MANAGE_GUILD = discord.Permissions(manage_guild=True)
ADMIN = discord.Permissions(administrator=True)
NO_PERMS = discord.Permissions.none()

UNDER = app_commands.Choice(name="under (oversold)", value="UNDER")
OVER = app_commands.Choice(name="over (overbought)", value="OVER")


@pytest.fixture
async def cmd_env(tmp_path):
    """Wire a temp DB into the module-level bot and build a standard guild."""
    db = Database(str(tmp_path / "cmd.db"))
    await db.initialize()

    old_db, old_scheduler = main_mod.bot.db, main_mod.bot.scheduler
    main_mod.bot.db = db
    main_mod.bot.scheduler = None

    guild = FakeGuild(4242, channels=make_standard_channels())
    yield SimpleNamespace(db=db, guild=guild)

    main_mod.bot.db = old_db
    main_mod.bot.scheduler = old_scheduler


def interaction(env, perms=NO_PERMS, user_id=42):
    return FakeInteraction(env.guild, FakeUser(user_id=user_id, permissions=perms))


class TestGuildOnly:
    def test_all_commands_are_guild_only(self):
        for name in ("subscribe", "subscribe-bands", "unsubscribe", "unsubscribe-all",
                     "admin-unsubscribe", "remove-ticker", "list", "run-now",
                     "set-defaults", "ticker-info", "catalog-stats",
                     "reload-catalog", "scheduler-status"):
            cmd = main_mod.bot.tree.get_command(name)
            assert cmd is not None, f"command {name} missing"
            assert cmd.guild_only, f"command {name} must be guild-only"


class TestSubscribeValidation:
    async def test_unknown_ticker_rejected(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe.callback(itx, ticker="NOPE.XX", condition=UNDER, threshold=30)
        assert "not in the instrument catalog" in itx.messages[0]

    async def test_threshold_out_of_range(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe.callback(itx, ticker="EQNR.OL", condition=UNDER, threshold=150)
        assert "between 0 and 100" in itx.messages[0]

    async def test_impossible_under_threshold_zero(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe.callback(itx, ticker="EQNR.OL", condition=UNDER, threshold=0)
        assert "can never trigger" in itx.messages[0]

    async def test_impossible_over_threshold_hundred(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe.callback(itx, ticker="EQNR.OL", condition=OVER, threshold=100)
        assert "can never trigger" in itx.messages[0]

    async def test_unsupported_period_rejected(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe.callback(itx, ticker="EQNR.OL", condition=UNDER,
                                          threshold=30, period=21)
        assert "RSI14" in itx.messages[0]

    async def test_negative_cooldown_rejected(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe.callback(itx, ticker="EQNR.OL", condition=UNDER,
                                          threshold=30, cooldown=-5)
        assert "non-negative" in itx.messages[0]

    async def test_success_and_duplicate_detection(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe.callback(itx, ticker="eqnr.ol", condition=UNDER, threshold=30)
        assert "Subscription created" in itx.messages[0]
        assert "EQNR.OL" in itx.messages[0]  # case-insensitive handling

        dup = interaction(cmd_env)
        await main_mod.subscribe.callback(dup, ticker="EQNR.OL", condition=UNDER, threshold=30)
        assert "already exists" in dup.messages[0]


class TestSubscribeBandsValidation:
    async def test_inverted_bands_rejected(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe_bands.callback(itx, ticker="EQNR.OL",
                                                oversold=70, overbought=30)
        assert "must be less than" in itx.messages[0]

    async def test_impossible_bands_rejected(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe_bands.callback(itx, ticker="EQNR.OL",
                                                oversold=0, overbought=70)
        assert "can never trigger" in itx.messages[0]

    async def test_creates_both_subscriptions(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.subscribe_bands.callback(itx, ticker="EQNR.OL")
        assert "Created" in itx.messages[0]
        subs = await cmd_env.db.get_subscriptions_by_guild(guild_id=cmd_env.guild.id)
        assert {s.condition for s in subs} == {"UNDER", "OVER"}


class TestUnsubscribeOwnership:
    async def test_cannot_remove_someone_elses_subscription(self, cmd_env):
        sub = await cmd_env.db.create_subscription(
            guild_id=cmd_env.guild.id, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )
        itx = interaction(cmd_env, user_id=2)
        await main_mod.unsubscribe.callback(itx, id=sub.id)
        assert "Permission Denied" in itx.messages[0]
        assert await cmd_env.db.get_subscription(sub.id) is not None

    async def test_owner_can_remove(self, cmd_env):
        sub = await cmd_env.db.create_subscription(
            guild_id=cmd_env.guild.id, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=7,
        )
        itx = interaction(cmd_env, user_id=7)
        await main_mod.unsubscribe.callback(itx, id=sub.id)
        assert "Subscription removed" in itx.messages[0]
        assert await cmd_env.db.get_subscription(sub.id) is None

    async def test_wrong_guild_rejected(self, cmd_env):
        sub = await cmd_env.db.create_subscription(
            guild_id=999999, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=7,
        )
        itx = interaction(cmd_env, user_id=7)
        await main_mod.unsubscribe.callback(itx, id=sub.id)
        assert "does not belong to this server" in itx.messages[0]


class TestAdminCommands:
    async def test_admin_unsubscribe_requires_admin(self, cmd_env):
        sub = await cmd_env.db.create_subscription(
            guild_id=cmd_env.guild.id, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )
        itx = interaction(cmd_env, perms=MANAGE_GUILD)  # manage_guild is NOT enough
        await main_mod.admin_unsubscribe.callback(itx, id=sub.id)
        assert "Permission Denied" in itx.messages[0]
        assert await cmd_env.db.get_subscription(sub.id) is not None

    async def test_admin_unsubscribe_works_for_admin(self, cmd_env):
        sub = await cmd_env.db.create_subscription(
            guild_id=cmd_env.guild.id, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )
        itx = interaction(cmd_env, perms=ADMIN)
        await main_mod.admin_unsubscribe.callback(itx, id=sub.id, reason="cleanup")
        assert "removed by admin" in itx.messages[0]
        assert await cmd_env.db.get_subscription(sub.id) is None

    async def test_reload_catalog_requires_admin(self, cmd_env):
        itx = interaction(cmd_env, perms=NO_PERMS)
        await main_mod.reload_catalog.callback(itx)
        assert "Permission Denied" in itx.messages[0]


class TestRunNowCommand:
    async def test_requires_manage_guild(self, cmd_env):
        itx = interaction(cmd_env, perms=NO_PERMS)
        await main_mod.run_now.callback(itx)
        assert "Permission Denied" in itx.messages[0]

    async def test_uses_scheduler_pipeline(self, cmd_env):
        calls = []

        class FakeSched:
            async def run_now(self, guild_id, triggered_by=None):
                calls.append((guild_id, triggered_by))
                return {
                    'success': True, 'duration_seconds': 1.2,
                    'tickers_total': 3, 'tickers_ok': 3, 'tickers_failed': 0,
                    'failed_tickers': [], 'persisted': 3,
                    'guilds': {cmd_env.guild.id: {
                        'oversold_total': 1, 'overbought_total': 0,
                        'sub_alerts_under': 0, 'sub_alerts_over': 0,
                        'messages_sent': 2, 'channel_issues': [],
                    }},
                    'guilds_skipped_disabled': 0,
                }

        main_mod.bot.scheduler = FakeSched()
        itx = interaction(cmd_env, perms=MANAGE_GUILD)
        await main_mod.run_now.callback(itx)

        assert calls == [(cmd_env.guild.id, "tester")]
        assert any("Manual RSI Scan Complete" in m for m in itx.edited)

    async def test_reports_missing_channels(self, cmd_env):
        cmd_env.guild.text_channels = []  # no channels at all
        itx = interaction(cmd_env, perms=MANAGE_GUILD)
        main_mod.bot.scheduler = object()  # must not be reached
        await main_mod.run_now.callback(itx)
        assert "Channel/Permission Issues" in itx.messages[0]


class TestSetDefaultsCommand:
    async def test_requires_manage_guild(self, cmd_env):
        itx = interaction(cmd_env, perms=NO_PERMS)
        await main_mod.set_defaults.callback(itx, default_cooldown=12)
        assert "Permission Denied" in itx.messages[0]

    async def test_invalid_schedule_times_rejected(self, cmd_env):
        for bad in ("25:00", "18:3", "junk", "18:30:00", "-1:15"):
            itx = interaction(cmd_env, perms=MANAGE_GUILD)
            await main_mod.set_defaults.callback(itx, schedule_time=bad)
            assert "HH:MM" in itx.messages[0], f"{bad!r} should be rejected"

    async def test_effective_threshold_cross_validation(self, cmd_env):
        # Existing overbought default is 70; oversold=80 would be impossible
        itx = interaction(cmd_env, perms=MANAGE_GUILD)
        await main_mod.set_defaults.callback(itx, auto_oversold=80)
        assert "must be less than" in itx.messages[0]

    async def test_schedule_time_change_reschedules_daily_job(self, cmd_env):
        reschedules = []

        class FakeSched:
            def reschedule_guild_daily(self, guild_id, schedule_time):
                reschedules.append((guild_id, schedule_time))
                return pytz.timezone("Europe/Oslo").localize(datetime(2026, 7, 6, 7, 45))

        main_mod.bot.scheduler = FakeSched()
        itx = interaction(cmd_env, perms=MANAGE_GUILD)
        await main_mod.set_defaults.callback(itx, schedule_time="07:45")

        assert reschedules == [(cmd_env.guild.id, "07:45")]
        assert "Daily check rescheduled" in itx.messages[0]

        config = await cmd_env.db.get_guild_config(cmd_env.guild.id)
        assert config.default_schedule_time == "07:45"

    async def test_schedule_toggle_persists_and_logs(self, cmd_env):
        toggle = app_commands.Choice(name="Disabled", value="false")
        itx = interaction(cmd_env, perms=MANAGE_GUILD)
        await main_mod.set_defaults.callback(itx, schedule_enabled=toggle)

        config = await cmd_env.db.get_guild_config(cmd_env.guild.id)
        assert config.schedule_enabled is False
        changelog = cmd_env.guild.channel("server-changelog")
        assert any("Schedule Settings Changed" in m for m in changelog.sent)


class TestSchedulerStatusCommand:
    async def test_requires_manage_guild(self, cmd_env):
        itx = interaction(cmd_env, perms=NO_PERMS)
        await main_mod.scheduler_status.callback(itx)
        assert "Permission Denied" in itx.messages[0]

    async def test_warns_when_scheduler_missing(self, cmd_env):
        main_mod.bot.scheduler = None
        itx = interaction(cmd_env, perms=MANAGE_GUILD)
        await main_mod.scheduler_status.callback(itx)
        assert "NOT initialized" in itx.messages[0]

    async def test_renders_full_status(self, cmd_env):
        oslo = pytz.timezone("Europe/Oslo")

        class FakeSched:
            def get_status(self):
                return {
                    'running': True, 'timezone': 'Europe/Oslo',
                    'scan_in_progress': False,
                    'jobs': [{'id': 'europe_autoscan', 'name': 'Europe',
                              'next_run_time': oslo.localize(datetime(2026, 7, 6, 9, 30))}],
                    'last_scan': {
                        'type': 'scheduled', 'region': 'europe', 'success': True,
                        'finished': oslo.localize(datetime(2026, 7, 3, 17, 30)),
                        'tickers_ok': 80, 'tickers_failed': 2,
                        'duration_seconds': 42.0, 'guilds': {4242: {}},
                        'guilds_skipped_disabled': 0, 'error': None,
                    },
                    'last_error': None,
                }

        main_mod.bot.scheduler = FakeSched()
        itx = interaction(cmd_env, perms=MANAGE_GUILD)
        await main_mod.scheduler_status.callback(itx)

        text = itx.messages[0]
        assert "Scheduler Status" in text
        assert "europe_autoscan" in text
        assert "Latest Scan" in text and "80 OK" in text
        assert "None since startup" in text
        # Channel health for the standard channels
        assert "#rsi-oversold: ✅ OK" in text


class TestRemoveTickerPolicy:
    async def test_remove_ticker_requires_admin(self, cmd_env):
        itx = interaction(cmd_env, perms=NO_PERMS)
        await main_mod.remove_ticker_cmd.callback(itx, ticker="EQNR.OL")
        assert "Permission Denied" in itx.messages[0]

    async def test_removal_disables_subscriptions(self, cmd_env, tmp_path, monkeypatch):
        """Removing a ticker must not leave silently broken subscriptions."""
        sub = await cmd_env.db.create_subscription(
            guild_id=cmd_env.guild.id, ticker="FAKE.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )

        from bot.repositories.ticker_catalog import Instrument

        async def fake_remove(ticker):
            return True, "removed", Instrument("FAKE.OL", "Fake ASA", "OSL:FAKE")

        monkeypatch.setattr(main_mod, "remove_ticker", fake_remove)

        itx = interaction(cmd_env, perms=ADMIN)
        await main_mod.remove_ticker_cmd.callback(itx, ticker="FAKE.OL")

        assert "Subscriptions disabled" in itx.messages[0]
        refreshed = await cmd_env.db.get_subscription(sub.id)
        assert refreshed.enabled is False


class TestTickerInfoAndStats:
    async def test_ticker_info_unknown_ticker(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.ticker_info.callback(itx, ticker="ZZZZ.QQ")
        assert "not found in catalog" in itx.messages[0]

    async def test_ticker_info_known_without_rsi(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.ticker_info.callback(itx, ticker="EQNR.OL")
        assert "EQNR.OL" in itx.messages[0]
        assert "Not yet available" in itx.messages[0]

    async def test_catalog_stats_renders(self, cmd_env):
        itx = interaction(cmd_env)
        await main_mod.catalog_stats.callback(itx)
        assert "Bot Statistics" in itx.messages[0]
        assert "scheduler-status" in itx.messages[0]
