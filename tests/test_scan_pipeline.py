"""
Tests for the unified scan pipeline (RSIScheduler._execute_scan):
- change detection (first scan posts, repeats don't, new entrants do)
- changelog status message on every scheduled scan
- schedule_enabled toggle and manual bypass
- guilds without a config row
- missing channels / missing permissions
- TradingView failures
- concurrent-scan prevention and RSI result reuse (cache)
- /run-now using the same pipeline
"""
import asyncio
from types import SimpleNamespace

import pytest

from bot.config import (
    OVERSOLD_CHANNEL_NAME, OVERBOUGHT_CHANNEL_NAME, CHANGELOG_CHANNEL_NAME,
)
from bot.services.scheduler import RSIScheduler
from tests.conftest import (
    FakeBot, FakeGuild, FakeChannel, FakeCatalog, FakeRSICalculator,
    make_standard_channels,
)


class TestChangeDetection:
    async def test_first_scan_posts_qualifying_tickers(self, scan_env):
        await scan_env.scheduler._execute_scan('europe')

        # AAA.OL (RSI 25 < 34) is oversold and new -> posted
        assert len(scan_env.oversold.sent) == 1
        assert "AAA.OL" in scan_env.oversold.sent[0]
        # No overbought europe tickers -> nothing posted there
        assert scan_env.overbought.sent == []

    async def test_repeat_scan_posts_nothing_new(self, scan_env):
        await scan_env.scheduler._execute_scan('europe')
        oversold_after_first = len(scan_env.oversold.sent)

        await scan_env.scheduler._execute_scan('europe')

        # Same data, no new entrants -> no additional alert-channel post
        assert len(scan_env.oversold.sent) == oversold_after_first
        # But the changelog got a status message for BOTH scans
        assert len(scan_env.changelog.sent) == 2

    async def test_new_entrant_is_posted_on_later_scan(self, scan_env):
        await scan_env.scheduler._execute_scan('europe')
        assert len(scan_env.oversold.sent) == 1

        # BBB is a US ticker; make a *europe* ticker newly oversold instead
        scan_env.scheduler.catalog = FakeCatalog({
            "AAA.OL": "Alpha ASA", "DDD.OL": "Delta ASA", "BBB": "Beta Inc", "CCC.TO": "Gamma",
        })
        scan_env.scheduler.rsi_calculator = FakeRSICalculator({
            "AAA.OL": 25.0, "DDD.OL": 20.0, "BBB": 50.0, "CCC.TO": 80.0,
        })
        scan_env.scheduler._rsi_cache.clear()

        await scan_env.scheduler._execute_scan('europe')

        assert len(scan_env.oversold.sent) == 2
        # Only the NEW entrant is listed, not the already-known AAA.OL
        assert "DDD.OL" in scan_env.oversold.sent[1]
        assert "AAA.OL" not in scan_env.oversold.sent[1]

    async def test_region_scan_does_not_wipe_other_regions_state(self, scan_env):
        """A US scan must not clear Europe tickers from today's state."""
        await scan_env.scheduler._execute_scan('europe')   # AAA.OL recorded
        scan_env.scheduler._rsi_cache.clear()
        await scan_env.scheduler._execute_scan('us_canada')  # CCC.TO overbought
        scan_env.scheduler._rsi_cache.clear()
        posts_before = len(scan_env.oversold.sent)

        # Another europe scan: AAA.OL is still known -> no duplicate post
        await scan_env.scheduler._execute_scan('europe')
        assert len(scan_env.oversold.sent) == posts_before


class TestChangelogStatus:
    async def test_changelog_posted_even_with_zero_hits(self, temp_db):
        channels = make_standard_channels()
        guild = FakeGuild(5, channels=channels)
        scheduler = RSIScheduler(FakeBot(temp_db, [guild]))
        scheduler.catalog = FakeCatalog({"NEU.OL": "Neutral ASA"})
        scheduler.rsi_calculator = FakeRSICalculator({"NEU.OL": 50.0})

        await scheduler._execute_scan('europe')

        changelog = guild.channel(CHANGELOG_CHANNEL_NAME)
        assert len(changelog.sent) == 1
        assert "Auto-Scan Complete" in changelog.sent[0]
        assert guild.channel(OVERSOLD_CHANNEL_NAME).sent == []
        assert guild.channel(OVERBOUGHT_CHANNEL_NAME).sent == []

    async def test_tradingview_failure_is_flagged_in_changelog(self, scan_env):
        scan_env.scheduler.rsi_calculator = FakeRSICalculator(fail_all=True)

        summary = await scan_env.scheduler._execute_scan('europe')

        assert summary['tickers_ok'] == 0
        assert summary['tickers_failed'] > 0
        assert len(scan_env.changelog.sent) == 1
        assert "TradingView data fetch FAILED" in scan_env.changelog.sent[0]
        # Failure is a scan outcome, not a crash
        assert summary['success'] is True


class TestScheduleToggle:
    async def test_disabled_guild_is_skipped(self, scan_env):
        await scan_env.db.update_guild_config(guild_id=scan_env.guild.id, schedule_enabled=False)

        summary = await scan_env.scheduler._execute_scan('europe')

        assert summary['guilds_skipped_disabled'] == 1
        assert scan_env.guild.id not in summary['guilds']
        assert scan_env.oversold.sent == []
        assert scan_env.changelog.sent == []

    async def test_manual_run_bypasses_disabled_schedule(self, scan_env):
        await scan_env.db.update_guild_config(guild_id=scan_env.guild.id, schedule_enabled=False)

        summary = await scan_env.scheduler.run_now(
            guild_id=scan_env.guild.id, triggered_by="tester"
        )

        assert summary['guilds_skipped_disabled'] == 0
        assert scan_env.guild.id in summary['guilds']
        # Manual runs post full lists + a changelog message
        assert any("AAA.OL" in m for m in scan_env.oversold.sent)
        assert any("Manual" in m for m in scan_env.changelog.sent)

    async def test_guild_without_config_row_still_scanned(self, temp_db):
        """A guild that never ran a slash command must still get auto-scans."""
        channels = make_standard_channels()
        guild = FakeGuild(777, channels=channels)  # no guild_config row exists
        scheduler = RSIScheduler(FakeBot(temp_db, [guild]))
        scheduler.catalog = FakeCatalog({"AAA.OL": "Alpha ASA"})
        scheduler.rsi_calculator = FakeRSICalculator({"AAA.OL": 25.0})

        summary = await scheduler._execute_scan('europe')

        assert guild.id in summary['guilds']
        assert len(guild.channel(CHANGELOG_CHANNEL_NAME).sent) == 1
        # Config was auto-created with defaults
        config = await temp_db.get_guild_config(guild.id)
        assert config is not None and config.schedule_enabled is True


class TestChannelProblems:
    async def test_missing_channels_do_not_crash(self, temp_db):
        guild = FakeGuild(9, channels=[])  # no channels at all
        scheduler = RSIScheduler(FakeBot(temp_db, [guild]))
        scheduler.catalog = FakeCatalog({"AAA.OL": "Alpha ASA"})
        scheduler.rsi_calculator = FakeRSICalculator({"AAA.OL": 25.0})

        summary = await scheduler._execute_scan('europe')

        assert summary['success'] is True
        issues = summary['guilds'][guild.id]['channel_issues']
        assert any(OVERSOLD_CHANNEL_NAME in i for i in issues)
        assert any(CHANGELOG_CHANNEL_NAME in i for i in issues)

    async def test_missing_send_permission_recorded(self, temp_db):
        channels = make_standard_channels(can_send=False)
        guild = FakeGuild(10, channels=channels)
        scheduler = RSIScheduler(FakeBot(temp_db, [guild]))
        scheduler.catalog = FakeCatalog({"AAA.OL": "Alpha ASA"})
        scheduler.rsi_calculator = FakeRSICalculator({"AAA.OL": 25.0})

        summary = await scheduler._execute_scan('europe')

        assert summary['success'] is True
        # Nothing was sent anywhere (no permission)
        for ch in channels:
            assert ch.sent == []
        issues = summary['guilds'][guild.id]['channel_issues']
        assert any("permission" in i.lower() for i in issues)


class TestRegionScoping:
    async def test_europe_scan_only_fetches_europe_tickers(self, scan_env):
        await scan_env.scheduler._execute_scan('europe')
        fetched = scan_env.scheduler.rsi_calculator.calls[0]
        assert "AAA.OL" in fetched
        assert "BBB" not in fetched and "CCC.TO" not in fetched

    async def test_us_scan_only_fetches_us_tickers(self, scan_env):
        await scan_env.scheduler._execute_scan('us_canada')
        fetched = scan_env.scheduler.rsi_calculator.calls[0]
        assert set(fetched) == {"BBB", "CCC.TO"}

    async def test_manual_scan_covers_all_regions(self, scan_env):
        await scan_env.scheduler.run_now(guild_id=scan_env.guild.id, triggered_by="t")
        fetched = scan_env.scheduler.rsi_calculator.calls[0]
        assert set(fetched) == {"AAA.OL", "BBB", "CCC.TO"}


class TestConcurrencyAndCache:
    async def test_scans_never_run_concurrently(self, scan_env):
        state = {'active': 0, 'max_active': 0}
        original = scan_env.scheduler._fetch_rsi

        async def slow_fetch(tickers):
            state['active'] += 1
            state['max_active'] = max(state['max_active'], state['active'])
            await asyncio.sleep(0.05)
            result = await original(tickers)
            state['active'] -= 1
            return result

        scan_env.scheduler._fetch_rsi = slow_fetch

        await asyncio.gather(
            scan_env.scheduler._execute_scan('europe'),
            scan_env.scheduler._execute_scan('us_canada'),
        )

        assert state['max_active'] == 1

    async def test_rsi_cache_prevents_duplicate_fetches(self, scan_env):
        await scan_env.scheduler._execute_scan('europe')
        await scan_env.scheduler._execute_scan('europe')

        # Second scan (within TTL) must not hit the calculator again
        assert len(scan_env.scheduler.rsi_calculator.calls) == 1

    async def test_daily_check_reuses_regional_scan_data(self, scan_env):
        # Subscription on the europe ticker
        await scan_env.db.create_subscription(
            guild_id=scan_env.guild.id, ticker="AAA.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )
        await scan_env.scheduler._execute_scan('europe')
        calls_after_scan = len(scan_env.scheduler.rsi_calculator.calls)

        await scan_env.scheduler._run_daily_check(scan_env.guild.id)

        # Daily check served entirely from cache
        assert len(scan_env.scheduler.rsi_calculator.calls) == calls_after_scan


class TestRunNowPipeline:
    async def test_run_now_uses_shared_pipeline(self, scan_env):
        """run_now must delegate to _execute_scan (single source of truth)."""
        recorded = {}
        original = scan_env.scheduler._execute_scan

        async def spy(region, **kwargs):
            recorded['region'] = region
            recorded.update(kwargs)
            return await original(region, **kwargs)

        scan_env.scheduler._execute_scan = spy
        await scan_env.scheduler.run_now(guild_id=scan_env.guild.id, triggered_by="alice")

        assert recorded['region'] == 'all'
        assert recorded['manual'] is True
        assert recorded['only_guild_id'] == scan_env.guild.id
        assert recorded['triggered_by'] == "alice"

    async def test_run_now_summary_and_status_tracking(self, scan_env):
        summary = await scan_env.scheduler.run_now(
            guild_id=scan_env.guild.id, triggered_by="alice"
        )

        assert summary['success'] is True
        assert summary['tickers_ok'] == 3
        guild_result = summary['guilds'][scan_env.guild.id]
        assert guild_result['oversold_total'] == 1     # AAA.OL
        assert guild_result['overbought_total'] == 1   # CCC.TO
        assert guild_result['changelog_posted'] is True

        # get_status exposes the completed scan for /scheduler-status
        status = scan_env.scheduler.get_status()
        assert status['last_scan']['type'] == 'manual'
        assert status['last_error'] is None


class TestSubscriptionAlerts:
    async def test_subscription_alert_included_in_scan_post(self, scan_env):
        await scan_env.db.create_subscription(
            guild_id=scan_env.guild.id, ticker="AAA.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )

        await scan_env.scheduler._execute_scan('europe')

        combined = "\n".join(scan_env.oversold.sent)
        assert "Subscription Alerts" in combined
        assert "AAA.OL" in combined

    async def test_daily_check_posts_alerts_and_changelog(self, scan_env):
        await scan_env.db.create_subscription(
            guild_id=scan_env.guild.id, ticker="AAA.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )

        await scan_env.scheduler._run_daily_check(scan_env.guild.id)

        assert any("AAA.OL" in m for m in scan_env.oversold.sent)
        assert any("Daily Subscription Check" in m for m in scan_env.changelog.sent)

    async def test_daily_check_respects_schedule_disabled(self, scan_env):
        await scan_env.db.create_subscription(
            guild_id=scan_env.guild.id, ticker="AAA.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )
        await scan_env.db.update_guild_config(guild_id=scan_env.guild.id, schedule_enabled=False)

        await scan_env.scheduler._run_daily_check(scan_env.guild.id)

        assert scan_env.oversold.sent == []
        assert scan_env.changelog.sent == []
