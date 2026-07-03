"""
AlertEngine guild-scoping tests.

Regression tests for the multi-guild bug where evaluating one guild's
subscriptions consumed the crossing/cooldown state of every other guild's
subscriptions for the same ticker (their alerts were then silently dropped).
"""
from datetime import datetime

from bot.cogs.alert_engine import AlertEngine
from bot.services.market_data.rsi_calculator import RSIResult


def make_result(ticker: str, rsi: float) -> RSIResult:
    return RSIResult(
        ticker=ticker, rsi_values={14: rsi}, last_date="2026-07-03",
        last_close=100.0, success=True, data_timestamp=datetime.utcnow(),
    )


class TestGuildScoping:
    async def test_each_guild_gets_its_own_alert(self, temp_db):
        engine = AlertEngine(temp_db)
        for guild_id in (1, 2):
            await temp_db.get_or_create_guild_config(guild_id)
            await temp_db.create_subscription(
                guild_id=guild_id, ticker="EQNR.OL", condition="UNDER",
                threshold=30, period=14, cooldown_hours=24, created_by_user_id=guild_id,
            )

        results = {"EQNR.OL": make_result("EQNR.OL", 22.0)}

        # Evaluate guild 1 first, then guild 2 - guild 2's crossing state must
        # not have been consumed by guild 1's evaluation.
        alerts_g1 = await engine.evaluate_subscriptions(results, guild_id=1)
        alerts_g2 = await engine.evaluate_subscriptions(results, guild_id=2)

        assert len(alerts_g1['UNDER']) == 1
        assert alerts_g1['UNDER'][0].guild_id == 1
        assert len(alerts_g2['UNDER']) == 1
        assert alerts_g2['UNDER'][0].guild_id == 2

    async def test_scoped_evaluation_only_touches_own_state(self, temp_db):
        engine = AlertEngine(temp_db)
        for guild_id in (1, 2):
            await temp_db.get_or_create_guild_config(guild_id)
        sub1 = await temp_db.create_subscription(
            guild_id=1, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )
        sub2 = await temp_db.create_subscription(
            guild_id=2, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=2,
        )

        await engine.evaluate_subscriptions({"EQNR.OL": make_result("EQNR.OL", 22.0)}, guild_id=1)

        state1 = await temp_db.get_subscription_state(sub1.id)
        state2 = await temp_db.get_subscription_state(sub2.id)
        assert state1.last_rsi == 22.0          # updated
        assert state2.last_rsi is None          # untouched
        assert state2.last_status == "UNKNOWN"  # crossing still pending

    async def test_no_repeat_alert_same_guild_crossing_mode(self, temp_db):
        engine = AlertEngine(temp_db)
        await temp_db.get_or_create_guild_config(1)
        await temp_db.create_subscription(
            guild_id=1, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=24, created_by_user_id=1,
        )
        results = {"EQNR.OL": make_result("EQNR.OL", 22.0)}

        first = await engine.evaluate_subscriptions(results, guild_id=1)
        second = await engine.evaluate_subscriptions(results, guild_id=1)

        assert len(first['UNDER']) == 1   # crossing detected
        assert len(second['UNDER']) == 0  # still below, no re-alert

    async def test_days_in_zone_increments_on_new_day(self, temp_db):
        engine = AlertEngine(temp_db)
        await temp_db.get_or_create_guild_config(1)
        sub = await temp_db.create_subscription(
            guild_id=1, ticker="EQNR.OL", condition="UNDER",
            threshold=30, period=14, cooldown_hours=0, created_by_user_id=1,
        )

        day1 = {"EQNR.OL": make_result("EQNR.OL", 22.0)}
        day1["EQNR.OL"].last_date = "2026-07-02"
        await engine.evaluate_subscriptions(day1, guild_id=1)

        day2 = {"EQNR.OL": make_result("EQNR.OL", 21.0)}
        day2["EQNR.OL"].last_date = "2026-07-03"
        await engine.evaluate_subscriptions(day2, guild_id=1)

        state = await temp_db.get_subscription_state(sub.id)
        assert state.days_in_zone == 2
