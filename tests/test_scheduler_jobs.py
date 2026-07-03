"""
Scheduler lifecycle and job registration tests:
- jobs registered on start with next run times
- Europe/Oslo weekday :30 window behavior
- duplicate-start and duplicate-job prevention
- per-guild daily jobs from config, live rescheduling
- clean shutdown
"""
import asyncio
from datetime import datetime

import pytest
import pytz

from bot.services.scheduler import (
    RSIScheduler, classify_ticker_region, parse_schedule_time,
    EUROPE_JOB_ID, US_JOB_ID, MAINTENANCE_JOB_ID, DAILY_JOB_PREFIX,
)
from tests.conftest import FakeBot, FakeGuild

OSLO = pytz.timezone("Europe/Oslo")


def next_fire(job, year, month, day, hour, minute):
    """Next fire time of a job's trigger after the given Oslo-local time."""
    now = OSLO.localize(datetime(year, month, day, hour, minute))
    return job.trigger.get_next_fire_time(None, now)


@pytest.fixture
async def started_scheduler(temp_db):
    bot = FakeBot(temp_db, [FakeGuild(1)])
    scheduler = RSIScheduler(bot)
    await scheduler.start()
    yield scheduler
    scheduler.stop()


class TestJobRegistration:
    async def test_regional_jobs_registered_with_next_run(self, started_scheduler):
        jobs = {j.id: j for j in started_scheduler.scheduler.get_jobs()}
        assert EUROPE_JOB_ID in jobs
        assert US_JOB_ID in jobs
        assert MAINTENANCE_JOB_ID in jobs
        for job in jobs.values():
            assert job.next_run_time is not None

    async def test_duplicate_start_is_ignored(self, started_scheduler):
        before = {j.id for j in started_scheduler.scheduler.get_jobs()}
        await started_scheduler.start()  # second call must be a no-op
        after = {j.id for j in started_scheduler.scheduler.get_jobs()}
        assert before == after
        assert started_scheduler.scheduler.running

    async def test_stop_shuts_down(self, started_scheduler):
        started_scheduler.stop()
        # AsyncIOScheduler.shutdown() executes on the next loop iteration
        await asyncio.sleep(0.01)
        assert not started_scheduler.scheduler.running
        assert not started_scheduler.get_status()['running']


class TestScheduleWindows:
    """Europe 09:30-17:30 and US/Canada 15:30-22:30, Europe/Oslo, weekdays."""

    async def test_europe_first_run_of_day(self, started_scheduler):
        job = started_scheduler.scheduler.get_job(EUROPE_JOB_ID)
        # Wednesday 2026-07-01 08:00 -> 09:30 same day
        fire = next_fire(job, 2026, 7, 1, 8, 0)
        assert (fire.hour, fire.minute) == (9, 30)
        assert fire.date().isoformat() == "2026-07-01"

    async def test_europe_hourly_at_30(self, started_scheduler):
        job = started_scheduler.scheduler.get_job(EUROPE_JOB_ID)
        # 09:31 -> next at 10:30; 17:29 -> 17:30; 17:31 -> next day 09:30
        assert (next_fire(job, 2026, 7, 1, 9, 31).hour,
                next_fire(job, 2026, 7, 1, 9, 31).minute) == (10, 30)
        assert (next_fire(job, 2026, 7, 1, 17, 29).hour,
                next_fire(job, 2026, 7, 1, 17, 29).minute) == (17, 30)
        after_close = next_fire(job, 2026, 7, 1, 17, 31)
        assert (after_close.hour, after_close.minute) == (9, 30)
        assert after_close.date().isoformat() == "2026-07-02"

    async def test_europe_skips_weekend(self, started_scheduler):
        job = started_scheduler.scheduler.get_job(EUROPE_JOB_ID)
        # Friday 2026-07-03 18:00 -> Monday 2026-07-06 09:30
        fire = next_fire(job, 2026, 7, 3, 18, 0)
        assert fire.date().isoformat() == "2026-07-06"
        assert (fire.hour, fire.minute) == (9, 30)

    async def test_us_window(self, started_scheduler):
        job = started_scheduler.scheduler.get_job(US_JOB_ID)
        # Wednesday 14:00 -> 15:30; 22:31 -> next day 15:30
        fire = next_fire(job, 2026, 7, 1, 14, 0)
        assert (fire.hour, fire.minute) == (15, 30)
        late = next_fire(job, 2026, 7, 1, 22, 31)
        assert (late.hour, late.minute) == (15, 30)
        assert late.date().isoformat() == "2026-07-02"

    async def test_timezone_is_oslo(self, started_scheduler):
        job = started_scheduler.scheduler.get_job(EUROPE_JOB_ID)
        fire = next_fire(job, 2026, 7, 1, 8, 0)
        assert fire.utcoffset() == OSLO.localize(datetime(2026, 7, 1, 9, 30)).utcoffset()


class TestPerGuildDailyJobs:
    async def test_sync_creates_job_per_guild_config(self, started_scheduler, temp_db):
        await temp_db.get_or_create_guild_config(101)
        await temp_db.get_or_create_guild_config(202)
        await temp_db.update_guild_config(guild_id=202, default_schedule_time="07:45")

        await started_scheduler.sync_guild_daily_jobs()

        job_a = started_scheduler.scheduler.get_job(f"{DAILY_JOB_PREFIX}101")
        job_b = started_scheduler.scheduler.get_job(f"{DAILY_JOB_PREFIX}202")
        assert job_a is not None and job_b is not None
        assert (next_fire(job_a, 2026, 7, 1, 0, 0).hour,
                next_fire(job_a, 2026, 7, 1, 0, 0).minute) == (18, 30)  # default
        assert (next_fire(job_b, 2026, 7, 1, 0, 0).hour,
                next_fire(job_b, 2026, 7, 1, 0, 0).minute) == (7, 45)

    async def test_sync_is_idempotent(self, started_scheduler, temp_db):
        await temp_db.get_or_create_guild_config(101)
        await started_scheduler.sync_guild_daily_jobs()
        count_first = len(started_scheduler.scheduler.get_jobs())
        await started_scheduler.sync_guild_daily_jobs()  # e.g. on_ready re-fired
        assert len(started_scheduler.scheduler.get_jobs()) == count_first

    async def test_reschedule_takes_effect_without_restart(self, started_scheduler, temp_db):
        await temp_db.get_or_create_guild_config(101)
        await started_scheduler.sync_guild_daily_jobs()

        next_run = started_scheduler.reschedule_guild_daily(101, "06:15")
        assert next_run is not None

        job = started_scheduler.scheduler.get_job(f"{DAILY_JOB_PREFIX}101")
        fire = next_fire(job, 2026, 7, 1, 0, 0)
        assert (fire.hour, fire.minute) == (6, 15)

    async def test_remove_guild_daily_job(self, started_scheduler, temp_db):
        await temp_db.get_or_create_guild_config(101)
        await started_scheduler.sync_guild_daily_jobs()
        started_scheduler.remove_guild_daily_job(101)
        assert started_scheduler.scheduler.get_job(f"{DAILY_JOB_PREFIX}101") is None


class TestHelpers:
    def test_classify_ticker_region(self):
        assert classify_ticker_region("EQNR.OL") == 'europe'
        assert classify_ticker_region("cint.st") == 'europe'
        assert classify_ticker_region("AAPL") == 'us_canada'
        assert classify_ticker_region("SHOP.TO") == 'us_canada'
        assert classify_ticker_region("7203.T") == 'other'

    def test_parse_schedule_time(self):
        assert parse_schedule_time("18:30") == (18, 30)
        assert parse_schedule_time("7:05") == (7, 5)
        assert parse_schedule_time(None) == (18, 30)      # falls back to default
        assert parse_schedule_time("garbage") == (18, 30)
        assert parse_schedule_time("25:00") == (18, 30)
