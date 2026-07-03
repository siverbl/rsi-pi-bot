"""
Scheduler module for RSI Discord Bot.

Single source of truth for scan execution. Both the APScheduler cron jobs and
the manual /run-now slash command go through ``RSIScheduler._execute_scan``.

Design notes for 24/7 Raspberry Pi operation:

- One cron job per market region (Europe 09:30-17:30, US/Canada 15:30-22:30,
  Europe/Oslo, weekdays, at :30) instead of one job per hour.
- One daily subscription-check job per guild, using that guild's configured
  ``schedule_time``. Rescheduled live when /set-defaults changes the time.
- All scan work is serialized through an asyncio.Lock so overlapping jobs
  (e.g. the 18:30 US scan and an 18:30 daily check) never run TradingView
  fetches or SQLite writes concurrently.
- RSI results are cached for RSI_CACHE_TTL_SECONDS so back-to-back jobs reuse
  data instead of re-querying TradingView.
- Jobs wait for the Discord gateway (`bot.wait_until_ready()`) before touching
  guilds, so a scan firing right after a restart cannot see an empty cache.
- Guilds are taken from ``bot.guilds`` (with configs auto-created), never only
  from rows that happen to exist in the guild_config table.
- Change-detection state is merged per region: a Europe scan only rewrites the
  state of tickers it actually scanned, so it cannot wipe the US tickers'
  state recorded by the overlapping US job (and vice versa).
- Scan outcomes and job errors are recorded on the scheduler instance and
  exposed through ``get_status()`` for the /scheduler-status command.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any

import discord
import pytz
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from bot.config import (
    DEFAULT_TIMEZONE, DEFAULT_SCHEDULE_TIME,
    OVERSOLD_CHANNEL_NAME, OVERBOUGHT_CHANNEL_NAME, CHANGELOG_CHANNEL_NAME,
    EUROPEAN_SUFFIXES, US_CANADA_SUFFIXES,
    EUROPE_MARKET_START_HOUR, EUROPE_MARKET_END_HOUR,
    US_MARKET_START_HOUR, US_MARKET_END_HOUR,
    DISCORD_SAFE_LIMIT, RSI_CACHE_TTL_SECONDS
)
from bot.repositories.database import Database
from bot.services.market_data.rsi_calculator import RSICalculator, RSIResult
from bot.cogs.alert_engine import AlertEngine, Alert, format_alert_list
from bot.repositories.ticker_catalog import get_catalog
from bot.utils.message_utils import chunk_message

logger = logging.getLogger(__name__)

EUROPE_JOB_ID = "europe_autoscan"
US_JOB_ID = "us_autoscan"
MAINTENANCE_JOB_ID = "db_maintenance"
DAILY_JOB_PREFIX = "daily_check_"


def get_alert_channels(guild: discord.Guild) -> Tuple[Optional[discord.TextChannel], Optional[discord.TextChannel]]:
    """Get the fixed alert channels for a guild."""
    oversold_channel = discord.utils.get(guild.text_channels, name=OVERSOLD_CHANNEL_NAME)
    overbought_channel = discord.utils.get(guild.text_channels, name=OVERBOUGHT_CHANNEL_NAME)
    return oversold_channel, overbought_channel


def get_changelog_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    """Get the changelog channel for a guild."""
    return discord.utils.get(guild.text_channels, name=CHANGELOG_CHANNEL_NAME)


def can_send_to_channel(channel: Optional[discord.TextChannel], bot_member) -> bool:
    """Check if the bot can send messages to a channel."""
    if not channel:
        return False
    perms = channel.permissions_for(bot_member)
    return perms.send_messages


def classify_ticker_region(ticker: str) -> str:
    """
    Classify a ticker as 'europe', 'us_canada', or 'other'.

    Args:
        ticker: Yahoo Finance ticker symbol

    Returns:
        'europe', 'us_canada', or 'other'
    """
    ticker_upper = ticker.upper()

    for suffix in EUROPEAN_SUFFIXES:
        if ticker_upper.endswith(suffix):
            return 'europe'

    for suffix in US_CANADA_SUFFIXES:
        if ticker_upper.endswith(suffix):
            return 'us_canada'

    # No suffix = US stock
    if '.' not in ticker_upper:
        return 'us_canada'

    return 'other'


def determine_rsi_state(rsi_value: float, oversold_threshold: float, overbought_threshold: float) -> str:
    """
    Determine RSI state based on thresholds.

    Returns:
        'OVERSOLD', 'OVERBOUGHT', or 'NEUTRAL'
    """
    if rsi_value < oversold_threshold:
        return 'OVERSOLD'
    elif rsi_value > overbought_threshold:
        return 'OVERBOUGHT'
    else:
        return 'NEUTRAL'


def parse_schedule_time(value: Optional[str]) -> Tuple[int, int]:
    """Parse an HH:MM string, falling back to DEFAULT_SCHEDULE_TIME."""
    for candidate in (value, DEFAULT_SCHEDULE_TIME):
        if not candidate:
            continue
        parts = str(candidate).split(":")
        if len(parts) != 2:
            continue
        try:
            hour, minute = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    return 18, 30


class RSIScheduler:
    """
    Owns all scheduled work and the shared scan pipeline:

    - Regional auto-scans (catalog + subscriptions, change detection).
    - Per-guild daily subscription checks at each guild's schedule_time.
    - Manual /run-now runs (same pipeline, schedule restrictions bypassed).
    """

    # Regions included in each scan type. 'other' (unclassifiable suffixes)
    # rides along with the Europe window so those tickers still get scanned
    # daily instead of never.
    REGION_SETS = {
        'europe': {'europe', 'other'},
        'us_canada': {'us_canada'},
        'all': {'europe', 'us_canada', 'other'},
    }

    def __init__(self, bot):
        self.bot = bot
        self.db: Database = bot.db
        self.rsi_calculator = RSICalculator()
        self.alert_engine = AlertEngine(self.db)
        self.timezone = pytz.timezone(DEFAULT_TIMEZONE)
        self.catalog = get_catalog()

        # The executor must be created while the event loop is running, so the
        # scheduler is only ever constructed/started from setup_hook().
        self.scheduler = AsyncIOScheduler(timezone=self.timezone)

        self._started = False
        # Serializes every scan (regional, daily, manual) so a Raspberry Pi
        # never runs two TradingView fetch pipelines at once.
        self._scan_lock = asyncio.Lock()

        # Short-lived RSI result cache: ticker -> (RSIResult, fetched_at UTC).
        self._rsi_cache: Dict[str, Tuple[RSIResult, datetime]] = {}

        # Observability (exposed via get_status / the /scheduler-status command)
        self.last_scan: Optional[Dict[str, Any]] = None
        self.last_error: Optional[Dict[str, Any]] = None

    # ==================== Lifecycle ====================

    async def start(self):
        """Start the scheduler and register jobs. Safe to call only once."""
        if self._started:
            logger.warning("Scheduler start() called twice - ignoring duplicate start")
            return

        logger.info("=" * 60)
        logger.info("Starting RSI scheduler (timezone: %s)", DEFAULT_TIMEZONE)
        logger.info("=" * 60)

        self._add_regional_autoscan_jobs()
        self._add_maintenance_job()
        self.scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)
        self.scheduler.add_listener(self._on_job_missed, EVENT_JOB_MISSED)

        self.scheduler.start()
        self._started = True

        self.log_jobs()
        self._warn_unclassified_tickers()

    def stop(self):
        """Stop the scheduler cleanly."""
        if self._started and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("RSI scheduler stopped")
        self._started = False

    def log_jobs(self):
        """Log every registered job and its next run time."""
        jobs = self.scheduler.get_jobs()
        logger.info("Scheduler has %d registered jobs:", len(jobs))
        for job in jobs:
            logger.info("  - %s: next run at %s", job.id, job.next_run_time)

    def _warn_unclassified_tickers(self):
        """Log catalog tickers that fall outside the Europe/US-Canada windows."""
        other = [t for t in self.catalog.get_all_tickers() if classify_ticker_region(t) == 'other']
        if other:
            logger.warning(
                "%d catalog tickers have unrecognized region suffixes and will be "
                "scanned during the Europe window: %s",
                len(other), ", ".join(sorted(other))
            )

    def _on_job_error(self, event):
        """APScheduler listener: record and log job exceptions."""
        logger.error(
            "Scheduled job %r raised an exception: %s",
            event.job_id, event.exception,
            exc_info=(type(event.exception), event.exception, event.exception.__traceback__)
        )
        self.last_error = {
            'time': datetime.now(self.timezone),
            'source': f"job:{event.job_id}",
            'error': f"{type(event.exception).__name__}: {event.exception}",
        }

    def _on_job_missed(self, event):
        """APScheduler listener: log missed runs (e.g. Pi clock jump / overload)."""
        logger.warning("Scheduled job %r missed its run time %s", event.job_id, event.scheduled_run_time)

    # ==================== Job registration ====================

    def _add_regional_autoscan_jobs(self):
        """Register one cron job per market region (weekdays, at :30)."""
        self.scheduler.add_job(
            self._run_europe_autoscan,
            trigger=CronTrigger(
                hour=f"{EUROPE_MARKET_START_HOUR}-{EUROPE_MARKET_END_HOUR}",
                minute=30,
                day_of_week='mon-fri',
                timezone=self.timezone,
            ),
            id=EUROPE_JOB_ID,
            name=f"Europe Auto-Scan ({EUROPE_MARKET_START_HOUR}:30-{EUROPE_MARKET_END_HOUR}:30)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._run_us_autoscan,
            trigger=CronTrigger(
                hour=f"{US_MARKET_START_HOUR}-{US_MARKET_END_HOUR}",
                minute=30,
                day_of_week='mon-fri',
                timezone=self.timezone,
            ),
            id=US_JOB_ID,
            name=f"US/Canada Auto-Scan ({US_MARKET_START_HOUR}:30-{US_MARKET_END_HOUR}:30)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
            replace_existing=True,
        )
        logger.info(
            "Registered auto-scan jobs: Europe %d:30-%d:30, US/Canada %d:30-%d:30 (%s, weekdays)",
            EUROPE_MARKET_START_HOUR, EUROPE_MARKET_END_HOUR,
            US_MARKET_START_HOUR, US_MARKET_END_HOUR, DEFAULT_TIMEZONE
        )

    def _add_maintenance_job(self):
        """Nightly DB housekeeping (old auto-scan state, stale RSI rows)."""
        self.scheduler.add_job(
            self._run_maintenance,
            trigger=CronTrigger(hour=3, minute=17, timezone=self.timezone),
            id=MAINTENANCE_JOB_ID,
            name="Database maintenance",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
            replace_existing=True,
        )

    async def sync_guild_daily_jobs(self):
        """
        Ensure every guild with a config has a daily subscription-check job at
        its configured schedule_time. Idempotent - safe to call from on_ready
        (including reconnects) and on_guild_join.
        """
        configs = await self.db.get_all_guild_configs()
        for config in configs:
            self.schedule_guild_daily_job(config.guild_id, config.default_schedule_time)
        logger.info("Synced daily subscription jobs for %d guilds", len(configs))

    def schedule_guild_daily_job(self, guild_id: int, schedule_time: Optional[str]):
        """Register (or replace) the daily subscription-check job for a guild."""
        hour, minute = parse_schedule_time(schedule_time)
        job = self.scheduler.add_job(
            self._run_daily_check,
            trigger=CronTrigger(
                hour=hour, minute=minute,
                day_of_week='mon-fri',
                timezone=self.timezone,
            ),
            id=f"{DAILY_JOB_PREFIX}{guild_id}",
            name=f"Daily subscription check (guild {guild_id}, {hour:02d}:{minute:02d})",
            args=[guild_id],
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
            replace_existing=True,
        )
        logger.info(
            "Daily subscription check for guild %s scheduled at %02d:%02d %s (next run: %s)",
            guild_id, hour, minute, DEFAULT_TIMEZONE, job.next_run_time
        )
        return job

    def reschedule_guild_daily(self, guild_id: int, schedule_time: str) -> Optional[datetime]:
        """
        Apply a changed schedule_time immediately (no restart needed).

        Returns:
            The job's next run time, or None if the scheduler isn't running.
        """
        job = self.schedule_guild_daily_job(guild_id, schedule_time)
        return getattr(job, 'next_run_time', None)

    def remove_guild_daily_job(self, guild_id: int):
        """Remove a guild's daily job (e.g. when the bot leaves the guild)."""
        job_id = f"{DAILY_JOB_PREFIX}{guild_id}"
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)
            logger.info("Removed daily subscription job for guild %s", guild_id)

    # ==================== Status / observability ====================

    def get_status(self) -> Dict[str, Any]:
        """Snapshot of scheduler health for /scheduler-status."""
        jobs = []
        for job in self.scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time,
            })
        return {
            'running': self._started and self.scheduler.running,
            'timezone': DEFAULT_TIMEZONE,
            'jobs': jobs,
            'last_scan': self.last_scan,
            'last_error': self.last_error,
            'scan_in_progress': self._scan_lock.locked(),
        }

    # ==================== Job entry points ====================

    async def _run_europe_autoscan(self):
        """Cron entry point: Europe market window auto-scan."""
        await self._execute_scan('europe')

    async def _run_us_autoscan(self):
        """Cron entry point: US/Canada market window auto-scan."""
        await self._execute_scan('us_canada')

    async def run_now(self, guild_id: int, triggered_by: Optional[str] = None) -> Dict[str, Any]:
        """
        Manual /run-now: run the same scan pipeline for all regions, for one
        guild, bypassing schedule_enabled and posting full result lists.
        """
        logger.info("Manual run_now requested for guild %s by %s", guild_id, triggered_by)
        return await self._execute_scan(
            'all', manual=True, only_guild_id=guild_id, triggered_by=triggered_by
        )

    # ==================== Core scan pipeline ====================

    async def _execute_scan(
        self,
        region: str,
        *,
        manual: bool = False,
        only_guild_id: Optional[int] = None,
        triggered_by: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        The single scan pipeline used by scheduled jobs and /run-now.

        1. Wait for the Discord gateway, take the scan lock.
        2. Collect catalog + subscription tickers for the region(s).
        3. Fetch RSI14 (with short-TTL cache) and persist values.
        4. Per guild: change detection, subscription evaluation, channel
           posts, and a changelog status message.

        Scheduled runs skip guilds with schedule_enabled=False and only post
        newly-entered tickers. Manual runs bypass the toggle and post the full
        current lists.
        """
        await self.bot.wait_until_ready()

        if self._scan_lock.locked():
            logger.info("Scan (%s) waiting for a previous scan to finish", region)

        async with self._scan_lock:
            start_time = datetime.now(self.timezone)
            today = start_time.strftime("%Y-%m-%d")
            region_display = 'ALL' if region == 'all' else region.replace('_', '/').upper()
            scan_type = 'manual' if manual else 'scheduled'

            logger.info("=" * 60)
            logger.info("SCAN START: %s (%s) at %s", region_display, scan_type,
                        start_time.strftime('%Y-%m-%d %H:%M:%S %Z'))
            logger.info("=" * 60)

            summary: Dict[str, Any] = {
                'success': False,
                'type': scan_type,
                'region': region,
                'started': start_time,
                'tickers_total': 0,
                'tickers_ok': 0,
                'tickers_failed': 0,
                'failed_tickers': [],
                'persisted': 0,
                'guilds': {},
                'guilds_skipped_disabled': 0,
                'error': None,
            }

            try:
                regions = self.REGION_SETS.get(region, {region})

                # Target guilds come from the live gateway cache, NOT from the
                # guild_config table, so brand-new guilds are always included.
                if only_guild_id is not None:
                    guild = self.bot.get_guild(only_guild_id)
                    guilds = [guild] if guild else []
                    if not guild:
                        raise RuntimeError(f"Guild {only_guild_id} not found in bot cache")
                else:
                    guilds = list(self.bot.guilds)

                # Catalog tickers for the region(s)
                region_catalog_tickers = [
                    t for t in self.catalog.get_all_tickers()
                    if classify_ticker_region(t) in regions
                ]

                # Subscription tickers for the target guilds, same region(s)
                subs_by_guild: Dict[int, List[Dict]] = {}
                sub_tickers: Set[str] = set()
                for guild in guilds:
                    guild_subs = await self.db.get_subscriptions_with_state(guild_id=guild.id)
                    region_subs = [s for s in guild_subs if classify_ticker_region(s['ticker']) in regions]
                    subs_by_guild[guild.id] = region_subs
                    sub_tickers.update(s['ticker'] for s in region_subs)

                all_tickers = sorted(set(region_catalog_tickers) | sub_tickers)
                summary['tickers_total'] = len(all_tickers)

                logger.info(
                    "Scan scope: %d catalog tickers, %d subscription tickers, %d guilds",
                    len(region_catalog_tickers), len(sub_tickers), len(guilds)
                )

                if not all_tickers:
                    logger.info("No tickers to scan for region(s) %s - nothing to do", regions)
                    summary['success'] = True
                    return summary

                # Fetch RSI (cache-aware) and split successes/failures
                rsi_results = await self._fetch_rsi(all_tickers)

                successful_results: Dict[str, RSIResult] = {}
                failed_tickers: List[Tuple[str, str]] = []
                for ticker in all_tickers:
                    result = rsi_results.get(ticker)
                    if result and result.success and result.rsi_values.get(14) is not None:
                        successful_results[ticker] = result
                    else:
                        error = (result.error if result else None) or "No response from provider"
                        failed_tickers.append((ticker, error))

                summary['tickers_ok'] = len(successful_results)
                summary['tickers_failed'] = len(failed_tickers)
                summary['failed_tickers'] = [t for t, _ in failed_tickers]

                logger.info("RSI fetch: %d success, %d failed",
                            len(successful_results), len(failed_tickers))
                if failed_tickers and not successful_results:
                    logger.error(
                        "TradingView returned no usable data for any of %d tickers. "
                        "First errors: %s",
                        len(all_tickers),
                        "; ".join(f"{t}: {e}" for t, e in failed_tickers[:3])
                    )

                # Persist RSI values
                data_timestamp = None
                rsi_batch = []
                for ticker, result in successful_results.items():
                    rsi_14 = result.rsi_values.get(14)
                    if result.data_timestamp and not data_timestamp:
                        data_timestamp = result.data_timestamp
                    instrument = self.catalog.get_instrument(ticker)
                    rsi_batch.append({
                        'ticker': ticker,
                        'rsi_14': rsi_14,
                        'data_date': result.last_date or today,
                        'tradingview_slug': instrument.tradingview_slug if instrument else None,
                        'last_close': result.last_close,
                        'data_timestamp': result.data_timestamp,
                    })
                if rsi_batch:
                    try:
                        await self.db.upsert_ticker_rsi_batch(rsi_batch)
                        summary['persisted'] = len(rsi_batch)
                        logger.info("Persisted RSI values for %d tickers", len(rsi_batch))
                    except Exception:
                        logger.exception("Failed to persist RSI batch to SQLite")

                # Process each guild
                for guild in guilds:
                    config = await self.db.get_or_create_guild_config(guild.id)

                    if not manual and not config.schedule_enabled:
                        logger.info("Skipping guild %s (%s): schedule disabled",
                                    guild.id, getattr(guild, 'name', '?'))
                        summary['guilds_skipped_disabled'] += 1
                        continue

                    try:
                        guild_result = await self._process_guild(
                            guild=guild,
                            config=config,
                            region_display=region_display,
                            manual=manual,
                            triggered_by=triggered_by,
                            today=today,
                            start_time=start_time,
                            rsi_results=successful_results,
                            region_catalog_tickers=region_catalog_tickers,
                            guild_subscriptions=subs_by_guild.get(guild.id, []),
                            failed_tickers=failed_tickers,
                            data_timestamp=data_timestamp,
                        )
                        summary['guilds'][guild.id] = guild_result
                    except Exception:
                        logger.exception("Error processing guild %s during %s scan",
                                         guild.id, region_display)
                        summary['guilds'][guild.id] = {'error': 'processing failed (see logs)'}

                summary['success'] = True
                return summary

            except Exception as e:
                logger.exception("Scan failed: %s (%s)", region_display, scan_type)
                self.last_error = {
                    'time': datetime.now(self.timezone),
                    'source': f"scan:{region}",
                    'error': f"{type(e).__name__}: {e}",
                }
                summary['error'] = str(e)
                if manual:
                    raise
                return summary

            finally:
                end_time = datetime.now(self.timezone)
                duration = (end_time - start_time).total_seconds()
                summary['finished'] = end_time
                summary['duration_seconds'] = duration
                self.last_scan = summary
                logger.info("=" * 60)
                logger.info("SCAN COMPLETE: %s (%s) in %.1fs - %d/%d tickers OK, "
                            "%d guilds processed, %d skipped (disabled)",
                            region_display, scan_type, duration,
                            summary['tickers_ok'], summary['tickers_total'],
                            len(summary['guilds']), summary['guilds_skipped_disabled'])
                logger.info("=" * 60)

    async def _fetch_rsi(self, tickers: List[str]) -> Dict[str, RSIResult]:
        """
        Fetch RSI14 for tickers, serving recent results from the in-memory
        cache (TTL RSI_CACHE_TTL_SECONDS) to avoid duplicate TradingView load
        when jobs run back-to-back (e.g. 18:30 US scan + 18:30 daily check).
        """
        now = datetime.utcnow()
        cached: Dict[str, RSIResult] = {}
        to_fetch: List[str] = []

        for ticker in tickers:
            entry = self._rsi_cache.get(ticker)
            if entry and (now - entry[1]).total_seconds() < RSI_CACHE_TTL_SECONDS:
                cached[ticker] = entry[0]
            else:
                to_fetch.append(ticker)

        if cached:
            logger.info("RSI cache: %d hits, %d to fetch", len(cached), len(to_fetch))

        results = dict(cached)
        if to_fetch:
            fetched = await self.rsi_calculator.calculate_rsi_for_tickers(
                {t: [14] for t in to_fetch}
            )
            results.update(fetched)
            fetch_time = datetime.utcnow()
            for ticker, result in fetched.items():
                if result.success:
                    self._rsi_cache[ticker] = (result, fetch_time)

        return results

    async def _process_guild(
        self,
        guild: discord.Guild,
        config,
        region_display: str,
        manual: bool,
        triggered_by: Optional[str],
        today: str,
        start_time: datetime,
        rsi_results: Dict[str, RSIResult],
        region_catalog_tickers: List[str],
        guild_subscriptions: List[Dict],
        failed_tickers: List[Tuple[str, str]],
        data_timestamp: Optional[datetime],
    ) -> Dict[str, Any]:
        """
        Apply scan results to one guild: change detection, subscription
        evaluation, channel posts, state update, changelog status message.
        """
        oversold_threshold = config.auto_oversold_threshold
        overbought_threshold = config.auto_overbought_threshold

        # --- Evaluate catalog tickers against thresholds ---
        current_oversold: Dict[str, Tuple[float, RSIResult]] = {}
        current_overbought: Dict[str, Tuple[float, RSIResult]] = {}
        scanned_ok: Set[str] = set()

        for ticker in region_catalog_tickers:
            result = rsi_results.get(ticker)
            if not result or not result.rsi_values:
                continue
            rsi_14 = result.rsi_values.get(14)
            if rsi_14 is None:
                continue
            scanned_ok.add(ticker)
            if rsi_14 < oversold_threshold:
                current_oversold[ticker] = (rsi_14, result)
            if rsi_14 > overbought_threshold:
                current_overbought[ticker] = (rsi_14, result)

        current_oversold_tickers = set(current_oversold)
        current_overbought_tickers = set(current_overbought)

        # --- Change detection (against today's persisted state) ---
        prev_oversold_state = await self.db.get_auto_scan_state(guild.id, today, 'UNDER')
        prev_overbought_state = await self.db.get_auto_scan_state(guild.id, today, 'OVER')
        prev_oversold = prev_oversold_state.last_tickers if prev_oversold_state else set()
        prev_overbought = prev_overbought_state.last_tickers if prev_overbought_state else set()

        newly_oversold = current_oversold_tickers - prev_oversold
        newly_overbought = current_overbought_tickers - prev_overbought

        logger.info(
            "Guild %s change detection: oversold %d total (%d new), overbought %d total (%d new)",
            guild.id, len(current_oversold_tickers), len(newly_oversold),
            len(current_overbought_tickers), len(newly_overbought)
        )

        # Manual runs post the full current lists; scheduled runs only new entries.
        post_oversold = current_oversold if manual else {t: current_oversold[t] for t in newly_oversold}
        post_overbought = current_overbought if manual else {t: current_overbought[t] for t in newly_overbought}

        # --- Evaluate this guild's subscriptions (guild-scoped state updates) ---
        subscription_alerts: Dict[str, List[Alert]] = {'UNDER': [], 'OVER': []}
        if guild_subscriptions:
            alerts_by_condition = await self.alert_engine.evaluate_subscriptions(
                rsi_results=rsi_results,
                dry_run=False,
                guild_id=guild.id,
            )
            subscription_alerts['UNDER'] = alerts_by_condition.get('UNDER', [])
            subscription_alerts['OVER'] = alerts_by_condition.get('OVER', [])

        # --- Channels and permissions ---
        oversold_ch, overbought_ch = get_alert_channels(guild)
        changelog_ch = get_changelog_channel(guild)
        channel_issues: List[str] = []

        for name, ch in ((OVERSOLD_CHANNEL_NAME, oversold_ch),
                         (OVERBOUGHT_CHANNEL_NAME, overbought_ch),
                         (CHANGELOG_CHANNEL_NAME, changelog_ch)):
            if not ch:
                channel_issues.append(f"#{name}: channel not found")
            elif not can_send_to_channel(ch, guild.me):
                channel_issues.append(f"#{name}: missing Send Messages permission")

        if channel_issues:
            logger.warning("Guild %s channel issues: %s", guild.id, "; ".join(channel_issues))

        messages_sent = 0
        has_oversold_content = bool(post_oversold) or bool(subscription_alerts['UNDER'])
        has_overbought_content = bool(post_overbought) or bool(subscription_alerts['OVER'])

        # --- Post to alert channels ---
        if can_send_to_channel(oversold_ch, guild.me) and (has_oversold_content or manual):
            messages_sent += await self._post_combined_alerts(
                channel=oversold_ch,
                condition='UNDER',
                threshold=oversold_threshold,
                catalog_hits=post_oversold,
                subscription_alerts=subscription_alerts['UNDER'],
                data_timestamp=data_timestamp,
                region_display=region_display,
                manual=manual,
            )

        if can_send_to_channel(overbought_ch, guild.me) and (has_overbought_content or manual):
            messages_sent += await self._post_combined_alerts(
                channel=overbought_ch,
                condition='OVER',
                threshold=overbought_threshold,
                catalog_hits=post_overbought,
                subscription_alerts=subscription_alerts['OVER'],
                data_timestamp=data_timestamp,
                region_display=region_display,
                manual=manual,
            )

        # --- Update change-detection state ---
        # Merge instead of replace: only tickers actually scanned this run may
        # change state, so an overlapping scan of the *other* region cannot be
        # wiped out (Europe and US jobs both run 15:30-17:30).
        merged_oversold = (prev_oversold - scanned_ok) | current_oversold_tickers
        merged_overbought = (prev_overbought - scanned_ok) | current_overbought_tickers

        try:
            await self.db.update_auto_scan_state(
                guild_id=guild.id, scan_date=today, condition='UNDER',
                tickers=merged_oversold, increment_post_count=has_oversold_content,
            )
            await self.db.update_auto_scan_state(
                guild_id=guild.id, scan_date=today, condition='OVER',
                tickers=merged_overbought, increment_post_count=has_overbought_content,
            )
        except Exception:
            logger.exception("Failed to update auto-scan state for guild %s", guild.id)

        # --- Always post a status message to the changelog ---
        end_time = datetime.now(self.timezone)
        changelog_posted = False
        if can_send_to_channel(changelog_ch, guild.me):
            catalog_failed = [t for t, _ in failed_tickers if t in set(region_catalog_tickers)]
            guild_sub_tickers = set(s['ticker'] for s in guild_subscriptions)
            subscription_failed = [t for t, _ in failed_tickers if t in guild_sub_tickers]

            changelog_posted = await self._post_changelog_message(
                channel=changelog_ch,
                region_display=region_display,
                manual=manual,
                triggered_by=triggered_by,
                start_time=start_time,
                end_time=end_time,
                catalog_total=len(region_catalog_tickers),
                catalog_success=len(scanned_ok),
                catalog_failed=catalog_failed,
                subscription_total=len(guild_subscriptions),
                subscription_success=len([s for s in guild_subscriptions if s['ticker'] in rsi_results]),
                subscription_failed=subscription_failed,
                oversold_total=len(current_oversold_tickers),
                oversold_new=len(newly_oversold),
                oversold_sub_alerts=len(subscription_alerts['UNDER']),
                overbought_total=len(current_overbought_tickers),
                overbought_new=len(newly_overbought),
                overbought_sub_alerts=len(subscription_alerts['OVER']),
                oversold_threshold=oversold_threshold,
                overbought_threshold=overbought_threshold,
                data_timestamp=data_timestamp,
                messages_sent=messages_sent,
                posted_oversold=has_oversold_content,
                posted_overbought=has_overbought_content,
                channel_issues=channel_issues,
            )
        else:
            logger.warning(
                "Guild %s: cannot post scan status to #%s (missing channel or permission)",
                guild.id, CHANGELOG_CHANNEL_NAME
            )

        return {
            'oversold_total': len(current_oversold_tickers),
            'oversold_new': len(newly_oversold),
            'overbought_total': len(current_overbought_tickers),
            'overbought_new': len(newly_overbought),
            'sub_alerts_under': len(subscription_alerts['UNDER']),
            'sub_alerts_over': len(subscription_alerts['OVER']),
            'messages_sent': messages_sent,
            'changelog_posted': changelog_posted,
            'channel_issues': channel_issues,
        }

    # ==================== Message formatting/sending ====================

    async def _post_combined_alerts(
        self,
        channel: discord.TextChannel,
        condition: str,
        threshold: float,
        catalog_hits: Dict[str, Tuple[float, RSIResult]],
        subscription_alerts: List[Alert],
        data_timestamp: Optional[datetime],
        region_display: str,
        manual: bool = False,
    ) -> int:
        """
        Post catalog hits + subscription alerts to an alert channel.

        Returns:
            Number of messages sent.
        """
        run_label = " (Manual Run)" if manual else ""

        if condition == 'UNDER':
            header = f"📉 **Auto-Scan: Oversold ({region_display})**{run_label}\n"
            header += f"Threshold: RSI < {threshold}\n"
            sorted_catalog = sorted(catalog_hits.items(), key=lambda x: x[1][0])
            empty_text = f"No stocks currently meeting oversold criteria (RSI < {threshold})."
        else:
            header = f"📈 **Auto-Scan: Overbought ({region_display})**{run_label}\n"
            header += f"Threshold: RSI > {threshold}\n"
            sorted_catalog = sorted(catalog_hits.items(), key=lambda x: -x[1][0])
            empty_text = f"No stocks currently meeting overbought criteria (RSI > {threshold})."

        if data_timestamp:
            header += f"Data as of: {data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}\n"
        header += "\n"

        lines = []

        if sorted_catalog:
            label = "**📊 Catalog Tickers:**" if manual else "**📊 Catalog Tickers (newly entered zone):**"
            lines.append(label)
            for i, (ticker, (rsi_val, _result)) in enumerate(sorted_catalog, 1):
                instrument = self.catalog.get_instrument(ticker)
                name = instrument.name if instrument else ticker
                url = instrument.tradingview_url if instrument else ""
                if url:
                    lines.append(f"{i}) **{ticker}** — [{name}](<{url}>) — RSI14: **{rsi_val:.1f}**")
                else:
                    lines.append(f"{i}) **{ticker}** — {name} — RSI14: **{rsi_val:.1f}**")
            lines.append("")

        if subscription_alerts:
            lines.append("**🔔 Subscription Alerts:**")
            for i, alert in enumerate(subscription_alerts, 1):
                instrument = self.catalog.get_instrument(alert.ticker)
                url = instrument.tradingview_url if instrument else alert.tradingview_url
                rule_symbol = "<" if alert.condition == "UNDER" else ">"
                if alert.just_crossed or alert.days_in_zone <= 1:
                    persistence = "🆕 **just crossed**"
                else:
                    persistence = f"⏱️ **day {alert.days_in_zone}**"
                if url:
                    lines.append(
                        f"{i}) **{alert.ticker}** — [{alert.name}](<{url}>) — "
                        f"RSI{alert.period}: **{alert.rsi_value:.1f}** | "
                        f"Rule: **{rule_symbol} {alert.threshold}** | {persistence}"
                    )
                else:
                    lines.append(
                        f"{i}) **{alert.ticker}** — {alert.name} — "
                        f"RSI{alert.period}: **{alert.rsi_value:.1f}** | "
                        f"Rule: **{rule_symbol} {alert.threshold}** | {persistence}"
                    )

        if not lines:
            if not manual:
                return 0
            lines = [empty_text]

        content = header + "\n".join(lines)
        sent_count = 0
        for msg in chunk_message(content, max_length=DISCORD_SAFE_LIMIT):
            try:
                await channel.send(msg, suppress_embeds=True)
                sent_count += 1
            except discord.HTTPException as e:
                logger.error("Failed to send alert message to #%s: %s", channel.name, e)
        return sent_count

    async def _post_changelog_message(
        self,
        channel: discord.TextChannel,
        region_display: str,
        manual: bool,
        triggered_by: Optional[str],
        start_time: datetime,
        end_time: datetime,
        catalog_total: int,
        catalog_success: int,
        catalog_failed: List[str],
        subscription_total: int,
        subscription_success: int,
        subscription_failed: List[str],
        oversold_total: int,
        oversold_new: int,
        oversold_sub_alerts: int,
        overbought_total: int,
        overbought_new: int,
        overbought_sub_alerts: int,
        oversold_threshold: float,
        overbought_threshold: float,
        data_timestamp: Optional[datetime],
        messages_sent: int,
        posted_oversold: bool,
        posted_overbought: bool,
        channel_issues: List[str],
    ) -> bool:
        """
        Post the per-scan status message to #server-changelog.
        Always attempted for every processed scan, even with zero hits.

        Returns:
            True if the message was sent.
        """
        duration = (end_time - start_time).total_seconds()

        if manual:
            trigger_line = f"Trigger: 👤 Manual (`/run-now`) by {triggered_by or 'unknown'}\n"
            title = f"🔄 **Manual RSI Scan Complete** ({region_display})"
        else:
            trigger_line = "Trigger: 🕒 Scheduled auto-scan\n"
            title = f"🔄 **Auto-Scan Complete** ({region_display})"

        msg = f"{title}\n{trigger_line}\n"

        total_attempted = catalog_total + subscription_total
        total_success = catalog_success + subscription_success
        if total_attempted > 0 and total_success == 0:
            msg += "🚨 **TradingView data fetch FAILED for all tickers - no results this run.**\n\n"

        msg += "**⏱️ Timing:**\n"
        msg += f"• Start: {start_time.strftime('%H:%M:%S')} | End: {end_time.strftime('%H:%M:%S')} | Duration: {duration:.1f}s\n"
        if data_timestamp:
            msg += f"• Data timestamp: {data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}\n"
        msg += "\n"

        msg += "**📊 Catalog Scan:**\n"
        msg += f"• Tickers: {catalog_success}/{catalog_total} successful\n"
        if catalog_failed:
            preview = catalog_failed[:5]
            msg += f"• ❌ Failed ({len(catalog_failed)}): {', '.join(preview)}"
            if len(catalog_failed) > 5:
                msg += f" (+{len(catalog_failed) - 5} more)"
            msg += "\n"
        msg += "\n"

        msg += "**🔔 Subscriptions:**\n"
        msg += f"• Total: {subscription_total} | Successful: {subscription_success}\n"
        if subscription_failed:
            preview = subscription_failed[:5]
            msg += f"• ❌ Failed ({len(subscription_failed)}): {', '.join(preview)}"
            if len(subscription_failed) > 5:
                msg += f" (+{len(subscription_failed) - 5} more)"
            msg += "\n"
        msg += "\n"

        msg += "**📈 Results:**\n"
        msg += f"• Oversold (< {oversold_threshold}): {oversold_total} total, **{oversold_new} new**"
        if oversold_sub_alerts:
            msg += f", {oversold_sub_alerts} sub alerts"
        msg += "\n"
        msg += f"• Overbought (> {overbought_threshold}): {overbought_total} total, **{overbought_new} new**"
        if overbought_sub_alerts:
            msg += f", {overbought_sub_alerts} sub alerts"
        msg += "\n\n"

        msg += "**📬 Posted Updates:**\n"
        msg += f"• #{OVERSOLD_CHANNEL_NAME}: {'✅ Posted' if posted_oversold else '⏭️ No new hits'}\n"
        msg += f"• #{OVERBOUGHT_CHANNEL_NAME}: {'✅ Posted' if posted_overbought else '⏭️ No new hits'}\n"
        msg += f"• Messages sent: {messages_sent}\n"

        if channel_issues:
            msg += "\n⚠️ **Channel Issues:**\n"
            for issue in channel_issues:
                msg += f"• {issue}\n"

        try:
            for chunk in chunk_message(msg, max_length=DISCORD_SAFE_LIMIT):
                await channel.send(chunk)
            return True
        except discord.HTTPException as e:
            logger.error("Failed to send changelog message: %s", e)
            return False

    # ==================== Daily subscription check ====================

    async def _run_daily_check(self, guild_id: int):
        """
        Per-guild daily subscription check at the guild's configured
        schedule_time. Subscription-only (catalog auto-scans are handled by
        the regional jobs); reuses cached RSI data when a regional scan just
        ran (e.g. both at 18:30).
        """
        await self.bot.wait_until_ready()

        if self._scan_lock.locked():
            logger.info("Daily check for guild %s waiting for a running scan", guild_id)

        async with self._scan_lock:
            start_time = datetime.now(self.timezone)
            logger.info("DAILY CHECK START: guild %s at %s",
                        guild_id, start_time.strftime('%Y-%m-%d %H:%M:%S %Z'))

            try:
                config = await self.db.get_or_create_guild_config(guild_id)
                if not config.schedule_enabled:
                    logger.info("Daily check skipped for guild %s: schedule disabled", guild_id)
                    return

                guild = self.bot.get_guild(guild_id)
                if not guild:
                    logger.warning("Daily check: guild %s not in bot cache, skipping", guild_id)
                    return

                subs = await self.db.get_subscriptions_with_state(guild_id=guild_id)
                if not subs:
                    logger.info("Daily check: guild %s has no active subscriptions", guild_id)
                    return

                tickers = sorted(set(s['ticker'] for s in subs))
                rsi_results = await self._fetch_rsi(tickers)

                successful = sum(1 for r in rsi_results.values() if r.success)
                failed = len(rsi_results) - successful

                alerts_by_condition = await self.alert_engine.evaluate_subscriptions(
                    rsi_results, dry_run=False, guild_id=guild_id
                )
                under_alerts = alerts_by_condition.get('UNDER', [])
                over_alerts = alerts_by_condition.get('OVER', [])

                oversold_ch, overbought_ch = get_alert_channels(guild)
                changelog_ch = get_changelog_channel(guild)

                sent_count = 0
                send_errors: List[str] = []

                if under_alerts:
                    if can_send_to_channel(oversold_ch, guild.me):
                        try:
                            for msg in format_alert_list(under_alerts, 'UNDER'):
                                await oversold_ch.send(msg, suppress_embeds=True)
                                sent_count += 1
                        except discord.HTTPException as e:
                            logger.error("Daily check: failed sending to #%s in guild %s: %s",
                                         OVERSOLD_CHANNEL_NAME, guild_id, e)
                            send_errors.append(f"#{OVERSOLD_CHANNEL_NAME}: {e}")
                    else:
                        send_errors.append(f"#{OVERSOLD_CHANNEL_NAME}: missing channel or permission")

                if over_alerts:
                    if can_send_to_channel(overbought_ch, guild.me):
                        try:
                            for msg in format_alert_list(over_alerts, 'OVER'):
                                await overbought_ch.send(msg, suppress_embeds=True)
                                sent_count += 1
                        except discord.HTTPException as e:
                            logger.error("Daily check: failed sending to #%s in guild %s: %s",
                                         OVERBOUGHT_CHANNEL_NAME, guild_id, e)
                            send_errors.append(f"#{OVERBOUGHT_CHANNEL_NAME}: {e}")
                    else:
                        send_errors.append(f"#{OVERBOUGHT_CHANNEL_NAME}: missing channel or permission")

                end_time = datetime.now(self.timezone)
                duration = (end_time - start_time).total_seconds()

                # Brief daily status to the changelog
                if can_send_to_channel(changelog_ch, guild.me):
                    status = (
                        f"🗓️ **Daily Subscription Check** ({start_time.strftime('%H:%M %Z')})\n"
                        f"• Subscriptions: {len(subs)} | Tickers: {successful}/{len(tickers)} OK\n"
                        f"• Alerts: {len(under_alerts)} oversold, {len(over_alerts)} overbought\n"
                        f"• Messages sent: {sent_count} | Duration: {duration:.1f}s"
                    )
                    if send_errors:
                        status += "\n⚠️ Errors:\n" + "\n".join(f"• {e}" for e in send_errors)
                    try:
                        await changelog_ch.send(status)
                    except discord.HTTPException as e:
                        logger.error("Daily check: failed to post changelog for guild %s: %s", guild_id, e)

                logger.info(
                    "DAILY CHECK COMPLETE: guild %s in %.1fs - %d/%d tickers OK, "
                    "%d alerts, %d messages",
                    guild_id, duration, successful, len(tickers),
                    len(under_alerts) + len(over_alerts), sent_count
                )

            except Exception as e:
                logger.exception("Error in daily check for guild %s", guild_id)
                self.last_error = {
                    'time': datetime.now(self.timezone),
                    'source': f"daily_check:{guild_id}",
                    'error': f"{type(e).__name__}: {e}",
                }

    # ==================== Maintenance ====================

    async def _run_maintenance(self):
        """Nightly database housekeeping."""
        logger.info("Running scheduled database maintenance")
        try:
            await self.db.cleanup_old_auto_scan_states(days_to_keep=7)
            removed = await self.db.cleanup_old_ticker_rsi(days_to_keep=30)
            logger.info("Database maintenance complete (removed %s stale RSI rows)", removed)
        except Exception:
            logger.exception("Database maintenance failed")
