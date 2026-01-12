"""
Scheduler module for RSI Discord Bot.

FIXED IMPLEMENTATION - Addresses the following issues:
1. RSI persistence for all tickers (spec section 4)
2. Change detection for auto-scan (only alert on state transitions)
3. Proper subscription inclusion in auto-scans
4. Comprehensive changelog messages with start/end time and failure lists
5. Reliable operation under systemd on Raspberry Pi

Auto-Scan Specification (from spec section 2):
- Runs at minute :30 on weekdays (Mon-Fri) during market hours
- European window: 09:30-17:30 Europe/Oslo
- US/Canada window: 15:30-22:30 Europe/Oslo
- Evaluates both catalog tickers AND manual subscriptions
- Posts to #rsi-oversold and #rsi-overbought ONLY on state change
- Always posts status to #server-changelog with failure details
"""
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any

import discord
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.asyncio import AsyncIOExecutor

from bot.config import (
    DEFAULT_TIMEZONE, DEFAULT_SCHEDULE_TIME,
    OVERSOLD_CHANNEL_NAME, OVERBOUGHT_CHANNEL_NAME, CHANGELOG_CHANNEL_NAME,
    EUROPEAN_SUFFIXES, US_CANADA_SUFFIXES,
    EUROPE_MARKET_START_HOUR, EUROPE_MARKET_START_MINUTE,
    EUROPE_MARKET_END_HOUR, EUROPE_MARKET_END_MINUTE,
    US_MARKET_START_HOUR, US_MARKET_START_MINUTE,
    US_MARKET_END_HOUR, US_MARKET_END_MINUTE,
    DISCORD_SAFE_LIMIT, TV_BATCH_SIZE
)
from bot.repositories.database import Database, AutoScanState
from bot.services.market_data.rsi_calculator import RSICalculator, RSIResult
from bot.cogs.alert_engine import AlertEngine, Alert, format_alert_list
from bot.repositories.ticker_catalog import get_catalog
from bot.utils.message_utils import chunk_message

logger = logging.getLogger(__name__)


def get_alert_channels(guild: discord.Guild) -> Tuple[Optional[discord.TextChannel], Optional[discord.TextChannel]]:
    """Get the fixed alert channels for a guild."""
    oversold_channel = discord.utils.get(guild.text_channels, name=OVERSOLD_CHANNEL_NAME)
    overbought_channel = discord.utils.get(guild.text_channels, name=OVERBOUGHT_CHANNEL_NAME)
    return oversold_channel, overbought_channel


def get_changelog_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    """Get the changelog channel for a guild."""
    return discord.utils.get(guild.text_channels, name=CHANGELOG_CHANNEL_NAME)


def can_send_to_channel(channel: discord.TextChannel, bot_member: discord.Member) -> bool:
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


class RSIScheduler:
    """
    Manages scheduled RSI check jobs including:
    - Hourly automatic scans for catalog tickers + subscriptions (with change detection)
    - Daily subscription-based alerts
    - Schedule enable/disable per guild
    
    ROOT CAUSE OF ORIGINAL FAILURE:
    The original scheduler did not implement proper change detection for catalog tickers
    during auto-scans. It would post all tickers meeting threshold criteria every time,
    rather than only posting when tickers ENTER the oversold/overbought state.
    Additionally, RSI values were not persisted for catalog tickers, only for subscriptions.
    """

    def __init__(self, bot):
        self.bot = bot
        self.db: Database = bot.db
        self.rsi_calculator = RSICalculator()
        self.alert_engine = AlertEngine(self.db)
        self.timezone = pytz.timezone(DEFAULT_TIMEZONE)
        self.catalog = get_catalog()
        
        # Configure scheduler with proper settings for reliability
        jobstores = {'default': MemoryJobStore()}
        executors = {'default': AsyncIOExecutor()}
        job_defaults = {
            'coalesce': True,  # Combine missed runs
            'max_instances': 1,  # Prevent overlapping
            'misfire_grace_time': 600  # 10 minute grace period
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=self.timezone
        )
        
        self._guild_jobs: Dict[int, str] = {}

    async def start(self):
        """Start the scheduler and set up jobs."""
        logger.info("=" * 60)
        logger.info("Starting RSI scheduler...")
        logger.info(f"Timezone: {DEFAULT_TIMEZONE}")
        logger.info("=" * 60)

        # Add hourly auto-scan jobs (primary feature)
        self._add_hourly_autoscan_jobs()
        
        # Add daily subscription check job (legacy compatibility)
        self._add_daily_subscription_job()

        self.scheduler.start()
        
        # Log all scheduled jobs
        jobs = self.scheduler.get_jobs()
        logger.info(f"Scheduler started with {len(jobs)} jobs")
        for job in jobs:
            logger.info(f"  - {job.id}: next run at {job.next_run_time}")

    def _add_daily_subscription_job(self):
        """Add the default daily subscription check job."""
        try:
            hour, minute = map(int, DEFAULT_SCHEDULE_TIME.split(":"))
        except ValueError:
            hour, minute = 18, 30

        trigger = CronTrigger(
            hour=hour,
            minute=minute,
            day_of_week='mon-fri',
            timezone=self.timezone
        )

        self.scheduler.add_job(
            self._run_daily_check,
            trigger=trigger,
            id="daily_rsi_check",
            name="Daily RSI Check",
            replace_existing=True
        )

        logger.info(f"Scheduled daily RSI check at {hour:02d}:{minute:02d} {DEFAULT_TIMEZONE} (weekdays)")
    
    def _add_hourly_autoscan_jobs(self):
        """
        Add hourly auto-scan jobs for both market regions.
        
        Schedule per spec section 2.1:
        - Europe: 09:30, 10:30, 11:30, 12:30, 13:30, 14:30, 15:30, 16:30, 17:30
        - US/Canada: 15:30, 16:30, 17:30, 18:30, 19:30, 20:30, 21:30, 22:30
        - Weekdays only (Mon-Fri)
        """
        # Europe market hours: 09:30 - 17:30 (hours 9-17 at :30)
        europe_hours = list(range(EUROPE_MARKET_START_HOUR, EUROPE_MARKET_END_HOUR + 1))
        for hour in europe_hours:
            trigger = CronTrigger(
                hour=hour,
                minute=30,
                day_of_week='mon-fri',
                timezone=self.timezone
            )
            self.scheduler.add_job(
                self._run_europe_autoscan,
                trigger=trigger,
                id=f"europe_autoscan_{hour}",
                name=f"Europe Auto-Scan {hour}:30",
                replace_existing=True,
            )
        
        # US/Canada market hours: 15:30 - 22:30 (hours 15-22 at :30)
        us_hours = list(range(US_MARKET_START_HOUR, US_MARKET_END_HOUR + 1))
        for hour in us_hours:
            trigger = CronTrigger(
                hour=hour,
                minute=30,
                day_of_week='mon-fri',
                timezone=self.timezone
            )
            self.scheduler.add_job(
                self._run_us_autoscan,
                trigger=trigger,
                id=f"us_autoscan_{hour}",
                name=f"US/Canada Auto-Scan {hour}:30",
                replace_existing=True,
            )
        
        logger.info(
            f"Scheduled auto-scan jobs: "
            f"Europe {EUROPE_MARKET_START_HOUR}:30-{EUROPE_MARKET_END_HOUR}:30 ({len(europe_hours)} runs), "
            f"US/Canada {US_MARKET_START_HOUR}:30-{US_MARKET_END_HOUR}:30 ({len(us_hours)} runs) (weekdays)"
        )

    async def _run_europe_autoscan(self):
        """Run automatic RSI scan for European tickers."""
        await self._run_autoscan('europe')
    
    async def _run_us_autoscan(self):
        """Run automatic RSI scan for US/Canada tickers."""
        await self._run_autoscan('us_canada')

    async def _run_autoscan(self, region: str):
        """
        Run automatic RSI scan for a specific region.
        
        FIXED IMPLEMENTATION - This scan:
        1. Gets all catalog tickers for the region
        2. Gets all manual subscriptions for the region
        3. Fetches RSI14 for all unique tickers
        4. Persists RSI values to database (spec section 4)
        5. Applies CHANGE DETECTION: only alerts on state transitions (spec section 2.4)
        6. Evaluates subscriptions via AlertEngine
        7. Posts to channels ONLY if there are NEW state changes
        8. Always posts status to #server-changelog with start/end time and failures
        """
        start_time = datetime.now(self.timezone)
        today = start_time.strftime("%Y-%m-%d")
        region_display = region.replace('_', '/').title()
        
        logger.info("=" * 60)
        logger.info(f"AUTO-SCAN START: {region_display}")
        logger.info(f"Time: {start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info("=" * 60)
        
        try:
            # ======================================================================
            # Step 1: Get catalog tickers for this region
            # ======================================================================
            all_catalog_tickers = self.catalog.get_all_tickers()
            region_catalog_tickers = [t for t in all_catalog_tickers if classify_ticker_region(t) == region]
            
            logger.info(f"Catalog tickers for {region}: {len(region_catalog_tickers)}")
            
            # ======================================================================
            # Step 2: Get subscription tickers for this region
            # ======================================================================
            all_subscriptions = await self.db.get_subscriptions_with_state()
            region_subscription_tickers: Set[str] = set()
            region_subscriptions: List[Dict] = []
            
            for sub in all_subscriptions:
                ticker = sub['ticker']
                if classify_ticker_region(ticker) == region:
                    region_subscription_tickers.add(ticker)
                    region_subscriptions.append(sub)
            
            logger.info(f"Subscription tickers for {region}: {len(region_subscription_tickers)} (from {len(region_subscriptions)} subscriptions)")
            
            # ======================================================================
            # Step 3: Combine and fetch RSI for all unique tickers
            # ======================================================================
            all_tickers = list(set(region_catalog_tickers) | region_subscription_tickers)
            
            if not all_tickers:
                logger.info(f"No {region} tickers to scan, skipping")
                return
            
            logger.info(f"Fetching RSI for {len(all_tickers)} unique tickers")
            
            ticker_periods = {t: [14] for t in all_tickers}
            rsi_results = await self.rsi_calculator.calculate_rsi_for_tickers(ticker_periods)
            
            # ======================================================================
            # Step 4: Track successes and failures
            # ======================================================================
            successful_results: Dict[str, RSIResult] = {}
            failed_tickers: List[Tuple[str, str]] = []  # (ticker, error_reason)
            
            for ticker in all_tickers:
                result = rsi_results.get(ticker)
                if result and result.success:
                    successful_results[ticker] = result
                else:
                    error = result.error if result else "No response from provider"
                    failed_tickers.append((ticker, error))
            
            logger.info(f"RSI fetch: {len(successful_results)} success, {len(failed_tickers)} failed")
            
            # ======================================================================
            # Step 5: PERSIST RSI VALUES (Spec Section 4)
            # ======================================================================
            rsi_batch = []
            data_timestamp = None
            
            for ticker, result in successful_results.items():
                rsi_14 = result.rsi_values.get(14)
                if rsi_14 is not None:
                    # Get data timestamp from result
                    if result.data_timestamp and not data_timestamp:
                        data_timestamp = result.data_timestamp
                    
                    # Get tradingview slug from catalog
                    instrument = self.catalog.get_instrument(ticker)
                    tv_slug = instrument.tradingview_slug if instrument else None
                    
                    rsi_batch.append({
                        'ticker': ticker,
                        'rsi_14': rsi_14,
                        'data_date': result.last_date or today,
                        'tradingview_slug': tv_slug,
                        'last_close': result.last_close,
                        'data_timestamp': result.data_timestamp
                    })
            
            if rsi_batch:
                await self.db.upsert_ticker_rsi_batch(rsi_batch)
                logger.info(f"Persisted RSI values for {len(rsi_batch)} tickers")
            
            # ======================================================================
            # Step 6: Process each guild
            # ======================================================================
            guild_ids = await self.db.get_all_guild_ids()
            
            for guild_id in guild_ids:
                guild = self.bot.get_guild(guild_id)
                if not guild:
                    logger.warning(f"Guild {guild_id} not accessible")
                    continue
                
                # Check if schedule is enabled
                config = await self.db.get_or_create_guild_config(guild_id)
                if not config.schedule_enabled:
                    logger.info(f"Skipping auto-scan for guild {guild_id}: schedule disabled")
                    continue
                
                await self._process_guild_autoscan(
                    guild=guild,
                    region=region,
                    today=today,
                    start_time=start_time,
                    rsi_results=successful_results,
                    region_catalog_tickers=region_catalog_tickers,
                    region_subscriptions=region_subscriptions,
                    failed_tickers=failed_tickers,
                    data_timestamp=data_timestamp
                )
            
            end_time = datetime.now(self.timezone)
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info(f"AUTO-SCAN COMPLETE: {region_display}")
            logger.info(f"Duration: {duration:.1f}s")
            logger.info(f"Tickers: {len(successful_results)}/{len(all_tickers)} successful")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error in {region} auto-scan: {e}", exc_info=True)

    async def _process_guild_autoscan(
        self,
        guild: discord.Guild,
        region: str,
        today: str,
        start_time: datetime,
        rsi_results: Dict[str, RSIResult],
        region_catalog_tickers: List[str],
        region_subscriptions: List[Dict],
        failed_tickers: List[Tuple[str, str]],
        data_timestamp: Optional[datetime]
    ):
        """
        Process auto-scan results for a single guild with CHANGE DETECTION.
        
        CHANGE DETECTION LOGIC (Spec Section 2.4):
        - Track previous state (OVERSOLD, OVERBOUGHT, NEUTRAL) per ticker per day
        - Only post alerts when a ticker ENTERS oversold/overbought state
        - Do not repeat alerts if ticker stays in same state across runs
        """
        config = await self.db.get_or_create_guild_config(guild.id)
        oversold_threshold = config.auto_oversold_threshold
        overbought_threshold = config.auto_overbought_threshold
        
        # Get previous scan state for today
        prev_oversold_state = await self.db.get_auto_scan_state(guild.id, today, 'UNDER')
        prev_overbought_state = await self.db.get_auto_scan_state(guild.id, today, 'OVER')
        
        prev_oversold_tickers = prev_oversold_state.last_tickers if prev_oversold_state else set()
        prev_overbought_tickers = prev_overbought_state.last_tickers if prev_overbought_state else set()
        
        # ======================================================================
        # Evaluate catalog tickers with change detection
        # ======================================================================
        current_oversold: Dict[str, Tuple[float, RSIResult]] = {}
        current_overbought: Dict[str, Tuple[float, RSIResult]] = {}
        
        for ticker in region_catalog_tickers:
            result = rsi_results.get(ticker)
            if not result or not result.rsi_values:
                continue
            
            rsi_14 = result.rsi_values.get(14)
            if rsi_14 is None:
                continue
            
            if rsi_14 < oversold_threshold:
                current_oversold[ticker] = (rsi_14, result)
            
            if rsi_14 > overbought_threshold:
                current_overbought[ticker] = (rsi_14, result)
        
        # CHANGE DETECTION: Find only NEW entries
        current_oversold_tickers = set(current_oversold.keys())
        current_overbought_tickers = set(current_overbought.keys())
        
        newly_oversold = current_oversold_tickers - prev_oversold_tickers
        newly_overbought = current_overbought_tickers - prev_overbought_tickers
        
        logger.info(
            f"Guild {guild.id} catalog change detection: "
            f"oversold {len(current_oversold_tickers)} total ({len(newly_oversold)} new), "
            f"overbought {len(current_overbought_tickers)} total ({len(newly_overbought)} new)"
        )
        
        # Filter to only new entries for posting
        new_oversold_catalog = {t: current_oversold[t] for t in newly_oversold}
        new_overbought_catalog = {t: current_overbought[t] for t in newly_overbought}
        
        # ======================================================================
        # Evaluate subscriptions for this guild
        # ======================================================================
        guild_subscriptions = [s for s in region_subscriptions if s['guild_id'] == guild.id]
        
        # Use AlertEngine for subscription evaluation (handles crossing logic)
        subscription_alerts = {'UNDER': [], 'OVER': []}
        
        if guild_subscriptions:
            # Create filtered RSI results for just subscription tickers
            sub_tickers = set(s['ticker'] for s in guild_subscriptions)
            sub_rsi_results = {t: rsi_results[t] for t in sub_tickers if t in rsi_results}
            
            if sub_rsi_results:
                alerts_by_condition = await self.alert_engine.evaluate_subscriptions(
                    rsi_results=sub_rsi_results,
                    dry_run=False
                )
                
                # Filter to only this guild
                subscription_alerts['UNDER'] = [
                    a for a in alerts_by_condition.get('UNDER', [])
                    if a.guild_id == guild.id
                ]
                subscription_alerts['OVER'] = [
                    a for a in alerts_by_condition.get('OVER', [])
                    if a.guild_id == guild.id
                ]
        
        logger.info(
            f"Guild {guild.id} subscription alerts: "
            f"UNDER {len(subscription_alerts['UNDER'])}, OVER {len(subscription_alerts['OVER'])}"
        )
        
        # ======================================================================
        # Determine if we have ANY new alerts to post
        # ======================================================================
        has_new_oversold = len(new_oversold_catalog) > 0 or len(subscription_alerts['UNDER']) > 0
        has_new_overbought = len(new_overbought_catalog) > 0 or len(subscription_alerts['OVER']) > 0
        
        # Get channels
        oversold_ch, overbought_ch = get_alert_channels(guild)
        changelog_ch = get_changelog_channel(guild)
        
        messages_sent = 0
        
        # ======================================================================
        # Post to oversold channel ONLY if there are NEW state changes
        # ======================================================================
        if has_new_oversold and oversold_ch and can_send_to_channel(oversold_ch, guild.me):
            messages_sent += await self._post_combined_alerts(
                channel=oversold_ch,
                condition='UNDER',
                threshold=oversold_threshold,
                catalog_hits=new_oversold_catalog,
                subscription_alerts=subscription_alerts['UNDER'],
                data_timestamp=data_timestamp,
                region=region
            )
        
        # ======================================================================
        # Post to overbought channel ONLY if there are NEW state changes
        # ======================================================================
        if has_new_overbought and overbought_ch and can_send_to_channel(overbought_ch, guild.me):
            messages_sent += await self._post_combined_alerts(
                channel=overbought_ch,
                condition='OVER',
                threshold=overbought_threshold,
                catalog_hits=new_overbought_catalog,
                subscription_alerts=subscription_alerts['OVER'],
                data_timestamp=data_timestamp,
                region=region
            )
        
        # ======================================================================
        # Update state for change detection (track current state, not just new)
        # ======================================================================
        await self.db.update_auto_scan_state(
            guild_id=guild.id,
            scan_date=today,
            condition='UNDER',
            tickers=current_oversold_tickers,
            increment_post_count=has_new_oversold
        )
        
        await self.db.update_auto_scan_state(
            guild_id=guild.id,
            scan_date=today,
            condition='OVER',
            tickers=current_overbought_tickers,
            increment_post_count=has_new_overbought
        )
        
        # ======================================================================
        # Always post status to changelog (spec section 3.2)
        # ======================================================================
        end_time = datetime.now(self.timezone)
        
        if changelog_ch and can_send_to_channel(changelog_ch, guild.me):
            # Separate failures for catalog vs subscriptions
            catalog_failed = [t for t, _ in failed_tickers if t in region_catalog_tickers]
            sub_tickers = set(s['ticker'] for s in guild_subscriptions)
            subscription_failed = [t for t, _ in failed_tickers if t in sub_tickers]
            
            await self._post_changelog_message(
                channel=changelog_ch,
                region=region,
                start_time=start_time,
                end_time=end_time,
                catalog_total=len(region_catalog_tickers),
                catalog_success=len([t for t in region_catalog_tickers if t in rsi_results]),
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
                posted_oversold=has_new_oversold,
                posted_overbought=has_new_overbought
            )

    async def _post_combined_alerts(
        self,
        channel: discord.TextChannel,
        condition: str,
        threshold: float,
        catalog_hits: Dict[str, Tuple[float, RSIResult]],
        subscription_alerts: List[Alert],
        data_timestamp: Optional[datetime],
        region: str
    ) -> int:
        """
        Post combined auto-scan + subscription alerts to a channel.
        Only called when there are NEW state changes.
        
        Returns:
            Number of messages sent
        """
        region_display = region.replace('_', '/').upper()
        
        if condition == 'UNDER':
            header = f"📉 **Auto-Scan: Oversold ({region_display})**\n"
            header += f"Threshold: RSI < {threshold}\n"
            sorted_catalog = sorted(catalog_hits.items(), key=lambda x: x[1][0])
        else:
            header = f"📈 **Auto-Scan: Overbought ({region_display})**\n"
            header += f"Threshold: RSI > {threshold}\n"
            sorted_catalog = sorted(catalog_hits.items(), key=lambda x: -x[1][0])
        
        if data_timestamp:
            header += f"Data as of: {data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}\n"
        header += "\n"
        
        lines = []
        
        # Add catalog hits (these are NEW entries only)
        if sorted_catalog:
            lines.append("**📊 Catalog Tickers (newly entered zone):**")
            for i, (ticker, (rsi_val, result)) in enumerate(sorted_catalog, 1):
                instrument = self.catalog.get_instrument(ticker)
                name = instrument.name if instrument else ticker
                url = instrument.tradingview_url if instrument else ""
                
                if url:
                    line = f"{i}) **{ticker}** — [{name}](<{url}>) — RSI14: **{rsi_val:.1f}**"
                else:
                    line = f"{i}) **{ticker}** — {name} — RSI14: **{rsi_val:.1f}**"
                lines.append(line)
            lines.append("")
        
        # Add subscription alerts
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
                    line = (
                        f"{i}) **{alert.ticker}** — [{alert.name}](<{url}>) — "
                        f"RSI{alert.period}: **{alert.rsi_value:.1f}** | "
                        f"Rule: **{rule_symbol} {alert.threshold}** | {persistence}"
                    )
                else:
                    line = (
                        f"{i}) **{alert.ticker}** — {alert.name} — "
                        f"RSI{alert.period}: **{alert.rsi_value:.1f}** | "
                        f"Rule: **{rule_symbol} {alert.threshold}** | {persistence}"
                    )
                lines.append(line)
        
        # Chunk and send
        content = header + "\n".join(lines)
        messages = chunk_message(content, max_length=DISCORD_SAFE_LIMIT)
        
        sent_count = 0
        for msg in messages:
            try:
                await channel.send(msg, suppress_embeds=True)
                sent_count += 1
            except discord.HTTPException as e:
                logger.error(f"Failed to send auto-scan alert: {e}")
        
        return sent_count

    async def _post_changelog_message(
        self,
        channel: discord.TextChannel,
        region: str,
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
        posted_overbought: bool
    ):
        """
        Post comprehensive auto-scan status to changelog channel.
        ALWAYS posted, even if there are zero alert hits.
        
        Includes per spec section 3.2:
        - Region window (EU/NA)
        - Start time and end time (duration)
        - Total tickers attempted (catalog + subscriptions)
        - Successful vs failed counts
        - List of failed tickers
        """
        region_display = region.replace('_', '/').upper()
        duration = (end_time - start_time).total_seconds()
        
        msg = f"🔄 **Auto-Scan Complete** ({region_display})\n\n"
        
        # Timing (spec requirement)
        msg += f"**⏱️ Timing:**\n"
        msg += f"• Start: {start_time.strftime('%H:%M:%S')}\n"
        msg += f"• End: {end_time.strftime('%H:%M:%S')}\n"
        msg += f"• Duration: {duration:.1f}s\n"
        
        if data_timestamp:
            msg += f"• Data timestamp: {data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}\n"
        msg += "\n"
        
        # Catalog scan results
        total_attempted = catalog_total + len(set(s for s in subscription_failed))
        catalog_failed_count = len(catalog_failed)
        
        msg += f"**📊 Catalog Scan:**\n"
        msg += f"• Tickers: {catalog_success}/{catalog_total} successful\n"
        
        if catalog_failed:
            failed_preview = catalog_failed[:5]
            msg += f"• ❌ Failed ({catalog_failed_count}): {', '.join(failed_preview)}"
            if catalog_failed_count > 5:
                msg += f" (+{catalog_failed_count - 5} more)"
            msg += "\n"
        msg += "\n"
        
        # Subscription evaluation
        subscription_failed_count = len(subscription_failed)
        
        msg += f"**🔔 Subscriptions:**\n"
        msg += f"• Total: {subscription_total}\n"
        msg += f"• Successful: {subscription_success}\n"
        
        if subscription_failed:
            failed_preview = subscription_failed[:5]
            msg += f"• ❌ Failed ({subscription_failed_count}): {', '.join(failed_preview)}"
            if subscription_failed_count > 5:
                msg += f" (+{subscription_failed_count - 5} more)"
            msg += "\n"
        msg += "\n"
        
        # Thresholds and hits with change detection info
        msg += f"**📈 Results:**\n"
        msg += f"• Oversold (< {oversold_threshold}): {oversold_total} total, **{oversold_new} new**"
        if oversold_sub_alerts > 0:
            msg += f", {oversold_sub_alerts} sub alerts"
        msg += "\n"
        
        msg += f"• Overbought (> {overbought_threshold}): {overbought_total} total, **{overbought_new} new**"
        if overbought_sub_alerts > 0:
            msg += f", {overbought_sub_alerts} sub alerts"
        msg += "\n\n"
        
        # Posted updates
        msg += f"**📬 Posted Updates:**\n"
        msg += f"• #{OVERSOLD_CHANNEL_NAME}: {'✅ Posted' if posted_oversold else '⏭️ No new hits'}\n"
        msg += f"• #{OVERBOUGHT_CHANNEL_NAME}: {'✅ Posted' if posted_overbought else '⏭️ No new hits'}\n"
        msg += f"• Messages sent: {messages_sent}\n"
        
        try:
            await channel.send(msg)
        except discord.HTTPException as e:
            logger.error(f"Failed to send changelog message: {e}")

    async def run_now(self, guild_id: Optional[int] = None) -> Dict:
        """
        Run auto-scan immediately for both regions, bypassing schedule restrictions.
        
        Args:
            guild_id: Optional specific guild to run for (runs all if None)
        
        Returns:
            Dict with run results
        """
        logger.info(f"Running manual auto-scan (run_now) for guild_id={guild_id}")
        
        start_time = datetime.now(self.timezone)
        
        # Run both regions
        await self._run_autoscan('europe')
        await self._run_autoscan('us_canada')
        
        end_time = datetime.now(self.timezone)
        duration = (end_time - start_time).total_seconds()
        
        return {
            "success": True,
            "message": f"Auto-scan completed for both regions in {duration:.1f}s",
            "duration_seconds": duration
        }

    async def _run_daily_check(self):
        """Execute the daily RSI check for all guilds (subscription-based only)."""
        start_time = datetime.now(self.timezone)
        logger.info(f"Starting daily RSI check at {start_time.isoformat()}")

        try:
            guild_ids = await self.db.get_all_guild_ids()
            enabled_guilds = []
            for guild_id in guild_ids:
                config = await self.db.get_or_create_guild_config(guild_id)
                if config.schedule_enabled:
                    enabled_guilds.append(guild_id)
                else:
                    logger.info(f"Skipping daily check for guild {guild_id}: schedule disabled")
            
            if not enabled_guilds:
                logger.info("No guilds with schedule enabled, skipping daily check")
                return

            subscriptions_data = await self.db.get_subscriptions_with_state()
            subscriptions_data = [s for s in subscriptions_data if s['guild_id'] in enabled_guilds]

            if not subscriptions_data:
                logger.info("No active subscriptions found for enabled guilds")
                return

            logger.info(f"Found {len(subscriptions_data)} active subscriptions")

            ticker_periods: Dict[str, List[int]] = {}
            guilds_with_subs: Set[int] = set()

            for sub in subscriptions_data:
                ticker = sub['ticker']
                period = sub['period']
                guild_id = sub['guild_id']

                if ticker not in ticker_periods:
                    ticker_periods[ticker] = []
                if period not in ticker_periods[ticker]:
                    ticker_periods[ticker].append(period)

                guilds_with_subs.add(guild_id)

            logger.info(
                f"Need RSI data for {len(ticker_periods)} tickers "
                f"across {len(guilds_with_subs)} guilds"
            )

            rsi_results = await self.rsi_calculator.calculate_rsi_for_tickers(
                ticker_periods
            )

            successful = sum(1 for r in rsi_results.values() if r.success)
            failed = len(rsi_results) - successful
            logger.info(f"RSI calculation: {successful} success, {failed} failed")

            for ticker, result in rsi_results.items():
                if not result.success:
                    logger.warning(f"Failed to get RSI for {ticker}: {result.error}")

            alerts_by_condition = await self.alert_engine.evaluate_subscriptions(
                rsi_results, dry_run=False
            )

            under_alerts = alerts_by_condition.get('UNDER', [])
            over_alerts = alerts_by_condition.get('OVER', [])
            total_alerts = len(under_alerts) + len(over_alerts)
            
            logger.info(f"Generated {total_alerts} alerts (UNDER: {len(under_alerts)}, OVER: {len(over_alerts)})")

            sent_count = 0
            error_count = 0

            alerts_by_guild: Dict[int, Dict[str, List]] = {}
            for alert in under_alerts:
                if alert.guild_id not in alerts_by_guild:
                    alerts_by_guild[alert.guild_id] = {'UNDER': [], 'OVER': []}
                alerts_by_guild[alert.guild_id]['UNDER'].append(alert)
            
            for alert in over_alerts:
                if alert.guild_id not in alerts_by_guild:
                    alerts_by_guild[alert.guild_id] = {'UNDER': [], 'OVER': []}
                alerts_by_guild[alert.guild_id]['OVER'].append(alert)

            for guild_id in guilds_with_subs:
                guild = self.bot.get_guild(guild_id)
                if not guild:
                    logger.warning(f"Guild {guild_id} not found")
                    continue

                oversold_ch, overbought_ch = get_alert_channels(guild)
                
                if not oversold_ch:
                    logger.warning(f"Channel #{OVERSOLD_CHANNEL_NAME} not found in guild {guild_id}")
                if not overbought_ch:
                    logger.warning(f"Channel #{OVERBOUGHT_CHANNEL_NAME} not found in guild {guild_id}")

                guild_alerts = alerts_by_guild.get(guild_id, {'UNDER': [], 'OVER': []})

                if oversold_ch and can_send_to_channel(oversold_ch, guild.me):
                    try:
                        if guild_alerts['UNDER']:
                            messages = format_alert_list(guild_alerts['UNDER'], 'UNDER')
                            for msg in messages:
                                await oversold_ch.send(msg, suppress_embeds=True)
                                sent_count += 1
                    except discord.Forbidden:
                        logger.error(f"Permission denied sending to #{OVERSOLD_CHANNEL_NAME} in guild {guild_id}")
                        error_count += 1
                    except Exception as e:
                        logger.error(f"Error sending to #{OVERSOLD_CHANNEL_NAME} in guild {guild_id}: {e}")
                        error_count += 1

                if overbought_ch and can_send_to_channel(overbought_ch, guild.me):
                    try:
                        if guild_alerts['OVER']:
                            messages = format_alert_list(guild_alerts['OVER'], 'OVER')
                            for msg in messages:
                                await overbought_ch.send(msg, suppress_embeds=True)
                                sent_count += 1
                    except discord.Forbidden:
                        logger.error(f"Permission denied sending to #{OVERBOUGHT_CHANNEL_NAME} in guild {guild_id}")
                        error_count += 1
                    except Exception as e:
                        logger.error(f"Error sending to #{OVERBOUGHT_CHANNEL_NAME} in guild {guild_id}: {e}")
                        error_count += 1

            end_time = datetime.now(self.timezone)
            duration = (end_time - start_time).total_seconds()

            logger.info(
                f"Daily RSI check complete in {duration:.1f}s - "
                f"Tickers: {successful}/{len(ticker_periods)} | "
                f"Subscriptions: {len(subscriptions_data)} | "
                f"Alerts: {total_alerts} | "
                f"Messages sent: {sent_count} | "
                f"Errors: {error_count}"
            )
            
            # Cleanup old states
            await self.db.cleanup_old_auto_scan_states(days_to_keep=7)

        except Exception as e:
            logger.error(f"Error in daily RSI check: {e}", exc_info=True)

    async def run_for_guild(self, guild_id: int, dry_run: bool = False) -> dict:
        """Run RSI check for a specific guild."""
        logger.info(f"Running RSI check for guild {guild_id} (dry_run={dry_run})")

        subs = await self.db.get_subscriptions_by_guild(
            guild_id=guild_id, enabled_only=True
        )

        if not subs:
            return {
                "success": True,
                "message": "No active subscriptions",
                "subscriptions": 0,
                "tickers": 0,
                "alerts": 0
            }

        ticker_periods: Dict[str, List[int]] = {}
        for sub in subs:
            if sub.ticker not in ticker_periods:
                ticker_periods[sub.ticker] = []
            if sub.period not in ticker_periods[sub.ticker]:
                ticker_periods[sub.ticker].append(sub.period)

        rsi_results = await self.rsi_calculator.calculate_rsi_for_tickers(
            ticker_periods
        )

        successful = sum(1 for r in rsi_results.values() if r.success)
        failed = len(rsi_results) - successful

        alerts_by_condition = await self.alert_engine.evaluate_subscriptions(
            rsi_results, dry_run=dry_run
        )

        under_alerts = alerts_by_condition.get('UNDER', [])
        over_alerts = alerts_by_condition.get('OVER', [])
        total_alerts = len(under_alerts) + len(over_alerts)

        return {
            "success": True,
            "subscriptions": len(subs),
            "tickers_requested": len(ticker_periods),
            "tickers_success": successful,
            "tickers_failed": failed,
            "alerts": total_alerts,
            "under_alerts": under_alerts,
            "over_alerts": over_alerts,
            "rsi_results": rsi_results
        }

    def stop(self):
        """Stop the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("RSI scheduler stopped")


async def setup_scheduler(bot):
    """Set up the scheduler for a bot instance."""
    scheduler = RSIScheduler(bot)
    await scheduler.start()
    return scheduler
