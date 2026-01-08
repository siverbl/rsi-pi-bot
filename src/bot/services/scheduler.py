"""
Scheduler module for RSI Discord Bot.
Handles scheduled RSI checks including:
- Daily subscription-based alerts
- Hourly automatic scans for all tickers
"""
import logging
from datetime import datetime, date
from typing import Dict, List, Set, Optional, Tuple

import discord
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from bot.config import (
    DEFAULT_TIMEZONE, DEFAULT_SCHEDULE_TIME,
    OVERSOLD_CHANNEL_NAME, OVERBOUGHT_CHANNEL_NAME, CHANGELOG_CHANNEL_NAME,
    EUROPEAN_SUFFIXES, US_CANADA_SUFFIXES,
    EUROPE_MARKET_START_HOUR, EUROPE_MARKET_START_MINUTE,
    EUROPE_MARKET_END_HOUR, EUROPE_MARKET_END_MINUTE,
    US_MARKET_START_HOUR, US_MARKET_START_MINUTE,
    US_MARKET_END_HOUR, US_MARKET_END_MINUTE,
    DISCORD_SAFE_LIMIT
)
from bot.repositories.database import Database, AutoScanState
from bot.services.market_data.rsi_calculator import RSICalculator
from bot.cogs.alert_engine import AlertEngine, format_alert_list
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
    
    # Check European suffixes
    for suffix in EUROPEAN_SUFFIXES:
        if ticker_upper.endswith(suffix):
            return 'europe'
    
    # Check US/Canada suffixes
    for suffix in US_CANADA_SUFFIXES:
        if ticker_upper.endswith(suffix):
            return 'us_canada'
    
    # No suffix = US stock
    if '.' not in ticker_upper:
        return 'us_canada'
    
    return 'other'


def is_market_hours(region: str, tz: pytz.timezone) -> bool:
    """
    Check if current time is within market hours for a region.
    
    Args:
        region: 'europe' or 'us_canada'
        tz: Timezone to use for checking (Europe/Oslo)
    
    Returns:
        True if within market hours
    """
    now = datetime.now(tz)
    current_time = now.hour * 60 + now.minute
    
    if region == 'europe':
        start = EUROPE_MARKET_START_HOUR * 60 + EUROPE_MARKET_START_MINUTE
        end = EUROPE_MARKET_END_HOUR * 60 + EUROPE_MARKET_END_MINUTE
    elif region == 'us_canada':
        start = US_MARKET_START_HOUR * 60 + US_MARKET_START_MINUTE
        end = US_MARKET_END_HOUR * 60 + US_MARKET_END_MINUTE
    else:
        return False
    
    return start <= current_time <= end


class RSIScheduler:
    """
    Manages scheduled RSI check jobs including:
    - Daily subscription-based checks
    - Hourly automatic scans for all tickers
    """

    def __init__(self, bot):
        self.bot = bot
        self.db: Database = bot.db
        self.rsi_calculator = RSICalculator()
        self.alert_engine = AlertEngine(self.db)
        self.scheduler = AsyncIOScheduler()
        self.timezone = pytz.timezone(DEFAULT_TIMEZONE)
        self.catalog = get_catalog()

        # Track scheduled jobs per guild
        self._guild_jobs: Dict[int, str] = {}

    async def start(self):
        """Start the scheduler and set up jobs."""
        logger.info("Starting RSI scheduler...")

        # Add daily subscription check job
        self._add_daily_subscription_job()
        
        # Add hourly auto-scan jobs
        self._add_hourly_autoscan_jobs()

        self.scheduler.start()
        logger.info("RSI scheduler started")

    def _add_daily_subscription_job(self):
        """Add the default daily subscription check job."""
        try:
            hour, minute = map(int, DEFAULT_SCHEDULE_TIME.split(":"))
        except ValueError:
            hour, minute = 18, 30

        trigger = CronTrigger(
            hour=hour,
            minute=minute,
            timezone=self.timezone
        )

        self.scheduler.add_job(
            self._run_daily_check,
            trigger=trigger,
            id="daily_rsi_check",
            name="Daily RSI Check",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True
        )

        logger.info(f"Scheduled daily RSI check at {hour:02d}:{minute:02d} {DEFAULT_TIMEZONE}")
    
    def _add_hourly_autoscan_jobs(self):
        """Add hourly auto-scan jobs for both market regions."""
        # Europe market hours: 09:30 - 17:30
        # Run at :30 past each hour within window
        for hour in range(EUROPE_MARKET_START_HOUR, EUROPE_MARKET_END_HOUR + 1):
            trigger = CronTrigger(
                hour=hour,
                minute=30,
                timezone=self.timezone,
                day_of_week='mon-fri'  # Only weekdays
            )
            self.scheduler.add_job(
                self._run_europe_autoscan,
                trigger=trigger,
                id=f"europe_autoscan_{hour}",
                name=f"Europe Auto-Scan {hour}:30",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
                replace_existing=True,
            )
        
        # US/Canada market hours: 15:30 - 22:30 (Europe/Oslo time)
        for hour in range(US_MARKET_START_HOUR, US_MARKET_END_HOUR + 1):
            trigger = CronTrigger(
                hour=hour,
                minute=30,
                timezone=self.timezone,
                day_of_week='mon-fri'
            )
            self.scheduler.add_job(
                self._run_us_autoscan,
                trigger=trigger,
                id=f"us_autoscan_{hour}",
                name=f"US/Canada Auto-Scan {hour}:30",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
                replace_existing=True,
            )
        
        logger.info("Scheduled hourly auto-scan jobs for Europe and US/Canada markets")

    async def _run_europe_autoscan(self):
        """Run automatic RSI scan for European tickers."""
        await self._run_autoscan('europe')
    
    async def _run_us_autoscan(self):
        """Run automatic RSI scan for US/Canada tickers."""
        await self._run_autoscan('us_canada')

    async def _run_autoscan(self, region: str):
        """
        Run automatic RSI scan for a specific region.
        
        This scan:
        1. Gets all tickers from catalog for the region
        2. Fetches RSI14 for all tickers
        3. Filters by admin-set thresholds
        4. Posts to channels only if the set of qualifying tickers changed
        5. Always posts status to #server-changelog
        """
        start_time = datetime.now(self.timezone)
        today = start_time.strftime("%Y-%m-%d")
        
        logger.info(f"Starting {region} auto-scan at {start_time.isoformat()}")
        
        # Get all tickers and filter by region
        all_tickers = self.catalog.get_all_tickers()
        region_tickers = [t for t in all_tickers if classify_ticker_region(t) == region]
        
        if not region_tickers:
            logger.info(f"No {region} tickers in catalog, skipping auto-scan")
            return
        
        logger.info(f"Auto-scanning {len(region_tickers)} {region} tickers")
        
        # Fetch RSI for all tickers
        ticker_periods = {t: [14] for t in region_tickers}
        rsi_results = await self.rsi_calculator.calculate_rsi_for_tickers(ticker_periods)
        
        # Count successes and failures
        successful = sum(1 for r in rsi_results.values() if r.success)
        failed = len(rsi_results) - successful
        failed_tickers = [t for t, r in rsi_results.items() if not r.success][:5]  # First 5 failures
        
        # Calculate batch count (approximate; TradingView batch size)
        from bot.config import TV_BATCH_SIZE
        batch_count = (len(region_tickers) + TV_BATCH_SIZE - 1) // TV_BATCH_SIZE
        
        # Get data timestamp from first successful result
        data_timestamp = None
        for result in rsi_results.values():
            if result.success and result.data_timestamp:
                data_timestamp = result.data_timestamp
                break
        
        # Process each guild
        guild_ids = await self.db.get_all_guild_ids()
        for guild_id in guild_ids:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue
            
            await self._process_guild_autoscan(
                guild=guild,
                region=region,
                today=today,
                rsi_results=rsi_results,
                successful=successful,
                failed=failed,
                failed_tickers=failed_tickers,
                batch_count=batch_count,
                data_timestamp=data_timestamp
            )
        
        end_time = datetime.now(self.timezone)
        duration = (end_time - start_time).total_seconds()
        logger.info(f"{region.title()} auto-scan complete in {duration:.1f}s")

    async def _process_guild_autoscan(
        self,
        guild: discord.Guild,
        region: str,
        today: str,
        rsi_results: Dict,
        successful: int,
        failed: int,
        failed_tickers: List[str],
        batch_count: int,
        data_timestamp: Optional[datetime]
    ):
        """Process auto-scan results for a single guild."""
        config = await self.db.get_or_create_guild_config(guild.id)
        
        oversold_threshold = config.auto_oversold_threshold
        overbought_threshold = config.auto_overbought_threshold
        
        # Find tickers meeting thresholds
        oversold_tickers: Set[str] = set()
        overbought_tickers: Set[str] = set()
        oversold_data: Dict = {}
        overbought_data: Dict = {}
        
        for ticker, result in rsi_results.items():
            if not result.success or not result.rsi_values:
                continue
            
            rsi_14 = result.rsi_values.get(14)
            if rsi_14 is None:
                continue
            
            if rsi_14 < oversold_threshold:
                oversold_tickers.add(ticker)
                oversold_data[ticker] = (rsi_14, result)
            
            if rsi_14 > overbought_threshold:
                overbought_tickers.add(ticker)
                overbought_data[ticker] = (rsi_14, result)
        
        # Get previous state
        prev_oversold_state = await self.db.get_auto_scan_state(guild.id, today, 'UNDER')
        prev_overbought_state = await self.db.get_auto_scan_state(guild.id, today, 'OVER')
        
        # Determine if we should post (first scan of day OR set changed)
        post_oversold = False
        post_overbought = False
        
        if prev_oversold_state is None or prev_oversold_state.post_count == 0:
            # First scan of day - always post
            post_oversold = len(oversold_tickers) > 0
        elif oversold_tickers != prev_oversold_state.last_tickers:
            # Set changed - post
            post_oversold = True
        
        if prev_overbought_state is None or prev_overbought_state.post_count == 0:
            post_overbought = len(overbought_tickers) > 0
        elif overbought_tickers != prev_overbought_state.last_tickers:
            post_overbought = True
        
        # Get channels
        oversold_ch, overbought_ch = get_alert_channels(guild)
        changelog_ch = get_changelog_channel(guild)
        
        # Post to alert channels if needed
        if post_oversold and oversold_ch and can_send_to_channel(oversold_ch, guild.me):
            await self._post_autoscan_alerts(
                channel=oversold_ch,
                condition='UNDER',
                threshold=oversold_threshold,
                ticker_data=oversold_data,
                data_timestamp=data_timestamp,
                region=region
            )
            await self.db.update_auto_scan_state(
                guild.id, today, 'UNDER', oversold_tickers, increment_post_count=True
            )
        else:
            await self.db.update_auto_scan_state(
                guild.id, today, 'UNDER', oversold_tickers, increment_post_count=False
            )
        
        if post_overbought and overbought_ch and can_send_to_channel(overbought_ch, guild.me):
            await self._post_autoscan_alerts(
                channel=overbought_ch,
                condition='OVER',
                threshold=overbought_threshold,
                ticker_data=overbought_data,
                data_timestamp=data_timestamp,
                region=region
            )
            await self.db.update_auto_scan_state(
                guild.id, today, 'OVER', overbought_tickers, increment_post_count=True
            )
        else:
            await self.db.update_auto_scan_state(
                guild.id, today, 'OVER', overbought_tickers, increment_post_count=False
            )
        
        # Always post status to changelog
        if changelog_ch and can_send_to_channel(changelog_ch, guild.me):
            await self._post_autoscan_status(
                channel=changelog_ch,
                region=region,
                total_tickers=len(rsi_results),
                successful=successful,
                failed=failed,
                failed_tickers=failed_tickers,
                batch_count=batch_count,
                oversold_count=len(oversold_tickers),
                overbought_count=len(overbought_tickers),
                posted_oversold=post_oversold,
                posted_overbought=post_overbought,
                oversold_threshold=oversold_threshold,
                overbought_threshold=overbought_threshold,
                data_timestamp=data_timestamp
            )

    async def _post_autoscan_alerts(
        self,
        channel: discord.TextChannel,
        condition: str,
        threshold: float,
        ticker_data: Dict[str, Tuple[float, any]],
        data_timestamp: Optional[datetime],
        region: str
    ):
        """Post auto-scan alerts to a channel."""
        if condition == 'UNDER':
            header = f"📉 **RSI Auto-Scan: Oversold ({region.replace('_', '/').title()})**\n"
            header += f"Threshold: RSI < {threshold}\n"
            # Sort by RSI ascending (lowest first)
            sorted_tickers = sorted(ticker_data.items(), key=lambda x: x[1][0])
        else:
            header = f"📈 **RSI Auto-Scan: Overbought ({region.replace('_', '/').title()})**\n"
            header += f"Threshold: RSI > {threshold}\n"
            # Sort by RSI descending (highest first)
            sorted_tickers = sorted(ticker_data.items(), key=lambda x: -x[1][0])
        
        if data_timestamp:
            header += f"Data as of: {data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}\n"
        header += "\n"
        
        # Build list of alerts
        lines = []
        for i, (ticker, (rsi_val, result)) in enumerate(sorted_tickers, 1):
            instrument = self.catalog.get_instrument(ticker)
            name = instrument.name if instrument else ticker
            url = instrument.tradingview_url if instrument else ""
            
            if url:
                line = f"{i}) **{ticker}** — [{name}]({url}) — RSI14: **{rsi_val:.1f}**"
            else:
                line = f"{i}) **{ticker}** — {name} — RSI14: **{rsi_val:.1f}**"
            lines.append(line)
        
        # Chunk and send
        content = header + "\n".join(lines)
        messages = chunk_message(content, max_length=DISCORD_SAFE_LIMIT)
        
        for msg in messages:
            try:
                await channel.send(msg)
            except discord.HTTPException as e:
                logger.error(f"Failed to send auto-scan alert: {e}")

    async def _post_autoscan_status(
        self,
        channel: discord.TextChannel,
        region: str,
        total_tickers: int,
        successful: int,
        failed: int,
        failed_tickers: List[str],
        batch_count: int,
        oversold_count: int,
        overbought_count: int,
        posted_oversold: bool,
        posted_overbought: bool,
        oversold_threshold: float,
        overbought_threshold: float,
        data_timestamp: Optional[datetime]
    ):
        """Post auto-scan status to changelog channel."""
        now = datetime.now(self.timezone)
        
        msg = f"🔄 **Auto-Scan Status** ({region.replace('_', '/').title()})\n"
        msg += f"Time: {now.strftime('%Y-%m-%d %H:%M %Z')}\n\n"
        msg += f"**Scan Results:**\n"
        msg += f"• Tickers checked: {total_tickers}\n"
        msg += f"• Batches: {batch_count}\n"
        msg += f"• Success: {successful} | Errors: {failed}\n"
        
        if failed_tickers:
            msg += f"• Failed examples: {', '.join(failed_tickers)}\n"
        
        if data_timestamp:
            msg += f"• Data timestamp: {data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}\n"
        
        msg += f"\n**Thresholds:**\n"
        msg += f"• Oversold: < {oversold_threshold} ({oversold_count} tickers)\n"
        msg += f"• Overbought: > {overbought_threshold} ({overbought_count} tickers)\n"
        
        msg += f"\n**Posted Updates:**\n"
        msg += f"• #{OVERSOLD_CHANNEL_NAME}: {'✅ Yes' if posted_oversold else '⏭️ No change'}\n"
        msg += f"• #{OVERBOUGHT_CHANNEL_NAME}: {'✅ Yes' if posted_overbought else '⏭️ No change'}\n"
        
        try:
            await channel.send(msg)
        except discord.HTTPException as e:
            logger.error(f"Failed to send auto-scan status: {e}")

    async def _run_daily_check(self):
        """Execute the daily RSI check for all guilds (subscription-based)."""
        start_time = datetime.now(self.timezone)
        logger.info(f"Starting daily RSI check at {start_time.isoformat()}")

        try:
            # Step 1: Load all active subscriptions
            subscriptions_data = await self.db.get_subscriptions_with_state()

            if not subscriptions_data:
                logger.info("No active subscriptions found")
                return

            logger.info(f"Found {len(subscriptions_data)} active subscriptions")

            # Step 2: Determine unique tickers and periods needed
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

            # Step 3: Fetch historical data and calculate RSI
            rsi_results = await self.rsi_calculator.calculate_rsi_for_tickers(
                ticker_periods
            )

            successful = sum(1 for r in rsi_results.values() if r.success)
            failed = len(rsi_results) - successful
            logger.info(f"RSI calculation: {successful} success, {failed} failed")

            # Log failed tickers
            for ticker, result in rsi_results.items():
                if not result.success:
                    logger.warning(f"Failed to get RSI for {ticker}: {result.error}")

            # Step 4: Evaluate subscriptions and generate alerts
            alerts_by_condition = await self.alert_engine.evaluate_subscriptions(
                rsi_results, dry_run=False
            )

            under_alerts = alerts_by_condition.get('UNDER', [])
            over_alerts = alerts_by_condition.get('OVER', [])
            total_alerts = len(under_alerts) + len(over_alerts)
            
            logger.info(f"Generated {total_alerts} alerts (UNDER: {len(under_alerts)}, OVER: {len(over_alerts)})")

            # Step 5: Send alerts to channels (grouped by guild)
            sent_count = 0
            error_count = 0

            # Group alerts by guild
            alerts_by_guild: Dict[int, Dict[str, List]] = {}
            for alert in under_alerts:
                if alert.guild_id not in alerts_by_guild:
                    alerts_by_guild[alert.guild_id] = {'UNDER': [], 'OVER': []}
                alerts_by_guild[alert.guild_id]['UNDER'].append(alert)
            
            for alert in over_alerts:
                if alert.guild_id not in alerts_by_guild:
                    alerts_by_guild[alert.guild_id] = {'UNDER': [], 'OVER': []}
                alerts_by_guild[alert.guild_id]['OVER'].append(alert)

            # Process each guild
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

                # Send UNDER alerts to oversold channel
                if oversold_ch and can_send_to_channel(oversold_ch, guild.me):
                    try:
                        if guild_alerts['UNDER']:
                            messages = format_alert_list(guild_alerts['UNDER'], 'UNDER')
                            for msg in messages:
                                await oversold_ch.send(msg)
                                sent_count += 1
                    except discord.Forbidden:
                        logger.error(f"Permission denied sending to #{OVERSOLD_CHANNEL_NAME} in guild {guild_id}")
                        error_count += 1
                    except Exception as e:
                        logger.error(f"Error sending to #{OVERSOLD_CHANNEL_NAME} in guild {guild_id}: {e}")
                        error_count += 1

                # Send OVER alerts to overbought channel
                if overbought_ch and can_send_to_channel(overbought_ch, guild.me):
                    try:
                        if guild_alerts['OVER']:
                            messages = format_alert_list(guild_alerts['OVER'], 'OVER')
                            for msg in messages:
                                await overbought_ch.send(msg)
                                sent_count += 1
                    except discord.Forbidden:
                        logger.error(f"Permission denied sending to #{OVERBOUGHT_CHANNEL_NAME} in guild {guild_id}")
                        error_count += 1
                    except Exception as e:
                        logger.error(f"Error sending to #{OVERBOUGHT_CHANNEL_NAME} in guild {guild_id}: {e}")
                        error_count += 1

            # Step 6: Log completion
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
            
            # Cleanup old auto-scan states
            await self.db.cleanup_old_auto_scan_states(days_to_keep=7)

        except Exception as e:
            logger.error(f"Error in daily RSI check: {e}", exc_info=True)

    async def run_for_guild(self, guild_id: int, dry_run: bool = False) -> dict:
        """Run RSI check for a specific guild."""
        logger.info(f"Running RSI check for guild {guild_id} (dry_run={dry_run})")

        # Get subscriptions for this guild
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

        # Determine unique tickers and periods
        ticker_periods: Dict[str, List[int]] = {}
        for sub in subs:
            if sub.ticker not in ticker_periods:
                ticker_periods[sub.ticker] = []
            if sub.period not in ticker_periods[sub.ticker]:
                ticker_periods[sub.ticker].append(sub.period)

        # Calculate RSI
        rsi_results = await self.rsi_calculator.calculate_rsi_for_tickers(
            ticker_periods
        )

        successful = sum(1 for r in rsi_results.values() if r.success)
        failed = len(rsi_results) - successful

        # Evaluate subscriptions
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
            self.scheduler.shutdown()
            logger.info("RSI scheduler stopped")


async def setup_scheduler(bot):
    """Set up the scheduler for a bot instance."""
    scheduler = RSIScheduler(bot)
    await scheduler.start()
    return scheduler
