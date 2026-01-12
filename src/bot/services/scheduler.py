"""
Scheduler module for RSI Discord Bot.
Handles scheduled RSI checks including:
- Daily subscription-based alerts
- Hourly automatic scans for all tickers + subscriptions
- Schedule enable/disable functionality

Auto-Scan Specification:
- Runs at minute :30 on weekdays (Mon-Fri) during market hours
- European window: 09:30-17:30 Europe/Oslo
- US/Canada window: 15:30-22:30 Europe/Oslo
- Evaluates both catalog tickers AND manual subscriptions
- Posts to #rsi-oversold and #rsi-overbought only when there are hits
- Reports all failures to #server-changelog
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
from bot.services.market_data.rsi_calculator import RSICalculator, RSIResult
from bot.cogs.alert_engine import AlertEngine, Alert, format_alert_list, format_no_alerts_message
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
    - Hourly automatic scans for all tickers + subscriptions
    - Schedule enable/disable per guild
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
        """
        Add hourly auto-scan jobs for both market regions.
        
        Schedule:
        - Europe: 09:30, 10:30, 11:30, 12:30, 13:30, 14:30, 15:30, 16:30, 17:30
        - US/Canada: 15:30, 16:30, 17:30, 18:30, 19:30, 20:30, 21:30, 22:30
        - Weekdays only (Mon-Fri)
        """
        # Europe market hours: 09:30 - 17:30 (hours 9-17 at :30)
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
        
        # US/Canada market hours: 15:30 - 22:30 (hours 15-22 at :30)
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
        
        logger.info(
            f"Scheduled auto-scan jobs: "
            f"Europe {EUROPE_MARKET_START_HOUR}:30-{EUROPE_MARKET_END_HOUR}:30, "
            f"US/Canada {US_MARKET_START_HOUR}:30-{US_MARKET_END_HOUR}:30 (weekdays)"
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
        
        This scan:
        1. Gets all tickers from catalog for the region
        2. Gets all manual subscriptions for the region
        3. Fetches RSI14 for all unique tickers (combined)
        4. Filters catalog by admin-set thresholds
        5. Evaluates subscriptions via AlertEngine
        6. Posts to channels ONLY if there are hits
        7. Always posts status to #server-changelog with failure details
        """
        start_time = datetime.now(self.timezone)
        today = start_time.strftime("%Y-%m-%d")
        
        logger.info(f"Starting {region} auto-scan at {start_time.isoformat()}")
        
        # ======================================================================
        # Step 1: Get catalog tickers for this region
        # ======================================================================
        all_catalog_tickers = self.catalog.get_all_tickers()
        region_catalog_tickers = [t for t in all_catalog_tickers if classify_ticker_region(t) == region]
        
        logger.info(f"Catalog: {len(region_catalog_tickers)} {region} tickers")
        
        # ======================================================================
        # Step 2: Get subscription tickers for this region
        # ======================================================================
        all_subscriptions = await self.db.get_subscriptions_with_state()
        region_subscription_tickers: Set[str] = set()
        
        for sub in all_subscriptions:
            ticker = sub['ticker']
            if classify_ticker_region(ticker) == region:
                region_subscription_tickers.add(ticker)
        
        logger.info(f"Subscriptions: {len(region_subscription_tickers)} unique {region} tickers")
        
        # ======================================================================
        # Step 3: Combine and fetch RSI for all unique tickers
        # ======================================================================
        all_tickers = list(set(region_catalog_tickers) | region_subscription_tickers)
        
        if not all_tickers:
            logger.info(f"No {region} tickers to scan (catalog or subscriptions), skipping")
            return
        
        logger.info(f"Fetching RSI for {len(all_tickers)} unique {region} tickers")
        
        ticker_periods = {t: [14] for t in all_tickers}
        rsi_results = await self.rsi_calculator.calculate_rsi_for_tickers(ticker_periods)
        
        # ======================================================================
        # Step 4: Track failures SEPARATELY for catalog vs subscriptions
        # ======================================================================
        catalog_failed: List[str] = []
        catalog_success = 0
        
        for ticker in region_catalog_tickers:
            result = rsi_results.get(ticker)
            if result and result.success:
                catalog_success += 1
            else:
                catalog_failed.append(ticker)
        
        subscription_failed: List[str] = []
        subscription_success = 0
        
        for ticker in region_subscription_tickers:
            result = rsi_results.get(ticker)
            if result and result.success:
                subscription_success += 1
            else:
                subscription_failed.append(ticker)
        
        logger.info(
            f"RSI fetch results - Catalog: {catalog_success}/{len(region_catalog_tickers)} success, "
            f"Subscriptions: {subscription_success}/{len(region_subscription_tickers)} success"
        )
        
        # Get data timestamp from first successful result
        data_timestamp = None
        for result in rsi_results.values():
            if result.success and result.data_timestamp:
                data_timestamp = result.data_timestamp
                break
        
        # Calculate batch count (approximate)
        from bot.config import TV_BATCH_SIZE
        batch_count = (len(all_tickers) + TV_BATCH_SIZE - 1) // TV_BATCH_SIZE
        
        # ======================================================================
        # Step 5: Process each guild
        # ======================================================================
        guild_ids = await self.db.get_all_guild_ids()
        for guild_id in guild_ids:
            guild = self.bot.get_guild(guild_id)
            if not guild:
                continue
            
            # Check if schedule is enabled for this guild
            config = await self.db.get_or_create_guild_config(guild_id)
            if not config.schedule_enabled:
                logger.info(f"Skipping auto-scan for guild {guild_id}: schedule disabled")
                continue
            
            await self._process_guild_autoscan(
                guild=guild,
                region=region,
                today=today,
                rsi_results=rsi_results,
                region_catalog_tickers=region_catalog_tickers,
                region_subscription_tickers=region_subscription_tickers,
                all_subscriptions=all_subscriptions,
                catalog_success=catalog_success,
                catalog_failed=catalog_failed,
                subscription_success=subscription_success,
                subscription_failed=subscription_failed,
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
        rsi_results: Dict[str, RSIResult],
        region_catalog_tickers: List[str],
        region_subscription_tickers: Set[str],
        all_subscriptions: List[Dict],
        catalog_success: int,
        catalog_failed: List[str],
        subscription_success: int,
        subscription_failed: List[str],
        batch_count: int,
        data_timestamp: Optional[datetime]
    ):
        """
        Process auto-scan results for a single guild.
        
        - Evaluates catalog tickers against admin thresholds
        - Evaluates subscriptions via AlertEngine
        - Posts to alert channels ONLY if there are hits
        - Always posts status to #server-changelog
        """
        config = await self.db.get_or_create_guild_config(guild.id)
        
        oversold_threshold = config.auto_oversold_threshold
        overbought_threshold = config.auto_overbought_threshold
        
        # ======================================================================
        # Filter catalog tickers by threshold
        # ======================================================================
        oversold_catalog: Dict[str, Tuple[float, RSIResult]] = {}
        overbought_catalog: Dict[str, Tuple[float, RSIResult]] = {}
        
        for ticker in region_catalog_tickers:
            result = rsi_results.get(ticker)
            if not result or not result.success or not result.rsi_values:
                continue
            
            rsi_14 = result.rsi_values.get(14)
            if rsi_14 is None:
                continue
            
            if rsi_14 < oversold_threshold:
                oversold_catalog[ticker] = (rsi_14, result)
            
            if rsi_14 > overbought_threshold:
                overbought_catalog[ticker] = (rsi_14, result)
        
        # ======================================================================
        # Evaluate subscriptions for this guild via AlertEngine
        # ======================================================================
        # Filter subscriptions for this guild and region
        guild_subscriptions = [
            s for s in all_subscriptions 
            if s['guild_id'] == guild.id and classify_ticker_region(s['ticker']) == region
        ]
        
        # Evaluate subscriptions (handles crossing logic, cooldown, etc.)
        subscription_alerts = await self.alert_engine.evaluate_subscriptions(
            rsi_results=rsi_results,
            dry_run=False
        )
        
        # Filter alerts to only those for this guild and region
        subscription_alerts_under: List[Alert] = [
            a for a in subscription_alerts.get('UNDER', [])
            if a.guild_id == guild.id and classify_ticker_region(a.ticker) == region
        ]
        subscription_alerts_over: List[Alert] = [
            a for a in subscription_alerts.get('OVER', [])
            if a.guild_id == guild.id and classify_ticker_region(a.ticker) == region
        ]
        
        # Track subscription evaluation failures (tickers that had no RSI data)
        subscription_eval_failed = [
            s['ticker'] for s in guild_subscriptions 
            if s['ticker'] in subscription_failed
        ]
        
        # ======================================================================
        # Determine if we have any hits
        # ======================================================================
        has_oversold_hits = len(oversold_catalog) > 0 or len(subscription_alerts_under) > 0
        has_overbought_hits = len(overbought_catalog) > 0 or len(subscription_alerts_over) > 0
        
        # Get channels
        oversold_ch, overbought_ch = get_alert_channels(guild)
        changelog_ch = get_changelog_channel(guild)
        
        messages_sent = 0
        
        # ======================================================================
        # Post to oversold channel ONLY if there are hits
        # ======================================================================
        if has_oversold_hits and oversold_ch and can_send_to_channel(oversold_ch, guild.me):
            messages_sent += await self._post_combined_alerts(
                channel=oversold_ch,
                condition='UNDER',
                threshold=oversold_threshold,
                catalog_hits=oversold_catalog,
                subscription_alerts=subscription_alerts_under,
                data_timestamp=data_timestamp,
                region=region
            )
        
        # ======================================================================
        # Post to overbought channel ONLY if there are hits
        # ======================================================================
        if has_overbought_hits and overbought_ch and can_send_to_channel(overbought_ch, guild.me):
            messages_sent += await self._post_combined_alerts(
                channel=overbought_ch,
                condition='OVER',
                threshold=overbought_threshold,
                catalog_hits=overbought_catalog,
                subscription_alerts=subscription_alerts_over,
                data_timestamp=data_timestamp,
                region=region
            )
        
        # ======================================================================
        # Always post status to changelog
        # ======================================================================
        if changelog_ch and can_send_to_channel(changelog_ch, guild.me):
            await self._post_autoscan_status(
                channel=changelog_ch,
                region=region,
                catalog_total=len(region_catalog_tickers),
                catalog_success=catalog_success,
                catalog_failed=catalog_failed,
                subscription_total=len(guild_subscriptions),
                subscription_success=subscription_success,
                subscription_eval_failed=subscription_eval_failed,
                oversold_catalog_count=len(oversold_catalog),
                oversold_sub_count=len(subscription_alerts_under),
                overbought_catalog_count=len(overbought_catalog),
                overbought_sub_count=len(subscription_alerts_over),
                has_oversold_hits=has_oversold_hits,
                has_overbought_hits=has_overbought_hits,
                oversold_threshold=oversold_threshold,
                overbought_threshold=overbought_threshold,
                data_timestamp=data_timestamp,
                batch_count=batch_count
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
        
        Returns:
            Number of messages sent
        """
        region_display = region.replace('_', '/').title()
        
        if condition == 'UNDER':
            header = f"📉 **Auto-Scan: Oversold ({region_display})**\n"
            header += f"Threshold: RSI < {threshold}\n"
            # Sort catalog by RSI ascending (lowest first)
            sorted_catalog = sorted(catalog_hits.items(), key=lambda x: x[1][0])
        else:
            header = f"📈 **Auto-Scan: Overbought ({region_display})**\n"
            header += f"Threshold: RSI > {threshold}\n"
            # Sort catalog by RSI descending (highest first)
            sorted_catalog = sorted(catalog_hits.items(), key=lambda x: -x[1][0])
        
        if data_timestamp:
            header += f"Data as of: {data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}\n"
        header += "\n"
        
        lines = []
        
        # Add catalog hits
        if sorted_catalog:
            lines.append("**Catalog Tickers:**")
            for i, (ticker, (rsi_val, result)) in enumerate(sorted_catalog, 1):
                instrument = self.catalog.get_instrument(ticker)
                name = instrument.name if instrument else ticker
                url = instrument.tradingview_url if instrument else ""
                
                if url:
                    line = f"{i}) **{ticker}** — [{name}](<{url}>) — RSI14: **{rsi_val:.1f}**"
                else:
                    line = f"{i}) **{ticker}** — {name} — RSI14: **{rsi_val:.1f}**"
                lines.append(line)
            lines.append("")  # Blank line separator
        
        # Add subscription alerts
        if subscription_alerts:
            lines.append("🔔 **Subscription Alerts:**")
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

    async def _post_autoscan_status(
        self,
        channel: discord.TextChannel,
        region: str,
        catalog_total: int,
        catalog_success: int,
        catalog_failed: List[str],
        subscription_total: int,
        subscription_success: int,
        subscription_eval_failed: List[str],
        oversold_catalog_count: int,
        oversold_sub_count: int,
        overbought_catalog_count: int,
        overbought_sub_count: int,
        has_oversold_hits: bool,
        has_overbought_hits: bool,
        oversold_threshold: float,
        overbought_threshold: float,
        data_timestamp: Optional[datetime],
        batch_count: int
    ):
        """
        Post comprehensive auto-scan status to changelog channel.
        Includes separate failure reporting for catalog and subscriptions.
        """
        now = datetime.now(self.timezone)
        region_display = region.replace('_', '/').title()
        
        msg = f"🔄 **Auto-Scan Status** ({region_display})\n"
        msg += f"Time: {now.strftime('%Y-%m-%d %H:%M %Z')}\n\n"
        
        # Catalog scan results
        msg += f"**Catalog Scan Results:**\n"
        msg += f"• Tickers: {catalog_success}/{catalog_total} successful\n"
        msg += f"• Batches: {batch_count}\n"
        
        if catalog_failed:
            failed_display = catalog_failed[:5]
            msg += f"• ❌ **Failed ({len(catalog_failed)}):** {', '.join(failed_display)}"
            if len(catalog_failed) > 5:
                msg += f" (+{len(catalog_failed) - 5} more)"
            msg += "\n"
        
        # Subscription evaluation results
        msg += f"\n**Subscription Evaluation:**\n"
        msg += f"• Total subscriptions: {subscription_total}\n"
        msg += f"• Tickers with RSI data: {subscription_success}\n"
        
        if subscription_eval_failed:
            failed_display = subscription_eval_failed[:5]
            msg += f"• ❌ **Failed to evaluate ({len(subscription_eval_failed)}):** {', '.join(failed_display)}"
            if len(subscription_eval_failed) > 5:
                msg += f" (+{len(subscription_eval_failed) - 5} more)"
            msg += "\n"
        
        # Data timestamp
        if data_timestamp:
            msg += f"\n**Data Timestamp:** {data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}\n"
        
        # Thresholds and hit counts
        msg += f"\n**Thresholds & Hits:**\n"
        msg += f"• Oversold (< {oversold_threshold}): {oversold_catalog_count} catalog"
        if oversold_sub_count > 0:
            msg += f" + {oversold_sub_count} subscriptions"
        msg += "\n"
        
        msg += f"• Overbought (> {overbought_threshold}): {overbought_catalog_count} catalog"
        if overbought_sub_count > 0:
            msg += f" + {overbought_sub_count} subscriptions"
        msg += "\n"
        
        # Posted updates
        msg += f"\n**Posted Updates:**\n"
        msg += f"• #{OVERSOLD_CHANNEL_NAME}: {'✅ Posted' if has_oversold_hits else '⏭️ No hits'}\n"
        msg += f"• #{OVERBOUGHT_CHANNEL_NAME}: {'✅ Posted' if has_overbought_hits else '⏭️ No hits'}\n"
        
        try:
            await channel.send(msg)
        except discord.HTTPException as e:
            logger.error(f"Failed to send auto-scan status: {e}")

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
        """Execute the daily RSI check for all guilds (subscription-based)."""
        start_time = datetime.now(self.timezone)
        logger.info(f"Starting daily RSI check at {start_time.isoformat()}")

        try:
            # Get guild IDs to check schedule_enabled
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

            # Step 1: Load all active subscriptions (only for enabled guilds)
            subscriptions_data = await self.db.get_subscriptions_with_state()
            subscriptions_data = [s for s in subscriptions_data if s['guild_id'] in enabled_guilds]

            if not subscriptions_data:
                logger.info("No active subscriptions found for enabled guilds")
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

                # Send UNDER alerts to oversold channel (suppress embeds)
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

                # Send OVER alerts to overbought channel (suppress embeds)
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
