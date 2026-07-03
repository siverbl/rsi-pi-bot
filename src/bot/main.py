#!/usr/bin/env python3
"""RSI Discord Bot - Main Entry Point

TradingView-only build (RSI14 via TradingView Screener).

FIXED IMPLEMENTATION - Key changes:
1. /ticker-info now retrieves RSI from persistence table (spec section 4.2)
2. Scheduler properly initialized with change detection support
3. All commands preserved with proper functionality

Usage:
    export DISCORD_TOKEN=your_bot_token
    export PYTHONPATH=src
    python -m bot.main
"""
import logging
import re
import sys
from datetime import datetime
from typing import Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
import pytz

from bot.config import (
    DISCORD_TOKEN, DEFAULT_OVERSOLD_THRESHOLD,
    DEFAULT_OVERBOUGHT_THRESHOLD, OVERSOLD_CHANNEL_NAME, OVERBOUGHT_CHANNEL_NAME,
    CHANGELOG_CHANNEL_NAME, REQUEST_CHANNEL_NAME, LOG_PATH,
    DEFAULT_TIMEZONE
)
from bot.repositories.database import Database
from bot.repositories.ticker_catalog import get_catalog, validate_ticker, remove_ticker
from bot.services.market_data.providers import get_provider
from bot.services.scheduler import RSIScheduler
from bot.cogs.ticker_request import handle_request_message
from bot.utils.message_utils import format_subscription_list

# Strict HH:MM validation for /set-defaults schedule_time
SCHEDULE_TIME_PATTERN = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)


def get_alert_channels(guild: discord.Guild) -> Tuple[Optional[discord.TextChannel], Optional[discord.TextChannel], str]:
    """
    Get the fixed alert channels for a guild and verify permissions.
    
    Returns:
        Tuple of (oversold_channel, overbought_channel, error_message)
    """
    oversold_channel = discord.utils.get(guild.text_channels, name=OVERSOLD_CHANNEL_NAME)
    overbought_channel = discord.utils.get(guild.text_channels, name=OVERBOUGHT_CHANNEL_NAME)
    
    errors = []
    
    if not oversold_channel:
        errors.append(f"Channel `#{OVERSOLD_CHANNEL_NAME}` not found")
    if not overbought_channel:
        errors.append(f"Channel `#{OVERBOUGHT_CHANNEL_NAME}` not found")
    
    bot_member = guild.me
    if oversold_channel:
        perms = oversold_channel.permissions_for(bot_member)
        if not perms.send_messages:
            errors.append(f"Bot lacks **Send Messages** permission in `#{OVERSOLD_CHANNEL_NAME}`")
    
    if overbought_channel:
        perms = overbought_channel.permissions_for(bot_member)
        if not perms.send_messages:
            errors.append(f"Bot lacks **Send Messages** permission in `#{OVERBOUGHT_CHANNEL_NAME}`")
    
    error_msg = ""
    if errors:
        error_msg = (
            "❌ **Channel/Permission Issues:**\n" +
            "\n".join(f"• {e}" for e in errors) +
            "\n\n**To fix:**\n"
            "1. Create the channels if they don't exist\n"
            "2. Go to channel settings → Permissions\n"
            "3. Add the bot role and enable **Send Messages**"
        )
    
    return oversold_channel, overbought_channel, error_msg


def get_changelog_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    """Get the changelog channel for a guild."""
    return discord.utils.get(guild.text_channels, name=CHANGELOG_CHANNEL_NAME)


class RSIBot(commands.Bot):
    """Discord bot for RSI alerts with integrated scheduler."""

    def __init__(self):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True

        super().__init__(
            command_prefix="!",
            intents=intents
        )

        self.db = Database()
        self.catalog = get_catalog()
        # Scan execution (RSI fetching, alert evaluation) lives entirely in
        # RSIScheduler - the single source of truth for the scan pipeline.
        self.scheduler: Optional[RSIScheduler] = None
        self.health_runner = None

    async def setup_hook(self):
        """Initialize bot components."""
        logger.info("=" * 60)
        logger.info("RSI DISCORD BOT - STARTUP")
        logger.info("=" * 60)
        
        logger.info("Initializing database...")
        await self.db.initialize()

        logger.info("Loading ticker catalog...")
        self.catalog.load()
        logger.info(f"Loaded {len(self.catalog)} instruments")

        # Log provider info
        provider = get_provider()
        logger.info(f"RSI Data Provider: {provider.name}")

        logger.info("Starting scheduler...")
        self.scheduler = RSIScheduler(self)
        await self.scheduler.start()

        logger.info("Syncing slash commands...")
        await self.tree.sync()

        logger.info("=" * 60)
        logger.info("Bot setup complete")
        logger.info("=" * 60)

    async def on_ready(self):
        """Called when bot is ready (also fires again after reconnects)."""
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logger.info(f"Connected to {len(self.guilds)} guilds")
        logger.info(f"Ticker catalog contains {len(self.catalog)} instruments")
        provider = get_provider()
        logger.info(f"RSI Provider: {provider.name}")

        # Seed a default config for every guild so scheduled scans work
        # without anyone having to run a slash command first.
        for guild in self.guilds:
            try:
                await self.db.get_or_create_guild_config(guild.id)
            except Exception:
                logger.exception(f"Failed to ensure config for guild {guild.id}")

        # (Re)register per-guild daily jobs. Idempotent, so reconnects that
        # re-fire on_ready cannot create duplicate jobs.
        if self.scheduler:
            try:
                await self.scheduler.sync_guild_daily_jobs()
                self.scheduler.log_jobs()
            except Exception:
                logger.exception("Failed to sync per-guild daily jobs")

        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="RSI levels"
            )
        )

    async def on_guild_join(self, guild: discord.Guild):
        """Provision new guilds immediately: default config + daily job."""
        logger.info(f"Joined guild {guild.name} ({guild.id}) - creating default config")
        try:
            config = await self.db.get_or_create_guild_config(guild.id)
            if self.scheduler:
                self.scheduler.schedule_guild_daily_job(guild.id, config.default_schedule_time)
        except Exception:
            logger.exception(f"Failed to provision new guild {guild.id}")

    async def on_guild_remove(self, guild: discord.Guild):
        """Clean up scheduled work when the bot leaves a guild."""
        logger.info(f"Removed from guild {guild.name} ({guild.id})")
        if self.scheduler:
            self.scheduler.remove_guild_daily_job(guild.id)

    async def on_message(self, message: discord.Message):
        """Handle messages - used for #request channel ticker additions."""
        if message.author.bot:
            return
        
        if hasattr(message.channel, 'name') and message.channel.name == REQUEST_CHANNEL_NAME:
            response = await handle_request_message(message)
            if response:
                try:
                    await message.reply(response, mention_author=False)
                    if response.startswith("✅"):
                        self.catalog.reload()
                except discord.HTTPException as e:
                    logger.error(f"Failed to reply to request: {e}")

    async def close(self):
        """Clean shutdown."""
        logger.info("Shutting down bot...")
        if self.scheduler:
            self.scheduler.stop()
        if self.health_runner:
            await self.health_runner.cleanup()
        await super().close()
        logger.info("Bot shutdown complete")


# Create bot instance
bot = RSIBot()


# ==================== Slash Commands ====================

@bot.tree.command(name="subscribe", description="Create an RSI alert subscription")
@app_commands.guild_only()
@app_commands.describe(
    ticker="Stock ticker symbol (must exist in tickers.csv)",
    condition="Alert condition: 'under' or 'over'",
    threshold="RSI threshold value (0-100)",
    period="RSI period (default: server default or 14)",
    cooldown="Hours between alerts for same rule (default: server default or 24)"
)
@app_commands.choices(condition=[
    app_commands.Choice(name="under (oversold)", value="UNDER"),
    app_commands.Choice(name="over (overbought)", value="OVER")
])
async def subscribe(
    interaction: discord.Interaction,
    ticker: str,
    condition: app_commands.Choice[str],
    threshold: float,
    period: Optional[int] = None,
    cooldown: Optional[int] = None
):
    """Create a new RSI alert subscription."""
    await interaction.response.defer(ephemeral=True)

    is_valid, error = validate_ticker(ticker)
    if not is_valid:
        await interaction.followup.send(f"❌ {error}", ephemeral=True)
        return

    if not 0 <= threshold <= 100:
        await interaction.followup.send("❌ Threshold must be between 0 and 100", ephemeral=True)
        return

    # Reject thresholds that can never trigger (RSI is always within 0-100)
    if condition.value == "UNDER" and threshold <= 0:
        await interaction.followup.send(
            "❌ An UNDER alert with threshold 0 can never trigger (RSI is never below 0)",
            ephemeral=True
        )
        return
    if condition.value == "OVER" and threshold >= 100:
        await interaction.followup.send(
            "❌ An OVER alert with threshold 100 can never trigger (RSI is never above 100)",
            ephemeral=True
        )
        return

    if period is not None and period != 14:
        await interaction.followup.send("❌ Only RSI14 (period=14) is supported in this TradingView-only build", ephemeral=True)
        return

    if cooldown is not None and cooldown < 0:
        await interaction.followup.send("❌ Cooldown must be non-negative", ephemeral=True)
        return

    oversold_ch, overbought_ch, error_msg = get_alert_channels(interaction.guild)
    if error_msg:
        await interaction.followup.send(error_msg, ephemeral=True)
        return

    config = await bot.db.get_or_create_guild_config(interaction.guild_id)
    target_period = period if period is not None else config.default_rsi_period
    target_cooldown = cooldown if cooldown is not None else config.default_cooldown_hours

    ticker = ticker.upper().strip()
    target_channel = oversold_ch if condition.value == "UNDER" else overbought_ch

    exists = await bot.db.subscription_exists(
        guild_id=interaction.guild_id,
        ticker=ticker,
        condition=condition.value,
        threshold=threshold,
        period=target_period
    )

    if exists:
        await interaction.followup.send(
            "❌ A subscription with these exact parameters already exists",
            ephemeral=True
        )
        return

    try:
        sub = await bot.db.create_subscription(
            guild_id=interaction.guild_id,
            ticker=ticker,
            condition=condition.value,
            threshold=threshold,
            period=target_period,
            cooldown_hours=target_cooldown,
            created_by_user_id=interaction.user.id
        )

        instrument = bot.catalog.get_instrument(ticker)
        name = instrument.name if instrument else ticker

        await interaction.followup.send(
            f"✅ **Subscription created** (ID: `{sub.id}`)\n"
            f"• **Ticker:** {ticker} — {name}\n"
            f"• **Condition:** RSI{target_period} {condition.value} {threshold}\n"
            f"• **Alerts to:** {target_channel.mention}\n"
            f"• **Cooldown:** {target_cooldown} hours",
            ephemeral=True
        )

    except Exception as e:
        logger.error(f"Error creating subscription: {e}")
        await interaction.followup.send(f"❌ Failed to create subscription: {str(e)}", ephemeral=True)


@bot.tree.command(name="subscribe-bands", description="Create both oversold and overbought alerts for a ticker")
@app_commands.guild_only()
@app_commands.describe(
    ticker="Stock ticker symbol (must exist in tickers.csv)",
    oversold="Oversold threshold (default: 30)",
    overbought="Overbought threshold (default: 70)",
    period="RSI period (default: server default or 14)",
    cooldown="Hours between alerts (default: server default or 24)"
)
async def subscribe_bands(
    interaction: discord.Interaction,
    ticker: str,
    oversold: Optional[float] = None,
    overbought: Optional[float] = None,
    period: Optional[int] = None,
    cooldown: Optional[int] = None
):
    """Create both oversold (UNDER) and overbought (OVER) subscriptions."""
    await interaction.response.defer(ephemeral=True)

    is_valid, error = validate_ticker(ticker)
    if not is_valid:
        await interaction.followup.send(f"❌ {error}", ephemeral=True)
        return

    oversold_ch, overbought_ch, error_msg = get_alert_channels(interaction.guild)
    if error_msg:
        await interaction.followup.send(error_msg, ephemeral=True)
        return

    oversold_threshold = oversold if oversold is not None else DEFAULT_OVERSOLD_THRESHOLD
    overbought_threshold = overbought if overbought is not None else DEFAULT_OVERBOUGHT_THRESHOLD

    if not 0 <= oversold_threshold <= 100:
        await interaction.followup.send("❌ Oversold threshold must be between 0 and 100", ephemeral=True)
        return

    if not 0 <= overbought_threshold <= 100:
        await interaction.followup.send("❌ Overbought threshold must be between 0 and 100", ephemeral=True)
        return

    if oversold_threshold >= overbought_threshold:
        await interaction.followup.send("❌ Oversold threshold must be less than overbought threshold", ephemeral=True)
        return

    # Reject thresholds that can never trigger (RSI is always within 0-100)
    if oversold_threshold <= 0:
        await interaction.followup.send(
            "❌ An oversold threshold of 0 can never trigger (RSI is never below 0)",
            ephemeral=True
        )
        return
    if overbought_threshold >= 100:
        await interaction.followup.send(
            "❌ An overbought threshold of 100 can never trigger (RSI is never above 100)",
            ephemeral=True
        )
        return

    if period is not None and period != 14:
        await interaction.followup.send("❌ Only RSI14 (period=14) is supported in this TradingView-only build", ephemeral=True)
        return

    if cooldown is not None and cooldown < 0:
        await interaction.followup.send("❌ Cooldown must be non-negative", ephemeral=True)
        return

    config = await bot.db.get_or_create_guild_config(interaction.guild_id)
    target_period = period if period is not None else config.default_rsi_period
    target_cooldown = cooldown if cooldown is not None else config.default_cooldown_hours

    ticker = ticker.upper().strip()
    instrument = bot.catalog.get_instrument(ticker)
    name = instrument.name if instrument else ticker

    created_subs = []
    errors = []

    # Create UNDER subscription
    try:
        exists = await bot.db.subscription_exists(
            guild_id=interaction.guild_id,
            ticker=ticker,
            condition="UNDER",
            threshold=oversold_threshold,
            period=target_period
        )

        if exists:
            errors.append(f"UNDER {oversold_threshold} already exists")
        else:
            sub = await bot.db.create_subscription(
                guild_id=interaction.guild_id,
                ticker=ticker,
                condition="UNDER",
                threshold=oversold_threshold,
                period=target_period,
                cooldown_hours=target_cooldown,
                created_by_user_id=interaction.user.id
            )
            created_subs.append(f"UNDER {oversold_threshold} (ID: `{sub.id}`) → {oversold_ch.mention}")
    except Exception as e:
        errors.append(f"UNDER: {str(e)}")

    # Create OVER subscription
    try:
        exists = await bot.db.subscription_exists(
            guild_id=interaction.guild_id,
            ticker=ticker,
            condition="OVER",
            threshold=overbought_threshold,
            period=target_period
        )

        if exists:
            errors.append(f"OVER {overbought_threshold} already exists")
        else:
            sub = await bot.db.create_subscription(
                guild_id=interaction.guild_id,
                ticker=ticker,
                condition="OVER",
                threshold=overbought_threshold,
                period=target_period,
                cooldown_hours=target_cooldown,
                created_by_user_id=interaction.user.id
            )
            created_subs.append(f"OVER {overbought_threshold} (ID: `{sub.id}`) → {overbought_ch.mention}")
    except Exception as e:
        errors.append(f"OVER: {str(e)}")

    response_lines = [f"**{ticker} — {name}**\n"]

    if created_subs:
        response_lines.append("✅ **Created:**")
        for sub_info in created_subs:
            response_lines.append(f"• RSI{target_period} {sub_info}")
        response_lines.append(f"• Cooldown: {target_cooldown} hours")

    if errors:
        response_lines.append("\n⚠️ **Warnings:**")
        for error in errors:
            response_lines.append(f"• {error}")

    await interaction.followup.send("\n".join(response_lines), ephemeral=True)


@bot.tree.command(name="unsubscribe", description="Remove an RSI alert subscription (your own only)")
@app_commands.guild_only()
@app_commands.describe(id="Subscription ID to remove (from /list)")
async def unsubscribe(interaction: discord.Interaction, id: int):
    """Remove a subscription by ID."""
    await interaction.response.defer(ephemeral=True)

    sub = await bot.db.get_subscription(id)

    if not sub:
        await interaction.followup.send(f"❌ Subscription ID `{id}` not found", ephemeral=True)
        return

    if sub.guild_id != interaction.guild_id:
        await interaction.followup.send(f"❌ Subscription ID `{id}` does not belong to this server", ephemeral=True)
        return

    if sub.created_by_user_id != interaction.user.id:
        await interaction.followup.send(
            f"❌ **Permission Denied**\n"
            f"You can only remove subscriptions you created.\n"
            f"This subscription was created by <@{sub.created_by_user_id}>.\n\n"
            f"If you're an admin, use `/admin-unsubscribe`.",
            ephemeral=True
        )
        return

    deleted = await bot.db.delete_subscription(id, interaction.guild_id)

    if deleted:
        instrument = bot.catalog.get_instrument(sub.ticker)
        name = instrument.name if instrument else sub.ticker

        await interaction.followup.send(
            f"✅ **Subscription removed** (ID: `{id}`)\n"
            f"• **Ticker:** {sub.ticker} — {name}\n"
            f"• **Condition:** RSI{sub.period} {sub.condition} {sub.threshold}",
            ephemeral=True
        )
    else:
        await interaction.followup.send(f"❌ Failed to remove subscription ID `{id}`", ephemeral=True)


@bot.tree.command(name="unsubscribe-all", description="Remove all your subscriptions")
@app_commands.guild_only()
async def unsubscribe_all(interaction: discord.Interaction):
    """Remove all subscriptions created by the user."""
    await interaction.response.defer(ephemeral=True)

    user_subs = await bot.db.get_user_subscriptions(interaction.guild_id, interaction.user.id)

    if not user_subs:
        await interaction.followup.send("📋 You have no subscriptions to remove.", ephemeral=True)
        return

    deleted_count = await bot.db.delete_user_subscriptions(interaction.guild_id, interaction.user.id)

    if deleted_count > 0:
        await interaction.followup.send(
            f"✅ **Removed {deleted_count} subscription(s)**\n\n"
            f"All your RSI alert subscriptions have been cleared.",
            ephemeral=True
        )
    else:
        await interaction.followup.send("❌ Failed to remove subscriptions. Please try again.", ephemeral=True)


@bot.tree.command(name="admin-unsubscribe", description="[Admin] Remove any subscription by ID")
@app_commands.guild_only()
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    id="Subscription ID to remove",
    reason="Reason for removal (will be logged)"
)
async def admin_unsubscribe(
    interaction: discord.Interaction,
    id: int,
    reason: Optional[str] = None
):
    """Admin command to remove any subscription."""
    await interaction.response.defer(ephemeral=True)

    if not interaction.user.guild_permissions.administrator:
        await interaction.followup.send(
            "❌ **Permission Denied**\nThis command requires Administrator permission.",
            ephemeral=True
        )
        return

    sub = await bot.db.get_subscription(id)

    if not sub:
        await interaction.followup.send(f"❌ Subscription ID `{id}` not found", ephemeral=True)
        return

    if sub.guild_id != interaction.guild_id:
        await interaction.followup.send(f"❌ Subscription ID `{id}` does not belong to this server", ephemeral=True)
        return

    instrument = bot.catalog.get_instrument(sub.ticker)
    name = instrument.name if instrument else sub.ticker
    original_owner_id = sub.created_by_user_id

    deleted = await bot.db.delete_subscription(id, interaction.guild_id)

    if deleted:
        # Log to changelog
        changelog_ch = get_changelog_channel(interaction.guild)
        if changelog_ch:
            try:
                log_msg = (
                    f"🗑️ **Subscription Removed by Admin**\n"
                    f"• **ID:** `{id}`\n"
                    f"• **Ticker:** {sub.ticker} — {name}\n"
                    f"• **Condition:** RSI{sub.period} {sub.condition} {sub.threshold}\n"
                    f"• **Original owner:** <@{original_owner_id}>\n"
                    f"• **Removed by:** {interaction.user.mention}"
                )
                if reason:
                    log_msg += f"\n• **Reason:** {reason}"
                await changelog_ch.send(log_msg)
            except discord.HTTPException:
                pass
        
        await interaction.followup.send(
            f"✅ **Subscription removed by admin** (ID: `{id}`)\n"
            f"• **Ticker:** {sub.ticker} — {name}\n"
            f"• **Condition:** RSI{sub.period} {sub.condition} {sub.threshold}\n"
            f"• **Originally created by:** <@{original_owner_id}>\n"
            f"• **Action logged to:** `#{CHANGELOG_CHANNEL_NAME}`",
            ephemeral=True
        )
    else:
        await interaction.followup.send(f"❌ Failed to remove subscription ID `{id}`", ephemeral=True)


@bot.tree.command(name="remove-ticker", description="[Admin] Remove a ticker from the catalog")
@app_commands.guild_only()
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    ticker="Ticker symbol to remove (case-insensitive)"
)
async def remove_ticker_cmd(
    interaction: discord.Interaction,
    ticker: str
):
    """Admin command to remove a ticker from tickers.csv."""
    await interaction.response.defer(ephemeral=True)

    if not interaction.user.guild_permissions.administrator:
        await interaction.followup.send(
            "❌ **Permission Denied**\nThis command requires Administrator permission.",
            ephemeral=True
        )
        return

    ticker = ticker.upper().strip()
    logger.info(f"Admin {interaction.user} ({interaction.user.id}) removing ticker: {ticker}")

    success, message, removed_instrument = await remove_ticker(ticker)

    if success and removed_instrument:
        # Policy: a ticker removed from the catalog must not leave silently
        # broken subscriptions behind. Disable them (across all guilds) and
        # report the count; admins can re-add the ticker and re-subscribe.
        disabled_count = 0
        try:
            disabled_count = await bot.db.disable_subscriptions_for_ticker(ticker)
        except Exception:
            logger.exception(f"Failed to disable subscriptions for removed ticker {ticker}")

        # Log to changelog
        changelog_ch = get_changelog_channel(interaction.guild)
        if changelog_ch:
            try:
                log_msg = (
                    f"🗑️ **Ticker Removed from Catalog**\n"
                    f"• **Ticker:** `{removed_instrument.ticker}`\n"
                    f"• **Name:** {removed_instrument.name}\n"
                    f"• **TradingView:** `{removed_instrument.tradingview_slug}`\n"
                    f"• **Removed by:** {interaction.user.mention}"
                )
                if disabled_count:
                    log_msg += f"\n• **Subscriptions disabled:** {disabled_count}"
                await changelog_ch.send(log_msg)
            except discord.HTTPException:
                pass

        response = (
            f"✅ **Ticker removed from catalog**\n"
            f"• **Ticker:** `{removed_instrument.ticker}`\n"
            f"• **Name:** {removed_instrument.name}\n"
            f"• **TradingView slug:** `{removed_instrument.tradingview_slug}`\n"
            f"• **Logged to:** `#{CHANGELOG_CHANNEL_NAME}`"
        )
        if disabled_count:
            response += (
                f"\n• **Subscriptions disabled:** {disabled_count} "
                f"(they will no longer be evaluated)"
            )
        await interaction.followup.send(response, ephemeral=True)
        logger.info(f"Successfully removed ticker {ticker} from catalog")
    else:
        await interaction.followup.send(f"❌ {message}", ephemeral=True)
        logger.warning(f"Failed to remove ticker {ticker}: {message}")


@bot.tree.command(name="list", description="List RSI alert subscriptions")
@app_commands.guild_only()
@app_commands.describe(ticker="Filter by ticker (optional)")
async def list_subscriptions(interaction: discord.Interaction, ticker: Optional[str] = None):
    """List all subscriptions for this server with proper message chunking."""
    await interaction.response.defer(ephemeral=True)

    subs = await bot.db.get_subscriptions_by_guild(
        guild_id=interaction.guild_id,
        ticker=ticker.upper().strip() if ticker else None
    )

    if not subs:
        filter_text = f" for ticker `{ticker.upper()}`" if ticker else ""
        await interaction.followup.send(f"📋 No subscriptions found{filter_text}", ephemeral=True)
        return

    # Use the message chunking utility
    messages = format_subscription_list(
        subs, 
        bot.catalog, 
        OVERSOLD_CHANNEL_NAME, 
        OVERBOUGHT_CHANNEL_NAME
    )

    # Send first message as followup
    await interaction.followup.send(messages[0], ephemeral=True)
    
    # Send additional chunks if any
    for msg in messages[1:]:
        await interaction.followup.send(msg, ephemeral=True)


@bot.tree.command(name="run-now", description="Manually trigger RSI check (Admin)")
@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
async def run_now(interaction: discord.Interaction):
    """
    Manually trigger a full RSI scan.

    Uses the exact same scan pipeline as scheduled auto-scans (all regions),
    bypassing the schedule_enabled toggle. Posts the full oversold/overbought
    lists, evaluates subscriptions, and logs a summary to #server-changelog.
    """
    await interaction.response.defer(ephemeral=True)

    if not interaction.user.guild_permissions.manage_guild:
        await interaction.followup.send(
            "❌ **Permission Denied**\nThis command requires the Manage Server permission.",
            ephemeral=True
        )
        return

    if bot.scheduler is None:
        await interaction.followup.send(
            "❌ Scheduler is not initialized yet. Try again shortly.", ephemeral=True
        )
        return

    oversold_ch, overbought_ch, error_msg = get_alert_channels(interaction.guild)
    if error_msg:
        await interaction.followup.send(error_msg, ephemeral=True)
        return

    provider = get_provider()
    await interaction.followup.send(
        f"⏳ Running full RSI scan using {provider.name}...\nThis may take a minute.",
        ephemeral=True
    )

    try:
        summary = await bot.scheduler.run_now(
            guild_id=interaction.guild_id,
            triggered_by=str(interaction.user)
        )
    except Exception as e:
        logger.exception("Manual /run-now failed")
        await interaction.edit_original_response(
            content=f"❌ **Manual RSI scan failed:** {e}\nCheck the bot logs for details."
        )
        return

    guild_result = summary.get('guilds', {}).get(interaction.guild_id, {})
    sub_alerts = guild_result.get('sub_alerts_under', 0) + guild_result.get('sub_alerts_over', 0)
    lines = [
        "✅ **Manual RSI Scan Complete**",
        f"• **Provider:** {provider.name}",
        f"• **Duration:** {summary.get('duration_seconds', 0):.1f}s",
        f"• **Tickers:** {summary.get('tickers_ok', 0)} OK, "
        f"{summary.get('tickers_failed', 0)} failed (of {summary.get('tickers_total', 0)})",
        f"• **RSI values persisted:** {summary.get('persisted', 0)}",
        f"• **Oversold:** {guild_result.get('oversold_total', 0)} tickers → {oversold_ch.mention}",
        f"• **Overbought:** {guild_result.get('overbought_total', 0)} tickers → {overbought_ch.mention}",
        f"• **Subscription alerts:** {sub_alerts}",
        f"• **Messages sent:** {guild_result.get('messages_sent', 0)}",
        f"• Summary logged to: `#{CHANGELOG_CHANNEL_NAME}`",
    ]

    failed = summary.get('failed_tickers') or []
    if failed:
        preview = ", ".join(failed[:10])
        more = f" (+{len(failed) - 10} more)" if len(failed) > 10 else ""
        lines.append(f"\n⚠️ **Failed tickers:** {preview}{more}")

    issues = guild_result.get('channel_issues') or []
    if issues:
        lines.append("\n⚠️ **Channel issues:**")
        lines.extend(f"• {i}" for i in issues)

    await interaction.edit_original_response(content="\n".join(lines))


@bot.tree.command(name="set-defaults", description="Set server defaults (Admin)")
@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
@app_commands.describe(
    default_period="Default RSI period (must be 14)",
    default_cooldown="Default cooldown hours",
    schedule_time="Daily run time in HH:MM format (Europe/Oslo)",
    alert_mode="Alert mode: CROSSING or LEVEL",
    hysteresis="Hysteresis value for crossing detection",
    auto_oversold="Auto-scan oversold threshold (default: 34)",
    auto_overbought="Auto-scan overbought threshold (default: 70)",
    schedule_enabled="Enable or disable scheduled scans (true/false)"
)
@app_commands.choices(
    alert_mode=[
        app_commands.Choice(name="CROSSING", value="CROSSING"),
        app_commands.Choice(name="LEVEL", value="LEVEL")
    ],
    schedule_enabled=[
        app_commands.Choice(name="Enabled", value="true"),
        app_commands.Choice(name="Disabled", value="false")
    ]
)
async def set_defaults(
    interaction: discord.Interaction,
    default_period: Optional[int] = None,
    default_cooldown: Optional[int] = None,
    schedule_time: Optional[str] = None,
    alert_mode: Optional[app_commands.Choice[str]] = None,
    hysteresis: Optional[float] = None,
    auto_oversold: Optional[float] = None,
    auto_overbought: Optional[float] = None,
    schedule_enabled: Optional[app_commands.Choice[str]] = None
):
    """Set server-level default configuration including auto-scan thresholds and schedule toggle."""
    await interaction.response.defer(ephemeral=True)

    if not interaction.user.guild_permissions.manage_guild:
        await interaction.followup.send(
            "❌ **Permission Denied**\nThis command requires the Manage Server permission.",
            ephemeral=True
        )
        return

    # Validate inputs
    if default_period is not None and default_period != 14:
        await interaction.followup.send("❌ Only RSI14 (period=14) is supported in this TradingView-only build", ephemeral=True)
        return

    if default_cooldown is not None and default_cooldown < 0:
        await interaction.followup.send("❌ Cooldown must be non-negative", ephemeral=True)
        return

    if schedule_time is not None:
        schedule_time = schedule_time.strip()
        if not SCHEDULE_TIME_PATTERN.match(schedule_time):
            await interaction.followup.send(
                "❌ Schedule time must be in HH:MM format, 00:00-23:59 (e.g., 18:30)",
                ephemeral=True
            )
            return

    if hysteresis is not None and hysteresis < 0:
        await interaction.followup.send("❌ Hysteresis must be non-negative", ephemeral=True)
        return

    if auto_oversold is not None and not 0 <= auto_oversold <= 100:
        await interaction.followup.send("❌ Auto-oversold threshold must be between 0 and 100", ephemeral=True)
        return

    if auto_overbought is not None and not 0 <= auto_overbought <= 100:
        await interaction.followup.send("❌ Auto-overbought threshold must be between 0 and 100", ephemeral=True)
        return

    # Get old config for change detection
    old_config = await bot.db.get_or_create_guild_config(interaction.guild_id)
    old_schedule_enabled = old_config.schedule_enabled
    old_schedule_time = old_config.default_schedule_time

    # Cross-validate the *effective* threshold pair (mixing a new value with
    # an existing one must not produce an impossible oversold >= overbought)
    effective_oversold = auto_oversold if auto_oversold is not None else old_config.auto_oversold_threshold
    effective_overbought = auto_overbought if auto_overbought is not None else old_config.auto_overbought_threshold
    if effective_oversold >= effective_overbought:
        await interaction.followup.send(
            f"❌ Auto-oversold threshold ({effective_oversold}) must be less than "
            f"auto-overbought threshold ({effective_overbought})",
            ephemeral=True
        )
        return

    # Convert schedule_enabled choice to bool
    schedule_enabled_bool = None
    if schedule_enabled is not None:
        schedule_enabled_bool = schedule_enabled.value == "true"

    # Update config
    config = await bot.db.update_guild_config(
        guild_id=interaction.guild_id,
        default_rsi_period=default_period,
        default_schedule_time=schedule_time,
        default_cooldown_hours=default_cooldown,
        alert_mode=alert_mode.value if alert_mode else None,
        hysteresis=hysteresis,
        auto_oversold_threshold=auto_oversold,
        auto_overbought_threshold=auto_overbought,
        schedule_enabled=schedule_enabled_bool
    )

    # Apply a changed daily time to the running scheduler immediately
    next_daily_run = None
    schedule_time_changed = (
        schedule_time is not None and schedule_time != old_schedule_time
    )
    if schedule_time_changed and bot.scheduler:
        try:
            next_daily_run = bot.scheduler.reschedule_guild_daily(
                interaction.guild_id, config.default_schedule_time
            )
            logger.info(
                f"Rescheduled daily check for guild {interaction.guild_id} "
                f"to {config.default_schedule_time} (next run: {next_daily_run})"
            )
        except Exception:
            logger.exception(
                f"Failed to reschedule daily job for guild {interaction.guild_id}"
            )

    # Log schedule toggle change
    schedule_status = "✅ Enabled" if config.schedule_enabled else "❌ Disabled"
    schedule_changed = old_schedule_enabled != config.schedule_enabled

    if schedule_changed:
        logger.info(
            f"Schedule {'enabled' if config.schedule_enabled else 'disabled'} "
            f"for guild {interaction.guild_id} by {interaction.user}"
        )
        
        # Log to changelog
        changelog_ch = get_changelog_channel(interaction.guild)
        if changelog_ch:
            try:
                change_msg = (
                    f"⚙️ **Schedule Settings Changed**\n"
                    f"• **Schedule:** {'Enabled' if config.schedule_enabled else 'Disabled'}\n"
                    f"• **Changed by:** {interaction.user.mention}"
                )
                await changelog_ch.send(change_msg)
            except discord.HTTPException:
                pass

    # Build response
    response = (
        f"✅ **Server defaults updated**\n"
        f"• **Default RSI period:** {config.default_rsi_period}\n"
        f"• **Default cooldown:** {config.default_cooldown_hours} hours\n"
        f"• **Schedule time:** {config.default_schedule_time} (Europe/Oslo)\n"
        f"• **Alert mode:** {config.alert_mode}\n"
        f"• **Hysteresis:** {config.hysteresis}\n\n"
        f"**Auto-Scan Thresholds:**\n"
        f"• **Oversold:** < {config.auto_oversold_threshold}\n"
        f"• **Overbought:** > {config.auto_overbought_threshold}\n\n"
        f"**Scheduling:**\n"
        f"• **Status:** {schedule_status}"
    )
    
    if schedule_changed:
        response += " *(changed)*"

    if schedule_time_changed:
        if next_daily_run is not None:
            response += (
                f"\n• **Daily check rescheduled:** next run "
                f"{next_daily_run.strftime('%Y-%m-%d %H:%M %Z')}"
            )
        else:
            response += "\n• **Daily check time updated** (applies on next scheduler sync)"

    response += (
        f"\n\n**Fixed alert channels:**\n"
        f"• Oversold (UNDER): `#{OVERSOLD_CHANNEL_NAME}`\n"
        f"• Overbought (OVER): `#{OVERBOUGHT_CHANNEL_NAME}`"
    )

    await interaction.followup.send(response, ephemeral=True)


# ==================== FIXED: /ticker-info with RSI persistence (Spec Section 4.2) ====================

@bot.tree.command(name="ticker-info", description="Get information about a ticker")
@app_commands.guild_only()
@app_commands.describe(ticker="Stock ticker symbol to look up")
async def ticker_info(interaction: discord.Interaction, ticker: str):
    """
    Get information about a ticker from the catalog.
    
    FIXED IMPLEMENTATION (Spec Section 4.2):
    - Now retrieves RSI from the ticker_rsi persistence table
    - Shows most recently stored RSI value with timestamp
    - Indicates if data is stale
    """
    await interaction.response.defer(ephemeral=True)

    ticker = ticker.upper().strip()
    instrument = bot.catalog.get_instrument(ticker)

    if not instrument:
        results = bot.catalog.search_tickers(ticker, limit=5)
        if results:
            suggestions = "\n".join(f"• `{i.ticker}` — {i.name}" for i in results)
            await interaction.followup.send(
                f"❌ Ticker `{ticker}` not found in catalog.\n\n"
                f"**Did you mean:**\n{suggestions}",
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                f"❌ Ticker `{ticker}` not found in catalog.\n"
                f"Add it to `tickers.csv` to enable subscriptions.",
                ephemeral=True
            )
        return

    # Wrap URL in angle brackets to suppress embed preview
    tv_url = instrument.tradingview_url
    lines = [
        f"**{instrument.ticker} — {instrument.name}**",
        f"🔗 [TradingView](<{tv_url}>)",
        ""
    ]

    # FIXED: Get RSI data from persistence table (spec section 4.2)
    ticker_rsi = await bot.db.get_ticker_rsi(ticker)
    
    if ticker_rsi:
        # Calculate data age
        try:
            data_date = datetime.strptime(ticker_rsi.data_date, "%Y-%m-%d")
            days_old = (datetime.now() - data_date).days
        except ValueError:
            days_old = 999
        
        if days_old > 3:
            lines.append(f"⚠️ **RSI Data (STALE - {days_old} days old):**")
        elif days_old > 1:
            lines.append(f"📊 **RSI Data ({days_old} days old):**")
        else:
            lines.append("📊 **RSI Data:**")
        
        lines.append(f"• RSI14: **{ticker_rsi.rsi_14:.1f}**")
        
        if ticker_rsi.last_close:
            lines.append(f"• Last Close: {ticker_rsi.last_close:.2f}")
        
        lines.append(f"• Data Date: {ticker_rsi.data_date}")
        
        if ticker_rsi.data_timestamp:
            lines.append(f"• Fetched: {ticker_rsi.data_timestamp.strftime('%Y-%m-%d %H:%M UTC')}")
        
        lines.append(f"• Updated: {ticker_rsi.updated_at.strftime('%Y-%m-%d %H:%M UTC')}")
        lines.append("")
    else:
        # No persisted RSI data - check subscription state as fallback
        subs = await bot.db.get_subscriptions_by_guild(guild_id=interaction.guild_id, ticker=ticker)
        rsi_data = None
        
        if subs:
            for sub in subs:
                state = await bot.db.get_subscription_state(sub.id)
                if state and state.last_rsi is not None and state.last_date:
                    try:
                        last_date = datetime.strptime(state.last_date, "%Y-%m-%d")
                        days_old = (datetime.now() - last_date).days
                        if rsi_data is None or state.last_date > rsi_data['date']:
                            rsi_data = {
                                'rsi': state.last_rsi,
                                'close': state.last_close,
                                'date': state.last_date,
                                'period': sub.period,
                                'days_old': days_old
                            }
                    except ValueError:
                        pass
        
        if rsi_data:
            if rsi_data['days_old'] > 1:
                lines.append(f"⚠️ **RSI Data (from subscription state, {rsi_data['days_old']} days old):**")
            else:
                lines.append("📊 **RSI Data (from subscription state):**")
            lines.append(f"• RSI{rsi_data['period']}: **{rsi_data['rsi']:.1f}**")
            if rsi_data['close']:
                lines.append(f"• Last Close: {rsi_data['close']:.2f} ({rsi_data['date']})")
            lines.append("")
        else:
            lines.append("📊 **RSI Data:** Not yet available")
            lines.append("💡 RSI data is populated during scheduled or manual scans.")
            lines.append("")

    # Get subscriptions for this ticker
    subs = await bot.db.get_subscriptions_by_guild(guild_id=interaction.guild_id, ticker=ticker)
    
    if subs:
        under_subs = [s for s in subs if s.condition == "UNDER"]
        over_subs = [s for s in subs if s.condition == "OVER"]

        lines.append(f"🔔 **Active Subscriptions:** ({len(subs)} total)")
        
        if under_subs:
            for sub in under_subs:
                lines.append(f"• `{sub.id}` — RSI{sub.period} < {sub.threshold} → #{OVERSOLD_CHANNEL_NAME}")
        
        if over_subs:
            for sub in over_subs:
                lines.append(f"• `{sub.id}` — RSI{sub.period} > {sub.threshold} → #{OVERBOUGHT_CHANNEL_NAME}")
    else:
        lines.append("🔔 **Active Subscriptions:** None")
        lines.append("Use `/subscribe` or `/subscribe-bands` to add alerts for this ticker.")

    # Use suppress_embeds=True to prevent link preview
    await interaction.followup.send("\n".join(lines), ephemeral=True, suppress_embeds=True)


@bot.tree.command(name="catalog-stats", description="Show ticker catalog and subscription statistics")
@app_commands.guild_only()
async def catalog_stats(interaction: discord.Interaction):
    """Show statistics about the ticker catalog and subscriptions."""
    await interaction.response.defer(ephemeral=True)

    catalog_count = len(bot.catalog)
    provider = get_provider()
    
    all_subs = await bot.db.get_subscriptions_by_guild(
        guild_id=interaction.guild_id,
        enabled_only=False
    )
    
    total_subs = len(all_subs)
    enabled_subs = sum(1 for s in all_subs if s.enabled)
    under_subs = sum(1 for s in all_subs if s.condition == "UNDER" and s.enabled)
    over_subs = sum(1 for s in all_subs if s.condition == "OVER" and s.enabled)
    unique_tickers = len(set(s.ticker for s in all_subs if s.enabled))

    config = await bot.db.get_or_create_guild_config(interaction.guild_id)
    schedule_status = "✅ Enabled" if config.schedule_enabled else "❌ Disabled"

    await interaction.followup.send(
        f"📊 **Bot Statistics**\n\n"
        f"**RSI Data Provider:**\n"
        f"• {provider.name}\n\n"
        f"**Ticker Catalog:**\n"
        f"• Total instruments: {catalog_count}\n"
        f"• File: `tickers.csv`\n\n"
        f"**Subscriptions (this server):**\n"
        f"• Total active: **{enabled_subs}**\n"
        f"• Oversold alerts (UNDER): {under_subs}\n"
        f"• Overbought alerts (OVER): {over_subs}\n"
        f"• Unique tickers watched: {unique_tickers}\n\n"
        f"**Auto-Scan Thresholds:**\n"
        f"• Oversold: < {config.auto_oversold_threshold}\n"
        f"• Overbought: > {config.auto_overbought_threshold}\n\n"
        f"**Scheduling:**\n"
        f"• Status: {schedule_status}\n"
        f"• Time: {config.default_schedule_time} (Europe/Oslo)\n\n"
        f"**Alert Channels:**\n"
        f"• `#{OVERSOLD_CHANNEL_NAME}` — UNDER alerts\n"
        f"• `#{OVERBOUGHT_CHANNEL_NAME}` — OVER alerts\n\n"
        f"💡 Admins: use `/scheduler-status` for scheduling health details.",
        ephemeral=True
    )


@bot.tree.command(name="scheduler-status", description="Show scheduling health: jobs, next runs, last scan (Admin)")
@app_commands.guild_only()
@app_commands.default_permissions(manage_guild=True)
async def scheduler_status(interaction: discord.Interaction):
    """
    Inspect scheduler health for this server:
    schedule toggle, configured daily time, registered jobs with next run
    times, latest completed scan, latest error, and channel health.
    """
    await interaction.response.defer(ephemeral=True)

    if not interaction.user.guild_permissions.manage_guild:
        await interaction.followup.send(
            "❌ **Permission Denied**\nThis command requires the Manage Server permission.",
            ephemeral=True
        )
        return

    config = await bot.db.get_or_create_guild_config(interaction.guild_id)
    tz = pytz.timezone(DEFAULT_TIMEZONE)

    lines = ["🩺 **Scheduler Status**\n"]

    # Per-guild configuration
    lines.append("**This Server:**")
    lines.append(f"• Schedule: {'✅ Enabled' if config.schedule_enabled else '❌ Disabled'}")
    lines.append(f"• Daily check time: {config.default_schedule_time} ({DEFAULT_TIMEZONE})")
    lines.append("")

    if bot.scheduler is None:
        lines.append("🚨 **Scheduler is NOT initialized** — scheduled scans will not run.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)
        return

    status = bot.scheduler.get_status()

    lines.append("**Scheduler:**")
    lines.append(f"• Running: {'✅ Yes' if status['running'] else '🚨 NO — scans will not fire'}")
    if status.get('scan_in_progress'):
        lines.append("• A scan is running right now ⏳")
    lines.append("")

    lines.append("**Jobs & Next Runs:**")
    jobs = status.get('jobs', [])
    if not jobs:
        lines.append("• 🚨 No jobs registered")
    for job in jobs:
        next_run = job.get('next_run_time')
        if next_run:
            next_str = next_run.astimezone(tz).strftime('%a %Y-%m-%d %H:%M %Z')
        else:
            next_str = "not scheduled"
        lines.append(f"• `{job['id']}` → {next_str}")
    lines.append("")

    last_scan = status.get('last_scan')
    lines.append("**Latest Scan:**")
    if last_scan:
        finished = last_scan.get('finished')
        finished_str = finished.strftime('%Y-%m-%d %H:%M:%S %Z') if finished else "?"
        outcome = "✅ OK" if last_scan.get('success') else f"🚨 FAILED ({last_scan.get('error')})"
        lines.append(
            f"• {last_scan.get('type', '?')} / {last_scan.get('region', '?')} at {finished_str} — {outcome}"
        )
        lines.append(
            f"• Tickers: {last_scan.get('tickers_ok', 0)} OK, "
            f"{last_scan.get('tickers_failed', 0)} failed | "
            f"Duration: {last_scan.get('duration_seconds', 0):.1f}s | "
            f"Guilds processed: {len(last_scan.get('guilds', {}))}, "
            f"skipped (disabled): {last_scan.get('guilds_skipped_disabled', 0)}"
        )
    else:
        lines.append("• No scan has completed since the bot started.")
    lines.append("")

    last_error = status.get('last_error')
    lines.append("**Latest Error:**")
    if last_error:
        err_time = last_error.get('time')
        err_time_str = err_time.strftime('%Y-%m-%d %H:%M:%S %Z') if err_time else "?"
        lines.append(f"• {err_time_str} [{last_error.get('source', '?')}]: {last_error.get('error')}")
    else:
        lines.append("• None since startup 🎉")
    lines.append("")

    # Channel health for this guild
    lines.append("**Channel Health (this server):**")
    for name in (OVERSOLD_CHANNEL_NAME, OVERBOUGHT_CHANNEL_NAME, CHANGELOG_CHANNEL_NAME):
        channel = discord.utils.get(interaction.guild.text_channels, name=name)
        if not channel:
            lines.append(f"• #{name}: 🚨 not found")
        elif not channel.permissions_for(interaction.guild.me).send_messages:
            lines.append(f"• #{name}: ⚠️ missing Send Messages permission")
        else:
            lines.append(f"• #{name}: ✅ OK")

    await interaction.followup.send("\n".join(lines), ephemeral=True)


@bot.tree.command(name="reload-catalog", description="Reload the ticker catalog (Admin)")
@app_commands.guild_only()
@app_commands.default_permissions(administrator=True)
async def reload_catalog(interaction: discord.Interaction):
    """Reload the ticker catalog from tickers.csv."""
    await interaction.response.defer(ephemeral=True)

    if not interaction.user.guild_permissions.administrator:
        await interaction.followup.send(
            "❌ **Permission Denied**\nThis command requires Administrator permission.",
            ephemeral=True
        )
        return

    old_count = len(bot.catalog)
    success = bot.catalog.reload()
    new_count = len(bot.catalog)
    
    if success:
        await interaction.followup.send(
            f"✅ **Ticker catalog reloaded**\n"
            f"• Previous count: {old_count}\n"
            f"• New count: {new_count}\n"
            f"• Change: {new_count - old_count:+d}",
            ephemeral=True
        )
    else:
        await interaction.followup.send(
            "❌ Failed to reload ticker catalog. Check the logs for details.",
            ephemeral=True
        )


# ==================== Autocomplete ====================

@subscribe.autocomplete('ticker')
@subscribe_bands.autocomplete('ticker')
@ticker_info.autocomplete('ticker')
@list_subscriptions.autocomplete('ticker')
@remove_ticker_cmd.autocomplete('ticker')
async def ticker_autocomplete(interaction: discord.Interaction, current: str):
    """Autocomplete ticker symbols."""
    if not current:
        return []

    results = bot.catalog.search_tickers(current, limit=25)
    return [
        app_commands.Choice(name=f"{i.ticker} — {i.name[:40]}", value=i.ticker)
        for i in results
    ]


# ==================== Main ====================

def main():
    """Run the bot."""
    if not DISCORD_TOKEN:
        logger.error("DISCORD_TOKEN environment variable not set")
        print("Error: Please set the DISCORD_TOKEN environment variable")
        print("  export DISCORD_TOKEN=your_bot_token")
        print("  python main.py")
        sys.exit(1)

    logger.info("Starting RSI Discord Bot...")
    provider = get_provider()
    logger.info(f"RSI Provider: {provider.name}")
    bot.run(DISCORD_TOKEN)


if __name__ == "__main__":
    main()
