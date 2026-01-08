#!/usr/bin/env python3
"""RSI Discord Bot - Main Entry Point

TradingView-only build (RSI14 via TradingView Screener).

Usage:
    export DISCORD_TOKEN=your_bot_token
    export PYTHONPATH=src
    python -m bot.main
"""
import logging
import sys
from typing import Optional, Tuple, List
from pathlib import Path

import discord
from discord import app_commands
from discord.ext import commands


from bot.config import (
    DISCORD_TOKEN, DEFAULT_OVERSOLD_THRESHOLD,
    DEFAULT_OVERBOUGHT_THRESHOLD, OVERSOLD_CHANNEL_NAME, OVERBOUGHT_CHANNEL_NAME,
    CHANGELOG_CHANNEL_NAME, REQUEST_CHANNEL_NAME, LOG_PATH,
    DISCORD_SAFE_LIMIT
)
from bot.repositories.database import Database
from bot.repositories.ticker_catalog import get_catalog, validate_ticker
from bot.services.market_data.rsi_calculator import RSICalculator
from bot.services.market_data.providers import get_provider
from bot.cogs.alert_engine import AlertEngine, format_alert_list, format_no_alerts_message
from bot.services.scheduler import RSIScheduler
from bot.cogs.ticker_request import TickerRequestCog, handle_request_message
from bot.utils.message_utils import chunk_message, format_subscription_list

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
        self.rsi_calculator = RSICalculator()
        self.alert_engine = AlertEngine(self.db)
        self.scheduler: Optional[RSIScheduler] = None
        self.ticker_request_handler = TickerRequestCog(self)
        self.health_runner = None

    async def setup_hook(self):
        """Initialize bot components."""
        logger.info("Initializing database...")
        await self.db.initialize()

        logger.info("Loading ticker catalog...")
        self.catalog.load()

        # Log provider info
        provider = get_provider()
        logger.info(f"RSI Data Provider: {provider.name}")

        logger.info("Starting scheduler...")
        self.scheduler = RSIScheduler(self)
        await self.scheduler.start()

        logger.info("Syncing slash commands...")
        await self.tree.sync()

        logger.info("Bot setup complete")

    async def on_ready(self):
        """Called when bot is ready."""
        logger.info(f"Logged in as {self.user} (ID: {self.user.id})")
        logger.info(f"Connected to {len(self.guilds)} guilds")
        logger.info(f"Ticker catalog contains {len(self.catalog)} instruments")
        provider = get_provider()
        logger.info(f"RSI Provider: {provider.name}")
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="RSI levels"
            )
        )

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
        if self.scheduler:
            self.scheduler.stop()
        if self.health_runner:
            await self.health_runner.cleanup()
        await super().close()


# Create bot instance
bot = RSIBot()


# ==================== Slash Commands ====================

@bot.tree.command(name="subscribe", description="Create an RSI alert subscription")
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

    if period is not None and period != 14:
        await interaction.followup.send("❌ Only RSI14 (period=14) is supported in this TradingView-only build", ephemeral=True)
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

    if period is not None and period != 14:
        await interaction.followup.send("❌ Only RSI14 (period=14) is supported in this TradingView-only build", ephemeral=True)
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


@bot.tree.command(name="list", description="List RSI alert subscriptions")
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
@app_commands.default_permissions(manage_guild=True)
async def run_now(interaction: discord.Interaction):
    """Manually trigger RSI evaluation."""
    await interaction.response.defer(ephemeral=True)

    oversold_ch, overbought_ch, error_msg = get_alert_channels(interaction.guild)
    if error_msg:
        await interaction.followup.send(error_msg, ephemeral=True)
        return

    subs = await bot.db.get_subscriptions_by_guild(guild_id=interaction.guild_id, enabled_only=True)

    if not subs:
        await interaction.followup.send("📋 No active subscriptions in this server", ephemeral=True)
        return

    ticker_periods = {}
    for sub in subs:
        if sub.ticker not in ticker_periods:
            ticker_periods[sub.ticker] = []
        if sub.period not in ticker_periods[sub.ticker]:
            ticker_periods[sub.ticker].append(sub.period)

    provider = get_provider()
    await interaction.followup.send(
        f"⏳ Fetching RSI data for {len(ticker_periods)} tickers using {provider.name}...",
        ephemeral=True
    )

    rsi_results = await bot.rsi_calculator.calculate_rsi_for_tickers(ticker_periods)
    alerts_by_condition = await bot.alert_engine.evaluate_subscriptions(rsi_results, dry_run=False)

    successful = sum(1 for r in rsi_results.values() if r.success)
    failed = len(rsi_results) - successful
    under_count = len(alerts_by_condition.get('UNDER', []))
    over_count = len(alerts_by_condition.get('OVER', []))

    messages_sent = 0
    send_errors = []

    under_alerts = alerts_by_condition.get('UNDER', [])
    try:
        if under_alerts:
            messages = format_alert_list(under_alerts, 'UNDER')
            for msg in messages:
                await oversold_ch.send(msg)
                messages_sent += 1
        else:
            await oversold_ch.send(format_no_alerts_message('UNDER'))
            messages_sent += 1
    except discord.Forbidden:
        send_errors.append(f"Cannot send to {oversold_ch.mention} - missing permissions")
    except Exception as e:
        send_errors.append(f"Error sending to {oversold_ch.mention}: {str(e)}")

    over_alerts = alerts_by_condition.get('OVER', [])
    try:
        if over_alerts:
            messages = format_alert_list(over_alerts, 'OVER')
            for msg in messages:
                await overbought_ch.send(msg)
                messages_sent += 1
        else:
            await overbought_ch.send(format_no_alerts_message('OVER'))
            messages_sent += 1
    except discord.Forbidden:
        send_errors.append(f"Cannot send to {overbought_ch.mention} - missing permissions")
    except Exception as e:
        send_errors.append(f"Error sending to {overbought_ch.mention}: {str(e)}")

    summary = (
        f"✅ **RSI Check Complete**\n"
        f"• **Provider:** {provider.name}\n"
        f"• Tickers fetched: {successful} success, {failed} failed\n"
        f"• Subscriptions evaluated: {len(subs)}\n"
        f"• Alerts triggered: {under_count + over_count}\n"
        f"  - {oversold_ch.mention}: {under_count} oversold alerts\n"
        f"  - {overbought_ch.mention}: {over_count} overbought alerts\n"
        f"• Messages sent: {messages_sent}"
    )
    
    if send_errors:
        summary += "\n\n⚠️ **Errors:**\n" + "\n".join(f"• {e}" for e in send_errors)

    await interaction.edit_original_response(content=summary)


@bot.tree.command(name="set-defaults", description="Set server defaults (Admin)")
@app_commands.default_permissions(manage_guild=True)
@app_commands.describe(
    default_period="Default RSI period (must be 14)",
    default_cooldown="Default cooldown hours",
    schedule_time="Daily run time in HH:MM format (Europe/Oslo)",
    alert_mode="Alert mode: CROSSING or LEVEL",
    hysteresis="Hysteresis value for crossing detection",
    auto_oversold="Auto-scan oversold threshold (default: 34)",
    auto_overbought="Auto-scan overbought threshold (default: 70)"
)
@app_commands.choices(alert_mode=[
    app_commands.Choice(name="CROSSING", value="CROSSING"),
    app_commands.Choice(name="LEVEL", value="LEVEL")
])
async def set_defaults(
    interaction: discord.Interaction,
    default_period: Optional[int] = None,
    default_cooldown: Optional[int] = None,
    schedule_time: Optional[str] = None,
    alert_mode: Optional[app_commands.Choice[str]] = None,
    hysteresis: Optional[float] = None,
    auto_oversold: Optional[float] = None,
    auto_overbought: Optional[float] = None
):
    """Set server-level default configuration including auto-scan thresholds."""
    await interaction.response.defer(ephemeral=True)

    if default_period is not None and default_period != 14:
        await interaction.followup.send("❌ Only RSI14 (period=14) is supported in this TradingView-only build", ephemeral=True)
        return

    if default_cooldown is not None and default_cooldown < 0:
        await interaction.followup.send("❌ Cooldown must be non-negative", ephemeral=True)
        return

    if schedule_time is not None:
        try:
            parts = schedule_time.split(":")
            hour, minute = int(parts[0]), int(parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError()
        except (ValueError, IndexError):
            await interaction.followup.send("❌ Schedule time must be in HH:MM format (e.g., 18:30)", ephemeral=True)
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

    config = await bot.db.update_guild_config(
        guild_id=interaction.guild_id,
        default_rsi_period=default_period,
        default_schedule_time=schedule_time,
        default_cooldown_hours=default_cooldown,
        alert_mode=alert_mode.value if alert_mode else None,
        hysteresis=hysteresis,
        auto_oversold_threshold=auto_oversold,
        auto_overbought_threshold=auto_overbought
    )

    await interaction.followup.send(
        f"✅ **Server defaults updated**\n"
        f"• **Default RSI period:** {config.default_rsi_period}\n"
        f"• **Default cooldown:** {config.default_cooldown_hours} hours\n"
        f"• **Schedule time:** {config.default_schedule_time} (Europe/Oslo)\n"
        f"• **Alert mode:** {config.alert_mode}\n"
        f"• **Hysteresis:** {config.hysteresis}\n\n"
        f"**Auto-Scan Thresholds:**\n"
        f"• **Oversold:** < {config.auto_oversold_threshold}\n"
        f"• **Overbought:** > {config.auto_overbought_threshold}\n\n"
        f"**Fixed alert channels:**\n"
        f"• Oversold (UNDER): `#{OVERSOLD_CHANNEL_NAME}`\n"
        f"• Overbought (OVER): `#{OVERBOUGHT_CHANNEL_NAME}`",
        ephemeral=True
    )


@bot.tree.command(name="ticker-info", description="Get information about a ticker")
@app_commands.describe(ticker="Stock ticker symbol to look up")
async def ticker_info(interaction: discord.Interaction, ticker: str):
    """Get information about a ticker from the catalog."""
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

    lines = [
        f"**{instrument.ticker} — {instrument.name}**",
        f"🔗 [TradingView]({instrument.tradingview_url})",
        ""
    ]

    # Get subscriptions for this ticker
    subs = await bot.db.get_subscriptions_by_guild(guild_id=interaction.guild_id, ticker=ticker)

    # Get RSI data
    rsi_data = None
    if subs:
        for sub in subs:
            state = await bot.db.get_subscription_state(sub.id)
            if state and state.last_rsi is not None and state.last_date:
                from datetime import datetime
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
            lines.append(f"⚠️ **RSI Data (STALE - {rsi_data['days_old']} days old):**")
        else:
            lines.append("📊 **RSI Data:**")
        lines.append(f"• RSI{rsi_data['period']}: **{rsi_data['rsi']:.1f}**")
        lines.append(f"• Last Close: {rsi_data['close']:.2f} ({rsi_data['date']})")
        lines.append("")
    else:
        lines.append("📊 **RSI Data:** Not yet checked")
        lines.append("")

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

    await interaction.followup.send("\n".join(lines), ephemeral=True)


@bot.tree.command(name="catalog-stats", description="Show ticker catalog and subscription statistics")
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
        f"**Alert Channels:**\n"
        f"• `#{OVERSOLD_CHANNEL_NAME}` — UNDER alerts\n"
        f"• `#{OVERBOUGHT_CHANNEL_NAME}` — OVER alerts",
        ephemeral=True
    )


@bot.tree.command(name="reload-catalog", description="Reload the ticker catalog (Admin)")
@app_commands.default_permissions(administrator=True)
async def reload_catalog(interaction: discord.Interaction):
    """Reload the ticker catalog from tickers.csv."""
    await interaction.response.defer(ephemeral=True)
    
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
