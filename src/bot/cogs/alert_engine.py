"""
Alert Engine module for RSI Discord Bot.
Handles alert trigger logic including crossing detection, cooldown, and hysteresis.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass

from bot.repositories.database import Database, GuildConfig
from bot.services.market_data.rsi_calculator import RSIResult
from bot.repositories.ticker_catalog import get_catalog

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """Represents a triggered alert to be sent."""
    subscription_id: int
    guild_id: int
    channel_id: Optional[int]
    ticker: str
    name: str
    condition: str
    threshold: float
    period: int
    rsi_value: float
    last_date: str
    last_close: float
    tradingview_url: str
    days_in_zone: int
    just_crossed: bool  # True if this is the first day crossing
    previous_rsi: Optional[float] = None


class AlertEngine:
    """
    Evaluates subscriptions against RSI data and determines which alerts to trigger.
    """

    def __init__(self, db: Database):
        self.db = db
        self.catalog = get_catalog()

    async def evaluate_subscriptions(
        self,
        rsi_results: Dict[str, RSIResult],
        dry_run: bool = False
    ) -> Dict[str, List[Alert]]:
        """
        Evaluate all enabled subscriptions against RSI data.
        
        Args:
            rsi_results: Dict mapping ticker -> RSIResult
            dry_run: If True, don't update state or enforce cooldown
        
        Returns:
            Dict with keys 'UNDER' and 'OVER' mapping to lists of triggered alerts
        """
        alerts_by_condition: Dict[str, List[Alert]] = {
            'UNDER': [],
            'OVER': []
        }

        # Get all enabled subscriptions with their state
        subscriptions_data = await self.db.get_subscriptions_with_state()
        logger.info(f"Evaluating {len(subscriptions_data)} subscriptions")

        # Group by guild to get configs
        guild_configs: Dict[int, GuildConfig] = {}

        for sub_data in subscriptions_data:
            guild_id = sub_data['guild_id']

            # Get guild config (cached)
            if guild_id not in guild_configs:
                config = await self.db.get_or_create_guild_config(guild_id)
                guild_configs[guild_id] = config

            config = guild_configs[guild_id]

            alert = await self._evaluate_single_subscription(
                sub_data, rsi_results, config, dry_run
            )

            if alert:
                alerts_by_condition[alert.condition].append(alert)

        # Sort alerts
        # UNDER: lowest RSI first (ascending)
        alerts_by_condition['UNDER'].sort(key=lambda a: a.rsi_value)
        # OVER: highest RSI first (descending)
        alerts_by_condition['OVER'].sort(key=lambda a: -a.rsi_value)

        total = sum(len(a) for a in alerts_by_condition.values())
        logger.info(f"Generated {total} alerts (UNDER: {len(alerts_by_condition['UNDER'])}, OVER: {len(alerts_by_condition['OVER'])})")
        return alerts_by_condition

    async def _evaluate_single_subscription(
        self,
        sub_data: dict,
        rsi_results: Dict[str, RSIResult],
        config: GuildConfig,
        dry_run: bool
    ) -> Optional[Alert]:
        """
        Evaluate a single subscription and return an Alert if triggered.
        """
        ticker = sub_data['ticker']
        subscription_id = sub_data['id']
        condition = sub_data['condition']
        threshold = sub_data['threshold']
        period = sub_data['period']
        cooldown_hours = sub_data['cooldown_hours']
        guild_id = sub_data['guild_id']

        # Get RSI result for this ticker
        rsi_result = rsi_results.get(ticker)
        if not rsi_result or not rsi_result.success:
            logger.debug(f"No RSI data for {ticker}")
            return None

        # Get RSI value for the required period
        current_rsi = rsi_result.rsi_values.get(period)
        if current_rsi is None:
            logger.debug(f"No RSI{period} for {ticker}")
            return None

        # Get previous state
        last_rsi = sub_data.get('last_rsi')
        last_status = sub_data.get('last_status', 'UNKNOWN')
        last_alert_at_str = sub_data.get('last_alert_at')
        last_date = sub_data.get('last_date')
        days_in_zone = sub_data.get('days_in_zone', 0)

        # Parse last_alert_at
        last_alert_at = None
        if last_alert_at_str:
            try:
                last_alert_at = datetime.fromisoformat(last_alert_at_str)
            except ValueError:
                pass

        # Calculate current status
        hysteresis = config.hysteresis
        current_status = self._determine_status(
            current_rsi, threshold, condition, hysteresis
        )

        # Check if this is a new trading day
        new_date = rsi_result.last_date
        is_new_day = (last_date != new_date) if last_date else True

        # Update days_in_zone counter
        just_crossed = False
        if condition == "UNDER":
            if current_rsi < threshold:
                if last_status in ("ABOVE", "UNKNOWN") or days_in_zone == 0:
                    # Just crossed or first evaluation
                    new_days_in_zone = 1
                    just_crossed = True
                else:
                    new_days_in_zone = (days_in_zone + 1) if is_new_day else days_in_zone
            else:
                new_days_in_zone = 0
        else:  # OVER
            if current_rsi > threshold:
                if last_status in ("BELOW", "UNKNOWN") or days_in_zone == 0:
                    # Just crossed or first evaluation
                    new_days_in_zone = 1
                    just_crossed = True
                else:
                    new_days_in_zone = (days_in_zone + 1) if is_new_day else days_in_zone
            else:
                new_days_in_zone = 0

        # Determine if we should trigger an alert
        should_trigger = self._should_trigger_alert(
            condition=condition,
            threshold=threshold,
            current_rsi=current_rsi,
            last_status=last_status,
            current_status=current_status,
            alert_mode=config.alert_mode,
            hysteresis=hysteresis
        )

        # Check cooldown
        if should_trigger and not dry_run:
            if last_alert_at:
                cooldown_expiry = last_alert_at + timedelta(hours=cooldown_hours)
                if datetime.utcnow() < cooldown_expiry:
                    logger.debug(
                        f"Alert for {ticker} (sub {subscription_id}) suppressed by cooldown"
                    )
                    should_trigger = False

        # Update state (unless dry run)
        if not dry_run:
            await self.db.update_subscription_state(
                subscription_id=subscription_id,
                last_rsi=current_rsi,
                last_close=rsi_result.last_close,
                last_date=new_date,
                last_status=current_status,
                days_in_zone=new_days_in_zone,
                last_alert_at=datetime.utcnow() if should_trigger else None
            )

        if should_trigger:
            # Get instrument details
            instrument = self.catalog.get_instrument(ticker)
            name = instrument.name if instrument else ticker
            tradingview_url = instrument.tradingview_url if instrument else ""

            return Alert(
                subscription_id=subscription_id,
                guild_id=guild_id,
                channel_id=None,  # Not used with fixed channels
                ticker=ticker,
                name=name,
                condition=condition,
                threshold=threshold,
                period=period,
                rsi_value=current_rsi,
                last_date=rsi_result.last_date,
                last_close=rsi_result.last_close,
                tradingview_url=tradingview_url,
                days_in_zone=new_days_in_zone,
                just_crossed=just_crossed,
                previous_rsi=last_rsi
            )

        return None

    def _determine_status(
        self,
        rsi: float,
        threshold: float,
        condition: str,
        hysteresis: float
    ) -> str:
        """
        Determine the current status (ABOVE, BELOW, UNKNOWN) with hysteresis.
        """
        if condition == "UNDER":
            # For UNDER condition, we care about crossing below the threshold
            if rsi < threshold - hysteresis:
                return "BELOW"
            elif rsi > threshold + hysteresis:
                return "ABOVE"
            else:
                return "UNKNOWN"  # In hysteresis band
        else:  # OVER
            # For OVER condition, we care about crossing above the threshold
            if rsi > threshold + hysteresis:
                return "ABOVE"
            elif rsi < threshold - hysteresis:
                return "BELOW"
            else:
                return "UNKNOWN"  # In hysteresis band

    def _should_trigger_alert(
        self,
        condition: str,
        threshold: float,
        current_rsi: float,
        last_status: str,
        current_status: str,
        alert_mode: str,
        hysteresis: float
    ) -> bool:
        """
        Determine if an alert should be triggered based on the alert mode.
        """
        if alert_mode == "LEVEL":
            # LEVEL mode: trigger whenever condition is met
            if condition == "UNDER":
                return current_rsi < threshold
            else:  # OVER
                return current_rsi > threshold

        # CROSSING mode (default): only trigger on threshold crossing
        if condition == "UNDER":
            # UNDER triggers when RSI crosses from above to below threshold
            # Previous status was ABOVE (or UNKNOWN on first run), now BELOW
            crossed = (
                last_status in ("ABOVE", "UNKNOWN") and
                current_status == "BELOW"
            )
            # Also trigger if this is first evaluation and already below
            if last_status == "UNKNOWN" and current_rsi < threshold:
                return True
            return crossed

        else:  # OVER
            # OVER triggers when RSI crosses from below to above threshold
            crossed = (
                last_status in ("BELOW", "UNKNOWN") and
                current_status == "ABOVE"
            )
            # Also trigger if this is first evaluation and already above
            if last_status == "UNKNOWN" and current_rsi > threshold:
                return True
            return crossed


def format_single_alert(alert: Alert, index: int) -> str:
    """
    Format a single alert line in the required format.
    
    Example:
    1) **AUSS.OL** — [Austevoll Seafood](https://tradingview.com/...) — RSI14: **79.6** | Rule: **> 70.0** | ⏱️ **day 4**
    """
    # Determine rule symbol
    rule_symbol = "<" if alert.condition == "UNDER" else ">"
    
    # Determine persistence marker
    if alert.just_crossed or alert.days_in_zone <= 1:
        persistence = "🆕 **just crossed**"
    else:
        persistence = f"⏱️ **day {alert.days_in_zone}**"
    
    # Format the line
    line = (
        f"{index}) **{alert.ticker}** — "
        f"[{alert.name}]({alert.tradingview_url}) — "
        f"RSI{alert.period}: **{alert.rsi_value:.1f}** | "
        f"Rule: **{rule_symbol} {alert.threshold}** | "
        f"{persistence}"
    )
    
    return line


def format_alert_list(alerts: List[Alert], condition: str) -> List[str]:
    """
    Format a list of alerts into Discord messages.
    
    Args:
        alerts: List of alerts to format
        condition: 'UNDER' or 'OVER' for header
    
    Returns:
        List of message strings (split to stay under Discord's 2000 char limit)
    """
    if not alerts:
        return []
    
    # Build header
    if condition == "UNDER":
        header = "📉 **RSI Oversold Alerts**\n\n"
    else:
        header = "📈 **RSI Overbought Alerts**\n\n"
    
    messages = []
    current_lines = [header]
    current_length = len(header)
    
    for i, alert in enumerate(alerts, 1):
        line = format_single_alert(alert, i)
        line_with_newline = line + "\n"
        
        # Check if adding this line would exceed limit
        if current_length + len(line_with_newline) > 1900:
            # Finalize current message
            messages.append("".join(current_lines))
            # Start new message with continuation header
            cont_header = f"📊 **Continued ({condition})...**\n\n"
            current_lines = [cont_header]
            current_length = len(cont_header)
        
        current_lines.append(line_with_newline)
        current_length += len(line_with_newline)
    
    # Add final message
    if current_lines:
        messages.append("".join(current_lines))
    
    return messages


def format_no_alerts_message(condition: str) -> str:
    """Format a message when there are no alerts for a condition."""
    if condition == "UNDER":
        return "📉 **RSI Oversold Alerts**\n\nNo stocks currently meeting oversold criteria."
    else:
        return "📈 **RSI Overbought Alerts**\n\nNo stocks currently meeting overbought criteria."


# Legacy function for backwards compatibility
def format_grouped_alerts(alerts: List[Alert]) -> List[str]:
    """
    Legacy function - formats alerts grouped by condition.
    Now delegates to format_alert_list.
    """
    # Group by condition
    by_condition: Dict[str, List[Alert]] = {'UNDER': [], 'OVER': []}
    for alert in alerts:
        by_condition[alert.condition].append(alert)
    
    # Sort each group
    by_condition['UNDER'].sort(key=lambda a: a.rsi_value)
    by_condition['OVER'].sort(key=lambda a: -a.rsi_value)
    
    messages = []
    for condition in ['UNDER', 'OVER']:
        if by_condition[condition]:
            messages.extend(format_alert_list(by_condition[condition], condition))
    
    return messages
