"""
Discord Message Utilities for RSI Discord Bot.

Provides helper functions for safely handling Discord's 2000-character message limit.
"""
from typing import List


def chunk_message(
    content: str,
    max_length: int = 1900,
    split_on: str = "\n",
    continuation_prefix: str = ""
) -> List[str]:
    """
    Split a long message into chunks that fit within Discord's character limit.
    
    Args:
        content: The full message content to split
        max_length: Maximum characters per message (default 1900 for safety margin)
        split_on: Preferred split point (default: newline)
        continuation_prefix: Optional prefix for continuation messages
    
    Returns:
        List of message strings, each under max_length
    """
    if len(content) <= max_length:
        return [content]
    
    chunks = []
    current_chunk = ""
    
    # Split content by the preferred delimiter
    lines = content.split(split_on)
    
    for line in lines:
        line_with_sep = line + split_on
        
        # If adding this line would exceed limit
        if len(current_chunk) + len(line_with_sep) > max_length:
            # If current chunk has content, save it
            if current_chunk:
                chunks.append(current_chunk.rstrip(split_on))
                current_chunk = continuation_prefix
            
            # If single line is too long, force split it
            if len(line_with_sep) > max_length:
                # Split the long line into smaller pieces
                remaining = line
                while len(remaining) > max_length - len(continuation_prefix):
                    split_point = max_length - len(continuation_prefix) - 1
                    chunks.append(continuation_prefix + remaining[:split_point])
                    remaining = remaining[split_point:]
                current_chunk = continuation_prefix + remaining + split_on
            else:
                current_chunk = continuation_prefix + line_with_sep
        else:
            current_chunk += line_with_sep
    
    # Add any remaining content
    if current_chunk.strip():
        chunks.append(current_chunk.rstrip(split_on))
    
    return chunks


def chunk_list_message(
    header: str,
    items: List[str],
    max_length: int = 1900,
    continuation_header: str = "**...continued**\n\n"
) -> List[str]:
    """
    Create chunked messages from a header and list of items.
    
    This is optimized for lists where we don't want to split items mid-way.
    
    Args:
        header: The header text for the first message
        items: List of item strings (each should be under max_length)
        max_length: Maximum characters per message
        continuation_header: Header for continuation messages
    
    Returns:
        List of message strings
    """
    messages = []
    current_message = header
    
    for item in items:
        item_with_newline = item + "\n"
        
        # Check if adding this item exceeds the limit
        if len(current_message) + len(item_with_newline) > max_length:
            # Save current message and start new one
            if current_message != header:  # Only save if we have items
                messages.append(current_message.rstrip("\n"))
            current_message = continuation_header
        
        current_message += item_with_newline
    
    # Add final message if it has content beyond just the header
    if current_message and current_message not in (header, continuation_header):
        messages.append(current_message.rstrip("\n"))
    
    return messages


def format_subscription_list(
    subscriptions: list,
    catalog,
    oversold_channel_name: str,
    overbought_channel_name: str
) -> List[str]:
    """
    Format subscription list with proper chunking.
    
    Args:
        subscriptions: List of Subscription objects
        catalog: TickerCatalog instance for name lookups
        oversold_channel_name: Name of oversold channel
        overbought_channel_name: Name of overbought channel
    
    Returns:
        List of message strings ready to send
    """
    if not subscriptions:
        return ["📋 No subscriptions found"]
    
    # Group by condition
    under_subs = [s for s in subscriptions if s.condition == "UNDER"]
    over_subs = [s for s in subscriptions if s.condition == "OVER"]
    
    # Format each subscription as a line
    all_lines = []
    
    if under_subs:
        all_lines.append(f"**#{oversold_channel_name}** (UNDER/Oversold):")
        for sub in under_subs:
            instrument = catalog.get_instrument(sub.ticker)
            name = instrument.name if instrument else sub.ticker
            all_lines.append(
                f"`{sub.id}` — **{sub.ticker}** ({name}) "
                f"| RSI{sub.period} < {sub.threshold}"
            )
        all_lines.append("")  # Empty line separator
    
    if over_subs:
        all_lines.append(f"**#{overbought_channel_name}** (OVER/Overbought):")
        for sub in over_subs:
            instrument = catalog.get_instrument(sub.ticker)
            name = instrument.name if instrument else sub.ticker
            all_lines.append(
                f"`{sub.id}` — **{sub.ticker}** ({name}) "
                f"| RSI{sub.period} > {sub.threshold}"
            )
        all_lines.append("")
    
    header = f"📋 **Subscriptions** ({len(subscriptions)} total)\n\n"
    content = header + "\n".join(all_lines)
    
    return chunk_message(content, continuation_prefix="**...continued**\n\n")
