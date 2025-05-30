
"""
Emerald's Killfeed - Channel Router Utility
Centralized channel routing logic with server-specific fallbacks
"""

import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class ChannelRouter:
    """Centralized channel routing with server-specific fallback logic"""
    
    def __init__(self, bot):
        self.bot = bot
    
    async def get_channel_id(self, guild_id: int, server_id: str, channel_type: str) -> Optional[int]:
        """
        Get channel ID with server-specific fallback logic
        
        Priority:
        1. Server-specific channel (server_channels.{server_id}.{channel_type})
        2. Default server channel (server_channels.default.{channel_type})
        3. Legacy channel (channels.{channel_type})
        """
        try:
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                logger.debug(f"No guild config found for guild {guild_id}")
                return None

            server_channels = guild_config.get('server_channels', {})
            
            # Try server-specific channel first
            if server_id in server_channels:
                channel_id = server_channels[server_id].get(channel_type)
                if channel_id:
                    logger.debug(f"Using server-specific {channel_type} channel {channel_id} for server {server_id}")
                    return channel_id
            
            # Fall back to default server channels
            if 'default' in server_channels:
                channel_id = server_channels['default'].get(channel_type)
                if channel_id:
                    logger.debug(f"Using default {channel_type} channel {channel_id} for server {server_id}")
                    return channel_id
            
            # Legacy fallback to old channel structure
            legacy_channel_id = guild_config.get('channels', {}).get(channel_type)
            if legacy_channel_id:
                logger.debug(f"Using legacy {channel_type} channel {legacy_channel_id} for server {server_id}")
                return legacy_channel_id
            
            logger.debug(f"No {channel_type} channel configured for guild {guild_id}, server {server_id}")
            return None
            
        except Exception as e:
            logger.error(f"Failed to get {channel_type} channel for guild {guild_id}, server {server_id}: {e}")
            return None
    
    async def get_channel(self, guild_id: int, server_id: str, channel_type: str):
        """Get Discord channel object with server-specific fallback logic"""
        channel_id = await self.get_channel_id(guild_id, server_id, channel_type)
        if not channel_id:
            return None
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            logger.warning(f"Channel {channel_id} not found for {channel_type}")
            return None
        
        return channel
    
    async def send_embed_to_channel(self, guild_id: int, server_id: str, channel_type: str, embed, file=None):
        """Send embed to appropriate channel with server-specific routing"""
        try:
            channel = await self.get_channel(guild_id, server_id, channel_type)
            if not channel:
                return False
            
            # Queue embed with batch sender to avoid rate limits
            await self.bot.batch_sender.queue_embed(
                channel_id=channel.id,
                embed=embed,
                file=file
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to send embed to {channel_type} channel: {e}")
            return False
