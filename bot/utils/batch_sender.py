
"""
Batch Sender Utility - Handles Discord rate limits by batching embed sends
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict
import discord

logger = logging.getLogger(__name__)

class BatchSender:
    """
    Batches Discord embeds and sends them in controlled intervals to avoid rate limits
    """

    def __init__(self, bot):
        self.bot = bot
        self.message_queues: Dict[int, List[Dict[str, Any]]] = defaultdict(list)  # channel_id -> messages
        self.batch_size = 10  # Send up to 10 embeds per batch
        self.batch_interval = 2.0  # Wait 2 seconds between batches
        self.max_queue_size = 100  # Maximum messages per channel queue
        self.processing_channels: set = set()  # Track channels being processed
        
    async def queue_embed(self, channel_id: int, embed: discord.Embed, file: discord.File = None, content: str = None):
        """Queue an embed to be sent in batch"""
        try:
            if len(self.message_queues[channel_id]) >= self.max_queue_size:
                logger.warning(f"Message queue for channel {channel_id} is full, dropping message")
                return
                
            message_data = {
                'embed': embed,
                'file': file,
                'content': content,
                'timestamp': datetime.now(timezone.utc)
            }
            
            self.message_queues[channel_id].append(message_data)
            
            # Start processing this channel if not already processing
            if channel_id not in self.processing_channels:
                asyncio.create_task(self._process_channel_queue(channel_id))
                
        except Exception as e:
            logger.error(f"Failed to queue embed for channel {channel_id}: {e}")

    async def _process_channel_queue(self, channel_id: int):
        """Process the message queue for a specific channel"""
        if channel_id in self.processing_channels:
            return
            
        self.processing_channels.add(channel_id)
        
        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                logger.warning(f"Channel {channel_id} not found, clearing queue")
                self.message_queues[channel_id].clear()
                return
                
            while self.message_queues[channel_id]:
                # Get batch of messages
                batch = []
                for _ in range(min(self.batch_size, len(self.message_queues[channel_id]))):
                    if self.message_queues[channel_id]:
                        batch.append(self.message_queues[channel_id].pop(0))
                
                if not batch:
                    break
                    
                # Send batch with rate limit handling
                await self._send_batch(channel, batch)
                
                # Wait between batches
                if self.message_queues[channel_id]:  # More messages waiting
                    await asyncio.sleep(self.batch_interval)
                    
        except Exception as e:
            logger.error(f"Error processing queue for channel {channel_id}: {e}")
        finally:
            self.processing_channels.discard(channel_id)

    async def _send_batch(self, channel: discord.TextChannel, batch: List[Dict[str, Any]]):
        """Send a batch of messages with rate limit handling"""
        for message_data in batch:
            try:
                kwargs = {}
                if message_data['embed']:
                    kwargs['embed'] = message_data['embed']
                if message_data['file']:
                    kwargs['file'] = message_data['file']
                if message_data['content']:
                    kwargs['content'] = message_data['content']
                    
                await channel.send(**kwargs)
                
                # Small delay between individual messages in batch
                await asyncio.sleep(0.1)
                
            except discord.HTTPException as e:
                if e.status == 429:  # Rate limited
                    retry_after = getattr(e, 'retry_after', 5.0)
                    logger.warning(f"Rate limited, waiting {retry_after} seconds")
                    await asyncio.sleep(retry_after)
                    # Retry this message
                    try:
                        await channel.send(**kwargs)
                    except Exception as retry_error:
                        logger.error(f"Failed to send message after rate limit retry: {retry_error}")
                else:
                    logger.error(f"Failed to send message: {e}")
            except Exception as e:
                logger.error(f"Unexpected error sending message: {e}")

    async def flush_all_queues(self):
        """Force process all queues immediately (useful for shutdown)"""
        tasks = []
        for channel_id in list(self.message_queues.keys()):
            if self.message_queues[channel_id] and channel_id not in self.processing_channels:
                tasks.append(asyncio.create_task(self._process_channel_queue(channel_id)))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def get_queue_stats(self) -> Dict[str, Any]:
        """Get current queue statistics"""
        total_queued = sum(len(queue) for queue in self.message_queues.values())
        active_channels = len([q for q in self.message_queues.values() if q])
        
        return {
            'total_queued_messages': total_queued,
            'active_channels': active_channels,
            'processing_channels': len(self.processing_channels),
            'queues_by_channel': {str(ch_id): len(queue) for ch_id, queue in self.message_queues.items() if queue}
        }
