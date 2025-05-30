"""
Admin Batch Management Cog - Monitor and control batch sender
"""

import discord
from discord.ext import commands
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class AdminBatch(commands.Cog):
    """Admin commands for batch sender management"""

    def __init__(self, bot):
        self.bot = bot

    @discord.slash_command(name="batch_stats", description="Show batch sender statistics")
    @commands.has_permissions(administrator=True)
    async def batch_stats(self, ctx: discord.ApplicationContext):
        """Show current batch sender statistics"""
        try:
            if not hasattr(self.bot, 'batch_sender'):
                await ctx.respond("❌ Batch sender not initialized", ephemeral=True)
                return

            stats = self.bot.batch_sender.get_queue_stats()

            embed = discord.Embed(
                title="📊 Batch Sender Statistics",
                color=0x00FF00,
                timestamp=discord.utils.utcnow()
            )

            embed.add_field(
                name="📈 Queue Stats",
                value=f"**Total Queued:** {stats['total_queued_messages']}\n"
                      f"**Active Channels:** {stats['active_channels']}\n"
                      f"**Processing Channels:** {stats['processing_channels']}",
                inline=False
            )

            if stats['queues_by_channel']:
                channel_list = []
                for ch_id, count in list(stats['queues_by_channel'].items())[:10]:  # Show top 10
                    channel = self.bot.get_channel(int(ch_id))
                    channel_name = channel.name if channel else f"Channel {ch_id}"
                    channel_list.append(f"#{channel_name}: {count}")

                embed.add_field(
                    name="📋 Channel Queues",
                    value='\n'.join(channel_list) if channel_list else "No queued messages",
                    inline=False
                )

            await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error in batch_stats command: {e}")
            await ctx.respond(f"❌ Error getting batch stats: {e}", ephemeral=True)

    @discord.slash_command(name="flush_batches", description="Force flush all pending message batches")
    @commands.has_permissions(administrator=True)
    async def flush_batches(self, ctx: discord.ApplicationContext):
        """Force flush all pending message batches"""
        try:
            if not hasattr(self.bot, 'batch_sender'):
                await ctx.respond("❌ Batch sender not initialized", ephemeral=True)
                return

            stats_before = self.bot.batch_sender.get_queue_stats()
            await ctx.respond("🔄 Flushing all message batches...", ephemeral=True)

            await self.bot.batch_sender.flush_all_queues()

            stats_after = self.bot.batch_sender.get_queue_stats()

            embed = discord.Embed(
                title="✅ Batch Flush Complete",
                color=0x00FF00,
                timestamp=discord.utils.utcnow()
            )

            embed.add_field(
                name="📊 Results",
                value=f"**Messages Flushed:** {stats_before['total_queued_messages']}\n"
                      f"**Remaining Queued:** {stats_after['total_queued_messages']}",
                inline=False
            )

            await ctx.edit(embed=embed)

        except Exception as e:
            logger.error(f"Error in flush_batches command: {e}")
            await ctx.respond(f"❌ Error flushing batches: {e}", ephemeral=True)

    @discord.slash_command(name="debug_player_count", description="Debug current player count tracking")
    @commands.has_permissions(administrator=True)
    async def debug_player_count(self, ctx: discord.ApplicationContext, server_id: str = None):
        """Debug current player count tracking"""
        try:
            await ctx.defer(ephemeral=True)

            guild_id = ctx.guild_id

            # Get all servers for this guild
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                await ctx.respond("❌ No guild configuration found", ephemeral=True)
                return

            servers = guild_config.get('servers', [])
            if not servers:
                await ctx.respond("❌ No servers configured for this guild", ephemeral=True)
                return

            embed = discord.Embed(
                title="🐛 Player Count Debug Information",
                color=0xff9900,
                timestamp=discord.utils.utcnow()
            )

            # Check intelligent connection parser
            if hasattr(self.bot, 'log_parser') and hasattr(self.bot.log_parser, 'connection_parser'):
                connection_parser = self.bot.log_parser.connection_parser

                for server_config in servers:
                    server_name = server_config.get('name', 'Unknown')
                    current_server_id = str(server_config.get('_id', 'unknown'))

                    # Skip if specific server requested and this isn't it
                    if server_id and current_server_id != server_id:
                        continue

                    server_key = f"{guild_id}_{current_server_id}"

                    # Get current stats
                    stats = connection_parser.get_server_stats(server_key)

                    # Debug the state
                    connection_parser.debug_server_state(server_key)

                    embed.add_field(
                        name=f"🖥️ {server_name} (ID: {current_server_id})",
                        value=f"**Queue Count:** {stats.get('queue_count', 0)}\n"
                              f"**Player Count:** {stats.get('player_count', 0)}\n"
                              f"**Server Key:** `{server_key}`",
                        inline=False
                    )

                # Check if specific server was requested but not found
                if server_id:
                    found_server = any(str(s.get('_id', '')) == server_id for s in servers)
                    if not found_server:
                        embed.add_field(
                            name="❌ Server Not Found",
                            value=f"Server ID `{server_id}` not found in guild configuration",
                            inline=False
                        )

            else:
                embed.add_field(
                    name="❌ Parser Not Available",
                    value="Connection parser not found or not initialized",
                    inline=False
                )

            await ctx.respond(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"Error in debug_player_count command: {e}")
            await ctx.respond(f"❌ Error debugging player count: {str(e)}", ephemeral=True)

    @discord.slash_command(name="reset_player_count", description="Reset player count tracking for a server")
    @commands.has_permissions(administrator=True)
    async def reset_player_count(self, ctx: discord.ApplicationContext, server_id: str):
        """Reset player count tracking for a server"""
        try:
            guild_id = ctx.guild_id
            server_key = f"{guild_id}_{server_id}"

            # Reset in intelligent connection parser
            if hasattr(self.bot, 'log_parser') and hasattr(self.bot.log_parser, 'connection_parser'):
                connection_parser = self.bot.log_parser.connection_parser
                connection_parser.reset_server_counts(server_key)

                embed = discord.Embed(
                    title="🔄 Player Count Reset",
                    description=f"Player count tracking has been reset for server `{server_id}`",
                    color=0x00ff88,
                    timestamp=discord.utils.utcnow()
                )

                embed.add_field(
                    name="📊 New Counts",
                    value="**Queue Count:** 0\n**Player Count:** 0",
                    inline=False
                )

                await ctx.respond(embed=embed, ephemeral=True)
            else:
                await ctx.respond("❌ Connection parser not available for reset", ephemeral=True)

        except Exception as e:
            logger.error(f"Error in reset_player_count command: {e}")
            await ctx.respond(f"❌ Error resetting player count: {e}", ephemeral=True)

def setup(bot):
    bot.add_cog(AdminBatch(bot))