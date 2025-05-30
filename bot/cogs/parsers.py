"""
Emerald's Killfeed - Parser Management System
Manage killfeed parsing, log processing, and data collection
"""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import discord
from discord.ext import commands
from bot.cogs.autocomplete import ServerAutocomplete
from discord import Option
#from discord import app_commands # Removed app_commands import, not needed for py-cord 2.6.1

logger = logging.getLogger(__name__)

class Parsers(commands.Cog):
    """
    PARSER MANAGEMENT
    - Killfeed parser controls
    - Log processing management
    - Data collection status
    """

    def __init__(self, bot):
        self.bot = bot

    # Create subcommand group using SlashCommandGroup
    parser = discord.SlashCommandGroup("parser", "Parser management commands")

    @parser.command(name="status", description="Check parser status")
    async def parser_status(self, ctx: discord.ApplicationContext):
        """Check the status of all parsers"""
        try:
            embed = discord.Embed(
                title="🔍 Parser Status",
                description="Current status of all data parsers",
                color=0x3498DB,
                timestamp=datetime.now(timezone.utc)
            )

            # Killfeed parser status
            killfeed_status = "🟢 Active" if hasattr(self.bot, 'killfeed_parser') and self.bot.killfeed_parser else "🔴 Inactive"

            # Log parser status
            log_status = "🟢 Active" if hasattr(self.bot, 'log_parser') and self.bot.log_parser else "🔴 Inactive"

            # Historical parser status
            historical_status = "🟢 Active" if hasattr(self.bot, 'historical_parser') and self.bot.historical_parser else "🔴 Inactive"

            embed.add_field(
                name="📡 Killfeed Parser",
                value=f"Status: **{killfeed_status}**\nMonitors live PvP events",
                inline=True
            )

            embed.add_field(
                name="📜 Log Parser",
                value=f"Status: **{log_status}**\nProcesses server log files",
                inline=True
            )

            embed.add_field(
                name="📚 Historical Parser",
                value=f"Status: **{historical_status}**\nRefreshes historical data",
                inline=True
            )

            # Scheduler status
            scheduler_status = "🟢 Running" if self.bot.scheduler.running else "🔴 Stopped"
            embed.add_field(
                name="⏰ Background Scheduler",
                value=f"Status: **{scheduler_status}**\nManages automated tasks",
                inline=False
            )

            embed.set_thumbnail(url="attachment://main.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)

        except Exception as e:
            logger.error(f"Failed to check parser status: {e}")
            await ctx.respond("❌ Failed to retrieve parser status.", ephemeral=True)

    @parser.command(name="refresh", description="Manually refresh data for a server")
    @commands.has_permissions(administrator=True)
    @discord.option(
        name="server",
        description="Select a server",
        autocomplete=ServerAutocomplete.autocomplete_server_name
    )
    async def parser_refresh(self, ctx: discord.ApplicationContext, server: str = "default"):
        """Manually trigger a data refresh for a server"""
        try:
            guild_id = ctx.guild.id

            # Check if server exists in guild config - fixed database call
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                await ctx.respond("❌ This guild is not configured!", ephemeral=True)
                return

            # Find the server - now using server ID from autocomplete
            servers = guild_config.get('servers', [])
            server_found = False
            server_name = "Unknown"
            for srv in servers:
                if str(srv.get('_id')) == server:
                    server_found = True
                    server_name = srv.get('name', 'Unknown')
                    break

            if not server_found:
                await ctx.respond(f"❌ Server not found in this guild!", ephemeral=True)
                return

            # Defer response for potentially long operation
            await ctx.defer()

            # Trigger historical refresh if parser is available
            if hasattr(self.bot, 'historical_parser') and self.bot.historical_parser:
                try:
                    await self.bot.historical_parser.refresh_historical_data(guild_id, server)

                    embed = discord.Embed(
                        title="🔄 Data Refresh Initiated",
                        description=f"Historical data refresh started for server **{server_name}**",
                        color=0x00FF00,
                        timestamp=datetime.now(timezone.utc)
                    )

                    embed.add_field(
                        name="⏰ Duration",
                        value="This process may take several minutes",
                        inline=True
                    )

                    embed.add_field(
                        name="📊 Data Updated",
                        value="• Player statistics\n• Kill/death records\n• Historical trends",
                        inline=True
                    )

                    embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

                    await ctx.followup.send(embed=embed)

                except Exception as e:
                    logger.error(f"Failed to refresh data: {e}")
                    await ctx.followup.send("❌ Failed to start data refresh. Please try again later.")
            else:
                await ctx.followup.send("❌ Historical parser is not available!")

        except Exception as e:
            logger.error(f"Failed to refresh parser data: {e}")
            await ctx.respond("❌ Failed to initiate data refresh.", ephemeral=True)

    @parser.command(name="stats", description="Show parser statistics")
    async def parser_stats(self, ctx: discord.ApplicationContext):
        """Display parser performance statistics"""
        try:
            guild_id = ctx.guild.id

            embed = discord.Embed(
                title="📊 Parser Statistics",
                description="Performance metrics for data parsers",
                color=0x9B59B6,
                timestamp=datetime.now(timezone.utc)
            )

            # Get recent parsing stats from database - fixed database calls
            try:
                # Count recent killfeed entries (last 24 hours)
                recent_kills = await self.bot.db_manager.killfeed.count_documents({
                    'guild_id': guild_id,
                    'timestamp': {'$gte': datetime.now(timezone.utc).replace(hour=0, minute=0, second=0)}
                })

                # Count total players tracked
                total_players = await self.bot.db_manager.pvp_data.count_documents({
                    'guild_id': guild_id
                })

                # Count linked players
                linked_players = await self.bot.db_manager.players.count_documents({
                    'guild_id': guild_id
                })

                embed.add_field(
                    name="📈 Today's Activity",
                    value=f"• Kills Parsed: **{recent_kills}**\n• Players Tracked: **{total_players}**\n• Linked Users: **{linked_players}**",
                    inline=True
                )

                # Parser uptime
                uptime_status = "🟢 Operational" if self.bot.scheduler.running else "🔴 Down"
                embed.add_field(
                    name="⚡ System Health",
                    value=f"• Parser Status: **{uptime_status}**\n• Database: **🟢 Connected**\n• Scheduler: **🟢 Active**",
                    inline=True
                )

            except Exception as e:
                logger.error(f"Failed to get parser stats from database: {e}")
                embed.add_field(
                    name="⚠️ Statistics",
                    value="Unable to retrieve detailed statistics",
                    inline=False
                )

            embed.set_thumbnail(url="attachment://main.png")
            embed.set_footer(text="Powered by Discord.gg/EmeraldServers")

            await ctx.respond(embed=embed)

        except Exception as e:
            logger.error(f"Failed to show parser stats: {e}")
            await ctx.respond("❌ Failed to retrieve parser statistics.", ephemeral=True)

    @discord.slash_command(name="parse_historical", description="Parse historical data from CSV files")
    @commands.has_permissions(administrator=True)
    async def parse_historical(self, ctx: discord.ApplicationContext):
        """Parse historical data from CSV files"""
        try:
            if not self.bot.historical_parser:
                await ctx.respond("❌ Historical parser not initialized", ephemeral=True)
                return

            await ctx.defer()

            # Run historical parser
            await self.bot.historical_parser.run_historical_parser()

            embed = discord.Embed(
                title="📊 Historical Parser",
                description="Historical data parsing completed successfully",
                color=0x00FF00,
                timestamp=datetime.now(timezone.utc)
            )

            await ctx.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Historical parsing failed: {e}")
            await ctx.followup.send("❌ Historical parsing failed", ephemeral=True)

    @discord.slash_command(name="resetlogparser", description="Reset log parser state and player counts")
    @commands.has_permissions(administrator=True)
    async def resetlogparser(self, ctx: discord.ApplicationContext, 
                            server_id: Option(str, "Server ID to reset (leave empty for all)", required=False) = None):
        """Reset log parser state and player counts"""
        await ctx.defer()

        try:
            if not hasattr(self.bot, 'log_parser'):
                await ctx.followup.send("❌ Log parser not initialized")
                return

            guild_id = ctx.guild.id
            reset_count = 0

            if server_id:
                # Reset specific server
                server_key = f"{guild_id}_{server_id}"

                # Reset connection parser
                if hasattr(self.bot.log_parser, 'connection_parser'):
                    self.bot.log_parser.connection_parser.reset_server_counts(server_key)

                # Reset file states
                if server_key in self.bot.log_parser.file_states:
                    del self.bot.log_parser.file_states[server_key]

                # Reset legacy position tracking
                if server_key in self.bot.log_parser.last_log_position:
                    del self.bot.log_parser.last_log_position[server_key]

                reset_count = 1
                logger.info(f"Reset log parser for server {server_id} in guild {guild_id}")
            else:
                # Reset all servers for this guild
                guild_prefix = f"{guild_id}_"

                # Reset connection parser for all servers
                if hasattr(self.bot.log_parser, 'connection_parser'):
                    connection_parser = self.bot.log_parser.connection_parser
                    servers_to_reset = [k for k in connection_parser.server_counts.keys() if k.startswith(guild_prefix)]
                    for server_key in servers_to_reset:
                        connection_parser.reset_server_counts(server_key)
                        reset_count += 1

                # Reset file states
                keys_to_remove = [k for k in self.bot.log_parser.file_states.keys() if k.startswith(guild_prefix)]
                for key in keys_to_remove:
                    del self.bot.log_parser.file_states[key]

                # Reset legacy position tracking  
                keys_to_remove = [k for k in self.bot.log_parser.last_log_position.keys() if k.startswith(guild_prefix)]
                for key in keys_to_remove:
                    del self.bot.log_parser.last_log_position[key]

            # Create success embed
            embed = discord.Embed(
                title="🔄 Log Parser Reset",
                description=f"Successfully reset log parser state for {reset_count} server{'s' if reset_count != 1 else ''}",
                color=0x00FF00,
                timestamp=datetime.now(timezone.utc)
            )

            embed.add_field(
                name="What was reset:",
                value="• Player count tracking\n• Connection states\n• File position tracking\n• Parser will restart from current log position",
                inline=False
            )

            await ctx.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Failed to reset log parser: {e}")
            await ctx.followup.send(f"❌ Failed to reset: {str(e)}")

    @discord.slash_command(name="investigate_playercount", description="Deep investigation of player count issues")
    async def investigate_playercount(self, ctx: discord.ApplicationContext, 
                                     server_id: Option(str, "Specific server ID to investigate", required=False) = None):
        """Comprehensive player count investigation"""
        await ctx.defer(ephemeral=True)

        guild_id = ctx.guild.id

        if not hasattr(self.bot, 'log_parser') or not self.bot.log_parser:
            await ctx.followup.send("❌ Log parser not initialized")
            return

        # Get guild config
        guild_config = await self.bot.db_manager.get_guild(guild_id)
        if not guild_config or not guild_config.get('servers'):
            await ctx.followup.send("❌ No servers configured for this guild")
            return

        servers = guild_config.get('servers', [])
        connection_parser = self.bot.log_parser.connection_parser

        investigation_results = []

        for server_config in servers:
            server_name = server_config.get('name', 'Unknown')
            current_server_id = str(server_config.get('_id', 'unknown'))

            if server_id and current_server_id != server_id:
                continue

            server_key = f"{guild_id}_{current_server_id}"

            # 1. Verify regex patterns
            pattern_results = connection_parser.verify_regex_patterns()

            # 2. Test counting logic
            counting_results = connection_parser.test_counting_logic(server_key)

            # 3. Check file processing state
            file_state = self.bot.log_parser.file_states.get(server_key, {})

            investigation_results.append({
                'server_name': server_name,
                'server_id': current_server_id,
                'pattern_results': pattern_results,
                'counting_results': counting_results,
                'file_state': file_state
            })

        # Create detailed report
        embed = discord.Embed(
            title="🔬 Player Count Investigation Report",
            color=0x00ff00,
            timestamp=datetime.now(timezone.utc)
        )

        for result in investigation_results:
            pattern_summary = {k: v['match_count'] for k, v in result['pattern_results'].items()}
            counting = result['counting_results']

            embed.add_field(
                name=f"🔍 {result['server_name']} Investigation",
                value=f"**Pattern Matches:** {sum(pattern_summary.values())} total\n"
                      f"**Queue Count:** Manual={counting.get('manual_count', {}).get('queue_count', 0)}, "
                      f"Official={counting.get('official_stats', {}).get('queue_count', 0)}\n"
                      f"**Player Count:** Manual={counting.get('manual_count', {}).get('player_count', 0)}, "
                      f"Official={counting.get('official_stats', {}).get('player_count', 0)}\n"
                      f"**File State:** Size={result['file_state'].get('file_size', 0)}, "
                      f"Lines={result['file_state'].get('line_count', 0)}",
                inline=False
            )

        await ctx.followup.send(embed=embed)

    @discord.slash_command(name="test_log_parser", description="Test the unified log parser with sample data")
    @commands.has_permissions(administrator=True)
    async def test_log_parser(self, ctx: discord.ApplicationContext, lines: int = 10):
        """Test the unified log parser"""
        try:
            await ctx.defer()

            # Get parser instance
            if not hasattr(self.bot, 'unified_parser'):
                embed = discord.Embed(
                    title="❌ Parser Not Available",
                    description="The unified log parser is not initialized.",
                    color=0xFF0000
                )
                await ctx.followup.send(embed=embed)
                return

            parser = self.bot.unified_parser

            # Test with sample log data
            sample_logs = [
                "[2024.05.30-09.18.36:173] LogSFPS: Mission GA_Airport_mis_01_SFPSACMission switched to READY",
                "[2024.05.30-09.18.37:174] LogNet: Join request: /Game/Maps/world_1/World_1?eosid=|abc123def456?Name=TestPlayer",
                "[2024.05.30-09.18.38:175] LogOnline: Warning: Player |abc123def456 successfully registered!",
                "[2024.05.30-09.18.39:176] LogSFPS: Mission GA_Military_02_Mis1 switched to IN_PROGRESS",
                "[2024.05.30-09.18.40:177] UChannel::Close: Sending CloseBunch UniqueId: EOS:|abc123def456"
            ]

            test_content = "\n".join(sample_logs[:lines])

            # Parse the test content
            embeds = await parser.parse_log_content(test_content, str(ctx.guild_id))

            # Get parser status
            status = parser.get_parser_status()

            # Create response embed
            embed = discord.Embed(
                title="🧪 Log Parser Test Results",
                description=f"Tested with {lines} sample log lines",
                color=0x00AA00
            )

            embed.add_field(
                name="Test Data",
                value=f"```\n{test_content[:500]}{'...' if len(test_content) > 500 else ''}\n```",
                inline=False
            )

            embed.add_field(
                name="Results",
                value=f"**Events Parsed:** {len(embeds)}\n**Parser Status:** ✅ Working",
                inline=False
            )

            embed.add_field(
                name="Parser State",
                value=f"**Active Sessions:** {status['active_sessions']}\n**SFTP Connections:** {status['sftp_connections']}\n**File States:** {status['file_states']}",
                inline=False
            )

            await ctx.followup.send(embed=embed)

            # Send any generated embeds
            if embeds:
                for event_embed in embeds[:3]:  # Limit to first 3 to avoid spam
                    await ctx.followup.send(embed=event_embed)

                if len(embeds) > 3:
                    await ctx.followup.send(f"... and {len(embeds) - 3} more events")

        except Exception as e:
            logger.error(f"Test log parser error: {e}")
            embed = discord.Embed(
                title="❌ Test Failed",
                description=f"Error testing parser: {str(e)}",
                color=0xFF0000
            )
            await ctx.followup.send(embed=embed)

    @discord.slash_command(name="parser_status", description="Check the status of all parsers")
    @commands.has_permissions(administrator=True)
    async def parser_status(self, ctx: discord.ApplicationContext):
        """Check parser status and player tracking"""
        try:
            await ctx.defer()

            embed = discord.Embed(
                title="📊 Parser Status Report",
                color=0x0099FF
            )

            # Check unified parser
            if hasattr(self.bot, 'unified_parser'):
                parser = self.bot.unified_parser
                status = parser.get_parser_status()

                embed.add_field(
                    name="🔄 Unified Log Parser",
                    value=f"**Status:** ✅ Active\n**Active Sessions:** {status['active_sessions']}\n**Tracked Servers:** {status['total_tracked_servers']}\n**SFTP Connections:** {status['sftp_connections']}\n**Connection Status:** {status['connection_status']}",
                    inline=False
                )s']}\n**SFTP Connections:** {status['sftp_connections']}\n**File States:** {status['file_states']}",
                    inline=False
                )

                if status['active_players_by_guild']:
                    players_info = "\n".join([f"Guild {guild}: {count} players" for guild, count in status['active_players_by_guild'].items()])
                    embed.add_field(
                        name="👥 Active Players",
                        value=players_info,
                        inline=False
                    )
            else:
                embed.add_field(
                    name="🔄 Unified Log Parser",
                    value="❌ Not initialized",
                    inline=False
                )

            # Check killfeed parser
            if hasattr(self.bot, 'killfeed_parser'):
                embed.add_field(
                    name="💀 Killfeed Parser",
                    value="✅ Active",
                    inline=True
                )
            else:
                embed.add_field(
                    name="💀 Killfeed Parser",
                    value="❌ Not initialized",
                    inline=True
                )

            await ctx.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Parser status error: {e}")
            embed = discord.Embed(
                title="❌ Status Check Failed",
                description=f"Error checking parser status: {str(e)}",
                color=0xFF0000
            )
            await ctx.followup.send(embed=embed)

    @discord.slash_command(name="reset_parser_state", description="Reset parser file states to force cold start")
    @commands.has_permissions(administrator=True)
    async def reset_parser_state(self, ctx: discord.ApplicationContext):
        """Reset parser states for debugging"""
        try:
            await ctx.defer()

            if not hasattr(self.bot, 'unified_parser'):
                embed = discord.Embed(
                    title="❌ Parser Not Available",
                    description="The unified log parser is not initialized.",
                    color=0xFF0000
                )
                await ctx.followup.send(embed=embed)
                return

            parser = self.bot.unified_parser
            parser.reset_file_states()

            embed = discord.Embed(
                title="🔄 Parser State Reset",
                description="All parser file states have been reset. Next run will be a cold start.",
                color=0x00AA00
            )

            await ctx.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Reset parser state error: {e}")
            embed = discord.Embed(
                title="❌ Reset Failed",
                description=f"Error resetting parser state: {str(e)}",
                color=0xFF0000
            )
            await ctx.followup.send(embed=embed)

def setup(bot):
    bot.add_cog(Parsers(bot))