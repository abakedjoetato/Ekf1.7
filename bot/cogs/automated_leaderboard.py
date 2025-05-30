
"""
Emerald's Killfeed - Automated Consolidated Leaderboard
Posts and updates consolidated leaderboards every 30 minutes
"""

import discord
from discord.ext import commands, tasks
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class AutomatedLeaderboard(commands.Cog):
    """Automated consolidated leaderboard system"""

    def __init__(self, bot):
        self.bot = bot
        self.message_cache = {}  # Store {guild_id: message_id}
        
    async def cog_load(self):
        """Start the automated leaderboard task when cog loads"""
        logger.info("Starting automated leaderboard task...")
        self.automated_leaderboard_task.start()

    def cog_unload(self):
        """Stop the task when cog unloads"""
        self.automated_leaderboard_task.cancel()

    @tasks.loop(minutes=30)
    async def automated_leaderboard_task(self):
        """Run automated leaderboard updates every 30 minutes"""
        try:
            logger.info("Running automated leaderboard update...")
            
            # Get all guilds with leaderboard channels configured
            guilds_cursor = self.bot.db_manager.guilds.find({
                "channels.leaderboard": {"$exists": True, "$ne": None},
                "leaderboard_enabled": True
            })
            
            guilds_with_leaderboard = await guilds_cursor.to_list(length=None)
            
            for guild_config in guilds_with_leaderboard:
                try:
                    await self.update_guild_leaderboard(guild_config)
                except Exception as e:
                    guild_id = guild_config.get('guild_id', 'Unknown')
                    logger.error(f"Failed to update leaderboard for guild {guild_id}: {e}")
                    
            logger.info(f"Automated leaderboard update completed for {len(guilds_with_leaderboard)} guilds")
            
        except Exception as e:
            logger.error(f"Error in automated leaderboard task: {e}")

    @automated_leaderboard_task.before_loop
    async def before_automated_leaderboard(self):
        """Wait for bot to be ready before starting task"""
        await self.bot.wait_until_ready()

    async def update_guild_leaderboard(self, guild_config: Dict[str, Any]):
        """Update leaderboard for a specific guild"""
        try:
            guild_id = guild_config['guild_id']
            channel_id = guild_config['channels']['leaderboard']
            
            # Get the Discord guild and channel
            guild = self.bot.get_guild(guild_id)
            if not guild:
                logger.warning(f"Guild {guild_id} not found")
                return
                
            channel = guild.get_channel(channel_id)
            if not channel:
                logger.warning(f"Leaderboard channel {channel_id} not found in guild {guild_id}")
                return

            # Check if guild has premium access
            has_premium = await self.check_premium_access(guild_id)
            if not has_premium:
                logger.warning(f"Guild {guild_id} doesn't have premium access for automated leaderboards")
                return

            # Get server configuration
            servers = guild_config.get('servers', [])
            if not servers:
                logger.warning(f"No servers configured for guild {guild_id}")
                return
                
            # Use first server for now
            server_config = servers[0]
            server_id = server_config.get('server_id', server_config.get('_id', 'default'))
            server_name = server_config.get('name', f'Server {server_id}')

            # Create consolidated leaderboard embed
            embed, file = await self.create_consolidated_leaderboard(guild_id, server_id, server_name)
            
            if not embed:
                logger.warning(f"Failed to create leaderboard for guild {guild_id}")
                return

            # Try to edit existing message first
            existing_message_id = self.message_cache.get(guild_id)
            if existing_message_id:
                try:
                    existing_message = await channel.fetch_message(existing_message_id)
                    files = [file] if file else []
                    await existing_message.edit(embed=embed, attachments=files)
                    logger.info(f"Updated existing leaderboard message for guild {guild_id}")
                    return
                except discord.NotFound:
                    # Message was deleted, we'll create a new one
                    logger.info(f"Existing message not found for guild {guild_id}, creating new one")
                except Exception as e:
                    logger.error(f"Failed to edit existing message for guild {guild_id}: {e}")

            # Create new message
            files = [file] if file else []
            new_message = await channel.send(embed=embed, files=files)
            self.message_cache[guild_id] = new_message.id
            logger.info(f"Created new leaderboard message for guild {guild_id}")

        except Exception as e:
            logger.error(f"Failed to update guild leaderboard: {e}")

    async def check_premium_access(self, guild_id: int) -> bool:
        """Check if guild has premium access"""
        try:
            guild_doc = await self.bot.db_manager.get_guild(guild_id)
            if not guild_doc:
                return False
            
            servers = guild_doc.get('servers', [])
            for server_config in servers:
                server_id = server_config.get('server_id', server_config.get('_id', 'default'))
                if await self.bot.db_manager.is_premium_server(guild_id, server_id):
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check premium access: {e}")
            return False

    async def create_consolidated_leaderboard(self, guild_id: int, server_id: str, server_name: str):
        """Create consolidated leaderboard with top performers from each category"""
        try:
            # Title and description
            title = f"📊 CONSOLIDATED LEADERBOARD - {server_name}"
            description = "Top performers across all categories"

            # Get data for each category
            kills_data = await self.get_top_kills(guild_id, 5)
            kdr_data = await self.get_top_kdr(guild_id, 3)
            weapons_data = await self.get_top_weapons(guild_id, 3)
            distance_data = await self.get_top_distance(guild_id, 3)
            deaths_data = await self.get_top_deaths(guild_id, 3)
            faction_data = await self.get_top_faction(guild_id, 1)

            # Build consolidated rankings text
            rankings_sections = []

            # Top Killers (5)
            if kills_data:
                kills_text = []
                for i, player in enumerate(kills_data, 1):
                    player_name = player.get('player_name', 'Unknown')
                    kills = player.get('kills', 0)
                    faction = await self.get_player_faction(guild_id, player_name)
                    faction_tag = f" [{faction}]" if faction else ""
                    kills_text.append(f"**{i}.** {player_name}{faction_tag} — {kills:,} Kills")
                rankings_sections.append(f"**TOP KILLERS**\n" + "\n".join(kills_text))

            # Top KDR (3)
            if kdr_data:
                kdr_text = []
                for i, player in enumerate(kdr_data, 1):
                    player_name = player.get('player_name', 'Unknown')
                    kdr = player.get('kdr', 0.0)
                    kills = player.get('kills', 0)
                    deaths = player.get('deaths', 0)
                    faction = await self.get_player_faction(guild_id, player_name)
                    faction_tag = f" [{faction}]" if faction else ""
                    kdr_text.append(f"**{i}.** {player_name}{faction_tag} — KDR: {kdr:.2f} ({kills:,}/{deaths:,})")
                rankings_sections.append(f"**EFFICIENCY MASTERS**\n" + "\n".join(kdr_text))

            # Top Weapons (3)
            if weapons_data:
                weapons_text = []
                for i, weapon in enumerate(weapons_data, 1):
                    weapon_name = weapon.get('_id', 'Unknown')
                    kills = weapon.get('kills', 0)
                    top_killer = weapon.get('top_killer', 'Unknown')
                    faction = await self.get_player_faction(guild_id, top_killer)
                    faction_tag = f" [{faction}]" if faction else ""
                    weapons_text.append(f"**{i}.** {weapon_name} — {kills:,} Kills | Top: {top_killer}{faction_tag}")
                rankings_sections.append(f"**DEADLIEST WEAPONS**\n" + "\n".join(weapons_text))

            # Top Distance (3)
            if distance_data:
                distance_text = []
                for i, player in enumerate(distance_data, 1):
                    player_name = player.get('player_name', 'Unknown')
                    best_distance = player.get('personal_best_distance', 0.0)
                    faction = await self.get_player_faction(guild_id, player_name)
                    faction_tag = f" [{faction}]" if faction else ""
                    
                    if best_distance >= 1000:
                        distance_str = f"{best_distance/1000:.1f}km"
                    else:
                        distance_str = f"{best_distance:.0f}m"
                    
                    distance_text.append(f"**{i}.** {player_name}{faction_tag} — {distance_str}")
                rankings_sections.append(f"**PRECISION SNIPERS**\n" + "\n".join(distance_text))

            # Top Deaths (3)
            if deaths_data:
                deaths_text = []
                for i, player in enumerate(deaths_data, 1):
                    player_name = player.get('player_name', 'Unknown')
                    deaths = player.get('deaths', 0)
                    faction = await self.get_player_faction(guild_id, player_name)
                    faction_tag = f" [{faction}]" if faction else ""
                    deaths_text.append(f"**{i}.** {player_name}{faction_tag} — {deaths:,} Deaths")
                rankings_sections.append(f"**MOST FALLEN**\n" + "\n".join(deaths_text))

            # Top Faction (1)
            if faction_data:
                faction_info = faction_data[0]
                faction_name = faction_info.get('faction_name', 'Unknown')
                kills = faction_info.get('kills', 0)
                deaths = faction_info.get('deaths', 0)
                members = faction_info.get('member_count', 0)
                kdr = kills / max(deaths, 1) if deaths > 0 else kills
                
                faction_text = f"**1.** [{faction_name}] — {kills:,} Kills | KDR: {kdr:.2f} | {members} Members"
                rankings_sections.append(f"**DOMINANT FACTION**\n" + faction_text)

            # Combine all sections
            combined_rankings = "\n\n".join(rankings_sections)

            # Calculate totals
            total_kills = sum(p.get('kills', 0) for p in kills_data) if kills_data else 0
            total_deaths = sum(p.get('deaths', 0) for p in deaths_data) if deaths_data else 0

            # Create embed using EmbedFactory
            embed_data = {
                'title': title,
                'description': description,
                'rankings': combined_rankings[:4000],  # Discord embed limit
                'total_kills': total_kills,
                'total_deaths': total_deaths,
                'stat_type': 'consolidated',
                'style_variant': 'consolidated',
                'server_name': server_name,
                'thumbnail_url': 'attachment://Leaderboard.png'
            }

            embed, file = await EmbedFactory.build('leaderboard', embed_data)
            
            # Add update timestamp to footer
            timestamp_str = datetime.now(timezone.utc).strftime("%m/%d/%Y %I:%M %p UTC")
            embed.set_footer(text=f"Last Updated: {timestamp_str} | Powered by Discord.gg/EmeraldServers")
            
            return embed, file

        except Exception as e:
            logger.error(f"Failed to create consolidated leaderboard: {e}")
            return None, None

    async def get_top_kills(self, guild_id: int, limit: int) -> List[Dict[str, Any]]:
        """Get top killers"""
        try:
            cursor = self.bot.db_manager.pvp_data.find({
                "guild_id": guild_id,
                "kills": {"$gt": 0}
            }).sort("kills", -1).limit(limit)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Failed to get top kills: {e}")
            return []

    async def get_top_kdr(self, guild_id: int, limit: int) -> List[Dict[str, Any]]:
        """Get top KDR players"""
        try:
            cursor = self.bot.db_manager.pvp_data.find({
                "guild_id": guild_id,
                "kills": {"$gte": 1}
            }).limit(50)
            all_players = await cursor.to_list(length=None)
            
            # Calculate KDR and sort
            for player in all_players:
                kills = player.get('kills', 0)
                deaths = player.get('deaths', 0)
                player['kdr'] = kills / max(deaths, 1) if deaths > 0 else float(kills)
            
            return sorted(all_players, key=lambda x: x['kdr'], reverse=True)[:limit]
        except Exception as e:
            logger.error(f"Failed to get top KDR: {e}")
            return []

    async def get_top_weapons(self, guild_id: int, limit: int) -> List[Dict[str, Any]]:
        """Get top weapons"""
        try:
            cursor = self.bot.db_manager.kill_events.find({
                "guild_id": guild_id,
                "is_suicide": False,
                "weapon": {"$nin": ["Menu Suicide", "Suicide", "Falling", "suicide_by_relocation"]}
            })
            
            weapon_events = await cursor.to_list(length=None)
            
            # Group weapons
            weapon_stats = {}
            for event in weapon_events:
                weapon = event.get('weapon', 'Unknown')
                killer = event.get('killer', 'Unknown')
                
                if weapon not in weapon_stats:
                    weapon_stats[weapon] = {'kills': 0, 'top_killer': killer}
                weapon_stats[weapon]['kills'] += 1
            
            # Sort and limit
            weapons_data = []
            for weapon, stats in sorted(weapon_stats.items(), key=lambda x: x[1]['kills'], reverse=True)[:limit]:
                weapons_data.append({
                    '_id': weapon,
                    'kills': stats['kills'],
                    'top_killer': stats['top_killer']
                })
            
            return weapons_data
        except Exception as e:
            logger.error(f"Failed to get top weapons: {e}")
            return []

    async def get_top_distance(self, guild_id: int, limit: int) -> List[Dict[str, Any]]:
        """Get top distance kills"""
        try:
            cursor = self.bot.db_manager.pvp_data.find({
                "guild_id": guild_id,
                "personal_best_distance": {"$gt": 0}
            }).sort("personal_best_distance", -1).limit(limit)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Failed to get top distance: {e}")
            return []

    async def get_top_deaths(self, guild_id: int, limit: int) -> List[Dict[str, Any]]:
        """Get players with most deaths"""
        try:
            cursor = self.bot.db_manager.pvp_data.find({
                "guild_id": guild_id,
                "deaths": {"$gt": 0}
            }).sort("deaths", -1).limit(limit)
            return await cursor.to_list(length=None)
        except Exception as e:
            logger.error(f"Failed to get top deaths: {e}")
            return []

    async def get_top_faction(self, guild_id: int, limit: int) -> List[Dict[str, Any]]:
        """Get top faction"""
        try:
            factions_cursor = self.bot.db_manager.factions.find({"guild_id": guild_id})
            all_factions = await factions_cursor.to_list(length=None)
            
            faction_stats = {}
            
            for faction_doc in all_factions:
                faction_name = faction_doc.get('faction_name')
                faction_tag = faction_doc.get('faction_tag')
                faction_display = faction_tag if faction_tag else faction_name
                
                if not faction_display:
                    continue
                
                faction_stats[faction_display] = {
                    'kills': 0, 
                    'deaths': 0, 
                    'members': set(),
                    'faction_name': faction_name
                }
                
                # Get stats for each member
                for discord_id in faction_doc.get('members', []):
                    player_link = await self.bot.db_manager.players.find_one({
                        "guild_id": guild_id,
                        "discord_id": discord_id
                    })
                    
                    if not player_link:
                        continue
                        
                    for character in player_link.get('linked_characters', []):
                        player_stat = await self.bot.db_manager.pvp_data.find_one({
                            "guild_id": guild_id,
                            "player_name": character
                        })
                        
                        if player_stat:
                            faction_stats[faction_display]['kills'] += player_stat.get('kills', 0)
                            faction_stats[faction_display]['deaths'] += player_stat.get('deaths', 0)
                            faction_stats[faction_display]['members'].add(character)

            # Convert member sets to counts and sort by kills
            for faction_name in faction_stats:
                faction_stats[faction_name]['member_count'] = len(faction_stats[faction_name]['members'])
                del faction_stats[faction_name]['members']

            sorted_factions = sorted(faction_stats.items(), key=lambda x: x[1]['kills'], reverse=True)[:limit]
            
            return [{'faction_name': name, **stats} for name, stats in sorted_factions]
        except Exception as e:
            logger.error(f"Failed to get top faction: {e}")
            return []

    async def get_player_faction(self, guild_id: int, player_name: str) -> Optional[str]:
        """Get player's faction tag"""
        try:
            player_link = await self.bot.db_manager.players.find_one({
                "guild_id": guild_id,
                "linked_characters": player_name
            })
            
            if not player_link:
                return None
                
            discord_id = player_link.get('discord_id')
            if not discord_id:
                return None
            
            faction_doc = await self.bot.db_manager.factions.find_one({
                "guild_id": guild_id,
                "members": {"$in": [discord_id]}
            })
            
            if faction_doc:
                faction_tag = faction_doc.get('faction_tag')
                if faction_tag:
                    return faction_tag
                return faction_doc.get('faction_name')
            
            return None
        except Exception as e:
            logger.error(f"Error getting player faction for {player_name}: {e}")
            return None

def setup(bot):
    bot.add_cog(AutomatedLeaderboard(bot))
