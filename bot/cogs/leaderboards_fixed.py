"""
Emerald's Killfeed - Fixed Leaderboard System
Properly themed leaderboards using EmbedFactory
"""

import discord
from discord.ext import commands
import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, Any, List
# Removed autocomplete import to fix loading issues
from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class LeaderboardsFixed(commands.Cog):
    """Fixed leaderboard commands that actually use the themed factory"""

    def __init__(self, bot):
        self.bot = bot

    @discord.slash_command(
        name="leaderboard",
        description="View properly themed leaderboards"
    )
    async def leaderboard(self, ctx: discord.ApplicationContext,
                         stat: discord.Option(str, "Statistic to display", 
                                            choices=['kills', 'deaths', 'kdr', 'distance', 'weapons', 'factions']),
                         server: discord.Option(str, "Server to view stats for", required=False)):
        """Display properly themed leaderboard"""
        await ctx.defer()

        try:
            guild_id = ctx.guild.id if ctx.guild else None
            if not guild_id:
                await ctx.followup.send("This command can only be used in a server!", ephemeral=True)
                return

            # Get guild configuration
            guild_doc = await self.bot.db_manager.get_guild(guild_id)
            if not guild_doc or not guild_doc.get('servers'):
                await ctx.followup.send("No servers configured for this guild. Use `/addserver` first!", ephemeral=True)
                return

            # Select server
            if server:
                selected_server = None
                for server_config in guild_doc['servers']:
                    if server_config.get('name', '').lower() == server.lower() or server_config.get('server_id', '') == server:
                        selected_server = server_config
                        break

                if not selected_server:
                    await ctx.followup.send(f"Server '{server}' not found!", ephemeral=True)
                    return
            else:
                selected_server = guild_doc['servers'][0]

            server_id = selected_server.get('server_id', selected_server.get('_id', 'default'))
            server_name = selected_server.get('name', f'Server {server_id}')

            # Create themed leaderboard using EmbedFactory
            embed, file = await self.create_themed_leaderboard(guild_id, server_id, stat, server_name)

            if embed:
                files = [file] if file else []
                await ctx.followup.send(embed=embed, files=files)
            else:
                await ctx.followup.send(f"No data available for {stat} leaderboard on {server_name}!", ephemeral=True)

        except Exception as e:
            logger.error(f"Failed to show leaderboard: {e}")
            await ctx.followup.send("Failed to load leaderboard. Please try again later.", ephemeral=True)

    async def get_player_faction(self, guild_id: int, player_name: str) -> Optional[str]:
        """Get player's faction tag if they have one"""
        try:
            # First find the Discord ID for this player name
            player_link = await self.bot.db_manager.players.find_one({
                "guild_id": guild_id,
                "linked_characters": player_name
            })
            
            if not player_link:
                return None
                
            discord_id = player_link.get('discord_id')
            if not discord_id:
                return None
            
            # Now look up faction using Discord ID - ensure we're checking the members array properly
            faction_doc = await self.bot.db_manager.factions.find_one({
                "guild_id": guild_id,
                "members": {"$in": [discord_id]}
            })
            
            if faction_doc:
                # Return faction tag if available, otherwise faction name
                faction_tag = faction_doc.get('faction_tag')
                if faction_tag:
                    return faction_tag
                return faction_doc.get('faction_name')
            
            return None
        except Exception as e:
            logger.error(f"Error getting player faction for {player_name}: {e}")
            return None

    async def format_leaderboard_line(self, rank: int, player: Dict[str, Any], stat_type: str, guild_id: int) -> str:
        """Format a single leaderboard line with faction tags and clean styling"""
        player_name = player.get('player_name', 'Unknown')

        # Get faction tag
        faction = await self.get_player_faction(guild_id, player_name)
        faction_tag = f" [{faction}]" if faction else ""

        # Clean rank formatting without emojis - just bold numbers
        rank_display = f"**{rank}.**"

        # Format value based on stat type - show only the relevant stat
        if stat_type == 'kills':
            kills = player.get('kills', 0)
            value = f"{kills:,} Kills"

        elif stat_type == 'deaths':
            deaths = player.get('deaths', 0)
            value = f"{deaths:,} Deaths"

        elif stat_type == 'kdr':
            kdr = player.get('kdr', 0.0)
            kills = player.get('kills', 0)
            deaths = player.get('deaths', 0)

            # Recalculate KDR if it's 0 but we have kills/deaths data
            if kdr == 0.0 and kills > 0:
                kdr = kills / max(deaths, 1)

            value = f"KDR: {kdr:.2f} ({kills:,}/{deaths:,})"

        elif stat_type == 'distance':
            total_distance = player.get('total_distance', 0.0)
            best_distance = player.get('personal_best_distance', 0.0)

            # Show best distance as primary stat with total in parentheses
            if best_distance >= 1000:
                best_str = f"{best_distance/1000:.1f}km"
            else:
                best_str = f"{best_distance:.0f}m"

            if total_distance >= 1000:
                total_str = f"{total_distance/1000:.1f}km"
            else:
                total_str = f"{total_distance:.0f}m"

            value = f"Best: {best_str} (Total: {total_str})"

        else:
            value = str(player.get(stat_type, 0))

        return f"{rank_display} {player_name}{faction_tag} — {value}"

    async def create_themed_leaderboard(self, guild_id: int, server_id: str, stat_type: str, server_name: str) -> Tuple[Optional[discord.Embed], Optional[discord.File]]:
        """Create properly themed leaderboard using EmbedFactory"""
        try:
            # Themed title pools for each stat type
            title_pools = {
                'kills': ["Top Operators", "Elite Eliminators", "Death Dealers", "Blood Money Rankings"],
                'deaths': ["Most Fallen", "Casualty Reports", "Frequent Respawners", "Battle Casualties"],
                'kdr': ["Combat Efficiency", "Kill/Death Masters", "Survival Experts", "Tactical Legends"],
                'distance': ["Precision Masters", "Long Range Snipers", "Distance Champions", "Eagle Eyes"],
                'weapons': ["Arsenal Analysis", "Weapon Mastery", "Combat Tools", "Death Dealers"],
                'factions': ["Faction Dominance", "Alliance Power", "Territory Control", "War Machine"]
            }

            # Themed descriptions
            descriptions = {
                'kills': "Most eliminations on the battlefield",
                'deaths': "Those who've fallen in the line of duty",
                'kdr': "Elite warriors with the highest efficiency",
                'distance': "Snipers who strike from afar",
                'weapons': "Most lethal tools of war",
                'factions': "Dominant forces in the wasteland"
            }

            if stat_type == 'kills':
                # Guild-wide query (server filtering will be applied at guild level)
                cursor = self.bot.db_manager.pvp_data.find({
                    "guild_id": guild_id,
                    "kills": {"$gt": 0}
                }).sort("kills", -1).limit(10)
                players = await cursor.to_list(length=None)
                title = f"{random.choice(title_pools['kills'])} - {server_name}"
                description = descriptions['kills']

            elif stat_type == 'deaths':
                # Guild-wide query
                cursor = self.bot.db_manager.pvp_data.find({
                    "guild_id": guild_id,
                    "deaths": {"$gt": 0}
                }).sort("deaths", -1).limit(10)
                players = await cursor.to_list(length=None)
                title = f"{random.choice(title_pools['deaths'])} - {server_name}"
                description = descriptions['deaths']

            elif stat_type == 'kdr':
                # Guild-wide query
                cursor = self.bot.db_manager.pvp_data.find({
                    "guild_id": guild_id,
                    "kills": {"$gte": 1}
                }).limit(50)
                all_players = await cursor.to_list(length=None)
                
                # Calculate KDR and sort in Python
                for player in all_players:
                    kills = player.get('kills', 0)
                    deaths = player.get('deaths', 0)
                    player['kdr'] = kills / max(deaths, 1) if deaths > 0 else float(kills)
                
                players = sorted(all_players, key=lambda x: x['kdr'], reverse=True)[:10]
                title = f"{random.choice(title_pools['kdr'])} - {server_name}"
                description = descriptions['kdr']

            elif stat_type == 'distance':
                # Use the pvp_data collection for distance leaderboard - query by _id for server match
                pipeline = [
                    {"$match": {"guild_id": guild_id, "_id": server_id}},
                    {"$match": {"personal_best_distance": {"$gt": 0}}},  # Only players with distance kills
                    {"$addFields": {
                        "kdr": {"$cond": {
                            "if": {"$gt": ["$deaths", 0]},
                            "then": {"$divide": ["$kills", "$deaths"]},
                            "else": "$kills"
                        }}
                    }},
                    {"$sort": {"personal_best_distance": -1}},  # Sort by longest single kill distance
                    {"$limit": 10}
                ]
                players = await self.bot.db_manager.pvp_data.aggregate(pipeline).to_list(length=None)
                title = f"{random.choice(title_pools['distance'])} - {server_name}"
                description = descriptions['distance']

            elif stat_type == 'weapons':
                # Guild-wide weapons query
                cursor = self.bot.db_manager.kill_events.find({
                    "guild_id": guild_id,
                    "is_suicide": False,
                    "weapon": {"$nin": ["Menu Suicide", "Suicide", "Falling", "suicide_by_relocation"]}
                })
                
                weapon_events = await cursor.to_list(length=None)
                
                # Group weapons in Python
                weapon_stats = {}
                for event in weapon_events:
                    weapon = event.get('weapon', 'Unknown')
                    killer = event.get('killer', 'Unknown')
                    
                    if weapon not in weapon_stats:
                        weapon_stats[weapon] = {'kills': 0, 'top_killer': killer}
                    weapon_stats[weapon]['kills'] += 1
                
                # Sort and limit
                weapons_data = []
                for weapon, stats in sorted(weapon_stats.items(), key=lambda x: x[1]['kills'], reverse=True)[:10]:
                    weapons_data.append({
                        '_id': weapon,
                        'kills': stats['kills'],
                        'top_killer': stats['top_killer']
                    })

                if not weapons_data:
                    return None, None

                leaderboard_text = []
                for i, weapon in enumerate(weapons_data, 1):
                    weapon_name = weapon['_id'] or 'Unknown'
                    kills = weapon['kills']
                    top_killer = weapon['top_killer'] or 'Unknown'

                    # Clean weapon formatting without emojis
                    rank_display = f"**{i}.**"

                    # Get faction for top killer
                    faction = await self.get_player_faction(guild_id, top_killer)
                    faction_tag = f" [{faction}]" if faction else ""

                    # Clean weapon name formatting
                    if weapon_name and weapon_name != 'Unknown':
                        leaderboard_text.append(f"{rank_display} {weapon_name} — {kills:,} Kills | Top: {top_killer}{faction_tag}")
                    else:
                        leaderboard_text.append(f"{rank_display} Unknown Weapon — {kills:,} Kills | Top: {top_killer}{faction_tag}")

                title = f"{random.choice(title_pools['weapons'])} - {server_name}"
                embed_data = {
                    'title': title,
                    'description': descriptions['weapons'],
                    'rankings': "\n".join(leaderboard_text),
                    'total_kills': sum(w['kills'] for w in weapons_data),
                    'total_deaths': 0,
                    'stat_type': 'weapons',
                    'style_variant': 'weapons',
                    'server_name': server_name,
                    'thumbnail_url': 'attachment://WeaponStats.png'
                }

                embed, file = await EmbedFactory.build('leaderboard', embed_data)
                return embed, file

            elif stat_type == 'factions':
                # Get all factions for this guild first
                factions_cursor = self.bot.db_manager.factions.find({"guild_id": guild_id})
                all_factions = await factions_cursor.to_list(length=None)
                
                faction_stats = {}
                
                # Process each faction
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
                        # Get player's linked characters
                        player_link = await self.bot.db_manager.players.find_one({
                            "guild_id": guild_id,
                            "discord_id": discord_id
                        })
                        
                        if not player_link:
                            continue
                            
                        # Get stats for each character
                        for character in player_link.get('linked_characters', []):
                            player_stat = await self.bot.db_manager.pvp_data.find_one({
                                "guild_id": guild_id,
                                "player_name": character
                            })
                            
                            if player_stat:
                                faction_stats[faction_display]['kills'] += player_stat.get('kills', 0)
                                faction_stats[faction_display]['deaths'] += player_stat.get('deaths', 0)
                                faction_stats[faction_display]['members'].add(character)

                # Convert member sets to counts
                for faction_name in faction_stats:
                    faction_stats[faction_name]['member_count'] = len(faction_stats[faction_name]['members'])
                    del faction_stats[faction_name]['members']

                if not faction_stats:
                    return None, None

                # Sort by kills
                sorted_factions = sorted(faction_stats.items(), key=lambda x: x[1]['kills'], reverse=True)[:10]

                leaderboard_text = []
                for i, (faction_name, stats) in enumerate(sorted_factions, 1):
                    kills = stats['kills']
                    deaths = stats['deaths']
                    members = stats['member_count']
                    kdr = kills / max(deaths, 1) if deaths > 0 else kills

                    # Clean faction formatting without emojis
                    rank_display = f"**{i}.**"

                    # Format faction line with bracket notation
                    parts = [f"{kills:,} Kills"]
                    if kdr > 0 and deaths > 0:
                        parts.append(f"KDR: {kdr:.2f}")
                    parts.append(f"{members} Members")

                    leaderboard_text.append(f"{rank_display} [{faction_name}] — {' | '.join(parts)}")

                title = f"{random.choice(title_pools['factions'])} - {server_name}"
                embed_data = {
                    'title': title,
                    'description': descriptions['factions'],
                    'rankings': "\n".join(leaderboard_text),
                    'total_kills': sum(f[1]['kills'] for f in sorted_factions),
                    'total_deaths': sum(f[1]['deaths'] for f in sorted_factions),
                    'stat_type': 'factions',
                    'style_variant': 'factions',
                    'server_name': server_name,
                    'thumbnail_url': 'attachment://Faction.png'
                }

                embed, file = await EmbedFactory.build('leaderboard', embed_data)
                return embed, file

            else:
                return None, None

            if not players:
                return None, None

            # Create professional leaderboard text with advanced formatting
            leaderboard_text = []
            for i, player in enumerate(players, 1):
                formatted_line = await self.format_leaderboard_line(i, player, stat_type, guild_id)
                leaderboard_text.append(formatted_line)

            # All leaderboards use Leaderboard.png
            thumbnail_map = {
                'kills': 'attachment://Leaderboard.png',
                'deaths': 'attachment://Leaderboard.png',
                'kdr': 'attachment://Leaderboard.png',
                'distance': 'attachment://Leaderboard.png'
            }

            # Use EmbedFactory for proper theming with dynamic styling
            embed_data = {
                'title': title,
                'description': description,
                'rankings': "\n".join(leaderboard_text),
                'total_kills': sum(p.get('kills', 0) for p in players),
                'total_deaths': sum(p.get('deaths', 0) for p in players),
                'stat_type': stat_type,
                'style_variant': stat_type,
                'server_name': server_name,
                'thumbnail_url': thumbnail_map.get(stat_type, 'attachment://Leaderboard.png')
            }

            embed, file = await EmbedFactory.build('leaderboard', embed_data)
            return embed, file

        except Exception as e:
            logger.error(f"Failed to create themed leaderboard: {e}")
            return None, None

def setup(bot):
    bot.add_cog(LeaderboardsFixed(bot))