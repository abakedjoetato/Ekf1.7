"""Update send_log_embeds to use server-specific channels"""
"""
Emerald's Killfeed - Unified Log Parser System
Consolidated from fragmented parsers with complete mission normalization
PHASE 1 & 2 Complete Implementation
"""

import asyncio
import logging
import os
import re
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple

import aiofiles
import discord
import asyncssh
from discord.ext import commands

from bot.utils.embed_factory import EmbedFactory

logger = logging.getLogger(__name__)

class UnifiedLogParser:
    """
    UNIFIED LOG PARSER - Consolidates all log parsing functionality
    - Replaces log_parser.py, intelligent_log_parser.py, connection_parser.py
    - Implements complete mission normalization from actual Deadside.log analysis
    - Uses EmbedFactory for all outputs
    - Maintains guild isolation logic
    """

    def __init__(self, bot):
        self.bot = bot
        # All state dictionaries use guild_server keys for complete isolation
        self.last_log_position: Dict[str, int] = {}  # {guild_id}_{server_id} -> position
        self.log_file_hashes: Dict[str, str] = {}    # {guild_id}_{server_id} -> hash
        self.player_sessions: Dict[str, Dict[str, Any]] = {}  # {guild_id}_{player_id} -> session_data
        self.server_status: Dict[str, Dict[str, Any]] = {}    # {guild_id}_{server_id} -> status
        self.sftp_connections: Dict[str, asyncssh.SSHClientConnection] = {}  # {guild_id}_{server_id}_{host}_{port} -> connection
        self.file_states: Dict[str, Dict[str, Any]] = {}      # {guild_id}_{server_id} -> file_state
        self.player_lifecycle: Dict[str, Dict[str, Any]] = {} # {guild_id}_{player_id} -> lifecycle_data

        # Comprehensive log patterns from actual Deadside.log analysis
        self.patterns = self._compile_unified_patterns()

        # Complete mission normalization from real log data
        self.mission_mappings = self._get_complete_mission_mappings()

        # Load persistent state on startup
        asyncio.create_task(self._load_persistent_state())

    def _compile_unified_patterns(self) -> Dict[str, re.Pattern]:
        """Compile all log patterns from actual Deadside.log analysis"""
        return {
            # SERVER LIFECYCLE
            'log_rotation': re.compile(r'^Log file open, (\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'),
            'server_startup': re.compile(r'LogWorld: Bringing World.*up for play.*at (\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})'),
            'world_loaded': re.compile(r'LogLoad: Took .* seconds to LoadMap.*World_0'),
            'server_max_players': re.compile(r'playersmaxcount=(\d+)', re.IGNORECASE),

            # PLAYER CONNECTION LIFECYCLE - From actual log patterns
            'player_queue_join': re.compile(r'LogNet: Join request: /Game/Maps/world_\d+/World_\d+\?.*eosid=\|([a-f0-9]+).*Name=([^&\?]+)', re.IGNORECASE),
            'player_beacon_join': re.compile(r'LogBeacon: Beacon Join SFPSOnlineBeaconClient EOS:\|([a-f0-9]+)', re.IGNORECASE),
            'player_registered': re.compile(r'LogOnline: Warning: Player \|([a-f0-9]+) successfully registered!', re.IGNORECASE),
            'player_disconnect': re.compile(r'UChannel::Close: Sending CloseBunch.*UniqueId: EOS:\|([a-f0-9]+)', re.IGNORECASE),
            'player_cleanup': re.compile(r'UNetConnection::Close: Connection cleanup.*UniqueId: EOS:\|([a-f0-9]+)', re.IGNORECASE),

            # MISSION EVENTS - Patterns from actual Deadside.log
            'mission_respawn': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) will respawn in (\d+)', re.IGNORECASE),
            'mission_state_change': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to ([A-Z_]+)', re.IGNORECASE),
            'mission_ready': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to READY', re.IGNORECASE),
            'mission_initial': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to INITIAL', re.IGNORECASE),
            'mission_in_progress': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to IN_PROGRESS', re.IGNORECASE),
            'mission_completed': re.compile(r'LogSFPS: Mission (GA_[A-Za-z0-9_]+) switched to COMPLETED', re.IGNORECASE),

            # VEHICLE EVENTS
            'vehicle_spawn': re.compile(r'LogSFPS: \[ASFPSGameMode::NewVehicle_Add\] Add vehicle (BP_SFPSVehicle_[A-Za-z0-9_]+)', re.IGNORECASE),
            'vehicle_delete': re.compile(r'LogSFPS: \[ASFPSGameMode::NewVehicle_Del\] Del vehicle (BP_SFPSVehicle_[A-Za-z0-9_]+)', re.IGNORECASE),

            # TIMESTAMP EXTRACTION
            'timestamp': re.compile(r'\[(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2}:\d{3})\]')
        }

    def _get_complete_mission_mappings(self) -> Dict[str, str]:
        """
        Complete mission normalization from actual Deadside.log analysis
        Maps all discovered mission IDs to proper readable names
        """
        return {
            # Airport Missions
            'GA_Airport_mis_01_SFPSACMission': 'Airport Mission #1',
            'GA_Airport_mis_02_SFPSACMission': 'Airport Mission #2', 
            'GA_Airport_mis_03_SFPSACMission': 'Airport Mission #3',
            'GA_Airport_mis_04_SFPSACMission': 'Airport Mission #4',

            # Settlement Missions
            'GA_Beregovoy_Mis1': 'Beregovoy Settlement Mission',
            'GA_Settle_05_ChernyLog_Mis1': 'Cherny Log Settlement Mission',
            'GA_Settle_09_Mis_1': 'Settlement Mission #9',

            # Military Base Missions
            'GA_Military_02_Mis1': 'Military Base Mission #2',
            'GA_Military_03_Mis_01': 'Military Base Mission #3',
            'GA_Military_04_Mis1': 'Military Base Mission #4',
            'GA_Military_04_Mis_2': 'Military Base Mission #4B',

            # Industrial Missions
            'GA_Ind_01_m1': 'Industrial Zone Mission #1',
            'GA_Ind_02_Mis_1': 'Industrial Zone Mission #2',
            'GA_PromZone_6_Mis_1': 'Industrial Zone Mission #6',
            'GA_PromZone_Mis_01': 'Industrial Zone Mission A',
            'GA_PromZone_Mis_02': 'Industrial Zone Mission B',

            # Chemical Plant Missions
            'GA_KhimMash_Mis_01': 'Chemical Plant Mission #1',
            'GA_KhimMash_Mis_02': 'Chemical Plant Mission #2',

            # City Missions
            'GA_Kamensk_Ind_3_Mis_1': 'Kamensk Industrial Mission',
            'GA_Kamensk_Mis_1': 'Kamensk City Mission #1',
            'GA_Kamensk_Mis_2': 'Kamensk City Mission #2', 
            'GA_Kamensk_Mis_3': 'Kamensk City Mission #3',
            'GA_Krasnoe_Mis_1': 'Krasnoe City Mission',
            'GA_Vostok_Mis_1': 'Vostok City Mission',

            # Special Locations
            'GA_Bunker_01_Mis1': 'Underground Bunker Mission',
            'GA_Lighthouse_02_Mis1': 'Lighthouse Mission #2',
            'GA_Elevator_Mis_1': 'Elevator Complex Mission #1',
            'GA_Elevator_Mis_2': 'Elevator Complex Mission #2',

            # Resource Missions
            'GA_Sawmill_01_Mis1': 'Sawmill Mission #1',
            'GA_Sawmill_02_1_Mis1': 'Sawmill Mission #2A',
            'GA_Sawmill_03_Mis_01': 'Sawmill Mission #3',
            'GA_Bochki_Mis_1': 'Barrel Storage Mission',
            'GA_Dubovoe_0_Mis_1': 'Dubovoe Resource Mission',
        }

    def normalize_mission_name(self, mission_id: str) -> str:
        """
        Normalize mission ID to readable name
        Returns proper name from mapping or generates descriptive fallback
        """
        if mission_id in self.mission_mappings:
            return self.mission_mappings[mission_id]

        # Generate intelligent fallback for unmapped missions
        if '_Airport_' in mission_id:
            return f"Airport Mission ({mission_id.split('_')[-1]})"
        elif '_Military_' in mission_id:
            return f"Military Mission ({mission_id.split('_')[-1]})"
        elif '_Settle_' in mission_id:
            return f"Settlement Mission ({mission_id.split('_')[-1]})"
        elif '_Ind_' in mission_id or '_PromZone_' in mission_id:
            return f"Industrial Mission ({mission_id.split('_')[-1]})"
        elif '_KhimMash_' in mission_id:
            return f"Chemical Plant Mission ({mission_id.split('_')[-1]})"
        elif '_Bunker_' in mission_id:
            return f"Bunker Mission ({mission_id.split('_')[-1]})"
        elif '_Sawmill_' in mission_id:
            return f"Sawmill Mission ({mission_id.split('_')[-1]})"
        else:
            # Extract readable parts from mission ID
            parts = mission_id.replace('GA_', '').replace('_Mis', '').replace('_mis', '').split('_')
            readable_parts = [part.capitalize() for part in parts if part.isalpha()]
            if readable_parts:
                return f"{' '.join(readable_parts)} Mission"
            else:
                return f"Special Mission ({mission_id})"

    def get_mission_level(self, mission_id: str) -> int:
        """Determine mission difficulty level based on type"""
        if any(keyword in mission_id.lower() for keyword in ['military', 'bunker', 'khimmash']):
            return 5  # High tier
        elif any(keyword in mission_id.lower() for keyword in ['airport', 'promzone', 'kamensk']):
            return 4  # High-medium tier
        elif any(keyword in mission_id.lower() for keyword in ['ind_', 'industrial']):
            return 3  # Medium tier
        elif any(keyword in mission_id.lower() for keyword in ['sawmill', 'lighthouse', 'elevator']):
            return 2  # Low-medium tier
        else:
            return 1  # Low tier

    async def process_mission_event(self, guild_id: str, mission_id: str, state: str, respawn_time: Optional[int] = None) -> Optional[discord.Embed]:
        """
        Process mission event and create normalized embed
        Uses EmbedFactory for consistent formatting
        """
        try:
            normalized_name = self.normalize_mission_name(mission_id)
            mission_level = self.get_mission_level(mission_id)

            # Create embed using EmbedFactory
            if state == 'READY':
                embed = EmbedFactory.create_mission_embed(
                    title="🎯 Mission Available",
                    description=f"**{normalized_name}** is now available for completion",
                    mission_id=mission_id,
                    level=mission_level,
                    state="READY",
                    color=0x00FF00
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")
            elif state == 'IN_PROGRESS':
                embed = EmbedFactory.create_mission_embed(
                    title="⚔️ Mission In Progress", 
                    description=f"**{normalized_name}** is currently being completed",
                    mission_id=mission_id,
                    level=mission_level,
                    state="IN_PROGRESS",
                    color=0xFFAA00
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")
            elif state == 'COMPLETED':
                embed = EmbedFactory.create_mission_embed(
                    title="✅ Mission Completed",
                    description=f"**{normalized_name}** has been completed successfully",
                    mission_id=mission_id,
                    level=mission_level,
                    state="COMPLETED",
                    color=0x0099FF
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")
            elif respawn_time:
                embed = EmbedFactory.create_mission_embed(
                    title="🔄 Mission Respawning",
                    description=f"**{normalized_name}** will respawn in {respawn_time} seconds",
                    mission_id=mission_id,
                    level=mission_level,
                    state="RESPAWN",
                    respawn_time=respawn_time,
                    color=0x888888
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")
            else:
                embed = EmbedFactory.create_mission_embed(
                    title="📋 Mission Update",
                    description=f"**{normalized_name}** state: {state}",
                    mission_id=mission_id,
                    level=mission_level,
                    state=state,
                    color=0x666666
                )
                # Add metadata for channel routing
                embed.set_footer(text="Mission Event • Powered by Discord.gg/EmeraldServers")

            return embed

        except Exception as e:
            logger.error(f"Failed to process mission event: {e}")
            return None

    async def process_player_connection(self, guild_id: str, player_id: str, player_name: str, event_type: str) -> Optional[discord.Embed]:
        """
        Process player connection event with unified lifecycle tracking
        Uses EmbedFactory for consistent formatting
        """
        try:
            # Update player session tracking
            session_key = f"{guild_id}_{player_id}"

            if event_type == 'joined':
                # Track player join
                self.player_sessions[session_key] = {
                    'player_id': player_id,
                    'player_name': player_name,
                    'guild_id': guild_id,
                    'joined_at': datetime.now(timezone.utc).isoformat(),
                    'status': 'online'
                }

                # Update voice channel with new player count
                await self.update_voice_channel(guild_id)

                embed = EmbedFactory.create_connection_embed(
                    title="🟢 Player Connected",
                    description=f"**{player_name}** has joined the server",
                    player_name=player_name,
                    player_id=player_id,
                    color=0x00FF00
                )
                # Add metadata for channel routing
                embed.set_footer(text="Connection Event • Powered by Discord.gg/EmeraldServers")

            elif event_type == 'disconnected':
                # Track player disconnect
                if session_key in self.player_sessions:
                    self.player_sessions[session_key]['status'] = 'offline'
                    self.player_sessions[session_key]['left_at'] = datetime.now(timezone.utc).isoformat()

                # Update voice channel with new player count
                await self.update_voice_channel(guild_id)

                embed = EmbedFactory.create_connection_embed(
                    title="🔴 Player Disconnected", 
                    description=f"**{player_name}** has left the server",
                    player_name=player_name,
                    player_id=player_id,
                    color=0xFF0000
                )
                # Add metadata for channel routing
                embed.set_footer(text="Connection Event • Powered by Discord.gg/EmeraldServers")
            else:
                return None

            return embed

        except Exception as e:
            logger.error(f"Failed to process player connection: {e}")
            return None

    async def parse_log_content(self, content: str, guild_id: str, server_id: str) -> List[discord.Embed]:
        """
        Parse log content and return list of embeds for events
        Unified processing of all log events with incremental tracking
        """
        embeds = []
        lines = content.splitlines()
        total_lines = len(lines)

        # Check for incremental processing (hot start)
        server_key = f"{guild_id}_{server_id}"
        stored_state = self.file_states.get(server_key, {})
        last_processed = stored_state.get('line_count', 0)

        # Only process new lines in hot start mode
        if last_processed > 0 and last_processed < total_lines:
            new_lines = lines[last_processed:]
            logger.info(f"🔥 HOT START: Processing {len(new_lines)} new lines ({last_processed+1} to {total_lines})")
            lines = new_lines
        elif last_processed >= total_lines:
            logger.info("📊 No new lines to process")
            return embeds
        else:
            # First run or file reset - process all lines
            logger.info(f"🆕 PROCESSING ALL LINES: {total_lines} total lines")

        # Update file state BEFORE processing to prevent reprocessing
        self.file_states[server_key] = {
            'line_count': total_lines,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }

        # Save persistent state immediately
        await self._save_persistent_state()

        processed_events = 0
        for line_idx, line in enumerate(lines):
            try:
                # Mission events
                for pattern_name, pattern in self.patterns.items():
                    if pattern_name.startswith('mission_'):
                        match = pattern.search(line)
                        if match:
                            if pattern_name == 'mission_respawn':
                                mission_id, respawn_time = match.groups()
                                embed = await self.process_mission_event(
                                    guild_id, mission_id, 'RESPAWN', int(respawn_time)
                                )
                            elif pattern_name == 'mission_state_change':
                                mission_id, state = match.groups()
                                embed = await self.process_mission_event(
                                    guild_id, mission_id, state
                                )
                            else:
                                continue

                            if embed:
                                embeds.append(embed)
                                processed_events += 1
                                logger.info(f"📋 Processed {pattern_name}: {mission_id if 'mission_id' in locals() else 'event'}")

                            # Safety check - prevent massive embed generation
                            if processed_events > len(lines) * 2:
                                logger.error(f"⚠️ SAFETY BREAK: Generated {processed_events} events from {len(lines)} lines - stopping")
                                break

                # Player connection events - with name extraction
                player_queue_join = self.patterns['player_queue_join'].search(line)
                if player_queue_join:
                    player_id, player_name = player_queue_join.groups()
                    # Store name for later use when player registers
                    player_key = f"{guild_id}_{player_id}"
                    self.player_lifecycle[player_key] = {
                        'name': player_name,
                        'queue_joined': datetime.now(timezone.utc).isoformat()
                    }

                player_registered = self.patterns['player_registered'].search(line)
                if player_registered:
                    player_id = player_registered.group(1)
                    player_key = f"{guild_id}_{player_id}"

                    # Get player name from lifecycle tracking
                    player_name = "Unknown Player"
                    if player_key in self.player_lifecycle:
                        player_name = self.player_lifecycle[player_key].get('name', 'Unknown Player')

                    embed = await self.process_player_connection(
                        guild_id, player_id, player_name, 'joined'
                    )
                    if embed:
                        embeds.append(embed)
                        processed_events += 1

                player_disconnect = self.patterns['player_disconnect'].search(line)
                if player_disconnect:
                    player_id = player_disconnect.group(1)
                    player_key = f"{guild_id}_{player_id}"

                    # Get player name from session tracking
                    session_key = f"{guild_id}_{player_id}"
                    player_name = "Unknown Player"
                    if session_key in self.player_sessions:
                        player_name = self.player_sessions[session_key].get('player_name', 'Unknown Player')
                    elif player_key in self.player_lifecycle:
                        player_name = self.player_lifecycle[player_key].get('name', 'Unknown Player')

                    embed = await self.process_player_connection(
                        guild_id, player_id, player_name, 'disconnected'
                    )
                    if embed:
                        embeds.append(embed)
                        processed_events += 1

                # Safety check after each line
                if processed_events > len(lines) * 2:
                    logger.error(f"⚠️ SAFETY BREAK: Generated {processed_events} events from {len(lines)} lines - stopping processing")
                    break

            except Exception as e:
                logger.error(f"Error processing log line: {e}")
                continue

        return embeds

    async def _load_persistent_state(self):
        """Load persistent state from database"""
        try:
            self.file_states = {}

            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                # Load file states from database
                state_doc = await self.bot.db_manager.db['parser_state'].find_one({'_id': 'unified_parser_state'})

                if state_doc and 'file_states' in state_doc:
                    self.file_states = state_doc['file_states']
                    logger.info(f"Loaded persistent state for unified parser - {len(self.file_states)} server states")
                else:
                    logger.info("No persistent state found, starting fresh")
            else:
                logger.info("Database not available for state loading")

        except Exception as e:
            logger.error(f"Failed to load persistent state: {e}")
            self.file_states = {}

    async def _save_persistent_state(self):
        """Save persistent state for incremental processing"""
        try:
            if hasattr(self.bot, 'db_manager') and self.bot.db_manager:
                # Save file states to database
                state_doc = {
                    '_id': 'unified_parser_state',
                    'file_states': self.file_states,
                    'last_updated': datetime.now(timezone.utc).isoformat()
                }

                await self.bot.db_manager.db['parser_state'].replace_one(
                    {'_id': 'unified_parser_state'},
                    state_doc,
                    upsert=True
                )
                logger.debug(f"Persistent state saved - {len(self.file_states)} server states")
            else:
                logger.debug("Database not available for state persistence")
        except Exception as e:
            logger.error(f"Failed to save persistent state: {e}")

    def reset_file_states(self, server_key: Optional[str] = None, guild_id: Optional[int] = None):
        """Reset file states to force cold start on next run"""
        if server_key:
            if server_key in self.file_states:
                del self.file_states[server_key]
                logger.info(f"Reset file state for {server_key}")
        elif guild_id:
            # Reset all states for a specific guild
            guild_prefix = f"{guild_id}_"
            keys_to_remove = [k for k in self.file_states.keys() if k.startswith(guild_prefix)]
            for key in keys_to_remove:
                del self.file_states[key]
            logger.info(f"Reset all file states for guild {guild_id} ({len(keys_to_remove)} servers)")
        else:
            self.file_states.clear()
            logger.info("Reset all file states")

    def get_guild_server_state(self, guild_id: int, server_id: str) -> Dict[str, Any]:
        """Get isolated state for a specific guild-server combination"""
        server_key = f"{guild_id}_{server_id}"
        return {
            'file_state': self.file_states.get(server_key, {}),
            'server_status': self.server_status.get(server_key, {}),
            'active_players': [
                session for session_key, session in self.player_sessions.items()
                if session_key.startswith(f"{guild_id}_") and session.get('status') == 'online'
            ],
            'sftp_connected': any(
                conn_key.startswith(f"{guild_id}_{server_id}_") 
                for conn_key in self.sftp_connections.keys()
            )
        }

    def cleanup_guild_state(self, guild_id: int):
        """Clean up all state for a guild (when bot leaves guild)"""
        guild_prefix = f"{guild_id}_"

        # Clean up all state dictionaries
        for state_dict in [self.file_states, self.player_sessions, self.server_status, 
                          self.player_lifecycle, self.last_log_position, self.log_file_hashes]:
            keys_to_remove = [k for k in state_dict.keys() if k.startswith(guild_prefix)]
            for key in keys_to_remove:
                del state_dict[key]

        # Close SFTP connections for this guild
        conn_keys_to_remove = [k for k in self.sftp_connections.keys() if k.startswith(guild_prefix)]
        for conn_key in conn_keys_to_remove:
            try:
                self.sftp_connections[conn_key].close()
            except:
                pass
            del self.sftp_connections[conn_key]

        logger.info(f"Cleaned up all state for guild {guild_id}")

    def get_parser_status(self) -> Dict[str, Any]:
        """Get parser status for debugging"""
        active_sessions = sum(1 for session in self.player_sessions.values() if session.get('status') == 'online')

        return {
            'active_sessions': active_sessions,
            'total_tracked_servers': len(self.file_states),
            'sftp_connections': len(self.sftp_connections),
            'file_states': {k: v for k, v in self.file_states.items()},
            'connection_status': 'healthy' if self.sftp_connections else 'no_connections'
        }

    async def update_voice_channel(self, guild_id: str):
        """Update voice channel player count (placeholder for future implementation)"""
        # TODO: Implement voice channel player count updates
        # This would require channel configuration and Discord API calls
        pass

    async def get_server_channel(self, guild_id: int, server_id: str, channel_type: str) -> Optional[int]:
        """Get server-specific channel ID with fallback logic"""
        try:
            guild_config = await self.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return None

            server_channels = guild_config.get('server_channels', {})

            # Try server-specific channel first
            if server_id in server_channels:
                channel_id = server_channels[server_id].get(channel_type)
                if channel_id:
                    return channel_id

            # Fall back to default server channels
            if 'default' in server_channels:
                channel_id = server_channels['default'].get(channel_type)
                if channel_id:
                    return channel_id

            # Legacy fallback to old channel structure
            return guild_config.get('channels', {}).get(channel_type)

        except Exception as e:
            logger.error(f"Failed to get {channel_type} channel for guild {guild_id}, server {server_id}: {e}")
            return None

    async def send_log_embeds(self, guild_id: int, server_id: str, embeds_data: List[Dict[str, Any]]):
        """Send log embeds to appropriate channels based on event type with server-specific routing"""
        try:
            if not embeds_data:
                return

            # Channel mapping for different event types
            channel_mapping = {
                'mission_event': 'events',
                'airdrop_event': 'events', 
                'helicrash_event': 'events',
                'trader_event': 'events',
                'vehicle_event': 'events',
                'player_connection': 'connections',
                'player_disconnection': 'connections'
            }

            for embed_data in embeds_data:
                embed_type = embed_data.get('type')
                channel_type = channel_mapping.get(embed_type)

                if not channel_type:
                    logger.warning(f"Unknown embed type: {embed_type}")
                    continue

                # Get server-specific channel with fallback
                channel_id = await self.get_server_channel(guild_id, server_id, channel_type)
                if not channel_id:
                    logger.debug(f"No {channel_type} channel configured for guild {guild_id}, server {server_id}")
                    continue

                channel = self.bot.get_channel(channel_id)
                if not channel:
                    logger.warning(f"Channel {channel_id} not found for {channel_type}")
                    continue

                try:
                    await channel.send(embed=discord.Embed.from_dict(embed_data.get('embed')))
                    logger.info(f"Sent {channel_type} event to {channel.name} (ID: {channel_id})")
                except Exception as e:
                    logger.error(f"Failed to send {channel_type} event to channel {channel_id}: {e}")

        except Exception as e:
            logger.error(f"Failed to send log embeds: {e}")

    def _determine_channel_type(self, embed: discord.Embed) -> Optional[str]:
        """Determine which channel type an embed should go to based on its content"""
        if not embed.title:
            return None

        title_lower = embed.title.lower()

        # Map embed types to channel types
        if any(keyword in title_lower for keyword in ['airdrop', 'crate']):
            return 'events'
        elif any(keyword in title_lower for keyword in ['mission', 'objective']):
            return 'events'
        elif any(keyword in title_lower for keyword in ['helicopter', 'heli', 'crash']):
            return 'events'
        elif any(keyword in title_lower for keyword in ['connect', 'disconnect', 'join', 'left']):
            return 'connections'
        elif any(keyword in title_lower for keyword in ['bounty']):
            return 'bounties'
        else:
            # Default to events for most server activities
            return 'events'

    async def run_log_parser(self):
        """Main parsing method - unified entry point with cold/hot start detection"""
        try:
            logger.info("Running unified log parser...")

            # Get all guilds from database for production processing
            if not hasattr(self.bot, 'db_manager') or not self.bot.db_manager:
                logger.error("Database not available for log parsing")
                return

            try:
                guilds_cursor = self.bot.db_manager.guilds.find({})
                guilds_list = await guilds_cursor.to_list(length=None)

                if not guilds_list:
                    logger.info("No guilds found in database")
                    return

                total_servers_processed = 0

                for guild_doc in guilds_list:
                    guild_id = guild_doc.get('_id')
                    guild_name = guild_doc.get('name', 'Unknown')
                    servers = guild_doc.get('servers', [])

                    if not servers:
                        logger.debug(f"No servers configured for guild {guild_name}")
                        continue

                    logger.info(f"Processing {len(servers)} servers for guild: {guild_name}")

                    for server in servers:
                        try:
                            await self.parse_server_logs(guild_id, server)
                            total_servers_processed += 1
                        except Exception as e:
                            logger.error(f"Failed to parse logs for server {server.get('name', 'Unknown')}: {e}")
                            continue

                logger.info(f"Unified log parser completed - processed {total_servers_processed} servers")

            except Exception as e:
                logger.error(f"Failed to process guilds: {e}")

        except Exception as e:
            logger.error(f"Error in unified log parser: {e}")

    async def _process_cold_start(self, content: str, guild_id: str, server_id: str):
        """Process content during cold start - parse events but suppress embeds"""
        lines = content.splitlines()
        total_lines = len(lines)
        events_parsed = 0

        logger.info(f"🧊 COLD START: Processing {total_lines} lines without embeds")

        for line in lines:
            try:
                # Parse mission events for state tracking only
                for pattern_name, pattern in self.patterns.items():
                    if pattern_name.startswith('mission_'):
                        match = pattern.search(line)
                        if match:
                            events_parsed += 1
                            # Track the event but don't create embeds
                            logger.debug(f"Cold start: Parsed {pattern_name} (no embed)")
                            break

            except Exception as e:
                logger.debug(f"Error processing cold start line: {e}")
                continue

        # Update file state after cold start to mark all lines as processed
        server_key = f"{guild_id}_{server_id}"
        self.file_states[server_key] = {
            'line_count': total_lines,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }

        # Also save persistent state
        await self._save_persistent_state()

        logger.info(f"🧊 COLD START completed: {events_parsed} events parsed without embeds, file state updated to {total_lines} lines for server {server_key}")

    async def parse_server_logs(self, guild_id: int, server: Dict[str, Any]):
        """Parse logs for a specific server using SFTP"""
        server_name = server.get('name', 'Unknown')
        try:
            server_id = str(server.get('_id', 'unknown'))
            host = server.get('host')
            ssh_port = server.get('port', 22)  # Use 'port' field like other parsers
            ssh_user = server.get('username')   # Use 'username' field like other parsers
            ssh_password = server.get('password')  # Use 'password' field like other parsers
            # Fix directory resolution logic to match killfeed parser format: {host}_{_id}/Logs/Deadside.log
            log_path = server.get('log_path', f'./{host}_{server_id}/Logs/Deadside.log')

            if not all([host, ssh_user, ssh_password]):
                logger.warning(f"Missing SFTP credentials for server {server_name} - host: {host}, user: {ssh_user}, pass: {'***' if ssh_password else 'None'}")
                return

            logger.info(f"Connecting to server {server_name} ({host}:{ssh_port})")

            # Create SFTP connection with host-specific key
            connection_key = f"{guild_id}_{server_id}_{host}_{ssh_port}"

            # Validate host format
            if not host or not isinstance(host, str) or len(host.strip()) == 0:
                logger.error(f"Invalid host format for server {server_name}: '{host}'")
                return

            host = host.strip()  # Clean any whitespace
            logger.info(f"Attempting SFTP connection to: {host}:{ssh_port} with user: {ssh_user}")

            # Use existing connection or create new one
            if connection_key not in self.sftp_connections:
                try:
                    conn = await asyncssh.connect(
                        host,
                        port=ssh_port,
                        username=ssh_user,
                        password=ssh_password,
                        known_hosts=None,
                        client_keys=None,
                        server_host_key_algs=['ssh-rsa', 'rsa-sha2-256', 'rsa-sha2-512'],
                        kex_algs=['diffie-hellman-group14-sha256', 'diffie-hellman-group16-sha512', 'ecdh-sha2-nistp256', 'ecdh-sha2-nistp384', 'ecdh-sha2-nistp521'],
                        encryption_algs=['aes128-ctr', 'aes192-ctr', 'aes256-ctr', 'aes128-cbc', 'aes192-cbc', 'aes256-cbc'],
                        mac_algs=['hmac-sha2-256', 'hmac-sha2-512', 'hmac-sha1'],
                        connect_timeout=30.0,  # 30 second timeout
                        login_timeout=30.0,
                        compression_algs=None  # Disable compression for better compatibility
                    )
                    self.sftp_connections[connection_key] = conn
                    logger.info(f"Successfully connected to {host}:{ssh_port}")
                except Exception as e:
                    logger.error(f"Failed to connect to {host}:{ssh_port}: {e}")
                    return

            # Get the SFTP connection
            conn = self.sftp_connections[connection_key]

            try:
                # Create SFTP client
                async with conn.start_sftp_client() as sftp:
                    logger.info(f"Reading log file: {log_path}")

                    # Check if log file exists
                    try:
                        file_stat = await sftp.stat(log_path)
                        file_size = file_stat.size
                        logger.info(f"Log file found - size: {file_size} bytes")

                        # Read the log file content
                        async with sftp.open(log_path, 'r') as log_file:
                            content = await log_file.read()

                        if content:
                            lines = content.splitlines()
                            logger.info(f"Read {len(lines)} lines from {server_name}")

                            # Check if this is a cold start or hot start
                            server_key = f"{guild_id}_{server_id}"
                            stored_state = self.file_states.get(server_key, {})
                            last_processed = stored_state.get('line_count', 0)

                            is_warm_start = last_processed > 0

                            if not is_warm_start:
                                logger.info(f"🧊 COLD START detected for {server_name} - processing all {len(lines)} lines without embeds")
                                await self._process_cold_start(content, str(guild_id), server_id)
                            else:
                                logger.info(f"🔥 HOT START detected for {server_name} - last processed: {last_processed}, current: {len(lines)}")
                                # Use the actual parse_log_content method for proper event processing
                                embeds = await self.parse_log_content(content, str(guild_id), server_id)

                                # Log parser status and results
                                mode = "warm start" if is_warm_start else "cold start"
                                logger.info(f"🔍 Parser running in {mode} mode - found {len(embeds)} events")

                                if embeds:
                                    logger.info(f"✅ Generated {len(embeds)} events from {server_name}")
                                    # Send embeds to configured channels
                                    await self.send_log_embeds(guild_id, server_id, embeds)
                                else:
                                    logger.info(f"📊 No new events generated from {server_name}")
                        else:
                            logger.info(f"Log file {log_path} is empty")

                    except FileNotFoundError:
                        logger.warning(f"Log file not found: {log_path}")
                    except Exception as e:
                        logger.error(f"Error reading log file {log_path}: {e}")

            except Exception as e:
                logger.error(f"SFTP error for {server_name}: {e}")

        except Exception as e:
            logger.error(f"Error in parse_server_logs for {server_name}: {e}")
            return