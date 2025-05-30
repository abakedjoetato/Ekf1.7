import discord
from discord.ext import commands
from typing import List, Optional

class ServerAutocomplete:
    """
    Utility class to handle server name autocompletion for Discord slash commands.
    This replaces direct server_id inputs with user-friendly server names.
    """

    @staticmethod
    async def get_servers_for_guild(guild_id: int, database):
        """
        Fetch servers associated with a specific guild from the database.

        Args:
            guild_id: The Discord guild ID
            database: MongoDB database instance

        Returns:
            List of server documents containing name and ID
        """
        # Get guild configuration which contains the servers array
        guild_doc = await database.guilds.find_one({"guild_id": guild_id})

        if guild_doc and "servers" in guild_doc:
            return guild_doc["servers"]

        return []

    @staticmethod
    async def autocomplete_server_name(ctx: discord.AutocompleteContext):
        """Autocomplete for server names (guild-scoped only)"""
        try:
            guild_id = ctx.interaction.guild.id if ctx.interaction.guild else None
            if not guild_id:
                return []

            # Get only the current guild's configuration
            guild_config = await ctx.bot.db_manager.get_guild(guild_id)
            if not guild_config:
                return []

            servers = []
            guild_servers = guild_config.get('servers', [])
            for server in guild_servers:
                server_id = str(server.get('_id', server.get('server_id', 'unknown')))
                server_name = server.get('name', server.get('server_name', f'Server {server_id}'))
                # Show name but return ID for the command
                display_name = f"{server_name} (ID: {server_id})"
                servers.append((display_name, server_id))

            # Filter based on user input
            user_input = ctx.value.lower()
            filtered_servers = [(display, sid) for display, sid in servers if user_input in display.lower()]

            # Return just the server_ids for the command, but show display names
            return [discord.OptionChoice(name=display, value=sid) for display, sid in filtered_servers[:25]]

        except Exception as e:
            logger.error(f"Failed to autocomplete server names: {e}")
            return []

    @staticmethod
    async def autocomplete_server_name_with_guild(ctx: discord.AutocompleteContext):
        """Autocomplete for server names (cross-guild for premium management)"""
        try:
            # Check if user is bot owner or in home server
            is_owner = False
            home_guild = False
            
            if hasattr(ctx.bot, 'cogs') and 'Premium' in ctx.bot.cogs:
                premium_cog = ctx.bot.cogs['Premium']
                is_owner = premium_cog.is_bot_owner(ctx.interaction.user.id)
                
                if ctx.interaction.guild:
                    home_guild_doc = await ctx.bot.db_manager.guilds.find_one({
                        "guild_id": ctx.interaction.guild.id,
                        "is_home_server": True
                    })
                    home_guild = bool(home_guild_doc)
            
            # Only allow cross-guild access for bot owner or home guild admins
            if not is_owner and not home_guild:
                return []

            # Get all guilds if authorized
            servers = []
            all_guilds = await ctx.bot.db_manager.guilds.find({}).to_list(length=None)
            
            for guild_doc in all_guilds:
                guild_id = guild_doc.get('guild_id')
                guild_name = guild_doc.get('guild_name', f'Guild {guild_id}')
                guild_servers = guild_doc.get('servers', [])
                
                for server in guild_servers:
                    server_id = str(server.get('_id', server.get('server_id', 'unknown')))
                    server_name = server.get('name', server.get('server_name', f'Server {server_id}'))
                    # Show guild name, server name, and return server_id
                    display_name = f"[{guild_name}] {server_name} (ID: {server_id})"
                    servers.append((display_name, server_id))

            # Filter based on user input
            user_input = ctx.value.lower()
            filtered_servers = [(display, sid) for display, sid in servers if user_input in display.lower()]

            # Return server_ids for the command, but show display names
            return [discord.OptionChoice(name=display, value=sid) for display, sid in filtered_servers[:25]]

        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to autocomplete cross-guild server names: {e}")
            return []

    @staticmethod
    def get_server_id_from_name(server_name: str, servers: List[dict]) -> Optional[str]:
        """
        Convert a server name to its corresponding server_id.

        Args:
            server_name: The name of the server
            servers: List of server documents

        Returns:
            The server ID if found, None otherwise
        """
        for server in servers:
            # Check against name and server_name fields
            if server.get("name") == server_name or server.get("server_name") == server_name:
                # Return _id with backward compatibility fallback
                return str(server.get("_id", server.get("server_id", "unknown")))
        return None


class AutocompleteCog(commands.Cog):
    """
    Cog providing server name autocomplete functionality for slash commands.
    """

    def __init__(self, bot):
        self.bot = bot

    # This is an example of how to use the autocomplete in a slash command
    @discord.slash_command(name="example", description="Example command using server autocomplete")
    @discord.option(
        name="server",
        description="Select a server",
        autocomplete=ServerAutocomplete.autocomplete_server_name
    )
    async def example_command(self, ctx, server: str):
        """Example command showing how to use server autocomplete"""
        # Here server is the server_id value from the autocomplete
        await ctx.respond(f"Selected server ID: {server}")

    # Add additional commands that need autocomplete here

def setup(bot):
    bot.add_cog(AutocompleteCog(bot))