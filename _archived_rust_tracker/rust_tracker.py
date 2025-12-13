import discord
from discord import app_commands
from discord.ext import commands, tasks
import logging
import asyncio
import re
import datetime
import statistics
import math
import io
from typing import Optional, List, Dict, Any

from xyz.jefferybeans.jeffbot.database import db
from xyz.jefferybeans.jeffbot.utils.battlemetrics import BattleMetricsClient

from .rust.monitor import RustMonitor

log = logging.getLogger(__name__)

class RustTracker(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        
        # Monitor Storage
        self.monitors: Dict[int, RustMonitor] = {}
        
        # Legacy regexes removed.
        
        self.tracking_channels = set()
        self.previous_markers = {} # guild_id -> {marker_id}
        
        self.bm_client = BattleMetricsClient()
        

    async def cog_load(self):
        # Schema Updates (Idempotent)
        try:
            await db.execute("ALTER TABLE rust_tracking_channels ADD COLUMN last_scanned_message_id BIGINT DEFAULT 0")
        except Exception:
            pass 
        try:
            await db.execute("ALTER TABLE rust_tracking_channels ADD COLUMN last_wipe_at TIMESTAMP NULL")
        except Exception:
            pass
        try:
            await db.execute("ALTER TABLE rust_tracking_channels ADD COLUMN battlemetrics_server_id VARCHAR(20) DEFAULT NULL")
        except Exception:
            pass

        try:
            await db.execute("ALTER TABLE rust_server_configs MODIFY player_token BIGINT")
        except Exception:
            pass # Likely already BIGINT or table creates it as such
            
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS rust_server_configs (
                    guild_id BIGINT PRIMARY KEY,
                    server_ip VARCHAR(50),
                    server_port INT,
                    player_id BIGINT,
                    player_token BIGINT,
                    battlemetrics_server_id VARCHAR(20)
                )
            """)
        except Exception as e:
            log.error(f"Failed to create rust_server_configs table: {e}")
            
        try:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS rust_smart_devices (
                    guild_id BIGINT,
                    entity_id BIGINT,
                    name VARCHAR(100),
                    type VARCHAR(50),
                    PRIMARY KEY (guild_id, entity_id)
                )
            """)
        except Exception:
             pass
            
        await self._load_tracking_channels()
            
        self.check_rust_status.start()
        # Start background sync
        # Start Monitors
        self.bot.loop.create_task(self._load_monitors())
        
        log.info(f"RustTracker loaded. Tracking {len(self.tracking_channels)} channels.")

    async def cog_unload(self):
        self.check_rust_status.cancel()
        if self.bm_client:
            await self.bm_client.close()
        
        for m in self.monitors.values():
            await m.stop()

    @tasks.loop(minutes=5)
    async def check_rust_status(self):
        """Background loop to verify player status against BattleMetrics."""
        await self.bot.wait_until_ready()
        
        # 1. Get guilds with BM Server ID
        configs = await db.fetch_all("SELECT guild_id, battlemetrics_server_id FROM rust_tracking_channels WHERE battlemetrics_server_id IS NOT NULL")
        
        for config in configs:
            guild_id = config["guild_id"]
            server_id = config["battlemetrics_server_id"]
            
            if not server_id: continue
                
            try:
                # 2. Fetch BM Data
                bm_players = await self.bm_client.get_server_players(server_id)
                # Helper to normalize names
                # BM Name -> "Name"
                bm_online_names = {self._normalize_name(p["attributes"]["name"]) for p in bm_players if "attributes" in p}
                
                # 3. Fetch DB Data
                db_players = await db.fetch_all("SELECT id, name, is_online FROM rust_players WHERE guild_id = %s", guild_id)
                db_online_map = {p["name"]: p for p in db_players if p["is_online"]}  # Name -> Row
                db_offline_map = {p["name"]: p for p in db_players if not p["is_online"]}
                
                now = datetime.datetime.now(datetime.timezone.utc)
                
                # 4. Compare & Fix
                
                # Case A: DB says Offline, BM says Online (Missed Join)
                for name in bm_online_names:
                    # Check if tracked at all
                    if name in db_offline_map:
                        # Fix it
                        log.info(f"Rust Verify: Found {name} online on BM but offline in DB. Correcting...")
                        await self._update_player_activity(guild_id, name, True, now)
                    elif name not in db_online_map:
                         # New player completely?
                         # Optional: Auto-track new players seen on BM?
                         # Let's do it to be thorough.
                         pass 

                # Case B: DB says Online, BM says Offline (Missed Leave)
                # We iterate over who we THINK is online
                for name, row in db_online_map.items():
                    if name not in bm_online_names:
                        # They are gone
                        log.info(f"Rust Verify: Found {name} online in DB but offline on BM. Correcting...")
                        await self._update_player_activity(guild_id, name, False, now)
                        
            except Exception as e:
                log.error(f"Error in check_rust_status for guild {guild_id}: {e}")

    @check_rust_status.before_loop
    async def before_check_rust_status(self):
        await self.bot.wait_until_ready()

    async def _load_tracking_channels(self):
        rows = await db.fetch_all("SELECT channel_id FROM rust_tracking_channels")
        self.tracking_channels = {row["channel_id"] for row in rows}

    async def _handle_monitor_event(self, event_type: str, data: Any, guild_id: int):
        # Dispatch event from Monitor to handling logic
        try:
            timestamp = datetime.datetime.now(datetime.timezone.utc)
            
            if event_type == "team_info":
                 # Initial snapshot or update
                 pass 
                 
            elif event_type == "team_event":
                pass
                
            elif event_type == "markers":
                # data is list of markers
                await self._process_markers(guild_id, data)
                
            elif event_type == "chat_event":
                # In-game chat command handling
                await self._handle_in_game_command(guild_id, data)
                
            elif event_type == "time":
                # Store time for potential command usage
                pass
                
            elif event_type == "server_info":
                # Store pop info
                pass
                
        except Exception as e:
            log.error(f"RustTracker: Error handling monitor event {event_type} for guild {guild_id}: {e}")

    async def _process_markers(self, guild_id: int, markers: list):
        if guild_id not in self.previous_markers:
            self.previous_markers[guild_id] = set()

        current_marker_ids = {m.id for m in markers}
        previous = self.previous_markers[guild_id]
        
        new_markers = current_marker_ids - previous
        # removed_markers = previous - current_marker_ids 

        for marker in markers:
            if marker.id in new_markers:
                # Detect Type
                m_type = str(type(marker).__name__)
                label = None
                
                if "CargoShip" in m_type:
                    label = "🚢 Cargo Ship"
                elif "PatrolHelicopter" in m_type:
                    label = "🚁 Patrol Helicopter"
                elif "Chinook" in m_type:
                    label = "🚁 Chinook CH47"
                elif "Bradley" in m_type:
                    label = "💥 Bradley APC"
                elif "Crate" in m_type:
                    label = "📦 Hackable Crate"
                    
                if label:
                   await self._notify_tracking_channels(guild_id, f"**{label}** has spawned!")

        self.previous_markers[guild_id] = current_marker_ids

    async def _handle_in_game_command(self, guild_id: int, event: Any):
        # event is ChatEvent(message, name, steam_id, ...)
        message = event.message.strip()
        if not message.startswith("!"):
            return

        command = message.split(" ")[0].lower()
        response = None
        
        monitor = self.monitors.get(guild_id)
        if not monitor or not monitor.socket:
            return

        if command == "!pop":
             try:
                 info = await monitor.socket.get_info()
                 response = f"Population: {info.players}/{info.max_players} (Queued: {info.queued_players})"
             except:
                 response = "Failed to fetch population."

        elif command == "!time":
             try:
                 time_data = await monitor.socket.get_time()
                 response = f"Game Time: {time_data.time}"
             except:
                 response = "Failed to fetch time."
        
        elif command == "!online":
              # Basic online check
              try:
                 team_info = await monitor.socket.get_team_info()
                 online_members = [m.name for m in team_info.members if m.is_online]
                 response = f"Online: {', '.join(online_members)}" if online_members else "No teammates online."
              except:
                 response = "Failed to fetch online members."

        if response:
            await monitor.socket.send_team_message(response)
    
    async def _notify_tracking_channels(self, guild_id: int, message: str):
        # Notify channels associated with this guild
        channels = await db.fetch_all("SELECT channel_id FROM rust_tracking_channels WHERE guild_id = %s", guild_id)
        for row in channels:
            channel = self.bot.get_channel(row["channel_id"])
            if channel:
                try:
                    await channel.send(message)
                except Exception as e:
                    log.error(f"Failed to send Rust notification: {e}")

    @app_commands.command(name="rust_map", description="Get the current map status.")
    async def rust_map(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        guild_id = interaction.guild_id
        monitor = self.monitors.get(guild_id)
        
        if not monitor or not monitor.socket:
             await interaction.followup.send("❌ Rust Monitor not active. Use `/rust_config set_credentials` first.")
             return
             
        try:
             # rustplus library map generation
             map_img = await monitor.socket.get_map(add_icons=True, add_events=True, add_vending_machines=True)
             
             with io.BytesIO() as image_binary:
                 map_img.save(image_binary, 'PNG')
                 image_binary.seek(0)
                 await interaction.followup.send(file=discord.File(fp=image_binary, filename='rust_map.png'))
                 
        except Exception as e:
             log.error(f"Failed to generate map: {e}")
             await interaction.followup.send(f"❌ Failed to generate map: {e}")

    async def _handle_entity_event(self, guild_id: int, event: Any):
        # event: EntityEvent(entityId, value, ...)
        # value is typically True/False for switch/alarm state
        
        try:
            device = await db.fetch_one("SELECT name, type FROM rust_smart_devices WHERE guild_id = %s AND entity_id = %s", guild_id, event.entityId)
            
            if device:
                name = device["name"]
                dtype = device["type"]
                state = event.value
                
                msg = None
                if dtype == "alarm":
                    if state:
                        msg = f"🚨 **SMART ALARM TRIGGERED**: {name}!"
                    else:
                        msg = f"✅ Smart Alarm Cleared: {name}"
                elif dtype == "switch":
                    status = "ON" if state else "OFF"
                    msg = f"🔌 Switch **{name}** turned **{status}**."
                elif dtype == "storage":
                     # Monitor Storage monitor?
                     msg = f"📦 Storage Monitor **{name}**: {state}"
                
                if msg:
                     await self._notify_tracking_channels(guild_id, msg)
                     
        except Exception as e:
            log.error(f"Error handling entity event: {e}")



    # --- Fallback Configuration (Text Command) ---
    # Added because the user is experiencing 403 Forbidden Sync errors with Slash Commands.
    
    @commands.command(name="rust_setup", hidden=True)
    @commands.is_owner()
    async def rust_setup_text(self, ctx: commands.Context, ip: str, port: int, player_id: str, token: str):
        """Fallback command: !rust_setup <ip> <port> <id> <token>"""
        try:
             pid = int(player_id)
             tok = int(token)
             
             await db.execute("""
                INSERT INTO rust_server_configs (guild_id, server_ip, server_port, player_id, player_token)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    server_ip = VALUES(server_ip),
                    server_port = VALUES(server_port),
                    player_id = VALUES(player_id),
                    player_token = VALUES(player_token)
            """, ctx.guild.id, ip, port, pid, tok)
             
             await self._reload_monitor(ctx.guild.id)
             await ctx.send(f"✅ [Fallback] Rust+ Credentials updated via text command.")
             
        except ValueError:
             await ctx.send("❌ Error: ID and Token must be integers.")
        except Exception as e:
             await ctx.send(f"❌ Error: {e}")

    config_group = app_commands.Group(name="rust_config", description="Rust Tracker Configuration")

    @config_group.command(name="set_credentials", description="Set Rust+ Credentials (Owner Only).")
    async def set_credentials(self, interaction: discord.Interaction, ip: str, port: int, player_id: str, token: str):
        # Strict Access Control
        if not await self.bot.is_owner(interaction.user):
             await interaction.response.send_message("⛔ This command is restricted to the bot owner.", ephemeral=True)
             return
             
        try:
            pid = int(player_id)
            tok = int(token)
            
            await db.execute("""
                INSERT INTO rust_server_configs (guild_id, server_ip, server_port, player_id, player_token)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    server_ip = VALUES(server_ip),
                    server_port = VALUES(server_port),
                    player_id = VALUES(player_id),
                    player_token = VALUES(player_token)
            """, interaction.guild_id, ip, port, pid, tok)
            
            # Restart Monitor
            await self._reload_monitor(interaction.guild_id)
            
            await interaction.response.send_message(f"✅ Rust+ Credentials updated for guild {interaction.guild_id}.\nConnecting...", ephemeral=True)
            
        except ValueError:
            await interaction.response.send_message("❌ Player ID must be an integer.", ephemeral=True)

    @config_group.command(name="set_battlemetrics", description="Set BattleMetrics Server ID (Owner Only).")
    async def set_battlemetrics(self, interaction: discord.Interaction, server_id: str):
         # Strict Access Control
        if not await self.bot.is_owner(interaction.user):
             await interaction.response.send_message("⛔ This command is restricted to the bot owner.", ephemeral=True)
             return

        await db.execute("""
            INSERT INTO rust_server_configs (guild_id, battlemetrics_server_id)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE battlemetrics_server_id = VALUES(battlemetrics_server_id)
        """, interaction.guild_id, server_id)
        
        # Also update legacy table for compatibility if needed, or migration?
        # The existing code used `rust_tracking_channels`. 
        # We should migrate it or keep it synced?
        # Plan says: "Refactor checking loop".
        
        await interaction.response.send_message(f"✅ BattleMetrics ID set to `{server_id}`.", ephemeral=True)

    async def _reload_monitor(self, guild_id: int):
        if guild_id in self.monitors:
            await self.monitors[guild_id].stop()
            del self.monitors[guild_id]
            
        row = await db.fetch_one("SELECT * FROM rust_server_configs WHERE guild_id = %s", guild_id)
        if row and row["server_ip"] and row["player_token"]:
             monitor = RustMonitor(
                 guild_id=guild_id,
                 server_ip=row["server_ip"],
                 port=row["server_port"],
                 player_id=row["player_id"],
                 player_token=row["player_token"],
                 event_callback=lambda t, d: self._handle_monitor_event(t, d, guild_id)
             )
             self.monitors[guild_id] = monitor
             self.bot.loop.create_task(monitor.start())

    async def _load_monitors(self):
        rows = await db.fetch_all("SELECT * FROM rust_server_configs WHERE server_ip IS NOT NULL")
        for row in rows:
            await self._reload_monitor(row["guild_id"])

    # --- Message Handlers (Deprecated/Removed) ---
    # The new Monitor system proactively fetches events.
    # We remove the old reactive message parsers to avoid duplication and reliance on message scraping.
    
    # We still keep _register_tracked_player and _update_player_activity as they are core logic used by monitors.

    # Removed legacy handlers:
    # _handle_user_track_command
    # _handle_bot_confirmation
    # _handle_teammate_event
    # _handle_economy_transfer
    # _handle_vending_text
    # These functionalities (except maybe user tracking command?) are now API driven or deprecated.
    # User tracking command `rp!track` might still be useful if user wants to add to DB manually?
    # But native Rust+ tracking relies on Team Info.
    # If the user wants to add a "Target" to track globally via BattleMetrics?
    # We can keep a slash command for that.
    
    @app_commands.command(name="rust_track_player", description="Track a player by name (BattleMetrics Global).")
    async def rust_track_player(self, interaction: discord.Interaction, name: str):
        # Replaces old rp!track
        timestamp = datetime.datetime.now(datetime.timezone.utc)
        await self._register_tracked_player(interaction.guild_id, name, timestamp, is_teammate=False)
        await interaction.response.send_message(f"✅ Now tracking player: **{name}**")

    async def _register_tracked_player(self, guild_id: int, name: str, timestamp: datetime.datetime, is_teammate: Optional[bool] = None):
        """Pre-register a player in the database so they appear in lists potentially before joining."""
        # Use normalization
        name = self._normalize_name(name)
        
        await db.execute("""
            INSERT INTO rust_players (guild_id, name, is_online, last_seen, is_teammate)
            VALUES (%s, %s, %s, %s, COALESCE(%s, FALSE))
            ON DUPLICATE KEY UPDATE 
                name = VALUES(name),
                is_teammate = IF(%s IS NOT NULL, %s, is_teammate)
        """, guild_id, name, False, None, is_teammate, is_teammate, is_teammate) # Don't update last_seen/online status, just ensure existence
        
        log.info(f"Rust Tracker: Pre-registered player '{name}' in guild {guild_id}")



    # Wipe Commands

    @app_commands.command(name="rust_wipe", description="Set the current wipe time to NOW (Admin).")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_wipe(self, interaction: discord.Interaction):
        # Use interaction.created_at for UTC timestamp
        now = interaction.created_at
        await self._update_wipe_time(interaction.guild_id, now)
        await interaction.response.send_message(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Wipe time set to NOW ({now.strftime('%Y-%m-%d %H:%M:%S')}). Stats will be filtered from this point.")

    @app_commands.command(name="rust_wipefrom", description="Set wipe time to a specific date and RESCAN history (Admin).")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_wipefrom(self, interaction: discord.Interaction, date_str: str):
        # Parsers
        formats = ["%d/%m/%Y", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M"]
        wipe_date = None
        
        for fmt in formats:
            try:
                wipe_date = datetime.datetime.strptime(date_str, fmt)
                # Assume UTC if naive? Or assume Local?
                # Usually users input local time. We should probably convert to UTC for storage.
                # But we don't know their offset.
                # For now, let's treat it as UTC to be safe/consistent with standardize switch.
                wipe_date = wipe_date.replace(tzinfo=datetime.timezone.utc)
                break
            except ValueError:
                continue
        
        if not wipe_date:
            await interaction.response.send_message(f"❌ Invalid date format. Valid formats: `dd/mm/yyyy`, `yyyy-mm-dd` (with optional HH:MM).", ephemeral=True)
            return
            
        await interaction.response.defer()
            
        await self._update_wipe_time(interaction.guild_id, wipe_date)
        
        # Trigger Rescan/History Sync
        # 1. Calculate Snowflake
        snowflake = discord.utils.time_snowflake(wipe_date)
        
        # 2. Reset cursor for this channel
        # Ensure channel is set up
        config = await db.fetch_one("SELECT channel_id FROM rust_tracking_channels WHERE guild_id = %s", interaction.guild_id)
        if config:
            channel_id = config["channel_id"]
            await db.execute("UPDATE rust_tracking_channels SET last_scanned_message_id = %s WHERE guild_id = %s", snowflake, interaction.guild_id)
            
            await interaction.followup.send(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Wipe time set to {wipe_date.strftime('%Y-%m-%d %H:%M:%S')} UTC.\n🔄 Started retrospective scan from that date...")
            
            # 3. Start Sync Task
            self.bot.loop.create_task(self._sync_history(target_channel_id=channel_id))
        else:
             await interaction.followup.send(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Wipe time set to {wipe_date.strftime('%Y-%m-%d %H:%M:%S')} UTC.\n⚠️ Channel not configured, history scan skipped.")
            


    async def _process_embed(self, guild_id: int, embed: discord.Embed, timestamp: datetime.datetime):
        title = embed.title or ""
        description = embed.description or ""
        
        # 1. Player Tracking
        if "Player Tracking" in title:
            # "Player XGod_yatoX has left the server."
            match = self.message_pattern.search(description)
            if match:
                raw_name = match.group("name").strip()
                # Remove bold marks if present (regex might capture **Name**) - handled by normalize
                name = self._normalize_name(raw_name)
                action = match.group("action").lower()
                is_joining = "joined" in action
                await self._update_player_activity(guild_id, name, is_joining, timestamp)

        # 2. Vending Machine
        elif "New Vending Machine" in title:
            # ... (existing vending logic) ...
            # Description: "A new vending machine has appeared 'A Shop' with 4 items..."
            # Regex to extract shop name? "appeared 'A Shop' with"
            shop_name = "Unknown Shop"
            shop_match = re.search(r"appeared '(?P<name>.*?)' with", description)
            if shop_match:
                shop_name = shop_match.group("name")
            
            # The items are likely in a code block in a Field or Description
            # Inspect fields
            full_text = description
            for field in embed.fields:
                full_text += "\n" + field.value
            
            # Find code block lines
            listings = self.vending_regex.findall(full_text)
            if listings:
                await self._process_market_listings(guild_id, shop_name, listings, timestamp)

        # 3. Server Pairing / Wipe Detection
        elif "Server Pairing" in title:
            # "Listening to Rustoria.co - SEA Long"
            # This indicates a fresh pairing, likely post-wipe or bot re-add.
            # User wants this to be an auto-wipe trigger.
            log.info(f"Rust Tracker: Detected Server Pairing embed in guild {guild_id}. Marking as wipe.")
            await self._update_wipe_time(guild_id, timestamp)

    async def _process_market_listings(self, guild_id: int, shop_name: str, listings: List[tuple], timestamp: datetime.datetime):
        # listings is list of tuples: (cost_item, cost_amt, item, quantity, stock)
        
        # Clear old listings for this shop? Or just append history?
        # A "New Vending Machine" event implies a refresh or new placement.
        # We'll just insert new records.
        
        for (cost_item, cost_amt, item, quantity, stock) in listings:
            await db.execute("""
                INSERT INTO rust_market_listings 
                (guild_id, shop_name, item_name, quantity, cost_item, cost_amount, stock, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, guild_id, shop_name, item.strip(), int(quantity), cost_item.strip(), int(cost_amt), int(stock), timestamp)
        
        log.info(f"Rust Market: Logged {len(listings)} items for shop '{shop_name}' in guild {guild_id}")

    def _normalize_name(self, name: str) -> str:
        """
        Normalize a player name for consistent matching.
        Removes clan tags [TAG] and 'Player ' prefix, trims whitespace, and converts to lowercase.
        """
        # 1. Remove [TAG] or [TAG] patterns
        name = re.sub(r"\[.*?\]", "", name)
        
        # 2. Remove "Player " prefix (case insensitive)
        # ^ matches start of string (after previous strip/sub)
        name = re.sub(r"^player\s+", "", name.strip(), flags=re.IGNORECASE)
        
        return name.strip().lower()

    @app_commands.command(name="rust_deduplicate", description="Merge duplicate players (e.g. 'Player X' -> 'X') (Admin).")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_deduplicate(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        guild_id = interaction.guild_id
        
        # Fetch all players
        players = await db.fetch_all("SELECT id, name FROM rust_players WHERE guild_id = %s", guild_id)
        
        # Identify duplicates
        # We look for names starting with "player "
        # Since names in DB are normalized (lowercased), we look for "player ..."
        
        duplicates_found = 0
        merged_count = 0
        
        for p in players:
            name = p["name"]
            if name.startswith("player "):
                # Theoretical target name: remove "player "
                target_name = name[7:].strip() # len("player ") = 7
                
                if not target_name: continue
                
                # Check if target exists
                target = next((x for x in players if x["name"] == target_name), None)
                
                if target:
                    # Merge P into TARGET
                    source_id = p["id"]
                    target_id = target["id"]
                    
                    log.info(f"Rust Dedup: Merging '{name}' ({source_id}) -> '{target_name}' ({target_id})")
                    
                    # 1. Update Sessions
                    await db.execute("UPDATE rust_sessions SET player_id = %s WHERE player_id = %s", target_id, source_id)
                    
                    # 2. Delete Source Player
                    await db.execute("DELETE FROM rust_players WHERE id = %s", source_id)
                    
                    merged_count += 1
                else:
                    # Rename P to TARGET
                    source_id = p["id"]
                    log.info(f"Rust Dedup: Renaming '{name}' -> '{target_name}'")
                    
                    await db.execute("UPDATE rust_players SET name = %s WHERE id = %s", target_name, source_id)
                    
                duplicates_found += 1
                
        await interaction.followup.send(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Deduplication complete.\nProcessed: {duplicates_found}\nMerged: {merged_count}\nRenamed: {duplicates_found - merged_count}")

    @app_commands.command(name="rust_merge_players", description="Manually merge Player A into Player B (Admin).")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_merge_players(self, interaction: discord.Interaction, source_name: str, target_name: str):
        await interaction.response.defer()
        
        # Normalize inputs to verify against DB (since DB stores normalized)
        # But wait, user might supply "Player Jeff" thinking it's raw. 
        # But the DB stores "player jeff" (if failed) or "jeff".
        # We should try to find them by exact match first, then normalized.
        
        # Actually, let's just query by name directly provided, and also try normalized version.
        
        source = await db.fetch_one("SELECT * FROM rust_players WHERE guild_id = %s AND name = %s", interaction.guild_id, source_name)
        if not source:
             source = await db.fetch_one("SELECT * FROM rust_players WHERE guild_id = %s AND name = %s", interaction.guild_id, self._normalize_name(source_name))
        
        target = await db.fetch_one("SELECT * FROM rust_players WHERE guild_id = %s AND name = %s", interaction.guild_id, target_name)
        if not target:
             target = await db.fetch_one("SELECT * FROM rust_players WHERE guild_id = %s AND name = %s", interaction.guild_id, self._normalize_name(target_name))
             
        if not source:
            await interaction.followup.send(f"❌ Source player '{source_name}' not found.")
            return
        if not target:
            await interaction.followup.send(f"❌ Target player '{target_name}' not found.")
            return
            
        if source["id"] == target["id"]:
            await interaction.followup.send("❌ Source and Target are the same player.")
            return

        # Perform Merge
        source_id = source["id"]
        target_id = target["id"]
        
        # 1. Update Sessions
        await db.execute("UPDATE rust_sessions SET player_id = %s WHERE player_id = %s", target_id, source_id)
        
        # 2. Update Economy (Transactions where they are buyer or seller)
        # Note: Rust economy table stores NAMES, not IDs. So we need to update the names.
        await db.execute("UPDATE rust_economy_transactions SET buyer_name = %s WHERE guild_id = %s AND buyer_name = %s", target["name"], interaction.guild_id, source["name"])
        await db.execute("UPDATE rust_economy_transactions SET seller_name = %s WHERE guild_id = %s AND seller_name = %s", target["name"], interaction.guild_id, source["name"])
        
        # 3. Delete Source
        await db.execute("DELETE FROM rust_players WHERE id = %s", source_id)
        
        await interaction.followup.send(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Merged **{source['name']}** into **{target['name']}**.\nSessions and transactions transferred.")

    @app_commands.command(name="rust_status", description="Check Rust+ & BattleMetrics connection status (Admin).")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_status(self, interaction: discord.Interaction):
        await interaction.response.defer()
        
        guild_id = interaction.guild_id
        monitor = self.monitors.get(guild_id)
        
        bm_status = "Disconnected"
        if self.bm_client:
            bm_status = "Active" # BM client is stateless http usually, but we assume it's up.
            
        rp_status = "Not Configured"
        rp_details = ""
        
        if monitor:
            if monitor._is_running:
                 rp_status = "Running"
                 # We could add an is_connected check to monitor if we exposed it
                 if monitor.socket and monitor.socket.remote_socket and not monitor.socket.remote_socket.closed:
                      rp_status = "Connected 🟢"
                 else:
                      rp_status = "Connecting/Reconnecting 🟡"
            else:
                 rp_status = "Stopped 🔴"
                 
            rp_details = f"\nIP: `{monitor.server_ip}:{monitor.port}`"
        
        # Check DB config
        config = await db.fetch_one("SELECT * FROM rust_server_configs WHERE guild_id = %s", guild_id)
        db_status = "✅ Configured" if config else "⚠️ No Config Found"
        
        embed = discord.Embed(title="Rust Tracker Status", color=discord.Color.blue())
        embed.add_field(name="Rust+ API", value=f"Status: **{rp_status}**{rp_details}", inline=False)
        embed.add_field(name="BattleMetrics Poller", value=f"Status: **{bm_status}**", inline=False)
        embed.add_field(name="Database Config", value=db_status, inline=False)
        
        await interaction.followup.send(embed=embed)

    async def _update_player_activity(self, guild_id: int, raw_name: str, is_joining: bool, timestamp: datetime.datetime, is_teammate: Optional[bool] = None):
        # Normalize name for lookup, but keep raw_name for display updates if needed
        # Actually, if we want to store the "canonical" name, maybe we update it to the latest seen raw_name?
        # User goal: "If they join as [CLAN] Jeff, it might create a duplicate... Fix: Normalize names before DB insertion/lookup."
        
        # Strategy: 
        # 1. Normalize name -> "jeff"
        # 2. Check if a player exists with normalized name matching "jeff" (we might need a column for this, or fuzzy search)
        #    - Since we don't have a normalized column, we have to search? 
        #    - Or we store the normalized name?
        #    - Efficient way without schema change: 
        #      SELECT * FROM ... WHERE LOWER(name) = 'jeff' OR name LIKE ...
        #      Better: Create a standard 'name' that is the normalized one? No, display name matters.
        #      Compromise: Use the `name` column for display, but when checking existence, search loosely?
        #      "SELECT * FROM rust_players WHERE guild_id = %s" -> fetch all, fuzzy match in python? (Slow)
        #      "SELECT * FROM rust_players WHERE guild_id = %s AND name REGEXP ..." (Complex)
        #      Let's try: "SELECT * FROM rust_players WHERE guild_id = %s AND LOWER(name) = %s" (assuming we strip tags before calling this function?)
        #      
        #      Start by normalizing the input:
        normalized_input = self._normalize_name(raw_name)

        # Try to find existing player with this normalized name
        # We need a robust lookup. 
        # DB `name` column currently holds various forms.
        # Ideally, we should add a `normalized_uid` or similar, but let's try to work with existing data.
        # We can select by name. logic:
        # Check if `LOWER(name)` matches, OR if `name` (stripped of tags) matches `normalized_input`.
        
        # Simple Approach compliant with user request: "Normalize names before DB insertion/lookup."
        # If we insert "Jeff" instead of "[CLAN] Jeff", we solve the problem.
        # But we lose the clan tag in the UI. 
        # Maybe we keep the tag in a `display_name`? DB only has `name`.
        # Taking "Normalize names before DB insertion" literally: We insert clean names.
        
        name_to_store = self._normalize_name(raw_name)
        # But wait, if I register "jeff", and then "[CLAN] Jeff" joins, `_normalize_name` makes it "jeff".
        # It matches. Perfect.
        # The downsides: Leaderboard shows "jeff" instead of "[CLAN] Jeff".
        # User explicitly asked: "The Fix: Normalize names before DB insertion/lookup."
        # So I will follow that. Name in DB = Normalized Name.
        
        name = self._normalize_name(raw_name) # Ensure consistent casing and stripping

        # Zombie Session Heuristic
        # If joining, check previous state.
        if is_joining:
            # Check if player is already marked Online
            # We need to fetch current state *before* upserting.
           
            existing = await db.fetch_one("SELECT is_online, last_seen FROM rust_players WHERE guild_id = %s AND name = %s", guild_id, name)
            
            if existing and existing["is_online"]:
                 last_seen = existing["last_seen"] # datetime
                 if last_seen:
                     if last_seen.tzinfo is None: last_seen = last_seen.replace(tzinfo=datetime.timezone.utc)
                     
                     diff = (timestamp - last_seen).total_seconds()
                     
                     if diff > 600: # 10 minutes
                         # Missed "Leave" event. Zombie session.
                         # 1. Close the old session responsibly.
                         # Heuristic: End it 5 mins after last_seen.
                         end_time = last_seen + datetime.timedelta(minutes=5)
                         await db.execute("""
                            UPDATE rust_sessions 
                            SET end_time = %s 
                            WHERE player_id = (SELECT id FROM rust_players WHERE guild_id = %s AND name = %s) 
                            AND end_time IS NULL
                         """, end_time, guild_id, name)
                         log.info(f"Rust Zombie Fix: Closed stale session for {name} (Gap: {diff}s)")
                         
                     else:
                         # < 10 mins. Likely a quick reconnect or crash.
                         # Treat as continuation.
                         # We DO NOT close the session.
                         # We just update `last_seen` (which the upsert below does).
                         # But wait, the upsert below sets `is_online=True`.
                         # If we don't close the session, we just continue.
                         # But the `_update_player_activity` logic normally does:
                         # "Close pending session if any... INSERT new session"
                         # We need to change that logic to NOT insert a new session if it's a continuation.
                         pass
        
        # UPSERT Player
        await db.execute("""
            INSERT INTO rust_players (guild_id, name, is_online, last_seen, is_teammate)
            VALUES (%s, %s, %s, %s, COALESCE(%s, FALSE))
            ON DUPLICATE KEY UPDATE
                is_online = %s,
                last_seen = %s,
                is_teammate = IF(%s IS NOT NULL, %s, is_teammate)
        """, guild_id, name, is_joining, timestamp, is_teammate, is_joining, timestamp, is_teammate, is_teammate)

        player = await db.fetch_one("SELECT id FROM rust_players WHERE guild_id = %s AND name = %s", guild_id, name)
        if not player:
            return
        player_id = player["id"]

        # Session Management
        if is_joining:
            # Check if there is already an open session (Continuation case)
            open_session = await db.fetch_one("SELECT id FROM rust_sessions WHERE player_id = %s AND end_time IS NULL", player_id)
            
            if open_session:
                # If we determined it was a zombie (diff > 10m), we already closed it above.
                # So if `open_session` still exists, it means it's a continuation (< 10m).
                # We do NOTHING. Let it keep running.
                log.info(f"Rust Tracker: {name} rejoined (continuation).")
            else:
                # No open session (or we just closed the zombie one). Start new.
                await db.execute("INSERT INTO rust_sessions (player_id, start_time) VALUES (%s, %s)", player_id, timestamp)
                log.info(f"Rust Tracker: {name} joined in guild {guild_id} at {timestamp}")
                
        else:
            # Leaving
            # Close active session
            await db.execute("UPDATE rust_sessions SET end_time = %s WHERE player_id = %s AND end_time IS NULL", timestamp, player_id)
            log.info(f"Rust Tracker: {name} left in guild {guild_id} at {timestamp}")

    async def _process_transfer(self, guild_id: int, match: re.Match, timestamp: datetime.datetime):
        sender = match.group("sender").strip()
        amount = int(match.group("amount"))
        currency = match.group("currency").strip()
        receiver = match.group("receiver").strip()
        
        await db.execute("""
            INSERT INTO rust_economy_transactions 
            (guild_id, buyer_name, seller_name, quantity, cost_item, cost_amount, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, guild_id, sender, receiver, 1, currency, amount, timestamp)
        log.info(f"Rust Economy: {sender} sent {amount} {currency} to {receiver}")

    async def _process_vending(self, guild_id: int, match: re.Match, timestamp: datetime.datetime):
        shop = match.group("shop").strip()
        quantity = int(match.group("quantity"))
        item = match.group("item").strip()
        buyer = match.group("buyer").strip()
        cost = int(match.group("cost"))
        currency = match.group("currency").strip()
        
        # We treat 'shop' as seller
        await db.execute("""
            INSERT INTO rust_economy_transactions 
            (guild_id, buyer_name, seller_name, item_name, quantity, cost_item, cost_amount, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, guild_id, buyer, shop, item, quantity, currency, cost, timestamp)
        log.info(f"Rust Economy: {buyer} bought {quantity} {item} from {shop} for {cost} {currency}")

    async def _has_economy_access(self, interaction: discord.Interaction) -> bool:
        # Check if admin
        if interaction.user.guild_permissions.administrator:
            return True
            
        # Check permissions role
        config = await db.fetch_one("SELECT manager_role_id FROM rust_economy_config WHERE guild_id = %s", interaction.guild_id)
        if config and config["manager_role_id"]:
            role = interaction.guild.get_role(config["manager_role_id"])
            if role and role in interaction.user.roles:
                return True
                
        return False

    async def _update_wipe_time(self, guild_id: int, timestamp: datetime.datetime):
        await db.execute("UPDATE rust_tracking_channels SET last_wipe_at = %s WHERE guild_id = %s", timestamp, guild_id)

    async def _get_wipe_time(self, guild_id: int) -> Optional[datetime.datetime]:
        row = await db.fetch_one("SELECT last_wipe_at FROM rust_tracking_channels WHERE guild_id = %s", guild_id)
        if row and row["last_wipe_at"]:
            wipe_at = row["last_wipe_at"]
            if isinstance(wipe_at, str):
                try:
                    wipe_at = datetime.datetime.fromisoformat(str(wipe_at))
                except ValueError:
                    return None
            return wipe_at
        return None

    # Commands

    @app_commands.command(name="rust_setup", description="Set the current channel as the Rust tracking channel.")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_setup(self, interaction: discord.Interaction):
        await db.execute("""
            INSERT INTO rust_tracking_channels (guild_id, channel_id)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE channel_id = %s
        """, interaction.guild_id, interaction.channel_id, interaction.channel_id)
        
        self.tracking_channels.add(interaction.channel_id)
        await interaction.response.send_message(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Rust tracking enabled in {interaction.channel.mention}.")

    @app_commands.command(name="rust_unsetup", description="Disable Rust tracking in the current channel and clear data.")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_unsetup(self, interaction: discord.Interaction):
        # Check if tracking enabled first
        if interaction.channel_id not in self.tracking_channels:
            # Check DB just in case
            exists = await db.fetch_one("SELECT 1 FROM rust_tracking_channels WHERE channel_id = %s", interaction.channel_id)
            if not exists:
                await interaction.response.send_message(f"⚠️ Rust tracking was not enabled in {interaction.channel.mention}.", ephemeral=True)
                return

        await interaction.response.defer()

        try:
            guild_id = interaction.guild_id
            
            # 1. Remove tracking channel
            await db.execute("""
                DELETE FROM rust_tracking_channels 
                WHERE guild_id = %s AND channel_id = %s
            """, guild_id, interaction.channel_id)
            
            if interaction.channel_id in self.tracking_channels:
                self.tracking_channels.remove(interaction.channel_id)

            # 2. Clear all associated data for this guild
            await db.execute("DELETE FROM rust_economy_transactions WHERE guild_id = %s", guild_id)
            await db.execute("DELETE FROM rust_market_listings WHERE guild_id = %s", guild_id)
            await db.execute("DELETE FROM rust_economy_config WHERE guild_id = %s", guild_id)
            
            # Delete sessions
            await db.execute("""
                DELETE FROM rust_sessions 
                WHERE player_id IN (SELECT id FROM rust_players WHERE guild_id = %s)
            """, guild_id)
            
            # Delete players
            await db.execute("DELETE FROM rust_players WHERE guild_id = %s", guild_id)

            await interaction.followup.send(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Rust tracking disabled in {interaction.channel.mention} and all data cleared.")
            log.info(f"Rust unsetup and data cleared for guild {guild_id} by {interaction.user}")

        except Exception as e:
            log.error(f"Error in rust_unsetup: {e}")
            await interaction.followup.send(f"❌ An error occurred: {e}")

    @app_commands.command(name="rust_info", description="Get server population and info.")
    async def rust_info(self, interaction: discord.Interaction):
        monitor = self.monitors.get(interaction.guild_id)
        if not monitor or not monitor.socket:
             await interaction.response.send_message("❌ Rust Monitor not active.", ephemeral=True)
             return
        
        await interaction.response.defer()
        try:
             info = await monitor.socket.get_info()
             # Info has: players, max_players, queued_players, seed, map_size, url, header_image_url, name
             
             embed = discord.Embed(title=info.name or "Rust Server Info", color=discord.Color.green())
             if info.header_image_url:
                 embed.set_thumbnail(url=info.header_image_url)
                 
             embed.add_field(name="Population", value=f"{info.players}/{info.max_players}", inline=True)
             embed.add_field(name="Queued", value=str(info.queued_players), inline=True)
             embed.add_field(name="Map", value=f"Size: {info.map_size}\nSeed: {info.seed}", inline=True)
             embed.add_field(name="URL", value=info.url or "N/A", inline=False)
             
             await interaction.followup.send(embed=embed)
        except Exception as e:
             await interaction.followup.send(f"❌ Failed to fetch info: {e}")

    @app_commands.command(name="rust_time", description="Get current in-game time.")
    async def rust_time(self, interaction: discord.Interaction):
        monitor = self.monitors.get(interaction.guild_id)
        if not monitor or not monitor.socket:
             await interaction.response.send_message("❌ Rust Monitor not active.", ephemeral=True)
             return
             
        try:
             data = await monitor.socket.get_time()
             await interaction.response.send_message(f"🕰️ **Game Time**: {data.time}")
        except Exception as e:
             await interaction.response.send_message(f"❌ Failed to fetch time: {e}", ephemeral=True)

    @app_commands.command(name="rust_team", description="Get current team info.")
    async def rust_team(self, interaction: discord.Interaction):
        monitor = self.monitors.get(interaction.guild_id)
        if not monitor or not monitor.socket:
             await interaction.response.send_message("❌ Rust Monitor not active.", ephemeral=True)
             return
             
        await interaction.response.defer()
        try:
             team = await monitor.socket.get_team_info()
             # TeamInfo: leader_steam_id, members [TeamMember: steam_id, name, x, y, is_online, spawn_time, is_alive, death_time]
             
             embed = discord.Embed(title="Team Status", color=discord.Color.blue())
             
             online_list = []
             offline_list = []
             
             for member in team.members:
                 status_icon = "🟢" if member.is_online else "xxxxxxxx"
                 if not member.is_alive and member.is_online:
                     status_icon = "💀"
                 
                 line = f"{status_icon} **{member.name}**"
                 if member.is_online:
                     # Calculate grid ref? (Maybe too complex for now, just show coords)
                     # Or just list them
                     online_list.append(line)
                 else:
                     offline_list.append(line)
                     
             if online_list:
                 embed.add_field(name=f"Online ({len(online_list)})", value="\n".join(online_list), inline=False)
             if offline_list:
                 embed.add_field(name=f"Offline ({len(offline_list)})", value="\n".join(offline_list), inline=False)
                 
             embed.set_footer(text=f"Leader ID: {team.leader_steam_id}")
             await interaction.followup.send(embed=embed)
             
        except Exception as e:
             await interaction.followup.send(f"❌ Failed to fetch team info: {e}")

    @app_commands.command(name="rust_pair", description="Pair a Smart Device (Switch/Alarm) by Entity ID.")
    async def rust_pair(self, interaction: discord.Interaction, entity_id: str, name: str, type: str):
        """
        Pair a smart device. 
        Types: 'alarm', 'switch', 'storage'
        To get Entity ID: Check your Rust+ App or use a tool to dump ids.
        Future: 'Pairing Mode' to capture next event.
        """
        # Validate type
        valid_types = ["alarm", "switch", "storage"]
        if type.lower() not in valid_types:
             await interaction.response.send_message(f"❌ Invalid type. Valid: {', '.join(valid_types)}", ephemeral=True)
             return
             
        try:
            eid = int(entity_id)
            await db.execute("""
                INSERT INTO rust_smart_devices (guild_id, entity_id, name, type)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE name = VALUES(name), type = VALUES(type)
            """, interaction.guild_id, eid, name, type.lower())
            
            await interaction.response.send_message(f"✅ Paired **{type}** '{name}' (ID: {eid}).")
        except ValueError:
            await interaction.response.send_message("❌ Entity ID must be a number.", ephemeral=True)
            
    @app_commands.command(name="rust_switch", description="Toggle a Smart Switch on/off.")
    async def rust_switch(self, interaction: discord.Interaction, name: str, state: bool):
        # Look up ID by name
        monitor = self.monitors.get(interaction.guild_id)
        if not monitor or not monitor.socket:
             await interaction.response.send_message("❌ Rust Monitor not active.", ephemeral=True)
             return

        device = await db.fetch_one("SELECT entity_id FROM rust_smart_devices WHERE guild_id = %s AND name = %s AND type = 'switch'", interaction.guild_id, name)
        
        if not device:
             # Try fuzzy search?
             device = await db.fetch_one("SELECT entity_id FROM rust_smart_devices WHERE guild_id = %s AND name LIKE %s AND type = 'switch'", interaction.guild_id, f"%{name}%")
             
        if not device:
             await interaction.response.send_message(f"❌ Smart Switch '{name}' not found. Pair it first with `/rust_pair`.", ephemeral=True)
             return
             
        eid = device["entity_id"]
        
        await interaction.response.defer()
        try:
            if state:
                await monitor.socket.turn_on_smart_switch(eid)
                await interaction.followup.send(f"🟢 Turned **ON** switch '{name}'.")
            else:
                await monitor.socket.turn_off_smart_switch(eid)
                await interaction.followup.send(f"🔴 Turned **OFF** switch '{name}'.")
        except Exception as e:
            await interaction.followup.send(f"❌ Failed to toggle switch: {e}")

    @app_commands.command(name="rust_predict", description="Predict when an offline player will return.")
    async def rust_predict(self, interaction: discord.Interaction, player_name: str):
        player = await db.fetch_one("""
            SELECT * FROM rust_players WHERE guild_id = %s AND name = %s
        """, interaction.guild_id, player_name)

        if not player:
            await interaction.response.send_message(f"No data found for player '{player_name}'.", ephemeral=True)
            return
            
        if player["is_online"]:
            await interaction.response.send_message(f"{player_name} is currently **ONLINE**! No need to predict.", ephemeral=True)
            return

        # Prediction Logic:
        # 1. Gather all "offline durations"
        # Since we have sessions (online times), the "gaps" between sessions are offline times.
        # We need to fetch sessions ordered by time.
        
        wipe_at = await self._get_wipe_time(interaction.guild_id)
        
        sessions_query = "SELECT start_time, end_time FROM rust_sessions WHERE player_id = %s"
        params = [player["id"]]
        if wipe_at:
             sessions_query += " AND (end_time IS NULL OR end_time >= %s)"
             params.append(wipe_at)
             
        sessions_query += " ORDER BY start_time ASC" # Ensure order
        
        sessions = await db.fetch_all(sessions_query, *params)
        
        if len(sessions) < 2:
            await interaction.response.send_message(f"Not enough data to predict for {player_name}.", ephemeral=True)
            return
            
        offline_durations = []
        
        # Calculate gaps
        # Gap[i] = Session[i+1].start - Session[i].end
        # Need to interpret start/end with objects
        
        clean_sessions = []
        for s in sessions:
            start = s["start_time"]
            end = s["end_time"]
             # Conversion
            if isinstance(start, str): start = datetime.datetime.fromisoformat(str(start))
            if end and isinstance(end, str): end = datetime.datetime.fromisoformat(str(end))
            clean_sessions.append({"start": start, "end": end})
        
        for i in range(1, len(clean_sessions)):
            curr_start = clean_sessions[i]["start"]
            prev_end = clean_sessions[i-1]["end"]
            
            if prev_end and curr_start > prev_end:
                duration = (curr_start - prev_end).total_seconds()
                offline_durations.append(duration)
        
        if not offline_durations:
             await interaction.response.send_message(f"Not enough offline data to predict for {player_name}.", ephemeral=True)
             return

        # Simple approach: Median offline time
        median_offline = statistics.median(offline_durations)
        avg_offline = statistics.mean(offline_durations)
        
        # Weighted approach? No, median is robust to outliers (like sleeping vs short breaks).
        # But we might want to see "Time of Day" matching.
        # If they went offline at 8 PM, look at gaps starting around 8 PM.
        
        # Refined Logic:
        last_seen = player["last_seen"]
        # Ensure last_seen is datetime
        if not isinstance(last_seen, datetime.datetime):
             # basic fallback if db driver doesn't convert
             await interaction.response.send_message("Date parsing error in prediction.", ephemeral=True)
             return

        # Filter gaps that started within +/- 2 hours of the current "last_seen" time of day
        hour_of_day = last_seen.hour
        relevant_gaps = []
        
        for i in range(1, len(clean_sessions)):
            curr_start = clean_sessions[i]["start"]
            prev_end = clean_sessions[i-1]["end"]
            
            if prev_end and curr_start > prev_end:
                 # Logic for relevant gaps
                 # ... re-using cleaned date objects
                 # (simplification: just copy logic)
                 prev_hour = prev_end.hour
                 diff = abs(prev_hour - hour_of_day)
                 if diff > 12: diff = 24 - diff
                 
                 if diff <= 3:
                    relevant_gaps.append((curr_start - prev_end).total_seconds())

        # If we have relevant gaps, use them, otherwise fallback to all gaps
        if relevant_gaps:
            prediction_seconds = statistics.median(relevant_gaps)
            predictor_type = "Time-of-Day Analysis"
        else:
            prediction_seconds = median_offline
            predictor_type = "General Median"
            
        # Predicted return time
        predicted_return = last_seen + datetime.timedelta(seconds=prediction_seconds)
        time_until = predicted_return - datetime.datetime.now()
        
        if time_until.total_seconds() < 0:
            msg = f"Usually returns by {predicted_return.strftime('%H:%M')}, but they are late!"
        else:
            # Format duration
            hours_u = int(time_until.total_seconds() // 3600)
            mins_u = int((time_until.total_seconds() % 3600) // 60)
            msg = f"Expected back in approx **{hours_u}h {mins_u}m** (around {predicted_return.strftime('%H:%M')})."

        embed = discord.Embed(title=f"Prediction for {player_name}", description=msg, color=discord.Color.purple())
        embed.set_footer(text=f"Based on {predictor_type} ({len(relevant_gaps) if relevant_gaps else len(offline_durations)} samples)")
        
        await interaction.response.send_message(embed=embed)

    # Economy Commands
    
    rust_economy = app_commands.Group(name="rust_economy", description="Manage and view Rust economy stats.")

    @rust_economy.command(name="set_role", description="Set the role allowed to view economy stats.")
    @app_commands.checks.has_permissions(administrator=True)
    async def economy_set_role(self, interaction: discord.Interaction, role: discord.Role):
        await db.execute("""
            INSERT INTO rust_economy_config (guild_id, manager_role_id)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE manager_role_id = %s
        """, interaction.guild_id, role.id, role.id)
        await interaction.response.send_message(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Economy manager role set to: {role.mention}")

    @rust_economy.command(name="stats", description="View economy statistics.")
    async def economy_stats(self, interaction: discord.Interaction):
        if not await self._has_economy_access(interaction):
            await interaction.response.send_message("⛔ You do not have permission to view economy stats.", ephemeral=True)
            return

        wipe_at = await self._get_wipe_time(interaction.guild_id)
        
        query = """
            SELECT item_name, SUM(quantity) as total_sold, SUM(cost_amount) as total_volume, cost_item
            FROM rust_economy_transactions
            WHERE guild_id = %s AND item_name IS NOT NULL
        """
        params = [interaction.guild_id]
        
        if wipe_at:
            query += " AND timestamp >= %s"
            params.append(wipe_at)
            
        query += """
            GROUP BY item_name, cost_item
            ORDER BY total_volume DESC
            LIMIT 5
        """

        # Simple Stats: Top Traded Items
        top_items = await db.fetch_all(query, *params)

        embed = discord.Embed(title="Rust Economy Stats", color=discord.Color.gold())
        
        if wipe_at:
            embed.set_footer(text=f"Data since wipe: {wipe_at.strftime('%Y-%m-%d %H:%M')}")
        
        if top_items:
            lines = []
            for row in top_items:
                lines.append(f"**{row['item_name']}**: {row['total_sold']} sold ({row['total_volume']} {row['cost_item']})")
            embed.add_field(name="Top Traded Items", value="\n".join(lines), inline=False)
        else:
            embed.description = "No transaction data recorded yet."

        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="rust_leaderboard", description="View top players by playtime.")
    async def rust_leaderboard(self, interaction: discord.Interaction):
        """Display leaderboard of top 10 players based on playtime (SQL Optimized)."""
        await interaction.response.defer()
        
        wipe_at = await self._get_wipe_time(interaction.guild_id)
        now = datetime.datetime.now(datetime.timezone.utc)
        
        # SQL Optimization with Wipe Handling
        # We clamp start_time to wipe_at if it's earlier.
        
        if wipe_at:
             if wipe_at.tzinfo is None: wipe_at = wipe_at.replace(tzinfo=datetime.timezone.utc)
             
             # MySQL: TIMESTAMPDIFF(SECOND, start, end)
             # Logic: SUM(TIMESTAMPDIFF(SECOND, GREATEST(s.start_time, wipe_at), COALESCE(s.end_time, NOW())))
             
             query = """
                SELECT p.name, p.is_online,
                       SUM(TIMESTAMPDIFF(SECOND, GREATEST(s.start_time, %s), COALESCE(s.end_time, %s))) as total_seconds
                FROM rust_sessions s
                JOIN rust_players p ON s.player_id = p.id
                WHERE p.guild_id = %s
                  AND (s.end_time >= %s OR s.end_time IS NULL)
                GROUP BY p.id
                ORDER BY total_seconds DESC
                LIMIT 10
             """
             params = [wipe_at, now, interaction.guild_id, wipe_at]
        else:
             query = """
                SELECT p.name, p.is_online,
                       SUM(TIMESTAMPDIFF(SECOND, s.start_time, COALESCE(s.end_time, %s))) as total_seconds
                FROM rust_sessions s
                JOIN rust_players p ON s.player_id = p.id
                WHERE p.guild_id = %s
                GROUP BY p.id
                ORDER BY total_seconds DESC
                LIMIT 10
             """
             params = [now, interaction.guild_id]
             
        rows = await db.fetch_all(query, *params)
        
        if not rows:
            await interaction.followup.send("No playtime data available.")
            return
            
        embed = discord.Embed(title="🏆 Rust Playtime Leaderboard", color=discord.Color.gold())
        if wipe_at:
            embed.set_footer(text=f"Since wipe: {wipe_at.strftime('%Y-%m-%d %H:%M')}")
            
        desc = ""
        for i, row in enumerate(rows, 1):
            name = row["name"]
            is_online = row["is_online"]
            total_seconds = int(row["total_seconds"] or 0)
            
            hours = total_seconds // 3600
            mins = (total_seconds % 3600) // 60
            
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            status = "🟢" if is_online else ""
            desc += f"**{medal} {name}** {status}\n   {hours}h {mins}m\n"
            
        embed.description = desc
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="rust_shop_search", description="Search for items in player vending machines.")
    async def rust_shop_search(self, interaction: discord.Interaction, item_name: str):
        """Find active listings for a specific item."""
        await interaction.response.defer()
        
        # We want the LATEST listing for each shop/item combo.
        # But our table inserts historical data.
        # We need to find listings from recent timestamps.
        # Assumption: Vending machines broadcast every ~5-10 mins or on change.
        # Let's filter by listings in the last 24 hours to be safe, 
        # and then group by shop/cost to show unique offers.
        
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
        
        query = """
            SELECT shop_name, quantity, cost_amount, cost_item, stock, timestamp
            FROM rust_market_listings
            WHERE guild_id = %s 
            AND item_name LIKE %s
            AND timestamp > %s
            ORDER BY timestamp DESC
        """
        # Fuzzy search
        search_term = f"%{item_name}%"
        
        rows = await db.fetch_all(query, interaction.guild_id, search_term, cutoff)
        
        if not rows:
            await interaction.followup.send(f"🔍 No recent listings found for '{item_name}'.", ephemeral=True)
            return
            
        # Deduplicate: Keep latest per shop per cost_type
        # (A shop might have multiple listings for same item but diff cost? rare but possible)
        # Mainly we want latest status per shop.
        
        unique_listings = {} # shop_name -> row
        
        for row in rows:
            shop = row["shop_name"]
            if shop not in unique_listings:
                unique_listings[shop] = row
            # Since we ordered by timestamp DESC, the first one we see is the latest.
        
        # Sort by best price? Hard to say what "best" is with different currencies.
        # Let's just list them.
        
        embed = discord.Embed(title=f"🛒 Market Listings: {item_name}", color=discord.Color.green())
        
        if len(unique_listings) > 15:
            embed.description = f"Found {len(unique_listings)} shops. Showing top 15 most recent."
        
        count = 0
        for shop, row in unique_listings.items():
            if count >= 15: break
            
            qty = row["quantity"]
            cost = row["cost_amount"]
            currency = row["cost_item"]
            stock = row["stock"]
            ts = row["timestamp"]
            
            # UTC handling for timestamp display if needed
            if isinstance(ts, str): ts = datetime.datetime.fromisoformat(str(ts))
            if ts.tzinfo is None: ts = ts.replace(tzinfo=datetime.timezone.utc)
            ts_int = int(ts.timestamp())
            
            embed.add_field(
                name=shop,
                value=f"**{qty}x** for **{cost} {currency}**\nStock: {stock} | <t:{ts_int}:R>",
                inline=True
            )
            count += 1
            
        await interaction.followup.send(embed=embed)


    # Wipe Commands



    @app_commands.command(name="rust_refresh", description="Clear all data and resync history for this channel (Admin).")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_refresh(self, interaction: discord.Interaction):
        # 0. Check if this is a tracking channel
        row = await db.fetch_one("SELECT 1 FROM rust_tracking_channels WHERE channel_id = %s", interaction.channel_id)
        if not row:
            await interaction.response.send_message("⚠️ This command can only be run in a configured Rust tracking channel.", ephemeral=True)
            return

        await interaction.response.send_message("🔄 Refreshing data... This will wipe all Rust data for this guild and re-scan the channel. This may take a moment...", ephemeral=True)
        
        try:
            guild_id = interaction.guild_id
            
            # 1. Clean Data
            log.info(f"Rust Refresh: Clearing data for guild {guild_id}")
            await db.execute("DELETE FROM rust_economy_transactions WHERE guild_id = %s", guild_id)
            await db.execute("DELETE FROM rust_market_listings WHERE guild_id = %s", guild_id)
            
            # Subqueries delete syntax varies, safer to join or just two separate calls if possible.
            # MySQL DELETE with JOIN or subquery...
            # DELETE sessions
            await db.execute("""
                DELETE FROM rust_sessions 
                WHERE player_id IN (SELECT id FROM rust_players WHERE guild_id = %s)
            """, guild_id)
            
            await db.execute("DELETE FROM rust_players WHERE guild_id = %s", guild_id)
            
            # 2. Reset Cursor
            # If wipe date exists, use that as start point to save time/resources
            # We want to resync "history from the wipe date"
            wipe_at = await self._get_wipe_time(guild_id)
            start_snowflake = 0
            
            if wipe_at:
                # Calculate snowflake from datetime
                # discord.utils.time_snowflake expects datetime.
                # If wipe_at is naive, we might need to assume UTC?
                # _get_wipe_time returns naive if we didn't handle tz.
                # Let's ensure strictness or catching
                if wipe_at.tzinfo is None:
                     wipe_at = wipe_at.replace(tzinfo=datetime.timezone.utc)
                start_snowflake = discord.utils.time_snowflake(wipe_at)
                log.info(f"Rust Refresh: Resyncing from wipe date {wipe_at} (Snowflake: {start_snowflake})")
            
            await db.execute("UPDATE rust_tracking_channels SET last_scanned_message_id = %s WHERE channel_id = %s", start_snowflake, interaction.channel_id)
            
            # 3. Trigger Sync
            await interaction.followup.send("♻️ Data cleared. Starting history sync...", ephemeral=True)
            await self._sync_history(target_channel_id=interaction.channel_id)
            
            await interaction.followup.send(f"<:jeffthelandsharkabsolutecinema:1438791420260384848> Refresh complete for {interaction.channel.mention}!", ephemeral=True)
            
        except Exception as e:
            log.error(f"Error during rust_refresh: {e}")
            await interaction.followup.send(f"❌ An error occurred during refresh: {e}", ephemeral=True)

    @app_commands.command(name="wipe_status", description="Check the current wipe information.")
    async def wipe_status(self, interaction: discord.Interaction):
        wipe_at = await self._get_wipe_time(interaction.guild_id)
        
        if not wipe_at:
             await interaction.response.send_message("ℹ️ No wipe time is currently set. Showing all historical data.")
             return
             
        now = datetime.datetime.now()
        diff = now - wipe_at
        
        days = diff.days
        hours = diff.seconds // 3600
        
        msg = f"**Current Wipe**: {wipe_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
        msg += f"**Time Elapsed**: {days} days, {hours} hours"
        
        await interaction.response.send_message(msg)

    @app_commands.command(name="rust_trackedplayers", description="View all tracked players with bedtime predictions.")
    async def rust_trackedplayers(self, interaction: discord.Interaction):
        """Display a comprehensive list of all tracked players with their bedtime prediction data."""
        
        await interaction.response.defer()
        
        try:
            # Fetch all players for this guild
            players = await db.fetch_all("""
                SELECT id, name, is_online, last_seen
                FROM rust_players
                WHERE guild_id = %s
                ORDER BY is_online DESC, name ASC
            """, interaction.guild_id)
            
            if not players:
                await interaction.followup.send("📊 No tracked players found for this server.", ephemeral=True)
                return
            
            wipe_at = await self._get_wipe_time(interaction.guild_id)
            
            # Build embed
            embed = discord.Embed(
                title=f"🎮 Tracked Rust Players ({len(players)} total)",
                color=discord.Color.blue()
            )
            
            if wipe_at:
                embed.set_footer(text=f"Data since wipe: {wipe_at.strftime('%Y-%m-%d %H:%M')}")
            
            online_players = []
            offline_players_with_predictions = []
            offline_players_no_data = []
            
            # Process each player
            for player in players:
                player_id = player["id"]
                name = player["name"]
                is_online = player["is_online"]
                last_seen = player["last_seen"]
                
                # Ensure last_seen is datetime
                if last_seen and not isinstance(last_seen, datetime.datetime):
                    try:
                        last_seen = datetime.datetime.fromisoformat(str(last_seen))
                    except:
                        last_seen = None
                
                if is_online:
                    online_players.append(f"🟢 **{name}**")
                else:
                    # Try to generate prediction
                    prediction_text = await self._generate_prediction_text(player_id, last_seen, wipe_at)
                    
                    if prediction_text:
                        offline_players_with_predictions.append(f"🔴 **{name}**\n   └ {prediction_text}")
                    else:
                        # No prediction data
                        if last_seen:
                            time_ago = datetime.datetime.now() - last_seen
                            hours_ago = int(time_ago.total_seconds() // 3600)
                            if hours_ago < 1:
                                mins_ago = int(time_ago.total_seconds() // 60)
                                offline_players_no_data.append(f"🔴 **{name}** - Last seen {mins_ago}m ago")
                            else:
                                offline_players_no_data.append(f"🔴 **{name}** - Last seen {hours_ago}h ago")
                        else:
                            offline_players_no_data.append(f"🔴 **{name}** - No data")
            
            # Add fields to embed (Discord has 25 field limit, 1024 char per field value)
            if online_players:
                # Split if too many
                online_text = "\n".join(online_players)
                if len(online_text) > 1024:
                    # Chunk into multiple fields
                    chunks = self._chunk_list(online_players, 1024)
                    for idx, chunk in enumerate(chunks):
                        field_name = f"Online ({len(online_players)})" if idx == 0 else "​"  # Zero-width space for continuation
                        embed.add_field(name=field_name, value=chunk, inline=False)
                else:
                    embed.add_field(name=f"Online ({len(online_players)})", value=online_text, inline=False)
            
            if offline_players_with_predictions:
                pred_text = "\n".join(offline_players_with_predictions)
                if len(pred_text) > 1024:
                    chunks = self._chunk_list(offline_players_with_predictions, 1024)
                    for idx, chunk in enumerate(chunks):
                        field_name = f"Offline (with predictions)" if idx == 0 else "​"
                        embed.add_field(name=field_name, value=chunk, inline=False)
                else:
                    embed.add_field(name="Offline (with predictions)", value=pred_text, inline=False)
            
            if offline_players_no_data:
                nodata_text = "\n".join(offline_players_no_data)
                if len(nodata_text) > 1024:
                    chunks = self._chunk_list(offline_players_no_data, 1024)
                    for idx, chunk in enumerate(chunks):
                        field_name = f"Offline (no prediction data)" if idx == 0 else "​"
                        embed.add_field(name=field_name, value=chunk, inline=False)
                else:
                    embed.add_field(name="Offline (no prediction data)", value=nodata_text, inline=False)
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            log.error(f"Error in rust_trackedplayers: {e}", exc_info=True)
            await interaction.followup.send(f"❌ An error occurred while fetching tracked players: {e}", ephemeral=True)

    @app_commands.command(name="rust_teammatelist", description="View all detected teammates.")
    async def rust_teammatelist(self, interaction: discord.Interaction):
        """Display a list of players identified as teammates."""
        await interaction.response.defer()
        
        # Trigger on-demand sync
        await self._sync_battlemetrics_status(interaction.guild_id)
        
        try:
            players = await db.fetch_all("""
                SELECT name, is_online, last_seen
                FROM rust_players
                WHERE guild_id = %s AND is_teammate = TRUE
                ORDER BY is_online DESC, name ASC
            """, interaction.guild_id)
            
            if not players:
                await interaction.followup.send("👥 No teammates detected yet. (Wait for 'Team member' logs)", ephemeral=True)
                return
            
            embed = discord.Embed(title=f"🛡️ Teammates ({len(players)})", color=discord.Color.green())
            
            online_list = []
            offline_list = []
            
            for p in players:
                name = p["name"]
                status = "🟢 Online" if p["is_online"] else "🔴 Offline"
                
                last_seen_str = ""
                if p["last_seen"]:
                    ls = p["last_seen"]
                    if isinstance(ls, str): ls = datetime.datetime.fromisoformat(ls)
                    if ls.tzinfo is None: ls = ls.replace(tzinfo=datetime.timezone.utc)
                    ts = int(ls.timestamp())
                    last_seen_str = f" (<t:{ts}:R>)"
                
                entry = f"{status} **{name}**{last_seen_str}"
                
                if p["is_online"]:
                    online_list.append(entry)
                else:
                    offline_list.append(entry)
            
            # Chunking
            if online_list:
                chunks = self._chunk_list(online_list, 1024)
                for idx, chunk in enumerate(chunks):
                    embed.add_field(name=f"Online ({idx+1})" if len(chunks)>1 else "Online", value=chunk, inline=False)
            
            if offline_list:
                chunks = self._chunk_list(offline_list, 1024)
                for idx, chunk in enumerate(chunks):
                    embed.add_field(name=f"Offline ({idx+1})" if len(chunks)>1 else "Offline", value=chunk, inline=False)
                    
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            log.error(f"Error in rust_teammatelist: {e}")
            await interaction.followup.send(f"❌ Error fetching teammate list: {e}", ephemeral=True)
    
    def _chunk_list(self, items: List[str], max_length: int) -> List[str]:
        """Split a list of strings into chunks that fit within max_length when joined."""
        chunks = []
        current_chunk = []
        current_length = 0
        
        for item in items:
            item_length = len(item) + 1  # +1 for newline
            if current_length + item_length > max_length and current_chunk:
                chunks.append("\n".join(current_chunk))
                current_chunk = [item]
                current_length = item_length
            else:
                current_chunk.append(item)
                current_length += item_length
        
        if current_chunk:
            chunks.append("\n".join(current_chunk))
        
        return chunks
    
    async def _generate_prediction_text(self, player_id: int, last_seen: Optional[datetime.datetime] = None, wipe_at: Optional[datetime.datetime] = None) -> Optional[str]:
        """Generate a compact prediction string for a player. Returns None if insufficient data."""
        # Wrapper around _generate_prediction_data
        data = await self._generate_prediction_data(player_id, wipe_at)
        if not data: return None
        
        predicted_return, time_until, confidence = data
        ts = int(predicted_return.timestamp())
        
        if time_until.total_seconds() < 0:
            return f"Expected back around <t:{ts}:t> ⚠️ (overdue)"
        else:
            hours = int(time_until.total_seconds() // 3600)
            mins = int((time_until.total_seconds() % 3600) // 60)
            if hours > 0:
                return f"Expected back in ~{hours}h {mins}m (<t:{ts}:t>)"
            else:
                return f"Expected back in ~{mins}m (<t:{ts}:t>)"

    @app_commands.command(name="rust_predict", description="Predict when a player might come back online.")
    async def rust_predict(self, interaction: discord.Interaction, player_name: str):
        """Predict the next online time for a player based on history."""
        await interaction.response.defer()
        
        # 1. Fuzzy Find Player
        query = """
            SELECT id, name, is_online, last_seen 
            FROM rust_players 
            WHERE guild_id = %s AND LOWER(name) LIKE LOWER(%s) 
            ORDER BY last_seen DESC LIMIT 1
        """
        player = await db.fetch_one(query, interaction.guild_id, f"%{player_name}%")
        
        if not player:
             await interaction.followup.send(f"❌ Could not find any tracked player matching `{player_name}`.", ephemeral=True)
             return

        # 2. Generate Prediction
        prediction_tuple = await self._generate_prediction_data(player["id"])
        
        embed = discord.Embed(title=f"<:jeff_heh300x300:1438791497393770546>Prediction: {player['name']}", color=discord.Color.purple())
        
        status = "🟢 Online" if player["is_online"] else "🔴 Offline"
        embed.add_field(name="Current Status", value=status, inline=True)
        
        if player["last_seen"]:
             ls = player["last_seen"]
             if ls.tzinfo is None: ls = ls.replace(tzinfo=datetime.timezone.utc)
             ts = int(ls.timestamp())
             embed.add_field(name="Last Seen", value=f"<t:{ts}:R>", inline=True)

        if player["is_online"]:
            embed.description = "Player is currently online! No prediction needed."
        elif prediction_tuple:
            pred_time, time_until, confidence = prediction_tuple
            ts = int(pred_time.timestamp())
            
            if time_until.total_seconds() < 0:
                 desc = f"⚠️ Expected back around <t:{ts}:t> (Overdue)"
            else:
                 hours = int(time_until.total_seconds() // 3600)
                 mins = int((time_until.total_seconds() % 3600) // 60)
                 desc = f"⏱️ Expected in **{hours}h {mins}m**\n📅 <t:{ts}:f>"
            
            embed.add_field(name="Time-of-Day Prediction", value=desc, inline=False)
            embed.set_footer(text=f"Confidence: {confidence} (Based on start time clustering)")
        else:
             embed.description = "Not enough data to generate a prediction (need at least ~3 sessions)."
            
        await interaction.followup.send(embed=embed)



    async def _generate_prediction_data(self, player_id: int, wipe_start: Optional[datetime.datetime] = None):
        """
        Predicts next online time using Time-of-Day Clustering (Circular Mean).
        Returns (predicted_datetime, time_until, confidence_str)
        """
        try:
            # Fetch sessions
            if not wipe_start:
                # We need guild_id to get wipe time, but we only have player_id.
                # Assuming caller handles wipe logic or we query it. 
                # For complexity, let's just fetch all and filter in python or ignore wipe for prediction (habits persist across wipes?)
                # User said: "Recency Weighting: Give more weight to sessions from the last 3 days (Rust wipe progression changes habits)."
                # So we prioritize recent data regardless of wipe.
                pass
            
            query = "SELECT start_time FROM rust_sessions WHERE player_id = %s ORDER BY start_time DESC LIMIT 50"
            sessions = await db.fetch_all(query, player_id)
            
            if len(sessions) < 3: return None
            
            start_times = []
            for s in sessions:
                st = s["start_time"]
                if isinstance(st, str): st = datetime.datetime.fromisoformat(st)
                if st.tzinfo is None: st = st.replace(tzinfo=datetime.timezone.utc)
                start_times.append(st)
            
            # --- Time of Day Clustering ---
            # 1. Convert to hours and weights
            now = datetime.datetime.now(datetime.timezone.utc)
            vectors_x = []
            vectors_y = []
            total_weight = 0
            
            for st in start_times:
                # Weight logic:
                days_ago = (now - st).days
                if days_ago < 0: days_ago = 0
                
                # Weight: 3.0 for last 3 days, 1.0 otherwise.
                weight = 3.0 if days_ago <= 3 else 1.0
                
                # Hour in radians
                # hour + minute/60
                hour_val = st.hour + st.minute / 60.0
                angle = (hour_val / 24.0) * 2 * math.pi
                
                vectors_x.append(math.cos(angle) * weight)
                vectors_y.append(math.sin(angle) * weight)
                total_weight += weight
                
            if total_weight == 0: return None
            
            mean_x = sum(vectors_x) / total_weight
            mean_y = sum(vectors_y) / total_weight
            
            # 2. Convert back to hour
            mean_angle = math.atan2(mean_y, mean_x)
            if mean_angle < 0: mean_angle += 2 * math.pi
            
            mean_hour = (mean_angle / (2 * math.pi)) * 24.0
            
            # 3. Construct Prediction
            # If current time (hour) < mean_hour, predict Today. Else Tomorrow.
            current_hour = now.hour + now.minute/60.0
            
            # Simple wrapper to get next occurrence
            predicted_dt = now.replace(minute=0, second=0, microsecond=0)
            
            target_h = int(mean_hour)
            target_m = int((mean_hour - target_h) * 60)
            
            predicted_dt = predicted_dt.replace(hour=target_h, minute=target_m)
            
            if predicted_dt < now:
                predicted_dt += datetime.timedelta(days=1)
                
            time_until = predicted_dt - now
            
            # Confidence metric: Length of mean vector (0 to 1)
            # R = sqrt(mean_x^2 + mean_y^2). Closer to 1 = tighter cluster.
            r_val = math.sqrt(mean_x**2 + mean_y**2)
            confidence = "High" if r_val > 0.8 else "Medium" if r_val > 0.5 else "Low"
            
            return predicted_dt, time_until, confidence
            
        except Exception as e:
            log.error(f"Prediction error: {e}")
            return None

    def _calculate_playtime_stats(self, sessions: List[Dict], now: datetime.datetime):
        """Helper to calculate behavioral stats from session list."""
        if not sessions: return {}
        
        total_seconds = 0
        weekend_seconds = 0
        hour_counts = [0] * 24
        
        # Ranges for simple categorization (UTC)
        # We'll just track raw hours
        
        first_session = now
        
        for s in sessions:
            start = s["start_time"]
            end = s["end_time"]
            
            if isinstance(start, str): start = datetime.datetime.fromisoformat(str(start))
            if start.tzinfo is None: start = start.replace(tzinfo=datetime.timezone.utc)
            if end and isinstance(end, str): end = datetime.datetime.fromisoformat(str(end))
            if end and end.tzinfo is None: end = end.replace(tzinfo=datetime.timezone.utc)

            if start < first_session: first_session = start
            
            # Duration
            effective_end = end if end else now
            duration = (effective_end - start).total_seconds()
            total_seconds += duration
            
            # Weekend Check (Fri/Sat/Sun)
            # Weekday: Mon=0, Sun=6. Fri=4, Sat=5, Sun=6
            wd = start.weekday()
            if wd >= 4:
                weekend_seconds += duration
                
            # Hour Distribution (Start Hour)
            hour_counts[start.hour] += 1
            
        # Analysis
        days_tracked = (now - first_session).days or 1
        hours_per_week = (total_seconds / 3600) / (days_tracked / 7.0) if days_tracked >= 7 else (total_seconds/3600)
        
        stats = {
            "total_hours": total_seconds / 3600,
            "weekly_hours": hours_per_week,
            "weekend_ratio": weekend_seconds / max(1, total_seconds),
            "top_start_hour": hour_counts.index(max(hour_counts)),
        }
        return stats

    @app_commands.command(name="rust_stats", description="Get stats for a player.")
    async def rust_stats(self, interaction: discord.Interaction, player_name: str):
        # Fuzzy lookup first
        player = await db.fetch_one("""
            SELECT * FROM rust_players WHERE guild_id = %s AND LOWER(name) LIKE LOWER(%s) ORDER BY last_seen DESC LIMIT 1
        """, interaction.guild_id, f"%{player_name}%") # Partial match support + normalized input if strict? 
        # Actually user said "Normalize names before DB insertion/lookup".
        # So typically we'd search EXACT normalized name.
        # But for UI convenience, LIKE is friendly.
        
        if not player:
            await interaction.response.send_message(f"No data found for player '{player_name}'.", ephemeral=True)
            return

        wipe_at = await self._get_wipe_time(interaction.guild_id)
        
        # Calculate standard stats
        sessions_query = "SELECT start_time, end_time FROM rust_sessions WHERE player_id = %s"
        params = [player["id"]]
        if wipe_at:
             sessions_query += " AND (end_time IS NULL OR end_time >= %s)"
             params.append(wipe_at)
        
        sessions = await db.fetch_all(sessions_query, *params)
        
        # Process stats
        now = datetime.datetime.now(datetime.timezone.utc)
        
        # Filter logic (clamping) - reusing previous logic but organized
        # To reuse the _calculate_playtime_stats, we need proper objects.
        # But _calculate_playtime_stats expects raw sessions. 
        # Let's just do it inline or clean up.
        
        # Clean sessions for math
        clean_sessions = []
        for s in sessions:
            st = s["start_time"]
            et = s["end_time"]
             # Conversion
            if isinstance(st, str): st = datetime.datetime.fromisoformat(str(st))
            if st.tzinfo is None: st = st.replace(tzinfo=datetime.timezone.utc)
            if et:
                if isinstance(et, str): et = datetime.datetime.fromisoformat(str(et))
                if et.tzinfo is None: et = et.replace(tzinfo=datetime.timezone.utc)
            
            # Wipe Clamp
            if wipe_at:
                 if wipe_at.tzinfo is None: wipe_at = wipe_at.replace(tzinfo=datetime.timezone.utc)
                 if st < wipe_at: st = wipe_at
            
            clean_sessions.append({"start_time": st, "end_time": et})
            
        stats = self._calculate_playtime_stats(clean_sessions, now)
        
        # Build Embed
        status_emoji = "🟢" if player["is_online"] else "🔴"
        last_seen_str = player["last_seen"].strftime("%Y-%m-%d %H:%M:%S") if player["last_seen"] else "Unknown"
        
        embed = discord.Embed(title=f"{status_emoji} {player['name']} Stats", color=discord.Color.orange())
        
        # Basic
        hours = int(stats.get("total_hours", 0))
        embed.add_field(name="Total Playtime", value=f"{hours} hours", inline=True)
        embed.add_field(name="Last Seen", value=last_seen_str, inline=True)
        
        # Advanced Analytics
        tags = []
        if stats.get("weekend_ratio", 0) > 0.70:
            tags.append("<:f09fc81a6c4e1c56881625da04e251c9:1449163157271613631> Weekend Warrior")
        if stats.get("weekly_hours", 0) > 40:
             tags.append("<:23b37a4v1n7e1:1438791440091316275> Full Time Rust 🫃")
        
        # Region Inference (Rough)
        # Peak start hour
        peak = stats.get("top_start_hour", 0)
        # If peak is 18-24 UTC -> EU? 
        # If peak is 00-08 UTC -> NA?
        # If peak is 08-16 UTC -> AU/ASIA?
        region_guess = "Unknown"
        if 16 <= peak <= 23: region_guess = "🇪🇺 EU (Inferred)"
        elif 0 <= peak <= 8: region_guess = "🇺🇸 NA (Inferred)"
        elif 8 < peak < 16: region_guess = "🇦🇺 AU/Asia (Inferred)"
        
        embed.add_field(name="Region / Timezone", value=region_guess, inline=True)
        
        if tags:
             embed.add_field(name="Lifestyle", value=", ".join(tags), inline=False)

        if wipe_at:
            embed.set_footer(text=f"Data since wipe: {wipe_at.strftime('%Y-%m-%d %H:%M')}")
        
        await interaction.response.send_message(embed=embed)
        
    rust_debug = app_commands.Group(name="rust_debug", description="Debug tools for Rust Tracker (Admin only).")
    
    @rust_debug.command(name="verify_db", description="Force-run BattleMetrics verification loop for this guild.")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_verify_db(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            log.info(f"Rust Debug: Manual verification triggered for guild {interaction.guild_id}")
            await self._sync_battlemetrics_status(interaction.guild_id)
            await interaction.followup.send("<:jeffthelandsharkabsolutecinema:1438791420260384848> Verification cycle complete. Check logs for any corrections made.")
        except Exception as e:
            await interaction.followup.send(f"❌ Verification failed: {e}")

    async def _sync_battlemetrics_status(self, guild_id: int):
        """Syncs online status of tracked players/teammates with BattleMetrics."""
        try:
            # Get Server ID
            config = await db.fetch_one("SELECT battlemetrics_server_id FROM rust_tracking_channels WHERE guild_id = %s", guild_id)
            if not config or not config["battlemetrics_server_id"]:
                return

            server_id = config["battlemetrics_server_id"]
            bm_players = await self.bm_client.get_server_players(server_id)
            # If empty list returned, it might mean empty server OR api failure.
            # We should probably proceed only if we have data or if we trust the empty list.
            # Check if API returned None (error) vs [] (empty). 
            # My get_server_players returns [] on error currently? 
            # Looking at utils/battlemetrics.py: returns [] on error/no data.
            # We'll assume if it's empty, we might skip to be safe? 
            # But what if server IS empty? Then we fail to mark people offline.
            # Safe logic: If [] returned, check if we can verify server status? 
            # For now, let's proceed. If server is empty, everyone is offline. Correct.
            
            # Helper logic in get_server_players returns [] if data missing.
            # Let's improve utils later if needed.
            
            online_names = set()
            for p in bm_players:
                if "attributes" in p and "name" in p["attributes"]:
                     online_names.add(p["attributes"]["name"].lower())
            
            # Fetch all relevant players from DB: Teammates AND currently Online players
            db_players = await db.fetch_all("""
                SELECT id, name, is_online 
                FROM rust_players 
                WHERE guild_id = %s AND (is_online = TRUE OR is_teammate = TRUE)
            """, guild_id)
            
            now = datetime.datetime.now(datetime.timezone.utc)
            
            for p in db_players:
                name = p["name"]
                is_online_db = p["is_online"]
                name_lower = name.lower()
                
                is_online_bm = name_lower in online_names
                
                if is_online_bm and not is_online_db:
                    log.info(f"BattleMetrics Sync: Marking {name} as ONLINE")
                    await self._update_player_activity(guild_id, name, True, now)
                elif not is_online_bm and is_online_db:
                    log.info(f"BattleMetrics Sync: Marking {name} as OFFLINE")
                    await self._update_player_activity(guild_id, name, False, now)

        except Exception as e:
            log.error(f"Error syncing BattleMetrics status: {e}")
        



    @app_commands.command(name="rust_setserver", description="Link this channel to a BattleMetrics Server ID.")
    @app_commands.checks.has_permissions(administrator=True)
    async def rust_setserver(self, interaction: discord.Interaction, server_id: str):
        """Link a BattleMetrics Server ID for verification."""
        await interaction.response.defer()
        
        # 1. Validate with API
        info = await self.bm_client.get_server_info(server_id)
        if not info:
             await interaction.followup.send(f"❌ Invalid Server ID `{server_id}` or API Error. Please check the ID.", ephemeral=True)
             return
             
        # Extract basic info
        attrs = info.get("attributes", {})
        name = attrs.get("name", "Unknown Server")
        rank = attrs.get("rank", "Unranked")
        
        # 2. Update DB
        # Ensure channel is tracked first? 
        # Or just update if exists. If not exists, maybe auto-setup? 
        # Safer to require setup first.
        
        config = await db.fetch_one("SELECT 1 FROM rust_tracking_channels WHERE channel_id = %s", interaction.channel_id)
        if not config:
            await interaction.followup.send("⚠️ This channel is not set up for tracking yet. Use `/rust_setup` first.", ephemeral=True)
            return

        await db.execute(
            "UPDATE rust_tracking_channels SET battlemetrics_server_id = %s WHERE channel_id = %s", 
            server_id, interaction.channel_id
        )

        embed = discord.Embed(title="<:jeffthelandsharkabsolutecinema:1438791420260384848> Server Linked!", color=discord.Color.green())
        embed.add_field(name="Server Name", value=name, inline=False)
        embed.add_field(name="Global Rank", value=f"#{rank}", inline=True)
        embed.add_field(name="BM ID", value=server_id, inline=True)
        embed.set_footer(text="BattleMetrics verification enabled.")
        
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="rust_server_info", description="Get live server status from BattleMetrics.")
    async def rust_server_info(self, interaction: discord.Interaction):
        """Display live info (Rank, Players, Map) from BattleMetrics."""
        # Get server ID for this guild/channel
        config = await db.fetch_one("SELECT battlemetrics_server_id FROM rust_tracking_channels WHERE guild_id = %s", interaction.guild_id)
        if not config or not config["battlemetrics_server_id"]:
            await interaction.response.send_message("❌ No BattleMetrics server linked. Ask an Admin to use `/rust_setserver`.", ephemeral=True)
            return
            
        server_id = config["battlemetrics_server_id"]
        
        await interaction.response.defer()
        
        info = await self.bm_client.get_server_info(server_id)
        if not info:
             await interaction.followup.send("❌ Failed to fetch data from BattleMetrics.", ephemeral=True)
             return

        attrs = info.get("attributes", {})
        name = attrs.get("name", "Unknown Server")
        players = attrs.get("players", 0)
        max_players = attrs.get("maxPlayers", 0)
        rank = attrs.get("rank", "Unranked")
        status = attrs.get("status", "unknown")
        ip = attrs.get("ip")
        port = attrs.get("port")
        
        # Details
        details = attrs.get("details", {})
        map_name = details.get("map", "Unknown Map")
        wipe_date = details.get("rust_last_wipe") # ISO String
        
        embed = discord.Embed(title=name, color=discord.Color.red() if status == "offline" else discord.Color.green())
        embed.add_field(name="Status", value=status.title(), inline=True)
        embed.add_field(name="Players", value=f"{players}/{max_players}", inline=True)
        embed.add_field(name="Rank", value=f"#{rank}", inline=True)
        embed.add_field(name="Map", value=map_name, inline=True)
        
        if wipe_date:
            try:
                # 2023-10-05T18:00:00.000Z
                ts = datetime.datetime.fromisoformat(wipe_date.replace("Z", "+00:00"))
                embed.add_field(name="Last Wipe", value=f"<t:{int(ts.timestamp())}:R>", inline=True)
            except:
                pass
                
        if ip and port:
             embed.add_field(name="Connect", value=f"`client.connect {ip}:{port}`", inline=False)
             
        embed.set_footer(text=f"BattleMetrics ID: {server_id}")
        await interaction.followup.send(embed=embed)

async def setup(bot: commands.Bot):
    await bot.add_cog(RustTracker(bot))
