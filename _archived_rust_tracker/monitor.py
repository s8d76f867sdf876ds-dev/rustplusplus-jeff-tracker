import asyncio
import logging
import time
from typing import Optional, List, Callable, Dict, Any
from rustplus import RustSocket, EntityEvent, TeamEvent, ChatEvent, ServerDetails

log = logging.getLogger(__name__)

class RustMonitor:
    def __init__(self, 
                 guild_id: int,
                 server_ip: str, 
                 port: int, 
                 player_id: int, 
                 player_token: int,
                 event_callback: Callable):
        self.guild_id = guild_id
        self.server_ip = server_ip
        self.port = port
        self.player_id = player_id
        self.player_token = player_token
        self.event_callback = event_callback # async func(event_type, data)
        
        self.socket: Optional[RustSocket] = None
        self._is_running = False
        self._reconnect_task = None
        self._polling_task = None
        
    async def start(self):
        if self._is_running:
            return

        self._is_running = True
        log.info(f"RustMonitor: Starting for guild {self.guild_id} ({self.server_ip}:{self.port})")
        
        # Initialize Socket
        # Initialize Socket
        # Newer versions of rustplus require a ServerDetails object
        details = ServerDetails(self.server_ip, str(self.port), self.player_id, self.player_token)
        self.socket = RustSocket(details)
        
        # Register Listeners
        # syntax: @TeamEvent(socket_instance)
        

        @TeamEvent(self.socket)
        async def on_team_event(event: TeamEvent):
            await self._handle_team_event(event)

        @ChatEvent(self.socket)
        async def on_chat_event(event: ChatEvent):
            await self._handle_chat_event(event)
            
        @EntityEvent(self.socket)
        async def on_entity_event(event: EntityEvent):
             await self._handle_entity_event(event)
            
        # Connection Loop
        self._reconnect_task = asyncio.create_task(self._connection_loop())
        # Polling Loop
        self._polling_task = asyncio.create_task(self._polling_loop())

    async def stop(self):
        self._is_running = False
        if self._reconnect_task:
            self._reconnect_task.cancel()
        if self._polling_task:
            self._polling_task.cancel()
        
        if self.socket:
            try:
                await self.socket.disconnect()
            except Exception:
                pass
        log.info(f"RustMonitor: Stopped for guild {self.guild_id}")

    async def _connection_loop(self):
        backoff = 5
        while self._is_running:
            try:
                log.info(f"RustMonitor: Connecting to {self.server_ip}:{self.port}...")
                await self.socket.connect()
                log.info(f"RustMonitor: Connected! Guild {self.guild_id}")
                
                # Fetch initial team info
                await self._fetch_initial_state()
                
                # Use the library's hang() method to keep the connection alive
                # This will block until the connection is lost
                try:
                    await self.socket.hang()
                except Exception as hang_error:
                    log.warning(f"RustMonitor: Connection hang interrupted: {hang_error}")
                    
                log.warning(f"RustMonitor: Disconnected from {self.server_ip}. Reconnecting in {backoff}s...")
                
            except Exception as e:
                log.error(f"RustMonitor: Connection error for guild {self.guild_id}: {e}")
            
            # Backoff
            if self._is_running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60) # Cap at 60s

    async def _polling_loop(self):
        """Poll for map markers, time, and population."""
        while self._is_running:
            if self.socket and self.socket.ws and not self.socket.ws.closed:
                try:
                    # 1. Markers (Cargo, Heli, etc.)
                    markers = await self.socket.get_markers()
                    await self.event_callback("markers", markers)
                    
                    # 2. Time
                    time_data = await self.socket.get_time()
                    await self.event_callback("time", time_data)
                    
                    # 3. Pop (get_info)
                    info = await self.socket.get_info()
                    await self.event_callback("server_info", info)

                except Exception as e:
                    # Don't log spam if disconnected, but log real errors
                    if "Connection closed" not in str(e):
                        log.debug(f"RustMonitor: Polling error: {e}")
            
            await asyncio.sleep(10) # Poll every 10s

    async def _fetch_initial_state(self):
        try:
            # Get Team Info
            team_info = await self.socket.get_team_info()
            # Dispatch "Initial Team" event
            await self.event_callback("team_info", team_info)
        except Exception as e:
            log.error(f"RustMonitor: Failed to fetch initial team info: {e}")

    async def _handle_team_event(self, event: TeamEvent):
        await self.event_callback("team_event", event)

    async def _handle_chat_event(self, event: ChatEvent):
        await self.event_callback("chat_event", event)
        
    async def _handle_entity_event(self, event: EntityEvent):
        await self.event_callback("entity_event", event)
