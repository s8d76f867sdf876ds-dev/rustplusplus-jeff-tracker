# Rust Tracker Redesign Walkthrough

I have successfully redesigned the `rust_tracker` cog to move away from unreliable message scraping and use direct API connections.

## Key Changes

### 1. **Native Rust+ Integration**

- **New Component**: `cogs/rust/monitor.py` handles the Rust+ Socket connection.
- **Functionality**: Real-time detection of Team Events (Online/Offline/Death) and Team Chat.
- **Benefit**: Faster updates, no need for an external "RustPlusBot" spamming embeds.

### 2. **Refactored `RustTracker`**

- **Removed**: All regex-based message parsers (`message_pattern`, `teammate_pattern`, etc.).
- **Added**: `RustMonitor` integration in `_load_monitors`.
- **Added**: Restricted Configuration Commands.

### 3. **BattleMetrics Persistence**

- The bot still uses BattleMetrics for **global** server tracking (player lists, joins/leaves of non-teammates), ensuring you don't lose the "Global Tracker" feature that Rust+ natively lacks.

## Configuration & Usage

> [!CAUTION]
> Setup commands are restricted to the **Bot Owner**.

1. **Read the Guide**: A detailed setup guide is available at [rust_tracker_setup.md](file:///C:/Users/ppug/.gemini/antigravity/brain/8a83f8bb-15e0-409b-a52d-38178b5c7f19/rust_tracker_setup.md).
2. **Set Credentials**:

    ```
    /rust_config set_credentials ip:... port:... player_id:... token:...
    ```

3. **Check Status**:

    ```
    /rust_status
    ```

    This command now displays the connection health of both Rust+ and BattleMetrics.

## Verification

- **Dependencies**: Added `rustplus` to the environment.
- **Startup**: The cog initializes `RustMonitor` instances for configured guilds on load.
- **Fail-safe**: The monitor includes auto-reconnect logic with exponential backoff.
