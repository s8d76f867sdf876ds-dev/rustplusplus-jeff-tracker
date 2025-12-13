# Rust Tracker Setup Guide

This guide explains how to set up the Rust+ integration for the bot.

> [!CAUTION]
> **Access Control**: Configuration commands are restricted to the **Bot Owner** only. This is to protect sensitive credentials (IP, Port, Token).

## Prerequisites

1. **Rust Server**: You must have a running Rust server.
2. **Rust+ Enabled**: The server must have `app.port` configured and Rust+ enabled.
3. **Player Token**: You need a Player Token from an account that has joined the server.
    - This "Player" acts as the bot's eyes.
    - The bot will see what this player sees (Team Chat, Team Position).
    - It is recommended to use a dedicated "Camera/Bot" account if possible, or an Admin account.

## Finding Your Credentials

To connect, you need:

- **Server IP**
- **App Port** (usually Server Port + 67, e.g., 28082)
- **Steam ID** (64-bit integer)
- **Player Token** (integer)

### How to get the Token?

The official way requires complex setup, but community tools make it easy.

> [!NOTE]
> Use these tools at your own risk. They are widely used by the community but require you to log in with Steam.

1. **Replit Tool (Most Popular)**:
   - Search on Google for "Rust+ Token Grabber Replit".
   - You will find public Repls (online Python scripts) that let you log in via Steam and list your paired servers.
   - Example: [rustplus-token-grabber](https://replit.com/@liamcottle/rustplus-token-grabber) (by liamcottle, creator of a popular Rust+ library).

2. **Web Tools**:
   - There are websites that offer this service, e.g., `rustplus.bot-hosting.net`.

**Steps**:

1. Pair your mobile Rust+ app with the server.
2. Run the tool/script and log in with the Steam account you used on the phone.
3. Copy the **Token** (integer) and **IP/Port**.

## Configuration Commands

Run these commands in Discord.

### 1. Set Rust+ Credentials

```
/rust_config set_credentials ip:<ServerIP> port:<AppPort> player_id:<SteamID> token:<Token>
```

*Example*:
`/rust_config set_credentials ip:203.0.113.10 port:28082 player_id:76561198000000000 token:-123456789`

> [!TIP]
> The **Player Token** can be a very large integer. The command accepts it as a string to verify it is handled correctly. Just paste the number directly.

The bot will immediately attempt to connect. Watch the logs or wait for the "Connected" confirmation if implemented.

### 2. Set BattleMetrics ID (Global Tracking)

The Rust+ API only provides data about the **Team** the bot-player is in. To track global joins/leaves for the whole server, we use BattleMetrics.

1. Find your server on BattleMetrics (e.g., `https://www.battlemetrics.com/servers/rust/1234567`).
2. The ID is the number at the end (`1234567`).

```
/rust_config set_battlemetrics server_id:1234567
```

## Feature Commands

The bot now supports advanced RustPlus features:

### 🗺️ Maps

- `/rust_map`: Generates and sends a high-res image of the current server map, including monuments and vending machines.

### ℹ️ Server Info

- `/rust_info`: Shows server population, seed, map size, and join URL.
- `/rust_time`: Shows current in-game time.
- `/rust_team`: Lists online/offline team members.

### 🚨 Smart Devices (Alarms & Switches)

To use Smart Alarms or Switches, you must **pair** them with the bot.

1. **Get Entity ID**: Check your Rust+ App when you pair a device, or easier: flip the switch in-game and check logs (if debug enabled), or use an external tool to find the ID.
2. **Pair**:

   ```
   /rust_pair entity_id:12345678 name:"Main Garage" type:switch
   ```

3. **Control**:

   ```
   /rust_switch name:"Main Garage" state:True  (Turn ON)
   /rust_switch name:"Main Garage" state:False (Turn OFF)
   ```

### 💬 In-Game Chat Commands

Type these commands in the **Team Chat** in-game:

- `!pop`: Bot replies with player count.
- `!time`: Bot replies with game time.
- `!online`: Bot replies with list of online team members.

## Troubleshooting

- **Bot not connecting?**
  - Check if the IP/Port is reachable from the bot's host.
  - Check if the Token is expired or valid. Resend pairing notification if needed.
- **No Team Events?**
  - Ensure the Bot Account is actually IN a team.
  - The bot only receives events for its own team members.
