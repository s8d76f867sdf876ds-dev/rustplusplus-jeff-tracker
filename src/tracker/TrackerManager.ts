import { Client } from 'discord.js';
import { Database } from '../util/Database';
import { BattleMetricsClient } from './BattleMetrics';
// Removed explicit import of DiscordBot to avoid TS/CommonJS interop headache. Handled via `any` type in constructor.
// Note: In this codebase, DiscordBot is exported as class.

export class TrackerManager {
    private client: any; // Type as any to avoid strict typing issues with legacy JS structures
    private db: Database;
    private bmClient: BattleMetricsClient;
    private monitoredGuilds: Set<string> = new Set();

    constructor(client: any) {
        this.client = client;
        this.db = Database.getInstance();
        this.bmClient = new BattleMetricsClient();
    }

    public async start() {
        console.log('[JeffRustTracker] Tracker Manager started.');

        // Polling loop for BattleMetrics Sync (Every 5 minutes)
        setInterval(() => this.runBattleMetricsSync(), 5 * 60 * 1000);

        // Polling loop to attach listeners to new RustPlus instances (Every 30 seconds)
        setInterval(() => this.checkForNewInstances(), 30 * 1000);

        // Initial check
        this.checkForNewInstances();
    }

    private checkForNewInstances() {
        if (!this.client.rustplusInstances) return;

        for (const [guildId, rustplus] of Object.entries(this.client.rustplusInstances)) {
            if (!this.monitoredGuilds.has(guildId)) {
                this.attachListeners(guildId, rustplus);
                this.monitoredGuilds.add(guildId);
            }
        }
    }

    private attachListeners(guildId: string, rustplus: any) {
        console.log(`[JeffRustTracker] Attaching tracker listeners to guild ${guildId}`);

        // 1. Entity Event (Smart Devices)
        // rustplus.js emits 'entity' event with the entity object
        // But the bot wrapper might emit differently. 
        // Looking at RustPlus.js, it loads events from `rustplusEvents` and emits them.
        // It also seems to emit standard rustplus.js events?
        // Let's assume standard 'entity' event first.

        rustplus.on('entity', async (entity: any) => {
            await this.handleEntityEvent(guildId, entity);
        });

        // 2. Team Event
        rustplus.on('team', async (teamInfo: any) => {
            // teamInfo contains member status
            await this.handleTeamEvent(guildId, teamInfo);
        });

        // 3. Message Event (Chat - Optional, maybe for commands)
        rustplus.on('message', async (message: any) => {
            // Handle chat logic if needed
        });
    }

    private async handleEntityEvent(guildId: string, entity: any) {
        try {
            // entity = { entityId, value: true/false, ... }
            const rows = await this.db.query(
                "SELECT name, type FROM rust_smart_devices WHERE guild_id = ? AND entity_id = ?",
                [guildId, entity.entityId]
            ) as any[];

            if (rows.length > 0) {
                const device = rows[0]; // array of rows, array of fields? mysql2 promise query returns [rows, fields]
                // Wait, db.query returns [rows, fields] usually. 
                // My Database wrapper returns pool.query result.

                const data = Array.isArray(rows) ? rows[0] : rows;
                // Actually mysql2/promise `query` returns [RowDataPacket[], FieldPacket[]]

                if (Array.isArray(data) && data.length > 0) {
                    const { name, type } = data[0];
                    let msg = '';

                    if (type === 'alarm') {
                        msg = entity.value ? `🚨 **SMART ALARM TRIGGERED**: ${name}!` : `✅ Smart Alarm Cleared: ${name}`;
                    } else if (type === 'switch') {
                        msg = `🔌 Switch **${name}** turned **${entity.value ? 'ON' : 'OFF'}**.`;
                    } else if (type === 'storage') {
                        msg = `📦 Storage Monitor **${name}**: Capacity ${entity.capacity}`;
                    }

                    if (msg) {
                        await this.notifyTrackingChannels(guildId, msg);
                    }
                }
            }
        } catch (error) {
            console.error(`[JeffRustTracker] Error handling entity event for guild ${guildId}:`, error);
        }
    }

    private async handleTeamEvent(guildId: string, teamInfo: any) {
        // teamInfo.members = [{ steamId, name, isOnline, ... }]
        // Helper to update DB based on team info
        // This is "free" real-time tracking for teammates
        try {
            const members = teamInfo.members;
            const now = new Date();

            for (const member of members) {
                // Upsert player
                // We normalize name for consistency
                const normalizedName = member.name.toLowerCase().trim();

                await this.db.execute(`
                    INSERT INTO rust_players (guild_id, steam_id, name, is_online, last_seen, is_teammate)
                    VALUES (?, ?, ?, ?, ?, TRUE)
                    ON DUPLICATE KEY UPDATE 
                        is_online = VALUES(is_online),
                        last_seen = VALUES(last_seen),
                        is_teammate = TRUE
                `, [guildId, member.steamId, normalizedName, member.isOnline, now]);
            }
        } catch (error) {
            console.error(`[JeffRustTracker] Error handling team event for guild ${guildId}:`, error);
        }
    }

    private async notifyTrackingChannels(guildId: string, message: string) {
        try {
            // Get channels
            const [rows]: any[] = await this.db.query("SELECT channel_id FROM rust_tracking_channels WHERE guild_id = ?", [guildId]);

            for (const row of rows) {
                const channel = this.client.channels.cache.get(String(row.channel_id));
                if (channel && channel.isTextBased()) {
                    await channel.send(message).catch((e: any) => console.error(`Failed to send to channel ${row.channel_id}`, e));
                }
            }
        } catch (error) {
            console.error(`[JeffRustTracker] Notify Error:`, error);
        }
    }

    private async runBattleMetricsSync() {
        console.log('[JeffRustTracker] Starting BattleMetrics Sync...');

        try {
            const [configs]: any[] = await this.db.query("SELECT guild_id, battlemetrics_server_id FROM rust_server_configs WHERE battlemetrics_server_id IS NOT NULL");

            for (const config of configs) {
                const { guild_id, battlemetrics_server_id } = config;
                if (!battlemetrics_server_id) continue;

                const bmPlayers = await this.bmClient.getServerPlayers(battlemetrics_server_id);
                // bmPlayers = [{ attributes: { name, ... } }]

                const onlineNames = new Set(bmPlayers.map(p => p.attributes.name.toLowerCase().trim()));
                const now = new Date();

                // Get tracked players from DB
                const [dbPlayers]: any[] = await this.db.query("SELECT id, name, is_online FROM rust_players WHERE guild_id = ?", [guild_id]);

                // Compare
                for (const player of dbPlayers) {
                    const name = player.name.toLowerCase().trim();
                    const isOnlineBM = onlineNames.has(name);

                    if (player.is_online !== isOnlineBM) {
                        // Status changed
                        const status = isOnlineBM ? 'Online' : 'Offline';
                        await this.db.execute("UPDATE rust_players SET is_online = ?, last_seen = ? WHERE id = ?", [isOnlineBM, now, player.id]);

                        // Log session
                        if (isOnlineBM) {
                            // Start session
                            await this.db.execute("INSERT INTO rust_sessions (player_id, start_time) VALUES (?, ?)", [player.id, now]);
                            await this.notifyTrackingChannels(guild_id, `🟢 **${player.name}** is now **ONLINE**.`);
                        } else {
                            // End session
                            await this.db.execute("UPDATE rust_sessions SET end_time = ? WHERE player_id = ? AND end_time IS NULL", [now, player.id]);
                            await this.notifyTrackingChannels(guild_id, `🔴 **${player.name}** is now **OFFLINE**.`);
                        }
                    }
                }
            }
        } catch (error) {
            console.error('[JeffRustTracker] BM Sync Error:', error);
        }
    }
}
