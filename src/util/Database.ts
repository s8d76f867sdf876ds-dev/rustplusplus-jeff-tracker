import { createPool, Pool, PoolOptions } from 'mysql2/promise';
import * as dotenv from 'dotenv';
import path from 'path';

// Load .env from root to ensure process.env is populated for Config
dotenv.config({ path: path.join(__dirname, '../../.env') });

// Import Config
const Config = require('../../config');

export class Database {
    private static instance: Database;
    private pool: Pool;
    private isConnected: boolean = false;

    private constructor() {
        // Handle cases where host might include port (e.g. 1.2.3.4:3306)
        let host = Config.database.host;
        let port = Config.database.port;

        if (host.includes(':')) {
            const parts = host.split(':');
            host = parts[0];
            port = parseInt(parts[1]);
        }

        const dbConfig: PoolOptions = {
            host: host,
            port: port,
            user: Config.database.user,
            password: Config.database.password,
            database: Config.database.name,
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0,
            enableKeepAlive: true,
            keepAliveInitialDelay: 0
        };

        this.pool = createPool(dbConfig);
        console.log('[JeffRustTracker] Database pool created.');
    }

    public static getInstance(): Database {
        if (!Database.instance) {
            Database.instance = new Database();
        }
        return Database.instance;
    }

    public async init(): Promise<void> {
        try {
            const connection = await this.pool.getConnection();
            await connection.ping();
            console.log('[JeffRustTracker] Database connected successfully.');
            this.isConnected = true;
            connection.release();

            await this.ensureTables();
        } catch (error) {
            console.error('[JeffRustTracker] Database connection failed:', error);
            this.isConnected = false;
        }
    }

    private async ensureTables(): Promise<void> {
        // Idempotent table creation matching schema.sql
        // We do this to ensure the node bot can run independently if needed.

        const queries = [
            `CREATE TABLE IF NOT EXISTS rust_server_configs (
                guild_id BIGINT PRIMARY KEY,
                server_ip VARCHAR(50),
                server_port INT,
                player_id BIGINT,
                player_token BIGINT,
                battlemetrics_server_id VARCHAR(20) DEFAULT NULL
            )`,
            `CREATE TABLE IF NOT EXISTS rust_smart_devices (
                guild_id BIGINT,
                entity_id BIGINT,
                name VARCHAR(100),
                type VARCHAR(50),
                PRIMARY KEY (guild_id, entity_id)
            )`,
            `CREATE TABLE IF NOT EXISTS rust_tracking_channels (
                guild_id BIGINT,
                channel_id BIGINT PRIMARY KEY,
                last_scanned_message_id BIGINT DEFAULT 0,
                last_wipe_at TIMESTAMP NULL,
                battlemetrics_server_id VARCHAR(20) DEFAULT NULL
            )`,
            `CREATE TABLE IF NOT EXISTS rust_players (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                steam_id BIGINT,
                guild_id BIGINT,
                name VARCHAR(100),
                is_online BOOLEAN DEFAULT FALSE,
                last_seen TIMESTAMP NULL,
                is_teammate BOOLEAN DEFAULT FALSE,
                UNIQUE KEY unique_player (guild_id, name)
            )`,
            `CREATE TABLE IF NOT EXISTS rust_sessions (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                player_id BIGINT,
                start_time TIMESTAMP,
                end_time TIMESTAMP NULL,
                FOREIGN KEY (player_id) REFERENCES rust_players(id) ON DELETE CASCADE
            )`,
            `CREATE TABLE IF NOT EXISTS rust_market_listings (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                guild_id BIGINT,
                shop_name VARCHAR(100),
                item_name VARCHAR(100),
                quantity INT,
                cost_amount INT,
                cost_item VARCHAR(100),
                stock INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_search (guild_id, item_name)
            )`
        ];

        try {
            for (const query of queries) {
                await this.pool.execute(query);
            }
            console.log('[JeffRustTracker] Database tables verified.');
        } catch (error) {
            console.error('[JeffRustTracker] Failed to ensure tables:', error);
        }
    }

    public getPool(): Pool {
        return this.pool;
    }

    // Helper for executing queries
    public async execute(sql: string, params?: any[]): Promise<any> {
        return this.pool.execute(sql, params);
    }

    public async query(sql: string, params?: any[]): Promise<any> {
        return this.pool.query(sql, params);
    }
}
