-- Rust Tracker Schema Archive

-- Server Configurations
CREATE TABLE IF NOT EXISTS rust_server_configs (
    guild_id BIGINT PRIMARY KEY,
    server_ip VARCHAR(50),
    server_port INT,
    player_id BIGINT,
    player_token BIGINT,
    battlemetrics_server_id VARCHAR(20) DEFAULT NULL
);

-- Smart Devices (Alarms, Switches)
CREATE TABLE IF NOT EXISTS rust_smart_devices (
    guild_id BIGINT,
    entity_id BIGINT,
    name VARCHAR(100),
    type VARCHAR(50), -- 'alarm', 'switch', 'storage'
    PRIMARY KEY (guild_id, entity_id)
);

-- Tracking Channels Config
CREATE TABLE IF NOT EXISTS rust_tracking_channels (
    guild_id BIGINT,
    channel_id BIGINT PRIMARY KEY,
    last_scanned_message_id BIGINT DEFAULT 0,
    last_wipe_at TIMESTAMP NULL,
    battlemetrics_server_id VARCHAR(20) DEFAULT NULL
);

-- Players Table
CREATE TABLE IF NOT EXISTS rust_players (
    id BIGINT AUTO_INCREMENT PRIMARY KEY, -- Internal DB ID
    steam_id BIGINT, -- Raw Steam ID (if known) or derived
    guild_id BIGINT,
    name VARCHAR(100),
    is_online BOOLEAN DEFAULT FALSE,
    last_seen TIMESTAMP NULL,
    is_teammate BOOLEAN DEFAULT FALSE,
    UNIQUE KEY unique_player (guild_id, name)
);

-- Sessions Table (Playtime History)
CREATE TABLE IF NOT EXISTS rust_sessions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    player_id BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP NULL,
    FOREIGN KEY (player_id) REFERENCES rust_players(id) ON DELETE CASCADE
);

-- Market Listings (Vending Machines)
CREATE TABLE IF NOT EXISTS rust_market_listings (
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
);

-- Economy Transactions (Sales tracking)
CREATE TABLE IF NOT EXISTS rust_economy_transactions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    guild_id BIGINT,
    item_name VARCHAR(100),
    quantity INT,
    cost_amount INT,
    cost_item VARCHAR(100),
    buyer_name VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
