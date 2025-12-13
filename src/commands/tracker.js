const { SlashCommandBuilder } = require('@discordjs/builders');
const { Database } = require('../util/Database');

// Helper for intl (mocking or implementing basic get)
// The bot architecture passes `client` to execute.
const db = Database.getInstance();

module.exports = {
    name: 'tracker',
    getData(client, guildId) {
        return new SlashCommandBuilder()
            .setName('rust_tracker')
            .setDescription('Jeff Rust Tracker Commands')
            .addSubcommand(sub => sub
                .setName('setup')
                .setDescription('Configure Rust+ Credentials (Admin Only)')
                .addStringOption(opt => opt.setName('ip').setDescription('Server IP').setRequired(true))
                .addIntegerOption(opt => opt.setName('port').setDescription('App Port').setRequired(true))
                .addStringOption(opt => opt.setName('player_id').setDescription('SteamID (64)').setRequired(true))
                .addIntegerOption(opt => opt.setName('token').setDescription('Player Token').setRequired(true))
            )
            .addSubcommand(sub => sub
                .setName('wipe')
                .setDescription('Set wipe time to NOW (Admin Only)')
            )
            .addSubcommand(sub => sub
                .setName('status')
                .setDescription('Check Tracker Status')
            );
    },
    async execute(client, interaction) {
        if (!await client.validatePermissions(interaction)) return;

        const sub = interaction.options.getSubcommand();
        await interaction.deferReply({ ephemeral: true });

        // Admin check
        // client.validatePermissions checks mostly against config list.
        // We probably also want to enforce Admin permission for setup/wipe.
        if (sub === 'setup' || sub === 'wipe') {
            if (!interaction.member.permissions.has('Administrator')) {
                return interaction.editReply('❌ **Administrator** permission required.');
            }
        }

        if (sub === 'setup') {
            const ip = interaction.options.getString('ip');
            const port = interaction.options.getInteger('port');
            const playerId = interaction.options.getString('player_id'); // String because BigInt issues in JS?
            const token = interaction.options.getInteger('token');

            try {
                // Upsert config
                await db.execute(`
                    INSERT INTO rust_server_configs (guild_id, server_ip, server_port, player_id, player_token)
                    VALUES (?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        server_ip = VALUES(server_ip),
                        server_port = VALUES(server_port),
                        player_id = VALUES(player_id),
                        player_token = VALUES(player_token)
                `, [interaction.guildId, ip, port, playerId, token]);

                await interaction.editReply(`✅ Configuration saved for **${ip}:${port}**.`);

                // Trigger reload?
                // We'd need to tell DiscordBot to reload instances.
                // client.trackerManager.checkForNewInstances() runs every 30s.
                // But that checks existing rustplusInstances.
                // We need to tell the bot to CREATE a new instance from DB.
                // We'll update DiscordBot.js to support this.

                if (client.reloadRustPlusFromDB) {
                    await client.reloadRustPlusFromDB(interaction.guildId);
                    await interaction.followup({ content: '🔄 Attempting to connect...', ephemeral: true });
                } else {
                    await interaction.followup({ content: '⚠️ Please restart the bot or wait for auto-sync to apply changes.', ephemeral: true });
                }

            } catch (e) {
                console.error(e);
                await interaction.editReply(`❌ Error: ${e.message}`);
            }
        }
        else if (sub === 'wipe') {
            const now = new Date();
            await db.execute("UPDATE rust_tracking_channels SET last_wipe_at = ? WHERE guild_id = ?", [now, interaction.guildId]);
            await interaction.editReply(`✅ Wipe time set to **${now.toISOString()}**.`);

            // Notify via IPC? Not needed, we are in the bot.
            // Maybe broadcast to channel?
            const channelIdRow = (await db.query("SELECT channel_id FROM rust_tracking_channels WHERE guild_id = ?", [interaction.guildId]))[0]?.[0];
            if (channelIdRow) {
                const chan = client.channels.cache.get(channelIdRow.channel_id);
                if (chan) chan.send(`🧹 **Wipe Detected/Set!** Stats reset.`);
            }
        }
        else if (sub === 'status') {
            const rustplus = client.rustplusInstances[interaction.guildId];
            let status = 'Not Configured';
            if (rustplus) {
                if (rustplus.isOperational) status = '🟢 Connected';
                else status = '🟡 Connecting/Issue';
            }

            await interaction.editReply({
                embeds: [{
                    title: 'Jeff Rust Tracker Status',
                    fields: [
                        { name: 'Rust+ Connection', value: status },
                        { name: 'Tracker Manager', value: 'Active' }, // Since we are responding
                    ]
                }]
            });
        }
    }
};
