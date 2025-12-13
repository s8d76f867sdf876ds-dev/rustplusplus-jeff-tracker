import express, { Request, Response, NextFunction } from 'express';
import bodyParser from 'body-parser';
import { Client } from 'discord.js';
import { Database } from '../util/Database';

export class IpcServer {
    private app: express.Application;
    private port: number;
    private client: any; // Discord Client
    private db: Database;

    constructor(client: any, port: number = 3000) {
        this.client = client;
        this.port = port;
        this.app = express();
        this.db = Database.getInstance();

        this.setupMiddleware();
        this.setupRoutes();
    }

    private setupMiddleware() {
        this.app.use(bodyParser.json());

        // Auth middleware (Simple token check)
        this.app.use((req: Request, res: Response, next: NextFunction) => {
            const authHeader = req.header('authorization');
            const secret = process.env.IPC_SECRET || 'changeme';

            if (!authHeader || authHeader !== secret) {
                // Warn but maybe allow localhost? 
                // For now, strict auth if header provided, else check ip?
                // Let's rely on secret.
                if (process.env.NODE_ENV === 'production') {
                    res.status(403).json({ error: 'Unauthorized' });
                    return;
                }
            }
            next();
        });
    }

    private setupRoutes() {
        // Health Check
        this.app.get('/status', (req: Request, res: Response) => {
            res.json({
                status: 'ok',
                uptime: process.uptime(),
                guilds: this.client.guilds.cache.size
            });
        });

        // Broadcast Message
        this.app.post('/broadcast', async (req: Request, res: Response) => {
            const { guild_id, channel_id, message } = req.body;

            if (!message) {
                res.status(400).json({ error: 'Message required' });
                return;
            }

            try {
                let targetChannel;

                if (channel_id) {
                    targetChannel = this.client.channels.cache.get(channel_id);
                } else if (guild_id) {
                    // Try to find a default channel? Or error?
                    // Let's assume channel_id is preferred.
                    res.status(400).json({ error: 'Channel ID required for now' });
                    return;
                }

                if (targetChannel && targetChannel.isTextBased()) {
                    await targetChannel.send(message);
                    res.json({ success: true });
                } else {
                    res.status(404).json({ error: 'Channel not found or not text-based' });
                }
            } catch (error: any) {
                console.error('[IpcServer] Broadcast error:', error);
                res.status(500).json({ error: error.message });
            }
        });

        // Trigger Wipe
        this.app.post('/wipe', async (req: Request, res: Response) => {
            const { guild_id, timestamp } = req.body;
            // logic to set wipe time in DB
            try {
                const time = timestamp ? new Date(timestamp) : new Date();
                await this.db.execute(
                    "UPDATE rust_tracking_channels SET last_wipe_at = ? WHERE guild_id = ?",
                    [time, guild_id]
                );
                // Determine snowflake
                // const snowflake = ... (need discord.js utils or BigInt math)
                // Keeping it simple: Just update DB, TrackerManager uses DB.

                res.json({ success: true, wipe_time: time });
            } catch (error: any) {
                res.status(500).json({ error: error.message });
            }
        });
    }

    public start() {
        this.app.listen(this.port, () => {
            console.log(`[JeffRustTracker] IPC Server listening on port ${this.port} `);
        });
    }
}
