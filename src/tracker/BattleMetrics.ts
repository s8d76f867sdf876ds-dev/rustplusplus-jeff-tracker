import axios from 'axios';

export interface BattleMetricsPlayer {
    id: string;
    attributes: {
        name: string;
        id: string;
    };
}

export interface BattleMetricsServer {
    id: string;
    attributes: {
        name: string;
        ip: string;
        port: number;
        players: number;
        maxPlayers: number;
    };
}

export class BattleMetricsClient {
    private baseUrl = 'https://api.battlemetrics.com';

    public async getServerInfo(serverId: string): Promise<BattleMetricsServer | null> {
        try {
            const response = await axios.get(`${this.baseUrl}/servers/${serverId}`);
            return response.data.data;
        } catch (error) {
            console.error(`[BattleMetrics] Failed to fetch server info for ${serverId}:`, error);
            return null;
        }
    }

    public async getServerPlayers(serverId: string): Promise<BattleMetricsPlayer[]> {
        try {
            const response = await axios.get(`${this.baseUrl}/servers/${serverId}?include=player`);
            // The included players are in the 'included' array? Or inside data relationships?
            // Actually, /servers/{id} returns server info. To get players, we might need /players?filter[servers]={id}&page[size]=100
            // But the Python bot used `get_server_players`. Let's assume standard endpoint.
            // The efficient BM API usage for player list is usually via RCON or Server Query endpoint if available, but BM tracks it.
            // Correct endpoint for current players on a server:
            // https://api.battlemetrics.com/players?filter[servers]=<id>&filter[online]=true&page[size]=100

            const url = `${this.baseUrl}/players?filter[servers]=${serverId}&filter[online]=true&page[size]=100`;
            const res = await axios.get(url);

            // Handle pagination if > 100 players? 
            // For MVP, 100 is okay, but full servers imply we need looping.
            // Let's implement simple pagination.

            let players = res.data.data as BattleMetricsPlayer[];
            let next = res.data.links?.next;

            while (next) {
                try {
                    const nextRes = await axios.get(next);
                    players = players.concat(nextRes.data.data);
                    next = nextRes.data.links?.next;
                } catch {
                    break;
                }
            }

            return players;
        } catch (error) {
            console.error(`[BattleMetrics] Failed to fetch players for ${serverId}:`, error);
            return [];
        }
    }
}
