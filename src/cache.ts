import { logger } from "@saltcute/logger";
import { Cache as MemCache } from "memory-cache";
import { createClient } from "redis";
export class Cache<T extends object> {
    private logger;

    private memCache;
    private redisClient;

    private isRedisAvailable = true;
    constructor(private namespace: string) {
        this.logger = logger.child().withGroup(`${this.namespace}:cache`);
        this.memCache = new MemCache<string, T>();
        this.redisClient = createClient();
        this.redisClient.on("error", async () => {
            this.logger.error("Redis connection error, using memory-cache");
            this.isRedisAvailable = false;
            this.redisClient.destroy();
        });
        this.redisClient.connect();
    }
    private getKey(key: string) {
        return `${this.namespace}:${key}`;
    }
    /**
     * Get a value from cache.
     *
     * @param key Cache key.
     * @returns Cache value.
     */
    public async get(key: string) {
        const memCacheValue = this.memCache.get(this.getKey(key));
        if (!memCacheValue && this.isRedisAvailable) {
            const redisValue = await this.redisClient.get(this.getKey(key));
            if (redisValue) {
                try {
                    const parsed = JSON.parse(redisValue);
                    this.memCache.put(key, parsed, 1000);
                    return parsed;
                } catch {
                    await this.redisClient.del(key);
                    return null;
                }
            }
        } else return memCacheValue;
    }
    /**
     * Put a value into cache.
     *
     * @param key Cache key.
     * @param value Cache value.
     * @param ttl Cache TTL in milliseconds.
     */
    public async put(key: string, value: T, ttl: number) {
        if (this.isRedisAvailable) {
            this.memCache.put(this.getKey(key), value, 1000);
            await this.redisClient.set(
                this.getKey(key),
                JSON.stringify(value),
                {
                    expiration: {
                        type: "EX",
                        value: Math.trunc(ttl / 1000),
                    },
                },
            );
        } else this.memCache.put(this.getKey(key), value, ttl);
    }
}
