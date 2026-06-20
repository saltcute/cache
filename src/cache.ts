import { logger } from "@saltcute/logger";
import { Cache as MemCache } from "memory-cache";
import { createClient } from "redis";
export class Cache<T extends object> {
    private static readonly REDIS_HOT_KEY_MEM_CACHE_TTL = 1000;

    private logger;

    private memCache;
    private redisClient;

    private isRedisAvailable = true;
    constructor(private namespace: string) {
        this.logger = logger.child().withPrefix(`[${this.namespace}:cache]`);
        this.memCache = new MemCache<string, T>();
        this.redisClient = createClient();
        this.redisClient.on("error", async () => {
            this.logger.error("Redis connection error, using memory-cache.");
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
            const redisReadBegin = performance.now();
            const redisValue = await this.redisClient.get(this.getKey(key));
            const redisReadLapsed = performance.now() - redisReadBegin;

            if (redisValue) {
                try {
                    const parsed = JSON.parse(redisValue);
                    this.memCache.put(
                        this.getKey(key),
                        parsed,
                        Cache.REDIS_HOT_KEY_MEM_CACHE_TTL,
                    );
                    this.logger.trace(
                        `GET "${this.getKey(key)}" Redis HIT, took ${redisReadLapsed.toFixed(1)}ms.`,
                    );
                    return parsed;
                } catch {
                    this.logger.trace(
                        `GET "${this.getKey(key)}" Redis INVALID, took ${redisReadLapsed.toFixed(1)}ms.`,
                    );
                    await this.redisClient.del(this.getKey(key));
                    return null;
                }
            }
        } else {
            return memCacheValue;
        }
    }
    /**
     * Put a value into cache.
     *
     * @param key Cache key.
     * @param value Cache value.
     * @param ttl Cache TTL in milliseconds.
     */
    public async put(key: string, value: T, ttl: number) {
        const putBegin = performance.now();
        if (this.isRedisAvailable) {
            this.memCache.put(
                this.getKey(key),
                value,
                Cache.REDIS_HOT_KEY_MEM_CACHE_TTL,
            );
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
        const putLapsed = performance.now() - putBegin;
        this.logger.trace(
            `PUT "${this.getKey(key)}", took ${putLapsed.toFixed(1)}ms.`,
        );
    }
}
