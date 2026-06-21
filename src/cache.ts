import { logger } from "@saltcute/logger";
import { Cache as MemCache } from "memory-cache";
import { createClient } from "redis";
export class Cache<T extends object | Buffer> {
    private static readonly REDIS_HOT_KEY_MEM_CACHE_TTL = 1000;

    private logger;

    private memCache;
    private redisClient;

    private isRedisAvailable = true;
    constructor(private namespace: string) {
        this.logger = logger.child().withPrefix(`[${this.namespace}:cache]`);
        this.memCache = new MemCache<string, T>();
        this.redisClient = createClient({
            socket: {
                reconnectStrategy: (retries) => Math.min(retries * 100, 3000),
            },
        });
        this.redisClient.on("error", async (e) => {
            if (this.isRedisAvailable) {
                this.logger
                    .withError(e)
                    .error("Redis connection error, using memory-cache.");
                this.isRedisAvailable = false;
            }
        });
        this.redisClient.connect().catch((e) => {
            this.logger
                .withError(e)
                .error("Redis connection error, using memory-cache.");
            this.isRedisAvailable = false;
        });
        this.redisClient.on("ready", () => {
            this.isRedisAvailable = true; // recover when the socket comes back
        });
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
        try {
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
                        const parsed = Buffer.from(redisValue, "base64");
                        return parsed;
                    }
                }
            } else {
                return memCacheValue;
            }
        } catch (e) {
            this.logger
                .withError(e)
                .warn(
                    `Redis GET failed for "${this.getKey(key)}", falling back to memory-cache.`,
                );
            return this.memCache.get(this.getKey(key));
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
            try {
                if (value instanceof Buffer) {
                    await this.redisClient.set(
                        this.getKey(key),
                        value.toString("base64"),
                        {
                            expiration: {
                                type: "EX",
                                value: Math.trunc(ttl / 1000),
                            },
                        },
                    );
                } else {
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
                }
            } catch (e) {
                this.logger
                    .withError(e)
                    .warn(
                        `Redis PUT failed for "${this.getKey(key)}", value kept in memory-cache only.`,
                    );
            }
        } else this.memCache.put(this.getKey(key), value, ttl);
        const putLapsed = performance.now() - putBegin;
        this.logger.trace(
            `PUT "${this.getKey(key)}", took ${putLapsed.toFixed(1)}ms.`,
        );
    }
}
