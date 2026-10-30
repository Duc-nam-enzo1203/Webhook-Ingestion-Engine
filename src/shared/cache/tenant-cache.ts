import pg from 'pg';
import { LRUCache } from 'lru-cache';
import { getRedisClient, getRedisSubscriber } from '../db/redis.js';
import { getDbPool } from '../db/postgres.js';
import { logger } from '../observability/logger.js';

export interface TenantConfig {
  id: string;
  name: string;
  platform: 'shopify' | 'tiktok' | 'shopee';
  webhookSecret: string;
  rateLimitRps: number;
  maxConcurrency: number;
  downstreamUrl: string | null;
  isActive: boolean;
}

export class MultiTierTenantCache {
  private l1Cache: LRUCache<string, TenantConfig>;
  private isSubscribed = false;
  private readonly pubSubChannel = 'channel:tenant:invalidate';

  constructor(maxTenants = 5000, ttlMs = 30000) {
    this.l1Cache = new LRUCache<string, TenantConfig>({
      max: maxTenants,
      ttl: ttlMs,
    });
  }

  /**
   * Subscribes to Redis Pub/Sub for cross-instance L1 cache invalidation.
   */
  async initPubSub(): Promise<void> {
    if (this.isSubscribed) return;

    try {
      const subscriber = getRedisSubscriber();
      await subscriber.subscribe(this.pubSubChannel);

      subscriber.on('message', (channel, tenantId) => {
        if (channel === this.pubSubChannel && tenantId) {
          this.l1Cache.delete(tenantId);
          logger.debug({ tenantId }, 'L1 Tenant Cache invalidated via Pub/Sub');
        }
      });

      this.isSubscribed = true;
    } catch (err) {
      logger.warn({ err }, 'Redis Pub/Sub not available for tenant invalidation, relying on L1 TTL');
    }
  }

  /**
   * Retrieves tenant configuration through Multi-Tier caching:
   * L1 (Process RAM) -> L2 (Redis Hash) -> DB (PostgreSQL)
   */
  async getTenant(tenantId: string, poolOverride?: pg.Pool): Promise<TenantConfig | null> {
    // 1. Check L1 Cache
    const cachedL1 = this.l1Cache.get(tenantId);
    if (cachedL1) {
      return cachedL1;
    }

    // 2. Check L2 Redis Cache
    const redis = getRedisClient();
    const redisKey = `tenant:config:${tenantId}`;

    try {
      const cachedL2Raw = await redis.get(redisKey);
      if (cachedL2Raw) {
        const tenant: TenantConfig = JSON.parse(cachedL2Raw);
        this.l1Cache.set(tenantId, tenant);
        return tenant;
      }
    } catch (err) {
      logger.warn({ err, tenantId }, 'Failed to query Redis L2 cache, falling back to DB');
    }

    // 3. Fallback to PostgreSQL
    try {
      const pool = poolOverride || getDbPool();
      const res = await pool.query(
        `SELECT id, name, platform, webhook_secret, rate_limit_rps, max_concurrency, downstream_url, is_active 
         FROM tenants 
         WHERE id = $1 AND is_active = true`,
        [tenantId]
      );

      if (res.rows.length === 0) {
        return null;
      }

      const row = res.rows[0];
      const tenant: TenantConfig = {
        id: row.id,
        name: row.name,
        platform: row.platform,
        webhookSecret: row.webhook_secret,
        rateLimitRps: Number(row.rate_limit_rps) || 100,
        maxConcurrency: Number(row.max_concurrency) || 10,
        downstreamUrl: row.downstream_url,
        isActive: Boolean(row.is_active),
      };

      // Populate L1 & L2
      this.l1Cache.set(tenantId, tenant);
      await redis.set(redisKey, JSON.stringify(tenant), 'EX', 600).catch((err) => {
        logger.warn({ err }, 'Failed to populate Redis L2 cache');
      });

      return tenant;
    } catch (err) {
      logger.error({ err, tenantId }, 'Failed to load tenant from PostgreSQL due to database error');
      throw err;
    }
  }

  /**
   * Manually sets or primes tenant in L1 (useful for testing or seeding)
   */
  setTenantInL1(tenant: TenantConfig): void {
    this.l1Cache.set(tenant.id, tenant);
  }

  /**
   * Invalidates tenant across L1, L2, and broadcasts to other gateway nodes.
   */
  async invalidateTenant(tenantId: string): Promise<void> {
    this.l1Cache.delete(tenantId);

    try {
      const redis = getRedisClient();
      await redis.del(`tenant:config:${tenantId}`);
      await redis.publish(this.pubSubChannel, tenantId);
    } catch (err) {
      logger.warn({ err, tenantId }, 'Failed to broadcast tenant invalidation via Redis');
    }
  }
}

export const tenantCache = new MultiTierTenantCache();

