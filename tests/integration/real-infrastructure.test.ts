import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Redis } from 'ioredis';
import pg from 'pg';
import { env } from '../../src/config/env.js';
import { getIdempotencyRedisKey } from '../../src/worker/locks/idempotency.js';
import { buildGatewayServer } from '../../src/gateway/server.js';
import { WebhookProcessor } from '../../src/worker/processor.js';
import { FastifyInstance } from 'fastify';

/**
 * Real Infrastructure Integration Test Suite:
 * Executes against live PostgreSQL and live Redis instances when available
 * (such as in GitHub Actions CI service containers or local Docker stack).
 */
describe('Integration: Real Infrastructure Tests (PostgreSQL & Redis)', () => {
  let isAvailable = false;
  let redis: Redis | null = null;
  let pool: pg.Pool | null = null;
  let app: FastifyInstance | null = null;

  beforeAll(async () => {
    try {
      redis = new Redis({
        host: env.REDIS_HOST,
        port: env.REDIS_PORT,
        connectTimeout: 800,
        maxRetriesPerRequest: 1,
        lazyConnect: true,
      });
      redis.on('error', () => {});
      await redis.connect();
      await redis.ping();

      pool = new pg.Pool({
        connectionString: env.DATABASE_URL,
        connectionTimeoutMillis: 800,
      });
      await pool.query('SELECT 1');

      isAvailable = true;
      app = await buildGatewayServer();
    } catch {
      isAvailable = false;
    }
  });

  afterAll(async () => {
    if (redis) {
      await redis.quit().catch(() => {});
    }
    if (pool) {
      await pool.end().catch(() => {});
    }
    if (app) {
      await app.close().catch(() => {});
    }
  });

  it('verifies real Redis hash-tag lock deletion on DLQ Replay', async (ctx) => {
    if (!isAvailable || !redis || !pool || !app) {
      ctx.skip();
      return;
    }

    const tenantId = 'a0000000-0000-0000-0000-000000000001';
    const platform = 'shopify';
    const eventId = `evt_real_replay_${Date.now()}`;
    const redisKey = getIdempotencyRedisKey(tenantId, platform, eventId);

    // 1. Manually set the lock in REAL Redis with 'DONE' (TTL 86400)
    await redis.set(redisKey, 'DONE', 'EX', 86400);
    expect(await redis.get(redisKey)).toBe('DONE');

    // 2. Insert failed event into REAL PostgreSQL
    await pool.query(
      `INSERT INTO idempotency_keys (tenant_id, platform, event_id, status)
       VALUES ($1, $2, $3, 'DLQ')
       ON CONFLICT (tenant_id, platform, event_id) DO UPDATE SET status = 'DLQ'`,
      [tenantId, platform, eventId]
    );

    await pool.query(
      `INSERT INTO webhook_events (tenant_id, platform, event_id, entity_type, entity_id, delivery_status, retry_count, raw_payload, last_error)
       VALUES ($1, $2, $3, 'ORDER', 'ORD_REAL_1', 'DLQ', 5, '{"order_id": "ORD_REAL_1"}', 'Connection refused')`,
      [tenantId, platform, eventId]
    );

    // 3. Trigger replay via Gateway Admin API
    const res = await app.inject({
      method: 'POST',
      url: `/admin/dlq/${eventId}/replay`,
      headers: {
        'x-admin-key': env.ADMIN_API_KEY,
      },
    });

    expect(res.statusCode).toBe(200);

    // 4. CRITICAL ASSERTION: The REAL key in REAL Redis must be completely DELETED!
    const keyAfterReplay = await redis.get(redisKey);
    expect(keyAfterReplay).toBeNull();

    // 5. Assert: Postgres status transitioned to PERSISTED
    const dbCheck = await pool.query(
      `SELECT status FROM idempotency_keys WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
      [tenantId, platform, eventId]
    );
    expect(dbCheck.rows[0].status).toBe('PERSISTED');
  });

  it('verifies concurrent 20 workers race condition on REAL PostgreSQL unique constraint', async (ctx) => {
    if (!isAvailable || !pool) {
      ctx.skip();
      return;
    }

    const tenantId = 'a0000000-0000-0000-0000-000000000001';
    const platform = 'shopify';
    const eventId = `evt_race_real_${Date.now()}`;

    // Fire 20 concurrent transactions directly to REAL PostgreSQL
    const attempts = await Promise.all(
      Array.from({ length: 20 }, async (_, idx) => {
        const client = await pool!.connect();
        try {
          const res = await client.query(
            `INSERT INTO idempotency_keys (tenant_id, platform, event_id, status, locked_by)
             VALUES ($1, $2, $3, 'IN_PROGRESS', $4)
             ON CONFLICT (tenant_id, platform, event_id) DO NOTHING`,
            [tenantId, platform, eventId, `worker_${idx}`]
          );
          return res.rowCount; // 1 if inserted, 0 if conflict
        } finally {
          client.release();
        }
      })
    );

    const insertedCount = attempts.filter((cnt) => cnt === 1).length;
    const conflictedCount = attempts.filter((cnt) => cnt === 0).length;

    // Guaranteed by PostgreSQL PRIMARY KEY constraint on partitioned table
    expect(insertedCount).toBe(1);
    expect(conflictedCount).toBe(19);
  });
});
