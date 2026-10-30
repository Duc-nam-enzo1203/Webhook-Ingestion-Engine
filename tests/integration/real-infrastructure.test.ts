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

  it('verifies real PostgreSQL OCC State Upsert without EXCLUDED reference error and validates out-of-order hydration', async (ctx) => {
    if (!isAvailable || !pool) {
      ctx.skip();
      return;
    }

    const { executeOccStateUpsert } = await import('../../src/worker/state-machine/occ-state.js');
    const tenantId = 'a0000000-0000-0000-0000-000000000001';
    const platform = 'shopify';
    const entityId = `ord_occ_test_${Date.now()}`;

    // 1. First event: CANCELLED arrives first (Weight 90, unhydrated partial payload)
    const cancelledEvent = {
      tenantId,
      platform,
      eventId: `evt_cancel_${Date.now()}`,
      entityType: 'ORDER',
      entityId,
      eventType: 'orders/cancelled',
      currentState: 'CANCELLED',
      stateWeight: 90,
      externalVersion: 1761720300000,
      isHydrated: false,
      payload: {
        order_id: entityId,
        status: 'cancelled',
        cancel_reason: 'fraud_flag',
      },
      rawBody: '{}',
      receivedAt: new Date().toISOString(),
    };

    const res1 = await executeOccStateUpsert(pool, cancelledEvent);
    expect(res1.currentState).toBe('CANCELLED');
    expect(res1.isHydrated).toBe(false);
    expect(res1.isInserted).toBe(true);
    expect(res1.isStaleIgnored).toBe(false);
    // Crucial: Initial insert must NOT be marked as late hydration!
    expect(res1.isNewlyHydrated).toBe(false);
    expect(res1.mergedPayload.cancel_reason).toBe('fraud_flag');

    // 2. Second event: CREATED arrives later (Weight 10, hydrated payload with line_items)
    const createdEvent = {
      tenantId,
      platform,
      eventId: `evt_create_${Date.now()}`,
      entityType: 'ORDER',
      entityId,
      eventType: 'orders/create',
      currentState: 'CREATED',
      stateWeight: 10,
      externalVersion: 1761720000000,
      isHydrated: true,
      payload: {
        order_id: entityId,
        status: 'open',
        line_items: [{ sku: 'PROD-1', quantity: 2 }],
        customer: 'John Doe',
      },
      rawBody: '{}',
      receivedAt: new Date().toISOString(),
    };

    const res2 = await executeOccStateUpsert(pool, createdEvent);
    // State remains CANCELLED (does not regress to CREATED)
    expect(res2.currentState).toBe('CANCELLED');
    expect(res2.isHydrated).toBe(true);
    expect(res2.isInserted).toBe(false);
    expect(res2.isStaleIgnored).toBe(true);
    // Crucial: Late hydration was detected!
    expect(res2.isNewlyHydrated).toBe(true);
    // Both payloads bidirectionally merged:
    expect(res2.mergedPayload.cancel_reason).toBe('fraud_flag');
    expect(res2.mergedPayload.customer).toBe('John Doe');
    expect(res2.mergedPayload.line_items).toHaveLength(1);
    expect(res2.mergedPayload.status).toBe('cancelled');

    // 3. Duplicate CREATED arrives:
    const res3 = await executeOccStateUpsert(pool, createdEvent);
    expect(res3.isNewlyHydrated).toBe(false);
  });

  it('verifies processor preserves message in PEL and returns FAILED_RETRY when DB is offline during DLQ routing', async (ctx) => {
    if (!isAvailable || !redis) {
      ctx.skip();
      return;
    }

    // Dead pool pointing to an offline port to simulate PostgreSQL outage
    const deadPool = new pg.Pool({
      connectionString: 'postgresql://postgres:postgres@127.0.0.1:54399/webhook_engine',
      connectionTimeoutMillis: 500,
    });

    const processor = new WebhookProcessor('test_failover_worker', deadPool);
    const tenantId = 'a0000000-0000-0000-0000-000000000001';
    const platform = 'shopify';
    const eventId = `evt_db_dead_${Date.now()}`;

    // Prime tenant in L1 cache so tenant resolution passes from cache
    const { tenantCache } = await import('../../src/shared/cache/tenant-cache.js');
    tenantCache.setTenantInL1({
      id: tenantId,
      name: 'Test Tenant',
      platform: 'shopify',
      webhookSecret: 'test_secret',
      rateLimitRps: 1000,
      maxConcurrency: 10,
      downstreamUrl: 'http://localhost:4000/downstream',
      isActive: true,
    });

    // Call processEvent with max defer_count (5) to trigger direct DLQ routing
    const rawPayload = {
      event_id: eventId,
      tenant_id: tenantId,
      platform,
      defer_count: 5,
      data: JSON.stringify({
        tenantId,
        platform,
        eventId,
        entityType: 'ORDER',
        entityId: 'ORD_DEAD_DB',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: { order_id: 'ORD_DEAD_DB' },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    };

    try {
      const result = await processor.processEvent(
        redis,
        'webhooks:stream:00',
        '1000000000000-0',
        rawPayload,
        deadPool
      );

      // CRITICAL: Must return FAILED_RETRY (not DLQ) so stream.consumer does NOT XACK the message!
      expect(result.status).toBe('FAILED_RETRY');
      expect(result.error).toBe('DB_DOWN_CANNOT_DLQ');
    } finally {
      await deadPool.end().catch(() => {});
    }
  });

  it('verifies getTenant throws on DB failure and processor does not ACK when worker cold starts during DB outage', async (ctx) => {
    if (!isAvailable || !redis) {
      ctx.skip();
      return;
    }

    const deadPool = new pg.Pool({
      connectionString: 'postgresql://postgres:postgres@127.0.0.1:54399/webhook_engine',
      connectionTimeoutMillis: 500,
    });

    const processor = new WebhookProcessor('test_cold_start_worker', deadPool);
    const tenantId = `tenant_cold_${Date.now()}`;
    const platform = 'shopify';
    const eventId = `evt_cold_${Date.now()}`;

    // Ensure tenant is NOT in L1/L2 cache
    const { tenantCache } = await import('../../src/shared/cache/tenant-cache.js');
    await tenantCache.invalidateTenant(tenantId);

    const rawPayload = {
      event_id: eventId,
      tenant_id: tenantId,
      platform,
      defer_count: 5,
      data: JSON.stringify({
        tenantId,
        platform,
        eventId,
        entityType: 'ORDER',
        entityId: 'ORD_COLD_DB',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: { order_id: 'ORD_COLD_DB' },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    };

    try {
      const result = await processor.processEvent(
        redis,
        'webhooks:stream:00',
        '1000000000000-0',
        rawPayload,
        deadPool
      );

      // Must NOT return SUCCESS (must not ACK)! Must fail retry so message stays in PEL
      expect(result.status).toBe('FAILED_RETRY');
      expect(result.error).toBe('DB_DOWN_CANNOT_DLQ');
    } finally {
      await deadPool.end().catch(() => {});
    }
  });
});
