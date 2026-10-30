import { describe, it, expect, vi, beforeEach } from 'vitest';
import { buildGatewayServer } from '../../src/gateway/server.js';
import { FastifyInstance } from 'fastify';
import { env } from '../../src/config/env.js';

const mockDb = {
  webhookEvents: new Map<string, any>(),
  idempotencyKeys: new Map<string, any>(),
  entityStates: new Map<string, any>(),
};

const mockRedis = {
  del: vi.fn().mockResolvedValue(1),
  xadd: vi.fn().mockResolvedValue('1761720000000-0'),
  xlen: vi.fn().mockResolvedValue(0),
  xpending: vi.fn().mockResolvedValue([0, null, null, []]),
};

vi.mock('../../src/shared/db/redis.js', () => ({
  getRedisClient: () => mockRedis,
  getRedisSubscriber: () => mockRedis,
  closeRedisPool: vi.fn(),
}));

const mockDbPool = {
  query: vi.fn().mockImplementation(async (sql: string, params: any[] = []) => {
    // SELECT DLQ events
    if (sql.includes('FROM webhook_events') && sql.includes("delivery_status = 'DLQ'")) {
      if (sql.includes('WHERE event_id = $1')) {
        const eventId = params[0];
        for (const [_, v] of mockDb.webhookEvents.entries()) {
          if (v.event_id === eventId && v.delivery_status === 'DLQ') {
            return { rows: [v] };
          }
        }
        return { rows: [] };
      }
      return { rows: Array.from(mockDb.webhookEvents.values()).filter((v) => v.delivery_status === 'DLQ') };
    }

    // SELECT entity_states
    if (sql.includes('FROM entity_states')) {
      const key = `${params[0]}:${params[1]}:${params[2]}:${params[3]}`;
      const state = mockDb.entityStates.get(key);
      return { rows: state ? [state] : [] };
    }

    // UPDATE webhook_events
    if (sql.includes('UPDATE webhook_events')) {
      const key = `${params[0]}:${params[1]}:${params[2]}`;
      const existing = mockDb.webhookEvents.get(key) || {};
      mockDb.webhookEvents.set(key, { ...existing, delivery_status: 'QUEUED', retry_count: 0, last_error: null });
      return { rowCount: 1 };
    }

    // UPDATE idempotency_keys
    if (sql.includes('UPDATE idempotency_keys')) {
      const key = `${params[0]}:${params[1]}:${params[2]}`;
      mockDb.idempotencyKeys.set(key, { status: 'PERSISTED' });
      return { rowCount: 1 };
    }

    return { rows: [], rowCount: 1 };
  }),
};

vi.mock('../../src/shared/db/postgres.js', () => ({
  getDbPool: () => mockDbPool,
  closeDbPool: vi.fn(),
}));

describe('Integration: DLQ Admin & Safe Replay', () => {
  let app: FastifyInstance;
  const adminKey = env.ADMIN_API_KEY;
  const tenantId = 'a0000000-0000-0000-0000-000000000001';

  beforeEach(async () => {
    mockDb.webhookEvents.clear();
    mockDb.idempotencyKeys.clear();
    mockDb.entityStates.clear();
    vi.clearAllMocks();

    app = await buildGatewayServer();
  });

  it('rejects unauthenticated requests to /admin/dlq with 401 Unauthorized', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/admin/dlq',
    });

    expect(res.statusCode).toBe(401);
    expect(res.json().error).toContain('Unauthorized');
  });

  it('rejects unauthenticated requests to /admin/dlq/:eventId/replay with 401 Unauthorized', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/admin/dlq/evt_fail_123/replay',
    });

    expect(res.statusCode).toBe(401);
  });

  it('successfully inspects DLQ with valid admin token', async () => {
    mockDb.webhookEvents.set(`${tenantId}:shopify:evt_dlq_001`, {
      tenant_id: tenantId,
      platform: 'shopify',
      event_id: 'evt_dlq_001',
      entity_type: 'ORDER',
      entity_id: 'ORD_991',
      delivery_status: 'DLQ',
      retry_count: 5,
      last_error: 'Downstream HTTP 500 error after 5 retries',
      created_at: new Date().toISOString(),
    });

    const res = await app.inject({
      method: 'GET',
      url: '/admin/dlq',
      headers: {
        'x-admin-key': adminKey,
      },
    });

    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.total).toBe(1);
    expect(body.dlq_events[0].event_id).toBe('evt_dlq_001');
  });

  it('replays DLQ event: deletes Redis DONE lock, restores DB state, and re-enqueues canonical entity state', async () => {
    const eventId = 'evt_dlq_002';
    const key = `${tenantId}:shopify:${eventId}`;

    // Seed failed DLQ event in DB
    mockDb.webhookEvents.set(key, {
      tenant_id: tenantId,
      platform: 'shopify',
      event_id: eventId,
      entity_type: 'ORDER',
      entity_id: 'ORD_992',
      delivery_status: 'DLQ',
      raw_payload: JSON.stringify({ id: 'ORD_992', total: '499.00' }),
    });

    // Seed canonical entity state in PostgreSQL
    mockDb.entityStates.set(`${tenantId}:shopify:ORDER:ORD_992`, {
      current_state: 'PAID',
      state_weight: 20,
      is_hydrated: true,
      merged_payload: JSON.stringify({ id: 'ORD_992', total: '499.00', status: 'PAID' }),
    });

    const res = await app.inject({
      method: 'POST',
      url: `/admin/dlq/${eventId}/replay`,
      headers: {
        'x-admin-key': adminKey,
      },
    });

    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.status).toBe('REPLAYED');
    expect(body.event_id).toBe(eventId);
    expect(body.current_state).toBe('PAID'); // Verified that canonical state 'PAID' is used, not 'REPLAY'

    // 1. Assert: Redis Fast-Path lock was deleted with hash tag {tenantId} so worker will NOT drop it as ALREADY_DONE!
    expect(mockRedis.del).toHaveBeenCalledWith(`idemp:{${tenantId}}:shopify:${eventId}`);

    // 2. Assert: Postgres event is QUEUED and idempotency key is PERSISTED
    expect(mockDb.webhookEvents.get(key)?.delivery_status).toBe('QUEUED');
    expect(mockDb.idempotencyKeys.get(key)?.status).toBe('PERSISTED');

    // 3. Assert: XADD was called to re-enqueue
    expect(mockRedis.xadd).toHaveBeenCalled();
  });
});
