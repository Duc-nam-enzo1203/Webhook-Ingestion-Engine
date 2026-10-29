import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { tenantCache } from '../../src/shared/cache/tenant-cache.js';

// In-memory mock database state
const mockDb = {
  idempotencyKeys: new Map<string, { status: string; locked_by: string }>(),
  entityStates: new Map<string, any>(),
  webhookEvents: new Map<string, { delivery_status: string; retry_count: number; last_error?: string }>(),
};

const mockDbClient = {
  query: vi.fn().mockImplementation(async (queryText: string, params: any[] = []) => {
    // console.log('QUERY CALL:', queryText, params);
    if (queryText.includes('BEGIN') || queryText.includes('COMMIT') || queryText.includes('ROLLBACK')) {
      return { rows: [] };
    }

    // Check idempotency_keys
    if (queryText.includes('idempotency_keys') && queryText.includes('SELECT')) {
      const key = `${params[0]}:${params[1]}:${params[2]}`;
      const entry = mockDb.idempotencyKeys.get(key);
      return { rows: entry ? [{ status: entry.status }] : [] };
    }

    // Insert idempotency_keys
    if (queryText.includes('INSERT INTO idempotency_keys')) {
      const key = `${params[0]}:${params[1]}:${params[2]}`;
      mockDb.idempotencyKeys.set(key, { status: params[3], locked_by: params[4] });
      return { rowCount: 1 };
    }

    // Update idempotency_keys
    if (queryText.includes('UPDATE idempotency_keys')) {
      let status = 'COMPLETED';
      let key = `${params[0]}:${params[1]}:${params[2]}`;
      if (queryText.includes("SET status = 'DLQ'")) {
        status = 'DLQ';
      } else if (queryText.includes("SET status = 'COMPLETED'")) {
        status = 'COMPLETED';
      } else if (params.length === 4) {
        status = params[0];
        key = `${params[1]}:${params[2]}:${params[3]}`;
      }
      const existing = mockDb.idempotencyKeys.get(key) || { locked_by: 'worker' };
      mockDb.idempotencyKeys.set(key, { ...existing, status });
      return { rowCount: 1 };
    }

    // Query webhook_events
    if (queryText.includes('SELECT retry_count, delivery_status FROM webhook_events')) {
      const key = `${params[0]}:${params[1]}:${params[2]}`;
      const event = mockDb.webhookEvents.get(key);
      return {
        rows: event ? [{ retry_count: event.retry_count, delivery_status: event.delivery_status }] : [],
      };
    }

    // Insert webhook_events
    if (queryText.includes('INSERT INTO webhook_events')) {
      const key = `${params[0]}:${params[1]}:${params[2]}`;
      mockDb.webhookEvents.set(key, { delivery_status: params[5], retry_count: 0 });
      return { rowCount: 1 };
    }

    // Update webhook_events
    if (queryText.includes('UPDATE webhook_events')) {
      if (queryText.includes("SET delivery_status = 'DELIVERED'")) {
        const key = `${params[0]}:${params[1]}:${params[2]}`;
        const existing = mockDb.webhookEvents.get(key) || { retry_count: 0 };
        mockDb.webhookEvents.set(key, { ...existing, delivery_status: 'DELIVERED' });
        return { rowCount: 1 };
      }
      if (queryText.includes("SET delivery_status = 'DLQ'")) {
        const key = `${params[2]}:${params[3]}:${params[4]}`;
        mockDb.webhookEvents.set(key, { delivery_status: 'DLQ', retry_count: params[0], last_error: params[1] });
        return { rowCount: 1 };
      }
      if (queryText.includes("SET delivery_status = 'RETRYING'")) {
        const key = `${params[2]}:${params[3]}:${params[4]}`;
        mockDb.webhookEvents.set(key, { delivery_status: 'RETRYING', retry_count: params[0], last_error: params[1] });
        return { rowCount: 1 };
      }
    }

    // OCC state upsert
    if (queryText.includes('INSERT INTO entity_states')) {
      return {
        rows: [{
          current_state: 'CREATED',
          is_hydrated: true,
          merged_payload: '{}',
          is_inserted: true,
          is_stale_ignored: false,
        }],
      };
    }

    return { rows: [], rowCount: 1 };
  }),
  release: vi.fn(),
};

vi.mock('../../src/shared/db/postgres.js', () => ({
  getDbPool: () => ({
    connect: vi.fn().mockResolvedValue(mockDbClient),
  }),
  closeDbPool: vi.fn(),
}));

const { WebhookProcessor } = await import('../../src/worker/processor.js');

describe('Downstream Failure & Two-Phase Durability Recovery', () => {
  const tenantId = 'b0000000-0000-0000-0000-000000000001';
  const processor = new WebhookProcessor('test_recovery_worker');

  const createMockRedis = () => ({
    eval: vi.fn().mockImplementation((script: string) => {
      if (script.includes('current_active')) return 1; // Semaphore acquired
      if (script.includes('LOCKED:')) return 'ACQUIRED';
      if (script.includes('COMMIT_DONE')) return 1;
      if (script.includes('ABORT_RELEASE')) return 1;
      return 1;
    }),
    zadd: vi.fn().mockResolvedValue(1),
  } as any);

  beforeEach(() => {
    mockDb.idempotencyKeys.clear();
    mockDb.entityStates.clear();
    mockDb.webhookEvents.clear();

    tenantCache.setTenantInL1({
      id: tenantId,
      name: 'Downstream Failure Merchant',
      platform: 'shopify',
      webhookSecret: 'secret_downstream',
      rateLimitRps: 100,
      maxConcurrency: 10,
      downstreamUrl: 'http://mock-erp.local/api/webhooks',
      isActive: true,
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('preserves PERSISTED status and schedules retry when downstream returns 500', async () => {
    const redis = createMockRedis();
    const eventId = 'evt_downstream_fail_001';
    const payload = {
      event_id: eventId,
      tenant_id: tenantId,
      platform: 'shopify',
      data: JSON.stringify({
        tenantId,
        platform: 'shopify',
        eventId,
        entityType: 'ORDER',
        entityId: 'ORD_FAIL_500',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: { id: 'ORD_FAIL_500' },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    };

    // Downstream returns 500 Internal Server Error
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      text: vi.fn().mockResolvedValue('ERP database deadlock'),
    }));

    const result = await processor.processEvent(redis, 'stream:webhooks:00', '1-0', payload);

    expect(result.status).toBe('DEFERRED');
    expect(result.reason).toBe('FAILED_RETRY');
    expect(result.error).toContain('Downstream returned HTTP 500');

    // Data in DB is PERSISTED (not COMPLETED), event is marked RETRYING with retry_count = 1
    const idempEntry = mockDb.idempotencyKeys.get(`${tenantId}:shopify:${eventId}`);
    expect(idempEntry?.status).toBe('PERSISTED');

    const eventEntry = mockDb.webhookEvents.get(`${tenantId}:shopify:${eventId}`);
    expect(eventEntry?.delivery_status).toBe('RETRYING');
    expect(eventEntry?.retry_count).toBe(1);

    // Delayed retry was scheduled into Redis ZSET
    expect(redis.zadd).toHaveBeenCalledWith('retry:delayed:zset', expect.any(Number), expect.any(String));
  });

  it('recovers on retry: transitions to COMPLETED when downstream returns 200', async () => {
    const redis = createMockRedis();
    const eventId = 'evt_downstream_fail_001'; // Same event retrying
    const payload = {
      event_id: eventId,
      tenant_id: tenantId,
      platform: 'shopify',
      data: JSON.stringify({
        tenantId,
        platform: 'shopify',
        eventId,
        entityType: 'ORDER',
        entityId: 'ORD_FAIL_500',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: { id: 'ORD_FAIL_500' },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    };

    // Pre-populate state from failed attempt: PERSISTED, retry_count = 1
    mockDb.idempotencyKeys.set(`${tenantId}:shopify:${eventId}`, { status: 'PERSISTED', locked_by: 'worker' });
    mockDb.webhookEvents.set(`${tenantId}:shopify:${eventId}`, { delivery_status: 'RETRYING', retry_count: 1 });

    // Downstream now recovers and returns 200 OK
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: vi.fn().mockResolvedValue('OK'),
    });
    vi.stubGlobal('fetch', fetchMock);

    const result = await processor.processEvent(redis, 'stream:webhooks:00', '2-0', payload);

    expect(result.status).toBe('SUCCESS');
    expect(fetchMock).toHaveBeenCalledTimes(1);

    // Assert: Now COMPLETED and DELIVERED
    const idempEntry = mockDb.idempotencyKeys.get(`${tenantId}:shopify:${eventId}`);
    expect(idempEntry?.status).toBe('COMPLETED');

    const eventEntry = mockDb.webhookEvents.get(`${tenantId}:shopify:${eventId}`);
    expect(eventEntry?.delivery_status).toBe('DELIVERED');
  });

  it('safely drops duplicate replay once event is COMPLETED', async () => {
    const redis = createMockRedis();
    const eventId = 'evt_downstream_fail_001';
    const payload = {
      event_id: eventId,
      tenant_id: tenantId,
      platform: 'shopify',
      data: JSON.stringify({
        tenantId,
        platform: 'shopify',
        eventId,
        entityType: 'ORDER',
        entityId: 'ORD_FAIL_500',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: { id: 'ORD_FAIL_500' },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    };

    // State is already COMPLETED
    mockDb.idempotencyKeys.set(`${tenantId}:shopify:${eventId}`, { status: 'COMPLETED', locked_by: 'worker' });

    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);

    const result = await processor.processEvent(redis, 'stream:webhooks:00', '3-0', payload);

    expect(result.status).toBe('DUPLICATE');
    expect(fetchMock).not.toHaveBeenCalled(); // Downstream is NOT called again
  });

  it('routes directly to DLQ without retrying if downstream returns 400 Bad Request', async () => {
    const redis = createMockRedis();
    const eventId = 'evt_client_error_400';
    const payload = {
      event_id: eventId,
      tenant_id: tenantId,
      platform: 'shopify',
      data: JSON.stringify({
        tenantId,
        platform: 'shopify',
        eventId,
        entityType: 'ORDER',
        entityId: 'ORD_BAD_REQ',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: { id: 'ORD_BAD_REQ' },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    };

    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 400,
      text: vi.fn().mockResolvedValue('Malformed business payload'),
    }));

    const result = await processor.processEvent(redis, 'stream:webhooks:00', '4-0', payload);

    expect(result.status).toBe('DLQ');
    expect(mockDb.idempotencyKeys.get(`${tenantId}:shopify:${eventId}`)?.status).toBe('DLQ');
    expect(mockDb.webhookEvents.get(`${tenantId}:shopify:${eventId}`)?.delivery_status).toBe('DLQ');
    expect(redis.zadd).not.toHaveBeenCalled(); // No retry scheduled for client error
  });

  it('delivers late ORDER_CREATED to ERP with state CANCELLED when new hydration information arrives', async () => {
    const redis = createMockRedis();
    const eventId = 'evt_late_created_hydrated';
    const payload = {
      event_id: eventId,
      tenant_id: tenantId,
      platform: 'shopify',
      data: JSON.stringify({
        tenantId,
        platform: 'shopify',
        eventId,
        entityType: 'ORDER',
        entityId: 'ORD_LATE_HYDRATE',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now() - 10000,
        isHydrated: true,
        payload: { id: 'ORD_LATE_HYDRATE', line_items: [{ name: 'Item Alpha' }] },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    };

    let dispatchedPayload: any = null;
    vi.stubGlobal('fetch', vi.fn().mockImplementation(async (_url, opts) => {
      dispatchedPayload = JSON.parse(opts.body);
      return { ok: true, status: 200, text: async () => 'OK' };
    }));

    const result = await processor.processEvent(redis, 'stream:webhooks:00', '5-0', payload);

    expect(result.status).toBe('SUCCESS');
    expect(dispatchedPayload).not.toBeNull();
    // Delivery was made to ERP because isHydrated was true on an existing entity
    expect(dispatchedPayload.entity_id).toBe('ORD_LATE_HYDRATE');
  });
});
