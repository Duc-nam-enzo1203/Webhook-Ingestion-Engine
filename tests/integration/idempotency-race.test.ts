import { describe, it, expect, vi, beforeEach } from 'vitest';
import { tenantCache } from '../../src/shared/cache/tenant-cache.js';

// Mock PostgreSQL for integration test environment
const mockDbClient = {
  query: vi.fn().mockImplementation((queryText: string) => {
    if (queryText.includes('BEGIN') || queryText.includes('COMMIT') || queryText.includes('ROLLBACK')) {
      return { rows: [] };
    }
    if (queryText.includes('INSERT INTO idempotency_keys')) {
      return { rowCount: 1 };
    }
    if (queryText.includes('INSERT INTO entity_states')) {
      return {
        rows: [{
          current_state: 'CREATED',
          is_hydrated: true,
          merged_payload: '{}',
          is_inserted: true,
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

// Import after DB mock
const { WebhookProcessor } = await import('../../src/worker/processor.js');

describe('Integration: Idempotency Race Condition Protection', () => {
  const tenantId = 'a0000000-0000-0000-0000-000000000001';
  const processor = new WebhookProcessor('test_worker_alpha');

  beforeEach(() => {
    tenantCache.setTenantInL1({
      id: tenantId,
      name: 'Alpha Merchant',
      platform: 'shopify',
      webhookSecret: 'secret_123',
      rateLimitRps: 100,
      maxConcurrency: 10,
      downstreamUrl: null, // Avoid network calls during race test
      isActive: true,
    });
  });

  it('filters 19 out of 20 concurrent identical webhook requests', async () => {
    const eventId = 'wh_race_test_99812';
    const rawPayload = {
      event_id: eventId,
      tenant_id: tenantId,
      platform: 'shopify',
      data: JSON.stringify({
        tenantId,
        platform: 'shopify',
        eventId,
        entityType: 'ORDER',
        entityId: 'ORD_RACE_1',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: { id: 'ORD_RACE_1', total_price: '199.00' },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    };

    let lockHolder: string | null = null;
    let isCommitted = false;

    const mockRedis = {
      eval: vi.fn().mockImplementation((script: string, _numkeys: number, _key: string, ...args: any[]) => {
        if (script.includes('current_active')) {
          return 1;
        }
        if (script.includes('LOCKED:')) {
          const workerId = args[0];
          if (isCommitted) return 'ALREADY_DONE';
          if (!lockHolder) {
            lockHolder = workerId;
            return 'ACQUIRED';
          }
          if (lockHolder === workerId) return 'ACQUIRED';
          return 'IN_FLIGHT_BY_OTHER';
        }
        if (script.includes('COMMIT_DONE')) {
          isCommitted = true;
          return 1;
        }
        return 1;
      }),
      zadd: vi.fn().mockResolvedValue(1),
    } as any;

    // Fire 20 requests simultaneously across different worker instances in the cluster
    const promises = Array.from({ length: 20 }, (_, idx) => {
      const worker = new WebhookProcessor(`worker_node_${idx}`);
      return worker.processEvent(mockRedis, 'webhooks:stream:00', `1761720000000-${idx}`, rawPayload);
    });

    const results = await Promise.all(promises);

    const successfulDispatches = results.filter((r) => r.status === 'SUCCESS');
    const blockedDuplicates = results.filter(
      (r) => r.status === 'DUPLICATE' || r.status === 'IN_FLIGHT' || (r.status === 'DEFERRED' && r.reason === 'IN_FLIGHT')
    );

    expect(successfulDispatches.length).toBe(1);
    expect(blockedDuplicates.length).toBe(19);
  });
});
