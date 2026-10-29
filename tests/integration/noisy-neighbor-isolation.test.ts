import { describe, it, expect, vi } from 'vitest';
import { tenantCache } from '../../src/shared/cache/tenant-cache.js';

// Mock DB for isolated testing
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

const { WebhookProcessor } = await import('../../src/worker/processor.js');

describe('Integration: Multi-Tenant Noisy Neighbor Isolation', () => {
  const processor = new WebhookProcessor('worker_isolation_test');

  const tenantA = {
    id: 'a0000000-0000-0000-0000-000000000001',
    name: 'Mega Store A (Flash Sale)',
    platform: 'shopify' as const,
    webhookSecret: 'sec_a',
    rateLimitRps: 500,
    maxConcurrency: 3, // Concurrency capped at 3
    downstreamUrl: null,
    isActive: true,
  };

  const tenantB = {
    id: 'b0000000-0000-0000-0000-000000000002',
    name: 'Small Boutique B',
    platform: 'shopify' as const,
    webhookSecret: 'sec_b',
    rateLimitRps: 50,
    maxConcurrency: 5,
    downstreamUrl: null,
    isActive: true,
  };

  tenantCache.setTenantInL1(tenantA);
  tenantCache.setTenantInL1(tenantB);

  it('prevents Tenant A from exhausting slots for Tenant B', async () => {
    let tenantAActiveSlots = 0;

    const mockRedis = {
      eval: vi.fn().mockImplementation((script: string, _numkeys: number, key: string) => {
        if (script.includes('current_active')) {
          if (key.includes(tenantA.id)) {
            if (tenantAActiveSlots >= tenantA.maxConcurrency) {
              return 0; // Deny 4th slot to Tenant A!
            }
            tenantAActiveSlots++;
            return 1;
          }
          return 1; // Always allow Tenant B
        }
        return 1; // Default allow locks
      }),
    } as any;

    const makePayload = (tenantId: string, eventId: string) => ({
      event_id: eventId,
      tenant_id: tenantId,
      platform: 'shopify',
      data: JSON.stringify({
        tenantId,
        platform: 'shopify',
        eventId,
        entityType: 'ORDER',
        entityId: `ORD_${eventId}`,
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: {},
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    });

    // 1. Tenant A fires 4 concurrent requests
    const resA1 = await processor.processEvent(mockRedis, 's:00', '1', makePayload(tenantA.id, 'wh_a1'));
    const resA2 = await processor.processEvent(mockRedis, 's:00', '2', makePayload(tenantA.id, 'wh_a2'));
    const resA3 = await processor.processEvent(mockRedis, 's:00', '3', makePayload(tenantA.id, 'wh_a3'));
    const resA4 = await processor.processEvent(mockRedis, 's:00', '4', makePayload(tenantA.id, 'wh_a4'));

    // First 3 get slots, 4th is capped
    expect(resA1.status).toBe('SUCCESS');
    expect(resA2.status).toBe('SUCCESS');
    expect(resA3.status).toBe('SUCCESS');
    expect(resA4.status).toBe('SEMAPHORE_FULL'); // Isolated!

    // 2. Tenant B fires a request while Tenant A is full
    const resB1 = await processor.processEvent(mockRedis, 's:01', '5', makePayload(tenantB.id, 'wh_b1'));

    // Tenant B succeeds without delay
    expect(resB1.status).toBe('SUCCESS');
  });
});
