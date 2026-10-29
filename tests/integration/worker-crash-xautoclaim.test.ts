import { describe, it, expect, vi } from 'vitest';
import { StreamConsumerPool } from '../../src/worker/consumers/stream.consumer.js';

describe('Integration: Worker Crash Recovery via XAUTOCLAIM', () => {
  it('detects and claims orphaned messages from dead workers', async () => {
    const consumer = new StreamConsumerPool('worker_rescue_hero');

    const abandonedMessageId = '1761720000000-0';
    const abandonedPayload = [
      'event_id', 'wh_abandoned_001',
      'tenant_id', 'a0000000-0000-0000-0000-000000000001',
      'platform', 'shopify',
      'data', JSON.stringify({
        tenantId: 'a0000000-0000-0000-0000-000000000001',
        platform: 'shopify',
        eventId: 'wh_abandoned_001',
        entityType: 'ORDER',
        entityId: 'ORD_ZOMBIE_1',
        eventType: 'orders/create',
        currentState: 'CREATED',
        stateWeight: 10,
        externalVersion: Date.now(),
        isHydrated: true,
        payload: { id: 'ORD_ZOMBIE_1' },
        rawBody: '{}',
        receivedAt: new Date().toISOString(),
      }),
    ];

    let xackCalled = false;

    // Mock Redis returning 1 zombie message on xautoclaim
    const mockRedis = {
      xgroup: vi.fn().mockResolvedValue('OK'),
      xautoclaim: vi.fn().mockResolvedValue([
        '0-0', // next start ID
        [[abandonedMessageId, abandonedPayload]], // claimed messages array
      ]),
      xack: vi.fn().mockImplementation((_stream, _group, id) => {
        if (id === abandonedMessageId) {
          xackCalled = true;
        }
        return 1;
      }),
      eval: vi.fn().mockResolvedValue(1),
    } as any;

    consumer.startAutoclaimLoop(mockRedis, 50);

    // Fast-forward or trigger autoclaim interval
    await vi.waitFor(() => {
      expect(mockRedis.xautoclaim).toHaveBeenCalled();
    }, { timeout: 2000 });

    consumer.stop();
  });
});
