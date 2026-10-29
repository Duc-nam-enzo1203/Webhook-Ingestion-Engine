import { describe, it, expect, vi, beforeEach } from 'vitest';
import { DelayedRetryScheduler } from '../../src/worker/resilience/delayed-retry.js';

describe('DelayedRetryScheduler Atomic Claim & Zero Loss', () => {
  let scheduler: DelayedRetryScheduler;

  beforeEach(() => {
    scheduler = new DelayedRetryScheduler();
  });

  it('atomically claims and re-enqueues due items via Lua script', async () => {
    const rawPayload = {
      event_id: 'evt_delayed_01',
      tenant_id: 'a0000000-0000-0000-0000-000000000001',
      platform: 'shopify',
      data: '{}',
    };

    const serializedItem = JSON.stringify({
      streamKey: 'webhooks:stream:00',
      rawPayload,
      scheduledAt: Date.now() - 5000,
    });

    const mockRedis = {
      eval: vi.fn().mockResolvedValue([serializedItem]),
      xadd: vi.fn().mockResolvedValue('1761720000000-0'),
      zadd: vi.fn(),
    } as any;

    const replayed = await scheduler.processDueRetries(mockRedis);

    expect(replayed).toBe(1);
    expect(mockRedis.eval).toHaveBeenCalledWith(
      expect.stringContaining('ZRANGEBYSCORE'),
      1,
      'retry:delayed:zset',
      expect.any(Number),
      50
    );
    expect(mockRedis.xadd).toHaveBeenCalledWith(
      'webhooks:stream:00',
      '*',
      'event_id', 'evt_delayed_01',
      'tenant_id', rawPayload.tenant_id,
      'platform', 'shopify',
      'data', '{}'
    );
  });

  it('restores item to ZSET if XADD fails (Zero Retry Loss Guarantee)', async () => {
    const rawPayload = {
      event_id: 'evt_xadd_fail_02',
      tenant_id: 'a0000000-0000-0000-0000-000000000001',
      platform: 'shopify',
      data: '{}',
    };

    const serializedItem = JSON.stringify({
      streamKey: 'webhooks:stream:01',
      rawPayload,
      scheduledAt: Date.now() - 5000,
    });

    const mockRedis = {
      eval: vi.fn().mockResolvedValue([serializedItem]),
      xadd: vi.fn().mockRejectedValue(new Error('Redis connection timeout during XADD')),
      zadd: vi.fn().mockResolvedValue(1),
    } as any;

    const replayed = await scheduler.processDueRetries(mockRedis);

    expect(replayed).toBe(0);
    // Crucial: Item must be restored to ZSET so it can be retried later!
    expect(mockRedis.zadd).toHaveBeenCalledWith(
      'retry:delayed:zset',
      expect.any(Number),
      serializedItem
    );
  });
});
