import { describe, it, expect } from 'vitest';
import { ShuffleShardingRouter } from '../../src/gateway/router/shuffle-sharding.js';

describe('Shuffle Sharding Router (Power of Two Choices)', () => {
  const router = new ShuffleShardingRouter(16, 'webhooks:stream');

  it('generates deterministic candidate shards for a tenant', () => {
    const tenantId = 'a0000000-0000-0000-0000-000000000001';
    const [a1, b1] = router.getCandidateShards(tenantId);
    const [a2, b2] = router.getCandidateShards(tenantId);

    expect(a1).toBe(a2);
    expect(b1).toBe(b2);
    expect(a1).not.toBe(b1);
    expect(a1).toBeGreaterThanOrEqual(0);
    expect(a1).toBeLessThan(16);
    expect(b1).toBeGreaterThanOrEqual(0);
    expect(b1).toBeLessThan(16);
  });

  it('guarantees candidateA and candidateB are never equal across 500 tenants', () => {
    for (let i = 0; i < 500; i++) {
      const tenantId = `tenant_id_test_${i}`;
      const [candA, candB] = router.getCandidateShards(tenantId);
      expect(candA).not.toBe(candB);
    }
  });

  it('selects the shorter queue based on mock Redis lengths', async () => {
    const tenantId = 'test-merchant-xyz';
    const [candA, candB] = router.getCandidateShards(tenantId);

    // Mock Redis where candA has 500 items and candB has 25 items
    const mockRedis = {
      xlen: async (key: string) => {
        if (key.includes(String(candA).padStart(2, '0'))) return 500;
        if (key.includes(String(candB).padStart(2, '0'))) return 25;
        return 0;
      },
    } as any;

    const result = await router.selectShard(tenantId, mockRedis);
    expect(result.shardIndex).toBe(candB);
    expect(result.lengthA).toBe(500);
    expect(result.lengthB).toBe(25);
  });

  it('selects candidateA when candidateA is shorter or equal', async () => {
    const tenantId = 'test-merchant-xyz2';
    const [candA, candB] = router.getCandidateShards(tenantId);

    // Mock Redis where candA has 10 items and candB has 100 items
    const mockRedis = {
      xlen: async (key: string) => {
        if (key.includes(String(candA).padStart(2, '0'))) return 10;
        if (key.includes(String(candB).padStart(2, '0'))) return 100;
        return 0;
      },
    } as any;

    const result = await router.selectShard(tenantId, mockRedis);
    expect(result.shardIndex).toBe(candA);
    expect(result.lengthA).toBe(10);
    expect(result.lengthB).toBe(100);
  });
});

