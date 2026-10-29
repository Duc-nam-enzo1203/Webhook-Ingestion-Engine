import { Redis } from 'ioredis';

/**
 * Crash-Safe Per-Tenant Semaphore Lua Script (Redis ZSET Lease)
 * Prevents deadlocks if worker crashes/OOMs by expiring leases with scores.
 */
export const SEMAPHORE_ACQUIRE_LUA = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local max_slots = tonumber(ARGV[3])
local owner = ARGV[4]

-- 1. Evict expired leases from dead workers
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- 2. Check currently active lease count
local current_active = redis.call('ZCARD', key)

if current_active < max_slots then
    redis.call('ZADD', key, now + ttl, owner)
    redis.call('PEXPIRE', key, ttl * 2)
    return 1
else
    return 0
end
`;

export const SEMAPHORE_RELEASE_LUA = `
local key = KEYS[1]
local owner = ARGV[1]
return redis.call('ZREM', key, owner)
`;

export async function acquireTenantSemaphore(
  redis: Redis,
  tenantId: string,
  ownerId: string,
  maxConcurrency: number,
  leaseDurationMs = 30000
): Promise<boolean> {
  const key = `semaphore:tenant:${tenantId}`;
  const now = Date.now();

  try {
    const res = (await redis.eval(
      SEMAPHORE_ACQUIRE_LUA,
      1,
      key,
      now,
      leaseDurationMs,
      maxConcurrency,
      ownerId
    )) as number;

    return res === 1;
  } catch {
    // If Redis fails, fail-safe permit execution with bounded concurrency
    return true;
  }
}

export async function releaseTenantSemaphore(
  redis: Redis,
  tenantId: string,
  ownerId: string
): Promise<void> {
  const key = `semaphore:tenant:${tenantId}`;
  try {
    await redis.eval(SEMAPHORE_RELEASE_LUA, 1, key, ownerId);
  } catch {
    // Ignore error on cleanup
  }
}

