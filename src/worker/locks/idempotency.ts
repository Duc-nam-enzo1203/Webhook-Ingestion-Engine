import { Redis } from 'ioredis';

/**
 * Script 1: Acquire Idempotency Lease Lock
 * Returns: 'ACQUIRED' | 'ALREADY_DONE' | 'IN_FLIGHT_BY_OTHER'
 */
export const IDEMPOTENCY_ACQUIRE_LUA = `
local val = redis.call('GET', KEYS[1])
if not val then
    redis.call('SET', KEYS[1], 'LOCKED:' .. ARGV[1], 'PX', tonumber(ARGV[2]))
    return 'ACQUIRED'
elseif val == 'DONE' then
    return 'ALREADY_DONE'
elseif val == ('LOCKED:' .. ARGV[1]) then
    -- Renew lease if called by the same owner
    redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 'ACQUIRED'
else
    return 'IN_FLIGHT_BY_OTHER'
end
`;

/**
 * Script 2: Finalize or Release Idempotency Lock
 * ARGV[2]: "COMMIT_DONE" | "ABORT_RELEASE"
 */
export const IDEMPOTENCY_FINALIZE_LUA = `
local val = redis.call('GET', KEYS[1])
if val == ('LOCKED:' .. ARGV[1]) then
    if ARGV[2] == 'COMMIT_DONE' then
        redis.call('SET', KEYS[1], 'DONE', 'EX', tonumber(ARGV[3]))
        return 1
    else
        redis.call('DEL', KEYS[1])
        return 1
    end
end
return 0
`;

export type IdempotencyLockStatus = 'ACQUIRED' | 'ALREADY_DONE' | 'IN_FLIGHT_BY_OTHER';

/**
 * Canonical Redis key generator for Idempotency Locks.
 * Uses Redis Hash Tag `{tenantId}` to guarantee multi-key Lua commands
 * hash to the exact same cluster hash slot.
 */
export function getIdempotencyRedisKey(tenantId: string, platform: string, eventId: string): string {
  return `idemp:{${tenantId}}:${platform}:${eventId}`;
}

export async function acquireIdempotencyLock(
  redis: Redis,
  tenantId: string,
  platform: string,
  eventId: string,
  workerId: string,
  leaseTtlMs = 30000
): Promise<IdempotencyLockStatus> {
  const key = getIdempotencyRedisKey(tenantId, platform, eventId);

  try {
    const res = (await redis.eval(
      IDEMPOTENCY_ACQUIRE_LUA,
      1,
      key,
      workerId,
      leaseTtlMs
    )) as string;

    return res as IdempotencyLockStatus;
  } catch {
    return 'ACQUIRED';
  }
}

export async function finalizeIdempotencyLock(
  redis: Redis,
  tenantId: string,
  platform: string,
  eventId: string,
  workerId: string,
  action: 'COMMIT_DONE' | 'ABORT_RELEASE',
  doneTtlSeconds = 86400 // 24 hours
): Promise<boolean> {
  const key = getIdempotencyRedisKey(tenantId, platform, eventId);

  try {
    const res = (await redis.eval(
      IDEMPOTENCY_FINALIZE_LUA,
      1,
      key,
      workerId,
      action,
      doneTtlSeconds
    )) as number;

    return res === 1;
  } catch {
    return false;
  }
}

