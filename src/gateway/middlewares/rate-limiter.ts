import { Redis } from 'ioredis';

/**
 * Atomic Token Bucket Rate Limiter Lua Script
 * KEYS[1]: rate_limit:{tenant_id}
 * ARGV[1]: capacity (burst limit)
 * ARGV[2]: refill_rate_per_sec
 * ARGV[3]: current_timestamp_ms
 * ARGV[4]: cost (default 1)
 *
 * Returns: { allowed: 1 or 0, current_tokens: number, retry_after_sec: number }
 */
export const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4]) or 1

local data = redis.call('HMGET', key, 'tokens', 'last_updated')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens or not last_updated then
    tokens = capacity
    last_updated = now
else
    local elapsed_seconds = math.max(0, (now - last_updated) / 1000)
    tokens = math.min(capacity, tokens + elapsed_seconds * refill_rate)
    last_updated = now
end

if tokens >= cost then
    tokens = tokens - cost
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    redis.call('EXPIRE', key, math.ceil(capacity / refill_rate) * 2 + 10)
    return {1, math.floor(tokens), 0}
else
    local needed = cost - tokens
    local retry_after = math.ceil(needed / refill_rate)
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    redis.call('EXPIRE', key, retry_after + 10)
    return {0, math.floor(tokens), retry_after}
end
`;

export interface RateLimitResult {
  allowed: boolean;
  tokensLeft: number;
  retryAfterSec: number;
}

export async function checkTenantRateLimit(
  redis: Redis,
  tenantId: string,
  rateLimitRps: number,
  cost = 1
): Promise<RateLimitResult> {
  const key = `rate_limit:${tenantId}`;
  const now = Date.now();
  const burstCapacity = rateLimitRps * 2; // Allow burst up to 2x RPS

  try {
    const res = (await redis.eval(
      TOKEN_BUCKET_LUA,
      1,
      key,
      burstCapacity,
      rateLimitRps,
      now,
      cost
    )) as [number, number, number];

    return {
      allowed: res[0] === 1,
      tokensLeft: res[1],
      retryAfterSec: res[2],
    };
  } catch {
    // Fail-open strategy if Redis rate limiter errors to prevent blocking valid merchant traffic
    return {
      allowed: true,
      tokensLeft: rateLimitRps,
      retryAfterSec: 0,
    };
  }
}

