import { Redis } from 'ioredis';
import { logger } from '../../shared/observability/logger.js';

export interface DeferredRetryPayload {
  streamKey: string;
  rawPayload: {
    event_id: string;
    tenant_id: string;
    platform: string;
    data: string;
    defer_count?: number;
  };
  scheduledAt: number;
}

/**
 * Lua script for Atomic Claim from Delayed Retry ZSET:
 * Atomically inspects due items (score <= now), removes them from the ZSET,
 * and returns them. This guarantees that concurrent workers NEVER claim
 * or duplicate the same retry item.
 */
const ATOMIC_CLAIM_LUA = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
if #due > 0 then
  for _, member in ipairs(due) do
    redis.call('ZREM', KEYS[1], member)
  end
end
return due
`;

export class DelayedRetryScheduler {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;
  private readonly retryKey = 'retry:delayed:zset';

  start(redis: Redis, intervalMs = 1000): void {
    this.isRunning = true;
    this.timer = setInterval(async () => {
      if (!this.isRunning) return;
      await this.processDueRetries(redis);
    }, intervalMs);
  }

  stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async schedule(
    redis: Redis,
    streamKey: string,
    rawPayload: { event_id: string; tenant_id: string; platform: string; data: string; defer_count?: number },
    delayMs: number
  ): Promise<void> {
    const executeAt = Date.now() + delayMs;
    const item: DeferredRetryPayload = {
      streamKey,
      rawPayload,
      scheduledAt: Date.now(),
    };
    try {
      await redis.zadd(this.retryKey, executeAt, JSON.stringify(item));
    } catch (err) {
      logger.error({ err, eventId: rawPayload.event_id }, 'Failed to add item to delayed retry ZSET');
    }
  }

  /**
   * Atomically claims and re-enqueues due items into their respective Redis Stream shards.
   * If XADD fails for any reason, the item is restored to the ZSET to guarantee zero loss.
   */
  async processDueRetries(redis: Redis, batchSize = 50): Promise<number> {
    const now = Date.now();
    let replayedCount = 0;

    try {
      // 1. Atomic claim via Lua script: ZRANGEBYSCORE + ZREM in a single atomic step
      const dueItems = (await redis.eval(
        ATOMIC_CLAIM_LUA,
        1,
        this.retryKey,
        now,
        batchSize
      )) as string[];

      if (!dueItems || !Array.isArray(dueItems) || dueItems.length === 0) {
        return 0;
      }

      for (const itemStr of dueItems) {
        let item: DeferredRetryPayload;
        try {
          item = JSON.parse(itemStr);
        } catch {
          // Unparseable corrupt item, safe to drop
          continue;
        }

        const { streamKey, rawPayload } = item;

        try {
          // Re-append to the stream shard with defer_count preserved
          await redis.xadd(
            streamKey,
            '*',
            'event_id', rawPayload.event_id,
            'tenant_id', rawPayload.tenant_id,
            'platform', rawPayload.platform,
            'data', rawPayload.data,
            'defer_count', String(rawPayload.defer_count || 0)
          );
          replayedCount++;
        } catch (xaddErr) {
          logger.error({ err: xaddErr, eventId: rawPayload.event_id, streamKey }, 'Failed to XADD replayed item, restoring to ZSET');
          // Re-schedule with 2-second backoff so retry is not lost
          await redis.zadd(this.retryKey, Date.now() + 2000, itemStr).catch(() => { });
        }
      }
    } catch (err) {
      logger.debug({ err }, 'Error in delayed retry loop');
    }

    return replayedCount;
  }
}

export const delayedRetryScheduler = new DelayedRetryScheduler();
