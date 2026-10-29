import { Redis } from 'ioredis';
import { logger } from '../../shared/observability/logger.js';

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

  async schedule(redis: Redis, streamKey: string, rawPayload: any, delayMs: number): Promise<void> {
    const executeAt = Date.now() + delayMs;
    const item = JSON.stringify({
      streamKey,
      rawPayload,
      scheduledAt: Date.now(),
    });
    try {
      await redis.zadd(this.retryKey, executeAt, item);
    } catch (err) {
      logger.error({ err }, 'Failed to add item to delayed retry ZSET');
    }
  }

  async processDueRetries(redis: Redis): Promise<void> {
    const now = Date.now();
    try {
      // Fetch up to 50 items whose scheduled execution time has arrived
      const dueItems = (await redis.zrangebyscore(this.retryKey, '-inf', now, 'LIMIT', 0, 50)) as string[];
      if (!dueItems || dueItems.length === 0) return;

      for (const itemStr of dueItems) {
        try {
          const item = JSON.parse(itemStr);
          const { streamKey, rawPayload } = item;

          // Re-append to the stream shard
          await redis.xadd(
            streamKey,
            '*',
            'event_id', rawPayload.event_id,
            'tenant_id', rawPayload.tenant_id,
            'platform', rawPayload.platform,
            'data', rawPayload.data
          );

          // Remove from ZSET once re-queued
          await redis.zrem(this.retryKey, itemStr);
        } catch {
          await redis.zrem(this.retryKey, itemStr);
        }
      }
    } catch (err) {
      logger.debug({ err }, 'Error in delayed retry loop');
    }
  }
}

export const delayedRetryScheduler = new DelayedRetryScheduler();
