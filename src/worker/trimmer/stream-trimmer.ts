import { Redis } from 'ioredis';
import { env } from '../../config/env.js';
import { logger } from '../../shared/observability/logger.js';

export class SafeStreamTrimmer {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  start(redis: Redis, intervalMs = 10000): void {
    this.isRunning = true;
    this.timer = setInterval(async () => {
      if (!this.isRunning) return;
      await this.trimAllShards(redis);
    }, intervalMs);
  }

  stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async trimAllShards(redis: Redis): Promise<void> {
    for (let i = 0; i < env.STREAM_SHARD_COUNT; i++) {
      const shardKey = `${env.STREAM_PREFIX}:${String(i).padStart(2, '0')}`;
      try {
        await this.trimShard(redis, shardKey);
      } catch (err) {
        // Intermittent trimming errors are safe to catch
      }
    }
  }

  /**
   * Safe MINID Trimming:
   * Only prunes entries that are strictly older than the oldest pending entry across consumer groups.
   */
  async trimShard(redis: Redis, shardKey: string): Promise<number> {
    try {
      // 1. Inspect Pending Entries List (PEL) for oldest pending message
      const pendingSummary = (await redis.xpending(shardKey, 'webhook_workers')) as any;
      const pendingCount = Array.isArray(pendingSummary) ? Number(pendingSummary[0]) : 0;

      if (pendingCount > 0) {
        // Oldest unacknowledged message in PEL is the lower bound
        const oldestPendingId = pendingSummary[1]; // [count, min_id, max_id, consumers]
        if (oldestPendingId && typeof oldestPendingId === 'string' && oldestPendingId !== '0-0') {
          // XTRIM key MINID ~ oldestPendingId
          return await redis.xtrim(shardKey, 'MINID', '~', oldestPendingId);
        }
      } else {
        // No pending entries in PEL; check last-delivered-id of consumer group
        const groupInfo = (await redis.xinfo('GROUPS', shardKey).catch(() => [])) as any;
        if (Array.isArray(groupInfo)) {
          for (const g of groupInfo) {
            let lastDeliveredId = '';
            if (Array.isArray(g)) {
              for (let k = 0; k < g.length; k += 2) {
                if (g[k] === 'last-delivered-id') lastDeliveredId = g[k + 1];
              }
            }
            if (lastDeliveredId && lastDeliveredId !== '0-0') {
              return await redis.xtrim(shardKey, 'MINID', '~', lastDeliveredId);
            }
          }
        }
      }
    } catch (err) {
      logger.debug({ err, shardKey }, 'Safe trim skipped for shard');
    }
    return 0;
  }
}

export const safeStreamTrimmer = new SafeStreamTrimmer();
