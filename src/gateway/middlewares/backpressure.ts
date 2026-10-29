import { Redis } from 'ioredis';
import { env } from '../../config/env.js';
import { logger } from '../../shared/observability/logger.js';

export interface BackpressureCheckResult {
  overloaded: boolean;
  reason?: string;
  retryAfterSec: number;
}

export class BackpressureChecker {
  private lastMemoryCheck = 0;
  private isMemoryCritical = false;
  private readonly checkIntervalMs = 2000;

  async checkBackpressure(
    redis: Redis,
    shardQueueLength: number
  ): Promise<BackpressureCheckResult> {
    // 1. Check shard queue backlog
    if (shardQueueLength >= env.MAX_STREAM_QUEUE_LENGTH) {
      return {
        overloaded: true,
        reason: `Stream queue depth ${shardQueueLength} exceeds limit ${env.MAX_STREAM_QUEUE_LENGTH}`,
        retryAfterSec: 30,
      };
    }

    // 2. Periodic Redis Memory Check (every 2s)
    const now = Date.now();
    if (now - this.lastMemoryCheck > this.checkIntervalMs) {
      this.lastMemoryCheck = now;
      try {
        const memoryInfo = await redis.info('memory');
        const usedMatch = memoryInfo.match(/used_memory:(\d+)/);
        const maxMatch = memoryInfo.match(/maxmemory:(\d+)/);

        if (usedMatch && maxMatch) {
          const used = Number(usedMatch[1]);
          const max = Number(maxMatch[1]);
          if (max > 0 && used / max >= 0.85) {
            this.isMemoryCritical = true;
            logger.warn({ used, max }, 'Redis memory usage exceeded 85%, activating backpressure');
          } else {
            this.isMemoryCritical = false;
          }
        }
      } catch {
        // Ignore failure to fetch info
      }
    }

    if (this.isMemoryCritical) {
      return {
        overloaded: true,
        reason: 'Redis memory pressure critical (>85%)',
        retryAfterSec: 60,
      };
    }

    return {
      overloaded: false,
      retryAfterSec: 0,
    };
  }
}

export const backpressureChecker = new BackpressureChecker();

