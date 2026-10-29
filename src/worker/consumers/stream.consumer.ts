import { Redis } from 'ioredis';
import { env } from '../../config/env.js';
import { logger } from '../../shared/observability/logger.js';
import { getRedisClient } from '../../shared/db/redis.js';
import { WebhookProcessor } from '../processor.js';
import { streamBacklogRecords } from '../../shared/observability/metrics.js';
import { safeStreamTrimmer } from '../trimmer/stream-trimmer.js';
import { delayedRetryScheduler } from '../resilience/delayed-retry.js';
import { reconciliationSweeper } from '../reconciliation/sweeper.js';

export class StreamConsumerPool {
  private readonly groupName = 'webhook_workers';
  private readonly workerId: string;
  private readonly processor: WebhookProcessor;
  private isRunning = false;
  private autoclaimTimer: NodeJS.Timeout | null = null;

  constructor(workerId?: string) {
    this.workerId = workerId || `consumer_${process.pid}_${Math.random().toString(36).substring(2, 7)}`;
    this.processor = new WebhookProcessor(this.workerId);
  }

  /**
   * Initializes consumer groups for all stream shards
   */
  async initConsumerGroups(redis: Redis): Promise<void> {
    for (let i = 0; i < env.STREAM_SHARD_COUNT; i++) {
      const shardKey = `${env.STREAM_PREFIX}:${String(i).padStart(2, '0')}`;
      try {
        await redis.xgroup('CREATE', shardKey, this.groupName, '$', 'MKSTREAM');
      } catch (err: any) {
        if (!err.message?.includes('BUSYGROUP')) {
          logger.warn({ err, shardKey }, 'Could not create consumer group');
        }
      }
    }
  }

  /**
   * Background Zombie Recovery Loop (XAUTOCLAIM):
   * Recovers messages that have been stuck in PEL > 30s due to worker crash
   */
  startAutoclaimLoop(redis: Redis, intervalMs = 15000): void {
    const minIdleTimeMs = 30000;
    this.isRunning = true;

    this.autoclaimTimer = setInterval(async () => {
      if (!this.isRunning) return;

      for (let i = 0; i < env.STREAM_SHARD_COUNT; i++) {
        const shardKey = `${env.STREAM_PREFIX}:${String(i).padStart(2, '0')}`;
        try {
          const res = (await redis.xautoclaim(
            shardKey,
            this.groupName,
            this.workerId,
            minIdleTimeMs,
            '0-0',
            'COUNT',
            10
          )) as any;

          if (res && res[1] && res[1].length > 0) {
            const claimedMessages = res[1];
            logger.info(
              { shardKey, count: claimedMessages.length },
              '⚠️ Rescued zombie messages from PEL via XAUTOCLAIM'
            );

            await Promise.allSettled(
              claimedMessages.map(async (item: any) => {
                const msgId = item[0];
                const fields = item[1];
                const parsed = this.parseStreamFields(fields);
                if (parsed) {
                  const outcome = await this.processor.processEvent(redis, shardKey, msgId, parsed);
                  if (
                    outcome.status === 'SUCCESS' ||
                    outcome.status === 'DUPLICATE' ||
                    outcome.status === 'DLQ' ||
                    outcome.status === 'DEFERRED'
                  ) {
                    await redis.xack(shardKey, this.groupName, msgId);
                  }
                }
              })
            );
          }
        } catch {
          // Ignore intermittent recovery errors
        }
      }
    }, intervalMs);
  }

  /**
   * Main Read Loop across all stream shards with bounded concurrency
   */
  async start(): Promise<void> {
    this.isRunning = true;
    const redis = getRedisClient();

    await this.initConsumerGroups(redis);
    this.startAutoclaimLoop(redis);

    // Start Safe MINID Trimmer (runs every 10s)
    safeStreamTrimmer.start(redis, 10000);

    // Start Delayed Retry Loop (runs every 1s)
    delayedRetryScheduler.start(redis, 1000);

    // Start Reconciliation Sweeper for stuck PERSISTED events (runs every 30s)
    reconciliationSweeper.start(redis, 30000);

    logger.info({ workerId: this.workerId }, '🚀 Stream Consumer Worker Pool started');

    const shardKeys: string[] = [];
    const streamIds: string[] = [];
    for (let i = 0; i < env.STREAM_SHARD_COUNT; i++) {
      shardKeys.push(`${env.STREAM_PREFIX}:${String(i).padStart(2, '0')}`);
      streamIds.push('>');
    }

    while (this.isRunning) {
      try {
        const streams = (await redis.xreadgroup(
          'GROUP',
          this.groupName,
          this.workerId,
          'COUNT',
          10,
          'BLOCK',
          2000,
          'STREAMS',
          ...shardKeys,
          ...streamIds
        )) as any;

        if (streams && Array.isArray(streams)) {
          for (const [shardKey, messages] of streams) {
            streamBacklogRecords.set({ shard: shardKey }, messages.length);

            // Process all messages in batch concurrently, bounded by per-tenant semaphores
            await Promise.allSettled(
              messages.map(async ([msgId, fields]: [string, string[]]) => {
                const parsed = this.parseStreamFields(fields);
                if (parsed) {
                  const outcome = await this.processor.processEvent(redis, shardKey, msgId, parsed);
                  if (
                    outcome.status === 'SUCCESS' ||
                    outcome.status === 'DUPLICATE' ||
                    outcome.status === 'DLQ' ||
                    outcome.status === 'DEFERRED'
                  ) {
                    await redis.xack(shardKey, this.groupName, msgId);
                  }
                }
              })
            );
          }
        }
      } catch (err: any) {
        if (!this.isRunning) break;
        logger.error({ err }, 'Error in Stream Consumer Read Loop, sleeping 1s');
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }

  stop(): void {
    this.isRunning = false;
    if (this.autoclaimTimer) {
      clearInterval(this.autoclaimTimer);
      this.autoclaimTimer = null;
    }
    safeStreamTrimmer.stop();
    delayedRetryScheduler.stop();
    reconciliationSweeper.stop();
  }

  private parseStreamFields(fields: string[]): {
    event_id: string;
    tenant_id: string;
    platform: string;
    data: string;
    defer_count?: number;
  } | null {
    const obj: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) {
      obj[fields[i]] = fields[i + 1];
    }
    if (obj.event_id && obj.tenant_id && obj.data) {
      return {
        event_id: obj.event_id,
        tenant_id: obj.tenant_id,
        platform: obj.platform || 'shopify',
        data: obj.data,
        defer_count: obj.defer_count ? Number(obj.defer_count) : 0,
      };
    }
    return null;
  }
}

if (process.argv[1]?.endsWith('consumer.ts') || process.argv[1]?.endsWith('consumer.js')) {
  const pool = new StreamConsumerPool();
  pool.start().catch((err) => {
    logger.error({ err }, 'Worker process crashed');
    process.exit(1);
  });
}
