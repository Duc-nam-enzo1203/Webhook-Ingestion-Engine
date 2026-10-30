import { Redis } from 'ioredis';
import { getDbPool } from '../../shared/db/postgres.js';
import { logger } from '../../shared/observability/logger.js';
import { shuffleShardingRouter } from '../../gateway/router/shuffle-sharding.js';
import { VerifiedWebhookEvent } from '../../gateway/adapters/types.js';

export class ReconciliationSweeper {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  start(redis: Redis, intervalMs = 30000): void {
    this.isRunning = true;
    this.timer = setInterval(async () => {
      if (!this.isRunning) return;
      await this.sweepStuckEvents(redis);
    }, intervalMs);
    logger.info('🔄 Periodic Reconciliation Sweeper started (every 30s)');
  }

  stop(): void {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Scans PostgreSQL for events stuck in 'PERSISTED' or 'IN_PROGRESS' > 2 minutes
   * that have not reached 'DELIVERED' or 'DLQ'.
   * Re-enqueues them into their designated Redis Stream shards to guarantee recovery.
   */
  async sweepStuckEvents(redis: Redis, limit = 50): Promise<number> {
    const pool = getDbPool();
    let replayedCount = 0;

    try {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const query = `
          SELECT k.tenant_id, k.platform, k.event_id, 
                 e.entity_type, e.entity_id, e.raw_payload, e.retry_count
          FROM idempotency_keys k
          JOIN webhook_events e 
            ON k.tenant_id = e.tenant_id AND k.platform = e.platform AND k.event_id = e.event_id
          WHERE k.status IN ('PERSISTED', 'IN_PROGRESS')
            AND k.updated_at < NOW() - INTERVAL '2 minutes'
            AND e.delivery_status NOT IN ('DELIVERED', 'DLQ', 'RETRYING')
          ORDER BY k.updated_at ASC
          LIMIT $1
          FOR UPDATE OF k SKIP LOCKED
        `;

        const res = await client.query(query, [limit]);
        if (res.rows.length === 0) {
          await client.query('COMMIT');
          return 0;
        }

        logger.warn({ count: res.rows.length }, '🔍 Reconciliation Sweeper detected stuck events, re-enqueuing');

        for (const row of res.rows) {
          const { tenant_id, platform, event_id, entity_type, entity_id, raw_payload } = row;
          const payload = typeof raw_payload === 'string' ? JSON.parse(raw_payload) : raw_payload;

          const verifiedEvent: VerifiedWebhookEvent = {
            tenantId: tenant_id,
            platform,
            eventId: event_id,
            entityType: entity_type,
            entityId: entity_id,
            eventType: `${entity_type.toLowerCase()}/reconcile`,
            currentState: 'RECONCILE',
            stateWeight: 10,
            externalVersion: Date.now(),
            isHydrated: true,
            payload,
            rawBody: JSON.stringify(payload),
            receivedAt: new Date().toISOString(),
          };

          const shardSelection = await shuffleShardingRouter.selectShard(tenant_id, redis);

          // Update timestamp so other sweepers do not duplicate
          await client.query(
            `UPDATE idempotency_keys SET updated_at = NOW() WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
            [tenant_id, platform, event_id]
          );

          await redis.xadd(
            shardSelection.selectedShard,
            '*',
            'event_id', event_id,
            'tenant_id', tenant_id,
            'platform', platform,
            'data', JSON.stringify(verifiedEvent)
          );

          replayedCount++;
        }

        await client.query('COMMIT');
      } catch (innerErr) {
        try {
          await client.query('ROLLBACK');
        } catch {}
        throw innerErr;
      } finally {
        client.release();
      }
    } catch (err) {
      logger.debug({ err }, 'Error during reconciliation sweep pass');
    }

    return replayedCount;
  }
}

export const reconciliationSweeper = new ReconciliationSweeper();

