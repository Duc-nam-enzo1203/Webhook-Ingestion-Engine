import { Redis } from 'ioredis';
import { getDbPool } from '../shared/db/postgres.js';
import { logger } from '../shared/observability/logger.js';
import {
  workerProcessingDurationMs,
  idempotencyHitsTotal,
  outOfOrderResolvedTotal,
  dlqDepth
} from '../shared/observability/metrics.js';
import { VerifiedWebhookEvent } from '../gateway/adapters/types.js';
import { tenantCache } from '../shared/cache/tenant-cache.js';
import { acquireTenantSemaphore, releaseTenantSemaphore } from './locks/semaphore.js';
import { acquireIdempotencyLock, finalizeIdempotencyLock } from './locks/idempotency.js';
import { executeOccStateUpsert } from './state-machine/occ-state.js';
import { getTenantCircuitBreaker, calculateExponentialBackoffJitter } from './resilience/circuit-breaker.js';

export interface ProcessEventResult {
  status: 'SUCCESS' | 'DUPLICATE' | 'IN_FLIGHT' | 'CIRCUIT_OPEN' | 'SEMAPHORE_FULL' | 'FAILED_RETRY' | 'DLQ';
  eventId: string;
  durationMs: number;
  error?: string;
}

export class WebhookProcessor {
  private readonly workerId: string;

  constructor(workerId?: string) {
    this.workerId = workerId || `worker_${process.pid}_${Math.random().toString(36).substring(2, 7)}`;
  }

  async processEvent(
    redis: Redis,
    streamKey: string,
    messageId: string,
    rawPayload: { event_id: string; tenant_id: string; platform: string; data: string }
  ): Promise<ProcessEventResult> {
    const startTime = Date.now();
    const event: VerifiedWebhookEvent = JSON.parse(rawPayload.data);
    const { tenantId, platform, eventId } = event;

    // 1. Fetch Tenant Configuration
    const tenant = await tenantCache.getTenant(tenantId);
    if (!tenant) {
      logger.warn({ tenantId, eventId }, 'Skipping event for unknown tenant');
      return { status: 'SUCCESS', eventId, durationMs: Date.now() - startTime };
    }

    // 2. Per-Tenant Circuit Breaker Check
    const circuitBreaker = getTenantCircuitBreaker(tenantId);
    if (circuitBreaker.getState() === 'OPEN') {
      logger.warn({ tenantId, eventId }, 'Tenant circuit breaker OPEN, postponing dispatch');
      return { status: 'CIRCUIT_OPEN', eventId, durationMs: Date.now() - startTime };
    }

    // 3. Acquire Tenant ZSET Lease Semaphore (bounded concurrency)
    const ownerId = `${this.workerId}:${eventId}`;
    const hasSlot = await acquireTenantSemaphore(
      redis,
      tenantId,
      ownerId,
      tenant.maxConcurrency,
      30000
    );

    if (!hasSlot) {
      // Concurrency limit reached for this tenant; release back to stream
      return { status: 'SEMAPHORE_FULL', eventId, durationMs: Date.now() - startTime };
    }

    try {
      // 4. Tier-1 Fast-Path Idempotency Lock on Redis
      const lockStatus = await acquireIdempotencyLock(
        redis,
        tenantId,
        platform,
        eventId,
        this.workerId,
        30000
      );

      if (lockStatus === 'ALREADY_DONE') {
        idempotencyHitsTotal.inc({ tier: 'redis_done' });
        await releaseTenantSemaphore(redis, tenantId, ownerId);
        return { status: 'DUPLICATE', eventId, durationMs: Date.now() - startTime };
      }

      if (lockStatus === 'IN_FLIGHT_BY_OTHER') {
        idempotencyHitsTotal.inc({ tier: 'redis_inflight' });
        await releaseTenantSemaphore(redis, tenantId, ownerId);
        return { status: 'IN_FLIGHT', eventId, durationMs: Date.now() - startTime };
      }

      // 5. Tier-2 PostgreSQL Atomic Persistence & OCC State Machine
      const pool = getDbPool();
      const client = await pool.connect();

      let isDuplicateDb = false;
      try {
        await client.query('BEGIN');

        // Check & Insert into idempotency_keys
        const idempInsert = await client.query(
          `INSERT INTO idempotency_keys (tenant_id, platform, event_id, status, locked_by, locked_until)
           VALUES ($1, $2, $3, 'IN_PROGRESS', $4, NOW() + INTERVAL '30 seconds')
           ON CONFLICT (tenant_id, platform, event_id) DO NOTHING`,
          [tenantId, platform, eventId, this.workerId]
        );

        if (idempInsert.rowCount === 0) {
          // Already exists in Database! Check status
          const checkRes = await client.query(
            `SELECT status FROM idempotency_keys WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
            [tenantId, platform, eventId]
          );

          if (checkRes.rows[0]?.status === 'COMPLETED') {
            await client.query('ROLLBACK');
            idempotencyHitsTotal.inc({ tier: 'postgres_conflict' });
            await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
            await releaseTenantSemaphore(redis, tenantId, ownerId);
            return { status: 'DUPLICATE', eventId, durationMs: Date.now() - startTime };
          }
        }

        // Execute OCC State Machine & Bidirectional JSONB Hydration Upsert
        const occResult = await executeOccStateUpsert(client, event);
        if (occResult.isStaleIgnored) {
          outOfOrderResolvedTotal.inc({ resolution: 'ignored_stale_update' });
        } else if (occResult.isHydrated && !occResult.isInserted) {
          outOfOrderResolvedTotal.inc({ resolution: 'hydrated_late_create' });
        } else {
          outOfOrderResolvedTotal.inc({ resolution: 'normal_progression' });
        }

        // Append to webhook_events audit log
        await client.query(
          `INSERT INTO webhook_events (tenant_id, platform, event_id, entity_type, entity_id, delivery_status, raw_payload)
           VALUES ($1, $2, $3, $4, $5, 'DELIVERED', $6)`,
          [tenantId, platform, eventId, event.entityType, event.entityId, JSON.stringify(event.payload)]
        );

        // Mark idempotency key as COMPLETED
        await client.query(
          `UPDATE idempotency_keys SET status = 'COMPLETED', updated_at = NOW() 
           WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
          [tenantId, platform, eventId]
        );

        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK').catch(() => { });
        throw err;
      } finally {
        client.release();
      }

      // 6. Dispatch to Downstream ERP (with Timeout & Idempotency Header)
      const targetUrl = tenant.downstreamUrl;
      if (targetUrl) {
        await this.dispatchToDownstream(targetUrl, event);
      }
      circuitBreaker.recordSuccess();

      // 7. Commit Redis Lock to 'DONE' & Release Semaphore
      await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
      await releaseTenantSemaphore(redis, tenantId, ownerId);

      const durationMs = Date.now() - startTime;
      workerProcessingDurationMs.observe({ platform, outcome: 'success' }, durationMs);

      return { status: 'SUCCESS', eventId, durationMs };
    } catch (err: any) {
      circuitBreaker.recordFailure();
      await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'ABORT_RELEASE');
      await releaseTenantSemaphore(redis, tenantId, ownerId);

      const durationMs = Date.now() - startTime;
      workerProcessingDurationMs.observe({ platform, outcome: 'error' }, durationMs);
      logger.error({ err, tenantId, eventId }, 'Worker processing error occurred');

      return {
        status: 'FAILED_RETRY',
        eventId,
        durationMs,
        error: err.message || String(err)
      };
    }
  }

  private async dispatchToDownstream(url: string, event: VerifiedWebhookEvent): Promise<void> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 4000);

    try {
      const res = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Idempotency-Key': event.eventId,
          'X-Tenant-Id': event.tenantId,
          'X-Platform': event.platform,
        },
        body: JSON.stringify({
          event_id: event.eventId,
          entity_type: event.entityType,
          entity_id: event.entityId,
          event_type: event.eventType,
          current_state: event.currentState,
          payload: event.payload,
        }),
        signal: controller.signal,
      });

      if (!res.ok && res.status >= 500) {
        throw new Error(`Downstream server returned status ${res.status}`);
      }
    } finally {
      clearTimeout(timeout);
    }
  }
}

export const webhookProcessor = new WebhookProcessor();

