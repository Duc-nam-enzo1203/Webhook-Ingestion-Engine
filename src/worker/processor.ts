import { Redis } from 'ioredis';
import { getDbPool } from '../shared/db/postgres.js';
import { logger } from '../shared/observability/logger.js';
import {
  workerProcessingDurationMs,
  idempotencyHitsTotal,
  outOfOrderResolvedTotal,
  dlqDepth,
} from '../shared/observability/metrics.js';
import { VerifiedWebhookEvent } from '../gateway/adapters/types.js';
import { tenantCache } from '../shared/cache/tenant-cache.js';
import { acquireTenantSemaphore, releaseTenantSemaphore } from './locks/semaphore.js';
import { acquireIdempotencyLock, finalizeIdempotencyLock } from './locks/idempotency.js';
import { executeOccStateUpsert } from './state-machine/occ-state.js';
import { getTenantCircuitBreaker, calculateExponentialBackoffJitter } from './resilience/circuit-breaker.js';
import { delayedRetryScheduler } from './resilience/delayed-retry.js';

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
    let event: VerifiedWebhookEvent;
    try {
      event = JSON.parse(rawPayload.data);
    } catch (e: any) {
      logger.error({ err: e }, 'Corrupted JSON in stream message, routing to DLQ');
      dlqDepth.inc();
      return { status: 'DLQ', eventId: rawPayload.event_id, durationMs: Date.now() - startTime, error: 'MALFORMED_JSON' };
    }

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
      logger.warn({ tenantId, eventId }, 'Tenant circuit breaker OPEN, deferring via delayed retry');
      await delayedRetryScheduler.schedule(redis, streamKey, rawPayload, 5000);
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
      // Concurrency limit reached for this tenant; defer briefly (1s) rather than stalling PEL for 30s
      await delayedRetryScheduler.schedule(redis, streamKey, rawPayload, 1000);
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

      // 5. Tier-2 PostgreSQL Ingestion & State Resolution Transaction
      const pool = getDbPool();
      const client = await pool.connect();

      let deliveryNeeded = true;
      let existingRetryCount = 0;
      let resolvedState = event.currentState;
      let resolvedPayload = event.payload;

      try {
        await client.query('BEGIN');

        // Check if event already exists in idempotency_keys
        const idempCheck = await client.query(
          `SELECT status FROM idempotency_keys WHERE tenant_id = $1 AND platform = $2 AND event_id = $3 FOR UPDATE`,
          [tenantId, platform, eventId]
        );

        if (idempCheck.rows.length > 0) {
          const currentStatus = idempCheck.rows[0].status;

          // If ALREADY DELIVERED to downstream, this is a true duplicate:
          if (currentStatus === 'COMPLETED') {
            await client.query('ROLLBACK');
            idempotencyHitsTotal.inc({ tier: 'postgres_conflict' });
            await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
            await releaseTenantSemaphore(redis, tenantId, ownerId);
            return { status: 'DUPLICATE', eventId, durationMs: Date.now() - startTime };
          }

          if (currentStatus === 'DLQ') {
            // Already dead-lettered, don't re-dispatch automatically
            await client.query('ROLLBACK');
            await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
            await releaseTenantSemaphore(redis, tenantId, ownerId);
            return { status: 'DLQ', eventId, durationMs: Date.now() - startTime };
          }

          // If PERSISTED or IN_PROGRESS from previous crashed attempt: Fetch retry_count
          const eventCheck = await client.query(
            `SELECT retry_count, delivery_status FROM webhook_events 
             WHERE tenant_id = $1 AND platform = $2 AND event_id = $3 
             ORDER BY created_at DESC LIMIT 1`,
            [tenantId, platform, eventId]
          );

          if (eventCheck.rows.length > 0) {
            existingRetryCount = eventCheck.rows[0].retry_count;
            if (eventCheck.rows[0].delivery_status === 'DELIVERED') {
              await client.query('ROLLBACK');
              await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
              await releaseTenantSemaphore(redis, tenantId, ownerId);
              return { status: 'DUPLICATE', eventId, durationMs: Date.now() - startTime };
            }
          }
        } else {
          // First time seeing event: Insert into idempotency_keys as IN_PROGRESS
          await client.query(
            `INSERT INTO idempotency_keys (tenant_id, platform, event_id, status, locked_by, locked_until)
             VALUES ($1, $2, $3, 'IN_PROGRESS', $4, NOW() + INTERVAL '30 seconds')`,
            [tenantId, platform, eventId, this.workerId]
          );

          // Execute OCC State Machine & Bidirectional JSONB Hydration Upsert
          const occResult = await executeOccStateUpsert(client, event);
          resolvedState = occResult.currentState;
          resolvedPayload = occResult.mergedPayload;

          if (occResult.isStaleIgnored) {
            outOfOrderResolvedTotal.inc({ resolution: 'ignored_stale_update' });
            deliveryNeeded = false; // Stale updates must NOT be dispatched downstream!
          } else if (occResult.isHydrated && !occResult.isInserted) {
            outOfOrderResolvedTotal.inc({ resolution: 'hydrated_late_create' });
          } else {
            outOfOrderResolvedTotal.inc({ resolution: 'normal_progression' });
          }

          // Append to webhook_events audit log
          await client.query(
            `INSERT INTO webhook_events (tenant_id, platform, event_id, entity_type, entity_id, delivery_status, raw_payload)
             VALUES ($1, $2, $3, $4, $5, $6, $7)`,
            [tenantId, platform, eventId, event.entityType, event.entityId, deliveryNeeded ? 'QUEUED' : 'DELIVERED', JSON.stringify(event.payload)]
          );

          // If delivery is needed, mark PERSISTED (data is in DB, but ERP not yet notified)
          // If stale, mark COMPLETED immediately
          await client.query(
            `UPDATE idempotency_keys SET status = $1, updated_at = NOW() 
             WHERE tenant_id = $2 AND platform = $3 AND event_id = $4`,
            [deliveryNeeded ? 'PERSISTED' : 'COMPLETED', tenantId, platform, eventId]
          );
        }

        await client.query('COMMIT');
      } catch (dbErr) {
        try {
          await client.query('ROLLBACK');
        } catch {}
        throw dbErr;
      } finally {
        client.release();
      }

      // If this was a stale update (e.g. PAID arrived after CANCELLED), do not dispatch
      if (!deliveryNeeded) {
        await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
        await releaseTenantSemaphore(redis, tenantId, ownerId);
        return { status: 'SUCCESS', eventId, durationMs: Date.now() - startTime };
      }

      // 6. Dispatch to Downstream ERP with merged state & timeout
      const targetUrl = tenant.downstreamUrl;
      let dispatchError: { isClientError: boolean; status?: number; message: string } | null = null;

      if (targetUrl) {
        try {
          await this.dispatchToDownstream(targetUrl, {
            ...event,
            currentState: resolvedState,
            payload: resolvedPayload,
          });
          circuitBreaker.recordSuccess();
        } catch (err: any) {
          dispatchError = {
            isClientError: Boolean(err.isClientError),
            status: err.status,
            message: err.message || String(err),
          };
          circuitBreaker.recordFailure();
        }
      }

      // 7. Update Delivery Status in PostgreSQL
      const updateClient = await pool.connect();
      try {
        if (!dispatchError) {
          // Successful delivery: Mark COMPLETED in DB and finalize lock
          await updateClient.query(
            `UPDATE webhook_events 
             SET delivery_status = 'DELIVERED', delivered_at = NOW(), last_error = NULL
             WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
            [tenantId, platform, eventId]
          );

          await updateClient.query(
            `UPDATE idempotency_keys 
             SET status = 'COMPLETED', updated_at = NOW() 
             WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
            [tenantId, platform, eventId]
          );

          await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
          await releaseTenantSemaphore(redis, tenantId, ownerId);

          const durationMs = Date.now() - startTime;
          workerProcessingDurationMs.observe({ platform, outcome: 'success' }, durationMs);
          return { status: 'SUCCESS', eventId, durationMs };
        }

        // Delivery failed: Check if 4xx (permanent client error) or retry count exceeded
        const newRetryCount = existingRetryCount + 1;
        const shouldRouteToDlq = dispatchError.isClientError || newRetryCount >= 5;

        if (shouldRouteToDlq) {
          // Route to Dead-Letter Queue
          await updateClient.query(
            `UPDATE webhook_events 
             SET delivery_status = 'DLQ', retry_count = $1, last_error = $2
             WHERE tenant_id = $3 AND platform = $4 AND event_id = $5`,
            [newRetryCount, dispatchError.message, tenantId, platform, eventId]
          );

          await updateClient.query(
            `UPDATE idempotency_keys 
             SET status = 'DLQ', updated_at = NOW() 
             WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
            [tenantId, platform, eventId]
          );

          dlqDepth.inc();
          await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
          await releaseTenantSemaphore(redis, tenantId, ownerId);

          logger.error({ tenantId, eventId, retryCount: newRetryCount, err: dispatchError.message }, 'Event moved to DLQ');
          return { status: 'DLQ', eventId, durationMs: Date.now() - startTime, error: dispatchError.message };
        }

        // Retry with Exponential Backoff + Full Jitter
        const delayMs = calculateExponentialBackoffJitter(newRetryCount, 1000, 30000);
        await updateClient.query(
          `UPDATE webhook_events 
           SET delivery_status = 'RETRYING', retry_count = $1, last_error = $2
           WHERE tenant_id = $3 AND platform = $4 AND event_id = $5`,
          [newRetryCount, dispatchError.message, tenantId, platform, eventId]
        );

        // Schedule delayed retry in Redis ZSET
        await delayedRetryScheduler.schedule(redis, streamKey, rawPayload, delayMs);

        // Release lease lock so next attempt can acquire
        await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'ABORT_RELEASE');
        await releaseTenantSemaphore(redis, tenantId, ownerId);

        logger.warn({ tenantId, eventId, retryCount: newRetryCount, delayMs }, 'Downstream delivery failed, scheduled retry');
        return { status: 'FAILED_RETRY', eventId, durationMs: Date.now() - startTime, error: dispatchError.message };
      } finally {
        updateClient.release();
      }
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
        error: err.message || String(err),
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

      if (!res.ok) {
        const errorText = await res.text().catch(() => '');
        const isClient = res.status >= 400 && res.status < 500;
        const error: any = new Error(`Downstream returned HTTP ${res.status}: ${errorText.slice(0, 100)}`);
        error.status = res.status;
        error.isClientError = isClient;
        throw error;
      }
    } catch (err: any) {
      if (err.name === 'AbortError') {
        const timeoutError: any = new Error('Downstream request timed out after 4000ms');
        timeoutError.isClientError = false;
        throw timeoutError;
      }
      throw err;
    } finally {
      clearTimeout(timeout);
    }
  }
}

export const webhookProcessor = new WebhookProcessor();
