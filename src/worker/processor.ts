import { Redis } from 'ioredis';
import pg from 'pg';
import { env } from '../config/env.js';
import { getDbPool } from '../shared/db/postgres.js';
import { logger } from '../shared/observability/logger.js';
import {
  workerProcessingDurationMs,
  idempotencyHitsTotal,
  outOfOrderResolvedTotal,
  dlqDepth,
} from '../shared/observability/metrics.js';
import { VerifiedWebhookEvent } from '../gateway/adapters/types.js';
import { tenantCache, TenantConfig } from '../shared/cache/tenant-cache.js';
import { acquireTenantSemaphore, releaseTenantSemaphore } from './locks/semaphore.js';
import { acquireIdempotencyLock, finalizeIdempotencyLock } from './locks/idempotency.js';
import { executeOccStateUpsert } from './state-machine/occ-state.js';
import { getTenantCircuitBreaker, calculateExponentialBackoffJitter } from './resilience/circuit-breaker.js';
import { delayedRetryScheduler } from './resilience/delayed-retry.js';

export interface ProcessEventResult {
  status: 'SUCCESS' | 'DUPLICATE' | 'IN_FLIGHT' | 'CIRCUIT_OPEN' | 'SEMAPHORE_FULL' | 'FAILED_RETRY' | 'DLQ' | 'DEFERRED';
  eventId: string;
  durationMs: number;
  reason?: string;
  error?: string;
}

/**
 * Resolves the downstream URL based on execution environment.
 * If running inside Docker (where REDIS_HOST != localhost/127.0.0.1),
 * maps localhost:4000 to mock-erp:4000.
 */
export function resolveDownstreamUrl(dbUrl: string | null | undefined, envOverride?: string): string | null {
  if (dbUrl === null) return null;
  const url = dbUrl || envOverride;
  if (!url) return null;
  if (envOverride && (envOverride.includes('mock-erp') || !envOverride.includes('localhost'))) {
    return url.replace(/localhost:4000|127\.0\.0\.1:4000/g, 'mock-erp:4000');
  }
  const isDocker = env.REDIS_HOST !== '127.0.0.1' && env.REDIS_HOST !== 'localhost';
  if (isDocker) {
    return url.replace(/localhost:4000|127\.0\.0\.1:4000/g, 'mock-erp:4000');
  }
  return url;
}

export class WebhookProcessor {
  private readonly workerId: string;
  private readonly customPool?: pg.Pool;

  constructor(workerId?: string, customPool?: pg.Pool) {
    this.workerId = workerId || `worker_${process.pid}_${Math.random().toString(36).substring(2, 7)}`;
    this.customPool = customPool;
  }

  async processEvent(
    redis: Redis,
    streamKey: string,
    messageId: string,
    rawPayload: { event_id: string; tenant_id: string; platform: string; data: string; defer_count?: number },
    poolOverride?: pg.Pool
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
    const pool = poolOverride || this.customPool || getDbPool();

    // 1. Fetch Tenant Configuration (Distinguish non-existent tenant vs database outage)
    let tenant: TenantConfig | null;
    try {
      tenant = await tenantCache.getTenant(tenantId);
    } catch (err: any) {
      logger.error({ err, tenantId, eventId }, 'Failed to resolve tenant due to DB error, will retry without losing message');
      throw err; // Caught by outer catch to preserve message in PEL and retry with backoff
    }

    if (!tenant) {
      logger.warn({ tenantId, eventId }, 'Skipping event for non-existent tenant');
      return { status: 'SUCCESS', eventId, durationMs: Date.now() - startTime };
    }

    // 2. Per-Tenant Circuit Breaker Check
    const circuitBreaker = getTenantCircuitBreaker(tenantId);
    if (circuitBreaker.getState() === 'OPEN') {
      const deferCount = (rawPayload.defer_count || 0) + 1;
      if (deferCount >= 10) {
        logger.error({ tenantId, eventId, deferCount }, 'Tenant circuit breaker OPEN max attempts reached, moving to DLQ');
        const routed = await this.routeDirectlyToDlq(pool, event, 'CIRCUIT_OPEN_MAX_RETRIES', deferCount);
        if (routed) {
          dlqDepth.inc();
          return { status: 'DLQ', eventId, durationMs: Date.now() - startTime, error: 'CIRCUIT_OPEN_MAX_RETRIES' };
        }
        return { status: 'FAILED_RETRY', eventId, durationMs: Date.now() - startTime, error: 'DB_DOWN_CANNOT_DLQ' };
      }

      logger.warn({ tenantId, eventId, deferCount }, 'Tenant circuit breaker OPEN, deferring via delayed retry');
      await delayedRetryScheduler.schedule(redis, streamKey, { ...rawPayload, defer_count: deferCount }, 5000);
      return { status: 'DEFERRED', eventId, durationMs: Date.now() - startTime, reason: 'CIRCUIT_OPEN' };
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
      const deferCount = (rawPayload.defer_count || 0) + 1;
      if (deferCount >= 10) {
        logger.error({ tenantId, eventId, deferCount }, 'Tenant concurrency saturated max attempts reached, moving to DLQ');
        const routed = await this.routeDirectlyToDlq(pool, event, 'SEMAPHORE_SATURATION_MAX_RETRIES', deferCount);
        if (routed) {
          dlqDepth.inc();
          return { status: 'DLQ', eventId, durationMs: Date.now() - startTime, error: 'SEMAPHORE_SATURATION_MAX_RETRIES' };
        }
        return { status: 'FAILED_RETRY', eventId, durationMs: Date.now() - startTime, error: 'DB_DOWN_CANNOT_DLQ' };
      }

      logger.debug({ tenantId, eventId, deferCount }, 'Tenant semaphore full, deferring');
      await delayedRetryScheduler.schedule(redis, streamKey, { ...rawPayload, defer_count: deferCount }, 1000);
      return { status: 'DEFERRED', eventId, durationMs: Date.now() - startTime, reason: 'SEMAPHORE_FULL' };
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
        await delayedRetryScheduler.schedule(redis, streamKey, rawPayload, 1000);
        return { status: 'DEFERRED', eventId, durationMs: Date.now() - startTime, reason: 'IN_FLIGHT' };
      }

      // 5. Tier-2 PostgreSQL Ingestion & State Resolution Transaction
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

          // If PERSISTED or IN_PROGRESS: Read canonical DB state!
          const stateCheck = await client.query(
            `SELECT current_state, merged_payload FROM entity_states 
             WHERE tenant_id = $1 AND platform = $2 AND entity_type = $3 AND entity_id = $4`,
            [tenantId, platform, event.entityType, event.entityId]
          );

          if (stateCheck.rows.length > 0) {
            resolvedState = stateCheck.rows[0].current_state;
            resolvedPayload = typeof stateCheck.rows[0].merged_payload === 'string'
              ? JSON.parse(stateCheck.rows[0].merged_payload)
              : stateCheck.rows[0].merged_payload;
          }

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
          // First time seeing event: Insert into idempotency_keys as IN_PROGRESS with ON CONFLICT guard
          const insertRes = await client.query(
            `INSERT INTO idempotency_keys (tenant_id, platform, event_id, status, locked_by, locked_until)
             VALUES ($1, $2, $3, 'IN_PROGRESS', $4, NOW() + INTERVAL '30 seconds')
             ON CONFLICT (tenant_id, platform, event_id) DO NOTHING`,
            [tenantId, platform, eventId, this.workerId]
          );

          if (insertRes.rowCount === 0) {
            // Concurrent race won by another worker
            await client.query('ROLLBACK');
            await releaseTenantSemaphore(redis, tenantId, ownerId);
            return { status: 'DUPLICATE', eventId, durationMs: Date.now() - startTime };
          }

          // Execute OCC State Machine & Bidirectional JSONB Hydration Upsert
          const occResult = await executeOccStateUpsert(client, event);
          resolvedState = occResult.currentState;
          resolvedPayload = occResult.mergedPayload;

          // Late Hydration Resolution:
          // isNewlyHydrated is true ONLY if this entity was unhydrated prior to this update.
          if (occResult.isNewlyHydrated) {
            outOfOrderResolvedTotal.inc({ resolution: 'hydrated_late_create' });
            deliveryNeeded = true;
          } else if (occResult.isStaleIgnored) {
            outOfOrderResolvedTotal.inc({ resolution: 'ignored_stale_update' });
            deliveryNeeded = false; // Pure duplicate/stale event with no new info
          } else {
            outOfOrderResolvedTotal.inc({ resolution: 'normal_progression' });
            deliveryNeeded = true;
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
        } catch { }
        throw dbErr;
      } finally {
        client.release();
      }

      // If this was a stale update (e.g. PAID arrived after CANCELLED without new hydration), do not dispatch
      if (!deliveryNeeded) {
        await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'COMMIT_DONE');
        await releaseTenantSemaphore(redis, tenantId, ownerId);
        return { status: 'SUCCESS', eventId, durationMs: Date.now() - startTime };
      }

      // 6. Dispatch to Downstream ERP with merged state & timeout
      const targetUrl = resolveDownstreamUrl(tenant.downstreamUrl, env.DOWNSTREAM_URL);
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

        await updateClient.query(
          `UPDATE idempotency_keys 
           SET updated_at = NOW() 
           WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
          [tenantId, platform, eventId]
        );

        // Schedule delayed retry in Redis ZSET
        await delayedRetryScheduler.schedule(redis, streamKey, rawPayload, delayMs);

        // Release lease lock so next attempt can acquire
        await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'ABORT_RELEASE');
        await releaseTenantSemaphore(redis, tenantId, ownerId);

        logger.warn({ tenantId, eventId, retryCount: newRetryCount, delayMs }, 'Downstream delivery failed, scheduled retry');
        return {
          status: 'DEFERRED',
          eventId,
          durationMs: Date.now() - startTime,
          reason: 'FAILED_RETRY',
          error: dispatchError.message,
        };
      } finally {
        updateClient.release();
      }
    } catch (err: any) {
      await finalizeIdempotencyLock(redis, tenantId, platform, eventId, this.workerId, 'ABORT_RELEASE');
      await releaseTenantSemaphore(redis, tenantId, ownerId);

      const durationMs = Date.now() - startTime;
      workerProcessingDurationMs.observe({ platform, outcome: 'error' }, durationMs);
      logger.error({ err, tenantId, eventId }, 'Worker processing error occurred');

      const deferCount = (rawPayload.defer_count || 0) + 1;
      if (deferCount >= 5) {
        logger.error({ err, tenantId, eventId, deferCount }, 'Fatal system/DB error exceeded max attempts, attempting DLQ routing');
        const routed = await this.routeDirectlyToDlq(pool, event, `SYSTEM_ERROR: ${err.message || String(err)}`, deferCount);
        if (routed) {
          dlqDepth.inc();
          return { status: 'DLQ', eventId, durationMs, error: err.message || String(err) };
        }
        // If Postgres is down, recording to DLQ failed. DO NOT ACK!
        // Return FAILED_RETRY so stream.consumer does NOT ack and message is retained in PEL.
        logger.error({ tenantId, eventId }, 'Postgres outage prevented DLQ persistence, retaining message in stream PEL');
        return { status: 'FAILED_RETRY', eventId, durationMs, error: 'DB_DOWN_CANNOT_DLQ' };
      }

      // For DB / infrastructure outages, backoff exponentially
      const retryDelayMs = Math.min(30000, 3000 * Math.pow(2, deferCount - 1));
      await delayedRetryScheduler.schedule(redis, streamKey, { ...rawPayload, defer_count: deferCount }, retryDelayMs);
      return {
        status: 'DEFERRED',
        eventId,
        durationMs,
        reason: 'SYSTEM_ERROR',
        error: err.message || String(err),
      };
    }
  }

  /**
   * Directly routes an event to DLQ with an atomic upsert into PostgreSQL.
   * Guarantees zero data loss even if the event was deferred before the main DB transaction.
   * Returns true if successfully committed to PostgreSQL, false otherwise.
   */
  private async routeDirectlyToDlq(
    pool: pg.Pool,
    event: VerifiedWebhookEvent,
    errorMessage: string,
    retryCount = 0
  ): Promise<boolean> {
    const { tenantId, platform, eventId, entityType, entityId, payload } = event;
    try {
      const client = await pool.connect();
      try {
        await client.query(
          `INSERT INTO idempotency_keys (tenant_id, platform, event_id, status, locked_by, updated_at)
           VALUES ($1, $2, $3, 'DLQ', $4, NOW())
           ON CONFLICT (tenant_id, platform, event_id)
           DO UPDATE SET status = 'DLQ', updated_at = NOW()`,
          [tenantId, platform, eventId, this.workerId]
        );

        await client.query(
          `INSERT INTO webhook_events (tenant_id, platform, event_id, entity_type, entity_id, delivery_status, retry_count, raw_payload, last_error, created_at)
           VALUES ($1, $2, $3, $4, $5, 'DLQ', $6, $7, $8, NOW())`,
          [tenantId, platform, eventId, entityType, entityId, retryCount, JSON.stringify(payload), errorMessage]
        );
        return true;
      } finally {
        client.release();
      }
    } catch (err) {
      logger.error({ err, tenantId, eventId }, 'Failed to record direct DLQ routing');
      return false;
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
