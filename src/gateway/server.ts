import Fastify, { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import fastifyRawBody from 'fastify-raw-body';
import crypto from 'node:crypto';
import { env } from '../config/env.js';
import { logger } from '../shared/observability/logger.js';
import {
  metricsRegistry,
  gatewayRequestsTotal,
  gatewayLatencyMs
} from '../shared/observability/metrics.js';
import { getPlatformAdapter } from './adapters/index.js';
import { RawWebhookRequest, VerifiedWebhookEvent } from './adapters/types.js';
import { tenantCache } from '../shared/cache/tenant-cache.js';
import { getRedisClient } from '../shared/db/redis.js';
import { getDbPool } from '../shared/db/postgres.js';
import { shuffleShardingRouter } from './router/shuffle-sharding.js';
import { checkTenantRateLimit } from './middlewares/rate-limiter.js';
import { backpressureChecker } from './middlewares/backpressure.js';
import { getIdempotencyRedisKey } from '../worker/locks/idempotency.js';

export async function buildGatewayServer(): Promise<FastifyInstance> {
  const server = Fastify({
    logger: false, // Managed via Pino directly
    trustProxy: true,
  });

  // Preserve raw buffer for accurate HMAC calculation
  await server.register(fastifyRawBody, {
    field: 'rawBody',
    global: true,
    encoding: false, // Keep as Buffer
    runFirst: true,
  });

  // Initialize tenant pub/sub listener for L1 cache invalidation
  await tenantCache.initPubSub();

  // Health Check
  server.get('/health', async () => {
    return {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      uptimeSeconds: Math.floor(process.uptime()),
    };
  });

  // Prometheus Metrics
  server.get('/metrics', async (_req: FastifyRequest, reply: FastifyReply) => {
    reply.header('Content-Type', metricsRegistry.contentType);
    return reply.send(await metricsRegistry.metrics());
  });

  // Admin Authentication PreHandler with Timing Attack Protection
  const adminAuthPreHandler = async (req: FastifyRequest, reply: FastifyReply) => {
    const adminKey = (req.headers['x-admin-key'] as string) || (req.headers.authorization?.replace(/^Bearer\s+/i, ''));
    if (!adminKey) {
      return reply.code(401).send({ error: 'Unauthorized: Missing admin authentication key' });
    }
    const expectedKeyBuffer = Buffer.from(env.ADMIN_API_KEY);
    const actualKeyBuffer = Buffer.from(adminKey);
    if (
      expectedKeyBuffer.length !== actualKeyBuffer.length ||
      !crypto.timingSafeEqual(expectedKeyBuffer, actualKeyBuffer)
    ) {
      return reply.code(401).send({ error: 'Unauthorized: Invalid admin authentication key' });
    }
  };

  // DLQ Admin: Inspect dead-lettered events (Protected)
  server.get<{ Querystring: { tenant_id?: string; limit?: string; include_payload?: string } }>(
    '/admin/dlq',
    { preHandler: adminAuthPreHandler },
    async (req, reply) => {
      try {
        const pool = getDbPool();
        const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);
        const tenantId = req.query.tenant_id;
        const includePayload = req.query.include_payload === 'true';

        const queryFields = includePayload
          ? 'tenant_id, platform, event_id, entity_type, entity_id, retry_count, last_error, raw_payload, created_at'
          : 'tenant_id, platform, event_id, entity_type, entity_id, retry_count, last_error, created_at';

        let res;
        if (tenantId) {
          res = await pool.query(
            `SELECT ${queryFields} FROM webhook_events 
             WHERE delivery_status = 'DLQ' AND tenant_id = $1
             ORDER BY created_at DESC LIMIT $2`,
            [tenantId, limit]
          );
        } else {
          res = await pool.query(
            `SELECT ${queryFields} FROM webhook_events 
             WHERE delivery_status = 'DLQ' 
             ORDER BY created_at DESC LIMIT $1`,
            [limit]
          );
        }
        return reply.send({ total: res.rows.length, dlq_events: res.rows });
      } catch (err: any) {
        return reply.code(500).send({ error: err.message });
      }
    }
  );

  // DLQ Admin: Replay dead-lettered event (Protected)
  server.post<{
    Params: { eventId: string };
    Querystring: { tenant_id?: string; platform?: string };
  }>(
    '/admin/dlq/:eventId/replay',
    { preHandler: adminAuthPreHandler },
    async (req, reply) => {
      const { eventId } = req.params;
      const { tenant_id: queryTenantId, platform: queryPlatform } = req.query;

      try {
        const pool = getDbPool();

        // 1. Fetch event from webhook_events
        let query = `SELECT tenant_id, platform, event_id, raw_payload, entity_type, entity_id
                     FROM webhook_events 
                     WHERE event_id = $1 AND delivery_status = 'DLQ'`;
        const params: any[] = [eventId];

        if (queryTenantId) {
          params.push(queryTenantId);
          query += ` AND tenant_id = $${params.length}`;
        }
        if (queryPlatform) {
          params.push(queryPlatform);
          query += ` AND platform = $${params.length}`;
        }
        query += ` LIMIT 1`;

        const res = await pool.query(query, params);

        if (res.rows.length === 0) {
          return reply.code(404).send({ error: `Event ${eventId} not found in DLQ` });
        }

        const event = res.rows[0];
        const { tenant_id: tenantId, platform, entity_type: entityType, entity_id: entityId } = event;

        // 2. Fetch canonical state from entity_states (so ERP receives true current_state and payload, NOT 'REPLAY')
        let currentState = 'CREATED';
        let stateWeight = 10;
        let isHydrated = true;
        let payload = typeof event.raw_payload === 'string' ? JSON.parse(event.raw_payload) : event.raw_payload;

        const stateRes = await pool.query(
          `SELECT current_state, state_weight, is_hydrated, merged_payload 
           FROM entity_states 
           WHERE tenant_id = $1 AND platform = $2 AND entity_type = $3 AND entity_id = $4`,
          [tenantId, platform, entityType, entityId]
        );

        if (stateRes.rows.length > 0) {
          currentState = stateRes.rows[0].current_state;
          stateWeight = stateRes.rows[0].state_weight;
          isHydrated = Boolean(stateRes.rows[0].is_hydrated);
          payload = typeof stateRes.rows[0].merged_payload === 'string'
            ? JSON.parse(stateRes.rows[0].merged_payload)
            : stateRes.rows[0].merged_payload;
        }

        // 3. Atomically update Postgres webhook_events and idempotency_keys strictly by (tenant_id, platform, event_id)
        await pool.query(
          `UPDATE webhook_events 
           SET delivery_status = 'QUEUED', retry_count = 0, last_error = NULL 
           WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
          [tenantId, platform, eventId]
        );

        await pool.query(
          `UPDATE idempotency_keys 
           SET status = 'PERSISTED', updated_at = NOW() 
           WHERE tenant_id = $1 AND platform = $2 AND event_id = $3`,
          [tenantId, platform, eventId]
        );

        // 4. CRITICAL FIX: Delete Redis Fast-Path Idempotency Lock using canonical key with hash tags!
        // Previously, missing '{tenantId}' hash tags caused DEL to delete wrong key, leaving DONE lock active!
        const redis = getRedisClient();
        const lockKey = getIdempotencyRedisKey(tenantId, platform, eventId);
        await redis.del(lockKey);

        // 5. Select destination shard via Shuffle Sharding
        const shardSelection = await shuffleShardingRouter.selectShard(tenantId, redis);

        const verifiedEvent: VerifiedWebhookEvent = {
          tenantId,
          platform,
          eventId,
          entityType,
          entityId,
          eventType: `${entityType.toLowerCase()}/replay`,
          currentState,
          stateWeight,
          externalVersion: Date.now(),
          isHydrated,
          payload,
          rawBody: JSON.stringify(payload),
          receivedAt: new Date().toISOString(),
        };

        await redis.xadd(
          shardSelection.selectedShard,
          '*',
          'event_id', eventId,
          'tenant_id', tenantId,
          'platform', platform,
          'data', JSON.stringify(verifiedEvent)
        );

        return reply.send({
          status: 'REPLAYED',
          event_id: eventId,
          tenant_id: tenantId,
          platform,
          shard: shardSelection.selectedShard,
          current_state: currentState,
        });
      } catch (err: any) {
        return reply.code(500).send({ error: err.message });
      }
    }
  );

  // Main Webhook Ingestion Gateway Endpoint
  server.post('/webhooks/:platform', async (req: FastifyRequest<{ Params: { platform: string }; Querystring: { tenant_id?: string } }>, reply: FastifyReply) => {
    const startTime = Date.now();
    const { platform } = req.params;
    const adapter = getPlatformAdapter(platform);

    if (!adapter) {
      gatewayRequestsTotal.inc({ platform, status: '404' });
      return reply.code(404).send({ error: `Unsupported webhook platform: ${platform}` });
    }

    // Extract tenant_id from Header or Query parameter
    const tenantId =
      (req.headers['x-tenant-id'] as string) ||
      req.query.tenant_id;

    if (!tenantId) {
      gatewayRequestsTotal.inc({ platform, status: '400_missing_tenant' });
      return reply.code(400).send({ error: 'Missing tenant identifier (x-tenant-id header or tenant_id query)' });
    }

    // 1. Multi-Tier Cache lookup (L1 -> L2 -> DB)
    const tenant = await tenantCache.getTenant(tenantId);
    if (!tenant || !tenant.isActive) {
      gatewayRequestsTotal.inc({ platform, status: '403_inactive_tenant' });
      return reply.code(403).send({ error: 'Tenant is unknown or inactive' });
    }

    if (tenant.platform.toLowerCase() !== platform.toLowerCase()) {
      gatewayRequestsTotal.inc({ platform, status: '400_platform_mismatch' });
      return reply.code(400).send({ error: `Tenant is configured for platform '${tenant.platform}', not '${platform}'` });
    }

    // Prepare raw request structure
    const rawBodyBuffer = (req.rawBody as Buffer) || Buffer.from('');
    const rawReq: RawWebhookRequest = {
      platform,
      tenantId,
      headers: req.headers as Record<string, string | undefined>,
      query: (req.query || {}) as Record<string, any>,
      rawBody: rawBodyBuffer,
      path: req.url.split('?')[0],
      url: req.url,
    };

    // 2. Cryptographic HMAC Verification via Adapter
    const isSignatureValid = await adapter.verifySignature(rawReq, tenant.webhookSecret);
    if (!isSignatureValid) {
      gatewayRequestsTotal.inc({ platform, status: '401_invalid_signature' });
      return reply.code(401).send({ error: 'Invalid HMAC signature or replay protection failed' });
    }

    const redis = getRedisClient();

    // 3. Per-Tenant Token Bucket Rate Limiting
    const rateLimit = await checkTenantRateLimit(redis, tenant.id, tenant.rateLimitRps);
    if (!rateLimit.allowed) {
      gatewayRequestsTotal.inc({ platform, status: '429_rate_limited' });
      reply.header('Retry-After', String(rateLimit.retryAfterSec));
      return reply.code(429).send({
        error: 'Rate limit exceeded for tenant',
        retry_after_seconds: rateLimit.retryAfterSec,
      });
    }

    // 4. Shuffle Sharding Router: Pick shorter of two candidate shards
    const shardSelection = await shuffleShardingRouter.selectShard(tenant.id, redis);

    // 5. Active Backpressure Check (prevents OOM & blind MAXLEN data drops)
    const backpressure = await backpressureChecker.checkBackpressure(
      redis,
      shardSelection.selectedShard
    );
    if (backpressure.overloaded) {
      gatewayRequestsTotal.inc({ platform, status: '503_backpressure' });
      reply.header('Retry-After', String(backpressure.retryAfterSec));
      return reply.code(503).send({
        error: 'Ingestion engine experiencing backpressure',
        reason: backpressure.reason,
      });
    }

    // 6. Extract Normalized Event
    const verifiedEvent = await adapter.extractEvent(rawReq);

    // 7. Fast Ack Producer: Append to Redis Stream Shard
    try {
      await redis.xadd(
        shardSelection.selectedShard,
        '*',
        'event_id', verifiedEvent.eventId,
        'tenant_id', verifiedEvent.tenantId,
        'platform', verifiedEvent.platform,
        'data', JSON.stringify(verifiedEvent)
      );

      const elapsedMs = Date.now() - startTime;
      gatewayLatencyMs.observe({ platform }, elapsedMs);
      gatewayRequestsTotal.inc({ platform, status: '202' });

      return reply.code(202).send({
        status: 'ACCEPTED',
        event_id: verifiedEvent.eventId,
        entity_id: verifiedEvent.entityId,
        shard: shardSelection.selectedShard,
        latency_ms: elapsedMs,
      });
    } catch (err) {
      logger.error({ err, tenantId, eventId: verifiedEvent.eventId }, 'Failed to append to Redis Stream');
      gatewayRequestsTotal.inc({ platform, status: '500_stream_error' });
      return reply.code(500).send({ error: 'Internal broker queue failure' });
    }
  });

  return server;
}

// Start standalone if executed directly
if (process.argv[1]?.endsWith('server.ts') || process.argv[1]?.endsWith('server.js')) {
  buildGatewayServer().then((app) => {
    app.listen({ port: env.PORT, host: env.HOST }, (err, address) => {
      if (err) {
        logger.error({ err }, 'Failed to start Webhook Gateway');
        process.exit(1);
      }
      logger.info(`🚀 Webhook Ingestion Gateway listening on ${address}`);
    });
  });
}

