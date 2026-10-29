import Fastify, { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import fastifyRawBody from 'fastify-raw-body';
import { env } from '../config/env.js';
import { logger } from '../shared/observability/logger.js';
import { 
  metricsRegistry, 
  gatewayRequestsTotal, 
  gatewayLatencyMs 
} from '../shared/observability/metrics.js';
import { getPlatformAdapter } from './adapters/index.js';
import { RawWebhookRequest } from './adapters/types.js';
import { tenantCache } from '../shared/cache/tenant-cache.js';
import { getRedisClient } from '../shared/db/redis.js';
import { shuffleShardingRouter } from './router/shuffle-sharding.js';
import { checkTenantRateLimit } from './middlewares/rate-limiter.js';
import { backpressureChecker } from './middlewares/backpressure.js';

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
      Math.max(shardSelection.lengthA, shardSelection.lengthB)
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

