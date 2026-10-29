import Fastify, { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { env } from '../config/env.js';
import { logger } from '../shared/observability/logger.js';

export interface MockDownstreamConfig {
  delayMs?: number;
  failureRate?: number; // 0 to 1
}

export const tenantBehaviors = new Map<string, MockDownstreamConfig>();
export const receivedDownstreamEvents: Array<{
  tenantId: string;
  idempotencyKey: string;
  body: any;
  receivedAt: string;
}> = [];

export async function buildMockDownstreamServer(): Promise<FastifyInstance> {
  const app = Fastify({ logger: false });

  app.get('/health', async () => ({ status: 'mock_downstream_running' }));

  app.get('/events', async () => ({
    total: receivedDownstreamEvents.length,
    events: receivedDownstreamEvents,
  }));

  app.delete('/events', async () => {
    receivedDownstreamEvents.length = 0;
    return { status: 'cleared' };
  });

  app.post('/downstream/:tenantId', async (req: FastifyRequest<{ Params: { tenantId: string } }>, reply: FastifyReply) => {
    const { tenantId } = req.params;
    const idempotencyKey = (req.headers['x-idempotency-key'] as string) || 'missing_key';
    const behavior = tenantBehaviors.get(tenantId) || {};

    if (behavior.delayMs && behavior.delayMs > 0) {
      await new Promise((r) => setTimeout(r, behavior.delayMs));
    }

    if (behavior.failureRate && Math.random() < behavior.failureRate) {
      return reply.code(500).send({
        error: 'Simulated Downstream ERP Failure',
        tenantId,
        idempotencyKey,
      });
    }

    receivedDownstreamEvents.push({
      tenantId,
      idempotencyKey,
      body: req.body,
      receivedAt: new Date().toISOString(),
    });

    return reply.code(200).send({
      status: 'ACKNOWLEDGED_BY_ERP',
      tenantId,
      idempotencyKey,
    });
  });

  return app;
}

if (process.argv[1]?.endsWith('server.ts') || process.argv[1]?.endsWith('server.js')) {
  buildMockDownstreamServer().then((app) => {
    app.listen({ port: env.MOCK_DOWNSTREAM_PORT, host: env.HOST }, (err, address) => {
      if (err) {
        logger.error({ err }, 'Failed to start Mock Downstream ERP');
        process.exit(1);
      }
      logger.info(`🏭 Mock Downstream ERP listening on ${address}`);
    });
  });
}

