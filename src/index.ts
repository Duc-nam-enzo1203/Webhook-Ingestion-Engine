import { buildGatewayServer } from './gateway/server.js';
import { StreamConsumerPool } from './worker/consumers/stream.consumer.js';
import { buildMockDownstreamServer } from './mock-downstream/server.js';
import { env } from './config/env.js';
import { logger } from './shared/observability/logger.js';
import { closeRedisConnections } from './shared/db/redis.js';
import { closeDbPool } from './shared/db/postgres.js';

async function bootstrap() {
  logger.info({ env: env.NODE_ENV }, 'Starting Webhook Ingestion Engine Suite...');

  // 1. Start Mock Downstream ERP
  const mockServer = await buildMockDownstreamServer();
  await mockServer.listen({ port: env.MOCK_DOWNSTREAM_PORT, host: env.HOST });
  logger.info(`🏭 Mock ERP active on port ${env.MOCK_DOWNSTREAM_PORT}`);

  // 2. Start Gateway Server
  const gatewayServer = await buildGatewayServer();
  await gatewayServer.listen({ port: env.PORT, host: env.HOST });
  logger.info(`⚡ Webhook Gateway active on port ${env.PORT}`);

  // 3. Start Background Worker Pool
  const consumerPool = new StreamConsumerPool();
  consumerPool.start().catch((err) => {
    logger.error({ err }, 'Worker loop encountered error');
  });

  const shutdown = async () => {
    logger.info('Gracefully shutting down all services...');
    consumerPool.stop();
    await gatewayServer.close();
    await mockServer.close();
    await closeRedisConnections();
    await closeDbPool();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

bootstrap().catch(async (err) => {
  logger.error({ err }, 'Bootstrap fatal error');
  await closeRedisConnections();
  await closeDbPool();
  process.exit(1);
});

