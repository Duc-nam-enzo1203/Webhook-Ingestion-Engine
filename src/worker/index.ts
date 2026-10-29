import { StreamConsumerPool } from './consumers/stream.consumer.js';
import { logger } from '../shared/observability/logger.js';
import { closeRedisConnections } from '../shared/db/redis.js';
import { closeDbPool } from '../shared/db/postgres.js';

const consumerPool = new StreamConsumerPool();

async function startWorker() {
  logger.info('🚀 Launching Webhook Worker Engine...');
  await consumerPool.start();
}

startWorker().catch(async (err) => {
  logger.error({ err }, 'Worker process fatal error');
  await closeRedisConnections();
  await closeDbPool();
  process.exit(1);
});

process.on('SIGTERM', async () => {
  logger.info('Shutting down Worker...');
  consumerPool.stop();
  await closeRedisConnections();
  await closeDbPool();
  process.exit(0);
});

