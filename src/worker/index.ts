import http from 'http';
import { StreamConsumerPool } from './consumers/stream.consumer.js';
import { logger } from '../shared/observability/logger.js';
import { closeRedisConnections } from '../shared/db/redis.js';
import { closeDbPool } from '../shared/db/postgres.js';
import { metricsRegistry } from '../shared/observability/metrics.js';
import { env } from '../config/env.js';

const consumerPool = new StreamConsumerPool();

const metricsPort = env.WORKER_METRICS_PORT || 3002;
const metricsServer = http.createServer(async (req, res) => {
  if (req.url === '/metrics' && req.method === 'GET') {
    try {
      res.setHeader('Content-Type', metricsRegistry.contentType);
      const metrics = await metricsRegistry.metrics();
      res.writeHead(200);
      res.end(metrics);
    } catch (err: any) {
      res.writeHead(500);
      res.end(err.message || 'Metrics extraction failed');
    }
  } else if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', role: 'worker' }));
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

async function startWorker() {
  metricsServer.listen(metricsPort, '0.0.0.0', () => {
    logger.info({ port: metricsPort }, '📊 Worker Prometheus metrics server listening on /metrics');
  });

  logger.info('🚀 Launching Webhook Worker Engine...');
  await consumerPool.start();
}

startWorker().catch(async (err) => {
  logger.error({ err }, 'Worker process fatal error');
  metricsServer.close();
  await closeRedisConnections();
  await closeDbPool();
  process.exit(1);
});

process.on('SIGTERM', async () => {
  logger.info('Shutting down Worker...');
  metricsServer.close();
  consumerPool.stop();
  await closeRedisConnections();
  await closeDbPool();
  process.exit(0);
});

