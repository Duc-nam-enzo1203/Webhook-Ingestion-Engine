import { Redis } from 'ioredis';
import { env } from '../../config/env.js';
import { logger } from '../observability/logger.js';

let redisClient: Redis | null = null;
let subscriberClient: Redis | null = null;

export function getRedisClient(): Redis {
  if (!redisClient) {
    redisClient = new Redis({
      host: env.REDIS_HOST,
      port: env.REDIS_PORT,
      password: env.REDIS_PASSWORD || undefined,
      lazyConnect: true,
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      retryStrategy(times) {
        return Math.min(times * 100, 3000);
      },
    });

    redisClient.on('error', (err) => {
      logger.error({ err }, 'Redis Client connection error');
    });
  }

  return redisClient;
}

export function getRedisSubscriber(): Redis {
  if (!subscriberClient) {
    subscriberClient = new Redis({
      host: env.REDIS_HOST,
      port: env.REDIS_PORT,
      password: env.REDIS_PASSWORD || undefined,
      lazyConnect: true,
      maxRetriesPerRequest: 3,
    });

    subscriberClient.on('error', (err) => {
      logger.error({ err }, 'Redis Subscriber connection error');
    });
  }

  return subscriberClient;
}

export async function closeRedisConnections(): Promise<void> {
  if (redisClient) {
    await redisClient.quit().catch(() => {});
    redisClient = null;
  }
  if (subscriberClient) {
    await subscriberClient.quit().catch(() => {});
    subscriberClient = null;
  }
}

