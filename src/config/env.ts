import { z } from 'zod';
import dotenv from 'dotenv';

dotenv.config();

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().default(3000),
  HOST: z.string().default('0.0.0.0'),
  
  // PostgreSQL Configuration
  DATABASE_URL: z.string().default('postgresql://postgres:postgres@localhost:5432/webhook_engine'),
  DB_POOL_MAX: z.coerce.number().default(20),
  
  // Redis Configuration
  REDIS_HOST: z.string().default('127.0.0.1'),
  REDIS_PORT: z.coerce.number().default(6379),
  REDIS_PASSWORD: z.string().optional(),
  
  // Ingestion Shards
  STREAM_SHARD_COUNT: z.coerce.number().default(16),
  STREAM_PREFIX: z.string().default('webhooks:stream'),
  
  // Downstream Mock / Service URL
  DOWNSTREAM_URL: z.string().default('http://localhost:4000/downstream'),
  MOCK_DOWNSTREAM_PORT: z.coerce.number().default(4000),
  
  // Safety & Backpressure thresholds
  MAX_STREAM_QUEUE_LENGTH: z.coerce.number().default(50000),
  CIRCUIT_BREAKER_ERROR_THRESHOLD: z.coerce.number().default(50), // 50%
  CIRCUIT_BREAKER_RESET_TIMEOUT_MS: z.coerce.number().default(30000),
});

export type Env = z.infer<typeof envSchema>;

let parsedEnv: Env;

try {
  parsedEnv = envSchema.parse(process.env);
} catch (error) {
  if (error instanceof z.ZodError) {
    console.error('❌ Invalid environment variables:', error.format());
  } else {
    console.error('❌ Environment validation failed:', error);
  }
  process.exit(1);
}

export const env = parsedEnv;

