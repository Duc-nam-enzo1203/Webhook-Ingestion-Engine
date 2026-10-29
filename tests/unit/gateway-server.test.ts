import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { FastifyInstance } from 'fastify';
import { tenantCache } from '../../src/shared/cache/tenant-cache.js';
import { calculateHmac } from '../../src/gateway/adapters/crypto-utils.js';

// Mock Redis client for Unit Testing
const mockRedis = {
  eval: vi.fn().mockResolvedValue([1, 100, 0]),
  xlen: vi.fn().mockResolvedValue(10),
  info: vi.fn().mockResolvedValue('used_memory:1048576\r\nmaxmemory:104857600'),
  xadd: vi.fn().mockResolvedValue('1761720000000-0'),
  subscribe: vi.fn().mockResolvedValue(undefined),
  on: vi.fn(),
  get: vi.fn().mockResolvedValue(null),
  set: vi.fn().mockResolvedValue('OK'),
  del: vi.fn().mockResolvedValue(1),
  publish: vi.fn().mockResolvedValue(1),
};

vi.mock('../../src/shared/db/redis.js', () => ({
  getRedisClient: () => mockRedis,
  getRedisSubscriber: () => mockRedis,
  closeRedisConnections: vi.fn(),
}));

// Import server builder after mocking Redis
const { buildGatewayServer } = await import('../../src/gateway/server.js');

describe('Gateway HTTP Ingestion Server', () => {
  let app: FastifyInstance;

  const mockTenant = {
    id: 'a0000000-0000-0000-0000-000000000001',
    name: 'Test Shopify Store',
    platform: 'shopify' as const,
    webhookSecret: 'test_sec_key_123',
    rateLimitRps: 100,
    maxConcurrency: 10,
    downstreamUrl: 'http://localhost:4000/downstream',
    isActive: true,
  };

  beforeAll(async () => {
    app = await buildGatewayServer();
    // Prime the L1 tenant cache
    tenantCache.setTenantInL1(mockTenant);
  });

  afterAll(async () => {
    await app.close();
  });

  it('GET /health returns 200 and healthy status', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/health',
    });

    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.status).toBe('healthy');
    expect(body.uptimeSeconds).toBeDefined();
  });

  it('GET /metrics returns Prometheus formatted text', async () => {
    const res = await app.inject({
      method: 'GET',
      url: '/metrics',
    });

    expect(res.statusCode).toBe(200);
    expect(res.payload).toContain('webhook_gateway_requests_total');
  });

  it('POST /webhooks/unsupported returns 404', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/webhooks/unsupported',
    });

    expect(res.statusCode).toBe(404);
  });

  it('POST /webhooks/shopify without tenant returns 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/webhooks/shopify',
      headers: {
        'content-type': 'application/json',
      },
      payload: JSON.stringify({ id: 1 }),
    });

    expect(res.statusCode).toBe(400);
    expect(res.json().error).toContain('Missing tenant identifier');
  });

  it('POST /webhooks/shopify with invalid HMAC signature returns 401', async () => {
    const payload = JSON.stringify({ id: 1001, title: 'Item' });

    const res = await app.inject({
      method: 'POST',
      url: '/webhooks/shopify',
      headers: {
        'content-type': 'application/json',
        'x-tenant-id': mockTenant.id,
        'x-shopify-hmac-sha256': 'invalid_signature==',
        'x-shopify-topic': 'orders/create',
      },
      payload,
    });

    expect(res.statusCode).toBe(401);
    expect(res.json().error).toContain('Invalid HMAC signature');
  });

  it('POST /webhooks/shopify with valid signature returns 202 Accepted', async () => {
    const payload = JSON.stringify({
      id: 991823,
      line_items: [{ id: 1, name: 'Product A' }],
      created_at: '2026-10-29T08:00:00Z',
    });

    const rawBuffer = Buffer.from(payload, 'utf8');
    const validSignature = calculateHmac(rawBuffer, mockTenant.webhookSecret, 'base64');

    const res = await app.inject({
      method: 'POST',
      url: '/webhooks/shopify',
      headers: {
        'content-type': 'application/json',
        'x-tenant-id': mockTenant.id,
        'x-shopify-hmac-sha256': validSignature,
        'x-shopify-topic': 'orders/create',
        'x-shopify-webhook-id': 'wh_test_inject_001',
      },
      payload,
    });

    expect(res.statusCode).toBe(202);
    const body = res.json();
    expect(body.status).toBe('ACCEPTED');
    expect(body.event_id).toBe('wh_test_inject_001');
    expect(mockRedis.xadd).toHaveBeenCalled();
  });
});
