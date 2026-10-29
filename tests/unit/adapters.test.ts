import { describe, it, expect } from 'vitest';
import crypto from 'node:crypto';
import { 
  getPlatformAdapter, 
  ShopifyAdapter, 
  TikTokAdapter, 
  ShopeeAdapter,
  timingSafeEqualString,
  calculateHmac 
} from '../../src/gateway/adapters/index.js';
import { RawWebhookRequest } from '../../src/gateway/adapters/types.js';

describe('Crypto Utils', () => {
  it('timingSafeEqualString correctly verifies equal and unequal strings', () => {
    expect(timingSafeEqualString('secret_abc', 'secret_abc')).toBe(true);
    expect(timingSafeEqualString('secret_abc', 'secret_xyz')).toBe(false);
    expect(timingSafeEqualString('short', 'longer_string')).toBe(false);
  });
});

describe('Shopify Adapter', () => {
  const adapter = new ShopifyAdapter();
  const secret = 'shpss_sec_secret_alpha_key_9912';

  it('successfully verifies valid Shopify HMAC-SHA256 signature in Base64', async () => {
    const payload = JSON.stringify({
      id: 882910291,
      order_number: 1001,
      line_items: [{ id: 1, title: 'Item 1', quantity: 2, price: '50.00' }],
      created_at: '2026-10-29T08:00:00Z',
    });
    const rawBody = Buffer.from(payload, 'utf8');
    const signature = calculateHmac(rawBody, secret, 'base64');

    const req: RawWebhookRequest = {
      platform: 'shopify',
      tenantId: 'tenant-123',
      headers: {
        'x-shopify-hmac-sha256': signature,
        'x-shopify-topic': 'orders/create',
        'x-shopify-webhook-id': 'wh_shp_001',
      },
      query: {},
      rawBody,
      path: '/webhooks/shopify',
      url: '/webhooks/shopify',
    };

    const isValid = await adapter.verifySignature(req, secret);
    expect(isValid).toBe(true);

    const event = await adapter.extractEvent(req);
    expect(event.eventId).toBe('wh_shp_001');
    expect(event.entityId).toBe('882910291');
    expect(event.currentState).toBe('CREATED');
    expect(event.stateWeight).toBe(10);
    expect(event.isHydrated).toBe(true);
  });

  it('rejects invalid Shopify signature', async () => {
    const rawBody = Buffer.from(JSON.stringify({ id: 1 }), 'utf8');
    const req: RawWebhookRequest = {
      platform: 'shopify',
      tenantId: 'tenant-123',
      headers: {
        'x-shopify-hmac-sha256': 'invalid_base64_sig==',
        'x-shopify-topic': 'orders/create',
      },
      query: {},
      rawBody,
      path: '/webhooks/shopify',
      url: '/webhooks/shopify',
    };

    const isValid = await adapter.verifySignature(req, secret);
    expect(isValid).toBe(false);
  });

  it('correctly maps CANCELLED event with weight 99', async () => {
    const payload = JSON.stringify({
      id: 882910291,
      cancel_reason: 'customer',
      cancelled_at: '2026-10-29T09:00:00Z',
    });
    const rawBody = Buffer.from(payload, 'utf8');

    const req: RawWebhookRequest = {
      platform: 'shopify',
      tenantId: 'tenant-123',
      headers: {
        'x-shopify-topic': 'orders/cancelled',
      },
      query: {},
      rawBody,
      path: '/webhooks/shopify',
      url: '/webhooks/shopify',
    };

    const event = await adapter.extractEvent(req);
    expect(event.currentState).toBe('CANCELLED');
    expect(event.stateWeight).toBe(99);
    expect(event.isHydrated).toBe(false);
  });
});

describe('TikTok Shop Adapter', () => {
  const adapter = new TikTokAdapter();
  const secret = 'tt_secret_beta_key_7781';

  it('verifies valid TikTok Shop HMAC-SHA256 signature in Hex', async () => {
    const payload = JSON.stringify({
      type: 'ORDER_STATUS_CHANGE',
      tts_notification_id: 'tt_notif_998',
      data: {
        order_id: 'TT_ORD_5541',
        order_status: 140, // CANCELLED
      },
      timestamp: Math.floor(Date.now() / 1000),
    });
    const rawBody = Buffer.from(payload, 'utf8');
    const path = '/webhooks/tiktok';
    const query = { app_key: 'test_app' };
    
    // Canonical: secret + path + sorted_params + rawBody + secret
    const canonical = `${secret}${path}app_key=test_app${rawBody.toString('utf8')}${secret}`;
    const signature = calculateHmac(canonical, secret, 'hex');

    const req: RawWebhookRequest = {
      platform: 'tiktok',
      tenantId: 'tenant-beta',
      headers: {
        authorization: signature,
      },
      query,
      rawBody,
      path,
      url: `${path}?app_key=test_app`,
    };

    const isValid = await adapter.verifySignature(req, secret);
    expect(isValid).toBe(true);

    const event = await adapter.extractEvent(req);
    expect(event.eventId).toBe('tt_notif_998');
    expect(event.entityId).toBe('TT_ORD_5541');
    expect(event.currentState).toBe('CANCELLED');
    expect(event.stateWeight).toBe(99);
  });
});

describe('Shopee Adapter', () => {
  const adapter = new ShopeeAdapter();
  const secret = 'shopee_secret_gamma_key_1123';

  it('verifies valid Shopee HMAC-SHA256 signature in Hex', async () => {
    const payload = JSON.stringify({
      msg_id: 'shp_msg_7721',
      code: 'ORDER_STATUS_UPDATE',
      data: {
        ordersn: '261029ABCDEF',
        status: 'SHIPPED',
        update_time: 1761720000,
        items: [{ item_id: 101, name: 'T-Shirt', model_quantity_purchased: 1 }],
      },
    });
    const rawBody = Buffer.from(payload, 'utf8');
    const url = 'http://localhost:3000/webhooks/shopee';

    // Canonical: url + "|" + rawBody
    const canonical = `${url}|${rawBody.toString('utf8')}`;
    const signature = calculateHmac(canonical, secret, 'hex');

    const req: RawWebhookRequest = {
      platform: 'shopee',
      tenantId: 'tenant-gamma',
      headers: {
        authorization: signature,
      },
      query: {},
      rawBody,
      path: '/webhooks/shopee',
      url,
    };

    const isValid = await adapter.verifySignature(req, secret);
    expect(isValid).toBe(true);

    const event = await adapter.extractEvent(req);
    expect(event.eventId).toBe('shp_msg_7721');
    expect(event.entityId).toBe('261029ABCDEF');
    expect(event.currentState).toBe('SHIPPED');
    expect(event.stateWeight).toBe(30);
    expect(event.isHydrated).toBe(true);
  });
});

describe('Adapter Registry', () => {
  it('retrieves adapters case-insensitively', () => {
    expect(getPlatformAdapter('shopify')).toBeInstanceOf(ShopifyAdapter);
    expect(getPlatformAdapter('SHOPIFY')).toBeInstanceOf(ShopifyAdapter);
    expect(getPlatformAdapter('tiktok')).toBeInstanceOf(TikTokAdapter);
    expect(getPlatformAdapter('shopee')).toBeInstanceOf(ShopeeAdapter);
    expect(getPlatformAdapter('unknown')).toBeUndefined();
  });
});

