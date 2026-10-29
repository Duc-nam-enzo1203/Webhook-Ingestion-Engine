import { RawWebhookRequest, VerifiedWebhookEvent, WebhookPlatformAdapter } from './types.js';
import { calculateHmac, timingSafeEqualString } from './crypto-utils.js';

export class ShopifyAdapter implements WebhookPlatformAdapter {
  readonly platform = 'shopify' as const;

  async verifySignature(req: RawWebhookRequest, secret: string): Promise<boolean> {
    const signature = req.headers['x-shopify-hmac-sha256'];
    if (!signature || typeof signature !== 'string') {
      return false;
    }

    const calculated = calculateHmac(req.rawBody, secret, 'base64');
    return timingSafeEqualString(signature, calculated);
  }

  async extractEvent(req: RawWebhookRequest): Promise<VerifiedWebhookEvent> {
    const topic = (req.headers['x-shopify-topic'] as string) || 'orders/create';
    const rawString = req.rawBody.toString('utf8');
    const payload = JSON.parse(rawString);

    const eventId = 
      (req.headers['x-shopify-webhook-id'] as string) || 
      `shopify_${payload.id}_${topic}_${Date.now()}`;
      
    const entityId = String(payload.id || payload.order_id || 'unknown_order');

    let currentState = 'CREATED';
    let stateWeight = 10;
    let isHydrated = false;

    const lowerTopic = topic.toLowerCase();
    if (lowerTopic.includes('create')) {
      currentState = 'CREATED';
      stateWeight = 10;
      isHydrated = Array.isArray(payload.line_items) && payload.line_items.length > 0;
    } else if (lowerTopic.includes('paid')) {
      currentState = 'PAID';
      stateWeight = 20;
    } else if (lowerTopic.includes('fulfill')) {
      currentState = 'SHIPPED';
      stateWeight = 30;
    } else if (lowerTopic.includes('cancel')) {
      currentState = 'CANCELLED';
      stateWeight = 99;
    } else {
      currentState = 'UPDATED';
      stateWeight = 15;
    }

    const versionSource = payload.updated_at || payload.created_at;
    const externalVersion = versionSource ? new Date(versionSource).getTime() : Date.now();

    return {
      tenantId: req.tenantId,
      platform: this.platform,
      eventId,
      entityType: 'ORDER',
      entityId,
      eventType: topic,
      currentState,
      stateWeight,
      externalVersion,
      isHydrated,
      payload,
      rawBody: rawString,
      receivedAt: new Date().toISOString(),
    };
  }
}

