import { RawWebhookRequest, VerifiedWebhookEvent, WebhookPlatformAdapter } from './types.js';
import { calculateHmac, timingSafeEqualString } from './crypto-utils.js';

export class ShopeeAdapter implements WebhookPlatformAdapter {
  readonly platform = 'shopee' as const;

  async verifySignature(req: RawWebhookRequest, secret: string): Promise<boolean> {
    const signature = req.headers['authorization'];
    if (!signature || typeof signature !== 'string') {
      return false;
    }

    // Shopee canonical signing string: url + "|" + rawBody
    const baseString = `${req.url}|${req.rawBody.toString('utf8')}`;
    const calculated = calculateHmac(baseString, secret, 'hex');

    const isSignatureValid = timingSafeEqualString(signature, calculated);
    if (!isSignatureValid) {
      return false;
    }

    // Optional Replay Protection if timestamp is present in header or query
    const timestampHeader = req.headers['timestamp'] || req.query['timestamp'];
    if (timestampHeader) {
      const eventTimeSec = Number(timestampHeader);
      const nowSec = Math.floor(Date.now() / 1000);
      if (!isNaN(eventTimeSec) && Math.abs(nowSec - eventTimeSec) > 300) {
        return false;
      }
    }

    return true;
  }

  async extractEvent(req: RawWebhookRequest): Promise<VerifiedWebhookEvent> {
    const rawString = req.rawBody.toString('utf8');
    const payload = JSON.parse(rawString);

    const eventId = 
      payload.msg_id || 
      `shopee_${payload.code || 'msg'}_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

    const data = payload.data || payload;
    const entityId = String(data.ordersn || payload.ordersn || 'unknown_order');
    const status = String(data.status || payload.status || 'UNPAID').toUpperCase();

    let currentState = 'CREATED';
    let stateWeight = 10;
    let isHydrated = false;

    if (status === 'UNPAID' || status === 'READY_TO_SHIP') {
      currentState = 'CREATED';
      stateWeight = 10;
    } else if (status === 'PROCESSED' || status === 'RETRY_SHIP') {
      currentState = 'PAID';
      stateWeight = 20;
    } else if (status === 'SHIPPED') {
      currentState = 'SHIPPED';
      stateWeight = 30;
    } else if (status === 'COMPLETED') {
      currentState = 'COMPLETED';
      stateWeight = 40;
    } else if (status === 'CANCELLED' || status === 'IN_CANCEL') {
      currentState = 'CANCELLED';
      stateWeight = 99;
    }

    if (Array.isArray(data.items) && data.items.length > 0) {
      isHydrated = true;
    }

    const externalVersion = Number(data.update_time || payload.timestamp || Date.now());

    return {
      tenantId: req.tenantId,
      platform: this.platform,
      eventId,
      entityType: 'ORDER',
      entityId,
      eventType: String(payload.code || status),
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

