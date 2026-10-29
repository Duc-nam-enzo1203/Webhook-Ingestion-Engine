import crypto from 'node:crypto';
import { RawWebhookRequest, VerifiedWebhookEvent, WebhookPlatformAdapter } from './types.js';
import { calculateHmac, timingSafeEqualString } from './crypto-utils.js';

export class TikTokAdapter implements WebhookPlatformAdapter {
  readonly platform = 'tiktok' as const;

  async verifySignature(req: RawWebhookRequest, secret: string): Promise<boolean> {
    const signature =
      req.headers['authorization'] ||
      (req.query['sign'] as string | undefined);

    if (!signature || typeof signature !== 'string') {
      return false;
    }

    // Sort query params alphabetically excluding sign
    const queryKeys = Object.keys(req.query).filter(k => k !== 'sign').sort();
    const sortedParamStr = queryKeys.map(k => `${k}=${req.query[k]}`).join('&');

    // Canonical string: secret + path + sorted_params + rawBody + secret
    const canonicalString = `${secret}${req.path}${sortedParamStr}${req.rawBody.toString('utf8')}${secret}`;
    const calculated = calculateHmac(canonicalString, secret, 'hex');

    // Check signature constant-time
    const isSignatureValid = timingSafeEqualString(signature, calculated);
    if (!isSignatureValid) {
      return false;
    }

    // Optional Replay Protection if timestamp is present
    const timestampParam = req.query['timestamp'] || req.headers['x-tts-timestamp'];
    if (timestampParam) {
      const eventTimeSec = Number(timestampParam);
      const nowSec = Math.floor(Date.now() / 1000);
      if (!isNaN(eventTimeSec) && Math.abs(nowSec - eventTimeSec) > 300) {
        return false; // Timestamp drifted > 5 minutes
      }
    }

    return true;
  }

  async extractEvent(req: RawWebhookRequest): Promise<VerifiedWebhookEvent> {
    const rawString = req.rawBody.toString('utf8');
    let payload: any;
    try {
      payload = JSON.parse(rawString);
    } catch {
      throw new Error('Malformed JSON in TikTok webhook body');
    }

    const deterministicHash = crypto.createHash('sha256').update(`${req.path}:${rawString}`).digest('hex').substring(0, 16);
    const eventId =
      payload.tts_notification_id ||
      payload.event_id ||
      `tiktok_${payload.data?.order_id || 'order'}_${deterministicHash}`;

    const data = payload.data || payload;
    const entityId = String(data.order_id || payload.order_id || 'unknown_order');
    const eventType = String(payload.type || data.event_type || 'ORDER_STATUS_UPDATE');

    let currentState = 'CREATED';
    let stateWeight = 10;
    let isHydrated = false;

    const orderStatus = Number(data.order_status || payload.order_status);
    if (orderStatus === 100) {
      currentState = 'CREATED';
      stateWeight = 10;
    } else if (orderStatus === 111 || orderStatus === 112) {
      currentState = 'PAID';
      stateWeight = 20;
    } else if (orderStatus === 120 || orderStatus === 121 || orderStatus === 122) {
      currentState = 'SHIPPED';
      stateWeight = 30;
    } else if (orderStatus === 130) {
      currentState = 'COMPLETED';
      stateWeight = 40;
    } else if (orderStatus === 140 || eventType.includes('CANCEL')) {
      currentState = 'CANCELLED';
      stateWeight = 99;
    }

    if (Array.isArray(data.item_list) && data.item_list.length > 0) {
      isHydrated = true;
    }

    const externalVersion = Number(payload.timestamp || data.update_time || Date.now());

    return {
      tenantId: req.tenantId,
      platform: this.platform,
      eventId,
      entityType: 'ORDER',
      entityId,
      eventType,
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

