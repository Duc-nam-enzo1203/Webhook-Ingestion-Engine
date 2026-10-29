export interface RawWebhookRequest {
  platform: string;
  tenantId: string;
  headers: Record<string, string | undefined>;
  query: Record<string, any>;
  rawBody: Buffer;
  path: string;
  url: string;
}

export interface VerifiedWebhookEvent {
  tenantId: string;
  platform: 'shopify' | 'tiktok' | 'shopee';
  eventId: string;
  entityType: 'ORDER' | 'PRODUCT' | 'INVENTORY';
  entityId: string;
  eventType: string;
  currentState: string;
  stateWeight: number;
  externalVersion: number; // Unix timestamp in ms or sequence
  isHydrated: boolean;     // True if full initial payload (e.g. line_items)
  payload: Record<string, any>;
  rawBody: string;
  receivedAt: string;
}

export interface WebhookPlatformAdapter {
  readonly platform: 'shopify' | 'tiktok' | 'shopee';
  verifySignature(req: RawWebhookRequest, secret: string): Promise<boolean>;
  extractEvent(req: RawWebhookRequest): Promise<VerifiedWebhookEvent>;
}

