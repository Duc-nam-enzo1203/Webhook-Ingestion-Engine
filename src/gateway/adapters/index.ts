import { WebhookPlatformAdapter } from './types.js';
import { ShopifyAdapter } from './shopify.adapter.js';
import { TikTokAdapter } from './tiktok.adapter.js';
import { ShopeeAdapter } from './shopee.adapter.js';

export * from './types.js';
export * from './crypto-utils.js';
export * from './shopify.adapter.js';
export * from './tiktok.adapter.js';
export * from './shopee.adapter.js';

const adapters: Record<string, WebhookPlatformAdapter> = {
  shopify: new ShopifyAdapter(),
  tiktok: new TikTokAdapter(),
  shopee: new ShopeeAdapter(),
};

export function getPlatformAdapter(platform: string): WebhookPlatformAdapter | undefined {
  return adapters[platform.toLowerCase()];
}

