import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';

export const options = {
  scenarios: {
    // Tenant A: VIP Merchant flooding 2,000 RPS
    noisy_merchant_a: {
      executor: 'constant-arrival-rate',
      rate: 1000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 50,
      maxVUs: 100,
      exec: 'tenantA',
    },
    // Tenant B: Regular Merchant sending 20 RPS
    victim_merchant_b: {
      executor: 'constant-arrival-rate',
      rate: 20,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 10,
      maxVUs: 20,
      exec: 'tenantB',
    },
  },
  thresholds: {
    'http_req_duration{tenant:tenant_b}': ['p(99)<30'], // Tenant B latency remains unaffected!
  },
};

const SECRET_A = 'shpss_sec_secret_alpha_key_9912';
const TENANT_A = 'a0000000-0000-0000-0000-000000000001';

const SECRET_B = 'tt_secret_beta_key_7781';
const TENANT_B = 'b0000000-0000-0000-0000-000000000002';

const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:3000';

export function tenantA() {
  const orderId = Math.floor(Math.random() * 1000000);
  const payload = JSON.stringify({ id: orderId, status: 'OPEN' });
  const signature = crypto.hmac('sha256', SECRET_A, payload, 'base64');

  http.post(`${BASE_URL}/webhooks/shopify`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'x-tenant-id': TENANT_A,
      'x-shopify-hmac-sha256': signature,
      'x-shopify-topic': 'orders/create',
      'x-shopify-webhook-id': `wh_a_${Date.now()}_${Math.random()}`,
    },
    tags: { tenant: 'tenant_a' },
  });
}

export function tenantB() {
  const payload = JSON.stringify({
    type: 'ORDER_STATUS_CHANGE',
    tts_notification_id: `wh_b_${Date.now()}_${Math.random()}`,
    data: { order_id: `ORD_B_${Math.floor(Math.random() * 1000)}`, order_status: 100 },
  });
  
  const path = '/webhooks/tiktok';
  const canonical = `${SECRET_B}${path}${payload}${SECRET_B}`;
  const signature = crypto.hmac('sha256', SECRET_B, canonical, 'hex');

  const res = http.post(`${BASE_URL}/webhooks/tiktok`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'x-tenant-id': TENANT_B,
      authorization: signature,
    },
    tags: { tenant: 'tenant_b' },
  });

  check(res, {
    'tenant B response is 202': (r) => r.status === 202,
  });
}

