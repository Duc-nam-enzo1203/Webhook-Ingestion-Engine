import http from 'k6/http';
import { check, sleep } from 'k6';
import crypto from 'k6/crypto';

export const options = {
  stages: [
    { duration: '15s', target: 200 },  // Warm up
    { duration: '30s', target: 1500 }, // Flash sale ramp up
    { duration: '45s', target: 1500 }, // Sustain peak load
    { duration: '15s', target: 0 },    // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<25', 'p(99)<50'], // p99 latency < 50ms
    http_req_failed: ['rate<0.01'],              // Less than 1% errors
  },
};

const SECRET = 'shpss_sec_secret_alpha_key_9912';
const TENANT_ID = 'a0000000-0000-0000-0000-000000000001';
const BASE_URL = __ENV.GATEWAY_URL || 'http://localhost:3000';

export default function () {
  const orderId = Math.floor(Math.random() * 1000000);
  const eventId = `wh_k6_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;

  const payload = JSON.stringify({
    id: orderId,
    order_number: 10000 + orderId,
    line_items: [
      { id: 1, name: 'Flash Sale Item', price: '29.99', quantity: 1 }
    ],
    created_at: new Date().toISOString(),
  });

  // Calculate HMAC-SHA256 in Base64
  const signature = crypto.hmac('sha256', SECRET, payload, 'base64');

  const headers = {
    'Content-Type': 'application/json',
    'x-tenant-id': TENANT_ID,
    'x-shopify-hmac-sha256': signature,
    'x-shopify-topic': 'orders/create',
    'x-shopify-webhook-id': eventId,
  };

  const res = http.post(`${BASE_URL}/webhooks/shopify`, payload, { headers });

  check(res, {
    'status is 202 Accepted': (r) => r.status === 202,
  });

  sleep(0.01);
}

