# Webhook Ingestion Engine: Benchmark & Load Testing Guide

This document outlines the performance benchmarks, methodology, and reproduction steps for the Webhook Ingestion Engine using `k6`.

---

## 1. Test Methodology & Objectives

The engine is engineered to handle massive burst traffic from e-commerce platforms (Shopify, TikTok Shop, Shopee) without Head-of-Line blocking or data loss. The benchmarks evaluate:
1. **Ingestion Throughput & Latency:** Ingesting burst webhooks with HMAC-SHA256 verification and buffered queue dispatch.
2. **Multi-Tenant Isolation:** Ensuring a noisy or misconfigured tenant cannot saturate shared worker capacity or starve other merchants.
3. **Durability & Resource Bounds:** Verifying safe MINID trimming and bounded memory usage under high sustained volume.

---

## 2. Benchmark Scenarios

### Scenario 1: Flash Sale Spike Ingestion
* **Script:** `benchmarks/k6-flash-sale-spike.js`
* **Target:** Gateway endpoint `POST /webhooks/shopify`
* **Target Tenant:** Merchant Alpha (`id: a0000000-0000-0000-0000-000000000001`, `rate_limit_rps = 2000`, `max_concurrency = 50`)
* **Workload:** Rapid traffic ramp from 100 RPS to 1,500 RPS over 60 seconds with valid HMAC signatures.
* **Key Verification:**
  - Gateway responds `202 Accepted` on valid signatures.
  - Zero 5xx server errors.
  - Latency p95 and p99 remain sub-50ms when buffering into Redis Streams.

### Scenario 2: Multi-Tenant Noisy Neighbor Isolation
* **Script:** `benchmarks/k6-noisy-neighbor.js`
* **Target:** Dual-tenant concurrent traffic:
  - **Tenant A (Burst Merchant):** Flooding at 500+ RPS while downstream ERP is simulated with a 3,000ms latency.
  - **Tenant B (Steady Merchant):** Constant 20 RPS with a fast, healthy downstream ERP (20ms latency).
* **Key Verification:**
  - Tenant A worker slots are capped by its **ZSET Lease Semaphore** (`max_concurrency`), preventing thread exhaustion.
  - Tenant B experiences **zero Head-of-Line blocking**; its events continue processing smoothly without increased queue lag.

---

## 3. Running Benchmarks Locally

### Prerequisites
1. Start the complete system stack:
   ```bash
   make up
   ```
2. Ensure [k6](https://k6.io/) is installed on your load generator machine:
   ```bash
   # On macOS
   brew install k6

   # On Linux
   sudo gpg -k
   sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
   echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" | sudo tee /etc/apt/sources.list.d/k6.list
   sudo apt-get update && sudo apt-get install k6
   ```

### Execution Commands
```bash
# Execute Flash Sale Spike benchmark
make benchmark

# Or execute specific scenario directly with k6
k6 run benchmarks/k6-flash-sale-spike.js
k6 run benchmarks/k6-noisy-neighbor.js
```

---

## 4. Post-Benchmark Verification Queries

After running the benchmark suite, verify database and stream consistency using PostgreSQL:

```sql
-- 1. Verify 100% Idempotency (Zero duplicate processed events)
SELECT tenant_id, platform, event_id, COUNT(*) 
FROM idempotency_keys 
GROUP BY tenant_id, platform, event_id 
HAVING COUNT(*) > 1;
-- Expected output: 0 rows

-- 2. Verify Delivery Status Distribution
SELECT delivery_status, COUNT(*) 
FROM webhook_events 
GROUP BY delivery_status;

-- 3. Verify Out-of-Order State Preservation
SELECT current_state, is_hydrated, count(*) 
FROM entity_states 
GROUP BY current_state, is_hydrated;
```

---

> [!NOTE]
> Physical performance figures (such as exact RPS, p95/p99 latency) depend heavily on host CPU, disk I/O, network latency, and Docker resource limits. Run the reproducible k6 scripts above on your target infrastructure to collect environment-specific metrics.
