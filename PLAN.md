# Project Roadmap & Execution Plan: Webhook Ingestion Engine

## Implementation Status Tracker

- [x] **Phase 1: High-Speed Ingestion Gateway & Security**
  - [x] TypeScript Strict Configuration (`tsconfig.json`) & Pino structured logging.
  - [x] Custom Raw Buffer Parser for byte-accurate HMAC-SHA256 calculations.
  - [x] Constant-time signature comparison (`crypto.timingSafeEqual`) to prevent Timing Attacks.
  - [x] Platform Adapters for Shopify, TikTok Shop, and Shopee.
  - [x] Multi-Tier Tenant Cache (L1 LRU + L2 Redis + Pub/Sub Invalidation).
  - [x] Per-Tenant Token Bucket Rate Limiting (Atomic Redis Lua).
  - [x] Shuffle Sharding Router with Power of Two Choices across 16 Redis Stream shards.
  - [x] Fast Ack Producer (`202 Accepted` < 15ms).

- [x] **Phase 2: Resilient Worker Pool & Fault-Tolerant Core**
  - [x] Crash-Safe ZSET Lease Semaphore (auto-evicts dead worker leases).
  - [x] Two-Phase Idempotency Lock (`LOCKED:{worker_id}` vs `DONE`).
  - [x] Out-of-Order Event Resolution with Weighted FSM + Bidirectional JSONB Hydration in PostgreSQL.
  - [x] PostgreSQL 16+ Partitioned Schema (`idempotency_keys` HASH, `webhook_events` RANGE).
  - [x] Per-Tenant Circuit Breaker with Exponential Backoff and Full Jitter.
  - [x] Zombie Recovery Loop (`XAUTOCLAIM`) for PEL messages stuck > 30s.

- [x] **Phase 3: Observability, Infrastructure & Benchmarks**
  - [x] Prometheus Metrics (`/metrics` endpoint with p99 latency, backlog, and DLQ depth).
  - [x] Grafana Dashboard (`infra/grafana/dashboards/ingestion-overview.json`).
  - [x] Docker Compose setup with healthchecks and volume persistence.
  - [x] Mock Downstream ERP service for chaos/latency injection.
  - [x] k6 Load & Chaos Testing scripts (`k6-flash-sale-spike.js`, `k6-noisy-neighbor.js`).
  - [x] 4 Architecture Decision Records (ADR 0001 - 0004).

- [x] **Phase 4: Automated Testing & Verification**
  - [x] 24 Unit Tests covering crypto, adapters, routers, circuit breaker, and state machine.
  - [x] 4 Integration Tests covering race condition deduplication, out-of-order hydration, PEL rescue, and noisy neighbor isolation.
  - [x] Zero build errors on `npm run build`.
  - [x] GitHub Actions CI pipeline (`.github/workflows/ci.yml`).

