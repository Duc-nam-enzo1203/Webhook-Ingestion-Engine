# ⚡ High-Throughput & Fault-Tolerant Webhook Ingestion Engine (v3.0)

[![TypeScript](https://img.shields.io/badge/TypeScript-5.7-blue.svg)](https://www.typescriptlang.org/)
[![Fastify](https://img.shields.io/badge/Fastify-5.2-black.svg)](https://www.fastify.io/)
[![Redis Streams](https://img.shields.io/badge/Redis-Streams%207-red.svg)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![Prometheus](https://img.shields.io/badge/Prometheus-Metrics-orange.svg)](https://prometheus.io/)
[![Vitest](https://img.shields.io/badge/Vitest-3.0-green.svg)](https://vitest.dev/)

> **A production-grade, multi-tenant Webhook Ingestion & State Reconciliation Engine designed to ingest 5,000+ RPS with sub-15ms p99 latency, zero-head-of-line blocking, and out-of-order partial payload hydration.**

---

## 🏗️ Architecture Overview

```mermaid
graph TD
    subgraph External Platforms
        A1[Shopify Webhooks]
        A2[TikTok Shop Webhooks]
        A3[Shopee Webhooks]
    end

    subgraph Ingestion Gateway (Fastify)
        B1[Raw Body HMAC Verifier<br/>crypto.timingSafeEqual]
        B2[Multi-Tier Tenant Cache<br/>L1 In-Memory + L2 Redis + Pub/Sub]
        B3[Token Bucket Rate Limiter<br/>Redis Lua]
        B4[Shuffle Sharding Router<br/>Power of Two Choices]
    end

    subgraph Messaging Buffer (Redis Streams)
        S0[Shard 00]
        S1[Shard 01]
        SN[Shard 15]
        PEL[Background XAUTOCLAIM Loop<br/>Zombie PEL Rescue]
    end

    subgraph Worker Pool
        W1[Worker 1]
        W2[Worker 2]
        WN[Worker N]
    end

    subgraph State & Durability
        L1[(Redis: Two-Phase Lease Lock<br/>LOCKED vs DONE)]
        L2[(Redis: ZSET Semaphore<br/>Crash-Safe Tenant Concurrency)]
        DB1[(PostgreSQL: idempotency_keys<br/>PARTITION BY HASH)]
        DB2[(PostgreSQL: entity_states<br/>Weighted FSM + JSONB Hydration)]
        DB3[(PostgreSQL: webhook_events<br/>PARTITION BY RANGE)]
    end

    subgraph Downstream
        ERP[Downstream ERP / WMS API<br/>X-Idempotency-Key Header]
    end

    A1 & A2 & A3 -->|POST /webhooks/:platform| B1
    B1 --> B2 --> B3 --> B4
    B4 -->|XADD Shorter Shard -> 202 Accepted < 15ms| S0 & S1 & SN
    S0 & S1 & SN -->|XREADGROUP| W1 & W2 & WN
    PEL -.->|Rescue Stale Messages| W1 & W2 & WN
    W1 & W2 & WN <--> L1 & L2
    W1 & W2 & WN <--> DB1 & DB2 & DB3
    W1 & W2 & WN -->|Dispatch| ERP
```

---

## 🌟 Senior Differentiators & Engineering Truths

### 1. Honest Durability: Redis Streams (`appendfsync everysec`) + Active Backpressure
* Rejects false marketing claims of "Zero Data Loss" over HTTP.
* Gateway responds `202 Accepted` in **< 15ms** by buffering in Redis Streams. A bounded 1-second physical power-failure window is mitigated via:
  1. **Active Backpressure (HTTP 503 + Retry-After)** when Redis memory or backlog exceeds threshold (no blind `MAXLEN` drops).
  2. **Pull-based Reconciliation Poller** to reconcile gaps with platform APIs.

### 2. Out-of-Order Event Resolution: Weighted FSM + Bidirectional JSONB Hydration
When an `ORDER_CANCELLED` event (weight 99, contains only `{ order_id: 123, cancel_reason: "fraud" }`) arrives **before** the delayed `ORDER_CREATED` event (weight 10, contains customer info and `line_items`):
* Standard databases either drop late events (losing customer data) or blindly overwrite (resurrecting cancelled orders).
* Our atomic SQL engine inverts the PostgreSQL `||` operator based on `(state_weight, external_version)`:
  * Higher-weight payload is placed on the **right** of `||` to preserve terminal state (`CANCELLED`).
  * Lower-weight event is merged on the **left** of `||` to hydrate missing properties without regressing order status.

### 3. Crash-Proof Resource Isolation: ZSET Lease Semaphores
* Counters (`INCR/DECR`) leak when workers crash or encounter OOM (`SIGKILL`), permanently deadlocking merchant quotas.
* We utilize Redis `ZSET` leases where `score = now + lease_ttl`. Expired leases are atomically cleared via `ZREMRANGEBYSCORE` on every acquisition.

### 4. Head-of-Line Blocking Elimination: Shuffle Sharding (Power of Two Choices)
* To prevent Mega Flash Sales from starving smaller merchants on shared infrastructure, each tenant is deterministically assigned two shards.
* Incoming requests are dynamically routed to the shard with the shorter queue length, reducing max load disparity from $O(\frac{\log N}{\log \log N})$ to $O(\log \log N)$.

---

## 🚀 Quick Start (One-Command Experience)

### 1. Run with Docker Compose
```bash
make up
```
* **Gateway API:** `http://localhost:3000`
* **Mock Downstream ERP:** `http://localhost:4000`
* **Prometheus:** `http://localhost:9090`
* **Grafana Dashboard:** `http://localhost:3001` (User: `admin`, Pass: `admin`)

### 2. Run Test Suite
```bash
# Run all unit tests
make test
```

### 3. Run Flash Sale Benchmark
```bash
# Requires k6 installed
make benchmark
```

---

## 📚 Architecture Decision Records (ADRs)
Read our detailed trade-off analysis in `docs/adr/`:
* [ADR-0001: Bounded Durability Window via Redis Streams](docs/adr/0001-bounded-durability-redis-streams.md)
* [ADR-0002: Two-Phase Lease Locking & Partitioned Idempotency](docs/adr/0002-two-phase-idempotency-and-partitioning.md)
* [ADR-0003: Out-of-Order Resolution via Bidirectional JSONB Hydration](docs/adr/0003-out-of-order-bidirectional-hydration.md)
* [ADR-0004: Multi-Tenant Isolation via Shuffle Sharding & Semaphores](docs/adr/0004-shuffle-sharding-and-tenant-isolation.md)

---

## 📄 License
MIT License.

