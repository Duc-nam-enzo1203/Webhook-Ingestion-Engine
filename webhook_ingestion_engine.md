# Engineering Plan (v3.0): Multi-Tenant Fault-Tolerant Webhook Ingestion Engine

## 1. Executive Summary & Problem Statement

### 1.1. The Real-World Problem
Trong hệ thống E-commerce đa kênh (Shopify, TikTok Shop, Shopee, WooCommerce), việc tiếp nhận và đồng bộ dữ liệu qua Webhook đối mặt với 5 thách thức kỹ thuật cốt lõi ở quy mô lớn:
1. **Traffic Spikes & Noisy Neighbor (Head-of-Line Blocking):** Một merchant lớn chạy chương trình Flash Sale bắn 20,000 webhooks/phút vào hàng đợi chung sẽ làm nghẽn toàn bộ các merchant nhỏ đang dùng chung hạ tầng.
2. **Network Unreliability & Race Conditions:** Khi mạng chập chờn hoặc xử lý chậm, các sàn tự động retry. Hai bản sao của cùng một sự kiện có thể đến cùng lúc và được nhặt bởi 2 worker khác nhau, dẫn đến trừ tồn kho 2 lần hoặc nhân đôi đơn hàng.
3. **Out-of-Order & Partial Payloads:** Do kiến trúc phân tán phía sàn, sự kiện `ORDER_CANCELLED` (chỉ chứa `order_id` và `status`) có thể đến trước sự kiện `ORDER_CREATED` (chứa thông tin khách hàng, địa chỉ, `line_items`). Nếu chỉ bỏ qua sự kiện đến muộn, hệ thống sẽ mất sạch dữ liệu chi tiết đơn hàng; nếu ghi đè mù quáng, đơn hàng đã hủy sẽ sống lại.
4. **Worker Crashes & Lock Leaks:** Worker có thể bị OOM (Out of Memory) hoặc `SIGKILL` bất kỳ lúc nào. Các cơ chế khóa đơn giản (`SETNX` hoặc `INCR/DECR` counter) sẽ gây ra tình trạng chặn nhầm bản retry hợp lệ hoặc khóa chết (Deadlock) hạn mức xử lý của tenant.
5. **Cascading Failures:** Hệ thống ERP/Downstream của một khách hàng bị treo hoặc phản hồi chậm sẽ làm cạn kiệt Connection Pool, kéo sập toàn bộ cụm Worker xử lý chung.

### 1.2. Core Architectural Principles (Design Truths)
Thay vì đưa ra các tuyên bố marketing thiếu thực tế như *"Zero Data Loss"* hay *"Exactly-Once Delivery"*, hệ thống này được thiết kế dựa trên các nguyên lý kỹ thuật thực chứng:
* **At-Least-Once Delivery with End-to-End Idempotency:** Chấp nhận việc message có thể được thực thi nhiều hơn 1 lần khi xảy ra lỗi mạng giữa chừng, nhưng đảm bảo **tác động nghiệp vụ chỉ xảy ra đúng 1 lần** nhờ cơ chế Two-Phase Lease Locking, PostgreSQL Hash-Partitioned Unique Constraints và truyền `X-Idempotency-Key` xuống Downstream.
* **Bounded Durability Window + Active Backpressure:** Sử dụng Redis Streams (`appendfsync everysec`) để đạt độ trễ tiếp nhận p99 < 15ms. Chấp nhận cửa sổ rủi ro mất dữ liệu tối đa ~1 giây nếu sập nguồn vật lý toàn bộ cụm Redis, được bù đắp bởi:
  1. **Active Backpressure (`HTTP 429/503`)** tại Gateway khi bộ nhớ Redis vượt ngưỡng an toàn (tuyệt đối không dùng `MAXLEN` cắt bỏ dữ liệu chưa xử lý).
  2. **Pull-based Reconciliation Worker** đối soát định kỳ với API của sàn.
* **Shuffle Sharding & Lease-based Concurrency:** Cách ly tài nguyên giữa các tenant bằng thuật toán *Power of Two Choices* trên tập Stream Shards cố định, kết hợp Semaphore tự giải phóng theo thời gian (ZSET Lease) để chống rò rỉ tài nguyên khi worker chết.

---

## 2. System Architecture & Data Flow

```text
+---------------------------------------------------------------------------------------------------+
|                                     EXTERNAL PLATFORMS                                            |
|                        (Shopify, TikTok Shop, Shopee, WooCommerce)                                |
+-------------------------------------------------+-------------------------------------------------+
                                                  | HTTP POST (Raw Buffer + Signature Headers)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                INGESTION GATEWAY (Fastify)                                        |
|  1. Platform Adapter Strategy: Verify HMAC via crypto.timingSafeEqual (Raw Body)                  |
|  2. Multi-Tier Tenant Cache: L1 (In-Memory LRU, 30s) -> L2 (Redis, 10m) + Pub/Sub Invalidation    |
|  3. Backpressure Check: Reject with 503 + Retry-After if Redis memory / shard backlog > threshold |
|  4. Per-Tenant Rate Limiter: Redis Lua Token Bucket                                               |
|  5. Shuffle Sharding Router (Power of Two Choices): Pick shorter queue of 2 assigned shards       |
+-------------------------------------------------+-------------------------------------------------+
                                                  | XADD (No blind MAXLEN eviction) -> 202 Accepted
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                  REDIS STREAMS (SHARDED BROKER)                                   |
|   [webhooks:stream:00]   [webhooks:stream:01]   ...   [webhooks:stream:15]                        |
|   + Pending Entries List (PEL) monitored by Background XAUTOCLAIM Recovery Loop                   |
+-------------------------------------------------+-------------------------------------------------+
                                                  | XREADGROUP (Consumer Group Workers)
                                                  v
+---------------------------------------------------------------------------------------------------+
|                                   STATELESS WORKER POOL                                           |
|  1. Per-Tenant Circuit Breaker Check (Open -> Fast-fail to Delayed Retry ZSET)                    |
|  2. ZSET Lease Semaphore: Acquire tenant concurrency slot (Auto-evicts dead worker leases)        |
|  3. Tier-1 Redis Lease Lock: SET NX EX 30s ("LOCKED:{worker_id}") vs "DONE"                       |
|  4. Tier-2 DB Transaction:                                                                        |
|     - Insert/Verify on `idempotency_keys` (Partitioned by HASH(tenant_id))                        |
|     - Upsert `entity_states` (State Weight FSM + Bidirectional JSONB Hydration)                   |
|     - Append to `webhook_events` log (Partitioned by RANGE(created_at))                           |
|  5. Dispatch to Downstream ERP with `X-Idempotency-Key` header                                    |
|  6. Commit State -> Mark Redis "DONE" -> Release Semaphore -> XACK Stream -> Update Safe XTRIM    |
+---------------------------------------------------------------------------------------------------+
```

---

## 3. Production-Grade Database Schema (PostgreSQL 16+)

### Tại sao phải tách làm 3 bảng?
Trong PostgreSQL, một bảng được Partition theo thời gian (`RANGE (created_at)`) **bắt buộc phải đưa cột `created_at` vào trong `PRIMARY KEY` hoặc `UNIQUE CONSTRAINT`**. Nếu gộp chung bảng Log và bảng kiểm tra Idempotency làm một, khi một webhook bị retry qua ranh giới ngày hoặc có `created_at` khác nhau, PostgreSQL sẽ không chặn được bản ghi trùng lặp.

Do đó, thiết kế chuẩn bắt buộc phải tách thành 3 bảng chuyên biệt:

```sql
-- ============================================================================
-- 1. BẢNG KIỂM SOÁT IDEMPOTENCY TOÀN CỤC (Partitioned by HASH)
-- Mục đích: Đảm bảo tính UNIQUE tuyệt đối cho (tenant_id, platform, event_id)
-- bất chấp thời gian sự kiện được tạo ra. Giữ kích thước nhỏ gọn để luôn nằm trong RAM.
-- ============================================================================
CREATE TABLE idempotency_keys (
    tenant_id UUID NOT NULL,
    platform VARCHAR(32) NOT NULL,
    event_id VARCHAR(128) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    locked_by VARCHAR(64),
    locked_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, platform, event_id)
) PARTITION BY HASH (tenant_id);

-- Tạo 16 hash partitions để phân tán tải Index B-Tree
CREATE TABLE idempotency_keys_p0 PARTITION OF idempotency_keys FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE idempotency_keys_p1 PARTITION OF idempotency_keys FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE idempotency_keys_p2 PARTITION OF idempotency_keys FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE idempotency_keys_p3 PARTITION OF idempotency_keys FOR VALUES WITH (MODULUS 4, REMAINDER 3);


-- ============================================================================
-- 2. BẢNG QUẢN LÝ TRẠNG THÁI THỰC THỂ & CHỐNG OUT-OF-ORDER (FSM + Hydration)
-- Mục đích: Xử lý sự kiện đến sai thứ tự và gộp dữ liệu thiếu (Partial Payloads)
-- ============================================================================
CREATE TABLE entity_states (
    tenant_id UUID NOT NULL,
    platform VARCHAR(32) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,       -- 'ORDER', 'PRODUCT', 'INVENTORY'
    entity_id VARCHAR(128) NOT NULL,        -- ID đơn hàng hoặc sản phẩm trên sàn
    current_state VARCHAR(32) NOT NULL,     -- 'CREATED', 'PAID', 'SHIPPED', 'CANCELLED'
    state_weight INT NOT NULL,              -- CREATED=10, PAID=20, SHIPPED=30, COMPLETED=40, CANCELLED=99
    external_version BIGINT NOT NULL,       -- Sequence number hoặc Unix timestamp (ms) từ sàn
    is_hydrated BOOLEAN NOT NULL DEFAULT FALSE, -- TRUE khi đã nhận đủ dữ liệu gốc (như line_items)
    merged_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, platform, entity_type, entity_id)
);


-- ============================================================================
-- 3. BẢNG LƯU TRỮ LỊCH SỬ PAYLOAD & AUDIT LOG (Partitioned by RANGE)
-- Mục đích: Lưu trữ raw JSONB phục vụ debug, replay và đối soát.
-- Partition theo tháng/tuần để xóa dữ liệu cũ tức thì bằng DROP TABLE (Zero I/O spike).
-- ============================================================================
CREATE TABLE webhook_events (
    id BIGSERIAL,
    tenant_id UUID NOT NULL,
    platform VARCHAR(32) NOT NULL,
    event_id VARCHAR(128) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    delivery_status VARCHAR(20) NOT NULL CHECK (delivery_status IN ('QUEUED', 'DELIVERED', 'RETRYING', 'DLQ')),
    retry_count INT NOT NULL DEFAULT 0,
    raw_payload JSONB NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

CREATE TABLE webhook_events_2026_10 PARTITION OF webhook_events
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');

CREATE INDEX idx_webhook_events_lookup 
    ON webhook_events (tenant_id, platform, event_id, created_at);

CREATE INDEX idx_webhook_events_dlq 
    ON webhook_events (tenant_id, created_at) 
    WHERE delivery_status = 'DLQ';
```

---

## 4. Core Algorithms & Atomic Scripts (The "Senior" Differentiators)

### 4.1. Out-of-Order Upsert & Bidirectional JSONB Hydration SQL
Khi `ORDER_CANCELLED` (trọng số 99, chỉ có `{ "order_id": "123", "cancel_reason": "buyer_request" }`) đến trước `ORDER_CREATED` (trọng số 10, có `{ "order_id": "123", "status": "CREATED", "line_items": [...] }`):
* Nếu dùng `merged_payload || EXCLUDED.merged_payload` thông thường, dữ liệu của `ORDER_CREATED` đến sau sẽ ghi đè các trường trùng tên của `ORDER_CANCELLED`.
* **Giải pháp:** Đảo chiều toán tử `||` dựa trên so sánh `(state_weight, external_version)`. Sự kiện có trọng số cao hơn luôn nằm ở **bên phải** toán tử `||` để giữ quyền ưu tiên ghi đè; sự kiện cũ hơn đến muộn nằm ở **bên trái** để chỉ bổ sung các thuộc tính còn khuyết (`line_items`, `customer`).

```sql
-- Tham số truyền vào:
-- $1: tenant_id, $2: platform, $3: entity_type, $4: entity_id
-- $5: incoming_state, $6: incoming_weight, $7: incoming_version
-- $8: incoming_is_hydrated (TRUE nếu là event khởi tạo đầy đủ), $9: incoming_payload (JSONB)

INSERT INTO entity_states (
    tenant_id, platform, entity_type, entity_id,
    current_state, state_weight, external_version, is_hydrated, merged_payload, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
ON CONFLICT (tenant_id, platform, entity_type, entity_id)
DO UPDATE SET
    -- 1. Chỉ tiến trạng thái (hoặc cùng trọng số nhưng version mới hơn), không bao giờ lùi từ Terminal State (99)
    current_state = CASE
        WHEN EXCLUDED.state_weight > entity_states.state_weight
          OR (EXCLUDED.state_weight = entity_states.state_weight AND EXCLUDED.external_version > entity_states.external_version)
        THEN EXCLUDED.current_state
        ELSE entity_states.current_state
    END,
    state_weight = GREATEST(entity_states.state_weight, EXCLUDED.state_weight),
    external_version = GREATEST(entity_states.external_version, EXCLUDED.external_version),
    is_hydrated = entity_states.is_hydrated OR EXCLUDED.is_hydrated,
    -- 2. Bidirectional JSONB Merge: Bản ghi nào mới hơn (weight lớn hơn hoặc version lớn hơn) sẽ nằm bên phải dấu ||
    merged_payload = CASE
        WHEN EXCLUDED.state_weight > entity_states.state_weight
          OR (EXCLUDED.state_weight = entity_states.state_weight AND EXCLUDED.external_version >= entity_states.external_version)
        THEN entity_states.merged_payload || EXCLUDED.merged_payload
        ELSE EXCLUDED.merged_payload || entity_states.merged_payload
    END,
    updated_at = NOW()
RETURNING 
    current_state, 
    is_hydrated, 
    merged_payload,
    (xmax = 0) AS is_inserted;
```

### 4.2. Crash-Safe Per-Tenant Semaphore (Redis Lua ZSET Lease)
Không dùng `INCR`/`DECR` vì nếu Worker bị `SIGKILL`, counter sẽ không bao giờ giảm và làm khóa chết Tenant đó. Script Lua dưới đây dùng `ZSET` với `score = expiration_timestamp_ms`:

```lua
-- KEYS[1]: semaphore:tenant:{tenant_id}
-- ARGV[1]: current_time_ms
-- ARGV[2]: lease_duration_ms (e.g., 30000)
-- ARGV[3]: max_concurrency (e.g., 10)
-- ARGV[4]: lock_owner_id (worker_id + ":" + event_id)

local key = KEYS[1]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local max_slots = tonumber(ARGV[3])
local owner = ARGV[4]

-- 1. Dọn dẹp toàn bộ các lease đã hết hạn (do worker chết ngầm)
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)

-- 2. Kiểm tra số lượng slot đang active hiện tại
local current_active = redis.call('ZCARD', key)

if current_active < max_slots then
    -- Cấp slot mới và đặt hạn sử dụng
    redis.call('ZADD', key, now + ttl, owner)
    redis.call('PEXPIRE', key, ttl * 2)
    return 1 -- Acquire thành công
else
    return 0 -- Đã chạm trần concurrency của tenant này
end
```

### 4.3. Two-Phase Idempotency Lease Lock & Safe Release (Redis Lua)
Phân biệt rõ trạng thái `LOCKED:{worker_id}` (đang xử lý) và `DONE` (đã hoàn tất). Chỉ cho phép xóa khóa khi retry nếu đúng chính `worker_id` đó đang giữ khóa:

```lua
-- Script 1: Acquire Idempotency Lease
-- KEYS[1]: idemp:{tenant_id}:{platform}:{event_id}
-- ARGV[1]: worker_id
-- ARGV[2]: lease_ttl_ms (e.g., 30000)

local val = redis.call('GET', KEYS[1])
if not val then
    redis.call('SET', KEYS[1], 'LOCKED:' .. ARGV[1], 'PX', tonumber(ARGV[2]))
    return 'ACQUIRED'
elseif val == 'DONE' then
    return 'ALREADY_DONE'
elseif val == ('LOCKED:' .. ARGV[1]) then
    -- Gia hạn nếu chính worker này gọi lại
    redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
    return 'ACQUIRED'
else
    return 'IN_FLIGHT_BY_OTHER'
end
```

```lua
-- Script 2: Release Or Finalize Lock
-- KEYS[1]: idemp:{tenant_id}:{platform}:{event_id}
-- ARGV[1]: worker_id
-- ARGV[2]: action ("COMMIT_DONE" | "ABORT_RELEASE")
-- ARGV[3]: done_ttl_seconds (e.g., 86400 - 24h)

local val = redis.call('GET', KEYS[1])
if val == ('LOCKED:' .. ARGV[1]) then
    if ARGV[2] == 'COMMIT_DONE' then
        redis.call('SET', KEYS[1], 'DONE', 'EX', tonumber(ARGV[3]))
        return 1
    else
        redis.call('DEL', KEYS[1])
        return 1
    end
end
return 0
```

### 4.4. Shuffle Sharding Router (Power of Two Choices)
Để chống hiện tượng va chạm mã băm (Hash Collision) khi 1 shop nhỏ vô tình rơi vào chung shard với 1 shop chạy Flash Sale:
1. Hệ thống duy trì $N = 16$ Redis Streams (`webhooks:stream:0` -> `webhooks:stream:15`).
2. Từ `tenant_id`, dùng 2 hàm băm độc lập (`murmurhash3_x86_32` với 2 seed khác nhau) để chọn ra 2 shard ứng viên: `shardA` và `shardB`.
3. Gateway đọc nhanh độ dài hiện tại (`XLEN`) của `shardA` và `shardB` (được cache cục bộ mỗi 100ms), và đẩy message vào **shard nào đang có hàng đợi ngắn hơn**.
4. Về mặt toán học, *Power of Two Choices* giảm độ lệch tải tối đa từ $O(\frac{\log N}{\log \log N})$ xuống $O(\log \log N)$, triệt tiêu hoàn toàn điểm nghẽn đơn lẻ.

---

## 5. Platform Adapters Specification

Mỗi nền tảng thương mại điện tử có cơ chế bảo mật và cấu trúc dữ liệu hoàn toàn khác biệt. Hệ thống sử dụng **Strategy Pattern** (`WebhookPlatformAdapter`) để chuẩn hóa ngay từ cửa ngõ:

| Tiêu chí | Shopify Adapter | TikTok Shop Adapter | Shopee Adapter |
| :--- | :--- | :--- | :--- |
| **Signature Header** | `X-Shopify-Hmac-Sha256` | `Authorization` (hoặc query `sign`) | `Authorization` |
| **Thuật toán Hash** | `HMAC-SHA256(rawBody, secret)` -> **Base64** | `HMAC-SHA256(secret + path + sorted_params + rawBody + secret, secret)` -> **Hex** | `HMAC-SHA256(webhookUrl + "\|" + rawBody, secret)` -> **Hex** |
| **So sánh Chữ ký** | `crypto.timingSafeEqual` | `crypto.timingSafeEqual` | `crypto.timingSafeEqual` |
| **Replay Protection** | Không hỗ trợ timestamp header -> Chặn sớm bằng `X-Shopify-Webhook-Id` trên Redis | Kiểm tra trường `timestamp` trong payload/query lệch `< 300s` so với giờ server | Kiểm tra trường `timestamp` trong payload lệch `< 300s` |
| **Event ID Nguồn** | Header `X-Shopify-Webhook-Id` | Field `tts_notification_id` | Field `msg_id` (hoặc băm SHA256 của `ordersn + status + update_time`) |

---

## 6. Phased Implementation Roadmap

### Phase 1: High-Speed Gateway, Multi-Tier Cache & Shuffle Sharding (Day 1 - 2)
* [ ] Khởi tạo dự án TypeScript (Node.js 22 LTS) + Fastify. Cấu hình Custom Content-Type Parser giữ nguyên `rawBody` (`Buffer`) để verify HMAC chính xác từng byte.
* [ ] Xây dựng **Multi-Tier Tenant Config Cache**:
  * L1 Cache: `lru-cache` trên RAM của tiến trình Node.js (Max 5,000 tenants, TTL 30s).
  * L2 Cache: Redis Hash (`tenant:config:{id}`, TTL 10 phút).
  * Invalidation: Lắng nghe kênh Redis Pub/Sub `channel:tenant:invalidate` để xóa L1 cache ngay khi merchant đổi secret key.
* [ ] Xây dựng 3 Platform Adapters (`ShopifyAdapter`, `TikTokAdapter`, `ShopeeAdapter`) kèm bộ Unit Test kiểm tra chữ ký thật và chống Timing Attack.
* [ ] Triển khai **Active Backpressure & Shuffle Sharding Producer**:
  * Kiểm tra chỉ số `used_memory` của Redis và tổng `XLEN` của cặp shard ứng viên. Nếu vượt ngưỡng an toàn (ví dụ: `XLEN > 50,000`), trả về `HTTP 503 Service Unavailable` kèm header `Retry-After: 30` để sàn giữ lại message, tuyệt đối không nhận vào rồi xóa bằng `MAXLEN`.

### Phase 2: Resilient Worker Pool, Idempotency & State Machine (Day 3 - 5)
* [ ] Xây dựng Consumer đọc từ 16 Redis Stream shards qua `XREADGROUP`.
* [ ] Tích hợp **ZSET Lease Semaphore** và **Two-Phase Idempotency Lock** (kết hợp Redis Lua + PostgreSQL `idempotency_keys` hash-partitioned).
* [ ] Triển khai bộ giải quyết **Out-of-Order & JSONB Hydration** trên bảng `entity_states`.
* [ ] Tích hợp **Per-Tenant Circuit Breaker** (dùng `opossum` kết hợp đồng bộ trạng thái lên Redis):
  * Nếu Downstream ERP của Tenant A lỗi quá 50% trong cửa sổ 20 requests -> Chuyển trạng thái mạch của Tenant A sang `OPEN` trong 30 giây.
  * Khi mạch `OPEN`, worker không gọi HTTP mà đẩy thẳng sự kiện vào `retry:zset` với độ trễ Exponential Backoff + Full Jitter:
    $$\text{delay} = \text{random}\left(0, \min(\text{cap}, \text{base} \times 2^{\text{attempt}})\right)$$
* [ ] Xây dựng 2 Background Daemons:
  1. **Zombie Recovery Loop (`XAUTOCLAIM`):** Chạy mỗi 15 giây, thu hồi các message nằm trong PEL quá `30,000ms` (do worker chết giữa chừng) để xử lý lại.
  2. **Safe Stream Trimmer:** Cắt dọn các message đã xử lý trên Redis Stream bằng `XTRIM key MINID ~ <min_delivered_id>` (chỉ xóa những message mà cả Consumer Group và Delayed Queue đã xác nhận hoàn tất).

### Phase 3: Observability, Chaos Testing & Benchmarks (Day 6 - 7)
* [ ] Xuất Prometheus Metrics tại `/metrics`:
  * `webhook_gateway_requests_total{platform, status}`
  * `webhook_gateway_latency_ms` (Histogram)
  * `webhook_stream_backlog_records{shard}` (Gauge)
  * `webhook_worker_processing_duration_ms{platform, outcome}`
  * `webhook_idempotency_hits_total{tier="redis_done|redis_inflight|postgres_conflict"}`
  * `webhook_out_of_order_resolved_total{resolution="hydrated_late_create|ignored_stale_update"}`
  * `webhook_tenant_circuit_breaker_state{tenant_id}`
* [ ] Đóng gói toàn bộ hệ thống vào `docker-compose.yml` và `Makefile` (1 lệnh `make up` khởi chạy toàn bộ: Gateway, Worker, Postgres 16, Redis 7, Mock Downstream ERP, Prometheus, Grafana).
* [ ] Viết bộ **Integration & Chaos Tests** tự động bằng `Testcontainers`:
  * *Kịch bản 1 (Duplicate Race Condition):* Bắn đồng thời 20 requests giống hệt nhau ở cùng 1 millisecond -> Chỉ 1 request gọi xuống Mock ERP, 19 requests bị chặn bởi Redis/PG Lock.
  * *Kịch bản 2 (Out-of-Order Hydration):* Bắn `ORDER_CANCELLED` trước, 2 giây sau bắn `ORDER_CREATED` -> Kiểm tra DB thấy `current_state = 'CANCELLED'`, `is_hydrated = true`, và `merged_payload` có đủ `line_items`.
  * *Kịch bản 3 (Worker Crash Recovery):* Giả lập worker crash ngay sau khi acquire Redis Lock nhưng trước khi `XACK` -> Kiểm tra sau 30s tiến trình `XAUTOCLAIM` nhặt lại và hoàn tất đơn hàng.
  * *Kịch bản 4 (Noisy Neighbor Isolation):* Bơm 10,000 webhooks của Tenant A (đồng thời ép Mock ERP của Tenant A delay 3,000ms/request) -> Bắn 50 webhooks của Tenant B -> Chứng minh độ trễ p99 của Tenant B vẫn dưới 200ms.

---

## 7. Architectural Decision Records (Full ADRs)

### ADR-0001: Bounded Durability Window via Redis Streams vs. Direct PostgreSQL Writes
* **Status:** Accepted
* **Context:** Cổng tiếp nhận webhook phải chịu được các đợt tăng vọt lưu lượng (Flash Sale spike) từ các sàn TMĐT và phản hồi trong vòng dưới 50ms để tránh bị sàn đánh dấu timeout. Nếu ghi trực tiếp vào PostgreSQL ở tầng Gateway, chi phí I/O và爭 chấp khóa (Lock contention) trên B-Tree Index sẽ làm nghẽn Connection Pool khi lưu lượng vượt quá vài ngàn RPS.
* **Decision:** Sử dụng Redis Streams với cấu hình `appendfsync everysec` làm bộ đệm tiếp nhận sơ cấp (Primary Ingestion Buffer), kết hợp cơ chế **Active Backpressure** và **Safe `MINID` Trimming**.
* **Trade-offs & Mitigations:**
  * *Đánh đổi:* Không tuyên bố "Zero Data Loss" tuyệt đối. Với `appendfsync everysec`, nếu máy chủ vật lý chứa Redis bị mất điện đột ngột, tối đa 1 giây dữ liệu đã trả về `202 Accepted` có thể bị mất. Việc bật `appendfsync always` bị từ chối vì làm giảm thông lượng ghi đi 80-90%.
  * *Giảm thiểu rủi ro:*
    1. Tuyệt đối không dùng `XADD ... MAXLEN` cắt tự động khi hàng đợi đầy. Nếu Redis đạt 80% RAM hoặc backlog vượt ngưỡng, Gateway trả về `HTTP 503` để sàn TMĐT kích hoạt cơ chế retry của họ.
    2. Chạy kèm một tiến trình **Reconciliation Poller** định kỳ gọi API danh sách đơn hàng của sàn để kéo bù các đơn bị thiếu trong cửa sổ 1 giây nếu xảy ra sự cố phần cứng.

### ADR-0002: Two-Phase Lease Locking & Partitioned Idempotency Schema
* **Status:** Accepted
* **Context:** Trong hệ thống phân tán, không tồn tại "Exactly-Once Delivery" qua mạng HTTP. Nếu chỉ dùng `SET key NX EX 60` trên Redis làm khóa chống trùng, khi worker bị crash giữa chừng, các bản retry trong vòng 60 giây tiếp theo sẽ bị từ chối nhầm. Ngược lại, nếu gộp bảng kiểm tra trùng lặp vào chung bảng `webhook_events` (được partition theo `RANGE (created_at)`), PostgreSQL sẽ bắt buộc khóa chính phải kèm cột `created_at`, làm mất khả năng chặn trùng lặp toàn cục.
* **Decision:**
  1. Thiết kế khóa hai pha trên Redis: Trạng thái `LOCKED:{worker_id}` (có Lease TTL ngắn 30s, tự giải phóng khi lỗi hoặc hết hạn) tách biệt hoàn toàn với trạng thái `DONE` (TTL 24h).
  2. Tách bảng `idempotency_keys` riêng biệt và partition theo `HASH (tenant_id)` để duy trì `PRIMARY KEY (tenant_id, platform, event_id)` chặt chẽ ở cấp độ Database.
  3. Truyền header `X-Idempotency-Key` xuống hệ thống ERP phía sau để hoàn thiện chuỗi Idempotency đầu-cuối (End-to-End).

### ADR-0003: Out-of-Order Event Resolution via State Weights & Bidirectional JSONB Merge
* **Status:** Accepted
* **Context:** Đồng hồ giữa các máy chủ của sàn TMĐT có thể bị lệch (Clock Skew), hoặc mạng định tuyến làm sự kiện cập nhật (`ORDER_CANCELLED` - chỉ chứa trạng thái hủy) đến trước sự kiện gốc (`ORDER_CREATED` - chứa danh sách hàng hóa và thông tin người mua).
* **Decision:** Không dùng timestamp đơn thuần để bỏ qua (drop) sự kiện cũ. Áp dụng máy trạng thái hữu hạn có trọng số (**Weighted Finite State Machine**) kết hợp phép gộp JSONB hai chiều (**Bidirectional JSONB Hydration**) ngay trong một câu lệnh `INSERT ... ON CONFLICT DO UPDATE` nguyên tử của PostgreSQL.
* **Consequences:** Đảm bảo trạng thái đơn hàng không bao giờ bị lùi ngược từ trạng thái kết thúc (`CANCELLED` không bao giờ lùi về `CREATED`), đồng thời các trường dữ liệu chi tiết đến muộn vẫn được bồi đắp đầy đủ vào bản ghi mà không ghi đè lên các trường trạng thái mới hơn.

### ADR-0004: Multi-Tenant Isolation via Shuffle Sharding & ZSET Lease Semaphores
* **Status:** Accepted
* **Context:** Rate Limit tại cổng chỉ chặn được lưu lượng vượt hạn mức, nhưng không chặn được việc một khách hàng hợp lệ có gói cước lớn đẩy hàng chục ngàn sự kiện vào hàng đợi khi Downstream ERP của chính họ đang phản hồi chậm (2-3 giây/request). Nếu dùng chung 1 hàng đợi FIFO hoặc chia shard tĩnh đơn giản (`hash(tenant) % N`), các khách hàng nhỏ cùng shard sẽ bị vạ lây.
* **Decision:**
  1. Sử dụng **Shuffle Sharding kết hợp Power of Two Choices**: Mỗi tenant được gán 2 shards cố định trong tổng số 16 shards; Gateway luôn chọn shard có độ dài hàng đợi (`XLEN`) thấp hơn tại thời điểm gửi.
  2. Sử dụng **ZSET Lease Semaphore** để giới hạn số lượng worker tối đa mà 1 tenant được phép chiếm dụng đồng thời, tự động thu hồi slot nếu worker bị crash.
  3. Khởi tạo **Circuit Breaker độc lập cho từng Tenant** để ngắt mạch cô lập ngay lập tức khách hàng nào có hệ thống ERP bị sập.

---

## 8. Repository Structure

```text
webhook-ingestion-engine/
├── .github/
│   └── workflows/
│       └── ci.yml                        # Chạy Lint, Typecheck, Unit Test & Testcontainers Integration Test
├── docs/
│   ├── adr/
│   │   ├── 0001-bounded-durability-redis-streams.md
│   │   ├── 0002-two-phase-idempotency-and-partitioning.md
│   │   ├── 0003-out-of-order-bidirectional-hydration.md
│   │   └── 0004-shuffle-sharding-and-tenant-isolation.md
│   └── architecture-diagram.svg
├── infra/
│   ├── postgres/
│   │   └── init.sql                      # Schema 3 bảng Partitioned + Indexes
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       └── dashboards/
│           └── ingestion-overview.json   # Dashboard dựng sẵn
├── benchmarks/
│   ├── k6-flash-sale-spike.js            # Script giả lập 5,000 RPS kèm chữ ký HMAC thật
│   ├── k6-noisy-neighbor.js              # Script kiểm chứng cô lập tenant
│   └── RESULTS.md                        # Nơi lưu kết quả đo đạc thực tế từ máy chạy
├── src/
│   ├── config/
│   ├── gateway/
│   │   ├── adapters/                     # ShopifyAdapter, TikTokAdapter, ShopeeAdapter
│   │   ├── middlewares/                  # RawBody, Backpressure, RateLimiter
│   │   ├── router/                       # ShuffleShardingRouter (Power of Two Choices)
│   │   └── server.ts
│   ├── worker/
│   │   ├── consumers/                    # StreamConsumer & XAUTOCLAIM Recovery Loop
│   │   ├── locks/                        # Lua Scripts: ZSET Semaphore & Two-Phase Idempotency
│   │   ├── state-machine/                # Out-of-order Upsert & JSONB Hydration Engine
│   │   ├── resilience/                   # Per-Tenant Circuit Breaker & Jitter Backoff Scheduler
│   │   └── trimmer/                      # Safe MINID Stream Trimmer
│   ├── shared/
│   │   ├── cache/                        # L1 LRU + L2 Redis + Pub/Sub Invalidation
│   │   ├── db/                           # pg Pool & Transaction Helpers
│   │   └── observability/                # Pino Logger & Prometheus Metrics Registry
│   └── mock-downstream/                  # Server ERP giả lập có thể bơm lỗi/độ trễ theo tenant
├── tests/
│   ├── unit/
│   └── integration/                      # Chạy trên Postgres & Redis thật qua Testcontainers
│       ├── idempotency-race.test.ts
│       ├── out-of-order-hydration.test.ts
│       ├── worker-crash-xautoclaim.test.ts
│       └── noisy-neighbor-isolation.test.ts
├── docker-compose.yml
├── Makefile
├── package.json
├── tsconfig.json
├── PLAN.md
└── README.md
```

---

## 9. Definition of Done (Verification Checklist)

Trước khi ghim repo này lên trang cá nhân GitHub, dự án phải vượt qua 4 bài kiểm tra thực tế sau:
1. **Zero-Config Boot:** Chạy `make up` trên một máy trắng chỉ có Docker -> Toàn bộ hệ thống (Gateway, Worker, Postgres 16, Redis 7, Mock ERP, Prometheus, Grafana) khởi chạy thành công trong vòng 30 giây.
2. **Automated Chaos Suite:** Chạy `make test:integration` -> Pass toàn bộ 4 kịch bản khó nhất (Race condition 20 luồng đồng thời, Out-of-order hydration, Worker crash giữa chừng được cứu bởi `XAUTOCLAIM`, và Noisy neighbor isolation).
3. **Reproducible Benchmark:** Chạy `make benchmark` -> Script `k6` chạy trong 2 phút, in ra terminal chỉ số thực tế (p95, p99 latency, RPS, tỷ lệ lỗi) và đối chiếu khớp 100% số lượng bản ghi trong bảng `entity_states` của PostgreSQL.
4. **Clean Git History:** Lịch sử commit được chia nhỏ theo từng Phase/Feature chuẩn Conventional Commits (`feat(gateway): ...`, `feat(worker): ...`, `test(chaos): ...`), tuyệt đối không đẩy toàn bộ code trong 1 commit duy nhất.