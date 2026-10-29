-- ============================================================================
-- Webhook Ingestion Engine - Database Initialization Script
-- Target: PostgreSQL 16+
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- 0. BẢNG TENANTS (CẤU HÌNH MERCHANT)
-- ============================================================================
CREATE TABLE IF NOT EXISTS tenants (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    platform VARCHAR(32) NOT NULL, -- 'shopify', 'tiktok', 'shopee'
    webhook_secret VARCHAR(255) NOT NULL,
    rate_limit_rps INT NOT NULL DEFAULT 100,
    max_concurrency INT NOT NULL DEFAULT 10,
    downstream_url VARCHAR(512),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- 1. BẢNG KIỂM SOÁT IDEMPOTENCY TOÀN CỤC (Partitioned by HASH)
-- ============================================================================
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id UUID NOT NULL,
    platform VARCHAR(32) NOT NULL,
    event_id VARCHAR(128) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('IN_PROGRESS', 'PERSISTED', 'COMPLETED', 'FAILED', 'DLQ')),
    locked_by VARCHAR(64),
    locked_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, platform, event_id)
) PARTITION BY HASH (tenant_id);

CREATE TABLE IF NOT EXISTS idempotency_keys_p0 PARTITION OF idempotency_keys FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE IF NOT EXISTS idempotency_keys_p1 PARTITION OF idempotency_keys FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE IF NOT EXISTS idempotency_keys_p2 PARTITION OF idempotency_keys FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE IF NOT EXISTS idempotency_keys_p3 PARTITION OF idempotency_keys FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- ============================================================================
-- 2. BẢNG QUẢN LÝ TRẠNG THÁI THỰC THỂ & CHỐNG OUT-OF-ORDER (FSM + Hydration)
-- ============================================================================
CREATE TABLE IF NOT EXISTS entity_states (
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

CREATE INDEX IF NOT EXISTS idx_entity_states_updated ON entity_states (tenant_id, updated_at DESC);

-- ============================================================================
-- 3. BẢNG LƯU TRỮ LỊCH SỬ PAYLOAD & AUDIT LOG (Partitioned by RANGE)
-- ============================================================================
CREATE TABLE IF NOT EXISTS webhook_events (
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

-- Partitions cho năm 2026 (các tháng)
CREATE TABLE IF NOT EXISTS webhook_events_2026_10 PARTITION OF webhook_events
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS webhook_events_2026_11 PARTITION OF webhook_events
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS webhook_events_2026_12 PARTITION OF webhook_events
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');

CREATE INDEX IF NOT EXISTS idx_webhook_events_lookup 
    ON webhook_events (tenant_id, platform, event_id, created_at);

CREATE INDEX IF NOT EXISTS idx_webhook_events_dlq 
    ON webhook_events (tenant_id, created_at) 
    WHERE delivery_status = 'DLQ';

-- ============================================================================
-- SEED SAMPLE TENANTS CHO LOCAL DEVELOPMENT ONLY
-- WARNING: DO NOT USE THESE HARDCODED KEYS IN PRODUCTION ENVIRONMENTS.
-- ============================================================================
INSERT INTO tenants (id, name, platform, webhook_secret, rate_limit_rps, max_concurrency, downstream_url)
VALUES 
    ('a0000000-0000-0000-0000-000000000001', 'Shopify Merchant Alpha', 'shopify', 'shpss_sec_secret_alpha_key_9912', 150, 10, 'http://localhost:4000/downstream/alpha'),
    ('b0000000-0000-0000-0000-000000000002', 'TikTok Shop Beta', 'tiktok', 'tt_secret_beta_key_7781', 100, 10, 'http://localhost:4000/downstream/beta'),
    ('c0000000-0000-0000-0000-000000000003', 'Shopee Store Gamma', 'shopee', 'shopee_secret_gamma_key_1123', 200, 15, 'http://localhost:4000/downstream/gamma')
ON CONFLICT (id) DO NOTHING;

