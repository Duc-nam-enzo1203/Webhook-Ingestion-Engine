import client from 'prom-client';

// Initialize default Node.js runtime metrics (CPU, RAM, Event Loop)
export const metricsRegistry = new client.Registry();
client.collectDefaultMetrics({ register: metricsRegistry, prefix: 'webhook_engine_' });

export const gatewayRequestsTotal = new client.Counter({
  name: 'webhook_gateway_requests_total',
  help: 'Total number of webhook ingestion requests received at Gateway',
  labelNames: ['platform', 'status'],
  registers: [metricsRegistry],
});

export const gatewayLatencyMs = new client.Histogram({
  name: 'webhook_gateway_latency_ms',
  help: 'Latency of gateway handling in milliseconds',
  labelNames: ['platform'],
  buckets: [2, 5, 10, 15, 25, 50, 100, 250, 500, 1000],
  registers: [metricsRegistry],
});

export const streamBacklogRecords = new client.Gauge({
  name: 'webhook_stream_backlog_records',
  help: 'Number of pending records in Redis stream shard',
  labelNames: ['shard'],
  registers: [metricsRegistry],
});

export const workerProcessingDurationMs = new client.Histogram({
  name: 'webhook_worker_processing_duration_ms',
  help: 'Processing latency in worker pool',
  labelNames: ['platform', 'outcome'],
  buckets: [10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
  registers: [metricsRegistry],
});

export const idempotencyHitsTotal = new client.Counter({
  name: 'webhook_idempotency_hits_total',
  help: 'Total duplicate events filtered by tier',
  labelNames: ['tier'], // 'redis_done', 'redis_inflight', 'postgres_conflict'
  registers: [metricsRegistry],
});

export const outOfOrderResolvedTotal = new client.Counter({
  name: 'webhook_out_of_order_resolved_total',
  help: 'Total out-of-order events handled by resolution strategy',
  labelNames: ['resolution'], // 'hydrated_late_create', 'ignored_stale_update', 'normal_progression'
  registers: [metricsRegistry],
});

export const tenantCircuitBreakerState = new client.Gauge({
  name: 'webhook_tenant_circuit_breaker_state',
  help: 'Circuit breaker status per tenant (0=CLOSED, 1=HALF_OPEN, 2=OPEN)',
  labelNames: ['tenant_id'],
  registers: [metricsRegistry],
});

export const dlqDepth = new client.Gauge({
  name: 'webhook_dlq_depth',
  help: 'Number of failed events currently moved to Dead-Letter Queue',
  registers: [metricsRegistry],
});

