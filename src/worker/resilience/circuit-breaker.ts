import { tenantCircuitBreakerState } from '../../shared/observability/metrics.js';

export type CircuitState = 'CLOSED' | 'HALF_OPEN' | 'OPEN';

export interface CircuitBreakerConfig {
  errorThresholdPercentage: number; // e.g. 50%
  rollingWindowSize: number;         // e.g. 20 requests
  resetTimeoutMs: number;            // e.g. 30000ms
}

export class TenantCircuitBreaker {
  private state: CircuitState = 'CLOSED';
  private failures = 0;
  private successes = 0;
  private totalRequests = 0;
  private lastStateChangedAt = Date.now();
  private readonly config: CircuitBreakerConfig;
  readonly tenantId: string;

  constructor(tenantId: string, config?: Partial<CircuitBreakerConfig>) {
    this.tenantId = tenantId;
    this.config = {
      errorThresholdPercentage: config?.errorThresholdPercentage ?? 50,
      rollingWindowSize: config?.rollingWindowSize ?? 20,
      resetTimeoutMs: config?.resetTimeoutMs ?? 30000,
    };
    this.updateMetric();
  }

  getState(): CircuitState {
    const now = Date.now();
    if (this.state === 'OPEN' && now - this.lastStateChangedAt > this.config.resetTimeoutMs) {
      this.state = 'HALF_OPEN';
      this.lastStateChangedAt = now;
      this.updateMetric();
    }
    return this.state;
  }

  recordSuccess(): void {
    if (this.state === 'HALF_OPEN') {
      this.state = 'CLOSED';
      this.failures = 0;
      this.successes = 0;
      this.totalRequests = 0;
      this.lastStateChangedAt = Date.now();
      this.updateMetric();
      return;
    }

    this.successes++;
    this.totalRequests++;
    this.trimWindow();
  }

  recordFailure(): void {
    if (this.state === 'HALF_OPEN') {
      this.state = 'OPEN';
      this.lastStateChangedAt = Date.now();
      this.updateMetric();
      return;
    }

    this.failures++;
    this.totalRequests++;

    if (this.totalRequests >= this.config.rollingWindowSize) {
      const errorRate = (this.failures / this.totalRequests) * 100;
      if (errorRate >= this.config.errorThresholdPercentage) {
        this.state = 'OPEN';
        this.lastStateChangedAt = Date.now();
        this.updateMetric();
      }
      this.trimWindow();
    }
  }

  private trimWindow(): void {
    if (this.totalRequests > this.config.rollingWindowSize * 2) {
      this.failures = Math.floor(this.failures / 2);
      this.successes = Math.floor(this.successes / 2);
      this.totalRequests = this.failures + this.successes;
    }
  }

  private updateMetric(): void {
    const code = this.state === 'CLOSED' ? 0 : this.state === 'HALF_OPEN' ? 1 : 2;
    tenantCircuitBreakerState.set({ tenant_id: this.tenantId }, code);
  }
}

/**
 * Calculates exponential backoff with full jitter:
 * delay = floor(random() * min(cap, base * 2^attempt))
 */
export function calculateExponentialBackoffJitter(
  attempt: number,
  baseMs = 1000,
  capMs = 60000
): number {
  const exponential = Math.min(capMs, baseMs * Math.pow(2, attempt));
  return Math.floor(Math.random() * exponential);
}

// Global in-memory registry of Tenant Circuit Breakers
const breakers = new Map<string, TenantCircuitBreaker>();

export function getTenantCircuitBreaker(tenantId: string): TenantCircuitBreaker {
  let breaker = breakers.get(tenantId);
  if (!breaker) {
    breaker = new TenantCircuitBreaker(tenantId);
    breakers.set(tenantId, breaker);
  }
  return breaker;
}

