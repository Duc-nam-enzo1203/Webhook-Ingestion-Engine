import { describe, it, expect } from 'vitest';
import { 
  TenantCircuitBreaker, 
  calculateExponentialBackoffJitter 
} from '../../src/worker/resilience/circuit-breaker.js';

describe('Tenant Circuit Breaker & Exponential Backoff', () => {
  it('starts in CLOSED state', () => {
    const cb = new TenantCircuitBreaker('tenant-1', {
      errorThresholdPercentage: 50,
      rollingWindowSize: 10,
    });
    expect(cb.getState()).toBe('CLOSED');
  });

  it('trips to OPEN when error rate exceeds threshold in rolling window', () => {
    const cb = new TenantCircuitBreaker('tenant-2', {
      errorThresholdPercentage: 50,
      rollingWindowSize: 10,
    });

    // 4 successes, 6 failures -> 60% error rate
    for (let i = 0; i < 4; i++) cb.recordSuccess();
    for (let i = 0; i < 6; i++) cb.recordFailure();

    expect(cb.getState()).toBe('OPEN');
  });

  it('stays CLOSED when error rate is within threshold', () => {
    const cb = new TenantCircuitBreaker('tenant-3', {
      errorThresholdPercentage: 50,
      rollingWindowSize: 10,
    });

    // 8 successes, 2 failures -> 20% error rate
    for (let i = 0; i < 8; i++) cb.recordSuccess();
    for (let i = 0; i < 2; i++) cb.recordFailure();

    expect(cb.getState()).toBe('CLOSED');
  });

  it('calculateExponentialBackoffJitter generates bounded randomized delay', () => {
    for (let attempt = 0; attempt < 5; attempt++) {
      const delay = calculateExponentialBackoffJitter(attempt, 1000, 30000);
      const maxExpected = Math.min(30000, 1000 * Math.pow(2, attempt));
      expect(delay).toBeGreaterThanOrEqual(0);
      expect(delay).toBeLessThanOrEqual(maxExpected);
    }
  });
});

