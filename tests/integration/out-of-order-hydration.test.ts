import { describe, it, expect } from 'vitest';
import { simulateOccMerge } from '../../src/worker/state-machine/occ-state.js';

describe('Integration: End-to-End Out-of-Order Hydration & State Machine', () => {
  it('correctly hydrates late CREATED event without resurrecting CANCELLED order', () => {
    const orderId = 'ORD_E2E_4412';

    // Step 1: Out-of-order CANCELLED arrives first
    const eventCancelled = {
      currentState: 'CANCELLED',
      stateWeight: 99,
      externalVersion: 1761720300000, // 10:05 AM
      isHydrated: false,
      payload: {
        order_id: orderId,
        status: 'CANCELLED',
        cancel_reason: 'fraud_check_failed',
      },
    };

    const initialDbState = simulateOccMerge(null, eventCancelled);
    expect(initialDbState.currentState).toBe('CANCELLED');
    expect(initialDbState.isHydrated).toBe(false);

    // Step 2: Delayed CREATED arrives later
    const eventCreated = {
      currentState: 'CREATED',
      stateWeight: 10,
      externalVersion: 1761720000000, // 10:00 AM
      isHydrated: true,
      payload: {
        order_id: orderId,
        status: 'CREATED',
        customer: { name: 'Bob Smith', email: 'bob@example.com' },
        line_items: [
          { sku: 'LAPTOP-PRO', title: 'Laptop Pro', price: '1999.00', quantity: 1 }
        ],
        shipping_address: '456 Market St, San Francisco, CA',
      },
    };

    const mergedDbState = simulateOccMerge(
      {
        currentState: initialDbState.currentState,
        stateWeight: 99,
        externalVersion: 1761720300000,
        isHydrated: initialDbState.isHydrated,
        mergedPayload: initialDbState.mergedPayload,
      },
      eventCreated
    );

    // Verify: Final State remains CANCELLED
    expect(mergedDbState.currentState).toBe('CANCELLED');
    expect(mergedDbState.isHydrated).toBe(true);

    // Verify: Fields from CREATED were successfully hydrated into CANCELLED record
    expect(mergedDbState.mergedPayload.customer.name).toBe('Bob Smith');
    expect(mergedDbState.mergedPayload.line_items).toHaveLength(1);
    expect(mergedDbState.mergedPayload.shipping_address).toBe('456 Market St, San Francisco, CA');
    expect(mergedDbState.mergedPayload.cancel_reason).toBe('fraud_check_failed');
  });
});

