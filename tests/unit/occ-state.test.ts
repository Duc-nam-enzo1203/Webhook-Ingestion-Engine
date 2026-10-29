import { describe, it, expect } from 'vitest';
import { simulateOccMerge } from '../../src/worker/state-machine/occ-state.js';

describe('Out-of-Order OCC State Machine & Bidirectional JSONB Hydration', () => {
  it('handles initial event creation (ORDER_CREATED)', () => {
    const incoming = {
      currentState: 'CREATED',
      stateWeight: 10,
      externalVersion: 1761720000000,
      isHydrated: true,
      payload: {
        order_id: 'ORD_1001',
        status: 'OPEN',
        line_items: [{ id: 1, name: 'Item 1' }],
        customer: { name: 'Alice' },
      },
    };

    const result = simulateOccMerge(null, incoming);
    expect(result.currentState).toBe('CREATED');
    expect(result.isHydrated).toBe(true);
    expect(result.isInserted).toBe(true);
    expect(result.mergedPayload.line_items).toHaveLength(1);
    expect(result.mergedPayload.customer.name).toBe('Alice');
  });

  it('progresses state forward normally (CREATED -> PAID -> SHIPPED)', () => {
    const existing = {
      currentState: 'CREATED',
      stateWeight: 10,
      externalVersion: 1761720000000,
      isHydrated: true,
      mergedPayload: {
        order_id: 'ORD_1001',
        status: 'CREATED',
        line_items: [{ id: 1, name: 'Item 1' }],
      },
    };

    const incomingPaid = {
      currentState: 'PAID',
      stateWeight: 20,
      externalVersion: 1761720060000,
      isHydrated: false,
      payload: {
        order_id: 'ORD_1001',
        status: 'PAID',
        payment_method: 'credit_card',
      },
    };

    const result = simulateOccMerge(existing, incomingPaid);
    expect(result.currentState).toBe('PAID');
    expect(result.isHydrated).toBe(true); // Retains hydrated from existing
    expect(result.isStateAdvanced).toBe(true);
    expect(result.mergedPayload.status).toBe('PAID'); // Newer status overwrites
    expect(result.mergedPayload.payment_method).toBe('credit_card');
    expect(result.mergedPayload.line_items).toHaveLength(1); // Retains line_items
  });

  it('CRITICAL TEST: ORDER_CANCELLED arrives BEFORE ORDER_CREATED (Out-of-Order Resolution)', () => {
    // 1. ORDER_CANCELLED arrives first (Weight 99, only partial cancel payload)
    const incomingCancelled = {
      currentState: 'CANCELLED',
      stateWeight: 99,
      externalVersion: 1761720300000, // 10:05 AM
      isHydrated: false,
      payload: {
        order_id: 'ORD_9999',
        status: 'CANCELLED',
        cancel_reason: 'fraudulent_charge',
      },
    };

    const stateAfterCancel = simulateOccMerge(null, incomingCancelled);
    expect(stateAfterCancel.currentState).toBe('CANCELLED');
    expect(stateAfterCancel.isHydrated).toBe(false);

    // 2. ORDER_CREATED arrives 2 seconds later due to distributed network delay
    const incomingCreated = {
      currentState: 'CREATED',
      stateWeight: 10,
      externalVersion: 1761720000000, // 10:00 AM (older timestamp)
      isHydrated: true,
      payload: {
        order_id: 'ORD_9999',
        status: 'CREATED',
        line_items: [{ id: 88, name: 'MacBook Pro M4', price: 2499 }],
        shipping_address: '123 Tech Way',
      },
    };

    // 3. Merge late-arriving CREATED into already CANCELLED record
    const finalResolution = simulateOccMerge(
      {
        currentState: stateAfterCancel.currentState,
        stateWeight: 99,
        externalVersion: 1761720300000,
        isHydrated: stateAfterCancel.isHydrated,
        mergedPayload: stateAfterCancel.mergedPayload,
      },
      incomingCreated
    );

    // Assert: Order does NOT resurrect to CREATED
    expect(finalResolution.currentState).toBe('CANCELLED');
    expect(finalResolution.isStateAdvanced).toBe(false);
    expect(finalResolution.isStaleIgnored).toBe(true);
    expect(finalResolution.isNewlyHydrated).toBe(true); // First time hydration occurred!

    // Subsequent arrival of the same hydrated event on already-hydrated entity:
    const duplicateLate = simulateOccMerge(
      {
        currentState: finalResolution.currentState,
        stateWeight: 99,
        externalVersion: 1761720300000,
        isHydrated: finalResolution.isHydrated,
        mergedPayload: finalResolution.mergedPayload,
      },
      incomingCreated
    );
    // Must NOT be newly hydrated again
    expect(duplicateLate.isNewlyHydrated).toBe(false);

    // Assert: Order is now successfully HYDRATED with details
    expect(finalResolution.isHydrated).toBe(true);

    // Assert: Merged payload contains both cancel reason AND line_items
    expect(finalResolution.mergedPayload.cancel_reason).toBe('fraudulent_charge');
    expect(finalResolution.mergedPayload.shipping_address).toBe('123 Tech Way');
    expect(finalResolution.mergedPayload.line_items).toHaveLength(1);
    expect(finalResolution.mergedPayload.line_items[0].name).toBe('MacBook Pro M4');

    // Assert: High-priority status 'CANCELLED' was NOT overwritten by older 'CREATED'
    expect(finalResolution.mergedPayload.status).toBe('CANCELLED');
  });
});

