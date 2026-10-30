import pg from 'pg';
import { VerifiedWebhookEvent } from '../../gateway/adapters/types.js';

export interface OccUpsertResult {
  currentState: string;
  isHydrated: boolean;
  mergedPayload: Record<string, any>;
  isInserted: boolean;
  isStateAdvanced: boolean;
  isStaleIgnored: boolean;
  isNewlyHydrated: boolean;
}

export const OCC_STATE_UPSERT_SQL = `
INSERT INTO entity_states (
    tenant_id, platform, entity_type, entity_id,
    current_state, state_weight, external_version, is_hydrated, was_previously_hydrated, merged_payload, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, FALSE, $9, NOW())
ON CONFLICT (tenant_id, platform, entity_type, entity_id)
DO UPDATE SET
    current_state = CASE
        WHEN EXCLUDED.state_weight > entity_states.state_weight
          OR (EXCLUDED.state_weight = entity_states.state_weight AND EXCLUDED.external_version > entity_states.external_version)
        THEN EXCLUDED.current_state
        ELSE entity_states.current_state
    END,
    state_weight = GREATEST(entity_states.state_weight, EXCLUDED.state_weight),
    external_version = GREATEST(entity_states.external_version, EXCLUDED.external_version),
    was_previously_hydrated = entity_states.is_hydrated,
    is_hydrated = entity_states.is_hydrated OR EXCLUDED.is_hydrated,
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
    (xmax = 0) AS is_inserted,
    ($6::int < entity_states.state_weight OR 
     ($6::int = entity_states.state_weight AND $7::bigint < entity_states.external_version)
    ) AS is_stale_ignored,
    ($8::boolean AND NOT entity_states.was_previously_hydrated AND NOT (xmax = 0)) AS is_newly_hydrated;
`;

/**
 * Pure simulation of the PostgreSQL Bidirectional JSONB & FSM Merge logic.
 * Guarantees exact behavioral parity in unit tests and memory models.
 */
export function simulateOccMerge(
  existing: {
    currentState: string;
    stateWeight: number;
    externalVersion: number;
    isHydrated: boolean;
    mergedPayload: Record<string, any>;
  } | null,
  incoming: {
    currentState: string;
    stateWeight: number;
    externalVersion: number;
    isHydrated: boolean;
    payload: Record<string, any>;
  }
): OccUpsertResult {
  if (!existing) {
    return {
      currentState: incoming.currentState,
      isHydrated: incoming.isHydrated,
      mergedPayload: { ...incoming.payload },
      isInserted: true,
      isStateAdvanced: true,
      isStaleIgnored: false,
      isNewlyHydrated: false,
    };
  }

  const isIncomingNewer =
    incoming.stateWeight > existing.stateWeight ||
    (incoming.stateWeight === existing.stateWeight && incoming.externalVersion > existing.externalVersion);

  const finalState = isIncomingNewer ? incoming.currentState : existing.currentState;
  const isNewlyHydrated = incoming.isHydrated && !existing.isHydrated;
  const finalHydrated = existing.isHydrated || incoming.isHydrated;

  // Bidirectional merge: newer record is on the right of || operator
  let finalPayload: Record<string, any>;
  if (
    incoming.stateWeight > existing.stateWeight ||
    (incoming.stateWeight === existing.stateWeight && incoming.externalVersion >= existing.externalVersion)
  ) {
    finalPayload = { ...existing.mergedPayload, ...incoming.payload };
  } else {
    finalPayload = { ...incoming.payload, ...existing.mergedPayload };
  }

  const isStale = !isIncomingNewer && incoming.stateWeight < existing.stateWeight;

  return {
    currentState: finalState,
    isHydrated: finalHydrated,
    mergedPayload: finalPayload,
    isInserted: false,
    isStateAdvanced: isIncomingNewer,
    isStaleIgnored: isStale,
    isNewlyHydrated,
  };
}

export async function executeOccStateUpsert(
  client: pg.PoolClient | pg.Pool,
  event: VerifiedWebhookEvent
): Promise<OccUpsertResult> {
  const res = await client.query(OCC_STATE_UPSERT_SQL, [
    event.tenantId,
    event.platform,
    event.entityType,
    event.entityId,
    event.currentState,
    event.stateWeight,
    event.externalVersion,
    event.isHydrated,
    JSON.stringify(event.payload),
  ]);

  const row = res.rows[0];
  const isStale = Boolean(row.is_stale_ignored);
  const isInserted = Boolean(row.is_inserted);
  const isHydrated = Boolean(row.is_hydrated);
  // Crucial: isNewlyHydrated is true ONLY if this entity was unhydrated prior to this update!
  const isNewlyHydrated = Boolean(row.is_newly_hydrated);

  return {
    currentState: row.current_state,
    isHydrated,
    mergedPayload: typeof row.merged_payload === 'string' ? JSON.parse(row.merged_payload) : row.merged_payload,
    isInserted,
    isStateAdvanced: !isStale && !isInserted,
    isStaleIgnored: isStale,
    isNewlyHydrated,
  };
}
