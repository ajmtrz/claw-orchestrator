import { createHash } from 'node:crypto';
import type {
  AutoloopAgentRole,
  AutoloopPhase,
  PhysicalAgentGeneration,
  RecoveryAgentEvidence,
  RecoveryActionSnapshot,
  RecoveryAssessment,
  RecoveryInput,
  RecoveryIterationEvidence,
  RecoveryReceipt,
  RecoveryReviewEnvelope,
} from './types.js';
import { types as nodeTypes } from 'node:util';
import { canonicalizeMessage, type AnyAutoloopMessage } from './messages.js';

type NextSafeAction = RecoveryAssessment['next_safe_action'];
type AssessmentWithoutToken = Omit<RecoveryAssessment, 'recovery_token'>;

const CODER_ARTIFACTS = ['coder_summary', 'eval_output', 'diff'] as const;

function compareStrings(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function canonicalJson(value: unknown): string {
  const serialized = JSON.stringify(value, (_key, nestedValue: unknown) => {
    if (nestedValue === null || Array.isArray(nestedValue) || typeof nestedValue !== 'object') {
      return nestedValue;
    }

    return Object.fromEntries(Object.entries(nestedValue).sort(([left], [right]) => compareStrings(left, right)));
  });

  if (serialized === undefined) throw new TypeError('Recovery token input is not JSON-serializable');
  return serialized;
}

/** Stable digest of the exact action envelope reconstructed from durable bytes. */
export function recoveryActionDigest(action: unknown): string {
  return createHash('sha256').update(canonicalJson(action), 'utf8').digest('hex');
}

/** Reconstruct Task-5's immutable routing identity for a recovered agent action. */
export function recoveryActionDispatchId(
  runId: string,
  action: Extract<RecoveryActionSnapshot, { type: 'directive' | 'review_request' }>,
): string {
  const identity = [runId, action.msg_id, action.iter, action.from, action.to, action.type];
  return `dispatch_${createHash('sha256').update(JSON.stringify(identity), 'utf8').digest('hex')}`;
}

/** Digest an exact Coder/Reviewer envelope using the transport field order. */
export function recoveryLogicalMessageSha256(
  envelope: Extract<AnyAutoloopMessage, { type: 'directive' | 'review_request' }>,
): string {
  const canonical = canonicalizeMessage(envelope) as Extract<
    AnyAutoloopMessage,
    { type: 'directive' | 'review_request' }
  >;
  return createHash('sha256')
    .update(
      JSON.stringify({
        msg_id: canonical.msg_id,
        iter: canonical.iter,
        from: canonical.from,
        to: canonical.to,
        type: canonical.type,
        ts: canonical.ts,
        payload: canonical.payload,
      }),
      'utf8',
    )
    .digest('hex');
}

function sortEvidence(evidence: Iterable<string>): string[] {
  return [...new Set(evidence)].sort(compareStrings);
}

function sortAgents(agents: PhysicalAgentGeneration[]): PhysicalAgentGeneration[] {
  return agents.sort(
    (left, right) =>
      compareStrings(left.role, right.role) ||
      left.generation - right.generation ||
      compareStrings(left.session_name, right.session_name) ||
      compareStrings(left.owner_instance_id, right.owner_instance_id),
  );
}

function roleForPhase(phase: AutoloopPhase): AutoloopAgentRole | undefined {
  switch (phase) {
    case 'PLANNING':
      return 'planner';
    case 'AWAITING_CODER':
    case 'CODER_RUNNING':
      return 'coder';
    case 'AWAITING_REVIEW':
    case 'REVIEWER_RUNNING':
      return 'reviewer';
    case 'PAUSED_RECOVERABLE':
    case 'BLOCKED':
    case 'COMPLETED':
      return undefined;
  }
}

function latestIteration(input: RecoveryInput): RecoveryIterationEvidence | undefined {
  return [...input.iterations].sort((left, right) => right.iter - left.iter)[0];
}

function classifyGeneration(
  entry: RecoveryAgentEvidence,
  observedAt: number,
  evidence: string[],
): { generation: PhysicalAgentGeneration; blocksRecovery: boolean; newlyOrphaned: boolean } {
  const original = entry.generation;
  const key = `agent:${original.role}:${original.generation}`;
  const generation = { ...original };

  evidence.push(`${key}:runtime_${entry.matching_runtime}`);

  if (original.state === 'released' || original.state === 'orphaned') {
    evidence.push(`${key}:${original.state}`);
    return { generation, blocksRecovery: false, newlyOrphaned: false };
  }

  const leaseExpiresAt = Date.parse(original.lease_expires_at);
  if (Number.isNaN(observedAt) || Number.isNaN(leaseExpiresAt)) {
    generation.state = 'stale';
    evidence.push(`${key}:invalid_lease_evidence`);
    return { generation, blocksRecovery: true, newlyOrphaned: false };
  }

  const leaseExpired = leaseExpiresAt <= observedAt;
  evidence.push(`${key}:lease_${leaseExpired ? 'expired' : 'valid'}`);

  if (entry.matching_runtime === 'live') {
    generation.state = 'live';
    if (leaseExpired) evidence.push(`${key}:ownership_preserved`);
    return { generation, blocksRecovery: false, newlyOrphaned: false };
  }

  if (leaseExpired && entry.matching_runtime === 'absent') {
    generation.state = 'orphaned';
    evidence.push(`${key}:recoverable_orphan`);
    return { generation, blocksRecovery: false, newlyOrphaned: true };
  }

  generation.state = 'stale';
  evidence.push(`${key}:ownership_unresolved`);
  return { generation, blocksRecovery: true, newlyOrphaned: false };
}

function deriveLogicalPhase(
  input: RecoveryInput,
  iteration: RecoveryIterationEvidence | undefined,
  evidence: string[],
): { phase: AutoloopPhase; nextSafeAction: NextSafeAction } {
  if (input.completed) {
    evidence.push('run:completed');
    return { phase: 'COMPLETED', nextSafeAction: 'none' };
  }

  if (!iteration) {
    const legacy = input.legacy_state;
    if (legacy?.status === 'planning' && legacy.subagents_spawned !== true) {
      return { phase: 'PLANNING', nextSafeAction: 'resume_planner' };
    }
    evidence.push('ambiguity:legacy_state_without_durable_progress');
    return { phase: 'BLOCKED', nextSafeAction: 'manual_resolution' };
  }

  const artifacts = new Set(iteration.artifacts);
  const deliveries = input.deliveries.filter((delivery) => delivery.iter === iteration.iter);
  const coderDeliveries = deliveries.filter((delivery) => delivery.kind === 'coder_directive');
  const reviewDeliveries = deliveries.filter((delivery) => delivery.kind === 'review_request');
  const coderAcknowledged = coderDeliveries.some((delivery) => delivery.acknowledged);
  const reviewAcknowledged = reviewDeliveries.some((delivery) => delivery.acknowledged);
  const coderArtifactCount = CODER_ARTIFACTS.filter((artifact) => artifacts.has(artifact)).length;
  const coderArtifactsComplete = coderArtifactCount === CODER_ARTIFACTS.length;

  if (iteration.verdict) {
    const checkpointIter = input.legacy_state?.iter;
    if (checkpointIter !== undefined && checkpointIter > iteration.iter) {
      return { phase: 'PLANNING', nextSafeAction: 'resume_planner' };
    }
    if (checkpointIter === iteration.iter && reviewDeliveries.length > 0) {
      // The Reviewer persisted its canonical verdict before ACK and before the
      // Runner advanced the durable iteration checkpoint. Replaying the exact
      // review_request lets Task 5 reconcile the ACK and releases that same
      // verdict to the Runner without another Reviewer effect.
      evidence.push('recovery:review:verdict_unconsumed');
      return { phase: 'AWAITING_REVIEW', nextSafeAction: 'request_review' };
    }
    evidence.push(`ambiguity:iteration:${iteration.iter}:verdict_checkpoint_mismatch`);
    return { phase: 'BLOCKED', nextSafeAction: 'manual_resolution' };
  }

  if (reviewDeliveries.length > 0 && !coderArtifactsComplete) {
    evidence.push(`ambiguity:iteration:${iteration.iter}:review_delivery_without_complete_coder_artifacts`);
    return { phase: 'BLOCKED', nextSafeAction: 'manual_resolution' };
  }

  if (coderArtifactCount > 0 && !coderArtifactsComplete) {
    evidence.push(`ambiguity:iteration:${iteration.iter}:partial_coder_artifacts`);
    return { phase: 'BLOCKED', nextSafeAction: 'manual_resolution' };
  }

  if (coderArtifactsComplete) {
    if (!artifacts.has('directive')) {
      evidence.push(`ambiguity:iteration:${iteration.iter}:coder_artifacts_without_directive`);
      return { phase: 'BLOCKED', nextSafeAction: 'manual_resolution' };
    }
    return reviewAcknowledged
      ? { phase: 'REVIEWER_RUNNING', nextSafeAction: 'none' }
      : { phase: 'AWAITING_REVIEW', nextSafeAction: 'request_review' };
  }

  if (coderDeliveries.length > 0 && !artifacts.has('directive')) {
    evidence.push(`ambiguity:iteration:${iteration.iter}:coder_delivery_without_directive`);
    return { phase: 'BLOCKED', nextSafeAction: 'manual_resolution' };
  }

  if (artifacts.has('directive')) {
    return coderAcknowledged
      ? { phase: 'CODER_RUNNING', nextSafeAction: 'none' }
      : { phase: 'AWAITING_CODER', nextSafeAction: 'dispatch_coder' };
  }

  evidence.push(`ambiguity:iteration:${iteration.iter}:empty_durable_evidence`);
  return { phase: 'BLOCKED', nextSafeAction: 'manual_resolution' };
}

function buildAssessment(input: RecoveryInput): AssessmentWithoutToken {
  const evidence: string[] = [];
  const legacy = input.legacy_state;
  if (legacy?.status !== undefined) evidence.push(`legacy:status:${legacy.status}`);
  if (legacy?.iter !== undefined) evidence.push(`legacy:iter:${legacy.iter}`);
  if (legacy?.subagents_spawned !== undefined) {
    evidence.push(`legacy:subagents_spawned:${legacy.subagents_spawned}`);
  }
  if (legacy?.status_reason) evidence.push(`legacy:status_reason:${legacy.status_reason}`);

  for (const iteration of input.iterations) {
    for (const artifact of new Set(iteration.artifacts)) {
      evidence.push(`iteration:${iteration.iter}:artifact:${artifact}`);
    }
    if (iteration.verdict) evidence.push(`iteration:${iteration.iter}:verdict:${iteration.verdict}`);
  }

  for (const delivery of input.deliveries) {
    evidence.push(
      `delivery:${delivery.iter}:${delivery.kind}:${delivery.delivery_id}:${delivery.acknowledged ? 'acknowledged' : 'pending'}`,
    );
  }

  const iteration = latestIteration(input);
  let { phase, nextSafeAction } = deriveLogicalPhase(input, iteration, evidence);
  const logicalPhase = phase;
  const neededRole = roleForPhase(logicalPhase);
  const observedAt = Date.parse(input.observed_at);
  const classified = input.agents.map((entry) => classifyGeneration(entry, observedAt, evidence));
  const agents = sortAgents(classified.map(({ generation }) => generation));

  if (neededRole) {
    const relevant = classified
      .filter(({ generation }) => generation.role === neededRole && generation.state !== 'released')
      .sort((left, right) => right.generation.generation - left.generation.generation)[0];

    if (!relevant && (logicalPhase === 'CODER_RUNNING' || logicalPhase === 'REVIEWER_RUNNING')) {
      // An acknowledged durable delivery proves the logical dispatch, not a
      // surviving physical owner. A cold process must never be projected as a
      // running agent merely because its generation ledger is absent.
      evidence.push(`recovery:${neededRole}:no_live_generation`);
      phase = 'PAUSED_RECOVERABLE';
      nextSafeAction = neededRole === 'coder' ? 'dispatch_coder' : 'request_review';
    } else if (relevant?.blocksRecovery) {
      evidence.push(`ambiguity:agent:${neededRole}:${relevant.generation.generation}:ownership_unresolved`);
      phase = 'BLOCKED';
      nextSafeAction = 'manual_resolution';
    } else if (relevant?.newlyOrphaned && logicalPhase !== 'COMPLETED') {
      phase = 'PAUSED_RECOVERABLE';
      nextSafeAction =
        neededRole === 'planner' ? 'resume_planner' : neededRole === 'coder' ? 'dispatch_coder' : 'request_review';
    }
  }

  return {
    run_id: input.run_id,
    phase,
    evidence: sortEvidence(evidence),
    agents,
    pending_delivery_ids: [
      ...new Set(input.deliveries.filter((delivery) => !delivery.acknowledged).map((delivery) => delivery.delivery_id)),
    ].sort(compareStrings),
    next_safe_action: nextSafeAction,
    action_sha256:
      input.action_sha256 ??
      recoveryActionDigest({ kind: 'none', run_id: input.run_id, phase, next_safe_action: nextSafeAction }),
  };
}

function tokenForAssessment(assessment: AssessmentWithoutToken): string {
  return createHash('sha256')
    .update(canonicalJson({ schema_version: 1, ...assessment }))
    .digest('hex');
}

export function computeRecoveryToken(input: RecoveryInput): string {
  return tokenForAssessment(buildAssessment(input));
}

export function assessRecovery(input: RecoveryInput): RecoveryAssessment {
  const assessment = buildAssessment(input);
  return {
    ...assessment,
    recovery_token: tokenForAssessment(assessment),
  };
}

/**
 * Convert evidence that cannot safely reconstruct the selected recovery action
 * into a deterministic inspection result.  Inspection remains read-only while
 * apply has one stable, token-fenced manual-resolution outcome.
 */
export function blockRecoveryAssessment(assessment: RecoveryAssessment, evidence: string): RecoveryAssessment {
  const blocked: AssessmentWithoutToken = {
    ...assessment,
    phase: 'BLOCKED',
    evidence: sortEvidence([...assessment.evidence, evidence]),
    next_safe_action: 'manual_resolution',
  };
  return { ...blocked, recovery_token: tokenForAssessment(blocked) };
}

/** Rebind a recovery decision after durable runtime evidence proves it is already satisfied. */
export function rebindRecoveryAction(
  assessment: RecoveryAssessment,
  nextSafeAction: NextSafeAction,
  actionSha256: string,
  evidence: string,
): RecoveryAssessment {
  const rebound: AssessmentWithoutToken = {
    ...assessment,
    next_safe_action: nextSafeAction,
    action_sha256: actionSha256,
    evidence: sortEvidence([...assessment.evidence, evidence]),
  };
  return { ...rebound, recovery_token: tokenForAssessment(rebound) };
}

/** Parse only the versioned recovery receipt shape; other decision rows are ignored. */
export function parseRecoveryReceipt(value: unknown): RecoveryReceipt | undefined {
  if (
    typeof value !== 'object' ||
    value === null ||
    Array.isArray(value) ||
    nodeTypes.isProxy(value) ||
    !hasExactOwnDataFields(value, [
      'schema_version',
      'record_type',
      'kind',
      'run_id',
      'recovery_token',
      'action_sha256',
      'action_snapshot',
      'claim_id',
      'phase',
      'next_safe_action',
      'status',
      'recorded_at',
    ]) ||
    (value as Partial<RecoveryReceipt>).schema_version !== 1 ||
    (value as Partial<RecoveryReceipt>).record_type !== 'autoloop_recovery_receipt' ||
    (value as Partial<RecoveryReceipt>).kind !== 'autoloop_recovery_receipt' ||
    typeof (value as Partial<RecoveryReceipt>).run_id !== 'string' ||
    !(value as Partial<RecoveryReceipt>).run_id?.trim() ||
    (value as Partial<RecoveryReceipt>).run_id?.trim() !== (value as Partial<RecoveryReceipt>).run_id ||
    typeof (value as Partial<RecoveryReceipt>).recovery_token !== 'string' ||
    !/^[a-f0-9]{64}$/.test((value as Partial<RecoveryReceipt>).recovery_token ?? '') ||
    typeof (value as Partial<RecoveryReceipt>).action_sha256 !== 'string' ||
    !/^[a-f0-9]{64}$/.test((value as Partial<RecoveryReceipt>).action_sha256 ?? '') ||
    typeof (value as Partial<RecoveryReceipt>).claim_id !== 'string' ||
    !/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/.test((value as Partial<RecoveryReceipt>).claim_id ?? '') ||
    !Object.values({
      PLANNING: 'PLANNING',
      AWAITING_CODER: 'AWAITING_CODER',
      CODER_RUNNING: 'CODER_RUNNING',
      AWAITING_REVIEW: 'AWAITING_REVIEW',
      REVIEWER_RUNNING: 'REVIEWER_RUNNING',
      PAUSED_RECOVERABLE: 'PAUSED_RECOVERABLE',
      BLOCKED: 'BLOCKED',
      COMPLETED: 'COMPLETED',
    }).includes((value as Partial<RecoveryReceipt>).phase ?? '') ||
    !['none', 'resume_planner', 'dispatch_coder', 'request_review', 'manual_resolution'].includes(
      (value as Partial<RecoveryReceipt>).next_safe_action ?? '',
    ) ||
    ((value as Partial<RecoveryReceipt>).status !== 'prepared' &&
      (value as Partial<RecoveryReceipt>).status !== 'applied') ||
    typeof (value as Partial<RecoveryReceipt>).recorded_at !== 'string' ||
    Number.isNaN(Date.parse((value as Partial<RecoveryReceipt>).recorded_at ?? ''))
  ) {
    return undefined;
  }
  const candidate = value as RecoveryReceipt;
  const actionSnapshot = parseRecoveryActionSnapshot(candidate.action_snapshot);
  if (
    !actionSnapshot ||
    recoveryActionDigest(actionSnapshot) !== candidate.action_sha256 ||
    (actionSnapshot.iter !== undefined && actionSnapshot.iter < 0) ||
    (candidate.next_safe_action === 'dispatch_coder' && actionSnapshot.type !== 'directive') ||
    (candidate.next_safe_action === 'request_review' && actionSnapshot.type !== 'review_request') ||
    (candidate.next_safe_action === 'resume_planner' && actionSnapshot.type !== 'resume_planner') ||
    (candidate.next_safe_action === 'none' && actionSnapshot.type !== 'none') ||
    ((actionSnapshot.type === 'none' || actionSnapshot.type === 'resume_planner') &&
      (actionSnapshot.run_id !== candidate.run_id || actionSnapshot.phase !== candidate.phase)) ||
    candidate.next_safe_action === 'manual_resolution'
  ) {
    return undefined;
  }
  return { ...candidate, action_snapshot: actionSnapshot };
}

function hasExactOwnDataFields(value: object, expected: readonly string[]): boolean {
  if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) return false;
  const keys = Reflect.ownKeys(value);
  if (keys.length !== expected.length || keys.some((key) => typeof key !== 'string' || !expected.includes(key))) {
    return false;
  }
  return expected.every((key) => {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return !!descriptor && descriptor.enumerable === true && Object.hasOwn(descriptor, 'value');
  });
}

function parseRecoveryActionSnapshot(value: unknown): RecoveryActionSnapshot | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value) || nodeTypes.isProxy(value)) return undefined;
  const type = (value as { type?: unknown }).type;
  if (type === 'directive' || type === 'review_request') {
    let canonical: AnyAutoloopMessage;
    try {
      canonical = canonicalizeMessage(value as AnyAutoloopMessage);
    } catch {
      return undefined;
    }
    if (canonical.type !== type || JSON.stringify(canonical) !== JSON.stringify(value)) return undefined;
    return canonical;
  }
  if (
    (type !== 'none' && type !== 'resume_planner') ||
    !hasExactOwnDataFields(value, ['type', 'run_id', 'iter', 'phase']) ||
    typeof (value as { run_id?: unknown }).run_id !== 'string' ||
    !(value as { run_id: string }).run_id.trim() ||
    !Number.isSafeInteger((value as { iter?: unknown }).iter) ||
    (value as { iter: number }).iter < 0 ||
    ![
      'PLANNING',
      'AWAITING_CODER',
      'CODER_RUNNING',
      'AWAITING_REVIEW',
      'REVIEWER_RUNNING',
      'PAUSED_RECOVERABLE',
      'BLOCKED',
      'COMPLETED',
    ].includes(String((value as { phase?: unknown }).phase))
  ) {
    return undefined;
  }
  return { ...(value as Extract<RecoveryActionSnapshot, { type: 'none' | 'resume_planner' }>) };
}

/** Parse one strict, canonical, run-bound exact Reviewer recovery envelope. */
export function parseRecoveryReviewEnvelope(value: unknown): RecoveryReviewEnvelope | undefined {
  if (
    typeof value !== 'object' ||
    value === null ||
    Array.isArray(value) ||
    !hasExactOwnDataFields(value, ['schema_version', 'record_type', 'kind', 'run_id', 'envelope'])
  ) {
    return undefined;
  }
  const candidate = value as Partial<RecoveryReviewEnvelope>;
  if (
    candidate.schema_version !== 1 ||
    candidate.record_type !== 'autoloop_recovery_review_envelope' ||
    candidate.kind !== 'autoloop_recovery_review_envelope' ||
    typeof candidate.run_id !== 'string' ||
    !candidate.run_id.trim() ||
    candidate.run_id.trim() !== candidate.run_id
  ) {
    return undefined;
  }
  let envelope: AnyAutoloopMessage;
  try {
    envelope = canonicalizeMessage(candidate.envelope as AnyAutoloopMessage);
  } catch {
    return undefined;
  }
  if (
    envelope.type !== 'review_request' ||
    envelope.from !== 'runner' ||
    envelope.to !== 'reviewer' ||
    JSON.stringify(envelope) !== JSON.stringify(candidate.envelope)
  ) {
    return undefined;
  }
  return {
    schema_version: 1,
    record_type: 'autoloop_recovery_review_envelope',
    kind: 'autoloop_recovery_review_envelope',
    run_id: candidate.run_id,
    envelope,
  };
}
