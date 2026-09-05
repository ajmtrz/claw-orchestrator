import { createHash } from 'node:crypto';
import type {
  AutoloopAgentRole,
  AutoloopPhase,
  PhysicalAgentGeneration,
  RecoveryAgentEvidence,
  RecoveryAssessment,
  RecoveryInput,
  RecoveryIterationEvidence,
} from './types.js';

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
    return { phase: 'PLANNING', nextSafeAction: 'resume_planner' };
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

    if (relevant?.blocksRecovery) {
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
