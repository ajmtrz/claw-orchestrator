/**
 * Runner-level types for autoloop (three-agent architecture).
 */

import type {
  AnyAutoloopMessage,
  AutoloopOperationErrorCode,
  PushChannel,
  PushLevel,
  SendTimeoutPayload,
} from './messages.js';

export type AutoloopStatus = 'planning' | 'running' | 'paused' | 'terminated' | 'crashed';

export type AutoloopPhase =
  | 'PLANNING'
  | 'AWAITING_CODER'
  | 'CODER_RUNNING'
  | 'AWAITING_REVIEW'
  | 'REVIEWER_RUNNING'
  | 'PAUSED_RECOVERABLE'
  | 'BLOCKED'
  | 'COMPLETED';

export type AutoloopAgentRole = 'planner' | 'coder' | 'reviewer';

/** The three autoloop roles. Single source of truth — dispatcher and SessionManager both use it. */
export type AutoloopRoleName = AutoloopAgentRole;

export type AutoloopChatStateCode = 'AUTOLOOP_SEND_TIMEOUT' | 'AUTOLOOP_RUN_PAUSED' | 'AUTOLOOP_RUN_TERMINAL';

export type PublicAutoloopFailureCode = AutoloopOperationErrorCode | AutoloopChatStateCode;

/** Stable, data-only failure value shared by MCP and embedded HTTP/SSE. */
export interface PublicAutoloopFailure {
  readonly code: PublicAutoloopFailureCode;
  readonly message: string;
  readonly committed?: true;
  readonly retryable: boolean;
  readonly pending_dispatch?: Readonly<SendTimeoutPayload>;
  readonly status_reason?: string | null;
}

export interface PhysicalAgentGeneration {
  role: AutoloopAgentRole;
  generation: number;
  session_name: string;
  session_id?: string;
  owner_instance_id: string;
  created_at: string;
  last_activity_at: string;
  lease_expires_at: string;
  state: 'live' | 'stale' | 'orphaned' | 'released';
}

export type AgentRuntimeLiveness = 'live' | 'absent' | 'unknown';

const RECOVERABLE_AGENT_OWNER_PATTERN = /^session-manager:(\d+):[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i;

export function isRecoverableAgentOwnerInstanceId(value: string): boolean {
  const match = RECOVERABLE_AGENT_OWNER_PATTERN.exec(value);
  if (!match) return false;
  const ownerPid = Number(match[1]);
  return Number.isSafeInteger(ownerPid) && ownerPid > 0;
}

export class AutoloopAgentReleaseOwnerError extends Error {
  readonly code = 'AUTOLOOP_AGENT_RELEASE_OWNER_INVALID' as const;
  readonly retryable = false;

  constructor(ownerInstanceId: string) {
    super(`Autoloop release owner '${ownerInstanceId}' is not a recoverable SessionManager identity`);
    this.name = 'AutoloopAgentReleaseOwnerError';
  }
}

/** Durable evidence hooks run while the exact runtime generation remains fenced. */
export type AgentReservationReleaseOptions =
  | {
      /** Restore a just-created reservation whose durable ledger append failed. */
      rollbackUncommittedReservation: true;
      expectedOwnerInstanceId: string;
      expectedSessionId: string;
      releaseOwnerInstanceId?: never;
      beforeRelease?: never;
      persistReleaseEvidence?: never;
    }
  | {
      rollbackUncommittedReservation?: false;
      expectedOwnerInstanceId: string;
      /** Explicit undefined is the legacy generation-zero session identity. */
      expectedSessionId: string | undefined;
      /** Identifies the one runtime owner allowed to finish a pending release. */
      releaseOwnerInstanceId: string;
      /** Runs only after the exact release-owner fence is durable. */
      beforeRelease?: () => void;
      /** Runs after the pending tombstone is durable and before the name becomes reusable. */
      persistReleaseEvidence?: () => void;
    };

/** Runtime-only facts used to fence durable physical-agent generations. */
export interface AgentRuntimeProbe {
  inspect(sessionName: string, sessionId?: string): Promise<AgentRuntimeLiveness>;
  releaseReservation(
    sessionName: string,
    expectedGeneration: number,
    options: AgentReservationReleaseOptions,
  ): Promise<boolean>;
}

export interface RecoveryAssessment {
  run_id: string;
  phase: AutoloopPhase;
  evidence: string[];
  agents: PhysicalAgentGeneration[];
  pending_delivery_ids: string[];
  next_safe_action: 'none' | 'resume_planner' | 'dispatch_coder' | 'request_review' | 'manual_resolution';
  recovery_token: string;
}

export type RecoveryArtifactName = 'directive' | 'coder_summary' | 'eval_output' | 'diff';

export interface RecoveryIterationEvidence {
  iter: number;
  artifacts: readonly RecoveryArtifactName[];
  verdict?: 'advance' | 'hold' | 'rollback';
}

export interface RecoveryDeliveryEvidence {
  delivery_id: string;
  iter: number;
  kind: 'coder_directive' | 'review_request';
  acknowledged: boolean;
}

export type DeliveryKind = 'coder_directive' | 'review_request';
export type DeliveryTargetRole = Extract<AutoloopAgentRole, 'coder' | 'reviewer'>;

export interface DeliveryIntent {
  schema_version: 1;
  delivery_id: string;
  idempotency_key: string;
  kind: DeliveryKind;
  target_role: DeliveryTargetRole;
  target_generation: number;
  payload: unknown;
  payload_sha256: string;
  created_at: string;
}

/** Durable proof that a receiver accepted one exact persisted delivery payload. */
export interface DeliveryAcknowledgement {
  schema_version: 1;
  delivery_id: string;
  payload_sha256: string;
  acknowledged_at: string;
}

export interface PrepareDeliveryInput {
  idempotency_key: string;
  kind: DeliveryKind;
  target_role: DeliveryTargetRole;
  target_generation: number;
  payload: unknown;
}

export type AutoloopDeliveryOutboxErrorCode =
  | 'AUTOLOOP_DELIVERY_INPUT_INVALID'
  | 'AUTOLOOP_DELIVERY_LEDGER_INVALID'
  | 'AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT'
  | 'AUTOLOOP_DELIVERY_ACKNOWLEDGEMENT_CONFLICT'
  | 'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CONTENDED'
  | 'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CLEANUP_FAILED'
  | 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED';

const AUTOLOOP_DELIVERY_OUTBOX_RETRYABILITY = {
  AUTOLOOP_DELIVERY_INPUT_INVALID: false,
  AUTOLOOP_DELIVERY_LEDGER_INVALID: false,
  AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT: false,
  AUTOLOOP_DELIVERY_ACKNOWLEDGEMENT_CONFLICT: false,
  AUTOLOOP_DELIVERY_OUTBOX_LOCK_CONTENDED: true,
  AUTOLOOP_DELIVERY_OUTBOX_LOCK_CLEANUP_FAILED: false,
  AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED: false,
} as const satisfies Record<AutoloopDeliveryOutboxErrorCode, boolean>;

/** Stable failure contract for durable delivery preparation and recovery lookup. */
export class AutoloopDeliveryOutboxError extends Error {
  readonly retryable: boolean;
  declare readonly committed?: true;
  readonly secondaryErrors: Error[] = [];

  constructor(
    readonly code: AutoloopDeliveryOutboxErrorCode,
    message: string,
    options?: ErrorOptions & { committed?: true },
  ) {
    super(message, options);
    this.name = 'AutoloopDeliveryOutboxError';
    this.retryable = AUTOLOOP_DELIVERY_OUTBOX_RETRYABILITY[code];
    if (options?.committed) this.committed = true;
  }
}

export interface RecoveryAgentEvidence {
  generation: PhysicalAgentGeneration;
  /** Runtime observation for this exact generation and owner tuple. */
  matching_runtime: 'live' | 'absent' | 'unknown';
}

export interface RecoveryInput {
  run_id: string;
  /** Caller-supplied clock keeps assessment deterministic and side-effect free. */
  observed_at: string;
  /** Optional because state written before recovery support has no recovery fields. */
  legacy_state?: Partial<
    Pick<AutoloopState, 'status' | 'iter' | 'subagents_spawned' | 'status_reason' | 'pending_dispatch'>
  >;
  iterations: readonly RecoveryIterationEvidence[];
  deliveries: readonly RecoveryDeliveryEvidence[];
  agents: readonly RecoveryAgentEvidence[];
  /** Explicit durable terminal evidence; legacy `terminated` alone is not completion. */
  completed?: boolean;
}

export interface AutoloopState {
  run_id: string;
  status: AutoloopStatus;
  iter: number;
  /** Set once Planner calls `spawn_subagents`. Until then we are in "planning" mode. */
  subagents_spawned: boolean;
  started_at: string;
  workspace: string;
  ledger_dir: string;
  push_log_count: number;
  /** Last reason set when status flips to terminated/crashed/paused. */
  status_reason: string | null;
  /**
   * The one logical agent dispatch awaiting an explicit resume decision.
   * Optional for backward compatibility with state checkpointed before I3.
   */
  pending_dispatch?: SendTimeoutPayload | null;
  /**
   * Phase-error circuit breaker — incremented on every `phase_error` message,
   * cleared on each successful (non-error) `iter_done`. When it reaches
   * `AutoloopConfig.phaseErrorCircuit`, the runner auto-terminates.
   */
  consecutive_phase_errors: number;
  /** Recent (≤ 5) phase_error payloads kept around for circuit-trip push detail. */
  recent_phase_errors: Array<{
    ts: string;
    agent: string;
    phase: string;
    code?: PublicAutoloopFailureCode;
    committed?: true;
    retryable?: boolean;
    pending_dispatch?: Readonly<SendTimeoutPayload>;
    status_reason?: string | null;
    error: string;
  }>;
  /** Recent metric history (most-recent last, capped at MAX_METRIC_HISTORY). */
  metric_history: number[];
  /** ms since epoch of the last handled message; used by stall detector. */
  last_activity_at: number;
}

/** How many metric points the runner remembers for prior_metrics injection. */
export const MAX_METRIC_HISTORY = 20;
/** Schema version stamped onto every ledger artifact (directive/eval/verdict.json). */
export const LEDGER_SCHEMA_VERSION = 1;

/** Default wall-clock cap for one Planner, Coder, or Reviewer message. */
export const DEFAULT_SEND_TIMEOUT_MS = 600_000;
/** Shortest supported wall-clock cap for one agent message. */
export const MIN_SEND_TIMEOUT_MS = 5_000;
/** Longest supported wall-clock cap for one agent message. */
export const MAX_SEND_TIMEOUT_MS = 7_200_000;
/** Default inactivity lease for an Autoloop run. */
export const DEFAULT_ACTIVITY_LEASE_MS = 1_800_000;
/** Shortest supported inactivity lease. */
export const MIN_ACTIVITY_LEASE_MS = 60_000;
/** Longest supported inactivity lease. */
export const MAX_ACTIVITY_LEASE_MS = 7_200_000;
/** Default absolute lifetime cap for an Autoloop run. */
export const DEFAULT_AUTOLOOP_HARD_TIMEOUT_MS = 86_400_000;
/** Shortest supported absolute lifetime cap. */
export const MIN_AUTOLOOP_HARD_TIMEOUT_MS = 600_000;
/** Longest supported absolute lifetime cap (72 hours). */
export const MAX_AUTOLOOP_HARD_TIMEOUT_MS = 259_200_000;

/** User-configurable Autoloop timeout values. Omitted values use the defaults above. */
export interface AutoloopTimeoutConfig {
  sendTimeoutMs?: number;
  activityLeaseMs?: number;
  autoloopHardTimeoutMs?: number;
}

/** Fully resolved timeout values used by the Autoloop runtime. */
export type ResolvedAutoloopTimeoutConfig = Required<AutoloopTimeoutConfig>;

export interface AutoloopTimeoutFieldSchema {
  readonly type: 'number';
  readonly default: number;
  readonly minimum: number;
  readonly maximum: number;
}

/** JSON-Schema-compatible metadata shared by runtime validation and later API schemas. */
export const AUTOLOOP_TIMEOUT_SCHEMA = {
  sendTimeoutMs: {
    type: 'number',
    default: DEFAULT_SEND_TIMEOUT_MS,
    minimum: MIN_SEND_TIMEOUT_MS,
    maximum: MAX_SEND_TIMEOUT_MS,
  },
  activityLeaseMs: {
    type: 'number',
    default: DEFAULT_ACTIVITY_LEASE_MS,
    minimum: MIN_ACTIVITY_LEASE_MS,
    maximum: MAX_ACTIVITY_LEASE_MS,
  },
  autoloopHardTimeoutMs: {
    type: 'number',
    default: DEFAULT_AUTOLOOP_HARD_TIMEOUT_MS,
    minimum: MIN_AUTOLOOP_HARD_TIMEOUT_MS,
    maximum: MAX_AUTOLOOP_HARD_TIMEOUT_MS,
  },
} as const satisfies Record<keyof ResolvedAutoloopTimeoutConfig, AutoloopTimeoutFieldSchema>;

const AUTOLOOP_TIMEOUT_FIELDS = [
  'sendTimeoutMs',
  'activityLeaseMs',
  'autoloopHardTimeoutMs',
] as const satisfies readonly (keyof AutoloopTimeoutConfig)[];

export function validateAutoloopTimeoutConfig(config: AutoloopTimeoutConfig): void {
  for (const field of AUTOLOOP_TIMEOUT_FIELDS) {
    const value = config[field];
    if (value === undefined) continue;
    const { minimum, maximum } = AUTOLOOP_TIMEOUT_SCHEMA[field];
    if (typeof value !== 'number' || !Number.isFinite(value) || value < minimum || value > maximum) {
      throw new Error(`${field} must be a finite number in the inclusive range [${minimum}, ${maximum}]`);
    }
  }
}

export function resolveAutoloopTimeoutConfig(config: AutoloopTimeoutConfig = {}): ResolvedAutoloopTimeoutConfig {
  validateAutoloopTimeoutConfig(config);
  return {
    sendTimeoutMs: config.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS,
    activityLeaseMs: config.activityLeaseMs ?? DEFAULT_ACTIVITY_LEASE_MS,
    autoloopHardTimeoutMs: config.autoloopHardTimeoutMs ?? DEFAULT_AUTOLOOP_HARD_TIMEOUT_MS,
  };
}

export interface PushPolicyRule {
  silent?: boolean;
  level?: PushLevel;
  channel?: PushChannel;
}

export interface PushPolicy {
  on_start: PushPolicyRule;
  on_iter_done_ok: PushPolicyRule;
  on_target_hit: PushPolicyRule;
  on_metric_regression_2: PushPolicyRule;
  on_reviewer_reject_2: PushPolicyRule;
  on_phase_error: PushPolicyRule;
  on_stall_30min: PushPolicyRule;
  on_decision_needed: PushPolicyRule;
}

export const DEFAULT_PUSH_POLICY: PushPolicy = {
  on_start: { level: 'info', channel: 'wechat' },
  on_iter_done_ok: { silent: true },
  on_target_hit: { level: 'info', channel: 'both' },
  on_metric_regression_2: { level: 'warn', channel: 'both' },
  on_reviewer_reject_2: { level: 'warn', channel: 'both' },
  on_phase_error: { level: 'error', channel: 'both' },
  on_stall_30min: { level: 'warn', channel: 'wechat' },
  on_decision_needed: { level: 'decision', channel: 'both' },
};

/**
 * Pluggable agent layer. The runner stays transport-only; an AgentDispatcher
 * implementation owns the actual Claude (or mock) sessions and turns inbound
 * messages into outbound replies. S2/S3/S4 swap mocks for real persistent
 * sessions; the runner contract stays the same.
 */
export interface AgentDispatcher {
  /**
   * Deliver `env` to its target agent and return any messages the agent emits
   * synchronously in reply. Asynchronous emissions should also be returned
   * (the runner awaits this call).
   */
  deliver(env: AnyAutoloopMessage): Promise<AnyAutoloopMessage[]>;
  /** Called once when the runner is starting up — agent may pre-warm sessions. */
  init?(state: AutoloopState): Promise<void>;
  /** Called on terminate — agent must release sessions cleanly. */
  shutdown?(reason: string): Promise<void>;
}

export interface AutoloopConfig extends AutoloopTimeoutConfig {
  run_id: string;
  workspace: string;
  ledger_dir: string;
  /** Optional override; defaults to DEFAULT_PUSH_POLICY. */
  push_policy?: PushPolicy;
  /**
   * Notifier the runner calls when a `push_user` message arrives.
   * S3 will plug in the wechat→whatsapp→email fallback chain; S1/S2 use
   * a recording stub.
   */
  notifyUser: (level: PushLevel, summary: string, detail: string | undefined, channel: PushChannel) => Promise<void>;
  /** Agent transport layer (mockable). */
  dispatcher: AgentDispatcher;
  /**
   * Phase-error circuit threshold. After this many consecutive `phase_error`
   * messages the runner auto-terminates with reason `phase_error_circuit`
   * (and emits a decision-level push first). Default 3.
   */
  phaseErrorCircuit?: number;
  /**
   * Max messages routed in a single drain pass before the runner assumes a
   * message ping-pong loop and aborts. Raise for legitimately deep workflows
   * (many directive/policy-push chains per turn). Default 64.
   */
  maxDispatchDepth?: number;
  /**
   * Stall detection wall-clock budget (ms). When no message has been
   * processed for this long and status is 'running', the runner fires
   * `on_stall_30min`. Default 30 min.
   */
  stallMs?: number;
  /**
   * Stall check interval (ms). Default 30 s. Tests pass a smaller value
   * along with shorter `stallMs` so the assertions complete quickly.
   */
  stallCheckIntervalMs?: number;
}
