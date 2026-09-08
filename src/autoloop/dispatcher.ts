/**
 * ClaudeAgentDispatcher — wires the v2 runner to real persistent coding
 * sessions managed by SessionManager. The historical class name is retained
 * for compatibility; each Autoloop role may use a different engine.
 *
 * Naming convention:
 *   autoloop-<run_id>-planner
 *   autoloop-<run_id>-coder      (S4)
 *   autoloop-<run_id>-reviewer   (S4)
 *
 * Reply path:
 *   When the user chats, we sendMessage(planner, text) and capture the
 *   Planner's natural-language reply. The reply is *not* a v2 message —
 *   it is emitted as the dispatcher's own 'planner_reply' event so the
 *   `autoloop_chat` plugin tool can return it to the user. Structured
 *   signals (S3+) will be parsed out of the same reply text and pushed
 *   into the runner queue.
 */

import { EventEmitter } from 'node:events';
import { createHash, randomUUID } from 'node:crypto';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';
import { TextDecoder } from 'node:util';

import type { SessionManager } from '../session-manager.js';
import type { Logger } from '../logger.js';
import { ENGINE_TYPES, engineHasNativeConversation, type CustomEngineConfig, type EngineType } from '../types.js';
import { nullLogger } from '../logger.js';
import { spawn, type ChildProcess } from 'node:child_process';
import { capturePatch, changedFilesSince } from '../verify/baseline.js';
import { runContract } from '../verify/runner.js';
import { writeEvidence } from '../verify/evidence.js';
import type { AcceptanceContract } from '../verify/contract.js';
import {
  type AnyAutoloopMessage,
  type AutoloopOperationErrorCode,
  canonicalizeExactStringArrayElements,
  canonicalizeMessage,
  canonicalizeRequestReviewArgs,
  type CheckpointReviewRequestPayload,
  hasExactStringArrayElements,
  Msg,
  type RequestReviewArgs,
  type SendTimeoutPayload,
} from './messages.js';
import {
  AutoloopAgentReleaseOwnerError,
  DEFAULT_ACTIVITY_LEASE_MS,
  DEFAULT_PUSH_POLICY,
  DEFAULT_SEND_TIMEOUT_MS,
  LEDGER_SCHEMA_VERSION,
  isRecoverableAgentOwnerInstanceId,
  validateAutoloopTimeoutConfig,
  type AgentRuntimeProbe,
  type AgentRuntimeLiveness,
  type AgentDispatcher,
  type AutoloopRoleName,
  type AutoloopState,
  type PhysicalAgentGeneration,
  type PushPolicy,
} from './types.js';

import {
  applyValidatedPlannerToolCalls,
  canonicalizePlannerControls,
  canonicalPlannerControlsJson,
  parsePlannerReply,
  validatePlannerToolCalls,
  type PlannerToolCall,
  type PlannerToolEffects,
  type PlannerToolName,
  type PreparedReviewRequest,
  type ReviewRequestPreparationResult,
  MAX_PLANNER_CONTROL_BATCH_BYTES,
  type SpawnCoderArgs,
  type SpawnReviewerArgs,
  type SpawnSubagentsArgs,
} from './planner-tools.js';
import { extractIterComplete, extractReviewComplete, parseAgentReply } from './agent-tools.js';
import {
  isCommittedSecureLedgerError,
  SecureAutoloopLedger,
  type SecureAutoloopLedgerCommitError,
} from './secure-ledger.js';

export { openPrivateAutoloopDecisions, securePrivateAutoloopDecisionLedger } from './secure-ledger.js';

/**
 * Character budget for the replayed transcript handed to engines without native
 * conversation (see hasNativeConversation). Oldest turns are dropped first, so a
 * long run keeps the recent context instead of growing the prompt forever.
 */
const REPLAY_CHAR_BUDGET = 24_000;

export interface ClaudeAgentDispatcherConfig {
  manager: SessionManager;
  runId: string;
  workspace: string;
  /** Override the default Planner system prompt (default loads from configs/autoloop-planner-prompt.md). */
  plannerPromptPath?: string;
  /** Override Coder/Reviewer prompt paths (defaults walk-up to configs/autoloop-{coder,reviewer}-prompt.md). */
  coderPromptPath?: string;
  reviewerPromptPath?: string;
  /** Planner engine/model (default: claude/opus). */
  plannerEngine?: EngineType;
  plannerModel?: string;
  plannerCustomEngine?: CustomEngineConfig;
  /** Coder defaults. Engine/model can be overridden per spawn_subagents call. */
  coderEngine?: EngineType;
  coderModel?: string;
  coderCustomEngine?: CustomEngineConfig;
  /** Reviewer defaults. Engine/model can be overridden per spawn_subagents call. */
  reviewerEngine?: EngineType;
  reviewerModel?: string;
  reviewerCustomEngine?: CustomEngineConfig;
  /** Per-message wall-clock cap. Default 10 min. */
  sendTimeoutMs?: number;
  /** Physical-agent lease length. Defaults to the Autoloop activity lease. */
  agentLeaseMs?: number;
  /** Runtime/session-registry boundary. Production uses SessionManager. */
  runtimeProbe?: AgentRuntimeProbe;
  /** Stable owner identity for this SessionManager process. */
  ownerInstanceId?: string;
  /** Deterministic clock seam for lease tests. */
  now?: () => Date;
  /** Internal failure-atomic resume marker; never accepted from an agent. */
  suppressFailedStartAudit?: boolean;
  /** Shared capability pinned by SessionManager for every flat run-ledger operation. */
  secureLedger?: SecureAutoloopLedger;
  /**
   * Optional acceptance contract. When present the Reviewer's `advance` is no
   * longer sufficient on its own: the contract runs against the workspace and a
   * red result downgrades the verdict to `hold`.
   *
   * Supplied by the caller at autoloop start — never parsed out of Planner or
   * Reviewer output, which would put the agents back in charge of their own
   * grading.
   */
  contract?: AcceptanceContract;
  logger?: Logger;
  /**
   * Auto-compact thresholds (percent of context window). When the agent's
   * `contextPercent` (from getStats) climbs above its threshold after a
   * turn, the dispatcher dispatches `/compact <agent-specific summary>` to
   * that agent. Defaults: Planner 80%, Coder 70%, Reviewer 70%.
   *
   * Per the design doc §7: each agent's context is precious; don't let it
   * silently fill until the API rejects.
   */
  compactThresholds?: { planner?: number; coder?: number; reviewer?: number };
  /**
   * Push-policy ref that S3's update_push_policy mutates. Caller (SessionManager)
   * passes its own policy object so changes are visible to the runner.
   */
  pushPolicyRef?: PushPolicy;
  /** Called when Planner emits spawn_subagents. S4 implements; S3 records the intent. */
  onSpawnSubagents?: (args: SpawnSubagentsArgs) => Promise<void>;
  /** Called exactly after the durably verified spawn effect commits. */
  onSpawnSubagentsCommitted?: () => Promise<void> | void;
  /** Persist the effective non-secret role selection after a successful spawn. */
  onRoleSelectionChanged?: (selection: {
    coder: { engine: EngineType; model?: string };
    reviewer: { engine: EngineType; model?: string };
  }) => Promise<void> | void;
}

function resolveConfigByName(filename: string): string {
  const filePath = fileURLToPath(import.meta.url);
  let dir = path.dirname(filePath);
  for (let i = 0; i < 8; i++) {
    const candidate = path.join(dir, 'configs', filename);
    if (fs.existsSync(candidate)) return candidate;
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  return path.join(path.dirname(filePath), '..', 'configs', filename);
}
const resolveDefaultPlannerPrompt = (): string => resolveConfigByName('autoloop-planner-prompt.md');
const resolveDefaultCoderPrompt = (): string => resolveConfigByName('autoloop-coder-prompt.md');
const resolveDefaultReviewerPrompt = (): string => resolveConfigByName('autoloop-reviewer-prompt.md');

interface SendMessageResult {
  output: string;
  error?: string;
  /** Set when even the recovery retry failed — caller surfaces as phase_error. */
  fatal?: boolean;
  /** Stable classification when a typed recovery failure made the send fatal. */
  code?: AutoloopOperationErrorCode;
  /** Genuine send deadlines pause for an explicit resume instead of retrying. */
  recoverable_timeout?: SendTimeoutPayload;
}

const AUTOLOOP_OPERATION_RETRYABILITY = {
  AUTOLOOP_EMPTY_REPLY: true,
  AUTOLOOP_SESSION_NOT_CREATED: true,
  AUTOLOOP_ENGINE_FAILURE: true,
  AUTOLOOP_REQUIRED_TOOL_DENIED: true,
  AUTOLOOP_CONTROL_MALFORMED: false,
  AUTOLOOP_CONTROL_APPLICATION_FAILED: false,
  AUTOLOOP_CONTROL_NOT_PERSISTED: true,
  AUTOLOOP_RESET_POSTCONDITION_FAILED: false,
  AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE: false,
  AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE: false,
  AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE: false,
  AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID: false,
} as const satisfies Record<AutoloopOperationErrorCode, boolean>;

export class AutoloopOperationError extends Error {
  readonly retryable: boolean;
  /** Failures encountered while routing this error; never replace its public identity. */
  readonly secondaryErrors: Error[] = [];

  constructor(
    readonly code: AutoloopOperationErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = 'AutoloopOperationError';
    this.retryable = AUTOLOOP_OPERATION_RETRYABILITY[code];
  }
}

/**
 * A matching control row is already authoritative, but this process cannot
 * prove whether its effects completed. Keep the ordinary typed failure at the
 * public boundary while suppressing another decision-log row: moving the tail
 * would otherwise let a later recovery miss the committed control and replay
 * it.
 */
class CommittedPlannerControlReplayError extends AutoloopOperationError {
  constructor(
    message = 'Planner control event is already committed; refusing to repeat effects without a durable application receipt',
  ) {
    super('AUTOLOOP_CONTROL_APPLICATION_FAILED', message);
  }
}

/** Ledger inspection failed, so even best-effort audit must not write through it. */
class PlannerControlLedgerInvalidError extends AutoloopOperationError {
  constructor(message: string, options?: ErrorOptions) {
    super('AUTOLOOP_CONTROL_NOT_PERSISTED', message, options);
  }
}

function normalizePlannerOperationError(error: unknown): AutoloopOperationError | SecureAutoloopLedgerCommitError {
  if (error instanceof AutoloopOperationError || isCommittedSecureLedgerError(error)) return error;
  const cause = error instanceof Error ? error : new Error(String(error));
  return new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', `Planner engine transport failed: ${cause.message}`, {
    cause,
  });
}

export type AutoloopResetResult =
  | {
      ok: true;
      agent: AutoloopRoleName;
      previous_generation?: number;
      active_generation?: number;
      reusable: true;
    }
  | {
      ok: false;
      code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED';
      agent: AutoloopRoleName;
      previous_generation?: number;
      message: string;
      retryable: false;
    };

interface PlannerTurnResult {
  reply: string;
  generation?: PhysicalAgentGeneration;
  generationLiveness?: 'live' | 'absent' | 'unknown';
  requiredToolDenied?: boolean;
  persistedControl?: PlannerControlEvidence;
}

interface PlannerTurnExpectation {
  /** Preliminary engine/generation checks run before control parsing. */
  requireLogicalResult?: boolean;
  expectedGeneration?: PhysicalAgentGeneration;
  expectedControl?: Omit<PlannerControlEvidence, 'control_id' | 'persisted_at'>;
}

interface PlannerControlEvidence {
  control_id: string;
  persisted_at: string;
  dispatch_id: string;
  message_id: string;
  iter: number;
  generation: number;
  owner_instance_id: string;
  session_id?: string;
  tools: PlannerToolName[];
  controls: PlannerToolCall[];
  controls_sha256: string;
}

function plannerControlsSha256(controls: readonly PlannerToolCall[]): string {
  return createHash('sha256').update(canonicalPlannerControlsJson(controls)).digest('hex');
}

function plannerControlClaimMatches(
  observed: PlannerControlEvidence | undefined,
  expected: Omit<PlannerControlEvidence, 'control_id' | 'persisted_at'>,
): observed is PlannerControlEvidence {
  return Boolean(
    observed &&
    observed.dispatch_id === expected.dispatch_id &&
    observed.message_id === expected.message_id &&
    observed.iter === expected.iter &&
    observed.generation === expected.generation &&
    observed.owner_instance_id === expected.owner_instance_id &&
    observed.session_id === expected.session_id &&
    observed.controls_sha256 === expected.controls_sha256 &&
    plannerControlsSha256(observed.controls) === observed.controls_sha256 &&
    observed.tools.length === expected.tools.length &&
    observed.tools.every((tool, index) => tool === expected.tools[index]),
  );
}

function plannerControlEvidenceMatches(
  observed: PlannerControlEvidence | undefined,
  expected: PlannerControlEvidence,
): observed is PlannerControlEvidence {
  return Boolean(
    observed &&
    observed.control_id === expected.control_id &&
    observed.persisted_at === expected.persisted_at &&
    plannerControlClaimMatches(observed, expected),
  );
}

function plannerControlEvidenceFromTail(line: string): PlannerControlEvidence | undefined {
  if (!line.trim()) return undefined;
  let row: { kind?: unknown; payload?: unknown };
  try {
    row = JSON.parse(line) as { kind?: unknown; payload?: unknown };
  } catch {
    return undefined;
  }
  return row.kind === 'planner_turn_control' && row.payload && typeof row.payload === 'object'
    ? (row.payload as PlannerControlEvidence)
    : undefined;
}

const MAX_DECISION_LEDGER_BYTES = 64 * 1024 * 1024;
const MAX_DECISION_LEDGER_ROW_BYTES = MAX_PLANNER_CONTROL_BATCH_BYTES + 256 * 1024;
const MAX_REVIEW_REQUEST_IDENTITIES = 4_096;
const MAX_CONCURRENT_REVIEW_REQUEST_PREPARATIONS = 64;
const REVIEW_REQUEST_DECISION_KEYS = [
  'checkpoint_sha',
  'source_run_id',
  'source_iter',
  'target_iter',
  'scope',
  'idempotency_key',
  'request_digest',
] as const;
type ReviewEvidenceArtifact = 'directive.json' | 'eval_output.json' | 'coder_summary.txt' | 'diff.patch';
const MAX_GIT_EVIDENCE_STDERR_BYTES = 64 * 1024;
const MAX_GIT_HEAD_STDOUT_BYTES = 256;
const GIT_EVIDENCE_TIMEOUT_MS = 10_000;

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

type PersistedReviewVerdictPayload = {
  decision: string;
  metric: number | null;
  audit_notes: string;
  accepted?: boolean;
  evidence_id?: string;
};

const PERSISTED_REVIEW_VERDICT_KEYS = ['decision', 'metric', 'audit_notes', 'accepted', 'evidence_id'] as const;
const STORED_REVIEW_VERDICT_KEYS = new Set(['schema_version', 'iter', 'ts', ...PERSISTED_REVIEW_VERDICT_KEYS, 'flags']);
const INCOMING_REVIEW_VERDICT_KEYS = new Set([...PERSISTED_REVIEW_VERDICT_KEYS, 'flags']);

function canonicalPersistedVerdictPayload(
  payload: PersistedReviewVerdictPayload,
  storedEnvelope = false,
): PersistedReviewVerdictPayload {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
    throw new Error('Refusing to persist invalid immutable Reviewer verdict payload');
  }
  const allowedKeys = storedEnvelope ? STORED_REVIEW_VERDICT_KEYS : INCOMING_REVIEW_VERDICT_KEYS;
  const payloadKeys = Reflect.ownKeys(payload);
  for (let index = 0; index < payloadKeys.length; index += 1) {
    const key = payloadKeys[index];
    if (typeof key !== 'string' || !allowedKeys.has(key)) {
      throw new Error('Refusing to persist immutable Reviewer verdict payload with unsupported fields');
    }
  }
  const decisionDescriptor = Object.getOwnPropertyDescriptor(payload, 'decision');
  const metricDescriptor = Object.getOwnPropertyDescriptor(payload, 'metric');
  const auditNotesDescriptor = Object.getOwnPropertyDescriptor(payload, 'audit_notes');
  const acceptedDescriptor = Object.getOwnPropertyDescriptor(payload, 'accepted');
  const evidenceIdDescriptor = Object.getOwnPropertyDescriptor(payload, 'evidence_id');
  const flagsDescriptor = Object.getOwnPropertyDescriptor(payload, 'flags');
  if (
    !decisionDescriptor ||
    !Object.hasOwn(decisionDescriptor, 'value') ||
    !metricDescriptor ||
    !Object.hasOwn(metricDescriptor, 'value') ||
    !auditNotesDescriptor ||
    !Object.hasOwn(auditNotesDescriptor, 'value') ||
    (acceptedDescriptor !== undefined && !Object.hasOwn(acceptedDescriptor, 'value')) ||
    (evidenceIdDescriptor !== undefined && !Object.hasOwn(evidenceIdDescriptor, 'value'))
  ) {
    throw new Error('Refusing to persist invalid immutable Reviewer verdict payload');
  }
  const decision = decisionDescriptor.value;
  const metric = metricDescriptor.value;
  const auditNotes = auditNotesDescriptor.value;
  const accepted = acceptedDescriptor?.value;
  const evidenceId = evidenceIdDescriptor?.value;
  if (
    (decision !== 'advance' && decision !== 'hold' && decision !== 'rollback') ||
    (metric !== null && (typeof metric !== 'number' || !Number.isFinite(metric))) ||
    typeof auditNotes !== 'string' ||
    (acceptedDescriptor !== undefined && typeof accepted !== 'boolean') ||
    (evidenceIdDescriptor !== undefined && typeof evidenceId !== 'string') ||
    (flagsDescriptor !== undefined &&
      (!Object.hasOwn(flagsDescriptor, 'value') ||
        (flagsDescriptor.value !== undefined && !hasExactStringArrayElements(flagsDescriptor.value))))
  ) {
    throw new Error('Refusing to persist invalid immutable Reviewer verdict payload');
  }

  const canonical = Object.create(null) as PersistedReviewVerdictPayload;
  Object.defineProperty(canonical, 'decision', { enumerable: true, value: decision });
  Object.defineProperty(canonical, 'metric', { enumerable: true, value: metric });
  Object.defineProperty(canonical, 'audit_notes', { enumerable: true, value: auditNotes });
  if (acceptedDescriptor !== undefined) {
    Object.defineProperty(canonical, 'accepted', { enumerable: true, value: accepted });
  }
  if (evidenceIdDescriptor !== undefined) {
    Object.defineProperty(canonical, 'evidence_id', { enumerable: true, value: evidenceId });
  }
  Object.freeze(canonical);
  return canonical;
}

function serializePersistedVerdictV1(iter: number, ts: string, payload: PersistedReviewVerdictPayload): string {
  const persisted = Object.create(null) as Record<string, unknown>;
  persisted.schema_version = LEDGER_SCHEMA_VERSION;
  persisted.iter = iter;
  persisted.ts = ts;
  persisted.decision = payload.decision;
  persisted.metric = payload.metric;
  persisted.audit_notes = payload.audit_notes;
  if (Object.hasOwn(payload, 'accepted')) persisted.accepted = payload.accepted;
  if (Object.hasOwn(payload, 'evidence_id')) persisted.evidence_id = payload.evidence_id;
  return JSON.stringify(persisted, null, 2);
}

function samePersistedVerdictPayload(stored: Record<string, unknown>, payload: PersistedReviewVerdictPayload): boolean {
  const storedKeys = Reflect.ownKeys(stored);
  for (let index = 0; index < storedKeys.length; index += 1) {
    const key = storedKeys[index];
    if (typeof key !== 'string' || !STORED_REVIEW_VERDICT_KEYS.has(key)) return false;
  }
  const flagsDescriptor = Object.getOwnPropertyDescriptor(stored, 'flags');
  if (
    flagsDescriptor !== undefined &&
    (!Object.hasOwn(flagsDescriptor, 'value') || !hasExactStringArrayElements(flagsDescriptor.value))
  ) {
    return false;
  }
  let canonicalStored: PersistedReviewVerdictPayload;
  try {
    canonicalStored = canonicalPersistedVerdictPayload(stored as PersistedReviewVerdictPayload, true);
  } catch {
    return false;
  }
  for (let index = 0; index < PERSISTED_REVIEW_VERDICT_KEYS.length; index += 1) {
    const key = PERSISTED_REVIEW_VERDICT_KEYS[index];
    const storedHasKey = Object.hasOwn(canonicalStored, key);
    const expectedHasKey = Object.hasOwn(payload, key);
    if (storedHasKey !== expectedHasKey || (storedHasKey && canonicalStored[key] !== payload[key])) return false;
  }
  return true;
}

function serializeDirectiveV1(env: Extract<AnyAutoloopMessage, { type: 'directive' }>, dispatchId: string): string {
  const persisted = Object.create(null) as Record<string, unknown>;
  persisted.schema_version = LEDGER_SCHEMA_VERSION;
  persisted.iter = env.iter;
  persisted.ts = env.ts;
  persisted.message_id = env.msg_id;
  persisted.dispatch_id = dispatchId;
  persisted.goal = env.payload.goal;
  persisted.constraints = env.payload.constraints;
  persisted.success_criteria = env.payload.success_criteria;
  persisted.max_attempts = env.payload.max_attempts;
  return JSON.stringify(persisted, null, 2);
}

function buildCoderDirectivePrompt(env: Extract<AnyAutoloopMessage, { type: 'directive' }>): string {
  let prompt = `[directive iter=${env.iter}]\ngoal: ${env.payload.goal}`;
  const constraints = env.payload.constraints;
  if (constraints.length > 0) {
    prompt += '\nconstraints:';
    for (let index = 0; index < constraints.length; index += 1) prompt += `\n  - ${constraints[index]}`;
  }
  const successCriteria = env.payload.success_criteria;
  if (successCriteria.length > 0) {
    prompt += '\nsuccess_criteria:';
    for (let index = 0; index < successCriteria.length; index += 1) prompt += `\n  - ${successCriteria[index]}`;
  }
  prompt += `\nmax_attempts: ${env.payload.max_attempts}`;
  prompt += '\nRead plan.md / goal.json, make the change, run the evaluator, then emit `iter_complete`.';
  return prompt;
}

function validatedPlannerControlEvidence(value: unknown): PlannerControlEvidence | undefined {
  if (!isPlainRecord(value)) return undefined;
  const controls = value.controls;
  const tools = value.tools;
  if (
    typeof value.control_id !== 'string' ||
    typeof value.persisted_at !== 'string' ||
    typeof value.dispatch_id !== 'string' ||
    typeof value.message_id !== 'string' ||
    !Number.isSafeInteger(value.iter) ||
    !Number.isSafeInteger(value.generation) ||
    typeof value.owner_instance_id !== 'string' ||
    (value.session_id !== undefined && typeof value.session_id !== 'string') ||
    !Array.isArray(tools) ||
    !tools.every((tool) => typeof tool === 'string') ||
    !Array.isArray(controls) ||
    !controls.every(
      (control) => isPlainRecord(control) && typeof control.tool === 'string' && isPlainRecord(control.args),
    ) ||
    typeof value.controls_sha256 !== 'string' ||
    !/^[a-f0-9]{64}$/.test(value.controls_sha256)
  ) {
    return undefined;
  }
  return value as unknown as PlannerControlEvidence;
}

function readBoundedDecisionLedger(ledger: SecureAutoloopLedger): Array<{ kind: string; payload?: unknown }> {
  let handle;
  try {
    handle = ledger.openFlatFile('decisions.jsonl', 'read');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw error;
  }
  try {
    const before = fs.fstatSync(handle.fd);
    if (before.size > MAX_DECISION_LEDGER_BYTES) {
      throw new Error(`decisions.jsonl exceeds the ${MAX_DECISION_LEDGER_BYTES}-byte recovery limit`);
    }
    if (before.size === 0) return [];
    const bytes = Buffer.allocUnsafe(before.size);
    let offset = 0;
    while (offset < bytes.length) {
      const count = fs.readSync(handle.fd, bytes, offset, bytes.length - offset, offset);
      if (count <= 0) throw new Error('decisions.jsonl ended before its validated snapshot was complete');
      offset += count;
    }
    const after = fs.fstatSync(handle.fd);
    if (after.size !== before.size || after.dev !== before.dev || after.ino !== before.ino) {
      throw new Error('decisions.jsonl changed while its recovery snapshot was being validated');
    }
    if (bytes.at(-1) !== 0x0a) throw new Error('decisions.jsonl has an incomplete final record');
    let text: string;
    try {
      // `fatal` rejects replacement-character decoding without allocating a
      // second full-size Buffer. `ignoreBOM` keeps a leading BOM visible so
      // the JSON parser rejects it exactly as the previous decoder did.
      text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes);
    } catch (error) {
      throw new Error('decisions.jsonl is not valid UTF-8', { cause: error });
    }
    return text
      .slice(0, -1)
      .split('\n')
      .map((line, index) => {
        const normalized = line.endsWith('\r') ? line.slice(0, -1) : line;
        if (!normalized || Buffer.byteLength(normalized, 'utf8') > MAX_DECISION_LEDGER_ROW_BYTES) {
          throw new Error(`decisions.jsonl record ${index + 1} is empty or exceeds its byte limit`);
        }
        let row: unknown;
        try {
          row = JSON.parse(normalized);
        } catch (error) {
          throw new Error(`decisions.jsonl record ${index + 1} is malformed`, { cause: error });
        }
        if (!isPlainRecord(row) || typeof row.kind !== 'string' || !row.kind) {
          throw new Error(`decisions.jsonl record ${index + 1} is not a valid decision object`);
        }
        if (row.kind === 'planner_turn_control' && !validatedPlannerControlEvidence(row.payload)) {
          throw new Error(`decisions.jsonl record ${index + 1} has invalid Planner control evidence`);
        }
        return { kind: row.kind, payload: row.payload };
      });
  } finally {
    fs.closeSync(handle.fd);
  }
}

function findCommittedPlannerControl(
  ledger: SecureAutoloopLedger,
  expected: Omit<PlannerControlEvidence, 'control_id' | 'persisted_at'>,
): 'none' | 'matching' | 'conflicting' {
  const claims = readBoundedDecisionLedger(ledger)
    .filter((row) => row.kind === 'planner_turn_control')
    .map((row) => validatedPlannerControlEvidence(row.payload)!)
    .filter((evidence) => evidence.dispatch_id === expected.dispatch_id);
  if (claims.length === 0) return 'none';
  if (claims.length === 1 && plannerControlClaimMatches(claims[0], expected)) return 'matching';
  return 'conflicting';
}

interface IndexedReviewRequestClaim {
  signature?: string;
  conflicting: boolean;
}

interface CanonicalReviewRequestClaim {
  identityHash: string;
  signature?: string;
}

function reviewRequestClaim(payload: unknown): CanonicalReviewRequestClaim | undefined {
  if (!isPlainRecord(payload)) return undefined;
  const idempotencyDescriptor = Object.getOwnPropertyDescriptor(payload, 'idempotency_key');
  if (
    !idempotencyDescriptor ||
    !Object.hasOwn(idempotencyDescriptor, 'value') ||
    typeof idempotencyDescriptor.value !== 'string'
  ) {
    return undefined;
  }
  const identityHash = createHash('sha256').update(idempotencyDescriptor.value).digest('hex');
  const ownKeys = Reflect.ownKeys(payload);
  if (ownKeys.length !== REVIEW_REQUEST_DECISION_KEYS.length) return { identityHash };
  for (let index = 0; index < ownKeys.length; index += 1) {
    const ownKey = ownKeys[index];
    let allowed = false;
    for (let allowedIndex = 0; allowedIndex < REVIEW_REQUEST_DECISION_KEYS.length; allowedIndex += 1) {
      if (ownKey === REVIEW_REQUEST_DECISION_KEYS[allowedIndex]) {
        allowed = true;
        break;
      }
    }
    if (!allowed) return { identityHash };
  }

  const values = Object.create(null) as Record<(typeof REVIEW_REQUEST_DECISION_KEYS)[number], unknown>;
  for (const key of REVIEW_REQUEST_DECISION_KEYS) {
    const descriptor = Object.getOwnPropertyDescriptor(payload, key);
    if (!descriptor || !Object.hasOwn(descriptor, 'value')) return { identityHash };
    values[key] = descriptor.value;
  }
  if (
    typeof values.checkpoint_sha !== 'string' ||
    typeof values.source_run_id !== 'string' ||
    !Number.isSafeInteger(values.source_iter) ||
    !Number.isSafeInteger(values.target_iter) ||
    !hasExactStringArrayElements(values.scope) ||
    typeof values.request_digest !== 'string'
  ) {
    return { identityHash };
  }

  const hash = createHash('sha256');
  const appendString = (value: string): void => {
    hash.update(`${Buffer.byteLength(value, 'utf8')}:`);
    hash.update(value, 'utf8');
  };
  appendString(values.checkpoint_sha);
  appendString(values.source_run_id);
  hash.update(`n:${values.source_iter};n:${values.target_iter};`);
  hash.update(`a:${values.scope.length};`);
  for (let index = 0; index < values.scope.length; index += 1) appendString(values.scope[index]);
  appendString(idempotencyDescriptor.value);
  appendString(values.request_digest);
  return { identityHash, signature: hash.digest('hex') };
}

function indexReviewRequestClaims(
  rows: ReadonlyArray<{ kind: string; payload?: unknown }>,
): Map<string, IndexedReviewRequestClaim> {
  const index = new Map<string, IndexedReviewRequestClaim>();
  for (const row of rows) {
    if (row.kind !== 'request_review') continue;
    const claim = reviewRequestClaim(row.payload);
    if (!claim) continue;
    const existing = index.get(claim.identityHash);
    if (!existing) {
      if (index.size >= MAX_REVIEW_REQUEST_IDENTITIES) {
        throw new Error(
          `request_review durable identity index reached its ${MAX_REVIEW_REQUEST_IDENTITIES}-entry capacity`,
        );
      }
      index.set(claim.identityHash, {
        signature: claim.signature,
        conflicting: claim.signature === undefined,
      });
      continue;
    }
    if (existing.conflicting || claim.signature === undefined || existing.signature !== claim.signature) {
      index.set(claim.identityHash, { conflicting: true });
    }
  }
  return index;
}

function mergeReviewRequestClaimIndexes(
  loaded: Map<string, IndexedReviewRequestClaim>,
  cached: ReadonlyMap<string, IndexedReviewRequestClaim> | undefined,
): Map<string, IndexedReviewRequestClaim> {
  if (!cached) return loaded;
  const merged = new Map(loaded);
  for (const [identityHash, cachedClaim] of cached) {
    const loadedClaim = merged.get(identityHash);
    if (!loadedClaim) {
      merged.set(identityHash, cachedClaim);
    } else if (loadedClaim.conflicting || cachedClaim.conflicting || loadedClaim.signature !== cachedClaim.signature) {
      merged.set(identityHash, { conflicting: true });
    }
  }
  return merged;
}

function reviewRequestDecisionPayload(
  request: RequestReviewArgs,
  targetIter: number,
  digest: string,
): Readonly<Record<string, unknown>> {
  const payload = Object.create(null) as Record<string, unknown>;
  Object.defineProperties(payload, {
    checkpoint_sha: { enumerable: true, value: request.checkpoint_sha },
    source_run_id: { enumerable: true, value: request.source_run_id },
    source_iter: { enumerable: true, value: request.source_iter },
    target_iter: { enumerable: true, value: targetIter },
    scope: { enumerable: true, value: request.scope },
    idempotency_key: { enumerable: true, value: request.idempotency_key },
    request_digest: { enumerable: true, value: digest },
  });
  return Object.freeze(payload);
}

function assertPlannerTurnSucceeded(result: PlannerTurnResult, expected: PlannerTurnExpectation): void {
  if (expected.expectedGeneration) {
    const observed = result.generation;
    if (
      !observed ||
      observed.state !== 'live' ||
      result.generationLiveness !== 'live' ||
      observed.generation !== expected.expectedGeneration.generation ||
      observed.owner_instance_id !== expected.expectedGeneration.owner_instance_id ||
      observed.session_id !== expected.expectedGeneration.session_id
    ) {
      throw new AutoloopOperationError(
        'AUTOLOOP_SESSION_NOT_CREATED',
        `Planner generation ${expected.expectedGeneration.generation} was not live after its turn`,
      );
    }
  }
  if (result.requiredToolDenied) {
    throw new AutoloopOperationError(
      'AUTOLOOP_REQUIRED_TOOL_DENIED',
      'Planner turn completed without the engine accepting its required tool work',
    );
  }
  if (expected.expectedControl) {
    const persisted = result.persistedControl;
    if (!plannerControlClaimMatches(persisted, expected.expectedControl)) {
      throw new AutoloopOperationError(
        'AUTOLOOP_CONTROL_NOT_PERSISTED',
        'Planner control claims have no matching persisted event for this physical generation',
      );
    }
  }
  const hasVerifiedControl = expected.expectedControl !== undefined && result.persistedControl !== undefined;
  if (expected.requireLogicalResult !== false && !result.reply.trim() && !hasVerifiedControl) {
    throw new AutoloopOperationError(
      'AUTOLOOP_EMPTY_REPLY',
      'Planner transport completed without a non-empty logical reply',
    );
  }
}

type PendingSendTimeout = Omit<SendTimeoutPayload, 'error'>;

/**
 * Adapter send deadlines use one of these explicit signals. Deliberately do
 * not classify arbitrary messages containing "timeout": configuration errors
 * and other subprocess failures must retain reset-once/retry-once recovery.
 */
function genuineSendTimeoutMessage(error: unknown): string | null {
  const record = error as { name?: unknown; code?: unknown; message?: unknown } | null;
  const message = typeof error === 'string' ? error : typeof record?.message === 'string' ? record.message : '';
  const namedTimeout = record?.name === 'TimeoutError';
  const codedTimeout = record?.code === 'ETIMEDOUT';
  const adapterTimeout = /^Timeout waiting for (?:(?:.+ )?response|.+ turn to complete)$/i.test(message);
  return namedTimeout || codedTimeout || adapterTimeout ? message || 'Agent send timed out' : null;
}

/**
 * How many settled logical dispatches stay cached for dedup. One iteration
 * spends a handful, so this holds many iterations' worth of replay window while
 * keeping the retained diffs bounded.
 */
const MAX_RETAINED_DISPATCHES = 64;

/**
 * Hash only immutable logical routing identity. `msg_id` distinguishes two
 * intentional sends with otherwise identical content; envelope timestamps,
 * wall-clock time, random values, retry attempts, and mutable counters are not
 * inputs, so re-delivery in this or another dispatcher derives the same ID.
 */
function deriveDispatchId(runId: string, env: AnyAutoloopMessage): string {
  const identity = [] as unknown[];
  Object.defineProperty(identity, '0', { enumerable: true, value: runId });
  Object.defineProperty(identity, '1', { enumerable: true, value: env.msg_id });
  Object.defineProperty(identity, '2', { enumerable: true, value: env.iter });
  Object.defineProperty(identity, '3', { enumerable: true, value: env.from });
  Object.defineProperty(identity, '4', { enumerable: true, value: env.to });
  Object.defineProperty(identity, '5', { enumerable: true, value: env.type });
  Object.setPrototypeOf(identity, null);
  Object.freeze(identity);
  return `dispatch_${createHash('sha256').update(JSON.stringify(identity)).digest('hex')}`;
}

interface AutoloopRoleSelection {
  engine: EngineType;
  /** User-specified model. Undefined means use the role default for Claude, otherwise the engine default. */
  model?: string;
  /** Trusted config supplied at autoloop start/resume; never accepted from Planner output or written to the ledger. */
  customEngine?: CustomEngineConfig;
}

interface DecisionLogEntry {
  ts: string;
  kind:
    | 'terminate'
    | 'reset_agent'
    | 'update_push_policy'
    | 'compact'
    | 'spawn_coder'
    | 'spawn_reviewer'
    | 'spawn_subagents'
    | 'request_review'
    | 'planner_turn_control'
    | 'phase_error'
    | 'send_timeout'
    | 'policy_silence_blocked';
  actor: 'planner' | 'runner' | 'dispatcher';
  payload: Record<string, unknown>;
}

type AgentGenerationEventKind =
  | 'agent_generation_reserved'
  | 'agent_generation_started'
  | 'agent_generation_lease_renewed'
  | 'agent_generation_orphaned'
  | 'agent_generation_released';

type AutoloopAgentConflictCode =
  | 'AUTOLOOP_AGENT_LIVE_CONFLICT'
  | 'AUTOLOOP_AGENT_LIVENESS_UNKNOWN'
  | 'AUTOLOOP_AGENT_LEASE_ACTIVE'
  | 'AUTOLOOP_AGENT_GENERATION_CONFLICT'
  | 'AUTOLOOP_AGENT_ROLLBACK_POSTCONDITION_FAILED'
  | 'AUTOLOOP_AGENT_LEDGER_INVALID';

/** Coalesce physical-agent reconciliation across dispatcher handles owned by this process. */
const AGENT_START_OPERATIONS = new Map<string, Promise<void>>();

export class AutoloopAgentConflictError extends Error {
  constructor(
    readonly code: AutoloopAgentConflictCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = 'AutoloopAgentConflictError';
  }
}

function isAgentGenerationEventKind(value: unknown): value is AgentGenerationEventKind {
  return (
    value === 'agent_generation_reserved' ||
    value === 'agent_generation_started' ||
    value === 'agent_generation_lease_renewed' ||
    value === 'agent_generation_orphaned' ||
    value === 'agent_generation_released'
  );
}

function asPhysicalAgentGeneration(value: unknown): PhysicalAgentGeneration | null {
  if (typeof value !== 'object' || value === null) return null;
  const candidate = value as Partial<PhysicalAgentGeneration>;
  if (
    (candidate.role !== 'planner' && candidate.role !== 'coder' && candidate.role !== 'reviewer') ||
    !Number.isInteger(candidate.generation) ||
    typeof candidate.session_name !== 'string' ||
    typeof candidate.owner_instance_id !== 'string' ||
    typeof candidate.created_at !== 'string' ||
    typeof candidate.last_activity_at !== 'string' ||
    typeof candidate.lease_expires_at !== 'string' ||
    (candidate.state !== 'live' &&
      candidate.state !== 'stale' &&
      candidate.state !== 'orphaned' &&
      candidate.state !== 'released') ||
    (candidate.session_id !== undefined && typeof candidate.session_id !== 'string')
  ) {
    return null;
  }
  return { ...candidate } as PhysicalAgentGeneration;
}

export class ClaudeAgentDispatcher extends EventEmitter implements AgentDispatcher {
  readonly config: ClaudeAgentDispatcherConfig;
  private logger: Logger;
  private plannerName: string;
  private coderName: string;
  private reviewerName: string;
  private plannerStarted = false;
  private coderStarted = false;
  private reviewerStarted = false;
  /** Set synchronously when shutdown begins; no late turn may publish effects. */
  private terminal = false;
  private plannerSystemPrompt: string;
  private coderSystemPrompt: string;
  private reviewerSystemPrompt: string;
  private reviewerSessionPrompt: string | null = null;
  private plannerSelection: AutoloopRoleSelection;
  private coderSelection: AutoloopRoleSelection;
  private reviewerSelection: AutoloopRoleSelection;
  private readonly runtimeProbe: AgentRuntimeProbe;
  private readonly ownerInstanceId: string;
  private readonly now: () => Date;
  private readonly agentLeaseMs: number;
  /** Where Reviewer reads from. Created lazily by stageReviewSandbox(). */
  private reviewerSandboxDir: string;
  private ledgerDir: string;
  private readonly secureLedger: SecureAutoloopLedger;
  /**
   * One promise per immutable logical dispatch, so a re-delivered message is
   * coalesced onto the first send instead of spending a second agent turn.
   *
   * Bounded, because what these promises resolve to is not small: a Coder
   * dispatch returns `iter_artifacts` carrying the iteration's entire diff, and
   * a run may legitimately last up to `MAX_AUTOLOOP_HARD_TIMEOUT_MS` (72 h).
   * Retaining every one of those for the run's lifetime grows without limit.
   *
   * Evicting the oldest is safe: the queue consumes each message once, and the
   * only replay path (`restorePausedMessages`) re-queues messages that were
   * parked *before* delivery. Re-delivery therefore happens within a few
   * messages of the original, far inside this window — while an entry still
   * in flight is never evicted, so concurrent duplicates always coalesce.
   */
  private logicalDispatches = new Map<string, Promise<AnyAutoloopMessage[]>>();
  private readonly settledDispatches = new Set<string>();
  /**
   * Bounded heavy preparation state. Settled entries retain no Promise or
   * PreparedReviewRequest payload; Task 5 replaces this with the durable outbox.
   */
  private readonly reviewRequests = new Map<
    string,
    { digest: string; pending?: Promise<PreparedReviewRequest>; settled: boolean }
  >();
  /**
   * Prepared messages whose first in-process queue handoff was interrupted.
   * Task 5 replaces this bounded retry bridge with a restart-durable outbox.
   */
  private readonly releasedReviewRequests = new Map<
    string,
    {
      digest: string;
      prepared: PreparedReviewRequest;
      claimed: boolean;
      reconcileDecision?: Readonly<Record<string, unknown>>;
    }
  >();
  /**
   * Run-local accepted identity claims, keyed only by SHA-256(idempotency_key).
   * At capacity a new identity fails closed so an accepted identity is never
   * forgotten during this process lifetime. Task 5 owns restart durability.
   */
  private readonly reviewRequestIdentityHistory = new Map<string, string>();
  /** Lazy bounded view of durable request_review claims in decisions.jsonl. */
  private durableReviewRequestClaims: Map<string, IndexedReviewRequestClaim> | undefined;
  /** Distinct checkpoint preparations currently holding heavyweight process and artifact state. */
  private activeReviewRequestPreparations = 0;
  /** Planner's compatibility effect owns its commit hook after the configured handler returns. */
  private spawnCommitDeferralDepth = 0;
  /** Failed start errors whose physical generation was reconciled as durable/live. */
  private readonly reconciledStartFailures = new Map<AutoloopRoleName, unknown>();
  /** Serializes selection, startup, and rollback as one observable subagent transition. */
  private subagentSpawnTail: Promise<void> = Promise.resolve();
  /** Per-run FIFO gate for the Reviewer's one mutable sandbox and session. */
  private reviewerDispatchTail: Promise<void> = Promise.resolve();

  constructor(config: ClaudeAgentDispatcherConfig) {
    super();
    this.config = config;
    this.logger = config.logger ?? nullLogger;
    this.plannerName = `autoloop-${config.runId}-planner`;
    this.coderName = `autoloop-${config.runId}-coder`;
    this.reviewerName = `autoloop-${config.runId}-reviewer`;

    const promptPath = config.plannerPromptPath ?? resolveDefaultPlannerPrompt();
    this.plannerSystemPrompt = fs.readFileSync(promptPath, 'utf-8');
    this.coderSystemPrompt = fs.readFileSync(config.coderPromptPath ?? resolveDefaultCoderPrompt(), 'utf-8');
    this.reviewerSystemPrompt = fs.readFileSync(config.reviewerPromptPath ?? resolveDefaultReviewerPrompt(), 'utf-8');
    this.plannerSelection = {
      engine: config.plannerEngine ?? 'claude',
      model: config.plannerModel,
      customEngine: config.plannerCustomEngine,
    };
    this.coderSelection = {
      engine: config.coderEngine ?? 'claude',
      model: config.coderModel,
      customEngine: config.coderCustomEngine,
    };
    this.reviewerSelection = {
      engine: config.reviewerEngine ?? 'claude',
      model: config.reviewerModel,
      customEngine: config.reviewerCustomEngine,
    };
    this.runtimeProbe = config.runtimeProbe ?? config.manager;
    const ownerInstanceId = config.ownerInstanceId ?? config.manager.autoloopOwnerInstanceId;
    if (!ownerInstanceId || !isRecoverableAgentOwnerInstanceId(ownerInstanceId)) {
      throw new AutoloopAgentReleaseOwnerError(ownerInstanceId ?? 'missing');
    }
    this.ownerInstanceId = ownerInstanceId;
    this.now = config.now ?? (() => new Date());
    this.agentLeaseMs = config.agentLeaseMs ?? DEFAULT_ACTIVITY_LEASE_MS;
    this.secureLedger =
      config.secureLedger ?? SecureAutoloopLedger.open(config.workspace, config.runId, { create: true });
    this.ledgerDir = this.secureLedger.directory;
    this.reviewerSandboxDir = path.join(this.ledgerDir, 'reviewer_sandbox');
  }

  get sessionNames(): { planner: string; coder: string; reviewer: string } {
    return { planner: this.plannerName, coder: this.coderName, reviewer: this.reviewerName };
  }

  /** The run-scoped capability pinned when this dispatcher was constructed. */
  get secureLedgerCapability(): SecureAutoloopLedger {
    return this.secureLedger;
  }

  private sessionNameFor(role: AutoloopRoleName): string {
    return role === 'planner' ? this.plannerName : role === 'coder' ? this.coderName : this.reviewerName;
  }

  private roleStarted(role: AutoloopRoleName): boolean {
    return role === 'planner' ? this.plannerStarted : role === 'coder' ? this.coderStarted : this.reviewerStarted;
  }

  private setRoleStarted(role: AutoloopRoleName, started: boolean): void {
    if (role === 'planner') this.plannerStarted = started;
    else if (role === 'coder') this.coderStarted = started;
    else this.reviewerStarted = started;
  }

  private takeReconciledStartFailure(role: AutoloopRoleName, error: unknown): boolean {
    if (!this.reconciledStartFailures.has(role) || this.reconciledStartFailures.get(role) !== error) return false;
    this.reconciledStartFailures.delete(role);
    return true;
  }

  private readGenerationHistory(role: AutoloopRoleName): PhysicalAgentGeneration[] {
    const history: PhysicalAgentGeneration[] = [];
    const lines = (this.secureLedger.readFlatFile('agent-generations.jsonl') ?? '').split('\n');
    for (const line of lines) {
      if (!line) continue;
      let entry: { kind?: unknown; payload?: unknown };
      try {
        entry = JSON.parse(line) as { kind?: unknown; payload?: unknown };
      } catch {
        throw new AutoloopAgentConflictError(
          'AUTOLOOP_AGENT_LEDGER_INVALID',
          `Autoloop agent generation ledger for '${this.config.runId}' contains malformed JSON`,
        );
      }
      if (!isAgentGenerationEventKind(entry.kind)) continue;
      const generation = asPhysicalAgentGeneration(entry.payload);
      if (!generation) {
        throw new AutoloopAgentConflictError(
          'AUTOLOOP_AGENT_LEDGER_INVALID',
          `Autoloop agent generation ledger for '${this.config.runId}' contains invalid ${String(entry.kind)} evidence`,
        );
      }
      if (generation.role === role && generation.session_name === this.sessionNameFor(role)) {
        history.push(generation);
      }
    }
    return history;
  }

  private currentGeneration(role: AutoloopRoleName): PhysicalAgentGeneration | undefined {
    return this.readGenerationHistory(role).at(-1);
  }

  private appendGenerationEvent(kind: AgentGenerationEventKind, generation: PhysicalAgentGeneration): void {
    const line = JSON.stringify({
      schema_version: LEDGER_SCHEMA_VERSION,
      ts: this.now().toISOString(),
      kind,
      actor: 'dispatcher',
      payload: { ...generation },
    });
    this.secureLedger.appendFlatFile('agent-generations.jsonl', `${line}\n`, true);
  }

  private conflict(code: AutoloopAgentConflictCode, role: AutoloopRoleName, detail: string): never {
    const name = this.sessionNameFor(role);
    throw new AutoloopAgentConflictError(code, `Autoloop session name '${name}' ${detail}`);
  }

  private nextGeneration(role: AutoloopRoleName): number {
    return this.readGenerationHistory(role).reduce((highest, entry) => Math.max(highest, entry.generation), 0) + 1;
  }

  private newGeneration(role: AutoloopRoleName): PhysicalAgentGeneration & { session_id: string } {
    const now = this.now();
    const timestamp = now.toISOString();
    return {
      role,
      generation: this.nextGeneration(role),
      session_name: this.sessionNameFor(role),
      session_id: randomUUID(),
      owner_instance_id: this.ownerInstanceId,
      created_at: timestamp,
      last_activity_at: timestamp,
      lease_expires_at: new Date(now.getTime() + this.agentLeaseMs).toISOString(),
      state: 'stale',
    };
  }

  private async releaseGeneration(
    generation: PhysicalAgentGeneration,
    orphaned: boolean,
    onReleaseCommitted?: () => void,
  ): Promise<void> {
    const exactCurrentGeneration = (): PhysicalAgentGeneration => {
      const current = this.currentGeneration(generation.role);
      if (
        !current ||
        current.generation !== generation.generation ||
        current.owner_instance_id !== generation.owner_instance_id ||
        current.session_id !== generation.session_id
      ) {
        this.conflict(
          'AUTOLOOP_AGENT_GENERATION_CONFLICT',
          generation.role,
          `changed while generation ${generation.generation} was being released`,
        );
      }
      return current;
    };

    exactCurrentGeneration();
    const observedAt = this.now().toISOString();
    const released = await this.runtimeProbe.releaseReservation(generation.session_name, generation.generation, {
      expectedOwnerInstanceId: generation.owner_instance_id,
      expectedSessionId: generation.session_id,
      releaseOwnerInstanceId: this.ownerInstanceId,
      beforeRelease: () => {
        const current = exactCurrentGeneration();
        if (orphaned && current.state !== 'orphaned' && current.state !== 'released') {
          this.appendGenerationEvent('agent_generation_orphaned', {
            ...current,
            last_activity_at: observedAt,
            state: 'orphaned',
          });
        }
      },
      persistReleaseEvidence: () => {
        const current = exactCurrentGeneration();
        if (current.state === 'released') {
          // A prior process may have appended this row and crashed (or thrown)
          // before its durability barrier completed. Re-flush the authoritative
          // ledger before allowing the registry tombstone to commit.
          this.secureLedger.flushFlatFile('agent-generations.jsonl');
          onReleaseCommitted?.();
          return;
        }
        if (orphaned && current.state !== 'orphaned') {
          this.conflict(
            'AUTOLOOP_AGENT_GENERATION_CONFLICT',
            generation.role,
            `has no durable orphan evidence for generation ${generation.generation}`,
          );
        }
        this.appendGenerationEvent('agent_generation_released', {
          ...current,
          last_activity_at: observedAt,
          state: 'released',
        });
        onReleaseCommitted?.();
      },
    });
    if (!released) {
      this.conflict(
        'AUTOLOOP_AGENT_GENERATION_CONFLICT',
        generation.role,
        `could not compare-and-release generation ${generation.generation}`,
      );
    }
    if (exactCurrentGeneration().state !== 'released') {
      this.conflict(
        'AUTOLOOP_AGENT_GENERATION_CONFLICT',
        generation.role,
        `has no durable release evidence for generation ${generation.generation}`,
      );
    }
  }

  private async releaseStoppedGeneration(role: AutoloopRoleName): Promise<void> {
    const current = this.currentGeneration(role);
    if (!current || current.state === 'released') return;
    const runtime = await this.runtimeProbe.inspect(current.session_name, current.session_id);
    if (runtime !== 'absent') {
      this.conflict(
        runtime === 'live' ? 'AUTOLOOP_AGENT_LIVE_CONFLICT' : 'AUTOLOOP_AGENT_LIVENESS_UNKNOWN',
        role,
        `could not prove generation ${current.generation} absent after stop`,
      );
    }
    await this.releaseGeneration(current, false);
  }

  private async releaseLegacyReservation(role: AutoloopRoleName): Promise<void> {
    const sessionName = this.sessionNameFor(role);
    const observedAt = this.now().toISOString();
    const legacy: PhysicalAgentGeneration = {
      role,
      generation: 0,
      session_name: sessionName,
      owner_instance_id: 'legacy-registry',
      created_at: observedAt,
      last_activity_at: observedAt,
      lease_expires_at: observedAt,
      state: 'orphaned',
    };
    const released = await this.runtimeProbe.releaseReservation(sessionName, 0, {
      expectedOwnerInstanceId: legacy.owner_instance_id,
      expectedSessionId: legacy.session_id,
      releaseOwnerInstanceId: this.ownerInstanceId,
      beforeRelease: () => {
        const current = this.currentGeneration(role);
        if (!current) this.appendGenerationEvent('agent_generation_orphaned', legacy);
      },
      persistReleaseEvidence: () => {
        const current = this.currentGeneration(role);
        if (current?.generation === 0 && current.state === 'released') return;
        if (current?.generation !== 0 || current.state !== 'orphaned') {
          this.conflict('AUTOLOOP_AGENT_GENERATION_CONFLICT', role, 'has invalid legacy orphan evidence');
        }
        this.appendGenerationEvent('agent_generation_released', { ...current, state: 'released' });
      },
    });
    if (released) {
      const current = this.currentGeneration(role);
      if (current?.generation !== 0 || current.state !== 'released') {
        this.conflict('AUTOLOOP_AGENT_GENERATION_CONFLICT', role, 'has no durable legacy release evidence');
      }
    }
  }

  private async prepareGeneration(
    role: AutoloopRoleName,
  ): Promise<{ generation: PhysicalAgentGeneration; reuseLiveSession: boolean }> {
    const current = this.currentGeneration(role);
    const sessionName = this.sessionNameFor(role);

    if (current && current.state !== 'released') {
      const runtime = await this.runtimeProbe.inspect(sessionName, current.session_id);
      if (runtime === 'unknown') {
        this.conflict('AUTOLOOP_AGENT_LIVENESS_UNKNOWN', role, 'has unknown runtime liveness');
      }
      if (runtime === 'live') {
        if (current.owner_instance_id !== this.ownerInstanceId) {
          this.conflict('AUTOLOOP_AGENT_LIVE_CONFLICT', role, 'is already in use by a live owner');
        }
        if (!this.config.manager.reserveAgentGeneration(current, this.config.workspace)) {
          this.conflict(
            'AUTOLOOP_AGENT_GENERATION_CONFLICT',
            role,
            `no longer belongs to generation ${current.generation}`,
          );
        }
        const now = this.now();
        const renewed: PhysicalAgentGeneration = {
          ...current,
          last_activity_at: now.toISOString(),
          lease_expires_at: new Date(now.getTime() + this.agentLeaseMs).toISOString(),
          state: 'live',
        };
        this.appendGenerationEvent('agent_generation_lease_renewed', renewed);
        return { generation: renewed, reuseLiveSession: true };
      }

      const leaseExpiresAt = Date.parse(current.lease_expires_at);
      if (Number.isNaN(leaseExpiresAt)) {
        this.conflict('AUTOLOOP_AGENT_LEDGER_INVALID', role, 'has an invalid durable lease expiry');
      }
      if (current.state !== 'orphaned' && leaseExpiresAt > this.now().getTime()) {
        this.conflict('AUTOLOOP_AGENT_LEASE_ACTIVE', role, 'has an unexpired owner lease');
      }
      await this.releaseGeneration(current, true);
    } else {
      const runtime = await this.runtimeProbe.inspect(sessionName);
      if (runtime === 'live') {
        this.conflict('AUTOLOOP_AGENT_LIVE_CONFLICT', role, 'is already in use by a live owner');
      }
      if (runtime === 'unknown') {
        this.conflict('AUTOLOOP_AGENT_LIVENESS_UNKNOWN', role, 'has unknown runtime liveness');
      }
      if (!current) await this.releaseLegacyReservation(role);
    }

    const generation = this.newGeneration(role);
    let reserved = this.config.manager.reserveAgentGeneration(generation, this.config.workspace);
    if (!reserved && current?.state === 'released') {
      // Release evidence may have survived a crash before the registry's final
      // tombstone transition. Finish that exact transition, then retry once.
      await this.releaseGeneration(current, false);
      reserved = this.config.manager.reserveAgentGeneration(generation, this.config.workspace);
    }
    if (!reserved) {
      this.conflict(
        'AUTOLOOP_AGENT_GENERATION_CONFLICT',
        role,
        `could not reserve generation ${generation.generation}`,
      );
    }
    try {
      this.appendGenerationEvent('agent_generation_reserved', generation);
    } catch (err) {
      // The row is authoritative once its bytes have been written and fsynced,
      // even if the directory-entry barrier could not be completed. Rolling
      // back the registry reservation here would contradict durable evidence
      // and allow a duplicate logical generation on retry.
      if (isCommittedSecureLedgerError(err)) throw err;
      let rolledBack = false;
      let rollbackError: unknown;
      try {
        rolledBack = await this.runtimeProbe.releaseReservation(generation.session_name, generation.generation, {
          expectedOwnerInstanceId: generation.owner_instance_id,
          expectedSessionId: generation.session_id,
          rollbackUncommittedReservation: true,
        });
      } catch (releaseErr) {
        rollbackError = releaseErr;
      }
      if (!rolledBack) {
        const appendMessage = err instanceof Error ? err.message : String(err);
        const rollbackContext =
          rollbackError === undefined
            ? ''
            : `; rollback failed with ${rollbackError instanceof Error ? rollbackError.message : String(rollbackError)}`;
        throw new AutoloopAgentConflictError(
          'AUTOLOOP_AGENT_ROLLBACK_POSTCONDITION_FAILED',
          `Autoloop session name '${generation.session_name}' could not roll back uncommitted generation ${generation.generation} after reservation ledger append failed: ${appendMessage}${rollbackContext}`,
          { cause: err },
        );
      }
      throw err;
    }
    return { generation, reuseLiveSession: false };
  }

  private async ensureAgentSession(
    role: AutoloopRoleName,
    start: (generation: PhysicalAgentGeneration) => Promise<void>,
  ): Promise<void> {
    if (this.terminal) return;
    if (this.roleStarted(role) && this.currentGeneration(role)?.state === 'live') return;
    const operationKey = `${this.ownerInstanceId}\0${this.sessionNameFor(role)}`;
    const existing = AGENT_START_OPERATIONS.get(operationKey);
    if (existing) {
      await existing;
      if (!this.terminal) this.setRoleStarted(role, true);
      return;
    }

    const operation = (async () => {
      const prepared = await this.prepareGeneration(role);
      if (this.terminal) {
        if (!prepared.reuseLiveSession) await this.releaseGeneration(prepared.generation, true);
        return;
      }
      if (prepared.reuseLiveSession) {
        if (!this.terminal) this.setRoleStarted(role, true);
        return;
      }

      let physicalStarted = false;
      try {
        await start(prepared.generation);
        physicalStarted = true;
        if (this.terminal) {
          await this.config.manager.stopSession(prepared.generation.session_name);
          await this.releaseGeneration(prepared.generation, true);
          return;
        }
        this.appendGenerationEvent('agent_generation_started', {
          ...prepared.generation,
          last_activity_at: this.now().toISOString(),
          state: 'live',
        });
        this.setRoleStarted(role, true);
      } catch (err) {
        // start() returned successfully, so this exact generation was
        // physically created even when its started-event append failed. Freeze
        // the role before any awaited cleanup so concurrent selection changes
        // cannot rebind that process while its survival is being determined.
        if (physicalStarted) this.setRoleStarted(role, true);
        if (physicalStarted) {
          try {
            await this.config.manager.stopSession(prepared.generation.session_name);
          } catch {
            // The runtime probe below decides whether release is safe.
          }
        }
        let runtime: AgentRuntimeLiveness = 'unknown';
        try {
          runtime = await this.runtimeProbe.inspect(prepared.generation.session_name, prepared.generation.session_id);
        } catch (cleanupErr) {
          this.logger.warn?.(
            `[autoloop] failed to inspect generation ${prepared.generation.generation} after startup error: ${(cleanupErr as Error).message}`,
          );
        }
        if (runtime === 'absent') {
          try {
            await this.releaseGeneration(prepared.generation, true);
            if (physicalStarted) this.setRoleStarted(role, false);
          } catch (cleanupErr) {
            this.logger.warn?.(
              `[autoloop] failed to release generation ${prepared.generation.generation} after startup error: ${(cleanupErr as Error).message}`,
            );
          }
        } else if (physicalStarted) {
          let reconciled = false;
          try {
            const current = this.currentGeneration(role);
            if (
              !current ||
              current.generation !== prepared.generation.generation ||
              current.owner_instance_id !== prepared.generation.owner_instance_id ||
              current.session_id !== prepared.generation.session_id
            ) {
              this.conflict(
                'AUTOLOOP_AGENT_GENERATION_CONFLICT',
                role,
                `changed while generation ${prepared.generation.generation} startup was being reconciled`,
              );
            }
            if (current.state === 'live') {
              // A committed append can throw after writing the row. Re-flush
              // that authoritative row instead of appending duplicate evidence.
              this.secureLedger.flushFlatFile('agent-generations.jsonl');
              reconciled = true;
            } else if (current.state === 'stale') {
              this.appendGenerationEvent('agent_generation_started', {
                ...prepared.generation,
                last_activity_at: this.now().toISOString(),
                state: 'live',
              });
              reconciled = true;
            } else {
              this.conflict(
                'AUTOLOOP_AGENT_GENERATION_CONFLICT',
                role,
                `has invalid ${current.state} evidence while generation ${current.generation} startup is surviving`,
              );
            }
          } catch (cleanupErr) {
            // The original startup error remains the public failure. Keeping
            // roleStarted=true preserves the exact in-memory selection and
            // prevents a duplicate or cross-engine rebind until reconciliation
            // can be completed safely.
            this.logger.warn?.(
              `[autoloop] failed to reconcile surviving generation ${prepared.generation.generation} after startup error: ${(cleanupErr as Error).message}`,
            );
          }
          if (reconciled) this.reconciledStartFailures.set(role, err);
        }
        throw err;
      }
    })();
    AGENT_START_OPERATIONS.set(operationKey, operation);
    try {
      await operation;
    } finally {
      if (AGENT_START_OPERATIONS.get(operationKey) === operation) AGENT_START_OPERATIONS.delete(operationKey);
    }
  }

  async init(state: AutoloopState): Promise<void> {
    void state;
    await this.ensurePlanner();
  }

  async shutdown(reason: string, opts: { purge?: boolean } = {}): Promise<void> {
    this.terminal = true;
    if (!(reason === 'start-failed' && this.config.suppressFailedStartAudit)) {
      this.appendDecisionLog({
        kind: 'terminate',
        actor: reason === 'phase_error_circuit' ? 'runner' : 'planner',
        payload: { reason },
      });
    }
    // Best-effort cleanup. Stopping a non-existent session is a no-op.
    // keepPersisted: true keeps the persistedSessions entry on disk so a
    // later /autoloop/<id>/resume can re-attach the Planner's Claude
    // conversation. Only autoloopDelete passes purge:true (real teardown).
    for (const role of ['planner', 'coder', 'reviewer'] as const) {
      const name = this.sessionNameFor(role);
      try {
        await this.config.manager.stopSession(name, { keepPersisted: !opts.purge });
        await this.releaseStoppedGeneration(role);
      } catch (err) {
        this.logger.warn?.(`[autoloop] failed to stop ${name}: ${(err as Error).message}`);
      }
    }
  }

  async deliver(env: AnyAutoloopMessage): Promise<AnyAutoloopMessage[]> {
    // Runner callers validate before enqueueing, but direct/recovery callers
    // share this boundary. In particular, a forged review payload must not
    // choose a different iteration after the envelope has been routed.
    const message = canonicalizeMessage(env);
    if (this.terminal) return [];
    const dispatchId = deriveDispatchId(this.config.runId, message);
    const existing = this.logicalDispatches.get(dispatchId);
    if (existing) return await existing;

    const pending = this.deliverOnce(message, dispatchId).catch((error: unknown) => {
      const operationError = message.to === 'planner' ? normalizePlannerOperationError(error) : error;
      const shouldAuditOperationFailure =
        operationError instanceof AutoloopOperationError ||
        ((message.to === 'coder' || message.to === 'reviewer') && isCommittedSecureLedgerError(operationError));
      if (
        shouldAuditOperationFailure &&
        !(operationError instanceof CommittedPlannerControlReplayError) &&
        !(operationError instanceof PlannerControlLedgerInvalidError)
      ) {
        const committed = isCommittedSecureLedgerError(operationError);
        const auditFailure = this.appendDecisionLog({
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: message.to,
            phase: `${message.to}_turn`,
            code: operationError.code,
            ...(committed ? { committed: true, retryable: false } : {}),
            error: operationError.message,
          },
        });
        if (auditFailure) operationError.secondaryErrors.push(auditFailure);
      }
      throw operationError;
    });
    this.logicalDispatches.set(dispatchId, pending);
    // Mark settled before trimming so eviction can tell an in-flight dispatch
    // from a finished one. A rejection settles too; `deliver` still rethrows it
    // to this caller, and the entry is only a dedup record afterwards.
    void pending.then(
      () => this.settledDispatches.add(dispatchId),
      () => this.settledDispatches.add(dispatchId),
    );
    try {
      return await pending;
    } finally {
      this.trimLogicalDispatches();
    }
  }

  /** Drop the oldest settled dispatches once the retained set exceeds its cap. */
  private trimLogicalDispatches(): void {
    if (this.logicalDispatches.size <= MAX_RETAINED_DISPATCHES) return;
    for (const id of this.logicalDispatches.keys()) {
      if (this.logicalDispatches.size <= MAX_RETAINED_DISPATCHES) break;
      if (!this.settledDispatches.has(id)) continue; // still in flight — must stay
      this.logicalDispatches.delete(id);
      this.settledDispatches.delete(id);
    }
  }

  /** Current per-agent deadline, including the backward-compatible default. */
  get effectiveSendTimeoutMs(): number {
    return this.config.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS;
  }

  /**
   * Apply an already-authorized timeout migration. This seam intentionally has
   * no decrease mode: even an internal caller must supply a valid strict
   * increase, while SessionManager owns persistence and dispatch matching.
   */
  increaseSendTimeoutMs(next: number): void {
    validateAutoloopTimeoutConfig({ sendTimeoutMs: next });
    const current = this.effectiveSendTimeoutMs;
    if (next <= current) {
      throw new Error(`sendTimeoutMs must be strictly greater than the current effective value ${current}`);
    }
    this.config.sendTimeoutMs = next;
  }

  private async deliverOnce(env: AnyAutoloopMessage, dispatchId: string): Promise<AnyAutoloopMessage[]> {
    switch (env.to) {
      case 'planner':
        return await this.deliverToPlanner(env, dispatchId);
      case 'coder':
        return await this.deliverToCoder(env, dispatchId);
      case 'reviewer':
        return await this.serializeReviewerDispatch(() => this.deliverToReviewer(env, dispatchId));
      default:
        throw new Error(`[autoloop] unexpected dispatcher target: ${env.to}`);
    }
  }

  private async serializeReviewerDispatch<T>(dispatch: () => Promise<T>): Promise<T> {
    const predecessor = this.reviewerDispatchTail;
    let release!: () => void;
    this.reviewerDispatchTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await predecessor;
    try {
      return await dispatch();
    } finally {
      release();
    }
  }

  private roleModel(role: AutoloopRoleName, selection: AutoloopRoleSelection): string | undefined {
    if (selection.model !== undefined) return selection.model;
    if (selection.engine !== 'claude') return undefined;
    return role === 'planner' ? 'opus' : 'sonnet';
  }

  private validateSelection(role: AutoloopRoleName, selection: AutoloopRoleSelection): void {
    const label = role[0].toUpperCase() + role.slice(1);
    if (!ENGINE_TYPES.includes(selection.engine)) {
      throw new Error(`${label} engine '${String(selection.engine)}' is not supported`);
    }
    if (selection.engine === 'custom' && !selection.customEngine) {
      throw new Error(`${label} custom engine config is required`);
    }
  }

  /**
   * Stop a session we started during a failed spawn. Returns true only when the
   * session is genuinely gone — the caller uses that to decide whether it may
   * clear the role's `started` flag. Returning false keeps the role marked as
   * started, which is the safe lie: a later engine change is then rejected
   * instead of silently binding the run to a process that never went away.
   */
  private async stopRolledBackSession(role: AutoloopRoleName, name: string): Promise<boolean> {
    try {
      await this.config.manager.stopSession(name);
      await this.releaseStoppedGeneration(role);
      return true;
    } catch (stopErr) {
      this.logger.error?.(
        `[autoloop] rollback could not stop ${name}: ${(stopErr as Error).message} — ` +
          `leaving it marked started so a later engine change is rejected rather than silently ignored`,
      );
      this.appendDecisionLog({
        kind: 'phase_error',
        actor: 'dispatcher',
        payload: { agent: name, phase: 'rollback_stop', error: (stopErr as Error).message },
      });
      return false;
    }
  }

  /**
   * Does this engine carry conversation across sends on its own?
   *
   * claude keeps one subprocess alive; codex / codex-app resume a thread; agy
   * resumes a harvested `--conversation <uuid>`. Everything else (gemini,
   * cursor, opencode, and non-persistent custom engines) spawns a FRESH process
   * per send with zero memory of the last turn — for those the dispatcher must
   * replay the transcript in-band, or the role is amnesiac and a chat-driven
   * Planner can never remember the plan it just proposed (let alone whether the
   * user approved it).
   */
  private hasNativeConversation(selection: AutoloopRoleSelection): boolean {
    return engineHasNativeConversation(selection.engine, selection.customEngine);
  }

  /**
   * Replayed transcript for engines without native conversation. Capped so a
   * long run can't grow the prompt without bound: we keep the most recent
   * turns within REPLAY_CHAR_BUDGET, oldest dropped first.
   */
  private transcripts: Record<AutoloopRoleName, Array<{ who: 'user' | 'agent'; text: string }>> = {
    planner: [],
    coder: [],
    reviewer: [],
  };

  private recordTurn(role: AutoloopRoleName, who: 'user' | 'agent', text: string): void {
    if (!text) return;
    const log = this.transcripts[role];
    Object.defineProperty(log, String(log.length), {
      configurable: true,
      enumerable: true,
      value: { who, text },
      writable: true,
    });
    let budget = REPLAY_CHAR_BUDGET;
    let keepFrom = log.length;
    for (let i = log.length - 1; i >= 0; i--) {
      budget -= log[i].text.length;
      if (budget < 0) break;
      keepFrom = i;
    }
    if (keepFrom > 0) log.splice(0, keepFrom);
  }

  private renderHistory(role: AutoloopRoleName, selection: AutoloopRoleSelection): string | null {
    if (this.hasNativeConversation(selection)) return null;
    const log = this.transcripts[role];
    if (log.length === 0) return null;
    let history = '<conversation_history>';
    for (let index = 0; index < log.length; index += 1) {
      const entry = log[index];
      history += `\n<${entry.who}>\n${entry.text}\n</${entry.who}>`;
    }
    return `${history}\n</conversation_history>`;
  }

  private withRoleInstructions(
    role: AutoloopRoleName,
    selection: AutoloopRoleSelection,
    systemPrompt: string,
    message: string,
  ): string {
    if (selection.engine === 'claude') return message;
    let prompt = `<autoloop_role_instructions>\n${systemPrompt.trim()}\n</autoloop_role_instructions>\n`;
    const history = this.renderHistory(role, selection);
    if (history) prompt += `\n${history}\n`;
    return `${prompt}\n<autoloop_message>\n${message}\n</autoloop_message>`;
  }

  private nextCoderSelection(args: SpawnCoderArgs): AutoloopRoleSelection {
    const nextCoderEngine = args.coder_engine ?? this.coderSelection.engine;
    return {
      ...this.coderSelection,
      engine: nextCoderEngine,
      model:
        args.coder_model !== undefined
          ? args.coder_model
          : nextCoderEngine !== this.coderSelection.engine
            ? undefined
            : this.coderSelection.model,
    };
  }

  private nextReviewerSelection(args: SpawnReviewerArgs): AutoloopRoleSelection {
    const nextReviewerEngine = args.reviewer_engine ?? this.reviewerSelection.engine;
    return {
      ...this.reviewerSelection,
      engine: nextReviewerEngine,
      model:
        args.reviewer_model !== undefined
          ? args.reviewer_model
          : nextReviewerEngine !== this.reviewerSelection.engine
            ? undefined
            : this.reviewerSelection.model,
    };
  }

  private assertSelectionCanStart(role: 'coder' | 'reviewer', next: AutoloopRoleSelection): void {
    this.validateSelection(role, next);
    const current = role === 'coder' ? this.coderSelection : this.reviewerSelection;
    const changed = next.engine !== current.engine || this.roleModel(role, next) !== this.roleModel(role, current);
    if (role === 'coder' && this.coderStarted && changed) {
      throw new Error('Cannot change Coder engine or model after its session has started');
    }
    if (role === 'reviewer' && this.reviewerStarted && changed) {
      throw new Error('Cannot change Reviewer engine or model after its session has started');
    }
  }

  private requireLiveGeneration(role: 'coder' | 'reviewer'): PhysicalAgentGeneration {
    const generation = this.currentGeneration(role);
    if (!generation || generation.state !== 'live') {
      throw new Error(`Autoloop ${role} session did not create or reuse a live generation`);
    }
    return generation;
  }

  private async persistCurrentRoleSelection(): Promise<void> {
    await this.config.onRoleSelectionChanged?.({
      coder: { engine: this.coderSelection.engine, model: this.coderSelection.model },
      reviewer: { engine: this.reviewerSelection.engine, model: this.reviewerSelection.model },
    });
  }

  private async persistSurvivingRoleSelection(): Promise<void> {
    try {
      await this.persistCurrentRoleSelection();
    } catch (persistError) {
      // Rollback has already proved the physical session may still be live, so
      // retaining the in-memory selection is mandatory. Selection persistence
      // is secondary to the startup failure that led here and must not replace
      // that original public error.
      this.logger.error?.(
        `[autoloop] failed to persist a surviving role selection after startup error: ${(persistError as Error).message}`,
      );
    }
  }

  private async serializeSubagentSpawn<T>(operation: () => Promise<T>): Promise<T> {
    const predecessor = this.subagentSpawnTail;
    let release!: () => void;
    this.subagentSpawnTail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await predecessor;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  private async spawnCoderPrimitive(args: SpawnCoderArgs, record: boolean): Promise<PhysicalAgentGeneration> {
    if (this.terminal) throw new Error('Cannot start Coder after the Autoloop run became terminal');
    const nextCoder = this.nextCoderSelection(args);
    this.assertSelectionCanStart('coder', nextCoder);
    const previousCoder = this.coderSelection;
    const coderWasStarted = this.coderStarted;
    this.coderSelection = nextCoder;
    let generation: PhysicalAgentGeneration;
    try {
      await this.ensureCoder();
      if (this.terminal) throw new Error('Autoloop terminated while starting Coder');
      generation = this.requireLiveGeneration('coder');
    } catch (err) {
      // Direct primitives clean up their own failed start. The compatibility
      // wrapper passes record=false and owns every rollback attempt itself.
      if (record) {
        let restorePreviousSelection = true;
        if (!coderWasStarted && this.coderStarted) {
          if (this.takeReconciledStartFailure('coder', err)) {
            restorePreviousSelection = false;
          } else {
            const stopped = await this.stopRolledBackSession('coder', this.coderName);
            this.coderStarted = !stopped;
            restorePreviousSelection = stopped;
          }
        }
        if (restorePreviousSelection) this.coderSelection = previousCoder;
        else await this.persistSurvivingRoleSelection();
      }
      throw err;
    }
    if (record && !coderWasStarted) {
      this.appendDecisionLog({
        kind: 'spawn_coder',
        actor: 'planner',
        payload: {
          coder_engine: nextCoder.engine,
          coder_model: this.roleModel('coder', nextCoder),
        },
      });
      await this.persistCurrentRoleSelection();
    }
    return generation;
  }

  private async spawnReviewerPrimitive(args: SpawnReviewerArgs, record: boolean): Promise<PhysicalAgentGeneration> {
    if (this.terminal) throw new Error('Cannot start Reviewer after the Autoloop run became terminal');
    const nextReviewer = this.nextReviewerSelection(args);
    this.assertSelectionCanStart('reviewer', nextReviewer);
    const previousReviewer = this.reviewerSelection;
    const reviewerWasStarted = this.reviewerStarted;
    this.reviewerSelection = nextReviewer;
    let generation: PhysicalAgentGeneration;
    try {
      await this.ensureReviewer();
      if (this.terminal) throw new Error('Autoloop terminated while starting Reviewer');
      generation = this.requireLiveGeneration('reviewer');
    } catch (err) {
      // Direct primitives clean up their own failed start. The compatibility
      // wrapper passes record=false and owns every rollback attempt itself.
      if (record) {
        let restorePreviousSelection = true;
        if (!reviewerWasStarted && this.reviewerStarted) {
          if (this.takeReconciledStartFailure('reviewer', err)) {
            restorePreviousSelection = false;
          } else {
            const stopped = await this.stopRolledBackSession('reviewer', this.reviewerName);
            this.reviewerStarted = !stopped;
            restorePreviousSelection = stopped;
            if (stopped) this.reviewerSessionPrompt = null;
          }
        }
        if (restorePreviousSelection) this.reviewerSelection = previousReviewer;
        else await this.persistSurvivingRoleSelection();
      }
      throw err;
    }
    if (record && !reviewerWasStarted) {
      this.appendDecisionLog({
        kind: 'spawn_reviewer',
        actor: 'planner',
        payload: {
          reviewer_engine: nextReviewer.engine,
          reviewer_model: this.roleModel('reviewer', nextReviewer),
        },
      });
      await this.persistCurrentRoleSelection();
    }
    return generation;
  }

  /** Start only the Coder session. */
  async spawnCoder(args: SpawnCoderArgs = {}): Promise<PhysicalAgentGeneration> {
    this.assertSelectionCanStart('coder', this.nextCoderSelection(args));
    return await this.serializeSubagentSpawn(async () => {
      const wasStarted = this.coderStarted;
      const generation = await this.spawnCoderPrimitive(args, true);
      if (!wasStarted && this.spawnCommitDeferralDepth === 0) await this.config.onSpawnSubagentsCommitted?.();
      return generation;
    });
  }

  /** Start only the Reviewer session. */
  async spawnReviewer(args: SpawnReviewerArgs = {}): Promise<PhysicalAgentGeneration> {
    this.assertSelectionCanStart('reviewer', this.nextReviewerSelection(args));
    return await this.serializeSubagentSpawn(async () => {
      const wasStarted = this.reviewerStarted;
      const generation = await this.spawnReviewerPrimitive(args, true);
      if (!wasStarted && this.spawnCommitDeferralDepth === 0) await this.config.onSpawnSubagentsCommitted?.();
      return generation;
    });
  }

  /**
   * Start Coder + Reviewer sessions. Idempotent compatibility wrapper. Both
   * selections are validated before either independent primitive may start.
   */
  async spawnSubagents(args: SpawnSubagentsArgs = {}): Promise<void> {
    await this.serializeSubagentSpawn(async () => await this.spawnSubagentsTransaction(args));
  }

  private async spawnSubagentsTransaction(args: SpawnSubagentsArgs): Promise<void> {
    if (this.terminal) return;
    const coderArgs: SpawnCoderArgs = { coder_engine: args.coder_engine, coder_model: args.coder_model };
    const reviewerArgs: SpawnReviewerArgs = {
      reviewer_engine: args.reviewer_engine,
      reviewer_model: args.reviewer_model,
    };
    const nextCoder = this.nextCoderSelection(coderArgs);
    const nextReviewer = this.nextReviewerSelection(reviewerArgs);
    this.assertSelectionCanStart('coder', nextCoder);
    this.assertSelectionCanStart('reviewer', nextReviewer);

    const previousCoder = this.coderSelection;
    const previousReviewer = this.reviewerSelection;
    const coderWasStarted = this.coderStarted;
    const reviewerWasStarted = this.reviewerStarted;
    try {
      await this.spawnCoderPrimitive(coderArgs, false);
      if (this.terminal) throw new Error('Autoloop terminated while starting subagents');
      await this.spawnReviewerPrimitive(reviewerArgs, false);
      if (this.terminal) throw new Error('Autoloop terminated while starting subagents');
    } catch (err) {
      // Roll back only sessions started by this compatibility call. A failed
      // stop leaves the role marked started so later selection changes cannot
      // silently bind to the surviving process under different metadata.
      let coderSurvivedRollback = false;
      let reviewerSurvivedRollback = false;
      const coderReconciled = this.takeReconciledStartFailure('coder', err);
      const reviewerReconciled = this.takeReconciledStartFailure('reviewer', err);
      if (coderReconciled || reviewerReconciled) {
        coderSurvivedRollback = !coderWasStarted && this.coderStarted;
        reviewerSurvivedRollback = !reviewerWasStarted && this.reviewerStarted;
      } else {
        if (!coderWasStarted && this.coderStarted) {
          const stopped = await this.stopRolledBackSession('coder', this.coderName);
          this.coderStarted = !stopped;
          coderSurvivedRollback = !stopped;
        }
        if (!reviewerWasStarted && this.reviewerStarted) {
          const stopped = await this.stopRolledBackSession('reviewer', this.reviewerName);
          this.reviewerStarted = !stopped;
          reviewerSurvivedRollback = !stopped;
          if (stopped) this.reviewerSessionPrompt = null;
        }
      }
      this.coderSelection = coderSurvivedRollback ? nextCoder : previousCoder;
      this.reviewerSelection = reviewerSurvivedRollback ? nextReviewer : previousReviewer;
      if (coderSurvivedRollback || reviewerSurvivedRollback) await this.persistSurvivingRoleSelection();
      throw err;
    }
    const effectiveSelection = {
      coder: { engine: nextCoder.engine, model: nextCoder.model },
      reviewer: { engine: nextReviewer.engine, model: nextReviewer.model },
    };
    this.appendDecisionLog({
      kind: 'spawn_subagents',
      actor: 'planner',
      payload: {
        coder_engine: nextCoder.engine,
        coder_model: this.roleModel('coder', nextCoder),
        reviewer_engine: nextReviewer.engine,
        reviewer_model: this.roleModel('reviewer', nextReviewer),
      },
    });
    await this.config.onRoleSelectionChanged?.(effectiveSelection);
    if (this.spawnCommitDeferralDepth === 0) await this.config.onSpawnSubagentsCommitted?.();
  }

  private async assertWorkspaceHead(request: RequestReviewArgs): Promise<void> {
    const head = await this.runGitEvidence(
      ['git', 'rev-parse', '--verify', 'HEAD'],
      MAX_GIT_HEAD_STDOUT_BYTES,
      'workspace HEAD',
    );
    const headDetail = (head.err.length > 0 ? head.err : head.out).toString('utf8').trim().slice(0, 300);
    if (head.code !== 0) {
      throw new Error(
        `Reviewer-only checkpoint could not verify workspace HEAD (code=${head.code}): ${headDetail || 'no output'}`,
      );
    }
    const workspaceHead = head.out.toString('ascii').trim();
    if (workspaceHead !== request.checkpoint_sha) {
      throw new Error(
        `Reviewer-only checkpoint ${request.checkpoint_sha} does not match workspace HEAD ${workspaceHead || '(empty)'}`,
      );
    }
  }

  private async assertCheckpointPatch(request: RequestReviewArgs, importedPatch: Buffer): Promise<void> {
    const shown = await this.runGitEvidence(
      [
        'git',
        'show',
        '--no-ext-diff',
        '--no-textconv',
        '--format=',
        '--unified=3',
        '--no-renames',
        request.checkpoint_sha,
        '--',
      ],
      importedPatch.length,
      'checkpoint patch',
      importedPatch,
    );
    const showDetail = (shown.err.length > 0 ? shown.err : shown.out).toString('utf8').trim().slice(0, 300);
    if (shown.code !== 0) {
      throw new Error(
        `Reviewer-only checkpoint patch could not be read (code=${shown.code}): ${showDetail || 'no output'}`,
      );
    }
  }

  private preparedCheckpointReview(request: RequestReviewArgs, targetIter: number): PreparedReviewRequest {
    const payload = canonicalizeMessage(
      Msg.reviewRequest(targetIter, {
        iter: targetIter,
        ledger_path: this.ledgerDir,
        prior_metrics: [],
        ...request,
      }),
    ).payload as CheckpointReviewRequestPayload;
    return Object.freeze({
      status: 'prepared',
      target: 'reviewer',
      idempotency_key: request.idempotency_key,
      payload,
    });
  }

  private async prepareCheckpointReview(
    request: RequestReviewArgs,
    targetIter: number,
    digest: string,
  ): Promise<PreparedReviewRequest> {
    if (this.terminal) throw new Error('Cannot request review after the Autoloop run became terminal');

    // Establish repository identity before opening any caller-selected source
    // run. A wrong checkpoint cannot trigger source-ledger inspection.
    await this.assertWorkspaceHead(request);
    if (this.terminal) throw new Error('Autoloop terminated while preparing the Reviewer-only request');

    const sourceLedger =
      request.source_run_id === this.config.runId
        ? this.secureLedger
        : SecureAutoloopLedger.openReadOnly(this.config.workspace, request.source_run_id);
    const artifacts = new Map<ReviewEvidenceArtifact, Buffer>();
    const importedPatch = sourceLedger.readIterationArtifact(request.source_iter, 'diff.patch');
    if (importedPatch === undefined) {
      throw new Error(
        `Reviewer-only request requires source run '${request.source_run_id}' iter ${request.source_iter}/diff.patch`,
      );
    }
    await this.assertCheckpointPatch(request, importedPatch);
    artifacts.set('diff.patch', importedPatch);

    for (const name of ['directive.json', 'eval_output.json', 'coder_summary.txt'] as const) {
      const content = sourceLedger.readIterationArtifact(request.source_iter, name);
      if (content === undefined) {
        throw new Error(
          `Reviewer-only request requires source run '${request.source_run_id}' iter ${request.source_iter}/${name}`,
        );
      }
      artifacts.set(name, content);
    }
    if (this.terminal) throw new Error('Autoloop terminated while preparing the Reviewer-only request');

    const prepared = this.preparedCheckpointReview(request, targetIter);

    // Import exact source bytes into this run's immutable iteration boundary.
    // This gives the existing Reviewer sandbox staging path a complete local
    // artifact set without creating or directing a Coder.
    for (const [name, content] of artifacts) {
      this.secureLedger.writeIterationArtifact(targetIter, name, content);
    }
    const decisionPayload = reviewRequestDecisionPayload(request, targetIter, digest);
    this.persistReviewRequestDecision(decisionPayload);

    return prepared;
  }

  private trimReviewRequests(): void {
    if (this.reviewRequests.size <= MAX_RETAINED_DISPATCHES) return;
    for (const [key, entry] of this.reviewRequests) {
      if (this.reviewRequests.size <= MAX_RETAINED_DISPATCHES) break;
      if (!entry.settled) continue;
      this.reviewRequests.delete(key);
    }
  }

  private reviewRequestIdentityHash(idempotencyKey: string): string {
    return createHash('sha256').update(idempotencyKey).digest('hex');
  }

  private loadDurableReviewRequestClaims(forceRefresh = false): Map<string, IndexedReviewRequestClaim> {
    if (!forceRefresh && this.durableReviewRequestClaims) return this.durableReviewRequestClaims;
    const index = indexReviewRequestClaims(readBoundedDecisionLedger(this.secureLedger));
    if (index.size > 0) {
      // A cold process must establish the file + parent-directory barrier
      // before treating an existing request_review row as authority.
      this.secureLedger.flushFlatFile('decisions.jsonl');
    }
    const complete = mergeReviewRequestClaimIndexes(index, this.durableReviewRequestClaims);
    this.durableReviewRequestClaims = complete;
    return complete;
  }

  private findDurableReviewRequest(
    expected: Readonly<Record<string, unknown>>,
    forceRefresh = false,
  ): 'none' | 'matching' | 'conflicting' {
    const wanted = reviewRequestClaim(expected);
    if (!wanted?.signature) throw new Error('request_review durable claim is not canonical');
    const observed = this.loadDurableReviewRequestClaims(forceRefresh).get(wanted.identityHash);
    if (!observed) return 'none';
    if (observed.conflicting || observed.signature !== wanted.signature) return 'conflicting';
    return 'matching';
  }

  private cacheDurableReviewRequest(expected: Readonly<Record<string, unknown>>): void {
    const claim = reviewRequestClaim(expected);
    if (!claim?.signature) throw new Error('request_review durable claim is not canonical');
    const current = this.durableReviewRequestClaims;
    if (!current) throw new Error('request_review durable claim cache is unavailable');
    const index = new Map(current);
    const existing = index.get(claim.identityHash);
    if (existing && (existing.conflicting || existing.signature !== claim.signature)) {
      index.set(claim.identityHash, { conflicting: true });
    } else {
      index.set(claim.identityHash, { signature: claim.signature, conflicting: false });
    }
    this.durableReviewRequestClaims = index;
  }

  private durableReviewRequestIdentityCount(): number {
    const durable = this.durableReviewRequestClaims;
    let count = durable?.size ?? 0;
    for (const identityHash of this.reviewRequestIdentityHistory.keys()) {
      if (!durable?.has(identityHash)) count += 1;
    }
    return count;
  }

  private persistReviewRequestDecision(decisionPayload: Readonly<Record<string, unknown>>): void {
    const decision = {
      ts: this.now().toISOString(),
      kind: 'request_review',
      actor: 'planner',
      payload: decisionPayload,
    } satisfies DecisionLogEntry;
    try {
      this.secureLedger.appendFlatFile('decisions.jsonl', `${JSON.stringify(decision)}\n`, true);
      this.cacheDurableReviewRequest(decisionPayload);
    } catch (error) {
      if (!isCommittedSecureLedgerError(error) || error.operation !== 'secure_ledger_append') throw error;
      try {
        if (this.findDurableReviewRequest(decisionPayload, true) !== 'matching') throw error;
      } catch (reconciliationError) {
        if (reconciliationError !== error) {
          error.secondaryErrors.push(
            reconciliationError instanceof Error ? reconciliationError : new Error(String(reconciliationError)),
          );
        }
        throw error;
      }
    }
  }

  /** Re-arm one prepared message when its current queue handoff aborts. */
  private releaseReviewRequest(idempotencyKey: string, payload: CheckpointReviewRequestPayload): void {
    const identityHash = this.reviewRequestIdentityHash(idempotencyKey);
    const digest = this.reviewRequestIdentityHistory.get(identityHash);
    if (digest === undefined) return;
    const existing = this.releasedReviewRequests.get(identityHash);
    if (existing) {
      if (existing.digest === digest) existing.claimed = false;
      return;
    }
    this.releasedReviewRequests.set(identityHash, {
      digest,
      prepared: Object.freeze({
        status: 'prepared',
        target: 'reviewer',
        idempotency_key: idempotencyKey,
        payload,
      }),
      claimed: false,
    });
  }

  /** Complete the in-process handoff; later same-key calls are duplicates. */
  private acceptReviewRequest(idempotencyKey: string): void {
    this.releasedReviewRequests.delete(this.reviewRequestIdentityHash(idempotencyKey));
  }

  /** Prepare an existing checkpoint for one Runner-routed Reviewer delivery. */
  async requestReview(args: RequestReviewArgs, targetIter: number): Promise<ReviewRequestPreparationResult> {
    if (this.terminal) throw new Error('Cannot request review after the Autoloop run became terminal');
    const request = canonicalizeRequestReviewArgs(args);
    if (!Number.isSafeInteger(targetIter) || targetIter < 0) {
      throw new Error('request_review target iteration must be a nonnegative safe integer');
    }
    const digestMaterial = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(digestMaterial, 'target_iter', { enumerable: true, value: targetIter });
    Object.defineProperty(digestMaterial, 'request', { enumerable: true, value: request });
    const digest = createHash('sha256').update(JSON.stringify(digestMaterial)).digest('hex');
    const identityHash = this.reviewRequestIdentityHash(request.idempotency_key);
    const released = this.releasedReviewRequests.get(identityHash);
    if (released) {
      if (released.digest !== digest) {
        throw new Error(`request_review idempotency key '${request.idempotency_key}' conflicts with another request`);
      }
      if (released.reconcileDecision) {
        const durableClaim = this.findDurableReviewRequest(released.reconcileDecision, true);
        if (durableClaim === 'conflicting') {
          throw new Error(`request_review idempotency key '${request.idempotency_key}' conflicts with another request`);
        }
        if (durableClaim !== 'matching') {
          throw new Error(
            `request_review idempotency key '${request.idempotency_key}' has an unresolved committed decision`,
          );
        }
        delete released.reconcileDecision;
      }
      if (!released.claimed) {
        released.claimed = true;
        return released.prepared;
      }
      return Object.freeze({
        status: 'duplicate',
        target: 'reviewer',
        idempotency_key: request.idempotency_key,
      });
    }
    const inFlight = this.reviewRequests.get(identityHash);
    if (inFlight && !inFlight.settled && inFlight.pending) {
      if (inFlight.digest !== digest) {
        throw new Error(`request_review idempotency key '${request.idempotency_key}' conflicts with another request`);
      }
      await inFlight.pending;
      return Object.freeze({
        status: 'duplicate',
        target: 'reviewer',
        idempotency_key: request.idempotency_key,
      });
    }

    const decisionPayload = reviewRequestDecisionPayload(request, targetIter, digest);
    const durableClaim = this.findDurableReviewRequest(decisionPayload);
    if (durableClaim === 'matching') {
      this.reviewRequestIdentityHistory.set(identityHash, digest);
      return Object.freeze({
        status: 'duplicate',
        target: 'reviewer',
        idempotency_key: request.idempotency_key,
      });
    }
    if (durableClaim === 'conflicting') {
      throw new Error(`request_review idempotency key '${request.idempotency_key}' conflicts with another request`);
    }
    const acceptedDigest = this.reviewRequestIdentityHistory.get(identityHash);
    if (acceptedDigest !== undefined) {
      if (acceptedDigest !== digest) {
        throw new Error(`request_review idempotency key '${request.idempotency_key}' conflicts with another request`);
      }
      return Object.freeze({
        status: 'duplicate',
        target: 'reviewer',
        idempotency_key: request.idempotency_key,
      });
    }
    if (this.durableReviewRequestIdentityCount() >= MAX_REVIEW_REQUEST_IDENTITIES) {
      throw new Error(
        `request_review identity history reached its ${MAX_REVIEW_REQUEST_IDENTITIES}-entry capacity; refusing a new identity`,
      );
    }
    if (this.activeReviewRequestPreparations >= MAX_CONCURRENT_REVIEW_REQUEST_PREPARATIONS) {
      throw new Error(
        `request_review simultaneous preparation capacity is ${MAX_CONCURRENT_REVIEW_REQUEST_PREPARATIONS}; refusing a new identity`,
      );
    }
    if (this.releasedReviewRequests.size >= MAX_RETAINED_DISPATCHES) {
      throw new Error(
        `request_review interrupted handoff capacity is ${MAX_RETAINED_DISPATCHES}; retry an existing identity first`,
      );
    }

    this.reviewRequestIdentityHistory.set(identityHash, digest);
    this.activeReviewRequestPreparations += 1;
    const pending = this.prepareCheckpointReview(request, targetIter, digest);
    const entry: { digest: string; pending?: Promise<PreparedReviewRequest>; settled: boolean } = {
      digest,
      pending,
      settled: false,
    };
    this.reviewRequests.set(identityHash, entry);
    try {
      const result = await pending;
      entry.settled = true;
      delete entry.pending;
      this.trimReviewRequests();
      return result;
    } catch (error) {
      if (this.reviewRequests.get(identityHash) === entry) {
        this.reviewRequests.delete(identityHash);
      }
      let retainIdentity = false;
      let requiresReconciliation = false;
      if (isCommittedSecureLedgerError(error) && error.operation === 'secure_ledger_append') {
        try {
          retainIdentity = this.findDurableReviewRequest(decisionPayload, true) === 'matching';
        } catch {
          // A committed decision append with an unreadable ledger remains
          // ambiguous. Fail closed until a later in-process retry can reconcile it.
          retainIdentity = true;
          requiresReconciliation = true;
        }
      }
      if (retainIdentity) {
        this.releasedReviewRequests.set(identityHash, {
          digest,
          prepared: this.preparedCheckpointReview(request, targetIter),
          claimed: false,
          ...(requiresReconciliation ? { reconcileDecision: decisionPayload } : {}),
        });
      }
      if (!retainIdentity && this.reviewRequestIdentityHistory.get(identityHash) === digest) {
        this.reviewRequestIdentityHistory.delete(identityHash);
      }
      throw error;
    } finally {
      this.activeReviewRequestPreparations -= 1;
    }
  }

  /**
   * Reset a single subagent — stop its session, clear the started flag, and
   * (optionally) eagerly start a fresh one. The session-level system prompt is
   * the same; persistent state lives in `<ledger>/{coder,reviewer}_memory.md`
   * which the agent reads on its first turn after reset.
   *
   * Refuses to reset Planner without `force: true` — Planner reset throws away
   * the user-conversation context and must be a deliberate action.
   */
  async resetAgent(
    agent: 'planner' | 'coder' | 'reviewer',
    opts: { force?: boolean; eagerRestart?: boolean } = {},
  ): Promise<AutoloopResetResult> {
    if (agent === 'planner' && !opts.force) {
      return {
        ok: false,
        code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
        agent,
        message: 'Refusing to reset Planner without force=true (would discard chat context)',
        retryable: false,
      };
    }
    const name = agent === 'planner' ? this.plannerName : agent === 'coder' ? this.coderName : this.reviewerName;
    const previous = this.currentGeneration(agent);
    const priorStarted = this.roleStarted(agent);
    const priorReviewerPrompt = this.reviewerSessionPrompt;
    let previousGenerationReleased = previous?.state === 'released';
    if (previousGenerationReleased) {
      this.setRoleStarted(agent, false);
      if (agent === 'reviewer') this.reviewerSessionPrompt = null;
    }
    this.appendDecisionLog({
      kind: 'reset_agent',
      actor: 'dispatcher',
      payload: { agent, force: !!opts.force, eagerRestart: !!opts.eagerRestart },
    });
    let stopError: unknown;
    try {
      await this.config.manager.stopSession(name);
    } catch (err) {
      stopError = err;
      this.logger.warn?.(`[autoloop] resetAgent stop failed for ${name}: ${(err as Error).message}`);
    }

    const failure = (detail: string, cause?: unknown): AutoloopResetResult => {
      if (!previousGenerationReleased) {
        this.setRoleStarted(agent, priorStarted);
        if (agent === 'reviewer') this.reviewerSessionPrompt = priorReviewerPrompt;
      }
      return {
        ok: false,
        code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
        agent,
        previous_generation: previous?.generation,
        message: cause instanceof Error ? `${detail}: ${cause.message}` : detail,
        retryable: false,
      };
    };

    try {
      const stoppedLiveness = await this.runtimeProbe.inspect(name, previous?.session_id);
      if (stoppedLiveness !== 'absent') {
        return failure(
          `Autoloop session '${name}' remained ${stoppedLiveness} after reset stop${
            stopError instanceof Error ? ` (${stopError.message})` : ''
          }`,
        );
      }

      if (previous) {
        // The release event can be durable while SessionManager's matching
        // registry tombstone is still pending. Retrying the exact generation is
        // idempotent and lets releaseReservation finish that existing fence;
        // skipping it would leave repeated reset calls permanently blocked.
        await this.releaseGeneration(previous, false, () => {
          previousGenerationReleased = true;
          this.setRoleStarted(agent, false);
          if (agent === 'reviewer') this.reviewerSessionPrompt = null;
        });
      }

      const managerProbe = this.config.manager as SessionManager & {
        probeAgentNameReusable?: (sessionName: string, released?: PhysicalAgentGeneration) => boolean;
        isAgentGenerationReleased?: (generation: PhysicalAgentGeneration) => boolean;
      };
      if (managerProbe.probeAgentNameReusable) {
        if (!managerProbe.probeAgentNameReusable(name, previous)) {
          return failure(
            previous
              ? `Autoloop session '${name}' retained generation ${previous.generation} after release`
              : `Autoloop session '${name}' was not reusable after reset`,
          );
        }
      } else if (previous && managerProbe.isAgentGenerationReleased) {
        if (!managerProbe.isAgentGenerationReleased(previous)) {
          return failure(`Autoloop session '${name}' retained generation ${previous.generation} after release`);
        }
      } else {
        // Test doubles and legacy SessionManager implementations use the same
        // fenced reserve/rollback path as startup to prove the name is reusable.
        const probeGeneration = this.newGeneration(agent);
        if (!this.config.manager.reserveAgentGeneration(probeGeneration, this.config.workspace)) {
          return failure(`Autoloop session '${name}' could not reserve a replacement generation`);
        }
        const rolledBack = await this.runtimeProbe.releaseReservation(name, probeGeneration.generation, {
          rollbackUncommittedReservation: true,
          expectedOwnerInstanceId: probeGeneration.owner_instance_id,
          expectedSessionId: probeGeneration.session_id,
        });
        if (!rolledBack) {
          return failure(`Autoloop session '${name}' could not roll back its replacement probe`);
        }
      }

      // With no durable prior generation, authoritative name reusability is
      // the first point at which the legacy in-memory flag can be cleared.
      this.setRoleStarted(agent, false);
      if (agent === 'reviewer') this.reviewerSessionPrompt = null;

      let activeGeneration: number | undefined;
      if (opts.eagerRestart) {
        if (agent === 'planner') await this.ensurePlanner();
        else if (agent === 'coder') await this.ensureCoder();
        else await this.ensureReviewer();
        const active = this.currentGeneration(agent);
        const activeLiveness = active
          ? await this.runtimeProbe.inspect(active.session_name, active.session_id)
          : 'absent';
        if (!active || active.state !== 'live' || activeLiveness !== 'live') {
          return failure(`Autoloop session '${name}' did not create a live replacement generation`);
        }
        activeGeneration = active.generation;
      }

      return {
        ok: true,
        agent,
        previous_generation: previous?.generation,
        active_generation: activeGeneration,
        reusable: true,
      };
    } catch (error) {
      return failure(`Autoloop session '${name}' reset postcondition failed`, error);
    }
  }

  private pendingSendTimeout(env: AnyAutoloopMessage, agent: AutoloopRoleName, dispatchId: string): PendingSendTimeout {
    return {
      status: 'awaiting_resume',
      dispatch_id: dispatchId,
      agent,
      message_id: env.msg_id,
      message_type: env.type,
      iter: env.iter,
      timeout_ms: this.config.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS,
    };
  }

  private recoverableSendTimeout(pending: PendingSendTimeout, error: string): SendMessageResult {
    const timeout: SendTimeoutPayload = { ...pending, error };
    this.appendDecisionLog({
      kind: 'send_timeout',
      actor: 'dispatcher',
      payload: { ...timeout },
    });
    return { output: '', error, recoverable_timeout: timeout };
  }

  /** One physical send attempt with strict timeout classification. */
  private async sendAttempt(name: string, promptText: string, pending: PendingSendTimeout): Promise<SendMessageResult> {
    try {
      const result = (await this.config.manager.sendMessage(name, promptText, {
        timeout: this.config.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS,
        parentRunId: this.config.runId,
      })) as SendMessageResult;
      const timeoutMessage = result.error ? genuineSendTimeoutMessage(result.error) : null;
      return timeoutMessage ? this.recoverableSendTimeout(pending, timeoutMessage) : result;
    } catch (err) {
      const timeoutMessage = genuineSendTimeoutMessage(err);
      if (timeoutMessage) return this.recoverableSendTimeout(pending, timeoutMessage);
      throw err;
    }
  }

  /**
   * Wrap a subagent send. Genuine timeouts are never retried because the first
   * turn may still finish with side effects. Other throws/error results retain
   * the established reset-once/retry-once subprocess recovery path.
   */
  private async sendWithRecovery(
    agent: 'coder' | 'reviewer',
    name: string,
    promptText: string,
    pending: PendingSendTimeout,
  ): Promise<SendMessageResult> {
    try {
      const result = await this.sendAttempt(name, promptText, pending);
      if (result.recoverable_timeout || !result.error) return result;
      throw new Error(result.error);
    } catch (err) {
      this.logger.warn?.(`[autoloop] ${agent} send threw, attempting reset+retry: ${(err as Error).message}`);
      const reset = await this.resetAgent(agent, { eagerRestart: true });
      if (!reset.ok) return { output: '', error: reset.message, fatal: true, code: reset.code };
      // Let the freshly-restarted subprocess settle before retrying — an
      // immediate retry routinely hits the same transient failure (e.g. the
      // old socket still in TIME_WAIT → ECONNREFUSED). Small jitter avoids
      // lockstep retries across concurrent runs.
      await new Promise((r) => setTimeout(r, 500 + Math.floor(Math.random() * 250)));
      try {
        const result = await this.sendAttempt(name, promptText, pending);
        if (result.recoverable_timeout || !result.error) return result;
        throw new Error(result.error);
      } catch (err2) {
        this.logger.error?.(`[autoloop] ${agent} second attempt failed after reset: ${(err2 as Error).message}`);
        return {
          output: '',
          error: (err2 as Error).message,
          fatal: true,
          code: 'AUTOLOOP_ENGINE_FAILURE',
        };
      }
    }
  }

  /**
   * Append a structured audit row to `<ledger>/decisions.jsonl`. Best-effort:
   * any I/O failure is logged but never thrown. Captures terminate, reset,
   * push-policy mutations, compact triggers, subagent spawns, phase-error
   * passes, and policy-silence attempts that we rejected.
   */
  private appendDecisionLog(entry: Omit<DecisionLogEntry, 'ts'>): Error | undefined {
    try {
      const line = JSON.stringify({ ts: new Date().toISOString(), ...entry }) + '\n';
      this.secureLedger.appendFlatFile('decisions.jsonl', line);
      return undefined;
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.logger.warn?.(`[autoloop] decisions.jsonl append failed: ${error.message}`);
      return error;
    }
  }

  private syncCreatedControlFileDirectory(filePath: string): void {
    if (process.platform === 'win32') {
      // Node does not expose a supported directory handle that can be passed
      // to FlushFileBuffers on Windows. Keep the platform limitation explicit:
      // the file contents are flushed, but POSIX directory-entry durability is
      // not claimed here.
      this.logger.warn?.(
        '[autoloop] parent-directory fsync is unavailable on win32; control file contents were flushed without a POSIX directory-entry guarantee',
      );
      return;
    }
    const directoryFd = fs.openSync(path.dirname(filePath), 'r');
    try {
      fs.fsyncSync(directoryFd);
    } finally {
      fs.closeSync(directoryFd);
    }
  }

  /**
   * Replace a Planner-owned control artifact without ever following an
   * attacker-controlled destination. The temp file lives beside the target,
   * is flushed before rename, and the directory entry is flushed on POSIX.
   */
  private writeControlFileAtomically(target: string, content: string): void {
    let existing: fs.Stats | undefined;
    try {
      existing = fs.lstatSync(target);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
    if (existing && !existing.isFile()) {
      throw new Error(`Refusing to replace non-regular Planner control target '${target}'`);
    }

    const tempPath = path.join(path.dirname(target), `.${path.basename(target)}.${randomUUID()}.tmp`);
    let renamed = false;
    let fd: number | undefined;
    try {
      fd = fs.openSync(tempPath, 'wx', 0o600);
      fs.writeFileSync(fd, content, { encoding: 'utf8' });
      fs.fsyncSync(fd);
      fs.closeSync(fd);
      fd = undefined;
      fs.renameSync(tempPath, target);
      renamed = true;
      this.syncCreatedControlFileDirectory(target);
    } finally {
      if (fd !== undefined) {
        try {
          fs.closeSync(fd);
        } catch {
          // Preserve the primary write/flush failure.
        }
      }
      if (!renamed) {
        try {
          if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
        } catch {
          // Preserve the primary materialization failure.
        }
      }
    }
  }

  private persistPlannerControls(
    env: AnyAutoloopMessage,
    dispatchId: string,
    generation: PhysicalAgentGeneration,
    controls: readonly PlannerToolCall[],
    controlsSha256: string,
  ): PlannerControlEvidence {
    const canonicalControls = canonicalizePlannerControls(controls);
    const evidence: PlannerControlEvidence = {
      control_id: `planner_control_${randomUUID()}`,
      persisted_at: this.now().toISOString(),
      dispatch_id: dispatchId,
      message_id: env.msg_id,
      iter: env.iter,
      generation: generation.generation,
      owner_instance_id: generation.owner_instance_id,
      session_id: generation.session_id,
      tools: canonicalizeExactStringArrayElements(canonicalControls.map(({ tool }) => tool)) as PlannerToolName[],
      controls: canonicalControls,
      controls_sha256: controlsSha256,
    };
    const decision = {
      ts: evidence.persisted_at,
      kind: 'planner_turn_control',
      actor: 'planner',
      payload: { ...evidence },
    } satisfies DecisionLogEntry;
    let committedClaim: ReturnType<typeof findCommittedPlannerControl>;
    try {
      committedClaim = findCommittedPlannerControl(this.secureLedger, evidence);
    } catch (error) {
      throw new PlannerControlLedgerInvalidError(
        `Planner control ledger could not be validated: ${(error as Error).message}`,
        { cause: error },
      );
    }
    if (committedClaim !== 'none') {
      // Flush the already-committed ledger before treating its claim as an
      // authoritative recovery boundary. Never append a second claim or run
      // either the matching or conflicting effect.
      this.secureLedger.flushFlatFile('decisions.jsonl');
      throw new CommittedPlannerControlReplayError(
        committedClaim === 'matching'
          ? undefined
          : 'Planner control event conflicts with an already committed claim for this logical dispatch; refusing a second effect',
      );
    }
    const prepared = this.secureLedger.prepareFlatFileAppend('decisions.jsonl', `${JSON.stringify(decision)}\n`);
    try {
      // The control intent is a commit boundary, not ordinary best-effort
      // audit data. Commit through the checked capability and verify the same
      // opened inode's durable tail before any prepared effect can begin.
      try {
        prepared.commitDurable();
      } catch (error) {
        if (!isCommittedSecureLedgerError(error) || !prepared.committed) throw error;
        const committedEvidence = plannerControlEvidenceFromTail(prepared.readLastNonEmptyLine());
        if (!plannerControlEvidenceMatches(committedEvidence, evidence)) throw error;
        // Retry only the incomplete barrier; SecureAutoloopLedger remembers
        // that the control bytes are already committed and cannot append them
        // again. A persistent incomplete result keeps its original typed code.
        prepared.commitDurable();
      }
      const durableEvidence = plannerControlEvidenceFromTail(prepared.readLastNonEmptyLine());
      if (!plannerControlEvidenceMatches(durableEvidence, evidence)) {
        throw new Error('the appended control event did not match the durable tail');
      }
      return durableEvidence;
    } catch (error) {
      if (isCommittedSecureLedgerError(error)) throw error;
      if (error instanceof AutoloopOperationError) throw error;
      throw new AutoloopOperationError(
        'AUTOLOOP_CONTROL_NOT_PERSISTED',
        `Planner control event could not be persisted: ${(error as Error).message}`,
        { cause: error },
      );
    } finally {
      prepared.close();
    }
  }

  /**
   * Append a Planner-pane chat entry to <ledger>/chat.jsonl. Used by the
   * dashboard's GET /autoloop/<id>/chat_history endpoint so a browser
   * refresh / cross-process / re-opening a terminated run can replay the
   * conversation instead of starting visibly blank.
   */
  private appendChatEntry(entry: {
    who: 'user' | 'planner' | 'coder' | 'reviewer' | 'system';
    text: string;
    ts: string;
  }): void {
    try {
      const canonical = Object.create(null) as Record<string, unknown>;
      Object.defineProperty(canonical, 'who', { enumerable: true, value: entry.who });
      Object.defineProperty(canonical, 'text', { enumerable: true, value: entry.text });
      Object.defineProperty(canonical, 'ts', { enumerable: true, value: entry.ts });
      Object.freeze(canonical);
      this.secureLedger.appendFlatFile('chat.jsonl', JSON.stringify(canonical) + '\n');
    } catch (err) {
      this.logger.warn?.(`[autoloop] chat.jsonl append failed: ${(err as Error).message}`);
    }
  }

  // ─── Auto-compact ────────────────────────────────────────────────────────
  //
  // After each agent turn we check getStats().contextPercent. When it crosses
  // the per-agent threshold we send `/compact <hint>` to ask Claude Code to
  // drop chunks of history while preserving what each role needs to keep
  // working. /compact preserves the session id — no reset, no memory-file
  // dance, no reprime — so this is cheap.
  //
  // We track lastCompactAt per agent to avoid re-firing within 30 s in case
  // the immediate post-compact stats haven't refreshed yet.

  private lastCompactAt: Partial<Record<'planner' | 'coder' | 'reviewer', number>> = {};

  private compactSummaryFor(agent: 'planner' | 'coder' | 'reviewer'): string {
    if (agent === 'planner') {
      return [
        'Preserve: current plan.md state and goal.json criteria; what the user has asked',
        "for and approved; what directions have been tried and rejected; the user's style",
        'preferences for this run; iter-by-iter Reviewer verdicts. Drop: verbose tool',
        'output, intermediate file dumps, redundant context.',
      ].join(' ');
    }
    if (agent === 'coder') {
      return [
        'Preserve: codebase familiarity (what files do what), what patches you have already',
        'tried and why they failed, what is currently working, the current plan and goal.',
        'Drop: full file dumps, verbose stack traces, intermediate eval output beyond the',
        'last few iters.',
      ].join(' ');
    }
    return [
      'Preserve: patterns of fakery you have caught (in reviewer_memory.md), recent metric',
      'history, structural rules from goal.json, your accumulating model of what cheating',
      'looks like in this codebase. Drop: full diff dumps from older iters, verbose audit',
      'transcripts beyond the last few iters.',
    ].join(' ');
  }

  private async maybeCompact(agent: 'planner' | 'coder' | 'reviewer', name: string): Promise<void> {
    const cfg = this.config.compactThresholds ?? {};
    const threshold =
      agent === 'planner' ? (cfg.planner ?? 80) : agent === 'coder' ? (cfg.coder ?? 70) : (cfg.reviewer ?? 70);
    let pct: number | undefined;
    try {
      const stats = this.config.manager.getStatus(name).stats;
      pct = stats.contextPercent;
    } catch {
      // Session might be gone (terminate races); silent skip.
      return;
    }
    if (pct == null || pct < threshold) return;
    const last = this.lastCompactAt[agent] ?? 0;
    if (Date.now() - last < 30_000) return;
    this.lastCompactAt[agent] = Date.now();
    this.logger.info?.(
      `[autoloop/${this.config.runId}] ${agent} context ${pct.toFixed(0)}% ≥ ${threshold}% — auto-compact`,
    );
    this.emit('compact', { agent, percent: pct, threshold });
    this.appendDecisionLog({
      kind: 'compact',
      actor: 'dispatcher',
      payload: { agent, percent: pct, threshold },
    });
    try {
      await this.config.manager.compactSession(name, this.compactSummaryFor(agent));
    } catch (err) {
      this.logger.warn?.(`[autoloop/${this.config.runId}] compact ${agent} failed: ${(err as Error).message}`);
    }
  }

  // ─── Planner-specific ────────────────────────────────────────────────────

  private async ensurePlanner(): Promise<void> {
    if (this.plannerStarted) return;
    this.validateSelection('planner', this.plannerSelection);
    await this.ensureAgentSession('planner', async (generation) => {
      await this.config.manager.startSession(
        {
          name: this.plannerName,
          cwd: this.config.workspace,
          engine: this.plannerSelection.engine,
          model: this.roleModel('planner', this.plannerSelection),
          customEngine: this.plannerSelection.engine === 'custom' ? this.plannerSelection.customEngine : undefined,
          permissionMode: this.plannerSelection.engine === 'claude' ? 'plan' : 'manual',
          sandboxMode: 'read-only',
          systemPrompt: this.plannerSystemPrompt,
          // Hard role boundary: Planner must NEVER author content files itself.
          // Its only writes are plan.md / goal.json via the write_plan /
          // write_goal autoloop tools. Disallowing the editing tools here is
          // the load-bearing enforcement — prompt rules alone proved
          // insufficient (the model would happily produce user-requested
          // deliverables directly). Read/Glob/Grep/Bash stay enabled so
          // Planner can still discover, audit, and `git status` the workspace.
          disallowedTools: ['Write', 'Edit', 'MultiEdit', 'NotebookEdit'],
        },
        generation,
      );
    });
  }

  private plannerTurnCounters(): { turns: number; turnsSucceeded: number } | undefined {
    try {
      const stats = this.config.manager.getStatus(this.plannerName).stats;
      if (!Number.isFinite(stats.turns) || !Number.isFinite(stats.turnsSucceeded)) return undefined;
      return { turns: stats.turns, turnsSucceeded: stats.turnsSucceeded };
    } catch {
      return undefined;
    }
  }

  private async deliverToPlanner(env: AnyAutoloopMessage, dispatchId: string): Promise<AnyAutoloopMessage[]> {
    if (this.terminal) return [];
    if (env.type !== 'chat' && env.type !== 'directive_ack' && env.type !== 'iter_done') {
      // Other types (push_user / pause / resume / terminate) are runner-only
      // or planner-emitted; they should never arrive *to* planner.
      throw new Error(`[autoloop] planner does not accept message type=${env.type}`);
    }

    await this.ensurePlanner();
    if (this.terminal) return [];

    // Compose the prompt fed into the Planner session. For S2 we only handle
    // user chat; iter_done / directive_ack are wired in S4.
    let promptText: string;
    if (env.type === 'chat') {
      promptText = env.payload.text;
      // Persist user message so the dashboard can replay history after
      // refresh / cross-process / terminated-run reopen.
      this.appendChatEntry({ who: 'user', text: env.payload.text, ts: env.ts });
    } else if (env.type === 'directive_ack') {
      promptText = `[system] coder directive_ack iter=${env.iter}: ${JSON.stringify(env.payload)}`;
    } else {
      // iter_done
      promptText = `[system] iter ${env.iter} done. verdict=${env.payload.verdict} metric=${env.payload.metric}`;
    }

    const expectedGeneration = this.currentGeneration('planner');
    const countersBefore = this.plannerTurnCounters();
    const pendingTimeout = this.pendingSendTimeout(env, 'planner', dispatchId);
    let result: SendMessageResult;
    try {
      result = await this.sendAttempt(
        this.plannerName,
        this.withRoleInstructions('planner', this.plannerSelection, this.plannerSystemPrompt, promptText),
        pendingTimeout,
      );
    } catch (error) {
      throw new AutoloopOperationError(
        'AUTOLOOP_ENGINE_FAILURE',
        `Planner engine transport failed: ${(error as Error).message}`,
        { cause: error },
      );
    }

    if (result.recoverable_timeout) {
      if (this.terminal) return [];
      return [Msg.sendTimeout(env.iter, result.recoverable_timeout)];
    }

    if (this.terminal) return [];

    if (result.error) {
      this.logger.error?.(`[autoloop] planner send error: ${result.error}`);
      this.emit('planner_error', new Error(result.error));
      throw new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', `Planner engine turn failed: ${result.error}`);
    }

    const replyText = (result.output ?? '').trim();
    const observedGeneration = this.currentGeneration('planner');
    const generationLiveness = observedGeneration
      ? await this.runtimeProbe.inspect(observedGeneration.session_name, observedGeneration.session_id)
      : 'absent';
    if (this.terminal) return [];
    const countersAfter = this.plannerTurnCounters();
    // AGY reports required-tool denial only through the authoritative success
    // counter while still returning non-empty text. Missing/non-finite AGY
    // snapshots therefore cannot prove success. Other engines expose failure
    // in SendResult.error/is_error and retain that result/transport taxonomy.
    const counterEvidenceUnavailable = !countersBefore || !countersAfter;
    const requiredToolDenied =
      replyText.length > 0 &&
      (counterEvidenceUnavailable
        ? this.plannerSelection.engine === 'agy'
        : countersAfter.turns <= countersBefore.turns || countersAfter.turnsSucceeded <= countersBefore.turnsSucceeded);
    assertPlannerTurnSucceeded(
      { reply: replyText, generation: observedGeneration, generationLiveness, requiredToolDenied },
      { requireLogicalResult: false, expectedGeneration },
    );

    // S3: parse autoloop-fenced tool calls out of the reply, apply effects,
    // and bubble emitted messages back into the runner queue.
    const parsed = parsePlannerReply(replyText);
    if (parsed.parse_errors.length > 0) {
      this.logger.warn?.(`[autoloop] planner emitted ${parsed.parse_errors.length} malformed autoloop block(s)`);
      throw new AutoloopOperationError(
        'AUTOLOOP_CONTROL_MALFORMED',
        `Planner emitted malformed control: ${parsed.parse_errors
          .map(({ block_index, error }) => `block ${block_index}: ${error}`)
          .join('; ')}`,
      );
    }
    const validation = validatePlannerToolCalls(parsed.calls);
    if (validation.errors.length > 0) {
      throw new AutoloopOperationError(
        'AUTOLOOP_CONTROL_MALFORMED',
        `Planner emitted invalid control: ${validation.errors
          .map(({ tool, error }) => `${tool}: ${error}`)
          .join('; ')}`,
      );
    }
    if (validation.blocked_policy_silence.length > 0) {
      for (const key of validation.blocked_policy_silence) {
        this.logger.warn?.(`[autoloop] refused to set silent=true on critical policy key ${key}`);
      }
      this.appendDecisionLog({
        kind: 'policy_silence_blocked',
        actor: 'planner',
        payload: { keys: validation.blocked_policy_silence },
      });
    }
    // This allowlisted batch is the sole source for canonicalization, digest,
    // persistence, comparison, and application. Raw Planner arguments never
    // cross the durable control boundary.
    const normalizedControls = validation.calls;
    if (parsed.calls.length > 0 && normalizedControls.length === 0 && validation.blocked_policy_silence.length > 0) {
      throw new AutoloopOperationError(
        'AUTOLOOP_CONTROL_MALFORMED',
        'Planner emitted only a prohibited critical policy-silence control',
      );
    }
    const effects: PlannerToolEffects = {
      assertActive: () => {
        if (this.terminal) throw new Error('Autoloop run became terminal during Planner control application');
      },
      spawnCoder: async (args) => await this.spawnCoder(args),
      spawnReviewer: async (args) => await this.spawnReviewer(args),
      spawnSubagents: async (args) => {
        if (this.terminal) return;
        if (this.config.onSpawnSubagents) {
          this.spawnCommitDeferralDepth += 1;
          try {
            await this.config.onSpawnSubagents(args);
          } finally {
            this.spawnCommitDeferralDepth -= 1;
          }
          if (this.terminal) return;
          await this.config.onSpawnSubagentsCommitted?.();
        } else {
          this.logger.warn?.('[autoloop] spawn_subagents called but no handler is installed');
        }
      },
      requestReview: async (args, targetIter) => await this.requestReview(args, targetIter),
      releaseReviewRequest: (idempotencyKey, payload) => this.releaseReviewRequest(idempotencyKey, payload),
      updatePushPolicy: (delta) => {
        if (this.terminal) return;
        if (!this.config.pushPolicyRef) return;
        const applied: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(delta)) {
          const current = (this.config.pushPolicyRef as unknown as Record<string, Record<string, unknown>>)[k];
          const baseline = (DEFAULT_PUSH_POLICY as unknown as Record<string, Record<string, unknown>>)[k];
          const critical = k === 'on_phase_error' || k === 'on_decision_needed';
          const patch = v as Record<string, unknown>;
          const rule = critical
            ? { ...baseline, ...current, ...patch }
            : Object.keys(patch).length === 0
              ? {}
              : { ...current, ...patch };
          if (critical) {
            delete rule.silent;
            if (k === 'on_phase_error') rule.level = 'error';
            if (k === 'on_decision_needed' && rule.level !== 'error') rule.level = 'decision';
            if (rule.channel !== 'auto' && rule.channel !== 'both') rule.channel = baseline.channel;
          }
          (this.config.pushPolicyRef as unknown as Record<string, unknown>)[k] = rule;
          applied[k] = rule;
        }
        if (Object.keys(applied).length > 0) {
          this.appendDecisionLog({
            kind: 'update_push_policy',
            actor: 'planner',
            payload: { applied },
          });
        }
      },
      writePlanFile: async (file, content, commitMessage) => {
        // Author plan.md / goal.json on the Planner's behalf. The Planner
        // can't Write/Edit directly (disallowedTools), so this autoloop tool
        // is the single legitimate authoring path. Best-effort git commit
        // keeps the ledger honest.
        const target = path.join(this.config.workspace, file);
        this.writeControlFileAtomically(target, content);
        await this.gitCommit(file, commitMessage ?? `autoloop: planner writes ${file}`);
      },
    };
    const controlTools = normalizedControls.map(({ tool }) => tool);
    let persistedControl: PlannerControlEvidence | undefined;
    let expectedControl: PlannerTurnExpectation['expectedControl'];
    if (controlTools.length > 0) {
      if (this.terminal) return [];
      const controlGeneration = observedGeneration ?? expectedGeneration;
      if (!controlGeneration) {
        throw new AutoloopOperationError(
          'AUTOLOOP_SESSION_NOT_CREATED',
          'Planner control could not be bound to a physical generation',
        );
      }
      const controlsSha256 = createHash('sha256')
        .update(validation.controls_json ?? '[]')
        .digest('hex');
      expectedControl = {
        dispatch_id: dispatchId,
        message_id: env.msg_id,
        iter: env.iter,
        generation: controlGeneration.generation,
        owner_instance_id: controlGeneration.owner_instance_id,
        session_id: controlGeneration.session_id,
        tools: controlTools,
        controls: normalizedControls,
        controls_sha256: controlsSha256,
      };
      persistedControl = this.persistPlannerControls(
        env,
        dispatchId,
        controlGeneration,
        normalizedControls,
        controlsSha256,
      );
    }
    assertPlannerTurnSucceeded(
      {
        reply: parsed.cleaned_reply,
        generation: observedGeneration,
        generationLiveness,
        persistedControl,
      },
      { expectedGeneration, expectedControl },
    );
    if (this.terminal) return [];
    // Persist and verify the complete Planner control claim before invoking
    // any control handler. A ledger failure must leave every control effect at
    // zero, even when the reply itself was a successful engine turn.
    // After iter_done(N) the run has advanced to iter N+1 in runner state;
    // any directive Planner emits in response targets the new iter.
    const nextIter = env.type === 'iter_done' ? env.iter + 1 : env.iter;
    const handlerResult = await applyValidatedPlannerToolCalls(validation, effects, nextIter);
    for (const errEntry of handlerResult.errors) {
      this.logger.warn?.(`[autoloop] tool '${errEntry.tool}' failed: ${errEntry.error}`);
    }
    if (handlerResult.errors.length > 0) {
      throw new AutoloopOperationError(
        'AUTOLOOP_CONTROL_APPLICATION_FAILED',
        `Planner control application failed: ${handlerResult.errors
          .map(({ tool, error }) => `${tool}: ${error}`)
          .join('; ')}`,
      );
    }
    const reviewPayloads = handlerResult.emitted_messages
      .filter((message) => message.type === 'review_request' && 'idempotency_key' in message.payload)
      .map((message) => message.payload as CheckpointReviewRequestPayload);
    let handoffAccepted = reviewPayloads.length === 0;
    try {
      if (this.terminal) return [];
      // Replay history is accepted-turn state. Commit both sides together only
      // after parse, validation, durable evidence, and all control application
      // have passed; rejected Planner output must not be replayed on retry.
      this.recordTurn('planner', 'user', promptText);
      this.recordTurn('planner', 'agent', replyText);
      // Emit cleaned reply (without raw JSON blocks) for the chat tool to surface.
      const surfacedReply =
        parsed.cleaned_reply ||
        (persistedControl ? `Planner controls persisted: ${persistedControl.tools.join(', ')}` : '');
      if (surfacedReply) {
        this.emit('planner_reply', surfacedReply, {
          message_id: env.msg_id,
          dispatch_id: dispatchId,
          iter: env.iter,
        });
        this.appendChatEntry({ who: 'planner', text: surfacedReply, ts: new Date().toISOString() });
      }
      // Auto-compact after each Planner turn if context is filling up.
      await this.maybeCompact('planner', this.plannerName);
      if (this.terminal) return [];
      for (const payload of reviewPayloads) this.acceptReviewRequest(payload.idempotency_key);
      handoffAccepted = true;
      return handlerResult.emitted_messages;
    } finally {
      if (!handoffAccepted) {
        for (const payload of reviewPayloads) this.releaseReviewRequest(payload.idempotency_key, payload);
      }
    }
  }

  // ─── Coder ──────────────────────────────────────────────────────────────

  private async ensureCoder(): Promise<void> {
    if (this.coderStarted && this.currentGeneration('coder')?.state === 'live') return;
    this.validateSelection('coder', this.coderSelection);
    await this.ensureAgentSession('coder', async (generation) => {
      await this.config.manager.startSession(
        {
          name: this.coderName,
          cwd: this.config.workspace,
          engine: this.coderSelection.engine,
          model: this.roleModel('coder', this.coderSelection),
          customEngine: this.coderSelection.engine === 'custom' ? this.coderSelection.customEngine : undefined,
          permissionMode: 'bypassPermissions',
          systemPrompt: this.coderSystemPrompt,
        },
        generation,
      );
    });
  }

  private async deliverToCoder(env: AnyAutoloopMessage, dispatchId: string): Promise<AnyAutoloopMessage[]> {
    if (this.terminal) return [];
    if (env.type !== 'directive') {
      throw new Error(`[autoloop] coder does not accept message type=${env.type}`);
    }

    // Persist the complete immutable intent before reserving or starting a
    // physical Coder, writing its working heartbeat, or sending a prompt.
    // Preserve the exact schema-v1 byte shape: restart replay compares this
    // write-once artifact byte-for-byte, so even additive fields require a
    // versioned migration rather than an in-place serialization change.
    this.secureLedger.writeIterationArtifact(env.iter, 'directive.json', serializeDirectiveV1(env, dispatchId));
    if (this.terminal) return [];
    await this.ensureCoder();
    if (this.terminal) return [];

    const promptText = buildCoderDirectivePrompt(env);

    // Heartbeat so the dashboard's Coder pane shows "iter N started" even
    // before Coder produces a reply — useful for liveness checks on long
    // turns, and survives refresh because it's in chat.jsonl.
    this.appendChatEntry({
      who: 'coder',
      text: `🔨 Coder iter ${env.iter} working…`,
      ts: new Date().toISOString(),
    });

    const result = await this.sendWithRecovery(
      'coder',
      this.coderName,
      this.withRoleInstructions('coder', this.coderSelection, this.coderSystemPrompt, promptText),
      this.pendingSendTimeout(env, 'coder', dispatchId),
    );
    if (result.recoverable_timeout) {
      if (this.terminal) return [];
      return [Msg.sendTimeout(env.iter, result.recoverable_timeout)];
    }
    if (this.terminal) return [];
    // A3: subprocess died (recovery retry exhausted). Surface as phase_error
    // rather than silently masquerading as a "clarification request"; the
    // runner's circuit breaker can then trip after enough consecutive failures.
    if (result.fatal) {
      this.appendDecisionLog({
        kind: 'phase_error',
        actor: 'dispatcher',
        payload: { agent: 'coder', phase: 'send', code: result.code, error: result.error ?? 'unknown' },
      });
      return [
        Msg.phaseError(env.iter, {
          agent: 'coder',
          phase: 'send',
          code: result.code,
          error: result.error ?? 'unknown send failure',
        }),
      ];
    }
    this.recordTurn('coder', 'user', promptText);
    this.recordTurn('coder', 'agent', (result.output ?? '').trim());
    const replyText = (result.output ?? '').trim();
    const parsed = parseAgentReply(replyText);
    this.emit('coder_reply', parsed.cleaned_reply);
    if (parsed.cleaned_reply) {
      this.appendChatEntry({
        who: 'coder',
        text: parsed.cleaned_reply,
        ts: new Date().toISOString(),
      });
    }

    const ic = extractIterComplete(parsed.calls);
    if (!ic) {
      // No iter_complete emitted — could be a clarification request. Return a
      // directive_ack so Planner sees it next turn.
      await this.maybeCompact('coder', this.coderName);
      return [
        Msg.directiveAck(env.iter, {
          understood: false,
          clarification: parsed.cleaned_reply.slice(0, 500),
        }),
      ];
    }

    // Persist eval output to ledger.
    this.secureLedger.writeIterationArtifact(
      env.iter,
      'eval_output.json',
      JSON.stringify({ schema_version: LEDGER_SCHEMA_VERSION, iter: env.iter, eval_output: ic.eval_output }, null, 2),
    );
    this.secureLedger.writeIterationArtifact(
      env.iter,
      'coder_summary.txt',
      `${ic.summary}\n\n--- coder cleaned reply ---\n${parsed.cleaned_reply}\n`,
    );

    // Compute diff + files_changed via git so we don't trust Coder's claim.
    //
    // Two things this used to get wrong, both of which made the Reviewer audit a
    // picture that could not show what actually happened:
    //
    //  1. A bare `git diff` lists tracked modifications only. Files the Coder
    //     *created* appeared in neither the patch nor the `--name-only`
    //     fallback, while the `git add -A` a few lines below committed them
    //     anyway. `capturePatch` covers tracked changes ∪ untracked files.
    //  2. `ic.files_changed` — the Coder's own claim — won whenever it was
    //     supplied, so the git fallback only ran when the Coder said nothing.
    //     The comment above said we don't trust the claim; now we don't.
    const diffText = await capturePatch(this.config.workspace, 'HEAD');
    this.secureLedger.writeIterationArtifact(env.iter, 'diff.patch', diffText);
    const observed = await changedFilesSince(this.config.workspace, 'HEAD');
    const filesChanged = observed.map((f) => f.path);
    // Commit the iteration so Reviewer's git view is clean for the next iter.
    await this.runGit(['git', 'add', '-A']);
    const commitMsg = `autoloop/iter-${env.iter}: ${ic.summary}`.slice(0, 200);
    const commitRes = await this.runGit(['git', 'commit', '-m', commitMsg]);
    // A6: a non-"nothing to commit" failure (hook reject, signing missing,
    // index lock) means the next iter's diff would be wrong. Bail to runner.
    if (commitRes.code !== 0 && !/nothing to commit/i.test(commitRes.out + commitRes.err)) {
      this.appendDecisionLog({
        kind: 'phase_error',
        actor: 'dispatcher',
        payload: { agent: 'coder', phase: 'git_commit', error: commitRes.err.slice(0, 500) },
      });
      return [
        Msg.phaseError(env.iter, {
          agent: 'coder',
          phase: 'git_commit',
          error: `git commit failed (code=${commitRes.code}): ${commitRes.err.slice(0, 300)}`,
        }),
      ];
    }

    await this.maybeCompact('coder', this.coderName);
    return [
      Msg.iterArtifacts(env.iter, {
        diff: diffText,
        eval_output: ic.eval_output,
        files_changed: filesChanged,
      }),
    ];
  }

  // ─── Reviewer ───────────────────────────────────────────────────────────

  private readReviewerControlFile(name: 'plan.md' | 'goal.json'): Buffer | undefined {
    const target = path.join(this.config.workspace, name);
    let observed: fs.Stats;
    try {
      observed = fs.lstatSync(target);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
      throw error;
    }
    if (observed.isSymbolicLink() || !observed.isFile() || observed.nlink !== 1) {
      throw new Error(`Refusing unsafe Reviewer control source '${target}'`);
    }
    const fd = fs.openSync(target, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.nlink !== 1 || opened.dev !== observed.dev || opened.ino !== observed.ino) {
        throw new Error(`Reviewer control source identity changed while opening '${target}'`);
      }
      const content = fs.readFileSync(fd);
      const after = fs.lstatSync(target);
      if (
        after.isSymbolicLink() ||
        !after.isFile() ||
        after.nlink !== 1 ||
        after.dev !== opened.dev ||
        after.ino !== opened.ino ||
        after.size !== opened.size ||
        after.mtimeMs !== opened.mtimeMs
      ) {
        throw new Error(`Reviewer control source identity or contents changed while reading '${target}'`);
      }
      return content;
    } finally {
      fs.closeSync(fd);
    }
  }

  /**
   * Compose the Reviewer's system prompt with a frozen snapshot of
   * `reviewer_memory.md` appended. Read once at session start; mid-session
   * edits to the file do NOT take effect until the next Reviewer reset. This
   * keeps the per-iter prompt prefix stable so Claude's prefix cache hits.
   */
  private buildReviewerSystemPrompt(): string {
    const memory = this.secureLedger.readReviewerPersistentFile('reviewer_memory.md')?.trim() ?? '';
    if (!memory) return this.reviewerSystemPrompt;
    return `${this.reviewerSystemPrompt.trimEnd()}\n\n<frozen_memory_snapshot>\n${memory}\n</frozen_memory_snapshot>\n\nThe snapshot above was injected into your system prompt at session start\nand is frozen for this Reviewer session. Append new fakery patterns or\nobservations to reviewer_memory.md on disk; they will be re-injected on\nthe next Reviewer reset, not mid-session.`;
  }

  private async ensureReviewer(): Promise<void> {
    if (this.reviewerStarted && this.currentGeneration('reviewer')?.state === 'live') return;
    this.validateSelection('reviewer', this.reviewerSelection);
    this.secureLedger.ensureReviewerSandbox();
    const sessionPrompt = this.buildReviewerSystemPrompt();
    this.secureLedger.ensureReviewerSandbox();
    this.reviewerSessionPrompt = sessionPrompt;
    try {
      await this.ensureAgentSession('reviewer', async (generation) => {
        await this.config.manager.startSession(
          {
            name: this.reviewerName,
            cwd: this.reviewerSandboxDir,
            engine: this.reviewerSelection.engine,
            model: this.roleModel('reviewer', this.reviewerSelection),
            customEngine: this.reviewerSelection.engine === 'custom' ? this.reviewerSelection.customEngine : undefined,
            permissionMode: 'bypassPermissions',
            systemPrompt: sessionPrompt,
          },
          generation,
        );
      });
    } catch (err) {
      if (!this.reviewerStarted) this.reviewerSessionPrompt = null;
      throw err;
    }
  }

  /**
   * Stage the iter's artifacts into the Reviewer sandbox cwd. Reviewer is a
   * persistent session whose cwd is fixed at <ledger>/reviewer_sandbox/, so
   * every review must rewrite the sandbox to "this iter's view".
   */
  private stageReviewSandbox(iter: number): { priorVerdict: boolean } {
    const staged = this.secureLedger.stageReviewerSandbox(iter, {
      plan: this.readReviewerControlFile('plan.md'),
      goal: this.readReviewerControlFile('goal.json'),
    });
    return { priorVerdict: staged.priorVerdict };
  }

  private async deliverToReviewer(env: AnyAutoloopMessage, dispatchId: string): Promise<AnyAutoloopMessage[]> {
    if (this.terminal) return [];
    if (env.type !== 'review_request') {
      throw new Error(`[autoloop] reviewer does not accept message type=${env.type}`);
    }
    const iter = env.iter;
    const staged = this.stageReviewSandbox(iter);
    await this.ensureReviewer();
    if (this.terminal) return [];

    const promptText =
      'checkpoint_sha' in env.payload
        ? [
            `[review_request iter=${iter}]`,
            `Artifacts staged from run ${env.payload.source_run_id} iter ${env.payload.source_iter} at: iter-${iter}/ (directive.json, diff.patch, eval_output.json)`,
            `checkpoint_sha: ${env.payload.checkpoint_sha}`,
            `scope: ${JSON.stringify(env.payload.scope)}`,
            `prior_verdict: ${staged.priorVerdict ? 'prior_verdict.json' : '(none)'}`,
            `prior_metrics: ${JSON.stringify(env.payload.prior_metrics)}`,
            '',
            'Audit and emit `review_complete`.',
          ].join('\n')
        : `[review_request iter=${iter}]\nArtifacts staged at: iter-${iter}/ (directive.json, diff.patch, eval_output.json)\nprior_verdict: ${staged.priorVerdict ? 'prior_verdict.json' : '(none)'}\nprior_metrics: ${JSON.stringify(env.payload.prior_metrics)}\n\nAudit and emit \`review_complete\`.`;

    // Heartbeat so the dashboard's Reviewer pane shows "auditing" the moment
    // a review_request lands, instead of staying blank until the verdict.
    this.appendChatEntry({
      who: 'reviewer',
      text: `🔍 Reviewer iter ${iter} auditing…`,
      ts: new Date().toISOString(),
    });

    const result = await this.sendWithRecovery(
      'reviewer',
      this.reviewerName,
      this.withRoleInstructions(
        'reviewer',
        this.reviewerSelection,
        this.reviewerSessionPrompt ?? this.reviewerSystemPrompt,
        promptText,
      ),
      this.pendingSendTimeout(env, 'reviewer', dispatchId),
    );
    if (result.recoverable_timeout) {
      if (this.terminal) return [];
      return [Msg.sendTimeout(env.iter, result.recoverable_timeout)];
    }
    if (this.terminal) return [];
    if (result.fatal) {
      this.appendDecisionLog({
        kind: 'phase_error',
        actor: 'dispatcher',
        payload: { agent: 'reviewer', phase: 'send', code: result.code, error: result.error ?? 'unknown' },
      });
      return [
        Msg.phaseError(iter, {
          agent: 'reviewer',
          phase: 'send',
          code: result.code,
          error: result.error ?? 'unknown send failure',
        }),
      ];
    }
    this.recordTurn('reviewer', 'user', promptText);
    this.recordTurn('reviewer', 'agent', (result.output ?? '').trim());
    const replyText = (result.output ?? '').trim();
    const parsed = parseAgentReply(replyText);
    this.emit('reviewer_reply', parsed.cleaned_reply);
    if (parsed.cleaned_reply) {
      this.appendChatEntry({
        who: 'reviewer',
        text: parsed.cleaned_reply,
        ts: new Date().toISOString(),
      });
    }

    const rc = extractReviewComplete(parsed.calls);
    if (!rc) {
      // Reviewer didn't emit a verdict — treat as 'hold' with the cleaned
      // reply as audit notes so the loop doesn't stall silently.
      const verdict = Msg.reviewVerdict(iter, {
        decision: 'hold',
        metric: null,
        audit_notes: `[no verdict emitted] ${parsed.cleaned_reply.slice(0, 500)}`,
      });
      this.persistVerdict(iter, {
        decision: 'hold',
        metric: null,
        audit_notes: verdict.payload.audit_notes,
      });
      await this.maybeCompact('reviewer', this.reviewerName);
      return [verdict];
    }

    const gated = await this.gateVerdict(iter, rc);
    this.persistVerdict(iter, gated);
    await this.maybeCompact('reviewer', this.reviewerName);
    return [Msg.reviewVerdict(iter, gated)];
  }

  /**
   * Run the acceptance contract before letting an `advance` stand.
   *
   * The Reviewer is asked to "re-derive the metric independently", but its
   * sandbox holds only the iteration's artifacts — no code, no evaluator — so it
   * cannot, and its verdict is ultimately a reading of the Coder's own report.
   * When a contract is configured, this runs the checks against the real
   * workspace and downgrades `advance` to `hold` on red. Without a contract the
   * verdict passes through unchanged, exactly as before.
   */
  private async gateVerdict<T extends { decision: string; metric: number | null; audit_notes: string }>(
    iter: number,
    rc: T,
  ): Promise<T & { accepted?: true; evidence_id?: string }> {
    const contract = this.config.contract;
    if (!contract || rc.decision !== 'advance') return rc;

    const iterDir = path.join(this.ledgerDir, 'iter', String(iter));
    const evidenceId = `iter-${iter}`;
    const { results, passed, rounds } = await runContract(contract, {
      cwd: this.config.workspace,
      artifactDir: path.join(iterDir, 'evidence'),
      baseSha: undefined,
      logger: this.logger,
    });
    await writeEvidence({
      runDir: this.ledgerDir,
      runId: this.config.runId,
      node: `reviewer-iter-${iter}`,
      evidenceId,
      cwd: this.config.workspace,
      contractId: contract.id,
      results,
      rounds,
      logger: this.logger,
    });

    if (passed) {
      this.emit('target_hit', { iter, evidenceId });
      return { ...rc, accepted: true, evidence_id: evidenceId };
    }
    const failed = results.filter((r) => r.required && !r.passed).map((r) => r.detail);
    this.appendDecisionLog({
      kind: 'phase_error',
      actor: 'dispatcher',
      payload: { agent: 'reviewer', phase: 'acceptance', error: failed.join('; '), iter },
    });
    return {
      ...rc,
      decision: 'hold',
      audit_notes: `${rc.audit_notes}\n\n[acceptance] advance withheld — ${failed.join('; ')} (evidence: ${evidenceId})`,
    };
  }

  private persistVerdict(iter: number, payload: PersistedReviewVerdictPayload): void {
    const canonical = canonicalPersistedVerdictPayload(payload);
    const existing = this.secureLedger.readIterationArtifact(iter, 'verdict.json');
    if (existing !== undefined) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(existing.toString('utf8'));
      } catch (error) {
        throw new Error(`Refusing to replay malformed immutable Reviewer verdict for iteration ${iter}`, {
          cause: error,
        });
      }
      if (
        isPlainRecord(parsed) &&
        Object.hasOwn(parsed, 'schema_version') &&
        parsed.schema_version === LEDGER_SCHEMA_VERSION &&
        Object.hasOwn(parsed, 'iter') &&
        parsed.iter === iter &&
        Object.hasOwn(parsed, 'ts') &&
        typeof parsed.ts === 'string' &&
        samePersistedVerdictPayload(parsed, canonical)
      ) {
        return;
      }
      throw new Error(`Refusing to overwrite conflicting immutable Reviewer verdict for iteration ${iter}`);
    }
    this.secureLedger.writeIterationArtifact(
      iter,
      'verdict.json',
      serializePersistedVerdictV1(iter, new Date().toISOString(), canonical),
    );
  }

  /** Run a git command in the workspace; returns combined output. Used by Coder commits. */
  private async runGit(argv: string[]): Promise<{ code: number; out: string; err: string }> {
    return new Promise((resolve) => {
      const child = spawn(argv[0], argv.slice(1), {
        cwd: this.config.workspace,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let out = '';
      let err = '';
      child.stdout?.on('data', (b) => (out += b.toString()));
      child.stderr?.on('data', (b) => (err += b.toString()));
      child.on('error', (e) => resolve({ code: 127, out: '', err: (e as Error).message }));
      child.on('exit', (code) => resolve({ code: code ?? 0, out, err }));
    });
  }

  /** Internal deterministic test seam; production always uses argv-safe spawn. */
  private spawnGitEvidenceProcess(argv: string[]): ChildProcess {
    const environment = Object.create(null) as NodeJS.ProcessEnv;
    for (const [name, value] of Object.entries(process.env)) {
      if (!name.toUpperCase().startsWith('GIT_')) environment[name] = value;
    }
    return spawn(argv[0], argv.slice(1), {
      cwd: this.config.workspace,
      detached: process.platform !== 'win32',
      env: environment,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });
  }

  /** Kill the complete evidence command tree, not merely its immediate shell-free child. */
  private killGitEvidenceProcess(child: ChildProcess): void {
    const pid = child.pid;
    if (pid !== undefined && process.platform !== 'win32') {
      try {
        process.kill(-pid, 'SIGKILL');
        return;
      } catch {
        // The group may already have exited; fall through to the child handle.
      }
    } else if (pid !== undefined) {
      try {
        const killer = spawn('taskkill', ['/PID', String(pid), '/T', '/F'], {
          detached: false,
          stdio: 'ignore',
          windowsHide: true,
        });
        killer.unref();
      } catch {
        // Fall through to ChildProcess.kill when taskkill could not start.
      }
    }
    try {
      child.kill('SIGKILL');
    } catch {
      // Best effort after the promise has already been deterministically settled.
    }
  }

  /**
   * Run one bounded read-only Git evidence command. When expectedStdout is
   * provided, stdout is compared chunk-by-chunk and never accumulated.
   */
  private async runGitEvidence(
    argv: string[],
    maxStdoutBytes: number,
    label: string,
    expectedStdout?: Buffer,
  ): Promise<{ code: number; out: Buffer; err: Buffer }> {
    return await new Promise((resolve, reject) => {
      const child = this.spawnGitEvidenceProcess(argv);
      const stdout: Buffer[] = [];
      const stderr: Buffer[] = [];
      let stdoutBytes = 0;
      let stderrBytes = 0;
      let comparedBytes = 0;
      let settled = false;

      const cleanup = (): void => {
        clearTimeout(timeout);
        child.stdout?.removeListener('data', onStdout);
        child.stderr?.removeListener('data', onStderr);
        child.removeListener('error', onError);
        child.removeListener('close', onClose);
      };
      const fail = (error: Error): void => {
        if (settled) return;
        settled = true;
        cleanup();
        this.killGitEvidenceProcess(child);
        reject(error);
      };
      const onStdout = (value: Buffer | string): void => {
        if (settled) return;
        const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
        stdoutBytes += chunk.length;
        if (stdoutBytes > maxStdoutBytes) {
          fail(
            new Error(
              expectedStdout
                ? `Reviewer-only checkpoint Git ${label} output is longer than source diff.patch (${maxStdoutBytes} bytes)`
                : `Reviewer-only Git ${label} output is longer than the ${maxStdoutBytes}-byte limit`,
            ),
          );
          return;
        }
        if (expectedStdout) {
          if (
            comparedBytes + chunk.length > expectedStdout.length ||
            !chunk.equals(expectedStdout.subarray(comparedBytes, comparedBytes + chunk.length))
          ) {
            fail(new Error(`Reviewer-only source diff.patch does not match checkpoint Git ${label} output`));
            return;
          }
          comparedBytes += chunk.length;
          return;
        }
        stdout.push(chunk);
      };
      const onStderr = (value: Buffer | string): void => {
        if (settled) return;
        const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
        stderrBytes += chunk.length;
        if (stderrBytes > MAX_GIT_EVIDENCE_STDERR_BYTES) {
          fail(new Error(`Reviewer-only Git ${label} stderr exceeds the ${MAX_GIT_EVIDENCE_STDERR_BYTES}-byte limit`));
          return;
        }
        stderr.push(chunk);
      };
      const onError = (error: Error): void => {
        fail(new Error(`Reviewer-only Git ${label} could not start: ${error.message}`, { cause: error }));
      };
      const onClose = (code: number | null, signal: NodeJS.Signals | null): void => {
        if (settled) return;
        const exitCode = code ?? (signal ? 1 : 0);
        if (exitCode === 0 && expectedStdout && comparedBytes !== expectedStdout.length) {
          fail(new Error(`Reviewer-only source diff.patch does not match checkpoint Git ${label} output`));
          return;
        }
        settled = true;
        cleanup();
        resolve({
          code: exitCode,
          out: expectedStdout ? Buffer.alloc(0) : Buffer.concat(stdout, stdoutBytes),
          err: Buffer.concat(stderr, stderrBytes),
        });
      };
      const timeout = setTimeout(() => {
        fail(new Error(`Reviewer-only Git evidence ${label} timed out after ${GIT_EVIDENCE_TIMEOUT_MS}ms`));
      }, GIT_EVIDENCE_TIMEOUT_MS);
      timeout.unref();

      child.stdout?.on('data', onStdout);
      child.stderr?.on('data', onStderr);
      child.once('error', onError);
      child.once('close', onClose);
    });
  }

  // ─── git helper for write_plan_committed / write_goal_committed ──────────

  private async gitCommit(filename: string, message: string): Promise<void> {
    const run = (argv: string[], input?: string): Promise<{ code: number; out: string; err: string }> =>
      new Promise((resolve) => {
        const child = spawn(argv[0], argv.slice(1), {
          cwd: this.config.workspace,
          stdio: ['pipe', 'pipe', 'pipe'],
        });
        let out = '';
        let err = '';
        child.stdout?.on('data', (b) => (out += b.toString()));
        child.stderr?.on('data', (b) => (err += b.toString()));
        child.on('error', (e) => resolve({ code: 127, out: '', err: (e as Error).message }));
        child.on('exit', (code) => resolve({ code: code ?? 0, out, err }));
        child.stdin?.end(input);
      });

    const detailFor = (result: { code: number; out: string; err: string }): string =>
      (result.err || result.out || `exit ${result.code}`).trim().slice(0, 300);
    const repository = await run(['git', 'rev-parse', '--is-inside-work-tree']);
    if (repository.code !== 0) {
      if (/not a git repository/i.test(repository.err + repository.out)) {
        this.logger.info?.(`[autoloop] commit_${filename}: workspace is not a git repository`);
        return;
      }
      throw new Error(`git rev-parse failed for ${filename} (code=${repository.code}): ${detailFor(repository)}`);
    }
    if (repository.out.trim() !== 'true') {
      throw new Error(`git rev-parse did not confirm a work tree for ${filename}`);
    }

    // Planner owns exactly one control artifact. Scope every git operation to
    // that path so pre-existing staged or dirty product work remains untouched.
    const status = await run(['git', 'status', '--porcelain', '--', filename]);
    if (status.code !== 0) {
      throw new Error(`git status failed for ${filename} (code=${status.code}): ${detailFor(status)}`);
    }
    if (status.out.trim() === '') {
      this.logger.info?.(`[autoloop] commit_${filename}: no changes to commit`);
      return;
    }
    const priorIndex = await run(['git', 'ls-files', '--stage', '--', filename]);
    if (priorIndex.code !== 0) {
      throw new Error(`git ls-files failed for ${filename} (code=${priorIndex.code}): ${detailFor(priorIndex)}`);
    }
    const add = await run(['git', 'add', '--', filename]);
    if (add.code !== 0) {
      throw new Error(`git add failed for ${filename} (code=${add.code}): ${detailFor(add)}`);
    }
    const commit = await run(['git', 'commit', '--only', '-m', message, '--', filename]);
    if (commit.code !== 0) {
      const detail = detailFor(commit);
      const removeCurrent = await run(['git', 'update-index', '--force-remove', '--', filename]);
      let restoreError = removeCurrent.code === 0 ? '' : detailFor(removeCurrent);
      if (!restoreError && priorIndex.out.length > 0) {
        const restore = await run(['git', 'update-index', '--index-info'], priorIndex.out);
        if (restore.code !== 0) restoreError = detailFor(restore);
      }
      // Surface, don't just log: a silent commit failure leaves the file on disk
      // but uncommitted, so the next Coder iter sees inconsistent git state.
      const failure = `git commit failed for ${filename} (code=${commit.code}): ${detail}`;
      const surfaced = restoreError ? `${failure}; index restoration failed: ${restoreError}` : failure;
      this.logger.error?.(`[autoloop] ${surfaced}`);
      this.emit('planner_error', new Error(surfaced));
      throw new Error(surfaced);
    }
  }
}
