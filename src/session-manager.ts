/**
 * SessionManager — manages multiple PersistentClaudeSession instances
 *
 * Replaces the Express server layer. Pure class with no HTTP dependency.
 * Can be used by Plugin tools, CLI, or any other consumer.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import { execFile, execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
import * as http from 'node:http';
import { createRequire } from 'node:module';
import RE2 from 're2';

const _require = createRequire(import.meta.url);
function getPluginVersion(): string {
  try {
    // Walk up from this file to find package.json
    let dir = path.dirname(_require.resolve('./session-manager.js').replace('/dist/', '/'));
    for (let i = 0; i < 5; i++) {
      const pkgPath = path.join(dir, 'package.json');
      if (fs.existsSync(pkgPath)) {
        const pkg = JSON.parse(fs.readFileSync(pkgPath, 'utf8')) as { version?: string };
        if (pkg.version) return pkg.version;
      }
      dir = path.dirname(dir);
    }
  } catch {
    /* ignore */
  }
  return 'unknown';
}

// ─── Persistence ─────────────────────────────────────────────────────────────

const PERSIST_DIR = path.join(os.homedir(), '.openclaw');
const PERSIST_FILE = path.join(PERSIST_DIR, 'claude-sessions.json');
const PERSIST_LOCK_FILE = `${PERSIST_FILE}.lock`;
const MAX_RELEASED_REVIEW_IDENTITIES_PER_RUN = 64;
/** A recovery envelope is written before queue acceptance and may be replayed once by a crash retry. */
const MAX_RECOVERY_REVIEW_ENVELOPE_DUPLICATES = 2;
/** Bound untrusted decision-ledger indexing during cold recovery. */
const MAX_RECOVERY_REVIEW_ENVELOPE_INDEX_ROWS = 128;
// PERSIST_DISK_TTL_MS imported from ./constants.js

interface PersistedSession {
  name: string;
  claudeSessionId: string;
  cwd: string;
  model?: string;
  engine?: EngineType;
  sandboxMode?: SessionConfig['sandboxMode'];
  originalCreated: string;
  lastResumed: string;
  lastActivity: number;
  /** Generation fence for an Autoloop-owned physical session name. */
  agentGeneration?: number;
  agentOwnerInstanceId?: string;
  agentSessionId?: string;
  /** Exact generation remains unavailable while its release evidence is persisted. */
  agentReleasePending?: boolean;
  /** Runtime owner that won the durable compare-and-release fence. */
  agentReleaseOwnerInstanceId?: string;
  /** Retained after release so a stale caller cannot reuse an older generation. */
  agentReleasedGeneration?: number;
  agentReleasedOwnerInstanceId?: string;
  agentReleasedSessionId?: string;
}

const AGENT_FENCE_FIELDS = [
  'agentGeneration',
  'agentOwnerInstanceId',
  'agentSessionId',
  'agentReleasePending',
  'agentReleaseOwnerInstanceId',
  'agentReleasedGeneration',
  'agentReleasedOwnerInstanceId',
  'agentReleasedSessionId',
] as const satisfies ReadonlyArray<keyof PersistedSession>;

function hasAgentFence(session: PersistedSession): boolean {
  return AGENT_FENCE_FIELDS.some((field) => session[field] !== undefined);
}

function sameAgentFence(left: PersistedSession, right: PersistedSession): boolean {
  return AGENT_FENCE_FIELDS.every((field) => left[field] === right[field]);
}

/**
 * Merge an ordinary registry snapshot without letting a stale manager publish
 * or erase Autoloop fencing state. Agent transitions use the locked CAS path;
 * lifecycle persistence may update only a fence that is still authoritative.
 */
function mergeRegistrySnapshot(
  authoritative: Map<string, PersistedSession>,
  desired: Map<string, PersistedSession>,
): Map<string, PersistedSession> {
  const merged = new Map(desired);

  for (const [name, authoritativeSession] of authoritative) {
    const desiredSession = desired.get(name);
    if (!hasAgentFence(authoritativeSession)) {
      if (desiredSession && hasAgentFence(desiredSession)) merged.set(name, authoritativeSession);
      continue;
    }
    if (!desiredSession || !sameAgentFence(authoritativeSession, desiredSession)) {
      merged.set(name, authoritativeSession);
      continue;
    }

    const withAuthoritativeFence: PersistedSession = { ...desiredSession };
    const mutableFence = withAuthoritativeFence as unknown as Record<string, unknown>;
    for (const field of AGENT_FENCE_FIELDS) {
      const value = authoritativeSession[field];
      if (value === undefined) delete mutableFence[field];
      else mutableFence[field] = value;
    }
    merged.set(name, withAuthoritativeFence);
  }

  for (const [name, desiredSession] of desired) {
    if (!authoritative.has(name) && hasAgentFence(desiredSession)) merged.delete(name);
  }
  return merged;
}

/**
 * Refresh the local registry from disk without discarding metadata whose
 * debounced write is still pending. Local metadata is reusable only while the
 * exact authoritative fence is unchanged; a successor fence or authoritative
 * deletion always wins.
 */
function mergeRegistryView(
  authoritative: Map<string, PersistedSession>,
  local: Map<string, PersistedSession>,
): Map<string, PersistedSession> {
  const merged = new Map(authoritative);
  for (const [name, localSession] of local) {
    const authoritativeSession = authoritative.get(name);
    if (!authoritativeSession) {
      if (!hasAgentFence(localSession)) merged.set(name, localSession);
      continue;
    }
    if (!sameAgentFence(authoritativeSession, localSession)) continue;

    const withLocalMetadata: PersistedSession = { ...authoritativeSession, ...localSession };
    const mutableFence = withLocalMetadata as unknown as Record<string, unknown>;
    for (const field of AGENT_FENCE_FIELDS) {
      const value = authoritativeSession[field];
      if (value === undefined) delete mutableFence[field];
      else mutableFence[field] = value;
    }
    merged.set(name, withLocalMetadata);
  }
  return merged;
}

export type AutoloopAgentRegistryErrorCode =
  | 'AUTOLOOP_AGENT_REGISTRY_CORRUPT'
  | 'AUTOLOOP_AGENT_REGISTRY_READ_FAILED'
  | 'AUTOLOOP_AGENT_REGISTRY_LOCK_CLEANUP_FAILED'
  | 'AUTOLOOP_AGENT_REGISTRY_LOCK_CONTENDED'
  | 'AUTOLOOP_AGENT_REGISTRY_PERSIST_FAILED';

export class AutoloopAgentRegistryError extends Error {
  readonly code: AutoloopAgentRegistryErrorCode;
  readonly retryable: boolean;

  constructor(code: AutoloopAgentRegistryErrorCode, message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'AutoloopAgentRegistryError';
    this.code = code;
    this.retryable =
      code !== 'AUTOLOOP_AGENT_REGISTRY_CORRUPT' && code !== 'AUTOLOOP_AGENT_REGISTRY_LOCK_CLEANUP_FAILED';
  }
}

function loadPersistedSessions(): Map<string, PersistedSession> {
  if (!fs.existsSync(PERSIST_FILE)) return new Map();

  let raw: string;
  try {
    raw = fs.readFileSync(PERSIST_FILE, 'utf8');
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return new Map();
    throw new AutoloopAgentRegistryError(
      'AUTOLOOP_AGENT_REGISTRY_READ_FAILED',
      `Failed to read the shared session registry: ${(err as Error).message}`,
      { cause: err },
    );
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new AutoloopAgentRegistryError(
      'AUTOLOOP_AGENT_REGISTRY_CORRUPT',
      `The shared session registry contains malformed JSON: ${(err as Error).message}`,
      { cause: err },
    );
  }
  if (!Array.isArray(parsed)) {
    throw new AutoloopAgentRegistryError(
      'AUTOLOOP_AGENT_REGISTRY_CORRUPT',
      'The shared session registry root must be an array',
    );
  }
  if (
    parsed.some(
      (entry) =>
        !entry ||
        typeof entry !== 'object' ||
        typeof (entry as Partial<PersistedSession>).name !== 'string' ||
        typeof (entry as Partial<PersistedSession>).lastActivity !== 'number',
    )
  ) {
    throw new AutoloopAgentRegistryError(
      'AUTOLOOP_AGENT_REGISTRY_CORRUPT',
      'The shared session registry contains an invalid session entry',
    );
  }

  const arr = parsed as PersistedSession[];
  const now = Date.now();
  // A durable generation fence cannot expire merely because its ordinary
  // resumable-session metadata is old. Recovery must explicitly release it.
  const valid = arr.filter((session) => hasAgentFence(session) || now - session.lastActivity < PERSIST_DISK_TTL_MS);
  return new Map(valid.map((session) => [session.name, session]));
}

// Atomic write used only while PERSIST_LOCK_FILE is held.
function savePersistedSessions(
  sessions: Map<string, PersistedSession>,
  logger?: Logger,
): { ok: true } | { ok: false; error: AutoloopAgentRegistryError } {
  const tmp = PERSIST_FILE + '.tmp';
  try {
    fs.mkdirSync(PERSIST_DIR, { recursive: true });
    const arr = Array.from(sessions.values());
    fs.writeFileSync(tmp, JSON.stringify(arr, null, 2));
    const fileFd = fs.openSync(tmp, 'r+');
    try {
      fs.fsyncSync(fileFd);
    } finally {
      fs.closeSync(fileFd);
    }
    fs.renameSync(tmp, PERSIST_FILE);
    if (process.platform === 'win32') {
      (logger || createConsoleLogger('SessionManager')).warn(
        'Shared session registry was replaced, but parent-directory fsync is unavailable on win32',
      );
    } else {
      const directoryFd = fs.openSync(PERSIST_DIR, 'r');
      try {
        fs.fsyncSync(directoryFd);
      } finally {
        fs.closeSync(directoryFd);
      }
    }
    return { ok: true };
  } catch (err) {
    try {
      if (fs.existsSync(tmp)) fs.unlinkSync(tmp);
    } catch {
      // Preserve the primary persistence error. A later locked write will
      // replace the fixed-name temp file before attempting another rename.
    }
    (logger || createConsoleLogger('SessionManager')).warn('Failed to persist sessions:', (err as Error).message);
    return {
      ok: false,
      error: new AutoloopAgentRegistryError(
        'AUTOLOOP_AGENT_REGISTRY_PERSIST_FAILED',
        `Failed to persist the shared session registry: ${(err as Error).message}`,
        { cause: err },
      ),
    };
  }
}

interface DebouncedCallback {
  (): void;
  cancel(): void;
}

// Debounce helper — coalesces rapid writes into one
function makeDebounced(fn: () => void, ms: number): DebouncedCallback {
  let timer: ReturnType<typeof setTimeout> | null = null;
  const debounced = (() => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      fn();
    }, ms);
  }) as DebouncedCallback;
  debounced.cancel = () => {
    if (!timer) return;
    clearTimeout(timer);
    timer = null;
  };
  return debounced;
}

import { type Logger, createConsoleLogger } from './logger.js';
import { CircuitBreaker } from './circuit-breaker.js';
import { detectRepoLang } from './kernel/repo.js';
import { isFileLockReleaseError, withFileLock } from './kernel/file-lock.js';
import { RunKernel, runDir as kernelRunDir } from './kernel/engine.js';
import { registerDefaultExecutors } from './kernel/nodes/index.js';
import { autoloopStateFromRecord, makeAutoloopExecutor, type AutoloopHandle } from './kernel/nodes/autoloop.js';
import { leaseIsStale, loadRun, readLease, readNodeOutput, type RunSummary } from './kernel/store.js';
import {
  LEGACY_NODE,
  joinFindings,
  toCouncilSession,
  toFanoutSession,
  toUltraplanResult,
  toUltrareviewResult,
  type FanoutNodeData,
} from './kernel/projections.js';
import {
  legacyCouncilWorkflow,
  legacyFanoutWorkflow,
  legacyUltraplanWorkflow,
  splitAgentSecrets,
} from './kernel/templates/index.js';
import type { KernelEvent, RunRecord, RunState, WorkflowSpec } from './kernel/types.js';
import { normalizeContract } from './verify/contract.js';
import { runContract } from './verify/runner.js';
import { evidenceDir, listEvidence, readEvidence, writeEvidence, type EvidenceBundle } from './verify/evidence.js';
import {
  annotateVerdicts,
  appendRunRow,
  readRunLedger,
  summarizeRuns,
  type RunLedgerQuery,
  type RunLedgerRow,
  type RunLedgerSummary,
} from './run-ledger.js';
import { checkBudget, isBudgetExceeded } from './budget.js';
import { InboxManager, type SessionLookup } from './inbox-manager.js';
import { sanitizeCwd, validateName } from './validation.js';
import { PersistentClaudeSession } from './persistent-session.js';
import {
  cloneTranscript,
  DEFAULT_HANDOFF_CHARS,
  MIN_HANDOFF_CHARS,
  newTranscript,
  recordExchange,
  renderHandoff,
  type Transcript,
} from './handoff.js';
import { PersistentGeminiSession } from './persistent-gemini-session.js';
import { PersistentCodexSession } from './persistent-codex-session.js';
import { PersistentCodexAppServerSession } from './persistent-codex-app-session.js';
import { PersistentCursorSession } from './persistent-cursor-session.js';
import { PersistentGrokSession } from './persistent-grok-session.js';
import { PersistentOpencodeSession } from './persistent-opencode-session.js';
import { PersistentAgySession } from './persistent-agy-session.js';
import { PersistentCustomSession } from './persistent-custom-session.js';
import { resolveCustomEngine } from './engine-presets.js';
import {
  type SessionConfig,
  type SessionInfo,
  type SendOptions,
  type PermissionDenial,
  type SendResult,
  type PluginConfig,
  type EffortLevel,
  ENGINE_TYPES,
  type EngineType,
  type CustomEngineConfig,
  type AgentInfo,
  type SkillInfo,
  type RuleInfo,
  type StreamEvent,
  type ISession,
  type CouncilConfig,
  type CouncilSession,
  type CouncilReviewResult,
  type CouncilAcceptResult,
  type CouncilRejectResult,
  type InboxMessage,
  type UltraplanResult,
  type UltrareviewResult,
  overrideModelPricing,
  ENGINE_BINARY_NAMES,
} from './types.js';
import { resolveAlias, isClaudeModel, lookupModel } from './models.js';
import { isAgyConversationId } from './agy-conversation.js';
import { Council } from './council.js';
import { Fanout, type FanoutConfig, type FanoutSession, type FanoutAgentSpec } from './fanout.js';
import { AutoloopRunner } from './autoloop/runner.js';
import {
  AutoloopOperationError,
  ClaudeAgentDispatcher,
  type AutoloopResetResult,
  type ClaudeAgentDispatcherConfig,
} from './autoloop/dispatcher.js';
import type {
  AgentReservationReleaseOptions,
  AgentRuntimeLiveness,
  AgentRuntimeProbe,
  AutoloopChatStateCode,
  AutoloopRecoveryErrorCode,
  AutoloopState,
  RecoveryAgentEvidence,
  RecoveryActionSnapshot,
  RecoveryAssessment,
  RecoveryDeliveryEvidence,
  RecoveryIterationEvidence,
  RecoveryReceipt,
  RecoveryReviewEnvelope,
  RecoveryResult,
  PhysicalAgentGeneration,
  PublicAutoloopFailure,
  PublicAutoloopFailureCode,
  PushPolicy,
} from './autoloop/types.js';
import {
  AutoloopRecoveryError,
  AutoloopAgentReleaseOwnerError,
  DEFAULT_PUSH_POLICY,
  DEFAULT_SEND_TIMEOUT_MS,
  isRecoverableAgentOwnerInstanceId,
  validateAutoloopTimeoutConfig,
} from './autoloop/types.js';
import {
  assessRecovery,
  blockRecoveryAssessment,
  parseRecoveryReceipt,
  parseRecoveryReviewEnvelope,
  rebindRecoveryAction,
  recoveryActionDispatchId,
  recoveryActionDigest,
  recoveryLogicalMessageSha256,
} from './autoloop/recovery.js';
import {
  parseOutboxDecisionLedgerRow,
  validateOutboxDecisionLedgerGraph,
  type DeliveryLedgerRow,
} from './autoloop/outbox.js';
import {
  Msg as AutoloopMsg,
  canonicalizeRequestReviewArgs,
  type AutoloopMessageType,
  type AutoloopOperationErrorCode,
  type PushChannel,
  type PushLevel,
  type RequestReviewArgs,
  type SendTimeoutPayload,
} from './autoloop/messages.js';
import { appendPushLog, notifyUserFallbackChain } from './autoloop/notify.js';
import {
  isCommittedSecureLedgerError,
  SecureAutoloopLedger,
  type SecureAutoloopLedgerCommitError,
  type SecureAutoloopPreparedAppend,
} from './autoloop/secure-ledger.js';
import { UltraappManager } from './ultraapp/manager.js';
import { UltraappStore, defaultStoreRoot } from './ultraapp/store.js';
import type { UltraappRouter } from './ultraapp/router.js';
import {
  PERSIST_DISK_TTL_MS,
  DEBOUNCED_SAVE_MS,
  CLEANUP_INTERVAL_MS,
  TURN_TIMEOUT_MS,
  GREP_HISTORY_FETCH,
  ULTRAPLAN_TIMEOUT_MS,
  STOP_SIGKILL_DELAY_MS,
  SESSION_EVENT,
  DEFAULT_HISTORY_LIMIT,
} from './constants.js';

// ─── Internal Types ──────────────────────────────────────────────────────────

interface ManagedSession {
  session: ISession;
  config: SessionConfig;
  created: string;
  lastActivity: number;
  cwd: string;
  claudeSessionId?: string;
  skipPersistence?: boolean;
  /**
   * Per-session send chain. Concurrent sendMessage() calls on the same session
   * MUST serialize, otherwise PersistentClaudeSession's single _streamCallbacks
   * field and shared TURN_COMPLETE listener race — the second caller would
   * receive the first caller's response. Each call awaits the previous chain
   * link, then installs its own; release happens in a finally block so a
   * thrown send still unblocks waiters.
   */
  sendChain?: Promise<unknown>;
  /**
   * Latched once cumulative spend reaches `config.maxBudgetUsd`. The gate in
   * sendMessage re-derives this from getCost() anyway; the flag exists so the
   * session listings can show *why* a session stopped accepting turns.
   */
  budgetExhausted?: boolean;
  /**
   * The conversation as sent and answered, kept for `handoffSession()`. Not the
   * engine's history buffer, which is capped by event count and loses the
   * opening request first on a long session. Created on first send.
   */
  transcript?: Transcript;
  /**
   * The rendered history of a session this one was handed off from, put in
   * front of the first message it receives. Cleared only once a send succeeds,
   * so a first turn that fails does not strand the conversation it was carrying.
   */
  pendingHandoff?: string;
}

/**
 * Structural type for the `codex-app` engine session, exposing the app-server
 * v2 RPC methods used by the codex_interrupt/steer/fork/rollback/models tools.
 * The `interrupt` method is the discriminator for "this is a codex-app session".
 */
type CodexAppSession = ISession & {
  interrupt: () => Promise<{ interrupted: boolean }>;
  steer: (text: string) => Promise<{ steered: boolean; turnId?: string; text?: string }>;
  forkThread: () => Promise<{ threadId: string }>;
  rollback: (numTurns: number) => Promise<void>;
  listModels: () => Promise<unknown[]>;
  listThreads: (opts?: {
    cwd?: string;
    searchTerm?: string;
    archived?: boolean;
    cursor?: string;
    limit?: number;
  }) => Promise<{ data: unknown[]; nextCursor: string | null }>;
};

// ─── Cross-process visibility ───────────────────────────────────────────────
//
// There used to be two separate answers here, neither of them good. Councils
// were enumerated by reading `~/.openclaw/council-logs/*.md` and pulling the id,
// task and status out with regexes — a stub session with no responses and an
// empty config. Autoloop kept its own append-only JSONL registry at
// `~/.claw-orchestrator/autoloop-registry.jsonl`, with its own append / upsert /
// reverse-scan-dedup / rewrite-via-tmp-file implementation, because its ledgers
// lived in whichever workspace the user picked.
//
// Both are gone. Every mode is a kernel run, every run lives under one root, and
// the run store is the index — so `listRuns()` is the single answer, and it
// returns real records rather than reconstructions. Council transcripts are
// still written for humans to read; nothing parses them.

type AutoloopRoleName = 'planner' | 'coder' | 'reviewer';

interface SendTimeoutMigrationAuditRecord {
  schema_version?: 1;
  ts: string;
  kind: 'timeout_migration';
  actor: 'operator';
  timestamp: string;
  runId: string;
  field: 'sendTimeoutMs';
  oldValue: number;
  newValue: number;
  reason: 'recoverable_send_timeout_resume' | 'stored_run_resume';
  pendingDispatchId?: string;
}

interface StoredAutoloopResumeContext {
  effectiveSendTimeoutMs: number;
  pendingDispatch: SendTimeoutPayload | null;
}

/** Resume-only custom configurations must never enter a recovery receipt. */
interface RecoveryBootOverrides {
  plannerCustomEngine?: CustomEngineConfig;
  coderCustomEngine?: CustomEngineConfig;
  reviewerCustomEngine?: CustomEngineConfig;
  sendTimeoutMs?: number;
}

interface PreparedSendTimeoutMigrationAppend {
  append: SecureAutoloopPreparedAppend;
  expectedTail: string;
}

class AutoloopChatStateError extends Error {
  constructor(
    readonly code: AutoloopChatStateCode,
    message: string,
    readonly retryable: boolean,
    readonly pending_dispatch?: SendTimeoutPayload,
    readonly status_reason?: string | null,
  ) {
    super(message);
    this.name = 'AutoloopChatStateError';
  }
}

export type { PublicAutoloopFailure, PublicAutoloopFailureCode };

export interface PublicAutoloopUnknownFailure {
  readonly message: string;
}

interface DetachedAutoloopPhaseFailure {
  readonly agent: 'planner';
  readonly phase: 'planner_turn';
  readonly code?: PublicAutoloopFailureCode;
  readonly committed?: true;
  readonly retryable?: boolean;
  readonly pending_dispatch?: Readonly<SendTimeoutPayload>;
  readonly status_reason?: string | null;
  readonly error: string;
}

interface DurableDetachedAutoloopPhaseFailure extends DetachedAutoloopPhaseFailure {
  readonly detached_failure_id?: string;
}

interface DurableDetachedAutoloopFailureRow {
  readonly ts: string;
  readonly payload: Readonly<DurableDetachedAutoloopPhaseFailure>;
  readonly startByteOffset: number;
}

interface DetachedFailureLedgerCursor {
  readonly version: 1;
  readonly byteOffset: number;
  readonly prefixSha256: string;
}

interface AutoloopChatFailureBinding {
  readonly logicalId: string;
  readonly preaudited?: DurableDetachedAutoloopFailureRow;
  readonly runnerProjection?: AutoloopState['recent_phase_errors'][number];
}

const DETACHED_AUTOLOOP_FAILURE_ID = Symbol('detachedAutoloopFailureId');

const AUTOLOOP_OPERATION_ERROR_RETRYABILITY = Object.freeze({
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
} as const satisfies Record<AutoloopOperationErrorCode, boolean>);

const AUTOLOOP_CHAT_STATE_RETRYABILITY = Object.freeze({
  AUTOLOOP_SEND_TIMEOUT: true,
  AUTOLOOP_RUN_PAUSED: false,
  AUTOLOOP_RUN_TERMINAL: false,
} as const satisfies Record<AutoloopChatStateCode, boolean>);

const AUTOLOOP_RECOVERY_ERROR_RETRYABILITY = Object.freeze({
  AUTOLOOP_RECOVERY_TOKEN_REQUIRED: false,
  AUTOLOOP_RECOVERY_TOKEN_STALE: false,
  AUTOLOOP_RECOVERY_MANUAL_RESOLUTION_REQUIRED: false,
  AUTOLOOP_RECOVERY_INCOMPLETE: false,
} as const satisfies Record<AutoloopRecoveryErrorCode, boolean>);

const COMMITTED_AUTOLOOP_LEDGER_ERROR_CODES = new Set<PublicAutoloopFailureCode>([
  'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
  'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
  'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE',
  'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
]);

function isAutoloopChatStateCode(value: unknown): value is AutoloopChatStateCode {
  return typeof value === 'string' && Object.hasOwn(AUTOLOOP_CHAT_STATE_RETRYABILITY, value);
}

function isAutoloopRecoveryErrorCode(value: unknown): value is AutoloopRecoveryErrorCode {
  return typeof value === 'string' && Object.hasOwn(AUTOLOOP_RECOVERY_ERROR_RETRYABILITY, value);
}

function isPublicAutoloopFailureCode(value: unknown): value is PublicAutoloopFailureCode {
  return isAutoloopOperationErrorCode(value) || isAutoloopChatStateCode(value) || isAutoloopRecoveryErrorCode(value);
}

function publicAutoloopFailureRetryable(code: PublicAutoloopFailureCode): boolean {
  if (isAutoloopOperationErrorCode(code)) return AUTOLOOP_OPERATION_ERROR_RETRYABILITY[code];
  if (isAutoloopChatStateCode(code)) return AUTOLOOP_CHAT_STATE_RETRYABILITY[code];
  return AUTOLOOP_RECOVERY_ERROR_RETRYABILITY[code];
}

function isCommittedAutoloopLedgerErrorCode(code: PublicAutoloopFailureCode): boolean {
  return COMMITTED_AUTOLOOP_LEDGER_ERROR_CODES.has(code);
}

function isAutoloopOperationErrorCode(value: unknown): value is AutoloopOperationErrorCode {
  return typeof value === 'string' && Object.hasOwn(AUTOLOOP_OPERATION_ERROR_RETRYABILITY, value);
}

function ownDataValue(value: object, key: PropertyKey): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor && Object.hasOwn(descriptor, 'value') ? descriptor.value : undefined;
}

function safeOwnErrorMessage(error: unknown): string {
  if (typeof error === 'string') return error;
  if ((typeof error !== 'object' || error === null) && typeof error !== 'function') return 'unknown error';
  try {
    const message = ownDataValue(error, 'message');
    return typeof message === 'string' ? message : 'unknown error';
  } catch {
    return 'unknown error';
  }
}

function publicData<T extends object>(fields: T): Readonly<T> {
  return Object.freeze(Object.assign(Object.create(null) as T, fields));
}

function snapshotPendingDispatch(value: unknown): Readonly<SendTimeoutPayload> | undefined {
  if (typeof value !== 'object' || value === null) return undefined;
  const status = ownDataValue(value, 'status');
  const dispatchId = ownDataValue(value, 'dispatch_id');
  const agent = ownDataValue(value, 'agent');
  const messageId = ownDataValue(value, 'message_id');
  const messageType = ownDataValue(value, 'message_type');
  const iter = ownDataValue(value, 'iter');
  const timeoutMs = ownDataValue(value, 'timeout_ms');
  const pendingError = ownDataValue(value, 'error');
  if (
    status !== 'awaiting_resume' ||
    typeof dispatchId !== 'string' ||
    dispatchId.length === 0 ||
    (agent !== 'planner' && agent !== 'coder' && agent !== 'reviewer') ||
    typeof messageId !== 'string' ||
    typeof messageType !== 'string' ||
    !new Set<AutoloopMessageType>([
      'chat',
      'directive',
      'directive_ack',
      'iter_artifacts',
      'review_request',
      'review_verdict',
      'iter_done',
      'push_user',
      'pause',
      'resume',
      'terminate',
      'phase_error',
      'send_timeout',
    ]).has(messageType as AutoloopMessageType) ||
    typeof iter !== 'number' ||
    typeof timeoutMs !== 'number' ||
    typeof pendingError !== 'string'
  ) {
    return undefined;
  }
  return publicData({
    status,
    dispatch_id: dispatchId,
    agent,
    message_id: messageId,
    message_type: messageType as AutoloopMessageType,
    iter,
    timeout_ms: timeoutMs,
    error: pendingError,
  });
}

/**
 * Convert only recognized typed Autoloop failures. Unknown errors intentionally
 * return undefined so adapters retain their existing generic failure path.
 */
export function toPublicAutoloopFailure(error: unknown): Readonly<PublicAutoloopFailure> | undefined {
  const committedLedgerError = isCommittedSecureLedgerError(error);
  if (error instanceof AutoloopOperationError || committedLedgerError) {
    const code = ownDataValue(error, 'code');
    const messageValue = ownDataValue(error, 'message');
    if (!isAutoloopOperationErrorCode(code)) return undefined;
    const committed = committedLedgerError && isCommittedAutoloopLedgerErrorCode(code);
    if (!committed && typeof messageValue !== 'string') return undefined;
    return publicData({
      code,
      message: typeof messageValue === 'string' ? messageValue : 'unknown error',
      ...(committed ? { committed: true as const } : {}),
      retryable: AUTOLOOP_OPERATION_ERROR_RETRYABILITY[code],
    });
  }

  if (error instanceof AutoloopRecoveryError) {
    const code = ownDataValue(error, 'code');
    const message = ownDataValue(error, 'message');
    if (!isAutoloopRecoveryErrorCode(code) || typeof message !== 'string') return undefined;
    return publicData({ code, message, retryable: AUTOLOOP_RECOVERY_ERROR_RETRYABILITY[code] });
  }

  if (error instanceof Error && ownDataValue(error, 'name') === 'AutoloopChatStateError') {
    const codeValue = ownDataValue(error, 'code');
    if (typeof codeValue !== 'string' || !Object.hasOwn(AUTOLOOP_CHAT_STATE_RETRYABILITY, codeValue)) {
      return undefined;
    }
    const code = codeValue as AutoloopChatStateCode;
    const message = ownDataValue(error, 'message');
    if (typeof message !== 'string') return undefined;
    const pending = snapshotPendingDispatch(ownDataValue(error, 'pending_dispatch'));
    const rawStatusReason = ownDataValue(error, 'status_reason');
    const statusReason = typeof rawStatusReason === 'string' || rawStatusReason === null ? rawStatusReason : undefined;
    return publicData({
      code,
      message,
      retryable: AUTOLOOP_CHAT_STATE_RETRYABILITY[code],
      ...(pending ? { pending_dispatch: pending } : {}),
      ...(statusReason !== undefined ? { status_reason: statusReason } : {}),
    });
  }

  if (
    typeof error === 'object' &&
    error !== null &&
    ownDataValue(error, 'ok') === false &&
    ownDataValue(error, 'code') === 'AUTOLOOP_RESET_POSTCONDITION_FAILED' &&
    typeof ownDataValue(error, 'message') === 'string' &&
    ownDataValue(error, 'retryable') === false
  ) {
    return publicData({
      code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
      message: ownDataValue(error, 'message') as string,
      retryable: false,
    });
  }

  return undefined;
}

function detachedPhaseFailure(
  failure: Readonly<PublicAutoloopFailure | PublicAutoloopUnknownFailure>,
  detachedFailureId?: string,
): Readonly<DurableDetachedAutoloopPhaseFailure> {
  if ('code' in failure) {
    return publicData({
      agent: 'planner' as const,
      phase: 'planner_turn' as const,
      code: failure.code,
      ...(failure.committed === true && isCommittedAutoloopLedgerErrorCode(failure.code)
        ? { committed: true as const }
        : {}),
      retryable: publicAutoloopFailureRetryable(failure.code),
      ...(failure.pending_dispatch ? { pending_dispatch: failure.pending_dispatch } : {}),
      ...(failure.status_reason !== undefined ? { status_reason: failure.status_reason } : {}),
      error: failure.message,
      ...(detachedFailureId ? { detached_failure_id: detachedFailureId } : {}),
    });
  }
  return publicData({
    agent: 'planner' as const,
    phase: 'planner_turn' as const,
    error: failure.message,
    ...(detachedFailureId ? { detached_failure_id: detachedFailureId } : {}),
  });
}

function snapshotDurableDetachedPhaseFailure(
  value: unknown,
): Readonly<DurableDetachedAutoloopPhaseFailure> | undefined {
  if (typeof value !== 'object' || value === null) return undefined;
  if (
    ownDataValue(value, 'agent') !== 'planner' ||
    ownDataValue(value, 'phase') !== 'planner_turn' ||
    typeof ownDataValue(value, 'error') !== 'string'
  ) {
    return undefined;
  }
  const error = ownDataValue(value, 'error') as string;
  const codeValue = ownDataValue(value, 'code');
  const detachedFailureIdValue = ownDataValue(value, 'detached_failure_id');
  const detachedFailureId =
    typeof detachedFailureIdValue === 'string' && detachedFailureIdValue.length > 0
      ? detachedFailureIdValue
      : undefined;
  if (codeValue === undefined) {
    return publicData({
      agent: 'planner' as const,
      phase: 'planner_turn' as const,
      error,
      ...(detachedFailureId ? { detached_failure_id: detachedFailureId } : {}),
    });
  }
  if (!isPublicAutoloopFailureCode(codeValue)) return undefined;
  const committed = ownDataValue(value, 'committed') === true && isCommittedAutoloopLedgerErrorCode(codeValue);
  const pending = snapshotPendingDispatch(ownDataValue(value, 'pending_dispatch'));
  const statusReasonValue = ownDataValue(value, 'status_reason');
  const statusReason =
    typeof statusReasonValue === 'string' || statusReasonValue === null ? statusReasonValue : undefined;
  return publicData({
    agent: 'planner' as const,
    phase: 'planner_turn' as const,
    code: codeValue,
    ...(committed ? { committed: true as const } : {}),
    retryable: publicAutoloopFailureRetryable(codeValue),
    ...(pending ? { pending_dispatch: pending } : {}),
    ...(statusReason !== undefined ? { status_reason: statusReason } : {}),
    error,
    ...(detachedFailureId ? { detached_failure_id: detachedFailureId } : {}),
  });
}

function readDurableDetachedFailureRows(contents: string): DurableDetachedAutoloopFailureRow[] {
  const rows: DurableDetachedAutoloopFailureRow[] = [];
  let byteOffset = 0;
  const lines = contents.split('\n');
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const startByteOffset = byteOffset;
    const lineByteLength = Buffer.byteLength(line, 'utf8');
    byteOffset = startByteOffset + lineByteLength + (index < lines.length - 1 ? 1 : 0);
    if (!line.trim()) continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(line) as unknown;
    } catch {
      continue;
    }
    if (typeof parsed !== 'object' || parsed === null) continue;
    const row = parsed as Record<string, unknown>;
    if (ownDataValue(row, 'kind') !== 'phase_error' || ownDataValue(row, 'actor') !== 'dispatcher') continue;
    const payload = snapshotDurableDetachedPhaseFailure(ownDataValue(row, 'payload'));
    if (!payload) continue;
    const tsValue = ownDataValue(row, 'ts');
    rows.push(
      publicData({
        ts: typeof tsValue === 'string' ? tsValue : '',
        payload,
        startByteOffset,
      }),
    );
  }
  return rows;
}

function detachedFailureLedgerCursor(contents: string): Readonly<DetachedFailureLedgerCursor> {
  return publicData({
    version: 1 as const,
    byteOffset: Buffer.byteLength(contents, 'utf8'),
    prefixSha256: createHash('sha256').update(contents, 'utf8').digest('hex'),
  });
}

function snapshotDetachedFailureLedgerCursor(value: unknown): Readonly<DetachedFailureLedgerCursor> | undefined {
  if (typeof value !== 'object' || value === null) return undefined;
  const version = ownDataValue(value, 'version');
  const byteOffset = ownDataValue(value, 'byteOffset');
  const prefixSha256 = ownDataValue(value, 'prefixSha256');
  if (
    version !== 1 ||
    typeof byteOffset !== 'number' ||
    !Number.isSafeInteger(byteOffset) ||
    byteOffset < 0 ||
    typeof prefixSha256 !== 'string' ||
    !/^[a-f0-9]{64}$/.test(prefixSha256)
  ) {
    return undefined;
  }
  return publicData({ version, byteOffset, prefixSha256 });
}

function validatedDetachedFailureCursorOffset(contents: string, cursor: Readonly<DetachedFailureLedgerCursor>): number {
  const bytes = Buffer.from(contents, 'utf8');
  if (cursor.byteOffset > bytes.length || (cursor.byteOffset > 0 && bytes[cursor.byteOffset - 1] !== 0x0a)) {
    throw new Error('decisions.jsonl no longer contains the checkpointed detached-failure prefix boundary');
  }
  const prefixSha256 = createHash('sha256').update(bytes.subarray(0, cursor.byteOffset)).digest('hex');
  if (prefixSha256 !== cursor.prefixSha256) {
    throw new Error('decisions.jsonl no longer matches the checkpointed detached-failure prefix');
  }
  return cursor.byteOffset;
}

function detachedPhaseFailureKey(value: DetachedAutoloopPhaseFailure): string {
  return JSON.stringify(
    publicData({
      code: value.code ?? null,
      committed: value.code && value.committed === true && isCommittedAutoloopLedgerErrorCode(value.code) ? true : null,
      retryable: value.code ? publicAutoloopFailureRetryable(value.code) : null,
      pending_dispatch: value.pending_dispatch ?? null,
      status_reason: value.status_reason ?? null,
      error: value.error,
    }),
  );
}

function sameDetachedPhaseFailure(left: DetachedAutoloopPhaseFailure, right: DetachedAutoloopPhaseFailure): boolean {
  return detachedPhaseFailureKey(left) === detachedPhaseFailureKey(right);
}

function newlyAppendedDispatcherPreaudit(
  before: string,
  after: string,
  expected: DetachedAutoloopPhaseFailure,
): DurableDetachedAutoloopFailureRow | undefined {
  if (!after.startsWith(before)) return undefined;
  const appendedLines = after
    .slice(before.length)
    .split('\n')
    .filter((line) => line.trim().length > 0);
  const lastLine = appendedLines.at(-1);
  if (!lastLine) return undefined;
  const rows = readDurableDetachedFailureRows(`${lastLine}\n`);
  if (rows.length !== 1) return undefined;
  const row = rows[0];
  if (row.payload.detached_failure_id !== undefined || !sameDetachedPhaseFailure(row.payload, expected)) {
    return undefined;
  }
  return row;
}

function rowIsAfterCheckpoint(rowTimestamp: string, checkpointTimestamp: string): boolean {
  const rowTime = Date.parse(rowTimestamp);
  const checkpointTime = Date.parse(checkpointTimestamp);
  return Number.isFinite(rowTime) && Number.isFinite(checkpointTime) && rowTime > checkpointTime;
}

function detachedStateEntry(
  ts: string,
  payload: Readonly<DurableDetachedAutoloopPhaseFailure>,
  detachedFailureId?: string,
): AutoloopState['recent_phase_errors'][number] {
  const entry = Object.assign(Object.create(null), {
    ts,
    agent: payload.agent,
    phase: payload.phase,
    ...(payload.code ? { code: payload.code, retryable: publicAutoloopFailureRetryable(payload.code) } : {}),
    ...(payload.code && payload.committed === true && isCommittedAutoloopLedgerErrorCode(payload.code)
      ? { committed: true as const }
      : {}),
    ...(payload.pending_dispatch ? { pending_dispatch: payload.pending_dispatch } : {}),
    ...(payload.status_reason !== undefined ? { status_reason: payload.status_reason } : {}),
    error: payload.error,
  }) as AutoloopState['recent_phase_errors'][number];
  if (detachedFailureId) {
    Object.defineProperty(entry, DETACHED_AUTOLOOP_FAILURE_ID, { value: detachedFailureId });
  }
  return Object.freeze(entry);
}

function isSendTimeoutPayload(value: unknown): value is SendTimeoutPayload {
  if (typeof value !== 'object' || value === null) return false;
  const pending = value as Partial<SendTimeoutPayload>;
  return (
    pending.status === 'awaiting_resume' &&
    typeof pending.dispatch_id === 'string' &&
    pending.dispatch_id.length > 0 &&
    (pending.agent === 'planner' || pending.agent === 'coder' || pending.agent === 'reviewer') &&
    typeof pending.message_id === 'string' &&
    typeof pending.message_type === 'string' &&
    typeof pending.iter === 'number' &&
    typeof pending.timeout_ms === 'number' &&
    typeof pending.error === 'string'
  );
}

interface StoredTimeoutObservation {
  ts: string;
  payload: SendTimeoutPayload;
}

/** Legacy timeout rows have no owner tuple. Only an unambiguous, durable
 * release/start interval can prove that a later baseline timeout belongs to a
 * replacement owner. A timestamp or a reset request alone is not authority.
 */
function provesTimeoutGenerationReset(
  ledger: SecureAutoloopLedger,
  runId: string,
  previous: StoredTimeoutObservation | undefined,
  next: StoredTimeoutObservation,
): boolean {
  if (!previous) return false;
  const earlier = Date.parse(previous.ts),
    later = Date.parse(next.ts);
  if (!Number.isFinite(earlier) || !Number.isFinite(later) || earlier >= later) return false;
  const sameIdentity = (a: PhysicalAgentGeneration, b: PhysicalAgentGeneration) =>
    a.role === b.role &&
    a.generation === b.generation &&
    a.session_name === b.session_name &&
    a.owner_instance_id === b.owner_instance_id &&
    a.session_id === b.session_id &&
    a.created_at === b.created_at;
  const history: Array<{ ts: number; kind: string; generation: PhysicalAgentGeneration }> = [];
  const current = new Map<string, (typeof history)[number]>();
  let lastTime = -Infinity;
  for (const line of (ledger.readFlatFile('agent-generations.jsonl') ?? '').split('\n')) {
    if (!line.trim()) continue;
    let row;
    try {
      row = JSON.parse(line);
    } catch {
      return false;
    }
    const g = row?.payload as PhysicalAgentGeneration | undefined;
    const ts = Date.parse(row?.ts);
    if (
      row?.schema_version !== 1 ||
      !g ||
      !['planner', 'coder', 'reviewer'].includes(g.role) ||
      !Number.isSafeInteger(g.generation) ||
      g.generation < 1 ||
      g.session_name !== `autoloop-${runId}-${g.role}` ||
      !isRecoverableAgentOwnerInstanceId(g.owner_instance_id) ||
      typeof g.session_id !== 'string' ||
      !g.session_id ||
      !Number.isFinite(Date.parse(g.created_at)) ||
      !Number.isFinite(ts) ||
      Date.parse(g.created_at) > ts ||
      ts < lastTime
    )
      return false;
    const prior = current.get(g.role);
    const kind = row.kind;
    if (kind === 'agent_generation_reserved') {
      if (
        g.state !== 'stale' ||
        (prior
          ? prior.kind !== 'agent_generation_released' ||
            g.generation !== prior.generation.generation + 1 ||
            g.session_id === prior.generation.session_id
          : g.generation !== 1)
      )
        return false;
    } else {
      if (!prior || !sameIdentity(prior.generation, g)) return false;
      if (kind === 'agent_generation_started') {
        if (prior.kind !== 'agent_generation_reserved' || g.state !== 'live') return false;
      } else if (kind === 'agent_generation_lease_renewed') {
        if (!['agent_generation_started', 'agent_generation_lease_renewed'].includes(prior.kind) || g.state !== 'live')
          return false;
      } else if (kind === 'agent_generation_orphaned') {
        if (prior.kind === 'agent_generation_released' || g.state !== 'orphaned') return false;
      } else if (kind === 'agent_generation_released') {
        if (g.state !== 'released') return false;
      } else return false;
    }
    const event = { ts, kind, generation: g };
    history.push(event);
    current.set(g.role, event);
    lastTime = ts;
  }
  const activeAt = (role: string, at: number) => {
    const event = history.filter((entry) => entry.generation.role === role && entry.ts <= at).at(-1);
    // Equal cross-ledger timestamps cannot prove which event came first.
    return event && event.ts < at && event.generation.state === 'live' ? event.generation : undefined;
  };
  const oldOwner = activeAt(previous.payload.agent, earlier);
  const newOwner = activeAt(next.payload.agent, later);
  return Boolean(
    oldOwner &&
    newOwner &&
    oldOwner.owner_instance_id !== newOwner.owner_instance_id &&
    history.some(
      (event) =>
        event.kind === 'agent_generation_released' &&
        sameIdentity(event.generation, oldOwner) &&
        event.ts > earlier &&
        event.ts < Date.parse(newOwner.created_at),
    ),
  );
}

/**
 * Replay only timeout-resume audit rows. The original run spec remains the
 * immutable starting point; each coherent append advances the effective value.
 * Failing closed on a malformed migration prevents a corrupt audit tail from
 * accidentally authorizing a timeout decrease after process reconstruction.
 */
function readStoredAutoloopResumeContext(
  ledger: SecureAutoloopLedger,
  runId: string,
  originalSendTimeoutMs: unknown,
): StoredAutoloopResumeContext {
  validateAutoloopTimeoutConfig({ sendTimeoutMs: originalSendTimeoutMs as number | undefined });
  let effectiveSendTimeoutMs = (originalSendTimeoutMs as number | undefined) ?? DEFAULT_SEND_TIMEOUT_MS;
  let pendingDispatch: SendTimeoutPayload | null = null;
  let pendingObservation: StoredTimeoutObservation | undefined;
  let previousMigratedTimeout: StoredTimeoutObservation | undefined;
  let generationReset = false;
  const auditContents = ledger.readFlatFile('decisions.jsonl') ?? '';

  const lines = auditContents.split('\n');
  for (const line of lines) {
    if (!line.trim()) continue;
    let row: Record<string, unknown>;
    try {
      row = JSON.parse(line) as Record<string, unknown>;
    } catch {
      throw new Error(`Cannot safely resume Autoloop '${runId}': decisions.jsonl contains malformed JSON`);
    }
    if (row.kind === 'send_timeout' && isSendTimeoutPayload(row.payload)) {
      const observation = { ts: String(row.ts), payload: row.payload };
      if (row.payload.timeout_ms < effectiveSendTimeoutMs) {
        if (
          row.payload.timeout_ms !== (originalSendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS) ||
          !provesTimeoutGenerationReset(ledger, runId, previousMigratedTimeout, observation)
        ) {
          throw new Error(`Cannot safely resume Autoloop '${runId}': timeout generation reset lacks durable proof`);
        }
        effectiveSendTimeoutMs = row.payload.timeout_ms;
        generationReset = true;
      }
      pendingDispatch = row.payload;
      pendingObservation = observation;
      continue;
    }
    if (row.kind === 'terminate') {
      pendingDispatch = null;
      pendingObservation = undefined;
      generationReset = false;
      continue;
    }
    if (row.kind !== 'timeout_migration' || row.runId !== runId || row.field !== 'sendTimeoutMs') continue;
    const oldValue = row.oldValue;
    const newValue = row.newValue;
    try {
      validateAutoloopTimeoutConfig({ sendTimeoutMs: oldValue as number });
      validateAutoloopTimeoutConfig({ sendTimeoutMs: newValue as number });
    } catch {
      throw new Error(`Cannot safely resume Autoloop '${runId}': timeout migration audit is invalid`);
    }
    if (
      oldValue !== effectiveSendTimeoutMs ||
      (newValue as number) <= (oldValue as number) ||
      (generationReset &&
        (!pendingDispatch ||
          row.pendingDispatchId !== pendingDispatch.dispatch_id ||
          pendingDispatch.timeout_ms !== oldValue))
    ) {
      throw new Error(`Cannot safely resume Autoloop '${runId}': timeout migration audit chain is inconsistent`);
    }
    effectiveSendTimeoutMs = newValue as number;
    if (pendingDispatch && row.pendingDispatchId === pendingDispatch.dispatch_id) {
      previousMigratedTimeout = pendingObservation;
      pendingDispatch = null;
      pendingObservation = undefined;
    }
    generationReset = false;
  }
  return { effectiveSendTimeoutMs, pendingDispatch };
}

function validateSendTimeoutIncrease(value: unknown, current: number): asserts value is number {
  validateAutoloopTimeoutConfig({ sendTimeoutMs: value as number });
  if ((value as number) <= current) {
    throw new Error(`sendTimeoutMs must be strictly greater than the current effective value ${current}`);
  }
}

function encodeSendTimeoutMigration(
  migration: Omit<SendTimeoutMigrationAuditRecord, 'ts' | 'timestamp' | 'kind' | 'actor'>,
): string {
  const timestamp = new Date().toISOString();
  const record: SendTimeoutMigrationAuditRecord = {
    schema_version: 1,
    ts: timestamp,
    kind: 'timeout_migration',
    actor: 'operator',
    timestamp,
    ...migration,
  };
  return `${JSON.stringify(record)}\n`;
}

function appendSendTimeoutMigration(
  ledger: SecureAutoloopLedger,
  migration: Omit<SendTimeoutMigrationAuditRecord, 'ts' | 'timestamp' | 'kind' | 'actor'>,
): SecureAutoloopLedgerCommitError | undefined {
  const prepared = prepareSendTimeoutMigrationAppend(ledger, migration);
  try {
    return commitPreparedSendTimeoutMigration(prepared);
  } finally {
    prepared.append.close();
  }
}

/**
 * Hold an append-capable descriptor before a stored run is restarted. Opening
 * it is the fallible permission/path part of the append; doing that first keeps
 * an unavailable audit ledger from starting agents or changing kernel state.
 * The descriptor stays open across startup so the eventual commit cannot be
 * redirected by a path replacement.
 */
function prepareSendTimeoutMigrationAppend(
  ledger: SecureAutoloopLedger,
  migration: Omit<SendTimeoutMigrationAuditRecord, 'ts' | 'timestamp' | 'kind' | 'actor'>,
): PreparedSendTimeoutMigrationAppend {
  const encoded = encodeSendTimeoutMigration(migration);
  return {
    append: ledger.prepareFlatFileAppend('decisions.jsonl', encoded),
    expectedTail: encoded.slice(0, -1),
  };
}

function assertPreparedSendTimeoutMigrationTail(prepared: PreparedSendTimeoutMigrationAppend): void {
  if (prepared.append.readLastNonEmptyLine() !== prepared.expectedTail) {
    throw new Error('Committed timeout migration does not match the pinned decisions.jsonl tail');
  }
}

function commitPreparedSendTimeoutMigration(
  prepared: PreparedSendTimeoutMigrationAppend,
): SecureAutoloopLedgerCommitError | undefined {
  try {
    prepared.append.commitDurable();
  } catch (error) {
    if (!isCommittedSecureLedgerError(error) || !prepared.append.committed) throw error;
    assertPreparedSendTimeoutMigrationTail(prepared);
    try {
      // Resume only the incomplete barrier. The prepared capability remembers
      // that its bytes are already present, so this can never append twice.
      prepared.append.commitDurable();
    } catch (retryError) {
      if (!isCommittedSecureLedgerError(retryError) || !prepared.append.committed) throw retryError;
      assertPreparedSendTimeoutMigrationTail(prepared);
      return retryError;
    }
  }
  assertPreparedSendTimeoutMigrationTail(prepared);
  return undefined;
}

function isStringRecord(value: unknown): value is Record<string, string> {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value) &&
    Object.values(value).every((entry) => typeof entry === 'string')
  );
}

function validateAutoloopCustomEngine(role: AutoloopRoleName, config: CustomEngineConfig): void {
  const label = role[0].toUpperCase() + role.slice(1);
  const raw = config as unknown as Record<string, unknown>;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new Error(`${label} custom engine config must be an object`);
  }
  if (typeof raw.name !== 'string' || !raw.name.trim()) {
    throw new Error(`${label} custom engine config.name must be a non-empty string`);
  }
  if (typeof raw.bin !== 'string' || !raw.bin.trim()) {
    throw new Error(`${label} custom engine config.bin must be a non-empty string`);
  }
  if (typeof raw.args !== 'object' || raw.args === null || Array.isArray(raw.args)) {
    throw new Error(`${label} custom engine config.args must be an object`);
  }
  for (const [key, value] of Object.entries(raw.args)) {
    if (value === undefined) continue;
    if (key === 'extra') {
      if (!Array.isArray(value) || value.some((entry) => typeof entry !== 'string')) {
        throw new Error(`${label} custom engine config.args.extra must be an array of strings`);
      }
    } else if (typeof value !== 'string') {
      throw new Error(`${label} custom engine config.args.${key} must be a string`);
    }
  }
  if (raw.persistent !== undefined && typeof raw.persistent !== 'boolean') {
    throw new Error(`${label} custom engine config.persistent must be a boolean`);
  }
  if (raw.env !== undefined && !isStringRecord(raw.env)) {
    throw new Error(`${label} custom engine config.env must contain only string values`);
  }
  if (raw.permissionModes !== undefined && !isStringRecord(raw.permissionModes)) {
    throw new Error(`${label} custom engine config.permissionModes must contain only string values`);
  }
  if (
    raw.sanitizePatterns !== undefined &&
    (!Array.isArray(raw.sanitizePatterns) || raw.sanitizePatterns.some((entry) => typeof entry !== 'string'))
  ) {
    throw new Error(`${label} custom engine config.sanitizePatterns must be an array of strings`);
  }
}

function validateAutoloopRole(
  role: AutoloopRoleName,
  engine: EngineType | undefined,
  customEngine: CustomEngineConfig | undefined,
): EngineType {
  const resolved = engine ?? 'claude';
  const label = role[0].toUpperCase() + role.slice(1);
  if (!ENGINE_TYPES.includes(resolved)) {
    throw new Error(`${label} engine '${String(resolved)}' is not supported`);
  }
  if (resolved === 'custom') {
    if (!customEngine) throw new Error(`${label} custom engine config is required`);
    validateAutoloopCustomEngine(role, customEngine);
  }
  return resolved;
}

/**
 * The tool calls a turn's `result` event says the engine refused, normalized.
 *
 * Read defensively: the field is Claude Code's, and a persistent `custom`
 * engine emits a result event of its own shape, so anything that is not an
 * array of objects naming a tool is ignored rather than trusted.
 */
function readPermissionDenials(evt: Record<string, unknown> | undefined): PermissionDenial[] {
  const raw = evt?.permission_denials;
  if (!Array.isArray(raw)) return [];
  const out: PermissionDenial[] = [];
  for (const d of raw) {
    if (!d || typeof d !== 'object') continue;
    const r = d as Record<string, unknown>;
    if (typeof r.tool_name !== 'string') continue;
    out.push({
      toolName: r.tool_name,
      ...(typeof r.tool_use_id === 'string' ? { toolUseId: r.tool_use_id } : {}),
      ...('tool_input' in r ? { input: r.tool_input } : {}),
    });
  }
  return out;
}

export class SessionManager implements AgentRuntimeProbe {
  private static liveAutoloopOwnerInstanceIds = new Set<string>();
  private sessions = new Map<string, ManagedSession>();
  private _pendingSessions = new Map<string, Promise<SessionInfo>>();
  readonly autoloopOwnerInstanceId = `session-manager:${process.pid}:${randomUUID()}`;
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;
  private pluginConfig: PluginConfig;
  private persistedSessions: Map<string, PersistedSession>;
  private _debouncedSave: DebouncedCallback;
  private _proxyServer: http.Server | null = null;
  private _proxyPort: number | null = null;
  /** In-flight proxy startup, so concurrent callers share one server. */
  private _proxyStartPromise: Promise<number | null> | null = null;
  private _activePids = new Map<string, number>();
  private _agentReleasesInFlight = 0;
  private _agentReleaseWaiters: Array<() => void> = [];
  private _agentReleaseOperations = new Map<string, Promise<boolean>>();
  private _agentReleaseFenceClosed = false;
  private _shutdownPromise: Promise<void> | null = null;
  private _completedBeforeReleaseHooks = new Set<string>();
  private _completedReleaseEvidenceHooks = new Set<string>();
  private _circuitBreaker = new CircuitBreaker();
  private _inbox = new InboxManager();
  /** cwd → detected language, so the manifest probe runs once per directory. */
  private _repoLangCache = new Map<string, string | undefined>();
  private logger: Logger;
  private _ultraappManager: UltraappManager | null = null;
  private _ultraappRouter: UltraappRouter | null = null;
  private _ultraappRuntimeMode: 'host' | 'docker' = 'host';

  constructor(config?: Partial<PluginConfig>, logger?: Logger) {
    this.logger = logger || createConsoleLogger('SessionManager');
    this.pluginConfig = {
      claudeBin: config?.claudeBin || 'claude',
      defaultModel: config?.defaultModel,
      defaultPermissionMode: config?.defaultPermissionMode || 'acceptEdits',
      defaultEffort: config?.defaultEffort || 'auto',
      maxConcurrentSessions: config?.maxConcurrentSessions || 5,
      sessionTtlMinutes: config?.sessionTtlMinutes || 120,
    };

    // Apply pricing overrides if provided
    if (config?.pricingOverrides) {
      overrideModelPricing(config.pricingOverrides);
    }

    // Load persisted session registry from disk
    this.persistedSessions = loadPersistedSessions();
    SessionManager.liveAutoloopOwnerInstanceIds.add(this.autoloopOwnerInstanceId);
    // Clean up orphaned child processes from a previous unclean exit
    this._cleanupOrphanedPids();
    // Debounced writer — at most one write per 5 seconds on hot paths. The
    // eventual write still enters the shared registry lock so it cannot race a
    // generation transition from another process.
    this._debouncedSave = makeDebounced(() => this._persistRegistrySnapshot(), DEBOUNCED_SAVE_MS);

    // Start TTL cleanup timer
    this.cleanupTimer = setInterval(() => this._cleanupIdleSessions(), CLEANUP_INTERVAL_MS);
  }

  /**
   * Lazily-constructed ultraapp manager. The ultraapp manager itself uses
   * `this` as its session-manager dependency; building it lazily avoids any
   * circular initialisation concerns.
   */
  getUltraappManager(): UltraappManager {
    if (!this._ultraappManager) {
      this._ultraappManager = new UltraappManager({
        store: new UltraappStore(defaultStoreRoot()),
        sessionManager: this,
        router: this._ultraappRouter ?? undefined,
        runtimeMode: this._ultraappRuntimeMode,
        // The same kernel every other mode runs on, so an ultraapp build is a
        // run like any other: listed by `workflow_list`, visible in the Runs
        // tab, owned by one process, and resumable at a node boundary.
        kernel: this.kernel,
      });
    }
    return this._ultraappManager;
  }

  /**
   * Inject a started UltraappRouter so deploy + lifecycle wiring becomes
   * available. Must be called BEFORE the first `getUltraappManager()` call —
   * the manager is constructed lazily and reads the router reference at that
   * point. Production: bin/cli.ts wires this. Tests: leave unset to keep
   * v0.2-style "build-complete is resting state" behaviour.
   */
  setUltraappRouter(router: UltraappRouter): void {
    if (this._ultraappManager) {
      throw new Error('setUltraappRouter must be called before getUltraappManager');
    }
    this._ultraappRouter = router;
  }

  /**
   * Pick the ultraapp runtime mode. 'host' (default) spawns the generated
   * app as a regular Node process — works anywhere Node works, no Docker
   * required. 'docker' uses `docker build` + `docker run` for isolation,
   * intended for shared production hosts. Must be called before the first
   * `getUltraappManager()` call.
   */
  setUltraappRuntimeMode(mode: 'host' | 'docker'): void {
    if (this._ultraappManager) {
      throw new Error('setUltraappRuntimeMode must be called before getUltraappManager');
    }
    this._ultraappRuntimeMode = mode;
  }

  // ─── Session Lifecycle ─────────────────────────────────────────────────

  private _syncPersistedSessions(authoritative: Map<string, PersistedSession>): void {
    this.persistedSessions.clear();
    for (const [name, session] of authoritative) this.persistedSessions.set(name, session);
  }

  private _withAgentRegistryLock<T>(
    operation: (authoritative: Map<string, PersistedSession>) => {
      value: T;
      updatedSessions?: Map<string, PersistedSession>;
    },
  ): { ok: true; value: T } | { ok: false; error: AutoloopAgentRegistryError } {
    const localBefore = new Map(this.persistedSessions);
    let visibleSessions: Map<string, PersistedSession> | undefined;
    let locked: ReturnType<typeof withFileLock<{ persistError?: AutoloopAgentRegistryError; value: T }>>;
    try {
      locked = withFileLock(
        PERSIST_LOCK_FILE,
        () => {
          const authoritative = loadPersistedSessions();
          const result = operation(authoritative);
          const updated = result.updatedSessions;
          if (updated) {
            const persisted = savePersistedSessions(updated, this.logger);
            if (!persisted.ok) {
              visibleSessions = authoritative;
              return { persistError: persisted.error, value: result.value };
            }
          }
          visibleSessions = updated ?? authoritative;
          return { value: result.value };
        },
        { createParent: true },
      );
    } catch (err) {
      if (err instanceof AutoloopAgentRegistryError) return { ok: false, error: err };
      if (isFileLockReleaseError(err)) {
        return {
          ok: false,
          error: new AutoloopAgentRegistryError(
            'AUTOLOOP_AGENT_REGISTRY_LOCK_CLEANUP_FAILED',
            `The session registry write may have committed, but its lock could not be safely released: ${err.message}`,
            { cause: err },
          ),
        };
      }
      throw err;
    }
    if (!locked.ok) {
      if (locked.reason === 'cleanup_failed') {
        return {
          ok: false,
          error: new AutoloopAgentRegistryError(
            'AUTOLOOP_AGENT_REGISTRY_LOCK_CLEANUP_FAILED',
            `Could not safely clean up the shared session registry lock: ${locked.error}`,
            { cause: locked.cause },
          ),
        };
      }
      return {
        ok: false,
        error: new AutoloopAgentRegistryError(
          'AUTOLOOP_AGENT_REGISTRY_LOCK_CONTENDED',
          `Could not enter the shared session registry lock: ${locked.error}`,
        ),
      };
    }
    this._syncPersistedSessions(mergeRegistryView(visibleSessions!, localBefore));
    if (locked.value.persistError) return { ok: false, error: locked.value.persistError };
    return { ok: true, value: locked.value.value };
  }

  private _persistRegistrySnapshot(): boolean {
    const desired = new Map(this.persistedSessions);
    const transaction = this._withAgentRegistryLock((authoritative) => ({
      value: true,
      updatedSessions: mergeRegistrySnapshot(authoritative, desired),
    }));
    if (!transaction.ok) {
      this.logger.warn('Failed to persist sessions:', transaction.error.message);
      return false;
    }
    return transaction.value;
  }

  private _finishAgentRelease(): void {
    this._agentReleasesInFlight -= 1;
    if (this._agentReleasesInFlight !== 0) return;
    const waiters = this._agentReleaseWaiters.splice(0);
    for (const resolve of waiters) resolve();
  }

  private async _waitForAgentReleases(): Promise<void> {
    if (this._agentReleasesInFlight === 0) return;
    await new Promise<void>((resolve) => this._agentReleaseWaiters.push(resolve));
  }

  private _agentReleaseOperationKey(
    sessionName: string,
    expectedGeneration: number,
    options: Exclude<AgentReservationReleaseOptions, { rollbackUncommittedReservation: true }>,
  ): string {
    return [
      sessionName,
      expectedGeneration,
      options.expectedOwnerInstanceId,
      options.expectedSessionId ?? '',
      options.releaseOwnerInstanceId,
    ].join('\0');
  }

  /**
   * Atomically reserve an Autoloop physical name for one durable generation.
   * The existing session registry is the reservation store; no parallel
   * database is introduced.
   */
  reserveAgentGeneration(generation: PhysicalAgentGeneration, cwd: string): boolean {
    if (
      !Number.isInteger(generation.generation) ||
      generation.generation < 1 ||
      generation.session_name.length === 0 ||
      generation.owner_instance_id.length === 0 ||
      !generation.session_id
    ) {
      return false;
    }

    const transaction = this._withAgentRegistryLock((authoritative) => {
      const existing = authoritative.get(generation.session_name);
      if (existing?.agentReleasePending) return { value: false };
      if (existing?.agentGeneration !== undefined) {
        return {
          value:
            existing.agentGeneration === generation.generation &&
            existing.agentOwnerInstanceId === generation.owner_instance_id &&
            existing.agentSessionId === generation.session_id,
        };
      }
      if (existing && existing.agentReleasedGeneration === undefined) {
        // Legacy registry-only entries must be explicitly released as
        // generation zero before they can become a fenced reservation.
        return { value: false };
      }
      if (
        existing?.agentReleasedGeneration !== undefined &&
        generation.generation !== existing.agentReleasedGeneration + 1
      ) {
        return { value: false };
      }

      const observedAt = Date.parse(generation.last_activity_at);
      const updatedSessions = new Map(authoritative);
      updatedSessions.set(generation.session_name, {
        name: generation.session_name,
        claudeSessionId: existing?.claudeSessionId ?? '',
        cwd: existing?.cwd ?? cwd,
        model: existing?.model,
        engine: existing?.engine,
        sandboxMode: existing?.sandboxMode,
        originalCreated: existing?.originalCreated ?? generation.created_at,
        lastResumed: existing?.lastResumed ?? generation.created_at,
        lastActivity: Number.isNaN(observedAt) ? Date.now() : observedAt,
        agentGeneration: generation.generation,
        agentOwnerInstanceId: generation.owner_instance_id,
        agentSessionId: generation.session_id,
        agentReleasePending: undefined,
        agentReleaseOwnerInstanceId: undefined,
        agentReleasedGeneration: existing?.agentReleasedGeneration,
        agentReleasedOwnerInstanceId: existing?.agentReleasedOwnerInstanceId,
        agentReleasedSessionId: existing?.agentReleasedSessionId,
      });
      return { value: true, updatedSessions };
    });
    if (!transaction.ok) throw transaction.error;
    return transaction.value;
  }

  /**
   * Atomically prove that a physical name is reusable without reserving it.
   * This is the same authoritative predicate `reserveAgentGeneration` uses,
   * but it performs no registry write, so a failed probe cannot strand a new
   * occupied reservation or a rollback-pending fence.
   */
  probeAgentNameReusable(sessionName: string, released?: PhysicalAgentGeneration): boolean {
    if (this.sessions.has(sessionName) || this._pendingSessions.has(sessionName)) return false;
    const transaction = this._withAgentRegistryLock((authoritative) => {
      const reservation = authoritative.get(sessionName);
      if (!released) {
        return {
          value:
            reservation === undefined ||
            (reservation.agentGeneration === undefined &&
              reservation.agentReleasePending !== true &&
              reservation.agentReleasedGeneration !== undefined),
        };
      }
      return {
        value: Boolean(
          reservation &&
          reservation.agentGeneration === undefined &&
          reservation.agentReleasePending !== true &&
          reservation.agentReleasedGeneration === released.generation &&
          reservation.agentReleasedOwnerInstanceId === released.owner_instance_id &&
          reservation.agentReleasedSessionId === released.session_id,
        ),
      };
    });
    if (!transaction.ok) throw transaction.error;
    return transaction.value;
  }

  /** Backward-compatible exact-tombstone predicate. */
  isAgentGenerationReleased(generation: PhysicalAgentGeneration): boolean {
    return this.probeAgentNameReusable(generation.session_name, generation);
  }

  /** Inspect only runtime/session-registry facts for one physical name. */
  async inspect(sessionName: string, sessionId?: string): Promise<AgentRuntimeLiveness> {
    if (this.sessions.has(sessionName)) return 'live';
    if (this._pendingSessions.has(sessionName)) return 'unknown';

    const reservation = this.persistedSessions.get(sessionName);
    if (reservation?.agentSessionId && sessionId && reservation.agentSessionId !== sessionId) {
      return 'unknown';
    }
    return this._inspectSharedPidEvidence(sessionName) ?? 'absent';
  }

  private _inspectSharedPidEvidence(sessionName: string): AgentRuntimeLiveness | undefined {
    if (!fs.existsSync(SessionManager.PID_FILE)) return undefined;

    let entries: Record<string, unknown>;
    try {
      const parsed = JSON.parse(fs.readFileSync(SessionManager.PID_FILE, 'utf8')) as unknown;
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return 'unknown';
      entries = parsed as Record<string, unknown>;
    } catch {
      return 'unknown';
    }
    if (!Object.prototype.hasOwnProperty.call(entries, sessionName)) return undefined;

    const raw = entries[sessionName];
    if (typeof raw === 'number') return 'unknown';
    if (!raw || typeof raw !== 'object') return 'unknown';
    const entry = raw as { pid?: unknown; ownerPid?: unknown };
    if (
      typeof entry.pid !== 'number' ||
      !Number.isInteger(entry.pid) ||
      entry.pid <= 0 ||
      typeof entry.ownerPid !== 'number' ||
      !Number.isInteger(entry.ownerPid) ||
      entry.ownerPid <= 0
    ) {
      return 'unknown';
    }

    if (entry.ownerPid === process.pid) {
      const locallyOwnedPid = this._activePids.get(sessionName);
      if (locallyOwnedPid !== undefined) {
        if (locallyOwnedPid !== entry.pid) return 'unknown';
        return this._probePidLiveness(entry.pid);
      }
      // A host-shared file can retain an entry written by an earlier manager
      // instance in this same process. The process being alive is not proof
      // that this manager still owns the child.
      return this._probePidLiveness(entry.pid) === 'absent' ? 'absent' : 'unknown';
    }

    const ownerLiveness = this._probePidLiveness(entry.ownerPid);
    if (ownerLiveness === 'live') return 'live';
    if (ownerLiveness === 'unknown') return 'unknown';
    return this._probePidLiveness(entry.pid) === 'absent' ? 'absent' : 'unknown';
  }

  private _probePidLiveness(pid: number): AgentRuntimeLiveness {
    try {
      process.kill(pid, 0);
      return 'live';
    } catch (err) {
      return (err as { code?: string }).code === 'ESRCH' ? 'absent' : 'unknown';
    }
  }

  private _inspectReleaseOwner(ownerInstanceId: string): AgentRuntimeLiveness {
    if (SessionManager.liveAutoloopOwnerInstanceIds.has(ownerInstanceId)) return 'live';
    const match = /^session-manager:(\d+):[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.exec(ownerInstanceId);
    if (!match) return 'unknown';
    const ownerPid = Number(match[1]);
    if (!Number.isSafeInteger(ownerPid) || ownerPid <= 0) return 'unknown';
    // A same-process owner absent from the local live-owner set completed
    // shutdown. Merely probing our own process would otherwise make the stale
    // owner look live forever.
    if (ownerPid === process.pid) return 'absent';
    return this._probePidLiveness(ownerPid);
  }

  /**
   * Compare-and-release a name reservation. Active or in-flight sessions are
   * never released, and a stale generation cannot release its replacement.
   */
  async releaseReservation(
    sessionName: string,
    expectedGeneration: number,
    options: AgentReservationReleaseOptions,
  ): Promise<boolean> {
    if (!options.rollbackUncommittedReservation && !isRecoverableAgentOwnerInstanceId(options.releaseOwnerInstanceId)) {
      throw new AutoloopAgentReleaseOwnerError(options.releaseOwnerInstanceId);
    }
    if (this.sessions.has(sessionName) || this._pendingSessions.has(sessionName)) return false;

    if (options.rollbackUncommittedReservation) {
      if (this._agentReleaseFenceClosed) return false;
      const rollback = this._withAgentRegistryLock((authoritative) => {
        const existing = authoritative.get(sessionName);
        if (
          !existing ||
          existing.agentReleasePending ||
          existing.agentGeneration !== expectedGeneration ||
          options.expectedOwnerInstanceId === undefined ||
          existing.agentOwnerInstanceId !== options.expectedOwnerInstanceId ||
          options.expectedSessionId === undefined ||
          existing.agentSessionId !== options.expectedSessionId
        ) {
          return { value: false };
        }
        const updatedSessions = new Map(authoritative);
        if (existing.agentReleasedGeneration === undefined) {
          updatedSessions.delete(sessionName);
        } else {
          updatedSessions.set(sessionName, {
            ...existing,
            agentGeneration: undefined,
            agentOwnerInstanceId: undefined,
            agentSessionId: undefined,
            agentReleasePending: undefined,
            agentReleaseOwnerInstanceId: undefined,
          });
        }
        return { value: true, updatedSessions };
      });
      if (!rollback.ok) throw rollback.error;
      return rollback.value;
    }

    const releaseOperationKey = this._agentReleaseOperationKey(sessionName, expectedGeneration, options);
    const activeRelease = this._agentReleaseOperations.get(releaseOperationKey);
    if (activeRelease) return await activeRelease;
    if (this._agentReleaseFenceClosed) return false;

    const activeTupleMatches = (reservation: PersistedSession): boolean =>
      reservation.agentGeneration === expectedGeneration &&
      reservation.agentOwnerInstanceId === options.expectedOwnerInstanceId &&
      reservation.agentSessionId === options.expectedSessionId;
    const claim = this._withAgentRegistryLock((authoritative) => {
      const existing = authoritative.get(sessionName);
      if (!existing) return { value: 'rejected' as const };

      if (
        existing.agentGeneration === undefined &&
        existing.agentReleasePending !== true &&
        existing.agentReleasedGeneration !== undefined
      ) {
        if (existing.agentReleasedGeneration !== expectedGeneration) {
          return { value: 'rejected' as const };
        }
        if (
          (existing.agentReleasedOwnerInstanceId !== undefined &&
            existing.agentReleasedOwnerInstanceId !== options.expectedOwnerInstanceId) ||
          (existing.agentReleasedSessionId !== undefined &&
            existing.agentReleasedSessionId !== options.expectedSessionId)
        ) {
          return { value: 'rejected' as const };
        }
        return { value: 'idempotent' as const };
      }

      if (existing.agentReleasePending) {
        if (!activeTupleMatches(existing) || !options.releaseOwnerInstanceId) {
          return { value: 'rejected' as const };
        }
        const priorReleaseOwner = existing.agentReleaseOwnerInstanceId;
        if (priorReleaseOwner === options.releaseOwnerInstanceId) return { value: 'claimed' as const };
        if (priorReleaseOwner !== undefined && this._inspectReleaseOwner(priorReleaseOwner) !== 'absent') {
          return { value: 'rejected' as const };
        }
        const updatedSessions = new Map(authoritative);
        updatedSessions.set(sessionName, {
          ...existing,
          agentReleaseOwnerInstanceId: options.releaseOwnerInstanceId,
        });
        return { value: 'claimed' as const, updatedSessions };
      }

      if (existing.agentGeneration !== undefined) {
        if (!activeTupleMatches(existing) || !options.releaseOwnerInstanceId) {
          return { value: 'rejected' as const };
        }
        const updatedSessions = new Map(authoritative);
        updatedSessions.set(sessionName, {
          ...existing,
          agentReleasePending: true,
          agentReleaseOwnerInstanceId: options.releaseOwnerInstanceId,
        });
        return { value: 'claimed' as const, updatedSessions };
      }

      // A pre-generation registry entry is fenced as generation zero instead
      // of being deleted, so a crash cannot expose its name between evidence
      // writes.
      if (
        expectedGeneration !== 0 ||
        options.expectedOwnerInstanceId !== 'legacy-registry' ||
        !Object.prototype.hasOwnProperty.call(options, 'expectedSessionId') ||
        !options.releaseOwnerInstanceId
      ) {
        return { value: 'rejected' as const };
      }
      const updatedSessions = new Map(authoritative);
      updatedSessions.set(sessionName, {
        ...existing,
        agentGeneration: 0,
        agentOwnerInstanceId: options.expectedOwnerInstanceId,
        agentSessionId: options.expectedSessionId,
        agentReleasePending: true,
        agentReleaseOwnerInstanceId: options.releaseOwnerInstanceId,
      });
      return { value: 'claimed' as const, updatedSessions };
    });
    if (!claim.ok) throw claim.error;
    if (claim.value === 'rejected') return false;
    if (claim.value === 'idempotent') return true;

    this._agentReleasesInFlight += 1;
    let releaseOperation: Promise<boolean>;
    try {
      releaseOperation = Promise.resolve().then(() => {
        try {
          if (options.beforeRelease && !this._completedBeforeReleaseHooks.has(releaseOperationKey)) {
            options.beforeRelease();
            this._completedBeforeReleaseHooks.add(releaseOperationKey);
          }

          // Returning false without this hook deliberately leaves the durable
          // tombstone in place. A caller may retry with the evidence writer, but may
          // not make the physical name reusable without it.
          if (!options.persistReleaseEvidence) return false;
          if (!this._completedReleaseEvidenceHooks.has(releaseOperationKey)) {
            options.persistReleaseEvidence();
            this._completedReleaseEvidenceHooks.add(releaseOperationKey);
          }

          const completion = this._withAgentRegistryLock((authoritative) => {
            const stillPending = authoritative.get(sessionName);
            if (
              !stillPending?.agentReleasePending ||
              !activeTupleMatches(stillPending) ||
              stillPending.agentReleaseOwnerInstanceId !== options.releaseOwnerInstanceId
            ) {
              return { value: false };
            }
            const updatedSessions = new Map(authoritative);
            updatedSessions.set(sessionName, {
              ...stillPending,
              agentGeneration: undefined,
              agentOwnerInstanceId: undefined,
              agentSessionId: undefined,
              agentReleasePending: undefined,
              agentReleaseOwnerInstanceId: undefined,
              agentReleasedGeneration: expectedGeneration,
              agentReleasedOwnerInstanceId: stillPending.agentOwnerInstanceId,
              agentReleasedSessionId: stillPending.agentSessionId,
            });
            return { value: true, updatedSessions };
          });
          if (!completion.ok) throw completion.error;
          if (completion.value) {
            this._completedBeforeReleaseHooks.delete(releaseOperationKey);
            this._completedReleaseEvidenceHooks.delete(releaseOperationKey);
          }
          return completion.value;
        } finally {
          this._finishAgentRelease();
        }
      });
    } catch (err) {
      this._finishAgentRelease();
      throw err;
    }
    this._agentReleaseOperations.set(releaseOperationKey, releaseOperation);
    try {
      return await releaseOperation;
    } finally {
      if (this._agentReleaseOperations.get(releaseOperationKey) === releaseOperation) {
        this._agentReleaseOperations.delete(releaseOperationKey);
      }
    }
  }

  async startSession(
    config: Partial<SessionConfig> & { name?: string },
    agentGeneration?: PhysicalAgentGeneration,
  ): Promise<SessionInfo> {
    const name = config.name || `session-${Date.now()}`;

    const reservation = this.persistedSessions.get(name);
    const reservationMatches =
      agentGeneration !== undefined &&
      reservation?.agentReleasePending !== true &&
      reservation?.agentGeneration === agentGeneration.generation &&
      reservation.agentOwnerInstanceId === agentGeneration.owner_instance_id &&
      reservation.agentSessionId === agentGeneration.session_id;
    if (
      (reservation?.agentGeneration !== undefined ||
        reservation?.agentReleasePending === true ||
        reservation?.agentReleasedGeneration !== undefined ||
        agentGeneration !== undefined) &&
      !reservationMatches
    ) {
      throw Object.assign(new Error(`Autoloop session name '${name}' has a conflicting generation reservation`), {
        code: 'AUTOLOOP_AGENT_GENERATION_CONFLICT',
      });
    }

    // Check pending first — a concurrent caller may have already started creation
    const pending = this._pendingSessions.get(name);
    if (pending) return pending;

    if (this.sessions.has(name)) {
      const existing = this.sessions.get(name)!;
      return this._toSessionInfo(name, existing);
    }

    // Create the promise and register it in _pendingSessions BEFORE any async work,
    // so concurrent callers arriving between now and completion see the pending entry.
    const promise = this._doStartSession(name, config);
    this._pendingSessions.set(name, promise);
    try {
      return await promise;
    } finally {
      this._pendingSessions.delete(name);
    }
  }

  private async _doStartSession(
    name: string,
    config: Partial<SessionConfig> & { name?: string },
  ): Promise<SessionInfo> {
    if (this.sessions.size >= this.pluginConfig.maxConcurrentSessions) {
      throw new Error(`Max concurrent sessions (${this.pluginConfig.maxConcurrentSessions}) reached`);
    }

    // Auto-resume: if we have a persisted claudeSessionId for this name, inject it.
    // Skip when the caller asked for no persistence — either spelling. This read
    // used to be `skipPersistence` alone, through a cast, and that field is set
    // only by in-process callers (openai-compat bridge, ACP adapter); everything
    // that arrives over the CLI (`--skip-persistence`) or the MCP tool spells it
    // `noSessionPersistence`, so those sessions were written to the registry and
    // auto-resumed on the next start under the same name.
    const skipPersist = !!(config.skipPersistence || config.noSessionPersistence);
    const persisted = skipPersist ? undefined : this.persistedSessions.get(name);
    // Unified: only use resumeSessionId (claudeResumeId is an internal alias, not exposed)
    const resumeId = config.resumeSessionId ?? persisted?.claudeSessionId;

    // ORDER IS LOAD-BEARING — do not "fix" it by moving `...config` up.
    //
    // Object spread copies own keys even when their value is `undefined`, so any
    // key the caller sets EXPLICITLY (even to undefined) wins over the resolved
    // fallbacks above it. That is deliberate: the autoloop dispatcher passes
    // `model: undefined` for a non-Claude role to mean "use that engine's own
    // default", which must NOT be replaced by the Claude-shaped global default;
    // likewise `sandboxMode: undefined` means "no sandbox". Callers that simply
    // omit a key (MCP session_start, HTTP /session/start, auto-resume by name)
    // leave it absent, so the persisted/default fallback below still applies.
    const fullConfig: SessionConfig = {
      name,
      cwd: config.cwd || persisted?.cwd || process.cwd(),
      permissionMode: config.permissionMode || this.pluginConfig.defaultPermissionMode,
      effort: config.effort || this.pluginConfig.defaultEffort,
      model: config.model || persisted?.model || this.pluginConfig.defaultModel,
      sandboxMode: config.sandboxMode ?? persisted?.sandboxMode,
      ...config,
      ...(resumeId ? { resumeSessionId: resumeId } : {}),
    };

    // Resolve model alias
    if (fullConfig.model) {
      fullConfig.resolvedModel = this._resolveModel(fullConfig.model, fullConfig.modelOverrides);
    }

    // Auto-inject proxy baseUrl for non-Claude models on the claude engine.
    // Starts a local proxy server that converts Anthropic → OpenAI format
    // and forwards to the OpenClaw gateway. Zero config required.
    const engine: EngineType = fullConfig.engine || persisted?.engine || 'claude';
    // Write the resolved engine back so downstream consumers of the managed
    // config (agy resume-id lookups, _persistSession's registry entry) see the
    // real engine even when it came from the persisted registry.
    fullConfig.engine = engine;

    // Circuit breaker — reject early if engine is in backoff
    this._circuitBreaker.check(engine);

    if (engine === 'claude' && fullConfig.resolvedModel && !fullConfig.baseUrl) {
      if (!isClaudeModel(fullConfig.resolvedModel!)) {
        const proxyPort = await this._ensureProxyServer();
        if (proxyPort) {
          fullConfig.baseUrl = `http://127.0.0.1:${proxyPort}`;
        }
      }
    }
    const session = this._createSession(engine, fullConfig);

    session.on(SESSION_EVENT.LOG, (...args: unknown[]) => this.logger.info(`[Session:${name}]`, ...args));

    try {
      await session.start();
    } catch (err) {
      this._circuitBreaker.recordFailure(engine);
      throw err;
    }

    // Engine started successfully — reset circuit breaker
    this._circuitBreaker.reset(engine);

    // Track child process PID for orphan cleanup
    if (session.pid) {
      this._activePids.set(name, session.pid);
      this._savePids();
    }

    const managed: ManagedSession = {
      session,
      config: fullConfig,
      created: persisted?.originalCreated || new Date().toISOString(),
      lastActivity: Date.now(),
      cwd: fullConfig.cwd,
      claudeSessionId: this._sessionResumeId(engine, session),
      skipPersistence: skipPersist,
    };

    this.sessions.set(name, managed);

    // Persist registry after session is live (skip for ephemeral sessions
    // like the openai-compat bridge that set skipPersistence: true)
    if (!skipPersist) {
      this._persistSession(name, managed);
    }

    return this._toSessionInfo(name, managed);
  }

  async sendMessage(name: string, message: string, options: SendOptions = {}): Promise<SendResult> {
    const managed = this._getSession(name);

    // Per-session serialization. Two concurrent sendMessage() calls on the
    // same session previously raced on PersistentClaudeSession._streamCallbacks
    // and the shared TURN_COMPLETE listener — the second caller would receive
    // the first caller's response, and stream callbacks would clobber each
    // other. Chain waiters via a per-session promise so a slow turn blocks
    // (rather than corrupts) subsequent sends.
    const prior = managed.sendChain ?? Promise.resolve();
    let releaseChain!: () => void;
    const link = new Promise<void>((resolve) => {
      releaseChain = resolve;
    });
    managed.sendChain = prior.then(() => link).catch(() => link);
    try {
      await prior;
    } catch {
      /* prior failure shouldn't block this caller */
    }

    // The prior-chain await can sleep arbitrarily long. In that window a
    // concurrent stopSession() may have stopped this session and removed it
    // from the map. Re-check before writing, so we fail cleanly instead of
    // calling send() on a detached/stopped session (TOCTOU on the sessions map).
    if (this.sessions.get(name) !== managed) {
      releaseChain();
      if (managed.sendChain === link) managed.sendChain = undefined;
      throw new Error(`Session '${name}' was stopped while a prior turn was in flight`);
    }

    try {
      managed.lastActivity = Date.now();

      // Spend cap, enforced here rather than per-engine. Claude Code also gets
      // --max-budget-usd (an in-CLI stop is cheaper than an after-the-fact one),
      // but every other engine ignores that flag, so this gate is what actually
      // makes `maxBudgetUsd` mean something on codex/cursor/agy/opencode/custom.
      checkBudget(this._spentUsd(managed), managed.config.maxBudgetUsd, {
        session: name,
        engine: managed.config.engine || 'claude',
      });

      const sendOpts: Record<string, unknown> = {
        waitForComplete: true,
        timeout: options.timeout || TURN_TIMEOUT_MS,
      };

      if (options.effort) sendOpts.effort = options.effort;
      if (options.plan) sendOpts.plan = true;

      if (options.onEvent || options.onChunk) {
        // A throwing user callback must not corrupt the turn or leave the
        // sendChain unreleased — isolate each invocation.
        const safe = (fn: () => void): void => {
          try {
            fn();
          } catch (err) {
            this.logger.warn?.(`sendMessage stream callback threw: ${(err as Error).message}`);
          }
        };
        sendOpts.callbacks = {
          onText: (text: string) => {
            safe(() => options.onChunk?.(text));
            safe(() => options.onEvent?.({ type: 'text', result: text } as StreamEvent));
          },
          onToolUse: (event: unknown) => {
            safe(() => options.onEvent?.({ type: 'tool_use', ...(event as object) } as StreamEvent));
          },
          onToolResult: (event: unknown) => {
            safe(() => options.onEvent?.({ type: 'tool_result', ...(event as object) } as StreamEvent));
          },
        };
      }

      // Ledger bookkeeping. The snapshot/record pair brackets the one place
      // every caller funnels through (council, fanout, autoloop, ACP,
      // openai-compat, MCP, CLI), so a single hook covers all of them.
      const ledgerBefore = this._statsSnapshot(managed);
      const startedAt = Date.now();
      let turnError: string | undefined;

      try {
        // A session handed off from another engine carries that conversation in
        // front of its first message; after that the engine holds it itself.
        const outgoing = managed.pendingHandoff ? `${managed.pendingHandoff}\n\n${message}` : message;
        const result = await managed.session.send(outgoing, sendOpts);

        // Update the resume-capable session ID if available (skip disk persist
        // for ephemeral sessions that were started with skipPersistence)
        const resumableId = this._managedResumeId(managed);
        if (resumableId) {
          managed.claudeSessionId = resumableId;
          if (!managed.skipPersistence) {
            this._persistSession(name, managed);
          }
        }

        if ('text' in result) {
          // The CLI reports turn-level failures (invalid --model, auth loss) as a
          // result event with is_error and the explanation as its text — without
          // surfacing that here, the error text is indistinguishable from a reply.
          //
          // Deliberately NOT widened to `stop_reason === 'error'`: agy reaches this
          // point with that stop reason while carrying a usable reply, and `error` is
          // read as a hard failure downstream — openai-compat answers 502 and drops
          // the reply, ultraplan discards the plan. The ledger learns the outcome from
          // the session's own counter instead (see `_recordRunTurn`).
          const evt = (result as { event?: Record<string, unknown> }).event;
          if (evt?.is_error) {
            turnError = String((evt.result as string) || result.text || 'turn failed');
          }
          // Surfaced here because this is the one place every caller funnels
          // through. The result event was dropped at this line, and it is the
          // only record of a blocked call: the turn itself still reports success.
          const permissionDenials = readPermissionDenials(evt);
          // The record holds what the caller said, never the replayed history in
          // front of it — a second handoff would otherwise nest one inside the other.
          managed.transcript ??= newTranscript();
          recordExchange(managed.transcript, 'user', message);
          if (!turnError) {
            recordExchange(managed.transcript, 'assistant', result.text);
            managed.pendingHandoff = undefined;
          }
          return {
            output: result.text,
            sessionId: this._managedResumeId(managed),
            error: turnError,
            events: [],
            ...(permissionDenials.length ? { permissionDenials } : {}),
          };
        }

        return { output: '', sessionId: this._managedResumeId(managed), events: [] };
      } catch (err) {
        turnError = (err as Error).message;
        throw err;
      } finally {
        this._recordRunTurn(name, managed, ledgerBefore, startedAt, turnError, options.parentRunId, {
          nodeKind: options.nodeKind,
          taskKind: options.taskKind,
        });
      }
    } finally {
      releaseChain();
      // If this was the tail of the chain, clear it so memory doesn't grow.
      if (managed.sendChain === link) managed.sendChain = undefined;
    }
  }

  // ─── Handoff ───────────────────────────────────────────────────────────

  /**
   * Continue a session's conversation in a new session on another engine.
   *
   * The source is left running and untouched — this is a fork, not a move: the
   * two go their separate ways from here, and the caller stops the source if it
   * is done with it. The new session inherits the source's working directory and
   * its engine-neutral settings (permission and sandbox mode, effort, spend cap,
   * system prompts, extra directories), and nothing tied to the source engine
   * (model, tool allowlists written in its tool names, resume ids, profiles).
   *
   * The conversation travels as text in front of the new session's first message
   * — see `src/handoff.ts` for why, and for what is kept when it does not all fit.
   * With `message`, that first message is sent now and its reply returned; without
   * it, the history waits for whatever the caller sends next.
   */
  async handoffSession(
    name: string,
    opts: {
      engine: EngineType;
      model?: string;
      newName?: string;
      message?: string;
      maxChars?: number;
      customEngine?: SessionConfig['customEngine'];
    },
  ): Promise<{
    name: string;
    engine: EngineType;
    from: { name: string; engine: EngineType };
    carried: { turns: number; omitted: number; chars: number };
    result?: SendResult;
  }> {
    const source = this._getSession(name);
    const record = source.transcript;
    if (!record || record.entries.length === 0) {
      throw new Error(`Session '${name}' has no completed exchange to hand off yet`);
    }
    if (opts.maxChars !== undefined && (!Number.isFinite(opts.maxChars) || opts.maxChars < MIN_HANDOFF_CHARS)) {
      throw new Error(`maxChars must be a number of at least ${MIN_HANDOFF_CHARS}`);
    }
    const fromEngine = (source.config.engine || 'claude') as EngineType;
    const targetName = opts.newName ?? `${name}-${opts.engine}`;

    const inherited: Partial<SessionConfig> = {
      cwd: source.cwd,
      permissionMode: source.config.permissionMode,
      dangerouslySkipPermissions: source.config.dangerouslySkipPermissions,
      sandboxMode: source.config.sandboxMode,
      effort: source.config.effort,
      maxBudgetUsd: source.config.maxBudgetUsd,
      systemPrompt: source.config.systemPrompt,
      appendSystemPrompt: source.config.appendSystemPrompt,
      addDir: source.config.addDir,
    };
    for (const k of Object.keys(inherited) as (keyof SessionConfig)[]) {
      if (inherited[k] === undefined) delete inherited[k];
    }

    const rendered = renderHandoff(
      record,
      { engine: fromEngine, model: source.config.model, cwd: source.cwd },
      opts.maxChars ?? DEFAULT_HANDOFF_CHARS,
    );

    await this.startSession({
      ...inherited,
      name: targetName,
      engine: opts.engine,
      ...(opts.model ? { model: opts.model } : {}),
      ...(opts.customEngine ? { customEngine: opts.customEngine } : {}),
    });
    const target = this._getSession(targetName);
    // The new session's record starts as the source's, so a second handoff from
    // it carries the whole conversation rather than only the part it has seen.
    target.transcript = cloneTranscript(record);
    target.pendingHandoff = rendered.text;

    const out = {
      name: targetName,
      engine: opts.engine,
      from: { name, engine: fromEngine },
      carried: { turns: rendered.turns, omitted: rendered.omitted, chars: rendered.text.length },
    };
    if (opts.message === undefined) return out;
    return { ...out, result: await this.sendMessage(targetName, opts.message) };
  }

  // ─── Run ledger + budget bookkeeping ───────────────────────────────────
  //
  // Every helper here is defensive by design: a session that throws from
  // getCost()/getStats() (unknown model pricing, engine already torn down)
  // must degrade to "no data" rather than fail the turn it is measuring.

  /** Cumulative USD this session has spent, or 0 when the engine can't say. */
  private _spentUsd(managed: ManagedSession): number {
    try {
      const total = managed.session.getCost()?.totalUsd;
      return Number.isFinite(total) ? total : 0;
    } catch {
      return 0;
    }
  }

  private _statsSnapshot(managed: ManagedSession): {
    turns: number;
    turnsSucceeded: number;
    tokensIn: number;
    tokensOut: number;
    cachedTokens: number;
    toolCalls: number;
    toolErrors: number;
    costUsd: number;
  } {
    const empty = {
      turns: 0,
      turnsSucceeded: 0,
      tokensIn: 0,
      tokensOut: 0,
      cachedTokens: 0,
      toolCalls: 0,
      toolErrors: 0,
      costUsd: 0,
    };
    try {
      const st = managed.session.getStats();
      return {
        turns: st.turns || 0,
        turnsSucceeded: st.turnsSucceeded || 0,
        tokensIn: st.tokensIn || 0,
        tokensOut: st.tokensOut || 0,
        cachedTokens: st.cachedTokens || 0,
        toolCalls: st.toolCalls || 0,
        toolErrors: st.toolErrors || 0,
        costUsd: this._spentUsd(managed),
      };
    } catch {
      return empty;
    }
  }

  /**
   * Append one ledger row for the turn that just settled, and latch
   * budgetExhausted when this turn took the session over its cap.
   *
   * Rows carry per-turn deltas rather than session totals so that summing a
   * query gives the spend for that window without double-counting.
   */
  private _recordRunTurn(
    name: string,
    managed: ManagedSession,
    before: ReturnType<SessionManager['_statsSnapshot']>,
    startedAt: number,
    error: string | undefined,
    parent: string | undefined,
    dims: { nodeKind?: string; taskKind?: string } = {},
  ): void {
    const after = this._statsSnapshot(managed);
    const delta = (a: number, b: number): number => Math.max(0, a - b);
    const row: RunLedgerRow = {
      ts: new Date().toISOString(),
      session: name,
      engine: (managed.config.engine || 'claude') as EngineType,
      cwd: managed.cwd,
      turn: after.turns || before.turns + 1,
      tokensIn: delta(after.tokensIn, before.tokensIn),
      tokensOut: delta(after.tokensOut, before.tokensOut),
      cachedTokens: delta(after.cachedTokens, before.cachedTokens),
      costUsd: Math.round(delta(after.costUsd, before.costUsd) * 10000) / 10000,
      tokensEstimated: this._turnWasEstimated(managed),
      durationMs: Date.now() - startedAt,
      toolCalls: delta(after.toolCalls, before.toolCalls),
      toolErrors: delta(after.toolErrors, before.toolErrors),
      // One signal, not two predicates: the row is successful when the session's own
      // counter moved, so `ok` and `stats.turnsSucceeded` cannot disagree. `turns` is the
      // guard — when no turn was recorded at all (a getStats() that threw and left both
      // snapshots empty, so the counter cannot be read) this falls back to "nothing was
      // thrown", which is what the field meant before. The counter is also what closes
      // `stop_reason: 'process_exit'`: a CLI that dies mid-turn resolves without a result
      // event, so the counter never moves and the row is no longer recorded as a success.
      ok: !error && (after.turns > before.turns ? after.turnsSucceeded > before.turnsSucceeded : true),
    };
    // Fall back to the engine's own reported model, so a session started
    // without an explicit `model` still records what actually answered.
    const model = managed.config.resolvedModel || managed.config.model || this._reportedModel(managed);
    if (model) row.model = model;
    if (error) row.error = error.slice(0, 500);
    if (parent) row.parent = parent;
    if (dims.nodeKind) row.nodeKind = dims.nodeKind;
    if (dims.taskKind) row.taskKind = dims.taskKind;
    // Detected from a manifest, never guessed. `verified` is deliberately absent
    // here: the verdict does not exist yet at turn time, and is joined in at read
    // time by annotateVerdicts().
    const repoLang = this._repoLang(managed.cwd);
    if (repoLang) row.repoLang = repoLang;

    appendRunRow(row, this.logger);

    if (isBudgetExceeded(after.costUsd, managed.config.maxBudgetUsd)) {
      managed.budgetExhausted = true;
    }
  }

  /**
   * Repo language for the ledger row, memoised per cwd — the detector stats a
   * handful of manifest paths and a turn-rate filesystem probe is wasteful when
   * a session's cwd never changes.
   */
  private _repoLang(cwd: string): string | undefined {
    if (!cwd) return undefined;
    if (!this._repoLangCache.has(cwd)) {
      this._repoLangCache.set(cwd, detectRepoLang(cwd));
    }
    return this._repoLangCache.get(cwd);
  }

  private _reportedModel(managed: ManagedSession): string | undefined {
    try {
      return managed.session.getCost()?.model || undefined;
    } catch {
      return undefined;
    }
  }

  private _turnWasEstimated(managed: ManagedSession): boolean {
    try {
      return managed.session.getStats().tokensEstimated === true;
    } catch {
      return false;
    }
  }

  /**
   * Query the durable run ledger. Unlike getStats()/getCost(), this survives a
   * process restart and covers sessions this manager never owned.
   */
  getRunLedger(query: RunLedgerQuery = {}): { rows: RunLedgerRow[]; summary: RunLedgerSummary } {
    // Join each row to the verdict of the run it belonged to. The turns that did
    // the work all finish before the verifier that judged it, so the verdict
    // cannot be written at turn time — see `annotateVerdicts`.
    //
    // `verified` is deliberately withheld from the read: applying it there would
    // filter on a field no row carries yet and return nothing. It is applied
    // after the join instead.
    const { verified, ...readQuery } = query;
    const rows = annotateVerdicts(readRunLedger(readQuery, this.logger), (parent) => {
      const record = loadRun(parent);
      if (!record || record.outcome === 'unverified') return undefined;
      return {
        verified: record.outcome === 'verified',
        evidenceId: record.evidenceId,
        contractId: record.spec?.contract?.id,
      };
    });
    const filtered = verified === undefined ? rows : rows.filter((r) => r.verified === verified);
    return { rows: filtered, summary: summarizeRuns(filtered) };
  }

  // ─── Workflow kernel ──────────────────────────────────────────────────────

  /**
   * Lazily built, like every other subsystem here — constructing it at plugin
   * load would create run directories for a process that may never run anything.
   */
  private get kernel(): RunKernel {
    if (!this._kernel) {
      const kernel = registerDefaultExecutors(new RunKernel({ manager: this, logger: this.logger }), (name) =>
        this._resolveTemplate(name),
      );
      // The autoloop engine needs sessions, prompt files and push channels, so
      // its executor is registered here with a builder closed over `this`
      // rather than living in the kernel.
      kernel.setExecutor(
        'autoloop',
        makeAutoloopExecutor({
          boot: (config, secrets) =>
            this._bootAutoloop({
              ...(config as Parameters<SessionManager['_bootAutoloop']>[0]),
              // Custom-engine configs never reach the spec, so they come from
              // the run's in-memory secret bag — supplied at start, and
              // re-supplied by the caller on a resume.
              ...(secrets as Partial<Parameters<SessionManager['_bootAutoloop']>[0]>),
            }),
          ready: (key, value) => {
            const deferred = this._autoloopReady.get(key);
            if (!deferred) return;
            if (value instanceof Error) deferred.reject(value);
            else deferred.resolve(value);
          },
          waitForExit: (handle, signal) => this._awaitAutoloopExit(handle, signal),
          registerPublisher: (runId, publish) => this._autoloopPublishers.set(runId, publish),
          unregisterPublisher: (runId) => this._autoloopPublishers.delete(runId),
          extra: (runId) => {
            const roleSelection = this._autoloopSelection.get(runId);
            const handle = kernel.handle<AutoloopHandle & { dispatcher: ClaudeAgentDispatcher }>(runId, LEGACY_NODE);
            if (!handle) throw new Error(`Autoloop run '${runId}' has no live ledger while publishing its checkpoint`);
            const decisionLog = handle.dispatcher.secureLedgerCapability.readFlatFile('decisions.jsonl') ?? '';
            return {
              ...(roleSelection ? { roleSelection } : {}),
              // Internal recovery metadata, intentionally outside AutoloopState:
              // the hash proves the saved byte boundary is still a prefix, and
              // the byte offset supplies append causality without trusting time.
              detachedFailureLedgerCursor: detachedFailureLedgerCursor(decisionLog),
            };
          },
        }),
      );
      this._kernel = kernel;
    }
    return this._kernel;
  }

  /** Named built-ins available to `subflow` nodes and to `workflow_start`. */
  private _resolveTemplate(name: string): WorkflowSpec | undefined {
    // Built-ins need caller arguments, so a bare name only resolves to a
    // previously started run's spec — a subflow referencing a template by name
    // without arguments has nothing to run.
    const record = loadRun(name);
    return record?.spec;
  }

  /**
   * Subscribe to kernel events (for the SSE endpoint). Returns an unsubscribe
   * function — SessionManager is not an EventEmitter, and making it one just for
   * this would widen its surface for one consumer.
   */
  onWorkflowEvent(listener: (e: { runId: string; event: KernelEvent }) => void): () => void {
    const k = this.kernel;
    k.on('kernel-event', listener);
    return () => {
      k.off('kernel-event', listener);
    };
  }

  async workflowStart(
    spec: WorkflowSpec,
    opts: { runId?: string; cwd?: string; contract?: unknown } = {},
  ): Promise<RunRecord> {
    return this.kernel.start(spec, opts);
  }

  workflowStatus(runId: string): RunRecord {
    const record = this.kernel.get(runId);
    if (!record) throw new Error(`Workflow run '${runId}' not found`);
    return record;
  }

  workflowList(query: { workflow?: string; state?: RunState; limit?: number } = {}): RunSummary[] {
    return this.kernel.list(query);
  }

  workflowCancel(runId: string): { cancelled: boolean } {
    return { cancelled: this.kernel.cancel(runId) };
  }

  /**
   * Re-attach to a run.
   *
   * `secrets` re-supplies the material the spec deliberately does not carry —
   * per-agent custom-engine configs, keyed as `{ agentCustomEngines: { <name>: cfg } }`.
   * A run that used one cannot be resumed in a fresh process without them,
   * because they were never written down.
   */
  async workflowResume(runId: string, opts: { secrets?: Record<string, unknown> } = {}): Promise<RunRecord> {
    const stored = loadRun(runId);
    if (stored?.workflow === 'autoloop') {
      const supplied = opts.secrets ?? {};
      const named =
        supplied.agentCustomEngines &&
        typeof supplied.agentCustomEngines === 'object' &&
        !Array.isArray(supplied.agentCustomEngines)
          ? (supplied.agentCustomEngines as Record<string, unknown>)
          : {};
      await this.autoloopResume(runId, {
        plannerCustomEngine: (named.planner ?? supplied.plannerCustomEngine) as CustomEngineConfig | undefined,
        coderCustomEngine: (named.coder ?? supplied.coderCustomEngine) as CustomEngineConfig | undefined,
        reviewerCustomEngine: (named.reviewer ?? supplied.reviewerCustomEngine) as CustomEngineConfig | undefined,
      });
      const resumed = loadRun(runId);
      if (!resumed) throw new Error(`Workflow run '${runId}' not found after Autoloop recovery`);
      return resumed;
    }
    return this.kernel.resume(runId, { secrets: opts.secrets });
  }

  workflowSteer(runId: string, text: string): { steered: boolean } {
    return { steered: this.kernel.steer(runId, text) };
  }

  workflowApprove(runId: string, approved: boolean): { answered: boolean } {
    return { answered: this.kernel.approve(runId, approved) };
  }

  workflowDelete(runId: string): void {
    this.kernel.delete(runId);
  }

  workflowEvidence(runId: string, evidenceId?: string): EvidenceBundle | undefined {
    const dir = kernelRunDir(runId);
    const id = evidenceId ?? this.kernel.get(runId)?.evidenceId ?? listEvidence(dir).at(-1);
    return id ? readEvidence(dir, id) : undefined;
  }

  /**
   * Run an acceptance contract against a directory, outside any workflow.
   *
   * This is the escape hatch for work that did not come through the kernel — a
   * plain `session_send` that edited a repo, or a run from an older version. The
   * contract comes from the caller and is normalized before anything executes.
   */
  async verifyRun(args: { cwd: string; contract: unknown; baseSha?: string; label?: string }): Promise<EvidenceBundle> {
    const contract = normalizeContract(args.contract);
    if (!contract) throw new Error('verifyRun requires a contract with at least one recognised check');
    const runId = args.label || `verify-${Date.now().toString(36)}`;
    const dir = kernelRunDir(runId);
    const evidenceId = 'verify-01';
    const { results, rounds } = await runContract(contract, {
      cwd: args.cwd,
      artifactDir: evidenceDir(dir, evidenceId),
      baseSha: args.baseSha,
      logger: this.logger,
    });
    return writeEvidence({
      runDir: dir,
      runId,
      node: 'run',
      evidenceId,
      cwd: args.cwd,
      baseSha: args.baseSha,
      contractId: contract.id,
      results,
      rounds,
      logger: this.logger,
    });
  }

  async stopSession(name: string, opts: { keepPersisted?: boolean } = {}): Promise<void> {
    const managed = this._getSession(name);
    managed.session.stop();
    this.sessions.delete(name);
    // Remove PID tracking
    this._activePids.delete(name);
    this._savePids();
    if (!opts.keepPersisted) {
      // Explicit stop = user intent to end session — remove from disk too.
      // Callers that want the session resumable (autoloop terminate that
      // should still allow /autoloop/<id>/resume to reattach the Planner's
      // Claude conversation) pass keepPersisted: true.
      const persisted = this.persistedSessions.get(name);
      if (persisted?.agentGeneration !== undefined) {
        // Keep the generation fence until the dispatcher has durably appended
        // its release evidence and performs compare-and-release.
        this.persistedSessions.set(name, { ...persisted, claudeSessionId: '' });
      } else {
        this.persistedSessions.delete(name);
      }
      this._persistRegistrySnapshot();
    }
  }

  listSessions(): SessionInfo[] {
    return Array.from(this.sessions.entries()).map(([name, managed]) => this._toSessionInfo(name, managed));
  }

  listPersistedSessions(): PersistedSession[] {
    return Array.from(this.persistedSessions.values());
  }

  getStatus(name: string): SessionInfo & { stats: ReturnType<ISession['getStats']> } {
    const managed = this._getSession(name);
    return {
      ...this._toSessionInfo(name, managed),
      stats: managed.session.getStats(),
    };
  }

  // ─── Session Operations ────────────────────────────────────────────────

  async grepSession(
    name: string,
    pattern: string,
    limit = DEFAULT_HISTORY_LIMIT,
  ): Promise<Array<{ time: string; type: string; content: string }>> {
    const managed = this._getSession(name);
    const history = managed.session.getHistory(GREP_HISTORY_FETCH);
    const regex = new RE2(pattern, 'i');
    return history
      .filter((ev) => regex.test(JSON.stringify(ev)))
      .slice(0, limit)
      .map((ev) => ({
        time: ev.time,
        type: ev.type,
        content: JSON.stringify(ev.event),
      }));
  }

  async compactSession(name: string, summary?: string): Promise<void> {
    const managed = this._getSession(name);
    await managed.session.compact(summary);
  }

  setEffort(name: string, level: EffortLevel): void {
    const managed = this._getSession(name);
    managed.session.setEffort(level);
    managed.config.effort = level;
  }

  /**
   * Switch model for a session.
   * Updates in-memory config only (takes effect on next restart/resume).
   * For immediate effect, call restartWithConfig() explicitly.
   */
  setModel(name: string, model: string): void {
    const managed = this._getSession(name);
    const resolved = this._resolveModel(model, managed.config.modelOverrides);
    managed.config.model = model;
    managed.config.resolvedModel = resolved;
  }

  /**
   * Switch model immediately by restarting the session with --resume.
   * Conversation history is preserved via the claude session ID.
   *
   * Guards:
   * - Rejects if session is currently processing a message (busy guard)
   * - Validates model string against known aliases before restarting
   * - Rolls back to old session if startSession fails
   */
  async switchModel(name: string, model: string): Promise<SessionInfo> {
    const managed = this._getSession(name);

    // Busy guard — don't restart mid-message
    if (managed.session.isBusy) {
      throw new Error(
        `Session '${name}' is currently processing a message. Wait for it to finish before switching model.`,
      );
    }

    // An agy session with no harvested conversation yet has no history to
    // preserve — restart it fresh instead of rejecting the switch.
    const sessionId = this._managedResumeId(managed);
    if (!sessionId && managed.config.engine !== 'agy') {
      throw new Error(`Session '${name}' has no claude session ID — cannot resume after restart`);
    }

    // Validate against the registry, not against a frozen prefix list. The list
    // was ['claude-','gemini-','gpt-','anthropic/','google/','openai/'] and the
    // registry has since grown grok-4.6, composer-*, o3, o4-mini and
    // codex-mini-latest — every one of which `_createSession` can dispatch and
    // this guard rejected. A provider-qualified string stays accepted because
    // the error message below offers it.
    const resolvedModel = this._resolveModel(model, managed.config.modelOverrides);
    const looksValid = !!lookupModel(resolvedModel) || resolvedModel.includes('/');
    if (!looksValid) {
      throw new Error(
        `Unknown model '${model}' (resolved: '${resolvedModel}'). Use a known alias (opus, sonnet, haiku, gemini-pro, etc.) or a full provider/model string.`,
      );
    }

    const oldConfig = { ...managed.config };
    managed.session.stop();
    this.sessions.delete(name);

    try {
      return await this.startSession({
        ...oldConfig,
        name,
        model,
        ...(sessionId ? { resumeSessionId: sessionId } : {}),
      });
    } catch (err) {
      // Rollback: restart with original config
      this.logger.error(`switchModel failed for '${name}', attempting rollback:`, err);
      try {
        await this.startSession({ ...oldConfig, name, ...(sessionId ? { resumeSessionId: sessionId } : {}) });
      } catch (rollbackErr) {
        this.logger.error(`Rollback also failed for '${name}':`, rollbackErr);
      }
      throw new Error(`Failed to switch model for '${name}': ${(err as Error).message}`);
    }
  }

  /**
   * Update allowedTools or disallowedTools at runtime.
   *
   * The claude CLI does not support changing tool lists while running, so
   * the only way to apply new constraints is to restart the process with
   * the updated flags and --resume to replay conversation history.
   *
   * Guards:
   * - Rejects if session is busy
   * - Rolls back to old session if startSession fails
   * - merge:true adds tools; removeTools removes specific tools from the list
   */
  async updateTools(
    name: string,
    opts: {
      allowedTools?: string[];
      disallowedTools?: string[];
      removeTools?: string[];
      merge?: boolean;
    },
  ): Promise<SessionInfo> {
    const managed = this._getSession(name);

    // Busy guard
    if (managed.session.isBusy) {
      throw new Error(
        `Session '${name}' is currently processing a message. Wait for it to finish before updating tools.`,
      );
    }

    // An agy session with no harvested conversation yet has no history to
    // preserve — restart it fresh instead of rejecting the update.
    const sessionId = this._managedResumeId(managed);
    if (!sessionId && managed.config.engine !== 'agy') {
      throw new Error(`Session '${name}' has no claude session ID — cannot resume after restart`);
    }

    const oldConfig = { ...managed.config };
    let newAllowed = opts.allowedTools;
    let newDisallowed = opts.disallowedTools;

    if (opts.merge) {
      newAllowed = opts.allowedTools
        ? [...new Set([...(oldConfig.allowedTools || []), ...opts.allowedTools])]
        : oldConfig.allowedTools;
      newDisallowed = opts.disallowedTools
        ? [...new Set([...(oldConfig.disallowedTools || []), ...opts.disallowedTools])]
        : oldConfig.disallowedTools;
    }

    // Remove specific tools if requested
    if (opts.removeTools?.length) {
      const removeSet = new Set(opts.removeTools);
      if (newAllowed) newAllowed = newAllowed.filter((t) => !removeSet.has(t));
      if (newDisallowed) newDisallowed = newDisallowed.filter((t) => !removeSet.has(t));
    }

    managed.session.stop();
    this.sessions.delete(name);

    try {
      return await this.startSession({
        ...oldConfig,
        name,
        allowedTools: newAllowed,
        disallowedTools: newDisallowed,
        ...(sessionId ? { resumeSessionId: sessionId } : {}),
      });
    } catch (err) {
      this.logger.error(`updateTools failed for '${name}', attempting rollback:`, err);
      try {
        await this.startSession({ ...oldConfig, name, ...(sessionId ? { resumeSessionId: sessionId } : {}) });
      } catch (rollbackErr) {
        this.logger.error(`Rollback also failed for '${name}':`, rollbackErr);
      }
      throw new Error(`Failed to update tools for '${name}': ${(err as Error).message}`);
    }
  }

  getCost(name: string) {
    const managed = this._getSession(name);
    return managed.session.getCost();
  }

  // ─── Agent/Skill/Rule Management ──────────────────────────────────────

  listAgents(cwd?: string): AgentInfo[] {
    const safeCwd = sanitizeCwd(cwd);
    const projectDir = path.join(safeCwd || os.homedir(), '.claude', 'agents');
    const globalDir = path.join(os.homedir(), '.claude', 'agents');
    const project = this._listMdFiles(projectDir);
    const global = this._listMdFiles(globalDir);
    const seen = new Set(project.map((a) => a.name));
    return [...project, ...global.filter((a) => !seen.has(a.name))];
  }

  createAgent(name: string, cwd?: string, description?: string, prompt?: string): string {
    validateName(name);
    const safeCwd = sanitizeCwd(cwd);
    const dir = path.join(safeCwd || os.homedir(), '.claude', 'agents');
    fs.mkdirSync(dir, { recursive: true });
    const filePath = path.join(dir, `${name}.md`);
    const content = `---\ndescription: ${description || name}\n---\n\n${prompt || `You are ${name}.`}\n`;
    fs.writeFileSync(filePath, content);
    return filePath;
  }

  listSkills(cwd?: string): SkillInfo[] {
    const safeCwd = sanitizeCwd(cwd);
    const dirs = [
      path.join(safeCwd || os.homedir(), '.claude', 'skills'),
      path.join(os.homedir(), '.claude', 'skills'),
    ];
    const all: SkillInfo[] = [];
    const seen = new Set<string>();
    for (const dir of dirs) {
      if (!fs.existsSync(dir)) continue;
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        if (!entry.isDirectory() || seen.has(entry.name)) continue;
        seen.add(entry.name);
        const skillMd = path.join(dir, entry.name, 'SKILL.md');
        let description = '';
        if (fs.existsSync(skillMd)) {
          const content = fs.readFileSync(skillMd, 'utf8');
          const match = content.match(/^---\n[\s\S]*?description:\s*(.+)/m);
          if (match) description = match[1].trim();
        }
        all.push({ name: entry.name, hasSkillMd: fs.existsSync(skillMd), description });
      }
    }
    return all;
  }

  createSkill(name: string, cwd?: string, opts?: { description?: string; prompt?: string; trigger?: string }): string {
    validateName(name);
    const safeCwd = sanitizeCwd(cwd);
    const dir = path.join(safeCwd || os.homedir(), '.claude', 'skills', name);
    fs.mkdirSync(dir, { recursive: true });
    const filePath = path.join(dir, 'SKILL.md');
    let content = '---\n';
    if (opts?.description) content += `description: ${opts.description}\n`;
    if (opts?.trigger) content += `trigger: ${opts.trigger}\n`;
    content += `---\n\n${opts?.prompt || `# ${name}\n\nSkill instructions here.\n`}\n`;
    fs.writeFileSync(filePath, content);
    return filePath;
  }

  listRules(cwd?: string): RuleInfo[] {
    const safeCwd = sanitizeCwd(cwd);
    const dirs = [path.join(safeCwd || os.homedir(), '.claude', 'rules'), path.join(os.homedir(), '.claude', 'rules')];
    const all: RuleInfo[] = [];
    const seen = new Set<string>();
    for (const dir of dirs) {
      if (!fs.existsSync(dir)) continue;
      for (const f of fs.readdirSync(dir).filter((f) => f.endsWith('.md'))) {
        const name = f.replace('.md', '');
        if (seen.has(name)) continue;
        seen.add(name);
        const content = fs.readFileSync(path.join(dir, f), 'utf8');
        const descMatch = content.match(/^---\n[\s\S]*?description:\s*(.+)/m);
        const pathsMatch = content.match(/^---\n[\s\S]*?paths:\s*(.+)/m);
        const ifMatch = content.match(/^---\n[\s\S]*?if:\s*(.+)/m);
        all.push({
          name,
          file: f,
          description: descMatch?.[1]?.trim() || '',
          paths: pathsMatch?.[1]?.trim() || '',
          condition: ifMatch?.[1]?.trim() || '',
        });
      }
    }
    return all;
  }

  createRule(
    name: string,
    cwd?: string,
    opts?: { description?: string; content?: string; paths?: string; condition?: string },
  ): string {
    validateName(name);
    const safeCwd = sanitizeCwd(cwd);
    const dir = path.join(safeCwd || os.homedir(), '.claude', 'rules');
    fs.mkdirSync(dir, { recursive: true });
    const filePath = path.join(dir, `${name}.md`);
    let fileContent = '---\n';
    if (opts?.description) fileContent += `description: ${opts.description}\n`;
    if (opts?.paths) fileContent += `paths: ${opts.paths}\n`;
    if (opts?.condition) fileContent += `if: ${opts.condition}\n`;
    fileContent += `---\n\n${opts?.content || `# ${name}\n\nRule instructions here.\n`}\n`;
    fs.writeFileSync(filePath, fileContent);
    return filePath;
  }

  // ─── Agent Teams ───────────────────────────────────────────────────────

  async teamList(name: string): Promise<string> {
    // Validate the calling session exists, but list all other sessions as virtual
    // teammates regardless of engine. Claude Code's native Agent Teams (v2.1.32+,
    // gated by CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS) is an in-process TUI
    // mechanism — it has no `/team` slash command and no stdin-driven mailbox
    // accessible to a subprocess wrapper. Earlier code assumed `/team` existed
    // and got back `Unknown command: /team` (issue #48).
    this._getSession(name);

    const teammates: string[] = [];
    for (const [sessionName, m] of this.sessions) {
      if (sessionName === name) continue;
      const eng = m.config.engine || 'claude';
      const stats = m.session.getStats();
      const status = m.session.isBusy ? 'busy' : m.session.isPaused ? 'paused' : 'idle';
      teammates.push(`- ${sessionName} (${eng}, ${status}, ${stats.turns} turns)`);
    }
    return teammates.length > 0
      ? `Virtual team (${teammates.length} sessions):\n${teammates.join('\n')}`
      : 'No other active sessions';
  }

  async teamSend(name: string, teammate: string, message: string): Promise<SendResult> {
    const managed = this._getSession(name);

    if (!this.sessions.has(teammate)) {
      throw new Error(`Target session '${teammate}' not found. Use team_list to see available sessions.`);
    }
    const deliveryResult = await this.sessionSendTo(name, teammate, message, `team message from ${name}`);
    return {
      output: deliveryResult.delivered
        ? `Message delivered to ${teammate}`
        : `Message queued for ${teammate} (session is busy)`,
      sessionId: this._managedResumeId(managed),
      events: [],
    };
  }

  // ─── Health ────────────────────────────────────────────────────────────

  /**
   * Returns an overview of all active sessions — analogous to a dashboard.
   * Unlike coding_session_status (single session), this gives the aggregate
   * view: how many sessions are running, which are busy, total uptime, etc.
   */
  health(): {
    ok: boolean;
    version: string;
    sessions: number;
    sessionNames: string[];
    uptime: number;
    details: Array<{
      name: string;
      ready: boolean;
      busy: boolean;
      paused: boolean;
      turns: number;
      turnsSucceeded: number;
      costUsd: number;
      contextPercent: number;
      lastActivity: string | null;
    }>;
    circuitBreakers: Record<string, { failures: number; backoffUntil: string | null }>;
  } {
    const details = Array.from(this.sessions.entries()).map(([name, managed]) => {
      const stats = managed.session.getStats();
      return {
        name,
        ready: stats.isReady,
        busy: managed.session.isBusy,
        paused: managed.session.isPaused,
        turns: stats.turns,
        turnsSucceeded: stats.turnsSucceeded,
        costUsd: stats.costUsd,
        contextPercent: stats.contextPercent,
        lastActivity: stats.lastActivity,
      };
    });

    return {
      ok: true,
      version: getPluginVersion(),
      sessions: this.sessions.size,
      sessionNames: Array.from(this.sessions.keys()),
      uptime: process.uptime(),
      details,
      circuitBreakers: this._circuitBreaker.getStatus(),
    };
  }

  /** Return plugin version from package.json */
  getVersion(): string {
    return getPluginVersion();
  }

  // ─── Shutdown ──────────────────────────────────────────────────────────

  /**
   * Gracefully shut down the session manager.
   *
   * 1. Cancels the periodic TTL cleanup timer
   * 2. Stops all ultrareview polling intervals
   * 3. Sends SIGTERM to all active session child processes
   * 4. Persists final session registry to disk
   *
   * After shutdown(), no new sessions can be started. Idempotent.
   */
  shutdown(): Promise<void> {
    if (this._shutdownPromise) return this._shutdownPromise;
    let resolveShutdown!: () => void;
    let rejectShutdown!: (reason?: unknown) => void;
    this._shutdownPromise = new Promise<void>((resolve, reject) => {
      resolveShutdown = resolve;
      rejectShutdown = reject;
    });
    void this._performShutdown().then(resolveShutdown, rejectShutdown);
    return this._shutdownPromise;
  }

  private async _performShutdown(): Promise<void> {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
    // Council, fan-out, ultraplan and ultrareview no longer have timers or maps
    // to tear down here: the kernel owns their lifecycle, and `shutdown` on it
    // cancels every live run. Four separate 30-minute TTL closures used to sit
    // in this method, each capturing `this`.
    // Autoloops included: cancelling their run stops the loop, which shuts down
    // its three persistent agents. One teardown path for every mode.
    if (this._kernel) await this._kernel.shutdown();
    // Stop all sessions
    for (const [name, managed] of this.sessions) {
      try {
        managed.session.stop();
      } catch {
        // Best-effort — session may already be dead; must not block cleanup
      }
      this.logger.info(`Stopped session: ${name}`);
    }
    this.sessions.clear();
    // Clear PID tracking
    this._activePids.clear();
    this._savePids();
    // Stop proxy server
    if (this._proxyServer) {
      this._proxyServer.close();
      this._proxyServer = null;
      this._proxyPort = null;
    }
    // Kernel teardown may legitimately release Autoloop agents. Once it has
    // finished initiating those releases, close the fence before observing
    // the registered-operation count so no later claim can escape the wait.
    this._agentReleaseFenceClosed = true;
    await this._waitForAgentReleases();
    this._debouncedSave.cancel();
    // Persist final state (TTL-expired sessions already removed by cleanup)
    this._persistRegistrySnapshot();
    SessionManager.liveAutoloopOwnerInstanceIds.delete(this.autoloopOwnerInstanceId);
  }

  // ─── Codex /goal helpers (codex-app engine only) ─────────────────────

  /**
   * Send a `/goal <args>` slash command to a `codex-app` session. Used by
   * the `codex_goal_*` tools. The server-side parser interprets the slash
   * command and emits goal-related notifications which the session class
   * caches.
   *
   * Errors clearly when called against a non-`codex-app` session — those
   * sessions cannot interpret `/goal` (the `codex exec` path has no slash
   * command surface).
   */
  async codexGoalCommand(
    name: string,
    slashArgs: string,
    timeoutMs?: number,
  ): Promise<{ ok: true; text: string; goal: unknown }> {
    const managed = this.sessions.get(name);
    if (!managed) throw new Error(`Session not found: ${name}`);
    const session = managed.session as ISession & {
      sendGoalCommand?: (args: string, timeoutMs?: number) => Promise<{ text: string; goal: unknown }>;
    };
    if (typeof session.sendGoalCommand !== 'function') {
      const engine = managed.config.engine || 'claude';
      throw new Error(
        `Session "${name}" uses engine "${engine}" which does not support /goal. ` +
          `Start a session with engine: "codex-app" to use the goal tools.`,
      );
    }
    const result = await session.sendGoalCommand(slashArgs, timeoutMs);
    return { ok: true, text: result.text, goal: result.goal };
  }

  /**
   * Read the cached goal state from a `codex-app` session without sending
   * any command. Returns null if no goal is set or the session has not yet
   * received a `thread/goal/updated` notification.
   */
  codexGoalGet(name: string): { ok: true; goal: unknown } {
    const managed = this.sessions.get(name);
    if (!managed) throw new Error(`Session not found: ${name}`);
    const session = managed.session as ISession & { goal?: unknown };
    if (!('goal' in session)) {
      const engine = managed.config.engine || 'claude';
      throw new Error(
        `Session "${name}" uses engine "${engine}" which does not track goal state. ` +
          `Start a session with engine: "codex-app" to use the goal tools.`,
      );
    }
    return { ok: true, goal: session.goal ?? null };
  }

  // ─── Codex app-server v2 RPCs (codex-app engine only, Codex 0.137) ────────
  //
  // turn/interrupt, turn/steer, thread/fork, thread/rollback, model/list — the
  // high-value app-server surface beyond /goal. Each requires a `codex-app`
  // session; the discriminator is the presence of the `interrupt` method.

  private _getCodexAppSession(name: string, feature: string): CodexAppSession {
    const managed = this.sessions.get(name);
    if (!managed) throw new Error(`Session not found: ${name}`);
    const session = managed.session as CodexAppSession;
    if (typeof session.interrupt !== 'function') {
      const engine = managed.config.engine || 'claude';
      throw new Error(
        `Session "${name}" uses engine "${engine}" which does not support ${feature}. ` +
          `Start a session with engine: "codex-app".`,
      );
    }
    return session;
  }

  async codexInterrupt(name: string): Promise<{ ok: true; interrupted: boolean }> {
    const r = await this._getCodexAppSession(name, 'turn/interrupt').interrupt();
    return { ok: true, ...r };
  }

  async codexSteer(
    name: string,
    text: string,
  ): Promise<{ ok: true; steered: boolean; turnId?: string; text?: string }> {
    const r = await this._getCodexAppSession(name, 'turn/steer').steer(text);
    return { ok: true, ...r };
  }

  async codexForkThread(name: string): Promise<{ ok: true; threadId: string }> {
    const r = await this._getCodexAppSession(name, 'thread/fork').forkThread();
    return { ok: true, ...r };
  }

  async codexRollback(name: string, numTurns: number): Promise<{ ok: true; numTurns: number }> {
    await this._getCodexAppSession(name, 'thread/rollback').rollback(numTurns);
    return { ok: true, numTurns };
  }

  async codexModels(name: string): Promise<{ ok: true; models: unknown[] }> {
    const models = await this._getCodexAppSession(name, 'model/list').listModels();
    return { ok: true, models };
  }

  async codexThreads(
    name: string,
    opts: { cwd?: string; searchTerm?: string; archived?: boolean; cursor?: string; limit?: number } = {},
  ): Promise<{ ok: true; data: unknown[]; nextCursor: string | null }> {
    const r = await this._getCodexAppSession(name, 'thread/list').listThreads(opts);
    return { ok: true, ...r };
  }

  // ─── Claude /goal helpers (CLI 2.1.139, claude engine only) ────────
  //
  // Claude Code's /goal slash command works in non-interactive stream-json
  // sessions: the CLI parses any user message starting with `/goal` and
  // routes it to the goal subsystem. Unlike Codex's app-server protocol,
  // Claude does not emit a separate goal-state notification — the only
  // surface is the assistant's reply text. These wrappers are thin
  // pre-formatters around `sendMessage()` that enforce the engine guard
  // and pass the slash text through.

  private _assertClaudeSession(name: string): void {
    const managed = this.sessions.get(name);
    if (!managed) throw new Error(`Session not found: ${name}`);
    const engine = managed.config.engine || 'claude';
    if (engine !== 'claude') {
      throw new Error(
        `Session "${name}" uses engine "${engine}" which does not support Claude /goal. ` +
          `Start a session with engine: "claude" (or omit engine) to use claude_goal_* tools.`,
      );
    }
  }

  /** Send `/goal <objective>` to a claude session. Sets a completion condition that
   *  Claude Code pursues across turns, evaluating after each turn via Haiku. */
  async claudeGoalSet(name: string, objective: string, timeoutMs?: number): Promise<unknown> {
    this._assertClaudeSession(name);
    return await this.sendMessage(name, `/goal ${objective}`, { timeout: timeoutMs });
  }

  /** Send `/goal clear` to remove the active goal. */
  async claudeGoalClear(name: string, timeoutMs?: number): Promise<unknown> {
    this._assertClaudeSession(name);
    return await this.sendMessage(name, '/goal clear', { timeout: timeoutMs });
  }

  /** Send bare `/goal` to query the active goal (elapsed time, turns, tokens). */
  async claudeGoalStatus(name: string, timeoutMs?: number): Promise<unknown> {
    this._assertClaudeSession(name);
    return await this.sendMessage(name, '/goal', { timeout: timeoutMs });
  }

  // ─── Plugin Details (CLI 2.1.139) ─────────────────────────────────────

  /**
   * Wraps `claude plugin details <name>` — prints the plugin's component
   * inventory (commands, hooks, MCP servers, agents, skills) plus the
   * per-session token cost of loading it. Returns raw stdout/stderr.
   */
  async pluginDetails(name: string): Promise<{ stdout: string; stderr: string }> {
    if (!name || typeof name !== 'string') throw new Error('plugin name required');
    const { stdout, stderr } = await execFileAsync(this.pluginConfig.claudeBin, ['plugin', 'details', name], {
      maxBuffer: 8 * 1024 * 1024,
    });
    return { stdout, stderr };
  }

  /**
   * Wraps `claude agents --json` — lists Claude Code background agent sessions
   * (state/model/title/progress). One-shot spawn; not tied to a managed session.
   * `all` adds `--all` (include completed); `cwd` scopes to a directory.
   */
  async claudeAgentsList(opts: { all?: boolean; cwd?: string } = {}): Promise<{ ok: true; agents: unknown[] }> {
    const args = ['agents', '--json'];
    if (opts.all) args.push('--all');
    if (opts.cwd) args.push('--cwd', path.resolve(opts.cwd));
    const { stdout } = await execFileAsync(this.pluginConfig.claudeBin, args, {
      maxBuffer: 16 * 1024 * 1024,
      timeout: 30_000,
    });
    let agents: unknown[] = [];
    const trimmed = stdout.trim();
    if (trimmed) {
      try {
        const parsed = JSON.parse(trimmed);
        agents = Array.isArray(parsed) ? parsed : ((parsed as { agents?: unknown[] }).agents ?? []);
      } catch {
        throw new Error(`claude agents --json returned non-JSON output: ${trimmed.slice(0, 200)}`);
      }
    }
    return { ok: true, agents };
  }

  // ─── Codex one-shot wrappers ──────────────────────────────────────────

  private _codexBin(): string {
    return process.env.CODEX_BIN || 'codex';
  }

  /**
   * Parse Codex `--json` JSONL output from a stdout buffer.
   *
   * Codex 0.128 emits one event per line: `thread.started`, `turn.started`,
   * `item.completed` (with `item.type === 'agent_message'` for assistant text
   * or tool-use types for shell/MCP calls), `turn.completed` (with usage).
   *
   * Returns the concatenated assistant text plus the thread_id (if present)
   * and the raw event list for callers that want full visibility.
   */
  private _parseCodexJsonl(stdout: string): {
    assistantText: string;
    threadId?: string;
    usage?: {
      input_tokens?: number;
      output_tokens?: number;
      cached_input_tokens?: number;
      reasoning_output_tokens?: number;
    };
    events: unknown[];
  } {
    let assistantText = '';
    let threadId: string | undefined;
    let usage:
      | {
          input_tokens?: number;
          output_tokens?: number;
          cached_input_tokens?: number;
          reasoning_output_tokens?: number;
        }
      | undefined;
    const events: unknown[] = [];
    for (const line of stdout.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      let ev: unknown;
      try {
        ev = JSON.parse(trimmed);
      } catch {
        continue; // Non-JSON lines are tolerated (banners, etc.)
      }
      events.push(ev);
      const e = ev as { type?: string };
      if (e.type === 'thread.started') {
        const t = ev as { thread_id?: string };
        if (t.thread_id) threadId = t.thread_id;
      } else if (e.type === 'item.completed') {
        const it = ev as { item?: { type?: string; text?: string } };
        if (it.item?.type === 'agent_message' && typeof it.item.text === 'string') {
          assistantText += it.item.text;
        }
      } else if (e.type === 'turn.completed') {
        const tc = ev as { usage?: typeof usage };
        if (tc.usage) usage = tc.usage;
      }
    }
    return { assistantText, threadId, usage, events };
  }

  /**
   * Wraps `codex exec resume [SESSION_ID|--last] [PROMPT]` (Codex 0.119+).
   *
   * Resumes a previously recorded Codex thread by UUID/name or picks the most
   * recent via `--last`. Always uses `--json` + `--sandbox workspace-write`
   * so the output can be parsed into structured fields.
   *
   * Note: this is a one-shot operation independent of the session manager's
   * tracked sessions. For in-session continuity (each send within one session
   * resumes the prior thread automatically), `PersistentCodexSession`
   * already handles that via the captured `thread_id` from `thread.started`.
   */
  async codexResume(opts: {
    sessionId?: string;
    last?: boolean;
    message: string;
    cwd?: string;
    model?: string;
    timeout?: number;
  }): Promise<{
    ok: true;
    text: string;
    threadId?: string;
    usage?: {
      input_tokens?: number;
      output_tokens?: number;
      cached_input_tokens?: number;
      reasoning_output_tokens?: number;
    };
    events: unknown[];
  }> {
    if (!opts.sessionId && !opts.last) {
      throw new Error('codexResume requires either sessionId or last=true');
    }
    // `codex exec resume` does not accept --sandbox or -C — sandbox policy
    // and cwd are inherited from the original session. Forward `cwd` via
    // the spawn process's working directory so Codex's --last picker scopes
    // correctly when no SESSION_ID is given.
    const args: string[] = ['exec', 'resume'];
    if (opts.last) args.push('--last');
    else if (opts.sessionId) args.push(opts.sessionId);
    args.push('--skip-git-repo-check', '--json');
    if (opts.model) args.push('--model', opts.model);
    args.push(opts.message);
    const { stdout } = await execFileAsync(this._codexBin(), args, {
      cwd: opts.cwd ? path.resolve(opts.cwd) : undefined,
      maxBuffer: 32 * 1024 * 1024,
      timeout: opts.timeout || 300_000,
    });
    const parsed = this._parseCodexJsonl(stdout);
    return {
      ok: true,
      text: parsed.assistantText,
      threadId: parsed.threadId,
      usage: parsed.usage,
      events: parsed.events,
    };
  }

  /**
   * Wraps `codex review [PROMPT] [--uncommitted | --base BRANCH | --commit SHA]`.
   *
   * Codex 0.128's review subcommand outputs plain text (no `--json` flag),
   * so the wrapper just captures stdout/stderr verbatim.
   */
  async codexReview(opts: {
    prompt?: string;
    cwd?: string;
    uncommitted?: boolean;
    base?: string;
    commit?: string;
    title?: string;
    model?: string;
    timeout?: number;
  }): Promise<{ ok: true; stdout: string; stderr: string }> {
    // Mutex: at most one diff scope flag.
    const scopes = [opts.uncommitted, opts.base, opts.commit].filter((v) => v != null && v !== false);
    if (scopes.length > 1) {
      throw new Error('codexReview: --uncommitted, --base, and --commit are mutually exclusive');
    }
    // Validate git refs: reject leading-dash (argument injection) and shell/path
    // metacharacters. args go through execFile (no shell) but a '--flag'-shaped
    // value could still be misread by codex's parser.
    const GIT_REF = /^[A-Za-z0-9][A-Za-z0-9._/-]*$/;
    if (opts.base != null && !GIT_REF.test(opts.base)) {
      throw new Error(`codexReview: invalid base ref '${opts.base}'`);
    }
    if (opts.commit != null && !GIT_REF.test(opts.commit)) {
      throw new Error(`codexReview: invalid commit ref '${opts.commit}'`);
    }
    const args: string[] = ['review'];
    if (opts.uncommitted) args.push('--uncommitted');
    if (opts.base) args.push('--base', opts.base);
    if (opts.commit) args.push('--commit', opts.commit);
    if (opts.title) args.push('--title', opts.title);
    if (opts.model) args.push('-c', `model="${opts.model}"`);
    if (opts.prompt) args.push(opts.prompt);
    const { stdout, stderr } = await execFileAsync(this._codexBin(), args, {
      cwd: opts.cwd ? path.resolve(opts.cwd) : undefined,
      maxBuffer: 16 * 1024 * 1024,
      timeout: opts.timeout || 600_000,
    });
    return { ok: true, stdout, stderr };
  }

  // ─── Project Purge (CLI 2.1.126) ──────────────────────────────────────

  /**
   * Wraps `claude project purge` — deletes Claude Code state for a project
   * (transcripts, tasks, file history, config entry).
   *
   * Defaults to dry-run for safety: callers must pass `dryRun: false` to
   * actually delete. When `all` is true, `path` is ignored.
   *
   * The `--yes` flag is always passed (we have no TTY for confirmation prompts);
   * safety is enforced via the dry-run default at the wrapper level instead.
   */
  async purgeProject(opts: {
    path?: string;
    all?: boolean;
    dryRun?: boolean;
  }): Promise<{ stdout: string; stderr: string; dryRun: boolean }> {
    const dryRun = opts.dryRun !== false; // default true
    const args = ['project', 'purge'];
    if (opts.all) args.push('--all');
    if (dryRun) args.push('--dry-run');
    else args.push('--yes');
    if (!opts.all && opts.path) args.push(path.resolve(opts.path));
    const { stdout, stderr } = await execFileAsync(this.pluginConfig.claudeBin, args, {
      maxBuffer: 8 * 1024 * 1024,
    });
    return { stdout, stderr, dryRun };
  }

  // ─── Auto Proxy ───────────────────────────────────────────────────────

  /**
   * Read OpenClaw gateway config from ~/.openclaw/openclaw.json.
   * Returns { url, key } or null if not configured.
   */
  private _readGatewayConfig(): { url: string; key: string } | null {
    try {
      const configPath = path.join(os.homedir(), '.openclaw', 'openclaw.json');
      if (!fs.existsSync(configPath)) return null;
      const config = JSON.parse(fs.readFileSync(configPath, 'utf8')) as Record<string, unknown>;
      const gw = config.gateway as Record<string, unknown> | undefined;
      if (!gw) return null;

      const port = (gw.port as number) || 18789;
      const auth = gw.auth as Record<string, string> | undefined;
      // Support both password and token auth modes
      const key = auth?.password || auth?.token || '';

      return { url: `http://127.0.0.1:${port}/v1`, key };
    } catch {
      return null;
    }
  }

  /**
   * Start a local proxy server (if not running) that converts Anthropic format
   * to OpenAI format and forwards to the OpenClaw gateway.
   * Returns the proxy port, or null if gateway is not available.
   */
  private async _ensureProxyServer(): Promise<number | null> {
    if (this._proxyPort) return this._proxyPort;
    // The port is only assigned inside listen()'s callback, several awaits
    // later, so a bare null-check is not an idempotency guard: council and
    // fanout start their agents with Promise.all under distinct names, and
    // `_pendingSessions` only serialises per name. Two callers each bound their
    // own server; the last one to call back won `_proxyServer`, and shutdown()
    // closed only that one. Memoised the same way `startSession` memoises
    // `_pendingSessions`.
    if (this._proxyStartPromise) return this._proxyStartPromise;
    this._proxyStartPromise = this._startProxyServer().finally(() => {
      this._proxyStartPromise = null;
    });
    return this._proxyStartPromise;
  }

  private async _startProxyServer(): Promise<number | null> {
    // Auto-detect gateway config
    const gwConfig = this._readGatewayConfig();
    const gatewayUrl = process.env.GATEWAY_URL || gwConfig?.url;
    const gatewayKey = process.env.GATEWAY_KEY || gwConfig?.key;

    if (!gatewayUrl) {
      this.logger.info('No OpenClaw gateway found — proxy not available');
      return null;
    }

    // Lazy import to avoid circular deps
    const { createProxyHandler } = await import('./proxy/handler.js');
    const proxyHandler = createProxyHandler(undefined, {
      anthropicApiKey: process.env.ANTHROPIC_API_KEY,
      openaiApiKey: process.env.OPENAI_API_KEY,
      geminiApiKey: process.env.GEMINI_API_KEY,
      gatewayUrl,
      gatewayKey,
    });

    return new Promise((resolve) => {
      const server = http.createServer((req, res) => {
        let body = '';
        req.on('data', (chunk: Buffer) => {
          body += chunk.toString();
        });
        req.on('end', () => {
          const httpReq = {
            method: req.method || 'GET',
            url: req.url || '/',
            headers: req.headers as Record<string, string>,
            json: async () => JSON.parse(body),
          };
          const httpRes = {
            status: (code: number) => {
              res.statusCode = code;
              return httpRes;
            },
            json: (data: unknown) => {
              res.setHeader('Content-Type', 'application/json');
              res.end(JSON.stringify(data));
            },
            setHeader: (k: string, v: string) => res.setHeader(k, v),
            write: (data: string) => res.write(data),
            end: () => res.end(),
            flushHeaders: () => res.flushHeaders(),
          };
          proxyHandler(httpReq, httpRes).catch((err) => {
            res.statusCode = 500;
            res.end(JSON.stringify({ error: (err as Error).message }));
          });
        });
      });

      server.listen(0, '127.0.0.1', () => {
        const addr = server.address() as { port: number };
        this._proxyServer = server;
        this._proxyPort = addr.port;
        this.logger.info(`Auto-proxy started on port ${addr.port} (gateway: ${gatewayUrl})`);
        resolve(addr.port);
      });

      server.on('error', (err) => {
        this.logger.error('Failed to start proxy server:', err.message);
        resolve(null);
      });
    });
  }

  // ─── Private ───────────────────────────────────────────────────────────

  private _persistSession(name: string, managed: ManagedSession): void {
    const resumeSessionId = this._managedResumeId(managed);
    const existing = this.persistedSessions.get(name);
    if (!resumeSessionId) {
      if (
        managed.config.engine === 'agy' &&
        existing?.agentGeneration === undefined &&
        this.persistedSessions.delete(name)
      ) {
        this._debouncedSave();
      }
      return;
    }
    managed.claudeSessionId = resumeSessionId;
    this.persistedSessions.set(name, {
      name,
      claudeSessionId: resumeSessionId,
      cwd: managed.cwd,
      model: managed.config.resolvedModel || managed.config.model,
      engine: managed.config.engine,
      sandboxMode: managed.config.sandboxMode,
      originalCreated: existing?.originalCreated || managed.created,
      lastResumed: new Date().toISOString(),
      lastActivity: managed.lastActivity,
      agentGeneration: existing?.agentGeneration,
      agentOwnerInstanceId: existing?.agentOwnerInstanceId,
      agentSessionId: existing?.agentSessionId,
      agentReleasePending: existing?.agentReleasePending,
      agentReleaseOwnerInstanceId: existing?.agentReleaseOwnerInstanceId,
      agentReleasedGeneration: existing?.agentReleasedGeneration,
      agentReleasedOwnerInstanceId: existing?.agentReleasedOwnerInstanceId,
      agentReleasedSessionId: existing?.agentReleasedSessionId,
    });
    this._debouncedSave();
  }

  // ─── PID Tracking ──────────────────────────────────────────────────────

  private static PID_FILE = path.join(os.homedir(), '.openclaw', 'session-pids.json');

  private _savePids(): void {
    try {
      const dir = path.dirname(SessionManager.PID_FILE);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      // The PID file is host-shared: any SessionManager (gateway, dashboard,
      // standalone test runner) writes here. We MUST NOT overwrite entries
      // owned by another live SessionManager process — that would erase its
      // record of pids it spawned, and its next cleanup pass might decide
      // they're orphans and kill them. Read-merge-write keyed by ownerPid.
      let existing: Record<string, unknown> = {};
      try {
        existing = JSON.parse(fs.readFileSync(SessionManager.PID_FILE, 'utf8')) as Record<string, unknown>;
      } catch {
        /* missing or malformed — start fresh */
      }
      const merged: Record<string, { pid: number; ownerPid: number; since: string }> = {};
      const now = new Date().toISOString();
      // Keep entries from OTHER LIVE owners untouched. Entries whose
      // ownerPid is a dead process are stale bookkeeping — drop them so
      // the file doesn't grow unboundedly across server restarts. The
      // child processes those entries used to track were already reaped
      // by _cleanupOrphanedPids (which runs at SessionManager init,
      // before the first save).
      for (const [name, raw] of Object.entries(existing)) {
        if (typeof raw === 'number') continue; // legacy format — drop on first save
        const entry = raw as { pid?: number; ownerPid?: number; since?: string };
        if (typeof entry.pid !== 'number' || typeof entry.ownerPid !== 'number') continue;
        if (entry.ownerPid === process.pid) continue; // ours; we're about to rewrite
        try {
          process.kill(entry.ownerPid, 0);
        } catch {
          continue; // owner dead — stale entry, drop it
        }
        merged[name] = {
          pid: entry.pid,
          ownerPid: entry.ownerPid,
          since: entry.since ?? now,
        };
      }
      // Add OUR current entries
      for (const [name, pid] of this._activePids) {
        merged[name] = { pid, ownerPid: process.pid, since: now };
      }
      fs.writeFileSync(SessionManager.PID_FILE, JSON.stringify(merged));
    } catch {
      /* best effort */
    }
  }

  /**
   * Verify that a PID belongs to a known coding CLI before killing it.
   * Prevents killing unrelated processes if the OS recycled the PID.
   */
  private _isKnownCliProcess(pid: number): boolean {
    // Match known CLI binaries by basename to avoid false positives
    // (e.g., 'agent' must not match 'ssh-agent' or 'gpg-agent')
    // Anchor each name to executable/path position ((?:^|[/\s])name(?:[\s/]|$))
    // so a hyphenated lookalike ('vim claude-notes.md', 'ssh-agent') can never
    // match, while the real binary ('claude', '/usr/local/bin/claude',
    // 'node /x/claude/cli.js') still does. \b alone treated '-' as a boundary.
    // Built from ENGINE_BINARY_NAMES rather than restated here: this list had
    // fallen a binary behind (no `grok`), and the failure is silent — an orphan
    // that matches nothing is logged as "alive but not a known CLI" and left
    // running for the life of the machine.
    const knownPatterns = [
      ...ENGINE_BINARY_NAMES.map((bin) => new RegExp(`(?:^|[/\\s])${bin}(?:[\\s/]|$)`)),
      /(?:^|\/)agent(?:[\s/]|$)/, // 'agent' only as executable/after a slash (not ssh-agent)
    ];
    try {
      const cmd = execFileSync('ps', ['-p', String(pid), '-o', 'command='], {
        encoding: 'utf8',
        timeout: 3_000,
      }).trim();
      return knownPatterns.some((pattern) => pattern.test(cmd));
    } catch {
      return false; // ps failed — process likely dead or not accessible
    }
  }

  private _cleanupOrphanedPids(): void {
    try {
      if (!fs.existsSync(SessionManager.PID_FILE)) return;
      const data = JSON.parse(fs.readFileSync(SessionManager.PID_FILE, 'utf8')) as Record<string, unknown>;
      for (const [name, raw] of Object.entries(data)) {
        // Resolve entry shape: legacy = number; current = { pid, ownerPid, since }.
        let pid: number;
        let ownerPid: number | null;
        if (typeof raw === 'number') {
          pid = raw;
          ownerPid = null; // unknown owner — treat conservatively (skip kill)
        } else if (raw && typeof raw === 'object') {
          const e = raw as { pid?: number; ownerPid?: number };
          if (typeof e.pid !== 'number') continue;
          pid = e.pid;
          ownerPid = typeof e.ownerPid === 'number' ? e.ownerPid : null;
        } else {
          continue;
        }
        // Cross-process safety: if this PID has a known owner SessionManager
        // and that owner is still alive, the child is NOT an orphan — it's
        // owned by another live manager. Only kill if owner is dead or unknown
        // AND the conservative legacy-format path has been ruled out.
        if (ownerPid !== null && ownerPid !== process.pid) {
          let ownerAlive = false;
          try {
            process.kill(ownerPid, 0);
            ownerAlive = true;
          } catch {
            /* owner dead */
          }
          if (ownerAlive) {
            this.logger.info(`PID ${pid} (session: ${name}) owned by live SessionManager pid=${ownerPid} — skipping`);
            continue;
          }
        } else if (ownerPid === null) {
          // Legacy format with no owner info — too risky to kill if a host
          // shares the file across managers. Skip; the entry will be cleaned
          // up on the next save (read-merge-write drops legacy format).
          this.logger.info(`PID ${pid} (session: ${name}) is legacy-format (no ownerPid) — skipping kill`);
          continue;
        }
        try {
          process.kill(pid, 0); // check if alive
          // Alive — but verify it's actually a coding CLI, not a recycled PID
          if (!this._isKnownCliProcess(pid)) {
            this.logger.info(`PID ${pid} (session: ${name}) is alive but not a known CLI — skipping kill`);
            continue;
          }
          this.logger.info(`Killing orphaned process ${pid} (session: ${name})`);
          // Graceful shutdown: SIGTERM first
          try {
            process.kill(-pid, 'SIGTERM');
          } catch {
            /* group kill failed */
          }
          try {
            process.kill(pid, 'SIGTERM');
          } catch {
            /* individual kill failed */
          }
          // Give process time to shut down, then SIGKILL
          const orphanSigkill = setTimeout(() => {
            try {
              process.kill(pid, 0);
              process.kill(-pid, 'SIGKILL');
            } catch {
              /* already dead or group kill failed */
            }
            try {
              process.kill(pid, 0);
              process.kill(pid, 'SIGKILL');
            } catch {
              /* already dead */
            }
          }, STOP_SIGKILL_DELAY_MS);
          orphanSigkill.unref(); // force-kill fallback must not keep the loop alive
        } catch {
          // Process already dead — nothing to do
        }
      }
    } catch {
      /* file doesn't exist or parse error */
    }
    // Clear the PID file
    this._savePids();
  }

  // Circuit breaker is delegated to this._circuitBreaker (src/circuit-breaker.ts)

  private _getSession(name: string): ManagedSession {
    const managed = this.sessions.get(name);
    if (!managed) throw new Error(`Session '${name}' not found`);
    return managed;
  }

  private _toSessionInfo(name: string, managed: ManagedSession): SessionInfo {
    const stats = managed.session.getStats();
    const resumeSessionId = this._managedResumeId(managed);
    if (resumeSessionId) managed.claudeSessionId = resumeSessionId;
    const costUsd = this._spentUsd(managed);
    const info: SessionInfo = {
      name,
      claudeSessionId: resumeSessionId,
      created: managed.created,
      cwd: managed.cwd,
      model: managed.config.resolvedModel || managed.config.model,
      paused: false,
      stats,
      costUsd: Math.round(costUsd * 10000) / 10000,
    };
    if (managed.config.maxBudgetUsd) {
      info.budgetUsd = managed.config.maxBudgetUsd;
      info.budgetExhausted = managed.budgetExhausted || isBudgetExceeded(costUsd, managed.config.maxBudgetUsd);
    }
    return info;
  }

  private _resolveModel(alias: string, overrides?: Record<string, string>): string {
    if (overrides?.[alias]) return overrides[alias];
    return resolveAlias(alias);
  }

  private _managedResumeId(managed: ManagedSession): string | undefined {
    return (
      this._sessionResumeId(managed.config.engine, managed.session) ||
      this._storedResumeId(managed.config.engine, managed.claudeSessionId)
    );
  }

  /**
   * Return only IDs that can actually resume the engine. Agy and Codex expose
   * harvested conversation/thread IDs; their BaseOneShot sessionId values are
   * synthetic wrapper identifiers and must never be persisted for resume.
   */
  private _sessionResumeId(engine: EngineType | undefined, session: ISession): string | undefined {
    if (engine === 'agy') {
      const conversationId = (session as { conversationId?: string }).conversationId;
      return isAgyConversationId(conversationId) ? conversationId : undefined;
    }
    if (engine === 'codex') {
      return (session as { threadId?: string }).threadId;
    }
    return session.sessionId;
  }

  private _storedResumeId(engine: EngineType | undefined, id: string | undefined): string | undefined {
    if (engine === 'agy') return isAgyConversationId(id) ? id : undefined;
    if (engine === 'codex') return id && !/^codex-\d+-/.test(id) ? id : undefined;
    if (engine === 'grok') return id && !/^grok-\d+-/.test(id) ? id : undefined;
    return id;
  }

  private _listMdFiles(dir: string): AgentInfo[] {
    if (!fs.existsSync(dir)) return [];
    return fs
      .readdirSync(dir)
      .filter((f) => f.endsWith('.md'))
      .map((f) => {
        const content = fs.readFileSync(path.join(dir, f), 'utf8');
        const match = content.match(/^---\n[\s\S]*?description:\s*(.+)/m);
        return { name: f.replace('.md', ''), file: f, description: match?.[1]?.trim() || '' };
      });
  }

  private _createSession(engine: EngineType, config: SessionConfig): ISession {
    // A `customEngine` given as a preset id is expanded here, once, so every
    // consumer downstream sees a config object rather than having to know both
    // shapes. An unknown id throws instead of falling through to the default
    // engine, which would silently run something the caller did not ask for.
    if (typeof config.customEngine === 'string') {
      config = { ...config, customEngine: resolveCustomEngine(config.customEngine) };
    }
    switch (engine) {
      case 'gemini':
        return new PersistentGeminiSession(config, process.env.GEMINI_BIN);
      case 'agy':
        return new PersistentAgySession(config, process.env.AGY_BIN);
      case 'codex':
        return new PersistentCodexSession(config, process.env.CODEX_BIN);
      case 'codex-app':
        return new PersistentCodexAppServerSession(config, process.env.CODEX_BIN);
      case 'cursor':
        return new PersistentCursorSession(config, process.env.CURSOR_BIN);
      case 'grok':
        return new PersistentGrokSession(config, process.env.GROK_BIN);
      case 'opencode':
        return new PersistentOpencodeSession(config, process.env.OPENCODE_BIN);
      case 'custom':
        if (!config.customEngine) throw new Error('customEngine config is required for engine type "custom"');
        return new PersistentCustomSession(config);
      case 'claude':
      default:
        return new PersistentClaudeSession(config, this.pluginConfig.claudeBin);
    }
  }

  // ─── Council ──────────────────────────────────────────────────────────
  //
  // The council's lifecycle belongs to the run kernel now. What used to live
  // here — a `Map` of live `Council` objects, a 30-minute TTL timer per entry,
  // and a `councilList` that regex-scraped markdown transcripts to see runs from
  // other processes — is gone. A council is a one-node workflow; its state is
  // the run record, which is durable, cross-process, and does not evaporate.
  //
  // What still needs a live object is in-flight control: `inject` and `abort`
  // have to reach the `Council` instance that is running right now. The kernel
  // publishes it for the duration of the node, and says so honestly — after a
  // restart the run is readable and resumable, but there is no turn to inject
  // into.

  async councilStart(task: string, config: CouncilConfig): Promise<CouncilSession> {
    const runId = `council-${Date.now().toString(36)}-${randomUUID().slice(0, 8)}`;
    const { agents, secrets } = splitAgentSecrets(config.agents);
    const record = await this.kernel.start(
      legacyCouncilWorkflow({
        task,
        cwd: config.projectDir,
        agents,
        maxRounds: config.maxRounds,
        timeoutMs: config.agentTimeoutMs,
        maxTurnsPerAgent: config.maxTurnsPerAgent,
        maxBudgetUsd: config.maxBudgetUsd,
        defaultPermissionMode: config.defaultPermissionMode,
      }),
      { runId, cwd: config.projectDir, secrets: { agentCustomEngines: secrets } },
    );
    return toCouncilSession(record);
  }

  councilStatus(id: string): CouncilSession | undefined {
    const record = loadRun(id);
    if (!record || record.workflow !== 'council') return undefined;
    return toCouncilSession(record);
  }

  /**
   * Every council this machine has run, newest first.
   *
   * Cross-process visibility used to come from scraping `~/.openclaw/council-logs/*.md`
   * with a regex and fabricating a stub session with no responses and an empty
   * config. Runs are stored records now, so the dashboard sees the real thing.
   */
  councilList(): CouncilSession[] {
    return this.kernel
      .list({ workflow: 'council' })
      .map((r) => loadRun(r.runId))
      .filter((r): r is RunRecord => Boolean(r))
      .map(toCouncilSession);
  }

  /** Used by embedded-server to subscribe to a council's event stream. */
  getCouncil(id: string): Council | undefined {
    return this.kernel.handle<Council>(id, LEGACY_NODE);
  }

  /** The live council for a run, or a clear error about why there isn't one. */
  private _liveCouncil(id: string): Council {
    const council = this.kernel.handle<Council>(id, LEGACY_NODE);
    if (council) return council;
    const record = loadRun(id);
    if (!record) throw new Error(`Council '${id}' not found`);
    throw new Error(
      `Council '${id}' is ${record.state} and not running in this process — its record is readable, but there is no live round to act on`,
    );
  }

  councilAbort(id: string): void {
    // Cancel the run first so the kernel stops advancing, then abort the engine
    // so the current round tears down its worktrees.
    if (!this.kernel.cancel(id) && !loadRun(id)) throw new Error(`Council '${id}' not found`);
    this.kernel.handle<Council>(id, LEGACY_NODE)?.abort();
  }

  councilInject(id: string, message: string): void {
    this._liveCouncil(id).injectMessage(message);
  }

  async councilReview(id: string): Promise<CouncilReviewResult> {
    return this._councilForPostProcessing(id).review();
  }

  async councilAccept(id: string): Promise<CouncilAcceptResult> {
    return this._councilForPostProcessing(id).accept();
  }

  async councilReject(id: string, feedback: string): Promise<CouncilRejectResult> {
    return this._councilForPostProcessing(id).reject(feedback);
  }

  /**
   * A `Council` for review / accept / reject.
   *
   * These three act on the git state a finished council left behind — branches,
   * worktrees, plan.md — so they do not need the instance that produced it, only
   * one pointed at the same project directory. Reconstructing from the run
   * record is what makes them work after a restart, which the in-memory map made
   * impossible.
   */
  private _councilForPostProcessing(id: string): Council {
    const live = this.kernel.handle<Council>(id, LEGACY_NODE);
    if (live) return live;
    const record = loadRun(id);
    if (!record || record.workflow !== 'council') throw new Error(`Council '${id}' not found`);
    const session = toCouncilSession(record);
    const council = new Council(session.config, this, this.logger);
    council.adoptSession(session);
    return council;
  }

  // ─── Fan-out (parallel multi-engine task, no consensus) ────────────────
  //
  // Also a one-node workflow. This is the mode the old design failed hardest:
  // a fan-out wrote nothing to disk at all, so 30 minutes after it finished
  // `fanoutStatus` threw "not found" and the results were simply gone.

  /**
   * Start a fan-out: run the task across N engine/model agents in parallel and
   * collect their answers (optional synthesis). Runs in the background; poll
   * with fanoutStatus. Distinct from council — no rounds, votes, or worktrees.
   */
  async fanoutStart(config: FanoutConfig): Promise<FanoutSession> {
    if (!config.agents?.length) throw new Error('fanoutStart: at least one agent is required');
    const names = config.agents.map((a) => a.name);
    if (new Set(names).size !== names.length) {
      throw new Error('fanoutStart: agent names must be unique (they form session names)');
    }
    const runId = `fanout-${Date.now().toString(36)}-${randomUUID().slice(0, 8)}`;
    const { agents, secrets } = splitAgentSecrets(config.agents);
    const record = await this.kernel.start(
      legacyFanoutWorkflow({
        task: config.task,
        cwd: config.projectDir,
        agents,
        synthesize: config.synthesize,
        synthesisEngine: config.synthesisEngine,
        synthesisModel: config.synthesisModel,
        synthesisPermissionMode: config.synthesisPermissionMode,
        maxTurnsPerAgent: config.maxTurnsPerAgent,
        maxBudgetUsd: config.maxBudgetUsd,
        timeoutMs: config.agentTimeoutMs,
      }),
      { runId, cwd: config.projectDir, secrets: { agentCustomEngines: secrets } },
    );
    return toFanoutSession(record);
  }

  fanoutStatus(id: string): FanoutSession {
    const record = loadRun(id);
    if (!record) throw new Error(`Fanout '${id}' not found`);
    return toFanoutSession(record);
  }

  fanoutAbort(id: string): void {
    if (!loadRun(id)) throw new Error(`Fanout '${id}' not found`);
    this.kernel.cancel(id);
    this.kernel.handle<Fanout>(id, LEGACY_NODE)?.abort();
  }

  // ─── Inbox (cross-session messaging) — delegated to InboxManager ────

  private get _sessionLookup(): SessionLookup {
    return {
      getSession: (name) => this.sessions.get(name),
      exists: (name) => this.sessions.has(name),
      allNames: () => this.sessions.keys(),
    };
  }

  async sessionSendTo(
    from: string,
    to: string,
    message: string,
    summary?: string,
  ): Promise<{ delivered: boolean; queued: boolean }> {
    return this._inbox.sendTo(from, to, message, this._sessionLookup, summary, (name, err) => {
      this.logger.error(`Broadcast delivery to '${name}' failed:`, err.message);
    });
  }

  sessionInbox(name: string, unreadOnly = true): InboxMessage[] {
    return this._inbox.inbox(name, unreadOnly);
  }

  async sessionDeliverInbox(name: string): Promise<number> {
    return this._inbox.deliverInbox(name, this._sessionLookup);
  }

  // ─── Ultraplan ────────────────────────────────────────────────────────
  //
  // A one-node workflow. What is gone: a `Map` of results, and an inline
  // 30-minute timer that doubled as the timeout — a plan still running when the
  // TTL fired was rewritten as `error: 'Timed out (TTL expired)'` and then
  // deleted, so a long plan could be destroyed by its own eviction timer. The
  // node's `timeoutMs` is the timeout now, and the record does not expire.

  private _kernel: RunKernel | null = null;
  /**
   * Deferreds resolved by the `autoloop` node once its engine is up, so
   * `autoloopStart` can return the Planner session name the caller expects
   * without polling.
   */
  /**
   * Deferreds resolved by the `autoloop` node once its engine is up.
   *
   * Keyed by the start's tag rather than its run id. A run id gets reused — a
   * start that failed frees it for a retry — so keying on the id let a dying
   * start settle, or clear, the retry's deferred instead of its own, and the
   * retry then waited forever for a signal with nowhere to land.
   */
  private _autoloopReady = new Map<
    string,
    { resolve: (v: { plannerSession: string; state: AutoloopState }) => void; reject: (e: Error) => void }
  >();
  /**
   * Run ids with a start in flight — the window between "run created" and
   * "engine up". Deleting inside it would drop the run while its Planner
   * session is still being created, orphaning a session that finishes a moment
   * later with nothing pointing at it.
   */
  private _autoloopStarting = new Map<string, string>();
  /** Latest role selection per run, published into the node payload. */
  private _autoloopSelection = new Map<string, unknown>();
  /** Per-run checkpoint refreshers, registered by the autoloop node executor. */
  private _autoloopPublishers = new Map<string, () => void>();
  /**
   * Per-run Planner-chat transaction tails. Dispatcher reply/phase-error events
   * are run-scoped rather than message-scoped, so only one listener pair may
   * own a run at a time.
   */
  private _autoloopChatTransactions = new Map<string, Promise<void>>();
  private _autoloopReviewTransactions = new Map<string, Promise<void>>();
  /** One in-process recovery transaction per logical run and token; durable receipts fence restarts. */
  private _autoloopRecoveryTransactions = new Map<string, Promise<RecoveryResult>>();
  /** One in-process receipt-free stored resume per logical run boundary. */
  private _autoloopStoredResumeTransactions = new Map<string, Promise<AutoloopState>>();
  private _autoloopReleasedReviewIterations = new Map<string, Map<string, number>>();
  private _autoloopReviewDeleting = new Set<string>();
  private _autoloopReviewDeleteCounts = new Map<string, number>();
  /** Logical Planner chat identity and any causally observed Dispatcher audit. */
  private _autoloopFailureBindings = new WeakMap<object, Readonly<AutoloopChatFailureBinding>>();
  /** Successfully published logical bindings, including projections that later roll out of state. */
  private _completedAutoloopFailureBindings = new WeakSet<object>();
  /** Stable retry id for direct callers whose failure has no Planner chat id. */
  private _detachedAutoloopFailureIds = new WeakMap<object, string>();
  /** In-flight logical recordings; settled entries are always removed. */
  private _detachedAutoloopFailureRecordings = new Map<
    string,
    Promise<Readonly<PublicAutoloopFailure | PublicAutoloopUnknownFailure>>
  >();

  async ultraplanStart(
    task: string,
    opts?: { model?: string; cwd?: string; timeout?: number },
  ): Promise<UltraplanResult> {
    const runId = `ultraplan-${Date.now().toString(36)}-${randomUUID().slice(0, 8)}`;
    const cwd = opts?.cwd || process.cwd();
    const record = await this.kernel.start(
      legacyUltraplanWorkflow({
        task,
        cwd,
        model: opts?.model || 'opus',
        timeoutMs: opts?.timeout || ULTRAPLAN_TIMEOUT_MS,
      }),
      { runId, cwd },
    );
    return toUltraplanResult(record, undefined);
  }

  ultraplanStatus(id: string): UltraplanResult | undefined {
    const record = loadRun(id);
    if (!record || record.workflow !== 'ultraplan') return undefined;
    // Read the plan from the node artifact, not the record's preview: a plan is
    // routinely longer than the inline cap, and returning a truncated one would
    // quietly hand back a broken deliverable.
    return toUltraplanResult(record, readNodeOutput(id, LEGACY_NODE));
  }

  // ─── Ultrareview ──────────────────────────────────────────────────────

  // No map and no poller. Ultrareview used to hold its results in a `Map`, then
  // `setInterval` every 5 seconds asking the fan-out whether it had finished —
  // which meant its correctness depended on the fan-out's 30-minute eviction
  // timer: evict first and the poll threw, the interval was cleared, and the
  // review stayed `running` forever. It is one run now, and there is nothing to
  // poll.
  async ultrareviewStart(
    cwd: string,
    opts?: {
      agentCount?: number;
      maxDurationMinutes?: number;
      model?: string;
      focus?: string;
      engines?: EngineType[];
    },
  ): Promise<UltrareviewResult> {
    const id = `ultrareview-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const agentCount = Math.min(20, Math.max(1, opts?.agentCount || 5));

    const result: UltrareviewResult = {
      id,
      status: 'running',
      councilId: '',
      agentCount,
      startTime: new Date().toISOString(),
    };

    // Build reviewer agents
    const reviewAngles = [
      {
        name: 'SecurityReviewer',
        emoji: '🔒',
        persona:
          'You are a security expert. Focus on: injection vulnerabilities, auth flaws, data exposure, OWASP top 10, secrets in code.',
      },
      {
        name: 'LogicReviewer',
        emoji: '🧠',
        persona:
          'You are a logic analyst. Focus on: off-by-one errors, race conditions, null/undefined handling, edge cases, incorrect assumptions.',
      },
      {
        name: 'PerformanceReviewer',
        emoji: '⚡',
        persona:
          'You are a performance engineer. Focus on: O(n^2) loops, memory leaks, unnecessary allocations, missing caching, N+1 queries.',
      },
      {
        name: 'APIReviewer',
        emoji: '🔌',
        persona:
          'You are an API design reviewer. Focus on: inconsistent interfaces, missing validation, error handling gaps, backwards compatibility.',
      },
      {
        name: 'TestReviewer',
        emoji: '🧪',
        persona:
          'You are a test coverage analyst. Focus on: untested code paths, missing edge case tests, flaky test patterns, assertion quality.',
      },
      {
        name: 'TypeReviewer',
        emoji: '📐',
        persona:
          'You are a type safety reviewer. Focus on: any casts, unsafe assertions, missing null checks, generic misuse, type narrowing gaps.',
      },
      {
        name: 'ConcurrencyReviewer',
        emoji: '🔀',
        persona:
          'You are a concurrency expert. Focus on: race conditions, deadlocks, shared state mutations, async error handling, promise leaks.',
      },
      {
        name: 'ErrorReviewer',
        emoji: '💥',
        persona:
          'You are an error handling reviewer. Focus on: swallowed errors, missing try/catch, unhelpful error messages, crash-on-startup paths.',
      },
      {
        name: 'DependencyReviewer',
        emoji: '📦',
        persona:
          'You are a dependency auditor. Focus on: outdated packages, known CVEs, unnecessary dependencies, license issues.',
      },
      {
        name: 'ReadabilityReviewer',
        emoji: '📖',
        persona:
          'You are a readability reviewer. Focus on: unclear naming, complex functions, missing context, dead code, confusing control flow.',
      },
      {
        name: 'DataReviewer',
        emoji: '💾',
        persona:
          'You are a data integrity reviewer. Focus on: data validation, schema mismatches, migration issues, encoding problems, data loss paths.',
      },
      {
        name: 'ConfigReviewer',
        emoji: '⚙️',
        persona:
          'You are a configuration reviewer. Focus on: hardcoded values, missing env vars, insecure defaults, missing fallbacks.',
      },
      {
        name: 'ScalabilityReviewer',
        emoji: '📈',
        persona:
          'You are a scalability reviewer. Focus on: single points of failure, stateful bottlenecks, missing pagination, unbounded growth.',
      },
      {
        name: 'DocReviewer',
        emoji: '📝',
        persona:
          'You are a documentation reviewer. Focus on: outdated docs, missing API docs, misleading comments, undocumented behavior.',
      },
      {
        name: 'A11yReviewer',
        emoji: '♿',
        persona:
          'You are an accessibility reviewer. Focus on: missing ARIA labels, keyboard navigation, color contrast, screen reader support.',
      },
      {
        name: 'I18nReviewer',
        emoji: '🌍',
        persona:
          'You are an i18n reviewer. Focus on: hardcoded strings, locale handling, date/number formatting, RTL support.',
      },
      {
        name: 'NetworkReviewer',
        emoji: '🌐',
        persona:
          'You are a network reviewer. Focus on: missing timeouts, retry logic, connection pooling, request size limits.',
      },
      {
        name: 'AuthReviewer',
        emoji: '🔑',
        persona:
          'You are an auth reviewer. Focus on: token handling, session management, CSRF protection, permission checks.',
      },
      {
        name: 'CryptoReviewer',
        emoji: '🔐',
        persona:
          'You are a cryptography reviewer. Focus on: weak algorithms, key management, random number generation, hash collisions.',
      },
      {
        name: 'MemoryReviewer',
        emoji: '🧹',
        persona:
          'You are a memory reviewer. Focus on: memory leaks, circular references, large object retention, stream handling.',
      },
    ];

    const maxMinutes = Math.min(25, Math.max(5, opts?.maxDurationMinutes || 10));
    const focus = opts?.focus || 'Find bugs, security issues, and code quality problems';
    const reviewInstruction =
      `# Code Review Task\n\nReview the codebase in this project. ${focus}.\n\n` +
      `Examine the code from your specialty angle and report bugs found with file paths and line numbers.`;

    // Cross-engine review: round-robin the requested engines across reviewers
    // (default claude-only — unchanged behavior). Each reviewer's persona is
    // its prompt; per-agent failures are isolated by the fan-out runner.
    const engines = opts?.engines?.length ? opts.engines : (['claude'] as EngineType[]);
    const agents: FanoutAgentSpec[] = reviewAngles.slice(0, agentCount).map((a, i) => ({
      name: a.name,
      engine: engines[i % engines.length],
      model: opts?.model,
      prompt: `${a.persona}\n\n${reviewInstruction}`,
      // Review is read-only: keep reviewers out of edit mode so they analyse and
      // report without modifying the very code they review. (Unlike council,
      // fan-out shares the project dir — there is no worktree to sandbox edits.
      // `plan` constrains the claude engine; non-claude reviewers, which are
      // opt-in via `engines`, run under their engine's default sandbox.)
      permissionMode: 'plan',
    }));

    const runId = id;
    await this.kernel.start(
      legacyFanoutWorkflow({
        name: 'ultrareview',
        task: reviewInstruction,
        cwd,
        // Each reviewer's own prompt and `permissionMode: 'plan'` travel with it.
        // They were being dropped, so every reviewer got the shared task under
        // `bypassPermissions` — a read-only review that could edit the code.
        agents,
        synthesize: true,
        // The synthesiser reads the reviewers' text, not the code, and it shares
        // the project directory — so it is held to the same read-only rule. It
        // was not, which meant an ultrareview could still write through its
        // final pass.
        synthesisPermissionMode: 'plan',
        maxTurnsPerAgent: 20,
        timeoutMs: maxMinutes * 60 * 1000,
      }),
      { runId, cwd },
    );
    // `councilId` is kept for the UltrareviewResult contract; it holds the run
    // id, which is also the fan-out id — they are the same run now.
    result.councilId = runId;
    return result;
  }

  ultrareviewStatus(id: string): UltrareviewResult | undefined {
    const record = loadRun(id);
    if (!record || record.workflow !== 'ultrareview') return undefined;
    const data = record.nodes[LEGACY_NODE]?.data as FanoutNodeData | undefined;
    return toUltrareviewResult(record, joinFindings(data));
  }

  // ─── Autoloop (three-agent architecture) ───────────────────────────

  // No map, no registry file, and no start/delete fences.
  //
  // What used to live here: `autoloops`, holding the live runner and dispatcher;
  // `_deletingAutoloops` and `_startingAutoloops`, two `Set`s that existed only
  // because a start and a delete could race each other over that map; and four
  // bespoke helpers over `~/.claw-orchestrator/autoloop-registry.jsonl` for
  // cross-process listing. A run has exactly one owner now, run ids collide in
  // the run store rather than in a map that only saw this process, and the
  // record is the registry.

  /**
   * Build and start the Planner/Coder/Reviewer engine for a run.
   *
   * This is everything `autoloopStart` used to be except the bookkeeping: the
   * `autoloops` map and the private JSONL registry are gone, and the kernel owns
   * the lifecycle. Called from the `autoloop` node executor, which holds the
   * returned objects for as long as the loop runs.
   */
  /**
   * Resolve until the loop stops.
   *
   * The runner is an event emitter, not a promise: it settles when a
   * `terminate` envelope is drained or the phase-error circuit trips. Cancelling
   * the run stops it too, which is what makes `workflow_cancel` work on an
   * autoloop.
   */
  private _awaitAutoloopExit(handle: AutoloopHandle, signal: { aborted: boolean }): Promise<void> {
    return new Promise<void>((resolve) => {
      const runner = handle.runner as unknown as {
        state: AutoloopState;
        on(event: string, fn: () => void): void;
        off(event: string, fn: () => void): void;
        stop(): void;
        waitForTermination(): Promise<void>;
      };
      const done = (): boolean => runner.state.status === 'terminated' || runner.state.status === 'crashed';
      const finishNaturalExit = (): void => {
        if (runner.state.status === 'crashed') {
          resolve();
          return;
        }
        // `terminated` is published when teardown starts. Keep the kernel node
        // live until the runner's dispatcher shutdown has actually settled so
        // SessionManager shutdown cannot close release admission too early.
        void runner.waitForTermination().then(resolve, resolve);
      };
      if (done()) {
        finishNaturalExit();
        return;
      }
      let settling = false;
      const check = (): void => {
        if ((!done() && !signal.aborted) || settling) return;
        settling = true;
        runner.off('state', check);
        clearInterval(poll);
        if (!signal.aborted) {
          finishNaturalExit();
          return;
        }
        // Cancelling a run has to tear the loop down the way a stop does.
        // Without this the three persistent agents keep running and their
        // session names stay claimed, so the run cannot be restarted — the
        // failure looks like "session name already in use" a long way from
        // its cause. The kernel exit remains pending through that teardown so
        // SessionManager shutdown cannot close its release-admission fence
        // while the dispatcher is still releasing physical generations.
        runner.stop();
        void handle.dispatcher
          .shutdown('cancelled')
          .catch(() => undefined)
          .then(() => resolve());
      };
      runner.on('state', check);
      // The runner emits on state changes, but a cancel arrives out of band and
      // a crashed loop may emit nothing at all, so poll as the backstop.
      const poll = setInterval(check, 1000);
      if (typeof poll.unref === 'function') poll.unref();
    });
  }

  private async _bootAutoloop(opts: {
    runId: string;
    workspace: string;
    plannerPromptPath?: string;
    plannerEngine?: EngineType;
    plannerModel?: string;
    plannerCustomEngine?: CustomEngineConfig;
    coderEngine?: EngineType;
    coderModel?: string;
    coderCustomEngine?: CustomEngineConfig;
    reviewerEngine?: EngineType;
    reviewerModel?: string;
    reviewerCustomEngine?: CustomEngineConfig;
    sendTimeoutMs?: number;
    activityLeaseMs?: number;
    autoloopHardTimeoutMs?: number;
    /** Internal restart marker: a failed timeout migration must not append a
     *  cleanup decision or purge the resumable session registry. Never stored. */
    _resumeTimeoutMigration?: boolean;
    /** In-memory commit barrier for a prepared append-only migration record. */
    _commitTimeoutMigration?: () => void;
    /** Run-scoped capability pinned before a stored resume transaction starts. */
    _secureLedger?: SecureAutoloopLedger;
  }): Promise<{
    runner: AutoloopRunner;
    dispatcher: ClaudeAgentDispatcher;
    ledgerDir: string;
    pushPolicy: PushPolicy;
  }> {
    const plannerEngine = validateAutoloopRole('planner', opts.plannerEngine, opts.plannerCustomEngine);
    const coderEngine = validateAutoloopRole('coder', opts.coderEngine, opts.coderCustomEngine);
    const reviewerEngine = validateAutoloopRole('reviewer', opts.reviewerEngine, opts.reviewerCustomEngine);
    const secureLedger =
      opts._secureLedger ??
      SecureAutoloopLedger.open(opts.workspace, opts.runId, {
        create: true,
        logger: this.logger,
      });
    // A resume may carry a capability opened before the runtime was booted.
    // Revalidate its pinned path and every existing flat ledger before any
    // physical agent session can start.
    secureLedger.assertIdentity();
    secureLedger.validateExistingFlatFiles();
    const ledgerDir = secureLedger.directory;
    // Per-run policy object — mutable so Planner's update_push_policy is visible
    // to the runner without re-wiring.
    const pushPolicy: PushPolicy = JSON.parse(JSON.stringify(DEFAULT_PUSH_POLICY)) as PushPolicy;
    const runId = opts.runId;
    let runnerRef: AutoloopRunner | null = null;
    let dispatcherRef: ClaudeAgentDispatcher | null = null;
    const dispatcherConfig: ClaudeAgentDispatcherConfig = {
      manager: this,
      runId: opts.runId,
      workspace: opts.workspace,
      plannerPromptPath: opts.plannerPromptPath,
      plannerEngine,
      plannerModel: opts.plannerModel,
      plannerCustomEngine: opts.plannerCustomEngine,
      coderEngine,
      coderModel: opts.coderModel,
      coderCustomEngine: opts.coderCustomEngine,
      reviewerEngine,
      reviewerModel: opts.reviewerModel,
      reviewerCustomEngine: opts.reviewerCustomEngine,
      sendTimeoutMs: opts.sendTimeoutMs,
      agentLeaseMs: opts.activityLeaseMs,
      runtimeProbe: this,
      ownerInstanceId: this.autoloopOwnerInstanceId,
      suppressFailedStartAudit: opts._resumeTimeoutMigration,
      secureLedger,
      logger: this.logger,
      pushPolicyRef: pushPolicy,
      onSpawnSubagents: async (args) => {
        this.logger.info?.(`[autoloop/${runId}] spawn_subagents starting Coder + Reviewer sessions`);
        await dispatcherRef?.spawnSubagents(args);
      },
      onSpawnSubagentsCommitted: () => runnerRef?.markSubagentsSpawned(),
      onRoleSelectionChanged: async (selection) => {
        // Used to write a row into a private append-only registry file. The run
        // record is the registry now, so this just refreshes the published
        // payload the `autoloop_status` projection reads.
        this._autoloopSelection.set(runId, selection);
        this._autoloopPublishers.get(runId)?.();
      },
    };
    const dispatcher = new ClaudeAgentDispatcher(dispatcherConfig);
    dispatcherRef = dispatcher;
    const runner = new AutoloopRunner({
      run_id: opts.runId,
      workspace: opts.workspace,
      ledger_dir: ledgerDir,
      push_policy: pushPolicy,
      notifyUser: async (level: PushLevel, summary: string, detail, channel: PushChannel) => {
        const result = await notifyUserFallbackChain({
          level,
          summary,
          detail,
          channel,
          logger: this.logger,
        });
        appendPushLog(
          secureLedger,
          {
            ts: new Date().toISOString(),
            level,
            summary,
            detail,
            channel_requested: channel,
            channel_used: result.channel_used,
          },
          this.logger,
        );
        this.logger.info?.(
          `[autoloop/${runId}] push level=${level} channel=${channel}→${result.channel_used} summary="${summary.slice(0, 80)}"`,
        );
      },
      dispatcher,
      persistReviewEnvelope: async (envelope) => {
        this._appendRecoveryReviewEnvelope(secureLedger, runId, envelope);
      },
      sendTimeoutMs: opts.sendTimeoutMs,
      activityLeaseMs: opts.activityLeaseMs,
      autoloopHardTimeoutMs: opts.autoloopHardTimeoutMs,
    });
    runnerRef = runner;
    try {
      await runner.start();
      // A reconstructed migration becomes externally visible only after its
      // Planner is ready and its prepared audit append has committed. Keeping
      // this inside the startup try makes an append failure follow the same
      // cleanup path as any other failed resume.
      opts._commitTimeoutMigration?.();
    } catch (err) {
      try {
        await dispatcher.shutdown('start-failed', {
          purge: !opts._resumeTimeoutMigration,
        });
      } catch (cleanupErr) {
        this.logger.warn?.(`[autoloop/${runId}] cleanup after failed start failed: ${(cleanupErr as Error).message}`);
      }
      runner.stop();
      throw err;
    }
    return { runner, dispatcher, ledgerDir, pushPolicy };
  }

  /**
   * Start a v2 autoloop in chat mode. Creates the Planner persistent session,
   * returns the run handle. Coder/Reviewer are NOT started until S3's
   * spawn_subagents tool is called.
   *
   * The run is a kernel run whose single `autoloop` node holds the loop for as
   * long as it lives. That is what replaced the `autoloops` map, the
   * `autoloop-registry.jsonl` file with its four bespoke read/write helpers, and
   * the two `Set`s that fenced start against delete: a run has one owner now,
   * and `runId` collisions are refused by the run store rather than by a map
   * lookup that only saw this process.
   */
  async autoloopStart(opts: {
    runId: string;
    workspace: string;
    plannerPromptPath?: string;
    plannerEngine?: EngineType;
    plannerModel?: string;
    plannerCustomEngine?: CustomEngineConfig;
    coderEngine?: EngineType;
    coderModel?: string;
    coderCustomEngine?: CustomEngineConfig;
    reviewerEngine?: EngineType;
    reviewerModel?: string;
    reviewerCustomEngine?: CustomEngineConfig;
    sendTimeoutMs?: number;
    activityLeaseMs?: number;
    autoloopHardTimeoutMs?: number;
  }): Promise<{ runId: string; plannerSession: string; state: AutoloopState }> {
    // Fail before the run directory exists, so a rejected start leaves nothing.
    validateAutoloopTimeoutConfig({
      sendTimeoutMs: opts.sendTimeoutMs,
      activityLeaseMs: opts.activityLeaseMs,
      autoloopHardTimeoutMs: opts.autoloopHardTimeoutMs,
    });
    validateAutoloopRole('planner', opts.plannerEngine, opts.plannerCustomEngine);
    validateAutoloopRole('coder', opts.coderEngine, opts.coderCustomEngine);
    validateAutoloopRole('reviewer', opts.reviewerEngine, opts.reviewerCustomEngine);

    const tag = `${opts.runId}:${randomUUID()}`;
    const ready = new Promise<{ plannerSession: string; state: AutoloopState }>((resolve, reject) => {
      this._autoloopReady.set(tag, { resolve, reject });
    });
    this._autoloopStarting.set(tag, opts.runId);
    // Custom-engine configs hold credentials and the spec is written to disk, so
    // they travel in memory. Without this split, `spec.json` contained the token
    // from `CustomEngineConfig.env` in plain text.
    const { plannerCustomEngine, coderCustomEngine, reviewerCustomEngine, ...persistable } = opts;
    await this.kernel.start(
      {
        name: 'autoloop',
        cwd: opts.workspace,
        nodes: [
          {
            id: LEGACY_NODE,
            kind: 'autoloop',
            workspace: opts.workspace,
            config: persistable as Record<string, unknown>,
          },
        ],
      },
      {
        runId: opts.runId,
        cwd: opts.workspace,
        tag,
        secrets: { plannerCustomEngine, coderCustomEngine, reviewerCustomEngine },
      },
    );
    try {
      const { plannerSession, state } = await ready;
      return { runId: opts.runId, plannerSession, state };
    } catch (err) {
      // A start that never came up must not leave the id claimed. The store
      // refuses to reuse a run id, so without this a failed Planner startup
      // would make that id permanently unusable.
      //
      // Tag-guarded: by the time this runs, a retry may already hold the id, and
      // deleting it would take out the run that replaced us.
      this.kernel.delete(opts.runId, { expectTag: tag });
      throw err;
    } finally {
      this._autoloopReady.delete(tag);
      this._autoloopStarting.delete(tag);
    }
  }

  /**
   * Inject a user chat message into a v2 run's Planner. Returns the Planner's
   * natural-language reply.
   */
  async autoloopChat(runId: string, text: string): Promise<{ reply: string }> {
    const predecessor = this._autoloopChatTransactions.get(runId) ?? Promise.resolve();
    let release!: () => void;
    const transaction = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tail = predecessor.then(() => transaction);
    this._autoloopChatTransactions.set(runId, tail);
    await predecessor;
    try {
      return await this._autoloopChatTransaction(runId, text);
    } finally {
      release();
      if (this._autoloopChatTransactions.get(runId) === tail) {
        this._autoloopChatTransactions.delete(runId);
      }
    }
  }

  private _bindAutoloopChatFailure(
    error: unknown,
    logicalId: string,
    ledger: SecureAutoloopLedger,
    decisionLogBefore: string | undefined,
    runnerEntriesBefore: ReadonlySet<object>,
    recentRunnerEntries: readonly AutoloopState['recent_phase_errors'][number][],
    relatedError?: unknown,
  ): unknown {
    let preaudited: DurableDetachedAutoloopFailureRow | undefined;
    let runnerProjection: AutoloopState['recent_phase_errors'][number] | undefined;
    const typed = toPublicAutoloopFailure(error);
    if (typed && decisionLogBefore !== undefined) {
      try {
        const decisionLogAfter = ledger.readFlatFile('decisions.jsonl') ?? '';
        preaudited = newlyAppendedDispatcherPreaudit(decisionLogBefore, decisionLogAfter, detachedPhaseFailure(typed));
      } catch {
        // No exact ledger proof means no preaudit suppression. The detached
        // adapter will append its own identified row instead.
      }
    }
    if (typed) {
      const targetKey = detachedPhaseFailureKey(detachedPhaseFailure(typed));
      for (let index = recentRunnerEntries.length - 1; index >= 0; index -= 1) {
        const entry = recentRunnerEntries[index];
        if (
          !runnerEntriesBefore.has(entry as object) &&
          ownDataValue(entry as object, DETACHED_AUTOLOOP_FAILURE_ID) === undefined &&
          detachedPhaseFailureKey(entry as DetachedAutoloopPhaseFailure) === targetKey
        ) {
          runnerProjection = entry;
          break;
        }
      }
    }
    const binding = publicData({
      logicalId,
      ...(preaudited ? { preaudited } : {}),
      ...(runnerProjection ? { runnerProjection } : {}),
    });
    const pending = [error, relatedError];
    const seen = new Set<object>();
    while (pending.length > 0) {
      const candidate = pending.pop();
      if (!((typeof candidate === 'object' && candidate !== null) || typeof candidate === 'function')) continue;
      const reference = candidate as object;
      if (seen.has(reference)) continue;
      seen.add(reference);
      this._autoloopFailureBindings.set(reference, binding);
      try {
        pending.push(ownDataValue(reference, 'cause'));
      } catch {
        // A hostile cause descriptor cannot invalidate binding the surfaced
        // error itself.
      }
    }
    return error;
  }

  private async _autoloopChatTransaction(runId: string, text: string): Promise<{ reply: string }> {
    const ctx = this._liveAutoloop(runId);
    const chatEnvelope = AutoloopMsg.chat(ctx.runner.state.iter, { text });
    const runnerEntriesBefore = new Set<object>(ctx.runner.state.recent_phase_errors.map((entry) => entry as object));
    let decisionLogBefore: string | undefined;
    try {
      decisionLogBefore = ctx.dispatcher.secureLedgerCapability.readFlatFile('decisions.jsonl') ?? '';
    } catch {
      // Failure to establish an exact pre-send prefix only disables reuse of a
      // Dispatcher row; it must not block the Planner chat itself.
    }
    let reply = '';
    const onReply = (...args: unknown[]) => {
      const t = args[0];
      const identity = args[1] as { message_id?: unknown } | undefined;
      if (typeof t === 'string' && identity?.message_id === chatEnvelope.msg_id) reply = t;
    };
    ctx.dispatcher.on('planner_reply', onReply);
    try {
      try {
        await ctx.runner.send(chatEnvelope);
      } catch (error) {
        let cause: unknown;
        if ((typeof error === 'object' && error !== null) || typeof error === 'function') {
          try {
            cause = ownDataValue(error, 'cause');
          } catch {
            // An inaccessible descriptor is not safe evidence of a committed
            // secure-ledger cause. Bind and surface the original rejection.
          }
        }
        const surfaced = isCommittedSecureLedgerError(cause) ? cause : error;
        throw this._bindAutoloopChatFailure(
          surfaced,
          chatEnvelope.msg_id,
          ctx.dispatcher.secureLedgerCapability,
          decisionLogBefore,
          runnerEntriesBefore,
          ctx.runner.state.recent_phase_errors,
          error,
        );
      }
    } finally {
      ctx.dispatcher.off('planner_reply', onReply);
    }
    const pending = ctx.runner.state.pending_dispatch;
    if (!reply.trim() && pending?.agent === 'planner' && pending.message_id === chatEnvelope.msg_id) {
      throw this._bindAutoloopChatFailure(
        new AutoloopChatStateError(
          'AUTOLOOP_SEND_TIMEOUT',
          `Planner send '${pending.dispatch_id}' reached its deadline and is awaiting explicit resume`,
          true,
          { ...pending },
          ctx.runner.state.status_reason,
        ),
        chatEnvelope.msg_id,
        ctx.dispatcher.secureLedgerCapability,
        decisionLogBefore,
        runnerEntriesBefore,
        ctx.runner.state.recent_phase_errors,
      );
    }
    if (!reply.trim() && (ctx.runner.state.status === 'terminated' || ctx.runner.state.status === 'crashed')) {
      throw this._bindAutoloopChatFailure(
        new AutoloopChatStateError(
          'AUTOLOOP_RUN_TERMINAL',
          `Autoloop run '${runId}' became ${ctx.runner.state.status} before Planner produced a reply`,
          false,
          undefined,
          ctx.runner.state.status_reason,
        ),
        chatEnvelope.msg_id,
        ctx.dispatcher.secureLedgerCapability,
        decisionLogBefore,
        runnerEntriesBefore,
        ctx.runner.state.recent_phase_errors,
      );
    }
    if (!reply.trim() && ctx.runner.state.status === 'paused') {
      throw this._bindAutoloopChatFailure(
        new AutoloopChatStateError(
          'AUTOLOOP_RUN_PAUSED',
          `Autoloop run '${runId}' is paused; Planner chat '${chatEnvelope.msg_id}' remains parked`,
          false,
          undefined,
          ctx.runner.state.status_reason,
        ),
        chatEnvelope.msg_id,
        ctx.dispatcher.secureLedgerCapability,
        decisionLogBefore,
        runnerEntriesBefore,
        ctx.runner.state.recent_phase_errors,
      );
    }
    if (!reply.trim()) {
      throw this._bindAutoloopChatFailure(
        new AutoloopOperationError(
          'AUTOLOOP_EMPTY_REPLY',
          'Planner transport completed without a non-empty logical reply',
        ),
        chatEnvelope.msg_id,
        ctx.dispatcher.secureLedgerCapability,
        decisionLogBefore,
        runnerEntriesBefore,
        ctx.runner.state.recent_phase_errors,
      );
    }
    return { reply };
  }

  /**
   * Complete the fire-and-forget HTTP boundary after its accepted chat rejects.
   * Runner-originated operation failures are already durable; adapter-originated
   * typed failures are recorded once without replaying the original chat.
   */
  async recordDetachedAutoloopChatFailure(
    runId: string,
    error: unknown,
  ): Promise<Readonly<PublicAutoloopFailure | PublicAutoloopUnknownFailure>> {
    const typedFailure = toPublicAutoloopFailure(error);
    const unknownMessage =
      error instanceof Error && typeof ownDataValue(error, 'message') === 'string'
        ? (ownDataValue(error, 'message') as string)
        : 'Autoloop chat failed after the request was accepted';
    const failure = typedFailure ?? publicData({ message: unknownMessage });
    const reference = (typeof error === 'object' && error !== null) || typeof error === 'function' ? error : undefined;
    const binding = reference ? this._autoloopFailureBindings.get(reference) : undefined;
    if (binding && this._completedAutoloopFailureBindings.has(binding)) return failure;
    const transactionLogicalId = binding?.logicalId;
    const phaseFailure = detachedPhaseFailure(failure);
    const reservationKey = transactionLogicalId
      ? `${runId}:logical:${transactionLogicalId}`
      : `${runId}:fallback:${detachedPhaseFailureKey(phaseFailure)}`;
    const existing = this._detachedAutoloopFailureRecordings.get(reservationKey);
    if (existing) return await existing;
    let detachedFailureId = transactionLogicalId;
    if (!detachedFailureId && reference) detachedFailureId = this._detachedAutoloopFailureIds.get(reference);
    if (!detachedFailureId) {
      detachedFailureId = randomUUID();
      if (reference) this._detachedAutoloopFailureIds.set(reference, detachedFailureId);
    }

    // Schedule after reservation publication. A synchronous append seam can
    // re-enter this API, so invoking the recorder before Map.set would leave a
    // check-then-write window even though appendFlatFile itself is synchronous.
    const recording = Promise.resolve().then(() =>
      this._recordDetachedAutoloopChatFailure(
        runId,
        failure,
        detachedFailureId!,
        binding?.preaudited,
        binding?.runnerProjection,
      ),
    );
    const settled = recording.then(
      (value) => {
        if (binding) this._completedAutoloopFailureBindings.add(binding);
        if (this._detachedAutoloopFailureRecordings.get(reservationKey) === settled) {
          this._detachedAutoloopFailureRecordings.delete(reservationKey);
        }
        return value;
      },
      (recordError: unknown) => {
        if (this._detachedAutoloopFailureRecordings.get(reservationKey) === settled) {
          this._detachedAutoloopFailureRecordings.delete(reservationKey);
        }
        throw recordError;
      },
    );
    this._detachedAutoloopFailureRecordings.set(reservationKey, settled);
    return await settled;
  }

  private _recordDetachedAutoloopChatFailure(
    runId: string,
    failure: Readonly<PublicAutoloopFailure | PublicAutoloopUnknownFailure>,
    detachedFailureId: string,
    preaudited: DurableDetachedAutoloopFailureRow | undefined,
    boundRunnerProjection: AutoloopState['recent_phase_errors'][number] | undefined,
  ): Readonly<PublicAutoloopFailure | PublicAutoloopUnknownFailure> {
    const ctx = this.getAutoloop(runId);
    const storedRecord = ctx ? undefined : loadRun(runId);
    let ledger = ctx?.dispatcher.secureLedgerCapability;
    if (!ledger && storedRecord?.workflow === 'autoloop') {
      ledger = SecureAutoloopLedger.open(storedRecord.cwd, runId, {
        validateExistingFlatFiles: ['decisions.jsonl'],
      });
    }
    if (!ledger) throw new Error(`Autoloop run '${runId}' has no durable ledger for detached failure recording`);

    const phasePayload = detachedPhaseFailure(failure, detachedFailureId);
    const rows = readDurableDetachedFailureRows(ledger.readFlatFile('decisions.jsonl') ?? '');
    const targetKey = detachedPhaseFailureKey(phasePayload);
    const identifiedRow = [...rows]
      .reverse()
      .find(
        (row) =>
          row.payload.detached_failure_id === detachedFailureId && detachedPhaseFailureKey(row.payload) === targetKey,
      );
    const provenPreaudit =
      preaudited &&
      preaudited.payload.detached_failure_id === undefined &&
      detachedPhaseFailureKey(preaudited.payload) === targetKey &&
      rows.some(
        (row) =>
          row.ts === preaudited.ts &&
          row.payload.detached_failure_id === undefined &&
          detachedPhaseFailureKey(row.payload) === targetKey,
      )
        ? preaudited
        : undefined;
    const durableRow = identifiedRow ?? (ctx ? provenPreaudit : undefined);
    const alreadyDurable = durableRow !== undefined;
    const recordedAt = durableRow?.ts || provenPreaudit?.ts || new Date().toISOString();

    if (!alreadyDurable) {
      const envelope = publicData({
        ts: recordedAt,
        kind: 'phase_error' as const,
        actor: 'dispatcher' as const,
        payload: phasePayload,
      });
      ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(envelope)}\n`);
    }

    if (ctx) {
      const recent = ctx.runner.state.recent_phase_errors as Array<
        AutoloopState['recent_phase_errors'][number] & {
          readonly code?: PublicAutoloopFailureCode;
          readonly committed?: true;
          readonly retryable?: boolean;
          readonly pending_dispatch?: Readonly<SendTimeoutPayload>;
          readonly status_reason?: string | null;
          readonly [DETACHED_AUTOLOOP_FAILURE_ID]?: string;
        }
      >;
      const alreadyInState = recent.some((entry) => entry[DETACHED_AUTOLOOP_FAILURE_ID] === detachedFailureId);
      if (!alreadyInState) {
        const runnerProjectionIndex = boundRunnerProjection ? recent.indexOf(boundRunnerProjection) : -1;
        const runnerProjectionFailureId =
          runnerProjectionIndex >= 0 ? recent[runnerProjectionIndex][DETACHED_AUTOLOOP_FAILURE_ID] : undefined;
        const runnerProjectionHasForeignId =
          runnerProjectionFailureId !== undefined && runnerProjectionFailureId !== detachedFailureId;

        const observablePhasePayload = detachedPhaseFailure(failure);
        if (runnerProjectionIndex >= 0 && recent[runnerProjectionIndex][DETACHED_AUTOLOOP_FAILURE_ID] === undefined) {
          const runnerProjection = recent[runnerProjectionIndex];
          recent[runnerProjectionIndex] = detachedStateEntry(runnerProjection.ts, phasePayload, detachedFailureId);
          try {
            ctx.runner.emit('state', ctx.runner.state);
          } catch (publishError) {
            try {
              this.logger.warn?.(
                `[autoloop/${runId}] detached failure listener threw: ${safeOwnErrorMessage(publishError)}`,
              );
            } catch {
              // Durable append/state effects cannot be retried safely merely
              // because diagnostic extraction or the warning sink failed.
            }
          }
        } else if (boundRunnerProjection === undefined || runnerProjectionHasForeignId) {
          ctx.runner.state.consecutive_phase_errors += 1;
          recent.push(detachedStateEntry(recordedAt, phasePayload, detachedFailureId));
          if (recent.length > 5) recent.splice(0, recent.length - 5);
          try {
            ctx.runner.emit('state', ctx.runner.state);
            ctx.runner.emit('phase_error', observablePhasePayload);
          } catch (publishError) {
            try {
              this.logger.warn?.(
                `[autoloop/${runId}] detached failure listener threw: ${safeOwnErrorMessage(publishError)}`,
              );
            } catch {
              // Durable append/state effects cannot be retried safely merely
              // because diagnostic extraction or the warning sink failed.
            }
          }
        }
        try {
          ctx.runner.emit('autoloop_failure', failure);
        } catch (publishError) {
          try {
            this.logger.warn?.(
              `[autoloop/${runId}] detached failure publication threw: ${safeOwnErrorMessage(publishError)}`,
            );
          } catch {
            // Durable append/state effects cannot be retried safely merely
            // because diagnostic extraction or the warning sink failed.
          }
        }
      }
    }
    return failure;
  }

  /**
   * The running loop for a run, or a clear reason why there is not one.
   *
   * Chatting with a Planner needs the live dispatcher; a run that finished or
   * belongs to another process has a readable record and no one to talk to.
   */
  private _liveAutoloop(
    runId: string,
    activity = 'chatting',
  ): AutoloopHandle & {
    runner: AutoloopRunner;
    dispatcher: ClaudeAgentDispatcher;
  } {
    const handle = this.kernel.handle<AutoloopHandle & { runner: AutoloopRunner; dispatcher: ClaudeAgentDispatcher }>(
      runId,
      LEGACY_NODE,
    );
    if (handle) return handle;
    const record = loadRun(runId);
    if (!record || record.workflow !== 'autoloop') throw new Error(`Autoloop run '${runId}' not found`);
    throw new Error(
      `Autoloop run '${runId}' is ${record.state} and not running in this process — resume it before ${activity}`,
    );
  }

  autoloopStatus(runId: string): AutoloopState | undefined {
    const live = this.kernel.handle<AutoloopHandle>(runId, LEGACY_NODE)?.runner.state;
    if (live) return live;
    // Not running here. The record still holds the last state the loop
    // published, so a historical run opens with its real iteration count and
    // workspace instead of the all-zero stub the registry fallback produced.
    const record = loadRun(runId);
    if (!record || record.workflow !== 'autoloop') return undefined;
    const state = autoloopStateFromRecord(record);
    if (!state) return undefined;
    // A detached HTTP rejection can arrive after the live node has published
    // its terminal checkpoint and unregistered its handle. Recover those
    // post-202 rows from the durable ledger so later status/SSE snapshots do
    // not erase the failure merely because no runner remains in memory.
    try {
      const ledger = SecureAutoloopLedger.open(record.cwd, runId, {
        validateExistingFlatFiles: ['decisions.jsonl'],
      });
      const decisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
      const nodeData = record.nodes[LEGACY_NODE]?.data;
      let checkpointCursorOffset: number | undefined;
      if (typeof nodeData === 'object' && nodeData !== null && Object.hasOwn(nodeData, 'detachedFailureLedgerCursor')) {
        const cursor = snapshotDetachedFailureLedgerCursor(ownDataValue(nodeData, 'detachedFailureLedgerCursor'));
        if (!cursor) throw new Error('Autoloop checkpoint contains a malformed detached-failure ledger cursor');
        checkpointCursorOffset = validatedDetachedFailureCursorOffset(decisionLog, cursor);
      }
      const recovered = readDurableDetachedFailureRows(decisionLog);
      const checkpointPrefixEnd = checkpointCursorOffset ?? 0;
      const hasAuthenticatedCheckpointPrefix = checkpointPrefixEnd > 0;
      const checkpointCounts = new Map<string, number>();
      for (const current of state.recent_phase_errors) {
        const key = detachedPhaseFailureKey(current as DetachedAutoloopPhaseFailure);
        checkpointCounts.set(key, (checkpointCounts.get(key) ?? 0) + 1);
      }
      const seenDetachedIds = new Set<string>();
      let added = 0;
      for (const row of recovered) {
        const detachedFailureId = row.payload.detached_failure_id;
        const key = detachedPhaseFailureKey(row.payload);

        if (hasAuthenticatedCheckpointPrefix && row.startByteOffset < checkpointPrefixEnd) {
          const checkpointCount = checkpointCounts.get(key) ?? 0;
          if (checkpointCount > 0) checkpointCounts.set(key, checkpointCount - 1);
          if (detachedFailureId) seenDetachedIds.add(detachedFailureId);
          continue;
        }

        if (!detachedFailureId || seenDetachedIds.has(detachedFailureId)) continue;
        seenDetachedIds.add(detachedFailureId);
        if (!hasAuthenticatedCheckpointPrefix) {
          const checkpointCount = checkpointCounts.get(key) ?? 0;
          if (checkpointCount > 0) {
            checkpointCounts.set(key, checkpointCount - 1);
            continue;
          }
        }
        const isAfterCheckpoint =
          checkpointCursorOffset === undefined
            ? rowIsAfterCheckpoint(row.ts, record.updatedAt)
            : row.startByteOffset >= checkpointCursorOffset;
        if (!isAfterCheckpoint) continue;
        state.recent_phase_errors.push(detachedStateEntry(row.ts, row.payload, detachedFailureId));
        added += 1;
      }
      state.consecutive_phase_errors += added;
      if (state.recent_phase_errors.length > 5)
        state.recent_phase_errors.splice(0, state.recent_phase_errors.length - 5);
    } catch (error) {
      this.logger.warn?.(
        `[autoloop/${runId}] failed to recover detached failure status: ${safeOwnErrorMessage(error)}`,
      );
    }
    return state;
  }

  private _recoveryReceiptRows(ledger: SecureAutoloopLedger, runId: string): RecoveryReceipt[] {
    const rows: RecoveryReceipt[] = [];
    const contents = ledger.readFlatFile('decisions.jsonl') ?? '';
    for (const [index, line] of contents.split('\n').entries()) {
      if (!line) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line) as unknown;
      } catch {
        throw new Error(`Autoloop recovery ledger contains malformed JSON at decisions row ${index + 1}`);
      }
      const isReceiptRow =
        typeof parsed === 'object' &&
        parsed !== null &&
        (parsed as { record_type?: unknown }).record_type === 'autoloop_recovery_receipt';
      const receipt = parseRecoveryReceipt(parsed);
      if (isReceiptRow && !receipt) {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' has malformed recovery receipt at decisions row ${index + 1}`,
        );
      }
      if (!receipt) continue;
      if (receipt.run_id !== runId) {
        throw new Error(`Autoloop recovery receipt at decisions row ${index + 1} belongs to another run`);
      }
      rows.push(receipt);
    }
    const byToken = new Map<string, Array<{ receipt: RecoveryReceipt; index: number }>>();
    for (const [index, receipt] of rows.entries()) {
      const matching = byToken.get(receipt.recovery_token) ?? [];
      matching.push({ receipt, index });
      byToken.set(receipt.recovery_token, matching);
    }
    for (const [token, matching] of byToken) {
      const prepared = matching.filter(({ receipt }) => receipt.status === 'prepared');
      const applied = matching.filter(({ receipt }) => receipt.status === 'applied');
      if (
        prepared.length !== 1 ||
        applied.length > 1 ||
        (applied.length === 1 && applied[0].index < prepared[0].index)
      ) {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' has an invalid recovery receipt graph for token '${token}'`,
        );
      }
      if (
        applied[0] &&
        (applied[0].receipt.action_sha256 !== prepared[0].receipt.action_sha256 ||
          JSON.stringify(applied[0].receipt.action_snapshot) !== JSON.stringify(prepared[0].receipt.action_snapshot) ||
          applied[0].receipt.claim_id !== prepared[0].receipt.claim_id ||
          applied[0].receipt.phase !== prepared[0].receipt.phase ||
          applied[0].receipt.next_safe_action !== prepared[0].receipt.next_safe_action)
      ) {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' has conflicting recovery receipt evidence for token '${token}'`,
        );
      }
    }
    return rows;
  }

  private _appendRecoveryReceipt(
    ledger: SecureAutoloopLedger,
    receipt: RecoveryReceipt,
    validatePrepared?: (prepared: RecoveryReceipt) => void,
  ): { receipt: RecoveryReceipt; appended: boolean } {
    const canonicalReceipt = parseRecoveryReceipt(receipt);
    if (!canonicalReceipt) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${receipt.run_id}' recovery receipt is malformed`,
      );
    }
    receipt = canonicalReceipt;
    const lock = withFileLock(
      path.join(ledger.directory, '.autoloop-recovery.lock'),
      () => {
        // The caller's asynchronous inspection is necessarily outside this
        // synchronous cross-process lock. Re-read the exact durable action
        // while holding the lock before its prepared receipt can fence it.
        // This deliberately cannot await: recovery-envelope persistence uses
        // this same lock.
        if (receipt.status === 'prepared') validatePrepared?.(receipt);
        const rows = this._recoveryReceiptRows(ledger, receipt.run_id);
        const matching = rows.filter((row) => row.recovery_token === receipt.recovery_token);
        const existingPrepared = matching.filter((row) => row.status === 'prepared');
        const existingApplied = matching.filter((row) => row.status === 'applied');
        if (
          existingPrepared.length > 1 ||
          existingApplied.length > 1 ||
          (existingApplied.length > 0 && existingPrepared.length === 0)
        ) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${receipt.run_id}' has ambiguous recovery receipts for the supplied token`,
          );
        }
        if (existingApplied[0]) return { receipt: existingApplied[0], appended: false };
        if (receipt.status === 'prepared' && existingPrepared[0])
          return { receipt: existingPrepared[0], appended: false };
        if (receipt.status === 'prepared') {
          const appliedTokens = new Set(
            rows.filter((row) => row.status === 'applied').map((row) => row.recovery_token),
          );
          const unresolvedPrepared = rows.find(
            (row) => row.status === 'prepared' && !appliedTokens.has(row.recovery_token),
          );
          if (unresolvedPrepared) {
            throw new AutoloopRecoveryError(
              'AUTOLOOP_RECOVERY_INCOMPLETE',
              `Autoloop run '${receipt.run_id}' has an unresolved prepared recovery receipt`,
            );
          }
        }
        ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(receipt)}\n`, true);
        const observed = this._recoveryReceiptRows(ledger, receipt.run_id).filter(
          (row) => row.recovery_token === receipt.recovery_token,
        );
        const observedStatus = observed.filter((row) => row.status === receipt.status);
        if (observedStatus.length !== 1) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${receipt.run_id}' recovery receipt was not durably observed`,
          );
        }
        return { receipt: observedStatus[0], appended: true };
      },
      { waitMs: 500 },
    );
    if (!lock.ok) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${receipt.run_id}' recovery receipt lock is ${lock.reason}`,
      );
    }
    return lock.value;
  }

  /**
   * Validate the exact action bytes that bind an inspected token immediately
   * before a prepared receipt is durably appended. This is synchronous because
   * it executes while `.autoloop-recovery.lock` is owned.
   */
  private _validatePreparedRecoveryAction(
    ledger: SecureAutoloopLedger,
    receipt: RecoveryReceipt,
    recovered: {
      assessment: RecoveryAssessment;
      state: AutoloopState;
      directive?: { iter: number; envelope: ReturnType<typeof AutoloopMsg.directive> };
      review?: RecoveryReviewEnvelope;
    },
    expectedEvidenceDigest: string,
  ): void {
    const stale = (): never => {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_TOKEN_STALE',
        `recovery_token is stale for Autoloop run '${receipt.run_id}'`,
      );
    };
    if (
      receipt.recovery_token !== recovered.assessment.recovery_token ||
      receipt.action_sha256 !== recovered.assessment.action_sha256 ||
      this._recoveryClaimEvidenceDigest(ledger, this._recoveryClaimCurrentState(receipt.run_id)) !==
        expectedEvidenceDigest
    ) {
      return stale();
    }
    const local = this.kernel.handle(receipt.run_id, LEGACY_NODE);
    const lease = readLease(receipt.run_id);
    if (lease && !local && lease.ownerId !== this.kernel.ownerId && !leaseIsStale(lease)) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${receipt.run_id}' is still owned by another live workflow kernel`,
      );
    }

    let exactAction: RecoveryActionSnapshot;
    if (receipt.next_safe_action === 'request_review') {
      if (!recovered.review) return stale();
      const matches: RecoveryReviewEnvelope[] = [];
      for (const line of (ledger.readFlatFile('decisions.jsonl') ?? '').split('\n')) {
        if (!line) continue;
        let parsed: unknown;
        try {
          parsed = JSON.parse(line) as unknown;
        } catch {
          return stale();
        }
        const review = parseRecoveryReviewEnvelope(parsed);
        if (review?.run_id === receipt.run_id && review.envelope.msg_id === recovered.review.envelope.msg_id) {
          matches.push(review);
        }
      }
      if (matches.length !== 1) return stale();
      exactAction = matches[0].envelope;
    } else if (receipt.next_safe_action === 'dispatch_coder') {
      if (!recovered.directive) return stale();
      const bytes = ledger.readIterationArtifact(recovered.directive.iter, 'directive.json');
      if (!bytes) return stale();
      let candidate: Partial<{
        iter: unknown;
        message_id: unknown;
        ts: unknown;
        goal: unknown;
        constraints: unknown;
        success_criteria: unknown;
        max_attempts: unknown;
      }>;
      try {
        candidate = JSON.parse(bytes.toString('utf8')) as typeof candidate;
      } catch {
        return stale();
      }
      if (
        candidate.iter !== recovered.directive.iter ||
        typeof candidate.message_id !== 'string' ||
        typeof candidate.ts !== 'string' ||
        typeof candidate.goal !== 'string' ||
        !Array.isArray(candidate.constraints) ||
        !candidate.constraints.every((value) => typeof value === 'string') ||
        !Array.isArray(candidate.success_criteria) ||
        !candidate.success_criteria.every((value) => typeof value === 'string') ||
        !Number.isSafeInteger(candidate.max_attempts)
      ) {
        return stale();
      }
      exactAction = {
        msg_id: candidate.message_id,
        iter: candidate.iter as number,
        from: 'planner',
        to: 'coder',
        type: 'directive',
        ts: candidate.ts,
        payload: {
          goal: candidate.goal,
          constraints: candidate.constraints,
          success_criteria: candidate.success_criteria,
          max_attempts: candidate.max_attempts as number,
        },
      };
    } else {
      if (receipt.next_safe_action !== 'none' && receipt.next_safe_action !== 'resume_planner') return stale();
      const currentState = this._recoveryClaimCurrentState(receipt.run_id);
      if (currentState.iter !== recovered.state.iter) return stale();
      // The remaining actions have no envelope bytes. Their exact action is
      // reconstructed from the current live-or-durable boundary so a cold
      // Planner can reach the disk-boot path without weakening the byte fence.
      exactAction = {
        type: receipt.next_safe_action,
        run_id: receipt.run_id,
        iter: currentState.iter,
        phase: receipt.phase,
      };
    }
    if (
      recoveryActionDigest(exactAction) !== receipt.action_sha256 ||
      JSON.stringify(exactAction) !== JSON.stringify(receipt.action_snapshot)
    ) {
      return stale();
    }
  }

  private _recoveryActionSnapshot(recovered: {
    assessment: RecoveryAssessment;
    state: AutoloopState;
    directive?: { iter: number; envelope: ReturnType<typeof AutoloopMsg.directive> };
    review?: RecoveryReviewEnvelope;
  }): RecoveryActionSnapshot {
    if (recovered.assessment.next_safe_action === 'dispatch_coder' && recovered.directive) {
      return structuredClone(recovered.directive.envelope);
    }
    if (recovered.assessment.next_safe_action === 'request_review' && recovered.review) {
      return structuredClone(recovered.review.envelope);
    }
    if (
      recovered.assessment.next_safe_action === 'none' ||
      recovered.assessment.next_safe_action === 'resume_planner'
    ) {
      return {
        type: recovered.assessment.next_safe_action,
        run_id: recovered.assessment.run_id,
        iter: recovered.state.iter,
        phase: recovered.assessment.phase,
      };
    }
    throw new AutoloopRecoveryError(
      'AUTOLOOP_RECOVERY_INCOMPLETE',
      `Autoloop run '${recovered.assessment.run_id}' lacks an exact recovery action snapshot`,
    );
  }

  /** Bind recovery authority to the exact kernel ownership generation. */
  private _recoveryLeaseEvidence(runId: string): string {
    const lease = readLease(runId);
    if (!lease) return 'kernel:lease:none';
    const identity = createHash('sha256')
      .update(
        JSON.stringify({
          runId: lease.runId,
          incarnationId: lease.incarnationId,
          ownerId: lease.ownerId,
          acquisitionId: lease.acquisitionId,
          fence: lease.fence,
          pid: lease.pid,
          host: lease.host,
          acquiredAt: lease.acquiredAt,
        }),
        'utf8',
      )
      .digest('hex');
    // renewedAt is deliberately excluded: heartbeats preserve ownership,
    // while every new acquisition changes at least acquisitionId and fence.
    return `kernel:lease:${identity}`;
  }

  /** Byte-fence every durable input used to reconstruct a recovery assessment. */
  private _recoveryClaimEvidenceDigest(ledger: SecureAutoloopLedger, state: AutoloopState): string {
    const digest = createHash('sha256');
    const add = (name: string, bytes: Buffer | string | undefined): void => {
      digest.update(name, 'utf8');
      digest.update('\0', 'utf8');
      digest.update(bytes ?? '');
      digest.update('\0', 'utf8');
    };
    add(
      'state',
      JSON.stringify({
        status: state.status,
        iter: state.iter,
        subagents_spawned: state.subagents_spawned,
        status_reason: state.status_reason,
        pending_dispatch: state.pending_dispatch ?? null,
      }),
    );
    add('kernel-lease', this._recoveryLeaseEvidence(state.run_id));
    add('decisions.jsonl', ledger.readFlatFile('decisions.jsonl'));
    add('agent-generations.jsonl', ledger.readFlatFile('agent-generations.jsonl'));
    for (let iter = 0; iter <= state.iter; iter += 1) {
      for (const name of [
        'directive.json',
        'coder_summary.txt',
        'eval_output.json',
        'diff.patch',
        'verdict.json',
      ] as const) {
        add(`iter/${iter}/${name}`, ledger.readIterationArtifact(iter, name));
      }
    }
    return digest.digest('hex');
  }

  private _recoveryClaimCurrentState(runId: string): AutoloopState {
    const live = this.kernel.handle<AutoloopHandle & { runner: AutoloopRunner }>(runId, LEGACY_NODE);
    if (live) return live.runner.state;
    const record = loadRun(runId);
    const state = record?.workflow === 'autoloop' ? autoloopStateFromRecord(record) : undefined;
    if (!state) throw new AutoloopRecoveryError('AUTOLOOP_RECOVERY_TOKEN_STALE', `Autoloop run '${runId}' changed`);
    return state;
  }

  /**
   * Persist one fully canonical Reviewer message at the SessionManager/ledger
   * boundary. The Runner can only enqueue after this returns, which keeps the
   * envelope identity independent of transient runner/dispatcher memory.
   */
  private _appendRecoveryReviewEnvelope(
    ledger: SecureAutoloopLedger,
    runId: string,
    envelope: Extract<import('./autoloop/messages.js').AnyAutoloopMessage, { type: 'review_request' }>,
  ): RecoveryReviewEnvelope {
    const candidate: RecoveryReviewEnvelope = {
      schema_version: 1,
      record_type: 'autoloop_recovery_review_envelope',
      kind: 'autoloop_recovery_review_envelope',
      run_id: runId,
      envelope,
    };
    const serialized = JSON.stringify(candidate);
    const rows = (): RecoveryReviewEnvelope[] => {
      const found: RecoveryReviewEnvelope[] = [];
      for (const [index, line] of (ledger.readFlatFile('decisions.jsonl') ?? '').split('\n').entries()) {
        if (!line) continue;
        let parsed: unknown;
        try {
          parsed = JSON.parse(line) as unknown;
        } catch {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${runId}' has malformed decisions evidence at row ${index + 1}`,
          );
        }
        const isEnvelopeRow =
          typeof parsed === 'object' &&
          parsed !== null &&
          (parsed as { record_type?: unknown }).record_type === 'autoloop_recovery_review_envelope';
        const row = parseRecoveryReviewEnvelope(parsed);
        if (isEnvelopeRow && !row) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${runId}' has malformed Reviewer recovery envelope at row ${index + 1}`,
          );
        }
        if (!row) continue;
        if (row.run_id !== runId) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop Reviewer recovery envelope at row ${index + 1} belongs to another run`,
          );
        }
        found.push(row);
      }
      return found;
    };
    const lock = withFileLock(
      path.join(ledger.directory, '.autoloop-recovery.lock'),
      () => {
        const matching = rows().filter((row) => row.envelope.msg_id === envelope.msg_id);
        if (matching.length > 2 || matching.some((row) => JSON.stringify(row) !== serialized)) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${runId}' has conflicting Reviewer recovery envelopes for '${envelope.msg_id}'`,
          );
        }
        if (matching.length > 0) return matching[0];
        ledger.appendFlatFile('decisions.jsonl', `${serialized}\n`, true);
        const observed = rows().filter((row) => row.envelope.msg_id === envelope.msg_id);
        if (observed.length !== 1 || JSON.stringify(observed[0]) !== serialized) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${runId}' Reviewer recovery envelope was not durably observed`,
          );
        }
        return observed[0];
      },
      { waitMs: 500 },
    );
    if (!lock.ok) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' Reviewer recovery envelope lock is ${lock.reason}`,
      );
    }
    return lock.value;
  }

  private async _recoveryInput(
    runId: string,
    options: { readOnly?: boolean } = {},
  ): Promise<{
    assessment: RecoveryAssessment;
    ledger: SecureAutoloopLedger;
    state: AutoloopState;
    deliveries: RecoveryDeliveryEvidence[];
    directive?: { iter: number; envelope: ReturnType<typeof AutoloopMsg.directive> };
    review?: RecoveryReviewEnvelope;
  }> {
    const live = this.kernel.handle<AutoloopHandle & { runner: AutoloopRunner }>(runId, LEGACY_NODE);
    const record = loadRun(runId);
    if (!live && (!record || record.workflow !== 'autoloop')) throw new Error(`Autoloop run '${runId}' not found`);
    const state = live?.runner.state ?? (record ? autoloopStateFromRecord(record) : undefined);
    if (!state) throw new Error(`Autoloop run '${runId}' has no durable state checkpoint`);
    const workspace = live?.runner.config.workspace ?? record!.cwd;
    const ledgerOptions = {
      validateExistingFlatFiles: ['decisions.jsonl', 'agent-generations.jsonl'] as const,
      logger: this.logger,
    };
    const ledger = options.readOnly
      ? SecureAutoloopLedger.openReadOnly(workspace, runId, ledgerOptions)
      : SecureAutoloopLedger.open(workspace, runId, ledgerOptions);
    // Validate the complete receipt relation during inspection as well as
    // apply. A malformed or orphaned receipt is never safe to derive from.
    this._recoveryReceiptRows(ledger, runId);

    const iterations: RecoveryIterationEvidence[] = [];
    let directive: { iter: number; envelope: ReturnType<typeof AutoloopMsg.directive> } | undefined;
    let directiveEvidenceProblem: string | undefined;
    const noteDirectiveEvidenceProblem = (reason: string): void => {
      directiveEvidenceProblem ??= reason;
    };
    const dispatchIter = new Map<string, number>();
    const directivesByDispatchIdentity = new Map<string, ReturnType<typeof AutoloopMsg.directive>>();
    for (let iter = 0; iter <= state.iter; iter += 1) {
      const names: Array<[string, RecoveryIterationEvidence['artifacts'][number]]> = [
        ['directive.json', 'directive'],
        ['coder_summary.txt', 'coder_summary'],
        ['eval_output.json', 'eval_output'],
        ['diff.patch', 'diff'],
      ];
      const artifacts: RecoveryIterationEvidence['artifacts'][number][] = [];
      for (const [file, artifact] of names) {
        if (
          ledger.readIterationArtifact(
            iter,
            file as 'directive.json' | 'coder_summary.txt' | 'eval_output.json' | 'diff.patch',
          )
        ) {
          artifacts.push(artifact);
        }
      }
      let verdict: RecoveryIterationEvidence['verdict'];
      const verdictBytes = ledger.readIterationArtifact(iter, 'verdict.json');
      if (verdictBytes) {
        let persistedVerdict: unknown;
        try {
          persistedVerdict = JSON.parse(verdictBytes.toString('utf8')) as unknown;
        } catch {
          throw new Error(`Autoloop run '${runId}' has malformed verdict evidence for iteration ${iter}`);
        }
        const decision =
          typeof persistedVerdict === 'object' && persistedVerdict !== null
            ? (persistedVerdict as { decision?: unknown }).decision
            : undefined;
        if (decision !== 'advance' && decision !== 'hold' && decision !== 'rollback') {
          throw new Error(`Autoloop run '${runId}' has ambiguous verdict evidence for iteration ${iter}`);
        }
        verdict = decision;
      }
      if (artifacts.length > 0 || verdict) iterations.push({ iter, artifacts, ...(verdict ? { verdict } : {}) });

      const directiveBytes = ledger.readIterationArtifact(iter, 'directive.json');
      if (directiveBytes) {
        let persisted: unknown;
        try {
          persisted = JSON.parse(directiveBytes.toString('utf8')) as unknown;
        } catch {
          noteDirectiveEvidenceProblem('malformed_exact_directive');
          continue;
        }
        const candidate = persisted as Partial<{
          iter: unknown;
          message_id: unknown;
          ts: unknown;
          dispatch_id: unknown;
          goal: unknown;
          constraints: unknown;
          success_criteria: unknown;
          max_attempts: unknown;
        }>;
        if (
          candidate.iter !== iter ||
          typeof candidate.message_id !== 'string' ||
          typeof candidate.ts !== 'string' ||
          typeof candidate.dispatch_id !== 'string' ||
          typeof candidate.goal !== 'string' ||
          !Array.isArray(candidate.constraints) ||
          !candidate.constraints.every((value) => typeof value === 'string') ||
          !Array.isArray(candidate.success_criteria) ||
          !candidate.success_criteria.every((value) => typeof value === 'string') ||
          !Number.isSafeInteger(candidate.max_attempts)
        ) {
          noteDirectiveEvidenceProblem('malformed_exact_directive');
          continue;
        }
        const exactDirective: { iter: number; envelope: ReturnType<typeof AutoloopMsg.directive> } = {
          iter,
          envelope: {
            msg_id: candidate.message_id,
            iter,
            from: 'planner',
            to: 'coder',
            type: 'directive',
            ts: candidate.ts,
            payload: {
              goal: candidate.goal,
              constraints: candidate.constraints,
              success_criteria: candidate.success_criteria,
              max_attempts: candidate.max_attempts as number,
            },
          },
        };
        dispatchIter.set(candidate.dispatch_id, iter);
        directivesByDispatchIdentity.set(candidate.dispatch_id, exactDirective.envelope);
        directive = exactDirective;
      }
    }

    const acknowledged = new Map<string, string>();
    const reviewEnvelopes: RecoveryReviewEnvelope[] = [];
    let reviewEvidenceProblem: string | undefined;
    const noteReviewEvidenceProblem = (reason: string): void => {
      reviewEvidenceProblem ??= reason;
    };
    const intents: Array<{
      delivery_id: string;
      idempotency_key: string;
      kind: 'coder_directive' | 'review_request';
      target_role: 'coder' | 'reviewer';
      payload_sha256: string;
      logical_message_sha256: string;
    }> = [];
    const deliveryRows: DeliveryLedgerRow[] = [];
    for (const [index, line] of (ledger.readFlatFile('decisions.jsonl') ?? '').split('\n').entries()) {
      if (!line) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line) as unknown;
      } catch {
        throw new Error(`Autoloop run '${runId}' has malformed decisions evidence at row ${index + 1}`);
      }
      // Recovery receipts share the append-only decision ledger but are not
      // Task-5 outbox records. Classify them first so the outbox validator
      // remains strict for every row it owns.
      const isReceiptRow =
        typeof parsed === 'object' &&
        parsed !== null &&
        (parsed as { record_type?: unknown }).record_type === 'autoloop_recovery_receipt';
      const receipt = parseRecoveryReceipt(parsed);
      if (isReceiptRow && !receipt) {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' has malformed recovery receipt at decisions row ${index + 1}`,
        );
      }
      if (receipt) continue;
      const isReviewEnvelopeRow =
        typeof parsed === 'object' &&
        parsed !== null &&
        (parsed as { record_type?: unknown }).record_type === 'autoloop_recovery_review_envelope';
      const reviewEnvelope = parseRecoveryReviewEnvelope(parsed);
      if (isReviewEnvelopeRow && !reviewEnvelope) {
        noteReviewEvidenceProblem('malformed_exact_envelope');
        continue;
      }
      if (reviewEnvelope) {
        if (reviewEnvelope.run_id !== runId) {
          noteReviewEvidenceProblem('foreign_exact_envelope');
          continue;
        }
        reviewEnvelopes.push(reviewEnvelope);
        if (reviewEnvelopes.length > MAX_RECOVERY_REVIEW_ENVELOPE_INDEX_ROWS) {
          noteReviewEvidenceProblem('envelope_index_cap_exceeded');
        }
        continue;
      }
      const row = parseOutboxDecisionLedgerRow(parsed, index + 1);
      if (!row) continue;
      deliveryRows.push(row);
      if (row.kind === 'acknowledgement') {
        const existing = acknowledged.get(row.acknowledgement.delivery_id);
        if (existing !== undefined && existing !== row.acknowledgement.payload_sha256) {
          throw new Error(`Autoloop run '${runId}' has conflicting acknowledgement provenance`);
        }
        acknowledged.set(row.acknowledgement.delivery_id, row.acknowledgement.payload_sha256);
      }
      if (row.kind === 'intent') {
        const payload = row.intent.payload;
        const payloadKeys =
          typeof payload === 'object' && payload !== null && !Array.isArray(payload) ? Reflect.ownKeys(payload) : [];
        const promptDescriptor =
          typeof payload === 'object' && payload !== null && !Array.isArray(payload)
            ? Object.getOwnPropertyDescriptor(payload, 'prompt')
            : undefined;
        const logicalDigestDescriptor =
          typeof payload === 'object' && payload !== null && !Array.isArray(payload)
            ? Object.getOwnPropertyDescriptor(payload, 'logical_message_sha256')
            : undefined;
        const logicalMessageDigest =
          payloadKeys.length === 2 &&
          payloadKeys.includes('prompt') &&
          payloadKeys.includes('logical_message_sha256') &&
          promptDescriptor?.enumerable === true &&
          Object.hasOwn(promptDescriptor, 'value') &&
          typeof promptDescriptor.value === 'string' &&
          logicalDigestDescriptor?.enumerable === true &&
          Object.hasOwn(logicalDigestDescriptor, 'value') &&
          typeof logicalDigestDescriptor.value === 'string' &&
          /^[a-f0-9]{64}$/.test(logicalDigestDescriptor.value)
            ? logicalDigestDescriptor.value
            : undefined;
        if (!logicalMessageDigest) {
          if (row.intent.kind === 'coder_directive') noteDirectiveEvidenceProblem('malformed_intent_payload');
          else noteReviewEvidenceProblem('malformed_intent_payload');
          continue;
        }
        intents.push({
          delivery_id: row.intent.delivery_id,
          idempotency_key: row.intent.idempotency_key,
          kind: row.intent.kind,
          target_role: row.intent.target_role,
          payload_sha256: row.intent.payload_sha256,
          logical_message_sha256: logicalMessageDigest,
        });
      }
    }
    // Do not derive a recovery action from a subset of delivery rows. The
    // Task-5 graph is append ordered and an orphan/conflict makes every later
    // recovery effect ambiguous.
    validateOutboxDecisionLedgerGraph(deliveryRows);

    const generations = new Map<string, PhysicalAgentGeneration>();
    for (const [index, line] of (ledger.readFlatFile('agent-generations.jsonl') ?? '').split('\n').entries()) {
      if (!line) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(line) as { kind?: unknown; payload?: unknown };
      } catch {
        throw new Error(`Autoloop run '${runId}' has malformed generation evidence at row ${index + 1}`);
      }
      const entry = parsed as { kind?: unknown; payload?: unknown };
      if (
        entry.kind !== 'agent_generation_reserved' &&
        entry.kind !== 'agent_generation_started' &&
        entry.kind !== 'agent_generation_lease_renewed' &&
        entry.kind !== 'agent_generation_orphaned' &&
        entry.kind !== 'agent_generation_released'
      ) {
        continue;
      }
      const generation = entry.payload as Partial<PhysicalAgentGeneration>;
      if (
        !generation ||
        (generation.role !== 'planner' && generation.role !== 'coder' && generation.role !== 'reviewer') ||
        !Number.isSafeInteger(generation.generation) ||
        typeof generation.session_name !== 'string' ||
        typeof generation.owner_instance_id !== 'string' ||
        typeof generation.created_at !== 'string' ||
        typeof generation.last_activity_at !== 'string' ||
        typeof generation.lease_expires_at !== 'string' ||
        (generation.state !== 'live' &&
          generation.state !== 'stale' &&
          generation.state !== 'orphaned' &&
          generation.state !== 'released')
      ) {
        throw new Error(`Autoloop run '${runId}' has ambiguous generation evidence at row ${index + 1}`);
      }
      const key = generation.role;
      const current = generations.get(key);
      if (!current || (generation.generation as number) >= current.generation)
        generations.set(key, generation as PhysicalAgentGeneration);
    }
    const agents: RecoveryAgentEvidence[] = [];
    for (const generation of generations.values()) {
      agents.push({
        generation: { ...generation },
        matching_runtime: await this.inspect(generation.session_name, generation.session_id),
      });
    }
    const reviewForCurrentIteration = reviewEnvelopes.filter((row) => row.envelope.iter === state.iter);
    const reviewIdentities = new Map<string, RecoveryReviewEnvelope[]>();
    for (const row of reviewForCurrentIteration) {
      const matching = reviewIdentities.get(row.envelope.msg_id) ?? [];
      matching.push(row);
      reviewIdentities.set(row.envelope.msg_id, matching);
    }
    if (
      reviewIdentities.size > 1 ||
      [...reviewIdentities.values()].some(
        (rows) =>
          rows.length > MAX_RECOVERY_REVIEW_ENVELOPE_DUPLICATES ||
          rows.some((row) => JSON.stringify(row) !== JSON.stringify(rows[0])),
      )
    ) {
      noteReviewEvidenceProblem('conflicting_current_identity');
    }
    const review = reviewIdentities.values().next().value?.[0] as RecoveryReviewEnvelope | undefined;
    const reviewsByDispatchIdentity = new Map<string, RecoveryReviewEnvelope>();
    for (const row of reviewEnvelopes) {
      const dispatchIdentity = `dispatch_${createHash('sha256')
        .update(
          JSON.stringify([
            runId,
            row.envelope.msg_id,
            row.envelope.iter,
            row.envelope.from,
            row.envelope.to,
            row.envelope.type,
          ]),
          'utf8',
        )
        .digest('hex')}`;
      const existing = reviewsByDispatchIdentity.get(dispatchIdentity);
      if (existing && JSON.stringify(existing) !== JSON.stringify(row)) {
        noteReviewEvidenceProblem('conflicting_dispatch_identity');
        continue;
      }
      reviewsByDispatchIdentity.set(dispatchIdentity, row);
    }
    const deliveries: RecoveryDeliveryEvidence[] = [];
    for (const intent of intents) {
      const acknowledgement = acknowledged.get(intent.delivery_id);
      if (acknowledgement !== undefined && acknowledgement !== intent.payload_sha256) {
        if (intent.kind === 'coder_directive') noteDirectiveEvidenceProblem('conflicting_acknowledgement');
        else noteReviewEvidenceProblem('conflicting_acknowledgement');
        continue;
      }
      if (intent.kind === 'coder_directive') {
        const iter = dispatchIter.get(intent.idempotency_key);
        const envelope = directivesByDispatchIdentity.get(intent.idempotency_key);
        if (intent.target_role !== 'coder' || iter === undefined || !envelope) {
          noteDirectiveEvidenceProblem('missing_exact_directive');
          continue;
        }
        if (recoveryLogicalMessageSha256(envelope) !== intent.logical_message_sha256) {
          noteDirectiveEvidenceProblem('intent_digest_mismatch');
          continue;
        }
        deliveries.push({
          delivery_id: intent.delivery_id,
          idempotency_key: intent.idempotency_key,
          iter,
          kind: intent.kind,
          acknowledged: acknowledgement !== undefined,
        });
        continue;
      }
      const envelope = reviewsByDispatchIdentity.get(intent.idempotency_key);
      if (
        intent.target_role !== 'reviewer' ||
        !envelope ||
        envelope.envelope.from !== 'runner' ||
        envelope.envelope.to !== 'reviewer' ||
        envelope.envelope.type !== 'review_request'
      ) {
        noteReviewEvidenceProblem('missing_exact_envelope');
        continue;
      }
      if (recoveryLogicalMessageSha256(envelope.envelope) !== intent.logical_message_sha256) {
        noteReviewEvidenceProblem('intent_digest_mismatch');
        continue;
      }
      deliveries.push({
        delivery_id: intent.delivery_id,
        idempotency_key: intent.idempotency_key,
        iter: envelope.envelope.iter,
        kind: intent.kind,
        acknowledged: acknowledgement !== undefined,
      });
    }
    let assessment = assessRecovery({
      run_id: runId,
      observed_at: new Date().toISOString(),
      legacy_state: state,
      iterations,
      deliveries,
      agents,
      completed: state.status === 'terminated' && state.status_reason === 'completed',
    });
    const actionEvidenceProblem =
      assessment.next_safe_action === 'dispatch_coder'
        ? (directiveEvidenceProblem ??
          (!directive || directive.iter !== state.iter ? 'missing_exact_directive' : undefined))
        : assessment.next_safe_action === 'request_review'
          ? (reviewEvidenceProblem ??
            (!review || review.envelope.iter !== state.iter ? 'missing_exact_envelope' : undefined))
          : undefined;
    if (actionEvidenceProblem) {
      const role = assessment.next_safe_action === 'dispatch_coder' ? 'coder' : 'review';
      assessment = blockRecoveryAssessment(assessment, `ambiguity:recovery:${role}:${actionEvidenceProblem}`);
    } else {
      const exactAction =
        assessment.next_safe_action === 'dispatch_coder'
          ? directive!.envelope
          : assessment.next_safe_action === 'request_review'
            ? review!.envelope
            : {
                type: assessment.next_safe_action,
                run_id: runId,
                iter: state.iter,
                phase: assessment.phase,
              };
      assessment = assessRecovery({
        run_id: runId,
        observed_at: new Date().toISOString(),
        legacy_state: state,
        iterations,
        deliveries,
        agents,
        completed: state.status === 'terminated' && state.status_reason === 'completed',
        action_sha256: recoveryActionDigest(exactAction),
      });
      if (
        assessment.next_safe_action === 'resume_planner' &&
        (live?.runner.state.status === 'planning' || live?.runner.state.status === 'running')
      ) {
        assessment = rebindRecoveryAction(
          assessment,
          'none',
          recoveryActionDigest({ type: 'none', run_id: runId, iter: state.iter, phase: assessment.phase }),
          'recovery:planner:already_satisfied',
        );
      } else if (
        (assessment.next_safe_action === 'dispatch_coder' || assessment.next_safe_action === 'request_review') &&
        live &&
        (live.runner.state.status === 'paused' ||
          live.runner.state.status === 'terminated' ||
          live.runner.state.status === 'crashed')
      ) {
        assessment = blockRecoveryAssessment(assessment, `ambiguity:recovery:runner:${live.runner.state.status}`);
      }
    }
    assessment = rebindRecoveryAction(
      assessment,
      assessment.next_safe_action,
      assessment.action_sha256,
      this._recoveryLeaseEvidence(runId),
    );
    return {
      assessment,
      ledger,
      state,
      deliveries,
      directive,
      review,
    };
  }

  /** Internal token-fenced inspection/apply core. Public MCP/HTTP wiring is deliberately deferred. */
  async autoloopRecover(
    runId: string,
    options: { apply?: boolean; recovery_token?: string } = {},
  ): Promise<RecoveryResult> {
    return await this._autoloopRecover(runId, options);
  }

  private async _autoloopRecover(
    runId: string,
    options: { apply?: boolean; recovery_token?: string } = {},
    bootOverrides: RecoveryBootOverrides = {},
  ): Promise<RecoveryResult> {
    if (!options.apply) {
      const { assessment } = await this._recoveryInput(runId, { readOnly: true });
      return { assessment };
    }
    if (!options.recovery_token) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_TOKEN_REQUIRED',
        'recovery_token is required when apply is true',
      );
    }
    const transactionKey = `${runId}\u0000${options.recovery_token}`;
    const existing = this._autoloopRecoveryTransactions.get(transactionKey);
    if (existing) return await existing;
    const operation = this._autoloopRecoverApply(runId, options.recovery_token, bootOverrides);
    this._autoloopRecoveryTransactions.set(transactionKey, operation);
    try {
      return await operation;
    } finally {
      if (this._autoloopRecoveryTransactions.get(transactionKey) === operation) {
        this._autoloopRecoveryTransactions.delete(transactionKey);
      }
    }
  }

  private async _autoloopRecoverApply(
    runId: string,
    token: string,
    bootOverrides: RecoveryBootOverrides,
  ): Promise<RecoveryResult> {
    const inspected = await this._recoveryInput(runId, { readOnly: true });
    const inspectedAllReceipts = this._recoveryReceiptRows(inspected.ledger, runId);
    const inspectedReceipts = inspectedAllReceipts.filter((row) => row.recovery_token === token);
    const inspectedApplied = inspectedReceipts.find((row) => row.status === 'applied');
    if (inspectedApplied) {
      // A durable applied pair is the authority for exact replay even when its
      // effect advanced the live state and therefore changed the current token.
      return { assessment: inspected.assessment, receipt: inspectedApplied };
    }
    const inspectedAppliedTokens = new Set(
      inspectedAllReceipts.filter((row) => row.status === 'applied').map((row) => row.recovery_token),
    );
    if (
      inspectedAllReceipts.some((row) => row.status === 'prepared' && !inspectedAppliedTokens.has(row.recovery_token))
    ) {
      // A durable prepared claim outranks later state/token drift: its physical
      // effect may already have happened, so no newer boundary is safe to run.
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' has an unresolved prepared recovery receipt`,
      );
    }
    if (token !== inspected.assessment.recovery_token) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_TOKEN_STALE',
        `recovery_token is stale for Autoloop run '${runId}'`,
      );
    }
    // Applying may harden mutable ledger state, but only after the current
    // token and exact action digest have been reconstructed through the
    // read-only boundary. Re-open and compare once more immediately before
    // claiming the durable effect.
    const recovered = await this._recoveryInput(runId);
    const { assessment, ledger } = recovered;
    if (token !== assessment.recovery_token) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_TOKEN_STALE',
        `recovery_token is stale for Autoloop run '${runId}'`,
      );
    }
    const allReceipts = this._recoveryReceiptRows(ledger, runId);
    const existingReceipts = allReceipts.filter((row) => row.recovery_token === token);
    if (existingReceipts.some((row) => row.status === 'applied')) {
      const applied = existingReceipts.filter((row) => row.status === 'applied');
      if (applied.length !== 1 || existingReceipts.filter((row) => row.status === 'prepared').length !== 1) {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' has ambiguous applied recovery receipts`,
        );
      }
      return { assessment, receipt: applied[0] };
    }
    const appliedTokens = new Set(
      allReceipts.filter((row) => row.status === 'applied').map((row) => row.recovery_token),
    );
    const unresolvedPrepared = allReceipts.find(
      (row) => row.status === 'prepared' && !appliedTokens.has(row.recovery_token),
    );
    if (unresolvedPrepared) {
      // Any prior durable claim may already have executed its external effect.
      // A changed assessment/token cannot make that unknown outcome safe to
      // supersede, so fail closed until the original claim is resolved.
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' has an unresolved prepared recovery receipt`,
      );
    }
    if (existingReceipts.length > 0) {
      // A process can die after fencing this token but before (or during) the
      // external effect. Replaying it would turn an unknown outcome into a
      // duplicate effect, so preserve the evidence and require resolution.
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' has an unresolved prepared recovery receipt`,
      );
    }
    if (assessment.next_safe_action === 'manual_resolution') {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_MANUAL_RESOLUTION_REQUIRED',
        `Autoloop run '${runId}' has ambiguous recovery evidence`,
      );
    }
    const claimState = structuredClone(recovered.state);
    const claimEvidenceDigest = this._recoveryClaimEvidenceDigest(ledger, claimState);
    const actionSnapshot = this._recoveryActionSnapshot({ ...recovered, state: claimState });
    if (recoveryActionDigest(actionSnapshot) !== assessment.action_sha256) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_TOKEN_STALE',
        `recovery_token is stale for Autoloop run '${runId}'`,
      );
    }
    const preparedClaim = this._appendRecoveryReceipt(
      ledger,
      {
        schema_version: 1,
        record_type: 'autoloop_recovery_receipt',
        kind: 'autoloop_recovery_receipt',
        run_id: runId,
        recovery_token: token,
        action_sha256: assessment.action_sha256,
        action_snapshot: actionSnapshot,
        claim_id: randomUUID(),
        phase: assessment.phase,
        next_safe_action: assessment.next_safe_action,
        status: 'prepared',
        recorded_at: new Date().toISOString(),
      },
      (preparedReceipt) =>
        this._validatePreparedRecoveryAction(
          ledger,
          preparedReceipt,
          { ...recovered, state: claimState },
          claimEvidenceDigest,
        ),
    );
    const prepared = preparedClaim.receipt;
    if (!preparedClaim.appended) {
      // Another process owns an unresolved durable claim. The effect outcome
      // cannot be inferred, so this caller must not replay it.
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' recovery effect is claimed by another process`,
      );
    }
    if (prepared.status !== 'prepared') {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' recovery receipt is invalid`,
      );
    }
    const prior = this._recoveryReceiptRows(ledger, runId).filter((row) => row.recovery_token === token);
    if (prior.length !== 1 || prior[0].status !== 'prepared') {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' recovery effect is unresolved`,
      );
    }

    const live = this.getAutoloop(runId);
    const recoveryBootConfig = (config: Record<string, unknown>): Parameters<SessionManager['_bootAutoloop']>[0] =>
      ({
        ...config,
        // These caller-supplied configs are process-local recovery inputs. In
        // particular they must not be copied into a receipt or durable spec.
        plannerCustomEngine: bootOverrides.plannerCustomEngine,
        coderCustomEngine: bootOverrides.coderCustomEngine,
        reviewerCustomEngine: bootOverrides.reviewerCustomEngine,
        sendTimeoutMs: bootOverrides.sendTimeoutMs ?? config.sendTimeoutMs,
        _secureLedger: ledger,
      }) as Parameters<SessionManager['_bootAutoloop']>[0];
    let plannerTransitionProven = false;
    if (assessment.next_safe_action === 'resume_planner') {
      if (live) {
        if (live.runner.state.status === 'paused' && !live.runner.state.pending_dispatch) {
          await live.runner.send(AutoloopMsg.resume(live.runner.state.iter));
          const postResumeStatus = live.runner.state.status as AutoloopState['status'];
          plannerTransitionProven =
            (postResumeStatus === 'planning' || postResumeStatus === 'running') && !live.runner.state.pending_dispatch;
        }
      } else {
        const record = loadRun(runId);
        const config = (
          record?.spec.nodes.find((node) => node.id === LEGACY_NODE) as { config?: Record<string, unknown> } | undefined
        )?.config;
        if (!record || record.workflow !== 'autoloop' || !config) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${runId}' cannot resume from disk`,
          );
        }
        await this._resumeAutoloopRun(runId, recoveryBootConfig(config));
        const resumed = this.getAutoloop(runId);
        plannerTransitionProven =
          !!resumed &&
          (resumed.runner.state.status === 'planning' || resumed.runner.state.status === 'running') &&
          !resumed.runner.state.pending_dispatch;
      }
    } else if (assessment.next_safe_action === 'dispatch_coder') {
      let handle = live ?? this.getAutoloop(runId);
      if (!handle) {
        const record = loadRun(runId);
        const config = (
          record?.spec.nodes.find((node) => node.id === LEGACY_NODE) as { config?: Record<string, unknown> } | undefined
        )?.config;
        if (!record || record.workflow !== 'autoloop' || !config) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${runId}' cannot resume from disk`,
          );
        }
        await this._resumeAutoloopRun(runId, recoveryBootConfig(config));
        handle = this.getAutoloop(runId);
      }
      if (!handle || prepared.action_snapshot.type !== 'directive') {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' lacks an exact Coder delivery to recover`,
        );
      }
      try {
        await handle.runner.send(prepared.action_snapshot, { requireRootDelivery: true });
      } catch {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' Coder recovery did not reach the agent dispatcher`,
        );
      }
    } else if (assessment.next_safe_action === 'request_review') {
      let handle = live ?? this.getAutoloop(runId);
      if (!handle) {
        const record = loadRun(runId);
        const config = (
          record?.spec.nodes.find((node) => node.id === LEGACY_NODE) as { config?: Record<string, unknown> } | undefined
        )?.config;
        if (!record || record.workflow !== 'autoloop' || !config) {
          throw new AutoloopRecoveryError(
            'AUTOLOOP_RECOVERY_INCOMPLETE',
            `Autoloop run '${runId}' cannot resume Reviewer recovery from disk`,
          );
        }
        await this._resumeAutoloopRun(runId, recoveryBootConfig(config));
        handle = this.getAutoloop(runId);
      }
      if (!handle || prepared.action_snapshot.type !== 'review_request') {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' lacks an exact durable Reviewer request to recover`,
        );
      }
      try {
        await handle.runner.send(prepared.action_snapshot, { requireRootDelivery: true });
      } catch {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' Reviewer recovery did not reach the agent dispatcher`,
        );
      }
    }
    if (assessment.next_safe_action === 'resume_planner') {
      const resumed = this.getAutoloop(runId);
      if (
        !plannerTransitionProven ||
        !resumed ||
        (resumed.runner.state.status !== 'planning' && resumed.runner.state.status !== 'running') ||
        resumed.runner.state.pending_dispatch
      ) {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' Planner recovery has no proven live postcondition`,
        );
      }
    } else if (assessment.next_safe_action === 'dispatch_coder' || assessment.next_safe_action === 'request_review') {
      const observed = await this._recoveryInput(runId, { readOnly: true });
      const kind = assessment.next_safe_action === 'dispatch_coder' ? 'coder_directive' : 'review_request';
      const actionSnapshot = prepared.action_snapshot;
      if (actionSnapshot.type !== 'directive' && actionSnapshot.type !== 'review_request') {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' recovery action has no agent delivery identity`,
        );
      }
      const acknowledged = observed.deliveries.some(
        (delivery) =>
          delivery.kind === kind &&
          delivery.iter === actionSnapshot.iter &&
          delivery.idempotency_key === recoveryActionDispatchId(runId, actionSnapshot) &&
          delivery.acknowledged,
      );
      if (!acknowledged) {
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' ${kind} recovery has no durable acknowledgement`,
        );
      }
    }
    const appliedClaim = this._appendRecoveryReceipt(ledger, {
      ...prepared,
      status: 'applied',
      recorded_at: new Date().toISOString(),
    });
    const receipt = appliedClaim.receipt;
    if (receipt.status !== 'applied') {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' recovery effect remains unresolved`,
      );
    }
    return { assessment, receipt };
  }

  autoloopList(): AutoloopState[] {
    return this.kernel
      .list({ workflow: 'autoloop' })
      .map((r) => this.autoloopStatus(r.runId))
      .filter((s): s is AutoloopState => Boolean(s));
  }

  async autoloopResetAgent(
    runId: string,
    agent: 'planner' | 'coder' | 'reviewer',
    opts: { force?: boolean; eagerRestart?: boolean } = {},
  ): Promise<boolean> {
    const result = await this.autoloopResetAgentResult(runId, agent, opts);
    return result?.ok ?? false;
  }

  async autoloopResetAgentResult(
    runId: string,
    agent: 'planner' | 'coder' | 'reviewer',
    opts: { force?: boolean; eagerRestart?: boolean } = {},
  ): Promise<AutoloopResetResult | undefined> {
    const ctx = this.kernel.handle<AutoloopHandle & { dispatcher: ClaudeAgentDispatcher }>(runId, LEGACY_NODE);
    if (!ctx) return undefined;
    return await ctx.dispatcher.resetAgent(agent, opts);
  }

  /** Serialize internal single-role recovery primitives with reviewer-only transitions. */
  private async _withAutoloopRoleMutation<T>(runId: string, operation: () => Promise<T>): Promise<T> {
    if (this._autoloopReviewDeleting.has(runId)) throw new Error(`Autoloop run '${runId}' is being deleted`);
    const predecessor = this._autoloopReviewTransactions.get(runId) ?? Promise.resolve();
    let release!: () => void;
    const transaction = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tail = predecessor.then(() => transaction);
    this._autoloopReviewTransactions.set(runId, tail);
    await predecessor;
    try {
      if (this._autoloopReviewDeleting.has(runId)) throw new Error(`Autoloop run '${runId}' is being deleted`);
      const result = await operation();
      return result;
    } finally {
      release();
      if (this._autoloopReviewTransactions.get(runId) === tail) this._autoloopReviewTransactions.delete(runId);
    }
  }

  async autoloopSpawnCoder(
    runId: string,
    args: { coder_engine?: EngineType; coder_model?: string } = {},
  ): Promise<PhysicalAgentGeneration> {
    if (
      typeof args !== 'object' ||
      args === null ||
      (Object.getPrototypeOf(args) !== Object.prototype && Object.getPrototypeOf(args) !== null)
    ) {
      throw new Error('autoloop_spawn_coder arguments must not contain inherited data');
    }
    if (
      Reflect.ownKeys(args).some(
        (key) => typeof key !== 'string' || (key !== 'coder_engine' && key !== 'coder_model' && key !== 'run_id'),
      )
    ) {
      throw new Error('autoloop_spawn_coder arguments contain an unknown field');
    }
    const own = Object.create(null) as { coder_engine?: EngineType; coder_model?: string };
    const engineDescriptor = Object.getOwnPropertyDescriptor(args, 'coder_engine');
    const modelDescriptor = Object.getOwnPropertyDescriptor(args, 'coder_model');
    if (engineDescriptor && !Object.hasOwn(engineDescriptor, 'value'))
      throw new Error('autoloop_spawn_coder coder_engine must be an own data property');
    if (modelDescriptor && !Object.hasOwn(modelDescriptor, 'value'))
      throw new Error('autoloop_spawn_coder coder_model must be an own data property');
    const engine = engineDescriptor?.value;
    const model = modelDescriptor?.value;
    if (
      engine !== undefined &&
      (typeof engine !== 'string' || engine === 'custom' || !ENGINE_TYPES.includes(engine as EngineType))
    )
      throw new Error(`Coder engine '${String(engine)}' is not supported`);
    if (model !== undefined && (typeof model !== 'string' || model.length === 0 || model.length > 512))
      throw new Error('autoloop_spawn_coder coder_model must be a non-empty string of at most 512 characters');
    if (engine !== undefined) own.coder_engine = engine as EngineType;
    if (model !== undefined) own.coder_model = model;
    return await this._withAutoloopRoleMutation(runId, async () => {
      const ctx = this._liveAutoloop(runId);
      return await ctx.dispatcher.spawnCoder(own);
    });
  }

  /** Internal Task 4 boundary: start exactly one Reviewer generation for a live run. */
  async autoloopSpawnReviewer(
    runId: string,
    args: { reviewer_engine?: EngineType; reviewer_model?: string } = {},
  ): Promise<PhysicalAgentGeneration> {
    if (
      typeof args !== 'object' ||
      args === null ||
      (Object.getPrototypeOf(args) !== Object.prototype && Object.getPrototypeOf(args) !== null)
    ) {
      throw new Error('autoloop_spawn_reviewer arguments must not contain inherited data');
    }
    if (
      Reflect.ownKeys(args).some(
        (key) => typeof key !== 'string' || (key !== 'reviewer_engine' && key !== 'reviewer_model' && key !== 'run_id'),
      )
    ) {
      throw new Error('autoloop_spawn_reviewer arguments contain an unknown field');
    }
    const own = Object.create(null) as { reviewer_engine?: EngineType; reviewer_model?: string };
    const engineDescriptor = Object.getOwnPropertyDescriptor(args, 'reviewer_engine');
    const modelDescriptor = Object.getOwnPropertyDescriptor(args, 'reviewer_model');
    if (engineDescriptor && !Object.hasOwn(engineDescriptor, 'value'))
      throw new Error('autoloop_spawn_reviewer reviewer_engine must be an own data property');
    if (modelDescriptor && !Object.hasOwn(modelDescriptor, 'value'))
      throw new Error('autoloop_spawn_reviewer reviewer_model must be an own data property');
    const engine = engineDescriptor?.value;
    const model = modelDescriptor?.value;
    if (
      engine !== undefined &&
      (typeof engine !== 'string' || engine === 'custom' || !ENGINE_TYPES.includes(engine as EngineType))
    )
      throw new Error(`Reviewer engine '${String(engine)}' is not supported`);
    if (model !== undefined && (typeof model !== 'string' || model.length === 0 || model.length > 512))
      throw new Error('autoloop_spawn_reviewer reviewer_model must be a non-empty string of at most 512 characters');
    if (engine !== undefined) own.reviewer_engine = engine as EngineType;
    if (model !== undefined) own.reviewer_model = model;
    return await this._withAutoloopRoleMutation(runId, async () => {
      const ctx = this._liveAutoloop(runId);
      return await ctx.dispatcher.spawnReviewer(own);
    });
  }

  /**
   * Persist and enqueue one checkpoint-bound Reviewer-only request.
   * Preparation is durable before Runner acceptance; duplicates enqueue nothing.
   */
  async autoloopRequestReview(
    runId: string,
    input: RequestReviewArgs,
  ): Promise<{
    status: 'prepared' | 'duplicate';
    target: 'reviewer';
    idempotency_key: string;
  }> {
    const request = canonicalizeRequestReviewArgs(input);
    if (this._autoloopReviewDeleting.has(runId)) {
      throw new Error(`Autoloop run '${runId}' is being deleted`);
    }
    const predecessor = this._autoloopReviewTransactions.get(runId) ?? Promise.resolve();
    let release!: () => void;
    const transaction = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tail = predecessor.then(() => transaction);
    this._autoloopReviewTransactions.set(runId, tail);
    await predecessor;
    try {
      if (this._autoloopReviewDeleting.has(runId)) {
        throw new Error(`Autoloop run '${runId}' is being deleted`);
      }
      return await this._autoloopRequestReviewTransaction(runId, request);
    } finally {
      release();
      if (this._autoloopReviewTransactions.get(runId) === tail) {
        this._autoloopReviewTransactions.delete(runId);
      }
    }
  }

  private async _autoloopRequestReviewTransaction(
    runId: string,
    input: RequestReviewArgs,
  ): Promise<{
    status: 'prepared' | 'duplicate';
    target: 'reviewer';
    idempotency_key: string;
  }> {
    const request = canonicalizeRequestReviewArgs(input);
    const ctx = this._liveAutoloop(runId, 'requesting review');
    if (ctx.runner.state.status === 'paused') {
      throw new AutoloopChatStateError(
        'AUTOLOOP_RUN_PAUSED',
        `Autoloop run '${runId}' is paused; resume it before requesting review`,
        false,
        undefined,
        ctx.runner.state.status_reason,
      );
    }
    if (ctx.runner.state.status === 'terminated' || ctx.runner.state.status === 'crashed') {
      throw new AutoloopChatStateError(
        'AUTOLOOP_RUN_TERMINAL',
        `Autoloop run '${runId}' is terminal and cannot accept a review request`,
        false,
        undefined,
        ctx.runner.state.status_reason,
      );
    }
    const releasedIterations = this._autoloopReleasedReviewIterations.get(runId);
    if (
      !releasedIterations?.has(request.idempotency_key) &&
      (releasedIterations?.size ?? 0) >= MAX_RELEASED_REVIEW_IDENTITIES_PER_RUN
    ) {
      throw new Error('request_review retry capacity is exhausted for this Autoloop run');
    }
    const targetIter = releasedIterations?.get(request.idempotency_key) ?? ctx.runner.state.iter;
    const prepared = await ctx.dispatcher.requestReview(request, targetIter);
    if (prepared.status === 'prepared') {
      try {
        const statusBeforeSend = ctx.runner.state.status as string;
        if (statusBeforeSend === 'paused') {
          throw new AutoloopChatStateError(
            'AUTOLOOP_RUN_PAUSED',
            `Autoloop run '${runId}' became paused before Reviewer-only queue delivery`,
            false,
          );
        }
        if (statusBeforeSend === 'terminated' || statusBeforeSend === 'crashed') {
          throw new AutoloopChatStateError(
            'AUTOLOOP_RUN_TERMINAL',
            'Autoloop run became terminal before Reviewer-only queue delivery',
            false,
          );
        }
        await ctx.runner.send(AutoloopMsg.reviewRequest(targetIter, prepared.payload));
      } catch (error) {
        if (isCommittedSecureLedgerError(error)) throw error;
        ctx.dispatcher['releaseReviewRequest'](prepared.idempotency_key, prepared.payload);
        const byIdentity = releasedIterations ?? new Map<string, number>();
        byIdentity.set(prepared.idempotency_key, prepared.payload.iter);
        this._autoloopReleasedReviewIterations.set(runId, byIdentity);
        if (error instanceof Error && /was not delivered because the run became terminal$/.test(error.message)) {
          throw new AutoloopChatStateError(
            'AUTOLOOP_RUN_TERMINAL',
            'Autoloop run became terminal before Reviewer-only queue delivery',
            false,
          );
        }
        if (error instanceof Error && /was not delivered because the run is paused$/.test(error.message)) {
          throw new AutoloopChatStateError(
            'AUTOLOOP_RUN_PAUSED',
            `Autoloop run '${runId}' became paused before Reviewer-only queue delivery`,
            false,
          );
        }
        throw error;
      }
      ctx.dispatcher['acceptReviewRequest'](prepared.idempotency_key);
      releasedIterations?.delete(request.idempotency_key);
    } else {
      releasedIterations?.delete(request.idempotency_key);
    }
    if (releasedIterations?.size === 0) this._autoloopReleasedReviewIterations.delete(runId);
    return publicData({
      status: prepared.status,
      target: 'reviewer' as const,
      idempotency_key: prepared.idempotency_key,
    });
  }

  async autoloopStop(runId: string, reason = 'user-stop'): Promise<boolean> {
    return await this._withAutoloopRoleMutation(runId, async () => {
      const ctx = this.kernel.handle<AutoloopHandle & { runner: AutoloopRunner }>(runId, LEGACY_NODE);
      if (!ctx) return false;
      // Soft stop: a terminate envelope, so the three persistent agents shut down
      // and the persisted sessions survive for a later resume. The node's exit
      // watcher sees the status change and lets the run finish on its own.
      await ctx.runner.send(AutoloopMsg.terminate(ctx.runner.state.iter, { reason }));
      return true;
    });
  }

  /**
   * Re-attach a terminated run that lives in the registry but not in this
   * process's in-memory map. Re-creates dispatcher + runner with the same
   * run_id / workspace; ensurePlanner will pick up the Planner's claudeSessionId
   * from persistedSessions (kept on disk because dispatcher.shutdown was
   * called with keepPersisted) and Claude will resume the prior conversation.
   *
   * Returns the new in-memory state. Throws if the registry has no record
   * of this run.
   *
   * Note: if persistedSessions for the planner is empty (older run that
   * pre-dates this feature, OR the run was explicitly deleted), Claude will
   * start a fresh session with the same system prompt — chat memory from
   * Claude's own context is lost, but the chat.jsonl history we now persist
   * is still served via /autoloop/<id>/chat_history so the dashboard can
   * replay the conversation visually.
   */
  /**
   * Which roles of a stored autoloop run need a custom-engine config before it
   * can be resumed.
   *
   * Custom-engine configs are never persisted, so a resume has to be given them
   * again — and a caller that cannot find out which roles need one can only
   * guess. The dashboard's Resume button used to send an empty body
   * unconditionally, which meant a custom-engine run could be resumed from the
   * library and from the HTTP API but not from the UI that offers the button.
   *
   * Returns role names only. Nothing here is sensitive: the engine kind is
   * already in `spec.json`, and the answer is a list of roles, not credentials.
   */
  autoloopResumeRequirements(runId: string): { runId: string; rolesNeedingCustomEngine: AutoloopRoleName[] } {
    const record = loadRun(runId);
    if (!record || record.workflow !== 'autoloop') throw new Error(`Autoloop run '${runId}' not found`);
    const config = (record.spec.nodes.find((n) => n.id === LEGACY_NODE) as { config?: Record<string, unknown> })
      ?.config;
    const roles: AutoloopRoleName[] = [];
    for (const role of ['planner', 'coder', 'reviewer'] as AutoloopRoleName[]) {
      if (config?.[`${role}Engine`] === 'custom') roles.push(role);
    }
    return { runId, rolesNeedingCustomEngine: roles };
  }

  /**
   * Order a receipt-free stored resume against recovery's prepared claim.
   *
   * The lock is intentionally synchronous. `start` invokes `_resumeAutoloopRun`,
   * whose call to `kernel.resume` acquires and checkpoints the durable run lease
   * before returning its Promise. We release the recovery lock immediately
   * after that synchronous ownership boundary; we never pretend to hold it
   * across the asynchronous boot.
   */
  private _orderStoredResumeAgainstRecovery(
    ledger: SecureAutoloopLedger,
    runId: string,
    start: () => Promise<AutoloopState>,
  ): { kind: 'recovery'; receipt: RecoveryReceipt } | { kind: 'legacy'; operation: Promise<AutoloopState> } {
    const locked = withFileLock(
      path.join(ledger.directory, '.autoloop-recovery.lock'),
      () => {
        // The earlier snapshot only selects this compatibility candidate. The
        // decision itself is made here, alongside recovery's receipt append.
        const receipts = this._recoveryReceiptRows(ledger, runId);
        const appliedTokens = new Set(
          receipts.filter((row) => row.status === 'applied').map((row) => row.recovery_token),
        );
        const unresolved = receipts.find((row) => row.status === 'prepared' && !appliedTokens.has(row.recovery_token));
        const applied = [...receipts].reverse().find((row) => row.status === 'applied');
        const authoritative = unresolved ?? applied;
        if (authoritative) return { kind: 'recovery' as const, receipt: authoritative };

        // Calling an async function runs through its first await synchronously.
        // `_resumeAutoloopRun` reaches `kernel.resume`, and `kernel.resume`
        // durably acquires the lease without awaiting, before `start` returns.
        return { kind: 'legacy' as const, operation: start() };
      },
      { waitMs: 500 },
    );
    if (!locked.ok) {
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' stored resume lock is ${locked.reason}`,
      );
    }
    return locked.value;
  }

  async autoloopResume(
    runId: string,
    opts: {
      plannerCustomEngine?: CustomEngineConfig;
      coderCustomEngine?: CustomEngineConfig;
      reviewerCustomEngine?: CustomEngineConfig;
      /** Optional I4 migration. Omission preserves the historical resume path. */
      sendTimeoutMs?: number;
      /** Guards the exact I3 logical dispatch being advanced, when supplied. */
      pendingDispatchId?: string;
    } = {},
  ): Promise<AutoloopState> {
    const hasTimeoutIncrease = opts.sendTimeoutMs !== undefined;
    if (!hasTimeoutIncrease && opts.pendingDispatchId !== undefined) {
      throw new Error('pendingDispatchId requires a sendTimeoutMs increase');
    }
    if (
      opts.pendingDispatchId !== undefined &&
      (typeof opts.pendingDispatchId !== 'string' || opts.pendingDispatchId.length === 0)
    ) {
      throw new Error('pendingDispatchId must be a non-empty string');
    }

    const live = this.kernel.handle<AutoloopHandle & { runner: AutoloopRunner; dispatcher: ClaudeAgentDispatcher }>(
      runId,
      LEGACY_NODE,
    );
    if (live) {
      if (!hasTimeoutIncrease) return live.runner.state;

      const pending = live.runner.state.pending_dispatch;
      if (live.runner.state.status !== 'paused' || !pending) {
        throw new Error(`Autoloop run '${runId}' is not awaiting a recoverable send timeout`);
      }
      if (opts.pendingDispatchId === undefined) {
        throw new Error(`pendingDispatchId is required to resume timed-out dispatch '${pending.dispatch_id}'`);
      }
      const current = live.dispatcher.effectiveSendTimeoutMs;
      validateSendTimeoutIncrease(opts.sendTimeoutMs, current);
      if (opts.pendingDispatchId !== undefined && opts.pendingDispatchId !== pending.dispatch_id) {
        throw new Error(
          `pending dispatch '${opts.pendingDispatchId}' does not match current dispatch '${pending.dispatch_id}'`,
        );
      }

      // The checks above and the three operations below are synchronous. Audit
      // first, so an append failure leaves both the dispatcher and runner
      // untouched; after that no asynchronous work can swap the pending id.
      const migrationCommitError = appendSendTimeoutMigration(live.dispatcher.secureLedgerCapability, {
        runId,
        field: 'sendTimeoutMs',
        oldValue: current,
        newValue: opts.sendTimeoutMs,
        reason: 'recoverable_send_timeout_resume',
        pendingDispatchId: pending.dispatch_id,
      });
      live.dispatcher.increaseSendTimeoutMs(opts.sendTimeoutMs);
      if (!live.runner.resumeTimedOutDispatch(pending.dispatch_id)) {
        throw new Error(`Autoloop run '${runId}' pending dispatch changed during resume`);
      }
      this._autoloopPublishers.get(runId)?.();
      if (migrationCommitError) throw migrationCommitError.withAppliedOutcome('send_timeout_migration');
      return live.runner.state;
    }

    const record = loadRun(runId);
    if (!record || record.workflow !== 'autoloop') throw new Error(`Autoloop run '${runId}' not found`);
    const config = (record.spec.nodes.find((n) => n.id === LEGACY_NODE) as { config?: Record<string, unknown> })
      ?.config;
    if (!config) throw new Error(`Autoloop run '${runId}' has no stored configuration to restart from`);

    // Validate the full restart configuration before touching anything. The
    // spec is the immutable record of how the run was started, so a resume
    // reproduces it exactly instead of reconstructing it from a registry row
    // whose older versions omitted the engine fields entirely.
    validateAutoloopRole('planner', config.plannerEngine as EngineType | undefined, opts.plannerCustomEngine);
    validateAutoloopRole('coder', config.coderEngine as EngineType | undefined, opts.coderCustomEngine);
    validateAutoloopRole('reviewer', config.reviewerEngine as EngineType | undefined, opts.reviewerCustomEngine);

    const workspace = typeof config.workspace === 'string' ? config.workspace : record.cwd;
    // Pin one run capability for the complete stored-resume transaction. It
    // remains the authority for audit replay, any prepared migration append,
    // and the dispatcher that boots below; no stage re-resolves the path.
    const secureLedger = SecureAutoloopLedger.open(workspace, runId, {
      create: false,
      validateExistingFlatFiles: ['decisions.jsonl'],
      logger: this.logger,
    });
    const storedContext = readStoredAutoloopResumeContext(secureLedger, runId, config.sendTimeoutMs);
    const nodeState = (record.nodes[LEGACY_NODE]?.data as { state?: AutoloopState } | undefined)?.state;
    const recordCarriesPending = nodeState
      ? Object.prototype.hasOwnProperty.call(nodeState, 'pending_dispatch')
      : false;
    const pending = recordCarriesPending
      ? isSendTimeoutPayload(nodeState?.pending_dispatch)
        ? nodeState.pending_dispatch
        : null
      : storedContext.pendingDispatch;

    const originalSendTimeoutMs = (config.sendTimeoutMs as number | undefined) ?? DEFAULT_SEND_TIMEOUT_MS;
    const recoveryReceipts = this._recoveryReceiptRows(secureLedger, runId);
    const appliedRecoveryTokens = new Set(
      recoveryReceipts.filter((row) => row.status === 'applied').map((row) => row.recovery_token),
    );
    const unresolvedRecoveryReceipt = recoveryReceipts.find(
      (row) => row.status === 'prepared' && !appliedRecoveryTokens.has(row.recovery_token),
    );
    const latestAppliedRecoveryReceipt = [...recoveryReceipts].reverse().find((row) => row.status === 'applied');
    const authoritativeRecoveryReceipt = unresolvedRecoveryReceipt ?? latestAppliedRecoveryReceipt;
    const hasRecoveryReceipt = recoveryReceipts.length > 0;
    const hasLegacyStoredResumeState =
      !hasRecoveryReceipt &&
      (storedContext.effectiveSendTimeoutMs !== originalSendTimeoutMs ||
        pending !== null ||
        record.state === 'cancelled');
    if (hasRecoveryReceipt || (!hasTimeoutIncrease && !hasLegacyStoredResumeState)) {
      // A receipt is authoritative over every compatibility shortcut. Without
      // one, legacy timeout state and receipt-free cancellation keep their
      // established path below.
      const bootOverrides: RecoveryBootOverrides = {
        plannerCustomEngine: opts.plannerCustomEngine,
        coderCustomEngine: opts.coderCustomEngine,
        reviewerCustomEngine: opts.reviewerCustomEngine,
        sendTimeoutMs: storedContext.effectiveSendTimeoutMs,
      };
      const recoveryToken =
        authoritativeRecoveryReceipt?.recovery_token ?? (await this._autoloopRecover(runId)).assessment.recovery_token;
      const recovery = await this._autoloopRecover(
        runId,
        { apply: true, recovery_token: recoveryToken },
        bootOverrides,
      );
      const recovered = this.getAutoloop(runId);
      if (recovered) return recovered.runner.state;
      if (recovery.receipt?.status === 'applied') {
        const replayedState = autoloopStateFromRecord(record);
        if (replayedState) return replayedState;
      }
      throw new AutoloopRecoveryError(
        'AUTOLOOP_RECOVERY_INCOMPLETE',
        `Autoloop run '${runId}' recovery has no current live state`,
      );
    }

    if (!hasTimeoutIncrease) {
      const existing = this._autoloopStoredResumeTransactions.get(runId);
      if (existing) return await existing;
    }

    if (hasTimeoutIncrease && pending && opts.pendingDispatchId === undefined) {
      throw new Error(`pendingDispatchId is required to resume timed-out dispatch '${pending.dispatch_id}'`);
    }
    if (opts.pendingDispatchId !== undefined) {
      if (!pending) {
        throw new Error(`Autoloop run '${runId}' has no pending dispatch to match '${opts.pendingDispatchId}'`);
      }
      if (opts.pendingDispatchId !== pending.dispatch_id) {
        throw new Error(
          `pending dispatch '${opts.pendingDispatchId}' does not match stored dispatch '${pending.dispatch_id}'`,
        );
      }
    }

    const nextSendTimeoutMs = opts.sendTimeoutMs ?? storedContext.effectiveSendTimeoutMs;
    if (hasTimeoutIncrease) validateSendTimeoutIncrease(nextSendTimeoutMs, storedContext.effectiveSendTimeoutMs);

    const migration: Omit<SendTimeoutMigrationAuditRecord, 'ts' | 'timestamp' | 'kind' | 'actor'> | undefined =
      hasTimeoutIncrease
        ? {
            runId,
            field: 'sendTimeoutMs',
            oldValue: storedContext.effectiveSendTimeoutMs,
            newValue: nextSendTimeoutMs,
            reason: pending ? 'recoverable_send_timeout_resume' : 'stored_run_resume',
            ...(pending ? { pendingDispatchId: pending.dispatch_id } : {}),
          }
        : undefined;
    const preparedMigration = migration ? prepareSendTimeoutMigrationAppend(secureLedger, migration) : undefined;
    let migrationCommitted = false;
    let migrationCommitError: SecureAutoloopLedgerCommitError | undefined;
    let resumeOperation: Promise<AutoloopState> | undefined;
    try {
      // Custom-engine configs are never persisted (they can carry secrets), so
      // a resume must be given them again by the caller.
      const ordered = this._orderStoredResumeAgainstRecovery(secureLedger, runId, () =>
        this._resumeAutoloopRun(
          runId,
          {
            ...config,
            // Effective migrations are replayed from append-only audit rather
            // than written back into the immutable original spec.
            sendTimeoutMs: nextSendTimeoutMs,
            plannerCustomEngine: opts.plannerCustomEngine,
            coderCustomEngine: opts.coderCustomEngine,
            reviewerCustomEngine: opts.reviewerCustomEngine,
            _secureLedger: secureLedger,
          } as Parameters<SessionManager['_bootAutoloop']>[0],
          {
            timeoutMigration: hasTimeoutIncrease,
            commitTimeoutMigration: preparedMigration
              ? () => {
                  if (migrationCommitted) return;
                  try {
                    migrationCommitError = commitPreparedSendTimeoutMigration(preparedMigration);
                  } finally {
                    // Bytes committed is itself a terminal append state even if
                    // a durability barrier remains incomplete. Any boot retry
                    // must observe this row, never append the migration again.
                    migrationCommitted = preparedMigration.append.committed;
                  }
                }
              : undefined,
          },
        ),
      );
      if (ordered.kind === 'recovery') {
        const recovery = await this._autoloopRecover(
          runId,
          { apply: true, recovery_token: ordered.receipt.recovery_token },
          {
            plannerCustomEngine: opts.plannerCustomEngine,
            coderCustomEngine: opts.coderCustomEngine,
            reviewerCustomEngine: opts.reviewerCustomEngine,
            sendTimeoutMs: storedContext.effectiveSendTimeoutMs,
          },
        );
        const recovered = this.getAutoloop(runId);
        if (recovered) return recovered.runner.state;
        if (recovery.receipt?.status === 'applied') {
          const replayedRecord = loadRun(runId);
          const replayedState = replayedRecord ? autoloopStateFromRecord(replayedRecord) : undefined;
          if (replayedState) return replayedState;
        }
        throw new AutoloopRecoveryError(
          'AUTOLOOP_RECOVERY_INCOMPLETE',
          `Autoloop run '${runId}' recovery has no current live state`,
        );
      }
      resumeOperation = ordered.operation;
      if (!hasTimeoutIncrease) this._autoloopStoredResumeTransactions.set(runId, resumeOperation);
      const state = await resumeOperation;
      if (migrationCommitError) throw migrationCommitError.withAppliedOutcome('send_timeout_migration');
      return state;
    } finally {
      if (resumeOperation && this._autoloopStoredResumeTransactions.get(runId) === resumeOperation) {
        this._autoloopStoredResumeTransactions.delete(runId);
      }
      if (preparedMigration) {
        try {
          preparedMigration.append.close();
        } catch (err) {
          // Descriptor cleanup cannot retroactively turn a committed append
          // and successful startup into a failed migration.
          this.logger.warn?.(`[autoloop/${runId}] failed to close migration audit: ${(err as Error).message}`);
        }
      }
    }
  }

  /** Re-attach a stored autoloop run: same run id, same spec, fresh engine. */
  private async _resumeAutoloopRun(
    runId: string,
    config: Parameters<SessionManager['_bootAutoloop']>[0],
    opts: { timeoutMigration?: boolean; commitTimeoutMigration?: () => void } = {},
  ): Promise<AutoloopState> {
    const tag = `${runId}:${randomUUID()}`;
    const ready = new Promise<{ plannerSession: string; state: AutoloopState }>((resolve, reject) => {
      this._autoloopReady.set(tag, { resolve, reject });
    });
    this._autoloopStarting.set(tag, runId);
    // The custom-engine configs the caller re-supplied go into the run's secret
    // bag, which is where the node reads them from. They used to be stashed in a
    // separate map the executor no longer consulted, so a resume in a fresh
    // process — the case that matters — silently got none of them.
    const secrets = {
      plannerCustomEngine: config.plannerCustomEngine,
      coderCustomEngine: config.coderCustomEngine,
      reviewerCustomEngine: config.reviewerCustomEngine,
      // Resume-only effective value. This travels in the in-memory bag so the
      // immutable spec continues to describe the run's original configuration.
      sendTimeoutMs: config.sendTimeoutMs,
      _resumeTimeoutMigration: opts.timeoutMigration || undefined,
      _commitTimeoutMigration: opts.commitTimeoutMigration,
      _secureLedger: config._secureLedger,
    };
    try {
      // `restart: true` because an autoloop resume means "bring the loop back
      // up", not "carry on from where the kernel left off" — the run is
      // normally terminated when someone resumes it.
      const record = await this.kernel.resume(runId, { restart: true, secrets, tag });
      // Race readiness against the run ending: a node that fails before it
      // publishes would otherwise leave this awaiting a signal that is never
      // coming.
      const finished = this.kernel
        .wait(record.runId)
        .then((r) => Promise.reject(new Error(r?.error ?? `autoloop run '${runId}' ended before it came up`)));
      const { state } = await Promise.race([ready, finished]);
      return state;
    } finally {
      this._autoloopReady.delete(tag);
      this._autoloopStarting.delete(tag);
    }
  }

  /**
   * Delete a run: really gone, not paused.
   *
   * The two `Set` fences this used to open with — one refusing a delete while a
   * start was in flight, one blocking a concurrent start during the async
   * teardown — protected a shared `Map` that no longer exists. Cancelling the
   * run is what stops it, and the run store refuses to recreate a live id.
   */
  async autoloopDelete(runId: string): Promise<boolean> {
    // Refuse to tear down a run that is still coming up: its Planner session is
    // mid-startSession, so deleting now would drop the run and orphan a session
    // that finishes starting a moment later. `_autoloopReady` holds an entry for
    // exactly the window between "run created" and "engine up", which is the
    // window that used to need a dedicated `_startingAutoloops` Set.
    if ([...this._autoloopStarting.values()].includes(runId)) {
      throw new Error(`Autoloop with id '${runId}' is still starting`);
    }
    const deleteCount = (this._autoloopReviewDeleteCounts.get(runId) ?? 0) + 1;
    this._autoloopReviewDeleteCounts.set(runId, deleteCount);
    this._autoloopReviewDeleting.add(runId);
    try {
      await (this._autoloopReviewTransactions.get(runId) ?? Promise.resolve());
      this._autoloopReleasedReviewIterations.delete(runId);
      this._autoloopReviewTransactions.delete(runId);
      const ctx = this.kernel.handle<AutoloopHandle & { runner: AutoloopRunner; dispatcher: ClaudeAgentDispatcher }>(
        runId,
        LEGACY_NODE,
      );
      let touched = false;
      if (ctx) {
        // Delete = "really gone". Call dispatcher.shutdown directly with
        // purge:true so persistedSessions entries are removed too —
        // otherwise the Claude Planner conversation lingers on disk and the
        // run could be /resume'd back to life. Bypassing runner.send is
        // intentional: the runner's terminate path is meant to be the
        // soft-pause we use for autoloopStop / autoloopResume, which keeps
        // persisted state intact.
        try {
          await ctx.dispatcher.shutdown('user-delete', { purge: true });
        } catch (err) {
          this.logger.warn?.(`[autoloop/${runId}] dispatcher shutdown during delete failed: ${(err as Error).message}`);
        }
        try {
          ctx.runner.stop();
        } catch {
          /* runner may already be stopped */
        }
        this.kernel.cancel(runId);
        touched = true;
      } else {
        // Disk-only run: ensure any leftover persistedSessions entry for the
        // Planner is cleaned up so it isn't resumed by accident later.
        try {
          await this.stopSession(`autoloop-${runId}-planner`);
        } catch {
          /* session not in memory — fine */
        }
        this.persistedSessions.delete(`autoloop-${runId}-planner`);
        this.persistedSessions.delete(`autoloop-${runId}-coder`);
        this.persistedSessions.delete(`autoloop-${runId}-reviewer`);
        const names = [`autoloop-${runId}-planner`, `autoloop-${runId}-coder`, `autoloop-${runId}-reviewer`];
        this._withAgentRegistryLock((authoritative) => {
          const updatedSessions = new Map(authoritative);
          for (const name of names) updatedSessions.delete(name);
          return { value: true, updatedSessions };
        });
      }
      // No registry to scrub: the run record IS the registry, and removing it is
      // the delete. The ledger directory under tasks/<runId>/ is deliberately left
      // alone — postmortem artifacts (chat history, push log, plan.md) outlive the
      // run, exactly as before.
      if (loadRun(runId)) {
        this.kernel.delete(runId);
        touched = true;
      }
      return touched;
    } finally {
      const remaining = (this._autoloopReviewDeleteCounts.get(runId) ?? 1) - 1;
      if (remaining > 0) {
        this._autoloopReviewDeleteCounts.set(runId, remaining);
      } else {
        this._autoloopReviewDeleteCounts.delete(runId);
        this._autoloopReviewDeleting.delete(runId);
      }
    }
  }

  /** Used by embedded-server to attach SSE listeners. Live runs only. */
  getAutoloop(runId: string): { runner: AutoloopRunner; dispatcher: ClaudeAgentDispatcher } | undefined {
    const handle = this.kernel.handle<{ runner: AutoloopRunner; dispatcher: ClaudeAgentDispatcher }>(
      runId,
      LEGACY_NODE,
    );
    return handle ? { runner: handle.runner, dispatcher: handle.dispatcher } : undefined;
  }

  private _cleanupIdleSessions(): void {
    const ttlMs = this.pluginConfig.sessionTtlMinutes * 60_000;
    const now = Date.now();
    let pidsChanged = false;
    for (const [name, managed] of this.sessions) {
      if (now - managed.lastActivity > ttlMs) {
        this.logger.info(`Cleaning up idle in-memory session: ${name}`);
        try {
          managed.session.stop();
        } catch {
          // Best-effort — session may already be dead; must not block TTL cleanup
        }
        this.sessions.delete(name);
        // The child is gone, so its PID must go too — `stopSession` does this
        // and the TTL path did not, so `_activePids` only ever grew and the
        // next `_savePids()` rewrote those dead PIDs to disk under the current
        // owner. After an unclean exit `_cleanupOrphanedPids()` reads them back
        // and probes each one; a PID the OS has since recycled to a
        // coding-CLI-shaped process gets killed.
        this._activePids.delete(name);
        pidsChanged = true;
        // NOTE: do NOT delete from persistedSessions — idle cleanup is
        // in-memory only. Persisted entries survive for PERSIST_DISK_TTL_MS
        // (7 days) so the session can be resumed after a gateway restart.
      }
    }
    if (pidsChanged) this._savePids();
    // Prune disk entries that exceeded the longer disk TTL
    let pruned = false;
    for (const [name, entry] of this.persistedSessions) {
      if (now - entry.lastActivity > PERSIST_DISK_TTL_MS) {
        this.persistedSessions.delete(name);
        pruned = true;
      }
    }
    if (pruned) this._persistRegistrySnapshot();
  }
}
