/**
 * Inbox message envelope + discriminated union of v2 message types.
 *
 * Wire format: serialised as JSON in InboxManager's `text` field, with
 * `summary` set to the message `type` for human-readable inbox listings.
 * Routing addressing convention: session name = `autoloop:<run_id>:<role>`,
 * where role ∈ { planner, coder, reviewer, runner }. The literal `user`
 * is *not* a session — it appears only as the `from` of an external chat
 * injection (`autoloop_chat`) and as the `to` of `push_user` messages
 * (which the runner consumes to invoke notify_user).
 */

export type AutoloopRole = 'planner' | 'coder' | 'reviewer' | 'runner' | 'user';

export interface AutoloopEnvelope<T extends AutoloopMessageType = AutoloopMessageType> {
  msg_id: string;
  iter: number;
  from: AutoloopRole;
  to: AutoloopRole;
  type: T;
  ts: string;
  payload: PayloadFor<T>;
}

// ─── Payloads ────────────────────────────────────────────────────────────────

export interface UserChatPayload {
  text: string;
}

export interface DirectivePayload {
  goal: string;
  constraints: string[];
  success_criteria: string[];
  max_attempts: number;
}

export interface DirectiveAckPayload {
  understood: boolean;
  clarification?: string;
}

export interface IterArtifactsPayload {
  diff: string;
  eval_output: unknown; // Loosely typed at this layer; v1's EvalOutput shape will be reused in S4.
  files_changed: string[];
}

export interface ReviewRequestPayload {
  iter: number;
  ledger_path: string;
  prior_metrics: number[];
}

export interface ReviewVerdictPayload {
  decision: 'advance' | 'hold' | 'rollback';
  metric: number | null;
  audit_notes: string;
  /**
   * Set only when an acceptance contract ran and passed. This is what makes
   * `on_target_hit` fireable: before it existed the policy key was declared,
   * defaulted and whitelisted for updates, but had no firing site anywhere —
   * autoloop had no way to notice it had succeeded, only ways to notice it was
   * failing. Absent means no contract was configured, not that it failed.
   */
  accepted?: boolean;
  /** Evidence bundle id backing `accepted`. */
  evidence_id?: string;
}

export interface IterDonePayload {
  iter: number;
  verdict: 'advance' | 'hold' | 'rollback';
  metric: number | null;
  regression?: boolean;
}

export type PushLevel = 'info' | 'warn' | 'decision' | 'error';
export type PushChannel = 'auto' | 'wechat' | 'webchat' | 'both' | 'email';

export interface PushUserPayload {
  level: PushLevel;
  summary: string;
  detail?: string;
  channel: PushChannel;
}

export interface PausePayload {
  reason: string;
}

export type ResumePayload = Record<string, never>;

export interface TerminatePayload {
  reason: string;
}

export type AutoloopOperationErrorCode =
  | 'AUTOLOOP_EMPTY_REPLY'
  | 'AUTOLOOP_SESSION_NOT_CREATED'
  | 'AUTOLOOP_ENGINE_FAILURE'
  | 'AUTOLOOP_REQUIRED_TOOL_DENIED'
  | 'AUTOLOOP_CONTROL_MALFORMED'
  | 'AUTOLOOP_CONTROL_APPLICATION_FAILED'
  | 'AUTOLOOP_CONTROL_NOT_PERSISTED'
  | 'AUTOLOOP_RESET_POSTCONDITION_FAILED'
  | 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE'
  | 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE'
  | 'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE'
  | 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID';

/**
 * Surfaced when an agent subprocess dies, a phase-bound side effect fails
 * (e.g., git commit), or any other unrecoverable per-iter error needs to
 * become visible to the runner instead of being swallowed inside a fake
 * directive_ack. The runner counts consecutive phase_errors and trips a
 * circuit-breaker terminate when the configured threshold is reached.
 */
export interface PhaseErrorPayload {
  agent: 'planner' | 'coder' | 'reviewer';
  phase: string;
  /** Stable internal classification. Public HTTP/MCP mapping is handled separately. */
  code?: AutoloopOperationErrorCode;
  /** True when durable ledger or sandbox state was committed before the failure was detected. */
  committed?: true;
  /** Committed filesystem outcomes are never safe to retry as ordinary engine turns. */
  retryable?: false;
  error: string;
}

/**
 * Recoverable record emitted when an agent turn reaches its configured send
 * deadline. The dispatch identity is stable for the original logical message,
 * so a later resume can refer to this exact turn without guessing or replaying
 * it implicitly.
 */
export interface SendTimeoutPayload {
  status: 'awaiting_resume';
  dispatch_id: string;
  agent: 'planner' | 'coder' | 'reviewer';
  message_id: string;
  message_type: AutoloopMessageType;
  iter: number;
  timeout_ms: number;
  error: string;
}

// ─── Discriminated union ─────────────────────────────────────────────────────

export type AutoloopMessageType =
  | 'chat'
  | 'directive'
  | 'directive_ack'
  | 'iter_artifacts'
  | 'review_request'
  | 'review_verdict'
  | 'iter_done'
  | 'push_user'
  | 'pause'
  | 'resume'
  | 'terminate'
  | 'phase_error'
  | 'send_timeout';

type PayloadMap = {
  chat: UserChatPayload;
  directive: DirectivePayload;
  directive_ack: DirectiveAckPayload;
  iter_artifacts: IterArtifactsPayload;
  review_request: ReviewRequestPayload;
  review_verdict: ReviewVerdictPayload;
  iter_done: IterDonePayload;
  push_user: PushUserPayload;
  pause: PausePayload;
  resume: ResumePayload;
  terminate: TerminatePayload;
  phase_error: PhaseErrorPayload;
  send_timeout: SendTimeoutPayload;
};

export type PayloadFor<T extends AutoloopMessageType> = PayloadMap[T];

export type AnyAutoloopMessage = {
  [T in AutoloopMessageType]: AutoloopEnvelope<T>;
}[AutoloopMessageType];

// ─── Sender/recipient validity table ────────────────────────────────────────
//
// Allowed (from, to, type) tuples. Anything else is a routing error caught
// by `validateMessage`. Centralising this table keeps the runner's switch
// statements honest and gives us one place to update when v2.1 adds new
// message types (e.g. weixin-inbound chat reply).

const ALLOWED_ROUTES: ReadonlyArray<readonly [AutoloopRole, AutoloopRole, AutoloopMessageType]> = [
  ['user', 'planner', 'chat'],
  ['planner', 'coder', 'directive'],
  ['coder', 'planner', 'directive_ack'],
  ['coder', 'runner', 'iter_artifacts'],
  ['runner', 'reviewer', 'review_request'],
  ['reviewer', 'runner', 'review_verdict'],
  ['runner', 'planner', 'iter_done'],
  ['planner', 'user', 'push_user'],
  ['planner', 'runner', 'pause'],
  ['planner', 'runner', 'resume'],
  ['planner', 'runner', 'terminate'],
  ['coder', 'runner', 'phase_error'],
  ['reviewer', 'runner', 'phase_error'],
  ['planner', 'runner', 'phase_error'],
  ['coder', 'runner', 'send_timeout'],
  ['reviewer', 'runner', 'send_timeout'],
  ['planner', 'runner', 'send_timeout'],
];

export interface AutoloopRoutingIdentity {
  readonly msg_id: string;
  readonly iter: number;
  readonly from: string;
  readonly to: string;
  readonly type: string;
  readonly ts: string;
}

const ROUTING_IDENTITY_FIELDS = ['msg_id', 'iter', 'from', 'to', 'type', 'ts'] as const;

function safeRoutingIdentity(value: unknown): AutoloopRoutingIdentity | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return undefined;
  const captured = Object.create(null) as Record<string, unknown>;
  for (let index = 0; index < ROUTING_IDENTITY_FIELDS.length; index += 1) {
    const key = ROUTING_IDENTITY_FIELDS[index];
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !Object.hasOwn(descriptor, 'value')) return undefined;
    captured[key] = descriptor.value;
  }
  if (
    typeof captured.msg_id !== 'string' ||
    !Number.isSafeInteger(captured.iter) ||
    (captured.iter as number) < 0 ||
    typeof captured.from !== 'string' ||
    typeof captured.to !== 'string' ||
    typeof captured.type !== 'string' ||
    typeof captured.ts !== 'string'
  ) {
    return undefined;
  }
  const identity = Object.create(null) as Record<string, unknown>;
  for (let index = 0; index < ROUTING_IDENTITY_FIELDS.length; index += 1) {
    const key = ROUTING_IDENTITY_FIELDS[index];
    Object.defineProperty(identity, key, { enumerable: true, value: captured[key] });
  }
  Object.freeze(identity);
  return identity as unknown as AutoloopRoutingIdentity;
}

export class AutoloopRoutingError extends Error {
  declare readonly envelope?: AutoloopRoutingIdentity;

  constructor(msg: string, envelope?: unknown) {
    super(msg);
    this.name = 'AutoloopRoutingError';
    const identity = safeRoutingIdentity(envelope);
    if (identity !== undefined) {
      Object.defineProperty(this, 'envelope', { enumerable: true, value: identity });
    }
  }
}

function invalidDeliveryPayload(type: string, detail: string): never {
  const label = type === 'directive_ack' ? 'Directive_ack' : type[0].toUpperCase() + type.slice(1);
  throw new AutoloopRoutingError(`${label} payload is invalid: ${detail}`);
}

function canonicalPayloadFields(
  payload: unknown,
  type: string,
  allowed: readonly string[],
  required: readonly string[],
): Record<string, unknown> {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
    invalidDeliveryPayload(type, 'expected an object');
  }
  const keys = Reflect.ownKeys(payload);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    let supported = typeof key === 'string';
    if (supported) {
      supported = false;
      for (let allowedIndex = 0; allowedIndex < allowed.length; allowedIndex += 1) {
        if (key === allowed[allowedIndex]) {
          supported = true;
          break;
        }
      }
    }
    if (!supported) invalidDeliveryPayload(type, 'contains unsupported fields');
  }

  const fields = Object.create(null) as Record<string, unknown>;
  for (let index = 0; index < allowed.length; index += 1) {
    const key = allowed[index];
    const descriptor = Object.getOwnPropertyDescriptor(payload, key);
    let isRequired = false;
    for (let requiredIndex = 0; requiredIndex < required.length; requiredIndex += 1) {
      if (key === required[requiredIndex]) {
        isRequired = true;
        break;
      }
    }
    if (!descriptor) {
      if (isRequired) invalidDeliveryPayload(type, `${key} must be an own data property`);
      continue;
    }
    if (!Object.hasOwn(descriptor, 'value')) {
      invalidDeliveryPayload(type, `${key} must be an own data property`);
    }
    Object.defineProperty(fields, key, { enumerable: true, value: descriptor.value });
  }
  return fields;
}

function canonicalPrimitiveArray<T>(
  value: unknown,
  type: string,
  key: string,
  accepts: (candidate: unknown) => candidate is T,
  expected: string,
): T[] {
  if (!Array.isArray(value)) {
    invalidDeliveryPayload(type, `${key} must be ${expected}`);
  }
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
  if (
    !lengthDescriptor ||
    !Object.hasOwn(lengthDescriptor, 'value') ||
    !Number.isSafeInteger(lengthDescriptor.value) ||
    lengthDescriptor.value < 0
  ) {
    invalidDeliveryPayload(type, `${key} must have an own data length`);
  }
  const length = lengthDescriptor.value as number;
  const ownKeys = Reflect.ownKeys(value);
  if (ownKeys.length !== length + 1) {
    invalidDeliveryPayload(type, `${key} must contain only exact contiguous indices`);
  }
  const clone: T[] = [];
  for (let index = 0; index < length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (!descriptor || !Object.hasOwn(descriptor, 'value') || !accepts(descriptor.value)) {
      invalidDeliveryPayload(type, `${key} must be ${expected}`);
    }
    Object.defineProperty(clone, String(index), {
      configurable: true,
      enumerable: true,
      value: descriptor.value,
      writable: true,
    });
  }
  Object.setPrototypeOf(clone, null);
  Object.freeze(clone);
  return clone;
}

function canonicalDirectiveStringArray(value: unknown, key: 'constraints' | 'success_criteria'): string[] {
  return canonicalPrimitiveArray(
    value,
    'directive',
    key,
    (candidate): candidate is string => typeof candidate === 'string',
    'an array of strings',
  );
}

const MAX_EVAL_OUTPUT_DEPTH = 64;
const MAX_EVAL_OUTPUT_CONTAINER_ITEMS = 10_000;
const MAX_EVAL_OUTPUT_NODES = 100_000;
const MAX_EVAL_OUTPUT_STRING_CODE_UNITS = 1_048_576;
const MAX_EVAL_OUTPUT_TOTAL_STRING_CODE_UNITS = 4_194_304;

interface EvalOutputSnapshotState {
  active: WeakSet<object>;
  nodes: number;
  stringCodeUnits: number;
}

function accountEvalOutputString(value: string, state: EvalOutputSnapshotState): void {
  if (value.length > MAX_EVAL_OUTPUT_STRING_CODE_UNITS) {
    invalidDeliveryPayload('iter_artifacts', 'eval_output contains an oversized string');
  }
  state.stringCodeUnits += value.length;
  if (state.stringCodeUnits > MAX_EVAL_OUTPUT_TOTAL_STRING_CODE_UNITS) {
    invalidDeliveryPayload('iter_artifacts', 'eval_output exceeds the total string-size limit');
  }
}

function canonicalEvalOutputValue(value: unknown, depth: number, state: EvalOutputSnapshotState): unknown {
  if (depth > MAX_EVAL_OUTPUT_DEPTH) {
    invalidDeliveryPayload('iter_artifacts', 'eval_output exceeds the nesting-depth limit');
  }
  state.nodes += 1;
  if (state.nodes > MAX_EVAL_OUTPUT_NODES) {
    invalidDeliveryPayload('iter_artifacts', 'eval_output exceeds the node-count limit');
  }

  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    accountEvalOutputString(value, state);
    return value;
  }
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      invalidDeliveryPayload('iter_artifacts', 'eval_output numbers must be finite');
    }
    return value;
  }
  if (typeof value !== 'object') {
    invalidDeliveryPayload('iter_artifacts', 'eval_output contains a non-JSON value');
  }
  if (state.active.has(value)) {
    invalidDeliveryPayload('iter_artifacts', 'eval_output must not contain cycles');
  }

  if (Array.isArray(value)) {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Array.prototype && prototype !== null) {
      invalidDeliveryPayload('iter_artifacts', 'eval_output arrays must not contain inherited data');
    }
    const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
    if (
      !lengthDescriptor ||
      !Object.hasOwn(lengthDescriptor, 'value') ||
      !Number.isSafeInteger(lengthDescriptor.value) ||
      lengthDescriptor.value < 0 ||
      lengthDescriptor.value > MAX_EVAL_OUTPUT_CONTAINER_ITEMS
    ) {
      invalidDeliveryPayload('iter_artifacts', 'eval_output array length is invalid or unbounded');
    }
    const length = lengthDescriptor.value as number;
    const keys = Reflect.ownKeys(value);
    if (keys.length !== length + 1) {
      invalidDeliveryPayload('iter_artifacts', 'eval_output arrays must contain only exact contiguous indices');
    }

    state.active.add(value);
    try {
      const canonical: unknown[] = [];
      for (let index = 0; index < length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (!descriptor || !Object.hasOwn(descriptor, 'value') || descriptor.enumerable !== true) {
          invalidDeliveryPayload('iter_artifacts', 'eval_output array entries must be own enumerable data');
        }
        Object.defineProperty(canonical, String(index), {
          configurable: true,
          enumerable: true,
          value: canonicalEvalOutputValue(descriptor.value, depth + 1, state),
          writable: true,
        });
      }
      Object.setPrototypeOf(canonical, null);
      Object.freeze(canonical);
      return canonical;
    } finally {
      state.active.delete(value);
    }
  }

  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) {
    invalidDeliveryPayload('iter_artifacts', 'eval_output objects must not contain inherited data');
  }
  const keys = Reflect.ownKeys(value);
  if (keys.length > MAX_EVAL_OUTPUT_CONTAINER_ITEMS) {
    invalidDeliveryPayload('iter_artifacts', 'eval_output object has too many fields');
  }

  state.active.add(value);
  try {
    const canonical = Object.create(null) as Record<string, unknown>;
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      if (typeof key !== 'string') {
        invalidDeliveryPayload('iter_artifacts', 'eval_output object fields must be strings');
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !Object.hasOwn(descriptor, 'value') || descriptor.enumerable !== true) {
        invalidDeliveryPayload('iter_artifacts', 'eval_output fields must be own enumerable data');
      }
      accountEvalOutputString(key, state);
      Object.defineProperty(canonical, key, {
        enumerable: true,
        value: canonicalEvalOutputValue(descriptor.value, depth + 1, state),
      });
    }
    Object.freeze(canonical);
    return canonical;
  } finally {
    state.active.delete(value);
  }
}

function canonicalEvalOutput(value: unknown): unknown {
  return canonicalEvalOutputValue(value, 0, {
    active: new WeakSet<object>(),
    nodes: 0,
    stringCodeUnits: 0,
  });
}

function canonicalDirectivePayload(payload: unknown): DirectivePayload {
  const fields = canonicalPayloadFields(
    payload,
    'directive',
    ['goal', 'constraints', 'success_criteria', 'max_attempts'],
    ['goal', 'constraints', 'success_criteria', 'max_attempts'],
  );
  const goal = fields.goal;
  const constraints = canonicalDirectiveStringArray(fields.constraints, 'constraints');
  const successCriteria = canonicalDirectiveStringArray(fields.success_criteria, 'success_criteria');
  const maxAttempts = fields.max_attempts;
  if (typeof goal !== 'string') {
    invalidDeliveryPayload('directive', 'goal must be a string');
  }
  if (!Number.isSafeInteger(maxAttempts) || (maxAttempts as number) <= 0) {
    invalidDeliveryPayload('directive', 'max_attempts must be a positive safe integer');
  }

  const canonical = Object.create(null) as DirectivePayload;
  Object.defineProperty(canonical, 'goal', { enumerable: true, value: goal });
  Object.defineProperty(canonical, 'constraints', { enumerable: true, value: constraints });
  Object.defineProperty(canonical, 'success_criteria', { enumerable: true, value: successCriteria });
  Object.defineProperty(canonical, 'max_attempts', { enumerable: true, value: maxAttempts });
  Object.freeze(canonical);
  return canonical;
}

function canonicalChatPayload(payload: unknown): UserChatPayload {
  const fields = canonicalPayloadFields(payload, 'chat', ['text'], ['text']);
  if (typeof fields.text !== 'string') invalidDeliveryPayload('chat', 'text must be a string');
  const canonical = Object.create(null) as UserChatPayload;
  Object.defineProperty(canonical, 'text', { enumerable: true, value: fields.text });
  Object.freeze(canonical);
  return canonical;
}

function canonicalDirectiveAckPayload(payload: unknown): DirectiveAckPayload {
  const fields = canonicalPayloadFields(payload, 'directive_ack', ['understood', 'clarification'], ['understood']);
  if (typeof fields.understood !== 'boolean') {
    invalidDeliveryPayload('directive_ack', 'understood must be a boolean');
  }
  const hasClarification = Object.hasOwn(fields, 'clarification');
  if (hasClarification && typeof fields.clarification !== 'string') {
    invalidDeliveryPayload('directive_ack', 'clarification must be a string when present');
  }
  const canonical = Object.create(null) as DirectiveAckPayload;
  Object.defineProperty(canonical, 'understood', { enumerable: true, value: fields.understood });
  if (hasClarification) {
    Object.defineProperty(canonical, 'clarification', { enumerable: true, value: fields.clarification });
  }
  Object.freeze(canonical);
  return canonical;
}

function isAllowedValue<T extends string>(value: unknown, allowed: readonly T[]): value is T {
  for (let index = 0; index < allowed.length; index += 1) {
    if (value === allowed[index]) return true;
  }
  return false;
}

const REVIEW_DECISIONS = ['advance', 'hold', 'rollback'] as const;
const PUSH_LEVELS = ['info', 'warn', 'decision', 'error'] as const;
const PUSH_CHANNELS = ['auto', 'wechat', 'webchat', 'both', 'email'] as const;
const AGENT_ROLES = ['planner', 'coder', 'reviewer'] as const;
const OPERATION_ERROR_CODES = [
  'AUTOLOOP_EMPTY_REPLY',
  'AUTOLOOP_SESSION_NOT_CREATED',
  'AUTOLOOP_ENGINE_FAILURE',
  'AUTOLOOP_REQUIRED_TOOL_DENIED',
  'AUTOLOOP_CONTROL_MALFORMED',
  'AUTOLOOP_CONTROL_APPLICATION_FAILED',
  'AUTOLOOP_CONTROL_NOT_PERSISTED',
  'AUTOLOOP_RESET_POSTCONDITION_FAILED',
  'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
  'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
  'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE',
  'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
] as const satisfies readonly AutoloopOperationErrorCode[];
const MESSAGE_TYPES = [
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
] as const satisfies readonly AutoloopMessageType[];

function canonicalIterArtifactsPayload(payload: unknown): IterArtifactsPayload {
  const fields = canonicalPayloadFields(
    payload,
    'iter_artifacts',
    ['diff', 'eval_output', 'files_changed'],
    ['diff', 'eval_output', 'files_changed'],
  );
  if (typeof fields.diff !== 'string') {
    invalidDeliveryPayload('iter_artifacts', 'diff must be a string');
  }
  const evalOutput = canonicalEvalOutput(fields.eval_output);
  const filesChanged = canonicalPrimitiveArray(
    fields.files_changed,
    'iter_artifacts',
    'files_changed',
    (candidate): candidate is string => typeof candidate === 'string',
    'an array of strings',
  );
  const canonical = Object.create(null) as IterArtifactsPayload;
  Object.defineProperty(canonical, 'diff', { enumerable: true, value: fields.diff });
  Object.defineProperty(canonical, 'eval_output', { enumerable: true, value: evalOutput });
  Object.defineProperty(canonical, 'files_changed', { enumerable: true, value: filesChanged });
  Object.freeze(canonical);
  return canonical;
}

function canonicalReviewVerdictPayload(payload: unknown): ReviewVerdictPayload {
  const fields = canonicalPayloadFields(
    payload,
    'review_verdict',
    // `extractReviewComplete` currently carries its explicitly-undefined or
    // exact-array runtime flags through the dispatcher. They are compatibility
    // input only and never cross the canonical message boundary.
    ['decision', 'metric', 'audit_notes', 'accepted', 'evidence_id', 'flags'],
    ['decision', 'metric', 'audit_notes'],
  );
  if (!isAllowedValue(fields.decision, REVIEW_DECISIONS)) {
    invalidDeliveryPayload('review_verdict', 'decision must be advance, hold, or rollback');
  }
  if (fields.metric !== null && (typeof fields.metric !== 'number' || !Number.isFinite(fields.metric))) {
    invalidDeliveryPayload('review_verdict', 'metric must be null or a finite number');
  }
  if (typeof fields.audit_notes !== 'string') {
    invalidDeliveryPayload('review_verdict', 'audit_notes must be a string');
  }
  const hasAccepted = Object.hasOwn(fields, 'accepted');
  const hasEvidenceId = Object.hasOwn(fields, 'evidence_id');
  const hasFlags = Object.hasOwn(fields, 'flags');
  if (hasAccepted && fields.accepted !== undefined && typeof fields.accepted !== 'boolean') {
    invalidDeliveryPayload('review_verdict', 'accepted must be a boolean when present');
  }
  if (hasEvidenceId && fields.evidence_id !== undefined && typeof fields.evidence_id !== 'string') {
    invalidDeliveryPayload('review_verdict', 'evidence_id must be a string when present');
  }
  if (hasFlags && fields.flags !== undefined) {
    canonicalPrimitiveArray(
      fields.flags,
      'review_verdict',
      'flags',
      (candidate): candidate is string => typeof candidate === 'string',
      'an array of strings',
    );
  }
  const canonical = Object.create(null) as ReviewVerdictPayload;
  Object.defineProperty(canonical, 'decision', { enumerable: true, value: fields.decision });
  Object.defineProperty(canonical, 'metric', { enumerable: true, value: fields.metric });
  Object.defineProperty(canonical, 'audit_notes', { enumerable: true, value: fields.audit_notes });
  if (hasAccepted && fields.accepted !== undefined) {
    Object.defineProperty(canonical, 'accepted', { enumerable: true, value: fields.accepted });
  }
  if (hasEvidenceId && fields.evidence_id !== undefined) {
    Object.defineProperty(canonical, 'evidence_id', { enumerable: true, value: fields.evidence_id });
  }
  Object.freeze(canonical);
  return canonical;
}

function canonicalPushUserPayload(payload: unknown): PushUserPayload {
  const fields = canonicalPayloadFields(
    payload,
    'push_user',
    ['level', 'summary', 'detail', 'channel'],
    ['level', 'summary', 'channel'],
  );
  if (!isAllowedValue(fields.level, PUSH_LEVELS)) {
    invalidDeliveryPayload('push_user', 'level must be info, warn, decision, or error');
  }
  if (typeof fields.summary !== 'string') {
    invalidDeliveryPayload('push_user', 'summary must be a string');
  }
  if (!isAllowedValue(fields.channel, PUSH_CHANNELS)) {
    invalidDeliveryPayload('push_user', 'channel is invalid');
  }
  const hasDetail = Object.hasOwn(fields, 'detail');
  if (hasDetail && fields.detail !== undefined && typeof fields.detail !== 'string') {
    invalidDeliveryPayload('push_user', 'detail must be a string when present');
  }
  const canonical = Object.create(null) as PushUserPayload;
  Object.defineProperty(canonical, 'level', { enumerable: true, value: fields.level });
  Object.defineProperty(canonical, 'summary', { enumerable: true, value: fields.summary });
  if (hasDetail && fields.detail !== undefined) {
    Object.defineProperty(canonical, 'detail', { enumerable: true, value: fields.detail });
  }
  Object.defineProperty(canonical, 'channel', { enumerable: true, value: fields.channel });
  Object.freeze(canonical);
  return canonical;
}

function canonicalPausePayload(payload: unknown): PausePayload {
  const fields = canonicalPayloadFields(payload, 'pause', ['reason'], ['reason']);
  if (typeof fields.reason !== 'string') invalidDeliveryPayload('pause', 'reason must be a string');
  const canonical = Object.create(null) as PausePayload;
  Object.defineProperty(canonical, 'reason', { enumerable: true, value: fields.reason });
  Object.freeze(canonical);
  return canonical;
}

function canonicalResumePayload(payload: unknown): ResumePayload {
  canonicalPayloadFields(payload, 'resume', [], []);
  const canonical = Object.create(null) as ResumePayload;
  Object.freeze(canonical);
  return canonical;
}

function canonicalTerminatePayload(payload: unknown): TerminatePayload {
  const fields = canonicalPayloadFields(payload, 'terminate', ['reason'], ['reason']);
  if (typeof fields.reason !== 'string') invalidDeliveryPayload('terminate', 'reason must be a string');
  const canonical = Object.create(null) as TerminatePayload;
  Object.defineProperty(canonical, 'reason', { enumerable: true, value: fields.reason });
  Object.freeze(canonical);
  return canonical;
}

function canonicalPhaseErrorPayload(payload: unknown, envelopeFrom: string): PhaseErrorPayload {
  const fields = canonicalPayloadFields(
    payload,
    'phase_error',
    ['agent', 'phase', 'code', 'committed', 'retryable', 'error'],
    ['agent', 'phase', 'error'],
  );
  if (!isAllowedValue(fields.agent, AGENT_ROLES)) {
    invalidDeliveryPayload('phase_error', 'agent must be planner, coder, or reviewer');
  }
  if (fields.agent !== envelopeFrom) {
    invalidDeliveryPayload('phase_error', 'agent must match the envelope sender');
  }
  if (typeof fields.phase !== 'string') invalidDeliveryPayload('phase_error', 'phase must be a string');
  if (typeof fields.error !== 'string') invalidDeliveryPayload('phase_error', 'error must be a string');
  const hasCode = Object.hasOwn(fields, 'code');
  const hasCommitted = Object.hasOwn(fields, 'committed');
  const hasRetryable = Object.hasOwn(fields, 'retryable');
  if (hasCode && fields.code !== undefined && !isAllowedValue(fields.code, OPERATION_ERROR_CODES)) {
    invalidDeliveryPayload('phase_error', 'code is invalid');
  }
  if (hasCommitted && fields.committed !== undefined && fields.committed !== true) {
    invalidDeliveryPayload('phase_error', 'committed must be true when present');
  }
  if (hasRetryable && fields.retryable !== undefined && fields.retryable !== false) {
    invalidDeliveryPayload('phase_error', 'retryable must be false when present');
  }
  const canonical = Object.create(null) as PhaseErrorPayload;
  Object.defineProperty(canonical, 'agent', { enumerable: true, value: fields.agent });
  Object.defineProperty(canonical, 'phase', { enumerable: true, value: fields.phase });
  if (hasCode && fields.code !== undefined) {
    Object.defineProperty(canonical, 'code', { enumerable: true, value: fields.code });
  }
  if (hasCommitted && fields.committed !== undefined) {
    Object.defineProperty(canonical, 'committed', { enumerable: true, value: fields.committed });
  }
  if (hasRetryable && fields.retryable !== undefined) {
    Object.defineProperty(canonical, 'retryable', { enumerable: true, value: fields.retryable });
  }
  Object.defineProperty(canonical, 'error', { enumerable: true, value: fields.error });
  Object.freeze(canonical);
  return canonical;
}

function canonicalSendTimeoutPayload(payload: unknown, envelopeIter: number, envelopeFrom: string): SendTimeoutPayload {
  const fields = canonicalPayloadFields(
    payload,
    'send_timeout',
    ['status', 'dispatch_id', 'agent', 'message_id', 'message_type', 'iter', 'timeout_ms', 'error'],
    ['status', 'dispatch_id', 'agent', 'message_id', 'message_type', 'iter', 'timeout_ms', 'error'],
  );
  if (fields.status !== 'awaiting_resume') {
    invalidDeliveryPayload('send_timeout', 'status must be awaiting_resume');
  }
  if (typeof fields.dispatch_id !== 'string') {
    invalidDeliveryPayload('send_timeout', 'dispatch_id must be a string');
  }
  if (!isAllowedValue(fields.agent, AGENT_ROLES)) {
    invalidDeliveryPayload('send_timeout', 'agent must be planner, coder, or reviewer');
  }
  if (fields.agent !== envelopeFrom) {
    invalidDeliveryPayload('send_timeout', 'agent must match the envelope sender');
  }
  if (typeof fields.message_id !== 'string') {
    invalidDeliveryPayload('send_timeout', 'message_id must be a string');
  }
  if (!isAllowedValue(fields.message_type, MESSAGE_TYPES)) {
    invalidDeliveryPayload('send_timeout', 'message_type is invalid');
  }
  if (!Number.isSafeInteger(fields.iter) || (fields.iter as number) < 0) {
    invalidDeliveryPayload('send_timeout', 'iter must be a nonnegative safe integer');
  }
  if (fields.iter !== envelopeIter) {
    invalidDeliveryPayload('send_timeout', `iter ${String(fields.iter)} does not match envelope iter ${envelopeIter}`);
  }
  if (
    typeof fields.timeout_ms !== 'number' ||
    !Number.isFinite(fields.timeout_ms) ||
    fields.timeout_ms <= 0 ||
    Math.abs(fields.timeout_ms) > Number.MAX_SAFE_INTEGER
  ) {
    invalidDeliveryPayload('send_timeout', 'timeout_ms must be a positive finite safe number');
  }
  if (typeof fields.error !== 'string') invalidDeliveryPayload('send_timeout', 'error must be a string');

  const canonical = Object.create(null) as SendTimeoutPayload;
  for (const key of [
    'status',
    'dispatch_id',
    'agent',
    'message_id',
    'message_type',
    'iter',
    'timeout_ms',
    'error',
  ] as const) {
    Object.defineProperty(canonical, key, { enumerable: true, value: fields[key] });
  }
  Object.freeze(canonical);
  return canonical;
}

function canonicalIterDonePayload(payload: unknown, envelopeIter: number): IterDonePayload {
  const fields = canonicalPayloadFields(
    payload,
    'iter_done',
    ['iter', 'verdict', 'metric', 'regression'],
    ['iter', 'verdict', 'metric'],
  );
  if (!Number.isSafeInteger(fields.iter) || (fields.iter as number) < 0) {
    invalidDeliveryPayload('iter_done', 'iter must be a nonnegative safe integer');
  }
  if (fields.iter !== envelopeIter) {
    invalidDeliveryPayload('iter_done', `iter ${String(fields.iter)} does not match envelope iter ${envelopeIter}`);
  }
  if (!isAllowedValue(fields.verdict, REVIEW_DECISIONS)) {
    invalidDeliveryPayload('iter_done', 'verdict must be advance, hold, or rollback');
  }
  if (fields.metric !== null && (typeof fields.metric !== 'number' || !Number.isFinite(fields.metric))) {
    invalidDeliveryPayload('iter_done', 'metric must be null or a finite number');
  }
  const hasRegression = Object.hasOwn(fields, 'regression');
  if (hasRegression && typeof fields.regression !== 'boolean') {
    invalidDeliveryPayload('iter_done', 'regression must be a boolean when present');
  }
  const canonical = Object.create(null) as IterDonePayload;
  Object.defineProperty(canonical, 'iter', { enumerable: true, value: fields.iter });
  Object.defineProperty(canonical, 'verdict', { enumerable: true, value: fields.verdict });
  Object.defineProperty(canonical, 'metric', { enumerable: true, value: fields.metric });
  if (hasRegression) Object.defineProperty(canonical, 'regression', { enumerable: true, value: fields.regression });
  Object.freeze(canonical);
  return canonical;
}

function canonicalReviewRequestPayload(payload: unknown, envelopeIter: number): ReviewRequestPayload {
  const fields = canonicalPayloadFields(
    payload,
    'review_request',
    ['iter', 'ledger_path', 'prior_metrics'],
    ['iter', 'ledger_path', 'prior_metrics'],
  );
  if (!Number.isSafeInteger(fields.iter) || (fields.iter as number) < 0) {
    invalidDeliveryPayload('review_request', 'iter must be a nonnegative safe integer');
  }
  if (fields.iter !== envelopeIter) {
    throw new AutoloopRoutingError(
      `Invalid review_request iteration: envelope iter=${envelopeIter} does not match payload iter=${String(fields.iter)}`,
    );
  }
  if (typeof fields.ledger_path !== 'string') {
    invalidDeliveryPayload('review_request', 'ledger_path must be a string');
  }
  const priorMetrics = canonicalPrimitiveArray(
    fields.prior_metrics,
    'review_request',
    'prior_metrics',
    (candidate): candidate is number => typeof candidate === 'number' && Number.isFinite(candidate),
    'an array of finite numbers',
  );
  const canonical = Object.create(null) as ReviewRequestPayload;
  Object.defineProperty(canonical, 'iter', { enumerable: true, value: fields.iter });
  Object.defineProperty(canonical, 'ledger_path', { enumerable: true, value: fields.ledger_path });
  Object.defineProperty(canonical, 'prior_metrics', { enumerable: true, value: priorMetrics });
  Object.freeze(canonical);
  return canonical;
}

const MESSAGE_IDENTITY_FIELDS = ['msg_id', 'iter', 'from', 'to', 'type', 'ts', 'payload'] as const;

/**
 * Validate and snapshot one public message before any routing side effect.
 * Only own data properties cross this boundary; the returned envelope and all
 * payloads whose delivery contract is schema-aware are immutable null-prototype
 * values.
 */
export function canonicalizeMessage(env: AnyAutoloopMessage): AnyAutoloopMessage {
  if (typeof env !== 'object' || env === null || Array.isArray(env)) {
    throw new AutoloopRoutingError('Invalid v2 envelope identity: expected an object');
  }
  const captured = Object.create(null) as Record<string, unknown>;
  for (let index = 0; index < MESSAGE_IDENTITY_FIELDS.length; index += 1) {
    const key = MESSAGE_IDENTITY_FIELDS[index];
    const descriptor = Object.getOwnPropertyDescriptor(env, key);
    if (!descriptor || !Object.hasOwn(descriptor, 'value')) {
      throw new AutoloopRoutingError(`Invalid v2 envelope identity: ${key} must be an own data property`);
    }
    captured[key] = descriptor.value;
  }

  const msgId = captured.msg_id;
  const iter = captured.iter;
  const from = captured.from;
  const to = captured.to;
  const type = captured.type;
  const ts = captured.ts;
  if (
    typeof msgId !== 'string' ||
    !Number.isSafeInteger(iter) ||
    (iter as number) < 0 ||
    typeof from !== 'string' ||
    typeof to !== 'string' ||
    typeof type !== 'string' ||
    typeof ts !== 'string'
  ) {
    throw new AutoloopRoutingError('Invalid v2 envelope identity types or iteration');
  }

  let validRoute = false;
  for (let index = 0; index < ALLOWED_ROUTES.length; index += 1) {
    const route = ALLOWED_ROUTES[index];
    if (route[0] === from && route[1] === to && route[2] === type) {
      validRoute = true;
      break;
    }
  }
  if (!validRoute) {
    throw new AutoloopRoutingError(`Invalid v2 routing: ${from} → ${to} (type=${type})`, captured);
  }

  switch (type as AutoloopMessageType) {
    case 'chat':
      captured.payload = canonicalChatPayload(captured.payload);
      break;
    case 'directive':
      captured.payload = canonicalDirectivePayload(captured.payload);
      break;
    case 'directive_ack':
      captured.payload = canonicalDirectiveAckPayload(captured.payload);
      break;
    case 'iter_artifacts':
      captured.payload = canonicalIterArtifactsPayload(captured.payload);
      break;
    case 'review_request':
      captured.payload = canonicalReviewRequestPayload(captured.payload, iter as number);
      break;
    case 'review_verdict':
      captured.payload = canonicalReviewVerdictPayload(captured.payload);
      break;
    case 'iter_done':
      captured.payload = canonicalIterDonePayload(captured.payload, iter as number);
      break;
    case 'push_user':
      captured.payload = canonicalPushUserPayload(captured.payload);
      break;
    case 'pause':
      captured.payload = canonicalPausePayload(captured.payload);
      break;
    case 'resume':
      captured.payload = canonicalResumePayload(captured.payload);
      break;
    case 'terminate':
      captured.payload = canonicalTerminatePayload(captured.payload);
      break;
    case 'phase_error':
      captured.payload = canonicalPhaseErrorPayload(captured.payload, from as string);
      break;
    case 'send_timeout':
      captured.payload = canonicalSendTimeoutPayload(captured.payload, iter as number, from as string);
      break;
  }

  const identity = Object.create(null) as Record<string, unknown>;
  for (let index = 0; index < MESSAGE_IDENTITY_FIELDS.length; index += 1) {
    const key = MESSAGE_IDENTITY_FIELDS[index];
    Object.defineProperty(identity, key, { enumerable: true, value: captured[key] });
  }
  Object.freeze(identity);
  return identity as unknown as AnyAutoloopMessage;
}

export function validateMessage(env: AnyAutoloopMessage): AnyAutoloopMessage {
  return canonicalizeMessage(env);
}

// ─── Constructors ────────────────────────────────────────────────────────────

let __counter = 0;
function nextMsgId(): string {
  // Cheap monotonic IDs; collisions across runs are not load-bearing.
  __counter = (__counter + 1) | 0;
  return `m_${Date.now().toString(36)}_${__counter.toString(36)}`;
}

function envelope<T extends AutoloopMessageType>(
  iter: number,
  from: AutoloopRole,
  to: AutoloopRole,
  type: T,
  payload: PayloadFor<T>,
): AutoloopEnvelope<T> {
  return {
    msg_id: nextMsgId(),
    iter,
    from,
    to,
    type,
    ts: new Date().toISOString(),
    payload,
  };
}

export const Msg = {
  chat: (iter: number, payload: UserChatPayload) => envelope(iter, 'user', 'planner', 'chat', payload),
  directive: (iter: number, payload: DirectivePayload) => envelope(iter, 'planner', 'coder', 'directive', payload),
  directiveAck: (iter: number, payload: DirectiveAckPayload) =>
    envelope(iter, 'coder', 'planner', 'directive_ack', payload),
  iterArtifacts: (iter: number, payload: IterArtifactsPayload) =>
    envelope(iter, 'coder', 'runner', 'iter_artifacts', payload),
  reviewRequest: (iter: number, payload: ReviewRequestPayload) =>
    envelope(iter, 'runner', 'reviewer', 'review_request', payload),
  reviewVerdict: (iter: number, payload: ReviewVerdictPayload) =>
    envelope(iter, 'reviewer', 'runner', 'review_verdict', payload),
  iterDone: (iter: number, payload: IterDonePayload) => envelope(iter, 'runner', 'planner', 'iter_done', payload),
  pushUser: (iter: number, payload: PushUserPayload) => envelope(iter, 'planner', 'user', 'push_user', payload),
  pause: (iter: number, payload: PausePayload) => envelope(iter, 'planner', 'runner', 'pause', payload),
  resume: (iter: number) => envelope(iter, 'planner', 'runner', 'resume', {}),
  terminate: (iter: number, payload: TerminatePayload) => envelope(iter, 'planner', 'runner', 'terminate', payload),
  phaseError: (iter: number, payload: PhaseErrorPayload) =>
    envelope(iter, payload.agent, 'runner', 'phase_error', payload),
  sendTimeout: (iter: number, payload: SendTimeoutPayload) =>
    envelope(iter, payload.agent, 'runner', 'send_timeout', payload),
};

// ─── Wire serialisation (for InboxManager transport) ─────────────────────────

export function serialise(env: AnyAutoloopMessage): { text: string; summary: string } {
  return {
    text: JSON.stringify(env),
    summary: env.type,
  };
}

export function deserialise(text: string): AnyAutoloopMessage {
  let parsed: AnyAutoloopMessage;
  try {
    parsed = JSON.parse(text) as AnyAutoloopMessage;
  } catch (err) {
    throw new AutoloopRoutingError(`Malformed v2 envelope (invalid JSON: ${(err as Error).message})`);
  }
  if (
    typeof parsed !== 'object' ||
    parsed === null ||
    typeof parsed.type !== 'string' ||
    typeof parsed.from !== 'string' ||
    typeof parsed.to !== 'string'
  ) {
    throw new AutoloopRoutingError('Malformed v2 envelope (not an object or missing type/from/to)');
  }
  // Reject envelopes whose from→to→type isn't an allowed route, so a corrupt or
  // forged message never reaches the dispatcher.
  return validateMessage(parsed);
}
