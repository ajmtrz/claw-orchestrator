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

export class AutoloopRoutingError extends Error {
  constructor(
    msg: string,
    public envelope?: AnyAutoloopMessage,
  ) {
    super(msg);
    this.name = 'AutoloopRoutingError';
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
  if (fields.verdict !== 'advance' && fields.verdict !== 'hold' && fields.verdict !== 'rollback') {
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
    throw new AutoloopRoutingError(`Invalid v2 routing: ${from} → ${to} (type=${type})`, env);
  }

  if (type === 'chat') {
    captured.payload = canonicalChatPayload(captured.payload);
  } else if (type === 'directive') {
    captured.payload = canonicalDirectivePayload(captured.payload);
  } else if (type === 'directive_ack') {
    captured.payload = canonicalDirectiveAckPayload(captured.payload);
  } else if (type === 'iter_done') {
    captured.payload = canonicalIterDonePayload(captured.payload, iter as number);
  } else if (type === 'review_request') {
    captured.payload = canonicalReviewRequestPayload(captured.payload, iter as number);
  }

  const identity = Object.create(null) as Record<string, unknown>;
  for (let index = 0; index < MESSAGE_IDENTITY_FIELDS.length; index += 1) {
    const key = MESSAGE_IDENTITY_FIELDS[index];
    Object.defineProperty(identity, key, { enumerable: true, value: captured[key] });
  }
  Object.freeze(identity);
  return identity as unknown as AnyAutoloopMessage;
}

export function validateMessage(env: AnyAutoloopMessage): void {
  canonicalizeMessage(env);
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
  validateMessage(parsed);
  return parsed;
}
