/**
 * Planner-emitted "tool calls" — parsing + handler dispatch.
 *
 * The Planner is a Claude Code subprocess; we cannot register first-class
 * MCP tools without standing up an MCP server. Instead, the Planner emits
 * structured intent as **fenced code blocks tagged `autoloop`**:
 *
 *   ```autoloop
 *   {"tool": "notify_user", "args": {"level": "info", "summary": "plan ready"}}
 *   ```
 *
 * After each Planner turn, the dispatcher scans the reply for these blocks,
 * validates them, and translates them into runner-queue messages or direct
 * runner-state mutations. Multiple blocks per turn are allowed and processed
 * in order.
 *
 * The naming is stable so a future MCP-based implementation can swap the
 * parser for real tool dispatch without changing any Planner-facing
 * semantics.
 */

import { ENGINE_TYPES, type EngineType } from '../types.js';
import {
  type AnyAutoloopMessage,
  canonicalizeRequestReviewArgs,
  type CheckpointReviewRequestPayload,
  MAX_REQUEST_REVIEW_METADATA_BYTES,
  MAX_REQUEST_REVIEW_SCOPE_ITEMS,
  Msg,
  type PushChannel,
  type PushLevel,
  type RequestReviewArgs,
} from './messages.js';

export type PlannerToolName =
  | 'notify_user'
  | 'spawn_coder'
  | 'spawn_reviewer'
  | 'spawn_subagents'
  | 'request_review'
  | 'send_directive'
  | 'pause_loop'
  | 'resume_loop'
  | 'terminate'
  | 'update_push_policy'
  | 'write_plan'
  | 'write_goal';

export interface PlannerToolCall {
  tool: PlannerToolName;
  args: Record<string, unknown>;
}

export interface PlannerToolParseResult {
  calls: PlannerToolCall[];
  /** Reply text with autoloop blocks stripped — what we actually show to user. */
  cleaned_reply: string;
  /** Per-block parse errors (block kept in cleaned reply for forensics). */
  parse_errors: Array<{ block_index: number; error: string }>;
}

const FENCE_RE = /```autoloop\s*\n([\s\S]*?)\n```/g;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

/**
 * Scan reply text for `autoloop` fenced JSON blocks. Returns parsed tool calls
 * plus a cleaned reply with the blocks removed (so we don't show raw JSON to
 * the user).
 */
export function parsePlannerReply(reply: string): PlannerToolParseResult {
  const calls: PlannerToolCall[] = [];
  const parse_errors: Array<{ block_index: number; error: string }> = [];
  let blockIndex = 0;
  const cleaned = reply.replace(FENCE_RE, (_match, body: string) => {
    const idx = blockIndex++;
    try {
      const parsed = JSON.parse(body.trim()) as PlannerToolCall;
      if (!isPlainObject(parsed) || typeof parsed.tool !== 'string' || !isPlainObject(parsed.args)) {
        parse_errors.push({ block_index: idx, error: 'block missing tool/args fields' });
        return ''; // strip even malformed blocks so user doesn't see raw JSON
      }
      calls.push(parsed);
    } catch (err) {
      parse_errors.push({ block_index: idx, error: (err as Error).message });
    }
    return '';
  });
  return { calls, cleaned_reply: cleaned.trim(), parse_errors };
}

// ─── Side-effect interface ───────────────────────────────────────────────────
//
// Most tool calls translate directly to v2 messages and go back to the runner
// via the dispatcher's return value. Only these three need real side effects
// outside the message bus.

export interface SpawnSubagentsArgs {
  coder_model?: string;
  coder_engine?: EngineType;
  reviewer_model?: string;
  reviewer_engine?: EngineType;
  initial_directive?: {
    goal: string;
    constraints?: string[];
    success_criteria?: string[];
    max_attempts?: number;
  };
}

export interface SpawnCoderArgs {
  coder_model?: string;
  coder_engine?: EngineType;
}

export interface SpawnReviewerArgs {
  reviewer_model?: string;
  reviewer_engine?: EngineType;
}

export interface PreparedReviewRequest {
  status: 'prepared';
  target: 'reviewer';
  idempotency_key: string;
  payload: CheckpointReviewRequestPayload;
}

export interface DuplicateReviewRequest {
  status: 'duplicate';
  target: 'reviewer';
  idempotency_key: string;
}

/** Task 4 preparation result; Reviewer delivery remains owned by the Runner queue. */
export type ReviewRequestPreparationResult = PreparedReviewRequest | DuplicateReviewRequest;

export interface PlannerToolEffects {
  /** Abort the batch when its owning run can no longer accept effects. */
  assertActive?: () => void;
  /** Start Coder + Reviewer persistent sessions. */
  spawnSubagents: (args: SpawnSubagentsArgs) => Promise<void>;
  /** Start only the Coder persistent session. */
  spawnCoder?: (args: SpawnCoderArgs) => Promise<unknown>;
  /** Start only the Reviewer persistent session. */
  spawnReviewer?: (args: SpawnReviewerArgs) => Promise<unknown>;
  /** Prepare an existing checkpoint for a Runner-routed Reviewer request. */
  requestReview?: (args: RequestReviewArgs, targetIter: number) => Promise<ReviewRequestPreparationResult>;
  /** Release a prepared request when the current handoff aborts before queue acceptance. */
  releaseReviewRequest?: (idempotencyKey: string, payload: CheckpointReviewRequestPayload) => void;
  /** Apply an already validated/canonical in-memory push-policy delta. */
  updatePushPolicy: (delta: Record<string, unknown>) => void;
  /** Atomically materialize the complete plan/goal write set before later effects. */
  writePlanFiles: (writes: readonly PlannerArtifactWrite[]) => Promise<void>;
}

export interface PlannerArtifactWrite {
  file: 'plan.md' | 'goal.json';
  content: string;
  commitMessage?: string;
}

// ─── Tool execution ──────────────────────────────────────────────────────────

export interface PlannerToolHandlerResult {
  /** Messages the runner should push into its own queue. */
  emitted_messages: AnyAutoloopMessage[];
  /** Errors encountered while handling this batch (does not throw). */
  errors: Array<{ tool: string; error: string }>;
}

interface PreparedPlannerToolCall {
  tool: string;
  artifact?: PlannerArtifactWrite;
  apply: () => Promise<AnyAutoloopMessage[]> | AnyAutoloopMessage[];
}

const VALID_PUSH_LEVELS = new Set<PushLevel>(['info', 'warn', 'decision', 'error']);
const VALID_PUSH_CHANNELS = new Set<PushChannel>(['auto', 'wechat', 'webchat', 'both', 'email']);
const FALLBACK_CAPABLE_PUSH_CHANNELS = new Set<PushChannel>(['auto', 'both']);
const PUSH_POLICY_KEYS = new Set([
  'on_start',
  'on_iter_done_ok',
  'on_target_hit',
  'on_metric_regression_2',
  'on_reviewer_reject_2',
  'on_phase_error',
  'on_stall_30min',
  'on_decision_needed',
]);
const UNSILENCEABLE_PUSH_POLICY_KEYS = new Set(['on_phase_error', 'on_decision_needed']);
const PUSH_POLICY_RULE_FIELDS = new Set(['channel', 'level', 'silent']);
const PUSH_LEVEL_STRENGTH: Record<PushLevel, number> = { info: 0, warn: 1, decision: 2, error: 3 };
const MINIMUM_CRITICAL_PUSH_LEVEL: Readonly<Record<string, PushLevel>> = {
  on_phase_error: 'error',
  on_decision_needed: 'decision',
};

/** Maximum UTF-8 bytes accepted for one durable metadata string. */
export const MAX_PLANNER_CONTROL_METADATA_BYTES = 8_192;
/** Maximum entries accepted in one directive metadata array. */
export const MAX_PLANNER_CONTROL_ARRAY_ITEMS = 128;
/** Maximum controls accepted from one Planner turn. */
export const MAX_PLANNER_CONTROL_CALLS = 64;
/**
 * Maximum UTF-8 bytes accepted for one durable write_plan/write_goal payload.
 * The complete normalized batch has a separate ceiling with 64 KiB reserved
 * for its JSON structure and bounded metadata.
 */
export const MAX_PLANNER_CONTROL_CONTENT_BYTES = 1_048_576;
/**
 * Maximum UTF-8 bytes in the final normalized JSON control batch. Planner
 * controls are synchronously appended, fsynced, and tail-verified; bounding
 * the serialized representation (not only artifact content) bounds that work.
 */
export const MAX_PLANNER_CONTROL_BATCH_BYTES = MAX_PLANNER_CONTROL_CONTENT_BYTES + 65_536;

function boundedString(value: string, label: string, maxBytes = MAX_PLANNER_CONTROL_METADATA_BYTES): string {
  if (Buffer.byteLength(value, 'utf8') > maxBytes) {
    throw new Error(`${label} exceeds the ${maxBytes}-byte UTF-8 limit`);
  }
  return value;
}

function nonEmptyString(value: unknown, label: string, maxBytes = MAX_PLANNER_CONTROL_METADATA_BYTES): string {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${label} must be a non-empty string`);
  return boundedString(value, label, maxBytes);
}

function boundedPlannerContent(value: unknown, label: 'write_plan content' | 'write_goal content'): string {
  return nonEmptyString(value, label, MAX_PLANNER_CONTROL_CONTENT_BYTES);
}

function defaultArtifactCommitMessage(file: 'plan.md' | 'goal.json'): string {
  return `autoloop: planner writes ${file}`;
}

function optionalString(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string') throw new Error(`${label} must be a string`);
  return boundedString(value, label);
}

function optionalStringArray(value: unknown, label: string): string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) throw new Error(`${label} must be an array of strings`);
  if (value.length > MAX_PLANNER_CONTROL_ARRAY_ITEMS) {
    throw new Error(`${label} exceeds the ${MAX_PLANNER_CONTROL_ARRAY_ITEMS}-item limit`);
  }
  const result = new Array<string>(value.length);
  for (let index = 0; index < value.length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (!descriptor || !Object.hasOwn(descriptor, 'value')) {
      throw new Error(`${label}[${index}] must be an own data property`);
    }
    if (typeof descriptor.value !== 'string') throw new Error(`${label} must be an array of strings`);
    Object.defineProperty(result, String(index), {
      configurable: true,
      enumerable: true,
      value: boundedString(descriptor.value, `${label}[${index}]`),
      writable: true,
    });
  }
  return result;
}

function optionalPositiveInteger(value: unknown, label: string): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value) || (value as number) <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }
  return value as number;
}

function sanitizeDirectiveArgs(
  raw: Record<string, unknown>,
  label: 'spawn_subagents initial_directive' | 'send_directive',
): NonNullable<SpawnSubagentsArgs['initial_directive']> {
  const valueFor = (field: 'goal' | 'constraints' | 'success_criteria' | 'max_attempts'): unknown => {
    const descriptor = Object.getOwnPropertyDescriptor(raw, field);
    if (descriptor && !Object.hasOwn(descriptor, 'value')) {
      throw new Error(`${label} ${field} must be an own data property`);
    }
    return descriptor?.value;
  };
  const directive: NonNullable<SpawnSubagentsArgs['initial_directive']> = {
    goal: nonEmptyString(valueFor('goal'), `${label} goal`),
    constraints: [],
    success_criteria: [],
    max_attempts: 1,
  };
  const constraints = optionalStringArray(valueFor('constraints'), `${label} constraints`);
  const successCriteria = optionalStringArray(valueFor('success_criteria'), `${label} success_criteria`);
  const maxAttempts = optionalPositiveInteger(valueFor('max_attempts'), `${label} max_attempts`);
  if (constraints !== undefined) directive.constraints = constraints;
  if (successCriteria !== undefined) directive.success_criteria = successCriteria;
  if (maxAttempts !== undefined) directive.max_attempts = maxAttempts;
  return directive;
}

function sanitizePushPolicyDelta(raw: Record<string, unknown>, blockedSilence: string[]): Record<string, unknown> {
  if (Object.keys(raw).length === 0) {
    throw new Error('update_push_policy must include at least one policy key');
  }
  const delta: Record<string, unknown> = {};
  const unsafeFallbackChannels: Array<{ key: string; channel: PushChannel }> = [];
  for (const [key, value] of Object.entries(raw)) {
    if (!PUSH_POLICY_KEYS.has(key)) throw new Error(`update_push_policy key '${key}' is not supported`);
    if (!isPlainObject(value)) throw new Error(`update_push_policy ${key} must be a plain object`);
    const input = value;
    const unknownFields = Object.keys(input).filter((field) => !PUSH_POLICY_RULE_FIELDS.has(field));
    if (unknownFields.length > 0) {
      throw new Error(`update_push_policy ${key} has unknown field '${unknownFields[0]}'`);
    }
    const rule: Record<string, unknown> = {};
    let prohibitedSilenceOnly = false;
    if ('channel' in input) {
      if (typeof input.channel !== 'string' || !VALID_PUSH_CHANNELS.has(input.channel as PushChannel)) {
        throw new Error(`update_push_policy ${key} channel '${String(input.channel)}' is not supported`);
      }
      if (
        UNSILENCEABLE_PUSH_POLICY_KEYS.has(key) &&
        !FALLBACK_CAPABLE_PUSH_CHANNELS.has(input.channel as PushChannel)
      ) {
        unsafeFallbackChannels.push({ key, channel: input.channel as PushChannel });
      }
      rule.channel = input.channel;
    }
    if ('level' in input) {
      if (typeof input.level !== 'string' || !VALID_PUSH_LEVELS.has(input.level as PushLevel)) {
        throw new Error(`update_push_policy ${key} level '${String(input.level)}' is not supported`);
      }
      const minimum = MINIMUM_CRITICAL_PUSH_LEVEL[key];
      if (minimum && PUSH_LEVEL_STRENGTH[input.level as PushLevel] < PUSH_LEVEL_STRENGTH[minimum]) {
        throw new Error(`update_push_policy ${key} level '${input.level}' weakens required level '${minimum}'`);
      }
      rule.level = input.level;
    }
    if ('silent' in input) {
      if (typeof input.silent !== 'boolean') {
        throw new Error(`update_push_policy ${key} silent must be a boolean`);
      }
      if (input.silent && UNSILENCEABLE_PUSH_POLICY_KEYS.has(key)) {
        blockedSilence.push(key);
        prohibitedSilenceOnly = Object.keys(input).length === 1;
      } else rule.silent = input.silent;
    }
    if (!prohibitedSilenceOnly) delta[key] = rule;
  }
  const unsafeFallbackChannel = unsafeFallbackChannels[0];
  if (unsafeFallbackChannel) {
    throw new Error(
      `update_push_policy ${unsafeFallbackChannel.key} channel '${unsafeFallbackChannel.channel}' bypasses the required fallback chain`,
    );
  }
  return delta;
}

/**
 * Validate one Planner control and return the exact allowlisted representation
 * that may be persisted and applied. Unknown fields are deliberately dropped;
 * fields whose presence changes safety or semantics are rejected when invalid.
 */
function sanitizePlannerToolCall(call: PlannerToolCall, blockedPolicySilence: string[] = []): PlannerToolCall {
  if (!isPlainObject(call.args)) throw new Error(`${call.tool} args must be a plain object`);
  const raw = call.args;
  switch (call.tool) {
    case 'notify_user': {
      const summary = nonEmptyString(raw.summary, 'notify_user summary');
      const args: Record<string, unknown> = { summary, level: 'info', channel: 'auto' };
      if (raw.level !== undefined) {
        if (typeof raw.level !== 'string' || !VALID_PUSH_LEVELS.has(raw.level as PushLevel)) {
          throw new Error(`notify_user level '${String(raw.level)}' is not supported`);
        }
        args.level = raw.level;
      }
      const detail = optionalString(raw.detail, 'notify_user detail');
      if (detail !== undefined) args.detail = detail;
      if (raw.channel !== undefined) {
        if (typeof raw.channel !== 'string' || !VALID_PUSH_CHANNELS.has(raw.channel as PushChannel)) {
          throw new Error(`notify_user channel '${String(raw.channel)}' is not supported`);
        }
        args.channel = raw.channel;
      }
      return { tool: call.tool, args };
    }
    case 'spawn_subagents': {
      for (const field of [
        'coder_custom_engine',
        'reviewer_custom_engine',
        'coderCustomEngine',
        'reviewerCustomEngine',
        'customEngine',
      ] as const) {
        if (Object.hasOwn(raw, field)) {
          throw new Error('spawn_subagents cannot include custom engine config; configure it at autoloop_start');
        }
      }
      const ownDataValue = (field: keyof SpawnSubagentsArgs): unknown => {
        const descriptor = Object.getOwnPropertyDescriptor(raw, field);
        if (descriptor && !Object.hasOwn(descriptor, 'value')) {
          throw new Error(`spawn_subagents ${field} must be an own data property`);
        }
        return descriptor?.value;
      };
      const args = Object.create(null) as Record<string, unknown>;
      for (const field of ['coder_engine', 'reviewer_engine'] as const) {
        const value = ownDataValue(field);
        if (value !== undefined && (typeof value !== 'string' || !ENGINE_TYPES.includes(value as EngineType))) {
          throw new Error(`spawn_subagents ${field} has unknown engine '${String(value)}'`);
        }
        if (value !== undefined) args[field] = value;
      }
      for (const field of ['coder_model', 'reviewer_model'] as const) {
        const value = optionalString(ownDataValue(field), `spawn_subagents ${field}`);
        if (value !== undefined) args[field] = value;
      }
      const initialDirective = ownDataValue('initial_directive');
      if (initialDirective !== undefined) {
        if (!isPlainObject(initialDirective)) {
          throw new Error('spawn_subagents initial_directive must be an object');
        }
        args.initial_directive = sanitizeDirectiveArgs(initialDirective, 'spawn_subagents initial_directive');
      }
      return { tool: call.tool, args };
    }
    case 'spawn_coder': {
      if (
        Object.hasOwn(raw, 'coder_custom_engine') ||
        Object.hasOwn(raw, 'coderCustomEngine') ||
        Object.hasOwn(raw, 'customEngine')
      ) {
        throw new Error('spawn_coder cannot include custom engine config; configure it at autoloop_start');
      }
      const args: Record<string, unknown> = {};
      const engineDescriptor = Object.getOwnPropertyDescriptor(raw, 'coder_engine');
      if (engineDescriptor && !Object.hasOwn(engineDescriptor, 'value')) {
        throw new Error('spawn_coder coder_engine must be an own data property');
      }
      const engine = engineDescriptor?.value;
      if (engine !== undefined && (typeof engine !== 'string' || !ENGINE_TYPES.includes(engine as EngineType))) {
        throw new Error(`spawn_coder coder_engine has unknown engine '${String(engine)}'`);
      }
      if (engine !== undefined) args.coder_engine = engine;
      const modelDescriptor = Object.getOwnPropertyDescriptor(raw, 'coder_model');
      if (modelDescriptor && !Object.hasOwn(modelDescriptor, 'value')) {
        throw new Error('spawn_coder coder_model must be an own data property');
      }
      const model = optionalString(modelDescriptor?.value, 'spawn_coder coder_model');
      if (model !== undefined) args.coder_model = model;
      return { tool: call.tool, args };
    }
    case 'spawn_reviewer': {
      if (
        Object.hasOwn(raw, 'reviewer_custom_engine') ||
        Object.hasOwn(raw, 'reviewerCustomEngine') ||
        Object.hasOwn(raw, 'customEngine')
      ) {
        throw new Error('spawn_reviewer cannot include custom engine config; configure it at autoloop_start');
      }
      const args: Record<string, unknown> = {};
      const engineDescriptor = Object.getOwnPropertyDescriptor(raw, 'reviewer_engine');
      if (engineDescriptor && !Object.hasOwn(engineDescriptor, 'value')) {
        throw new Error('spawn_reviewer reviewer_engine must be an own data property');
      }
      const engine = engineDescriptor?.value;
      if (engine !== undefined && (typeof engine !== 'string' || !ENGINE_TYPES.includes(engine as EngineType))) {
        throw new Error(`spawn_reviewer reviewer_engine has unknown engine '${String(engine)}'`);
      }
      if (engine !== undefined) args.reviewer_engine = engine;
      const modelDescriptor = Object.getOwnPropertyDescriptor(raw, 'reviewer_model');
      if (modelDescriptor && !Object.hasOwn(modelDescriptor, 'value')) {
        throw new Error('spawn_reviewer reviewer_model must be an own data property');
      }
      const model = optionalString(modelDescriptor?.value, 'spawn_reviewer reviewer_model');
      if (model !== undefined) args.reviewer_model = model;
      return { tool: call.tool, args };
    }
    case 'request_review': {
      const canonical = canonicalizeRequestReviewArgs(raw);
      boundedString(canonical.source_run_id, 'request_review source_run_id');
      boundedString(canonical.idempotency_key, 'request_review idempotency_key');
      if (canonical.scope.length > MAX_REQUEST_REVIEW_SCOPE_ITEMS) {
        throw new Error(`request_review scope exceeds the ${MAX_REQUEST_REVIEW_SCOPE_ITEMS}-item limit`);
      }
      for (let index = 0; index < canonical.scope.length; index += 1) {
        boundedString(canonical.scope[index], `request_review scope[${index}]`, MAX_REQUEST_REVIEW_METADATA_BYTES);
      }
      return { tool: call.tool, args: canonical as unknown as Record<string, unknown> };
    }
    case 'send_directive':
      return { tool: call.tool, args: sanitizeDirectiveArgs(raw, 'send_directive') };
    case 'pause_loop': {
      const reason = raw.reason === undefined ? 'planner-pause' : nonEmptyString(raw.reason, 'pause_loop reason');
      return { tool: call.tool, args: { reason } };
    }
    case 'resume_loop':
      return { tool: call.tool, args: {} };
    case 'terminate': {
      const reason = raw.reason === undefined ? 'planner-terminate' : nonEmptyString(raw.reason, 'terminate reason');
      return { tool: call.tool, args: { reason } };
    }
    case 'update_push_policy':
      return { tool: call.tool, args: sanitizePushPolicyDelta(raw, blockedPolicySilence) };
    case 'write_plan': {
      const content = boundedPlannerContent(raw.content, 'write_plan content');
      const commitMessage =
        raw.commit_message === undefined
          ? defaultArtifactCommitMessage('plan.md')
          : nonEmptyString(raw.commit_message, 'write_plan commit_message');
      return {
        tool: call.tool,
        args: { content, commit_message: commitMessage },
      };
    }
    case 'write_goal': {
      const content = boundedPlannerContent(raw.content, 'write_goal content');
      let parsed: unknown;
      try {
        parsed = JSON.parse(content);
      } catch (error) {
        throw new Error(`write_goal content is not valid JSON: ${(error as Error).message}`);
      }
      if (!isPlainObject(parsed)) throw new Error('write_goal content must encode a plain JSON object');
      const commitMessage =
        raw.commit_message === undefined
          ? defaultArtifactCommitMessage('goal.json')
          : nonEmptyString(raw.commit_message, 'write_goal commit_message');
      return {
        tool: call.tool,
        args: { content, commit_message: commitMessage },
      };
    }
    default:
      throw new Error(`unknown planner tool: ${call.tool as string}`);
  }
}

export interface PlannerToolValidationResult {
  calls: PlannerToolCall[];
  errors: Array<{ tool: string; error: string }>;
  blocked_policy_silence: string[];
  /** Canonical normalized bytes used for batch sizing and the durable digest. */
  controls_json?: string;
}

function shadowInheritedArrayToJSON<T>(value: T[]): T[] {
  Object.defineProperty(value, 'toJSON', { value: undefined });
  return value;
}

function normalizePlannerControlValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    const normalized = new Array<unknown>(value.length);
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      Object.defineProperty(normalized, String(index), {
        configurable: true,
        enumerable: true,
        value: normalizePlannerControlValue(descriptor?.value),
        writable: true,
      });
    }
    return shadowInheritedArrayToJSON(normalized);
  }
  if (!value || typeof value !== 'object') return value;
  const normalized = Object.create(null) as Record<string, unknown>;
  for (const [key, entry] of Object.entries(value as Record<string, unknown>).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  )) {
    Object.defineProperty(normalized, key, {
      configurable: true,
      enumerable: true,
      value: normalizePlannerControlValue(entry),
      writable: true,
    });
  }
  return normalized;
}

export function canonicalizePlannerControls(controls: readonly PlannerToolCall[]): PlannerToolCall[] {
  return shadowInheritedArrayToJSON(
    controls.map(({ tool, args }) => ({
      tool,
      args: normalizePlannerControlValue(args) as Record<string, unknown>,
    })),
  );
}

export function canonicalPlannerControlsJson(controls: readonly PlannerToolCall[]): string {
  return JSON.stringify(canonicalizePlannerControls(controls));
}

/** Validate and sanitize the complete batch without performing any effect. */
export function validatePlannerToolCalls(calls: readonly PlannerToolCall[]): PlannerToolValidationResult {
  if (calls.length > MAX_PLANNER_CONTROL_CALLS) {
    return {
      calls: [],
      errors: [
        {
          tool: 'batch',
          error: `Planner control batch exceeds the ${MAX_PLANNER_CONTROL_CALLS}-control limit`,
        },
      ],
      blocked_policy_silence: [],
    };
  }
  const singletonControl = calls.find(
    ({ tool }) => tool === 'request_review' || tool === 'spawn_coder' || tool === 'spawn_reviewer',
  );
  if (calls.length !== 1 && singletonControl) {
    return {
      calls: [],
      errors: [
        {
          tool: singletonControl.tool,
          error: `${singletonControl.tool} must be the only Planner control in its batch`,
        },
      ],
      blocked_policy_silence: [],
    };
  }
  const nonFinalLifecycle = calls.findIndex(
    ({ tool }, index) => (tool === 'pause_loop' || tool === 'terminate') && index !== calls.length - 1,
  );
  if (nonFinalLifecycle >= 0) {
    const tool = calls[nonFinalLifecycle].tool;
    return {
      calls: [],
      errors: [{ tool, error: `${tool} must be the final Planner control in its batch` }],
      blocked_policy_silence: [],
    };
  }
  const validated = shadowInheritedArrayToJSON<PlannerToolCall>([]);
  const errors: Array<{ tool: string; error: string }> = [];
  const blockedPolicySilence: string[] = [];
  // Exact JSON-array accounting lets us reject an oversized batch as soon as
  // the first overflowing row is known. At most the accepted prefix plus the
  // current bounded row is retained; we never accumulate dozens of one-MiB
  // artifact bodies only to discover the total ceiling at the end.
  let normalizedBatchBytes = 2; // opening + closing brackets
  for (const call of calls) {
    try {
      const blockedBefore = blockedPolicySilence.length;
      const sanitized = sanitizePlannerToolCall(call, blockedPolicySilence);
      const isBlockedSilenceOnlyControl =
        sanitized.tool === 'update_push_policy' &&
        blockedPolicySilence.length > blockedBefore &&
        Object.keys(sanitized.args).length === 0;
      if (!isBlockedSilenceOnlyControl) {
        const normalized = canonicalizePlannerControls([sanitized])[0];
        const rowBytes = Buffer.byteLength(JSON.stringify(normalized), 'utf8');
        const nextBytes = normalizedBatchBytes + (validated.length > 0 ? 1 : 0) + rowBytes;
        if (nextBytes > MAX_PLANNER_CONTROL_BATCH_BYTES) {
          return {
            calls: [],
            errors: [
              {
                tool: 'batch',
                error: `Planner control batch exceeds the ${MAX_PLANNER_CONTROL_BATCH_BYTES}-byte UTF-8 limit`,
              },
            ],
            blocked_policy_silence: [],
          };
        }
        normalizedBatchBytes = nextBytes;
        validated.push(normalized);
      }
    } catch (error) {
      errors.push({ tool: call.tool, error: (error as Error).message });
    }
  }
  if (errors.length > 0) return { calls: [], errors, blocked_policy_silence: [] };
  for (const singleton of ['write_plan', 'write_goal', 'spawn_subagents'] as const) {
    if (validated.filter(({ tool }) => tool === singleton).length > 1) {
      return {
        calls: [],
        errors: [{ tool: singleton, error: `duplicate ${singleton} control in one Planner batch` }],
        blocked_policy_silence: [],
      };
    }
  }
  if (validated.length === 0 && blockedPolicySilence.length > 0) {
    return {
      calls: [],
      errors: [
        {
          tool: 'update_push_policy',
          error: 'Planner control batch cannot contain only prohibited critical policy silence',
        },
      ],
      blocked_policy_silence: blockedPolicySilence,
    };
  }

  // Keep an exact final check at the acceptance boundary even though the
  // incremental accounting above is exact. This protects future changes to
  // normalization from accidentally weakening the durable byte limit.
  const controlsJson = JSON.stringify(validated);
  if (Buffer.byteLength(controlsJson, 'utf8') > MAX_PLANNER_CONTROL_BATCH_BYTES) {
    return {
      calls: [],
      errors: [
        {
          tool: 'batch',
          error: `Planner control batch exceeds the ${MAX_PLANNER_CONTROL_BATCH_BYTES}-byte UTF-8 limit`,
        },
      ],
      blocked_policy_silence: [],
    };
  }
  return {
    calls: validated,
    errors: [],
    blocked_policy_silence: blockedPolicySilence,
    controls_json: controlsJson,
  };
}

/**
 * Validate one deterministic control and prepare (but do not execute) its
 * effect. The caller prepares the complete batch before invoking any returned
 * closure, so a later invalid control cannot leave an earlier direct effect.
 */
function preparePlannerToolCall(call: PlannerToolCall, fx: PlannerToolEffects, iter: number): PreparedPlannerToolCall {
  switch (call.tool) {
    case 'notify_user': {
      const { level, summary, detail, channel } = call.args as {
        level?: PushLevel;
        summary: string;
        detail?: string;
        channel?: PushChannel;
      };
      return {
        tool: call.tool,
        apply: () => [
          Msg.pushUser(iter, {
            level: level ?? 'info',
            summary,
            detail,
            channel: channel ?? 'auto',
          }),
        ],
      };
    }
    case 'spawn_subagents': {
      const raw = call.args;
      const ownValue = (field: keyof SpawnSubagentsArgs): unknown => Object.getOwnPropertyDescriptor(raw, field)?.value;
      const args = Object.create(null) as SpawnSubagentsArgs;
      const coderEngine = ownValue('coder_engine');
      const coderModel = ownValue('coder_model');
      const reviewerEngine = ownValue('reviewer_engine');
      const reviewerModel = ownValue('reviewer_model');
      const initialDirective = ownValue('initial_directive');
      if (coderEngine !== undefined) args.coder_engine = coderEngine as EngineType;
      if (coderModel !== undefined) args.coder_model = coderModel as string;
      if (reviewerEngine !== undefined) args.reviewer_engine = reviewerEngine as EngineType;
      if (reviewerModel !== undefined) args.reviewer_model = reviewerModel as string;
      if (initialDirective !== undefined)
        args.initial_directive = initialDirective as SpawnSubagentsArgs['initial_directive'];
      return {
        tool: call.tool,
        apply: async () => {
          await fx.spawnSubagents(args);
          const init = args.initial_directive;
          return init?.goal
            ? [
                Msg.directive(iter, {
                  goal: init.goal,
                  constraints: init.constraints ?? [],
                  success_criteria: init.success_criteria ?? [],
                  max_attempts: init.max_attempts ?? 1,
                }),
              ]
            : [];
        },
      };
    }
    case 'spawn_coder': {
      const raw = call.args;
      const args = Object.create(null) as SpawnCoderArgs;
      const coderEngine = Object.getOwnPropertyDescriptor(raw, 'coder_engine')?.value;
      const coderModel = Object.getOwnPropertyDescriptor(raw, 'coder_model')?.value;
      if (coderEngine !== undefined) args.coder_engine = coderEngine as EngineType;
      if (coderModel !== undefined) args.coder_model = coderModel as string;
      return {
        tool: call.tool,
        apply: async () => {
          if (!fx.spawnCoder) throw new Error('spawn_coder handler is not installed');
          await fx.spawnCoder(args);
          return [];
        },
      };
    }
    case 'spawn_reviewer': {
      const raw = call.args;
      const args = Object.create(null) as SpawnReviewerArgs;
      const reviewerEngine = Object.getOwnPropertyDescriptor(raw, 'reviewer_engine')?.value;
      const reviewerModel = Object.getOwnPropertyDescriptor(raw, 'reviewer_model')?.value;
      if (reviewerEngine !== undefined) args.reviewer_engine = reviewerEngine as EngineType;
      if (reviewerModel !== undefined) args.reviewer_model = reviewerModel as string;
      return {
        tool: call.tool,
        apply: async () => {
          if (!fx.spawnReviewer) throw new Error('spawn_reviewer handler is not installed');
          await fx.spawnReviewer(args);
          return [];
        },
      };
    }
    case 'request_review':
      return {
        tool: call.tool,
        apply: async () => {
          if (!fx.requestReview) throw new Error('request_review handler is not installed');
          if (!fx.releaseReviewRequest) throw new Error('request_review release handler is not installed');
          const result = await fx.requestReview(call.args as unknown as RequestReviewArgs, iter);
          return result.status === 'prepared' ? [Msg.reviewRequest(iter, result.payload)] : [];
        },
      };
    case 'send_directive': {
      const { goal, constraints, success_criteria, max_attempts } = call.args as {
        goal: string;
        constraints?: string[];
        success_criteria?: string[];
        max_attempts?: number;
      };
      return {
        tool: call.tool,
        apply: () => [
          Msg.directive(iter, {
            goal,
            constraints: constraints ?? [],
            success_criteria: success_criteria ?? [],
            max_attempts: max_attempts ?? 1,
          }),
        ],
      };
    }
    case 'pause_loop':
      return {
        tool: call.tool,
        apply: () => [
          Msg.pause(iter, {
            reason: ((call.args as { reason?: string }).reason as string) ?? 'planner-pause',
          }),
        ],
      };
    case 'resume_loop':
      return { tool: call.tool, apply: () => [Msg.resume(iter)] };
    case 'terminate':
      return {
        tool: call.tool,
        apply: () => [
          Msg.terminate(iter, {
            reason: ((call.args as { reason?: string }).reason as string) ?? 'planner-terminate',
          }),
        ],
      };
    case 'update_push_policy':
      return {
        tool: call.tool,
        apply: () => {
          fx.updatePushPolicy(call.args);
          return [];
        },
      };
    case 'write_plan': {
      const { content, commit_message } = call.args as { content: string; commit_message?: string };
      return {
        tool: call.tool,
        artifact: { file: 'plan.md', content, commitMessage: commit_message },
        apply: () => [],
      };
    }
    case 'write_goal': {
      const { content, commit_message } = call.args as { content: string; commit_message?: string };
      return {
        tool: call.tool,
        artifact: { file: 'goal.json', content, commitMessage: commit_message },
        apply: () => [],
      };
    }
    default:
      throw new Error(`unknown planner tool: ${call.tool as string}`);
  }
}

/**
 * Apply a batch of parsed tool calls in order. Returns any v2 envelopes that
 * the dispatcher should hand back to the runner so it can route them.
 *
 * Note: notify_user / pause_loop / resume_loop / terminate / send_directive
 * become v2 messages and flow through the runner's normal queue (so policy,
 * dedup, push_log accounting all apply). `request_review` first prepares its
 * durable checkpoint evidence, then becomes a routed v2 message. Only
 * spawn_coder / spawn_reviewer / spawn_subagents / commit / push-policy
 * mutation are direct side effects.
 */
export async function applyPlannerToolCalls(
  calls: PlannerToolCall[],
  fx: PlannerToolEffects,
  iter: number,
): Promise<PlannerToolHandlerResult> {
  const validation = validatePlannerToolCalls(calls);
  return await applyValidatedPlannerToolCalls(validation, fx, iter);
}

/**
 * Trusted internal apply path for the exact result returned by
 * `validatePlannerToolCalls`. The dispatcher already crossed the complete
 * untrusted boundary before durable persistence, so validating and serializing
 * the same MiB-scale batch a second time adds work without adding a fence.
 */
export async function applyValidatedPlannerToolCalls(
  validation: PlannerToolValidationResult,
  fx: PlannerToolEffects,
  iter: number,
): Promise<PlannerToolHandlerResult> {
  const emitted_messages: AnyAutoloopMessage[] = [];
  if (validation.errors.length > 0) return { emitted_messages, errors: validation.errors };
  if (validation.controls_json === undefined) {
    return {
      emitted_messages,
      errors: [{ tool: 'batch', error: 'Planner controls were not validated before application' }],
    };
  }
  const errors: Array<{ tool: string; error: string }> = [];
  const prepared: PreparedPlannerToolCall[] = [];

  for (const call of validation.calls) prepared.push(preparePlannerToolCall(call, fx, iter));

  const artifacts = prepared.flatMap(({ artifact }) => (artifact ? [artifact] : []));
  if (artifacts.length > 0) {
    try {
      fx.assertActive?.();
      await fx.writePlanFiles(artifacts);
      fx.assertActive?.();
    } catch (err) {
      return {
        emitted_messages: [],
        errors: [{ tool: artifacts.map(({ file }) => file).join(','), error: (err as Error).message }],
      };
    }
  }

  for (const control of prepared) {
    if (control.artifact) continue;
    let messages: AnyAutoloopMessage[] = [];
    try {
      fx.assertActive?.();
      messages = await control.apply();
      fx.assertActive?.();
      emitted_messages.push(...messages);
    } catch (err) {
      for (const message of messages) {
        if (message.type !== 'review_request') continue;
        const payload = message.payload as Partial<CheckpointReviewRequestPayload>;
        if (typeof payload.idempotency_key === 'string') {
          fx.releaseReviewRequest?.(payload.idempotency_key, payload as CheckpointReviewRequestPayload);
        }
      }
      errors.push({ tool: control.tool, error: (err as Error).message });
      break;
    }
  }

  return { emitted_messages, errors };
}
