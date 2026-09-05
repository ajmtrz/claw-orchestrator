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
import { type AnyAutoloopMessage, Msg, type PushChannel, type PushLevel } from './messages.js';

export type PlannerToolName =
  | 'notify_user'
  | 'spawn_subagents'
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

export interface PlannerToolEffects {
  /** Start Coder + Reviewer persistent sessions. */
  spawnSubagents: (args: SpawnSubagentsArgs) => Promise<void>;
  /** Apply an already validated/canonical in-memory push-policy delta. */
  updatePushPolicy: (delta: Record<string, unknown>) => void;
  /**
   * Write content to <workspace>/<file> (plan.md or goal.json), then
   * best-effort `git add && git commit`. The Planner has no Write/Edit
   * tools — this autoloop tool is the only path to author plan.md/goal.json,
   * which physically prevents the Planner from doing Coder work.
   */
  writePlanFile: (file: 'plan.md' | 'goal.json', content: string, commitMessage?: string) => Promise<void>;
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
  apply: () => Promise<AnyAutoloopMessage[]> | AnyAutoloopMessage[];
}

const VALID_PUSH_LEVELS = new Set<PushLevel>(['info', 'warn', 'decision', 'error']);
const VALID_PUSH_CHANNELS = new Set<PushChannel>(['auto', 'wechat', 'webchat', 'both', 'email']);
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

function optionalString(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string') throw new Error(`${label} must be a string`);
  return boundedString(value, label);
}

function optionalStringArray(value: unknown, label: string): string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || !value.every((entry) => typeof entry === 'string')) {
    throw new Error(`${label} must be an array of strings`);
  }
  if (value.length > MAX_PLANNER_CONTROL_ARRAY_ITEMS) {
    throw new Error(`${label} exceeds the ${MAX_PLANNER_CONTROL_ARRAY_ITEMS}-item limit`);
  }
  return value.map((entry, index) => boundedString(entry, `${label}[${index}]`));
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
  const directive: NonNullable<SpawnSubagentsArgs['initial_directive']> = {
    goal: nonEmptyString(raw.goal, `${label} goal`),
  };
  const constraints = optionalStringArray(raw.constraints, `${label} constraints`);
  const successCriteria = optionalStringArray(raw.success_criteria, `${label} success_criteria`);
  const maxAttempts = optionalPositiveInteger(raw.max_attempts, `${label} max_attempts`);
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
      rule.channel = input.channel;
    }
    if ('level' in input) {
      if (typeof input.level !== 'string' || !VALID_PUSH_LEVELS.has(input.level as PushLevel)) {
        throw new Error(`update_push_policy ${key} level '${String(input.level)}' is not supported`);
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
      const args: Record<string, unknown> = { summary };
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
      if (
        'coder_custom_engine' in raw ||
        'reviewer_custom_engine' in raw ||
        'coderCustomEngine' in raw ||
        'reviewerCustomEngine' in raw ||
        'customEngine' in raw
      ) {
        throw new Error('spawn_subagents cannot include custom engine config; configure it at autoloop_start');
      }
      const args: Record<string, unknown> = {};
      for (const field of ['coder_engine', 'reviewer_engine'] as const) {
        const value = raw[field];
        if (value !== undefined && (typeof value !== 'string' || !ENGINE_TYPES.includes(value as EngineType))) {
          throw new Error(`spawn_subagents ${field} has unknown engine '${String(value)}'`);
        }
        if (value !== undefined) args[field] = value;
      }
      for (const field of ['coder_model', 'reviewer_model'] as const) {
        const value = optionalString(raw[field], `spawn_subagents ${field}`);
        if (value !== undefined) args[field] = value;
      }
      if (raw.initial_directive !== undefined) {
        if (!isPlainObject(raw.initial_directive)) {
          throw new Error('spawn_subagents initial_directive must be an object');
        }
        args.initial_directive = sanitizeDirectiveArgs(
          raw.initial_directive as Record<string, unknown>,
          'spawn_subagents initial_directive',
        );
      }
      return { tool: call.tool, args };
    }
    case 'send_directive':
      return { tool: call.tool, args: sanitizeDirectiveArgs(raw, 'send_directive') };
    case 'pause_loop': {
      const reason = optionalString(raw.reason, 'pause_loop reason');
      return { tool: call.tool, args: reason === undefined ? {} : { reason } };
    }
    case 'resume_loop':
      return { tool: call.tool, args: {} };
    case 'terminate': {
      const reason = optionalString(raw.reason, 'terminate reason');
      return { tool: call.tool, args: reason === undefined ? {} : { reason } };
    }
    case 'update_push_policy':
      return { tool: call.tool, args: sanitizePushPolicyDelta(raw, blockedPolicySilence) };
    case 'write_plan': {
      const content = boundedPlannerContent(raw.content, 'write_plan content');
      const commitMessage = optionalString(raw.commit_message, 'write_plan commit_message');
      return {
        tool: call.tool,
        args: commitMessage === undefined ? { content } : { content, commit_message: commitMessage },
      };
    }
    case 'write_goal': {
      const content = boundedPlannerContent(raw.content, 'write_goal content');
      try {
        JSON.parse(content);
      } catch (error) {
        throw new Error(`write_goal content is not valid JSON: ${(error as Error).message}`);
      }
      const commitMessage = optionalString(raw.commit_message, 'write_goal commit_message');
      return {
        tool: call.tool,
        args: commitMessage === undefined ? { content } : { content, commit_message: commitMessage },
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

function normalizePlannerControlValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizePlannerControlValue);
  if (!value || typeof value !== 'object') return value;
  return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([key, entry]) => [key, normalizePlannerControlValue(entry)]),
  );
}

function normalizePlannerControls(controls: readonly PlannerToolCall[]): PlannerToolCall[] {
  return controls.map(({ tool, args }) => ({
    tool,
    args: normalizePlannerControlValue(args) as Record<string, unknown>,
  }));
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
  const validated: PlannerToolCall[] = [];
  const errors: Array<{ tool: string; error: string }> = [];
  const blockedPolicySilence: string[] = [];
  for (const call of calls) {
    try {
      const blockedBefore = blockedPolicySilence.length;
      const sanitized = sanitizePlannerToolCall(call, blockedPolicySilence);
      const isBlockedSilenceOnlyControl =
        sanitized.tool === 'update_push_policy' &&
        blockedPolicySilence.length > blockedBefore &&
        Object.keys(sanitized.args).length === 0;
      if (!isBlockedSilenceOnlyControl) validated.push(sanitized);
    } catch (error) {
      errors.push({ tool: call.tool, error: (error as Error).message });
    }
  }
  if (errors.length > 0) return { calls: [], errors, blocked_policy_silence: [] };

  const normalized = normalizePlannerControls(validated);
  const controlsJson = JSON.stringify(normalized);
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
    calls: normalized,
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
      const args: SpawnSubagentsArgs = {};
      if (raw.coder_engine !== undefined) args.coder_engine = raw.coder_engine as EngineType;
      if (raw.coder_model !== undefined) args.coder_model = raw.coder_model as string;
      if (raw.reviewer_engine !== undefined) args.reviewer_engine = raw.reviewer_engine as EngineType;
      if (raw.reviewer_model !== undefined) args.reviewer_model = raw.reviewer_model as string;
      if (raw.initial_directive !== undefined)
        args.initial_directive = raw.initial_directive as SpawnSubagentsArgs['initial_directive'];
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
        apply: async () => {
          await fx.writePlanFile('plan.md', content, commit_message);
          return [];
        },
      };
    }
    case 'write_goal': {
      const { content, commit_message } = call.args as { content: string; commit_message?: string };
      return {
        tool: call.tool,
        apply: async () => {
          await fx.writePlanFile('goal.json', content, commit_message);
          return [];
        },
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
 * dedup, push_log accounting all apply). Only spawn_subagents / commit /
 * push-policy mutation are direct side effects.
 */
export async function applyPlannerToolCalls(
  calls: PlannerToolCall[],
  fx: PlannerToolEffects,
  iter: number,
): Promise<PlannerToolHandlerResult> {
  const emitted_messages: AnyAutoloopMessage[] = [];
  const validation = validatePlannerToolCalls(calls);
  if (validation.errors.length > 0) return { emitted_messages, errors: validation.errors };
  const errors: Array<{ tool: string; error: string }> = [];
  const prepared: PreparedPlannerToolCall[] = [];

  for (const call of validation.calls) prepared.push(preparePlannerToolCall(call, fx, iter));

  for (const control of prepared) {
    try {
      emitted_messages.push(...(await control.apply()));
    } catch (err) {
      errors.push({ tool: control.tool, error: (err as Error).message });
      break;
    }
  }

  return { emitted_messages, errors };
}
