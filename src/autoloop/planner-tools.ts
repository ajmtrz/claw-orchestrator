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
      if (typeof parsed?.tool !== 'string' || typeof parsed?.args !== 'object' || parsed.args === null) {
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
  /** Mutate in-memory push policy (key→rule object). Unknown keys ignored. */
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

function nonEmptyString(value: unknown, label: string): string {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${label} must be a non-empty string`);
  return value;
}

function optionalString(value: unknown, label: string): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== 'string') throw new Error(`${label} must be a string`);
  return value;
}

function optionalStringArray(value: unknown, label: string): string[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value) || !value.every((entry) => typeof entry === 'string')) {
    throw new Error(`${label} must be an array of strings`);
  }
  return [...value];
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

function sanitizePushPolicyDelta(raw: Record<string, unknown>): Record<string, unknown> {
  const delta: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(raw)) {
    if (!PUSH_POLICY_KEYS.has(key) || typeof value !== 'object' || value === null || Array.isArray(value)) continue;
    const input = value as Record<string, unknown>;
    const rule: Record<string, unknown> = {};
    if (typeof input.silent === 'boolean') rule.silent = input.silent;
    if (typeof input.level === 'string' && VALID_PUSH_LEVELS.has(input.level as PushLevel)) rule.level = input.level;
    if (typeof input.channel === 'string' && VALID_PUSH_CHANNELS.has(input.channel as PushChannel)) {
      rule.channel = input.channel;
    }
    delta[key] = rule;
  }
  return delta;
}

/**
 * Validate one Planner control and return the exact allowlisted representation
 * that may be persisted and applied. Unknown fields are deliberately dropped;
 * fields whose presence changes safety or semantics are rejected when invalid.
 */
function sanitizePlannerToolCall(call: PlannerToolCall): PlannerToolCall {
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
        if (
          typeof raw.initial_directive !== 'object' ||
          raw.initial_directive === null ||
          Array.isArray(raw.initial_directive)
        ) {
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
      return { tool: call.tool, args: sanitizePushPolicyDelta(raw) };
    case 'write_plan': {
      const content = nonEmptyString(raw.content, 'write_plan content');
      const commitMessage = optionalString(raw.commit_message, 'write_plan commit_message');
      return {
        tool: call.tool,
        args: commitMessage === undefined ? { content } : { content, commit_message: commitMessage },
      };
    }
    case 'write_goal': {
      const content = nonEmptyString(raw.content, 'write_goal content');
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
}

/** Validate and sanitize the complete batch without performing any effect. */
export function validatePlannerToolCalls(calls: readonly PlannerToolCall[]): PlannerToolValidationResult {
  const validated: PlannerToolCall[] = [];
  const errors: Array<{ tool: string; error: string }> = [];
  for (const call of calls) {
    try {
      validated.push(sanitizePlannerToolCall(call));
    } catch (error) {
      errors.push({ tool: call.tool, error: (error as Error).message });
    }
  }
  return { calls: errors.length === 0 ? validated : [], errors };
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
    }
  }

  return { emitted_messages, errors };
}
