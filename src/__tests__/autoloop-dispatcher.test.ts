/**
 * Tests for ClaudeAgentDispatcher — the layer between the runner's message
 * bus and the real persistent Claude sessions. We stub SessionManager so the
 * tests stay hermetic; only behaviour owned by the dispatcher (frozen-memory
 * injection, sandbox staging, send-failure surfacing, decisions audit, policy
 * silencing guard) is exercised.
 */

import { afterEach, beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { execFileSync, type ChildProcess } from 'node:child_process';
import { PassThrough } from 'node:stream';

import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import { SecureAutoloopLedger } from '../autoloop/secure-ledger.js';
import {
  applyPlannerToolCalls,
  parsePlannerReply,
  type PlannerToolCall,
  type PlannerToolEffects,
  type PreparedReviewRequest,
  type ReviewRequestPreparationResult,
  validatePlannerToolCalls,
} from '../autoloop/planner-tools.js';
import { AutoloopRunner } from '../autoloop/runner.js';
import {
  AutoloopRoutingError,
  type AnyAutoloopMessage,
  type CheckpointReviewRequestPayload,
  Msg,
  validateMessage,
} from '../autoloop/messages.js';
import type { SessionManager } from '../session-manager.js';
import type {
  AgentReservationReleaseOptions,
  AutoloopState,
  PhysicalAgentGeneration,
  PushPolicy,
} from '../autoloop/types.js';
import { DEFAULT_PUSH_POLICY, LEDGER_SCHEMA_VERSION } from '../autoloop/types.js';

const TEST_OWNER_INSTANCE_ID = `session-manager:${process.pid}:00000000-0000-4000-8000-000000000001`;

interface StubCalls {
  startSession: ReturnType<typeof vi.fn>;
  sendMessage: ReturnType<typeof vi.fn>;
  stopSession: ReturnType<typeof vi.fn>;
  inspect: ReturnType<typeof vi.fn>;
  releaseReservation: ReturnType<typeof vi.fn>;
  reserveAgentGeneration: ReturnType<typeof vi.fn>;
  getStatus: ReturnType<typeof vi.fn>;
  compactSession: ReturnType<typeof vi.fn>;
}

function makeStubManager(
  opts: {
    sendOutput?: string;
    sendOutputs?: string[];
    sendThrows?: number;
    startThrowsFor?: 'planner' | 'coder' | 'reviewer';
    contextPercent?: number;
  } = {},
): {
  manager: SessionManager;
  calls: StubCalls;
  activeNames: Set<string>;
  reservations: Map<string, PhysicalAgentGeneration>;
} {
  let throwsRemaining = opts.sendThrows ?? 0;
  let sendIndex = 0;
  const activeNames = new Set<string>();
  const reservations = new Map<string, PhysicalAgentGeneration>();
  const pendingReleases = new Map<string, { generation: PhysicalAgentGeneration; releaseOwnerInstanceId: string }>();
  const releasedGenerations = new Map<string, number>();
  const calls: StubCalls = {
    startSession: vi.fn(async (config: { name: string }) => {
      if (opts.startThrowsFor && config.name.endsWith(`-${opts.startThrowsFor}`)) {
        throw new Error(`${opts.startThrowsFor} failed to start`);
      }
      activeNames.add(config.name);
      return {
        name: config.name,
        state: 'ready',
        claudeSessionId: reservations.get(config.name)?.session_id,
      };
    }),
    sendMessage: vi.fn(async () => {
      if (throwsRemaining > 0) {
        throwsRemaining -= 1;
        throw new Error('subprocess died');
      }
      const output = opts.sendOutputs?.[sendIndex] ?? opts.sendOutput ?? '';
      sendIndex += 1;
      return { output, error: undefined };
    }),
    stopSession: vi.fn(async (name: string) => {
      activeNames.delete(name);
    }),
    inspect: vi.fn(async (name: string, sessionId?: string) => {
      const reservation = reservations.get(name);
      if (reservation && sessionId && reservation.session_id !== sessionId) return 'unknown';
      return activeNames.has(name) ? 'live' : 'absent';
    }),
    releaseReservation: vi.fn(
      async (name: string, expectedGeneration: number, options: AgentReservationReleaseOptions) => {
        if (options.rollbackUncommittedReservation) {
          const reservation = reservations.get(name);
          if (
            pendingReleases.has(name) ||
            activeNames.has(name) ||
            reservation?.generation !== expectedGeneration ||
            options.expectedOwnerInstanceId === undefined ||
            reservation.owner_instance_id !== options.expectedOwnerInstanceId ||
            options.expectedSessionId === undefined ||
            reservation.session_id !== options.expectedSessionId
          ) {
            return false;
          }
          reservations.delete(name);
          return true;
        }
        const pending = pendingReleases.get(name);
        if (pending) {
          if (
            pending.generation.generation !== expectedGeneration ||
            pending.generation.owner_instance_id !== options.expectedOwnerInstanceId ||
            pending.generation.session_id !== options.expectedSessionId ||
            pending.releaseOwnerInstanceId !== options.releaseOwnerInstanceId
          ) {
            return false;
          }
        } else {
          if (releasedGenerations.get(name) === expectedGeneration) return true;
          const reservation = reservations.get(name);
          if (activeNames.has(name) || !reservation || !options.releaseOwnerInstanceId) return false;
          let target: PhysicalAgentGeneration;
          if (expectedGeneration === 0) {
            if (reservation.generation !== 0 || options.expectedOwnerInstanceId !== 'legacy-registry') return false;
            target = {
              ...reservation,
              owner_instance_id: options.expectedOwnerInstanceId,
              session_id: options.expectedSessionId,
            };
          } else {
            if (
              reservation.generation !== expectedGeneration ||
              reservation.owner_instance_id !== options.expectedOwnerInstanceId ||
              reservation.session_id !== options.expectedSessionId
            ) {
              return false;
            }
            target = reservation;
          }
          pendingReleases.set(name, {
            generation: target,
            releaseOwnerInstanceId: options.releaseOwnerInstanceId,
          });
        }
        options.beforeRelease?.();
        if (!options.persistReleaseEvidence) return false;
        options.persistReleaseEvidence();
        reservations.delete(name);
        pendingReleases.delete(name);
        releasedGenerations.set(name, expectedGeneration);
        return true;
      },
    ),
    reserveAgentGeneration: vi.fn((candidate: PhysicalAgentGeneration) => {
      if (pendingReleases.has(candidate.session_name)) return false;
      const existing = reservations.get(candidate.session_name);
      if (
        existing &&
        (existing.generation !== candidate.generation ||
          existing.owner_instance_id !== candidate.owner_instance_id ||
          existing.session_id !== candidate.session_id)
      ) {
        return false;
      }
      const releasedGeneration = releasedGenerations.get(candidate.session_name);
      if (releasedGeneration !== undefined && candidate.generation !== releasedGeneration + 1) return false;
      reservations.set(candidate.session_name, candidate);
      return true;
    }),
    getStatus: vi.fn(() => ({
      stats: { contextPercent: opts.contextPercent ?? 10, tokensIn: 0, tokensOut: 0, cachedTokens: 0 },
    })),
    compactSession: vi.fn(async () => undefined),
  };
  const manager = {
    autoloopOwnerInstanceId: TEST_OWNER_INSTANCE_ID,
    startSession: calls.startSession,
    sendMessage: calls.sendMessage,
    stopSession: calls.stopSession,
    inspect: calls.inspect,
    releaseReservation: calls.releaseReservation,
    reserveAgentGeneration: calls.reserveAgentGeneration,
    getStatus: calls.getStatus,
    compactSession: calls.compactSession,
  } as unknown as SessionManager;
  return { manager, calls, activeNames, reservations };
}

let tmpRoot: string;

function permissions(target: string): number {
  return fs.statSync(target).mode & 0o777;
}

function replaceWithSameBytes(target: string): void {
  const replacement = `${target}.same-content-replacement`;
  fs.writeFileSync(replacement, fs.readFileSync(target), { mode: 0o600 });
  fs.renameSync(replacement, target);
}

beforeEach(() => {
  tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-disp-'));
});

afterEach(() => {
  vi.useRealTimers();
  fs.rmSync(tmpRoot, { recursive: true, force: true });
});

async function withPrototypeDescriptors<T>(
  changes: ReadonlyArray<{ target: object; key: PropertyKey; descriptor: PropertyDescriptor }>,
  action: () => T | Promise<T>,
): Promise<{ result: T | undefined; thrown: unknown }> {
  const originals = changes.map(({ target, key }) => ({
    target,
    key,
    descriptor: Object.getOwnPropertyDescriptor(target, key),
  }));
  let result: T | undefined;
  let thrown: unknown;

  try {
    for (const { target, key, descriptor } of changes) Object.defineProperty(target, key, descriptor);
    try {
      result = await action();
    } catch (error) {
      thrown = error;
    }
  } finally {
    for (let index = originals.length - 1; index >= 0; index -= 1) {
      const { target, key, descriptor } = originals[index];
      if (descriptor === undefined) Reflect.deleteProperty(target, key);
      else Object.defineProperty(target, key, descriptor);
    }
  }

  for (const { target, key, descriptor } of originals) {
    expect(Object.getOwnPropertyDescriptor(target, key)).toEqual(descriptor);
  }
  return { result, thrown };
}

function makeDispatcher(
  overrides: Partial<ConstructorParameters<typeof ClaudeAgentDispatcher>[0]> = {},
  managerOpts?: Parameters<typeof makeStubManager>[0],
): {
  dispatcher: ClaudeAgentDispatcher;
  calls: StubCalls;
  ledgerDir: string;
  workspace: string;
  activeNames: Set<string>;
  reservations: Map<string, PhysicalAgentGeneration>;
} {
  const { manager, calls, activeNames, reservations } = makeStubManager(managerOpts);
  const workspace = tmpRoot;
  const dispatcher = new ClaudeAgentDispatcher({
    manager,
    runId: 'r1',
    workspace,
    ...overrides,
  });
  const ledgerDir = path.join(workspace, 'tasks', 'r1');
  return { dispatcher, calls, ledgerDir, workspace, activeNames, reservations };
}

function ensureCompleteReviewArtifacts(dispatcher: ClaudeAgentDispatcher, iter: number): void {
  const ledger = dispatcher.secureLedgerCapability;
  const artifacts = {
    'directive.json': '{}\n',
    'eval_output.json': '{}\n',
    'coder_summary.txt': 'complete\n',
    'diff.patch': 'diff --git a/a b/a\n',
  } as const;
  for (const [name, content] of Object.entries(artifacts)) {
    const artifactName = name as keyof typeof artifacts;
    if (ledger.readIterationArtifact(iter, artifactName) === undefined) {
      ledger.writeIterationArtifact(iter, artifactName, content);
    }
  }
}

function commitCheckpoint(workspace: string, content: string): { sha: string; patch: Buffer } {
  const target = path.join(workspace, 'checkpoint.txt');
  fs.writeFileSync(target, content);
  execFileSync('git', ['add', '--', 'checkpoint.txt'], { cwd: workspace });
  execFileSync('git', ['commit', '-q', '-m', `checkpoint ${content.trim()}`], { cwd: workspace });
  const sha = execFileSync('git', ['rev-parse', '--verify', 'HEAD'], { cwd: workspace, encoding: 'utf8' }).trim();
  const patch = execFileSync('git', ['show', '--format=', '--unified=3', '--no-renames', sha, '--'], {
    cwd: workspace,
  });
  return { sha, patch };
}

function initializeEmptyRootCheckpointRepository(workspace: string): { sha: string; patch: Buffer } {
  execFileSync('git', ['init', '-q'], { cwd: workspace });
  execFileSync('git', ['config', 'user.email', 'autoloop-test@example.invalid'], { cwd: workspace });
  execFileSync('git', ['config', 'user.name', 'Autoloop Test'], { cwd: workspace });
  execFileSync('git', ['commit', '-q', '--allow-empty', '-m', 'empty root checkpoint'], { cwd: workspace });
  const sha = execFileSync('git', ['rev-parse', '--verify', 'HEAD'], { cwd: workspace, encoding: 'utf8' }).trim();
  const patch = execFileSync('git', ['show', '--format=', '--unified=3', '--no-renames', sha, '--'], {
    cwd: workspace,
  });
  return { sha, patch };
}

function initializeCheckpointRepository(
  workspace: string,
  content = 'checkpoint one\n',
): { sha: string; patch: Buffer } {
  execFileSync('git', ['init', '-q'], { cwd: workspace });
  execFileSync('git', ['config', 'user.email', 'autoloop-test@example.invalid'], { cwd: workspace });
  execFileSync('git', ['config', 'user.name', 'Autoloop Test'], { cwd: workspace });
  return commitCheckpoint(workspace, content);
}

function writeSourceReviewArtifacts(
  workspace: string,
  sourceRunId: string,
  sourceIter: number,
  patch: Buffer,
  options: {
    omit?: 'directive.json' | 'eval_output.json' | 'coder_summary.txt' | 'diff.patch';
    directive?: string | Buffer;
  } = {},
): SecureAutoloopLedger {
  const sourceLedger = SecureAutoloopLedger.open(workspace, sourceRunId, { create: true });
  const artifacts = {
    'directive.json': options.directive ?? '{"goal":"already implemented"}\n',
    'eval_output.json': '{"metric":1}\n',
    'coder_summary.txt': 'existing checkpoint\n',
    'diff.patch': patch,
  } as const;
  for (const [name, content] of Object.entries(artifacts)) {
    if (name !== options.omit) {
      sourceLedger.writeIterationArtifact(
        sourceIter,
        name as 'directive.json' | 'eval_output.json' | 'coder_summary.txt' | 'diff.patch',
        content,
      );
    }
  }
  return sourceLedger;
}

function reviewIdentityHash(idempotencyKey: string): string {
  return createHash('sha256').update(idempotencyKey).digest('hex');
}

function durableReviewDecisionPayload(
  idempotencyKey: string,
  overrides: Partial<{
    checkpoint_sha: string;
    source_run_id: string;
    source_iter: number;
    target_iter: number;
    scope: string[];
    request_digest: string;
  }> = {},
): Readonly<Record<string, unknown>> {
  return {
    checkpoint_sha: 'a'.repeat(40),
    source_run_id: 'source-run',
    source_iter: 3,
    target_iter: 0,
    scope: ['correctness'],
    idempotency_key: idempotencyKey,
    request_digest: createHash('sha256').update(`request:${idempotencyKey}`).digest('hex'),
    ...overrides,
  };
}

function durableReviewDecisionRow(payload: Readonly<Record<string, unknown>>): string {
  return JSON.stringify({
    ts: '2026-01-01T00:00:00.000Z',
    kind: 'request_review',
    actor: 'planner',
    payload,
  });
}

interface FakeGitChild extends EventEmitter {
  stdout: PassThrough;
  stderr: PassThrough;
  pid: number | undefined;
  kill: ReturnType<typeof vi.fn>;
}

function fakeGitChild(pid?: number): FakeGitChild {
  const child = new EventEmitter() as FakeGitChild;
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.pid = pid;
  child.kill = vi.fn(() => true);
  return child;
}

function requestReviewDecisions(ledgerDir: string): Array<Record<string, unknown>> {
  const target = path.join(ledgerDir, 'decisions.jsonl');
  if (!fs.existsSync(target)) return [];
  return fs
    .readFileSync(target, 'utf8')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .filter((entry) => entry.kind === 'request_review');
}

function decisionRows(ledgerDir: string, kind: string): Array<Record<string, unknown>> {
  const target = path.join(ledgerDir, 'decisions.jsonl');
  if (!fs.existsSync(target)) return [];
  return fs
    .readFileSync(target, 'utf8')
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .filter((entry) => entry.kind === kind);
}

describe('Planner control argument shape', () => {
  it.each([
    'notify_user',
    'spawn_subagents',
    'send_directive',
    'pause_loop',
    'resume_loop',
    'terminate',
    'update_push_policy',
    'write_plan',
    'write_goal',
  ])('rejects array arguments for %s at the parser boundary', (tool) => {
    const parsed = parsePlannerReply(['```autoloop', JSON.stringify({ tool, args: [] }), '```'].join('\n'));

    expect(parsed.calls).toEqual([]);
    expect(parsed.parse_errors).toEqual([
      expect.objectContaining({ block_index: 0, error: expect.stringContaining('tool/args') }),
    ]);
  });

  it('rejects non-plain object arguments at the validator boundary', () => {
    const validation = validatePlannerToolCalls([
      { tool: 'resume_loop', args: new Date() as unknown as Record<string, unknown> },
    ]);

    expect(validation.calls).toEqual([]);
    expect(validation.errors).toEqual([
      expect.objectContaining({ tool: 'resume_loop', error: expect.stringContaining('plain object') }),
    ]);
  });
});

describe('Planner control batch application', () => {
  it('stops after a failed spawn without applying later file, policy, or message effects', async () => {
    const effects: PlannerToolEffects = {
      spawnCoder: vi.fn(async () => undefined),
      spawnReviewer: vi.fn(async () => undefined),
      spawnSubagents: vi.fn(async () => {
        throw new Error('spawn failed before completion');
      }),
      updatePushPolicy: vi.fn(),
      writePlanFile: vi.fn(async () => undefined),
    };
    const controls: PlannerToolCall[] = [
      { tool: 'spawn_subagents', args: {} },
      { tool: 'write_plan', args: { content: '# Must not be written' } },
      { tool: 'write_goal', args: { content: '{"goal":"must not be written"}' } },
      { tool: 'update_push_policy', args: { on_start: { level: 'warn' } } },
      { tool: 'notify_user', args: { summary: 'must not be emitted' } },
    ];

    const result = await applyPlannerToolCalls(controls, effects, 0);

    expect(result).toEqual({
      emitted_messages: [],
      errors: [{ tool: 'spawn_subagents', error: 'spawn failed before completion' }],
    });
    expect(effects.spawnSubagents).toHaveBeenCalledTimes(1);
    expect(effects.writePlanFile).not.toHaveBeenCalled();
    expect(effects.updatePushPolicy).not.toHaveBeenCalled();
  });
});

describe('Planner durable control content bounds', () => {
  const EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES = 1_048_576;

  function contentOfBytes(tool: 'write_plan' | 'write_goal', bytes: number): string {
    return tool === 'write_goal' ? JSON.stringify({ goal: 'a'.repeat(bytes - 11) }) : 'a'.repeat(bytes);
  }

  it.each(['write_plan', 'write_goal'] as const)('accepts %s content at the exact UTF-8 byte bound', (tool) => {
    const content = contentOfBytes(tool, EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES);

    const validation = validatePlannerToolCalls([{ tool, args: { content } }]);

    expect(Buffer.byteLength(content, 'utf8')).toBe(EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES);
    expect(validation.errors).toEqual([]);
    expect(validation.calls).toEqual([
      {
        tool,
        args: {
          content,
          commit_message: `autoloop: planner writes ${tool === 'write_plan' ? 'plan.md' : 'goal.json'}`,
        },
      },
    ]);
  });

  it.each(['write_plan', 'write_goal'] as const)('rejects %s content one UTF-8 byte over the bound', (tool) => {
    const content = contentOfBytes(tool, EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES + 1);

    const validation = validatePlannerToolCalls([{ tool, args: { content } }]);

    expect(Buffer.byteLength(content, 'utf8')).toBe(EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES + 1);
    expect(validation.errors).toEqual([
      {
        tool,
        error: `${tool} content exceeds the ${EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES}-byte UTF-8 limit`,
      },
    ]);
    expect(validation.calls).toEqual([]);
  });

  it.each(['write_plan', 'write_goal'] as const)(
    'counts multibyte %s content in UTF-8 bytes instead of UTF-16 code units',
    (tool) => {
      const jsonOverhead = tool === 'write_goal' ? 2 : 0;
      const value = 'é'.repeat(Math.floor((EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES - jsonOverhead) / 2) + 1);
      const content = tool === 'write_goal' ? JSON.stringify(value) : value;

      const validation = validatePlannerToolCalls([{ tool, args: { content } }]);

      expect(content.length).toBeLessThan(EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES);
      expect(Buffer.byteLength(content, 'utf8')).toBeGreaterThan(EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES);
      expect(validation.errors).toEqual([
        {
          tool,
          error: `${tool} content exceeds the ${EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES}-byte UTF-8 limit`,
        },
      ]);
      expect(validation.calls).toEqual([]);
    },
  );

  it('prevalidates an oversized mixed batch before applying any effect or emitting any message', async () => {
    const effects: PlannerToolEffects = {
      spawnCoder: vi.fn(async () => undefined),
      spawnReviewer: vi.fn(async () => undefined),
      spawnSubagents: vi.fn(async () => undefined),
      updatePushPolicy: vi.fn(),
      writePlanFile: vi.fn(async () => undefined),
    };
    const oversizedGoal = contentOfBytes('write_goal', EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES + 1);
    const controls: PlannerToolCall[] = [
      { tool: 'update_push_policy', args: { on_start: { level: 'warn' } } },
      { tool: 'write_goal', args: { content: oversizedGoal } },
      { tool: 'notify_user', args: { summary: 'must not be emitted' } },
    ];

    const result = await applyPlannerToolCalls(controls, effects, 0);

    expect(result).toEqual({
      emitted_messages: [],
      errors: [
        {
          tool: 'write_goal',
          error: `write_goal content exceeds the ${EXPECTED_MAX_PLANNER_CONTROL_CONTENT_BYTES}-byte UTF-8 limit`,
        },
      ],
    });
    expect(effects.spawnSubagents).not.toHaveBeenCalled();
    expect(effects.updatePushPolicy).not.toHaveBeenCalled();
    expect(effects.writePlanFile).not.toHaveBeenCalled();
  });
});

function findStart(calls: StubCalls, role: 'planner' | 'coder' | 'reviewer'): Record<string, unknown> {
  const call = calls.startSession.mock.calls.find(
    (entry) => (entry[0] as { name: string }).name === `autoloop-r1-${role}`,
  );
  expect(call, `${role} startSession call`).toBeDefined();
  return call![0] as Record<string, unknown>;
}

interface ObservedSendTimeout {
  type: 'send_timeout';
  payload: {
    status: 'awaiting_resume';
    dispatch_id: string;
    agent: 'planner' | 'coder' | 'reviewer';
    message_id: string;
    message_type: string;
    iter: number;
    timeout_ms: number;
    error: string;
  };
}

function fixedIdentity<T extends AnyAutoloopMessage>(env: T, messageId: string, ts = '2026-09-03T00:00:00.000Z'): T {
  return { ...env, msg_id: messageId, ts };
}

function sendTimeout(replies: AnyAutoloopMessage[]): ObservedSendTimeout {
  expect(replies).toHaveLength(1);
  expect(replies[0].type).toBe('send_timeout');
  return replies[0] as unknown as ObservedSendTimeout;
}

function genuineSendTimeout(): Error {
  return new Error('Timeout waiting for response');
}

const AGENT_NOW = '2026-09-05T12:00:00.000Z';

type AgentGenerationEventKind =
  | 'agent_generation_reserved'
  | 'agent_generation_started'
  | 'agent_generation_lease_renewed'
  | 'agent_generation_orphaned'
  | 'agent_generation_released';

interface AgentGenerationEvent {
  kind: AgentGenerationEventKind;
  payload: PhysicalAgentGeneration;
}

function physicalGeneration(
  role: PhysicalAgentGeneration['role'],
  overrides: Partial<PhysicalAgentGeneration> = {},
): PhysicalAgentGeneration {
  return {
    role,
    generation: 1,
    session_name: `autoloop-r1-${role}`,
    session_id: `${role}-physical-1`,
    owner_instance_id: 'owner-old',
    created_at: '2026-09-05T10:00:00.000Z',
    last_activity_at: '2026-09-05T11:00:00.000Z',
    lease_expires_at: '2026-09-05T11:30:00.000Z',
    state: 'live',
    ...overrides,
  };
}

function appendGenerationEvent(
  ledgerDir: string,
  kind: AgentGenerationEventKind,
  generation: PhysicalAgentGeneration,
): void {
  fs.mkdirSync(ledgerDir, { recursive: true });
  fs.appendFileSync(
    path.join(ledgerDir, 'agent-generations.jsonl'),
    `${JSON.stringify({ ts: generation.last_activity_at, kind, actor: 'dispatcher', payload: generation })}\n`,
  );
}

function readGenerationEvents(ledgerDir: string): AgentGenerationEvent[] {
  const generationsPath = path.join(ledgerDir, 'agent-generations.jsonl');
  if (!fs.existsSync(generationsPath)) return [];
  return fs
    .readFileSync(generationsPath, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as { kind?: string; payload?: unknown })
    .filter((entry): entry is AgentGenerationEvent => entry.kind?.startsWith('agent_generation_') === true);
}

function injectStartedGenerationAppendFailure(dispatcher: ClaudeAgentDispatcher, role: 'coder' | 'reviewer'): Error {
  const failure = new Error(`injected ${role} started-event append failure`);
  const internal = dispatcher as unknown as {
    appendGenerationEvent(kind: AgentGenerationEventKind, generation: PhysicalAgentGeneration): void;
  };
  const appendGenerationEvent = internal.appendGenerationEvent.bind(dispatcher);
  let pending = true;
  vi.spyOn(internal, 'appendGenerationEvent').mockImplementation((kind, generation) => {
    if (pending && kind === 'agent_generation_started' && generation.role === role) {
      pending = false;
      throw failure;
    }
    appendGenerationEvent(kind, generation);
  });
  return failure;
}

describe('ClaudeAgentDispatcher — generation-fenced agent leases', () => {
  const state = {} as AutoloopState;

  it('rejects an opaque dispatcher owner before it can be used as a release claimant', () => {
    expect(() => makeDispatcher({ ownerInstanceId: 'opaque-dispatcher-owner' })).toThrow(
      expect.objectContaining({
        code: 'AUTOLOOP_AGENT_RELEASE_OWNER_INVALID',
        retryable: false,
      }),
    );
  });

  it('persists a generation reservation before creating the physical session', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
      agentLeaseMs: 60_000,
    });
    calls.startSession.mockImplementationOnce(async (config: { name: string }) => {
      expect(readGenerationEvents(ledgerDir)).toMatchObject([
        {
          kind: 'agent_generation_reserved',
          payload: {
            role: 'planner',
            generation: 1,
            session_name: config.name,
            owner_instance_id: TEST_OWNER_INSTANCE_ID,
            created_at: AGENT_NOW,
            lease_expires_at: '2026-09-05T12:01:00.000Z',
            state: 'stale',
          },
        },
      ]);
      return { name: config.name, state: 'ready' };
    });

    await dispatcher.init(state);

    expect(readGenerationEvents(ledgerDir).map((entry) => entry.kind)).toEqual([
      'agent_generation_reserved',
      'agent_generation_started',
    ]);
  });

  it('rolls back an uncommitted reservation when the reservation ledger append fails', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const generationsPath = path.join(ledgerDir, 'agent-generations.jsonl');
    const reserveAgentGeneration = calls.reserveAgentGeneration.getMockImplementation()!;
    calls.reserveAgentGeneration.mockImplementationOnce((generation: PhysicalAgentGeneration) => {
      const reserved = reserveAgentGeneration(generation);
      fs.mkdirSync(generationsPath, { recursive: true });
      return reserved;
    });

    await expect(dispatcher.init(state)).rejects.toThrow();
    fs.rmSync(generationsPath, { recursive: true });

    expect(calls.releaseReservation).toHaveBeenCalledWith(
      'autoloop-r1-planner',
      1,
      expect.objectContaining({
        expectedOwnerInstanceId: TEST_OWNER_INSTANCE_ID,
        expectedSessionId: expect.any(String),
        rollbackUncommittedReservation: true,
      }),
    );
    expect(reservations.has('autoloop-r1-planner')).toBe(false);

    const retry = new ClaudeAgentDispatcher({ ...dispatcher.config });
    await retry.init(state);

    expect(readGenerationEvents(ledgerDir).map((entry) => [entry.kind, entry.payload.generation])).toEqual([
      ['agent_generation_reserved', 1],
      ['agent_generation_started', 1],
    ]);
  });

  it('surfaces a typed rollback postcondition failure when append and rollback persistence both fail', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const generationsPath = path.join(ledgerDir, 'agent-generations.jsonl');
    const reserveAgentGeneration = calls.reserveAgentGeneration.getMockImplementation()!;
    calls.reserveAgentGeneration.mockImplementationOnce((generation: PhysicalAgentGeneration) => {
      const reserved = reserveAgentGeneration(generation);
      fs.mkdirSync(generationsPath, { recursive: true });
      return reserved;
    });
    const releaseReservation = calls.releaseReservation.getMockImplementation()!;
    calls.releaseReservation.mockImplementation(
      async (name: string, generation: number, options: AgentReservationReleaseOptions) => {
        if (options.rollbackUncommittedReservation) return false;
        return await releaseReservation(name, generation, options);
      },
    );

    let failure: unknown;
    try {
      await dispatcher.init(state);
    } catch (err) {
      failure = err;
    }
    fs.rmSync(generationsPath, { recursive: true });

    expect(failure).toMatchObject({
      name: 'AutoloopAgentConflictError',
      code: 'AUTOLOOP_AGENT_ROLLBACK_POSTCONDITION_FAILED',
      cause: expect.objectContaining({ code: 'EISDIR' }),
    });
    expect((failure as Error).message).toContain('could not roll back uncommitted generation 1');
    expect(reservations.has('autoloop-r1-planner')).toBe(true);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(readGenerationEvents(ledgerDir)).toEqual([]);
  });

  it('retains the append cause and rollback context when rollback release throws', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const generationsPath = path.join(ledgerDir, 'agent-generations.jsonl');
    const reserveAgentGeneration = calls.reserveAgentGeneration.getMockImplementation()!;
    calls.reserveAgentGeneration.mockImplementationOnce((generation: PhysicalAgentGeneration) => {
      const reserved = reserveAgentGeneration(generation);
      fs.mkdirSync(generationsPath, { recursive: true });
      return reserved;
    });
    const releaseReservation = calls.releaseReservation.getMockImplementation()!;
    calls.releaseReservation.mockImplementation(
      async (name: string, generation: number, options: AgentReservationReleaseOptions) => {
        if (options.rollbackUncommittedReservation) {
          throw Object.assign(new Error('rollback registry unavailable'), {
            code: 'AUTOLOOP_AGENT_REGISTRY_PERSIST_FAILED',
          });
        }
        return await releaseReservation(name, generation, options);
      },
    );

    let failure: unknown;
    try {
      await dispatcher.init(state);
    } catch (err) {
      failure = err;
    }
    fs.rmSync(generationsPath, { recursive: true });

    expect(failure).toMatchObject({
      name: 'AutoloopAgentConflictError',
      code: 'AUTOLOOP_AGENT_ROLLBACK_POSTCONDITION_FAILED',
      cause: expect.objectContaining({ code: 'EISDIR' }),
    });
    expect((failure as Error).message).toContain('rollback failed with rollback registry unavailable');
    expect(reservations.has('autoloop-r1-planner')).toBe(true);
    expect(calls.startSession).not.toHaveBeenCalled();
  });

  it('propagates registry operational failure instead of reporting a generation conflict', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    calls.reserveAgentGeneration.mockImplementationOnce(() => {
      throw Object.assign(new Error('shared registry lock unavailable'), {
        code: 'AUTOLOOP_AGENT_REGISTRY_LOCK_CONTENDED',
        retryable: true,
      });
    });

    await expect(dispatcher.init(state)).rejects.toMatchObject({
      code: 'AUTOLOOP_AGENT_REGISTRY_LOCK_CONTENDED',
      retryable: true,
    });
    expect(readGenerationEvents(ledgerDir)).toEqual([]);
    expect(calls.startSession).not.toHaveBeenCalled();
  });

  it('keeps a genuinely live conflicting owner as a typed hard stop', async () => {
    const { dispatcher, calls, ledgerDir, activeNames, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const current = physicalGeneration('planner');
    appendGenerationEvent(ledgerDir, 'agent_generation_started', current);
    reservations.set(current.session_name, current);
    activeNames.add(current.session_name);

    await expect(dispatcher.init(state)).rejects.toMatchObject({ code: 'AUTOLOOP_AGENT_LIVE_CONFLICT' });
    expect(calls.releaseReservation).not.toHaveBeenCalled();
    expect(calls.startSession).not.toHaveBeenCalled();
  });

  it('keeps unknown runtime liveness as a typed hard stop', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const current = physicalGeneration('planner');
    appendGenerationEvent(ledgerDir, 'agent_generation_started', current);
    reservations.set(current.session_name, current);
    calls.inspect.mockResolvedValueOnce('unknown');

    await expect(dispatcher.init(state)).rejects.toMatchObject({ code: 'AUTOLOOP_AGENT_LIVENESS_UNKNOWN' });
    expect(calls.releaseReservation).not.toHaveBeenCalled();
    expect(calls.startSession).not.toHaveBeenCalled();
  });

  it('does not reclaim an absent owner until its lease has expired', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const current = physicalGeneration('planner', {
      lease_expires_at: '2026-09-05T12:05:00.000Z',
    });
    appendGenerationEvent(ledgerDir, 'agent_generation_started', current);
    reservations.set(current.session_name, current);

    await expect(dispatcher.init(state)).rejects.toMatchObject({ code: 'AUTOLOOP_AGENT_LEASE_ACTIVE' });
    expect(calls.releaseReservation).not.toHaveBeenCalled();
    expect(calls.startSession).not.toHaveBeenCalled();
  });

  it('reclaims an expired dead owner and appends orphan and release evidence before name reuse', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
      agentLeaseMs: 60_000,
    });
    const current = physicalGeneration('planner');
    const competing = physicalGeneration('planner', {
      generation: 2,
      session_id: 'planner-competing-2',
      owner_instance_id: 'owner-competing',
    });
    appendGenerationEvent(ledgerDir, 'agent_generation_started', current);
    reservations.set(current.session_name, current);
    const releaseReservation = calls.releaseReservation.getMockImplementation()!;
    let competingReservation: boolean | undefined;
    calls.releaseReservation.mockImplementationOnce(
      async (name, generation, options: AgentReservationReleaseOptions) => {
        if (!options.persistReleaseEvidence) {
          // Pre-fix behavior released the registry here, then appended release
          // evidence after this call returned.
          reservations.delete(name);
          competingReservation = calls.reserveAgentGeneration(competing);
          expect(competingReservation).toBe(false);
          return true;
        }
        expect(options.persistReleaseEvidence).toBeTypeOf('function');
        return await releaseReservation(name, generation, {
          ...options,
          beforeRelease: () => {
            options.beforeRelease?.();
            expect(readGenerationEvents(ledgerDir).map((entry) => entry.kind)).toEqual([
              'agent_generation_started',
              'agent_generation_orphaned',
            ]);
          },
          persistReleaseEvidence: () => {
            expect(readGenerationEvents(ledgerDir).map((entry) => entry.kind)).toEqual([
              'agent_generation_started',
              'agent_generation_orphaned',
            ]);
            competingReservation = calls.reserveAgentGeneration(competing);
            options.persistReleaseEvidence?.();
          },
        });
      },
    );

    await dispatcher.init(state);

    expect(competingReservation).toBe(false);
    expect(calls.releaseReservation).toHaveBeenCalledWith(
      current.session_name,
      1,
      expect.objectContaining({
        expectedOwnerInstanceId: current.owner_instance_id,
        expectedSessionId: current.session_id,
        releaseOwnerInstanceId: TEST_OWNER_INSTANCE_ID,
        beforeRelease: expect.any(Function),
        persistReleaseEvidence: expect.any(Function),
      }),
    );
    const events = readGenerationEvents(ledgerDir);
    expect(events.map((entry) => entry.kind)).toEqual([
      'agent_generation_started',
      'agent_generation_orphaned',
      'agent_generation_released',
      'agent_generation_reserved',
      'agent_generation_started',
    ]);
    expect(events.map((entry) => entry.payload.generation)).toEqual([1, 1, 1, 2, 2]);
    expect(events[3].payload).toMatchObject({ owner_instance_id: TEST_OWNER_INSTANCE_ID, state: 'stale' });
  });

  it('reclaims a stale registry-only legacy reservation before generation one starts', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const legacy = physicalGeneration('planner', {
      generation: 0,
      session_id: 'legacy-registry-only',
      owner_instance_id: 'legacy-owner',
      state: 'stale',
    });
    reservations.set(legacy.session_name, legacy);
    const competing = physicalGeneration('planner', {
      generation: 1,
      session_id: 'planner-competing-1',
      owner_instance_id: 'owner-competing',
    });
    const releaseReservation = calls.releaseReservation.getMockImplementation()!;
    let competingReservation: boolean | undefined;
    calls.releaseReservation.mockImplementationOnce(
      async (name, generation, options: AgentReservationReleaseOptions) => {
        if (!options.persistReleaseEvidence) {
          // The legacy pre-fix path entered registry release before orphan
          // evidence existed; assert at that actual call boundary.
          expect(readGenerationEvents(ledgerDir).map((entry) => entry.kind)).toEqual(['agent_generation_orphaned']);
          return true;
        }
        expect(options.persistReleaseEvidence).toBeTypeOf('function');
        return await releaseReservation(name, generation, {
          ...options,
          beforeRelease: () => {
            options.beforeRelease?.();
            expect(readGenerationEvents(ledgerDir).map((entry) => entry.kind)).toEqual(['agent_generation_orphaned']);
          },
          persistReleaseEvidence: () => {
            expect(readGenerationEvents(ledgerDir).map((entry) => entry.kind)).toEqual(['agent_generation_orphaned']);
            competingReservation = calls.reserveAgentGeneration(competing);
            options.persistReleaseEvidence?.();
          },
        });
      },
    );

    await dispatcher.init(state);

    expect(competingReservation).toBe(false);
    expect(calls.releaseReservation).toHaveBeenCalledWith(
      legacy.session_name,
      0,
      expect.objectContaining({
        expectedOwnerInstanceId: 'legacy-registry',
        releaseOwnerInstanceId: TEST_OWNER_INSTANCE_ID,
        beforeRelease: expect.any(Function),
        persistReleaseEvidence: expect.any(Function),
      }),
    );
    expect(readGenerationEvents(ledgerDir).map((entry) => [entry.kind, entry.payload.generation])).toEqual([
      ['agent_generation_orphaned', 0],
      ['agent_generation_released', 0],
      ['agent_generation_reserved', 1],
      ['agent_generation_started', 1],
    ]);
  });

  it('rejects cleanup when the runtime reservation no longer matches the durable generation', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const durable = physicalGeneration('planner');
    const replacement = physicalGeneration('planner', {
      generation: 2,
      session_id: 'planner-physical-2',
      owner_instance_id: 'owner-replacement',
    });
    appendGenerationEvent(ledgerDir, 'agent_generation_started', durable);
    reservations.set(replacement.session_name, replacement);
    calls.inspect.mockResolvedValueOnce('absent');

    await expect(dispatcher.init(state)).rejects.toMatchObject({ code: 'AUTOLOOP_AGENT_GENERATION_CONFLICT' });
    expect(calls.releaseReservation).toHaveBeenCalledTimes(1);
    expect(readGenerationEvents(ledgerDir)).toHaveLength(1);
  });

  it('does not append release evidence when the registry cannot durably prepare the release', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const current = physicalGeneration('planner');
    appendGenerationEvent(ledgerDir, 'agent_generation_started', current);
    reservations.set(current.session_name, current);
    const before = structuredClone(reservations.get(current.session_name));
    let orphanCallbackProvided = false;
    calls.releaseReservation.mockImplementationOnce(
      async (_name, _generation, options: AgentReservationReleaseOptions) => {
        orphanCallbackProvided = options.beforeRelease !== undefined;
        return false;
      },
    );

    await expect(dispatcher.init(state)).rejects.toMatchObject({ code: 'AUTOLOOP_AGENT_GENERATION_CONFLICT' });

    expect(orphanCallbackProvided).toBe(true);
    expect(readGenerationEvents(ledgerDir).map((entry) => entry.kind)).toEqual(['agent_generation_started']);
    expect(reservations.get(current.session_name)).toEqual(before);
    expect(calls.reserveAgentGeneration).not.toHaveBeenCalled();
    expect(calls.startSession).not.toHaveBeenCalled();
  });

  it('retries a crash after registry preparation without duplicate evidence or a skipped generation', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const current = physicalGeneration('planner');
    appendGenerationEvent(ledgerDir, 'agent_generation_started', current);
    reservations.set(current.session_name, current);
    const generationsPath = path.join(ledgerDir, 'agent-generations.jsonl');
    const savedGenerationsPath = path.join(ledgerDir, 'agent-generations.saved');
    const releaseReservation = calls.releaseReservation.getMockImplementation()!;
    let interruptReleaseEvidence = true;
    calls.releaseReservation.mockImplementation(async (name, generation, options: AgentReservationReleaseOptions) => {
      return await releaseReservation(name, generation, {
        ...options,
        persistReleaseEvidence: () => {
          if (interruptReleaseEvidence) {
            interruptReleaseEvidence = false;
            fs.renameSync(generationsPath, savedGenerationsPath);
            fs.mkdirSync(generationsPath);
          }
          options.persistReleaseEvidence?.();
        },
      });
    });

    await expect(dispatcher.init(state)).rejects.toThrow();
    fs.rmSync(generationsPath, { recursive: true });
    fs.renameSync(savedGenerationsPath, generationsPath);
    expect(readGenerationEvents(ledgerDir).map((entry) => entry.kind)).toEqual([
      'agent_generation_started',
      'agent_generation_orphaned',
    ]);

    const retry = new ClaudeAgentDispatcher({ ...dispatcher.config });
    await retry.init(state);

    const events = readGenerationEvents(ledgerDir);
    expect(events.filter((entry) => entry.kind === 'agent_generation_orphaned')).toHaveLength(1);
    expect(events.filter((entry) => entry.kind === 'agent_generation_released')).toHaveLength(1);
    expect(
      events.filter((entry) => entry.kind === 'agent_generation_reserved').map((entry) => entry.payload.generation),
    ).toEqual([2]);
    expect(calls.releaseReservation).toHaveBeenCalledTimes(2);
  });

  it('coalesces two concurrent recoverers into one compare-and-release and one replacement', async () => {
    const { dispatcher, calls, ledgerDir, reservations } = makeDispatcher({
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      now: () => new Date(AGENT_NOW),
    });
    const competingDispatcher = new ClaudeAgentDispatcher({ ...dispatcher.config });
    const current = physicalGeneration('planner');
    appendGenerationEvent(ledgerDir, 'agent_generation_started', current);
    reservations.set(current.session_name, current);

    const results = await Promise.all([dispatcher.init(state), competingDispatcher.init(state)]);

    expect(results).toEqual([undefined, undefined]);
    expect(calls.releaseReservation).toHaveBeenCalledTimes(1);
    expect(
      calls.reserveAgentGeneration.mock.calls.filter(
        ([candidate]) => (candidate as PhysicalAgentGeneration).generation === 2,
      ),
    ).toHaveLength(1);
    expect(calls.startSession).toHaveBeenCalledTimes(1);
    expect(readGenerationEvents(ledgerDir).filter((entry) => entry.kind === 'agent_generation_released')).toHaveLength(
      1,
    );
    expect(readGenerationEvents(ledgerDir).filter((entry) => entry.kind === 'agent_generation_orphaned')).toHaveLength(
      1,
    );
  });
});

describe('ClaudeAgentDispatcher — role engine configuration', () => {
  it('spawnReviewer starts only a Reviewer generation', async () => {
    const { dispatcher, calls } = makeDispatcher();

    const generation = await (
      dispatcher as unknown as {
        spawnReviewer(args?: Record<string, unknown>): Promise<PhysicalAgentGeneration>;
      }
    ).spawnReviewer({ reviewer_engine: 'gemini', reviewer_model: 'gemini-review' });

    expect(generation).toMatchObject({ role: 'reviewer', state: 'live' });
    expect(calls.startSession.mock.calls.map(([config]) => (config as { name: string }).name)).toEqual([
      'autoloop-r1-reviewer',
    ]);
    expect(findStart(calls, 'reviewer')).toMatchObject({ engine: 'gemini', model: 'gemini-review' });
    expect(calls.startSession.mock.calls.some(([config]) => (config as { name: string }).name.endsWith('-coder'))).toBe(
      false,
    );
  });

  it('spawnCoder starts only a Coder generation', async () => {
    const { dispatcher, calls } = makeDispatcher();

    const generation = await (
      dispatcher as unknown as {
        spawnCoder(args?: Record<string, unknown>): Promise<PhysicalAgentGeneration>;
      }
    ).spawnCoder({ coder_engine: 'codex', coder_model: 'gpt-coder' });

    expect(generation).toMatchObject({ role: 'coder', state: 'live' });
    expect(calls.startSession.mock.calls.map(([config]) => (config as { name: string }).name)).toEqual([
      'autoloop-r1-coder',
    ]);
    expect(findStart(calls, 'coder')).toMatchObject({ engine: 'codex', model: 'gpt-coder' });
    expect(
      calls.startSession.mock.calls.some(([config]) => (config as { name: string }).name.endsWith('-reviewer')),
    ).toBe(false);
  });

  it('makes a repeated standalone Coder spawn an idempotent lifecycle no-op', async () => {
    const onSpawnSubagentsCommitted = vi.fn();
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ onSpawnSubagentsCommitted });

    const first = await dispatcher.spawnCoder({ coder_engine: 'codex', coder_model: 'gpt-coder' });
    const duplicate = await dispatcher.spawnCoder({ coder_engine: 'codex', coder_model: 'gpt-coder' });

    expect(duplicate).toEqual(first);
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-coder')),
    ).toHaveLength(1);
    expect(decisionRows(ledgerDir, 'spawn_coder')).toHaveLength(1);
    expect(onSpawnSubagentsCommitted).toHaveBeenCalledOnce();
  });

  it('makes a repeated standalone Reviewer spawn an idempotent lifecycle no-op', async () => {
    const onSpawnSubagentsCommitted = vi.fn();
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ onSpawnSubagentsCommitted });

    const first = await dispatcher.spawnReviewer({ reviewer_engine: 'gemini', reviewer_model: 'gemini-review' });
    const duplicate = await dispatcher.spawnReviewer({ reviewer_engine: 'gemini', reviewer_model: 'gemini-review' });

    expect(duplicate).toEqual(first);
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-reviewer')),
    ).toHaveLength(1);
    expect(decisionRows(ledgerDir, 'spawn_reviewer')).toHaveLength(1);
    expect(onSpawnSubagentsCommitted).toHaveBeenCalledOnce();
  });

  it('keeps the legacy Claude model defaults when no role overrides are provided', async () => {
    const { dispatcher, calls } = makeDispatcher({}, { sendOutput: 'Planner reply' });

    await dispatcher.deliver(Msg.chat(0, { text: 'hello' }));
    await dispatcher.spawnSubagents();

    expect(findStart(calls, 'planner')).toMatchObject({ engine: 'claude', model: 'opus' });
    expect(findStart(calls, 'coder')).toMatchObject({ engine: 'claude', model: 'sonnet' });
    expect(findStart(calls, 'reviewer')).toMatchObject({ engine: 'claude', model: 'sonnet' });
  });

  it('uses each non-Claude engine without injecting a Claude model default', async () => {
    const { dispatcher, calls } = makeDispatcher(
      {
        plannerEngine: 'codex',
        coderEngine: 'gemini',
        reviewerEngine: 'opencode',
      },
      { sendOutput: 'Planner reply' },
    );

    await dispatcher.deliver(Msg.chat(0, { text: 'hello' }));
    await dispatcher.spawnSubagents();

    for (const [role, engine] of [
      ['planner', 'codex'],
      ['coder', 'gemini'],
      ['reviewer', 'opencode'],
    ] as const) {
      const start = findStart(calls, role);
      expect(start.engine).toBe(engine);
      expect(start).toHaveProperty('model', undefined);
    }
  });

  it('delivers the Planner protocol in-band and starts non-Claude Planners read-only', async () => {
    const { dispatcher, calls } = makeDispatcher({ plannerEngine: 'codex' }, { sendOutput: 'Planner reply' });

    await dispatcher.deliver(Msg.chat(0, { text: 'inspect this repository' }));

    expect(findStart(calls, 'planner')).toMatchObject({
      permissionMode: 'manual',
      sandboxMode: 'read-only',
    });
    const prompt = calls.sendMessage.mock.calls[0][1] as string;
    expect(prompt).toContain('<autoloop_role_instructions>');
    expect(prompt).toContain('Planner');
    expect(prompt).toContain('inspect this repository');
  });

  it('replays prior Planner chat for one-shot engines without native conversation resume', async () => {
    const { dispatcher, calls } = makeDispatcher(
      { plannerEngine: 'gemini' },
      { sendOutputs: ['FIRST_PLANNER_REPLY', 'SECOND_PLANNER_REPLY'] },
    );

    await dispatcher.deliver(Msg.chat(0, { text: 'Remember plan ORCHID and option B.' }));
    await dispatcher.deliver(Msg.chat(0, { text: 'Continue with the plan.' }));

    const secondPrompt = calls.sendMessage.mock.calls[1][1] as string;
    expect(secondPrompt).toContain('<conversation_history>');
    expect(secondPrompt).toContain('Remember plan ORCHID and option B.');
    expect(secondPrompt).toContain('FIRST_PLANNER_REPLY');
    expect(secondPrompt).toContain('Continue with the plan.');
  });

  it('delivers Coder and Reviewer protocols in-band for non-Claude engines', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({
      coderEngine: 'codex',
      reviewerEngine: 'gemini',
    });
    await dispatcher.spawnSubagents();

    await dispatcher.deliver(
      Msg.directive(0, {
        goal: 'change one file',
        constraints: [],
        success_criteria: [],
        max_attempts: 1,
      }),
    );
    ensureCompleteReviewArtifacts(dispatcher, 0);
    await dispatcher.deliver(
      Msg.reviewRequest(0, {
        iter: 0,
        ledger_path: ledgerDir,
        prior_metrics: [],
      }),
    );

    const coderPrompt = calls.sendMessage.mock.calls[0][1] as string;
    const reviewerPrompt = calls.sendMessage.mock.calls[1][1] as string;
    expect(coderPrompt).toContain('<autoloop_role_instructions>');
    expect(coderPrompt).toContain('Coder');
    expect(coderPrompt).toContain('change one file');
    expect(reviewerPrompt).toContain('<autoloop_role_instructions>');
    expect(reviewerPrompt).toContain('Reviewer');
    expect(reviewerPrompt).toContain('[review_request iter=0]');
  });

  it('passes explicit role models and custom engine configs through to startSession', async () => {
    const plannerCustomEngine = {
      name: 'planner-cli',
      bin: 'planner-cli',
      args: {},
      env: { TEST_TOKEN: 'planner-secret-sentinel' },
    };
    const coderCustomEngine = { name: 'coder-cli', bin: 'coder-cli', args: {} };
    const reviewerCustomEngine = { name: 'reviewer-cli', bin: 'reviewer-cli', args: {} };
    const { dispatcher, calls } = makeDispatcher(
      {
        plannerEngine: 'custom',
        plannerModel: 'planner-model',
        plannerCustomEngine,
        coderEngine: 'custom',
        coderModel: 'coder-model',
        coderCustomEngine,
        reviewerEngine: 'custom',
        reviewerModel: 'reviewer-model',
        reviewerCustomEngine,
      },
      { sendOutput: 'Planner reply' },
    );

    await dispatcher.deliver(Msg.chat(0, { text: 'hello' }));
    await dispatcher.spawnSubagents();

    expect(findStart(calls, 'planner')).toMatchObject({
      engine: 'custom',
      model: 'planner-model',
      customEngine: plannerCustomEngine,
    });
    expect(findStart(calls, 'coder')).toMatchObject({
      engine: 'custom',
      model: 'coder-model',
      customEngine: coderCustomEngine,
    });
    expect(findStart(calls, 'reviewer')).toMatchObject({
      engine: 'custom',
      model: 'reviewer-model',
      customEngine: reviewerCustomEngine,
    });
  });

  it('rejects a custom role before startSession when its trusted config is missing', async () => {
    const { dispatcher, calls } = makeDispatcher({ plannerEngine: 'custom' });

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'hello' }))).rejects.toThrow(
      'Planner custom engine config is required',
    );
    expect(calls.startSession).not.toHaveBeenCalled();
  });

  it('applies spawn engine overrides and recomputes implicit model defaults', async () => {
    const { dispatcher, calls } = makeDispatcher({ reviewerEngine: 'gemini', reviewerModel: 'gemini-explicit' });

    await dispatcher.spawnSubagents({ coder_engine: 'codex' });

    expect(findStart(calls, 'coder')).toHaveProperty('model', undefined);
    expect(findStart(calls, 'coder').engine).toBe('codex');
    expect(findStart(calls, 'reviewer')).toMatchObject({ engine: 'gemini', model: 'gemini-explicit' });
  });

  it('drops a prior model when spawn changes only the engine', async () => {
    const { dispatcher, calls } = makeDispatcher({
      coderEngine: 'claude',
      coderModel: 'claude-specific-model',
    });

    await dispatcher.spawnSubagents({ coder_engine: 'codex' });

    expect(findStart(calls, 'coder')).toMatchObject({ engine: 'codex', model: undefined });
  });

  it('uses the current role engine when eagerly resetting a spawned subagent', async () => {
    const { dispatcher, calls } = makeDispatcher();
    await dispatcher.spawnSubagents({ coder_engine: 'codex', coder_model: 'gpt-coder' });

    await dispatcher.resetAgent('coder', { eagerRestart: true });

    const coderStarts = calls.startSession.mock.calls
      .map((entry) => entry[0] as Record<string, unknown>)
      .filter((config) => config.name === 'autoloop-r1-coder');
    expect(coderStarts).toHaveLength(2);
    expect(coderStarts[1]).toMatchObject({ engine: 'codex', model: 'gpt-coder' });
  });

  it('rejects engine changes after a subagent session has started', async () => {
    const { dispatcher, calls } = makeDispatcher();
    await dispatcher.spawnSubagents();

    await expect(dispatcher.spawnSubagents({ coder_engine: 'codex' })).rejects.toThrow(
      'Cannot change Coder engine or model after its session has started',
    );

    await dispatcher.resetAgent('coder', { eagerRestart: true });
    const coderStarts = calls.startSession.mock.calls
      .map((entry) => entry[0] as Record<string, unknown>)
      .filter((config) => config.name === 'autoloop-r1-coder');
    expect(coderStarts).toHaveLength(2);
    expect(coderStarts[1]).toMatchObject({ engine: 'claude', model: 'sonnet' });
  });

  it('commits the Runner lifecycle exactly once for each independent spawn effect', async () => {
    const onSpawnSubagentsCommitted = vi.fn();
    const { dispatcher } = makeDispatcher({ onSpawnSubagentsCommitted });

    await dispatcher.spawnCoder();
    expect(onSpawnSubagentsCommitted).toHaveBeenCalledTimes(1);
    await dispatcher.spawnReviewer();
    expect(onSpawnSubagentsCommitted).toHaveBeenCalledTimes(2);
  });

  it('commits the Runner lifecycle exactly once for the joint compatibility wrapper', async () => {
    const onSpawnSubagentsCommitted = vi.fn();
    const { dispatcher } = makeDispatcher({ onSpawnSubagentsCommitted });

    await dispatcher.spawnSubagents();

    expect(onSpawnSubagentsCommitted).toHaveBeenCalledTimes(1);
  });

  it('orders configured nested compatibility lifecycle as spawn-start, spawn-finish, then one commit', async () => {
    const order: string[] = [];
    const dispatcherRef: { current?: ClaudeAgentDispatcher } = {};
    const configured = makeDispatcher(
      {
        onSpawnSubagents: async (args) => {
          order.push('spawn-start');
          await dispatcherRef.current!.spawnSubagents(args);
          order.push('spawn-finish');
        },
        onSpawnSubagentsCommitted: () => {
          order.push('mark-committed');
        },
      },
      {
        sendOutput: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
      },
    );
    const dispatcher = configured.dispatcher;
    dispatcherRef.current = dispatcher;

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'start subagents' }))).resolves.toEqual([]);

    expect(order).toEqual(['spawn-start', 'spawn-finish', 'mark-committed']);
  });

  it('suppresses the configured compatibility commit when the run becomes terminal during its handler', async () => {
    const order: string[] = [];
    const dispatcherRef: { current?: ClaudeAgentDispatcher } = {};
    const configured = makeDispatcher(
      {
        onSpawnSubagents: async () => {
          order.push('spawn-start');
          (dispatcherRef.current as unknown as { terminal: boolean }).terminal = true;
          order.push('spawn-finish');
        },
        onSpawnSubagentsCommitted: () => {
          order.push('mark-committed');
        },
      },
      {
        sendOutput: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
      },
    );
    const dispatcher = configured.dispatcher;
    dispatcherRef.current = dispatcher;

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'start subagents' }))).rejects.toMatchObject({
      code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
    });

    expect(order).toEqual(['spawn-start', 'spawn-finish']);
  });

  it('stops a newly started Coder when Reviewer startup fails', async () => {
    const { dispatcher, calls } = makeDispatcher({}, { startThrowsFor: 'reviewer' });

    await expect(dispatcher.spawnSubagents()).rejects.toThrow('reviewer failed to start');

    expect(calls.stopSession).toHaveBeenCalledWith('autoloop-r1-coder');
    calls.startSession.mockImplementation(async () => ({ name: 'x', state: 'ready' }));
    await dispatcher.spawnSubagents();
    expect(findStart(calls, 'coder')).toBeDefined();
    expect(findStart(calls, 'reviewer')).toBeDefined();
  });

  it('spawn_subagents makes only the legacy single rollback attempt when stopping a new Coder fails', async () => {
    const { dispatcher, calls } = makeDispatcher({}, { startThrowsFor: 'reviewer' });
    calls.stopSession.mockRejectedValue(new Error('rollback stop failed'));

    await expect(dispatcher.spawnSubagents()).rejects.toThrow('reviewer failed to start');

    expect(calls.stopSession).toHaveBeenCalledTimes(1);
    expect(calls.stopSession).toHaveBeenCalledWith('autoloop-r1-coder');
  });

  it('keeps a newly started Coder selection when direct rollback cannot stop its live session', async () => {
    const onRoleSelectionChanged = vi.fn();
    const { dispatcher, calls } = makeDispatcher({ onRoleSelectionChanged });
    const internal = dispatcher as unknown as {
      requireLiveGeneration(role: 'coder' | 'reviewer'): PhysicalAgentGeneration;
    };
    const requireLiveGeneration = internal.requireLiveGeneration.bind(dispatcher);
    let failLate = true;
    vi.spyOn(internal, 'requireLiveGeneration').mockImplementation((role) => {
      if (role === 'coder' && failLate) {
        failLate = false;
        throw new Error('coder generation unavailable after startup');
      }
      return requireLiveGeneration(role);
    });
    calls.stopSession.mockRejectedValue(new Error('rollback stop failed'));

    await expect(dispatcher.spawnCoder({ coder_engine: 'codex', coder_model: 'gpt-coder' })).rejects.toThrow(
      'coder generation unavailable after startup',
    );

    await expect(dispatcher.spawnCoder({ coder_engine: 'gemini' })).rejects.toThrow(
      'Cannot change Coder engine or model after its session has started',
    );
    await expect(dispatcher.spawnCoder({ coder_engine: 'codex', coder_model: 'gpt-coder' })).resolves.toMatchObject({
      role: 'coder',
      state: 'live',
    });
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-coder')),
    ).toHaveLength(1);
    expect(onRoleSelectionChanged).toHaveBeenCalledWith({
      coder: { engine: 'codex', model: 'gpt-coder' },
      reviewer: { engine: 'claude', model: undefined },
    });
  });

  it('keeps a newly started Reviewer selection and frozen prompt when direct rollback cannot stop it', async () => {
    const onRoleSelectionChanged = vi.fn();
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ onRoleSelectionChanged });
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.mkdirSync(sandbox, { recursive: true });
    fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'surviving reviewer memory\n');
    const internal = dispatcher as unknown as {
      requireLiveGeneration(role: 'coder' | 'reviewer'): PhysicalAgentGeneration;
      reviewerSessionPrompt: string | null;
    };
    const requireLiveGeneration = internal.requireLiveGeneration.bind(dispatcher);
    let failLate = true;
    vi.spyOn(internal, 'requireLiveGeneration').mockImplementation((role) => {
      if (role === 'reviewer' && failLate) {
        failLate = false;
        throw new Error('reviewer generation unavailable after startup');
      }
      return requireLiveGeneration(role);
    });
    calls.stopSession.mockRejectedValue(new Error('rollback stop failed'));

    await expect(
      dispatcher.spawnReviewer({ reviewer_engine: 'gemini', reviewer_model: 'gemini-review' }),
    ).rejects.toThrow('reviewer generation unavailable after startup');

    expect(internal.reviewerSessionPrompt).toContain('surviving reviewer memory');
    await expect(dispatcher.spawnReviewer({ reviewer_engine: 'cursor' })).rejects.toThrow(
      'Cannot change Reviewer engine or model after its session has started',
    );
    await expect(
      dispatcher.spawnReviewer({ reviewer_engine: 'gemini', reviewer_model: 'gemini-review' }),
    ).resolves.toMatchObject({ role: 'reviewer', state: 'live' });
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-reviewer')),
    ).toHaveLength(1);
    expect(onRoleSelectionChanged).toHaveBeenCalledWith({
      coder: { engine: 'claude', model: undefined },
      reviewer: { engine: 'gemini', model: 'gemini-review' },
    });
  });

  it('keeps both new compatibility selections when late failure survives both rollback stops', async () => {
    const onRoleSelectionChanged = vi.fn();
    const { dispatcher, calls } = makeDispatcher({ onRoleSelectionChanged });
    const internal = dispatcher as unknown as {
      requireLiveGeneration(role: 'coder' | 'reviewer'): PhysicalAgentGeneration;
    };
    const requireLiveGeneration = internal.requireLiveGeneration.bind(dispatcher);
    let failReviewerLate = true;
    vi.spyOn(internal, 'requireLiveGeneration').mockImplementation((role) => {
      if (role === 'reviewer' && failReviewerLate) {
        failReviewerLate = false;
        throw new Error('reviewer generation unavailable after startup');
      }
      return requireLiveGeneration(role);
    });
    calls.stopSession.mockRejectedValue(new Error('rollback stop failed'));
    const selected = {
      coder_engine: 'codex' as const,
      coder_model: 'gpt-coder',
      reviewer_engine: 'gemini' as const,
      reviewer_model: 'gemini-review',
    };

    await expect(dispatcher.spawnSubagents(selected)).rejects.toThrow('reviewer generation unavailable after startup');

    await expect(dispatcher.spawnSubagents({ ...selected, coder_engine: 'cursor' })).rejects.toThrow(
      'Cannot change Coder engine or model after its session has started',
    );
    await expect(dispatcher.spawnSubagents({ ...selected, reviewer_engine: 'cursor' })).rejects.toThrow(
      'Cannot change Reviewer engine or model after its session has started',
    );
    await expect(dispatcher.spawnSubagents(selected)).resolves.toBeUndefined();
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-coder')),
    ).toHaveLength(1);
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-reviewer')),
    ).toHaveLength(1);
    expect(onRoleSelectionChanged).toHaveBeenCalledWith({
      coder: { engine: 'codex', model: 'gpt-coder' },
      reviewer: { engine: 'gemini', model: 'gemini-review' },
    });
  });

  it('keeps outer rollback ownership when a nested primitive fails after startup and each stop attempt fails', async () => {
    const { dispatcher, calls } = makeDispatcher();
    const internal = dispatcher as unknown as {
      requireLiveGeneration(role: 'coder' | 'reviewer'): PhysicalAgentGeneration;
    };
    const requireLiveGeneration = internal.requireLiveGeneration.bind(dispatcher);
    vi.spyOn(internal, 'requireLiveGeneration').mockImplementation((role) => {
      if (role === 'reviewer') throw new Error('reviewer generation unavailable after startup');
      return requireLiveGeneration(role);
    });
    calls.stopSession.mockRejectedValue(new Error('rollback stop failed'));

    await expect(dispatcher.spawnSubagents()).rejects.toThrow('reviewer generation unavailable after startup');

    const stoppedNames = calls.stopSession.mock.calls.map(([name]) => name as string);
    expect(stoppedNames.filter((name) => name === 'autoloop-r1-coder')).toHaveLength(1);
    expect(stoppedNames.filter((name) => name === 'autoloop-r1-reviewer')).toHaveLength(1);
  });

  it('keeps the next Coder selection visible while compatibility rollback is still stopping it', async () => {
    const { dispatcher, calls, activeNames } = makeDispatcher();
    const internal = dispatcher as unknown as {
      requireLiveGeneration(role: 'coder' | 'reviewer'): PhysicalAgentGeneration;
      coderSelection: { engine: string; model?: string };
    };
    const requireLiveGeneration = internal.requireLiveGeneration.bind(dispatcher);
    let failCoderLate = true;
    vi.spyOn(internal, 'requireLiveGeneration').mockImplementation((role) => {
      if (role === 'coder' && failCoderLate) {
        failCoderLate = false;
        throw new Error('coder generation unavailable after startup');
      }
      return requireLiveGeneration(role);
    });

    let signalStopEntered!: () => void;
    const stopEntered = new Promise<void>((resolve) => {
      signalStopEntered = resolve;
    });
    let releaseStop!: () => void;
    const stopHeld = new Promise<void>((resolve) => {
      releaseStop = resolve;
    });
    calls.stopSession.mockImplementation(async (name: string) => {
      if (name === 'autoloop-r1-coder') {
        signalStopEntered();
        await stopHeld;
      }
      activeNames.delete(name);
    });

    const failedCompatibilitySpawn = dispatcher.spawnSubagents({
      coder_engine: 'codex',
      coder_model: 'gpt-coder',
    });
    await stopEntered;
    const selectionDuringStop = { ...internal.coderSelection };
    const concurrentOldSelection = await dispatcher
      .spawnCoder({ coder_engine: 'claude' })
      .then(() => undefined)
      .catch((error: unknown) => error);
    releaseStop();
    const compatibilityFailure = await failedCompatibilitySpawn.catch((error: unknown) => error);

    expect(selectionDuringStop).toEqual({ engine: 'codex', model: 'gpt-coder', customEngine: undefined });
    expect(concurrentOldSelection).toEqual(
      expect.objectContaining({ message: 'Cannot change Coder engine or model after its session has started' }),
    );
    expect(compatibilityFailure).toEqual(
      expect.objectContaining({ message: 'coder generation unavailable after startup' }),
    );
  });

  it('keeps the next Reviewer selection visible while compatibility rollback is still stopping it', async () => {
    const { dispatcher, calls, activeNames } = makeDispatcher();
    await dispatcher.spawnCoder();
    const internal = dispatcher as unknown as {
      requireLiveGeneration(role: 'coder' | 'reviewer'): PhysicalAgentGeneration;
      reviewerSelection: { engine: string; model?: string };
    };
    const requireLiveGeneration = internal.requireLiveGeneration.bind(dispatcher);
    let failReviewerLate = true;
    vi.spyOn(internal, 'requireLiveGeneration').mockImplementation((role) => {
      if (role === 'reviewer' && failReviewerLate) {
        failReviewerLate = false;
        throw new Error('reviewer generation unavailable after startup');
      }
      return requireLiveGeneration(role);
    });

    let signalStopEntered!: () => void;
    const stopEntered = new Promise<void>((resolve) => {
      signalStopEntered = resolve;
    });
    let releaseStop!: () => void;
    const stopHeld = new Promise<void>((resolve) => {
      releaseStop = resolve;
    });
    calls.stopSession.mockImplementation(async (name: string) => {
      if (name === 'autoloop-r1-reviewer') {
        signalStopEntered();
        await stopHeld;
      }
      activeNames.delete(name);
    });

    const failedCompatibilitySpawn = dispatcher.spawnSubagents({
      reviewer_engine: 'gemini',
      reviewer_model: 'gemini-review',
    });
    await stopEntered;
    const selectionDuringStop = { ...internal.reviewerSelection };
    const concurrentOldSelection = await dispatcher
      .spawnReviewer({ reviewer_engine: 'claude' })
      .then(() => undefined)
      .catch((error: unknown) => error);
    releaseStop();
    const compatibilityFailure = await failedCompatibilitySpawn.catch((error: unknown) => error);

    expect(selectionDuringStop).toEqual({ engine: 'gemini', model: 'gemini-review', customEngine: undefined });
    expect(concurrentOldSelection).toEqual(
      expect.objectContaining({ message: 'Cannot change Reviewer engine or model after its session has started' }),
    );
    expect(compatibilityFailure).toEqual(
      expect.objectContaining({ message: 'reviewer generation unavailable after startup' }),
    );
  });

  it('keeps direct Coder metadata bound when its started-event append fails and liveness is unknown', async () => {
    const onRoleSelectionChanged = vi.fn();
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ onRoleSelectionChanged });
    const internal = dispatcher as unknown as {
      coderStarted: boolean;
      coderSelection: { engine: string; model?: string };
    };
    const appendFailure = injectStartedGenerationAppendFailure(dispatcher, 'coder');
    calls.stopSession.mockRejectedValue(new Error('cleanup stop failed'));
    calls.inspect.mockResolvedValueOnce('absent').mockResolvedValue('unknown');
    const selected = { coder_engine: 'codex' as const, coder_model: 'gpt-coder' };

    const startupFailure = await dispatcher
      .spawnCoder(selected)
      .then(() => undefined)
      .catch((error: unknown) => error);
    const stateAfterFailure = {
      started: internal.coderStarted,
      selection: { ...internal.coderSelection },
      events: readGenerationEvents(ledgerDir).map((entry) => entry.kind),
      persistedSelections: structuredClone(onRoleSelectionChanged.mock.calls),
    };
    const retry = await dispatcher.spawnCoder(selected);
    const changedSelectionFailure = await dispatcher
      .spawnCoder({ coder_engine: 'gemini' })
      .then(() => undefined)
      .catch((error: unknown) => error);

    expect(startupFailure).toBe(appendFailure);
    expect(stateAfterFailure).toEqual({
      started: true,
      selection: { engine: 'codex', model: 'gpt-coder', customEngine: undefined },
      events: ['agent_generation_reserved', 'agent_generation_started'],
      persistedSelections: [
        [
          {
            coder: { engine: 'codex', model: 'gpt-coder' },
            reviewer: { engine: 'claude', model: undefined },
          },
        ],
      ],
    });
    expect(retry).toMatchObject({ role: 'coder', generation: 1, state: 'live' });
    expect(changedSelectionFailure).toEqual(
      expect.objectContaining({ message: 'Cannot change Coder engine or model after its session has started' }),
    );
    expect(
      calls.releaseReservation.mock.calls.filter(
        ([name, generation]) => name === 'autoloop-r1-coder' && generation === 1,
      ),
    ).toHaveLength(0);
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-coder')),
    ).toHaveLength(1);
  });

  it('keeps direct Reviewer metadata and frozen prompt when its started-event append fails live', async () => {
    const onRoleSelectionChanged = vi.fn();
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ onRoleSelectionChanged });
    const reviewerSandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.mkdirSync(reviewerSandbox, { recursive: true });
    fs.writeFileSync(path.join(reviewerSandbox, 'reviewer_memory.md'), 'started append survival memory\n');
    const internal = dispatcher as unknown as {
      reviewerStarted: boolean;
      reviewerSelection: { engine: string; model?: string };
      reviewerSessionPrompt: string | null;
    };
    const appendFailure = injectStartedGenerationAppendFailure(dispatcher, 'reviewer');
    calls.stopSession.mockRejectedValue(new Error('cleanup stop failed'));
    const selected = { reviewer_engine: 'gemini' as const, reviewer_model: 'gemini-review' };

    const startupFailure = await dispatcher
      .spawnReviewer(selected)
      .then(() => undefined)
      .catch((error: unknown) => error);
    const stateAfterFailure = {
      started: internal.reviewerStarted,
      selection: { ...internal.reviewerSelection },
      prompt: internal.reviewerSessionPrompt,
      events: readGenerationEvents(ledgerDir).map((entry) => entry.kind),
      persistedSelections: structuredClone(onRoleSelectionChanged.mock.calls),
    };
    const retry = await dispatcher.spawnReviewer(selected);
    const changedSelectionFailure = await dispatcher
      .spawnReviewer({ reviewer_engine: 'cursor' })
      .then(() => undefined)
      .catch((error: unknown) => error);

    expect(startupFailure).toBe(appendFailure);
    expect(stateAfterFailure).toMatchObject({
      started: true,
      selection: { engine: 'gemini', model: 'gemini-review', customEngine: undefined },
      events: ['agent_generation_reserved', 'agent_generation_started'],
      persistedSelections: [
        [
          {
            coder: { engine: 'claude', model: undefined },
            reviewer: { engine: 'gemini', model: 'gemini-review' },
          },
        ],
      ],
    });
    expect(stateAfterFailure.prompt).toContain('started append survival memory');
    expect(retry).toMatchObject({ role: 'reviewer', generation: 1, state: 'live' });
    expect(changedSelectionFailure).toEqual(
      expect.objectContaining({ message: 'Cannot change Reviewer engine or model after its session has started' }),
    );
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-reviewer')),
    ).toHaveLength(1);
  });

  it('keeps compatibility selections bound when Reviewer started-event append fails live', async () => {
    const onRoleSelectionChanged = vi.fn();
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ onRoleSelectionChanged });
    const internal = dispatcher as unknown as {
      coderStarted: boolean;
      reviewerStarted: boolean;
      coderSelection: { engine: string; model?: string };
      reviewerSelection: { engine: string; model?: string };
    };
    const appendFailure = injectStartedGenerationAppendFailure(dispatcher, 'reviewer');
    calls.stopSession.mockRejectedValue(new Error('cleanup stop failed'));
    const selected = {
      coder_engine: 'codex' as const,
      coder_model: 'gpt-coder',
      reviewer_engine: 'gemini' as const,
      reviewer_model: 'gemini-review',
    };

    const startupFailure = await dispatcher
      .spawnSubagents(selected)
      .then(() => undefined)
      .catch((error: unknown) => error);
    const stateAfterFailure = {
      coderStarted: internal.coderStarted,
      reviewerStarted: internal.reviewerStarted,
      coderSelection: { ...internal.coderSelection },
      reviewerSelection: { ...internal.reviewerSelection },
      events: readGenerationEvents(ledgerDir).map((entry) => [entry.payload.role, entry.kind]),
      persistedSelections: structuredClone(onRoleSelectionChanged.mock.calls),
    };
    await dispatcher.spawnSubagents(selected);
    const changedCoderFailure = await dispatcher
      .spawnSubagents({ ...selected, coder_engine: 'cursor' })
      .then(() => undefined)
      .catch((error: unknown) => error);
    const changedReviewerFailure = await dispatcher
      .spawnSubagents({ ...selected, reviewer_engine: 'cursor' })
      .then(() => undefined)
      .catch((error: unknown) => error);

    expect(startupFailure).toBe(appendFailure);
    expect(stateAfterFailure).toEqual({
      coderStarted: true,
      reviewerStarted: true,
      coderSelection: { engine: 'codex', model: 'gpt-coder', customEngine: undefined },
      reviewerSelection: { engine: 'gemini', model: 'gemini-review', customEngine: undefined },
      events: [
        ['coder', 'agent_generation_reserved'],
        ['coder', 'agent_generation_started'],
        ['reviewer', 'agent_generation_reserved'],
        ['reviewer', 'agent_generation_started'],
      ],
      persistedSelections: [
        [
          {
            coder: { engine: 'codex', model: 'gpt-coder' },
            reviewer: { engine: 'gemini', model: 'gemini-review' },
          },
        ],
      ],
    });
    expect(changedCoderFailure).toEqual(
      expect.objectContaining({ message: 'Cannot change Coder engine or model after its session has started' }),
    );
    expect(changedReviewerFailure).toEqual(
      expect.objectContaining({ message: 'Cannot change Reviewer engine or model after its session has started' }),
    );
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-coder')),
    ).toHaveLength(1);
    expect(
      calls.startSession.mock.calls.filter(([config]) => (config as { name: string }).name.endsWith('-reviewer')),
    ).toHaveLength(1);
  });

  it('keeps the started-event append failure primary when surviving-selection persistence also fails', async () => {
    const persistenceFailure = new Error('surviving selection persistence failed');
    const onRoleSelectionChanged = vi.fn().mockRejectedValue(persistenceFailure);
    const { dispatcher, calls } = makeDispatcher({ onRoleSelectionChanged });
    const appendFailure = injectStartedGenerationAppendFailure(dispatcher, 'coder');
    calls.stopSession.mockRejectedValue(new Error('cleanup stop failed'));

    const startupFailure = await dispatcher
      .spawnCoder({ coder_engine: 'codex', coder_model: 'gpt-coder' })
      .then(() => undefined)
      .catch((error: unknown) => error);

    expect(startupFailure).toBe(appendFailure);
    expect(onRoleSelectionChanged).toHaveBeenCalledWith({
      coder: { engine: 'codex', model: 'gpt-coder' },
      reviewer: { engine: 'claude', model: undefined },
    });
  });

  it('spawn_subagents rolls back only sessions started by that call and restores failed selections', async () => {
    const { dispatcher, calls } = makeDispatcher();
    await (
      dispatcher as unknown as {
        spawnCoder(args?: Record<string, unknown>): Promise<PhysicalAgentGeneration>;
      }
    ).spawnCoder();
    calls.startSession.mockImplementation(async (config: { name: string }) => {
      if (config.name.endsWith('-reviewer')) throw new Error('reviewer failed to start');
      return { name: config.name, state: 'ready' };
    });

    await expect(dispatcher.spawnSubagents({ reviewer_engine: 'gemini' })).rejects.toThrow('reviewer failed to start');

    expect(calls.stopSession).not.toHaveBeenCalledWith('autoloop-r1-coder');
    calls.startSession.mockImplementation(async (config: { name: string }) => ({ name: config.name, state: 'ready' }));
    await (
      dispatcher as unknown as {
        spawnReviewer(args?: Record<string, unknown>): Promise<PhysicalAgentGeneration>;
      }
    ).spawnReviewer();
    const reviewerStarts = calls.startSession.mock.calls
      .map(([config]) => config as Record<string, unknown>)
      .filter((config) => config.name === 'autoloop-r1-reviewer');
    expect(reviewerStarts.at(-1)).toMatchObject({ engine: 'claude', model: 'sonnet' });
  });

  it('spawn_subagents validates both selections before starting either independent primitive', async () => {
    const { dispatcher, calls } = makeDispatcher();

    await expect(dispatcher.spawnSubagents({ coder_engine: 'codex', reviewer_engine: 'custom' })).rejects.toThrow(
      'Reviewer custom engine config is required',
    );

    expect(calls.startSession).not.toHaveBeenCalled();
    await (
      dispatcher as unknown as {
        spawnCoder(args?: Record<string, unknown>): Promise<PhysicalAgentGeneration>;
      }
    ).spawnCoder();
    expect(findStart(calls, 'coder')).toMatchObject({ engine: 'claude', model: 'sonnet' });
  });

  it('rejects a combined independent lifecycle batch before state, then accepts idempotent standalone retries', async () => {
    const onSpawnSubagentsCommitted = vi.fn();
    const combined = [
      '```autoloop',
      JSON.stringify({ tool: 'spawn_coder', args: { coder_engine: 'codex', coder_model: 'gpt-coder' } }),
      '```',
      '```autoloop',
      JSON.stringify({
        tool: 'spawn_reviewer',
        args: { reviewer_engine: 'gemini', reviewer_model: 'gemini-review' },
      }),
      '```',
    ].join('\n');
    const standaloneCoder = [
      '```autoloop',
      JSON.stringify({ tool: 'spawn_coder', args: { coder_engine: 'codex', coder_model: 'gpt-coder' } }),
      '```',
    ].join('\n');
    const standaloneReviewer = [
      '```autoloop',
      JSON.stringify({
        tool: 'spawn_reviewer',
        args: { reviewer_engine: 'gemini', reviewer_model: 'gemini-review' },
      }),
      '```',
    ].join('\n');
    const { dispatcher, calls, ledgerDir, activeNames } = makeDispatcher(
      { onSpawnSubagentsCommitted },
      {
        sendOutputs: [combined, standaloneCoder, standaloneReviewer, standaloneCoder],
        startThrowsFor: 'reviewer',
      },
    );

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'start both independently' }))).rejects.toMatchObject({
      code: 'AUTOLOOP_CONTROL_MALFORMED',
    });
    expect([...activeNames].filter((name) => name.endsWith('-coder') || name.endsWith('-reviewer'))).toEqual([]);
    expect(decisionRows(ledgerDir, 'spawn_coder')).toEqual([]);
    expect(decisionRows(ledgerDir, 'spawn_reviewer')).toEqual([]);
    expect(onSpawnSubagentsCommitted).not.toHaveBeenCalled();

    calls.startSession.mockImplementation(async (config: { name: string }) => {
      activeNames.add(config.name);
      return { name: config.name, state: 'ready' };
    });
    await dispatcher.deliver(Msg.chat(0, { text: 'start Coder alone' }));
    await dispatcher.deliver(Msg.chat(0, { text: 'start Reviewer alone' }));
    await dispatcher.deliver(Msg.chat(0, { text: 'retry Coder alone' }));

    expect(
      calls.startSession.mock.calls
        .map(([config]) => (config as { name: string }).name)
        .filter((name) => name.endsWith('-coder') || name.endsWith('-reviewer')),
    ).toEqual(['autoloop-r1-coder', 'autoloop-r1-reviewer']);
    expect(decisionRows(ledgerDir, 'spawn_coder')).toHaveLength(1);
    expect(decisionRows(ledgerDir, 'spawn_reviewer')).toHaveLength(1);
    expect(onSpawnSubagentsCommitted).toHaveBeenCalledTimes(2);
  });
});

describe('ClaudeAgentDispatcher — Reviewer-only checkpoint requests', () => {
  const reviewerReply = [
    'Independent review complete.',
    '```autoloop',
    JSON.stringify({
      tool: 'review_complete',
      args: { decision: 'advance', metric: 1, audit_notes: 'existing checkpoint reviewed' },
    }),
    '```',
  ].join('\n');

  it('exposes checkpoint-specific preparation types without the deprecated delivery alias', () => {
    expectTypeOf<PreparedReviewRequest['payload']>().toEqualTypeOf<CheckpointReviewRequestPayload>();
    expectTypeOf<
      Awaited<ReturnType<ClaudeAgentDispatcher['requestReview']>>
    >().toEqualTypeOf<ReviewRequestPreparationResult>();
  });

  it('prepares source iter 3 into target iter 0, leaves local iter 3 free, and delivers only through the emitted message', async () => {
    const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    const sourceLedger = writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    sourceLedger.writeIterationArtifact(2, 'verdict.json', '{"source":"prior verdict must not be imported"}\n');
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['security', 'regression'],
      idempotency_key: 'review-source-run-3',
    };
    const api = dispatcher as unknown as {
      requestReview(
        input: typeof args,
        targetIter: number,
      ): Promise<{
        status: 'prepared' | 'duplicate';
        target: 'reviewer';
        idempotency_key: string;
        payload?: Parameters<typeof Msg.reviewRequest>[1];
      }>;
    };

    const first = await api.requestReview(args, 0);

    expect(first).toMatchObject({
      status: 'prepared',
      target: 'reviewer',
      idempotency_key: 'review-source-run-3',
      payload: { iter: 0, source_iter: 3, checkpoint_sha: checkpoint.sha },
    });
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'diff.patch'))).toEqual(checkpoint.patch);
    expect(fs.existsSync(path.join(ledgerDir, 'iter', '3'))).toBe(false);

    const replies = await dispatcher.deliver(Msg.reviewRequest(0, first.payload!));

    expect(replies).toHaveLength(1);
    expect(replies[0].type).toBe('review_verdict');
    expect(calls.startSession.mock.calls.map(([config]) => (config as { name: string }).name)).toEqual([
      'autoloop-r1-reviewer',
    ]);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
    expect(calls.sendMessage.mock.calls[0][1]).toBe(
      [
        '[review_request iter=0]',
        'Artifacts staged from run source-run iter 3 at: iter-0/ (directive.json, diff.patch, eval_output.json)',
        `checkpoint_sha: ${checkpoint.sha}`,
        'scope: ["security","regression"]',
        'prior_verdict: (none)',
        'prior_metrics: []',
        '',
        'Audit and emit `review_complete`.',
      ].join('\n'),
    );
    expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'verdict.json'))).toBe(true);
    expect(fs.existsSync(path.join(ledgerDir, 'iter', '3'))).toBe(false);

    const duplicate = await api.requestReview({ ...args }, 0);
    expect(duplicate).toEqual({
      status: 'duplicate',
      target: 'reviewer',
      idempotency_key: 'review-source-run-3',
    });
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
  });

  it('crosses the request_review file and directory durability barriers before returning prepared', async () => {
    const barriers: string[] = [];
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name === 'decisions.jsonl' && (operation === 'append' || operation === 'flush')) {
            barriers.push(operation);
          }
        },
        beforeDirectorySync: ({ name }) => {
          if (name === 'decisions.jsonl') barriers.push('directory-sync');
        },
      },
    });
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);

    await expect(
      dispatcher.requestReview(
        {
          checkpoint_sha: checkpoint.sha,
          source_run_id: 'source-run',
          source_iter: 3,
          scope: ['durability'],
          idempotency_key: 'durable-review-claim',
        },
        0,
      ),
    ).resolves.toMatchObject({ status: 'prepared' });

    expect(barriers).toEqual(['append', 'flush', 'directory-sync']);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it.each(['file sync', 'directory sync', 'descriptor close'] as const)(
    'reconciles a committed request_review after one %s ambiguity without appending twice',
    async (boundary) => {
      let injected = false;
      let decisionAppends = 0;
      let directorySyncs = 0;
      const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
        create: true,
        testHooks: {
          beforeFileMutation: ({ name, operation }) => {
            if (name !== 'decisions.jsonl') return;
            if (operation === 'append') decisionAppends += 1;
            if (boundary === 'file sync' && operation === 'flush' && !injected) {
              injected = true;
              throw new Error('injected request_review file sync ambiguity');
            }
          },
          beforeDirectorySync: ({ name }) => {
            if (name !== 'decisions.jsonl') return;
            directorySyncs += 1;
            if (boundary === 'directory sync' && !injected) {
              injected = true;
              throw new Error('injected request_review directory sync ambiguity');
            }
          },
          closeFlatFileDescriptor: (fd) => {
            fs.closeSync(fd);
            if (boundary === 'descriptor close' && !injected) {
              injected = true;
              throw new Error('injected request_review descriptor close ambiguity');
            }
          },
        },
      });
      const { dispatcher, workspace, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
      const checkpoint = initializeCheckpointRepository(workspace);
      writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
      const args = {
        checkpoint_sha: checkpoint.sha,
        source_run_id: 'source-run',
        source_iter: 3,
        scope: ['durability'],
        idempotency_key: `durable-${boundary.replace(' ', '-')}`,
      };

      await expect(dispatcher.requestReview(args, 0)).resolves.toMatchObject({ status: 'prepared' });
      await expect(dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'duplicate' });

      expect(injected).toBe(true);
      expect(decisionAppends).toBe(1);
      expect(directorySyncs).toBeGreaterThan(0);
      expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
    },
  );

  it.each([
    { label: 'successful', omit: undefined },
    { label: 'failed', omit: 'coder_summary.txt' as const },
  ])('keeps foreign ledger permissions and bytes unchanged after a $label read-only import', async ({ omit }) => {
    const { dispatcher, workspace } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    const sourceLedger = writeSourceReviewArtifacts(workspace, 'foreign-source', 3, checkpoint.patch, { omit });
    sourceLedger.appendFlatFile('decisions.jsonl', '{"foreign":"preserved"}\n');
    const paths = {
      tasks: path.join(workspace, 'tasks'),
      run: sourceLedger.directory,
      iterRoot: path.join(sourceLedger.directory, 'iter'),
      iter: path.join(sourceLedger.directory, 'iter', '3'),
      decisions: path.join(sourceLedger.directory, 'decisions.jsonl'),
      directive: path.join(sourceLedger.directory, 'iter', '3', 'directive.json'),
    };
    fs.chmodSync(paths.tasks, 0o755);
    fs.chmodSync(paths.run, 0o751);
    fs.chmodSync(paths.iterRoot, 0o755);
    fs.chmodSync(paths.iter, 0o751);
    fs.chmodSync(paths.decisions, 0o644);
    fs.chmodSync(paths.directive, 0o640);
    const beforeModes = Object.fromEntries(Object.entries(paths).map(([name, target]) => [name, permissions(target)]));
    const beforeBytes = {
      decisions: fs.readFileSync(paths.decisions),
      directive: fs.readFileSync(paths.directive),
    };
    const request = dispatcher.requestReview(
      {
        checkpoint_sha: checkpoint.sha,
        source_run_id: 'foreign-source',
        source_iter: 3,
        scope: ['immutability'],
        idempotency_key: `foreign-read-only-${omit ?? 'success'}`,
      },
      0,
    );

    if (omit) await expect(request).rejects.toThrow(/coder_summary\.txt/i);
    else await expect(request).resolves.toMatchObject({ status: 'prepared' });

    expect(Object.fromEntries(Object.entries(paths).map(([name, target]) => [name, permissions(target)]))).toEqual(
      beforeModes,
    );
    expect(fs.readFileSync(paths.decisions)).toEqual(beforeBytes.decisions);
    expect(fs.readFileSync(paths.directive)).toEqual(beforeBytes.directive);
  });

  it('removes only a failed matching cache entry so same-digest retry succeeds while conflicting reuse rejects', async () => {
    const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    const sourceLedger = writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch, {
      omit: 'coder_summary.txt',
    });
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'retryable-review',
    };
    const api = dispatcher as unknown as {
      requestReview(input: typeof args, targetIter: number): Promise<{ status: string }>;
      reviewRequestIdentityHistory: Map<string, string>;
    };

    const failed = api.requestReview(args, 0);
    await expect(api.requestReview({ ...args, scope: ['different'] }, 0)).rejects.toThrow(/idempotency.*conflict/i);
    await expect(failed).rejects.toThrow(/coder_summary\.txt/i);
    expect(api.reviewRequestIdentityHistory.has(reviewIdentityHash(args.idempotency_key))).toBe(false);
    sourceLedger.writeIterationArtifact(3, 'coder_summary.txt', 'existing checkpoint\n');

    await expect(api.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'prepared' });
    expect(api.reviewRequestIdentityHistory.get(reviewIdentityHash(args.idempotency_key))).toMatch(/^[0-9a-f]{64}$/);
    await expect(api.requestReview({ ...args, scope: ['different'] }, 0)).rejects.toThrow(/idempotency.*conflict/i);

    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();
  });

  it('releases an identity when a committed nested import precedes its durable request claim', async () => {
    let failCommittedImport = true;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        afterNestedTemporaryUnlink: ({ relativePath }) => {
          if (!failCommittedImport || relativePath !== 'iter/0/diff.patch') return;
          failCommittedImport = false;
          throw new Error('injected committed import failure before request_review append');
        },
      },
    });
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'committed-import-before-claim',
    };
    const internal = dispatcher as unknown as { reviewRequestIdentityHistory: Map<string, string> };

    await expect(dispatcher.requestReview(args, 0)).rejects.toMatchObject({
      name: 'SecureAutoloopLedgerCommitError',
      committed: true,
      operation: 'secure_nested_artifact_write',
    });
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);
    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'diff.patch'))).toEqual(checkpoint.patch);
    expect(internal.reviewRequestIdentityHistory.has(reviewIdentityHash(args.idempotency_key))).toBe(false);

    await expect(dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'prepared' });
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
    await expect(dispatcher.requestReview({ ...args, scope: ['different'] }, 0)).rejects.toThrow(
      /idempotency.*conflict/i,
    );
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('releases an identity when a nested artifact commits before claim and the decisions lookup is unreadable', async () => {
    let blockDecisionReads = false;
    let failCommittedImport = true;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        afterNestedTemporaryUnlink: ({ relativePath }) => {
          if (!failCommittedImport || relativePath !== 'iter/0/diff.patch') return;
          failCommittedImport = false;
          blockDecisionReads = true;
          throw new Error('injected committed nested write before an unreadable decision lookup');
        },
      },
    });
    const openFlatFile = secureLedger.openFlatFile.bind(secureLedger);
    vi.spyOn(secureLedger, 'openFlatFile').mockImplementation((name, mode, create) => {
      if (blockDecisionReads && name === 'decisions.jsonl' && mode === 'read') {
        throw new Error('injected unreadable decisions ledger after nested commit');
      }
      return openFlatFile(name, mode, create);
    });
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'nested-commit-unreadable-decisions',
    };
    const internal = dispatcher as unknown as { reviewRequestIdentityHistory: Map<string, string> };
    const identityHash = reviewIdentityHash(args.idempotency_key);

    await expect(dispatcher.requestReview(args, 0)).rejects.toMatchObject({
      name: 'SecureAutoloopLedgerCommitError',
      committed: true,
      operation: 'secure_nested_artifact_write',
    });
    expect(internal.reviewRequestIdentityHistory.has(identityHash)).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);

    blockDecisionReads = false;
    await expect(dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'prepared' });
    await expect(dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'duplicate' });
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('reconciles a request_review row committed before descriptor-close failure without releasing or appending twice', async () => {
    const closeFailure = new Error('injected post-write close failure');
    let closeFailures = 0;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        closeFlatFileDescriptor: (fd) => {
          fs.closeSync(fd);
          if (closeFailures === 0) {
            closeFailures += 1;
            throw closeFailure;
          }
        },
      },
    });
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'post-write-close-review',
    };

    await expect(dispatcher.requestReview(args, 0)).resolves.toMatchObject({ status: 'prepared' });
    expect(closeFailures).toBe(1);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
    await expect(dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'duplicate' });
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('reclaims one Planner review_request after a committed row is temporarily unreadable', async () => {
    let blockReconciliationRead = false;
    let failRequestReviewClose = false;
    let decisionAppends = 0;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name !== 'decisions.jsonl' || operation !== 'append') return;
          decisionAppends += 1;
          if (decisionAppends === 1) failRequestReviewClose = true;
        },
        closeFlatFileDescriptor: (fd) => {
          fs.closeSync(fd);
          if (failRequestReviewClose) {
            failRequestReviewClose = false;
            blockReconciliationRead = true;
            throw new Error('injected post-write close failure');
          }
        },
      },
    });
    const openFlatFile = secureLedger.openFlatFile.bind(secureLedger);
    vi.spyOn(secureLedger, 'openFlatFile').mockImplementation((name, mode, create) => {
      if (blockReconciliationRead && name === 'decisions.jsonl' && mode === 'read') {
        throw new Error('injected reconciliation read failure');
      }
      return openFlatFile(name, mode, create);
    });
    const checkpoint = initializeCheckpointRepository(tmpRoot);
    writeSourceReviewArtifacts(tmpRoot, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'unreadable-committed-review',
    };
    const plannerReply = ['```autoloop', JSON.stringify({ tool: 'request_review', args }), '```'].join('\n');
    const { dispatcher, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutputs: [plannerReply, plannerReply] });
    const internal = dispatcher as unknown as {
      reviewRequestIdentityHistory: Map<string, string>;
      releasedReviewRequests: Map<string, unknown>;
    };

    await expect(dispatcher.requestReview(args, 0)).rejects.toMatchObject({
      name: 'SecureAutoloopLedgerCommitError',
      code: 'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE',
      committed: true,
      operation: 'secure_ledger_append',
    });
    expect(internal.reviewRequestIdentityHistory.has(reviewIdentityHash(args.idempotency_key))).toBe(true);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);

    blockReconciliationRead = false;
    const reclaimed = await dispatcher.deliver(Msg.chat(0, { text: 'retry exact review' }));

    expect(reclaimed).toHaveLength(1);
    expect(validateMessage(reclaimed[0])).toMatchObject({
      type: 'review_request',
      payload: expect.objectContaining({
        checkpoint_sha: checkpoint.sha,
        idempotency_key: args.idempotency_key,
      }),
    });
    expect(internal.releasedReviewRequests.size).toBe(0);
    await expect(dispatcher.deliver(Msg.chat(0, { text: 'retry accepted review' }))).resolves.toEqual([]);
    await expect(dispatcher.requestReview({ ...args, scope: ['different'] }, 0)).rejects.toThrow(
      /idempotency.*conflict/i,
    );
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('preserves the complete prior durable snapshot when a sibling caches success during a failed committed refresh', async () => {
    let failCommittedClose = false;
    let blockRefreshReads = false;
    let injectSibling = false;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        closeFlatFileDescriptor: (fd) => {
          fs.closeSync(fd);
          if (failCommittedClose) {
            failCommittedClose = false;
            blockRefreshReads = true;
            injectSibling = true;
            throw new Error('injected committed append close ambiguity');
          }
        },
      },
    });
    const historical = ['historical-review-one', 'historical-review-two'].map((idempotencyKey) =>
      durableReviewDecisionPayload(idempotencyKey),
    );
    secureLedger.appendFlatFile(
      'decisions.jsonl',
      `${historical.map((payload) => durableReviewDecisionRow(payload)).join('\n')}\n`,
    );
    const { dispatcher, workspace } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const sibling = durableReviewDecisionPayload('sibling-success');
    const internal = dispatcher as unknown as {
      durableReviewRequestClaims?: Map<string, unknown>;
      findDurableReviewRequest(
        expected: Readonly<Record<string, unknown>>,
        forceRefresh?: boolean,
      ): 'none' | 'matching' | 'conflicting';
      persistReviewRequestDecision(payload: Readonly<Record<string, unknown>>): void;
    };
    const openFlatFile = secureLedger.openFlatFile.bind(secureLedger);
    vi.spyOn(secureLedger, 'openFlatFile').mockImplementation((name, mode, create) => {
      if (blockRefreshReads && name === 'decisions.jsonl' && mode === 'read') {
        if (injectSibling) {
          injectSibling = false;
          internal.persistReviewRequestDecision(sibling);
        }
        throw new Error('injected committed refresh read failure');
      }
      return openFlatFile(name, mode, create);
    });
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'ambiguous-primary-review',
    };

    const pending = dispatcher.requestReview(args, 0);
    failCommittedClose = true;
    await expect(pending).rejects.toMatchObject({
      name: 'SecureAutoloopLedgerCommitError',
      committed: true,
      operation: 'secure_ledger_append',
    });

    expect(internal.durableReviewRequestClaims?.size).toBe(3);
    for (const payload of [...historical, sibling]) {
      expect(internal.findDurableReviewRequest(payload)).toBe('matching');
    }
  });

  it('retains fail-closed durable identity capacity when a failed committed refresh is reordered with sibling success', async () => {
    let failCommittedClose = false;
    let blockRefreshReads = false;
    let injectSibling = false;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        closeFlatFileDescriptor: (fd) => {
          fs.closeSync(fd);
          if (failCommittedClose) {
            failCommittedClose = false;
            blockRefreshReads = true;
            injectSibling = true;
            throw new Error('injected committed append close ambiguity at capacity');
          }
        },
      },
    });
    const historicalRows = Array.from({ length: 4_094 }, (_, index) =>
      durableReviewDecisionRow(durableReviewDecisionPayload(`capacity-history-${index}`)),
    );
    secureLedger.appendFlatFile('decisions.jsonl', `${historicalRows.join('\n')}\n`);
    const { dispatcher, workspace } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const sibling = durableReviewDecisionPayload('capacity-sibling-success');
    const internal = dispatcher as unknown as {
      persistReviewRequestDecision(payload: Readonly<Record<string, unknown>>): void;
      prepareCheckpointReview(request: Record<string, unknown>, targetIter: number, digest: string): Promise<unknown>;
    };
    const openFlatFile = secureLedger.openFlatFile.bind(secureLedger);
    vi.spyOn(secureLedger, 'openFlatFile').mockImplementation((name, mode, create) => {
      if (blockRefreshReads && name === 'decisions.jsonl' && mode === 'read') {
        if (injectSibling) {
          injectSibling = false;
          internal.persistReviewRequestDecision(sibling);
        }
        throw new Error('injected committed refresh read failure at capacity');
      }
      return openFlatFile(name, mode, create);
    });
    const ambiguous = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'capacity-ambiguous-primary',
    };

    const pending = dispatcher.requestReview(ambiguous, 0);
    failCommittedClose = true;
    await expect(pending).rejects.toMatchObject({ committed: true, operation: 'secure_ledger_append' });

    const prepare = vi.spyOn(internal, 'prepareCheckpointReview');
    await expect(
      dispatcher.requestReview(
        {
          checkpoint_sha: checkpoint.sha,
          source_run_id: 'source-run',
          source_iter: 3,
          scope: ['correctness'],
          idempotency_key: 'capacity-must-fail-closed',
        },
        1,
      ),
    ).rejects.toThrow(/identity.*4096|4096.*identity|identity.*capacity/i);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('deduplicates an exact durable request across a cold dispatcher and rejects a changed payload before preparation', async () => {
    const first = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(first.workspace);
    writeSourceReviewArtifacts(first.workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'cold-dispatcher-review',
    };
    await expect(first.dispatcher.requestReview(args, 0)).resolves.toMatchObject({ status: 'prepared' });
    expect(requestReviewDecisions(first.ledgerDir)).toHaveLength(1);

    const cold = makeDispatcher({}, { sendOutput: reviewerReply });
    const internal = cold.dispatcher as unknown as {
      prepareCheckpointReview(request: typeof args, targetIter: number, digest: string): Promise<unknown>;
    };
    const prepare = vi.spyOn(internal, 'prepareCheckpointReview');
    const openFlatFile = vi.spyOn(cold.dispatcher.secureLedgerCapability, 'openFlatFile');

    await expect(cold.dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'duplicate' });
    await expect(cold.dispatcher.requestReview({ ...args, scope: ['different'] }, 0)).rejects.toThrow(
      /idempotency.*conflict/i,
    );
    await expect(cold.dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'duplicate' });
    expect(prepare).not.toHaveBeenCalled();
    expect(
      openFlatFile.mock.calls.filter(([name, mode]) => name === 'decisions.jsonl' && mode === 'read'),
    ).toHaveLength(1);
    expect(requestReviewDecisions(first.ledgerDir)).toHaveLength(1);
  });

  it('treats repeated identical legacy request rows as one durable claim while any differing row conflicts', async () => {
    const first = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(first.workspace);
    writeSourceReviewArtifacts(first.workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'legacy-duplicate-review',
    };
    await first.dispatcher.requestReview(args, 0);
    const committed = requestReviewDecisions(first.ledgerDir)[0];
    first.dispatcher.secureLedgerCapability.appendFlatFile('decisions.jsonl', `${JSON.stringify(committed)}\n`);

    const compatible = makeDispatcher({}, { sendOutput: reviewerReply });
    await expect(compatible.dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'duplicate' });
    expect(requestReviewDecisions(first.ledgerDir)).toHaveLength(2);

    const conflictingRow = structuredClone(committed) as { payload: Record<string, unknown> };
    conflictingRow.payload.scope = ['different'];
    first.dispatcher.secureLedgerCapability.appendFlatFile('decisions.jsonl', `${JSON.stringify(conflictingRow)}\n`);
    const conflicted = makeDispatcher({}, { sendOutput: reviewerReply });

    await expect(conflicted.dispatcher.requestReview({ ...args }, 0)).rejects.toThrow(/idempotency.*conflict/i);
    expect(requestReviewDecisions(first.ledgerDir)).toHaveLength(3);
  });

  it('loads at most 4096 durable request identities and rejects the next unique claim before heavy preparation', async () => {
    const { dispatcher, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const decisions = Array.from({ length: 4_097 }, (_, index) => {
      const idempotencyKey = `persisted-capacity-${index}`;
      return JSON.stringify({
        ts: '2026-01-01T00:00:00.000Z',
        kind: 'request_review',
        actor: 'planner',
        payload: {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 3,
          target_iter: 0,
          scope: ['correctness'],
          idempotency_key: idempotencyKey,
          request_digest: createHash('sha256').update(idempotencyKey).digest('hex'),
        },
      });
    }).join('\n');
    dispatcher.secureLedgerCapability.appendFlatFile('decisions.jsonl', `${decisions}\n`);
    const internal = dispatcher as unknown as {
      prepareCheckpointReview(request: Record<string, unknown>, targetIter: number, digest: string): Promise<unknown>;
    };
    const prepare = vi.spyOn(internal, 'prepareCheckpointReview');
    const openFlatFile = vi.spyOn(dispatcher.secureLedgerCapability, 'openFlatFile');

    await expect(
      dispatcher.requestReview(
        {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 3,
          scope: ['correctness'],
          idempotency_key: 'persisted-capacity-overflow',
        },
        0,
      ),
    ).rejects.toThrow(/durable.*identity.*4096|4096.*durable.*identity|identity.*capacity/i);

    expect(prepare).not.toHaveBeenCalled();
    expect(
      openFlatFile.mock.calls.filter(([name, mode]) => name === 'decisions.jsonl' && mode === 'read'),
    ).toHaveLength(1);
    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
  });

  it('allocates exactly one full-size Buffer snapshot and no equivalent whole-ledger byte copy during cold lookup', async () => {
    const first = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(first.workspace);
    writeSourceReviewArtifacts(first.workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'no-second-full-buffer',
    };
    await first.dispatcher.requestReview(args, 0);
    first.dispatcher.secureLedgerCapability.appendFlatFile(
      'decisions.jsonl',
      `${JSON.stringify({ kind: 'allocation-oracle-padding', payload: 'x'.repeat(256 * 1024) })}\n`,
    );
    const ledgerBytes = fs.statSync(path.join(first.ledgerDir, 'decisions.jsonl')).size;
    const cold = makeDispatcher({}, { sendOutput: reviewerReply });
    const bufferAlloc = vi.spyOn(Buffer, 'alloc');
    const bufferAllocUnsafe = vi.spyOn(Buffer, 'allocUnsafe');
    const bufferAllocUnsafeSlow = vi.spyOn(Buffer, 'allocUnsafeSlow');
    const bufferFrom = vi.spyOn(Buffer, 'from');
    const bufferConcat = vi.spyOn(Buffer, 'concat');
    const bufferCopyBytesFrom = vi.spyOn(Buffer, 'copyBytesFrom');

    try {
      await expect(cold.dispatcher.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'duplicate' });
      const wholeLedgerAllocations = [
        ...bufferAlloc.mock.calls.map(([size]) => ({ method: 'alloc', size })),
        ...bufferAllocUnsafe.mock.calls.map(([size]) => ({ method: 'allocUnsafe', size })),
        ...bufferAllocUnsafeSlow.mock.calls.map(([size]) => ({ method: 'allocUnsafeSlow', size })),
        ...bufferFrom.mock.calls.map(([value]) => ({
          method: 'from',
          size:
            typeof value === 'string'
              ? Buffer.byteLength(value)
              : ArrayBuffer.isView(value)
                ? value.byteLength
                : value instanceof ArrayBuffer
                  ? value.byteLength
                  : Array.isArray(value)
                    ? value.length
                    : 0,
        })),
        ...bufferConcat.mock.calls.map(([values, totalLength]) => ({
          method: 'concat',
          size: totalLength ?? values.reduce((total, value) => total + value.byteLength, 0),
        })),
        ...bufferCopyBytesFrom.mock.calls.map(([view, offset = 0, length]) => ({
          method: 'copyBytesFrom',
          size: length ?? view.length - offset,
        })),
      ].filter(({ size }) => size >= ledgerBytes);

      expect(wholeLedgerAllocations).toEqual([{ method: 'allocUnsafe', size: ledgerBytes }]);
    } finally {
      bufferCopyBytesFrom.mockRestore();
      bufferConcat.mockRestore();
      bufferFrom.mockRestore();
      bufferAllocUnsafeSlow.mockRestore();
      bufferAllocUnsafe.mockRestore();
      bufferAlloc.mockRestore();
    }
  });

  it('rejects malformed UTF-8 and a leading BOM in the durable decision ledger before effects', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const target = path.join(ledgerDir, 'decisions.jsonl');
    const args = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'malformed-decision-ledger',
    };
    fs.writeFileSync(
      target,
      Buffer.concat([Buffer.from('{"kind":"request_review","payload":'), Buffer.from([0xff]), Buffer.from('}\n')]),
      { mode: 0o600 },
    );

    await expect(dispatcher.requestReview(args, 0)).rejects.toThrow(/decisions\.jsonl.*valid UTF-8/i);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);

    fs.writeFileSync(
      target,
      Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), Buffer.from('{"kind":"request_review","payload":{}}\n')]),
    );
    await expect(dispatcher.requestReview(args, 0)).rejects.toThrow(/decisions\.jsonl record 1.*malformed/i);
    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
  });

  it('coalesces concurrent same-key same-digest preparation into one effect and one duplicate result', async () => {
    const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'concurrent-review',
    };
    const api = dispatcher as unknown as {
      requestReview(input: typeof args, targetIter: number): Promise<{ status: string }>;
    };

    const results = await Promise.all([api.requestReview(args, 0), api.requestReview({ ...args }, 0)]);

    expect(results.map((result) => result.status).sort()).toEqual(['duplicate', 'prepared']);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();
  });

  it('coalesces a real same-key preparation failure, clears its slot and identity, and prepares one exact retry', async () => {
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    const sourceLedger = writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch, {
      omit: 'coder_summary.txt',
    });
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'concurrent-failing-review',
    };
    const api = dispatcher as unknown as {
      requestReview(input: typeof args, targetIter: number): Promise<{ status: string }>;
      prepareCheckpointReview(request: typeof args, targetIter: number, digest: string): Promise<unknown>;
      activeReviewRequestPreparations: number;
      reviewRequests: Map<string, unknown>;
      reviewRequestIdentityHistory: Map<string, string>;
    };
    const prepare = vi.spyOn(api, 'prepareCheckpointReview');

    const results = await Promise.allSettled([api.requestReview(args, 0), api.requestReview({ ...args }, 0)]);

    expect(results).toEqual([
      expect.objectContaining({
        status: 'rejected',
        reason: expect.objectContaining({ message: expect.stringMatching(/coder_summary\.txt/i) }),
      }),
      expect.objectContaining({
        status: 'rejected',
        reason: expect.objectContaining({ message: expect.stringMatching(/coder_summary\.txt/i) }),
      }),
    ]);
    expect(prepare).toHaveBeenCalledTimes(1);
    expect(api.activeReviewRequestPreparations).toBe(0);
    expect(api.reviewRequests.has(reviewIdentityHash(args.idempotency_key))).toBe(false);
    expect(api.reviewRequestIdentityHistory.has(reviewIdentityHash(args.idempotency_key))).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);

    sourceLedger.writeIterationArtifact(3, 'coder_summary.txt', 'existing checkpoint\n');
    await expect(api.requestReview({ ...args }, 0)).resolves.toMatchObject({ status: 'prepared' });
    expect(prepare).toHaveBeenCalledTimes(2);
    expect(api.activeReviewRequestPreparations).toBe(0);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('bounds 64 real preparations before artifact I/O while coalescing and releasing success/failure slots', async () => {
    const { dispatcher, workspace } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const base = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
    };
    type Request = typeof base & { idempotency_key: string };
    const api = dispatcher as unknown as {
      requestReview(input: Request, targetIter: number): Promise<{ status: string }>;
      prepareCheckpointReview(request: Request, targetIter: number, digest: string): Promise<unknown>;
      spawnGitEvidenceProcess(argv: string[]): ChildProcess;
      activeReviewRequestPreparations: number;
      reviewRequests: Map<string, unknown>;
      reviewRequestIdentityHistory: Map<string, string>;
    };
    const originalSpawn = api.spawnGitEvidenceProcess.bind(dispatcher);
    const headGates: FakeGitChild[] = [];
    const spawnGit = vi.spyOn(api, 'spawnGitEvidenceProcess').mockImplementation((argv) => {
      if (argv[1] === 'rev-parse' && headGates.length < 64) {
        const child = fakeGitChild();
        headGates.push(child);
        return child as unknown as ChildProcess;
      }
      return originalSpawn(argv);
    });
    const prepare = vi.spyOn(api, 'prepareCheckpointReview');
    const artifactReads = vi.spyOn(SecureAutoloopLedger.prototype, 'readIterationArtifact');
    const openFlatFile = vi.spyOn(dispatcher.secureLedgerCapability, 'openFlatFile');
    const decisionLedgerReads = () =>
      openFlatFile.mock.calls.filter(([name, mode]) => name === 'decisions.jsonl' && mode === 'read').length;
    const pending = Array.from({ length: 64 }, (_, index) =>
      api.requestReview({ ...base, idempotency_key: `simultaneous-${index}` }, 0),
    );

    try {
      expect(headGates).toHaveLength(64);
      expect(prepare).toHaveBeenCalledTimes(64);
      expect(api.activeReviewRequestPreparations).toBe(64);
      expect(artifactReads).not.toHaveBeenCalled();
      expect(decisionLedgerReads()).toBe(1);

      const overflowKey = 'capacity-overflow';
      await expect(api.requestReview({ ...base, idempotency_key: overflowKey }, 0)).rejects.toThrow(
        /simultaneous.*64|64.*simultaneous|preparation.*capacity/i,
      );
      expect(prepare).toHaveBeenCalledTimes(64);
      expect(artifactReads).not.toHaveBeenCalled();
      expect(decisionLedgerReads()).toBe(1);
      expect(api.reviewRequestIdentityHistory.has(reviewIdentityHash(overflowKey))).toBe(false);

      const coalesced = api.requestReview({ ...base, idempotency_key: 'simultaneous-0' }, 0);
      expect(prepare).toHaveBeenCalledTimes(64);
      headGates[0].stdout.write(`${checkpoint.sha}\n`);
      headGates[0].emit('close', 0, null);
      await expect(Promise.all([pending[0], coalesced])).resolves.toEqual([
        expect.objectContaining({ status: 'prepared' }),
        expect.objectContaining({ status: 'duplicate' }),
      ]);
      expect(api.activeReviewRequestPreparations).toBe(63);

      await expect(api.requestReview({ ...base, idempotency_key: overflowKey }, 0)).resolves.toMatchObject({
        status: 'prepared',
      });

      headGates[1].stdout.write(`${'b'.repeat(40)}\n`);
      headGates[1].emit('close', 0, null);
      await expect(pending[1]).rejects.toThrow(/does not match workspace HEAD/i);
      expect(api.reviewRequestIdentityHistory.has(reviewIdentityHash('simultaneous-1'))).toBe(false);
      expect(api.activeReviewRequestPreparations).toBe(62);
      await expect(api.requestReview({ ...base, idempotency_key: 'simultaneous-1' }, 0)).resolves.toMatchObject({
        status: 'prepared',
      });

      for (let index = 2; index < headGates.length; index += 1) {
        headGates[index].stdout.write(`${checkpoint.sha}\n`);
        headGates[index].emit('close', 0, null);
        await expect(pending[index]).resolves.toMatchObject({ status: 'prepared' });
      }
      expect(api.activeReviewRequestPreparations).toBe(0);
    } finally {
      for (const child of headGates) child.emit('close', 1, null);
      await Promise.allSettled(pending);
      artifactReads.mockRestore();
      openFlatFile.mockRestore();
      prepare.mockRestore();
      spawnGit.mockRestore();
    }
  });

  it('retains at most 64 successful review preparations and never evicts the active preparation', async () => {
    const { dispatcher, workspace } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const api = dispatcher as unknown as {
      requestReview(input: Record<string, unknown>, targetIter: number): Promise<{ status: string }>;
      reviewRequests: Map<string, { pending?: Promise<unknown>; settled: boolean }>;
    };
    const base = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
    };
    for (let index = 0; index < 64; index += 1) {
      await api.requestReview({ ...base, idempotency_key: `bounded-review-${index}` }, 0);
    }
    expect(api.reviewRequests.size).toBe(64);

    const active = api.requestReview({ ...base, idempotency_key: 'bounded-review-64' }, 0);
    expect(api.reviewRequests.size).toBe(65);
    const retainedActivePreparation = api.reviewRequests.has(reviewIdentityHash('bounded-review-64'));
    await active;

    expect(retainedActivePreparation).toBe(true);
    expect(api.reviewRequests.size).toBe(64);
    expect(api.reviewRequests.has(reviewIdentityHash('bounded-review-0'))).toBe(false);
    expect(api.reviewRequests.has(reviewIdentityHash('bounded-review-64'))).toBe(true);
    expect([...api.reviewRequests.keys()]).toEqual(expect.arrayContaining([expect.stringMatching(/^[0-9a-f]{64}$/)]));
    expect([...api.reviewRequests.values()].every((entry) => entry.settled && entry.pending === undefined)).toBe(true);

    const decisionsBeforeRetry = requestReviewDecisions(path.join(workspace, 'tasks', 'r1')).length;
    await expect(api.requestReview({ ...base, idempotency_key: 'bounded-review-0' }, 0)).resolves.toMatchObject({
      status: 'duplicate',
    });
    await expect(
      api.requestReview({ ...base, scope: ['different'], idempotency_key: 'bounded-review-0' }, 0),
    ).rejects.toThrow(/idempotency.*conflict/i);
    expect(requestReviewDecisions(path.join(workspace, 'tasks', 'r1'))).toHaveLength(decisionsBeforeRetry);
  });

  it('bounds interrupted handoffs while allowing one reclaim and freeing capacity only after acceptance', async () => {
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const base = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
    };
    type Request = typeof base & { idempotency_key: string };
    const api = dispatcher as unknown as {
      requestReview(input: Request, targetIter: number): Promise<ReviewRequestPreparationResult>;
      prepareCheckpointReview(request: Request, targetIter: number, digest: string): Promise<PreparedReviewRequest>;
      releaseReviewRequest(idempotencyKey: string, payload: CheckpointReviewRequestPayload): void;
      acceptReviewRequest(idempotencyKey: string): void;
      releasedReviewRequests: Map<string, unknown>;
    };

    for (let index = 0; index < 64; index += 1) {
      const idempotencyKey = `released-capacity-${index}`;
      const prepared = await api.requestReview({ ...base, idempotency_key: idempotencyKey }, 0);
      expect(prepared.status).toBe('prepared');
      api.releaseReviewRequest(idempotencyKey, (prepared as PreparedReviewRequest).payload);
    }
    expect(api.releasedReviewRequests.size).toBe(64);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(64);

    const prepare = vi.spyOn(api, 'prepareCheckpointReview');
    await expect(api.requestReview({ ...base, idempotency_key: 'released-capacity-overflow' }, 0)).rejects.toThrow(
      /interrupted handoff.*capacity|capacity.*interrupted handoff/i,
    );
    expect(prepare).not.toHaveBeenCalled();

    const reclaimed = await Promise.all([
      api.requestReview({ ...base, idempotency_key: 'released-capacity-0' }, 0),
      api.requestReview({ ...base, idempotency_key: 'released-capacity-0' }, 0),
    ]);
    expect(reclaimed.map((result) => result.status).sort()).toEqual(['duplicate', 'prepared']);
    expect(reclaimed.filter((result) => result.status === 'prepared')).toHaveLength(1);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(64);

    api.acceptReviewRequest('released-capacity-0');
    await expect(
      api.requestReview({ ...base, idempotency_key: 'released-capacity-after-accept' }, 0),
    ).resolves.toMatchObject({ status: 'prepared' });
    expect(prepare).toHaveBeenCalledTimes(1);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(65);
  });

  it('rejects new identities at the 4096-entry history cap while retaining old duplicate and conflict semantics', async () => {
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const oldRequest = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'history-old',
    };
    const api = dispatcher as unknown as {
      requestReview(input: Record<string, unknown>, targetIter: number): Promise<{ status: string }>;
      reviewRequests: Map<string, unknown>;
      reviewRequestIdentityHistory: Map<string, string>;
    };

    await expect(api.requestReview(oldRequest, 0)).resolves.toMatchObject({ status: 'prepared' });
    api.reviewRequests.clear();
    for (let index = 0; api.reviewRequestIdentityHistory.size < 4_096; index += 1) {
      api.reviewRequestIdentityHistory.set(
        reviewIdentityHash(`synthetic-identity-${index}`),
        reviewIdentityHash(`synthetic-digest-${index}`),
      );
    }

    expect(api.reviewRequestIdentityHistory.size).toBe(4_096);
    expect([...api.reviewRequestIdentityHistory.keys()].every((key) => /^[0-9a-f]{64}$/.test(key))).toBe(true);
    await expect(api.requestReview({ ...oldRequest, idempotency_key: 'history-new' }, 0)).rejects.toThrow(
      /identity history.*capacity|capacity.*identity history/i,
    );
    await expect(api.requestReview({ ...oldRequest }, 0)).resolves.toMatchObject({ status: 'duplicate' });
    await expect(api.requestReview({ ...oldRequest, scope: ['different'] }, 0)).rejects.toThrow(
      /idempotency.*conflict/i,
    );
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('does not let ambient GIT_DIR and GIT_WORK_TREE authenticate a foreign checkpoint', async () => {
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const workspaceCheckpoint = initializeCheckpointRepository(workspace, 'workspace checkpoint\n');
    const foreignWorkspace = path.join(workspace, 'foreign-repository');
    fs.mkdirSync(foreignWorkspace);
    const foreignCheckpoint = initializeCheckpointRepository(foreignWorkspace, 'foreign checkpoint\n');
    writeSourceReviewArtifacts(workspace, 'foreign-source', 3, foreignCheckpoint.patch);
    expect(foreignCheckpoint.sha).not.toBe(workspaceCheckpoint.sha);
    const priorGitDir = process.env.GIT_DIR;
    const priorGitWorkTree = process.env.GIT_WORK_TREE;
    process.env.GIT_DIR = path.join(foreignWorkspace, '.git');
    process.env.GIT_WORK_TREE = foreignWorkspace;

    try {
      await expect(
        dispatcher.requestReview(
          {
            checkpoint_sha: foreignCheckpoint.sha,
            source_run_id: 'foreign-source',
            source_iter: 3,
            scope: ['authenticity'],
            idempotency_key: 'foreign-git-environment',
          },
          0,
        ),
      ).rejects.toThrow(/checkpoint.*does not match workspace HEAD|workspace HEAD.*checkpoint/i);
    } finally {
      if (priorGitDir === undefined) delete process.env.GIT_DIR;
      else process.env.GIT_DIR = priorGitDir;
      if (priorGitWorkTree === undefined) delete process.env.GIT_WORK_TREE;
      else process.env.GIT_WORK_TREE = priorGitWorkTree;
    }

    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);
  });

  it.skipIf(process.platform === 'win32')('does not invoke an ambient GIT_EXTERNAL_DIFF helper', async () => {
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const internal = dispatcher as unknown as { spawnGitEvidenceProcess(argv: string[]): ChildProcess };
    const spawnGit = vi.spyOn(internal, 'spawnGitEvidenceProcess');
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const marker = path.join(workspace, 'external-diff-invoked');
    const helper = path.join(workspace, 'external-diff-helper.sh');
    fs.writeFileSync(helper, `#!/bin/sh\nprintf 'invoked\\n' >> '${marker}'\nexit 91\n`, { mode: 0o700 });
    const priorExternalDiff = process.env.GIT_EXTERNAL_DIFF;
    process.env.GIT_EXTERNAL_DIFF = helper;

    try {
      await expect(
        dispatcher.requestReview(
          {
            checkpoint_sha: checkpoint.sha,
            source_run_id: 'source-run',
            source_iter: 3,
            scope: ['no-external-helper'],
            idempotency_key: 'external-diff-disabled',
          },
          0,
        ),
      ).resolves.toMatchObject({ status: 'prepared' });
    } finally {
      if (priorExternalDiff === undefined) delete process.env.GIT_EXTERNAL_DIFF;
      else process.env.GIT_EXTERNAL_DIFF = priorExternalDiff;
    }

    const showArgv = spawnGit.mock.calls.find(([argv]) => argv[1] === 'show')?.[0];
    expect(showArgv).toEqual(expect.arrayContaining(['--no-ext-diff', '--no-textconv']));
    expect(fs.existsSync(marker)).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it.skipIf(process.platform === 'win32')('does not invoke a repository textconv helper', async () => {
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    initializeCheckpointRepository(workspace);
    fs.writeFileSync(path.join(workspace, '.gitattributes'), 'checkpoint.txt diff=review-evidence\n');
    execFileSync('git', ['add', '--', '.gitattributes'], { cwd: workspace });
    const checkpoint = commitCheckpoint(workspace, 'checkpoint with attributed diff\n');
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const marker = path.join(workspace, 'textconv-invoked');
    const helper = path.join(workspace, 'textconv-helper.sh');
    fs.writeFileSync(helper, `#!/bin/sh\nprintf 'invoked\\n' >> '${marker}'\nexec cat "$1"\n`, { mode: 0o700 });
    execFileSync('git', ['config', 'diff.review-evidence.textconv', helper], { cwd: workspace });

    await expect(
      dispatcher.requestReview(
        {
          checkpoint_sha: checkpoint.sha,
          source_run_id: 'source-run',
          source_iter: 3,
          scope: ['no-textconv-helper'],
          idempotency_key: 'textconv-disabled',
        },
        0,
      ),
    ).resolves.toMatchObject({ status: 'prepared' });

    expect(fs.existsSync(marker)).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('rejects a checkpoint that is not workspace HEAD before local writes, audit, session, or send effects', async () => {
    const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const first = initializeCheckpointRepository(workspace, 'checkpoint one\n');
    commitCheckpoint(workspace, 'checkpoint two\n');
    writeSourceReviewArtifacts(workspace, 'source-run', 3, first.patch);
    const openSourceLedger = vi.spyOn(SecureAutoloopLedger, 'openReadOnly');

    try {
      await expect(
        dispatcher.requestReview(
          {
            checkpoint_sha: first.sha,
            source_run_id: 'source-run',
            source_iter: 3,
            scope: ['correctness'],
            idempotency_key: 'wrong-head',
          },
          0,
        ),
      ).rejects.toThrow(/checkpoint.*HEAD|HEAD.*checkpoint/i);
      expect(openSourceLedger).not.toHaveBeenCalled();
    } finally {
      openSourceLedger.mockRestore();
    }

    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();
  });

  it('rejects a source patch that differs from the checkpoint commit before any target effect', async () => {
    const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, Buffer.from('not the checkpoint patch\n'));

    await expect(
      dispatcher.requestReview(
        {
          checkpoint_sha: checkpoint.sha,
          source_run_id: 'source-run',
          source_iter: 3,
          scope: ['correctness'],
          idempotency_key: 'wrong-patch',
        },
        0,
      ),
    ).rejects.toThrow(/diff\.patch.*checkpoint|checkpoint.*diff\.patch/i);

    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();
  });

  it('accepts an empty patch from an empty root checkpoint', async () => {
    const { dispatcher, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeEmptyRootCheckpointRepository(workspace);
    expect(checkpoint.patch).toHaveLength(0);
    writeSourceReviewArtifacts(workspace, 'source-run', 0, checkpoint.patch);

    await expect(
      dispatcher.requestReview(
        {
          checkpoint_sha: checkpoint.sha,
          source_run_id: 'source-run',
          source_iter: 0,
          scope: ['correctness'],
          idempotency_key: 'empty-root-checkpoint',
        },
        0,
      ),
    ).resolves.toMatchObject({ status: 'prepared' });

    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'diff.patch'))).toHaveLength(0);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it.skipIf(process.platform === 'win32')(
    'kills a POSIX Git evidence process group and falls back to the child handle when group kill fails',
    () => {
      const { dispatcher } = makeDispatcher();
      const internal = dispatcher as unknown as { killGitEvidenceProcess(child: ChildProcess): void };
      const processKill = vi.spyOn(process, 'kill');
      const grouped = fakeGitChild(424_242);
      const fallback = fakeGitChild(424_243);

      try {
        processKill.mockReturnValueOnce(true).mockImplementationOnce(() => {
          throw new Error('process group already unavailable');
        });

        internal.killGitEvidenceProcess(grouped as unknown as ChildProcess);
        internal.killGitEvidenceProcess(fallback as unknown as ChildProcess);

        expect(processKill).toHaveBeenNthCalledWith(1, -424_242, 'SIGKILL');
        expect(grouped.kill).not.toHaveBeenCalled();
        expect(processKill).toHaveBeenNthCalledWith(2, -424_243, 'SIGKILL');
        expect(fallback.kill).toHaveBeenCalledTimes(1);
        expect(fallback.kill).toHaveBeenCalledWith('SIGKILL');
      } finally {
        processKill.mockRestore();
      }
    },
  );

  it('does not copy Buffer chunks while collecting bounded Git evidence', async () => {
    const { dispatcher } = makeDispatcher();
    const child = fakeGitChild();
    const internal = dispatcher as unknown as {
      spawnGitEvidenceProcess(argv: string[]): ChildProcess;
      runGitEvidence(argv: string[], maxStdoutBytes: number, label: string): Promise<{ out: Buffer }>;
    };
    const spawnGit = vi.spyOn(internal, 'spawnGitEvidenceProcess').mockReturnValue(child as unknown as ChildProcess);
    const chunk = Buffer.from('bounded evidence\n');
    const run = internal.runGitEvidence(['git', 'rev-parse', 'HEAD'], 128, 'buffer identity');
    const bufferFrom = vi.spyOn(Buffer, 'from');

    try {
      child.stdout.write(chunk);
      child.emit('close', 0, null);
      const result = await run;

      expect(result.out).toEqual(chunk);
      expect(bufferFrom.mock.calls.some(([value]) => value === chunk)).toBe(false);
    } finally {
      bufferFrom.mockRestore();
      spawnGit.mockRestore();
    }
  });

  it('times out and kills a hung Git evidence command without effects, then releases the identity for retry', async () => {
    const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'hung-git-retry',
    };
    const internal = dispatcher as unknown as {
      spawnGitEvidenceProcess(argv: string[]): ChildProcess;
      reviewRequestIdentityHistory: Map<string, string>;
    };
    const hung = fakeGitChild();
    const spawnGit = vi.spyOn(internal, 'spawnGitEvidenceProcess').mockReturnValue(hung as unknown as ChildProcess);
    vi.useFakeTimers();

    try {
      const failure = expect(dispatcher.requestReview(args, 0)).rejects.toThrow(
        /Git evidence.*timed out|timed out.*Git/i,
      );
      await vi.advanceTimersByTimeAsync(30_001);
      await failure;

      expect(hung.kill).toHaveBeenCalled();
      expect(internal.reviewRequestIdentityHistory.has(reviewIdentityHash(args.idempotency_key))).toBe(false);
      expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
      expect(requestReviewDecisions(ledgerDir)).toEqual([]);
      expect(calls.startSession).not.toHaveBeenCalled();
      expect(calls.sendMessage).not.toHaveBeenCalled();
    } finally {
      spawnGit.mockRestore();
      vi.useRealTimers();
    }
    await expect(dispatcher.requestReview(args, 0)).resolves.toMatchObject({ status: 'prepared' });
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('aborts excess Git patch output without effects and allows the same request to retry', async () => {
    const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'excess-git-output-retry',
    };
    const internal = dispatcher as unknown as {
      spawnGitEvidenceProcess(argv: string[]): ChildProcess;
      reviewRequestIdentityHistory: Map<string, string>;
    };
    const originalSpawn = internal.spawnGitEvidenceProcess.bind(dispatcher);
    const excess = fakeGitChild();
    const spawnGit = vi.spyOn(internal, 'spawnGitEvidenceProcess').mockImplementation((argv) => {
      if (argv[1] !== 'show') return originalSpawn(argv);
      queueMicrotask(() => {
        excess.stdout.write(Buffer.concat([checkpoint.patch, Buffer.from('excess')]));
      });
      return excess as unknown as ChildProcess;
    });

    await expect(dispatcher.requestReview(args, 0)).rejects.toThrow(
      /checkpoint Git checkpoint patch output is longer than source diff\.patch/i,
    );

    expect(excess.kill).toHaveBeenCalled();
    expect(internal.reviewRequestIdentityHistory.has(reviewIdentityHash(args.idempotency_key))).toBe(false);
    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();

    spawnGit.mockRestore();
    await expect(dispatcher.requestReview(args, 0)).resolves.toMatchObject({ status: 'prepared' });
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it('rejects excess Git stderr without effects, kills the command, releases identity, and permits exact retry', async () => {
    const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'excess-git-stderr-retry',
    };
    const internal = dispatcher as unknown as {
      spawnGitEvidenceProcess(argv: string[]): ChildProcess;
      reviewRequestIdentityHistory: Map<string, string>;
    };
    const originalSpawn = internal.spawnGitEvidenceProcess.bind(dispatcher);
    const excess = fakeGitChild();
    const spawnGit = vi.spyOn(internal, 'spawnGitEvidenceProcess').mockImplementation((argv) => {
      if (argv[1] !== 'show') return originalSpawn(argv);
      queueMicrotask(() => excess.stderr.write(Buffer.alloc(64 * 1024 + 1, 0x65)));
      return excess as unknown as ChildProcess;
    });

    try {
      await expect(dispatcher.requestReview(args, 0)).rejects.toThrow(/stderr.*65536|65536.*stderr/i);
      expect(excess.kill).toHaveBeenCalled();
      expect(internal.reviewRequestIdentityHistory.has(reviewIdentityHash(args.idempotency_key))).toBe(false);
      expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
      expect(requestReviewDecisions(ledgerDir)).toEqual([]);
      expect(calls.startSession).not.toHaveBeenCalled();
      expect(calls.sendMessage).not.toHaveBeenCalled();
    } finally {
      spawnGit.mockRestore();
    }

    await expect(dispatcher.requestReview(args, 0)).resolves.toMatchObject({ status: 'prepared' });
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it.each(['diff.patch', 'directive.json', 'eval_output.json', 'coder_summary.txt'] as const)(
    'rejects oversized %s before target effects and lets the same key prepare once from a corrected source',
    async (artifactName) => {
      const { dispatcher, calls, workspace, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
      const checkpoint = initializeCheckpointRepository(workspace);
      const oversizedRunId = `oversized-${artifactName.replace('.', '-')}`;
      const correctedRunId = `corrected-${artifactName.replace('.', '-')}`;
      const sourceLedger = writeSourceReviewArtifacts(workspace, oversizedRunId, 3, checkpoint.patch, {
        omit: artifactName,
      });
      const oversizedPath = path.join(sourceLedger.directory, 'iter', '3', artifactName);
      const fd = fs.openSync(oversizedPath, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY, 0o600);
      try {
        fs.ftruncateSync(fd, 4_194_305);
      } finally {
        fs.closeSync(fd);
      }
      writeSourceReviewArtifacts(workspace, correctedRunId, 3, checkpoint.patch);
      const args = {
        checkpoint_sha: checkpoint.sha,
        source_run_id: oversizedRunId,
        source_iter: 3,
        scope: ['correctness'],
        idempotency_key: `oversized-${artifactName}`,
      };
      const internal = dispatcher as unknown as {
        activeReviewRequestPreparations: number;
        reviewRequests: Map<string, unknown>;
        reviewRequestIdentityHistory: Map<string, string>;
      };
      const identityHash = reviewIdentityHash(args.idempotency_key);

      await expect(dispatcher.requestReview(args, 0)).rejects.toThrow(
        new RegExp(`${artifactName.replace('.', '\\.')}.*4194304-byte limit`, 'i'),
      );

      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0'))).toBe(false);
      expect(requestReviewDecisions(ledgerDir)).toEqual([]);
      expect(internal.reviewRequests.has(identityHash)).toBe(false);
      expect(internal.reviewRequestIdentityHistory.has(identityHash)).toBe(false);
      expect(internal.activeReviewRequestPreparations).toBe(0);

      const corrected = { ...args, source_run_id: correctedRunId };
      await expect(dispatcher.requestReview(corrected, 0)).resolves.toMatchObject({ status: 'prepared' });
      await expect(dispatcher.requestReview({ ...corrected }, 0)).resolves.toMatchObject({ status: 'duplicate' });

      expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
      expect(calls.startSession).not.toHaveBeenCalled();
      expect(calls.sendMessage).not.toHaveBeenCalled();
    },
  );

  it('rejects an imported Reviewer artifact made oversized before sandbox staging without reading its content', async () => {
    const contentReads = vi.fn();
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: { beforeNestedChildContentRead: (event) => contentReads(event) },
    });
    const { dispatcher, workspace } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    await dispatcher.requestReview(
      {
        checkpoint_sha: checkpoint.sha,
        source_run_id: 'source-run',
        source_iter: 3,
        scope: ['bounded-stage'],
        idempotency_key: 'bounded-stage-after-import',
      },
      0,
    );
    const target = path.join(secureLedger.directory, 'iter', '0', 'directive.json');
    fs.truncateSync(target, 4_194_305);
    contentReads.mockClear();

    expect(() => secureLedger.stageReviewerSandbox(0)).toThrow(/directive\.json.*4194304-byte limit/i);
    expect(contentReads.mock.calls.some(([event]) => (event as { filePath: string }).filePath === target)).toBe(false);
  });

  it('uses bounded descriptor reads when an imported Reviewer artifact grows during immutable reconciliation', async () => {
    type DescriptorRead = {
      filePath: string;
      phase: 'content' | 'growth-probe';
      bufferLength: number;
      offset: number;
      length: number;
    };
    const descriptorReads: DescriptorRead[] = [];
    let target = '';
    let grow = false;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        beforeNestedChildContentRead: ({ filePath }) => {
          if (!grow || filePath !== target) return;
          grow = false;
          fs.appendFileSync(filePath, Buffer.alloc(4_194_305, 0x78));
        },
        beforeNestedChildDescriptorRead: (event) => descriptorReads.push(event),
      },
    });
    const { dispatcher, workspace } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    await dispatcher.requestReview(
      {
        checkpoint_sha: checkpoint.sha,
        source_run_id: 'source-run',
        source_iter: 3,
        scope: ['bounded-reconcile'],
        idempotency_key: 'bounded-reconcile-after-import',
      },
      0,
    );
    target = path.join(secureLedger.directory, 'iter', '0', 'directive.json');
    const exact = fs.readFileSync(target);
    descriptorReads.length = 0;
    grow = true;

    expect(() => secureLedger.writeIterationArtifact(0, 'directive.json', exact)).toThrow(
      /directive\.json.*(?:grew|4194304-byte limit)/i,
    );
    expect(descriptorReads.some(({ phase }) => phase === 'growth-probe')).toBe(true);
    expect(
      descriptorReads
        .filter(({ phase }) => phase === 'content')
        .every(({ bufferLength, offset, length }) => bufferLength <= 4_194_304 && offset + length <= 4_194_304),
    ).toBe(true);
  });

  it('rejects same-byte replacement of an imported Reviewer artifact before staging', async () => {
    let target = '';
    let replace = false;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        afterNestedChildLstat: ({ filePath }) => {
          if (!replace || filePath !== target) return;
          replace = false;
          replaceWithSameBytes(target);
        },
      },
    });
    const { dispatcher, workspace } = makeDispatcher({ secureLedger }, { sendOutput: reviewerReply });
    const checkpoint = initializeCheckpointRepository(workspace);
    writeSourceReviewArtifacts(workspace, 'source-run', 3, checkpoint.patch);
    await dispatcher.requestReview(
      {
        checkpoint_sha: checkpoint.sha,
        source_run_id: 'source-run',
        source_iter: 3,
        scope: ['replacement-defense'],
        idempotency_key: 'replacement-after-import',
      },
      0,
    );
    target = path.join(secureLedger.directory, 'iter', '0', 'directive.json');
    replace = true;

    expect(() => secureLedger.stageReviewerSandbox(0)).toThrow(/directive\.json.*identity changed/i);
  });

  it('routes a real Planner request through dispatcher preparation and suppresses its post-cache duplicate', async () => {
    const checkpoint = initializeCheckpointRepository(tmpRoot);
    writeSourceReviewArtifacts(tmpRoot, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'integrated-planner-review',
    };
    const plannerReply = ['```autoloop', JSON.stringify({ tool: 'request_review', args }), '```'].join('\n');
    const { dispatcher, ledgerDir } = makeDispatcher({}, { sendOutputs: [plannerReply, plannerReply] });
    const internal = dispatcher as unknown as { reviewRequests: Map<string, unknown> };

    const first = await dispatcher.deliver(Msg.chat(0, { text: 'review the checkpoint' }));
    internal.reviewRequests.clear();
    const duplicate = await dispatcher.deliver(Msg.chat(0, { text: 'retry the same review request' }));

    expect(first).toHaveLength(1);
    expect(validateMessage(first[0])).toMatchObject({
      type: 'review_request',
      iter: 0,
      payload: expect.objectContaining({
        iter: 0,
        checkpoint_sha: checkpoint.sha,
        idempotency_key: args.idempotency_key,
      }),
    });
    expect(duplicate).toEqual([]);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
  });

  it.each(['post-preparation active fence', 'post-application terminal fence'] as const)(
    're-arms a prepared identity across the %s and lets concurrent retries emit exactly once',
    async (boundary) => {
      const checkpoint = initializeCheckpointRepository(tmpRoot);
      writeSourceReviewArtifacts(tmpRoot, 'source-run', 3, checkpoint.patch);
      const args = {
        checkpoint_sha: checkpoint.sha,
        source_run_id: 'source-run',
        source_iter: 3,
        scope: ['correctness'],
        idempotency_key: `rearm-${boundary.replaceAll(' ', '-')}`,
      };
      const plannerReply = ['```autoloop', JSON.stringify({ tool: 'request_review', args }), '```'].join('\n');
      const { dispatcher, ledgerDir } = makeDispatcher(
        {},
        { sendOutputs: [plannerReply, plannerReply, plannerReply, plannerReply] },
      );
      const internal = dispatcher as unknown as {
        terminal: boolean;
        requestReview(input: typeof args, targetIter: number): Promise<ReviewRequestPreparationResult>;
        prepareCheckpointReview(input: typeof args, targetIter: number, digest: string): Promise<PreparedReviewRequest>;
      };
      const prepare = vi.spyOn(internal, 'prepareCheckpointReview');
      const requestReview = internal.requestReview.bind(dispatcher);
      let injectBoundary = true;
      vi.spyOn(internal, 'requestReview').mockImplementation(async (request, targetIter) => {
        const result = await requestReview(request, targetIter);
        if (!injectBoundary || result.status !== 'prepared') return result;
        injectBoundary = false;
        if (boundary === 'post-preparation active fence') {
          internal.terminal = true;
        } else {
          let terminalReads = 0;
          Object.defineProperty(internal, 'terminal', {
            configurable: true,
            get: () => {
              terminalReads += 1;
              return terminalReads >= 2;
            },
            set: () => undefined,
          });
        }
        return result;
      });

      const interrupted = dispatcher.deliver(Msg.chat(0, { text: 'prepare then interrupt handoff' }));
      if (boundary === 'post-preparation active fence') {
        await expect(interrupted).rejects.toMatchObject({ code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED' });
      } else {
        await expect(interrupted).resolves.toEqual([]);
      }
      expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);

      Object.defineProperty(internal, 'terminal', {
        configurable: true,
        enumerable: true,
        value: false,
        writable: true,
      });
      const concurrent = await Promise.all([
        dispatcher.deliver(Msg.chat(0, { text: 'retry prepared review A' })),
        dispatcher.deliver(Msg.chat(0, { text: 'retry prepared review B' })),
      ]);
      const emitted = concurrent.flat();

      expect(concurrent.map((messages) => messages.length).sort()).toEqual([0, 1]);
      expect(emitted).toHaveLength(1);
      expect(validateMessage(emitted[0])).toMatchObject({
        type: 'review_request',
        payload: expect.objectContaining({ idempotency_key: args.idempotency_key }),
      });
      await expect(dispatcher.deliver(Msg.chat(0, { text: 'retry after accepted handoff' }))).resolves.toEqual([]);
      expect(prepare).toHaveBeenCalledOnce();
      expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
    },
  );

  it('rejects a combined Planner review batch before preparation and lets the same standalone key emit exactly once', async () => {
    const checkpoint = initializeCheckpointRepository(tmpRoot);
    writeSourceReviewArtifacts(tmpRoot, 'source-run', 3, checkpoint.patch);
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'singleton-integrated-review',
    };
    const reviewControl = { tool: 'request_review', args };
    const combinedReply = [
      '```autoloop',
      JSON.stringify(reviewControl),
      '```',
      '```autoloop',
      JSON.stringify({ tool: 'notify_user', args: { summary: 'must not emit' } }),
      '```',
    ].join('\n');
    const standaloneReply = ['```autoloop', JSON.stringify(reviewControl), '```'].join('\n');
    const { dispatcher, calls, ledgerDir } = makeDispatcher(
      {},
      { sendOutputs: [combinedReply, standaloneReply, standaloneReply] },
    );
    const internal = dispatcher as unknown as {
      reviewRequests: Map<string, unknown>;
      reviewRequestIdentityHistory: Map<string, string>;
    };

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'invalid combined review' }))).rejects.toMatchObject({
      code: 'AUTOLOOP_CONTROL_MALFORMED',
    });
    expect(internal.reviewRequests.size).toBe(0);
    expect(internal.reviewRequestIdentityHistory.size).toBe(0);
    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);

    const first = await dispatcher.deliver(Msg.chat(0, { text: 'standalone review' }));
    const duplicate = await dispatcher.deliver(Msg.chat(0, { text: 'duplicate standalone review' }));

    expect(first).toHaveLength(1);
    expect(validateMessage(first[0])).toMatchObject({
      type: 'review_request',
      payload: expect.objectContaining({ idempotency_key: args.idempotency_key }),
    });
    expect(duplicate).toEqual([]);
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
    expect(calls.startSession.mock.calls.map(([config]) => (config as { name: string }).name)).toEqual([
      'autoloop-r1-planner',
    ]);
    expect(calls.sendMessage).toHaveBeenCalledTimes(3);
  });

  it('keeps immutable imports on audit append failure, emits nothing, and succeeds exactly once on retry', async () => {
    let decisionAppends = 0;
    let failRequestAudit = true;
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name !== 'decisions.jsonl' || operation !== 'append') return;
          decisionAppends += 1;
          if (failRequestAudit && decisionAppends === 2) {
            failRequestAudit = false;
            throw new Error('deterministic request_review audit append failure');
          }
        },
      },
    });
    const checkpoint = initializeCheckpointRepository(tmpRoot);
    writeSourceReviewArtifacts(tmpRoot, 'source-run', 3, checkpoint.patch);
    writeSourceReviewArtifacts(tmpRoot, 'conflicting-source', 3, checkpoint.patch, {
      directive: '{"goal":"conflicting bytes"}\n',
    });
    const args = {
      checkpoint_sha: checkpoint.sha,
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'audit-retry',
    };
    const plannerReply = ['```autoloop', JSON.stringify({ tool: 'request_review', args }), '```'].join('\n');
    const { dispatcher, calls, ledgerDir } = makeDispatcher(
      { secureLedger },
      { sendOutputs: [plannerReply, plannerReply] },
    );
    const internal = dispatcher as unknown as {
      reviewRequests: Map<string, unknown>;
      reviewRequestIdentityHistory: Map<string, string>;
    };
    const unrelatedIdentityHash = reviewIdentityHash('unrelated-accepted-identity');
    internal.reviewRequestIdentityHistory.set(unrelatedIdentityHash, 'f'.repeat(64));
    internal.reviewRequests.set(unrelatedIdentityHash, { digest: 'f'.repeat(64), settled: true });

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'prepare review' }))).rejects.toMatchObject({
      code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
    });
    const importedBeforeRetry = new Map(
      ['directive.json', 'eval_output.json', 'coder_summary.txt', 'diff.patch'].map((name) => [
        name,
        fs.readFileSync(path.join(ledgerDir, 'iter', '0', name)),
      ]),
    );
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);
    expect(internal.reviewRequests.has(reviewIdentityHash(args.idempotency_key))).toBe(false);
    expect(internal.reviewRequestIdentityHistory.has(reviewIdentityHash(args.idempotency_key))).toBe(false);
    expect(internal.reviewRequests.has(unrelatedIdentityHash)).toBe(true);
    expect(internal.reviewRequestIdentityHistory.get(unrelatedIdentityHash)).toBe('f'.repeat(64));

    await expect(
      dispatcher.requestReview(
        {
          ...args,
          source_run_id: 'conflicting-source',
          idempotency_key: 'conflicting-import',
        },
        0,
      ),
    ).rejects.toThrow(/artifact.*different|different.*artifact|immutable|already exists/i);
    for (const [name, content] of importedBeforeRetry) {
      expect(fs.readFileSync(path.join(ledgerDir, 'iter', '0', name))).toEqual(content);
    }

    const retry = await dispatcher.deliver(Msg.chat(0, { text: 'retry review preparation' }));

    expect(retry).toHaveLength(1);
    expect(validateMessage(retry[0])).toMatchObject({
      type: 'review_request',
      payload: expect.objectContaining({ idempotency_key: args.idempotency_key }),
    });
    expect(requestReviewDecisions(ledgerDir)).toHaveLength(1);
    expect(calls.sendMessage).toHaveBeenCalledTimes(2);
    for (const [name, content] of importedBeforeRetry) {
      expect(fs.readFileSync(path.join(ledgerDir, 'iter', '0', name))).toEqual(content);
    }
  });

  it.each([
    ['padded scope member', { scope: [' security'] }],
    ['too many scope members', { scope: Array.from({ length: 129 }, () => 'security') }],
    ['oversized UTF-8 scope member', { scope: ['é'.repeat(4_097)] }],
    ['oversized UTF-8 idempotency key', { idempotency_key: 'é'.repeat(4_097) }],
  ])('rejects direct requestReview with %s before ledger, session, or send effects', async (_label, override) => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });

    await expect(
      dispatcher.requestReview(
        {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'direct-validation',
          ...override,
        },
        0,
      ),
    ).rejects.toThrow(/scope|idempotency_key/i);

    expect(fs.existsSync(path.join(ledgerDir, 'iter'))).toBe(false);
    expect(requestReviewDecisions(ledgerDir)).toEqual([]);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();
  });

  it('rejects hostile scope accessors before ledger, session, or send effects', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    let getterCalls = 0;
    const scope: string[] = [];
    Object.defineProperty(scope, '0', {
      enumerable: true,
      get: () => {
        getterCalls += 1;
        throw new Error('scope getter must not run');
      },
    });
    scope.length = 1;
    const requestReview = (
      dispatcher as unknown as {
        requestReview(input: Record<string, unknown>, targetIter: number): Promise<Record<string, unknown>>;
      }
    ).requestReview.bind(dispatcher);

    await expect(
      requestReview(
        {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope,
          idempotency_key: 'hostile-scope',
        },
        0,
      ),
    ).rejects.toThrow(/scope.*array|scope.*own|scope.*data/i);

    expect(getterCalls).toBe(0);
    expect(calls.startSession).not.toHaveBeenCalled();
    expect(calls.sendMessage).not.toHaveBeenCalled();
    expect(fs.readdirSync(ledgerDir).sort()).not.toContain('iter');
  });

  it('canonicalizes checkpoint review_request payloads and rejects partial or hostile variants', () => {
    const checkpoint = 'A'.repeat(40);
    const valid = validateMessage(
      Msg.reviewRequest(0, {
        iter: 0,
        ledger_path: '/trusted/run',
        prior_metrics: [],
        checkpoint_sha: checkpoint,
        source_run_id: 'source-run',
        source_iter: 3,
        scope: ['security'],
        idempotency_key: 'review-source-run-7',
      } as never),
    );
    expect(valid.payload).toMatchObject({ checkpoint_sha: checkpoint.toLowerCase(), scope: ['security'] });
    expect(Object.isFrozen((valid.payload as { scope: string[] }).scope)).toBe(true);

    expect(() =>
      validateMessage(
        Msg.reviewRequest(7, {
          iter: 7,
          ledger_path: '/trusted/run',
          prior_metrics: [],
          checkpoint_sha: checkpoint,
        } as never),
      ),
    ).toThrow(/source_run_id.*own data/i);

    let getterCalls = 0;
    const hostileScope: string[] = [];
    Object.defineProperty(hostileScope, '0', {
      enumerable: true,
      get: () => {
        getterCalls += 1;
        return 'security';
      },
    });
    hostileScope.length = 1;
    expect(() =>
      validateMessage(
        Msg.reviewRequest(7, {
          iter: 7,
          ledger_path: '/trusted/run',
          prior_metrics: [],
          checkpoint_sha: checkpoint,
          source_run_id: 'source-run',
          source_iter: 7,
          scope: hostileScope,
          idempotency_key: 'hostile-message',
        } as never),
      ),
    ).toThrow(/scope.*array|scope.*own|scope.*data/i);
    expect(getterCalls).toBe(0);

    const inherited = Object.create({ inherited: 'must not cross the message boundary' }) as Record<string, unknown>;
    Object.assign(inherited, {
      iter: 7,
      ledger_path: '/trusted/run',
      prior_metrics: [],
      checkpoint_sha: checkpoint,
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'hostile-prototype',
    });
    expect(() => validateMessage(Msg.reviewRequest(7, inherited as never))).toThrow(/prototype|inherited/i);

    const inheritedScope = ['security'];
    Object.setPrototypeOf(inheritedScope, Object.create(Array.prototype));
    expect(() =>
      validateMessage(
        Msg.reviewRequest(7, {
          iter: 7,
          ledger_path: '/trusted/run',
          prior_metrics: [],
          checkpoint_sha: checkpoint,
          source_run_id: 'source-run',
          source_iter: 7,
          scope: inheritedScope,
          idempotency_key: 'hostile-scope-prototype',
        } as never),
      ),
    ).toThrow(/scope.*prototype|scope.*inherited/i);
  });
});

describe('ClaudeAgentDispatcher — frozen reviewer memory', () => {
  it('injects reviewer_memory.md contents into the Reviewer system prompt at startSession', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher();
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.mkdirSync(sandbox, { recursive: true });
    fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'Pattern: ZEBRA_OFFSET = sentinel\n');

    await dispatcher.spawnSubagents();

    // Reviewer is the second startSession call (after Coder).
    const reviewerStart = calls.startSession.mock.calls.find(
      (c) => (c[0] as { name: string }).name === 'autoloop-r1-reviewer',
    );
    expect(reviewerStart).toBeDefined();
    const sp = (reviewerStart![0] as { systemPrompt: string }).systemPrompt;
    expect(sp).toContain('<frozen_memory_snapshot>');
    expect(sp).toContain('Pattern: ZEBRA_OFFSET = sentinel');
  });

  it('omits the frozen snapshot tag when reviewer_memory.md is missing', async () => {
    const { dispatcher, calls } = makeDispatcher();
    await dispatcher.spawnSubagents();
    const reviewerStart = calls.startSession.mock.calls.find(
      (c) => (c[0] as { name: string }).name === 'autoloop-r1-reviewer',
    );
    const sp = (reviewerStart![0] as { systemPrompt: string }).systemPrompt;
    expect(sp).not.toContain('<frozen_memory_snapshot>');
  });

  it.each([
    ['reviewer_memory.md', 'symlink'],
    ['reviewer_log.jsonl', 'hardlink'],
  ] as const)('rejects an unsafe %s %s before starting the Reviewer', async (name, kind) => {
    const { dispatcher, calls, ledgerDir, workspace } = makeDispatcher();
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    const external = path.join(workspace, `external-${name}`);
    fs.mkdirSync(sandbox);
    fs.writeFileSync(external, 'must remain external');
    if (kind === 'symlink') fs.symlinkSync(external, path.join(sandbox, name));
    else fs.linkSync(external, path.join(sandbox, name));

    await expect(dispatcher.spawnSubagents()).rejects.toThrow(/symbolic link|hardlink|link count|unsafe/i);

    expect(
      calls.startSession.mock.calls.some(([config]) => (config as { name: string }).name === 'autoloop-r1-reviewer'),
    ).toBe(false);
    expect(calls.sendMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(external, 'utf8')).toBe('must remain external');
  });

  it('rejects a directory masquerading as an allowed staged artifact before starting the Reviewer', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher();
    const ledger = dispatcher.secureLedgerCapability;
    ensureCompleteReviewArtifacts(dispatcher, 0);
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    const staged = path.join(sandbox, 'iter-0');
    fs.mkdirSync(path.join(staged, 'directive.json'), { recursive: true });
    fs.writeFileSync(path.join(staged, 'directive.json', 'nested-unapproved.txt'), 'must not be accepted');
    for (const name of ['eval_output.json', 'coder_summary.txt', 'diff.patch'] as const) {
      fs.writeFileSync(path.join(staged, name), ledger.readIterationArtifact(0, name)!);
    }

    await expect(dispatcher.spawnSubagents()).rejects.toThrow(/regular|directory|type|staged artifact/i);

    expect(
      calls.startSession.mock.calls.some(([config]) => (config as { name: string }).name === 'autoloop-r1-reviewer'),
    ).toBe(false);
    expect(calls.sendMessage).not.toHaveBeenCalled();
  });

  it('keeps the non-Claude Reviewer memory snapshot frozen after session start', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ reviewerEngine: 'gemini' });
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.mkdirSync(sandbox, { recursive: true });
    fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'frozen-old-memory');
    await dispatcher.spawnSubagents();
    fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'new-memory-must-wait-for-reset');
    ensureCompleteReviewArtifacts(dispatcher, 0);

    await dispatcher.deliver(
      Msg.reviewRequest(0, {
        iter: 0,
        ledger_path: ledgerDir,
        prior_metrics: [],
      }),
    );

    const prompt = calls.sendMessage.mock.calls[0][1] as string;
    expect(prompt).toContain('frozen-old-memory');
    expect(prompt).not.toContain('new-memory-must-wait-for-reset');
  });
});

describe('ClaudeAgentDispatcher — phase_error surfacing', () => {
  it('throws the retryable typed error when a Planner transport returns an empty logical reply', async () => {
    const { dispatcher } = makeDispatcher({}, { sendOutput: '   ' });

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'return a reply' }))).rejects.toMatchObject({
      name: 'AutoloopOperationError',
      code: 'AUTOLOOP_EMPTY_REPLY',
      retryable: true,
    });
  });

  it('returns a phase_error envelope (not a fake directive_ack) when Coder send fails twice', async () => {
    vi.useFakeTimers();
    const { dispatcher } = makeDispatcher({}, { sendThrows: 2 });
    await dispatcher.spawnSubagents();
    const pending = dispatcher.deliver(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
    );
    void pending.catch(() => undefined);
    await vi.runAllTimersAsync();
    const replies = await pending;
    expect(replies).toHaveLength(1);
    expect(replies[0].type).toBe('phase_error');
    if (replies[0].type === 'phase_error') {
      expect(replies[0].payload.agent).toBe('coder');
      expect(replies[0].payload.phase).toBe('send');
    }
  });
});

describe('ClaudeAgentDispatcher — recoverable send timeout and dispatch identity', () => {
  const roleCases: Array<['planner' | 'coder' | 'reviewer', (ledgerDir: string) => AnyAutoloopMessage]> = [
    ['planner', () => fixedIdentity(Msg.chat(3, { text: 'continue' }), 'logical-planner-3')],
    [
      'coder',
      () =>
        fixedIdentity(
          Msg.directive(3, { goal: 'ship I3', constraints: [], success_criteria: [], max_attempts: 1 }),
          'logical-coder-3',
        ),
    ],
    [
      'reviewer',
      (ledgerDir) =>
        fixedIdentity(
          Msg.reviewRequest(3, { iter: 3, ledger_path: ledgerDir, prior_metrics: [] }),
          'logical-reviewer-3',
        ),
    ],
  ];

  it.each(roleCases)(
    'classifies a genuine %s send timeout once without reset or automatic retry',
    async (role, makeMessage) => {
      vi.useFakeTimers();
      const { dispatcher, calls, ledgerDir } = makeDispatcher({ sendTimeoutMs: 7_200_000 });
      calls.sendMessage.mockRejectedValue(genuineSendTimeout());
      const message = makeMessage(ledgerDir);
      if (role === 'reviewer') ensureCompleteReviewArtifacts(dispatcher, message.iter);

      const pending = dispatcher.deliver(message);
      void pending.catch(() => undefined);
      await vi.runAllTimersAsync();
      const observed = sendTimeout(await pending);

      expect(observed.payload).toMatchObject({
        status: 'awaiting_resume',
        agent: role,
        message_id: message.msg_id,
        message_type: message.type,
        iter: message.iter,
        timeout_ms: 7_200_000,
        error: 'Timeout waiting for response',
      });
      expect(observed.payload.dispatch_id).toMatch(/^dispatch_[a-f0-9]{64}$/);
      expect(calls.sendMessage).toHaveBeenCalledTimes(1);
      expect(calls.stopSession).not.toHaveBeenCalled();
    },
  );

  it('keeps a non-timeout failure on the reset-once/retry-once phase_error path', async () => {
    vi.useFakeTimers();
    const { dispatcher, calls } = makeDispatcher();
    calls.sendMessage.mockRejectedValue(new Error('subprocess failed while loading timeout configuration'));
    const message = fixedIdentity(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
      'logical-non-timeout',
    );

    const pending = dispatcher.deliver(message);
    void pending.catch(() => undefined);
    await vi.runAllTimersAsync();
    const replies = await pending;

    expect(replies).toHaveLength(1);
    expect(replies[0].type).toBe('phase_error');
    expect(calls.sendMessage).toHaveBeenCalledTimes(2);
    expect(calls.stopSession).toHaveBeenCalledTimes(1);
  });

  it('coalesces concurrent and later re-delivery of one logical dispatch, while distinct identities still send', async () => {
    const { dispatcher, calls } = makeDispatcher();
    let resolveSend!: (value: { output: string; error: undefined }) => void;
    const underlying = new Promise<{ output: string; error: undefined }>((resolve) => {
      resolveSend = resolve;
    });
    calls.sendMessage.mockReturnValue(underlying);
    const first = fixedIdentity(Msg.chat(2, { text: 'same logical turn' }), 'logical-chat-a');
    const duplicate = fixedIdentity(
      Msg.chat(2, { text: 'same logical turn' }),
      'logical-chat-a',
      '2035-01-01T00:00:00.000Z',
    );

    const deliveryA = dispatcher.deliver(first);
    const deliveryB = dispatcher.deliver(duplicate);
    await vi.waitFor(() => expect(calls.sendMessage).toHaveBeenCalledTimes(1));

    resolveSend({ output: 'coalesced Planner reply', error: undefined });
    await Promise.all([deliveryA, deliveryB]);
    await dispatcher.deliver(duplicate);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);

    await dispatcher.deliver(fixedIdentity(Msg.chat(2, { text: 'same logical turn' }), 'logical-chat-b'));
    expect(calls.sendMessage).toHaveBeenCalledTimes(2);
  });

  it('bounds the retained dispatch cache while still coalescing a recent re-delivery', async () => {
    const { dispatcher, calls } = makeDispatcher();
    calls.sendMessage.mockResolvedValue({ output: 'Planner reply', error: undefined });
    const retained = dispatcher as unknown as { logicalDispatches: Map<string, unknown> };

    const recent = fixedIdentity(Msg.chat(1, { text: 'recent turn' }), 'logical-recent');
    await dispatcher.deliver(recent);
    const sendsAfterRecent = calls.sendMessage.mock.calls.length;

    // Far more distinct dispatches than the cache is allowed to hold.
    for (let i = 0; i < 200; i++) {
      await dispatcher.deliver(fixedIdentity(Msg.chat(1, { text: `turn ${i}` }), `logical-bulk-${i}`));
    }

    expect(retained.logicalDispatches.size).toBeLessThanOrEqual(64);

    // A dispatch well inside the retention window is still deduped, not re-sent.
    // Deliberately not the very last one: that survives even a cache of size 1,
    // so it could not tell a real window from a degenerate one.
    const sendsBeforeReplay = calls.sendMessage.mock.calls.length;
    await dispatcher.deliver(fixedIdentity(Msg.chat(1, { text: 'turn 190' }), 'logical-bulk-190'));
    expect(calls.sendMessage.mock.calls.length).toBe(sendsBeforeReplay);
    expect(sendsAfterRecent).toBeGreaterThan(0);
  });

  it('derives the same ID across dispatcher instances without using envelope time, but separates message identities', async () => {
    vi.useFakeTimers();
    const firstHarness = makeDispatcher({ sendTimeoutMs: 7_200_000 });
    const secondHarness = makeDispatcher({ sendTimeoutMs: 7_200_000 });
    firstHarness.calls.sendMessage.mockRejectedValue(genuineSendTimeout());
    secondHarness.calls.sendMessage.mockRejectedValue(genuineSendTimeout());
    const first = fixedIdentity(Msg.chat(4, { text: 'logical input' }), 'logical-stable-id', '2020-01-01T00:00:00Z');
    const sameLogical = fixedIdentity(
      Msg.chat(4, { text: 'logical input' }),
      'logical-stable-id',
      '2040-01-01T00:00:00Z',
    );
    const distinct = fixedIdentity(Msg.chat(4, { text: 'logical input' }), 'logical-distinct-id');

    const firstPending = firstHarness.dispatcher.deliver(first);
    const samePending = secondHarness.dispatcher.deliver(sameLogical);
    const distinctPending = firstHarness.dispatcher.deliver(distinct);
    for (const pending of [firstPending, samePending, distinctPending]) void pending.catch(() => undefined);
    await vi.runAllTimersAsync();
    const [firstResult, sameResult, distinctResult] = await Promise.all([firstPending, samePending, distinctPending]);
    const firstId = sendTimeout(firstResult).payload.dispatch_id;
    const sameId = sendTimeout(sameResult).payload.dispatch_id;
    const distinctId = sendTimeout(distinctResult).payload.dispatch_id;

    expect(sameId).toBe(firstId);
    expect(distinctId).not.toBe(firstId);
  });

  it('records one deterministic pending-dispatch audit row even when the logical dispatch is re-delivered', async () => {
    vi.useFakeTimers();
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ sendTimeoutMs: 7_200_000 });
    calls.sendMessage.mockRejectedValue(genuineSendTimeout());
    const message = fixedIdentity(Msg.chat(5, { text: 'audit this pending turn' }), 'logical-audit-id');

    const firstPending = dispatcher.deliver(message);
    void firstPending.catch(() => undefined);
    await vi.runAllTimersAsync();
    const first = sendTimeout(await firstPending);
    const second = sendTimeout(await dispatcher.deliver({ ...message }));
    const rows = fs
      .readFileSync(path.join(ledgerDir, 'decisions.jsonl'), 'utf-8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> })
      .filter((row) => row.kind === 'send_timeout');

    expect(second.payload.dispatch_id).toBe(first.payload.dispatch_id);
    expect(rows).toHaveLength(1);
    expect(rows[0].payload).toEqual(first.payload);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
  });
});

describe('AutoloopRunner — recoverable dispatcher timeout state', () => {
  function makeTimeoutRunner(hardTimeoutMs = 86_400_000): {
    runner: AutoloopRunner;
    calls: StubCalls;
    rejectSend: (error: Error) => void;
  } {
    const { dispatcher, calls, ledgerDir, workspace } = makeDispatcher({ sendTimeoutMs: 7_200_000 });
    let rejectSend!: (error: Error) => void;
    calls.sendMessage.mockImplementation(
      () =>
        new Promise((_resolve, reject) => {
          rejectSend = reject;
        }),
    );
    const runner = new AutoloopRunner({
      run_id: 'r1',
      workspace,
      ledger_dir: ledgerDir,
      dispatcher,
      notifyUser: vi.fn(async () => undefined),
      sendTimeoutMs: 7_200_000,
      activityLeaseMs: 7_200_000,
      autoloopHardTimeoutMs: hardTimeoutMs,
    });
    return { runner, calls, rejectSend: (error) => rejectSend(error) };
  }

  it('pauses in awaiting-resume state with pending metadata and one structured timeout event', async () => {
    const { runner, calls, rejectSend } = makeTimeoutRunner();
    const timeoutEvents: ObservedSendTimeout['payload'][] = [];
    runner.on('send_timeout', (event) => timeoutEvents.push(event as ObservedSendTimeout['payload']));
    await runner.start();

    const chat = runner.chat('wait for planner');
    await Promise.resolve();
    rejectSend(genuineSendTimeout());
    await chat;

    const state = runner.state as typeof runner.state & {
      pending_dispatch: ObservedSendTimeout['payload'] | null;
    };
    expect(state.status).toBe('paused');
    expect(state.status_reason).toBe(
      `awaiting_resume:send_timeout:planner:${state.pending_dispatch?.dispatch_id ?? ''}`,
    );
    expect(state.pending_dispatch).toMatchObject({
      status: 'awaiting_resume',
      agent: 'planner',
      timeout_ms: 7_200_000,
    });
    expect(timeoutEvents).toEqual([state.pending_dispatch]);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
    runner.stop();
  });

  it('keeps the awaiting-resume timeout reason stable instead of letting the idle lease overwrite it', async () => {
    vi.useFakeTimers();
    const { runner, rejectSend } = makeTimeoutRunner();
    const lifecycleTimeouts: Array<{ kind: string }> = [];
    runner.on('timeout', (event) => lifecycleTimeouts.push(event as { kind: string }));
    await runner.start();

    const chat = runner.chat('wait for planner');
    await Promise.resolve();
    rejectSend(genuineSendTimeout());
    await chat;
    const reason = runner.state.status_reason;

    await runner.send(Msg.resume(0));
    await vi.advanceTimersByTimeAsync(7_200_000);
    expect(runner.state.status).toBe('paused');
    expect(runner.state.status_reason).toBe(reason);
    expect(lifecycleTimeouts).toEqual([]);
    runner.stop();
  });

  it('keeps operator termination terminal when its queued stop wins a late timeout result', async () => {
    const { runner, rejectSend } = makeTimeoutRunner();
    const timeoutEvents: unknown[] = [];
    runner.on('send_timeout', (event) => timeoutEvents.push(event));
    await runner.start();

    const chat = runner.chat('slow planner turn');
    await Promise.resolve();
    await runner.send(Msg.terminate(0, { reason: 'operator_stop' }));
    rejectSend(genuineSendTimeout());
    await chat;

    const state = runner.state as typeof runner.state & { pending_dispatch: unknown | null };
    expect(state.status).toBe('terminated');
    expect(state.status_reason).toBe('operator_stop');
    expect(state.pending_dispatch).toBeNull();
    expect(timeoutEvents).toEqual([]);
  });

  it('keeps the absolute hard timeout terminal when an in-flight send times out later', async () => {
    vi.useFakeTimers();
    const { runner, rejectSend } = makeTimeoutRunner(600_000);
    const sendTimeoutEvents: unknown[] = [];
    const deadlineEvents: Array<{ kind: string }> = [];
    runner.on('send_timeout', (event) => sendTimeoutEvents.push(event));
    runner.on('timeout', (event) => deadlineEvents.push(event as { kind: string }));
    await runner.start();

    const chat = runner.chat('outlive hard cap');
    await Promise.resolve();
    await vi.advanceTimersByTimeAsync(600_000);
    expect(runner.state.status).toBe('terminated');
    rejectSend(genuineSendTimeout());
    await chat;

    const state = runner.state as typeof runner.state & { pending_dispatch: unknown | null };
    expect(state.status).toBe('terminated');
    expect(state.status_reason).toBe('hard_timeout_exceeded');
    expect(state.pending_dispatch).toBeNull();
    expect(sendTimeoutEvents).toEqual([]);
    expect(deadlineEvents.map((event) => event.kind)).toEqual(['hard_timeout_exceeded']);
  });
});

describe('ClaudeAgentDispatcher — updatePushPolicy guard', () => {
  it('rejects an atomic policy batch that weakens required decision severity without persisting controls or effects', async () => {
    const policyRef: PushPolicy = JSON.parse(JSON.stringify(DEFAULT_PUSH_POLICY));
    const policyBefore = JSON.stringify(policyRef);
    const reply = `OK
\`\`\`autoloop
{"tool": "update_push_policy", "args": {"on_phase_error": {"silent": true, "channel": "email"}, "on_decision_needed": {"silent": true, "level": "warn"}, "on_target_hit": {"silent": true}}}
\`\`\`
`;
    const { dispatcher, ledgerDir } = makeDispatcher({ pushPolicyRef: policyRef }, { sendOutput: reply });
    const surfacedReplies: string[] = [];
    dispatcher.on('planner_reply', (surfacedReply) => surfacedReplies.push(String(surfacedReply)));

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'weaken the critical policy' }))).rejects.toMatchObject({
      code: 'AUTOLOOP_CONTROL_MALFORMED',
      retryable: false,
      message: expect.stringContaining("on_decision_needed level 'warn' weakens required level 'decision'"),
    });

    expect(JSON.stringify(policyRef)).toBe(policyBefore);

    const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
    const lines = fs.existsSync(decisionsPath)
      ? fs
          .readFileSync(decisionsPath, 'utf-8')
          .trim()
          .split('\n')
          .filter(Boolean)
          .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> })
      : [];
    expect(lines.filter((line) => line.kind === 'planner_turn_control')).toEqual([]);
    expect(lines.filter((line) => line.kind === 'update_push_policy')).toEqual([]);
    expect(lines.filter((line) => line.kind === 'policy_silence_blocked')).toEqual([]);
    expect(surfacedReplies).toEqual([]);
  });

  it.each(['on_phase_error', 'on_decision_needed'] as const)(
    'classifies a prohibited silence-only %s control as non-retryable malformed input before durable persistence',
    async (key) => {
      const policyRef: PushPolicy = JSON.parse(JSON.stringify(DEFAULT_PUSH_POLICY));
      const policyBefore = JSON.stringify(policyRef);
      const reply = [
        '```autoloop',
        JSON.stringify({ tool: 'update_push_policy', args: { [key]: { silent: true } } }),
        '```',
      ].join('\n');
      const { dispatcher, ledgerDir } = makeDispatcher({ pushPolicyRef: policyRef }, { sendOutput: reply });
      const surfacedReplies: string[] = [];
      dispatcher.on('planner_reply', (surfacedReply) => surfacedReplies.push(String(surfacedReply)));

      await expect(dispatcher.deliver(Msg.chat(0, { text: 'do not silence critical policy' }))).rejects.toMatchObject({
        code: 'AUTOLOOP_CONTROL_MALFORMED',
        retryable: false,
        message: expect.stringContaining('cannot contain only prohibited critical policy silence'),
      });

      expect(JSON.stringify(policyRef)).toBe(policyBefore);
      const decisionText = fs.readFileSync(path.join(ledgerDir, 'decisions.jsonl'), 'utf-8');
      const lines = decisionText
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> });
      expect(lines.filter((line) => line.kind === 'planner_turn_control')).toEqual([]);
      expect(lines.filter((line) => line.kind === 'policy_silence_blocked')).toEqual([]);
      expect(lines.filter((line) => line.kind === 'update_push_policy')).toEqual([]);
      expect(decisionText).not.toContain('"silent":true');
      expect(surfacedReplies).toEqual([]);
    },
  );

  it.each(['on_phase_error', 'on_decision_needed'] as const)(
    'retains the mandatory minimum for an explicit empty %s rule without a silence-blocked audit',
    async (key) => {
      const policyRef: PushPolicy = JSON.parse(JSON.stringify(DEFAULT_PUSH_POLICY));
      const requiredRule =
        key === 'on_phase_error' ? { level: 'error', channel: 'both' } : { level: 'decision', channel: 'both' };
      const reply = ['```autoloop', JSON.stringify({ tool: 'update_push_policy', args: { [key]: {} } }), '```'].join(
        '\n',
      );
      const { dispatcher, ledgerDir } = makeDispatcher({ pushPolicyRef: policyRef }, { sendOutput: reply });
      const surfacedReplies: string[] = [];
      dispatcher.on('planner_reply', (surfacedReply) => surfacedReplies.push(String(surfacedReply)));

      await expect(dispatcher.deliver(Msg.chat(0, { text: 'reset the critical policy rule' }))).resolves.toEqual([]);

      expect(policyRef[key]).toEqual(requiredRule);
      const lines = fs
        .readFileSync(path.join(ledgerDir, 'decisions.jsonl'), 'utf-8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> });
      expect(lines.find((line) => line.kind === 'planner_turn_control')?.payload.controls).toEqual([
        { tool: 'update_push_policy', args: { [key]: {} } },
      ]);
      expect(lines.find((line) => line.kind === 'update_push_policy')?.payload).toEqual({
        applied: { [key]: requiredRule },
      });
      expect(lines.filter((line) => line.kind === 'policy_silence_blocked')).toEqual([]);
      expect(surfacedReplies).toEqual(['Planner controls persisted: update_push_policy']);
    },
  );
});

describe('ClaudeAgentDispatcher — stageReviewSandbox whitelist', () => {
  it('preserves reviewer_memory.md AND reviewer_log.jsonl across iters', async () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    await dispatcher.spawnSubagents();
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'memory');
    fs.writeFileSync(path.join(sandbox, 'reviewer_log.jsonl'), '{"a":1}\n');
    fs.writeFileSync(path.join(sandbox, 'scratch.txt'), 'temp');
    ensureCompleteReviewArtifacts(dispatcher, 0);

    // Reviewer needs to actually emit a review_complete or we'll observe a
    // 'hold' fallback. We just stub sendOutput to include a valid block.
    // Easier: directly call the private method via type assertion.
    (dispatcher as unknown as { stageReviewSandbox(iter: number): void }).stageReviewSandbox(0);

    expect(fs.existsSync(path.join(sandbox, 'reviewer_memory.md'))).toBe(true);
    expect(fs.existsSync(path.join(sandbox, 'reviewer_log.jsonl'))).toBe(true);
    expect(fs.existsSync(path.join(sandbox, 'scratch.txt'))).toBe(false);
  });

  it('does not send a Coder turn when directive persistence is unsafe', async () => {
    const { dispatcher, calls, ledgerDir, workspace } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    const iterDir = path.join(ledgerDir, 'iter', '0');
    const external = path.join(workspace, 'external-directive');
    fs.mkdirSync(iterDir, { recursive: true });
    fs.writeFileSync(external, 'sentinel');
    fs.symlinkSync(external, path.join(iterDir, 'directive.json'));

    await expect(
      dispatcher.deliver(Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 })),
    ).rejects.toThrow(/symbolic link|unsafe/i);

    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(external, 'utf8')).toBe('sentinel');
  });

  it('does not send a Reviewer turn through a pre-planted sandbox destination', async () => {
    const { dispatcher, calls, ledgerDir, workspace } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    const ledger = dispatcher.secureLedgerCapability;
    ledger.writeIterationArtifact(0, 'directive.json', '{}');
    ledger.writeIterationArtifact(0, 'eval_output.json', '{}');
    ledger.writeIterationArtifact(0, 'coder_summary.txt', 'complete');
    ledger.writeIterationArtifact(0, 'diff.patch', 'diff');
    const external = path.join(workspace, 'external-sandbox');
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.mkdirSync(external);
    fs.writeFileSync(path.join(external, 'sentinel'), 'protected');
    fs.mkdirSync(sandbox);
    fs.symlinkSync(external, path.join(sandbox, 'iter-0'), 'dir');

    await expect(
      dispatcher.deliver(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] })),
    ).rejects.toThrow(/symbolic link|unsafe/i);

    expect(calls.sendMessage).not.toHaveBeenCalled();
    expect(
      calls.startSession.mock.calls.some(([config]) => (config as { name: string }).name === 'autoloop-r1-reviewer'),
    ).toBe(false);
    expect(fs.readFileSync(path.join(external, 'sentinel'), 'utf8')).toBe('protected');
  });

  it('does not start or send the Reviewer when the authoritative artifact set is incomplete', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    const ledger = dispatcher.secureLedgerCapability;
    ledger.writeIterationArtifact(0, 'directive.json', '{}');
    ledger.writeIterationArtifact(0, 'eval_output.json', '{}');
    ledger.writeIterationArtifact(0, 'coder_summary.txt', 'complete');

    await expect(
      dispatcher.deliver(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] })),
    ).rejects.toThrow(/missing.*diff\.patch|complete artifact set/i);

    expect(calls.sendMessage).not.toHaveBeenCalled();
    expect(
      calls.startSession.mock.calls.some(([config]) => (config as { name: string }).name === 'autoloop-r1-reviewer'),
    ).toBe(false);
  });

  it.each([
    ['sandbox-reset', 'symlink', /Reviewer sandbox reset seam .*symbolic link/i],
    ['sandbox-reset', 'hardlink', /Reviewer sandbox reset seam .*hardlink with link count/i],
    [
      'sandbox-reset',
      'unapproved',
      /Reviewer sandbox reset seam membership changed unexpectedly; expected \[\], found \[unexpected-entry\]/i,
    ],
    ['sandbox-stage', 'symlink', /Reviewer sandbox final stage .*symbolic link/i],
    ['sandbox-stage', 'hardlink', /Reviewer sandbox final stage .*hardlink with link count/i],
    [
      'sandbox-stage',
      'unapproved',
      /Reviewer sandbox final stage membership changed unexpectedly; expected \[iter-0\], found \[iter-0, unexpected-entry\]/i,
    ],
  ] as const)('fails closed when a %s seam plants a %s entry', async (seam, kind, expectedDiagnostic) => {
    const workspace = tmpRoot;
    const external = path.join(workspace, `external-${seam}-${kind}`);
    fs.writeFileSync(external, 'must remain external');
    let armed = false;
    const secureLedger = SecureAutoloopLedger.open(workspace, 'r1', {
      create: true,
      testHooks: {
        beforeNestedMutation: (event) => {
          if (!armed || event.operation !== seam) return;
          if (seam === 'sandbox-stage' && event.relativePath !== 'reviewer_sandbox/iter-0') return;
          armed = false;
          const sandbox = path.join(secureLedger.directory, 'reviewer_sandbox');
          const planted =
            kind === 'unapproved'
              ? path.join(sandbox, 'unexpected-entry')
              : seam === 'sandbox-reset'
                ? path.join(sandbox, `scratch-${kind}`)
                : path.join(sandbox, 'plan.md');
          if (kind === 'symlink' || kind === 'hardlink') fs.unlinkSync(planted);
          if (kind === 'symlink') fs.symlinkSync(external, planted);
          else if (kind === 'hardlink') fs.linkSync(external, planted);
          else fs.writeFileSync(planted, 'unapproved');
        },
      },
    });
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: 'must not be sent' });
    ensureCompleteReviewArtifacts(dispatcher, 0);
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.mkdirSync(sandbox);
    if (seam === 'sandbox-reset' && kind !== 'unapproved') {
      fs.writeFileSync(path.join(sandbox, `scratch-${kind}`), 'safe before reset seam');
    }
    if (seam === 'sandbox-stage' && kind !== 'unapproved') {
      fs.writeFileSync(path.join(workspace, 'plan.md'), 'safe before stage seam');
    }
    armed = true;

    await expect(
      dispatcher.deliver(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] })),
    ).rejects.toThrow(expectedDiagnostic);

    expect(
      calls.startSession.mock.calls.some(([config]) => (config as { name: string }).name === 'autoloop-r1-reviewer'),
    ).toBe(false);
    expect(calls.sendMessage).not.toHaveBeenCalled();
    expect(fs.readFileSync(external, 'utf8')).toBe('must remain external');
  });

  it('treats a fresh directive message id for the same iteration as a conflict without a second Coder send', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-06T01:00:00.000Z'));
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'Coder acknowledged.' });
    const payload = { goal: 'one immutable effect', constraints: [], success_criteria: [], max_attempts: 1 };
    const first = Msg.directive(0, payload);

    await dispatcher.deliver(first);
    const distinct = Msg.directive(0, payload);
    expect(distinct.ts).toBe(first.ts);
    expect(distinct.msg_id).not.toBe(first.msg_id);

    const persisted = JSON.parse(
      fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'directive.json'), 'utf8'),
    ) as Record<string, unknown>;
    expect(persisted.message_id).toBe(first.msg_id);
    expect(persisted.dispatch_id).toMatch(/^dispatch_[a-f0-9]{64}$/);

    await expect(dispatcher.deliver(distinct)).rejects.toThrow(/conflicting|immutable|overwrite/i);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
  });

  it('stages only complete authoritative artifacts and persists an immutable Reviewer verdict', async () => {
    const coderReply = 'Coder acknowledged the bounded directive.';
    const reviewerReply = [
      'Independent review complete.',
      '```autoloop',
      JSON.stringify({
        tool: 'review_complete',
        args: { decision: 'advance', metric: 1, audit_notes: 'artifacts are complete' },
      }),
      '```',
    ].join('\n');
    const { dispatcher, calls, ledgerDir, workspace } = makeDispatcher(
      {},
      { sendOutputs: [coderReply, reviewerReply] },
    );
    const ledger = dispatcher.secureLedgerCapability;
    fs.writeFileSync(path.join(workspace, 'plan.md'), '# Plan\n');
    fs.writeFileSync(path.join(workspace, 'goal.json'), '{"goal":"bounded"}\n');
    ledger.writeIterationArtifact(0, 'verdict.json', '{"decision":"hold"}\n');

    await dispatcher.deliver(Msg.directive(1, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }));
    ledger.writeIterationArtifact(1, 'eval_output.json', '{"metric":1}\n');
    ledger.writeIterationArtifact(1, 'coder_summary.txt', 'complete\n');
    ledger.writeIterationArtifact(1, 'diff.patch', 'diff --git a/a b/a\n');
    fs.writeFileSync(path.join(ledgerDir, 'iter', '1', 'unexpected.txt'), 'must not stage');

    await expect(
      dispatcher.deliver(Msg.reviewRequest(1, { iter: 1, ledger_path: ledgerDir, prior_metrics: [] })),
    ).resolves.toEqual([
      expect.objectContaining({ type: 'review_verdict', payload: expect.objectContaining({ decision: 'advance' }) }),
    ]);

    expect(calls.sendMessage).toHaveBeenCalledTimes(2);
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    const staged = path.join(sandbox, 'iter-1');
    expect(fs.readdirSync(staged).sort()).toEqual(
      ['coder_summary.txt', 'diff.patch', 'directive.json', 'eval_output.json'].sort(),
    );
    expect(fs.readFileSync(path.join(staged, 'directive.json'), 'utf8')).toContain('"schema_version"');
    expect(fs.readFileSync(path.join(sandbox, 'plan.md'), 'utf8')).toBe('# Plan\n');
    expect(fs.readFileSync(path.join(sandbox, 'goal.json'), 'utf8')).toBe('{"goal":"bounded"}\n');
    expect(fs.readFileSync(path.join(sandbox, 'prior_verdict.json'), 'utf8')).toBe('{"decision":"hold"}\n');
    expect(permissions(staged)).toBe(0o700);
    expect(permissions(path.join(staged, 'directive.json'))).toBe(0o600);

    const verdict = ledger.readIterationArtifact(1, 'verdict.json')?.toString('utf8') ?? '';
    expect(JSON.parse(verdict)).toMatchObject({ decision: 'advance', metric: 1 });
    expect(() => ledger.writeIterationArtifact(1, 'verdict.json', '{"decision":"hold"}')).toThrow(
      /conflicting|immutable/i,
    );
    expect(ledger.readIterationArtifact(1, 'verdict.json')?.toString('utf8')).toBe(verdict);
  });

  it('keeps an identical semantic verdict byte-stable across replay and rejects a conflicting verdict', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-06T01:00:00.000Z'));
    const { dispatcher, ledgerDir } = makeDispatcher();
    const persistVerdict = (
      dispatcher as unknown as {
        persistVerdict(iter: number, payload: { decision: string; metric: number | null; audit_notes: string }): void;
      }
    ).persistVerdict.bind(dispatcher);
    const payload = { decision: 'advance', metric: 1, audit_notes: 'complete' };

    persistVerdict(0, payload);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);
    vi.setSystemTime(new Date('2026-09-06T02:00:00.000Z'));
    expect(() => persistVerdict(0, payload)).not.toThrow();
    expect(fs.readFileSync(verdictPath)).toEqual(first);

    expect(() => persistVerdict(0, { ...payload, decision: 'hold' })).toThrow(/conflicting|immutable/i);
    expect(fs.readFileSync(verdictPath)).toEqual(first);
  });
});

describe('ClaudeAgentDispatcher — canonical immutable Reviewer verdicts', () => {
  type VerdictCandidate = {
    decision: 'advance' | 'hold' | 'rollback';
    metric: number | null;
    audit_notes: string;
    flags?: string[];
    accepted?: boolean;
    evidence_id?: string;
  };

  function verdictMethods(dispatcher: ClaudeAgentDispatcher): {
    gateVerdict(iter: number, payload: VerdictCandidate): Promise<VerdictCandidate>;
    persistVerdict(iter: number, payload: VerdictCandidate): void;
  } {
    const methods = dispatcher as unknown as {
      gateVerdict(iter: number, payload: VerdictCandidate): Promise<VerdictCandidate>;
      persistVerdict(iter: number, payload: VerdictCandidate): void;
    };
    return {
      gateVerdict: methods.gateVerdict.bind(dispatcher),
      persistVerdict: methods.persistVerdict.bind(dispatcher),
    };
  }

  function withObjectPrototypePollution<T>(
    pollution: Record<string, unknown>,
    action: () => T,
  ): { result: T | undefined; thrown: unknown } {
    const originalDescriptors = new Map(
      Object.keys(pollution).map((key) => [key, Object.getOwnPropertyDescriptor(Object.prototype, key)]),
    );
    let result: T | undefined;
    let thrown: unknown;

    try {
      for (const [key, value] of Object.entries(pollution)) {
        Object.defineProperty(Object.prototype, key, {
          configurable: true,
          enumerable: false,
          value,
          writable: true,
        });
      }
      try {
        result = action();
      } catch (error) {
        thrown = error;
      }
    } finally {
      for (const [key, descriptor] of originalDescriptors) {
        if (descriptor === undefined) {
          Reflect.deleteProperty(Object.prototype, key);
        } else {
          Object.defineProperty(Object.prototype, key, descriptor);
        }
      }
    }

    for (const [key, descriptor] of originalDescriptors) {
      expect(Object.getOwnPropertyDescriptor(Object.prototype, key)).toEqual(descriptor);
    }
    return { result, thrown };
  }

  it('persists exact canonical verdict bytes without consulting an inherited Object.prototype.toJSON', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-06T01:00:00.000Z'));
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    let getterHits = 0;
    let calls = 0;

    const { thrown } = await withPrototypeDescriptors(
      [
        {
          target: Object.prototype,
          key: 'toJSON',
          descriptor: {
            configurable: true,
            get() {
              getterHits += 1;
              return () => {
                calls += 1;
                return { attacker_chosen_verdict: true };
              };
            },
          },
        },
      ],
      () =>
        persistVerdict(0, {
          decision: 'advance',
          metric: 1,
          audit_notes: 'canonical verdict bytes',
          accepted: true,
          evidence_id: 'iter-0',
        }),
    );

    expect(thrown).toBeUndefined();
    expect(getterHits).toBe(0);
    expect(calls).toBe(0);
    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'verdict.json'), 'utf8')).toBe(
      [
        '{',
        '  "schema_version": 1,',
        '  "iter": 0,',
        '  "ts": "2026-09-06T01:00:00.000Z",',
        '  "decision": "advance",',
        '  "metric": 1,',
        '  "audit_notes": "canonical verdict bytes",',
        '  "accepted": true,',
        '  "evidence_id": "iter-0"',
        '}',
      ].join('\n'),
    );
  });

  it('never persists ephemeral Reviewer flags in a new schema-v1 verdict', async () => {
    const reviewerReply = [
      'Independent review complete.',
      '```autoloop',
      JSON.stringify({
        tool: 'review_complete',
        args: {
          decision: 'advance',
          metric: 1,
          audit_notes: 'durable fields only',
          flags: ['runtime-only-warning'],
        },
      }),
      '```',
    ].join('\n');
    const { dispatcher, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    ensureCompleteReviewArtifacts(dispatcher, 0);

    await dispatcher.deliver(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] }));

    const verdict = JSON.parse(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'verdict.json'), 'utf8')) as Record<
      string,
      unknown
    >;
    expect(Object.keys(verdict)).toEqual(['schema_version', 'iter', 'ts', 'decision', 'metric', 'audit_notes']);
    expect(verdict).toMatchObject({
      schema_version: LEDGER_SCHEMA_VERSION,
      iter: 0,
      decision: 'advance',
      metric: 1,
      audit_notes: 'durable fields only',
    });
  });

  it('fails closed on malformed raw Reviewer flags before acceptance and never persists them', async () => {
    const reviewerReply = [
      'Independent review returned malformed runtime flags.',
      '```autoloop',
      JSON.stringify({
        tool: 'review_complete',
        args: {
          decision: 'advance',
          metric: 1,
          audit_notes: 'must not reach acceptance',
          flags: ['safe-looking', 7],
        },
      }),
      '```',
    ].join('\n');
    const { dispatcher, ledgerDir } = makeDispatcher(
      {
        contract: {
          id: 'malformed-flags-must-not-run',
          checks: [{ id: 'workspace-exists', spec: { type: 'file', path: '.', exists: true } }],
        },
      },
      { sendOutput: reviewerReply },
    );
    ensureCompleteReviewArtifacts(dispatcher, 0);
    const targetHit = vi.fn();
    dispatcher.on('target_hit', targetHit);

    const delivered = await dispatcher.deliver(
      Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] }),
    );

    expect(delivered).toEqual([
      expect.objectContaining({
        type: 'review_verdict',
        payload: expect.objectContaining({ decision: 'hold', metric: null }),
      }),
    ]);
    expect(targetHit).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'evidence'))).toBe(false);
    const verdict = JSON.parse(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'verdict.json'), 'utf8')) as Record<
      string,
      unknown
    >;
    expect(verdict.decision).toBe('hold');
    expect(verdict.audit_notes).toContain('[no verdict emitted]');
    expect(Object.hasOwn(verdict, 'flags')).toBe(false);
  });

  it('replays calculated acceptance byte-stably across absent and different runtime flags', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-06T01:00:00.000Z'));
    const { dispatcher, ledgerDir } = makeDispatcher({
      contract: {
        id: 'canonical-verdict-flags',
        checks: [{ id: 'workspace-exists', spec: { type: 'file', path: '.', exists: true } }],
      },
    });
    const { gateVerdict, persistVerdict } = verdictMethods(dispatcher);
    const gated = await gateVerdict(0, {
      decision: 'advance',
      metric: 1,
      audit_notes: 'acceptance passed',
      flags: ['first-runtime-flag'],
    });
    expect(gated).toMatchObject({ accepted: true, evidence_id: 'iter-0' });

    persistVerdict(0, gated);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);
    const stored = JSON.parse(first.toString('utf8')) as Record<string, unknown>;
    expect(Object.keys(stored)).toEqual([
      'schema_version',
      'iter',
      'ts',
      'decision',
      'metric',
      'audit_notes',
      'accepted',
      'evidence_id',
    ]);

    vi.setSystemTime(new Date('2026-09-06T02:00:00.000Z'));
    expect(() => persistVerdict(0, { ...gated, flags: undefined })).not.toThrow();
    expect(fs.readFileSync(verdictPath)).toEqual(first);
    expect(() => persistVerdict(0, { ...gated, flags: ['different-runtime-flag'] })).not.toThrow();
    expect(fs.readFileSync(verdictPath)).toEqual(first);
  });

  it('omits inherited optional fields from a first persisted verdict', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'hold',
      metric: null,
      audit_notes: 'optional prototype values are not durable evidence',
    };

    const { thrown } = withObjectPrototypePollution({ accepted: false, evidence_id: 'inherited-evidence' }, () =>
      persistVerdict(0, payload),
    );

    expect(thrown).toBeUndefined();
    const stored = JSON.parse(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'verdict.json'), 'utf8')) as Record<
      string,
      unknown
    >;
    expect(Object.keys(stored)).toEqual(['schema_version', 'iter', 'ts', 'decision', 'metric', 'audit_notes']);
    expect(stored).toMatchObject(payload);
  });

  it('rejects inherited required fields on a first persisted verdict', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const inheritedPayload = {
      decision: 'hold',
      metric: null,
      audit_notes: 'required prototype values are not durable evidence',
    };

    const { thrown } = withObjectPrototypePollution(inheritedPayload, () => persistVerdict(0, {} as VerdictCandidate));

    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).message).toMatch(/conflicting|immutable|invalid/i);
    expect(fs.existsSync(verdictPath)).toBe(false);
  });

  it.each(['decision', 'metric', 'audit_notes', 'accepted', 'evidence_id'] as const)(
    'rejects an own accessor for Reviewer verdict field %s without invoking it',
    (field) => {
      const { dispatcher, ledgerDir } = makeDispatcher();
      const { persistVerdict } = verdictMethods(dispatcher);
      const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
      const payload: Record<string, unknown> = {
        decision: 'advance',
        metric: 1,
        audit_notes: 'accessors are not durable evidence',
        accepted: true,
        evidence_id: 'iter-0',
      };
      const accessorValue = payload[field];
      let getterHits = 0;
      Object.defineProperty(payload, field, {
        configurable: true,
        enumerable: true,
        get() {
          getterHits += 1;
          return accessorValue;
        },
      });

      expect(() => persistVerdict(0, payload as unknown as VerdictCandidate)).toThrow(/conflicting|immutable|invalid/i);

      expect(getterHits).toBe(0);
      expect(fs.existsSync(verdictPath)).toBe(false);
    },
  );

  it.each([
    ['an unknown string field', 'runtime_metadata'],
    ['an unknown symbol field', Symbol('runtime-metadata')],
  ] as const)('rejects %s on an incoming immutable verdict before persistence', (_description, key) => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const payload: Record<PropertyKey, unknown> = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'unknown fields must not be silently stripped',
    };
    Object.defineProperty(payload, key, {
      configurable: true,
      enumerable: false,
      value: 'must be rejected',
      writable: true,
    });

    expect(() => persistVerdict(0, payload as unknown as VerdictCandidate)).toThrow(/unsupported|immutable|invalid/i);
    expect(fs.existsSync(verdictPath)).toBe(false);
  });

  it('snapshots each Reviewer verdict data descriptor once without ordinary Proxy reads', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-06T01:00:00.000Z'));
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const target = {
      decision: 'advance' as const,
      metric: 1,
      audit_notes: 'single immutable verdict snapshot',
      accepted: true,
      evidence_id: 'iter-0',
    };
    const descriptorReads = new Map<PropertyKey, number>();
    let ordinaryReads = 0;
    const payload = new Proxy(target, {
      get(inner, key, receiver) {
        ordinaryReads += 1;
        return Reflect.get(inner, key, receiver);
      },
      getOwnPropertyDescriptor(inner, key) {
        descriptorReads.set(key, (descriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(inner, key);
      },
    });

    persistVerdict(0, payload);

    expect(ordinaryReads).toBe(0);
    for (const field of ['decision', 'metric', 'audit_notes', 'accepted', 'evidence_id']) {
      expect(descriptorReads.get(field)).toBe(1);
    }
    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'verdict.json'), 'utf8')).toBe(
      [
        '{',
        '  "schema_version": 1,',
        '  "iter": 0,',
        '  "ts": "2026-09-06T01:00:00.000Z",',
        '  "decision": "advance",',
        '  "metric": 1,',
        '  "audit_notes": "single immutable verdict snapshot",',
        '  "accepted": true,',
        '  "evidence_id": "iter-0"',
        '}',
      ].join('\n'),
    );
  });

  it.each([
    ['decision outside the verdict allowlist', { decision: 'pause', metric: 1, audit_notes: 'invalid decision' }],
    ['non-finite metric', { decision: 'hold', metric: Number.NaN, audit_notes: 'invalid metric' }],
    ['non-string audit notes', { decision: 'hold', metric: null, audit_notes: 1 }],
    ['non-boolean accepted', { decision: 'advance', metric: 1, audit_notes: 'invalid accepted', accepted: 'yes' }],
    ['non-string evidence id', { decision: 'advance', metric: 1, audit_notes: 'invalid evidence', evidence_id: 1 }],
  ])('rejects a first persisted verdict with %s', (_description, payload) => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');

    expect(() => persistVerdict(0, payload as unknown as VerdictCandidate)).toThrow(/conflicting|immutable|invalid/i);
    expect(fs.existsSync(verdictPath)).toBe(false);
  });

  it.each([
    ['accepted', { accepted: undefined }],
    ['evidence_id', { evidence_id: undefined }],
  ] as const)('does not collapse an own undefined %s into absence on replay', (_field, optional) => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'hold',
      metric: null,
      audit_notes: 'own undefined is not absence',
    };
    persistVerdict(0, payload);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);

    expect(() => persistVerdict(0, { ...payload, ...optional })).toThrow(/conflicting|immutable|invalid/i);
    expect(fs.readFileSync(verdictPath)).toEqual(first);
  });

  it.each([
    ['an empty array', []],
    ['a string-only array', ['legacy-runtime-flag', 'legacy-secondary-flag']],
  ] as const)('tolerates legacy flags containing %s when comparing an existing verdict', (_description, flags) => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'legacy runtime flags',
      accepted: true,
      evidence_id: 'iter-0',
    };
    const legacyWithFlags = `${JSON.stringify(
      {
        schema_version: LEDGER_SCHEMA_VERSION,
        iter: 0,
        ts: '2026-09-06T01:00:00.000Z',
        ...payload,
        flags,
      },
      null,
      2,
    )}\n`;
    dispatcher.secureLedgerCapability.writeIterationArtifact(0, 'verdict.json', legacyWithFlags);
    const legacyPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(legacyPath);

    expect(() => persistVerdict(0, payload)).not.toThrow();
    expect(fs.readFileSync(legacyPath)).toEqual(first);
  });

  it.each([
    ['a string', 'legacy-runtime-flag'],
    ['a number', 1],
    ['a number-containing array', ['legacy-runtime-flag', 1]],
    ['a null-containing array', ['legacy-runtime-flag', null]],
    ['an object-containing array', ['legacy-runtime-flag', { warning: true }]],
    ['null', null],
    ['an object', { warning: true }],
  ])('rejects legacy flags containing %s while preserving the first verdict', (_description, flags) => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'invalid legacy runtime flags',
      accepted: true,
      evidence_id: 'iter-0',
    };
    const legacyWithInvalidFlags = `${JSON.stringify(
      {
        schema_version: LEDGER_SCHEMA_VERSION,
        iter: 0,
        ts: '2026-09-06T01:00:00.000Z',
        ...payload,
        flags,
      },
      null,
      2,
    )}\n`;
    dispatcher.secureLedgerCapability.writeIterationArtifact(0, 'verdict.json', legacyWithInvalidFlags);
    const legacyPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(legacyPath);

    expect(() => persistVerdict(0, payload)).toThrow(/conflicting|immutable/i);
    expect(fs.readFileSync(legacyPath)).toEqual(first);
  });

  it('validates compatible stored legacy flags without consulting polluted array some or every methods', async () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'legacy flags remain compatible',
      accepted: true,
      evidence_id: 'iter-0',
    };
    dispatcher.secureLedgerCapability.writeIterationArtifact(
      0,
      'verdict.json',
      JSON.stringify({
        schema_version: LEDGER_SCHEMA_VERSION,
        iter: 0,
        ts: '2026-09-06T01:00:00.000Z',
        ...payload,
        flags: ['legacy-runtime-flag'],
      }),
    );
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);
    const originalSome = Array.prototype.some;
    const originalEvery = Array.prototype.every;
    let someHits = 0;
    let everyHits = 0;

    const { thrown } = await withPrototypeDescriptors(
      [
        {
          target: Array.prototype,
          key: 'some',
          descriptor: {
            configurable: true,
            value: function pollutedSome(
              this: unknown[],
              predicate: (value: unknown, index: number, array: unknown[]) => unknown,
            ): boolean {
              if (this[0] === 'schema_version') {
                someHits += 1;
                return true;
              }
              return originalSome.call(this, predicate);
            },
            writable: true,
          },
        },
        {
          target: Array.prototype,
          key: 'every',
          descriptor: {
            configurable: true,
            value: function pollutedEvery(
              this: unknown[],
              predicate: (value: unknown, index: number, array: unknown[]) => unknown,
            ): boolean {
              if (this[0] === 'legacy-runtime-flag') {
                everyHits += 1;
                return false;
              }
              return originalEvery.call(this, predicate);
            },
            writable: true,
          },
        },
      ],
      () => persistVerdict(0, payload),
    );

    expect(thrown).toBeUndefined();
    expect(someHits).toBe(0);
    expect(everyHits).toBe(0);
    expect(fs.readFileSync(verdictPath)).toEqual(first);
  });

  it('accepts the shared canonical string-array shape when replaying a verdict with legacy flags', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'canonical runtime flags remain compatible',
      accepted: true,
      evidence_id: 'iter-0',
    };
    const directive = validateMessage(
      Msg.directive(0, {
        goal: 'derive a canonical string array',
        constraints: ['canonical-runtime-flag'],
        success_criteria: [],
        max_attempts: 1,
      }),
    );
    if (directive.type !== 'directive') throw new Error('expected directive');
    const canonicalFlags = directive.payload.constraints;
    dispatcher.secureLedgerCapability.writeIterationArtifact(
      0,
      'verdict.json',
      `${JSON.stringify(
        {
          schema_version: LEDGER_SCHEMA_VERSION,
          iter: 0,
          ts: '2026-09-06T01:00:00.000Z',
          ...payload,
          flags: ['legacy-runtime-flag'],
        },
        null,
        2,
      )}\n`,
    );
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);

    expect(Object.getOwnPropertyDescriptor(canonicalFlags, 'toJSON')).toEqual({
      configurable: false,
      enumerable: false,
      value: undefined,
      writable: false,
    });
    expect(() => persistVerdict(0, { ...payload, flags: canonicalFlags })).not.toThrow();
    expect(fs.readFileSync(verdictPath)).toEqual(first);
  });

  it.each(['named', 'symbol'] as const)(
    'rejects legacy flags whose ownKeys substitutes index 0 with a %s key without persisting a verdict',
    (substitutionKind) => {
      const { dispatcher, ledgerDir } = makeDispatcher();
      const { persistVerdict } = verdictMethods(dispatcher);
      const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
      const target = ['legacy-runtime-flag'];
      const replacementKey: PropertyKey = substitutionKind === 'named' ? 'metadata' : Symbol('legacy-flags-metadata');
      Object.defineProperty(target, replacementKey, {
        configurable: true,
        enumerable: true,
        value: 'unsupported',
        writable: true,
      });
      let getterHits = 0;
      const flags = new Proxy(target, {
        get(inner, key, receiver) {
          if (key === '0') getterHits += 1;
          return Reflect.get(inner, key, receiver);
        },
        ownKeys(inner) {
          return Reflect.ownKeys(inner).filter((key) => key !== '0');
        },
      });
      const payload = {
        decision: 'advance' as const,
        metric: 1,
        audit_notes: 'cardinality substitution is not a legacy flag shape',
        accepted: true,
        evidence_id: 'iter-0',
        flags,
      };

      expect(() => persistVerdict(0, payload)).toThrow(/immutable|invalid/i);
      expect(getterHits).toBe(0);
      expect(fs.existsSync(verdictPath)).toBe(false);
    },
  );

  it('rejects near-miss canonical flag shadows and malformed array shapes without invoking accessors', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'near-miss runtime flags fail closed',
      accepted: true,
      evidence_id: 'iter-0',
    };
    persistVerdict(0, payload);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);

    for (const kind of ['configurable-shadow', 'accessor-shadow', 'sparse', 'named', 'symbol'] as const) {
      const flags = ['runtime-flag'];
      let getterHits = 0;
      if (kind === 'configurable-shadow') {
        Object.defineProperty(flags, 'toJSON', { configurable: true, value: undefined });
      }
      if (kind === 'accessor-shadow') {
        Object.defineProperty(flags, 'toJSON', {
          configurable: true,
          get() {
            getterHits += 1;
            return undefined;
          },
        });
      }
      if (kind === 'sparse') delete flags[0];
      if (kind === 'named') Object.defineProperty(flags, 'metadata', { value: 'unsupported' });
      if (kind === 'symbol') Object.defineProperty(flags, Symbol('metadata'), { value: 'unsupported' });

      expect(() => persistVerdict(0, { ...payload, flags }), kind).toThrow(/conflicting|immutable|invalid/i);
      expect(getterHits, kind).toBe(0);
      expect(fs.readFileSync(verdictPath), kind).toEqual(first);
    }
  });

  it('rejects unknown own fields while preserving the first verdict', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'unknown legacy field',
      accepted: true,
      evidence_id: 'iter-0',
    };

    const legacyWithUnknown = JSON.stringify({
      schema_version: LEDGER_SCHEMA_VERSION,
      iter: 0,
      ts: '2026-09-06T01:00:00.000Z',
      ...payload,
      runtime_metadata: ['must not be ignored'],
    });
    dispatcher.secureLedgerCapability.writeIterationArtifact(0, 'verdict.json', legacyWithUnknown);
    const unknownPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const unknownFirst = fs.readFileSync(unknownPath);

    expect(() => persistVerdict(0, payload)).toThrow(/conflicting|immutable/i);
    expect(fs.readFileSync(unknownPath)).toEqual(unknownFirst);
  });

  it.each([
    ['decision value', {}, { decision: 'hold' }],
    ['metric value', {}, { metric: 2 }],
    ['audit_notes value', {}, { audit_notes: 'materially changed audit' }],
    ['accepted value', { accepted: true }, { accepted: false }],
    ['accepted presence', {}, { accepted: true }],
    ['accepted absence', { accepted: true }, { accepted: undefined }],
    ['evidence_id value', { evidence_id: 'iter-0' }, { evidence_id: 'iter-1' }],
    ['evidence_id presence', {}, { evidence_id: 'iter-0' }],
    ['evidence_id absence', { evidence_id: 'iter-0' }, { evidence_id: undefined }],
  ] satisfies Array<[string, Partial<VerdictCandidate>, Partial<VerdictCandidate>]>)(
    'rejects a change to durable %s while preserving the first verdict',
    (_field, initial, change) => {
      const { dispatcher, ledgerDir } = makeDispatcher();
      const { persistVerdict } = verdictMethods(dispatcher);
      const payload: VerdictCandidate = {
        decision: 'advance',
        metric: 1,
        audit_notes: 'immutable audit',
        ...initial,
      };
      persistVerdict(0, payload);
      const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
      const first = fs.readFileSync(verdictPath);

      expect(() => persistVerdict(0, { ...payload, ...change })).toThrow(/conflicting|immutable/i);
      expect(fs.readFileSync(verdictPath)).toEqual(first);
    },
  );

  it('rejects a sparse stored verdict when Object.prototype supplies matching durable fields', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'inherited values are not persisted values',
      accepted: true,
      evidence_id: 'iter-0',
    };
    const sparseVerdict = `${JSON.stringify(
      {
        schema_version: LEDGER_SCHEMA_VERSION,
        iter: 0,
        ts: '2026-09-06T01:00:00.000Z',
      },
      null,
      2,
    )}\n`;
    dispatcher.secureLedgerCapability.writeIterationArtifact(0, 'verdict.json', sparseVerdict);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);
    const inheritedDurableFields = {
      decision: payload.decision,
      metric: payload.metric,
      audit_notes: payload.audit_notes,
      accepted: payload.accepted,
      evidence_id: payload.evidence_id,
    };
    const originalDescriptors = new Map(
      Object.keys(inheritedDurableFields).map((key) => [key, Object.getOwnPropertyDescriptor(Object.prototype, key)]),
    );
    let conflict: unknown;

    try {
      for (const [key, value] of Object.entries(inheritedDurableFields)) {
        Object.defineProperty(Object.prototype, key, {
          configurable: true,
          enumerable: false,
          value,
          writable: true,
        });
      }

      try {
        persistVerdict(0, payload);
      } catch (error) {
        conflict = error;
      }
      expect(fs.readFileSync(verdictPath)).toEqual(first);
    } finally {
      for (const [key, descriptor] of originalDescriptors) {
        if (descriptor === undefined) {
          Reflect.deleteProperty(Object.prototype, key);
        } else {
          Object.defineProperty(Object.prototype, key, descriptor);
        }
      }
    }

    for (const [key, descriptor] of originalDescriptors) {
      expect(Object.getOwnPropertyDescriptor(Object.prototype, key)).toEqual(descriptor);
    }
    expect(conflict).toBeInstanceOf(Error);
    expect((conflict as Error).message).toMatch(/conflicting|immutable/i);
  });

  it('rejects a sparse incoming replay whose complete verdict is inherited from Object.prototype', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'a sparse replay cannot borrow durable identity',
      accepted: false,
      evidence_id: 'iter-0',
    };
    persistVerdict(0, payload);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);

    const { thrown } = withObjectPrototypePollution(payload, () => persistVerdict(0, {} as VerdictCandidate));

    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).message).toMatch(/conflicting|immutable|invalid/i);
    expect(fs.readFileSync(verdictPath)).toEqual(first);
  });

  it('rejects stored verdict envelope identity inherited from Object.prototype', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'advance',
      metric: 1,
      audit_notes: 'envelope identity must be stored',
      accepted: false,
      evidence_id: 'iter-0',
    };
    dispatcher.secureLedgerCapability.writeIterationArtifact(0, 'verdict.json', JSON.stringify(payload));
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);

    const { thrown } = withObjectPrototypePollution(
      { schema_version: LEDGER_SCHEMA_VERSION, iter: 0, ts: '2026-09-06T01:00:00.000Z' },
      () => persistVerdict(0, payload),
    );

    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).message).toMatch(/conflicting|immutable/i);
    expect(fs.readFileSync(verdictPath)).toEqual(first);
  });

  it('rejects a required-only hold replay against matching inherited stored durable fields', () => {
    const { dispatcher, ledgerDir } = makeDispatcher();
    const { persistVerdict } = verdictMethods(dispatcher);
    const payload: VerdictCandidate = {
      decision: 'hold',
      metric: null,
      audit_notes: 'required values must be stored as own properties',
    };
    const sparseVerdict = JSON.stringify({
      schema_version: LEDGER_SCHEMA_VERSION,
      iter: 0,
      ts: '2026-09-06T01:00:00.000Z',
    });
    dispatcher.secureLedgerCapability.writeIterationArtifact(0, 'verdict.json', sparseVerdict);
    const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
    const first = fs.readFileSync(verdictPath);

    const { thrown } = withObjectPrototypePollution(payload, () => persistVerdict(0, payload));

    expect(thrown).toBeInstanceOf(Error);
    expect((thrown as Error).message).toMatch(/conflicting|immutable/i);
    expect(fs.readFileSync(verdictPath)).toEqual(first);
  });

  it.each([
    [
      'stored accepted false versus incoming absence',
      { decision: 'hold', metric: 1, audit_notes: 'falsy presence', accepted: false },
      { decision: 'hold', metric: 1, audit_notes: 'falsy presence' },
    ],
    [
      'stored accepted absence versus incoming false',
      { decision: 'hold', metric: 1, audit_notes: 'falsy presence' },
      { decision: 'hold', metric: 1, audit_notes: 'falsy presence', accepted: false },
    ],
    [
      'stored metric null versus incoming absence',
      { decision: 'hold', metric: null, audit_notes: 'falsy presence' },
      { decision: 'hold', audit_notes: 'falsy presence' },
    ],
    [
      'stored metric absence versus incoming null',
      { decision: 'hold', audit_notes: 'falsy presence' },
      { decision: 'hold', metric: null, audit_notes: 'falsy presence' },
    ],
  ] satisfies Array<[string, Record<string, unknown>, Record<string, unknown>]>)(
    'rejects %s while preserving the first verdict',
    (_description, storedPayload, incomingPayload) => {
      const { dispatcher, ledgerDir } = makeDispatcher();
      const { persistVerdict } = verdictMethods(dispatcher);
      const storedVerdict = JSON.stringify({
        schema_version: LEDGER_SCHEMA_VERSION,
        iter: 0,
        ts: '2026-09-06T01:00:00.000Z',
        ...storedPayload,
      });
      dispatcher.secureLedgerCapability.writeIterationArtifact(0, 'verdict.json', storedVerdict);
      const verdictPath = path.join(ledgerDir, 'iter', '0', 'verdict.json');
      const first = fs.readFileSync(verdictPath);

      expect(() => persistVerdict(0, incomingPayload as VerdictCandidate)).toThrow(/conflicting|immutable|invalid/i);
      expect(fs.readFileSync(verdictPath)).toEqual(first);
    },
  );
});

describe('ClaudeAgentDispatcher — canonical immutable delivery payloads', () => {
  const reviewerReply = [
    'Independent review complete.',
    '```autoloop',
    JSON.stringify({
      tool: 'review_complete',
      args: { decision: 'advance', metric: 1, audit_notes: 'canonical request reviewed' },
    }),
    '```',
  ].join('\n');

  it('keeps distinct Reviewer dispatches queued through post-send persistence and compaction while coalescing one ID', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher(
      { compactThresholds: { reviewer: 50 } },
      { contextPercent: 90 },
    );
    ensureCompleteReviewArtifacts(dispatcher, 0);
    ensureCompleteReviewArtifacts(dispatcher, 1);
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    const sendOrder: number[] = [];
    const observedSandbox: Array<{ iter: number; staged: string[] }> = [];
    let signalFirstCompaction!: () => void;
    const firstCompactionEntered = new Promise<void>((resolve) => {
      signalFirstCompaction = resolve;
    });
    let releaseFirstCompaction!: () => void;
    const firstCompactionGate = new Promise<void>((resolve) => {
      releaseFirstCompaction = resolve;
    });
    calls.sendMessage.mockImplementation(async (_name, prompt: string) => {
      const iter = Number(/^\[review_request iter=(\d+)\]/.exec(prompt)?.[1]);
      sendOrder.push(iter);
      observedSandbox.push({
        iter,
        staged: fs
          .readdirSync(sandbox)
          .filter((entry) => entry.startsWith('iter-'))
          .sort(),
      });
      return { output: reviewerReply, error: undefined };
    });
    calls.compactSession.mockImplementationOnce(async () => {
      signalFirstCompaction();
      await firstCompactionGate;
    });
    const firstMessage = fixedIdentity(
      Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [0] }),
      'review-serialized-0',
    );
    const duplicateMessage = fixedIdentity(
      Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [0] }),
      'review-serialized-0',
      '2035-01-01T00:00:00.000Z',
    );
    const secondMessage = fixedIdentity(
      Msg.reviewRequest(1, { iter: 1, ledger_path: ledgerDir, prior_metrics: [0, 1] }),
      'review-serialized-1',
    );

    const first = dispatcher.deliver(firstMessage);
    await firstCompactionEntered;
    const duplicate = dispatcher.deliver(duplicateMessage);
    const second = dispatcher.deliver(secondMessage);
    let boundaryAssertion: unknown;
    try {
      expect(
        fs
          .readdirSync(sandbox)
          .filter((entry) => entry.startsWith('iter-'))
          .sort(),
      ).toEqual(['iter-0']);
      expect(calls.sendMessage).toHaveBeenCalledTimes(1);
      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'verdict.json'))).toBe(true);
      expect(fs.existsSync(path.join(ledgerDir, 'iter', '1', 'verdict.json'))).toBe(false);
    } catch (error) {
      boundaryAssertion = error;
    } finally {
      releaseFirstCompaction();
    }
    const [firstResult, duplicateResult, secondResult] = await Promise.all([first, duplicate, second]);
    if (boundaryAssertion) throw boundaryAssertion;

    expect(duplicateResult).toBe(firstResult);
    expect(firstResult).toEqual([
      expect.objectContaining({ type: 'review_verdict', payload: expect.objectContaining({ decision: 'advance' }) }),
    ]);
    expect(secondResult).toEqual([
      expect.objectContaining({ type: 'review_verdict', payload: expect.objectContaining({ decision: 'advance' }) }),
    ]);
    expect(sendOrder).toEqual([0, 1]);
    expect(observedSandbox).toEqual([
      { iter: 0, staged: ['iter-0'] },
      { iter: 1, staged: ['iter-1'] },
    ]);
    expect(calls.startSession).toHaveBeenCalledTimes(1);
    expect(calls.sendMessage).toHaveBeenCalledTimes(2);
  });

  it('releases the Reviewer FIFO after one recoverable send timeout and runs the next distinct review once', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher();
    ensureCompleteReviewArtifacts(dispatcher, 0);
    ensureCompleteReviewArtifacts(dispatcher, 1);
    calls.sendMessage
      .mockRejectedValueOnce(genuineSendTimeout())
      .mockResolvedValue({ output: reviewerReply, error: undefined });

    const first = dispatcher.deliver(
      fixedIdentity(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] }), 'review-timeout-0'),
    );
    const second = dispatcher.deliver(
      fixedIdentity(
        Msg.reviewRequest(1, { iter: 1, ledger_path: ledgerDir, prior_metrics: [] }),
        'review-after-timeout-1',
      ),
    );
    const [firstResult, secondResult] = await Promise.all([first, second]);

    expect(sendTimeout(firstResult).payload).toMatchObject({
      agent: 'reviewer',
      message_id: 'review-timeout-0',
      iter: 0,
    });
    expect(secondResult).toEqual([
      expect.objectContaining({ type: 'review_verdict', payload: expect.objectContaining({ decision: 'advance' }) }),
    ]);
    expect(calls.sendMessage).toHaveBeenCalledTimes(2);
    expect(calls.sendMessage.mock.calls.map((call) => call[1])).toEqual([
      expect.stringMatching(/^\[review_request iter=0\]/),
      expect.stringMatching(/^\[review_request iter=1\]/),
    ]);
  });

  it('releases the Reviewer FIFO after one fatal phase_error outcome and runs the next distinct review once', async () => {
    vi.useFakeTimers();
    const { dispatcher, calls, ledgerDir } = makeDispatcher();
    ensureCompleteReviewArtifacts(dispatcher, 0);
    ensureCompleteReviewArtifacts(dispatcher, 1);
    calls.sendMessage
      .mockRejectedValueOnce(new Error('first Reviewer process failed'))
      .mockRejectedValueOnce(new Error('replacement Reviewer process failed'))
      .mockResolvedValue({ output: reviewerReply, error: undefined });

    const first = dispatcher.deliver(
      fixedIdentity(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] }), 'review-fatal-0'),
    );
    const second = dispatcher.deliver(
      fixedIdentity(
        Msg.reviewRequest(1, { iter: 1, ledger_path: ledgerDir, prior_metrics: [] }),
        'review-after-fatal-1',
      ),
    );
    await vi.runAllTimersAsync();
    const [firstResult, secondResult] = await Promise.all([first, second]);

    expect(firstResult).toEqual([
      expect.objectContaining({
        type: 'phase_error',
        payload: expect.objectContaining({ agent: 'reviewer', phase: 'send' }),
      }),
    ]);
    expect(secondResult).toEqual([
      expect.objectContaining({ type: 'review_verdict', payload: expect.objectContaining({ decision: 'advance' }) }),
    ]);
    expect(calls.sendMessage).toHaveBeenCalledTimes(3);
    expect(calls.sendMessage.mock.calls.map((call) => call[1])).toEqual([
      expect.stringMatching(/^\[review_request iter=0\]/),
      expect.stringMatching(/^\[review_request iter=0\]/),
      expect.stringMatching(/^\[review_request iter=1\]/),
    ]);
  });

  it('releases the Reviewer dispatch queue after a failed stage so the next distinct request can run', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    ensureCompleteReviewArtifacts(dispatcher, 1);
    const first = dispatcher.deliver(
      fixedIdentity(
        Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] }),
        'review-failed-stage-0',
      ),
    );
    const second = dispatcher.deliver(
      fixedIdentity(
        Msg.reviewRequest(1, { iter: 1, ledger_path: ledgerDir, prior_metrics: [] }),
        'review-after-failed-stage-1',
      ),
    );

    await expect(first).rejects.toThrow(/complete artifact set.*missing/i);
    await expect(second).resolves.toEqual([
      expect.objectContaining({ type: 'review_verdict', payload: expect.objectContaining({ decision: 'advance' }) }),
    ]);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
    expect(calls.sendMessage.mock.calls[0][1]).toMatch(/^\[review_request iter=1\]/);
  });

  it('skips a queued Reviewer dispatch when the run becomes terminal during the active send', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher();
    ensureCompleteReviewArtifacts(dispatcher, 0);
    ensureCompleteReviewArtifacts(dispatcher, 1);
    let signalFirstSend!: () => void;
    const firstSendEntered = new Promise<void>((resolve) => {
      signalFirstSend = resolve;
    });
    let releaseFirstSend!: () => void;
    const firstSendGate = new Promise<void>((resolve) => {
      releaseFirstSend = resolve;
    });
    calls.sendMessage.mockImplementation(async () => {
      signalFirstSend();
      await firstSendGate;
      return { output: reviewerReply, error: undefined };
    });

    const first = dispatcher.deliver(
      fixedIdentity(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] }), 'review-terminal-0'),
    );
    await firstSendEntered;
    const second = dispatcher.deliver(
      fixedIdentity(Msg.reviewRequest(1, { iter: 1, ledger_path: ledgerDir, prior_metrics: [] }), 'review-terminal-1'),
    );
    await dispatcher.shutdown('operator-stop');
    releaseFirstSend();

    await expect(first).resolves.toEqual([]);
    await expect(second).resolves.toEqual([]);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
    expect(
      fs
        .readdirSync(path.join(ledgerDir, 'reviewer_sandbox'))
        .filter((entry) => entry.startsWith('iter-'))
        .sort(),
    ).toEqual(['iter-0']);
  });

  it('snapshots a proxied chat before Planner startup and uses only the snapshot for chat and prompt effects', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'Planner reply' });
    const target = { text: 'stable chat' };
    const descriptorReads = new Map<PropertyKey, number>();
    let ordinaryReads = 0;
    const payload = new Proxy(target, {
      get(inner, key, receiver) {
        ordinaryReads += 1;
        return Reflect.get(inner, key, receiver);
      },
      getOwnPropertyDescriptor(inner, key) {
        descriptorReads.set(key, (descriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(inner, key);
      },
    });
    const startSession = calls.startSession.getMockImplementation()!;
    calls.startSession.mockImplementation(async (config, generation) => {
      target.text = 'mutated after canonicalization';
      return await startSession(config, generation);
    });

    const message = Msg.chat(0, payload);
    await dispatcher.deliver(message);

    expect(ordinaryReads).toBe(0);
    expect(descriptorReads.get('text')).toBe(1);
    expect(calls.sendMessage.mock.calls[0][1]).toBe('stable chat');
    const chatLines = fs.readFileSync(path.join(ledgerDir, 'chat.jsonl'), 'utf8').trimEnd().split('\n');
    expect(chatLines[0]).toBe(JSON.stringify({ who: 'user', text: 'stable chat', ts: message.ts }));
    const chat = JSON.parse(chatLines[0]) as Record<string, unknown>;
    expect(chat).toMatchObject({ who: 'user', text: 'stable chat' });
  });

  it('snapshots a proxied review request before Reviewer startup and preserves exact clean prompt bytes', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    ensureCompleteReviewArtifacts(dispatcher, 4);
    const metrics = [1, 2];
    const target = { iter: 4, ledger_path: ledgerDir, prior_metrics: metrics };
    const descriptorReads = new Map<PropertyKey, number>();
    let ordinaryReads = 0;
    const payload = new Proxy(target, {
      get(inner, key, receiver) {
        ordinaryReads += 1;
        return Reflect.get(inner, key, receiver);
      },
      getOwnPropertyDescriptor(inner, key) {
        descriptorReads.set(key, (descriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(inner, key);
      },
    });
    const startSession = calls.startSession.getMockImplementation()!;
    calls.startSession.mockImplementation(async (config, generation) => {
      target.iter = 9;
      target.ledger_path = '/mutated/path';
      metrics[0] = 99;
      return await startSession(config, generation);
    });

    await dispatcher.deliver(Msg.reviewRequest(4, payload));

    expect(ordinaryReads).toBe(0);
    for (const field of ['iter', 'ledger_path', 'prior_metrics']) expect(descriptorReads.get(field)).toBe(1);
    expect(calls.sendMessage.mock.calls[0][1]).toBe(
      [
        '[review_request iter=4]',
        'Artifacts staged at: iter-4/ (directive.json, diff.patch, eval_output.json)',
        'prior_verdict: (none)',
        'prior_metrics: [1,2]',
        '',
        'Audit and emit `review_complete`.',
      ].join('\n'),
    );
  });

  it('preserves clean directive_ack and iter_done prompt bytes while snapshotting before Planner startup', async () => {
    const { dispatcher, calls } = makeDispatcher({}, { sendOutputs: ['ack reply', 'done reply'] });
    const ack = { understood: true, clarification: 'none' };
    const startSession = calls.startSession.getMockImplementation()!;
    calls.startSession.mockImplementation(async (config, generation) => {
      ack.understood = false;
      ack.clarification = 'mutated';
      return await startSession(config, generation);
    });

    await dispatcher.deliver(Msg.directiveAck(2, ack));
    await dispatcher.deliver(Msg.iterDone(2, { iter: 2, verdict: 'advance', metric: 1, regression: false }));

    expect(calls.sendMessage.mock.calls[0][1]).toBe(
      '[system] coder directive_ack iter=2: {"understood":true,"clarification":"none"}',
    );
    expect(calls.sendMessage.mock.calls[1][1]).toBe('[system] iter 2 done. verdict=advance metric=1');
  });

  it('snapshots an iter_done delivered first and never consults its mutated source after Planner startup', async () => {
    const { dispatcher, calls } = makeDispatcher({}, { sendOutput: 'done reply' });
    const target = { iter: 2, verdict: 'advance' as const, metric: 1, regression: false };
    const descriptorReads = new Map<PropertyKey, number>();
    let ordinaryReads = 0;
    let lateAccessorReads = 0;
    const payload = new Proxy(target, {
      get(inner, key, receiver) {
        ordinaryReads += 1;
        return Reflect.get(inner, key, receiver);
      },
      getOwnPropertyDescriptor(inner, key) {
        descriptorReads.set(key, (descriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(inner, key);
      },
    });
    const startSession = calls.startSession.getMockImplementation()!;
    calls.startSession.mockImplementation(async (config, generation) => {
      for (const key of ['iter', 'verdict', 'metric', 'regression'] as const) {
        const value = target[key];
        Object.defineProperty(target, key, {
          configurable: true,
          enumerable: true,
          get() {
            lateAccessorReads += 1;
            return value;
          },
        });
      }
      return await startSession(config, generation);
    });

    await dispatcher.deliver(Msg.iterDone(2, payload));

    expect(ordinaryReads).toBe(0);
    expect(lateAccessorReads).toBe(0);
    for (const field of ['iter', 'verdict', 'metric', 'regression']) expect(descriptorReads.get(field)).toBe(1);
    expect(calls.sendMessage.mock.calls[0][1]).toBe('[system] iter 2 done. verdict=advance metric=1');
  });

  it('rejects an iter_done envelope/payload mismatch before Planner, chat, or sandbox effects', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });

    await expect(
      dispatcher.deliver(Msg.iterDone(4, { iter: 5, verdict: 'advance', metric: 1 })),
    ).rejects.toBeInstanceOf(AutoloopRoutingError);

    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    expect(fs.existsSync(path.join(ledgerDir, 'decisions.jsonl'))).toBe(false);
    expect(fs.existsSync(path.join(ledgerDir, 'chat.jsonl'))).toBe(false);
    expect(fs.existsSync(path.join(ledgerDir, 'reviewer_sandbox'))).toBe(false);
  });

  it.each([
    ['chat', Msg.chat(0, Object.create({ text: 'inherited' }) as { text: string })],
    ['directive_ack', Msg.directiveAck(0, Object.create({ understood: true }) as { understood: boolean })],
    [
      'iter_done',
      Msg.iterDone(0, Object.create({ iter: 0, verdict: 'advance', metric: 1 }) as Parameters<typeof Msg.iterDone>[1]),
    ],
    [
      'review_request',
      Msg.reviewRequest(
        0,
        Object.create({ iter: 0, ledger_path: '/trusted/run', prior_metrics: [] }) as Parameters<
          typeof Msg.reviewRequest
        >[1],
      ),
    ],
  ] as const)('rejects inherited required fields for %s as a typed pre-effect routing error', async (_type, env) => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });

    await expect(dispatcher.deliver(env)).rejects.toBeInstanceOf(AutoloopRoutingError);

    expect(fs.existsSync(path.join(ledgerDir, 'chat.jsonl'))).toBe(false);
    expect(fs.existsSync(path.join(ledgerDir, 'reviewer_sandbox'))).toBe(false);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it.each([
    ['chat', () => Msg.chat(0, { text: 'chat', extra: true } as unknown as Parameters<typeof Msg.chat>[1])],
    [
      'directive_ack',
      () => Msg.directiveAck(0, { understood: true, extra: true } as unknown as Parameters<typeof Msg.directiveAck>[1]),
    ],
    [
      'iter_done',
      () =>
        Msg.iterDone(0, {
          iter: 0,
          verdict: 'advance',
          metric: 1,
          extra: true,
        } as unknown as Parameters<typeof Msg.iterDone>[1]),
    ],
    [
      'review_request',
      () =>
        Msg.reviewRequest(0, {
          iter: 0,
          ledger_path: '/trusted/run',
          prior_metrics: [],
          extra: true,
        } as unknown as Parameters<typeof Msg.reviewRequest>[1]),
    ],
  ] as const)('rejects extra own fields for %s before any effect', async (_type, buildMessage) => {
    const { dispatcher, calls } = makeDispatcher({}, { sendOutput: 'must not be sent' });

    await expect(dispatcher.deliver(buildMessage())).rejects.toBeInstanceOf(AutoloopRoutingError);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it.each([
    ['chat', () => Msg.chat(0, { text: 1 } as unknown as Parameters<typeof Msg.chat>[1])],
    [
      'directive_ack understood',
      () => Msg.directiveAck(0, { understood: 'yes' } as unknown as Parameters<typeof Msg.directiveAck>[1]),
    ],
    [
      'iter_done iter',
      () => Msg.iterDone(0, { iter: -1, verdict: 'advance', metric: 1 } as Parameters<typeof Msg.iterDone>[1]),
    ],
    [
      'iter_done verdict',
      () =>
        Msg.iterDone(0, {
          iter: 0,
          verdict: 'pause',
          metric: 1,
        } as unknown as Parameters<typeof Msg.iterDone>[1]),
    ],
    ['iter_done metric', () => Msg.iterDone(0, { iter: 0, verdict: 'hold', metric: Number.NaN })],
    [
      'review_request ledger_path',
      () =>
        Msg.reviewRequest(0, {
          iter: 0,
          ledger_path: 1,
          prior_metrics: [],
        } as unknown as Parameters<typeof Msg.reviewRequest>[1]),
    ],
    [
      'review_request ledger_path UTF-8 bound',
      () =>
        Msg.reviewRequest(0, {
          iter: 0,
          ledger_path: 'é'.repeat(4_097),
          prior_metrics: [],
        }),
    ],
    [
      'review_request metric',
      () => Msg.reviewRequest(0, { iter: 0, ledger_path: '/trusted/run', prior_metrics: [Number.POSITIVE_INFINITY] }),
    ],
  ] as const)('rejects invalid %s types or ranges as typed pre-effect errors', async (_description, buildMessage) => {
    const { dispatcher, calls } = makeDispatcher({}, { sendOutput: 'must not be sent' });

    await expect(dispatcher.deliver(buildMessage())).rejects.toBeInstanceOf(AutoloopRoutingError);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it.each([
    [
      'directive_ack clarification',
      Msg.directiveAck(0, { understood: true, clarification: undefined }),
      '[system] coder directive_ack iter=0: {"understood":true}',
    ],
    [
      'iter_done regression',
      Msg.iterDone(0, { iter: 0, verdict: 'hold', metric: null, regression: undefined }),
      '[system] iter 0 done. verdict=hold metric=null',
    ],
  ] as const)('accepts and strips own undefined %s before Planner delivery', async (_description, message, prompt) => {
    const { dispatcher, calls } = makeDispatcher({}, { sendOutput: 'Planner reply' });

    await expect(dispatcher.deliver(message)).resolves.toEqual([]);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
    expect(calls.sendMessage.mock.calls[0][1]).toBe(prompt);
  });

  it('types directive schema rejection as an AutoloopRoutingError at the same public boundary', async () => {
    const { dispatcher, calls } = makeDispatcher({}, { sendOutput: 'must not be sent' });

    await expect(
      dispatcher.deliver(
        Msg.directive(0, {
          goal: 'typed rejection',
          constraints: [],
          success_criteria: [],
          max_attempts: 0,
        }),
      ),
    ).rejects.toBeInstanceOf(AutoloopRoutingError);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it.each(['chat', 'directive_ack', 'iter_done', 'review_request'] as const)(
    'rejects symbol own fields for %s before any effect',
    async (type) => {
      const { dispatcher, calls } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      const payload: Record<PropertyKey, unknown> =
        type === 'chat'
          ? { text: 'chat' }
          : type === 'directive_ack'
            ? { understood: true }
            : type === 'iter_done'
              ? { iter: 0, verdict: 'advance', metric: 1 }
              : { iter: 0, ledger_path: '/trusted/run', prior_metrics: [] };
      Object.defineProperty(payload, Symbol('payload-metadata'), { value: true });
      const env =
        type === 'chat'
          ? Msg.chat(0, payload as unknown as Parameters<typeof Msg.chat>[1])
          : type === 'directive_ack'
            ? Msg.directiveAck(0, payload as unknown as Parameters<typeof Msg.directiveAck>[1])
            : type === 'iter_done'
              ? Msg.iterDone(0, payload as unknown as Parameters<typeof Msg.iterDone>[1])
              : Msg.reviewRequest(0, payload as unknown as Parameters<typeof Msg.reviewRequest>[1]);

      await expect(dispatcher.deliver(env)).rejects.toBeInstanceOf(AutoloopRoutingError);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it.each(['hole', 'accessor', 'named', 'symbol'] as const)(
    'rejects review_request prior_metrics with a %s without invoking array accessors',
    async (kind) => {
      const { dispatcher, calls } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      const metrics = [1];
      let getterHits = 0;
      if (kind === 'hole') metrics.length = 2;
      if (kind === 'accessor') {
        Object.defineProperty(metrics, '0', {
          configurable: true,
          enumerable: true,
          get() {
            getterHits += 1;
            return 1;
          },
        });
      }
      if (kind === 'named') Object.defineProperty(metrics, 'metadata', { value: 'unsupported' });
      if (kind === 'symbol') Object.defineProperty(metrics, Symbol('metadata'), { value: 'unsupported' });

      await expect(
        dispatcher.deliver(Msg.reviewRequest(0, { iter: 0, ledger_path: '/trusted/run', prior_metrics: metrics })),
      ).rejects.toBeInstanceOf(AutoloopRoutingError);

      expect(getterHits).toBe(0);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it('rejects own payload toJSON without invoking it and ignores inherited payload toJSON during prompt serialization', async () => {
    const ownPayload = { understood: true } as Record<string, unknown>;
    let ownToJsonHits = 0;
    Object.defineProperty(ownPayload, 'toJSON', {
      configurable: true,
      enumerable: false,
      get() {
        ownToJsonHits += 1;
        return () => ({ understood: false });
      },
    });
    const own = makeDispatcher({}, { sendOutput: 'must not be sent' });

    await expect(
      own.dispatcher.deliver(Msg.directiveAck(0, ownPayload as unknown as Parameters<typeof Msg.directiveAck>[1])),
    ).rejects.toBeInstanceOf(AutoloopRoutingError);
    expect(ownToJsonHits).toBe(0);
    expect(own.calls.startSession).toHaveBeenCalledTimes(0);
    expect(own.calls.sendMessage).toHaveBeenCalledTimes(0);

    const inheritedPayload = { understood: true, clarification: 'stable' };
    const inherited = makeDispatcher({}, { sendOutput: 'Planner reply' });
    let inheritedToJsonHits = 0;
    const { thrown } = await withPrototypeDescriptors(
      [
        {
          target: Object.prototype,
          key: 'toJSON',
          descriptor: {
            configurable: true,
            get() {
              if (this === inheritedPayload) {
                inheritedToJsonHits += 1;
                return () => ({ understood: false, clarification: 'attacker chosen' });
              }
              return undefined;
            },
          },
        },
      ],
      () => inherited.dispatcher.deliver(Msg.directiveAck(0, inheritedPayload)),
    );

    expect(thrown).toBeUndefined();
    expect(inheritedToJsonHits).toBe(0);
    expect(inherited.calls.sendMessage.mock.calls[0][1]).toBe(
      '[system] coder directive_ack iter=0: {"understood":true,"clarification":"stable"}',
    );
  });

  it('serializes chat audit records without consulting Object.prototype.toJSON', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'Planner reply' });
    let toJsonHits = 0;
    const { thrown } = await withPrototypeDescriptors(
      [
        {
          target: Object.prototype,
          key: 'toJSON',
          descriptor: {
            configurable: true,
            get() {
              if (Object.hasOwn(this, 'who') && Object.hasOwn(this, 'text') && Object.hasOwn(this, 'ts')) {
                toJsonHits += 1;
                return () => ({ attacker_chosen_chat: true });
              }
              return undefined;
            },
          },
        },
      ],
      () => dispatcher.deliver(Msg.chat(0, { text: 'stable chat audit' })),
    );

    expect(thrown).toBeUndefined();
    expect(toJsonHits).toBe(0);
    expect(calls.sendMessage.mock.calls[0][1]).toBe('stable chat audit');
    const firstChatLine = fs.readFileSync(path.join(ledgerDir, 'chat.jsonl'), 'utf8').split('\n')[0];
    expect(JSON.parse(firstChatLine)).toMatchObject({
      who: 'user',
      text: 'stable chat audit',
    });
  });

  it('builds Reviewer system/message prompts without polluted join or prior_metrics toJSON hooks', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: reviewerReply });
    ensureCompleteReviewArtifacts(dispatcher, 0);
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.mkdirSync(sandbox, { recursive: true });
    fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'stable reviewer memory\n');
    const priorMetrics = [1, 2];
    const originalJoin = Array.prototype.join;
    let joinHits = 0;
    let toJsonHits = 0;

    const { thrown } = await withPrototypeDescriptors(
      [
        {
          target: Array.prototype,
          key: 'join',
          descriptor: {
            configurable: true,
            value: function pollutedJoin(this: unknown[], separator?: string): string {
              if (this[0] === '[review_request iter=0]' || this[2] === '<frozen_memory_snapshot>') {
                joinHits += 1;
                return 'attacker-chosen-reviewer-prompt';
              }
              return originalJoin.call(this, separator);
            },
            writable: true,
          },
        },
        {
          target: Array.prototype,
          key: 'toJSON',
          descriptor: {
            configurable: true,
            get() {
              if (this === priorMetrics) {
                toJsonHits += 1;
                return () => ['attacker-chosen-metric'];
              }
              return undefined;
            },
          },
        },
      ],
      () => dispatcher.deliver(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: priorMetrics })),
    );

    expect(thrown).toBeUndefined();
    expect(joinHits).toBe(0);
    expect(toJsonHits).toBe(0);
    const reviewerStart = calls.startSession.mock.calls.find(
      ([config]) => (config as { name: string }).name === 'autoloop-r1-reviewer',
    );
    expect((reviewerStart?.[0] as { systemPrompt: string }).systemPrompt).toContain('stable reviewer memory');
    expect(calls.sendMessage.mock.calls[0][1]).toContain('prior_metrics: [1,2]');
    expect(calls.sendMessage.mock.calls[0][1]).not.toContain('attacker-chosen');
  });
});

describe('ClaudeAgentDispatcher — durable directive ordering and review iteration authority', () => {
  const directivePayload = {
    goal: 'persist this exact directive before starting a Coder',
    constraints: ['one writer', 'no speculative send'],
    success_criteria: ['durable intent precedes every agent effect'],
    max_attempts: 2,
  };
  const v1DirectiveMessageId = 'directive-restart-v1-2';
  const v1DirectiveTimestamp = '2026-09-03T00:00:00.000Z';
  const v1DirectiveBytes = [
    '{',
    '  "schema_version": 1,',
    '  "iter": 2,',
    `  "ts": "${v1DirectiveTimestamp}",`,
    `  "message_id": "${v1DirectiveMessageId}",`,
    '  "dispatch_id": "dispatch_107b7aedd81cedd949e7cbf6d971b759fd017304241766c27209186719946a5d",',
    '  "goal": "persist this exact directive before starting a Coder",',
    '  "constraints": [',
    '    "one writer",',
    '    "no speculative send"',
    '  ],',
    '  "success_criteria": [',
    '    "durable intent precedes every agent effect"',
    '  ],',
    '  "max_attempts": 2',
    '}',
  ].join('\n');

  const requiredDirectiveFields = ['goal', 'constraints', 'success_criteria', 'max_attempts'] as const;
  type RequiredDirectiveField = (typeof requiredDirectiveFields)[number];

  function sparseDirectivePayload(
    missing: RequiredDirectiveField,
    inherited: 'none' | 'data' | 'getter',
    onGetter: () => void,
  ): Parameters<typeof Msg.directive>[1] {
    const prototype = Object.create(null) as Record<string, unknown>;
    if (inherited === 'data') {
      Object.defineProperty(prototype, missing, {
        configurable: true,
        enumerable: false,
        value: directivePayload[missing],
        writable: true,
      });
    } else if (inherited === 'getter') {
      Object.defineProperty(prototype, missing, {
        configurable: true,
        enumerable: false,
        get() {
          onGetter();
          return directivePayload[missing];
        },
      });
    }
    const payload = Object.create(inherited === 'none' ? Object.prototype : prototype) as Record<string, unknown>;
    for (const key of requiredDirectiveFields) {
      if (key === missing) continue;
      Object.defineProperty(payload, key, {
        configurable: true,
        enumerable: true,
        value: directivePayload[key],
        writable: true,
      });
    }
    return payload as unknown as Parameters<typeof Msg.directive>[1];
  }

  const envelopeIdentityFields = ['msg_id', 'iter', 'from', 'to', 'type', 'ts', 'payload'] as const;

  it('rejects inherited envelope identity before persistence or any Coder effect', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    const valid = fixedIdentity(Msg.directive(0, directivePayload), 'inherited-envelope-identity');
    const inherited = Object.create(valid) as AnyAutoloopMessage;

    await expect(dispatcher.deliver(inherited)).rejects.toMatchObject({
      name: 'AutoloopRoutingError',
      message: expect.stringMatching(/envelope|identity|own data property/i),
    });

    expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it.each(envelopeIdentityFields)(
    'rejects an own accessor for envelope identity field %s without invoking it',
    async (field) => {
      const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      const envelope = fixedIdentity(
        Msg.directive(0, directivePayload),
        `accessor-envelope-${field}`,
      ) as unknown as Record<string, unknown>;
      const accessorValue = envelope[field];
      let getterHits = 0;
      Object.defineProperty(envelope, field, {
        configurable: true,
        enumerable: true,
        get() {
          getterHits += 1;
          return accessorValue;
        },
      });
      let thrown: unknown;

      try {
        await dispatcher.deliver(envelope as unknown as AnyAutoloopMessage);
      } catch (error) {
        thrown = error;
      }

      expect(getterHits).toBe(0);
      expect(thrown).toBeInstanceOf(AutoloopRoutingError);
      expect((thrown as Error).message).toMatch(/envelope|identity|own data property/i);
      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it.each([-1, 1.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid envelope iteration %s before persistence or any Coder effect',
    async (iter) => {
      const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      const envelope = fixedIdentity(Msg.directive(0, directivePayload), `invalid-envelope-iter-${String(iter)}`);
      Object.defineProperty(envelope, 'iter', {
        configurable: true,
        enumerable: true,
        value: iter,
        writable: true,
      });

      await expect(dispatcher.deliver(envelope)).rejects.toMatchObject({
        name: 'AutoloopRoutingError',
        message: expect.stringMatching(/envelope|identity|iteration|iter/i),
      });

      expect(fs.existsSync(path.join(ledgerDir, 'iter', String(iter), 'directive.json'))).toBe(false);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it('uses one immutable envelope and directive snapshot across persistence and Coder startup', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'Coder acknowledged.' });
    const payloadTarget = {
      goal: 'stable goal',
      constraints: ['stable constraint'],
      success_criteria: ['stable success'],
      max_attempts: 2,
    };
    const payloadDescriptorReads = new Map<PropertyKey, number>();
    let payloadOrdinaryReads = 0;
    const payload = new Proxy(payloadTarget, {
      get(target, key, receiver) {
        payloadOrdinaryReads += 1;
        return Reflect.get(target, key, receiver);
      },
      getOwnPropertyDescriptor(target, key) {
        payloadDescriptorReads.set(key, (payloadDescriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    const envelopeTarget = fixedIdentity(Msg.directive(2, payload), 'immutable-envelope-and-payload');
    const envelopeDescriptorReads = new Map<PropertyKey, number>();
    let envelopeOrdinaryReads = 0;
    const envelope = new Proxy(envelopeTarget, {
      get(target, key, receiver) {
        envelopeOrdinaryReads += 1;
        return Reflect.get(target, key, receiver);
      },
      getOwnPropertyDescriptor(target, key) {
        envelopeDescriptorReads.set(key, (envelopeDescriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    const reserveAgentGeneration = calls.reserveAgentGeneration.getMockImplementation()!;
    calls.reserveAgentGeneration.mockImplementation((generation: PhysicalAgentGeneration) => {
      payloadTarget.goal = 'mutated goal';
      payloadTarget.constraints[0] = 'mutated constraint';
      payloadTarget.success_criteria[0] = 'mutated success';
      payloadTarget.max_attempts = 99;
      return reserveAgentGeneration(generation);
    });

    await dispatcher.deliver(envelope);

    expect(envelopeOrdinaryReads).toBe(0);
    for (const field of envelopeIdentityFields) expect(envelopeDescriptorReads.get(field)).toBe(1);
    expect(payloadOrdinaryReads).toBe(0);
    for (const field of requiredDirectiveFields) expect(payloadDescriptorReads.get(field)).toBe(1);
    const persisted = JSON.parse(
      fs.readFileSync(path.join(ledgerDir, 'iter', '2', 'directive.json'), 'utf8'),
    ) as Record<string, unknown>;
    expect(persisted).toEqual({
      schema_version: LEDGER_SCHEMA_VERSION,
      iter: 2,
      ts: envelopeTarget.ts,
      message_id: envelopeTarget.msg_id,
      dispatch_id: expect.stringMatching(/^dispatch_[a-f0-9]{64}$/),
      goal: 'stable goal',
      constraints: ['stable constraint'],
      success_criteria: ['stable success'],
      max_attempts: 2,
    });
    const prompt = calls.sendMessage.mock.calls[0][1] as string;
    expect(prompt).toContain(
      [
        '[directive iter=2]',
        'goal: stable goal',
        'constraints:\n  - stable constraint',
        'success_criteria:\n  - stable success',
        'max_attempts: 2',
      ].join('\n'),
    );
    expect(prompt).not.toContain('mutated');
  });

  it.each(
    requiredDirectiveFields.flatMap((field) =>
      (['none', 'data', 'getter'] as const).map((inherited) => [field, inherited] as const),
    ),
  )(
    'rejects a directive with missing own %s supplied by %s inheritance before any Coder effect',
    async (field, inherited) => {
      const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      let getterHits = 0;
      const payload = sparseDirectivePayload(field, inherited, () => {
        getterHits += 1;
      });
      let thrown: unknown;

      try {
        await dispatcher.deliver(Msg.directive(0, payload));
      } catch (error) {
        thrown = error;
      }

      expect(getterHits).toBe(0);
      expect(thrown).toBeInstanceOf(AutoloopRoutingError);
      expect((thrown as Error).message).toMatch(/directive payload.*invalid/i);
      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it.each(requiredDirectiveFields)(
    'rejects an own accessor for directive field %s without invoking it',
    async (field) => {
      const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      const payload = { ...directivePayload } as Record<string, unknown>;
      let getterHits = 0;
      Object.defineProperty(payload, field, {
        configurable: true,
        enumerable: true,
        get() {
          getterHits += 1;
          return directivePayload[field];
        },
      });
      let thrown: unknown;

      try {
        await dispatcher.deliver(Msg.directive(0, payload as unknown as Parameters<typeof Msg.directive>[1]));
      } catch (error) {
        thrown = error;
      }

      expect(getterHits).toBe(0);
      expect(thrown).toBeInstanceOf(AutoloopRoutingError);
      expect((thrown as Error).message).toMatch(/directive payload.*invalid/i);
      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it.each(['constraints', 'success_criteria'] as const)(
    'rejects sparse holes in directive %s before persistence or any Coder effect',
    async (field) => {
      const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      const sparse = ['present'];
      sparse.length = 2;
      const payload = { ...directivePayload, [field]: sparse };

      await expect(dispatcher.deliver(Msg.directive(0, payload))).rejects.toMatchObject({
        name: 'AutoloopRoutingError',
        message: expect.stringMatching(/directive payload.*invalid/i),
      });

      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it.each(['constraints', 'success_criteria'] as const)(
    'rejects an index accessor in directive %s without invoking it',
    async (field) => {
      const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      const values = ['stable'];
      let getterHits = 0;
      Object.defineProperty(values, '0', {
        configurable: true,
        enumerable: true,
        get() {
          getterHits += 1;
          return 'attacker value';
        },
      });
      const payload = { ...directivePayload, [field]: values };

      await expect(dispatcher.deliver(Msg.directive(0, payload))).rejects.toMatchObject({
        name: 'AutoloopRoutingError',
        message: expect.stringMatching(/directive payload.*invalid/i),
      });

      expect(getterHits).toBe(0);
      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it.each(['map', 'join', 'push', 'metadata'] as const)(
    'rejects directive arrays with extra named own property %s without invoking it',
    async (property) => {
      const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
      const constraints = ['stable constraint'] as string[] & Record<string, unknown>;
      let methodHits = 0;
      Object.defineProperty(constraints, property, {
        configurable: true,
        enumerable: false,
        value() {
          methodHits += 1;
          return ['attacker constraint'];
        },
        writable: true,
      });

      await expect(dispatcher.deliver(Msg.directive(0, { ...directivePayload, constraints }))).rejects.toMatchObject({
        name: 'AutoloopRoutingError',
        message: expect.stringMatching(/directive payload.*invalid/i),
      });

      expect(methodHits).toBe(0);
      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    },
  );

  it('rejects directive arrays with extra symbol own properties before persistence or Coder effects', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    const constraints = ['stable constraint'];
    Object.defineProperty(constraints, Symbol('directive-array-metadata'), {
      configurable: true,
      enumerable: false,
      value: 'must not be stripped',
      writable: true,
    });

    await expect(dispatcher.deliver(Msg.directive(0, { ...directivePayload, constraints }))).rejects.toMatchObject({
      name: 'AutoloopRoutingError',
      message: expect.stringMatching(/directive payload.*invalid/i),
    });

    expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it('rejects directive payload symbols before persistence or any Coder effect', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    const payload = { ...directivePayload };
    Object.defineProperty(payload, Symbol('directive-payload-metadata'), {
      configurable: true,
      enumerable: false,
      value: 'must not be stripped',
      writable: true,
    });

    await expect(dispatcher.deliver(Msg.directive(0, payload))).rejects.toMatchObject({
      name: 'AutoloopRoutingError',
      message: expect.stringMatching(/directive payload.*(?:additional|reserved|unsupported)/i),
    });

    expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it.each([
    ['own undefined goal', { goal: undefined }],
    ['non-string goal', { goal: 1 }],
    ['own undefined constraints', { constraints: undefined }],
    ['scalar constraints', { constraints: 'one writer' }],
    ['constraints with a non-string member', { constraints: ['one writer', 1] }],
    ['own undefined success_criteria', { success_criteria: undefined }],
    ['scalar success_criteria', { success_criteria: 'durable first' }],
    ['success_criteria with a non-string member', { success_criteria: ['durable first', null] }],
    ['own undefined max_attempts', { max_attempts: undefined }],
    ['NaN max_attempts', { max_attempts: Number.NaN }],
    ['infinite max_attempts', { max_attempts: Number.POSITIVE_INFINITY }],
    ['fractional max_attempts', { max_attempts: 1.5 }],
    ['zero max_attempts', { max_attempts: 0 }],
    ['negative max_attempts', { max_attempts: -1 }],
    ['unsafe max_attempts', { max_attempts: Number.MAX_SAFE_INTEGER + 1 }],
    ['non-number max_attempts', { max_attempts: '1' }],
  ])('rejects a directive with %s before persistence or Coder effects', async (_description, change) => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    const payload = { ...directivePayload, ...change } as unknown as Parameters<typeof Msg.directive>[1];
    let thrown: unknown;

    try {
      await dispatcher.deliver(Msg.directive(0, payload));
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(AutoloopRoutingError);
    expect((thrown as Error).message).toMatch(/directive payload.*invalid/i);
    expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it('does not consult polluted array map, join, or push while freezing directive bytes and prompt text', async () => {
    const constraints = ['stable constraint'];
    const successCriteria = ['stable success'];
    const payload = {
      goal: 'stable goal',
      constraints,
      success_criteria: successCriteria,
      max_attempts: 2,
    };
    const { dispatcher, calls, ledgerDir } = makeDispatcher(
      { coderEngine: 'codex' },
      { sendOutput: 'Coder acknowledged.' },
    );
    await dispatcher.deliver(fixedIdentity(Msg.directive(1, payload), 'polluted-array-methods-warmup'));
    const originalMap = Array.prototype.map;
    const originalJoin = Array.prototype.join;
    const originalPush = Array.prototype.push;
    const mappedPayloadArrays = new WeakSet<object>();
    let mapHits = 0;
    let joinHits = 0;
    let pushHits = 0;

    const { thrown } = await withPrototypeDescriptors(
      [
        {
          target: Array.prototype,
          key: 'map',
          descriptor: {
            configurable: true,
            value: function pollutedMap(
              this: unknown[],
              callback: (value: unknown, index: number, array: unknown[]) => unknown,
              thisArg?: unknown,
            ): unknown[] {
              const result = originalMap.call(this, callback, thisArg);
              if (
                this === constraints ||
                this === successCriteria ||
                (typeof this[0] === 'object' && this[0] !== null && 'who' in this[0])
              ) {
                mapHits += 1;
                mappedPayloadArrays.add(result);
              }
              return result;
            },
            writable: true,
          },
        },
        {
          target: Array.prototype,
          key: 'join',
          descriptor: {
            configurable: true,
            value: function pollutedJoin(this: unknown[], separator?: string): string {
              if (
                mappedPayloadArrays.has(this) ||
                this[0] === '<autoloop_role_instructions>' ||
                this[0] === '<conversation_history>'
              ) {
                joinHits += 1;
                return 'attacker-chosen-prompt-array';
              }
              return originalJoin.call(this, separator);
            },
            writable: true,
          },
        },
        {
          target: Array.prototype,
          key: 'push',
          descriptor: {
            configurable: true,
            value: function pollutedPush(this: unknown[], ...items: unknown[]): number {
              if (
                items[0] === 'stable constraint' ||
                items[0] === 'stable success' ||
                items[0] === '<autoloop_message>' ||
                (typeof items[0] === 'object' && items[0] !== null && 'who' in items[0])
              ) {
                pushHits += 1;
                return originalPush.call(this, 'attacker-chosen-persisted-array');
              }
              return originalPush.apply(this, items);
            },
            writable: true,
          },
        },
      ],
      () => dispatcher.deliver(fixedIdentity(Msg.directive(2, payload), 'polluted-array-methods')),
    );

    expect(thrown).toBeUndefined();
    expect(mapHits).toBe(0);
    expect(joinHits).toBe(0);
    expect(pushHits).toBe(0);
    expect(JSON.parse(fs.readFileSync(path.join(ledgerDir, 'iter', '2', 'directive.json'), 'utf8'))).toMatchObject(
      payload,
    );
    const prompt = calls.sendMessage.mock.calls[1][1] as string;
    expect(prompt).toContain(
      [
        '[directive iter=2]',
        'goal: stable goal',
        'constraints:\n  - stable constraint',
        'success_criteria:\n  - stable success',
        'max_attempts: 2',
      ].join('\n'),
    );
    expect(prompt).not.toContain('attacker-chosen');
  });

  it('does not consult a polluted array iterator and preserves clean v1 bytes and distinct dispatch IDs', async () => {
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', { create: true });
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: 'must not be sent' });
    const stopAfterPersistence = new Error('stop after observing iterator-safe persistence');
    const writeIterationArtifact = secureLedger.writeIterationArtifact.bind(secureLedger);
    secureLedger.writeIterationArtifact = ((iter, name, content) => {
      const outcome = writeIterationArtifact(iter, name, content);
      if (name === 'directive.json') throw stopAfterPersistence;
      return outcome;
    }) as typeof secureLedger.writeIterationArtifact;
    const originalIterator = Array.prototype[Symbol.iterator];
    let iteratorHits = 0;
    let firstError: unknown;
    let secondError: unknown;
    const first = fixedIdentity(Msg.directive(2, directivePayload), v1DirectiveMessageId, v1DirectiveTimestamp);
    const second = fixedIdentity(
      Msg.directive(3, directivePayload),
      'directive-iterator-distinct-3',
      v1DirectiveTimestamp,
    );

    await withPrototypeDescriptors(
      [
        {
          target: Array.prototype,
          key: Symbol.iterator,
          descriptor: {
            configurable: true,
            value: function pollutedIterator(this: unknown[]): ArrayIterator<unknown> {
              iteratorHits += 1;
              return originalIterator.call(this);
            },
            writable: true,
          },
        },
      ],
      async () => {
        try {
          await dispatcher.deliver(first);
        } catch (error) {
          firstError = error;
        }
        try {
          await dispatcher.deliver(second);
        } catch (error) {
          secondError = error;
        }
      },
    );

    expect(firstError).toBe(stopAfterPersistence);
    expect(secondError).toBe(stopAfterPersistence);
    expect(iteratorHits).toBe(0);
    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '2', 'directive.json'), 'utf8')).toBe(v1DirectiveBytes);
    const firstPersisted = JSON.parse(v1DirectiveBytes) as { dispatch_id: string };
    const secondPersisted = JSON.parse(
      fs.readFileSync(path.join(ledgerDir, 'iter', '3', 'directive.json'), 'utf8'),
    ) as { dispatch_id: string };
    expect(secondPersisted.dispatch_id).toMatch(/^dispatch_[a-f0-9]{64}$/);
    expect(secondPersisted.dispatch_id).not.toBe(firstPersisted.dispatch_id);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it('persists exact canonical directive bytes without consulting inherited Object/Array toJSON hooks', async () => {
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', { create: true });
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: 'must not be sent' });
    const stopAfterPersistence = new Error('stop after observing directive persistence');
    const writeIterationArtifact = secureLedger.writeIterationArtifact.bind(secureLedger);
    vi.spyOn(secureLedger, 'writeIterationArtifact').mockImplementation((iter, name, content) => {
      const outcome = writeIterationArtifact(iter, name, content);
      if (name === 'directive.json') throw stopAfterPersistence;
      return outcome;
    });
    let objectGetterHits = 0;
    let objectCalls = 0;
    let arrayGetterHits = 0;
    let arrayCalls = 0;
    const directive = fixedIdentity(Msg.directive(2, directivePayload), v1DirectiveMessageId, v1DirectiveTimestamp);

    const { thrown } = await withPrototypeDescriptors(
      [
        {
          target: Object.prototype,
          key: 'toJSON',
          descriptor: {
            configurable: true,
            get() {
              objectGetterHits += 1;
              return () => {
                objectCalls += 1;
                return { attacker_chosen_directive: true };
              };
            },
          },
        },
        {
          target: Array.prototype,
          key: 'toJSON',
          descriptor: {
            configurable: true,
            get() {
              arrayGetterHits += 1;
              return () => {
                arrayCalls += 1;
                return ['attacker-chosen-array'];
              };
            },
          },
        },
      ],
      () => dispatcher.deliver(directive),
    );

    expect(thrown).toBe(stopAfterPersistence);
    expect(objectGetterHits).toBe(0);
    expect(objectCalls).toBe(0);
    expect(arrayGetterHits).toBe(0);
    expect(arrayCalls).toBe(0);
    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '2', 'directive.json'), 'utf8')).toBe(v1DirectiveBytes);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it('replays exact schema-v1 directive bytes after a dispatcher restart without changing the immutable artifact', async () => {
    const directive = fixedIdentity(Msg.directive(2, directivePayload), v1DirectiveMessageId, v1DirectiveTimestamp);
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', { create: true });
    secureLedger.writeIterationArtifact(2, 'directive.json', v1DirectiveBytes);

    // A new dispatcher has no in-memory logical-dispatch cache. Durable replay
    // compatibility therefore depends on reproducing the exact v1 bytes.
    const { dispatcher, calls, ledgerDir } = makeDispatcher(
      { secureLedger },
      { sendOutput: 'Coder acknowledged the persisted directive.' },
    );

    await expect(dispatcher.deliver(directive)).resolves.toHaveLength(1);

    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '2', 'directive.json'), 'utf8')).toBe(v1DirectiveBytes);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(1);
    expect(calls.startSession).toHaveBeenCalledTimes(1);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
  });

  it('fails closed on a different message identity after restart instead of equating semantic payloads', async () => {
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', { create: true });
    secureLedger.writeIterationArtifact(2, 'directive.json', v1DirectiveBytes);
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: 'must not be sent' });
    const distinctIntent = fixedIdentity(
      Msg.directive(2, directivePayload),
      'directive-restart-distinct-intent-2',
      v1DirectiveTimestamp,
    );

    // msg_id is the logical intent identity. Task 5's durable outbox and
    // acknowledgement protocol will own unacknowledged redelivery; payload
    // equality alone cannot prove that a second intent is safe to send.
    await expect(dispatcher.deliver(distinctIntent)).rejects.toMatchObject({
      name: 'Error',
      message: expect.stringMatching(/conflicting|immutable|overwrite/i),
    });

    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '2', 'directive.json'), 'utf8')).toBe(v1DirectiveBytes);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    expect(fs.existsSync(path.join(ledgerDir, 'chat.jsonl'))).toBe(false);
  });

  it('rejects a distinct message whose reserved payload keys forge byte-identical directive identity', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'Coder acknowledged.' });
    const first = fixedIdentity(Msg.directive(2, directivePayload), 'directive-canonical-first', v1DirectiveTimestamp);
    await dispatcher.deliver(first);
    const directivePath = path.join(ledgerDir, 'iter', '2', 'directive.json');
    const firstBytes = fs.readFileSync(directivePath);
    const persisted = JSON.parse(firstBytes.toString('utf8')) as { dispatch_id: string };
    const hostilePayload = {
      ...directivePayload,
      schema_version: LEDGER_SCHEMA_VERSION,
      iter: first.iter,
      ts: first.ts,
      message_id: first.msg_id,
      dispatch_id: persisted.dispatch_id,
    } as typeof directivePayload;
    const distinct = fixedIdentity(
      Msg.directive(2, hostilePayload),
      'directive-canonical-distinct',
      '2026-09-03T01:00:00.000Z',
    );

    await expect(dispatcher.deliver(distinct)).rejects.toMatchObject({
      name: 'AutoloopRoutingError',
      message: expect.stringMatching(/directive payload.*(?:reserved|unsupported)/i),
    });

    expect(fs.readFileSync(directivePath)).toEqual(firstBytes);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(1);
    expect(calls.startSession).toHaveBeenCalledTimes(1);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
  });

  it('rejects additional schema-v1 directive identity keys before persistence or Coder effects', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    const additionalIdentityPayload = {
      ...directivePayload,
      delivery_id: 'future-outbox-identity-must-not-enter-v1',
    } as typeof directivePayload;

    await expect(dispatcher.deliver(Msg.directive(0, additionalIdentityPayload))).rejects.toMatchObject({
      name: 'AutoloopRoutingError',
      message: expect.stringMatching(/directive payload.*(?:additional|unsupported)/i),
    });

    expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it('durably persists the complete directive before reserve, start, heartbeat, and send', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'Coder acknowledged.' });
    const ledger = dispatcher.secureLedgerCapability;
    const events: string[] = [];
    const writeIterationArtifact = ledger.writeIterationArtifact.bind(ledger);
    const appendFlatFile = ledger.appendFlatFile.bind(ledger);
    const reserveAgentGeneration = calls.reserveAgentGeneration.getMockImplementation()!;
    const startSession = calls.startSession.getMockImplementation()!;
    const sendMessage = calls.sendMessage.getMockImplementation()!;

    vi.spyOn(ledger, 'writeIterationArtifact').mockImplementation((iter, name, content) => {
      if (name === 'directive.json') events.push('persist:start');
      const outcome = writeIterationArtifact(iter, name, content);
      if (name === 'directive.json') events.push('persist:complete');
      return outcome;
    });
    calls.reserveAgentGeneration.mockImplementation((generation: PhysicalAgentGeneration) => {
      events.push('reserve');
      return reserveAgentGeneration(generation);
    });
    calls.startSession.mockImplementation(async (config, generation) => {
      events.push('start');
      return await startSession(config, generation);
    });
    vi.spyOn(ledger, 'appendFlatFile').mockImplementation((name, content, durable) => {
      if (name === 'chat.jsonl' && content.includes('Coder iter 2 working')) events.push('heartbeat');
      return appendFlatFile(name, content, durable);
    });
    calls.sendMessage.mockImplementation(async (name, message, options) => {
      events.push('send');
      return await sendMessage(name, message, options);
    });

    const directive = fixedIdentity(Msg.directive(2, directivePayload), 'directive-order-2');
    await dispatcher.deliver(directive);

    expect(events).toEqual(['persist:start', 'persist:complete', 'reserve', 'start', 'heartbeat', 'send']);
    const directivePath = path.join(ledgerDir, 'iter', '2', 'directive.json');
    const firstBytes = fs.readFileSync(directivePath);
    expect(JSON.parse(firstBytes.toString('utf8'))).toEqual({
      schema_version: LEDGER_SCHEMA_VERSION,
      iter: 2,
      ts: directive.ts,
      message_id: directive.msg_id,
      dispatch_id: expect.stringMatching(/^dispatch_[a-f0-9]{64}$/),
      ...directivePayload,
    });

    await dispatcher.deliver(directive);

    expect(fs.readFileSync(directivePath)).toEqual(firstBytes);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(1);
    expect(calls.startSession).toHaveBeenCalledTimes(1);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
  });

  it.each(['before-write', 'before-flush'] as const)(
    'propagates a directive %s failure before any Coder reservation or observable effect',
    async (phase) => {
      const failure = Object.assign(new Error(`injected directive ${phase} failure`), {
        code: `INJECTED_${phase.toUpperCase().replace('-', '_')}`,
      });
      const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
        create: true,
        testHooks: {
          beforeNestedTemporaryIo: (event) => {
            if (event.relativePath === 'iter/0/directive.json' && event.phase === phase) throw failure;
          },
        },
      });
      const { dispatcher, calls, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: 'must not be sent' });

      let observed: unknown;
      try {
        await dispatcher.deliver(Msg.directive(0, directivePayload));
      } catch (error) {
        observed = error;
      }

      expect(observed).toBe(failure);
      expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
      expect(calls.startSession).toHaveBeenCalledTimes(0);
      expect(calls.sendMessage).toHaveBeenCalledTimes(0);
      expect(fs.existsSync(path.join(ledgerDir, 'chat.jsonl'))).toBe(false);
      expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'directive.json'))).toBe(false);
    },
  );

  it('preserves a conflicting immutable directive and starts no Coder', async () => {
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', { create: true });
    secureLedger.writeIterationArtifact(0, 'directive.json', 'pre-existing conflicting bytes\n');
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: 'must not be sent' });
    const directivePath = path.join(ledgerDir, 'iter', '0', 'directive.json');
    const before = fs.readFileSync(directivePath);

    await expect(dispatcher.deliver(Msg.directive(0, directivePayload))).rejects.toThrow(/conflicting|immutable/i);

    expect(fs.readFileSync(directivePath)).toEqual(before);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    expect(fs.existsSync(path.join(ledgerDir, 'chat.jsonl'))).toBe(false);
  });

  it('propagates a committed directive failure without starting or messaging a Coder', async () => {
    const cause = new Error('injected failure after exclusive directive publish');
    const secureLedger = SecureAutoloopLedger.open(tmpRoot, 'r1', {
      create: true,
      testHooks: {
        afterNestedPublish: (event) => {
          if (event.relativePath === 'iter/0/directive.json') throw cause;
        },
      },
    });
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ secureLedger }, { sendOutput: 'must not be sent' });

    await expect(dispatcher.deliver(Msg.directive(0, directivePayload))).rejects.toMatchObject({
      name: 'SecureAutoloopLedgerCommitError',
      code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
      committed: true,
      retryable: false,
      cause,
    });

    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    expect(fs.existsSync(path.join(ledgerDir, 'chat.jsonl'))).toBe(false);
    expect(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'directive.json'), 'utf8')).toContain(
      directivePayload.goal,
    );
  });

  it('rejects a forged review_request iteration mismatch at the message validator', () => {
    const valid = fixedIdentity(
      Msg.reviewRequest(4, { iter: 4, ledger_path: '/trusted/run', prior_metrics: [] }),
      'review-mismatch-validator',
    );
    const forged = { ...valid, payload: { ...valid.payload, iter: 5 } } as AnyAutoloopMessage;

    expect(() => validateMessage(forged)).toThrowError(AutoloopRoutingError);
    expect(() => validateMessage(forged)).toThrow(/review_request.*envelope iter=4.*payload iter=5/i);
  });

  it('rejects a direct forged review_request before sandbox, reservation, start, or send', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    ensureCompleteReviewArtifacts(dispatcher, 4);
    const valid = fixedIdentity(
      Msg.reviewRequest(4, { iter: 4, ledger_path: ledgerDir, prior_metrics: [] }),
      'review-mismatch-direct',
    );
    const forged = { ...valid, payload: { ...valid.payload, iter: 5 } } as AnyAutoloopMessage;

    await expect(dispatcher.deliver(forged)).rejects.toBeInstanceOf(AutoloopRoutingError);

    expect(fs.existsSync(path.join(ledgerDir, 'reviewer_sandbox'))).toBe(false);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
  });

  it('rejects an accessor review_request iter without invoking it or causing Reviewer effects', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'must not be sent' });
    let payloadIterReads = 0;
    const base = fixedIdentity(
      Msg.reviewRequest(4, { iter: 4, ledger_path: ledgerDir, prior_metrics: [1] }),
      'review-accessor-iter',
    );
    const payload = {
      ledger_path: ledgerDir,
      prior_metrics: [1],
      get iter(): number {
        payloadIterReads += 1;
        return 4;
      },
    };
    const request = { ...base, payload } as AnyAutoloopMessage;

    await expect(dispatcher.deliver(request)).rejects.toBeInstanceOf(AutoloopRoutingError);

    expect(payloadIterReads).toBe(0);
    expect(calls.reserveAgentGeneration).toHaveBeenCalledTimes(0);
    expect(calls.startSession).toHaveBeenCalledTimes(0);
    expect(calls.sendMessage).toHaveBeenCalledTimes(0);
    expect(fs.existsSync(path.join(ledgerDir, 'reviewer_sandbox'))).toBe(false);
  });
});

describe('ClaudeAgentDispatcher — auto-compact', () => {
  it('fires compact + writes decisions.jsonl when contextPercent crosses threshold', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher(
      { compactThresholds: { planner: 50 } },
      { contextPercent: 90, sendOutput: 'no autoloop blocks here' },
    );

    await dispatcher.deliver(Msg.chat(0, { text: 'hi' }));

    expect(calls.compactSession).toHaveBeenCalledTimes(1);
    const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
    const lines = fs
      .readFileSync(decisionsPath, 'utf-8')
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l));
    const compactEntry = lines.find((l) => l.kind === 'compact');
    expect(compactEntry).toBeDefined();
    expect(compactEntry.payload.agent).toBe('planner');
  });
});

describe('ClaudeAgentDispatcher — ledger schema_version', () => {
  it('stamps schema_version on directive.json', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'no blocks' });
    await dispatcher.spawnSubagents();
    void calls; // unused
    await dispatcher.deliver(Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }));
    const written = JSON.parse(fs.readFileSync(path.join(ledgerDir, 'iter', '0', 'directive.json'), 'utf-8'));
    expect(written.schema_version).toBe(LEDGER_SCHEMA_VERSION);
  });
});
