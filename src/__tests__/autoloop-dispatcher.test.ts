/**
 * Tests for ClaudeAgentDispatcher — the layer between the runner's message
 * bus and the real persistent Claude sessions. We stub SessionManager so the
 * tests stay hermetic; only behaviour owned by the dispatcher (frozen-memory
 * injection, sandbox staging, send-failure surfacing, decisions audit, policy
 * silencing guard) is exercised.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import {
  applyPlannerToolCalls,
  parsePlannerReply,
  type PlannerToolCall,
  type PlannerToolEffects,
  validatePlannerToolCalls,
} from '../autoloop/planner-tools.js';
import { AutoloopRunner } from '../autoloop/runner.js';
import { type AnyAutoloopMessage, Msg } from '../autoloop/messages.js';
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

beforeEach(() => {
  tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-disp-'));
});

afterEach(() => {
  vi.useRealTimers();
  fs.rmSync(tmpRoot, { recursive: true, force: true });
});

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

  it('stops a newly started Coder when Reviewer startup fails', async () => {
    const { dispatcher, calls } = makeDispatcher({}, { startThrowsFor: 'reviewer' });

    await expect(dispatcher.spawnSubagents()).rejects.toThrow('reviewer failed to start');

    expect(calls.stopSession).toHaveBeenCalledWith('autoloop-r1-coder');
    calls.startSession.mockImplementation(async () => ({ name: 'x', state: 'ready' }));
    await dispatcher.spawnSubagents();
    expect(findStart(calls, 'coder')).toBeDefined();
    expect(findStart(calls, 'reviewer')).toBeDefined();
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

  it('keeps the non-Claude Reviewer memory snapshot frozen after session start', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({ reviewerEngine: 'gemini' });
    const sandbox = path.join(ledgerDir, 'reviewer_sandbox');
    fs.mkdirSync(sandbox, { recursive: true });
    fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'frozen-old-memory');
    await dispatcher.spawnSubagents();
    fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'new-memory-must-wait-for-reset');

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
    // Plant an iter dir so stageReviewSandbox can copy from it.
    const iterDir = path.join(ledgerDir, 'iter', '0');
    fs.mkdirSync(iterDir, { recursive: true });
    fs.writeFileSync(path.join(iterDir, 'directive.json'), '{}');

    // Reviewer needs to actually emit a review_complete or we'll observe a
    // 'hold' fallback. We just stub sendOutput to include a valid block.
    // Easier: directly call the private method via type assertion.
    (dispatcher as unknown as { stageReviewSandbox(iter: number): void }).stageReviewSandbox(0);

    expect(fs.existsSync(path.join(sandbox, 'reviewer_memory.md'))).toBe(true);
    expect(fs.existsSync(path.join(sandbox, 'reviewer_log.jsonl'))).toBe(true);
    expect(fs.existsSync(path.join(sandbox, 'scratch.txt'))).toBe(false);
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
