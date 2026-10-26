/**
 * Tests for ClaudeAgentDispatcher — the layer between the runner's message
 * bus and the real persistent Claude sessions. We stub SessionManager so the
 * tests stay hermetic; only behaviour owned by the dispatcher (frozen-memory
 * injection, sandbox staging, send-failure surfacing, decisions audit, policy
 * silencing guard) is exercised.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import fsDefault, * as fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import * as os from 'node:os';
import * as path from 'node:path';

import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import { AutoloopRunner } from '../autoloop/runner.js';
import { type AnyAutoloopMessage, Msg } from '../autoloop/messages.js';
import type { SessionManager } from '../session-manager.js';
import type { PushPolicy } from '../autoloop/types.js';
import { DEFAULT_PUSH_POLICY, LEDGER_SCHEMA_VERSION } from '../autoloop/types.js';

interface StubCalls {
  startSession: ReturnType<typeof vi.fn>;
  sendMessage: ReturnType<typeof vi.fn>;
  stopSession: ReturnType<typeof vi.fn>;
  hasSession: ReturnType<typeof vi.fn>;
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
): { manager: SessionManager; calls: StubCalls } {
  let throwsRemaining = opts.sendThrows ?? 0;
  let sendIndex = 0;
  const calls: StubCalls = {
    startSession: vi.fn(async (config: { name: string }) => {
      if (opts.startThrowsFor && config.name.endsWith(`-${opts.startThrowsFor}`)) {
        throw new Error(`${opts.startThrowsFor} failed to start`);
      }
      return { name: 'x', state: 'ready' };
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
    stopSession: vi.fn(async () => undefined),
    hasSession: vi.fn(() => true),
    getStatus: vi.fn(() => ({
      stats: { contextPercent: opts.contextPercent ?? 10, tokensIn: 0, tokensOut: 0, cachedTokens: 0 },
    })),
    compactSession: vi.fn(async () => undefined),
  };
  const manager = {
    startSession: calls.startSession,
    sendMessage: calls.sendMessage,
    stopSession: calls.stopSession,
    hasSession: calls.hasSession,
    getStatus: calls.getStatus,
    compactSession: calls.compactSession,
  } as unknown as SessionManager;
  return { manager, calls };
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
} {
  const { manager, calls } = makeStubManager(managerOpts);
  const workspace = tmpRoot;
  const dispatcher = new ClaudeAgentDispatcher({
    manager,
    runId: 'r1',
    workspace,
    ...overrides,
  });
  const ledgerDir = path.join(workspace, 'tasks', 'r1');
  return { dispatcher, calls, ledgerDir, workspace };
}

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

function plannerControl(tool: string, args: Record<string, unknown>): string {
  return `\`\`\`autoloop\n${JSON.stringify({ tool, args })}\n\`\`\``;
}

describe('ClaudeAgentDispatcher — role engine configuration', () => {
  it('keeps the legacy Claude model defaults when no role overrides are provided', async () => {
    const { dispatcher, calls } = makeDispatcher();

    await dispatcher.deliver(Msg.chat(0, { text: 'hello' }));
    await dispatcher.spawnSubagents();

    expect(findStart(calls, 'planner')).toMatchObject({
      engine: 'claude',
      model: 'opus',
      permissionMode: 'manual',
      sandboxMode: 'read-only',
    });
    expect(findStart(calls, 'coder')).toMatchObject({ engine: 'claude', model: 'sonnet' });
    expect(findStart(calls, 'reviewer')).toMatchObject({ engine: 'claude', model: 'sonnet' });
    expect(findStart(calls, 'planner')).not.toHaveProperty('effort');
    expect(findStart(calls, 'coder')).not.toHaveProperty('effort');
    expect(findStart(calls, 'reviewer')).not.toHaveProperty('effort');
  });

  it('uses each non-Claude engine without injecting a Claude model default', async () => {
    const { dispatcher, calls } = makeDispatcher({
      plannerEngine: 'codex',
      coderEngine: 'gemini',
      reviewerEngine: 'opencode',
    });

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

  it('keeps each configured effort across Planner engine overrides and eager resets', async () => {
    const { dispatcher, calls } = makeDispatcher({
      plannerEffort: 'high',
      coderEffort: 'ultra',
      reviewerEffort: 'low',
    });

    await dispatcher.deliver(Msg.chat(0, { text: 'hello' }));
    await dispatcher.spawnSubagents({
      coder_engine: 'codex',
      coder_model: 'gpt-coder',
      reviewer_engine: 'gemini',
      coder_effort: 'low',
      reviewer_effort: 'max',
    } as never);

    expect(findStart(calls, 'planner')).toMatchObject({ effort: 'high' });
    expect(findStart(calls, 'coder')).toMatchObject({ engine: 'codex', model: 'gpt-coder', effort: 'ultra' });
    expect(findStart(calls, 'reviewer')).toMatchObject({ engine: 'gemini', effort: 'low' });

    await dispatcher.resetAgent('coder', { eagerRestart: true });
    await dispatcher.resetAgent('reviewer', { eagerRestart: true });

    const latestStart = (role: 'coder' | 'reviewer') =>
      calls.startSession.mock.calls
        .map((entry) => entry[0] as Record<string, unknown>)
        .filter((config) => config.name === `autoloop-r1-${role}`)
        .at(-1);
    expect(latestStart('coder')).toMatchObject({ effort: 'ultra' });
    expect(latestStart('reviewer')).toMatchObject({ effort: 'low' });
  });

  it('delivers the Planner protocol in-band and starts non-Claude Planners read-only', async () => {
    const { dispatcher, calls } = makeDispatcher({ plannerEngine: 'codex' });

    await dispatcher.deliver(Msg.chat(0, { text: 'inspect this repository' }));

    const start = findStart(calls, 'planner');
    expect(start).toMatchObject({
      permissionMode: 'manual',
      sandboxMode: 'read-only',
    });
    expect(start).not.toHaveProperty('permissionMode', 'bypassPermissions');
    const systemPrompt = start.systemPrompt as string;
    expect(systemPrompt).toMatch(/native shell\/file tools must not create `plan\.md` or `goal\.json`/i);
    expect(systemPrompt).toContain('fenced in-band controls');
    expect(systemPrompt).toContain('`write_plan`, `write_goal`, and approved `spawn_subagents`');
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
    const { dispatcher, calls } = makeDispatcher({
      plannerEngine: 'custom',
      plannerModel: 'planner-model',
      plannerCustomEngine,
      coderEngine: 'custom',
      coderModel: 'coder-model',
      coderCustomEngine,
      reviewerEngine: 'custom',
      reviewerModel: 'reviewer-model',
      reviewerCustomEngine,
    });

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

  it('rejects an acp role before startSession: a role cannot name the ACP agent', async () => {
    const { dispatcher, calls } = makeDispatcher({ plannerEngine: 'acp' });

    await expect(dispatcher.deliver(Msg.chat(0, { text: 'hello' }))).rejects.toThrow(
      "Planner engine 'acp' is not supported by Autoloop",
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

  it('does not restart or clear a role when its live session cannot be stopped', async () => {
    const { dispatcher, calls } = makeDispatcher();
    await dispatcher.spawnSubagents();

    calls.stopSession.mockRejectedValueOnce(new Error('coder still live'));

    await expect(dispatcher.resetAgent('coder', { eagerRestart: true })).rejects.toThrow('coder still live');

    const coderStarts = calls.startSession.mock.calls.filter(
      (entry) => (entry[0] as { name: string }).name === 'autoloop-r1-coder',
    );
    expect(coderStarts).toHaveLength(1);
    await expect(dispatcher.spawnSubagents({ coder_engine: 'codex' })).rejects.toThrow(
      'Cannot change Coder engine or model after its session has started',
    );
  });

  it('restarts a role once when the failed stop confirms its session is already absent', async () => {
    const { manager, calls } = makeStubManager();
    calls.hasSession.mockReturnValue(false);
    const dispatcher = new ClaudeAgentDispatcher({ manager, runId: 'r1', workspace: tmpRoot });
    await dispatcher.spawnSubagents();

    calls.stopSession.mockRejectedValueOnce(new Error("Session 'autoloop-r1-coder' not found"));

    await expect(dispatcher.resetAgent('coder', { eagerRestart: true })).resolves.toBeUndefined();

    const coderStarts = calls.startSession.mock.calls.filter(
      (entry) => (entry[0] as { name: string }).name === 'autoloop-r1-coder',
    );
    expect(coderStarts).toHaveLength(2);
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

describe('ClaudeAgentDispatcher — Planner control transactions', () => {
  it('rejects a malformed control batch without writes, spawn, or an initial directive', async () => {
    const originalPlan = '# original plan\n';
    const originalGoal = '{"original":true}\n';
    fs.writeFileSync(path.join(tmpRoot, 'plan.md'), originalPlan);
    fs.writeFileSync(path.join(tmpRoot, 'goal.json'), originalGoal);
    const spawn = vi.fn(async () => undefined);
    const reply = [
      plannerControl('write_plan', { content: '# replacement plan\n' }),
      '```autoloop\n{malformed json\n```',
      plannerControl('spawn_subagents', { initial_directive: { goal: 'must not run' } }),
    ].join('\n');
    const { dispatcher } = makeDispatcher({ onSpawnSubagents: spawn }, { sendOutput: reply });

    const emitted = await dispatcher.deliver(Msg.chat(0, { text: 'apply the batch' }));

    expect(fs.readFileSync(path.join(tmpRoot, 'plan.md'), 'utf-8')).toBe(originalPlan);
    expect(fs.readFileSync(path.join(tmpRoot, 'goal.json'), 'utf-8')).toBe(originalGoal);
    expect(spawn).not.toHaveBeenCalled();
    expect(emitted.filter((message) => message.type === 'directive')).toEqual([]);
  });

  it('restores pre-existing artifacts and suppresses spawn when an artifact write fails', async () => {
    const originalPlan = '# original plan\n';
    const originalGoal = '{"original":true}\n';
    const replacementPlan = '# replacement plan\n';
    const replacementGoal = '{"replacement":true}\n';
    const planPath = path.join(tmpRoot, 'plan.md');
    const goalPath = path.join(tmpRoot, 'goal.json');
    fs.writeFileSync(planPath, originalPlan);
    fs.writeFileSync(goalPath, originalGoal);
    const spawn = vi.fn(async () => undefined);
    const reply = [
      plannerControl('write_plan', { content: replacementPlan }),
      plannerControl('write_goal', { content: replacementGoal }),
      plannerControl('spawn_subagents', { initial_directive: { goal: 'must not run' } }),
    ].join('\n');
    const { dispatcher } = makeDispatcher({ onSpawnSubagents: spawn }, { sendOutput: reply });

    // Fail the second visible replacement, after plan.md has been replaced,
    // so the transaction must restore the first file from its byte snapshot.
    const originalRenameSync = fsDefault.renameSync;
    let failed = false;
    fsDefault.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
      if (!failed && path.resolve(String(to)) === goalPath) {
        failed = true;
        throw new Error('simulated artifact replacement failure');
      }
      return originalRenameSync(from, to);
    }) as typeof fs.renameSync;
    syncBuiltinESMExports();
    let emitted: AnyAutoloopMessage[] = [];
    try {
      emitted = await dispatcher.deliver(Msg.chat(0, { text: 'apply the batch' }));
    } finally {
      fsDefault.renameSync = originalRenameSync;
      syncBuiltinESMExports();
    }

    expect(fs.readFileSync(planPath, 'utf-8')).toBe(originalPlan);
    expect(fs.readFileSync(goalPath, 'utf-8')).toBe(originalGoal);
    expect(spawn).not.toHaveBeenCalled();
    expect(emitted.filter((message) => message.type === 'directive')).toEqual([]);
  });

  it('materializes exact artifacts before exactly one spawn and initial directive', async () => {
    const plan = '# exact plan\n\nNo normalization.\n';
    const goal = '{\n  "scalar": null,\n  "gates": []\n}\n';
    const observedAtSpawn: Array<{ plan: string; goal: string }> = [];
    const spawn = vi.fn(async () => {
      observedAtSpawn.push({
        plan: fs.readFileSync(path.join(tmpRoot, 'plan.md'), 'utf-8'),
        goal: fs.readFileSync(path.join(tmpRoot, 'goal.json'), 'utf-8'),
      });
    });
    const reply = [
      plannerControl('spawn_subagents', { initial_directive: { goal: 'ship exact artifacts' } }),
      plannerControl('write_plan', { content: plan }),
      plannerControl('write_goal', { content: goal }),
    ].join('\n');
    const { dispatcher } = makeDispatcher({ onSpawnSubagents: spawn }, { sendOutput: reply });

    const emitted = await dispatcher.deliver(Msg.chat(0, { text: 'approved' }));

    expect(observedAtSpawn).toEqual([{ plan, goal }]);
    expect(spawn).toHaveBeenCalledTimes(1);
    expect(emitted.filter((message) => message.type === 'directive')).toHaveLength(1);
  });

  it('rejects duplicate spawn controls without duplicate startup or directives', async () => {
    const spawn = vi.fn(async () => undefined);
    const reply = [
      plannerControl('spawn_subagents', { initial_directive: { goal: 'first' } }),
      plannerControl('spawn_subagents', { initial_directive: { goal: 'second' } }),
    ].join('\n');
    const { dispatcher } = makeDispatcher({ onSpawnSubagents: spawn }, { sendOutput: reply });

    const emitted = await dispatcher.deliver(Msg.chat(0, { text: 'approved once' }));

    expect(spawn).not.toHaveBeenCalled();
    expect(emitted.filter((message) => message.type === 'directive')).toEqual([]);
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
  it('returns a phase_error instead of an empty directive_ack when Coder returns no output', async () => {
    const { dispatcher } = makeDispatcher({}, { sendOutput: '   ' });
    await dispatcher.spawnSubagents();

    const replies = await dispatcher.deliver(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
    );

    expect(replies).toHaveLength(1);
    expect(replies[0].type).toBe('phase_error');
    if (replies[0].type === 'phase_error') {
      expect(replies[0].payload.agent).toBe('coder');
    }
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

  it('returns a phase_error when automatic reset cannot stop a live Coder', async () => {
    const { dispatcher, calls } = makeDispatcher({}, { sendThrows: 1 });
    await dispatcher.spawnSubagents();
    calls.stopSession.mockRejectedValueOnce(new Error('coder still live'));

    const replies = await dispatcher.deliver(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
    );

    expect(replies).toHaveLength(1);
    expect(replies[0]).toMatchObject({
      type: 'phase_error',
      payload: { agent: 'coder', phase: 'send', error: 'coder still live' },
    });
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);
    expect(calls.stopSession).toHaveBeenCalledTimes(1);
    expect(
      calls.startSession.mock.calls.filter((entry) => (entry[0] as { name: string }).name === 'autoloop-r1-coder'),
    ).toHaveLength(1);
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
    await Promise.resolve();
    await Promise.resolve();
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);

    resolveSend({ output: '', error: undefined });
    await Promise.all([deliveryA, deliveryB]);
    await dispatcher.deliver(duplicate);
    expect(calls.sendMessage).toHaveBeenCalledTimes(1);

    await dispatcher.deliver(fixedIdentity(Msg.chat(2, { text: 'same logical turn' }), 'logical-chat-b'));
    expect(calls.sendMessage).toHaveBeenCalledTimes(2);
  });

  it('bounds the retained dispatch cache while still coalescing a recent re-delivery', async () => {
    const { dispatcher, calls } = makeDispatcher();
    calls.sendMessage.mockResolvedValue({ output: '', error: undefined });
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
  it('strips silent=true from on_phase_error / on_decision_needed but applies other fields', async () => {
    const policyRef: PushPolicy = JSON.parse(JSON.stringify(DEFAULT_PUSH_POLICY));
    const reply = `OK
\`\`\`autoloop
{"tool": "update_push_policy", "args": {"on_phase_error": {"silent": true, "channel": "email"}, "on_target_hit": {"silent": true}}}
\`\`\`
`;
    const { dispatcher, ledgerDir } = makeDispatcher({ pushPolicyRef: policyRef }, { sendOutput: reply });
    await dispatcher.deliver(Msg.chat(0, { text: 'hi' }));

    // on_phase_error: silent stripped, channel applied.
    expect(policyRef.on_phase_error.silent).not.toBe(true);
    expect(policyRef.on_phase_error.channel).toBe('email');
    // on_target_hit is not critical → silence honoured.
    expect(policyRef.on_target_hit.silent).toBe(true);

    // decisions.jsonl should record both the block + the merge.
    const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
    expect(fs.existsSync(decisionsPath)).toBe(true);
    const lines = fs
      .readFileSync(decisionsPath, 'utf-8')
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l));
    expect(lines.some((l) => l.kind === 'policy_silence_blocked')).toBe(true);
    expect(lines.some((l) => l.kind === 'update_push_policy')).toBe(true);
  });
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

// The role prompts send these entries through a tool rather than a file write —
// the Coder may not touch `tasks/` at all — so nothing lands unless the
// dispatcher appends what the tool carries.
describe('ClaudeAgentDispatcher — coder_log / reviewer_log', () => {
  const readJsonl = (file: string) =>
    fs
      .readFileSync(file, 'utf-8')
      .trim()
      .split('\n')
      .map((l) => JSON.parse(l));

  it('appends each coder_log message to <ledger>/coder_log.jsonl', async () => {
    const { dispatcher, ledgerDir } = makeDispatcher(
      {},
      {
        sendOutput: [
          'Tried the cache, no gain.',
          plannerControl('coder_log', { message: 'LRU cache did not move the metric' }),
          plannerControl('coder_log', { message: 'next: try batching' }),
        ].join('\n'),
      },
    );
    await dispatcher.spawnSubagents();
    await dispatcher.deliver(Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }));

    const rows = readJsonl(path.join(ledgerDir, 'coder_log.jsonl'));
    expect(rows.map((r) => [r.iter, r.message])).toEqual([
      [0, 'LRU cache did not move the metric'],
      [0, 'next: try batching'],
    ]);
    expect(typeof rows[0].ts).toBe('string');
  });

  it('appends reviewer_log to the reviewer_log.jsonl kept in the Reviewer sandbox', async () => {
    const { dispatcher, ledgerDir } = makeDispatcher(
      {},
      { sendOutput: plannerControl('reviewer_log', { message: 'eval set unchanged since iter 1' }) },
    );
    await dispatcher.spawnSubagents();
    await dispatcher.deliver(Msg.reviewRequest(2, { iter: 2, ledger_path: ledgerDir, prior_metrics: [] }));

    const rows = readJsonl(path.join(ledgerDir, 'reviewer_sandbox', 'reviewer_log.jsonl'));
    expect(rows.map((r) => [r.iter, r.message])).toEqual([[2, 'eval set unchanged since iter 1']]);
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

// An Autoloop left idle past `sessionTtlMinutes` has its role sessions evicted
// by the manager, while the dispatcher still believed them started — so the
// next message failed with "Session not found". It now checks, and starts the
// role again under the same name, which picks the persisted conversation back up.
describe('ClaudeAgentDispatcher — evicted role sessions', () => {
  function makeTrackedDispatcher() {
    const { manager, calls } = makeStubManager();
    const live = new Set<string>();
    calls.startSession.mockImplementation(async (config: { name: string }) => {
      live.add(config.name);
      return { name: config.name, state: 'ready' };
    });
    (manager as unknown as { hasSession: (name: string) => boolean }).hasSession = (name) => live.has(name);
    const dispatcher = new ClaudeAgentDispatcher({ manager, runId: 'r1', workspace: tmpRoot });
    const starts = (role: string) =>
      calls.startSession.mock.calls.filter((c) => (c[0] as { name: string }).name === `autoloop-r1-${role}`).length;
    return { dispatcher, live, starts, ledgerDir: path.join(tmpRoot, 'tasks', 'r1') };
  }

  it('starts the Planner again after its session was evicted', async () => {
    const { dispatcher, live, starts } = makeTrackedDispatcher();
    await dispatcher.deliver(Msg.chat(0, { text: 'hello' }));
    await dispatcher.deliver(Msg.chat(0, { text: 'still there?' }));
    expect(starts('planner')).toBe(1);

    live.delete('autoloop-r1-planner');
    await dispatcher.deliver(Msg.chat(0, { text: 'after a long idle' }));
    expect(starts('planner')).toBe(2);
  });

  it('starts the Coder and the Reviewer again after their sessions were evicted', async () => {
    const { dispatcher, live, starts, ledgerDir } = makeTrackedDispatcher();
    const directive = () =>
      dispatcher.deliver(
        Msg.directive(0, { goal: 'change one file', constraints: [], success_criteria: [], max_attempts: 1 }),
      );
    const review = () =>
      dispatcher.deliver(Msg.reviewRequest(0, { iter: 0, ledger_path: ledgerDir, prior_metrics: [] }));

    await dispatcher.spawnSubagents();
    await directive();
    await review();
    expect([starts('coder'), starts('reviewer')]).toEqual([1, 1]);

    live.delete('autoloop-r1-coder');
    live.delete('autoloop-r1-reviewer');
    await directive();
    await review();
    expect([starts('coder'), starts('reviewer')]).toEqual([2, 2]);
  });
});

/**
 * CLAWO-BUG-PARTIAL-CODER-TERMINAL-HANDOFF-20261025
 *
 * Authentic offline regression: real ClaudeAgentDispatcher + mocked manager.
 * Desired Planner prompt semantics must fail on base (lost text after 500 /
 * missing incomplete-turn handoff) and pass once the handoff is fixed.
 */
describe('ClaudeAgentDispatcher — partial Coder terminal handoff', () => {
  const TERMINAL_MARKER = 'TERMINAL_PARTIAL_NO_FREEZE_CONCLUSION_UNIQUE_9f3c';
  const PREFIX_PAD = 'A'.repeat(520);

  function longIncompleteCoderReply(): string {
    return `${PREFIX_PAD}\n${TERMINAL_MARKER}\nPlanner must see this after char 500.`;
  }

  function coderSendCount(calls: StubCalls): number {
    return calls.sendMessage.mock.calls.filter((c) => (c[0] as string) === 'autoloop-r1-coder').length;
  }

  function plannerPrompts(calls: StubCalls): string[] {
    return calls.sendMessage.mock.calls
      .filter((c) => (c[0] as string) === 'autoloop-r1-planner')
      .map((c) => c[1] as string);
  }

  it('G1/G2: Planner prompt retains terminal conclusion or readable full-reply ref after incomplete Coder turn', async () => {
    const reply = longIncompleteCoderReply();
    expect(reply.indexOf(TERMINAL_MARKER)).toBeGreaterThan(500);

    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: reply });
    await dispatcher.spawnSubagents();

    const coderReplies = await dispatcher.deliver(
      Msg.directive(0, { goal: 'ship handoff', constraints: [], success_criteria: [], max_attempts: 1 }),
    );
    expect(coderReplies).toHaveLength(1);
    expect(coderReplies[0].type).toBe('directive_ack');
    if (coderReplies[0].type !== 'directive_ack') throw new Error('expected directive_ack');
    expect(coderReplies[0].payload.understood).toBe(false);

    // Deliver the ack through the real Planner prompt path and inspect it.
    await dispatcher.deliver(coderReplies[0]);
    const prompts = plannerPrompts(calls);
    expect(prompts.length).toBeGreaterThanOrEqual(1);
    const plannerPrompt = prompts[prompts.length - 1];

    expect(plannerPrompt).toMatch(/without (a )?valid iter_complete/i);
    expect(plannerPrompt).toMatch(/send_directive/i);
    expect(plannerPrompt).toMatch(/same iter/i);
    expect(plannerPrompt).toMatch(/ask (the )?user/i);
    expect(plannerPrompt).toMatch(/\bstop\b/i);

    const replyPath = path.join(ledgerDir, 'iter', '0', 'coder_reply.txt');
    expect(fs.existsSync(replyPath)).toBe(true);
    expect(fs.readFileSync(replyPath, 'utf-8')).toContain(TERMINAL_MARKER);

    const hasInlineConclusion = plannerPrompt.includes(TERMINAL_MARKER);
    const hasReadableRef = plannerPrompt.includes('coder_reply.txt') && plannerPrompt.includes(replyPath);
    expect(hasInlineConclusion || hasReadableRef).toBe(true);
    // Prefix-only truncation must not be presented as the full conclusion.
    if (plannerPrompt.includes(TERMINAL_MARKER)) {
      expect(plannerPrompt).not.toMatch(new RegExp(`${'A'.repeat(500)}\\s*$`));
    }
  });

  it('G3: short request_clarification stays a clarification ack without incomplete-turn framing', async () => {
    const question = 'Should I touch the eval script?';
    const { dispatcher, calls } = makeDispatcher(
      {},
      {
        sendOutput: ['Need a decision before editing.', plannerControl('request_clarification', { question })].join(
          '\n',
        ),
      },
    );
    await dispatcher.spawnSubagents();
    const acks = await dispatcher.deliver(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
    );
    expect(acks[0]).toMatchObject({
      type: 'directive_ack',
      payload: { understood: false, clarification: question },
    });
    await dispatcher.deliver(acks[0]);
    const prompt = plannerPrompts(calls).at(-1) ?? '';
    expect(prompt).toContain(question);
    expect(prompt).not.toMatch(/without (a )?valid iter_complete/i);
  });

  it('G3: valid iter_complete still emits iter_artifacts (not an incomplete handoff)', async () => {
    const { dispatcher, workspace } = makeDispatcher(
      {},
      {
        sendOutput: [
          'done',
          plannerControl('iter_complete', {
            summary: 'noop',
            eval_output: { metric: 1, gates: [] },
            files_changed: [],
          }),
        ].join('\n'),
      },
    );
    // Workspace must be a git repo for the iter_complete commit path.
    const { execFileSync } = await import('node:child_process');
    execFileSync('git', ['init'], { cwd: workspace });
    execFileSync('git', ['config', 'user.email', 'test@example.com'], { cwd: workspace });
    execFileSync('git', ['config', 'user.name', 'Test'], { cwd: workspace });
    execFileSync('git', ['commit', '--allow-empty', '-m', 'base'], { cwd: workspace });

    await dispatcher.spawnSubagents();
    const out = await dispatcher.deliver(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
    );
    expect(out.some((m) => m.type === 'iter_artifacts')).toBe(true);
    expect(out.some((m) => m.type === 'directive_ack' && m.payload.understood === false)).toBe(false);
  });

  it('G5: oversized and control-bearing coder text is not newly interpolated into the Planner prompt', async () => {
    const controlPayload = `safe-prefix\u0007CONTROL_BEL_MARKER\nsuffix`;
    const oversized = `${'B'.repeat(12_000)}\n${TERMINAL_MARKER}\noversized-tail`;

    for (const toxic of [controlPayload, oversized]) {
      const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: toxic });
      await dispatcher.spawnSubagents();
      const acks = await dispatcher.deliver(
        Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
      );
      await dispatcher.deliver(acks[0]);
      const prompt = plannerPrompts(calls).at(-1) ?? '';
      expect(prompt).not.toContain('\u0007');
      expect(prompt).not.toContain('CONTROL_BEL_MARKER');
      // Full oversized body must not appear; a bounded/generic path + readable ref is required.
      expect(prompt.includes('B'.repeat(12_000))).toBe(false);
      const replyPath = path.join(ledgerDir, 'iter', '0', 'coder_reply.txt');
      expect(prompt).toContain('coder_reply.txt');
      expect(fs.existsSync(replyPath)).toBe(true);
      expect(fs.readFileSync(replyPath, 'utf-8')).toBe(toxic);
    }
  });

  it('does not attribute a prior conclusion when the current turn is malformed tool-only (empty cleaned_reply)', async () => {
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: 'OLD_CONCLUSION_UNIQUE' });
    await dispatcher.spawnSubagents();
    const d = () => Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 });
    await dispatcher.deliver(d());

    const malformedOnly =
      '```autoloop\n' +
      JSON.stringify({ tool: 'iter_complete', args: null, summary: 'CURRENT_FAILURE_UNIQUE' }) +
      '\n```';
    calls.sendMessage.mockResolvedValueOnce({ output: malformedOnly, error: undefined });
    const acks = await dispatcher.deliver(d());
    await dispatcher.deliver(acks[0]);
    const prompt = plannerPrompts(calls).at(-1) ?? '';
    const replyPath = path.join(ledgerDir, 'iter', '0', 'coder_reply.txt');
    expect(fs.existsSync(replyPath)).toBe(true);
    expect(fs.readFileSync(replyPath, 'utf-8')).toContain('CURRENT_FAILURE_UNIQUE');
    expect(
      prompt.includes('CURRENT_FAILURE_UNIQUE') ||
        fs.readFileSync(replyPath, 'utf-8').includes('CURRENT_FAILURE_UNIQUE'),
    ).toBe(true);
    expect(prompt).not.toContain('OLD_CONCLUSION_UNIQUE');
  });

  it('first-turn malformed tool-only handoff never claims a missing current-turn reply', async () => {
    const malformedOnly =
      '```autoloop\n' +
      JSON.stringify({ tool: 'iter_complete', args: null, summary: 'FIRST_TURN_CURRENT_UNIQUE' }) +
      '\n```';
    const { dispatcher, calls, ledgerDir } = makeDispatcher({}, { sendOutput: malformedOnly });
    await dispatcher.spawnSubagents();
    const acks = await dispatcher.deliver(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
    );
    await dispatcher.deliver(acks[0]);
    const prompt = plannerPrompts(calls).at(-1) ?? '';
    const replyPath = path.join(ledgerDir, 'iter', '0', 'coder_reply.txt');
    expect(fs.readFileSync(replyPath, 'utf-8')).toContain('FIRST_TURN_CURRENT_UNIQUE');
    expect(prompt).toContain('coder_reply.txt');
    expect(prompt).toContain(replyPath);
    expect(prompt).not.toMatch(/Full current-turn coder reply reference unavailable/i);
  });

  it('preserves a short multiline genuine clarification through whitespace normalization', async () => {
    const question = 'Should I edit the source?\nOr only document it?';
    const { dispatcher, calls } = makeDispatcher(
      {},
      {
        sendOutput: [question, plannerControl('request_clarification', { question })].join('\n'),
      },
    );
    await dispatcher.spawnSubagents();
    const acks = await dispatcher.deliver(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
    );
    expect(acks[0]).toMatchObject({
      type: 'directive_ack',
      payload: {
        understood: false,
        clarification: 'Should I edit the source? Or only document it?',
      },
    });
    await dispatcher.deliver(acks[0]);
    const prompt = plannerPrompts(calls).at(-1) ?? '';
    expect(prompt).toContain('Should I edit the source?');
    expect(prompt).not.toMatch(/without (a )?valid iter_complete/i);
  });

  it('G5 exact bounds, Cf and lone-surrogate rejection for reflected handoff text', async () => {
    const { boundAutoloopReflectedText, AUTOLOOP_CLARIFICATION_MAX, AUTOLOOP_CONCLUSION_MAX } =
      await import('../autoloop/reflected-text.js');

    expect(boundAutoloopReflectedText('x'.repeat(AUTOLOOP_CLARIFICATION_MAX), AUTOLOOP_CLARIFICATION_MAX)?.length).toBe(
      AUTOLOOP_CLARIFICATION_MAX,
    );
    expect(
      boundAutoloopReflectedText('x'.repeat(AUTOLOOP_CLARIFICATION_MAX + 1), AUTOLOOP_CLARIFICATION_MAX),
    ).toBeNull();
    expect(boundAutoloopReflectedText('y'.repeat(AUTOLOOP_CONCLUSION_MAX), AUTOLOOP_CONCLUSION_MAX)?.length).toBe(
      AUTOLOOP_CONCLUSION_MAX,
    );
    expect(boundAutoloopReflectedText('y'.repeat(AUTOLOOP_CONCLUSION_MAX + 1), AUTOLOOP_CONCLUSION_MAX)).toBeNull();
    expect(boundAutoloopReflectedText('ok\u200Bcf')).toBeNull();
    expect(boundAutoloopReflectedText('ok\u0007bel')).toBeNull();
    expect(boundAutoloopReflectedText('\uD800')).toBeNull();
    expect(boundAutoloopReflectedText('ok\uD800')).toBe('ok');

    const oversizedQ = 'Q'.repeat(AUTOLOOP_CLARIFICATION_MAX + 1);
    const { dispatcher, calls, ledgerDir } = makeDispatcher(
      {},
      {
        sendOutput: plannerControl('request_clarification', { question: oversizedQ }),
      },
    );
    await dispatcher.spawnSubagents();
    const acks = await dispatcher.deliver(
      Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }),
    );
    expect(acks[0]).toMatchObject({
      type: 'directive_ack',
      payload: { understood: false, reply_body_omitted: true },
    });
    expect((acks[0] as { payload: { clarification?: string } }).payload.clarification).toBeUndefined();
    await dispatcher.deliver(acks[0]);
    const prompt = plannerPrompts(calls).at(-1) ?? '';
    expect(prompt).not.toContain(oversizedQ);
    expect(prompt).toMatch(/without (a )?valid iter_complete/i);
    expect(prompt).toMatch(/omitted from prompt/i);
    const replyPath = path.join(ledgerDir, 'iter', '0', 'coder_reply.txt');
    expect(fs.readFileSync(replyPath, 'utf-8')).toBe(oversizedQ);
    expect(prompt).toContain(replyPath);
  });

  it('G4: runner-backed incomplete handoff does not review/advance/duplicate; explicit continuation sends once', async () => {
    const { manager, calls } = makeStubManager();
    const workspace = tmpRoot;
    const ledgerDir = path.join(workspace, 'tasks', 'r1');
    const dispatcher = new ClaudeAgentDispatcher({ manager, runId: 'r1', workspace });

    let coderTurns = 0;
    calls.sendMessage.mockImplementation(async (name: string) => {
      if (name === 'autoloop-r1-coder') {
        coderTurns += 1;
        // Both turns stay incomplete so the runner never enters review/advance;
        // the assertion is that the second send targets the same pending iter.
        return {
          output: `${longIncompleteCoderReply()}\n(coder turn ${coderTurns})`,
          error: undefined,
        };
      }
      if (name === 'autoloop-r1-planner') {
        // No tools: incomplete ack must not fabricate progress.
        return { output: 'acknowledged incomplete handoff', error: undefined };
      }
      return { output: '', error: undefined };
    });

    const deliveredTypes: string[] = [];
    const runner = new AutoloopRunner({
      run_id: 'r1',
      workspace,
      ledger_dir: ledgerDir,
      dispatcher,
      notifyUser: vi.fn(async () => undefined),
      stallCheckIntervalMs: 24 * 60 * 60 * 1000,
    });
    runner.on('message', (env: AnyAutoloopMessage) => {
      deliveredTypes.push(`${env.from}->${env.to}:${env.type}`);
    });

    await runner.start();
    await dispatcher.spawnSubagents();

    await runner.send(
      Msg.directive(0, { goal: 'incomplete path', constraints: [], success_criteria: [], max_attempts: 1 }),
    );

    expect(coderSendCount(calls)).toBe(1);
    expect(deliveredTypes.some((t) => t.includes('review_request'))).toBe(false);
    expect(deliveredTypes.some((t) => t.includes('iter_artifacts'))).toBe(false);
    expect(deliveredTypes.some((t) => t.includes('iter_done'))).toBe(false);
    expect(runner.state.iter).toBe(0);
    expect(fs.existsSync(path.join(ledgerDir, 'iter', '0', 'eval_output.json'))).toBe(false);

    const prompts = plannerPrompts(calls);
    expect(prompts.some((p) => /without (a )?valid iter_complete/i.test(p))).toBe(true);

    const sendsBefore = coderSendCount(calls);
    const continueDirective = Msg.directive(0, {
      goal: 'continue same iter',
      constraints: [],
      success_criteria: [],
      max_attempts: 1,
    });
    expect(continueDirective.iter).toBe(runner.state.iter);
    await runner.send(continueDirective);
    expect(coderSendCount(calls)).toBe(sendsBefore + 1);
    expect(coderTurns).toBe(2);
    expect(runner.state.iter).toBe(0);
    expect(deliveredTypes.filter((t) => t.includes('review_request'))).toEqual([]);
    // Second incomplete handoff remains resolvable on the Planner path.
    expect(
      plannerPrompts(calls).filter((p) => /without (a )?valid iter_complete/i.test(p)).length,
    ).toBeGreaterThanOrEqual(2);

    runner.stop();
  });
});
