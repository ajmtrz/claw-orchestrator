import { describe, it, expect, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import plugin from '../index.js';
import { SessionManager, toPublicAutoloopFailure } from '../session-manager.js';
import { AutoloopOperationError } from '../autoloop/dispatcher.js';
import { SecureAutoloopLedgerCommitError } from '../autoloop/secure-ledger.js';
import { ENGINE_TYPES } from '../types.js';
import { __rejectCustomEngineOverHttpForTest as rejectCustomEngineOverHttp } from '../embedded-server.js';
import type { PluginConfig, PermissionMode, EffortLevel } from '../types.js';

const COMMITTED_LEDGER_CODES = [
  'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
  'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
  'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE',
  'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
] as const;

interface RegisteredTool {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
  execute: (toolCallId: string, params: Record<string, unknown>) => Promise<unknown>;
}

interface RegisteredRoute {
  path: string;
}

interface RegisteredService {
  stop: () => void;
}

function collectRegistration(): {
  tools: RegisteredTool[];
  routes: RegisteredRoute[];
  services: RegisteredService[];
} {
  const tools: RegisteredTool[] = [];
  const routes: RegisteredRoute[] = [];
  const services: RegisteredService[] = [];
  // Minimal stub PluginAPI — just enough to capture registration calls.
  const fakeApi = {
    pluginConfig: {},
    logger: { info: () => {}, error: () => {}, warn: () => {} },
    registerTool: (def: RegisteredTool) => {
      tools.push(def);
    },
    on: () => {},
    registerHttpRoute: (def: { path: string }) => {
      routes.push({ path: def.path });
    },
    registerService: (def: RegisteredService) => {
      services.push(def);
    },
  };
  (plugin as unknown as { register: (api: unknown) => void }).register(fakeApi);
  return { tools, routes, services };
}

const CANONICAL_RENAMED_TOOLS = [
  'session_start',
  'session_send',
  'session_stop',
  'session_list',
  'sessions_overview',
  'coding_session_status',
  'session_grep',
  'session_compact',
  'coding_agents_list',
  'team_list',
  'team_send',
  'session_update_tools',
  'session_switch_model',
  'project_purge',
  'session_send_to',
  'session_inbox',
  'session_deliver_inbox',
];

const UNCHANGED_TOOLS = [
  'codex_resume',
  'codex_review',
  'codex_goal_set',
  'codex_goal_get',
  'codex_goal_pause',
  'codex_goal_resume',
  'codex_goal_clear',
  'council_start',
  'council_status',
  'council_abort',
  'council_inject',
  'council_review',
  'council_accept',
  'council_reject',
  'ultraplan_start',
  'ultraplan_status',
  'ultrareview_start',
  'ultrareview_status',
];

describe('plugin tool registration', () => {
  const { tools, routes } = collectRegistration();
  const byName = new Map(tools.map((t) => [t.name, t]));
  const routePaths = new Set(routes.map((r) => r.path));

  it('snapshots public failures without invoking accessors or inherited serialization hooks', () => {
    // Production mutation caught: reading typed fields more than once, accepting
    // inherited fields, or returning an ordinary object lets hostile getters or
    // Object.prototype.toJSON rewrite the consumer-visible failure.
    let getterCalls = 0;
    const failure = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'stable failure');
    Object.defineProperty(failure, 'retryable', {
      configurable: true,
      enumerable: true,
      get: () => {
        getterCalls += 1;
        return false;
      },
    });
    const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value: () => ({ forged: true }),
    });
    try {
      const publicFailure = toPublicAutoloopFailure(failure);
      expect(publicFailure).toEqual({
        code: 'AUTOLOOP_ENGINE_FAILURE',
        message: 'stable failure',
        retryable: true,
      });
      expect(getterCalls).toBe(0);
      expect(JSON.parse(JSON.stringify(publicFailure))).toEqual({
        code: 'AUTOLOOP_ENGINE_FAILURE',
        message: 'stable failure',
        retryable: true,
      });
    } finally {
      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
    }
  });

  it('registers all canonical engine-neutral tool names', () => {
    for (const name of CANONICAL_RENAMED_TOOLS) {
      expect(byName.has(name), `missing canonical tool: ${name}`).toBe(true);
    }
  });

  it('does not register deprecated engine-coupled aliases', () => {
    // v3.0 aliases (claude_session_*, claude_team_*, etc.) were removed in v3.1.
    // The `claude_goal_*` family (4.1.0+) and `claude_agents_list` (4.2.0+) are
    // allowed because they wrap genuinely Claude-CLI-specific subcommands and
    // mirror the existing `codex_*` naming.
    const allowedClaudeTools = new Set([
      'claude_goal_set',
      'claude_goal_clear',
      'claude_goal_status',
      'claude_agents_list',
    ]);
    for (const tool of tools) {
      if (allowedClaudeTools.has(tool.name)) continue;
      expect(tool.name.startsWith('claude_'), `deprecated alias still registered: ${tool.name}`).toBe(false);
    }
  });

  it('keeps codex_*, council_*, ultra* tool names unchanged', () => {
    for (const name of UNCHANGED_TOOLS) {
      expect(byName.has(name), `missing unchanged tool: ${name}`).toBe(true);
    }
  });

  it('keeps the legacy proxy route as a compatibility alias', () => {
    expect(routePaths.has('/v1/claw-orchestrator-proxy')).toBe(true);
    expect(routePaths.has('/v1/claude-code-proxy')).toBe(true);
  });

  it('registers the full ultraapp MCP tool surface (read + write)', () => {
    const ULTRAAPP_TOOLS = [
      // read
      'ultraapp_list',
      'ultraapp_get',
      'ultraapp_status',
      // write
      'ultraapp_new',
      'ultraapp_answer',
      'ultraapp_add_file',
      'ultraapp_spec_edit',
      'ultraapp_build_start',
      'ultraapp_build_cancel',
      'ultraapp_feedback',
      'ultraapp_promote_version',
      'ultraapp_start_container',
      'ultraapp_stop_container',
      'ultraapp_delete',
    ];
    for (const name of ULTRAAPP_TOOLS) {
      expect(byName.has(name), `missing ultraapp tool: ${name}`).toBe(true);
    }
  });

  it('exposes sandboxMode on session_start for cross-engine read-only sessions', () => {
    const tool = byName.get('session_start');
    expect(tool).toBeDefined();
    const properties = (tool!.parameters.properties ?? {}) as Record<string, Record<string, unknown>>;
    expect(properties.sandboxMode?.enum).toEqual(['read-only', 'workspace-write', 'danger-full-access']);
  });

  it('exposes independent role engines and trusted custom configs on autoloop_start', () => {
    const tool = byName.get('autoloop_start');
    expect(tool).toBeDefined();
    expect(tool!.description).not.toContain('persistent Planner (Claude Opus by default)');
    const properties = (tool!.parameters.properties ?? {}) as Record<string, Record<string, unknown>>;

    for (const role of ['planner', 'coder', 'reviewer'] as const) {
      expect(properties[`${role}_engine`]?.enum).toEqual(ENGINE_TYPES);
      expect(properties[`${role}_model`]?.type).toBe('string');
      expect(properties[`${role}_custom_engine`]?.type).toBe('object');
      expect(properties[`${role}_custom_engine`]?.required).toEqual(['name', 'bin', 'args']);
    }
  });

  it('declares exact Autoloop timeout bounds/defaults and forwards the snake_case values once', async () => {
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_start');
    expect(tool).toBeDefined();
    const properties = (tool!.parameters.properties ?? {}) as Record<string, Record<string, unknown>>;

    expect(properties.send_timeout_ms).toMatchObject({
      type: 'number',
      default: 600_000,
      minimum: 5_000,
      maximum: 7_200_000,
    });
    expect(properties.activity_lease_ms).toMatchObject({
      type: 'number',
      default: 1_800_000,
      minimum: 60_000,
      maximum: 7_200_000,
    });
    expect(properties.autoloop_hard_timeout_ms).toMatchObject({
      type: 'number',
      default: 86_400_000,
      minimum: 600_000,
      maximum: 259_200_000,
    });
    for (const field of ['send_timeout_ms', 'activity_lease_ms', 'autoloop_hard_timeout_ms']) {
      expect(properties[field].description).toEqual(expect.any(String));
      expect(String(properties[field].description).length).toBeGreaterThan(20);
    }

    const start = vi.spyOn(SessionManager.prototype, 'autoloopStart').mockResolvedValue({
      runId: 'tool-timeouts',
      plannerSession: 'autoloop-tool-timeouts-planner',
      state: {} as never,
    });
    // The handler canonicalises the workspace, and on macOS `/tmp` is a symlink
    // to `/private/tmp` — so a literal `/tmp` here fails the round-trip on every
    // Mac while passing on the Linux runner. Start from the resolved path.
    const workspace = fs.realpathSync(os.tmpdir());
    const previousNoServer = process.env.CLAWO_NO_EMBEDDED_SERVER;
    process.env.CLAWO_NO_EMBEDDED_SERVER = '1';
    try {
      await tool!.execute('timeout-contract', {
        run_id: 'tool-timeouts',
        workspace,
        send_timeout_ms: 7_200_000,
        activity_lease_ms: 60_000,
        autoloop_hard_timeout_ms: 259_200_000,
      });
      expect(start).toHaveBeenCalledWith(
        expect.objectContaining({
          runId: 'tool-timeouts',
          workspace,
          sendTimeoutMs: 7_200_000,
          activityLeaseMs: 60_000,
          autoloopHardTimeoutMs: 259_200_000,
        }),
      );
    } finally {
      registration.services[0]?.stop();
      start.mockRestore();
      if (previousNoServer === undefined) delete process.env.CLAWO_NO_EMBEDDED_SERVER;
      else process.env.CLAWO_NO_EMBEDDED_SERVER = previousNoServer;
    }
  });

  it('rejects malformed tool timeout values before invoking autoloopStart', async () => {
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_start')!;
    const start = vi
      .spyOn(SessionManager.prototype, 'autoloopStart')
      .mockRejectedValue(new Error('autoloopStart must not be reached'));

    try {
      for (const invalid of [
        { send_timeout_ms: 4_999 },
        { send_timeout_ms: 7_200_001 },
        { send_timeout_ms: Number.POSITIVE_INFINITY },
        { activity_lease_ms: 59_999 },
        { activity_lease_ms: '1800000' },
        { autoloop_hard_timeout_ms: 599_999 },
        { autoloop_hard_timeout_ms: Number.NaN },
      ]) {
        await expect(
          tool.execute('invalid-timeout', { run_id: 'invalid-timeout', workspace: '/tmp', ...invalid }),
        ).rejects.toThrow(/must be a finite number in the inclusive range/);
      }
      expect(start).not.toHaveBeenCalled();
    } finally {
      registration.services[0]?.stop();
      start.mockRestore();
    }
  });

  it('registers the v4.2.0 tools (codex app-server RPCs, claude_agents_list, fan-out)', () => {
    const NEW_4_2_0_TOOLS = [
      'codex_interrupt',
      'codex_steer',
      'codex_fork',
      'codex_rollback',
      'codex_models',
      'codex_thread_list',
      'claude_agents_list',
      'fanout_start',
      'fanout_status',
      'fanout_abort',
    ];
    for (const name of NEW_4_2_0_TOOLS) {
      expect(byName.has(name), `missing v4.2.0 tool: ${name}`).toBe(true);
    }
  });
});

describe('Autoloop public failure tool boundaries', () => {
  it.each([
    ['retryable', 'AUTOLOOP_ENGINE_FAILURE', true],
    ['non-retryable', 'AUTOLOOP_CONTROL_MALFORMED', false],
  ] as const)('keeps a %s typed Planner failure structured through autoloop_chat', async (_label, code, retryable) => {
    // Production mutation caught: removing the typed-error adapter would
    // throw an undifferentiated MCP failure instead of returning its stable
    // code, message, and retry classification.
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_chat')!;
    const message = `${code} reached the public Planner boundary`;
    const chat = vi
      .spyOn(SessionManager.prototype, 'autoloopChat')
      .mockRejectedValueOnce(new AutoloopOperationError(code, message));

    try {
      const result = (await tool.execute('typed-chat', { run_id: 'typed-chat', text: 'continue' })) as {
        content: Array<{ text: string }>;
        details: unknown;
      };
      expect(result.details).toEqual({ ok: false, error: { code, message, retryable } });
      expect(JSON.parse(result.content[0].text)).toEqual(result.details);
      const details = result.details as { error: object };
      expect(Object.getPrototypeOf(details)).toBeNull();
      expect(Object.getPrototypeOf(details.error)).toBeNull();
      expect(Object.isFrozen(details)).toBe(true);
      expect(Object.isFrozen(details.error)).toBe(true);
    } finally {
      registration.services[0]?.stop();
      chat.mockRestore();
    }
  });

  it.each(COMMITTED_LEDGER_CODES)(
    'preserves committed secure-ledger code %s through mapping and MCP chat',
    async (code) => {
      // Production mutation caught: treating committed secure-ledger failures as
      // ordinary typed failures drops the already-applied outcome and invites a
      // caller to retry a logical effect whose bytes are already committed.
      const registration = collectRegistration();
      const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_chat')!;
      const message = `${code} Planner control bytes are already committed`;
      const failure = new SecureAutoloopLedgerCommitError(code, message, {
        cause: new Error('fsync failed after commit'),
      });
      const chat = vi.spyOn(SessionManager.prototype, 'autoloopChat').mockRejectedValueOnce(failure);

      try {
        expect(toPublicAutoloopFailure(failure)).toEqual({
          code,
          message,
          committed: true,
          retryable: false,
        });
        const result = (await tool.execute('committed-chat', {
          run_id: 'committed-chat',
          text: 'continue without replay',
        })) as {
          content: Array<{ text: string }>;
          details: { ok: false; error: Record<string, unknown> };
        };
        expect(result.details).toEqual({
          ok: false,
          error: {
            code,
            message,
            committed: true,
            retryable: false,
          },
        });
        expect(JSON.parse(result.content[0].text)).toEqual(result.details);
        expect(Object.getPrototypeOf(result.details)).toBeNull();
        expect(Object.getPrototypeOf(result.details.error)).toBeNull();
        expect(Object.isFrozen(result.details)).toBe(true);
        expect(Object.isFrozen(result.details.error)).toBe(true);
      } finally {
        registration.services[0]?.stop();
        chat.mockRestore();
      }
    },
  );

  it.each(COMMITTED_LEDGER_CODES)(
    'keeps committed code %s typed with an inaccessible message and protects the complete MCP result',
    async (code) => {
      // Production mutation caught: requiring an own string message before
      // recognizing the real secure-ledger instance drops committed=true and
      // converts an already-applied outcome into a retryable-looking unknown;
      // returning it through the generic result wrapper also lets an inherited
      // toJSON erase that committed identity from the complete MCP result.
      const registration = collectRegistration();
      const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_chat')!;
      const failure = new SecureAutoloopLedgerCommitError(code, 'message replaced after construction', {
        cause: new Error('fsync failed after commit'),
      });
      let getterCalls = 0;
      Object.defineProperty(failure, 'message', {
        configurable: true,
        get() {
          getterCalls += 1;
          throw new Error('committed message getter must not run');
        },
      });
      const chat = vi
        .spyOn(SessionManager.prototype, 'autoloopChat')
        .mockResolvedValueOnce({ reply: 'warm plugin initialization' })
        .mockRejectedValueOnce(failure);
      const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
      let inheritedHookCalls = 0;

      try {
        await tool.execute('committed-accessor-warmup', {
          run_id: 'committed-accessor-warmup',
          text: 'initialize before installing hostile prototype state',
        });
        Object.defineProperty(Object.prototype, 'toJSON', {
          configurable: true,
          value() {
            inheritedHookCalls += 1;
            return { forged_committed_result: true };
          },
        });
        let rejected = false;
        const result = (await tool
          .execute('committed-accessor-chat', {
            run_id: 'committed-accessor-chat',
            text: 'do not replay committed bytes',
          })
          .catch(() => {
            rejected = true;
            return undefined;
          })) as
          | { content: Array<{ text: string }>; details: { ok: false; error: Record<string, unknown> } }
          | undefined;
        const serializedResult = JSON.parse(JSON.stringify(result)) as unknown;

        expect(rejected).toBe(false);
        expect(toPublicAutoloopFailure(failure)).toEqual({
          code,
          message: 'unknown error',
          committed: true,
          retryable: false,
        });
        expect(result?.details).toEqual({
          ok: false,
          error: {
            code,
            message: 'unknown error',
            committed: true,
            retryable: false,
          },
        });
        expect(JSON.parse(result!.content[0].text)).toEqual(result!.details);
        expect(serializedResult).toEqual(result);
        expect(inheritedHookCalls).toBe(0);
        expect(getterCalls).toBe(0);
      } finally {
        if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
        else delete (Object.prototype as { toJSON?: unknown }).toJSON;
        registration.services[0]?.stop();
        chat.mockRestore();
      }
    },
  );

  it('snapshots MCP status and list results from own data without invoking public-state hooks', async () => {
    // Production mutation caught: routing Autoloop state through the generic
    // tool normalizer invokes inherited toJSON and enumerable state accessors,
    // allowing the consumer-visible content to be forged.
    const registration = collectRegistration();
    const statusTool = registration.tools.find((candidate) => candidate.name === 'autoloop_status')!;
    const listTool = registration.tools.find((candidate) => candidate.name === 'autoloop_list')!;
    const pendingDispatch = {
      status: 'awaiting_resume' as const,
      dispatch_id: 'dispatch-planner-2',
      agent: 'planner' as const,
      message_id: 'chat-2',
      message_type: 'chat' as const,
      iter: 2,
      timeout_ms: 600_000,
      error: 'Timeout waiting for response',
    };
    const phaseError = {
      ts: '2026-09-07T08:01:00.000Z',
      agent: 'planner' as const,
      phase: 'planner_turn' as const,
      code: 'AUTOLOOP_SEND_TIMEOUT' as const,
      retryable: true,
      pending_dispatch: pendingDispatch,
      error: 'Planner dispatch timed out',
    };
    const state = {
      run_id: 'mcp-own-data-state',
      status: 'running',
      iter: 2,
      subagents_spawned: true,
      started_at: '2026-09-07T08:00:00.000Z',
      workspace: '/workspace',
      ledger_dir: '/ledger',
      push_log_count: 0,
      status_reason: null,
      pending_dispatch: pendingDispatch,
      consecutive_phase_errors: 1,
      recent_phase_errors: [phaseError],
      metric_history: [],
      last_activity_at: 1234,
    };
    const expectedPendingDispatch = {
      status: 'awaiting_resume',
      dispatch_id: 'dispatch-planner-2',
      agent: 'planner',
      message_id: 'chat-2',
      message_type: 'chat',
      iter: 2,
      timeout_ms: 600_000,
      error: 'Timeout waiting for response',
    };
    const expectedState = {
      run_id: 'mcp-own-data-state',
      status: 'running',
      iter: 2,
      subagents_spawned: true,
      started_at: '2026-09-07T08:00:00.000Z',
      workspace: '/workspace',
      ledger_dir: '/ledger',
      push_log_count: 0,
      status_reason: null,
      pending_dispatch: expectedPendingDispatch,
      consecutive_phase_errors: 1,
      recent_phase_errors: [
        {
          ts: '2026-09-07T08:01:00.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_SEND_TIMEOUT',
          retryable: true,
          pending_dispatch: expectedPendingDispatch,
          error: 'Planner dispatch timed out',
        },
      ],
      metric_history: [],
      last_activity_at: 1234,
    };
    const status = vi.spyOn(SessionManager.prototype, 'autoloopStatus').mockReturnValueOnce(undefined);
    const list = vi.spyOn(SessionManager.prototype, 'autoloopList').mockReturnValue([state] as never);
    // Initialise the lazy manager before installing the hostile prototype. The
    // assertion is scoped to the two Autoloop state egress boundaries, not
    // unrelated constructor bookkeeping performed on first plugin use.
    await statusTool.execute('status-own-data-warmup', { run_id: 'missing-warmup' });
    status.mockReturnValue(state as never);
    const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    const accessorKey = 'hostile_enumerable_accessor';
    let inheritedHookCalls = 0;
    let ownAccessorCalls = 0;
    let nestedAccessorCalls = 0;
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value(this: object) {
        inheritedHookCalls += 1;
        if (
          Object.getOwnPropertyDescriptor(this, 'state')?.value ||
          Object.getOwnPropertyDescriptor(this, 'run_id')?.value
        ) {
          return this;
        }
        return { forged_by_inherited_to_json: true };
      },
    });
    Object.defineProperty(state, accessorKey, {
      configurable: true,
      enumerable: true,
      get() {
        ownAccessorCalls += 1;
        return 'leaked accessor value';
      },
    });
    Object.defineProperty(pendingDispatch, accessorKey, {
      configurable: true,
      enumerable: true,
      get() {
        nestedAccessorCalls += 1;
        return 'leaked pending-dispatch accessor value';
      },
    });
    Object.defineProperty(phaseError, accessorKey, {
      configurable: true,
      enumerable: true,
      get() {
        nestedAccessorCalls += 1;
        return 'leaked phase-error accessor value';
      },
    });

    try {
      const statusResult = (await statusTool.execute('status-own-data', {
        run_id: state.run_id,
      })) as { content: Array<{ text: string }>; details: Record<string, unknown> };
      const listResult = (await listTool.execute('list-own-data', {})) as {
        content: Array<{ text: string }>;
        details: Record<string, unknown>;
      };
      const serializedStatusResult = JSON.parse(JSON.stringify(statusResult)) as unknown;
      const serializedListResult = JSON.parse(JSON.stringify(listResult)) as unknown;
      state.status = 'crashed';
      pendingDispatch.timeout_ms = 1;
      phaseError.error = 'mutated after execute';

      expect({ inheritedHookCalls, ownAccessorCalls, nestedAccessorCalls }).toEqual({
        inheritedHookCalls: 0,
        ownAccessorCalls: 0,
        nestedAccessorCalls: 0,
      });
      expect(statusResult.details).toEqual({ ok: true, state: expectedState });
      expect(listResult.details).toEqual({ ok: true, runs: [expectedState] });
      expect(JSON.parse(statusResult.content[0].text)).toEqual(statusResult.details);
      expect(JSON.parse(listResult.content[0].text)).toEqual(listResult.details);
      expect(serializedStatusResult).toEqual(statusResult);
      expect(serializedListResult).toEqual(listResult);

      const statusState = statusResult.details.state as Record<string, unknown>;
      const statusPending = statusState.pending_dispatch as object;
      const statusRecent = statusState.recent_phase_errors as Array<Record<string, unknown>>;
      const statusPhaseError = statusRecent[0]!;
      const statusPhasePending = statusPhaseError.pending_dispatch as object;
      const runs = listResult.details.runs as Array<Record<string, unknown>>;
      const listRecent = runs[0]!.recent_phase_errors as Array<Record<string, unknown>>;
      expect(Object.getOwnPropertyDescriptor(statusState, accessorKey)).toBeUndefined();
      expect(Object.getPrototypeOf(statusResult.details)).toBeNull();
      expect(Object.getPrototypeOf(statusState)).toBeNull();
      expect(Object.getPrototypeOf(statusPending)).toBeNull();
      expect(Object.getPrototypeOf(statusPhaseError)).toBeNull();
      expect(Object.getPrototypeOf(statusPhasePending)).toBeNull();
      expect(Object.getPrototypeOf(listResult.details)).toBeNull();
      expect(Object.getPrototypeOf(runs[0]!)).toBeNull();
      expect(Object.getPrototypeOf(listRecent[0]!)).toBeNull();
      expect(Object.getPrototypeOf(listRecent[0]!.pending_dispatch as object)).toBeNull();
      expect(Object.isFrozen(statusResult.details)).toBe(true);
      expect(Object.isFrozen(statusState)).toBe(true);
      expect(Object.isFrozen(statusPending)).toBe(true);
      expect(Object.isFrozen(statusRecent)).toBe(true);
      expect(Object.isFrozen(statusPhaseError)).toBe(true);
      expect(Object.isFrozen(statusPhasePending)).toBe(true);
      expect(Object.isFrozen(listResult.details)).toBe(true);
      expect(Object.isFrozen(runs)).toBe(true);
      expect(Object.isFrozen(runs[0])).toBe(true);
      expect(Object.isFrozen(listRecent)).toBe(true);
      expect(Object.isFrozen(listRecent[0])).toBe(true);
      expect(Object.isFrozen(statusResult.content)).toBe(true);
      expect(Object.isFrozen(statusResult.content[0])).toBe(true);
      expect(Object.isFrozen(listResult.content)).toBe(true);
      expect(Object.isFrozen(listResult.content[0])).toBe(true);
    } finally {
      delete (state as typeof state & Record<string, unknown>)[accessorKey];
      delete (pendingDispatch as typeof pendingDispatch & Record<string, unknown>)[accessorKey];
      delete (phaseError as typeof phaseError & Record<string, unknown>)[accessorKey];
      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      registration.services[0]?.stop();
      status.mockRestore();
      list.mockRestore();
    }
  });

  it('snapshots MCP status not-found through the Autoloop public result boundary', async () => {
    // Production mutation caught: returning the legacy not-found envelope
    // directly sends it through the generic normalizer, where a polluted
    // Object.prototype.toJSON can forge content while details remain different.
    const registration = collectRegistration();
    const statusTool = registration.tools.find((candidate) => candidate.name === 'autoloop_status')!;
    const status = vi.spyOn(SessionManager.prototype, 'autoloopStatus').mockReturnValue(undefined);
    await statusTool.execute('missing-status-warmup', { run_id: 'missing-status-warmup' });
    const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    let hookCalls = 0;
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value() {
        hookCalls += 1;
        return { forged_status_not_found: true };
      },
    });

    try {
      const result = (await statusTool.execute('missing-status', { run_id: 'missing-status' })) as {
        content: Array<{ text: string }>;
        details: unknown;
      };
      expect(hookCalls).toBe(0);
      expect(result.details).toEqual({ ok: false, error: 'Run not found' });
      expect(JSON.parse(result.content[0].text)).toEqual(result.details);
      expect(Object.getPrototypeOf(result.details as object)).toBeNull();
      expect(Object.isFrozen(result.details)).toBe(true);
      expect(Object.isFrozen(result.content)).toBe(true);
      expect(Object.isFrozen(result.content[0])).toBe(true);
    } finally {
      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      registration.services[0]?.stop();
      status.mockRestore();
    }
  });

  it('clones chat-state metadata at the MCP boundary without exposing secondary error internals', async () => {
    // Production mutation caught: returning the error's pending_dispatch
    // reference would let later internal mutation rewrite a result the caller
    // already received; spreading the Error would also leak cause/stack data.
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_chat')!;
    const pendingDispatch = {
      status: 'awaiting_resume' as const,
      dispatch_id: 'dispatch-planner-7',
      agent: 'planner' as const,
      message_id: 'chat-7',
      message_type: 'chat' as const,
      iter: 7,
      timeout_ms: 600_000,
      error: 'Timeout waiting for response',
    };
    const typedStateFailure = Object.assign(new Error('Planner dispatch is awaiting explicit resume'), {
      name: 'AutoloopChatStateError',
      code: 'AUTOLOOP_SEND_TIMEOUT' as const,
      // The source object lies; the public value must be table-derived.
      retryable: false,
      pending_dispatch: pendingDispatch,
      status_reason: 'awaiting_resume:send_timeout:planner:dispatch-planner-7',
      secondaryErrors: [new Error('private cleanup detail')],
    });
    const chat = vi.spyOn(SessionManager.prototype, 'autoloopChat').mockRejectedValueOnce(typedStateFailure);

    try {
      const toolResult = (await tool.execute('typed-chat-state', {
        run_id: 'typed-chat-state',
        text: 'continue',
      })) as {
        content: Array<{ text: string }>;
        details: {
          ok: false;
          error: { pending_dispatch: { timeout_ms: number }; status_reason: string };
        };
      };
      const result = toolResult.details;
      pendingDispatch.timeout_ms = 1;
      typedStateFailure.status_reason = 'mutated-after-return';

      expect(result).toEqual({
        ok: false,
        error: {
          code: 'AUTOLOOP_SEND_TIMEOUT',
          message: 'Planner dispatch is awaiting explicit resume',
          retryable: true,
          pending_dispatch: {
            status: 'awaiting_resume',
            dispatch_id: 'dispatch-planner-7',
            agent: 'planner',
            message_id: 'chat-7',
            message_type: 'chat',
            iter: 7,
            timeout_ms: 600_000,
            error: 'Timeout waiting for response',
          },
          status_reason: 'awaiting_resume:send_timeout:planner:dispatch-planner-7',
        },
      });
      expect(result.error).not.toHaveProperty('stack');
      expect(result.error).not.toHaveProperty('cause');
      expect(result.error).not.toHaveProperty('secondaryErrors');
      expect(Object.getPrototypeOf(result)).toBeNull();
      expect(Object.getPrototypeOf(result.error)).toBeNull();
      expect(Object.getPrototypeOf(result.error.pending_dispatch)).toBeNull();
      expect(Object.isFrozen(result)).toBe(true);
      expect(Object.isFrozen(result.error)).toBe(true);
      expect(Object.isFrozen(result.error.pending_dispatch)).toBe(true);
    } finally {
      registration.services[0]?.stop();
      chat.mockRestore();
    }
  });

  it('ignores hostile chat-state accessors and inherited metadata without invoking them', () => {
    // Production mutation caught: structural spreads/property reads would invoke
    // attacker-owned accessors or accept inherited pending-dispatch fields.
    let getterCalls = 0;
    const hostilePending = Object.create({
      status: 'awaiting_resume',
      dispatch_id: 'inherited-dispatch',
      agent: 'planner',
      message_id: 'inherited-message',
      message_type: 'chat',
      iter: 1,
      timeout_ms: 600_000,
      error: 'inherited timeout',
    }) as Record<string, unknown>;
    Object.defineProperty(hostilePending, 'status', {
      enumerable: true,
      get: () => {
        getterCalls += 1;
        return 'awaiting_resume';
      },
    });
    const error = Object.assign(new Error('hostile chat state'), {
      name: 'AutoloopChatStateError',
      code: 'AUTOLOOP_SEND_TIMEOUT',
      retryable: true,
      pending_dispatch: hostilePending,
      status_reason: 'safe-status-reason',
    });

    const failure = toPublicAutoloopFailure(error);
    expect(failure).toEqual({
      code: 'AUTOLOOP_SEND_TIMEOUT',
      message: 'hostile chat state',
      retryable: true,
      status_reason: 'safe-status-reason',
    });
    expect(getterCalls).toBe(0);
    expect(Object.getPrototypeOf(failure!)).toBeNull();
    expect(Object.isFrozen(failure)).toBe(true);
  });

  it('keeps unknown chat errors generic while retaining the existing success contract', async () => {
    // Production mutation caught: an over-broad structural classifier would
    // falsely assign a stable Autoloop code to an ordinary Error.
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_chat')!;
    const chat = vi.spyOn(SessionManager.prototype, 'autoloopChat');
    try {
      chat.mockRejectedValueOnce(new Error('ordinary unknown Planner failure'));
      await expect(tool.execute('unknown-chat', { run_id: 'unknown-chat', text: 'continue' })).rejects.toThrow(
        'ordinary unknown Planner failure',
      );

      chat.mockResolvedValueOnce({ reply: 'Planner success remains unchanged' });
      const success = (await tool.execute('successful-chat', {
        run_id: 'successful-chat',
        text: 'continue',
      })) as { details: unknown };
      expect(success.details).toEqual({ ok: true, reply: 'Planner success remains unchanged' });
    } finally {
      registration.services[0]?.stop();
      chat.mockRestore();
    }
  });

  it('distinguishes a structured reset postcondition failure from a missing run and preserves legacy false', async () => {
    // Production mutation caught: making the public tool consume the legacy
    // boolean again would relabel this exact failed postcondition as not-found;
    // returning the typed failure through the generic wrapper lets inherited
    // toJSON forge the complete MCP result even when details remain correct.
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_reset_agent')!;
    const prototype = SessionManager.prototype as unknown as {
      autoloopResetAgentResult?: () => Promise<unknown>;
    };
    const prior = Object.getOwnPropertyDescriptor(prototype, 'autoloopResetAgentResult');
    const result = {
      ok: false as const,
      code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' as const,
      agent: 'planner' as const,
      previous_generation: 4,
      message: 'Planner generation 4 remained occupied after reset',
      retryable: false as const,
    };
    const resetResult = vi.fn().mockResolvedValueOnce(undefined).mockResolvedValue(result);
    Object.defineProperty(prototype, 'autoloopResetAgentResult', {
      configurable: true,
      value: resetResult,
    });
    const manager = new SessionManager({});
    const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');

    try {
      const missingReset = (await tool.execute('missing-reset', {
        run_id: 'missing-reset',
        agent: 'planner',
      })) as { details: unknown };
      expect(missingReset.details).toEqual({ ok: false, error: 'Run not found' });

      let inheritedHookCalls = 0;
      Object.defineProperty(Object.prototype, 'toJSON', {
        configurable: true,
        value() {
          inheritedHookCalls += 1;
          return { forged_reset_result: true };
        },
      });
      const typedReset = (await tool.execute('typed-reset', {
        run_id: 'typed-reset',
        agent: 'planner',
        force: true,
      })) as { content: Array<{ text: string }>; details: unknown };
      const serializedReset = JSON.parse(JSON.stringify(typedReset)) as unknown;
      expect(typedReset.details).toEqual({
        ok: false,
        error: {
          code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
          message: 'Planner generation 4 remained occupied after reset',
          retryable: false,
        },
      });
      expect(JSON.parse(typedReset.content[0].text)).toEqual(typedReset.details);
      expect(serializedReset).toEqual(typedReset);
      expect(inheritedHookCalls).toBe(0);

      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      await expect(manager.autoloopResetAgent('typed-reset', 'planner', { force: true })).resolves.toBe(false);
    } finally {
      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      await manager.shutdown();
      registration.services[0]?.stop();
      if (prior) Object.defineProperty(prototype, 'autoloopResetAgentResult', prior);
      else delete prototype.autoloopResetAgentResult;
    }
  });

  it('fails closed when the structured reset adapter returns a malformed failure', async () => {
    // Production mutation caught: a non-null assertion converts an invalid
    // reset result into { ok: false, error: undefined }.
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_reset_agent')!;
    const reset = vi.spyOn(SessionManager.prototype, 'autoloopResetAgentResult').mockResolvedValueOnce({
      ok: false,
      code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
      agent: 'planner',
      retryable: false,
    } as never);

    try {
      await expect(
        tool.execute('malformed-reset', { run_id: 'malformed-reset', agent: 'planner', force: true }),
      ).rejects.toThrow(/malformed structured reset result/i);
    } finally {
      registration.services[0]?.stop();
      reset.mockRestore();
    }
  });
});

// The OpenClaw plugin manifest declares the tool contract separately from the
// code that registers them, so the two drift silently: six `autoloop_*` tools
// were registered for releases without ever being declared, and three files
// quoted three different tool counts (69 registered, 65 in the README, 63 in the
// manifest and CLAUDE.md). A host that trusts the manifest simply never sees the
// undeclared tools. Parity is cheap to assert, so assert it rather than relying
// on remembering the convention.
describe('openclaw.plugin.json parity', () => {
  it('declares exactly the tools the plugin registers', async () => {
    const { readFileSync } = await import('node:fs');
    const { fileURLToPath } = await import('node:url');
    const { dirname, join } = await import('node:path');
    const here = dirname(fileURLToPath(import.meta.url));
    const manifest = JSON.parse(readFileSync(join(here, '../../openclaw.plugin.json'), 'utf8')) as {
      contracts: { tools: string[] };
    };

    const registered = collectRegistration().tools.map((t) => t.name);
    const declared = manifest.contracts.tools;

    // Sorted comparison, so the failure message names the offending tools
    // rather than reporting an opaque count mismatch.
    expect([...declared].sort()).toEqual([...registered].sort());
  });

  it('registers no duplicate tool names', () => {
    const names = collectRegistration().tools.map((t) => t.name);
    expect(new Set(names).size).toBe(names.length);
  });

  // The tool contract was asserted above while `configSchema` was not, and it
  // drifted the same way for the same reason: `pricingOverrides` was a real
  // PluginConfig field the manifest never declared, and two enums went stale
  // against their types. Values matter as much as keys here — the host validates
  // config against this schema and refuses to load the plugin when it fails, so
  // a missing enum member is not cosmetic: `defaultPermissionMode: 'manual'`,
  // the name current CLIs use, could not be configured at all.
  it('declares exactly the PluginConfig fields, with matching enums', async () => {
    const { readFileSync } = await import('node:fs');
    const { fileURLToPath } = await import('node:url');
    const { dirname, join } = await import('node:path');
    const here = dirname(fileURLToPath(import.meta.url));
    const manifest = JSON.parse(readFileSync(join(here, '../../openclaw.plugin.json'), 'utf8')) as {
      configSchema: { properties: Record<string, { enum?: string[] }> };
    };

    // Exhaustive by construction: adding a PluginConfig field without adding it
    // here is a compile error, so this list cannot silently fall behind.
    const expected: Record<keyof PluginConfig, true> = {
      claudeBin: true,
      defaultModel: true,
      defaultPermissionMode: true,
      defaultEffort: true,
      maxConcurrentSessions: true,
      sessionTtlMinutes: true,
      proxy: true,
      pricingOverrides: true,
    };

    expect(Object.keys(manifest.configSchema.properties).sort()).toEqual(Object.keys(expected).sort());

    // The enums are the values a host will reject config against, so compare
    // them to the unions they mirror rather than trusting them to keep up.
    const permissionModes: PermissionMode[] = [
      'acceptEdits',
      'bypassPermissions',
      'default',
      'manual',
      'dontAsk',
      'plan',
      'auto',
    ];
    const effortLevels: EffortLevel[] = ['low', 'medium', 'high', 'xhigh', 'max', 'auto'];

    expect([...(manifest.configSchema.properties.defaultPermissionMode.enum ?? [])].sort()).toEqual(
      [...permissionModes].sort(),
    );
    expect([...(manifest.configSchema.properties.defaultEffort.enum ?? [])].sort()).toEqual([...effortLevels].sort());
  });

  // A host echoes the tool list back inside its request, so every schema this
  // plugin registers ends up as request-body content. Several of them describe
  // custom-engine properties — `session_start.customEngine`,
  // `autoloop_start.{planner,coder,reviewer}_custom_engine`, and
  // `council_start.agents.items.properties.customEngine`, which sits deeper and
  // behind an array. The HTTP guard used to match any object under such a key,
  // so it rejected the whole request and every tool-bearing turn through
  // `/v1/chat/completions` returned 400. Checked against the real registry
  // rather than a fixture, so a tool added later is covered too.
  it('does not let the HTTP custom-engine guard reject its own tool schemas', () => {
    const registration = collectRegistration();
    try {
      const asHostToolList = {
        model: 'claude-sonnet-5',
        messages: [{ role: 'user', content: 'hi' }],
        tools: registration.tools.map((t) => ({
          type: 'function',
          function: { name: t.name, description: t.description, parameters: t.parameters },
        })),
      };

      expect(registration.tools.length).toBeGreaterThan(70);
      expect(rejectCustomEngineOverHttp(asHostToolList)).toBeNull();
    } finally {
      registration.services[0]?.stop();
    }
  });
});
