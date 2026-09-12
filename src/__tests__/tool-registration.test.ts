import { describe, it, expect, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import plugin from '../index.js';
import { SessionManager, toPublicAutoloopFailure } from '../session-manager.js';
import { AutoloopOperationError } from '../autoloop/dispatcher.js';
import { AutoloopRecoveryError } from '../autoloop/types.js';
import { Msg } from '../autoloop/messages.js';
import { AutoloopRunner } from '../autoloop/runner.js';
import { SecureAutoloopLedgerCommitError } from '../autoloop/secure-ledger.js';
import { ENGINE_TYPES } from '../types.js';
import { __rejectCustomEngineOverHttpForTest as rejectCustomEngineOverHttp } from '../embedded-server.js';
import type { PluginConfig, PermissionMode, EffortLevel } from '../types.js';
import type { AutoloopState, PublicAutoloopFailure, PublicAutoloopFailureCode } from '../index.js';
import type { AgentDispatcher } from '../autoloop/types.js';

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

  it('exports the complete public Autoloop phase-error contract', () => {
    // Production mutation caught: narrowing recent phase errors back to only
    // operation failures makes accepted chat-state JSON impossible to consume
    // through the package's public TypeScript surface.
    const pendingDispatch = {
      status: 'awaiting_resume',
      dispatch_id: 'dispatch-planner-public-contract',
      agent: 'planner',
      message_id: 'chat-public-contract',
      message_type: 'chat',
      iter: 4,
      timeout_ms: 600_000,
      error: 'Timeout waiting for response',
    } as const;
    const chatStateCodes = [
      'AUTOLOOP_SEND_TIMEOUT',
      'AUTOLOOP_RUN_PAUSED',
      'AUTOLOOP_RUN_TERMINAL',
    ] as const satisfies readonly PublicAutoloopFailureCode[];
    const recentPhaseErrors: AutoloopState['recent_phase_errors'] = [
      {
        ts: '2026-09-07T20:00:00.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_SEND_TIMEOUT',
        retryable: true,
        pending_dispatch: pendingDispatch,
        status_reason: 'awaiting_resume:send_timeout:planner:dispatch-planner-public-contract',
        error: 'Planner dispatch is awaiting explicit resume',
      },
      {
        ts: '2026-09-07T20:01:00.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_RUN_PAUSED',
        retryable: false,
        status_reason: null,
        error: 'Autoloop run is paused',
      },
      {
        ts: '2026-09-07T20:02:00.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_RUN_TERMINAL',
        retryable: false,
        status_reason: 'goal completed',
        error: 'Autoloop run is terminal',
      },
      {
        ts: '2026-09-07T20:03:00.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
        error: 'Ledger data committed before file sync completed',
      },
    ];
    const publicFailure = {
      code: 'AUTOLOOP_SEND_TIMEOUT',
      message: 'Planner dispatch is awaiting explicit resume',
      retryable: true,
      pending_dispatch: pendingDispatch,
      status_reason: 'awaiting_resume:send_timeout:planner:dispatch-planner-public-contract',
    } as const satisfies PublicAutoloopFailure;

    expect(chatStateCodes).toEqual(['AUTOLOOP_SEND_TIMEOUT', 'AUTOLOOP_RUN_PAUSED', 'AUTOLOOP_RUN_TERMINAL']);
    expect(recentPhaseErrors.map(({ code }) => code)).toEqual([
      ...chatStateCodes,
      'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
    ]);
    expect(publicFailure.pending_dispatch).toEqual(pendingDispatch);
  });

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

describe('Task 6 Slice 6.2 public recovery MCP boundary', () => {
  it('registers the recovery schema, delegates inspection and apply unchanged, and projects typed domain errors', async () => {
    // Production mutation caught: omitting this transport adapter, changing the
    // caller-supplied recovery token, or bypassing the established typed-error
    // projection makes recovery unsafe or opaque at the MCP boundary.
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_recover');
    const recover = vi.spyOn(SessionManager.prototype as never, 'autoloopRecover' as never);
    try {
      expect(tool).toBeDefined();
      expect(tool!.parameters).toEqual({
        type: 'object',
        additionalProperties: false,
        properties: {
          run_id: { type: 'string', description: 'Run id to inspect or recover' },
          apply: { type: 'boolean', description: 'Apply the assessed recovery action (default false)' },
          recovery_token: { type: 'string', description: 'Exact token returned by recovery inspection' },
        },
        required: ['run_id'],
      });

      recover
        .mockResolvedValueOnce({ assessment: { recovery_token: 'inspect-token', next_safe_action: 'none' } } as never)
        .mockResolvedValueOnce({
          assessment: { recovery_token: 'apply-token', next_safe_action: 'none' },
          receipt: { status: 'applied', recovery_token: 'apply-token' },
        } as never)
        .mockRejectedValueOnce(
          new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'recovery domain failure') as never,
        );

      await expect(tool!.execute('recover-inspect', { run_id: 'run-1' })).resolves.toMatchObject({
        details: { ok: true, assessment: { recovery_token: 'inspect-token', next_safe_action: 'none' } },
      });
      expect(recover).toHaveBeenLastCalledWith('run-1', {});

      await expect(
        tool!.execute('recover-apply', { run_id: 'run-1', apply: true, recovery_token: 'apply-token' }),
      ).resolves.toMatchObject({
        details: {
          ok: true,
          assessment: { recovery_token: 'apply-token', next_safe_action: 'none' },
          receipt: { status: 'applied', recovery_token: 'apply-token' },
        },
      });
      expect(recover).toHaveBeenLastCalledWith('run-1', { apply: true, recovery_token: 'apply-token' });

      await expect(tool!.execute('recover-error', { run_id: 'run-1' })).resolves.toMatchObject({
        details: {
          ok: false,
          error: {
            code: 'AUTOLOOP_ENGINE_FAILURE',
            message: 'recovery domain failure',
            retryable: true,
          },
        },
      });
    } finally {
      registration.services[0]?.stop();
      recover.mockRestore();
    }
  });

  it('rejects recovery arguments unless every supplied field is an own data property of the exact public shape', async () => {
    // Production mutation caught: reading arbitrary properties directly lets
    // inherited values, accessors, unknown keys, and truthy wire values cross
    // the MCP boundary into a recovery effect.
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_recover')!;
    const recover = vi.spyOn(SessionManager.prototype as never, 'autoloopRecover' as never);
    const inheritedRunId = Object.create({ run_id: 'inherited-run' }) as Record<string, unknown>;
    const inheritedToken = Object.create({ recovery_token: 'inherited-token' }) as Record<string, unknown>;
    Object.defineProperty(inheritedToken, 'run_id', { value: 'run-1', enumerable: true });
    const accessorRunId = {} as Record<string, unknown>;
    Object.defineProperty(accessorRunId, 'run_id', { get: () => 'run-1', enumerable: true });
    const accessorToken = { run_id: 'run-1' } as Record<string, unknown>;
    Object.defineProperty(accessorToken, 'recovery_token', { get: () => 'token', enumerable: true });

    try {
      for (const args of [
        inheritedRunId,
        inheritedToken,
        accessorRunId,
        accessorToken,
        { run_id: 1 },
        { run_id: 'run-1', recovery_token: 1 },
        { run_id: 'run-1', apply: 'false' },
        { run_id: 'run-1', unexpected: true },
      ] as Record<string, unknown>[]) {
        await expect(tool.execute('recover-invalid', args)).rejects.toThrow();
      }
      expect(recover).not.toHaveBeenCalled();
    } finally {
      registration.services[0]?.stop();
      recover.mockRestore();
    }
  });

  it.each([
    ['AUTOLOOP_RECOVERY_TOKEN_REQUIRED', 'apply requires a recovery token'],
    ['AUTOLOOP_RECOVERY_TOKEN_STALE', 'recovery token is stale'],
    ['AUTOLOOP_RECOVERY_MANUAL_RESOLUTION_REQUIRED', 'recovery requires manual resolution'],
    ['AUTOLOOP_RECOVERY_INCOMPLETE', 'recovery is incomplete'],
  ] as const)('projects the typed recovery failure %s as non-retryable MCP data', async (code, message) => {
    // Production mutation caught: omitting a recovery code from the public
    // mapper turns a known safe recovery outcome into an opaque MCP failure.
    const registration = collectRegistration();
    const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_recover')!;
    const recover = vi
      .spyOn(SessionManager.prototype as never, 'autoloopRecover' as never)
      .mockRejectedValueOnce(new AutoloopRecoveryError(code, message) as never);
    const args =
      code === 'AUTOLOOP_RECOVERY_TOKEN_REQUIRED'
        ? { run_id: 'run-1', apply: true }
        : { run_id: 'run-1', apply: true, recovery_token: 'stale-token' };

    try {
      await expect(tool.execute('recover-typed-error', args)).resolves.toMatchObject({
        details: { ok: false, error: { code, message, retryable: false } },
      });
    } finally {
      registration.services[0]?.stop();
      recover.mockRestore();
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

describe('Task 4B public reviewer-only registration', () => {
  it('keeps typed failures structured through autoloop_request_review', async () => {
    const registration = collectRegistration();
    const call = vi
      .spyOn(SessionManager.prototype as never, 'autoloopRequestReview' as never)
      .mockRejectedValueOnce(new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'typed Task 4B failure') as never);
    try {
      const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_request_review')!;
      const result = (await tool.execute('typed-task4b', {
        run_id: 'run',
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      })) as { details: unknown };
      expect(result.details).toEqual({
        ok: false,
        error: { code: 'AUTOLOOP_ENGINE_FAILURE', message: 'typed Task 4B failure', retryable: true },
      });
    } finally {
      registration.services[0]?.stop();
      call.mockRestore();
    }
  });

  it.each([
    ['AUTOLOOP_RUN_PAUSED', "Autoloop run 'run' is paused; resume it before requesting review"],
    ['AUTOLOOP_RUN_TERMINAL', "Autoloop run 'run' is terminal and cannot accept a review request"],
  ] as const)('keeps request_review state failure %s structured at the MCP boundary', async (code, message) => {
    const registration = collectRegistration();
    const failure = Object.assign(new Error(message), { name: 'AutoloopChatStateError', code, retryable: false });
    const call = vi
      .spyOn(SessionManager.prototype as never, 'autoloopRequestReview' as never)
      .mockRejectedValueOnce(failure as never);
    try {
      const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_request_review')!;
      const result = (await tool.execute('typed-task4b-state', {
        run_id: 'run',
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-state',
      })) as { details: unknown };
      expect(result.details).toEqual({ ok: false, error: { code, message, retryable: false } });
    } finally {
      registration.services[0]?.stop();
      call.mockRestore();
    }
  });
  const NEW_TOOLS = ['autoloop_request_review'] as const;
  const INTERNAL_ONLY_TOOLS = ['autoloop_spawn_coder', 'autoloop_spawn_reviewer'] as const;

  it('registers each public Task 4B tool exactly once with the manifest in exact parity', async () => {
    const { readFileSync } = await import('node:fs');
    const { fileURLToPath } = await import('node:url');
    const { dirname, join } = await import('node:path');
    const here = dirname(fileURLToPath(import.meta.url));
    const manifest = JSON.parse(readFileSync(join(here, '../../openclaw.plugin.json'), 'utf8')) as {
      contracts: { tools: string[] };
    };
    const registered = collectRegistration().tools.map((tool) => tool.name);

    for (const name of NEW_TOOLS) {
      expect(registered.filter((candidate) => candidate === name)).toHaveLength(1);
      expect(manifest.contracts.tools.filter((candidate) => candidate === name)).toHaveLength(1);
    }
    for (const name of INTERNAL_ONLY_TOOLS) {
      expect(registered).not.toContain(name);
      expect(manifest.contracts.tools).not.toContain(name);
    }
    expect([...manifest.contracts.tools].sort()).toEqual([...registered].sort());
  });

  it('publishes the checkpoint-bound request_review schema without a Coder field', () => {
    const registration = collectRegistration();
    try {
      const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_request_review');
      expect(tool).toBeDefined();
      expect(tool!.parameters).toEqual({
        type: 'object',
        additionalProperties: false,
        properties: {
          run_id: { type: 'string', minLength: 1, maxLength: 512 },
          checkpoint_sha: { type: 'string', pattern: '^[0-9a-fA-F]{40}$' },
          source_run_id: {
            type: 'string',
            minLength: 1,
            maxLength: 8192,
            pattern: '^[A-Za-z0-9][A-Za-z0-9._-]*$',
          },
          source_iter: { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER },
          scope: {
            type: 'array',
            minItems: 1,
            maxItems: 128,
            items: { type: 'string', minLength: 1, maxLength: 8192 },
          },
          idempotency_key: { type: 'string', minLength: 1, maxLength: 8192 },
        },
        required: ['run_id', 'checkpoint_sha', 'source_run_id', 'source_iter', 'scope', 'idempotency_key'],
      });
      expect(JSON.stringify(tool!.parameters)).not.toMatch(/coder/i);
    } finally {
      registration.services[0]?.stop();
    }
  });

  it('keeps independent role-spawn primitives out of the public MCP registry', () => {
    const registration = collectRegistration();
    try {
      for (const name of INTERNAL_ONLY_TOOLS) {
        expect(registration.tools.find((candidate) => candidate.name === name)).toBeUndefined();
      }
    } finally {
      registration.services[0]?.stop();
    }
  });

  it.each([
    ['coder', 'autoloopSpawnCoder', { coder_engine: 'custom' }],
    ['reviewer', 'autoloopSpawnReviewer', { reviewer_engine: 'custom' }],
  ] as const)('rejects custom %s spawn before live-run effects', async (_role, method, args) => {
    const manager = new SessionManager({});
    const live = vi.spyOn(manager as never, '_liveAutoloop' as never);
    await expect((manager[method] as (runId: string, input: unknown) => Promise<unknown>)('run', args)).rejects.toThrow(
      /not supported/,
    );
    expect(live).not.toHaveBeenCalled();
    await manager.shutdown();
  });

  it.each([
    ['coder', 'autoloopSpawnCoder', {}],
    ['reviewer', 'autoloopSpawnReviewer', {}],
  ] as const)('fences %s spawn while the run is being deleted', async (_role, method, args) => {
    const manager = new SessionManager({});
    ((manager as unknown as Record<string, unknown>)['_autoloopReviewDeleting'] as Set<string>).add('run');
    const live = vi.spyOn(manager as never, '_liveAutoloop' as never);
    await expect((manager[method] as (runId: string, input: unknown) => Promise<unknown>)('run', args)).rejects.toThrow(
      "Autoloop run 'run' is being deleted",
    );
    expect(live).not.toHaveBeenCalled();
    await manager.shutdown();
  });

  it('serializes an in-flight spawn against delete without reporting failure after its effect succeeds', async () => {
    const manager = new SessionManager({});
    let releaseSpawn!: () => void;
    const blocked = new Promise<void>((resolve) => {
      releaseSpawn = resolve;
    });
    const spawnCoder = vi.fn(async () => {
      await blocked;
      return { role: 'coder', generation: 2 };
    });
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({ dispatcher: { spawnCoder } } as never);
    const registry = vi.spyOn(manager as never, '_withAgentRegistryLock' as never);
    const spawn = manager.autoloopSpawnCoder('run');
    await vi.waitFor(() => expect(spawnCoder).toHaveBeenCalledOnce());
    registry.mockClear();
    const deletion = manager.autoloopDelete('run');
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(registry).not.toHaveBeenCalled();
    releaseSpawn();
    await expect(spawn).resolves.toEqual({ role: 'coder', generation: 2 });
    await expect(deletion).resolves.toBe(false);
    await manager.shutdown();
  });

  it('persists then queues one Reviewer-only request at the live iteration without a Coder effect', async () => {
    const manager = new SessionManager({});
    const send = vi.fn(async (_message: unknown) => undefined);
    const requestReview = vi.fn(async () => ({
      status: 'prepared' as const,
      target: 'reviewer' as const,
      idempotency_key: 'review-7',
      payload: {
        iter: 9,
        ledger_path: '/ledger',
        prior_metrics: [],
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      },
    }));
    const spawnCoder = vi.fn();
    const acceptReviewRequest = vi.fn();
    const releaseReviewRequest = vi.fn();
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9 }, send },
      dispatcher: {
        requestReview,
        spawnCoder,
        acceptReviewRequest,
        releaseReviewRequest,
      },
    } as never);

    try {
      const result = await manager.autoloopRequestReview('run', {
        checkpoint_sha: 'A'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      });
      expect(requestReview).toHaveBeenCalledWith(
        {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        },
        9,
      );
      expect(send).toHaveBeenCalledOnce();
      expect(acceptReviewRequest).toHaveBeenCalledWith('review-7');
      expect(requestReview.mock.invocationCallOrder[0]).toBeLessThan(send.mock.invocationCallOrder[0]);
      expect(send.mock.invocationCallOrder[0]).toBeLessThan(acceptReviewRequest.mock.invocationCallOrder[0]);
      expect(releaseReviewRequest).not.toHaveBeenCalled();
      expect(send.mock.calls[0][0]).toMatchObject({
        type: 'review_request',
        iter: 9,
        to: 'reviewer',
        payload: {
          iter: 9,
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          idempotency_key: 'review-7',
        },
      });
      expect(spawnCoder).not.toHaveBeenCalled();
      expect(result).toEqual({ status: 'prepared', target: 'reviewer', idempotency_key: 'review-7' });
    } finally {
      await manager.shutdown();
    }
  });

  it('rejects internal spawn accessors before live-run lookup or role effects', async () => {
    const manager = new SessionManager({});
    const live = vi.spyOn(manager as never, '_liveAutoloop' as never);
    const args = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(args, 'coder_engine', { enumerable: true, get: () => 'codex' });
    try {
      await expect(manager.autoloopSpawnCoder('run', args as never)).rejects.toThrow(/own data property/i);
      expect(live).not.toHaveBeenCalled();
    } finally {
      await manager.shutdown();
    }
  });

  it('validates request_review before probing live-run state', async () => {
    const manager = new SessionManager({});
    const live = vi.spyOn(manager as never, '_liveAutoloop' as never);
    try {
      await expect(manager.autoloopRequestReview('missing', { checkpoint_sha: 'bad' } as never)).rejects.toThrow(
        /request_review/i,
      );
      expect(live).not.toHaveBeenCalled();
      expect(
        (
          (manager as unknown as Record<string, unknown>)['_autoloopReviewTransactions'] as Map<string, Promise<void>>
        ).has('missing'),
      ).toBe(false);
    } finally {
      await manager.shutdown();
    }
  });

  it('does not enqueue or accept a duplicate Reviewer-only request', async () => {
    const manager = new SessionManager({});
    const send = vi.fn();
    const acceptReviewRequest = vi.fn();
    const releaseReviewRequest = vi.fn();
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9 }, send },
      dispatcher: {
        requestReview: vi.fn(async () => ({
          status: 'duplicate',
          target: 'reviewer',
          idempotency_key: 'review-7',
        })),
        acceptReviewRequest,
        releaseReviewRequest,
      },
    } as never);
    try {
      await expect(
        manager.autoloopRequestReview('run', {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        }),
      ).resolves.toEqual({ status: 'duplicate', target: 'reviewer', idempotency_key: 'review-7' });
      expect(send).not.toHaveBeenCalled();
      expect(acceptReviewRequest).not.toHaveBeenCalled();
      expect(releaseReviewRequest).not.toHaveBeenCalled();
    } finally {
      await manager.shutdown();
    }
  });

  it('retries a released review with its persisted iteration after the live iteration advances', async () => {
    const manager = new SessionManager({});
    const state = { iter: 9 };
    const payload = {
      iter: 9,
      ledger_path: '/ledger',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    };
    const requestReview = vi.fn(async (_request: unknown, targetIter: number) => ({
      status: 'prepared' as const,
      target: 'reviewer' as const,
      idempotency_key: 'review-7',
      payload: { ...payload, iter: targetIter },
    }));
    const releaseReviewRequest = vi.fn();
    const acceptReviewRequest = vi.fn();
    const send = vi.fn().mockRejectedValueOnce(new Error('queue unavailable')).mockResolvedValueOnce(undefined);
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state, send },
      dispatcher: { requestReview, releaseReviewRequest, acceptReviewRequest },
    } as never);
    const input = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    };
    try {
      await expect(manager.autoloopRequestReview('run', input)).rejects.toThrow('queue unavailable');
      state.iter = 10;
      await manager.autoloopRequestReview('run', input);
      expect(requestReview.mock.calls.map((call) => call[1])).toEqual([9, 9]);
      expect(acceptReviewRequest).toHaveBeenCalledWith('review-7');
    } finally {
      await manager.shutdown();
    }
  });

  it('clears failed-delivery rearm state when the run is deleted', async () => {
    const manager = new SessionManager({});
    const payload = {
      iter: 9,
      ledger_path: '/ledger',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    };
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9 }, send: vi.fn().mockRejectedValue(new Error('queue unavailable')) },
      dispatcher: {
        requestReview: vi.fn(async () => ({
          status: 'prepared',
          target: 'reviewer',
          idempotency_key: 'review-7',
          payload,
        })),
        releaseReviewRequest: vi.fn(),
        acceptReviewRequest: vi.fn(),
      },
    } as never);
    try {
      await expect(
        manager.autoloopRequestReview('run', {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        }),
      ).rejects.toThrow('queue unavailable');
      expect(
        (
          (manager as unknown as Record<string, unknown>)['_autoloopReleasedReviewIterations'] as Map<
            string,
            Map<string, number>
          >
        ).get('run')?.size,
      ).toBe(1);
      await manager.autoloopDelete('run');
      expect(
        (
          (manager as unknown as Record<string, unknown>)['_autoloopReleasedReviewIterations'] as Map<
            string,
            Map<string, number>
          >
        ).has('run'),
      ).toBe(false);
    } finally {
      await manager.shutdown();
    }
  });

  it('strips the MCP routing run_id before validating and dispatching request_review', async () => {
    const registration = collectRegistration();
    const requestReview = vi
      .spyOn(SessionManager.prototype as never, 'autoloopRequestReview' as never)
      .mockResolvedValue({ status: 'prepared', target: 'reviewer', idempotency_key: 'review-7' } as never);
    try {
      const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_request_review')!;
      await tool.execute('review-only', {
        run_id: 'run',
        checkpoint_sha: 'A'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      });
      expect(requestReview).toHaveBeenCalledWith('run', {
        checkpoint_sha: 'A'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      });
    } finally {
      registration.services[0]?.stop();
      requestReview.mockRestore();
    }
  });

  it('rejects an accessor MCP run_id before request_review dispatch', async () => {
    const registration = collectRegistration();
    const requestReview = vi.spyOn(SessionManager.prototype as never, 'autoloopRequestReview' as never);
    const args = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(args, 'run_id', {
      enumerable: true,
      get: () => {
        throw new Error('run_id accessor executed');
      },
    });
    Object.assign(args, {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    });
    try {
      const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_request_review')!;
      await expect(tool.execute('review-only', args)).rejects.toThrow(/run_id.*own data property/i);
      expect(requestReview).not.toHaveBeenCalled();
    } finally {
      registration.services[0]?.stop();
      requestReview.mockRestore();
    }
  });

  it('rejects accessor MCP request_review fields even when Object.prototype.value is polluted', async () => {
    const registration = collectRegistration();
    const requestReview = vi.spyOn(SessionManager.prototype as never, 'autoloopRequestReview' as never);
    const args = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(args, 'run_id', { enumerable: true, value: 'run' });
    Object.defineProperty(args, 'checkpoint_sha', {
      enumerable: true,
      get: () => {
        throw new Error('checkpoint accessor executed');
      },
    });
    Object.assign(args, {
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    });
    Object.defineProperty(Object.prototype, 'value', { configurable: true, value: 'a'.repeat(40) });
    let caught: unknown;
    try {
      const tool = registration.tools.find((candidate) => candidate.name === 'autoloop_request_review')!;
      try {
        await tool.execute('review-only', args);
      } catch (error) {
        caught = error;
      }
    } finally {
      delete (Object.prototype as Record<string, unknown>).value;
      registration.services[0]?.stop();
      requestReview.mockRestore();
    }
    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toMatch(/invalid property descriptor|own data property/i);
    expect(requestReview).not.toHaveBeenCalled();
  });

  it('re-arms a durably prepared review when queue delivery fails', async () => {
    const manager = new SessionManager({});
    const payload = {
      iter: 9,
      ledger_path: '/ledger',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    };
    const requestReview = vi.fn(async () => ({
      status: 'prepared' as const,
      target: 'reviewer' as const,
      idempotency_key: 'review-7',
      payload,
    }));
    const releaseReviewRequest = vi.fn();
    const acceptReviewRequest = vi.fn();
    const send = vi.fn(async () => {
      throw new Error('queue unavailable');
    });
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9 }, send },
      dispatcher: { requestReview, releaseReviewRequest, acceptReviewRequest },
    } as never);
    try {
      await expect(
        manager.autoloopRequestReview('run', {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        }),
      ).rejects.toThrow('queue unavailable');
      expect(releaseReviewRequest).toHaveBeenCalledWith('review-7', payload);
      expect(acceptReviewRequest).not.toHaveBeenCalled();
    } finally {
      await manager.shutdown();
    }
  });

  it('does not release or rearm a Reviewer identity after a committed ledger failure', async () => {
    const manager = new SessionManager({});
    const payload = {
      iter: 9,
      ledger_path: '/ledger',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'committed-review',
    };
    const failure = new SecureAutoloopLedgerCommitError(
      'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
      'Reviewer bytes are already committed',
      { cause: new Error('simulated fsync interruption'), effectsApplied: true },
    );
    const releaseReviewRequest = vi.fn();
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9, status: 'running' }, send: vi.fn().mockRejectedValue(failure) },
      dispatcher: {
        requestReview: vi.fn().mockResolvedValue({
          status: 'prepared',
          target: 'reviewer',
          idempotency_key: 'committed-review',
          payload,
        }),
        releaseReviewRequest,
        acceptReviewRequest: vi.fn(),
      },
    } as never);
    await expect(
      manager.autoloopRequestReview('run', {
        checkpoint_sha: payload.checkpoint_sha,
        source_run_id: payload.source_run_id,
        source_iter: payload.source_iter,
        scope: payload.scope,
        idempotency_key: payload.idempotency_key,
      }),
    ).rejects.toBe(failure);
    expect(releaseReviewRequest).not.toHaveBeenCalled();
    expect(
      (
        (manager as unknown as Record<string, unknown>)['_autoloopReleasedReviewIterations'] as Map<string, unknown>
      ).has('run'),
    ).toBe(false);
    await manager.shutdown();
  });

  it('caps released Reviewer identities at 64 while allowing an existing identity retry', async () => {
    const manager = new SessionManager({});
    const released = new Map(Array.from({ length: 64 }, (_, index) => [`review-${index}`, 9]));
    (
      (manager as unknown as Record<string, unknown>)['_autoloopReleasedReviewIterations'] as Map<
        string,
        Map<string, number>
      >
    ).set('run', released);
    const live = vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 10, status: 'running' }, send: vi.fn() },
      dispatcher: { requestReview: vi.fn() },
    } as never);
    const base = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
    };
    await expect(manager.autoloopRequestReview('run', { ...base, idempotency_key: 'new-review' })).rejects.toThrow(
      'request_review retry capacity is exhausted',
    );
    expect(live).toHaveBeenCalledWith('run', 'requesting review');
    live.mockClear();
    live.mockReturnValue({
      runner: { state: { iter: 10, status: 'running' }, send: vi.fn().mockResolvedValue(undefined) },
      dispatcher: {
        requestReview: vi.fn().mockResolvedValue({
          status: 'duplicate',
          target: 'reviewer',
          idempotency_key: 'review-0',
        }),
      },
    } as never);
    await expect(manager.autoloopRequestReview('run', { ...base, idempotency_key: 'review-0' })).resolves.toMatchObject(
      {
        status: 'duplicate',
      },
    );
    await manager.shutdown();
  });

  it.each(['terminated', 'crashed'] as const)(
    'releases a prepared review when the run becomes %s before queue delivery',
    async (terminalStatus) => {
      const manager = new SessionManager({});
      const state = { iter: 9, status: 'running' };
      const payload = {
        iter: 9,
        ledger_path: '/ledger',
        prior_metrics: [],
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      };
      const releaseReviewRequest = vi.fn();
      const acceptReviewRequest = vi.fn();
      const send = vi.fn();
      vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
        runner: { state, send },
        dispatcher: {
          requestReview: vi.fn(async () => {
            state.status = terminalStatus;
            return { status: 'prepared', target: 'reviewer', idempotency_key: 'review-7', payload };
          }),
          releaseReviewRequest,
          acceptReviewRequest,
        },
      } as never);
      await expect(
        manager.autoloopRequestReview('run', {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        }),
      ).rejects.toThrow(/terminal/i);
      expect(send).not.toHaveBeenCalled();
      expect(releaseReviewRequest).toHaveBeenCalledWith('review-7', payload);
      expect(acceptReviewRequest).not.toHaveBeenCalled();
      await manager.shutdown();
    },
  );

  it('accepts exactly once when queue delivery resolves before the run becomes terminal', async () => {
    const manager = new SessionManager({});
    const state = { iter: 9, status: 'running' };
    const payload = {
      iter: 9,
      ledger_path: '/ledger',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    };
    const releaseReviewRequest = vi.fn();
    const acceptReviewRequest = vi.fn();
    const send = vi.fn(async () => {
      state.status = 'terminated';
    });
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state, send },
      dispatcher: {
        requestReview: vi.fn().mockResolvedValue({
          status: 'prepared',
          target: 'reviewer',
          idempotency_key: 'review-7',
          payload,
        }),
        releaseReviewRequest,
        acceptReviewRequest,
      },
    } as never);
    await expect(
      manager.autoloopRequestReview('run', {
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      }),
    ).resolves.toMatchObject({ status: 'prepared' });
    expect(releaseReviewRequest).not.toHaveBeenCalled();
    expect(acceptReviewRequest).toHaveBeenCalledWith('review-7');
    await manager.shutdown();
  });

  it('rejects a queued Reviewer handoff that termination discards before delivery', async () => {
    let releaseActiveDelivery!: () => void;
    const activeDelivery = new Promise<void>((resolve) => {
      releaseActiveDelivery = resolve;
    });
    const delivered: string[] = [];
    const dispatcher: AgentDispatcher = {
      async deliver(message) {
        delivered.push(message.type);
        if (message.type === 'chat') await activeDelivery;
        return [];
      },
    };
    const runner = new AutoloopRunner({
      run_id: 'terminal-discard',
      workspace: '/tmp/terminal-discard',
      ledger_dir: '/tmp/terminal-discard/ledger',
      dispatcher,
      notifyUser: async () => undefined,
      stallCheckIntervalMs: 24 * 60 * 60 * 1000,
    });
    const inFlight = runner.send(Msg.chat(0, { text: 'block the queue' }));
    await vi.waitFor(() => expect(delivered).toEqual(['chat']));
    const review = runner.send(
      Msg.reviewRequest(0, {
        iter: 0,
        ledger_path: '/ledger',
        prior_metrics: [],
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      }),
    );
    const reviewOutcome = expect(review).rejects.toThrow(/not delivered.*terminal/i);

    await runner.send(Msg.terminate(0, { reason: 'operator-stop' }));
    expect(delivered).toEqual(['chat']);
    releaseActiveDelivery();
    await expect(inFlight).resolves.toBeUndefined();
    await reviewOutcome;
    runner.stop();
  });

  it('rejects rather than acknowledges a Reviewer handoff that reaches a paused runner', async () => {
    const delivered: string[] = [];
    const dispatcher: AgentDispatcher = {
      async deliver(message) {
        delivered.push(message.type);
        return [];
      },
    };
    const runner = new AutoloopRunner({
      run_id: 'paused-terminal-discard',
      workspace: '/tmp/paused-terminal-discard',
      ledger_dir: '/tmp/paused-terminal-discard/ledger',
      dispatcher,
      notifyUser: async () => undefined,
    });
    await runner.send(Msg.pause(0, { reason: 'operator-pause' }));
    const review = runner.send(
      Msg.reviewRequest(0, {
        iter: 0,
        ledger_path: '/ledger',
        prior_metrics: [],
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 0,
        scope: ['security'],
        idempotency_key: 'paused-public-review',
      }),
    );
    await expect(review).rejects.toThrow(/not delivered.*paused/i);
    await runner.send(Msg.terminate(0, { reason: 'operator-stop' }));
    expect(delivered).toEqual([]);
    runner.stop();
  });

  it('parks and resumes the legacy internal review_request synthesized from iter_artifacts', async () => {
    const delivered: string[] = [];
    const dispatcher: AgentDispatcher = {
      async deliver(message) {
        delivered.push(message.type);
        return [];
      },
    };
    const runner = new AutoloopRunner({
      run_id: 'paused-internal-review',
      workspace: '/tmp/paused-internal-review',
      ledger_dir: '/tmp/paused-internal-review/ledger',
      dispatcher,
      notifyUser: async () => undefined,
    });
    await runner.send(Msg.pause(0, { reason: 'operator-pause' }));
    const internalReview = runner.send(
      Msg.iterArtifacts(0, { diff: 'patch', eval_output: { passed: true }, files_changed: ['a.ts'] }),
    );
    await expect(Promise.race([internalReview.then(() => 'settled'), Promise.resolve('pending')])).resolves.toBe(
      'pending',
    );
    expect(delivered).toEqual([]);
    await runner.send(Msg.resume(0));
    await internalReview;
    expect(delivered).toEqual(['review_request']);
    runner.stop();
  });

  it('does not double-close sender accounting when the legacy pause buffer evicts an old message', async () => {
    const runner = new AutoloopRunner({
      run_id: 'paused-buffer-accounting',
      workspace: '/tmp/paused-buffer-accounting',
      ledger_dir: '/tmp/paused-buffer-accounting/ledger',
      dispatcher: {
        async deliver() {
          return [];
        },
      },
      notifyUser: async () => undefined,
    });
    runner.on('error', () => undefined);
    await runner.send(Msg.pause(0, { reason: 'operator-pause' }));
    const oldest = Msg.chat(0, { text: 'oldest parked message' });
    const sibling = Msg.chat(0, { text: 'sibling parked message' });
    const sender = {
      id: oldest.msg_id,
      pending: 2,
      settled: false,
      rootDelivered: false,
      promise: Promise.resolve(),
      resolve: vi.fn(),
      reject: vi.fn(),
    };
    const internals = runner as unknown as {
      pausedBuffer: ReturnType<typeof Msg.chat>[];
      messageSenders: Map<ReturnType<typeof Msg.chat>, typeof sender>;
    };
    internals.pausedBuffer.push(
      oldest,
      sibling,
      ...Array.from({ length: 998 }, (_, i) => Msg.chat(0, { text: `parked-${i}` })),
    );
    internals.messageSenders.set(oldest, sender);
    internals.messageSenders.set(sibling, sender);

    await runner.send(Msg.chat(0, { text: 'overflow trigger' }));

    expect(sender).toMatchObject({ pending: 2, settled: false });
    expect(sender).not.toHaveProperty('failure');
    expect(sender.reject).not.toHaveBeenCalled();
    runner.stop();
  });

  it('keeps a delivered public root successful when termination discards a queued descendant', async () => {
    const delivered: string[] = [];
    const holder: { runner?: AutoloopRunner } = {};
    const dispatcher: AgentDispatcher = {
      async deliver(message) {
        delivered.push(message.type);
        if (message.type === 'review_request') {
          return [Msg.chat(0, { text: 'first descendant' }), Msg.chat(0, { text: 'discarded descendant' })];
        }
        if (message.type === 'chat') {
          await holder.runner!.send(Msg.terminate(0, { reason: 'terminal-after-root' }));
        }
        return [];
      },
    };
    const runner = new AutoloopRunner({
      run_id: 'descendant-terminal-discard',
      workspace: '/tmp/descendant-terminal-discard',
      ledger_dir: '/tmp/descendant-terminal-discard/ledger',
      dispatcher,
      notifyUser: async () => undefined,
    });
    holder.runner = runner;
    await expect(
      runner.send(
        Msg.reviewRequest(0, {
          iter: 0,
          ledger_path: '/ledger',
          prior_metrics: [],
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 0,
          scope: ['security'],
          idempotency_key: 'delivered-root',
        }),
      ),
    ).resolves.toBeUndefined();
    expect(delivered).toEqual(['review_request', 'chat']);
    runner.stop();
  });

  it('rejects a paused run before persisting a Reviewer-only request', async () => {
    const manager = new SessionManager({});
    const requestReview = vi.fn();
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9, status: 'paused' }, send: vi.fn() },
      dispatcher: { requestReview },
    } as never);
    await expect(
      manager.autoloopRequestReview('run', {
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      }),
    ).rejects.toThrow(/paused/i);
    expect(requestReview).not.toHaveBeenCalled();
    await manager.shutdown();
  });

  it.each(['terminated', 'crashed'] as const)(
    'rejects an already-%s run with a typed terminal failure before persistence',
    async (status) => {
      const manager = new SessionManager({});
      const requestReview = vi.fn();
      vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
        runner: { state: { iter: 9, status }, send: vi.fn() },
        dispatcher: { requestReview },
      } as never);
      const failure = manager.autoloopRequestReview('run', {
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'terminal-before-prepare',
      });
      await expect(failure).rejects.toMatchObject({
        name: 'AutoloopChatStateError',
        code: 'AUTOLOOP_RUN_TERMINAL',
      });
      expect(requestReview).not.toHaveBeenCalled();
      await manager.shutdown();
    },
  );

  it.each([
    [
      "Autoloop message 'review-raw' was not delivered because the run became terminal",
      'AUTOLOOP_RUN_TERMINAL',
      'Autoloop run became terminal before Reviewer-only queue delivery',
    ],
    [
      "Autoloop message 'review-raw' was not delivered because the run is paused",
      'AUTOLOOP_RUN_PAUSED',
      "Autoloop run 'run' became paused before Reviewer-only queue delivery",
    ],
  ] as const)('remaps raw runner non-delivery to typed public failure %s', async (raw, code, message) => {
    const manager = new SessionManager({});
    const payload = {
      iter: 9,
      ledger_path: '/ledger',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'raw-remap',
    };
    const releaseReviewRequest = vi.fn();
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9, status: 'running' }, send: vi.fn().mockRejectedValue(new Error(raw)) },
      dispatcher: {
        requestReview: vi.fn().mockResolvedValue({
          status: 'prepared',
          target: 'reviewer',
          idempotency_key: 'raw-remap',
          payload,
        }),
        releaseReviewRequest,
        acceptReviewRequest: vi.fn(),
      },
    } as never);
    const failure = manager.autoloopRequestReview('run', {
      checkpoint_sha: payload.checkpoint_sha,
      source_run_id: payload.source_run_id,
      source_iter: payload.source_iter,
      scope: payload.scope,
      idempotency_key: payload.idempotency_key,
    });
    await expect(failure).rejects.toMatchObject({ name: 'AutoloopChatStateError', code, message });
    expect(releaseReviewRequest).toHaveBeenCalledWith('raw-remap', payload);
    await manager.shutdown();
  });

  it('releases a prepared request if the run pauses during persistence before queue delivery', async () => {
    const manager = new SessionManager({});
    const state = { iter: 9, status: 'running' };
    const payload = {
      iter: 9,
      ledger_path: '/ledger',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    };
    const send = vi.fn();
    const releaseReviewRequest = vi.fn();
    const acceptReviewRequest = vi.fn();
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state, send },
      dispatcher: {
        requestReview: vi.fn(async () => {
          state.status = 'paused';
          return { status: 'prepared', target: 'reviewer', idempotency_key: 'review-7', payload };
        }),
        releaseReviewRequest,
        acceptReviewRequest,
      },
    } as never);
    await expect(
      manager.autoloopRequestReview('run', {
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      }),
    ).rejects.toThrow("Autoloop run 'run' became paused before Reviewer-only queue delivery");
    expect(send).not.toHaveBeenCalled();
    expect(releaseReviewRequest).toHaveBeenCalledWith('review-7', payload);
    expect(acceptReviewRequest).not.toHaveBeenCalled();
    await manager.shutdown();
  });

  it('serializes stop behind an in-flight public mutation', async () => {
    const manager = new SessionManager({});
    let release!: () => void;
    const predecessor = new Promise<void>((resolve) => {
      release = resolve;
    });
    ((manager as unknown as Record<string, unknown>)['_autoloopReviewTransactions'] as Map<string, Promise<void>>).set(
      'run',
      predecessor,
    );
    const send = vi.fn().mockResolvedValue(undefined);
    vi.spyOn(
      (manager as unknown as { kernel: { handle: (...args: unknown[]) => unknown } }).kernel,
      'handle',
    ).mockReturnValue({
      runner: { state: { iter: 9 }, send },
    });
    const stop = manager.autoloopStop('run');
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(send).not.toHaveBeenCalled();
    release();
    await expect(stop).resolves.toBe(true);
    expect(send).toHaveBeenCalledOnce();
    await manager.shutdown();
  });

  it('does not report stop failure when delete begins after terminate delivery starts', async () => {
    const manager = new SessionManager({});
    let releaseStop!: () => void;
    const blocked = new Promise<void>((resolve) => {
      releaseStop = resolve;
    });
    const send = vi.fn(async () => await blocked);
    vi.spyOn(
      (manager as unknown as { kernel: { handle: (...args: unknown[]) => unknown } }).kernel,
      'handle',
    ).mockReturnValue({
      runner: { state: { iter: 9 }, send, stop: vi.fn() },
      dispatcher: { shutdown: vi.fn().mockResolvedValue(undefined) },
    });
    const registry = vi.spyOn(manager as never, '_withAgentRegistryLock' as never);
    const stop = manager.autoloopStop('run');
    await vi.waitFor(() => expect(send).toHaveBeenCalledOnce());
    registry.mockClear();
    const deletion = manager.autoloopDelete('run');
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(registry).not.toHaveBeenCalled();
    releaseStop();
    await expect(stop).resolves.toBe(true);
    await deletion;
    await manager.shutdown();
  });

  it('does not release and redeliver after queue delivery succeeds but acceptance throws', async () => {
    const manager = new SessionManager({});
    const payload = {
      iter: 9,
      ledger_path: '/ledger',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    };
    const releaseReviewRequest = vi.fn();
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9, status: 'running' }, send: vi.fn().mockResolvedValue(undefined) },
      dispatcher: {
        requestReview: vi.fn().mockResolvedValue({
          status: 'prepared',
          target: 'reviewer',
          idempotency_key: 'review-7',
          payload,
        }),
        releaseReviewRequest,
        acceptReviewRequest: vi.fn(() => {
          throw new Error('accept unavailable');
        }),
      },
    } as never);
    await expect(
      manager.autoloopRequestReview('run', {
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
      }),
    ).rejects.toThrow('accept unavailable');
    expect(releaseReviewRequest).not.toHaveBeenCalled();
    await manager.shutdown();
  });

  it('serializes public review preparation and delivery per run', async () => {
    const manager = new SessionManager({});
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const requestReview = vi
      .fn()
      .mockImplementationOnce(async (request: { idempotency_key: string }) => {
        await firstBlocked;
        return {
          status: 'duplicate' as const,
          target: 'reviewer' as const,
          idempotency_key: request.idempotency_key,
        };
      })
      .mockImplementationOnce(async (request: { idempotency_key: string }) => ({
        status: 'duplicate' as const,
        target: 'reviewer' as const,
        idempotency_key: request.idempotency_key,
      }));
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9 }, send: vi.fn() },
      dispatcher: { requestReview },
    } as never);
    const first = manager.autoloopRequestReview('run', {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7a',
    });
    const second = manager.autoloopRequestReview('run', {
      checkpoint_sha: 'b'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 8,
      scope: ['logic'],
      idempotency_key: 'review-7b',
    });
    try {
      await vi.waitFor(() => expect(requestReview).toHaveBeenCalledTimes(1));
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(requestReview).toHaveBeenCalledTimes(1);
    } finally {
      releaseFirst();
      await Promise.all([first, second]);
      await manager.shutdown();
    }
    expect(requestReview).toHaveBeenCalledTimes(2);
  });

  it('rechecks the delete fence after waiting for an earlier review transaction', async () => {
    const manager = new SessionManager({});
    let releaseFirst!: () => void;
    const firstBlocked = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const requestReview = vi.fn().mockImplementationOnce(async () => {
      await firstBlocked;
      return { status: 'duplicate', target: 'reviewer', idempotency_key: 'review-7a' };
    });
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9 }, send: vi.fn() },
      dispatcher: { requestReview },
    } as never);
    const base = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
    };
    const first = manager.autoloopRequestReview('run', { ...base, idempotency_key: 'review-7a' });
    const second = manager.autoloopRequestReview('run', { ...base, idempotency_key: 'review-7b' });
    await vi.waitFor(() => expect(requestReview).toHaveBeenCalledTimes(1));
    ((manager as unknown as Record<string, unknown>)['_autoloopReviewDeleting'] as Set<string>).add('run');
    releaseFirst();
    await expect(first).resolves.toMatchObject({ status: 'duplicate' });
    await expect(second).rejects.toThrow("Autoloop run 'run' is being deleted");
    expect(requestReview).toHaveBeenCalledTimes(1);
    await manager.shutdown();
  });

  it('always releases the delete fence when teardown throws', async () => {
    const manager = new SessionManager({});
    const registryLock = vi.spyOn(manager as never, '_withAgentRegistryLock' as never).mockImplementation(() => {
      throw new Error('registry unavailable');
    });
    await expect(manager.autoloopDelete('run')).rejects.toThrow('registry unavailable');
    expect(((manager as unknown as Record<string, unknown>)['_autoloopReviewDeleting'] as Set<string>).has('run')).toBe(
      false,
    );
    registryLock.mockRestore();
    await manager.shutdown();
  });

  it('keeps the delete fence while any overlapping delete remains in flight', async () => {
    const manager = new SessionManager({});
    let releaseSecond!: () => void;
    const secondBlocked = new Promise<void>((resolve) => {
      releaseSecond = resolve;
    });
    const shutdown = vi
      .fn()
      .mockResolvedValueOnce(undefined)
      .mockImplementationOnce(async () => await secondBlocked);
    vi.spyOn(
      (manager as unknown as { kernel: { handle: (...args: unknown[]) => unknown } }).kernel,
      'handle',
    ).mockReturnValue({ dispatcher: { shutdown }, runner: { stop: vi.fn() } });
    const first = manager.autoloopDelete('run');
    const second = manager.autoloopDelete('run');
    await expect(first).resolves.toBe(true);
    expect(((manager as unknown as Record<string, unknown>)['_autoloopReviewDeleting'] as Set<string>).has('run')).toBe(
      true,
    );
    await expect(manager.autoloopSpawnCoder('run')).rejects.toThrow(/being deleted/);
    releaseSecond();
    await second;
    expect(((manager as unknown as Record<string, unknown>)['_autoloopReviewDeleting'] as Set<string>).has('run')).toBe(
      false,
    );
    await manager.shutdown();
  });

  it('rejects inherited request_review input before persistence or queue effects', async () => {
    const manager = new SessionManager({});
    const requestReview = vi.fn();
    const send = vi.fn();
    vi.spyOn(manager as never, '_liveAutoloop' as never).mockReturnValue({
      runner: { state: { iter: 9 }, send },
      dispatcher: { requestReview },
    } as never);
    const inherited = Object.create({ source_iter: 7 }) as Record<string, unknown>;
    Object.assign(inherited, {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      scope: ['security'],
      idempotency_key: 'review-7',
    });

    try {
      await expect(manager.autoloopRequestReview('run', inherited as never)).rejects.toThrow(/inherited data/i);
      expect(requestReview).not.toHaveBeenCalled();
      expect(send).not.toHaveBeenCalled();
    } finally {
      await manager.shutdown();
    }
  });
});
