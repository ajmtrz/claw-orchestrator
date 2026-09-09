import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as net from 'node:net';
import * as path from 'node:path';
import { EventEmitter } from 'node:events';
import { createHash } from 'node:crypto';
import { SessionManager } from '../session-manager.js';
import { EmbeddedServer } from '../embedded-server.js';
import { AutoloopOperationError } from '../autoloop/dispatcher.js';
import { AutoloopRunner } from '../autoloop/runner.js';
import { SecureAutoloopLedger, SecureAutoloopLedgerCommitError } from '../autoloop/secure-ledger.js';
import type { AutoloopState } from '../autoloop/types.js';
import {
  commit,
  createAndAcquire,
  loadRun,
  readLease,
  releaseLease,
  renewLease,
  runDir,
  type RunGuard,
} from '../kernel/store.js';
import type { RunRecord, WorkflowSpec } from '../kernel/types.js';
import type { CouncilSession } from '../types.js';
import { useIsolatedHome } from './helpers/isolate-home.js';

const COMMITTED_LEDGER_CODES = [
  'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
  'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
  'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE',
  'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
] as const;

type DetachedFixtureCode = 'AUTOLOOP_ENGINE_FAILURE' | 'AUTOLOOP_RUN_PAUSED' | (typeof COMMITTED_LEDGER_CODES)[number];

// Tests construct real EmbeddedServer instances, which write to
// ~/.openclaw/server-token and re-read it per request. Isolating $HOME to a
// per-file temp dir keeps that token file local to this worker, so the real
// user token is never touched and parallel test files don't clobber each
// other's token (which otherwise causes timing-dependent 401s).
useIsolatedHome();

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const port = (srv.address() as net.AddressInfo).port;
      srv.close(() => resolve(port));
    });
    srv.on('error', reject);
  });
}

function detachedState(runId: string, workspace: string, ledgerDir: string): AutoloopState {
  return {
    run_id: runId,
    status: 'terminated',
    iter: 3,
    subagents_spawned: true,
    started_at: '2026-09-07T08:00:00.000Z',
    workspace,
    ledger_dir: ledgerDir,
    push_log_count: 0,
    status_reason: 'historical',
    pending_dispatch: null,
    consecutive_phase_errors: 0,
    recent_phase_errors: [],
    metric_history: [],
    last_activity_at: 1234,
  };
}

function stageStoredAutoloopRun(
  runId: string,
  workspace: string,
  ledgerDir: string,
  state = detachedState(runId, workspace, ledgerDir),
  updatedAt = state.started_at,
  checkpointDecisionLog?: string,
): RunGuard {
  const spec: WorkflowSpec = {
    name: 'autoloop',
    cwd: workspace,
    nodes: [{ id: 'main', kind: 'autoloop', workspace, config: {} }],
  };
  const guard = createAndAcquire(runId, spec, `test-owner-${runId}`);
  const record: RunRecord = {
    runId,
    workflow: 'autoloop',
    spec,
    state: 'completed',
    outcome: 'unverified',
    cwd: workspace,
    createdAt: state.started_at,
    updatedAt,
    endedAt: state.started_at,
    currentNode: 'main',
    nodes: {
      main: {
        id: 'main',
        kind: 'autoloop',
        state: 'succeeded',
        attempts: 1,
        visits: 1,
        data: {
          state,
          plannerSession: `autoloop-${runId}-planner`,
          ...(checkpointDecisionLog !== undefined
            ? {
                detachedFailureLedgerCursor: {
                  version: 1,
                  byteOffset: Buffer.byteLength(checkpointDecisionLog, 'utf8'),
                  prefixSha256: createHash('sha256').update(checkpointDecisionLog, 'utf8').digest('hex'),
                },
              }
            : {}),
        },
      },
    },
  };
  const result = commit(guard, { record });
  if (result.outcome !== 'committed') throw new Error(`failed to stage ${runId}: ${result.reason}`);
  return guard;
}

function fakeDetachedHandle(ledger: SecureAutoloopLedger, state: AutoloopState = detachedState('', '', '')) {
  const runner = Object.assign(new EventEmitter(), { state, send: vi.fn() });
  const dispatcher = Object.assign(new EventEmitter(), { secureLedgerCapability: ledger });
  return { runner, dispatcher };
}

function chatStateFailure(
  code: 'AUTOLOOP_SEND_TIMEOUT' | 'AUTOLOOP_RUN_PAUSED' | 'AUTOLOOP_RUN_TERMINAL',
  message: string,
) {
  const pendingDispatch =
    code === 'AUTOLOOP_SEND_TIMEOUT'
      ? {
          status: 'awaiting_resume' as const,
          dispatch_id: 'dispatch-planner-3',
          agent: 'planner' as const,
          message_id: 'chat-3',
          message_type: 'chat' as const,
          iter: 3,
          timeout_ms: 600_000,
          error: 'Timeout waiting for response',
        }
      : undefined;
  return Object.assign(new Error(message), {
    name: 'AutoloopChatStateError',
    code,
    retryable: code === 'AUTOLOOP_SEND_TIMEOUT',
    pending_dispatch: pendingDispatch,
    status_reason:
      code === 'AUTOLOOP_SEND_TIMEOUT'
        ? 'awaiting_resume:send_timeout:planner:dispatch-planner-3'
        : code === 'AUTOLOOP_RUN_PAUSED'
          ? 'operator-paused'
          : 'phase_error_circuit',
  });
}

function decisionRows(ledger: SecureAutoloopLedger): Array<Record<string, unknown>> {
  return (ledger.readFlatFile('decisions.jsonl') ?? '')
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function appendDetachedFailureRow(
  ledger: SecureAutoloopLedger,
  row: {
    ts: string;
    id?: string;
    error: string;
    code?: DetachedFixtureCode;
    committed?: true;
  },
): void {
  ledger.appendFlatFile(
    'decisions.jsonl',
    `${JSON.stringify({
      ts: row.ts,
      kind: 'phase_error',
      actor: 'dispatcher',
      payload: {
        agent: 'planner',
        phase: 'planner_turn',
        code: row.code ?? 'AUTOLOOP_ENGINE_FAILURE',
        retryable:
          row.code !== 'AUTOLOOP_RUN_PAUSED' &&
          !COMMITTED_LEDGER_CODES.includes(row.code as (typeof COMMITTED_LEDGER_CODES)[number]),
        ...(row.committed ? { committed: true } : {}),
        error: row.error,
        ...(row.id ? { detached_failure_id: row.id } : {}),
      },
    })}\n`,
  );
}

async function historicalAutoloopSnapshot(
  port: number,
  token: string,
  runId: string,
): Promise<{ state: AutoloopState }> {
  const events = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(events.status).toBe(200);
  const body = await events.text();
  const snapshotData = body.match(/event: snapshot\ndata: ([^\n]+)/)?.[1];
  expect(snapshotData).toBeDefined();
  return JSON.parse(snapshotData!) as { state: AutoloopState };
}

describe('token file write-order', () => {
  it('does NOT overwrite ~/.openclaw/server-token when bind fails (EADDRINUSE)', async () => {
    const mgr1 = new SessionManager({});
    // EmbeddedServer treats `0 || DEFAULT_SERVER_PORT` as DEFAULT, not ephemeral,
    // so we explicitly grab a free port to keep this test isolated from any
    // standalone clawo-serve that may be running on the default port.
    const ephemeral = await freePort();
    const s1 = new EmbeddedServer(mgr1, ephemeral);
    const port = await s1.start();
    expect(port).toBeGreaterThan(0);

    const tokenPath = path.join(os.homedir(), '.openclaw', 'server-token');
    const winnerToken = fs.readFileSync(tokenPath, 'utf-8').trim();

    // Second instance forced onto the same port → must hit EADDRINUSE and skip
    // WITHOUT touching the token file the winner wrote.
    const mgr2 = new SessionManager({});
    const s2 = new EmbeddedServer(mgr2, port);
    const port2 = await s2.start();
    expect(port2).toBe(0);

    const afterToken = fs.readFileSync(tokenPath, 'utf-8').trim();
    expect(afterToken).toBe(winnerToken);

    await s1.stop();
    await mgr1.shutdown();
    await mgr2.shutdown();
  });

  it('reuses the on-disk token across restarts so the browser cookie stays valid', async () => {
    const mgr1 = new SessionManager({});
    const ephemeral = await freePort();
    const s1 = new EmbeddedServer(mgr1, ephemeral);
    const port1 = await s1.start();
    expect(port1).toBeGreaterThan(0);

    const tokenPath = path.join(os.homedir(), '.openclaw', 'server-token');
    const firstToken = fs.readFileSync(tokenPath, 'utf-8').trim();
    expect(firstToken).toMatch(/^[0-9a-fA-F]{32,}$/);

    // Stop the first server. The new contract is: stop does NOT remove the
    // token file. The token persists so the next server can pick it up.
    await s1.stop();
    expect(fs.existsSync(tokenPath)).toBe(true);
    const tokenAfterStop = fs.readFileSync(tokenPath, 'utf-8').trim();
    expect(tokenAfterStop).toBe(firstToken);

    // Start a fresh server on a (probably different) free port. It must
    // adopt the persisted token instead of rotating.
    const ephemeral2 = await freePort();
    const mgr2 = new SessionManager({});
    const s2 = new EmbeddedServer(mgr2, ephemeral2);
    const port2 = await s2.start();
    expect(port2).toBeGreaterThan(0);

    const secondToken = fs.readFileSync(tokenPath, 'utf-8').trim();
    expect(secondToken).toBe(firstToken);

    await s2.stop();
    await mgr1.shutdown();
    await mgr2.shutdown();
  });
});

describe('POST /council/new', () => {
  let manager: SessionManager;
  let server: EmbeddedServer;
  let port: number;
  let token: string;

  beforeAll(async () => {
    manager = new SessionManager({});
    // councilStart spawns real Claude subprocesses — stub it for the unit test.
    vi.spyOn(manager, 'councilStart').mockImplementation(
      (task: string): CouncilSession => ({
        id: 'fake-council-id-001',
        task,
        status: 'running',
        startTime: '2026-05-13T05:00:00.000Z',
        responses: [],
        config: { agents: [], maxRounds: 0, projectDir: '/tmp' },
      }),
    );
    const ephemeral = await freePort();
    server = new EmbeddedServer(manager, ephemeral);
    port = await server.start();
    token = fs.readFileSync(path.join(os.homedir(), '.openclaw', 'server-token'), 'utf-8').trim();
  });
  afterAll(async () => {
    await server.stop();
    await manager.shutdown();
  });

  it('starts a council and returns its id', async () => {
    const r = await fetch(`http://127.0.0.1:${port}/council/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ task: 'test task', projectDir: '/tmp' }),
    });
    expect(r.status).toBe(200);
    const j = (await r.json()) as { ok: boolean; id: string; status: string };
    expect(j.ok).toBe(true);
    expect(j.id).toBe('fake-council-id-001');
    expect(j.status).toBe('running');
    expect(manager.councilStart).toHaveBeenCalledOnce();
  });

  it('returns 400 when task is missing', async () => {
    const r = await fetch(`http://127.0.0.1:${port}/council/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ projectDir: '/tmp' }),
    });
    expect(r.status).toBe(400);
  });

  it('returns 400 when projectDir is missing', async () => {
    const r = await fetch(`http://127.0.0.1:${port}/council/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ task: 'just a task' }),
    });
    expect(r.status).toBe(400);
  });
});

describe('POST /autoloop/new', () => {
  let manager: SessionManager;
  let server: EmbeddedServer;
  let port: number;
  let token: string;

  beforeAll(async () => {
    manager = new SessionManager({});
    // autoloopStart kicks off Claude subprocesses — stub for unit tests.
    vi.spyOn(manager, 'autoloopStart').mockImplementation(async (opts) => ({
      runId: opts.runId,
      plannerSession: `planner-${opts.runId}`,
      state: {
        run_id: opts.runId,
        status: 'planning',
        iter: 0,
        subagents_spawned: false,
        started_at: '2026-05-13T05:00:00.000Z',
        workspace: opts.workspace,
        ledger_dir: path.join(opts.workspace, 'tasks', opts.runId),
        push_log_count: 0,
        status_reason: null,
        consecutive_phase_errors: 0,
        recent_phase_errors: [],
        metric_history: [],
        last_activity_at: 0,
      },
    }));
    const ephemeral = await freePort();
    server = new EmbeddedServer(manager, ephemeral);
    port = await server.start();
    token = fs.readFileSync(path.join(os.homedir(), '.openclaw', 'server-token'), 'utf-8').trim();
  });
  afterAll(async () => {
    await server.stop();
    await manager.shutdown();
  });

  it('starts an autoloop and returns a server-generated run_id', async () => {
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ workspace: '/tmp' }),
    });
    expect(r.status).toBe(200);
    const j = (await r.json()) as { ok: boolean; run_id: string };
    expect(j.ok).toBe(true);
    expect(j.run_id).toMatch(/^auto-\d+-[a-f0-9]+$/);
  });

  it('honors an explicit well-shaped run_id', async () => {
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ workspace: '/tmp', run_id: 'my-custom-id' }),
    });
    expect(r.status).toBe(200);
    const j = (await r.json()) as { run_id: string };
    expect(j.run_id).toBe('my-custom-id');
  });

  it('passes independent role engines and models to autoloopStart', async () => {
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({
        workspace: '/tmp',
        run_id: 'multi-engine-http',
        planner_engine: 'codex',
        planner_model: 'gpt-planner',
        coder_engine: 'opencode',
        coder_model: 'anthropic/claude-sonnet-5',
        reviewer_engine: 'gemini',
        reviewer_model: 'gemini-reviewer',
      }),
    });

    expect(r.status).toBe(200);
    expect(manager.autoloopStart).toHaveBeenLastCalledWith({
      runId: 'multi-engine-http',
      workspace: fs.realpathSync('/tmp'),
      plannerEngine: 'codex',
      plannerModel: 'gpt-planner',
      coderEngine: 'opencode',
      coderModel: 'anthropic/claude-sonnet-5',
      reviewerEngine: 'gemini',
      reviewerModel: 'gemini-reviewer',
      sendTimeoutMs: undefined,
      activityLeaseMs: undefined,
      autoloopHardTimeoutMs: undefined,
    });
  });

  it('accepts inclusive timeout boundaries and maps every wire field exactly once', async () => {
    for (const [suffix, timeouts] of [
      ['minimums', { send_timeout_ms: 5_000, activity_lease_ms: 60_000, autoloop_hard_timeout_ms: 600_000 }],
      ['maximums', { send_timeout_ms: 7_200_000, activity_lease_ms: 7_200_000, autoloop_hard_timeout_ms: 259_200_000 }],
    ] as const) {
      const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ workspace: '/tmp', run_id: `timeouts-${suffix}`, ...timeouts }),
      });

      expect(r.status).toBe(200);
      expect(manager.autoloopStart).toHaveBeenLastCalledWith(
        expect.objectContaining({
          runId: `timeouts-${suffix}`,
          sendTimeoutMs: timeouts.send_timeout_ms,
          activityLeaseMs: timeouts.activity_lease_ms,
          autoloopHardTimeoutMs: timeouts.autoloop_hard_timeout_ms,
        }),
      );
    }
  });

  it('rejects malformed and out-of-range start timeouts without invoking autoloopStart', async () => {
    const start = vi.mocked(manager.autoloopStart);
    for (const body of [
      JSON.stringify({ workspace: '/tmp', send_timeout_ms: 4_999 }),
      JSON.stringify({ workspace: '/tmp', send_timeout_ms: 7_200_001 }),
      JSON.stringify({ workspace: '/tmp', send_timeout_ms: '600000' }),
      JSON.stringify({ workspace: '/tmp', activity_lease_ms: 59_999 }),
      JSON.stringify({ workspace: '/tmp', activity_lease_ms: 7_200_001 }),
      JSON.stringify({ workspace: '/tmp', activity_lease_ms: null }),
      JSON.stringify({ workspace: '/tmp', autoloop_hard_timeout_ms: 599_999 }),
      JSON.stringify({ workspace: '/tmp', autoloop_hard_timeout_ms: 259_200_001 }),
      '{"workspace":"/tmp","autoloop_hard_timeout_ms":1e999}',
    ]) {
      start.mockClear();
      const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body,
      });
      expect(r.status).toBe(400);
      expect(start).not.toHaveBeenCalled();
    }
  });

  // A custom engine names an executable to spawn. This HTTP surface is routinely
  // reverse-tunnelled to a public hostname and its token is a monitoring
  // credential, so accepting one from the request body would turn any dashboard
  // session into remote code execution. Built-in engines stay selectable.
  it('refuses a custom engine supplied over HTTP instead of spawning its binary', async () => {
    vi.mocked(manager.autoloopStart).mockClear();
    for (const key of ['planner_custom_engine', 'coder_custom_engine', 'reviewer_custom_engine']) {
      const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          workspace: '/tmp',
          run_id: `rce-${key}`,
          [`${key.split('_')[0]}_engine`]: 'custom',
          [key]: { name: 'pwn', bin: '/bin/sh', args: { extra: ['-c', 'echo owned'] } },
        }),
      });
      expect(r.status).toBe(400);
      const payload = (await r.json()) as { ok: boolean; error: string };
      expect(payload.ok).toBe(false);
      expect(payload.error).toContain('may not be given as an inline config over HTTP');
      expect(manager.autoloopStart).not.toHaveBeenCalled();
    }
  });

  it('returns 400 when autoloop role configuration is invalid', async () => {
    vi.mocked(manager.autoloopStart).mockRejectedValueOnce(new Error("Planner engine 'not-real' is not supported"));
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ workspace: '/tmp', planner_engine: 'not-real' }),
    });
    expect(r.status).toBe(400);
  });

  it('returns 409 when an Autoloop reserved session name is already active', async () => {
    vi.mocked(manager.autoloopStart).mockRejectedValueOnce(
      new Error("Autoloop session name 'autoloop-conflict-planner' is already in use"),
    );
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ workspace: '/tmp', run_id: 'conflict' }),
    });
    expect(r.status).toBe(409);
  });

  it('rejects a malformed run_id (server-generates instead)', async () => {
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ workspace: '/tmp', run_id: 'has spaces and !@#$' }),
    });
    expect(r.status).toBe(200);
    const j = (await r.json()) as { run_id: string };
    expect(j.run_id).toMatch(/^auto-\d+-[a-f0-9]+$/);
  });

  it('returns 400 when workspace is missing', async () => {
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/new`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: '{}',
    });
    expect(r.status).toBe(400);
  });
});

describe('POST /autoloop/:id/chat', () => {
  let manager: SessionManager;
  let server: EmbeddedServer;
  let port: number;
  let token: string;

  beforeAll(async () => {
    manager = new SessionManager({});
    const ephemeral = await freePort();
    server = new EmbeddedServer(manager, ephemeral);
    port = await server.start();
    token = fs.readFileSync(path.join(os.homedir(), '.openclaw', 'server-token'), 'utf-8').trim();
  });
  afterAll(async () => {
    await server.stop();
    await manager.shutdown();
  });

  it('returns 202 immediately (fire-and-forget) and dispatches autoloopChat in the background', async () => {
    // Pretend the run is alive in memory so the handler clears its 404 gate.
    vi.spyOn(manager, 'getAutoloop').mockReturnValue({
      runner: {} as never,
      dispatcher: {} as never,
    });
    // autoloopChat may take a long time — simulate by never resolving within
    // the test window. The handler must NOT await it.
    const slow = new Promise<{ reply: string }>(() => {
      /* never resolves */
    });
    const spy = vi.spyOn(manager, 'autoloopChat').mockReturnValue(slow);

    const r = await fetch(`http://127.0.0.1:${port}/autoloop/run-xyz/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ text: 'hello' }),
    });
    expect(r.status).toBe(202);
    const j = (await r.json()) as { ok: boolean; queued: boolean };
    expect(j.ok).toBe(true);
    expect(j.queued).toBe(true);
    expect(spy).toHaveBeenCalledWith('run-xyz', 'hello');
  });

  it('serializes every chat response envelope without inherited toJSON and dispatches an accepted chat once', async () => {
    // Production mutation caught: routing any chat response through the
    // route-local JSON.stringify helper lets Object.prototype.toJSON forge the
    // public envelope even though the accepted chat has already dispatched.
    const acceptedRunId = 'chat-envelope-accepted';
    const missingRunId = 'chat-envelope-missing';
    const get = vi
      .spyOn(manager, 'getAutoloop')
      .mockImplementation((runId) =>
        runId === acceptedRunId
          ? ({ runner: {} as never, dispatcher: {} as never } as ReturnType<SessionManager['getAutoloop']>)
          : undefined,
      );
    const chat = vi.spyOn(manager, 'autoloopChat').mockResolvedValue({ reply: 'accepted once' });
    const warmup = await fetch(`http://127.0.0.1:${port}/health`);
    expect(warmup.status).toBe(200);
    await warmup.arrayBuffer();
    const prior = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    let inheritedHookCalls = 0;
    const responses: Array<{ status: number; body: string }> = [];

    try {
      Object.defineProperty(Object.prototype, 'toJSON', {
        configurable: true,
        value() {
          inheritedHookCalls += 1;
          return { forged_chat_envelope: true };
        },
      });

      for (const request of [
        { runId: acceptedRunId, body: '{"text":"accept this once"}' },
        { runId: acceptedRunId, body: '{"text":"   "}' },
        { runId: missingRunId, body: '{"text":"missing run"}' },
      ]) {
        const response = await fetch(`http://127.0.0.1:${port}/autoloop/${request.runId}/chat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
          body: request.body,
        });
        responses.push({ status: response.status, body: await response.text() });
      }
    } finally {
      if (prior) Object.defineProperty(Object.prototype, 'toJSON', prior);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
    }

    try {
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(inheritedHookCalls).toBe(0);
      expect(responses).toEqual([
        { status: 202, body: '{"ok":true,"queued":true}' },
        { status: 400, body: '{"ok":false,"error":"text (non-empty string) required"}' },
        { status: 404, body: `{"ok":false,"error":"Autoloop run '${missingRunId}' not found"}` },
      ]);
      expect(chat).toHaveBeenCalledTimes(1);
      expect(chat).toHaveBeenCalledWith(acceptedRunId, 'accept this once');
    } finally {
      chat.mockRestore();
      get.mockRestore();
    }
  });

  it('records and streams one structured typed failure after the 202 response without replaying chat', async () => {
    // Production mutation caught: restoring the fire-and-forget console.warn
    // catch would leave no durable phase-error/status evidence and no typed SSE
    // result after the already-accepted HTTP request.
    const runId = 'detached-typed-failure';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const dispatcherEvents = new EventEmitter();
    const runner = new AutoloopRunner({
      run_id: runId,
      workspace,
      ledger_dir: ledger.directory,
      dispatcher: { deliver: async () => [] },
      notifyUser: async () => {},
    });
    await runner.start();
    const dispatcher = Object.assign(dispatcherEvents, { secureLedgerCapability: ledger });
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue({ runner, dispatcher } as never);
    const chat = vi
      .spyOn(manager, 'autoloopChat')
      .mockRejectedValueOnce(new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'detached Planner failed'));
    const controller = new AbortController();

    try {
      const eventsResponse = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
        headers: { Authorization: `Bearer ${token}` },
        signal: controller.signal,
      });
      expect(eventsResponse.status).toBe(200);
      const reader = eventsResponse.body!.getReader();
      const decoder = new TextDecoder();
      let stream = '';
      const publicFailure = (async (): Promise<unknown> => {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) throw new Error('SSE stream ended before autoloop_failure');
          stream += decoder.decode(value, { stream: true });
          const frames = stream.split('\n\n');
          stream = frames.pop() ?? '';
          for (const frame of frames) {
            const event = frame.match(/^event: (.+)$/m)?.[1];
            const data = frame.match(/^data: (.+)$/m)?.[1];
            if (event === 'autoloop_failure' && data) return JSON.parse(data) as unknown;
          }
        }
      })();
      // A RED assertion below may abort the stream before this promise is
      // awaited. Attach a handler immediately so that expected test cleanup
      // never becomes a process-level unhandled rejection.
      void publicFailure.catch(() => undefined);

      const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ text: 'start the detached Planner turn' }),
      });
      expect(response.status).toBe(202);
      await expect(response.json()).resolves.toEqual({ ok: true, queued: true });

      await vi.waitFor(() => {
        expect(runner.state.recent_phase_errors).toEqual([
          expect.objectContaining({
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'detached Planner failed',
          }),
        ]);
      });
      await expect(publicFailure).resolves.toEqual({
        code: 'AUTOLOOP_ENGINE_FAILURE',
        message: 'detached Planner failed',
        retryable: true,
      });

      const decisions = fs
        .readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line) as { kind: string; payload: { code?: string } });
      expect(
        decisions.filter((row) => row.kind === 'phase_error' && row.payload.code === 'AUTOLOOP_ENGINE_FAILURE'),
      ).toHaveLength(1);
      expect(chat).toHaveBeenCalledTimes(1);
    } finally {
      controller.abort();
      runner.stop();
      get.mockRestore();
      chat.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('serializes live Autoloop SSE frames without invoking inherited toJSON', async () => {
    // Production mutation caught: routing live Autoloop SSE through the generic
    // JSON.stringify sender lets Object.prototype.toJSON replace snapshot and
    // state frames before consumers can observe the typed failure boundary.
    const runId = 'live-autoloop-sse-own-data';
    const state = detachedState(runId, '/workspace', '/ledger');
    const expectedState = { ...state };
    const runner = Object.assign(new EventEmitter(), { state });
    const dispatcher = new EventEmitter();
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue({ runner, dispatcher } as never);
    const controller = new AbortController();
    const warmup = await fetch(`http://127.0.0.1:${port}/health`);
    expect(warmup.status).toBe(200);
    await warmup.arrayBuffer();
    const prior = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    const accessorKey = 'hostile_enumerable_accessor';
    const priorOwnAccessor = Object.getOwnPropertyDescriptor(state, accessorKey);
    let inheritedHookCalls = 0;
    let ownAccessorCalls = 0;
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value() {
        inheritedHookCalls += 1;
        return { erased_by_inherited_to_json: true };
      },
    });
    Object.defineProperty(state, accessorKey, {
      configurable: true,
      enumerable: true,
      get() {
        ownAccessorCalls += 1;
        throw new Error('live SSE serializer invoked an own accessor');
      },
    });
    const failure = Object.freeze(
      Object.assign(Object.create(null) as Record<string, unknown>, {
        code: 'AUTOLOOP_ENGINE_FAILURE',
        message: 'live SSE typed failure',
        retryable: true,
      }),
    );

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
        headers: { Authorization: `Bearer ${token}` },
        signal: controller.signal,
      });
      expect(response.status).toBe(200);
      const reader = response.body!.getReader();
      const decoder = new TextDecoder();
      const frames = new Map<string, unknown>();
      let buffered = '';

      runner.emit('state', state);
      runner.emit('autoloop_failure', failure);
      while (frames.size < 3) {
        const { done, value } = await reader.read();
        if (done) throw new Error('live SSE stream ended before all expected frames');
        buffered += decoder.decode(value, { stream: true });
        const chunks = buffered.split('\n\n');
        buffered = chunks.pop() ?? '';
        for (const chunk of chunks) {
          const event = chunk.match(/^event: (.+)$/m)?.[1];
          const data = chunk.match(/^data: (.+)$/m)?.[1];
          if (event && data && ['snapshot', 'state', 'autoloop_failure'].includes(event)) {
            frames.set(event, JSON.parse(data) as unknown);
          }
        }
      }

      expect(inheritedHookCalls).toBe(0);
      expect(ownAccessorCalls).toBe(0);
      expect(frames.get('snapshot')).toEqual({ state: expectedState });
      expect(frames.get('state')).toEqual(expectedState);
      expect(frames.get('autoloop_failure')).toEqual({
        code: 'AUTOLOOP_ENGINE_FAILURE',
        message: 'live SSE typed failure',
        retryable: true,
      });
      await reader.cancel();
    } finally {
      controller.abort();
      if (priorOwnAccessor) Object.defineProperty(state, accessorKey, priorOwnAccessor);
      else delete (state as AutoloopState & Record<string, unknown>)[accessorKey];
      if (prior) Object.defineProperty(Object.prototype, 'toJSON', prior);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      get.mockRestore();
    }
  });

  it('emits deterministic live planner_error frames without invoking hostile error hooks', async () => {
    // Production mutation caught: evaluating err.message/String(err) before
    // entering the guarded SSE serializer lets message accessors and primitive
    // coercion throw out of EventEmitter.emit and prevents a stable frame.
    const runId = 'live-planner-error-own-data';
    const state = detachedState(runId, '/workspace', '/ledger');
    const runner = Object.assign(new EventEmitter(), { state });
    const dispatcher = new EventEmitter();
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue({ runner, dispatcher } as never);
    const controller = new AbortController();
    let ownMessageCalls = 0;
    let inheritedMessageCalls = 0;
    let coercionCalls = 0;
    const ownAccessor = new Error('replace this own message');
    Object.defineProperty(ownAccessor, 'message', {
      configurable: true,
      get() {
        ownMessageCalls += 1;
        throw new Error('own message accessor must not run');
      },
    });
    const inheritedAccessor = new Error();
    const inheritedPrototype = Object.create(Error.prototype) as object;
    Object.defineProperty(inheritedPrototype, 'message', {
      configurable: true,
      get() {
        inheritedMessageCalls += 1;
        throw new Error('inherited message accessor must not run');
      },
    });
    Object.setPrototypeOf(inheritedAccessor, inheritedPrototype);
    const throwingCoercion = Object.create(null) as Record<PropertyKey, unknown>;
    Object.defineProperty(throwingCoercion, Symbol.toPrimitive, {
      value() {
        coercionCalls += 1;
        throw new Error('primitive coercion must not run');
      },
    });

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
        headers: { Authorization: `Bearer ${token}` },
        signal: controller.signal,
      });
      expect(response.status).toBe(200);
      const reader = response.body!.getReader();
      const decoder = new TextDecoder();
      const escaped: boolean[] = [];
      for (const hostile of [ownAccessor, inheritedAccessor, throwingCoercion]) {
        try {
          dispatcher.emit('planner_error', hostile);
          escaped.push(false);
        } catch {
          escaped.push(true);
        }
      }
      dispatcher.emit('planner_error', 'sentinel planner error');

      const messages: string[] = [];
      let buffered = '';
      while (!messages.includes('sentinel planner error')) {
        const { done, value } = await reader.read();
        if (done) throw new Error('live SSE stream ended before the planner_error sentinel');
        buffered += decoder.decode(value, { stream: true });
        const chunks = buffered.split('\n\n');
        buffered = chunks.pop() ?? '';
        for (const chunk of chunks) {
          const event = chunk.match(/^event: (.+)$/m)?.[1];
          const data = chunk.match(/^data: (.+)$/m)?.[1];
          if (event === 'planner_error' && data) {
            const payload = JSON.parse(data) as { message?: unknown };
            if (typeof payload.message === 'string') messages.push(payload.message);
          }
        }
      }

      expect(escaped).toEqual([false, false, false]);
      expect({ ownMessageCalls, inheritedMessageCalls, coercionCalls }).toEqual({
        ownMessageCalls: 0,
        inheritedMessageCalls: 0,
        coercionCalls: 0,
      });
      expect(messages).toEqual(['unknown error', 'unknown error', 'unknown error', 'sentinel planner error']);
      await reader.cancel();
    } finally {
      controller.abort();
      get.mockRestore();
    }
  });

  it('keeps an unknown detached rejection generic after the compatible 202 response', async () => {
    // Production mutation caught: a loose classifier would manufacture an
    // Autoloop code for an ordinary detached Error and emit a typed event.
    const runId = 'detached-unknown';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const runner = Object.assign(new EventEmitter(), {
      state: detachedState(runId, workspace, ledger.directory),
      send: vi.fn(),
    });
    const dispatcher = Object.assign(new EventEmitter(), { secureLedgerCapability: ledger });
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue({ runner, dispatcher } as never);
    const chat = vi.spyOn(manager, 'autoloopChat').mockRejectedValueOnce(new Error('ordinary detached failure'));
    const failures: unknown[] = [];
    runner.on('autoloop_failure', (failure) => failures.push(failure));

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ text: 'keep this unknown' }),
      });
      expect(response.status).toBe(202);
      await expect(response.json()).resolves.toEqual({ ok: true, queued: true });
      await vi.waitFor(() => expect(chat).toHaveBeenCalledTimes(1));
      await vi.waitFor(() => expect(failures).toHaveLength(1));

      expect(failures).toEqual([{ message: 'ordinary detached failure' }]);
      expect(runner.state.recent_phase_errors).toEqual([
        expect.objectContaining({
          agent: 'planner',
          phase: 'planner_turn',
          error: 'ordinary detached failure',
        }),
      ]);
      expect(runner.send).not.toHaveBeenCalled();
    } finally {
      chat.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('reserves detached failure identity synchronously and never renews the runner lease', async () => {
    // Production mutation caught: reserving after persistence or routing via
    // runner.send permits concurrent duplicate rows and renews last_activity_at.
    const runId = 'detached-concurrent-failure';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const runner = Object.assign(new EventEmitter(), {
      state: {
        iter: 3,
        last_activity_at: 1234,
        consecutive_phase_errors: 0,
        recent_phase_errors: [] as unknown[],
      },
      send: vi.fn(() => {
        throw new Error('public send must not be used for bookkeeping');
      }),
    });
    const dispatcher = Object.assign(new EventEmitter(), { secureLedgerCapability: ledger });
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue({ runner, dispatcher } as never);
    const error = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'one logical rejection');
    try {
      const [first, second] = await Promise.all([
        manager.recordDetachedAutoloopChatFailure(runId, error),
        manager.recordDetachedAutoloopChatFailure(runId, error),
      ]);
      expect(first).toEqual(second);
      expect(runner.send).not.toHaveBeenCalled();
      expect(runner.state.last_activity_at).toBe(1234);
      const rows = fs
        .readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .filter((line) => JSON.parse(line).payload?.error === 'one logical rejection');
      expect(rows).toHaveLength(1);
      expect(runner.state.recent_phase_errors).toHaveLength(1);
    } finally {
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it.each(['state', 'phase_error', 'autoloop_failure'] as const)(
    'does not retry detached persistence or recount when a %s listener throws',
    async (eventName) => {
      // Production mutation caught: treating EventEmitter publication failure
      // as recording failure would retry an already-durable logical effect with
      // a second row/id or reject the detached containment promise.
      const runId = `detached-no-listener-retry-${eventName.replace('_', '-')}`;
      const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
      const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
      const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
      const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
      const failure = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', `${eventName} listener failure`);
      let listenerCalls = 0;
      handle.runner.on(eventName, () => {
        listenerCalls += 1;
        throw new Error(`${eventName} observer failed`);
      });

      try {
        const first = await manager.recordDetachedAutoloopChatFailure(runId, failure);
        const firstRows = decisionRows(ledger);
        const firstEntry = handle.runner.state.recent_phase_errors[0] as object;
        const idSymbol = Object.getOwnPropertySymbols(firstEntry).find(
          (candidate) => candidate.description === 'detachedAutoloopFailureId',
        );
        const firstId = idSymbol ? Reflect.get(firstEntry, idSymbol) : undefined;

        const second = await manager.recordDetachedAutoloopChatFailure(runId, failure);
        const finalRows = decisionRows(ledger);
        const finalEntry = handle.runner.state.recent_phase_errors[0] as object;
        const finalId = idSymbol ? Reflect.get(finalEntry, idSymbol) : undefined;

        expect(first).toEqual({
          code: 'AUTOLOOP_ENGINE_FAILURE',
          message: `${eventName} listener failure`,
          retryable: true,
        });
        expect(second).toEqual(first);
        expect(listenerCalls).toBe(1);
        expect(firstRows).toHaveLength(1);
        expect(finalRows).toHaveLength(1);
        expect((finalRows[0].payload as { detached_failure_id?: unknown }).detached_failure_id).toBe(firstId);
        expect(finalId).toBe(firstId);
        expect(handle.runner.state.recent_phase_errors).toHaveLength(1);
        expect(handle.runner.state.consecutive_phase_errors).toBe(1);
      } finally {
        get.mockRestore();
        fs.rmSync(workspace, { recursive: true, force: true });
      }
    },
  );

  it('contains hostile listener inspection and warning failures after the detached effect is durable', async () => {
    // Production mutation caught: descriptor inspection or logger.warn inside
    // a publication catch can throw after append/state mutation, rejecting the
    // recorder and inviting a duplicate retry of an already-durable effect.
    const runId = 'detached-hostile-listener-diagnostics';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const warn = vi.fn(() => {
      throw new Error('warning sink failed');
    });
    const containedManager = new SessionManager({}, { debug: () => {}, info: () => {}, warn, error: () => {} });
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const get = vi.spyOn(containedManager, 'getAutoloop').mockReturnValue(handle as never);
    const failure = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'durable before diagnostics');
    const hostileThrown = new Proxy(new Error('hostile listener error'), {
      getOwnPropertyDescriptor() {
        throw new Error('listener descriptor trap failed');
      },
    });
    let listenerCalls = 0;
    handle.runner.on('autoloop_failure', () => {
      listenerCalls += 1;
      throw hostileThrown;
    });

    try {
      let first: Awaited<ReturnType<SessionManager['recordDetachedAutoloopChatFailure']>> | undefined;
      let firstRejected = false;
      try {
        first = await containedManager.recordDetachedAutoloopChatFailure(runId, failure);
      } catch {
        firstRejected = true;
      }
      const firstRows = decisionRows(ledger);
      const firstEntry = handle.runner.state.recent_phase_errors[0] as object;
      const idSymbol = Object.getOwnPropertySymbols(firstEntry).find(
        (candidate) => candidate.description === 'detachedAutoloopFailureId',
      );
      const firstId = idSymbol ? Reflect.get(firstEntry, idSymbol) : undefined;

      const second = await containedManager.recordDetachedAutoloopChatFailure(runId, failure);
      const finalRows = decisionRows(ledger);
      const finalEntry = handle.runner.state.recent_phase_errors[0] as object;

      expect(firstRejected).toBe(false);
      expect(first).toEqual({
        code: 'AUTOLOOP_ENGINE_FAILURE',
        message: 'durable before diagnostics',
        retryable: true,
      });
      expect(second).toEqual(first);
      expect(listenerCalls).toBe(1);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(firstRows).toHaveLength(1);
      expect(finalRows).toHaveLength(1);
      expect((finalRows[0].payload as { detached_failure_id?: unknown }).detached_failure_id).toBe(firstId);
      expect(idSymbol ? Reflect.get(finalEntry, idSymbol) : undefined).toBe(firstId);
      expect(handle.runner.state.recent_phase_errors).toHaveLength(1);
      expect(handle.runner.state.consecutive_phase_errors).toBe(1);
    } finally {
      get.mockRestore();
      await containedManager.shutdown();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('coalesces re-entry from the persistence seam across distinct wrappers for one logical failure', async () => {
    // Production mutation caught: object-identity-only dedup lets an equivalent
    // wrapper re-enter appendFlatFile and produce a second row/state entry.
    const runId = 'detached-reentrant-failure';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const originalAppend = ledger.appendFlatFile.bind(ledger);
    let entered = false;
    let reentered: Promise<unknown> | undefined;
    const append = vi.spyOn(ledger, 'appendFlatFile').mockImplementation((file, data, flush) => {
      if (!entered) {
        entered = true;
        reentered = manager.recordDetachedAutoloopChatFailure(
          runId,
          new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'same logical re-entrant failure'),
        );
      }
      return originalAppend(file, data, flush);
    });

    try {
      const first = manager.recordDetachedAutoloopChatFailure(
        runId,
        new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'same logical re-entrant failure'),
      );
      await Promise.resolve();
      expect(reentered).toBeDefined();
      await Promise.all([first, reentered!]);

      expect(
        decisionRows(ledger).filter(
          (row) => (row.payload as { error?: unknown })?.error === 'same logical re-entrant failure',
        ),
      ).toHaveLength(1);
      expect(handle.runner.state.recent_phase_errors).toHaveLength(1);
    } finally {
      append.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('coalesces distinct wrappers bound by one real Planner message id', async () => {
    // Production mutation caught: binding only the wrapper rethrown by
    // autoloopChat leaves its same-turn cause on the shape fallback, so a later
    // recording of that cause appends the same logical rejection twice.
    const runId = 'detached-one-message-two-wrappers';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const inner = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'same message rejection');
    const outer = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'same message rejection', { cause: inner });
    handle.runner.send.mockRejectedValueOnce(outer);

    try {
      await expect(manager.autoloopChat(runId, 'bind this message')).rejects.toBe(outer);
      const sent = handle.runner.send.mock.calls[0]?.[0] as { msg_id?: unknown } | undefined;
      expect(sent?.msg_id).toEqual(expect.any(String));

      await manager.recordDetachedAutoloopChatFailure(runId, outer);
      await manager.recordDetachedAutoloopChatFailure(runId, inner);

      expect(
        decisionRows(ledger).filter((row) => (row.payload as { error?: unknown })?.error === 'same message rejection'),
      ).toHaveLength(1);
      expect(handle.runner.state.recent_phase_errors).toHaveLength(1);
    } finally {
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('preserves a committed own-data cause through detached HTTP, causal retagging, state routes, and SSE', async () => {
    // Production mutation caught: dropping `committed` while surfacing,
    // persisting, retagging, or serializing a Runner-projected secure-ledger
    // outcome makes already-committed bytes look safe to retry.
    const runId = 'detached-committed-own-data-cause';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const handle = fakeDetachedHandle(ledger, state);
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const committed = new SecureAutoloopLedgerCommitError(
      'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
      'Planner decision committed before file sync failed',
      { cause: new Error('fsync failed after commit') },
    );
    const wrapper = new Error('Planner delivery wrapper', { cause: committed });
    const publicFailures: unknown[] = [];
    handle.runner.on('autoloop_failure', (failure) => publicFailures.push(failure));
    handle.runner.send.mockImplementationOnce(async () => {
      state.consecutive_phase_errors = 1;
      state.recent_phase_errors.push(
        Object.freeze({
          ts: '2026-09-07T08:01:00.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
          committed: true as const,
          retryable: false as const,
          error: 'Planner decision committed before file sync failed',
        }),
      );
      throw wrapper;
    });
    const record = vi.spyOn(manager, 'recordDetachedAutoloopChatFailure');

    try {
      const accepted = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ text: 'surface the committed outcome' }),
      });
      expect(accepted.status).toBe(202);
      await vi.waitFor(() => expect(decisionRows(ledger)).toHaveLength(1));

      const messageId = (handle.runner.send.mock.calls[0]?.[0] as { msg_id?: unknown } | undefined)?.msg_id;
      expect(messageId).toEqual(expect.any(String));
      expect(record.mock.calls[0]?.[1]).toBe(committed);

      await manager.recordDetachedAutoloopChatFailure(runId, wrapper);

      const rows = decisionRows(ledger);
      const stateEntry = handle.runner.state.recent_phase_errors[0];
      const stateIdSymbol = Object.getOwnPropertySymbols(stateEntry as object).find(
        (candidate) => candidate.description === 'detachedAutoloopFailureId',
      );
      expect(record.mock.calls.slice(0, 2).map((call) => call[1])).toEqual([committed, wrapper]);
      expect(rows).toEqual([
        expect.objectContaining({
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
            committed: true,
            retryable: false,
            error: 'Planner decision committed before file sync failed',
            detached_failure_id: messageId,
          },
        }),
      ]);
      expect(handle.runner.state.recent_phase_errors).toHaveLength(1);
      expect(stateEntry).toEqual({
        ts: '2026-09-07T08:01:00.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
        error: 'Planner decision committed before file sync failed',
      });
      expect(stateIdSymbol ? Reflect.get(stateEntry as object, stateIdSymbol) : undefined).toBe(messageId);
      expect(handle.runner.state.consecutive_phase_errors).toBe(1);
      expect(publicFailures).toEqual([
        {
          code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
          message: 'Planner decision committed before file sync failed',
          committed: true,
          retryable: false,
        },
      ]);

      const status = vi.spyOn(manager, 'autoloopStatus').mockReturnValue(state);
      const list = vi.spyOn(manager, 'autoloopList').mockReturnValue([state]);
      const resume = vi.spyOn(manager, 'autoloopResume').mockResolvedValue(state);
      get.mockReturnValue(undefined);
      try {
        const stateResponse = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/state`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        const listResponse = await fetch(`http://127.0.0.1:${port}/autoloop/list`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        const resumeResponse = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/resume`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
          body: '{}',
        });
        const historicalState = await historicalAutoloopSnapshot(port, token, runId);
        for (const observed of [
          ((await stateResponse.json()) as { state: AutoloopState }).state,
          ((await listResponse.json()) as { runs: AutoloopState[] }).runs[0],
          ((await resumeResponse.json()) as { state: AutoloopState }).state,
          historicalState.state,
        ]) {
          expect(observed.recent_phase_errors).toEqual([
            expect.objectContaining({
              code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
              committed: true,
              retryable: false,
              error: 'Planner decision committed before file sync failed',
            }),
          ]);
        }
      } finally {
        status.mockRestore();
        list.mockRestore();
        resume.mockRestore();
      }
    } finally {
      record.mockRestore();
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it.each(COMMITTED_LEDGER_CODES)(
    'preserves inaccessible-message committed code %s through durable, live SSE, HTTP, and cold egress',
    async (code) => {
      // Production mutation caught: degrading any recognized secure-ledger
      // instance to an unknown failure when its own message is an accessor loses
      // committed=true in durable/live output or during cold HTTP/SSE recovery.
      const runId = `detached-committed-message-accessor-${code.toLowerCase()}`;
      const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
      const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
      const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
      const guard = stageStoredAutoloopRun(
        runId,
        workspace,
        ledger.directory,
        detachedState(runId, workspace, ledger.directory),
        '2026-09-07T08:05:00.000Z',
        '',
      );
      releaseLease(guard);
      const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
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
      const chat = vi.spyOn(manager, 'autoloopChat').mockRejectedValueOnce(failure);
      const publicFailures: unknown[] = [];
      handle.runner.on('autoloop_failure', (publicFailure) => publicFailures.push(publicFailure));
      const controller = new AbortController();

      try {
        const eventsResponse = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
          headers: { Authorization: `Bearer ${token}` },
          signal: controller.signal,
        });
        expect(eventsResponse.status).toBe(200);
        const reader = eventsResponse.body!.getReader();
        const decoder = new TextDecoder();
        let stream = '';
        const streamedFailure = (async (): Promise<unknown> => {
          for (;;) {
            const { done, value } = await reader.read();
            if (done) throw new Error('SSE stream ended before committed autoloop_failure');
            stream += decoder.decode(value, { stream: true });
            const frames = stream.split('\n\n');
            stream = frames.pop() ?? '';
            for (const frame of frames) {
              const event = frame.match(/^event: (.+)$/m)?.[1];
              const data = frame.match(/^data: (.+)$/m)?.[1];
              if (event === 'autoloop_failure' && data) return JSON.parse(data) as unknown;
            }
          }
        })();
        void streamedFailure.catch(() => undefined);

        const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/chat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
          body: JSON.stringify({ text: 'record the already-committed outcome' }),
        });
        expect(response.status).toBe(202);
        await expect(response.json()).resolves.toEqual({ ok: true, queued: true });
        await vi.waitFor(() => expect(decisionRows(ledger)).toHaveLength(1));

        const expectedPublicFailure = {
          code,
          message: 'unknown error',
          committed: true,
          retryable: false,
        };
        expect(getterCalls).toBe(0);
        expect(publicFailures).toEqual([expectedPublicFailure]);
        await expect(streamedFailure).resolves.toEqual(expectedPublicFailure);
        expect(decisionRows(ledger)[0]).toMatchObject({
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: 'planner',
            phase: 'planner_turn',
            code,
            committed: true,
            retryable: false,
            error: 'unknown error',
            detached_failure_id: expect.any(String),
          },
        });
        expect(handle.runner.state.recent_phase_errors).toEqual([
          expect.objectContaining({
            code,
            committed: true,
            retryable: false,
            error: 'unknown error',
          }),
        ]);
        expect(handle.runner.state.consecutive_phase_errors).toBe(1);

        controller.abort();
        get.mockReturnValue(undefined);
        const coldState = manager.autoloopStatus(runId);
        expect(coldState?.consecutive_phase_errors).toBe(1);
        expect(coldState?.recent_phase_errors).toEqual([
          expect.objectContaining({ code, committed: true, retryable: false, error: 'unknown error' }),
        ]);

        const stateResponse = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/state`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        expect(stateResponse.status).toBe(200);
        const stateBody = (await stateResponse.json()) as { state: AutoloopState };
        expect(stateBody.state.recent_phase_errors).toEqual([
          expect.objectContaining({ code, committed: true, retryable: false, error: 'unknown error' }),
        ]);

        const historicalState = (await historicalAutoloopSnapshot(port, token, runId)).state;
        expect(historicalState.consecutive_phase_errors).toBe(1);
        expect(historicalState.recent_phase_errors).toEqual([
          expect.objectContaining({ code, committed: true, retryable: false, error: 'unknown error' }),
        ]);
        expect(getterCalls).toBe(0);
      } finally {
        controller.abort();
        chat.mockRestore();
        get.mockRestore();
        fs.rmSync(workspace, { recursive: true, force: true });
      }
    },
  );

  it('ignores an inherited data-property committed cause instead of surfacing it', async () => {
    // Production mutation caught: accepting inherited `cause` data would let a
    // prototype replace the actual Planner rejection with a committed outcome.
    const runId = 'detached-inherited-committed-cause';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const live = vi
      .spyOn(
        manager as unknown as { _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle> },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const inheritedCause = new SecureAutoloopLedgerCommitError(
      'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
      'prototype-forged committed failure',
      { cause: new Error('forged cause') },
    );
    const wrapper = new Error('actual Planner wrapper failure');
    const inheritedPrototype = Object.create(Object.getPrototypeOf(wrapper)) as object;
    Object.defineProperty(inheritedPrototype, 'cause', { value: inheritedCause });
    Object.setPrototypeOf(wrapper, inheritedPrototype);
    handle.runner.send.mockRejectedValueOnce(wrapper);

    try {
      await expect(manager.autoloopChat(runId, 'ignore inherited cause')).rejects.toBe(wrapper);
      await manager.recordDetachedAutoloopChatFailure(runId, wrapper);
      expect(decisionRows(ledger)).toEqual([
        expect.objectContaining({
          payload: expect.objectContaining({ error: 'actual Planner wrapper failure' }),
        }),
      ]);
      expect(decisionRows(ledger)[0].payload).not.toHaveProperty('code');
      expect(decisionRows(ledger)[0].payload).not.toHaveProperty('committed');
    } finally {
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('binds failures without invoking own or inherited cause accessors', async () => {
    // Production mutation caught: reading error.cause normally before binding
    // lets an accessor replace the rejection and strands the original failure
    // on lossy equal-shape fallback identity instead of its Planner message id.
    const runId = 'detached-hostile-cause-accessors';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = { ...detachedState(runId, workspace, ledger.directory), status: 'running' as const };
    const handle = fakeDetachedHandle(ledger, state);
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const failures = [
      new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'hostile cause equal-shape failure'),
      new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'hostile cause equal-shape failure'),
    ];
    let causeAccessorCalls = 0;
    Object.defineProperty(failures[0], 'cause', {
      configurable: true,
      get() {
        causeAccessorCalls += 1;
        throw new Error('own cause accessor invoked');
      },
    });
    const inheritedPrototype = Object.create(Object.getPrototypeOf(failures[1])) as object;
    Object.defineProperty(inheritedPrototype, 'cause', {
      configurable: true,
      get() {
        causeAccessorCalls += 1;
        throw new Error('inherited cause accessor invoked');
      },
    });
    Object.setPrototypeOf(failures[1], inheritedPrototype);
    handle.runner.send.mockRejectedValueOnce(failures[0]).mockRejectedValueOnce(failures[1]);
    const phaseErrors: unknown[] = [];
    const publicFailures: unknown[] = [];
    handle.runner.on('phase_error', (failure) => phaseErrors.push(failure));
    handle.runner.on('autoloop_failure', (failure) => publicFailures.push(failure));

    try {
      const outcomes: unknown[] = [];
      for (const text of ['own accessor chat', 'inherited accessor chat']) {
        try {
          await manager.autoloopChat(runId, text);
        } catch (error) {
          outcomes.push(error);
        }
      }
      const messageIds = handle.runner.send.mock.calls.map(([message]) => (message as { msg_id?: unknown }).msg_id);
      expect(causeAccessorCalls).toBe(0);
      expect(outcomes).toEqual(failures);
      expect(messageIds).toEqual([expect.any(String), expect.any(String)]);
      expect(messageIds[0]).not.toBe(messageIds[1]);

      await manager.recordDetachedAutoloopChatFailure(runId, failures[0]);
      await manager.recordDetachedAutoloopChatFailure(runId, failures[1]);
      const rows = decisionRows(ledger).filter(
        (row) => (row.payload as { error?: unknown })?.error === 'hostile cause equal-shape failure',
      );
      const stateIds = handle.runner.state.recent_phase_errors.map((entry) => {
        const symbol = Object.getOwnPropertySymbols(entry as object).find(
          (candidate) => candidate.description === 'detachedAutoloopFailureId',
        );
        return symbol ? Reflect.get(entry as object, symbol) : undefined;
      });
      expect(rows.map((row) => (row.payload as { detached_failure_id?: unknown }).detached_failure_id)).toEqual(
        messageIds,
      );
      expect(stateIds).toEqual(messageIds);
      expect(handle.runner.state.consecutive_phase_errors).toBe(2);
      expect(phaseErrors).toHaveLength(2);
      expect(publicFailures).toHaveLength(2);

      await manager.recordDetachedAutoloopChatFailure(runId, failures[0]);
      await manager.recordDetachedAutoloopChatFailure(runId, failures[1]);
      expect({
        rowCount: decisionRows(ledger).filter(
          (row) => (row.payload as { error?: unknown })?.error === 'hostile cause equal-shape failure',
        ).length,
        recentCount: handle.runner.state.recent_phase_errors.length,
        consecutive: handle.runner.state.consecutive_phase_errors,
        phaseErrorCount: phaseErrors.length,
        publicFailureCount: publicFailures.length,
      }).toEqual({
        rowCount: 2,
        recentCount: 2,
        consecutive: 2,
        phaseErrorCount: 2,
        publicFailureCount: 2,
      });
    } finally {
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('records identical public failures from different real Planner message ids independently', async () => {
    // Production mutation caught: using public code/text as the durable chat
    // identity coalesces two intentional Planner chats that happen to reject
    // with the same public failure.
    const runId = 'detached-two-identical-messages';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const first = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'repeatable public failure');
    const second = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'repeatable public failure');
    handle.runner.send.mockRejectedValueOnce(first).mockRejectedValueOnce(second);

    try {
      await expect(manager.autoloopChat(runId, 'first logical chat')).rejects.toBe(first);
      await manager.recordDetachedAutoloopChatFailure(runId, first);
      await expect(manager.autoloopChat(runId, 'second logical chat')).rejects.toBe(second);
      await manager.recordDetachedAutoloopChatFailure(runId, second);

      const sentIds = handle.runner.send.mock.calls.map(([message]) => (message as { msg_id?: unknown }).msg_id);
      expect(sentIds).toHaveLength(2);
      expect(sentIds[0]).not.toBe(sentIds[1]);
      expect(
        decisionRows(ledger).filter(
          (row) => (row.payload as { error?: unknown })?.error === 'repeatable public failure',
        ),
      ).toHaveLength(2);
      expect(handle.runner.state.recent_phase_errors).toHaveLength(2);
    } finally {
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('releases a failed append reservation so the same logical rejection can be durably retried', async () => {
    // Production mutation caught: a permanent success WeakSet suppresses the
    // retry even though the first durable append never happened.
    const runId = 'detached-append-retry';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const error = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'retry after failed append');
    const originalAppend = ledger.appendFlatFile.bind(ledger);
    const append = vi
      .spyOn(ledger, 'appendFlatFile')
      .mockImplementationOnce(() => {
        throw new Error('injected append failure');
      })
      .mockImplementation((file, data, flush) => originalAppend(file, data, flush));

    try {
      let firstRejected = false;
      try {
        await manager.recordDetachedAutoloopChatFailure(runId, error);
      } catch {
        firstRejected = true;
      }
      await manager.recordDetachedAutoloopChatFailure(runId, error);

      expect(
        decisionRows(ledger).filter(
          (row) => (row.payload as { error?: unknown })?.error === 'retry after failed append',
        ),
      ).toHaveLength(1);
      expect(handle.runner.state.recent_phase_errors).toHaveLength(1);
      expect(firstRejected).toBe(true);
    } finally {
      append.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('releases a failed open reservation so a disappeared run can retry the same logical rejection', async () => {
    // Production mutation caught: reserving the error object before opening
    // the historical ledger permanently loses the failure when open throws.
    const runId = 'detached-open-retry';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory);
    releaseLease(guard);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(undefined);
    const error = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'retry after failed open');
    const originalOpen = SecureAutoloopLedger.open.bind(SecureAutoloopLedger);
    const open = vi
      .spyOn(SecureAutoloopLedger, 'open')
      .mockImplementationOnce(() => {
        throw new Error('injected open failure');
      })
      .mockImplementation((...args) => originalOpen(...args));

    try {
      let firstRejected = false;
      try {
        await manager.recordDetachedAutoloopChatFailure(runId, error);
      } catch {
        firstRejected = true;
      }
      await manager.recordDetachedAutoloopChatFailure(runId, error);

      expect(
        decisionRows(ledger).filter((row) => (row.payload as { error?: unknown })?.error === 'retry after failed open'),
      ).toHaveLength(1);
      expect(firstRejected).toBe(true);
    } finally {
      open.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('uses the same-turn Dispatcher-preaudited row without a second append and preserves its identity', async () => {
    // Production mutation caught: preaudit reuse without a causally bound
    // Planner chat can either duplicate this row or replace its timestamp and
    // table-derived public identity in observable state.
    const runId = 'detached-preaudited';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const timestamp = '2026-09-07T08:01:00.000Z';
    const failure = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'Dispatcher already audited this failure');
    const append = vi.spyOn(ledger, 'appendFlatFile');
    handle.runner.send.mockImplementationOnce(async () => {
      ledger.appendFlatFile(
        'decisions.jsonl',
        `${JSON.stringify({
          ts: timestamp,
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'Dispatcher already audited this failure',
          },
        })}\n`,
      );
      throw failure;
    });

    try {
      await expect(manager.autoloopChat(runId, 'audit this exact Planner chat')).rejects.toBe(failure);
      await manager.recordDetachedAutoloopChatFailure(runId, failure);

      expect(append).toHaveBeenCalledTimes(1);
      expect(decisionRows(ledger)).toEqual([
        {
          ts: timestamp,
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'Dispatcher already audited this failure',
          },
        },
      ]);
      expect(handle.runner.state.recent_phase_errors).toEqual([
        {
          ts: timestamp,
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'Dispatcher already audited this failure',
        },
      ]);
    } finally {
      append.mockRestore();
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('upgrades each real Runner-projected preaudit exactly once by its Planner message id', async () => {
    // Production mutation caught: matching a live Runner entry only by its
    // public code/message skips retryability, causal tagging, and the typed
    // event, while also conflating a later equal-shape Planner chat.
    const runId = 'detached-runner-preaudit-upgrade';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const failures = [
      new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'same public Runner failure'),
      new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'same public Runner failure'),
    ];
    const auditTimestamps = ['2026-09-07T08:01:10.000Z', '2026-09-07T08:01:20.000Z'];
    let deliveryIndex = 0;
    const dispatcher = Object.assign(new EventEmitter(), {
      secureLedgerCapability: ledger,
      deliver: async () => {
        const failure = failures[deliveryIndex];
        const timestamp = auditTimestamps[deliveryIndex];
        if (!failure || !timestamp) throw new Error('unexpected extra Planner delivery');
        deliveryIndex += 1;
        ledger.appendFlatFile(
          'decisions.jsonl',
          `${JSON.stringify({
            ts: timestamp,
            kind: 'phase_error',
            actor: 'dispatcher',
            payload: {
              agent: 'planner',
              phase: 'planner_turn',
              code: 'AUTOLOOP_ENGINE_FAILURE',
              error: 'same public Runner failure',
            },
          })}\n`,
        );
        throw failure;
      },
    });
    const runner = new AutoloopRunner({
      run_id: runId,
      workspace,
      ledger_dir: ledger.directory,
      dispatcher,
      phaseErrorCircuit: 10,
      notifyUser: async () => {},
    });
    await runner.start();
    const handle = { runner, dispatcher };
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): typeof handle;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const phaseErrors: unknown[] = [];
    const publicFailures: unknown[] = [];
    const states: Array<{
      consecutive: number;
      entries: AutoloopState['recent_phase_errors'];
    }> = [];
    const routedMessages: Array<{ type?: unknown; msg_id?: unknown }> = [];
    runner.on('message', (message) => routedMessages.push(message as { type?: unknown; msg_id?: unknown }));
    runner.on('phase_error', (failure) => phaseErrors.push(failure));
    runner.on('autoloop_failure', (failure) => publicFailures.push(failure));
    runner.on('state', (state: AutoloopState) =>
      states.push({
        consecutive: state.consecutive_phase_errors,
        entries: state.recent_phase_errors.slice(),
      }),
    );

    try {
      await expect(manager.autoloopChat(runId, 'first equal-shape chat')).rejects.toBe(failures[0]);
      const firstMessageId = routedMessages.find((message) => message.type === 'chat')?.msg_id;
      const firstRunnerEntry = runner.state.recent_phase_errors[0];
      expect({
        consecutive: runner.state.consecutive_phase_errors,
        entry: firstRunnerEntry,
        retryable: Object.hasOwn(firstRunnerEntry as object, 'retryable'),
        tagged: Object.getOwnPropertySymbols(firstRunnerEntry as object),
      }).toEqual({
        consecutive: 1,
        entry: {
          ts: expect.any(String),
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          error: 'same public Runner failure',
        },
        retryable: false,
        tagged: [],
      });
      expect(firstMessageId).toEqual(expect.any(String));

      const firstTimestamp = firstRunnerEntry.ts;
      const stateCountBeforeFirstUpgrade = states.length;
      await manager.recordDetachedAutoloopChatFailure(runId, failures[0]);
      const firstUpgradedEntry = runner.state.recent_phase_errors[0];
      const firstTags = Object.getOwnPropertySymbols(firstUpgradedEntry as object);
      expect(firstUpgradedEntry).not.toBe(firstRunnerEntry);
      expect(firstUpgradedEntry).toEqual({
        ts: firstTimestamp,
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        retryable: true,
        error: 'same public Runner failure',
      });
      expect(firstTags.map((symbol) => symbol.description)).toEqual(['detachedAutoloopFailureId']);
      expect(firstTags.map((symbol) => Reflect.get(firstUpgradedEntry as object, symbol))).toEqual([firstMessageId]);
      expect(phaseErrors).toHaveLength(1);
      expect(publicFailures).toEqual([
        {
          code: 'AUTOLOOP_ENGINE_FAILURE',
          message: 'same public Runner failure',
          retryable: true,
        },
      ]);
      expect(states).toHaveLength(stateCountBeforeFirstUpgrade + 1);
      expect(states.at(-1)).toEqual({ consecutive: 1, entries: [firstUpgradedEntry] });

      const stateCountAfterFirstRecord = states.length;
      await manager.recordDetachedAutoloopChatFailure(runId, failures[0]);
      expect({
        consecutive: runner.state.consecutive_phase_errors,
        entries: runner.state.recent_phase_errors,
        phaseErrorCount: phaseErrors.length,
        publicFailureCount: publicFailures.length,
        stateCount: states.length,
      }).toEqual({
        consecutive: 1,
        entries: [firstUpgradedEntry],
        phaseErrorCount: 1,
        publicFailureCount: 1,
        stateCount: stateCountAfterFirstRecord,
      });

      await expect(manager.autoloopChat(runId, 'second equal-shape chat')).rejects.toBe(failures[1]);
      const messageIds = routedMessages.filter((message) => message.type === 'chat').map((message) => message.msg_id);
      expect(messageIds).toEqual([firstMessageId, expect.any(String)]);
      expect(messageIds[1]).not.toBe(firstMessageId);
      const secondRunnerEntry = runner.state.recent_phase_errors[1];
      expect(runner.state.consecutive_phase_errors).toBe(2);
      expect(Object.hasOwn(secondRunnerEntry as object, 'retryable')).toBe(false);
      expect(Object.getOwnPropertySymbols(secondRunnerEntry as object)).toEqual([]);
      const secondTimestamp = secondRunnerEntry.ts;

      const stateCountBeforeSecondUpgrade = states.length;
      await manager.recordDetachedAutoloopChatFailure(runId, failures[1]);
      const secondUpgradedEntry = runner.state.recent_phase_errors[1];
      const secondTags = Object.getOwnPropertySymbols(secondUpgradedEntry as object);
      expect(secondUpgradedEntry).not.toBe(secondRunnerEntry);
      expect(secondUpgradedEntry).toEqual({
        ts: secondTimestamp,
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        retryable: true,
        error: 'same public Runner failure',
      });
      expect(secondTags.map((symbol) => symbol.description)).toEqual(['detachedAutoloopFailureId']);
      expect(secondTags.map((symbol) => Reflect.get(secondUpgradedEntry as object, symbol))).toEqual([messageIds[1]]);
      expect(Reflect.get(firstUpgradedEntry as object, firstTags[0]!)).not.toBe(
        Reflect.get(secondUpgradedEntry as object, secondTags[0]!),
      );
      expect(states).toHaveLength(stateCountBeforeSecondUpgrade + 1);
      expect(states.at(-1)).toEqual({
        consecutive: 2,
        entries: [firstUpgradedEntry, secondUpgradedEntry],
      });
      expect({
        consecutive: runner.state.consecutive_phase_errors,
        entries: runner.state.recent_phase_errors,
        phaseErrorCount: phaseErrors.length,
        publicFailures,
        auditRows: decisionRows(ledger).map((row) => row.payload),
      }).toEqual({
        consecutive: 2,
        entries: [firstUpgradedEntry, secondUpgradedEntry],
        phaseErrorCount: 2,
        publicFailures: [
          {
            code: 'AUTOLOOP_ENGINE_FAILURE',
            message: 'same public Runner failure',
            retryable: true,
          },
          {
            code: 'AUTOLOOP_ENGINE_FAILURE',
            message: 'same public Runner failure',
            retryable: true,
          },
        ],
        auditRows: [
          {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'same public Runner failure',
          },
          {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'same public Runner failure',
          },
        ],
      });

      const stateCountAfterSecondRecord = states.length;
      await manager.recordDetachedAutoloopChatFailure(runId, failures[1]);
      expect({
        consecutive: runner.state.consecutive_phase_errors,
        entries: runner.state.recent_phase_errors,
        phaseErrorCount: phaseErrors.length,
        publicFailureCount: publicFailures.length,
        stateCount: states.length,
      }).toEqual({
        consecutive: 2,
        entries: [firstUpgradedEntry, secondUpgradedEntry],
        phaseErrorCount: 2,
        publicFailureCount: 2,
        stateCount: stateCountAfterSecondRecord,
      });
    } finally {
      runner.stop();
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('causally upgrades overlapping equal-shape Runner failures by Planner message id', async () => {
    // Production mutation caught: finding the newest untagged equal-shape
    // entry at detached-recording time can attach chat A's id to chat B's
    // Runner entry after both queued chats have reached their failure boundary.
    const runId = 'detached-runner-overlapping-causal-upgrade';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const failures = [
      new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'overlapping equal public failure'),
      new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'overlapping equal public failure'),
    ];
    let releaseFirstDelivery!: () => void;
    const firstDeliveryMayFail = new Promise<void>((resolve) => {
      releaseFirstDelivery = resolve;
    });
    let observeFirstDelivery!: () => void;
    const firstDeliveryStarted = new Promise<void>((resolve) => {
      observeFirstDelivery = resolve;
    });
    let deliveryIndex = 0;
    const dispatcher = Object.assign(new EventEmitter(), {
      secureLedgerCapability: ledger,
      deliver: async () => {
        const index = deliveryIndex;
        deliveryIndex += 1;
        const failure = failures[index];
        if (!failure) throw new Error('unexpected extra Planner delivery');
        if (index === 0) {
          observeFirstDelivery();
          await firstDeliveryMayFail;
        }
        ledger.appendFlatFile(
          'decisions.jsonl',
          `${JSON.stringify({
            ts: `2026-09-07T08:02:${index}0.000Z`,
            kind: 'phase_error',
            actor: 'dispatcher',
            payload: {
              agent: 'planner',
              phase: 'planner_turn',
              code: 'AUTOLOOP_ENGINE_FAILURE',
              error: 'overlapping equal public failure',
            },
          })}\n`,
        );
        throw failure;
      },
    });
    const runner = new AutoloopRunner({
      run_id: runId,
      workspace,
      ledger_dir: ledger.directory,
      dispatcher,
      phaseErrorCircuit: 10,
      notifyUser: async () => {},
    });
    await runner.start();
    const handle = { runner, dispatcher };
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): typeof handle;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const routedMessages: Array<{ type?: unknown; msg_id?: unknown }> = [];
    const phaseErrors: unknown[] = [];
    const publicFailures: unknown[] = [];
    runner.on('message', (message) => routedMessages.push(message as { type?: unknown; msg_id?: unknown }));
    runner.on('phase_error', (failure) => phaseErrors.push(failure));
    runner.on('autoloop_failure', (failure) => publicFailures.push(failure));

    try {
      const firstChat = manager.autoloopChat(runId, 'first overlapping chat');
      await firstDeliveryStarted;
      const secondChat = manager.autoloopChat(runId, 'second queued chat');
      releaseFirstDelivery();
      const outcomes = await Promise.allSettled([firstChat, secondChat]);
      expect(outcomes).toEqual([
        { status: 'rejected', reason: failures[0] },
        { status: 'rejected', reason: failures[1] },
      ]);

      const messageIds = routedMessages.filter((message) => message.type === 'chat').map((message) => message.msg_id);
      const originalEntries = runner.state.recent_phase_errors.slice();
      expect(messageIds).toEqual([expect.any(String), expect.any(String)]);
      expect(messageIds[0]).not.toBe(messageIds[1]);
      expect(runner.state.consecutive_phase_errors).toBe(2);
      expect(phaseErrors).toHaveLength(2);
      expect(originalEntries).toHaveLength(2);
      expect(originalEntries.every((entry) => Object.getOwnPropertySymbols(entry as object).length === 0)).toBe(true);

      await manager.recordDetachedAutoloopChatFailure(runId, failures[0]);
      await manager.recordDetachedAutoloopChatFailure(runId, failures[1]);

      const upgradedEntries = runner.state.recent_phase_errors;
      const detachedIds = upgradedEntries.map((entry) => {
        const symbol = Object.getOwnPropertySymbols(entry as object).find(
          (candidate) => candidate.description === 'detachedAutoloopFailureId',
        );
        return symbol ? Reflect.get(entry as object, symbol) : undefined;
      });
      expect(upgradedEntries[0]).not.toBe(originalEntries[0]);
      expect(upgradedEntries[1]).not.toBe(originalEntries[1]);
      expect(detachedIds).toEqual(messageIds);
      expect({
        consecutive: runner.state.consecutive_phase_errors,
        phaseErrorCount: phaseErrors.length,
        typedFailureCount: publicFailures.length,
        rows: decisionRows(ledger).map((row) => row.payload),
      }).toEqual({
        consecutive: 2,
        phaseErrorCount: 2,
        typedFailureCount: 2,
        rows: [
          {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'overlapping equal public failure',
          },
          {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'overlapping equal public failure',
          },
        ],
      });

      const beforeRetries = {
        entries: runner.state.recent_phase_errors.slice(),
        phaseErrorCount: phaseErrors.length,
        typedFailureCount: publicFailures.length,
        rows: decisionRows(ledger),
      };
      await manager.recordDetachedAutoloopChatFailure(runId, failures[0]);
      await manager.recordDetachedAutoloopChatFailure(runId, failures[1]);
      expect({
        entries: runner.state.recent_phase_errors,
        phaseErrorCount: phaseErrors.length,
        typedFailureCount: publicFailures.length,
        rows: decisionRows(ledger),
      }).toEqual(beforeRetries);
    } finally {
      releaseFirstDelivery();
      runner.stop();
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('does not bind a foreign tagged failure recorded while another chat is in flight', async () => {
    // Production mutation caught: selecting a new equal-shape entry without
    // requiring it to be untagged lets chat B bind chat A's detached state
    // entry, then suppresses B's own state/accounting as if its projection had
    // rolled out of the recent-five window.
    const runId = 'detached-foreign-tagged-overlap';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = { ...detachedState(runId, workspace, ledger.directory), status: 'running' as const };
    const handle = fakeDetachedHandle(ledger, state);
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    let releaseSecondSend!: () => void;
    const secondSendMayFinish = new Promise<void>((resolve) => {
      releaseSecondSend = resolve;
    });
    let observeSecondSend!: () => void;
    const secondSendStarted = new Promise<void>((resolve) => {
      observeSecondSend = resolve;
    });
    let sendIndex = 0;
    handle.runner.send.mockImplementation(async () => {
      const index = sendIndex;
      sendIndex += 1;
      if (index === 1) {
        observeSecondSend();
        await secondSendMayFinish;
      }
    });
    const phaseErrors: unknown[] = [];
    const publicFailures: unknown[] = [];
    handle.runner.on('phase_error', (failure) => phaseErrors.push(failure));
    handle.runner.on('autoloop_failure', (failure) => publicFailures.push(failure));

    try {
      let firstFailure: unknown;
      try {
        await manager.autoloopChat(runId, 'first empty Planner reply');
      } catch (error) {
        firstFailure = error;
      }
      expect(firstFailure).toBeInstanceOf(AutoloopOperationError);

      const secondChat = manager.autoloopChat(runId, 'second empty Planner reply');
      await secondSendStarted;
      await manager.recordDetachedAutoloopChatFailure(runId, firstFailure);
      expect(handle.runner.state.recent_phase_errors).toHaveLength(1);

      releaseSecondSend();
      let secondFailure: unknown;
      try {
        await secondChat;
      } catch (error) {
        secondFailure = error;
      }
      expect(secondFailure).toBeInstanceOf(AutoloopOperationError);
      expect(secondFailure).not.toBe(firstFailure);
      await manager.recordDetachedAutoloopChatFailure(runId, secondFailure);

      const messageIds = handle.runner.send.mock.calls.map(([message]) => (message as { msg_id?: unknown }).msg_id);
      const stateIds = handle.runner.state.recent_phase_errors.map((entry) => {
        const symbol = Object.getOwnPropertySymbols(entry as object).find(
          (candidate) => candidate.description === 'detachedAutoloopFailureId',
        );
        return symbol ? Reflect.get(entry as object, symbol) : undefined;
      });
      const rows = decisionRows(ledger).filter(
        (row) =>
          (row.payload as { error?: unknown })?.error ===
          'Planner transport completed without a non-empty logical reply',
      );
      expect(messageIds).toEqual([expect.any(String), expect.any(String)]);
      expect(messageIds[0]).not.toBe(messageIds[1]);
      expect(stateIds).toEqual(messageIds);
      expect(rows.map((row) => (row.payload as { detached_failure_id?: unknown }).detached_failure_id)).toEqual(
        messageIds,
      );
      expect({
        consecutive: handle.runner.state.consecutive_phase_errors,
        phaseErrorCount: phaseErrors.length,
        publicFailureCount: publicFailures.length,
        rowCount: rows.length,
      }).toEqual({ consecutive: 2, phaseErrorCount: 2, publicFailureCount: 2, rowCount: 2 });

      await manager.recordDetachedAutoloopChatFailure(runId, firstFailure);
      await manager.recordDetachedAutoloopChatFailure(runId, secondFailure);
      expect({
        recentCount: handle.runner.state.recent_phase_errors.length,
        consecutive: handle.runner.state.consecutive_phase_errors,
        phaseErrorCount: phaseErrors.length,
        publicFailureCount: publicFailures.length,
        rowCount: decisionRows(ledger).filter(
          (row) =>
            (row.payload as { error?: unknown })?.error ===
            'Planner transport completed without a non-empty logical reply',
        ).length,
      }).toEqual({ recentCount: 2, consecutive: 2, phaseErrorCount: 2, publicFailureCount: 2, rowCount: 2 });
    } finally {
      releaseSecondSend();
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('keeps a pre-existing untagged equal-shape entry when an adapter failure creates no Runner projection', async () => {
    // Production mutation caught: omitting the pre-send identity guard lets a
    // pre-existing equal-shape entry become this chat's causal projection,
    // replacing it instead of pushing a distinct identified state effect.
    const runId = 'detached-pre-existing-untagged-entry';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const seed = {
      ts: '2026-09-07T07:55:00.000Z',
      agent: 'planner' as const,
      phase: 'planner_turn' as const,
      code: 'AUTOLOOP_EMPTY_REPLY' as const,
      error: 'Planner transport completed without a non-empty logical reply',
    };
    const state = {
      ...detachedState(runId, workspace, ledger.directory),
      status: 'running' as const,
      consecutive_phase_errors: 1,
      recent_phase_errors: [seed],
    };
    const handle = fakeDetachedHandle(ledger, state);
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);

    try {
      let failure: unknown;
      try {
        await manager.autoloopChat(runId, 'fail without a Runner projection');
      } catch (error) {
        failure = error;
      }
      expect(failure).toBeInstanceOf(AutoloopOperationError);
      const messageId = (handle.runner.send.mock.calls[0]?.[0] as { msg_id?: unknown } | undefined)?.msg_id;
      expect(messageId).toEqual(expect.any(String));
      expect(handle.runner.state.recent_phase_errors[0]).toBe(seed);
      expect(Object.getOwnPropertySymbols(seed)).toEqual([]);

      await manager.recordDetachedAutoloopChatFailure(runId, failure);

      const [preservedSeed, recorded] = handle.runner.state.recent_phase_errors;
      const recordedIdSymbol = Object.getOwnPropertySymbols(recorded as object).find(
        (candidate) => candidate.description === 'detachedAutoloopFailureId',
      );
      expect(preservedSeed).toBe(seed);
      expect(Object.getOwnPropertySymbols(seed)).toEqual([]);
      expect(recorded).not.toBe(seed);
      expect(recordedIdSymbol ? Reflect.get(recorded as object, recordedIdSymbol) : undefined).toBe(messageId);
      expect(handle.runner.state.consecutive_phase_errors).toBe(2);
      expect(
        decisionRows(ledger).map((row) => (row.payload as { detached_failure_id?: unknown }).detached_failure_id),
      ).toEqual([messageId]);
    } finally {
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('does not retag or recount when a causally bound Runner entry rolls out before recording', async () => {
    // Production mutation caught: falling back to a later equal-shape entry
    // after the exact bound Runner object leaves the recent-five window steals
    // another chat's identity; falling back to append/count records it twice.
    const runId = 'detached-runner-causal-entry-rollout';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const failures = Array.from(
      { length: 6 },
      () => new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'rolled equal public failure'),
    );
    let deliveryIndex = 0;
    const dispatcher = Object.assign(new EventEmitter(), {
      secureLedgerCapability: ledger,
      deliver: async () => {
        const index = deliveryIndex;
        deliveryIndex += 1;
        const failure = failures[index];
        if (!failure) throw new Error('unexpected extra Planner delivery');
        ledger.appendFlatFile(
          'decisions.jsonl',
          `${JSON.stringify({
            ts: `2026-09-07T08:03:${index}0.000Z`,
            kind: 'phase_error',
            actor: 'dispatcher',
            payload: {
              agent: 'planner',
              phase: 'planner_turn',
              code: 'AUTOLOOP_ENGINE_FAILURE',
              error: 'rolled equal public failure',
            },
          })}\n`,
        );
        throw failure;
      },
    });
    const runner = new AutoloopRunner({
      run_id: runId,
      workspace,
      ledger_dir: ledger.directory,
      dispatcher,
      phaseErrorCircuit: 10,
      notifyUser: async () => {},
    });
    await runner.start();
    const handle = { runner, dispatcher };
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): typeof handle;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const phaseErrors: unknown[] = [];
    const publicFailures: unknown[] = [];
    runner.on('phase_error', (failure) => phaseErrors.push(failure));
    runner.on('autoloop_failure', (failure) => publicFailures.push(failure));

    try {
      for (let index = 0; index < failures.length; index += 1) {
        await expect(manager.autoloopChat(runId, `rollout chat ${index}`)).rejects.toBe(failures[index]);
      }
      const recentWindow = runner.state.recent_phase_errors.slice();
      expect({
        consecutive: runner.state.consecutive_phase_errors,
        recentCount: recentWindow.length,
        phaseErrorCount: phaseErrors.length,
      }).toEqual({ consecutive: 6, recentCount: 5, phaseErrorCount: 6 });

      await manager.recordDetachedAutoloopChatFailure(runId, failures[0]);
      expect(runner.state.recent_phase_errors.every((entry, index) => entry === recentWindow[index])).toBe(true);
      expect(
        runner.state.recent_phase_errors.every((entry) => Object.getOwnPropertySymbols(entry as object).length === 0),
      ).toBe(true);
      expect({
        consecutive: runner.state.consecutive_phase_errors,
        phaseErrorCount: phaseErrors.length,
        typedFailureCount: publicFailures.length,
        auditRowCount: decisionRows(ledger).length,
      }).toEqual({ consecutive: 6, phaseErrorCount: 6, typedFailureCount: 1, auditRowCount: 6 });

      await manager.recordDetachedAutoloopChatFailure(runId, failures[0]);
      expect({
        entriesUnchanged: runner.state.recent_phase_errors.every((entry, index) => entry === recentWindow[index]),
        consecutive: runner.state.consecutive_phase_errors,
        phaseErrorCount: phaseErrors.length,
        typedFailureCount: publicFailures.length,
        auditRowCount: decisionRows(ledger).length,
      }).toEqual({
        entriesUnchanged: true,
        consecutive: 6,
        phaseErrorCount: 6,
        typedFailureCount: 1,
        auditRowCount: 6,
      });
    } finally {
      runner.stop();
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('adds one identified row when the live handle disappears after a real Dispatcher preaudit', async () => {
    // Production mutation caught: treating a causally bound no-id preaudit as
    // sufficient after the live runner disappears leaves nothing that cold
    // status/SSE may safely recover past the checkpoint cursor.
    const runId = 'detached-preaudit-disappeared-handle';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      state.started_at,
      ledger.readFlatFile('decisions.jsonl') ?? '',
    );
    releaseLease(guard);
    const handle = fakeDetachedHandle(ledger, state);
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const timestamp = '2026-09-07T08:01:30.000Z';
    const failure = new AutoloopOperationError(
      'AUTOLOOP_ENGINE_FAILURE',
      'Dispatcher preaudit outlived its live runner',
    );
    handle.runner.send.mockImplementationOnce(async () => {
      ledger.appendFlatFile(
        'decisions.jsonl',
        `${JSON.stringify({
          ts: timestamp,
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'Dispatcher preaudit outlived its live runner',
          },
        })}\n`,
      );
      throw failure;
    });

    try {
      let boundFailure: unknown;
      try {
        await manager.autoloopChat(runId, 'bind the real Dispatcher preaudit');
      } catch (error) {
        boundFailure = error;
      }
      expect(boundFailure).toBe(failure);

      get.mockReturnValue(undefined);
      await manager.recordDetachedAutoloopChatFailure(runId, boundFailure);
      await manager.recordDetachedAutoloopChatFailure(runId, boundFailure);

      const rows = decisionRows(ledger).filter(
        (row) => (row.payload as { error?: unknown })?.error === 'Dispatcher preaudit outlived its live runner',
      );
      const noIdRows = rows.filter((row) => !Object.hasOwn(row.payload as object, 'detached_failure_id'));
      const identifiedRows = rows.filter((row) => Object.hasOwn(row.payload as object, 'detached_failure_id'));
      const expectedFailure = {
        ts: timestamp,
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        retryable: true,
        error: 'Dispatcher preaudit outlived its live runner',
      };
      const cold = manager.autoloopStatus(runId);
      const historical = (await historicalAutoloopSnapshot(port, token, runId)).state;

      expect({
        noIdRows: noIdRows.length,
        identifiedRows: identifiedRows.length,
        identifiedTimestamp: identifiedRows[0]?.ts,
        cold: {
          consecutive_phase_errors: cold?.consecutive_phase_errors,
          recent_phase_errors: cold?.recent_phase_errors,
        },
        historical: {
          consecutive_phase_errors: historical.consecutive_phase_errors,
          recent_phase_errors: historical.recent_phase_errors,
        },
      }).toEqual({
        noIdRows: 1,
        identifiedRows: 1,
        identifiedTimestamp: timestamp,
        cold: {
          consecutive_phase_errors: 1,
          recent_phase_errors: [expectedFailure],
        },
        historical: {
          consecutive_phase_errors: 1,
          recent_phase_errors: [expectedFailure],
        },
      });
      expect(handle.runner.state.recent_phase_errors).toEqual([]);
    } finally {
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('does not let an old equivalent no-id Dispatcher row suppress a later Planner chat', async () => {
    // Production mutation caught: shape-only preaudit matching treats every
    // future same-code/same-text rejection as the old audited row even when the
    // real Planner envelope carries a different message id.
    const runId = 'detached-stale-preaudit';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile(
      'decisions.jsonl',
      `${JSON.stringify({
        ts: '2026-09-07T07:00:00.000Z',
        kind: 'phase_error',
        actor: 'dispatcher',
        payload: {
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          error: 'same public failure on a later chat',
        },
      })}\n`,
    );
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const live = vi
      .spyOn(
        manager as unknown as {
          _liveAutoloop(id: string): ReturnType<typeof fakeDetachedHandle>;
        },
        '_liveAutoloop',
      )
      .mockReturnValue(handle);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const later = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'same public failure on a later chat');
    handle.runner.send.mockRejectedValueOnce(later);

    try {
      await expect(manager.autoloopChat(runId, 'a genuinely later Planner chat')).rejects.toBe(later);
      await manager.recordDetachedAutoloopChatFailure(runId, later);

      const rows = decisionRows(ledger).filter(
        (row) => (row.payload as { error?: unknown })?.error === 'same public failure on a later chat',
      );
      expect(rows).toHaveLength(2);
      expect(rows[0].payload).not.toHaveProperty('detached_failure_id');
      expect(rows[1].payload).toEqual(
        expect.objectContaining({
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'same public failure on a later chat',
          detached_failure_id: expect.any(String),
        }),
      );
      expect(handle.runner.state.recent_phase_errors).toHaveLength(1);
    } finally {
      live.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('does not call the kernel publisher or renew runner/kernel activity during detached bookkeeping', async () => {
    // Production mutation caught: invoking the registered checkpoint publisher
    // renews the kernel lease even though a detached rejection is not progress.
    const runId = 'detached-no-renewal';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const handle = fakeDetachedHandle(ledger, state);
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state);
    const leasePath = path.join(runDir(runId), 'lease.json');
    const lease = readLease(runId)!;
    fs.writeFileSync(leasePath, JSON.stringify({ ...lease, renewedAt: '2000-01-01T00:00:00.000Z' }));
    const publisher = vi.fn(() => renewLease(guard));
    const publishers = (manager as unknown as { _autoloopPublishers: Map<string, () => void> })._autoloopPublishers;
    publishers.set(runId, publisher);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);

    try {
      await manager.recordDetachedAutoloopChatFailure(
        runId,
        new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'bookkeeping is not progress'),
      );
      expect(publisher).not.toHaveBeenCalled();
      expect(readLease(runId)?.renewedAt).toBe('2000-01-01T00:00:00.000Z');
      expect(handle.runner.state.last_activity_at).toBe(1234);
      expect(handle.runner.send).not.toHaveBeenCalled();
    } finally {
      publishers.delete(runId);
      get.mockRestore();
      releaseLease(guard);
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('serializes the durable envelope and payload canonically under an inherited toJSON hook', async () => {
    // Production mutation caught: null-prototype public values are insufficient
    // if the JSONL envelope/payload are ordinary objects with inherited toJSON.
    const runId = 'detached-canonical-jsonl';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const handle = fakeDetachedHandle(ledger, detachedState(runId, workspace, ledger.directory));
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const prior = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value: () => ({ forged: true }),
    });

    try {
      await manager.recordDetachedAutoloopChatFailure(
        runId,
        new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'canonical durable failure'),
      );
      expect(decisionRows(ledger)).toEqual([
        expect.objectContaining({
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: expect.objectContaining({
            code: 'AUTOLOOP_ENGINE_FAILURE',
            retryable: true,
            error: 'canonical durable failure',
          }),
        }),
      ]);
    } finally {
      if (prior) Object.defineProperty(Object.prototype, 'toJSON', prior);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it.each([
    ['AUTOLOOP_SEND_TIMEOUT', true],
    ['AUTOLOOP_RUN_PAUSED', false],
    ['AUTOLOOP_RUN_TERMINAL', false],
  ] as const)('persists full %s identity after the compatible 202 response', async (code, retryable) => {
    // Production mutation caught: treating chat-state failures as generic
    // drops their stable code, table-derived retryability, and state metadata.
    const runId = `detached-${code.toLowerCase()}`;
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const handle = fakeDetachedHandle(ledger, state);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const failure = chatStateFailure(code, `${code} crossed the detached boundary`);
    const chat = vi.spyOn(manager, 'autoloopChat').mockRejectedValueOnce(failure);

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ text: 'record the typed chat-state failure' }),
      });
      expect(response.status).toBe(202);
      await vi.waitFor(() => expect(decisionRows(ledger)).toHaveLength(1));

      const payload = decisionRows(ledger)[0].payload as Record<string, unknown>;
      expect(payload).toMatchObject({
        code,
        retryable,
        error: `${code} crossed the detached boundary`,
        status_reason: failure.status_reason,
      });
      if (code === 'AUTOLOOP_SEND_TIMEOUT') {
        expect(payload.pending_dispatch).toEqual(failure.pending_dispatch);
      } else {
        expect(payload).not.toHaveProperty('pending_dispatch');
      }
      expect(handle.runner.state.recent_phase_errors).toEqual([
        expect.objectContaining({
          code,
          retryable,
          error: `${code} crossed the detached boundary`,
          status_reason: failure.status_reason,
          ...(code === 'AUTOLOOP_SEND_TIMEOUT' ? { pending_dispatch: failure.pending_dispatch } : {}),
        }),
      ]);
    } finally {
      chat.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('recovers a typed rejection after the live handle disappears in status and historical SSE', async () => {
    // Production mutation caught: consulting only the live handle drops a late
    // post-202 rejection before historical status/SSE can observe it.
    const runId = 'detached-disappeared-handle';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state);
    releaseLease(guard);
    const acceptedHandle = fakeDetachedHandle(ledger, state);
    const get = vi
      .spyOn(manager, 'getAutoloop')
      .mockReturnValueOnce(acceptedHandle as never)
      .mockReturnValue(undefined);
    const chat = vi
      .spyOn(manager, 'autoloopChat')
      .mockRejectedValueOnce(new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'late disappeared failure'));

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ text: 'accept before the handle disappears' }),
      });
      expect(response.status).toBe(202);
      await vi.waitFor(() => expect(decisionRows(ledger)).toHaveLength(1));

      expect(manager.autoloopStatus(runId)?.recent_phase_errors).toEqual([
        expect.objectContaining({
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'late disappeared failure',
        }),
      ]);

      const events = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      expect(events.status).toBe(200);
      const body = await events.text();
      const snapshotData = body.match(/event: snapshot\ndata: ([^\n]+)/)?.[1];
      expect(snapshotData).toBeDefined();
      expect(JSON.parse(snapshotData!).state.recent_phase_errors).toEqual([
        expect.objectContaining({
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'late disappeared failure',
        }),
      ]);
    } finally {
      chat.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('retains durability and historical status when the SSE listener closes before rejection', async () => {
    // Production mutation caught: coupling persistence to a live SSE listener
    // loses the failure as soon as the browser disconnects.
    const runId = 'detached-closed-sse';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state);
    releaseLease(guard);
    const handle = fakeDetachedHandle(ledger, state);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    let rejectChat!: (error: unknown) => void;
    const pendingChat = new Promise<{ reply: string }>((_resolve, reject) => {
      rejectChat = reject;
    });
    const chat = vi.spyOn(manager, 'autoloopChat').mockReturnValueOnce(pendingChat);
    const controller = new AbortController();

    try {
      const events = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
        headers: { Authorization: `Bearer ${token}` },
        signal: controller.signal,
      });
      expect(events.status).toBe(200);
      await events.body!.cancel();
      controller.abort();

      const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ text: 'reject after SSE disconnects' }),
      });
      expect(response.status).toBe(202);
      rejectChat(new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'failure after closed SSE'));
      await vi.waitFor(() => expect(decisionRows(ledger)).toHaveLength(1));
      get.mockReturnValue(undefined);

      expect(manager.autoloopStatus(runId)?.recent_phase_errors).toEqual([
        expect.objectContaining({
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'failure after closed SSE',
        }),
      ]);
    } finally {
      controller.abort();
      chat.mockRestore();
      get.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('recovers only post-checkpoint detached SEND_TIMEOUT state past malformed and ordinary rows', async () => {
    // Production mutation caught: treating every Dispatcher phase_error as a
    // detached HTTP rejection resurrects history cleared by a later success,
    // while losing pending_dispatch makes the recovered timeout unactionable.
    const runId = 'detached-malformed-ledger';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:05:00.000Z');
    releaseLease(guard);
    ledger.appendFlatFile(
      'decisions.jsonl',
      [
        JSON.stringify({
          ts: '2026-09-07T08:05:30.000Z',
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'ordinary Dispatcher history after the checkpoint',
          },
        }),
        JSON.stringify({
          ts: '2026-09-07T08:04:00.000Z',
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_RUN_PAUSED',
            retryable: false,
            status_reason: 'cleared-by-success',
            error: 'old detached failure cleared before checkpoint',
            detached_failure_id: 'detached-before-success',
          },
        }),
        '{ definitely-not-json',
      ].join('\n') + '\n',
    );
    const pendingDispatch = chatStateFailure('AUTOLOOP_SEND_TIMEOUT', 'late timeout').pending_dispatch!;

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 0,
        recent_phase_errors: [],
      });

      ledger.appendFlatFile(
        'decisions.jsonl',
        `${JSON.stringify({
          ts: '2026-09-07T08:06:00.000Z',
          kind: 'phase_error',
          actor: 'dispatcher',
          payload: {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_SEND_TIMEOUT',
            retryable: false,
            pending_dispatch: pendingDispatch,
            status_reason: 'awaiting_resume:send_timeout:planner:dispatch-planner-3',
            error: 'late timeout',
            detached_failure_id: 'detached-after-success',
          },
        })}\n`,
      );

      const expected = {
        consecutive_phase_errors: 1,
        recent_phase_errors: [
          {
            ts: '2026-09-07T08:06:00.000Z',
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_SEND_TIMEOUT',
            retryable: true,
            pending_dispatch: pendingDispatch,
            status_reason: 'awaiting_resume:send_timeout:planner:dispatch-planner-3',
            error: 'late timeout',
          },
        ],
      };
      expect(manager.autoloopStatus(runId)).toMatchObject(expected);

      const events = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      expect(events.status).toBe(200);
      const body = await events.text();
      const snapshotData = body.match(/event: snapshot\ndata: ([^\n]+)/)?.[1];
      expect(snapshotData).toBeDefined();
      expect(JSON.parse(snapshotData!).state).toMatchObject(expected);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('skips a literal null decision row while recording one durable identified failure', async () => {
    // Production mutation caught: narrowing JSON.parse('null') directly to a
    // record makes the tolerant reader throw before detached persistence, so
    // retries cannot establish one durable identified row.
    const runId = 'detached-null-row-recording';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', 'null\n');
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      '2026-09-07T08:05:00.000Z',
      checkpointDecisionLog,
    );
    releaseLease(guard);
    const failure = new AutoloopOperationError('AUTOLOOP_ENGINE_FAILURE', 'failure after literal null');

    try {
      const first = await manager.recordDetachedAutoloopChatFailure(runId, failure);
      const second = await manager.recordDetachedAutoloopChatFailure(runId, failure);
      const lines = (ledger.readFlatFile('decisions.jsonl') ?? '').split('\n').filter((line) => line.length > 0);

      expect(first).toEqual({
        code: 'AUTOLOOP_ENGINE_FAILURE',
        message: 'failure after literal null',
        retryable: true,
      });
      expect(second).toEqual(first);
      expect(lines).toHaveLength(2);
      expect(lines[0]).toBe('null');
      expect(JSON.parse(lines[1]) as unknown).toMatchObject({
        kind: 'phase_error',
        actor: 'dispatcher',
        payload: {
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'failure after literal null',
          detached_failure_id: expect.any(String),
        },
      });
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('recovers a valid identified failure after a literal null row in cold status and historical SSE', async () => {
    // Production mutation caught: allowing the parsed null through the
    // tolerant reader aborts the whole cold scan before its valid trailing
    // identified phase_error reaches status or historical SSE.
    const runId = 'detached-null-row-cold-recovery';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', 'null\n');
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      '2026-09-07T08:05:00.000Z',
      checkpointDecisionLog,
    );
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:06:00.000Z',
      id: 'identified-after-null',
      error: 'valid trailing failure after literal null',
    });
    const expected = {
      consecutive_phase_errors: 1,
      recent_phase_errors: [
        {
          ts: '2026-09-07T08:06:00.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'valid trailing failure after literal null',
        },
      ],
    };

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject(expected);
      expect((await historicalAutoloopSnapshot(port, token, runId)).state).toMatchObject(expected);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('does not duplicate a checkpointed detached failure when its audit timestamp differs', () => {
    // Production mutation caught: timestamp equality is not durable identity;
    // a checkpoint projection may timestamp the same logical failure
    // differently from its adapter audit row.
    const runId = 'detached-checkpoint-dedup';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    state.consecutive_phase_errors = 1;
    state.recent_phase_errors = [
      {
        ts: '2026-09-07T08:05:30.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        error: 'checkpointed detached rejection',
      },
    ];
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:06:00.000Z');
    releaseLease(guard);
    ledger.appendFlatFile(
      'decisions.jsonl',
      `${JSON.stringify({
        ts: '2026-09-07T08:05:00.000Z',
        kind: 'phase_error',
        actor: 'dispatcher',
        payload: {
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'checkpointed detached rejection',
          detached_failure_id: 'checkpointed-detached-id',
        },
      })}\n`,
    );

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 1,
        recent_phase_errors: [
          {
            ts: '2026-09-07T08:05:30.000Z',
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'checkpointed detached rejection',
          },
        ],
      });
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('recovers an equal-time row appended beyond the checkpoint cursor once in status and historical SSE', async () => {
    // Production mutation caught: replacing the decision-log cursor with
    // `row.ts > record.updatedAt` drops a causally late same-millisecond row.
    const runId = 'detached-cursor-equal-time';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const checkpointTime = '2026-09-07T08:05:00.000Z';
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, checkpointTime, '');
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: checkpointTime,
      id: 'equal-time-late-id',
      error: 'equal-time causally late failure',
    });

    const expected = {
      consecutive_phase_errors: 1,
      recent_phase_errors: [
        {
          ts: checkpointTime,
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'equal-time causally late failure',
        },
      ],
    };

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject(expected);
      expect((await historicalAutoloopSnapshot(port, token, runId)).state).toMatchObject(expected);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('recovers only a distinct suffix beyond a non-empty checkpoint cursor in cold status and historical SSE', async () => {
    // Production mutation caught: treating every non-zero cursor as fully
    // recovered suppresses a real detached suffix after a non-empty prefix.
    const runId = 'detached-cursor-non-empty-prefix';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:04:00.000Z',
      id: 'non-empty-prefix-id',
      error: 'checkpointed prefix must stay cleared',
    });
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      '2026-09-07T08:05:00.000Z',
      checkpointDecisionLog,
    );
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:06:00.000Z',
      id: 'non-empty-suffix-id',
      error: 'distinct suffix must be recovered',
    });

    const expected = {
      consecutive_phase_errors: 1,
      recent_phase_errors: [
        {
          ts: '2026-09-07T08:06:00.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'distinct suffix must be recovered',
        },
      ],
    };

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject(expected);
      expect((await historicalAutoloopSnapshot(port, token, runId)).state).toMatchObject(expected);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('recovers clock-skewed and invalid-time rows appended beyond the checkpoint cursor exactly once', async () => {
    // Production mutation caught: timestamp parsing/comparison is not causal;
    // replacing the persisted append cursor with it drops valid late rows.
    const runId = 'detached-cursor-clock-skew';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:05:00.000Z', '');
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T07:55:00.000Z',
      id: 'rewound-clock-late-id',
      error: 'late row after clock rollback',
    });
    appendDetachedFailureRow(ledger, {
      ts: 'not-a-timestamp',
      id: 'invalid-clock-late-id',
      error: 'late row with invalid clock',
    });

    const expected = {
      consecutive_phase_errors: 2,
      recent_phase_errors: [
        expect.objectContaining({ error: 'late row after clock rollback' }),
        expect.objectContaining({ error: 'late row with invalid clock' }),
      ],
    };

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject(expected);
      expect((await historicalAutoloopSnapshot(port, token, runId)).state).toMatchObject(expected);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('does not add a post-cursor row whose complete public identity is already checkpointed', () => {
    // Production mutation caught: skipping checkpoint identity multiplicity on
    // the post-watermark branch duplicates state and inflates its counter.
    const runId = 'detached-cursor-checkpoint-identity';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    state.consecutive_phase_errors = 1;
    state.recent_phase_errors = [
      {
        ts: '2026-09-07T08:04:30.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        error: 'identity already projected into checkpoint',
      },
    ];
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:05:00.000Z', '');
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:06:00.000Z',
      id: 'already-projected-id',
      error: 'identity already projected into checkpoint',
    });

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 1,
        recent_phase_errors: [
          {
            ts: '2026-09-07T08:04:30.000Z',
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'identity already projected into checkpoint',
          },
        ],
      });
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('preserves checkpoint identity multiplicity before admitting exactly one of three empty-cursor suffix ids', () => {
    // Production mutations caught: reducing checkpoint identity counts to Set
    // membership suppresses or admits the wrong number of identical suffixes,
    // while replacing the checkpoint entries with all three suffix rows keeps
    // the same length and counter but loses the original causal history.
    const runId = 'detached-cursor-checkpoint-multiplicity';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    state.consecutive_phase_errors = 2;
    state.recent_phase_errors = [
      {
        ts: '2026-09-07T08:04:10.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        error: 'repeated public identity',
      },
      {
        ts: '2026-09-07T08:04:20.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        error: 'repeated public identity',
      },
    ];
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:05:00.000Z', '');
    releaseLease(guard);
    const suffixes = [
      { ts: '2026-09-07T08:06:10.000Z', id: 'suffix-distinct-id-1' },
      { ts: '2026-09-07T08:06:20.000Z', id: 'suffix-distinct-id-2' },
      { ts: '2026-09-07T08:06:30.000Z', id: 'suffix-distinct-id-3' },
    ];
    for (const suffix of suffixes) {
      appendDetachedFailureRow(ledger, {
        ...suffix,
        error: 'repeated public identity',
      });
    }

    try {
      const recovered = manager.autoloopStatus(runId);
      expect(recovered?.consecutive_phase_errors).toBe(3);
      expect(recovered?.recent_phase_errors).toEqual([
        {
          ts: '2026-09-07T08:04:10.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          error: 'repeated public identity',
        },
        {
          ts: '2026-09-07T08:04:20.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          error: 'repeated public identity',
        },
        {
          ts: '2026-09-07T08:06:30.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'repeated public identity',
        },
      ]);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('uses ordinary authenticated prefix rows to reconcile two checkpoint effects before admitting all three suffix ids', async () => {
    // Production mutations caught: replacing checkpoint history with suffix
    // rows loses the original timestamps, while ignoring no-id rows in the
    // authenticated prefix spends checkpoint multiplicity on causal suffix ids.
    const runId = 'detached-cursor-no-id-prefix-multiplicity';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const checkpointFailures = [{ ts: '2026-09-07T08:04:10.000Z' }, { ts: '2026-09-07T08:04:20.000Z' }];
    for (const failure of checkpointFailures) {
      appendDetachedFailureRow(ledger, {
        ...failure,
        error: 'shared public identity across cursor',
      });
    }
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    const state = detachedState(runId, workspace, ledger.directory);
    state.consecutive_phase_errors = 2;
    state.recent_phase_errors = checkpointFailures.map(({ ts }) => ({
      ts,
      agent: 'planner',
      phase: 'planner_turn',
      code: 'AUTOLOOP_ENGINE_FAILURE',
      error: 'shared public identity across cursor',
    }));
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      '2026-09-07T08:05:00.000Z',
      checkpointDecisionLog,
    );
    releaseLease(guard);
    const suffixFailures = [
      { ts: '2026-09-07T08:06:10.000Z', id: 'suffix-after-cursor-id-1' },
      { ts: '2026-09-07T08:06:20.000Z', id: 'suffix-after-cursor-id-2' },
      { ts: '2026-09-07T08:06:30.000Z', id: 'suffix-after-cursor-id-3' },
    ];
    for (const failure of suffixFailures) {
      appendDetachedFailureRow(ledger, {
        ...failure,
        error: 'shared public identity across cursor',
      });
    }
    const expectedFailures = [
      {
        ts: '2026-09-07T08:04:10.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        error: 'shared public identity across cursor',
      },
      {
        ts: '2026-09-07T08:04:20.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        error: 'shared public identity across cursor',
      },
      {
        ts: '2026-09-07T08:06:10.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        retryable: true,
        error: 'shared public identity across cursor',
      },
      {
        ts: '2026-09-07T08:06:20.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        retryable: true,
        error: 'shared public identity across cursor',
      },
      {
        ts: '2026-09-07T08:06:30.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        retryable: true,
        error: 'shared public identity across cursor',
      },
    ];

    try {
      const recovered = manager.autoloopStatus(runId);
      expect(recovered?.consecutive_phase_errors).toBe(5);
      expect(recovered?.recent_phase_errors).toEqual(expectedFailures);

      const historical = (await historicalAutoloopSnapshot(port, token, runId)).state;
      expect(historical.consecutive_phase_errors).toBe(5);
      expect(historical.recent_phase_errors).toEqual(expectedFailures);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('counts an equal-shape identified suffix as a new effect after an authenticated checkpoint prefix', async () => {
    // Production mutation caught: shape suppression after an authenticated
    // cursor would collapse a new detached_failure_id into the checkpointed
    // equal-shape effect instead of preserving both causal events.
    const runId = 'detached-cursor-equal-shape-new-id';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:04:10.000Z',
      id: 'checkpoint-prefix-id',
      error: 'same public identity across authenticated cursor',
    });
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    const state = detachedState(runId, workspace, ledger.directory);
    state.consecutive_phase_errors = 1;
    state.recent_phase_errors = [
      {
        ts: '2026-09-07T08:04:10.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        error: 'same public identity across authenticated cursor',
      },
    ];
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      '2026-09-07T08:05:00.000Z',
      checkpointDecisionLog,
    );
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:06:10.000Z',
      id: 'new-suffix-id',
      error: 'same public identity across authenticated cursor',
    });
    const expected = {
      consecutive_phase_errors: 2,
      recent_phase_errors: [
        {
          ts: '2026-09-07T08:04:10.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          error: 'same public identity across authenticated cursor',
        },
        {
          ts: '2026-09-07T08:06:10.000Z',
          agent: 'planner',
          phase: 'planner_turn',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          error: 'same public identity across authenticated cursor',
        },
      ],
    };

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject(expected);
      expect((await historicalAutoloopSnapshot(port, token, runId)).state).toMatchObject(expected);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('keeps repeated cold status and historical SSE reads stable with seven durable suffix ids', async () => {
    // Production mutation caught: retaining a loaded checkpoint object or its
    // recent-error array across cold reads ratchets the counter after trimming.
    const runId = 'detached-cursor-seven-stable-cold-reads';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:05:00.000Z', '');
    releaseLease(guard);
    for (let index = 1; index <= 7; index += 1) {
      appendDetachedFailureRow(ledger, {
        ts: `2026-09-07T08:06:0${index}.000Z`,
        id: `stable-cold-suffix-id-${index}`,
        error: `stable cold suffix ${index}`,
      });
    }
    const checkpointPath = path.join(runDir(runId), 'run.json');
    const checkpointBefore = fs.readFileSync(checkpointPath, 'utf8');
    const checkpointHashBefore = createHash('sha256').update(checkpointBefore, 'utf8').digest('hex');
    const expected = {
      consecutive_phase_errors: 7,
      recent_phase_errors: [3, 4, 5, 6, 7].map((index) => ({
        ts: `2026-09-07T08:06:0${index}.000Z`,
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        retryable: true,
        error: `stable cold suffix ${index}`,
      })),
    };

    try {
      const first = manager.autoloopStatus(runId);
      const second = manager.autoloopStatus(runId);
      expect(first).toMatchObject(expected);
      expect(second).toEqual(first);

      const historical = (await historicalAutoloopSnapshot(port, token, runId)).state;
      expect(historical).toEqual(first);

      const checkpointAfter = fs.readFileSync(checkpointPath, 'utf8');
      expect(checkpointAfter).toBe(checkpointBefore);
      expect(createHash('sha256').update(checkpointAfter, 'utf8').digest('hex')).toBe(checkpointHashBefore);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('surfaces duplicate equal-time ledger lines with one detached failure id exactly once', () => {
    // Production mutation caught: using timestamp admission before durable-id
    // deduplication drops both same-time copies instead of surfacing one.
    const runId = 'detached-cursor-duplicate-id';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const checkpointTime = '2026-09-07T08:05:00.000Z';
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, checkpointTime, '');
    releaseLease(guard);
    const duplicate = {
      ts: checkpointTime,
      id: 'duplicated-detached-id',
      error: 'one logical detached failure',
    };
    appendDetachedFailureRow(ledger, duplicate);
    appendDetachedFailureRow(ledger, duplicate);

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 1,
        recent_phase_errors: [expect.objectContaining({ error: 'one logical detached failure' })],
      });
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('keeps pre-cursor detached rows cleared by a later checkpoint across equal and rewound clocks', () => {
    // Production mutation caught: replacing append order with wall-clock order
    // resurrects pre-success rows when the checkpoint clock moved backward.
    const runId = 'detached-cursor-success-clear';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const checkpointTime = '2026-09-07T08:05:00.000Z';
    appendDetachedFailureRow(ledger, {
      ts: checkpointTime,
      id: 'cleared-equal-time-id',
      error: 'equal-time failure cleared by success',
    });
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:06:00.000Z',
      id: 'cleared-before-clock-rollback-id',
      error: 'failure cleared before checkpoint clock rollback',
    });
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      checkpointTime,
      checkpointDecisionLog,
    );
    releaseLease(guard);

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 0,
        recent_phase_errors: [],
      });
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('captures the real decision-log cursor when the autoloop node publishes its checkpoint', async () => {
    // Production mutation caught: recovering a cursor that tests inject is
    // insufficient if the real node checkpoint never persists that cursor.
    const runId = 'detached-real-checkpoint-cursor';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    appendDetachedFailureRow(ledger, {
      ts: '2099-09-07T08:06:00.000Z',
      id: 'cleared-before-real-publish-id',
      error: 'preexisting row cleared by real node checkpoint',
    });
    const state = detachedState(runId, workspace, ledger.directory);
    state.status = 'crashed';
    state.status_reason = 'fixture exits after publishing';
    const runner = Object.assign(new EventEmitter(), {
      state,
      stop: vi.fn(),
      waitForTermination: vi.fn().mockResolvedValue(undefined),
    });
    const dispatcher = Object.assign(new EventEmitter(), {
      secureLedgerCapability: ledger,
      sessionNames: { planner: `autoloop-${runId}-planner` },
      shutdown: vi.fn().mockResolvedValue(undefined),
    });
    const boot = vi
      .spyOn(
        manager as unknown as {
          _bootAutoloop(opts: Record<string, unknown>): Promise<unknown>;
        },
        '_bootAutoloop',
      )
      .mockResolvedValue({ runner, dispatcher, ledgerDir: ledger.directory, pushPolicy: {} });

    try {
      await manager.autoloopStart({ runId, workspace });
      await vi.waitFor(() => expect(loadRun(runId)?.state).toBe('failed'));
      expect(manager.getAutoloop(runId)).toBeUndefined();
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 0,
        recent_phase_errors: [],
      });
    } finally {
      boot.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('uses UTF-8 bytes rather than UTF-16 code units to admit a suffix after a non-ASCII prefix', async () => {
    // Production mutation caught: counting JavaScript string length for the
    // cursor or row offsets places a multibyte suffix before its real boundary.
    const runId = 'detached-cursor-utf8-byte-offset';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:04:00.000Z',
      id: 'utf8-prefix-id',
      error: '日本語🚀 checkpoint prefix',
    });
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    expect(Buffer.byteLength(checkpointDecisionLog, 'utf8')).toBeGreaterThan(checkpointDecisionLog.length);
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      '2026-09-07T08:05:00.000Z',
      checkpointDecisionLog,
    );
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:06:00.000Z',
      id: 'utf8-suffix-id',
      error: 'suffix after multibyte prefix',
    });

    const expected = {
      consecutive_phase_errors: 1,
      recent_phase_errors: [expect.objectContaining({ error: 'suffix after multibyte prefix' })],
    };

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject(expected);
      expect((await historicalAutoloopSnapshot(port, token, runId)).state).toMatchObject(expected);
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('fails closed on a same-byte-length prefix rewrite with a valid newline before a tempting suffix', () => {
    // Production mutation caught: validating only the byte count and newline
    // boundary admits suffix rows after the checkpointed prefix was rewritten.
    const runId = 'detached-cursor-same-length-prefix-rewrite';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    appendDetachedFailureRow(ledger, {
      ts: '2099-09-07T08:06:00.000Z',
      id: 'original-prefix-id',
      error: 'original checkpointed prefix',
    });
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      '2026-09-07T08:05:00.000Z',
      checkpointDecisionLog,
    );
    releaseLease(guard);
    const rewritten = checkpointDecisionLog.replace('original checkpointed prefix', 'origjnal checkpointed prefix');
    expect(Buffer.byteLength(rewritten, 'utf8')).toBe(Buffer.byteLength(checkpointDecisionLog, 'utf8'));
    expect(rewritten.endsWith('\n')).toBe(true);
    fs.writeFileSync(path.join(ledger.directory, 'decisions.jsonl'), rewritten, 'utf8');
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:07:00.000Z',
      id: 'tempting-rewritten-suffix-id',
      error: 'tempting suffix after rewritten prefix',
    });

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 0,
        recent_phase_errors: [],
      });
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('fails closed separately when decisions.jsonl is truncated inside the checkpointed prefix', () => {
    // Production mutation caught: hash validation without an independent
    // byte-length/boundary guard can treat a truncated prefix as recoverable.
    const runId = 'detached-cursor-prefix-truncation';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    appendDetachedFailureRow(ledger, {
      ts: '2099-09-07T08:06:00.000Z',
      id: 'truncated-prefix-id',
      error: 'checkpointed prefix truncated later',
    });
    const checkpointDecisionLog = ledger.readFlatFile('decisions.jsonl') ?? '';
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(
      runId,
      workspace,
      ledger.directory,
      state,
      '2026-09-07T08:05:00.000Z',
      checkpointDecisionLog,
    );
    releaseLease(guard);
    fs.writeFileSync(
      path.join(ledger.directory, 'decisions.jsonl'),
      Buffer.from(checkpointDecisionLog, 'utf8').subarray(0, -1),
    );

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 0,
        recent_phase_errors: [],
      });
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it.each(COMMITTED_LEDGER_CODES)(
    'recovers committed for durable secure-ledger code %s and strips a forged engine flag',
    (code) => {
      // Production mutation caught: dropping every durable committed flag loses
      // real applied outcomes, while trusting the flag for an ordinary engine
      // code lets a forged JSONL row claim irreversible effects.
      const runId = `detached-cold-${code.toLowerCase()}`;
      const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
      const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
      const state = detachedState(runId, workspace, ledger.directory);
      const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:05:00.000Z', '');
      releaseLease(guard);
      appendDetachedFailureRow(ledger, {
        ts: '2026-09-07T08:06:00.000Z',
        id: 'valid-committed-ledger-row',
        code,
        committed: true,
        error: 'same durable outcome',
      });
      appendDetachedFailureRow(ledger, {
        ts: '2026-09-07T08:07:00.000Z',
        id: 'forged-committed-engine-row',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        committed: true,
        error: 'ordinary engine outcome',
      });

      try {
        const recovered = manager.autoloopStatus(runId);
        expect(recovered).toMatchObject({
          consecutive_phase_errors: 2,
          recent_phase_errors: [
            {
              ts: '2026-09-07T08:06:00.000Z',
              agent: 'planner',
              phase: 'planner_turn',
              code,
              committed: true,
              retryable: false,
              error: 'same durable outcome',
            },
            {
              ts: '2026-09-07T08:07:00.000Z',
              agent: 'planner',
              phase: 'planner_turn',
              code: 'AUTOLOOP_ENGINE_FAILURE',
              retryable: true,
              error: 'ordinary engine outcome',
            },
          ],
        });
        expect(Object.hasOwn(recovered!.recent_phase_errors[1] as object, 'committed')).toBe(false);
      } finally {
        fs.rmSync(workspace, { recursive: true, force: true });
      }
    },
  );

  it('does not collapse a noncommitted durable row into a committed checkpoint identity', () => {
    // Production mutation caught: omitting `committed` from the causal key
    // makes a later noncommitted failure consume an already-checkpointed
    // committed identity with the same code and message.
    const runId = 'detached-cold-committed-key';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    state.consecutive_phase_errors = 1;
    state.recent_phase_errors = [
      {
        ts: '2026-09-07T08:04:00.000Z',
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
        error: 'same code and message',
      },
    ];
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:05:00.000Z');
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:06:00.000Z',
      id: 'later-noncommitted-row',
      code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
      error: 'same code and message',
    });

    try {
      expect(manager.autoloopStatus(runId)).toMatchObject({
        consecutive_phase_errors: 2,
        recent_phase_errors: [
          {
            ts: '2026-09-07T08:04:00.000Z',
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
            committed: true,
            retryable: false,
            error: 'same code and message',
          },
          {
            ts: '2026-09-07T08:06:00.000Z',
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
            retryable: false,
            error: 'same code and message',
          },
        ],
      });
    } finally {
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('preserves a recovered failure in cold HTTP state and historical SSE under hostile inherited toJSON', async () => {
    // Production mutation caught: serializing ordinary state/envelope objects
    // lets an inherited toJSON accessor replace the recovered public failure.
    const runId = 'detached-cold-boundary-hostile-to-json';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const state = detachedState(runId, workspace, ledger.directory);
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory, state, '2026-09-07T08:05:00.000Z', '');
    releaseLease(guard);
    appendDetachedFailureRow(ledger, {
      ts: '2026-09-07T08:06:00.000Z',
      id: 'hostile-json-suffix-id',
      error: 'consumer-visible recovered failure',
    });
    const prior = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      get() {
        return () => ({ erased_by_hostile_prototype: true });
      },
    });

    const expectedFailure = {
      ts: '2026-09-07T08:06:00.000Z',
      agent: 'planner',
      phase: 'planner_turn',
      code: 'AUTOLOOP_ENGINE_FAILURE',
      retryable: true,
      error: 'consumer-visible recovered failure',
    };

    try {
      const stateResponse = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/state`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      expect(stateResponse.status).toBe(200);
      const stateBody = (await stateResponse.json()) as { state?: AutoloopState };
      expect(stateBody.state?.recent_phase_errors).toEqual([expectedFailure]);
      expect(stateBody.state?.consecutive_phase_errors).toBe(1);

      const historicalState = (await historicalAutoloopSnapshot(port, token, runId)).state;
      expect(historicalState.recent_phase_errors).toEqual([expectedFailure]);
      expect(historicalState.consecutive_phase_errors).toBe(1);
    } finally {
      if (prior) Object.defineProperty(Object.prototype, 'toJSON', prior);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('snapshots historical termination without invoking a status_reason getter or inherited toJSON', async () => {
    // Production mutation caught: reading histState.status_reason before the
    // safe snapshot invokes an attacker-owned getter and prevents the
    // historical terminated frame from completing.
    const runId = 'historical-terminated-own-data';
    const state = detachedState(runId, '/workspace', '/ledger');
    let statusReasonGetterCalls = 0;
    Object.defineProperty(state, 'status_reason', {
      configurable: true,
      enumerable: true,
      get() {
        statusReasonGetterCalls += 1;
        return 'forged-by-historical-status-reason-getter';
      },
    });
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(undefined);
    const status = vi.spyOn(manager, 'autoloopStatus').mockReturnValue(state);
    const warmup = await fetch(`http://127.0.0.1:${port}/health`);
    expect(warmup.status).toBe(200);
    await warmup.arrayBuffer();
    const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    let inheritedHookCalls = 0;
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value() {
        inheritedHookCalls += 1;
        return { forged_historical_event: true };
      },
    });

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/${runId}/events`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      expect(response.status).toBe(200);
      const body = await response.text();
      const snapshotData = body.match(/event: snapshot\ndata: ([^\n]+)/)?.[1];
      const terminatedData = body.match(/event: terminated\ndata: ([^\n]+)/)?.[1];
      expect(snapshotData).toBeDefined();
      expect(terminatedData).toBeDefined();
      const snapshot = JSON.parse(snapshotData!) as { state: Record<string, unknown> };
      expect(snapshot.state.run_id).toBe(runId);
      expect(Object.hasOwn(snapshot.state, 'status_reason')).toBe(false);
      expect(JSON.parse(terminatedData!)).toEqual({ reason: 'historical' });
      expect(statusReasonGetterCalls).toBe(0);
      expect(inheritedHookCalls).toBe(0);
    } finally {
      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      status.mockRestore();
      get.mockRestore();
    }
  });

  it('contains hostile cold-recovery warnings without reading a message getter', () => {
    // Production mutation caught: `(error as Error).message` in the cold
    // recovery catch invokes hostile values and lets status retrieval throw.
    const runId = 'detached-hostile-recovery-warning';
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), `${runId}-`));
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const guard = stageStoredAutoloopRun(runId, workspace, ledger.directory);
    releaseLease(guard);
    let getterCalls = 0;
    const hostile = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(hostile, 'message', {
      get: () => {
        getterCalls += 1;
        throw new Error('hostile recovery getter invoked');
      },
    });
    const open = vi.spyOn(SecureAutoloopLedger, 'open').mockImplementationOnce(() => {
      throw hostile;
    });

    try {
      expect(() => manager.autoloopStatus(runId)).not.toThrow();
      expect(getterCalls).toBe(0);
    } finally {
      open.mockRestore();
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('contains a hostile recording rejection without creating an unhandled post-202 promise', async () => {
    // Production mutation caught: reading recordError.message in the terminal
    // catch can itself throw and leave the replacement promise unhandled.
    const handle = fakeDetachedHandle({} as SecureAutoloopLedger);
    const get = vi.spyOn(manager, 'getAutoloop').mockReturnValue(handle as never);
    const chat = vi.spyOn(manager, 'autoloopChat').mockRejectedValueOnce(new Error('detached rejection'));
    const hostile = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(hostile, 'message', {
      enumerable: true,
      get: () => {
        throw new Error('hostile recording message getter invoked');
      },
    });
    const record = vi.spyOn(manager, 'recordDetachedAutoloopChatFailure').mockRejectedValueOnce(hostile);
    const warn = vi.spyOn(console, 'warn').mockImplementationOnce(() => {
      throw new Error('terminal warning sink failed');
    });
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/detached-contained/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ text: 'contain every post-202 rejection' }),
      });
      expect(response.status).toBe(202);
      await vi.waitFor(() => expect(record).toHaveBeenCalledTimes(1));
      await vi.waitFor(() => expect(warn).toHaveBeenCalledTimes(1));
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(unhandled).toEqual([]);
    } finally {
      process.off('unhandledRejection', onUnhandled);
      warn.mockRestore();
      record.mockRestore();
      chat.mockRestore();
      get.mockRestore();
    }
  });

  it('returns 400 when text is missing or empty', async () => {
    vi.spyOn(manager, 'getAutoloop').mockReturnValue({
      runner: {} as never,
      dispatcher: {} as never,
    });
    const r1 = await fetch(`http://127.0.0.1:${port}/autoloop/run-xyz/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: '{}',
    });
    expect(r1.status).toBe(400);
    const r2 = await fetch(`http://127.0.0.1:${port}/autoloop/run-xyz/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ text: '   ' }),
    });
    expect(r2.status).toBe(400);
  });

  it('returns 404 when the run is not in this process memory', async () => {
    vi.spyOn(manager, 'getAutoloop').mockReturnValue(undefined);
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/nope/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ text: 'hi' }),
    });
    expect(r.status).toBe(404);
  });
});

describe('POST /autoloop/:id/request_review', () => {
  let manager: SessionManager;
  let server: EmbeddedServer;
  let port: number;
  let token: string;

  beforeAll(async () => {
    manager = new SessionManager({});
    const ephemeral = await freePort();
    server = new EmbeddedServer(manager, ephemeral);
    port = await server.start();
    token = fs.readFileSync(path.join(os.homedir(), '.openclaw', 'server-token'), 'utf-8').trim();
  });
  afterAll(async () => {
    await server.stop();
    await manager.shutdown();
  });

  it('passes one canonical checkpoint-bound request to SessionManager and never names Coder', async () => {
    const requestReview = vi
      .spyOn(manager as never, 'autoloopRequestReview' as never)
      .mockResolvedValue({ status: 'prepared', target: 'reviewer', idempotency_key: 'review-7' } as never);
    const body = {
      run_id: 'run',
      checkpoint_sha: 'A'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    };

    const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/request_review`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify(body),
    });

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      ok: true,
      status: 'prepared',
      target: 'reviewer',
      idempotency_key: 'review-7',
    });
    expect(requestReview).toHaveBeenCalledWith('run', {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-7',
    });
    expect(JSON.stringify(requestReview.mock.calls)).not.toMatch(/coder/i);
    requestReview.mockRestore();
  });

  it.each([
    ['mismatched route run id', { run_id: 'other-run' }, /must match the route run id/],
    ['unknown field', { unexpected: true }, /unsupported field/],
  ])('rejects %s before a request_review SessionManager effect', async (_label, extra, message) => {
    const requestReview = vi.spyOn(manager as never, 'autoloopRequestReview' as never);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/request_review`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
          ...extra,
        }),
      });
      expect(response.status).toBe(400);
      await expect(response.json()).resolves.toMatchObject({ ok: false, error: expect.stringMatching(message) });
      expect(requestReview).not.toHaveBeenCalled();
    } finally {
      requestReview.mockRestore();
    }
  });

  it.each([
    ['short checkpoint', { checkpoint_sha: 'abc' }],
    ['negative iteration', { source_iter: -1 }],
    ['empty scope', { scope: [] }],
    ['empty identity', { idempotency_key: '' }],
  ])('rejects %s before a SessionManager effect', async (_label, override) => {
    const requestReview = vi.spyOn(manager as never, 'autoloopRequestReview' as never);
    const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/request_review`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security'],
        idempotency_key: 'review-7',
        ...override,
      }),
    });
    expect(response.status).toBe(400);
    expect(requestReview).not.toHaveBeenCalled();
    requestReview.mockRestore();
  });

  it('returns 404 for a missing live run', async () => {
    const requestReview = vi
      .spyOn(manager as never, 'autoloopRequestReview' as never)
      .mockRejectedValue(new Error("Autoloop run 'missing' not found") as never);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/missing/request_review`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        }),
      });
      expect(response.status).toBe(404);
    } finally {
      requestReview.mockRestore();
    }
  });

  it.each([
    'Cannot change Reviewer engine or model after its session has started',
    'Cannot start Reviewer after the Autoloop run became terminal',
    `Reviewer-only checkpoint ${'a'.repeat(40)} does not match workspace HEAD ${'b'.repeat(40)}`,
    "Reviewer-only request requires source run 'source-run' iter 7/diff.patch",
    'Reviewer-only checkpoint could not verify workspace HEAD (code=1): bad repository',
    'Reviewer-only checkpoint patch could not be read (code=1): missing object',
    'Autoloop terminated while preparing the Reviewer-only request',
    'Autoloop run became terminal before Reviewer-only queue delivery',
    'Cannot request review after the Autoloop run became terminal',
    'request_review retry capacity is exhausted for this Autoloop run',
  ])('maps reachable request_review client error to 400: %s', async (message) => {
    const requestReview = vi
      .spyOn(manager as never, 'autoloopRequestReview' as never)
      .mockRejectedValueOnce(new Error(message) as never);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/request_review`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        }),
      });
      expect(response.status).toBe(400);
      await expect(response.json()).resolves.toEqual({ ok: false, error: message });
    } finally {
      requestReview.mockRestore();
    }
  });

  it.each([
    ['AUTOLOOP_RUN_PAUSED', "Autoloop run 'run' is paused; resume it before requesting review", 409],
    ['AUTOLOOP_RUN_TERMINAL', 'Autoloop run became terminal before Reviewer-only queue delivery', 400],
    ['AUTOLOOP_RUN_TERMINAL', "Autoloop run 'run' is terminal and cannot accept a review request", 400],
  ] as const)('preserves typed request_review failure %s over HTTP', async (code, message, status) => {
    const failure = Object.assign(new Error(message), { name: 'AutoloopChatStateError', code, retryable: false });
    const requestReview = vi
      .spyOn(manager as never, 'autoloopRequestReview' as never)
      .mockRejectedValueOnce(failure as never);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/request_review`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'typed-http',
        }),
      });
      expect(response.status).toBe(status);
      await expect(response.json()).resolves.toEqual({
        ok: false,
        error: { code, message, retryable: false },
      });
    } finally {
      requestReview.mockRestore();
    }
  });

  it.each([
    `Autoloop run 'run' is being deleted`,
    `Autoloop run 'run' is paused and not running in this process`,
    `Autoloop run 'run' is paused; resume it before requesting review`,
    `Autoloop run 'run' became paused before Reviewer-only queue delivery`,
  ])('maps reachable request_review conflict to 409: %s', async (message) => {
    const requestReview = vi
      .spyOn(manager as never, 'autoloopRequestReview' as never)
      .mockRejectedValueOnce(new Error(message) as never);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/request_review`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        }),
      });
      expect(response.status).toBe(409);
      await expect(response.json()).resolves.toEqual({ ok: false, error: message });
    } finally {
      requestReview.mockRestore();
    }
  });

  it('rejects GET with 405 before a SessionManager effect', async () => {
    const requestReview = vi.spyOn(manager as never, 'autoloopRequestReview' as never);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/request_review`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      expect(response.status).toBe(405);
      expect(response.headers.get('allow')).toBe('POST');
      expect(requestReview).not.toHaveBeenCalled();
    } finally {
      requestReview.mockRestore();
    }
  });

  it('rejects a null JSON body before a SessionManager effect', async () => {
    const requestReview = vi.spyOn(manager as never, 'autoloopRequestReview' as never);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/request_review`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: 'null',
      });
      expect(response.status).toBe(400);
      expect(requestReview).not.toHaveBeenCalled();
    } finally {
      requestReview.mockRestore();
    }
  });
});

describe.each([
  ['spawn_coder', 'autoloopSpawnCoder'],
  ['spawn_reviewer', 'autoloopSpawnReviewer'],
] as const)('internal-only Autoloop primitive %s', (route, method) => {
  let manager: SessionManager;
  let server: EmbeddedServer;
  let port: number;
  let token: string;

  beforeAll(async () => {
    manager = new SessionManager({});
    server = new EmbeddedServer(manager, await freePort());
    port = await server.start();
    token = fs.readFileSync(path.join(os.homedir(), '.openclaw', 'server-token'), 'utf-8').trim();
  });
  afterAll(async () => {
    await server.stop();
    await manager.shutdown();
  });

  it('has no public HTTP route and causes no role effect', async () => {
    const selected = vi.spyOn(manager as never, method as never);
    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/run/${route}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: '{}',
      });
      expect(response.status).toBe(404);
      expect(selected).not.toHaveBeenCalled();
    } finally {
      selected.mockRestore();
    }
  });
});

describe('POST /autoloop/:id/delete', () => {
  let manager: SessionManager;
  let server: EmbeddedServer;
  let port: number;
  let token: string;

  beforeAll(async () => {
    manager = new SessionManager({});
    const ephemeral = await freePort();
    server = new EmbeddedServer(manager, ephemeral);
    port = await server.start();
    token = fs.readFileSync(path.join(os.homedir(), '.openclaw', 'server-token'), 'utf-8').trim();
  });
  afterAll(async () => {
    await server.stop();
    await manager.shutdown();
  });

  it('returns 200 when the run is deleted', async () => {
    const spy = vi.spyOn(manager, 'autoloopDelete').mockResolvedValue(true);
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/run-abc/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: '{}',
    });
    expect(r.status).toBe(200);
    const j = (await r.json()) as { ok: boolean };
    expect(j.ok).toBe(true);
    expect(spy).toHaveBeenCalledWith('run-abc');
  });

  it('returns 404 when nothing was removed', async () => {
    vi.spyOn(manager, 'autoloopDelete').mockResolvedValue(false);
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/missing/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: '{}',
    });
    expect(r.status).toBe(404);
  });
});

describe('GET /autoloop/list + POST /autoloop/:id/resume + GET /autoloop/:id/chat_history', () => {
  let manager: SessionManager;
  let server: EmbeddedServer;
  let port: number;
  let token: string;

  beforeAll(async () => {
    manager = new SessionManager({});
    const ephemeral = await freePort();
    server = new EmbeddedServer(manager, ephemeral);
    port = await server.start();
    token = fs.readFileSync(path.join(os.homedir(), '.openclaw', 'server-token'), 'utf-8').trim();
  });
  afterAll(async () => {
    await server.stop();
    await manager.shutdown();
  });

  it('serializes the state 404 through the safe Autoloop JSON boundary', async () => {
    // Production mutation caught: the successful state branch is snapshotted,
    // but its not-found sibling still uses generic JSON.stringify and lets an
    // inherited toJSON hook forge the exact legacy 404 body.
    const status = vi.spyOn(manager, 'autoloopStatus').mockReturnValue(undefined);
    const warmup = await fetch(`http://127.0.0.1:${port}/health`);
    expect(warmup.status).toBe(200);
    await warmup.arrayBuffer();
    const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    let hookCalls = 0;
    let observed: { status: number; contentType: string | null; body: string } | undefined;
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value() {
        hookCalls += 1;
        return { forged_state_not_found: true };
      },
    });

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/missing-safe-state/state`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      observed = {
        status: response.status,
        contentType: response.headers.get('content-type'),
        body: await response.text(),
      };
    } finally {
      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      status.mockRestore();
    }

    expect(hookCalls).toBe(0);
    expect(observed).toEqual({
      status: 404,
      contentType: 'application/json',
      body: '{"ok":false,"error":"run not found"}',
    });
  });

  it('serializes list state from own data without invoking inherited toJSON or own accessors', async () => {
    // Production mutation caught: the generic list JSON helper lets inherited
    // toJSON and enumerable state accessors rewrite or execute the response.
    const state = detachedState('list-safe-state', '/workspace', '/ledger');
    const expectedState = { ...state };
    const list = vi.spyOn(manager, 'autoloopList').mockReturnValue([state]);
    const warmup = await fetch(`http://127.0.0.1:${port}/health`);
    expect(warmup.status).toBe(200);
    await warmup.arrayBuffer();
    const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    const accessorKey = 'hostile_enumerable_accessor';
    const priorAccessor = Object.getOwnPropertyDescriptor(state, accessorKey);
    let inheritedHookCalls = 0;
    let ownAccessorCalls = 0;
    let observed: unknown;
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value(this: unknown) {
        inheritedHookCalls += 1;
        return this;
      },
    });
    Object.defineProperty(state, accessorKey, {
      configurable: true,
      enumerable: true,
      get() {
        ownAccessorCalls += 1;
        return 'list serializer invoked an own state accessor';
      },
    });

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/list`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      observed = {
        status: response.status,
        contentType: response.headers.get('content-type'),
        inheritedHookCalls,
        ownAccessorCalls,
        body: JSON.parse(await response.text()) as unknown,
      };
    } finally {
      if (priorAccessor) Object.defineProperty(state, accessorKey, priorAccessor);
      else delete (state as AutoloopState & Record<string, unknown>)[accessorKey];
      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      list.mockRestore();
    }

    expect(observed).toEqual({
      status: 200,
      contentType: 'application/json',
      inheritedHookCalls: 0,
      ownAccessorCalls: 0,
      body: { ok: true, runs: [expectedState] },
    });
  });

  it('serializes resumed state from own data without invoking inherited toJSON or own accessors', async () => {
    // Production mutation caught: routing successful resume state through the
    // generic JSON helper invokes inherited hooks and enumerable accessors.
    const state = detachedState('resume-safe-state', '/workspace', '/ledger');
    state.status = 'planning';
    const expectedState = { ...state };
    const resume = vi.spyOn(manager, 'autoloopResume').mockResolvedValue(state);
    const warmup = await fetch(`http://127.0.0.1:${port}/health`);
    expect(warmup.status).toBe(200);
    await warmup.arrayBuffer();
    const priorToJSON = Object.getOwnPropertyDescriptor(Object.prototype, 'toJSON');
    const accessorKey = 'hostile_enumerable_accessor';
    const priorAccessor = Object.getOwnPropertyDescriptor(state, accessorKey);
    let inheritedHookCalls = 0;
    let ownAccessorCalls = 0;
    let observed: unknown;
    Object.defineProperty(Object.prototype, 'toJSON', {
      configurable: true,
      value(this: unknown) {
        inheritedHookCalls += 1;
        return this;
      },
    });
    Object.defineProperty(state, accessorKey, {
      configurable: true,
      enumerable: true,
      get() {
        ownAccessorCalls += 1;
        return 'resume serializer invoked an own state accessor';
      },
    });

    try {
      const response = await fetch(`http://127.0.0.1:${port}/autoloop/resume-safe-state/resume`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: '{}',
      });
      observed = {
        status: response.status,
        contentType: response.headers.get('content-type'),
        inheritedHookCalls,
        ownAccessorCalls,
        body: JSON.parse(await response.text()) as unknown,
      };
    } finally {
      if (priorAccessor) Object.defineProperty(state, accessorKey, priorAccessor);
      else delete (state as AutoloopState & Record<string, unknown>)[accessorKey];
      if (priorToJSON) Object.defineProperty(Object.prototype, 'toJSON', priorToJSON);
      else delete (Object.prototype as { toJSON?: unknown }).toJSON;
      resume.mockRestore();
    }

    expect(observed).toEqual({
      status: 200,
      contentType: 'application/json',
      inheritedHookCalls: 0,
      ownAccessorCalls: 0,
      body: { ok: true, state: expectedState },
    });
  });

  it('resume returns the new in-memory state', async () => {
    vi.spyOn(manager, 'autoloopResume').mockResolvedValue({
      run_id: 'run-rsm',
      status: 'planning',
      iter: 0,
      subagents_spawned: false,
      started_at: '2026-05-13T10:00:00.000Z',
      workspace: '/tmp',
      ledger_dir: '/tmp/tasks/run-rsm',
      push_log_count: 0,
      status_reason: null,
      consecutive_phase_errors: 0,
      recent_phase_errors: [],
      metric_history: [],
      last_activity_at: 0,
    });
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/run-rsm/resume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: '{}',
    });
    expect(r.status).toBe(200);
    const j = (await r.json()) as { ok: boolean; state: { run_id: string; status: string } };
    expect(j.ok).toBe(true);
    expect(j.state.run_id).toBe('run-rsm');
    expect(j.state.status).toBe('planning');
    expect(manager.autoloopResume).toHaveBeenCalledWith('run-rsm', {
      plannerCustomEngine: undefined,
      coderCustomEngine: undefined,
      reviewerCustomEngine: undefined,
    });
  });

  it('resumes without accepting a custom engine from the request body', async () => {
    vi.spyOn(manager, 'autoloopResume').mockResolvedValue({
      run_id: 'custom-rsm',
      status: 'planning',
      iter: 0,
      subagents_spawned: false,
      started_at: '2026-05-13T10:00:00.000Z',
      workspace: '/tmp',
      ledger_dir: '/tmp/tasks/custom-rsm',
      push_log_count: 0,
      status_reason: null,
      consecutive_phase_errors: 0,
      recent_phase_errors: [],
      metric_history: [],
      last_activity_at: 0,
    });
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/custom-rsm/resume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({}),
    });

    expect(r.status).toBe(200);
    expect(manager.autoloopResume).toHaveBeenLastCalledWith('custom-rsm', {});
  });

  it('maps the approved timeout increase and pending identity alongside secret references', async () => {
    process.env.CLAWO_CUSTOM_ENGINE_PLANNER_REF = JSON.stringify({
      name: 'planner-safe',
      bin: '/opt/planner-safe',
      args: {},
    });
    const resume = vi.spyOn(manager, 'autoloopResume').mockResolvedValue({
      run_id: 'recoverable-rsm',
      status: 'running',
      iter: 2,
      subagents_spawned: true,
      started_at: '2026-05-13T10:00:00.000Z',
      workspace: '/tmp',
      ledger_dir: '/tmp/tasks/recoverable-rsm',
      push_log_count: 0,
      status_reason: null,
      pending_dispatch: null,
      consecutive_phase_errors: 0,
      recent_phase_errors: [],
      metric_history: [],
      last_activity_at: 0,
    });
    try {
      const r = await fetch(`http://127.0.0.1:${port}/autoloop/recoverable-rsm/resume`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({
          plannerCustomEngineRef: 'planner-ref',
          send_timeout_ms: 900_000,
          pending_dispatch_id: 'dispatch-planner-2',
        }),
      });

      expect(r.status).toBe(200);
      expect(resume).toHaveBeenLastCalledWith('recoverable-rsm', {
        plannerCustomEngine: { name: 'planner-safe', bin: '/opt/planner-safe', args: {} },
        sendTimeoutMs: 900_000,
        pendingDispatchId: 'dispatch-planner-2',
      });
    } finally {
      delete process.env.CLAWO_CUSTOM_ENGINE_PLANNER_REF;
    }
  });

  it('rejects invalid or unapproved resume controls without invoking autoloopResume', async () => {
    const resume = vi.spyOn(manager, 'autoloopResume');
    for (const body of [
      { send_timeout_ms: 4_999 },
      { send_timeout_ms: 7_200_001 },
      { send_timeout_ms: '700000' },
      { send_timeout_ms: null },
      { pending_dispatch_id: '' },
      { pending_dispatch_id: 42 },
      { activity_lease_ms: 2_000_000 },
      { autoloop_hard_timeout_ms: 90_000_000 },
      { allow_decrease: false },
    ]) {
      resume.mockClear();
      const r = await fetch(`http://127.0.0.1:${port}/autoloop/recoverable-rsm/resume`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify(body),
      });
      expect(r.status).toBe(400);
      expect(resume).not.toHaveBeenCalled();
    }
  });

  it('returns 400 when I4 rejects an equal or decreased timeout', async () => {
    const resume = vi
      .spyOn(manager, 'autoloopResume')
      .mockRejectedValue(new Error('sendTimeoutMs must be strictly greater than the current effective value 900000'));
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/recoverable-rsm/resume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ send_timeout_ms: 900_000, pending_dispatch_id: 'dispatch-planner-2' }),
    });

    expect(r.status).toBe(400);
    expect(resume).toHaveBeenCalledOnce();
  });

  it('refuses a custom engine supplied to resume over HTTP', async () => {
    const resumeSpy = vi.spyOn(manager, 'autoloopResume');
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/custom-rsm/resume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ planner_custom_engine: { name: 'pwn', bin: '/bin/sh', args: {} } }),
    });

    expect(r.status).toBe(400);
    const payload = (await r.json()) as { ok: boolean; error: string };
    expect(payload.error).toContain('may not be given as an inline config over HTTP');
    expect(resumeSpy).not.toHaveBeenCalled();
  });

  it('resume returns 400 when a custom engine config is required', async () => {
    vi.spyOn(manager, 'autoloopResume').mockRejectedValue(new Error('Planner custom engine config is required'));
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/custom-missing/resume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: '{}',
    });
    expect(r.status).toBe(400);
  });

  it('resume returns 404 when registry has no record', async () => {
    vi.spyOn(manager, 'autoloopResume').mockRejectedValue(new Error("Autoloop run 'nope' not found in registry"));
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/nope/resume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: '{}',
    });
    expect(r.status).toBe(404);
  });

  it('resume returns 500 for engine runtime failures rather than treating them as bad config', async () => {
    vi.spyOn(manager, 'autoloopResume').mockRejectedValue(
      new Error("Engine 'claude' circuit breaker open after 3 consecutive failures. Retry in 30s."),
    );
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/runtime-failure/resume`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: '{}',
    });
    expect(r.status).toBe(500);
  });

  it('chat_history reads <ledger>/chat.jsonl and returns entries', async () => {
    // Stage a fake ledger so /chat_history can read it.
    const tmpLedger = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-hist-'));
    fs.writeFileSync(
      path.join(tmpLedger, 'chat.jsonl'),
      [
        JSON.stringify({ who: 'user', text: 'hi', ts: '2026-05-13T01:00:00Z' }),
        JSON.stringify({ who: 'planner', text: 'hello back', ts: '2026-05-13T01:00:05Z' }),
      ].join('\n') + '\n',
    );
    vi.spyOn(manager, 'autoloopStatus').mockReturnValue({
      run_id: 'hist-run',
      status: 'terminated',
      iter: 0,
      subagents_spawned: false,
      started_at: '2026-05-13T01:00:00Z',
      workspace: '/tmp',
      ledger_dir: tmpLedger,
      push_log_count: 0,
      status_reason: 'historical',
      consecutive_phase_errors: 0,
      recent_phase_errors: [],
      metric_history: [],
      last_activity_at: 0,
    });
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/hist-run/chat_history`, {
      method: 'GET',
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(r.status).toBe(200);
    const j = (await r.json()) as { ok: boolean; entries: Array<{ who: string; text: string }> };
    expect(j.ok).toBe(true);
    expect(j.entries).toHaveLength(2);
    expect(j.entries[0].who).toBe('user');
    expect(j.entries[1].who).toBe('planner');
    fs.rmSync(tmpLedger, { recursive: true, force: true });
  });

  it('chat_history 404s when autoloopStatus has no record', async () => {
    vi.spyOn(manager, 'autoloopStatus').mockReturnValue(undefined);
    const r = await fetch(`http://127.0.0.1:${port}/autoloop/missing/chat_history`, {
      method: 'GET',
      headers: { Authorization: `Bearer ${token}` },
    });
    expect(r.status).toBe(404);
  });
});
