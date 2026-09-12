/**
 * Unit tests for SessionManager — the core orchestrator.
 *
 * Strategy: mock the ISession interface so no real CLI processes are spawned.
 * We test orchestration logic: lifecycle, concurrency guards, inbox, model
 * resolution, grep, ultraplan/ultrareview, and shutdown.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { execFileSync } from 'node:child_process';
import { EventEmitter } from 'node:events';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type {
  ISession,
  SessionConfig,
  SessionStats,
  SessionSendOptions,
  TurnResult,
  CostBreakdown,
  EffortLevel,
} from '../types.js';
import type { AgentReservationReleaseOptions, PhysicalAgentGeneration } from '../autoloop/types.js';
import type { PhaseErrorPayload } from '../autoloop/messages.js';
import type { PlannerToolCall } from '../autoloop/planner-tools.js';

// ─── Mock ISession ──────────────────────────────────────────────────────────

class MockSession extends EventEmitter implements ISession {
  sessionId?: string;
  conversationId?: string;
  threadId?: string;
  private _isReady = true;
  private _isPaused = false;
  private _isBusy = false;
  private _effort: EffortLevel = 'auto';
  private _history: Array<{ time: string; type: string; event: unknown }> = [];

  // Track calls for assertions
  startCalled = 0;
  stopCalled = 0;
  sendCalls: Array<{ message: string | unknown[]; options?: SessionSendOptions }> = [];
  compactCalls: string[] = [];
  /** Overrides the result event this session resolves with. */
  nextEvent?: Record<string, unknown>;
  /** Test seam for exercising real SessionManager/dispatcher send outcomes. */
  sendImplementation?: (
    message: string | unknown[],
    options?: SessionSendOptions,
  ) => Promise<TurnResult | { requestId: number; sent: boolean }>;
  /** Overrides `turnsSucceeded`, to simulate a turn the engine did not count. */
  turnsSucceededOverride?: number;

  get isReady() {
    return this._isReady;
  }
  get isPaused() {
    return this._isPaused;
  }
  get isBusy() {
    return this._isBusy;
  }

  setBusy(b: boolean) {
    this._isBusy = b;
  }
  setReady(r: boolean) {
    this._isReady = r;
  }

  async start(): Promise<this> {
    this.startCalled++;
    this.sessionId = `mock-session-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    return this;
  }

  stop(): void {
    this.stopCalled++;
  }

  pause(): void {
    this._isPaused = true;
  }

  resume(): void {
    this._isPaused = false;
  }

  async send(
    message: string | unknown[],
    options?: SessionSendOptions,
  ): Promise<TurnResult | { requestId: number; sent: boolean }> {
    this.sendCalls.push({ message, options });
    if (options?.waitForComplete === false) {
      return { requestId: 1, sent: true };
    }
    if (this.sendImplementation) return await this.sendImplementation(message, options);
    return {
      text: `response to: ${typeof message === 'string' ? message : JSON.stringify(message)}`,
      event: this.nextEvent ?? { type: 'result', result: 'done' },
    };
  }

  private _settledSends(): Array<{ message: string | unknown[]; options?: SessionSendOptions }> {
    return this.sendCalls.filter((c) => c.options?.waitForComplete !== false);
  }

  getStats(): SessionStats & { sessionId?: string; uptime: number } {
    return {
      // A fire-and-forget send has not settled a turn, so a real engine's counters
      // have not moved yet either.
      turns: this._settledSends().length,
      turnsSucceeded: this.turnsSucceededOverride ?? this._settledSends().length,
      toolCalls: 0,
      toolErrors: 0,
      tokensIn: 100,
      tokensOut: 50,
      cachedTokens: 0,
      costUsd: 0.01,
      isReady: this._isReady,
      startTime: new Date().toISOString(),
      lastActivity: new Date().toISOString(),
      contextPercent: 5,
      retries: 0,
      sessionId: this.sessionId,
      uptime: 60,
    };
  }

  getHistory(limit?: number): Array<{ time: string; type: string; event: unknown }> {
    const h = this._history;
    return limit ? h.slice(-limit) : h;
  }

  addHistory(entries: Array<{ time: string; type: string; event: unknown }>) {
    this._history.push(...entries);
  }

  getCost(): CostBreakdown {
    return {
      model: 'mock-model',
      tokensIn: 100,
      tokensOut: 50,
      cachedTokens: 0,
      pricing: { inputPer1M: 3, outputPer1M: 15, cachedPer1M: 0.3 },
      breakdown: { inputCost: 0.0003, cachedCost: 0, outputCost: 0.00075 },
      totalUsd: 0.00105,
    };
  }

  async compact(summary?: string): Promise<TurnResult | { requestId: number; sent: boolean }> {
    this.compactCalls.push(summary || '');
    return { text: 'compacted', event: { type: 'result' } };
  }

  getEffort(): EffortLevel {
    return this._effort;
  }
  setEffort(level: EffortLevel): void {
    this._effort = level;
  }
  resolveModel(alias: string): string {
    return alias;
  }
}

// ─── Mock Factory ─────────────────────────────────────────────────────────

let mockSessions: MockSession[] = [];
let createdConfigs: SessionConfig[] = [];

/**
 * We intercept the _createSession private method to inject MockSession
 * instances instead of real PersistentClaudeSession / PersistentCodexSession.
 */
function patchCreateSession(manager: InstanceType<typeof SessionManager>): void {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (manager as any)._createSession = (_engine: string, _config: SessionConfig): ISession => {
    const mock = new MockSession();
    if (_engine === 'agy' && _config.resumeSessionId) mock.conversationId = _config.resumeSessionId;
    mockSessions.push(mock);
    createdConfigs.push(_config);
    return mock;
  };
}

// ─── Mock fs for persistence tests ──────────────────────────────────────────

// We mock the module-level persistence functions by mocking the node:fs module
// BEFORE importing SessionManager. However, SessionManager also uses fs for
// agents/skills/rules, so we only mock what we need.

// The kernel's run store writes under CLAWO_WF_DIR. Redirect it for the whole
// file: these tests use fixed run ids, and without this they would write into —
// and collide inside — the developer's real ~/.claw-orchestrator/wf.
const TEST_WF_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-sm-wf-'));
process.env.CLAWO_WF_DIR = TEST_WF_DIR;

const persistenceFsState = vi.hoisted(() => ({
  files: new Map<string, string>(),
  descriptors: new Map<number, string>(),
  openPaths: new Map<number, string>(),
  nextDescriptor: 1_000_000,
}));

vi.mock('node:fs', async () => {
  const actual = await vi.importActual<typeof import('node:fs')>('node:fs');

  // Only the two files SessionManager persists into the developer's real
  // ~/.openclaw are stubbed. Everything else passes through.
  //
  // This used to no-op every write in the process, which kept the home
  // directory clean and quietly broke any other code that touched the disk —
  // the run store writes its checkpoints through the same `fs`, so a
  // kernel-backed mode looked like it had lost every run. A mock that stubs a
  // whole module to protect two paths is a landmine; this one names them.
  const isProtectedDataFile = (p: unknown): p is string =>
    typeof p === 'string' &&
    (p.endsWith('/claude-sessions.json') ||
      p.endsWith('/claude-sessions.json.tmp') ||
      p.endsWith('/session-pids.json'));
  const missingFile = (p: string): NodeJS.ErrnoException =>
    Object.assign(new Error(`ENOENT: no such file or directory, open '${p}'`), { code: 'ENOENT' });

  const existsSync = vi.fn((p: string) =>
    isProtectedDataFile(p) ? persistenceFsState.files.has(p) : actual.existsSync(p),
  );
  const readFileSync = vi.fn((p: string, enc?: string) => {
    if (!isProtectedDataFile(p)) return actual.readFileSync(p, enc as BufferEncoding);
    const value = persistenceFsState.files.get(p);
    if (value === undefined) throw missingFile(p);
    return value;
  });
  const writeFileSync = vi.fn((p: unknown, ...rest: unknown[]) => {
    if (isProtectedDataFile(p)) {
      const data = rest[0];
      persistenceFsState.files.set(p, Buffer.isBuffer(data) ? data.toString() : String(data));
      return;
    }
    (actual.writeFileSync as (...a: unknown[]) => void)(p, ...rest);
  });
  const appendFileSync = vi.fn((p: unknown, ...rest: unknown[]) => {
    if (isProtectedDataFile(p)) return;
    (actual.appendFileSync as (...a: unknown[]) => void)(p, ...rest);
  });
  const openSync = vi.fn((p: unknown, ...rest: unknown[]) => {
    if (isProtectedDataFile(p)) {
      const flags = String(rest[0] ?? 'r');
      if (flags.includes('r') && !persistenceFsState.files.has(p)) throw missingFile(p);
      const fd = persistenceFsState.nextDescriptor++;
      persistenceFsState.descriptors.set(fd, p);
      return fd;
    }
    const fd = (actual.openSync as (...a: unknown[]) => number)(p, ...rest);
    if (typeof p === 'string') persistenceFsState.openPaths.set(fd, p);
    return fd;
  });
  const writeSync = vi.fn((fd: unknown, ...rest: unknown[]) =>
    (actual.writeSync as (...a: unknown[]) => number)(fd, ...rest),
  );
  const fsyncSync = vi.fn((fd: number) => {
    if (persistenceFsState.descriptors.has(fd)) return;
    return actual.fsyncSync(fd);
  });
  const closeSync = vi.fn((fd: number) => {
    if (persistenceFsState.descriptors.delete(fd)) return;
    persistenceFsState.openPaths.delete(fd);
    return actual.closeSync(fd);
  });
  const mkdirSync = vi.fn((p: unknown, ...rest: unknown[]) => {
    return (actual.mkdirSync as (...a: unknown[]) => string | undefined)(p, ...rest);
  });
  const renameSync = vi.fn((from: unknown, to: unknown) => {
    if (isProtectedDataFile(from) && isProtectedDataFile(to)) {
      const value = persistenceFsState.files.get(from);
      if (value === undefined) throw missingFile(from);
      persistenceFsState.files.set(to, value);
      persistenceFsState.files.delete(from);
      return;
    }
    (actual.renameSync as (...a: unknown[]) => void)(from, to);
  });

  const unlinkSync = vi.fn((p: unknown) => {
    if (isProtectedDataFile(p)) {
      if (!persistenceFsState.files.delete(p)) throw missingFile(p);
      return;
    }
    (actual.unlinkSync as (path: unknown) => void)(p);
  });

  const shim = {
    existsSync,
    readFileSync,
    writeFileSync,
    appendFileSync,
    openSync,
    writeSync,
    fsyncSync,
    closeSync,
    mkdirSync,
    renameSync,
    unlinkSync,
    // The async persistence path is stubbed wholesale: nothing else in the
    // codebase uses these callback forms.
    writeFile: vi.fn((p: unknown, data: unknown, cb: (err: null) => void) => {
      if (isProtectedDataFile(p)) {
        persistenceFsState.files.set(p, Buffer.isBuffer(data) ? data.toString() : String(data));
      }
      cb(null);
    }),
    rename: vi.fn((from: unknown, to: unknown, cb: (err: NodeJS.ErrnoException | null) => void) => {
      if (isProtectedDataFile(from) && isProtectedDataFile(to)) {
        const value = persistenceFsState.files.get(from);
        if (value === undefined) return cb(missingFile(from));
        persistenceFsState.files.set(to, value);
        persistenceFsState.files.delete(from);
      }
      cb(null);
    }),
    mkdir: vi.fn((_p: unknown, _opts: unknown, cb: (err: null) => void) => cb(null)),
    unlink: vi.fn((p: unknown, cb: () => void) => {
      if (isProtectedDataFile(p)) persistenceFsState.files.delete(p);
      cb();
    }),
  };

  return { ...actual, ...shim, default: { ...actual, ...shim } };
});

// Import AFTER mocking fs
const { SessionManager } = await import('../session-manager.js');
const { Msg: AutoloopMsg } = await import('../autoloop/messages.js');
const { AutoloopOperationError } = await import('../autoloop/dispatcher.js');
const { applyValidatedPlannerToolCalls, validatePlannerToolCalls } = await import('../autoloop/planner-tools.js');

const SESSION_REGISTRY_FILE = path.join(os.homedir(), '.openclaw', 'claude-sessions.json');
const SESSION_PID_FILE = path.join(os.homedir(), '.openclaw', 'session-pids.json');
const nativeSetImmediate = setImmediate;

function isOpenPath(file: unknown, expectedPath: string): boolean {
  return (
    String(file) === expectedPath ||
    (typeof file === 'number' && persistenceFsState.openPaths.get(file) === expectedPath)
  );
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function seedCompleteLegacyReviewArtifacts(workspace: string, runId: string, iter: number): void {
  const iterDirectory = path.join(workspace, 'tasks', runId, 'iter', String(iter));
  fs.mkdirSync(iterDirectory, { recursive: true, mode: 0o700 });
  for (const [name, content] of Object.entries({
    'directive.json': '{}\n',
    'eval_output.json': '{}\n',
    'coder_summary.txt': 'complete\n',
    'diff.patch': 'diff --git a/a b/a\n',
  })) {
    fs.writeFileSync(path.join(iterDirectory, name), content, { mode: 0o600 });
  }
}

function successfulRoleReplyFromDeliveryPrompt(
  role: 'coder' | 'reviewer',
  message: string | unknown[],
  summary: string,
): string {
  expect(typeof message, `${role} durable delivery prompt`).toBe('string');
  const provenanceMatch = /<autoloop_delivery delivery_id="([^"]+)" payload_sha256="([a-f0-9]{64})">/.exec(
    String(message),
  );
  expect(provenanceMatch, `${role} durable delivery provenance`).not.toBeNull();
  const provenance = { delivery_id: provenanceMatch![1], payload_sha256: provenanceMatch![2] };
  const completion =
    role === 'coder'
      ? { tool: 'iter_complete', args: { summary, eval_output: {}, files_changed: [], ...provenance } }
      : { tool: 'review_complete', args: { decision: 'hold', metric: null, audit_notes: summary, ...provenance } };
  return [summary, '```autoloop', JSON.stringify(completion), '```'].join('\n');
}

function createManager(overrides?: Record<string, unknown>): InstanceType<typeof SessionManager> {
  const mgr = new SessionManager({
    claudeBin: 'mock-claude',
    maxConcurrentSessions: 5,
    sessionTtlMinutes: 120,
    defaultPermissionMode: 'acceptEdits',
    defaultEffort: 'auto',
    ...overrides,
  });
  patchCreateSession(mgr);
  return mgr;
}

function lastMock(): MockSession {
  return mockSessions[mockSessions.length - 1];
}

function managerGeneration(
  sessionName: string,
  overrides: Partial<PhysicalAgentGeneration> = {},
): PhysicalAgentGeneration {
  return {
    role: 'planner',
    generation: 1,
    session_name: sessionName,
    session_id: 'physical-session-1',
    owner_instance_id: 'owner-1',
    created_at: '2026-09-05T10:00:00.000Z',
    last_activity_at: '2026-09-05T11:00:00.000Z',
    lease_expires_at: '2026-09-05T11:30:00.000Z',
    state: 'stale',
    ...overrides,
  };
}

function mockSharedPidContents(contents: string): void {
  persistenceFsState.files.set(SESSION_PID_FILE, contents);
}

function mockSharedPidFile(entries: Record<string, unknown>): void {
  mockSharedPidContents(JSON.stringify(entries));
}

function persistManagerRegistry(manager: InstanceType<typeof SessionManager>): void {
  const reservations = (
    manager as unknown as {
      persistedSessions: Map<string, Record<string, unknown>>;
    }
  ).persistedSessions;
  persistenceFsState.files.set(SESSION_REGISTRY_FILE, JSON.stringify(Array.from(reservations.values())));
}

type ManagerReleaseOptions = AgentReservationReleaseOptions;

function uncheckedReleaseOptions(options: Record<string, unknown>): ManagerReleaseOptions {
  return options as unknown as ManagerReleaseOptions;
}

function runGit(workspace: string, ...args: string[]): string {
  return execFileSync('git', args, { cwd: workspace, encoding: 'utf8' });
}

function initializeGitWorkspace(workspace: string): string {
  runGit(workspace, 'init', '--quiet');
  runGit(workspace, 'config', 'user.name', 'Autoloop Test');
  runGit(workspace, 'config', 'user.email', 'autoloop-test@example.invalid');
  fs.writeFileSync(path.join(workspace, 'README.md'), 'baseline\n');
  runGit(workspace, 'add', '--', 'README.md');
  runGit(workspace, 'commit', '--quiet', '-m', 'test baseline');
  return runGit(workspace, 'rev-parse', 'HEAD').trim();
}

// ─── Tests ──────────────────────────────────────────────────────────────────

describe('SessionManager', () => {
  let mgr: InstanceType<typeof SessionManager>;

  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    persistenceFsState.files.clear();
    persistenceFsState.descriptors.clear();
    persistenceFsState.nextDescriptor = 1_000_000;
    mockSessions = [];
    createdConfigs = [];
    // Fresh run store per test. These cases use fixed run ids, and the store
    // now refuses to reuse one — which is the point, but it means the tests
    // have to start from an empty directory rather than leaking into each other.
    fs.rmSync(TEST_WF_DIR, { recursive: true, force: true });
    fs.mkdirSync(TEST_WF_DIR, { recursive: true });
    mgr = createManager();
  });

  afterEach(async () => {
    await mgr.shutdown();
    vi.useRealTimers();
  });

  // ─── Session Lifecycle ──────────────────────────────────────────────

  describe('session lifecycle', () => {
    it('startSession creates a session and returns SessionInfo', async () => {
      const info = await mgr.startSession({ name: 'test1', cwd: '/tmp' });

      expect(info.name).toBe('test1');
      expect(info.cwd).toBe('/tmp');
      expect(info.created).toBeDefined();
      expect(info.stats).toBeDefined();
      expect(info.stats.isReady).toBe(true);
      expect(lastMock().startCalled).toBe(1);
    });

    // ── "Do not save session to disk" means both stores.
    //
    //    The flag reached the engine (Claude Code's --no-session-persistence)
    //    but not this orchestrator's own registry, which is what auto-resume
    //    reads — so `clawo session-start x --skip-persistence` twice silently
    //    reattached to the first conversation.
    it('noSessionPersistence keeps the session out of the resume registry', async () => {
      await mgr.startSession({ name: 'ephemeral', cwd: '/tmp', noSessionPersistence: true });
      expect((mgr as unknown as { persistedSessions: Map<string, unknown> }).persistedSessions.has('ephemeral')).toBe(
        false,
      );
    });

    it('still registers an ordinary session', async () => {
      // Half the contract: skipping everything would be just as wrong.
      await mgr.startSession({ name: 'ordinary', cwd: '/tmp' });
      expect((mgr as unknown as { persistedSessions: Map<string, unknown> }).persistedSessions.has('ordinary')).toBe(
        true,
      );
    });

    // ── TTL cleanup must forget the PID it stopped.
    //
    //    `stopSession` deletes from `_activePids` and saves; the TTL path did
    //    not, so the map only ever grew and the next save rewrote dead PIDs to
    //    disk under the current owner. After an unclean exit those come back as
    //    orphan candidates and get probed — and a PID the OS has recycled to a
    //    coding-CLI-shaped process is killed.
    it('idle cleanup forgets the PID along with the session', async () => {
      await mgr.startSession({ name: 'idle-one', cwd: '/tmp' });
      const internals = mgr as unknown as {
        _activePids: Map<string, number>;
        sessions: Map<string, { lastActivity: number }>;
        _cleanupIdleSessions(): void;
      };
      internals._activePids.set('idle-one', 424242);
      internals.sessions.get('idle-one')!.lastActivity = 0; // long past the TTL

      internals._cleanupIdleSessions();

      expect(internals.sessions.has('idle-one')).toBe(false);
      expect(internals._activePids.has('idle-one')).toBe(false);
    });

    it('idle cleanup leaves a live session and its PID alone', async () => {
      await mgr.startSession({ name: 'busy-one', cwd: '/tmp' });
      const internals = mgr as unknown as {
        _activePids: Map<string, number>;
        sessions: Map<string, unknown>;
        _cleanupIdleSessions(): void;
      };
      internals._activePids.set('busy-one', 424243);

      internals._cleanupIdleSessions();

      expect(internals.sessions.has('busy-one')).toBe(true);
      expect(internals._activePids.get('busy-one')).toBe(424243);
    });

    // ── One proxy server, however many sessions start at once.
    //
    //    The `if (this._proxyPort)` guard is checked synchronously but the port
    //    is assigned inside listen()'s callback, several awaits later. Council
    //    and fanout start their agents with Promise.all under distinct names,
    //    and `_pendingSessions` only serialises per name — so two callers each
    //    bound a server and shutdown() closed only the last one.
    it('shares one proxy startup between concurrent callers', async () => {
      const internals = mgr as unknown as {
        _ensureProxyServer(): Promise<number | null>;
        _startProxyServer(): Promise<number | null>;
      };
      let starts = 0;
      internals._startProxyServer = async () => {
        starts++;
        await new Promise((r) => setTimeout(r, 20));
        return 4242;
      };

      const ports = await Promise.all([
        internals._ensureProxyServer(),
        internals._ensureProxyServer(),
        internals._ensureProxyServer(),
      ]);

      expect(starts).toBe(1);
      expect(ports).toEqual([4242, 4242, 4242]);
    });

    it('startSession returns existing session without re-creating', async () => {
      const info1 = await mgr.startSession({ name: 'dup', cwd: '/tmp' });
      const info2 = await mgr.startSession({ name: 'dup', cwd: '/other' });

      expect(info1.name).toBe(info2.name);
      // Only one mock was created
      expect(mockSessions.length).toBe(1);
    });

    it('startSession generates name if none provided', async () => {
      const info = await mgr.startSession({ cwd: '/tmp' });
      expect(info.name).toMatch(/^session-\d+$/);
    });

    it('stopSession removes the session', async () => {
      await mgr.startSession({ name: 'to-stop', cwd: '/tmp' });
      expect(mgr.listSessions().length).toBe(1);

      await mgr.stopSession('to-stop');
      expect(mgr.listSessions().length).toBe(0);
      expect(lastMock().stopCalled).toBe(1);
    });

    it('stopSession throws for unknown session', async () => {
      await expect(mgr.stopSession('nonexistent')).rejects.toThrow("Session 'nonexistent' not found");
    });

    it('listSessions returns all active sessions', async () => {
      await mgr.startSession({ name: 'a', cwd: '/tmp' });
      await mgr.startSession({ name: 'b', cwd: '/tmp' });
      await mgr.startSession({ name: 'c', cwd: '/tmp' });

      const list = mgr.listSessions();
      expect(list.length).toBe(3);
      expect(list.map((s) => s.name).sort()).toEqual(['a', 'b', 'c']);
    });

    it('getStatus returns detailed session info', async () => {
      await mgr.startSession({ name: 'status-test', cwd: '/tmp' });
      const status = mgr.getStatus('status-test');

      expect(status.name).toBe('status-test');
      expect(status.stats.uptime).toBeDefined();
      expect(status.stats.isReady).toBe(true);
    });

    it('getStatus throws for unknown session', () => {
      expect(() => mgr.getStatus('nope')).toThrow("Session 'nope' not found");
    });
  });

  describe('autoloop agent runtime probe', () => {
    it('types rollback and normal release as explicit mutually exclusive tuples', () => {
      const rollback = {
        rollbackUncommittedReservation: true,
        expectedOwnerInstanceId: 'reservation-owner',
        expectedSessionId: 'reservation-session',
      } satisfies AgentReservationReleaseOptions;
      const normal = {
        expectedOwnerInstanceId: 'reservation-owner',
        expectedSessionId: undefined,
        releaseOwnerInstanceId: 'release-owner',
      } satisfies AgentReservationReleaseOptions;

      // @ts-expect-error release without ownership evidence is never valid
      const empty: AgentReservationReleaseOptions = {};
      // @ts-expect-error rollback requires the exact physical session
      const incompleteRollback: AgentReservationReleaseOptions = {
        rollbackUncommittedReservation: true,
        expectedOwnerInstanceId: 'reservation-owner',
      };
      // @ts-expect-error normal release requires a durable release claimant
      const incompleteNormal: AgentReservationReleaseOptions = {
        expectedOwnerInstanceId: 'reservation-owner',
        expectedSessionId: 'reservation-session',
      };

      expect([rollback, normal, empty, incompleteRollback, incompleteNormal]).toHaveLength(5);
    });

    it('rejects an opaque release owner before it can persist a pending fence', async () => {
      const sessionName = 'autoloop-probe-opaque-release-owner-planner';
      const first = managerGeneration(sessionName);
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      let evidenceHookCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      const before = structuredClone(reservations.get(sessionName));
      await expect(
        mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: 'opaque-owner',
          beforeRelease: () => {
            evidenceHookCount += 1;
          },
          persistReleaseEvidence: () => {
            evidenceHookCount += 1;
          },
        }),
      ).rejects.toMatchObject({
        code: 'AUTOLOOP_AGENT_RELEASE_OWNER_INVALID',
        retryable: false,
      });

      expect(reservations.get(sessionName)).toEqual(before);
      expect(evidenceHookCount).toBe(0);
    });

    it('reports shared PID ownership by another live manager as live', async () => {
      const sessionName = 'autoloop-probe-shared-live-planner';
      mockSharedPidFile({
        [sessionName]: {
          pid: process.pid,
          ownerPid: process.ppid,
          since: '2026-09-05T10:00:00.000Z',
        },
      });

      await expect(mgr.inspect(sessionName)).resolves.toBe('live');
    });

    it('keeps a dead owner live child unknown and reports absent only when both PIDs are dead', async () => {
      const lingeringSessionName = 'autoloop-probe-shared-lingering-planner';
      const deadSessionName = 'autoloop-probe-shared-dead-planner';
      const deadOwnerPid = 2_000_000_000;
      const deadChildPid = 2_000_000_001;
      expect(() => process.kill(deadOwnerPid, 0)).toThrow();
      expect(() => process.kill(deadChildPid, 0)).toThrow();
      mockSharedPidFile({
        [lingeringSessionName]: {
          pid: process.pid,
          ownerPid: deadOwnerPid,
          since: '2026-09-05T10:00:00.000Z',
        },
        [deadSessionName]: {
          pid: deadChildPid,
          ownerPid: deadOwnerPid,
          since: '2026-09-05T10:00:00.000Z',
        },
      });

      await expect(mgr.inspect(lingeringSessionName)).resolves.toBe('unknown');
      await expect(mgr.inspect(deadSessionName)).resolves.toBe('absent');
    });

    it('does not infer liveness from a self PID when local ownership evidence is stale', async () => {
      const deadChildPid = 2_000_000_011;
      const indeterminateChildPid = 2_000_000_012;
      const originalKill = process.kill.bind(process);
      const killSpy = vi.spyOn(process, 'kill').mockImplementation(((pid: number, signal?: number | NodeJS.Signals) => {
        if (pid === deadChildPid) throw Object.assign(new Error('missing child'), { code: 'ESRCH' });
        if (pid === indeterminateChildPid) throw Object.assign(new Error('probe denied'), { code: 'EPERM' });
        return originalKill(pid, signal);
      }) as typeof process.kill);
      try {
        for (const [sessionName, childPid, expected] of [
          ['autoloop-probe-self-stale-dead', deadChildPid, 'absent'],
          ['autoloop-probe-self-stale-live', process.ppid, 'unknown'],
          ['autoloop-probe-self-stale-indeterminate', indeterminateChildPid, 'unknown'],
        ] as const) {
          mockSharedPidFile({
            [sessionName]: {
              pid: childPid,
              ownerPid: process.pid,
              since: '2026-09-05T10:00:00.000Z',
            },
          });
          await expect(mgr.inspect(sessionName)).resolves.toBe(expected);
        }
      } finally {
        killSpy.mockRestore();
      }
    });

    it('keeps malformed and incomplete shared PID evidence conservatively unknown', async () => {
      const sessionName = 'autoloop-probe-malformed-pid-evidence';
      const malformedInputs = [
        '{',
        '[]',
        '42',
        JSON.stringify({ [sessionName]: 42 }),
        JSON.stringify({ [sessionName]: { pid: 1234 } }),
        JSON.stringify({ [sessionName]: { pid: 0, ownerPid: 'invalid' } }),
      ];

      for (const contents of malformedInputs) {
        mockSharedPidContents(contents);
        await expect(mgr.inspect(sessionName)).resolves.toBe('unknown');
      }
    });

    it('fails closed when the shared session registry is corrupt, unreadable, or not an array', () => {
      const existingName = 'autoloop-probe-authority-existing-planner';
      const attemptedName = 'autoloop-probe-authority-attempted-planner';
      const existing = managerGeneration(existingName);
      const attempted = managerGeneration(attemptedName);
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;

      expect(mgr.reserveAgentGeneration(existing, '/tmp')).toBe(true);
      const inMemoryBefore = structuredClone(Array.from(reservations.entries()));

      for (const invalidAuthority of ['{', JSON.stringify({ entries: [] })]) {
        persistenceFsState.files.set(SESSION_REGISTRY_FILE, invalidAuthority);

        expect(() => mgr.reserveAgentGeneration(attempted, '/tmp')).toThrow(
          expect.objectContaining({
            code: 'AUTOLOOP_AGENT_REGISTRY_CORRUPT',
            retryable: false,
          }),
        );
        expect(persistenceFsState.files.get(SESSION_REGISTRY_FILE)).toBe(invalidAuthority);
        expect(Array.from(reservations.entries())).toEqual(inMemoryBefore);
      }

      const validAuthority = JSON.stringify(Array.from(reservations.values()));
      persistenceFsState.files.set(SESSION_REGISTRY_FILE, validAuthority);
      const readFile = vi.mocked(fs.readFileSync).getMockImplementation()!;
      vi.mocked(fs.readFileSync).mockImplementationOnce(((file: string, ...args: unknown[]) => {
        if (file === SESSION_REGISTRY_FILE) {
          throw Object.assign(new Error('registry read denied'), { code: 'EACCES' });
        }
        return (readFile as unknown as (file: string, ...rest: unknown[]) => unknown)(file, ...args);
      }) as typeof fs.readFileSync);

      expect(() => mgr.reserveAgentGeneration(attempted, '/tmp')).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_AGENT_REGISTRY_READ_FAILED',
          retryable: true,
        }),
      );
      expect(persistenceFsState.files.get(SESSION_REGISTRY_FILE)).toBe(validAuthority);
      expect(Array.from(reservations.entries())).toEqual(inMemoryBefore);
    });

    it("keeps one name's unflushed resume metadata when another name enters registry CAS", async () => {
      const firstName = 'autoloop-probe-unflushed-first-planner';
      const secondName = 'autoloop-probe-unflushed-second-planner';
      const first = managerGeneration(firstName);
      const second = managerGeneration(secondName, {
        session_id: 'physical-session-2',
        owner_instance_id: 'owner-2',
      });
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      await mgr.startSession({ name: firstName, cwd: '/tmp' }, first);
      const unflushedResumeId = reservations.get(firstName)?.claudeSessionId;
      expect(unflushedResumeId).toMatch(/^mock-session-/);
      expect(
        JSON.parse(persistenceFsState.files.get(SESSION_REGISTRY_FILE)!) as Array<Record<string, unknown>>,
      ).toEqual(expect.arrayContaining([expect.objectContaining({ name: firstName, claudeSessionId: '' })]));

      expect(mgr.reserveAgentGeneration(second, '/tmp')).toBe(true);
      expect(reservations.get(firstName)?.claudeSessionId).toBe(unflushedResumeId);
    });

    it('requires the full generation owner and session tuple to release an active reservation', async () => {
      const invalidOptions = [
        { expectedSessionId: 'physical-session-1' },
        { expectedOwnerInstanceId: 'owner-1' },
        { expectedOwnerInstanceId: 'owner-wrong', expectedSessionId: 'physical-session-1' },
        { expectedOwnerInstanceId: 'owner-1', expectedSessionId: 'physical-session-wrong' },
      ];
      let evidenceHookCount = 0;
      const results: boolean[] = [];

      for (const [index, tuple] of invalidOptions.entries()) {
        const sessionName = `autoloop-probe-incomplete-tuple-${index}-planner`;
        const generation = managerGeneration(sessionName);
        const reservations = (
          mgr as unknown as {
            persistedSessions: Map<string, Record<string, unknown>>;
          }
        ).persistedSessions;
        expect(mgr.reserveAgentGeneration(generation, '/tmp')).toBe(true);
        const before = structuredClone(reservations.get(sessionName));
        results.push(
          await mgr.releaseReservation(
            sessionName,
            generation.generation,
            uncheckedReleaseOptions({
              ...tuple,
              releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
              beforeRelease: () => {
                evidenceHookCount += 1;
              },
              persistReleaseEvidence: () => {
                evidenceHookCount += 1;
              },
            }),
          ),
        );
        expect(reservations.get(sessionName)).toEqual(before);
        expect(reservations.get(sessionName)).not.toHaveProperty('agentReleasePending');
      }

      expect(results).toEqual([false, false, false, false]);
      expect(evidenceHookCount).toBe(0);
    });

    it('rolls back an uncommitted generation without evidence and permits a replacement reservation', async () => {
      const sessionName = 'autoloop-probe-uncommitted-rollback-planner';
      const first = managerGeneration(sessionName);
      const replacement = managerGeneration(sessionName, {
        session_id: 'physical-session-replacement',
        owner_instance_id: 'owner-replacement',
      });
      let evidenceHookCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      await expect(
        mgr.releaseReservation(
          sessionName,
          first.generation,
          uncheckedReleaseOptions({
            expectedOwnerInstanceId: first.owner_instance_id,
            expectedSessionId: first.session_id,
            rollbackUncommittedReservation: true,
            beforeRelease: () => {
              evidenceHookCount += 1;
            },
            persistReleaseEvidence: () => {
              evidenceHookCount += 1;
            },
          }),
        ),
      ).resolves.toBe(true);

      expect(evidenceHookCount).toBe(0);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(true);
    });

    it('probes exact released-generation reusability atomically and rejects owner or session mismatch', async () => {
      const sessionName = 'autoloop-reset-probe-planner';
      const generation = managerGeneration(sessionName);
      expect(mgr.reserveAgentGeneration(generation, '/tmp')).toBe(true);
      await expect(
        mgr.releaseReservation(sessionName, generation.generation, {
          expectedOwnerInstanceId: generation.owner_instance_id,
          expectedSessionId: generation.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => undefined,
          persistReleaseEvidence: () => undefined,
        }),
      ).resolves.toBe(true);
      const before = persistenceFsState.files.get(SESSION_REGISTRY_FILE);
      const managerProbe = mgr as unknown as {
        probeAgentNameReusable?: (name: string, released?: PhysicalAgentGeneration) => boolean;
      };

      expect(managerProbe.probeAgentNameReusable?.(sessionName, generation)).toBe(true);
      expect(
        managerProbe.probeAgentNameReusable?.(sessionName, {
          ...generation,
          owner_instance_id: 'wrong-owner',
        }),
      ).toBe(false);
      expect(
        managerProbe.probeAgentNameReusable?.(sessionName, {
          ...generation,
          session_id: 'wrong-session',
        }),
      ).toBe(false);
      expect(persistenceFsState.files.get(SESSION_REGISTRY_FILE)).toBe(before);
    });

    it('restores an uncommitted reservation when rollback persistence fails and permits a safe retry', async () => {
      const sessionName = 'autoloop-probe-uncommitted-rollback-save-failure-planner';
      const first = managerGeneration(sessionName);
      const replacement = managerGeneration(sessionName, {
        session_id: 'replacement-physical-session',
        owner_instance_id: 'replacement-owner',
      });
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      const rollbackOptions: AgentReservationReleaseOptions = {
        rollbackUncommittedReservation: true,
        expectedOwnerInstanceId: first.owner_instance_id,
        expectedSessionId: first.session_id!,
      };

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      const before = structuredClone(reservations.get(sessionName));
      vi.mocked(fs.renameSync).mockImplementationOnce(() => {
        throw new Error('rollback registry rename failed');
      });

      await expect(mgr.releaseReservation(sessionName, first.generation, rollbackOptions)).rejects.toMatchObject({
        code: 'AUTOLOOP_AGENT_REGISTRY_PERSIST_FAILED',
      });
      expect(reservations.get(sessionName)).toEqual(before);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(false);

      await expect(mgr.releaseReservation(sessionName, first.generation, rollbackOptions)).resolves.toBe(true);
      expect(reservations.has(sessionName)).toBe(false);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(true);
    });

    it('restores the prior released tombstone when an uncommitted replacement rolls back', async () => {
      const sessionName = 'autoloop-probe-uncommitted-tombstone-planner';
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      reservations.set(sessionName, {
        name: sessionName,
        claudeSessionId: 'prior-session-id',
        cwd: '/tmp',
        originalCreated: '2026-09-05T09:00:00.000Z',
        lastResumed: '2026-09-05T09:00:00.000Z',
        lastActivity: Date.parse('2026-09-05T09:00:00.000Z'),
        agentReleasedGeneration: 1,
        agentReleasedOwnerInstanceId: 'prior-owner',
        agentReleasedSessionId: 'prior-physical-session',
      });
      persistManagerRegistry(mgr);
      const uncommitted = managerGeneration(sessionName, {
        generation: 2,
        session_id: 'uncommitted-physical-session',
        owner_instance_id: 'uncommitted-owner',
      });
      const replacement = managerGeneration(sessionName, {
        generation: 2,
        session_id: 'replacement-physical-session',
        owner_instance_id: 'replacement-owner',
      });

      expect(mgr.reserveAgentGeneration(uncommitted, '/tmp')).toBe(true);
      await expect(
        mgr.releaseReservation(sessionName, uncommitted.generation, {
          expectedOwnerInstanceId: uncommitted.owner_instance_id,
          expectedSessionId: uncommitted.session_id,
          rollbackUncommittedReservation: true,
        } as ManagerReleaseOptions),
      ).resolves.toBe(true);

      expect(reservations.get(sessionName)).toMatchObject({
        agentGeneration: undefined,
        agentReleasedGeneration: 1,
        agentReleasedOwnerInstanceId: 'prior-owner',
        agentReleasedSessionId: 'prior-physical-session',
      });
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(true);
    });

    it('generation-fences release of a stale registry-only reservation', async () => {
      const sessionName = 'autoloop-probe-stale-planner';
      const first = managerGeneration(sessionName);
      const replacement = managerGeneration(sessionName, {
        generation: 2,
        session_id: 'physical-session-2',
        owner_instance_id: 'owner-2',
      });

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      await expect(mgr.inspect(sessionName, first.session_id)).resolves.toBe('absent');
      let mismatchedOrphan = false;
      await expect(
        mgr.releaseReservation(sessionName, 2, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            mismatchedOrphan = true;
          },
        }),
      ).resolves.toBe(false);
      expect(mismatchedOrphan).toBe(false);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(false);
      let competingReservation: boolean | undefined;
      await expect(
        mgr.releaseReservation(sessionName, 1, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => undefined,
          persistReleaseEvidence: () => {
            competingReservation = mgr.reserveAgentGeneration(replacement, '/tmp');
          },
        } as ManagerReleaseOptions),
      ).resolves.toBe(true);
      expect(competingReservation).toBe(false);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(true);
    });

    it('reports pending creation as unknown and keeps a stopped name reserved until fenced release', async () => {
      const sessionName = 'autoloop-probe-pending-planner';
      const first = managerGeneration(sessionName);
      const replacement = managerGeneration(sessionName, {
        generation: 2,
        session_id: 'physical-session-2',
        owner_instance_id: 'owner-2',
      });
      let releaseStart!: () => void;
      const startGate = new Promise<void>((resolve) => {
        releaseStart = resolve;
      });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (mgr as any)._createSession = (): ISession => {
        const mock = new MockSession();
        mock.start = async () => {
          await startGate;
          mock.sessionId = 'engine-session-1';
          return mock;
        };
        mockSessions.push(mock);
        return mock;
      };

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      const starting = mgr.startSession({ name: sessionName, cwd: '/tmp' }, first);
      const releaseOptions: ManagerReleaseOptions = {
        expectedOwnerInstanceId: first.owner_instance_id,
        expectedSessionId: first.session_id,
        releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
      };

      await expect(mgr.inspect(sessionName, first.session_id)).resolves.toBe('unknown');
      await expect(mgr.releaseReservation(sessionName, 1, releaseOptions)).resolves.toBe(false);
      releaseStart();
      await starting;
      await expect(mgr.inspect(sessionName, first.session_id)).resolves.toBe('live');
      await expect(mgr.releaseReservation(sessionName, 1, releaseOptions)).resolves.toBe(false);

      await mgr.stopSession(sessionName);
      await expect(mgr.inspect(sessionName, first.session_id)).resolves.toBe('absent');
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(false);
      let competingReservation: boolean | undefined;
      await expect(
        mgr.releaseReservation(sessionName, 1, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => undefined,
          persistReleaseEvidence: () => {
            competingReservation = mgr.reserveAgentGeneration(replacement, '/tmp');
          },
        } as ManagerReleaseOptions),
      ).resolves.toBe(true);
      expect(competingReservation).toBe(false);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(true);
    });

    it('keeps a legacy generation-zero tombstone fenced until release evidence is durable', async () => {
      const sessionName = 'autoloop-probe-legacy-planner';
      const next = managerGeneration(sessionName);
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      reservations.set(sessionName, {
        name: sessionName,
        claudeSessionId: 'legacy-session-id',
        cwd: '/tmp',
        originalCreated: '2026-09-05T10:00:00.000Z',
        lastResumed: '2026-09-05T10:00:00.000Z',
        lastActivity: Date.parse('2026-09-05T10:00:00.000Z'),
      });
      persistManagerRegistry(mgr);
      let orphanEvidenceDurable = false;
      let releaseEvidenceDurable = false;
      let competingReservation: boolean | undefined;

      await expect(
        mgr.releaseReservation(sessionName, 0, {
          expectedOwnerInstanceId: 'legacy-registry',
          expectedSessionId: undefined,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            orphanEvidenceDurable = true;
          },
          persistReleaseEvidence: () => {
            expect(orphanEvidenceDurable).toBe(true);
            competingReservation = mgr.reserveAgentGeneration(next, '/tmp');
            releaseEvidenceDurable = true;
          },
        } as ManagerReleaseOptions),
      ).resolves.toBe(true);

      expect(orphanEvidenceDurable).toBe(true);
      expect(releaseEvidenceDurable).toBe(true);
      expect(competingReservation).toBe(false);
      expect(mgr.reserveAgentGeneration(next, '/tmp')).toBe(true);
    });

    it('treats a released legacy tombstone retry as side-effect-free idempotent success', async () => {
      const sessionName = 'autoloop-probe-released-legacy-planner';
      const legacyTombstone = {
        name: sessionName,
        claudeSessionId: 'legacy-session-id',
        cwd: '/tmp',
        originalCreated: '2026-09-05T10:00:00.000Z',
        lastResumed: '2026-09-05T10:00:00.000Z',
        lastActivity: Date.parse('2026-09-05T10:00:00.000Z'),
        agentReleasedGeneration: 0,
      };
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      reservations.set(sessionName, legacyTombstone);
      persistManagerRegistry(mgr);
      let evidenceHookCount = 0;

      await expect(
        mgr.releaseReservation(sessionName, 0, {
          expectedOwnerInstanceId: 'arbitrary-owner-that-was-never-stored',
          expectedSessionId: 'arbitrary-session-that-was-never-stored',
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            evidenceHookCount += 1;
          },
          persistReleaseEvidence: () => {
            evidenceHookCount += 1;
          },
        } as ManagerReleaseOptions),
      ).resolves.toBe(true);

      expect(evidenceHookCount).toBe(0);
      expect(reservations.get(sessionName)).toEqual(legacyTombstone);
    });

    it('rejects every mismatch against each field present on a released tombstone without side effects', async () => {
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      const baseTombstone = {
        claudeSessionId: 'released-session-id',
        cwd: '/tmp',
        originalCreated: '2026-09-05T10:00:00.000Z',
        lastResumed: '2026-09-05T10:00:00.000Z',
        lastActivity: Date.parse('2026-09-05T10:00:00.000Z'),
        agentReleasedGeneration: 1,
      };
      const cases = [
        {
          name: 'released-owner-only',
          stored: { agentReleasedOwnerInstanceId: 'stored-owner' },
          generation: 1,
          expectedOwnerInstanceId: 'wrong-owner',
          expectedSessionId: undefined,
        },
        {
          name: 'released-session-only',
          stored: { agentReleasedSessionId: 'stored-session' },
          generation: 1,
          expectedOwnerInstanceId: 'unused-owner',
          expectedSessionId: 'wrong-session',
        },
        {
          name: 'released-wrong-generation',
          stored: {},
          generation: 2,
          expectedOwnerInstanceId: 'unused-owner',
          expectedSessionId: undefined,
        },
        {
          name: 'released-modern-mismatch',
          stored: {
            agentReleasedOwnerInstanceId: 'stored-owner',
            agentReleasedSessionId: 'stored-session',
          },
          generation: 1,
          expectedOwnerInstanceId: 'stored-owner',
          expectedSessionId: 'wrong-session',
        },
      ];
      let evidenceHookCount = 0;

      for (const testCase of cases) {
        reservations.set(testCase.name, {
          ...baseTombstone,
          name: testCase.name,
          ...testCase.stored,
        });
      }
      persistManagerRegistry(mgr);

      for (const testCase of cases) {
        const before = structuredClone(reservations.get(testCase.name));
        await expect(
          mgr.releaseReservation(testCase.name, testCase.generation, {
            expectedOwnerInstanceId: testCase.expectedOwnerInstanceId,
            expectedSessionId: testCase.expectedSessionId,
            releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
            beforeRelease: () => {
              evidenceHookCount += 1;
            },
            persistReleaseEvidence: () => {
              evidenceHookCount += 1;
            },
          } as ManagerReleaseOptions),
        ).resolves.toBe(false);
        expect(reservations.get(testCase.name)).toEqual(before);
      }
      expect(evidenceHookCount).toBe(0);
    });

    it('accepts matching modern full-tuple, owner-only, and session-only tombstone retries without hooks', async () => {
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      const baseTombstone = {
        claudeSessionId: 'released-session-id',
        cwd: '/tmp',
        originalCreated: '2026-09-05T10:00:00.000Z',
        lastResumed: '2026-09-05T10:00:00.000Z',
        lastActivity: Date.parse('2026-09-05T10:00:00.000Z'),
        agentReleasedGeneration: 1,
      };
      const cases = [
        {
          name: 'released-matching-full-tuple',
          stored: {
            agentReleasedOwnerInstanceId: 'stored-owner',
            agentReleasedSessionId: 'stored-session',
          },
          expectedOwnerInstanceId: 'stored-owner',
          expectedSessionId: 'stored-session',
        },
        {
          name: 'released-matching-owner-only',
          stored: { agentReleasedOwnerInstanceId: 'stored-owner' },
          expectedOwnerInstanceId: 'stored-owner',
          expectedSessionId: undefined,
        },
        {
          name: 'released-matching-session-only',
          stored: { agentReleasedSessionId: 'stored-session' },
          expectedOwnerInstanceId: 'unused-owner',
          expectedSessionId: 'stored-session',
        },
      ];
      let evidenceHookCount = 0;

      for (const testCase of cases) {
        reservations.set(testCase.name, {
          ...baseTombstone,
          name: testCase.name,
          ...testCase.stored,
        });
      }
      persistManagerRegistry(mgr);

      for (const testCase of cases) {
        const before = structuredClone(reservations.get(testCase.name));
        await expect(
          mgr.releaseReservation(testCase.name, 1, {
            expectedOwnerInstanceId: testCase.expectedOwnerInstanceId,
            expectedSessionId: testCase.expectedSessionId,
            releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
            beforeRelease: () => {
              evidenceHookCount += 1;
            },
            persistReleaseEvidence: () => {
              evidenceHookCount += 1;
            },
          }),
        ).resolves.toBe(true);
        expect(reservations.get(testCase.name)).toEqual(before);
      }
      expect(evidenceHookCount).toBe(0);
    });

    it('keeps the generation reservation when an engine has no resumable conversation id yet', async () => {
      const sessionName = 'autoloop-probe-one-shot-planner';
      const generation = managerGeneration(sessionName);

      expect(mgr.reserveAgentGeneration(generation, '/tmp')).toBe(true);
      await mgr.startSession({ name: sessionName, cwd: '/tmp', engine: 'agy' }, generation);

      const persisted = (
        mgr as unknown as {
          persistedSessions: Map<string, { agentGeneration?: number; agentOwnerInstanceId?: string }>;
        }
      ).persistedSessions.get(sessionName);
      expect(persisted).toMatchObject({
        agentGeneration: generation.generation,
        agentOwnerInstanceId: generation.owner_instance_id,
      });
    });

    it('reports adopted registry-lock cleanup failure as terminal and non-retryable', () => {
      // This catches the registry adapter collapsing an entered lock cleanup
      // failure into ordinary retryable lock contention.
      const sessionName = 'autoloop-registry-cleanup-failed-planner';
      const lockPath = `${SESSION_REGISTRY_FILE}.lock`;
      const reclaimPath = `${lockPath}.reclaim`;
      fs.mkdirSync(path.dirname(lockPath), { recursive: true });
      fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
      fs.utimesSync(reclaimPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));
      const originalRmSync = fs.rmSync;
      const releaseFailure = new Error(
        'injected adopted registry-lock reclaim release failure',
      ) as NodeJS.ErrnoException;
      releaseFailure.code = 'EBUSY';
      const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
        if (String(target).startsWith(`${reclaimPath}.adopt-`)) throw releaseFailure;
        return originalRmSync(target, options);
      }) as typeof fs.rmSync);

      try {
        expect(() => mgr.reserveAgentGeneration(managerGeneration(sessionName), '/tmp')).toThrow(
          expect.objectContaining({
            code: 'AUTOLOOP_AGENT_REGISTRY_LOCK_CLEANUP_FAILED',
            retryable: false,
          }),
        );
      } finally {
        rmSpy.mockRestore();
        fs.rmSync(lockPath, { force: true });
        fs.rmSync(reclaimPath, { force: true });
      }

      expect((mgr as unknown as { persistedSessions: Map<string, unknown> }).persistedSessions.has(sessionName)).toBe(
        false,
      );
    });

    it('distinguishes registry lock and persistence failures from ownership rejection', async () => {
      const sessionName = 'autoloop-probe-persist-failure-planner';
      const first = managerGeneration(sessionName);
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, { agentGeneration?: number; agentOwnerInstanceId?: string }>;
        }
      ).persistedSessions;

      const openFile = vi.mocked(fs.openSync).getMockImplementation()!;
      vi.mocked(fs.openSync).mockImplementationOnce(((file: string, ...args: unknown[]) => {
        if (file.endsWith('/claude-sessions.json.lock')) {
          throw Object.assign(new Error('registry lock unavailable'), { code: 'EACCES' });
        }
        return (openFile as unknown as (file: string, ...rest: unknown[]) => number)(file, ...args);
      }) as typeof fs.openSync);
      expect(() => mgr.reserveAgentGeneration(first, '/tmp')).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_AGENT_REGISTRY_LOCK_CONTENDED',
          retryable: true,
        }),
      );
      expect(reservations.has(sessionName)).toBe(false);

      vi.mocked(fs.renameSync).mockImplementationOnce(() => {
        throw new Error('registry rename failed');
      });
      expect(() => mgr.reserveAgentGeneration(first, '/tmp')).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_AGENT_REGISTRY_PERSIST_FAILED',
          retryable: true,
        }),
      );
      expect(reservations.has(sessionName)).toBe(false);

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      let orphanEvidenceCount = 0;
      let releaseEvidenceCount = 0;
      vi.mocked(fs.renameSync).mockImplementationOnce(() => {
        throw new Error('registry rename failed');
      });
      await expect(
        mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            orphanEvidenceCount += 1;
          },
          persistReleaseEvidence: () => {
            releaseEvidenceCount += 1;
          },
        } as ManagerReleaseOptions),
      ).rejects.toMatchObject({
        code: 'AUTOLOOP_AGENT_REGISTRY_PERSIST_FAILED',
        retryable: true,
      });
      expect(orphanEvidenceCount).toBe(0);
      expect(releaseEvidenceCount).toBe(0);
      expect(reservations.get(sessionName)).toMatchObject({
        agentGeneration: first.generation,
        agentOwnerInstanceId: first.owner_instance_id,
      });
    });

    it('persists a single-winner release-owner fence before orphan evidence', async () => {
      const sessionName = 'autoloop-probe-release-owner-planner';
      const first = managerGeneration(sessionName);
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      let winnerHookCount = 0;
      let competingHookCount = 0;
      let competingRelease: Promise<boolean> | undefined;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      await expect(
        mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            winnerHookCount += 1;
            expect(reservations.get(sessionName)).toMatchObject({
              agentReleasePending: true,
              agentReleaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
            });
            competingRelease = mgr.releaseReservation(sessionName, first.generation, {
              expectedOwnerInstanceId: first.owner_instance_id,
              expectedSessionId: first.session_id,
              releaseOwnerInstanceId: `session-manager:${process.pid}:00000000-0000-4000-8000-000000000002`,
              beforeRelease: () => {
                competingHookCount += 1;
              },
              persistReleaseEvidence: () => {
                competingHookCount += 1;
              },
            } as ManagerReleaseOptions);
          },
          persistReleaseEvidence: () => {
            winnerHookCount += 1;
          },
        } as ManagerReleaseOptions),
      ).resolves.toBe(true);

      await expect(competingRelease).resolves.toBe(false);
      expect(winnerHookCount).toBe(2);
      expect(competingHookCount).toBe(0);
    });

    it('lets the first exact-tuple claimant own a pre-fix pending release with no release owner', async () => {
      const sessionName = 'autoloop-probe-ownerless-pending-planner';
      const first = managerGeneration(sessionName);
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      let evidenceHookCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      reservations.set(sessionName, {
        ...reservations.get(sessionName),
        agentReleasePending: true,
        agentReleaseOwnerInstanceId: undefined,
      });
      persistManagerRegistry(mgr);

      await expect(
        mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: 'wrong-physical-session',
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            evidenceHookCount += 1;
          },
          persistReleaseEvidence: () => {
            evidenceHookCount += 1;
          },
        }),
      ).resolves.toBe(false);
      expect(reservations.get(sessionName)).toMatchObject({ agentReleasePending: true });
      expect(reservations.get(sessionName)).not.toHaveProperty('agentReleaseOwnerInstanceId');
      expect(evidenceHookCount).toBe(0);

      await expect(
        mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            evidenceHookCount += 1;
          },
          persistReleaseEvidence: () => {
            evidenceHookCount += 1;
          },
        } as ManagerReleaseOptions),
      ).resolves.toBe(true);

      expect(evidenceHookCount).toBe(2);
      expect(reservations.get(sessionName)).toMatchObject({
        agentGeneration: undefined,
        agentReleasePending: undefined,
        agentReleasedGeneration: first.generation,
      });
    });

    it('transfers a pending release only after its prior manager owner shuts down and completes once', async () => {
      const sessionName = 'autoloop-probe-restart-release-owner-planner';
      const first = managerGeneration(sessionName);
      let priorOwnerHookCount = 0;
      let successorHookCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      await expect(
        mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            priorOwnerHookCount += 1;
            throw new Error('simulated crash after pending claim');
          },
          persistReleaseEvidence: () => {
            priorOwnerHookCount += 1;
          },
        } as ManagerReleaseOptions),
      ).rejects.toThrow('simulated crash after pending claim');

      const successor = createManager();
      const successorOptions: ManagerReleaseOptions = {
        expectedOwnerInstanceId: first.owner_instance_id,
        expectedSessionId: first.session_id,
        releaseOwnerInstanceId: successor.autoloopOwnerInstanceId,
        beforeRelease: () => {
          successorHookCount += 1;
        },
        persistReleaseEvidence: () => {
          successorHookCount += 1;
        },
      };
      try {
        await expect(successor.releaseReservation(sessionName, first.generation, successorOptions)).resolves.toBe(
          false,
        );
        expect(successorHookCount).toBe(0);

        await mgr.shutdown();

        await expect(successor.releaseReservation(sessionName, first.generation, successorOptions)).resolves.toBe(true);
        await expect(successor.releaseReservation(sessionName, first.generation, successorOptions)).resolves.toBe(true);
        expect(priorOwnerHookCount).toBe(1);
        expect(successorHookCount).toBe(2);
      } finally {
        await successor.shutdown();
      }
    });

    it('keeps the release owner live when shutdown starts inside an evidence hook', async () => {
      const sessionName = 'autoloop-probe-mid-hook-shutdown-planner';
      const first = managerGeneration(sessionName);
      const successor = createManager();
      let shutdown: Promise<void> | undefined;
      let competingRelease: Promise<boolean> | undefined;
      let priorEvidenceCount = 0;
      let successorEvidenceCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      try {
        await expect(
          mgr.releaseReservation(sessionName, first.generation, {
            expectedOwnerInstanceId: first.owner_instance_id,
            expectedSessionId: first.session_id,
            releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
            beforeRelease: () => {
              priorEvidenceCount += 1;
              shutdown = mgr.shutdown();
              competingRelease = successor.releaseReservation(sessionName, first.generation, {
                expectedOwnerInstanceId: first.owner_instance_id,
                expectedSessionId: first.session_id,
                releaseOwnerInstanceId: successor.autoloopOwnerInstanceId,
                beforeRelease: () => {
                  successorEvidenceCount += 1;
                },
                persistReleaseEvidence: () => {
                  successorEvidenceCount += 1;
                },
              });
            },
            persistReleaseEvidence: () => {
              priorEvidenceCount += 1;
            },
          }),
        ).resolves.toBe(true);

        await expect(competingRelease).resolves.toBe(false);
        await shutdown;
        expect(priorEvidenceCount).toBe(2);
        expect(successorEvidenceCount).toBe(0);
      } finally {
        await successor.shutdown();
      }
    });

    it('keeps the release owner live from durable claim through queued evidence hooks', async () => {
      const sessionName = 'autoloop-probe-pre-hook-shutdown-planner';
      const first = managerGeneration(sessionName);
      const successor = createManager();
      let priorEvidenceCount = 0;
      let successorEvidenceCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      try {
        const release = mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            priorEvidenceCount += 1;
          },
          persistReleaseEvidence: () => {
            priorEvidenceCount += 1;
          },
        });
        const shutdown = mgr.shutdown();
        const competingRelease = successor.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: successor.autoloopOwnerInstanceId,
          beforeRelease: () => {
            successorEvidenceCount += 1;
          },
          persistReleaseEvidence: () => {
            successorEvidenceCount += 1;
          },
        });

        await expect(release).resolves.toBe(true);
        await expect(competingRelease).resolves.toBe(false);
        await shutdown;
        expect(priorEvidenceCount).toBe(2);
        expect(successorEvidenceCount).toBe(0);
      } finally {
        await successor.shutdown();
      }
    });

    it('waits for a durably claimed release whose evidence hook body has not started', async () => {
      const sessionName = 'autoloop-probe-queued-release-shutdown-planner';
      const first = managerGeneration(sessionName);
      const successor = createManager();
      let priorEvidenceCount = 0;
      let successorEvidenceCount = 0;
      let startQueuedRelease: (() => void) | undefined;
      let queuedReleaseStarted = false;
      let resolveQueuedRelease!: (value: boolean) => void;
      let rejectQueuedRelease!: (reason?: unknown) => void;
      const queuedRelease = new Promise<boolean>((resolve, reject) => {
        resolveQueuedRelease = resolve;
        rejectQueuedRelease = reject;
      });

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      const nativePromiseResolve = Promise.resolve.bind(Promise);
      const resolveSpy = vi.spyOn(Promise, 'resolve').mockImplementationOnce((() => ({
        then: (runRelease: () => boolean) => {
          startQueuedRelease = () => {
            if (queuedReleaseStarted) return;
            queuedReleaseStarted = true;
            try {
              resolveQueuedRelease(runRelease());
            } catch (err) {
              rejectQueuedRelease(err);
            }
          };
          return queuedRelease;
        },
      })) as typeof Promise.resolve);
      let release: Promise<boolean>;
      try {
        release = mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            priorEvidenceCount += 1;
          },
          persistReleaseEvidence: () => {
            priorEvidenceCount += 1;
          },
        });
      } finally {
        resolveSpy.mockRestore();
      }

      expect(startQueuedRelease).toBeDefined();
      let shutdownSettled = false;
      const shutdown = mgr.shutdown().then(() => {
        shutdownSettled = true;
      });
      await nativePromiseResolve();
      await nativePromiseResolve();

      expect(shutdownSettled).toBe(false);
      expect(priorEvidenceCount).toBe(0);
      try {
        await expect(
          successor.releaseReservation(sessionName, first.generation, {
            expectedOwnerInstanceId: first.owner_instance_id,
            expectedSessionId: first.session_id,
            releaseOwnerInstanceId: successor.autoloopOwnerInstanceId,
            beforeRelease: () => {
              successorEvidenceCount += 1;
            },
            persistReleaseEvidence: () => {
              successorEvidenceCount += 1;
            },
          }),
        ).resolves.toBe(false);
        expect(successorEvidenceCount).toBe(0);

        startQueuedRelease!();
        await expect(release).resolves.toBe(true);
        await shutdown;
        expect(priorEvidenceCount).toBe(2);
      } finally {
        startQueuedRelease?.();
        await Promise.allSettled([release, shutdown]);
        await successor.shutdown();
      }
    });

    it('does not admit a new release after shutdown establishes its lifecycle fence', async () => {
      const sessionName = 'autoloop-probe-post-shutdown-release-planner';
      const first = managerGeneration(sessionName);
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      let evidenceHookCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      const before = structuredClone(reservations.get(sessionName));
      const shutdown = mgr.shutdown();
      await expect(
        mgr.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          beforeRelease: () => {
            evidenceHookCount += 1;
          },
          persistReleaseEvidence: () => {
            evidenceHookCount += 1;
          },
        }),
      ).resolves.toBe(false);
      await shutdown;

      expect(reservations.get(sessionName)).toEqual(before);
      expect(evidenceHookCount).toBe(0);
    });

    it('keeps a pending release fenced when the prior release owner is indeterminate', async () => {
      const sessionName = 'autoloop-probe-unknown-release-owner-planner';
      const first = managerGeneration(sessionName);
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      let evidenceHookCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      reservations.set(sessionName, {
        ...reservations.get(sessionName),
        agentReleasePending: true,
        agentReleaseOwnerInstanceId: 'session-manager:2000000000:not-a-valid-instance-id',
      });
      persistManagerRegistry(mgr);
      const before = structuredClone(reservations.get(sessionName));
      const claimant = createManager();
      try {
        await expect(
          claimant.releaseReservation(sessionName, first.generation, {
            expectedOwnerInstanceId: first.owner_instance_id,
            expectedSessionId: first.session_id,
            releaseOwnerInstanceId: claimant.autoloopOwnerInstanceId,
            beforeRelease: () => {
              evidenceHookCount += 1;
            },
            persistReleaseEvidence: () => {
              evidenceHookCount += 1;
            },
          }),
        ).resolves.toBe(false);

        expect(evidenceHookCount).toBe(0);
        expect(claimant.listPersistedSessions().find((entry) => entry.name === sessionName)).toEqual(before);
      } finally {
        await claimant.shutdown();
      }
    });

    it('takes over a well-formed pending release whose prior owner PID is dead', async () => {
      const sessionName = 'autoloop-probe-dead-release-owner-planner';
      const first = managerGeneration(sessionName);
      const deadOwnerPid = 2_000_000_000;
      const deadOwnerInstanceId = `session-manager:${deadOwnerPid}:00000000-0000-4000-8000-000000000003`;
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      let evidenceHookCount = 0;

      expect(() => process.kill(deadOwnerPid, 0)).toThrow();
      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      reservations.set(sessionName, {
        ...reservations.get(sessionName),
        agentReleasePending: true,
        agentReleaseOwnerInstanceId: deadOwnerInstanceId,
      });
      persistManagerRegistry(mgr);

      const claimant = createManager();
      try {
        await expect(
          claimant.releaseReservation(sessionName, first.generation, {
            expectedOwnerInstanceId: first.owner_instance_id,
            expectedSessionId: first.session_id,
            releaseOwnerInstanceId: claimant.autoloopOwnerInstanceId,
            beforeRelease: () => {
              evidenceHookCount += 1;
            },
            persistReleaseEvidence: () => {
              evidenceHookCount += 1;
            },
          }),
        ).resolves.toBe(true);
        expect(evidenceHookCount).toBe(2);
      } finally {
        await claimant.shutdown();
      }
    });

    it('uses shared disk CAS across same-process managers so one writer wins and a successor survives', async () => {
      const sessionName = 'autoloop-probe-cross-manager-cas-planner';
      const first = managerGeneration(sessionName);
      const successorGeneration = managerGeneration(sessionName, {
        generation: 2,
        owner_instance_id: 'successor-generation-owner',
        session_id: 'successor-generation-session',
      });
      const hookOwners: string[] = [];

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      const contender = createManager();
      const staleCompletion = createManager();
      let competingRelease: Promise<boolean> | undefined;
      const release = (manager: InstanceType<typeof SessionManager>, contend = false) =>
        manager.releaseReservation(sessionName, first.generation, {
          expectedOwnerInstanceId: first.owner_instance_id,
          expectedSessionId: first.session_id,
          releaseOwnerInstanceId: manager.autoloopOwnerInstanceId,
          beforeRelease: () => {
            hookOwners.push(manager.autoloopOwnerInstanceId);
            if (contend) competingRelease = release(contender);
          },
          persistReleaseEvidence: () => {
            hookOwners.push(manager.autoloopOwnerInstanceId);
          },
        } as ManagerReleaseOptions);

      try {
        await expect(release(mgr, true)).resolves.toBe(true);
        await expect(competingRelease).resolves.toBe(false);
        await expect(release(contender)).resolves.toBe(true);
        expect(hookOwners).toEqual([mgr.autoloopOwnerInstanceId, mgr.autoloopOwnerInstanceId]);
        expect(contender.reserveAgentGeneration(successorGeneration, '/tmp')).toBe(true);

        await expect(release(staleCompletion)).resolves.toBe(false);
        expect(hookOwners).toEqual([mgr.autoloopOwnerInstanceId, mgr.autoloopOwnerInstanceId]);
        const authoritativeSuccessor = staleCompletion
          .listPersistedSessions()
          .find((entry) => entry.name === sessionName);
        expect(authoritativeSuccessor).toMatchObject({
          agentGeneration: successorGeneration.generation,
          agentOwnerInstanceId: successorGeneration.owner_instance_id,
          agentSessionId: successorGeneration.session_id,
        });
        expect(authoritativeSuccessor).not.toHaveProperty('agentReleasePending');

        // The winning manager still holds a released-generation snapshot. Its
        // later lifecycle persistence must not overwrite the successor that a
        // different manager committed through the shared CAS authority.
        await mgr.shutdown();
        const verifier = createManager();
        try {
          expect(verifier.listPersistedSessions().find((entry) => entry.name === sessionName)).toMatchObject({
            agentGeneration: successorGeneration.generation,
            agentOwnerInstanceId: successorGeneration.owner_instance_id,
            agentSessionId: successorGeneration.session_id,
          });
        } finally {
          await verifier.shutdown();
        }
      } finally {
        await contender.shutdown();
        await staleCompletion.shutdown();
      }
    });

    it('keeps failed orphan evidence fenced for retry by the exact release owner', async () => {
      const sessionName = 'autoloop-probe-release-retry-planner';
      const first = managerGeneration(sessionName);
      const replacement = managerGeneration(sessionName, {
        generation: 2,
        session_id: 'physical-session-2',
        owner_instance_id: 'owner-2',
      });
      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      let failOrphanEvidence = true;
      let winnerOrphanHookCount = 0;
      let winnerReleaseHookCount = 0;
      let competingHookCount = 0;
      const winnerOptions: ManagerReleaseOptions = {
        expectedOwnerInstanceId: first.owner_instance_id,
        expectedSessionId: first.session_id,
        releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
        beforeRelease: () => {
          winnerOrphanHookCount += 1;
          if (failOrphanEvidence) {
            failOrphanEvidence = false;
            throw new Error('orphan evidence append failed');
          }
        },
        persistReleaseEvidence: () => {
          winnerReleaseHookCount += 1;
        },
      };

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      await expect(mgr.releaseReservation(sessionName, first.generation, winnerOptions)).rejects.toThrow(
        'orphan evidence append failed',
      );
      expect(reservations.get(sessionName)).toMatchObject({
        agentReleasePending: true,
        agentReleaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
      });
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(false);

      await expect(
        mgr.releaseReservation(sessionName, first.generation, {
          ...winnerOptions,
          releaseOwnerInstanceId: `session-manager:${process.pid}:00000000-0000-4000-8000-000000000002`,
          beforeRelease: () => {
            competingHookCount += 1;
          },
          persistReleaseEvidence: () => {
            competingHookCount += 1;
          },
        }),
      ).resolves.toBe(false);
      await expect(mgr.releaseReservation(sessionName, first.generation, winnerOptions)).resolves.toBe(true);

      expect(winnerOrphanHookCount).toBe(2);
      expect(winnerReleaseHookCount).toBe(1);
      expect(competingHookCount).toBe(0);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(true);
    });

    it('keeps a prepared tombstone fenced when completion persistence fails, then finishes idempotently', async () => {
      const sessionName = 'autoloop-probe-completion-failure-planner';
      const first = managerGeneration(sessionName);
      const replacement = managerGeneration(sessionName, {
        generation: 2,
        session_id: 'physical-session-2',
        owner_instance_id: 'owner-2',
      });
      const options: ManagerReleaseOptions = {
        expectedOwnerInstanceId: first.owner_instance_id,
        expectedSessionId: first.session_id,
        releaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
        beforeRelease: () => {
          beforeEvidenceCount += 1;
        },
        persistReleaseEvidence: () => {
          releaseEvidenceCount += 1;
        },
      };
      let beforeEvidenceCount = 0;
      let releaseEvidenceCount = 0;

      expect(mgr.reserveAgentGeneration(first, '/tmp')).toBe(true);
      const persistRename = vi.mocked(fs.renameSync).getMockImplementation()!;
      vi.mocked(fs.renameSync)
        .mockImplementationOnce(persistRename)
        .mockImplementationOnce(() => {
          throw new Error('registry rename failed');
        });
      await expect(mgr.releaseReservation(sessionName, 1, options)).rejects.toMatchObject({
        code: 'AUTOLOOP_AGENT_REGISTRY_PERSIST_FAILED',
      });
      expect(beforeEvidenceCount).toBe(1);
      expect(releaseEvidenceCount).toBe(1);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(false);
      await expect(mgr.releaseReservation(sessionName, 1, options)).resolves.toBe(true);
      await expect(mgr.releaseReservation(sessionName, 1, options)).resolves.toBe(true);
      expect(beforeEvidenceCount).toBe(1);
      expect(releaseEvidenceCount).toBe(1);
      expect(mgr.reserveAgentGeneration(replacement, '/tmp')).toBe(true);
    });
  });

  // ─── Concurrent Session Guard ───────────────────────────────────────

  describe('concurrent session guard (_pendingSessions)', () => {
    it('deduplicates concurrent startSession calls for same name', async () => {
      // Launch two starts concurrently for the same name
      const [info1, info2] = await Promise.all([
        mgr.startSession({ name: 'concurrent', cwd: '/tmp' }),
        mgr.startSession({ name: 'concurrent', cwd: '/tmp' }),
      ]);

      expect(info1.name).toBe('concurrent');
      expect(info2.name).toBe('concurrent');
      // Only one underlying session should have been created
      expect(mockSessions.length).toBe(1);
    });

    it('allows creation of different names concurrently', async () => {
      const [a, b] = await Promise.all([
        mgr.startSession({ name: 'alpha', cwd: '/tmp' }),
        mgr.startSession({ name: 'beta', cwd: '/tmp' }),
      ]);

      expect(a.name).toBe('alpha');
      expect(b.name).toBe('beta');
      expect(mockSessions.length).toBe(2);
    });
  });

  // ─── Max Concurrent Sessions ────────────────────────────────────────

  describe('max concurrent sessions', () => {
    it('throws when limit is reached', async () => {
      const maxMgr = createManager({ maxConcurrentSessions: 2 });

      await maxMgr.startSession({ name: 's1', cwd: '/tmp' });
      await maxMgr.startSession({ name: 's2', cwd: '/tmp' });

      await expect(maxMgr.startSession({ name: 's3', cwd: '/tmp' })).rejects.toThrow(
        'Max concurrent sessions (2) reached',
      );

      await maxMgr.shutdown();
    });

    it('allows creation after stopping a session', async () => {
      const maxMgr = createManager({ maxConcurrentSessions: 2 });

      await maxMgr.startSession({ name: 's1', cwd: '/tmp' });
      await maxMgr.startSession({ name: 's2', cwd: '/tmp' });
      await maxMgr.stopSession('s1');

      // Now should succeed
      const info = await maxMgr.startSession({ name: 's3', cwd: '/tmp' });
      expect(info.name).toBe('s3');

      await maxMgr.shutdown();
    });
  });

  // ─── sendMessage ────────────────────────────────────────────────────

  describe('sendMessage', () => {
    it('sends message and returns output', async () => {
      await mgr.startSession({ name: 'msg-test', cwd: '/tmp' });
      const result = await mgr.sendMessage('msg-test', 'hello world');

      expect(result.output).toContain('hello world');
      expect(result.sessionId).toBeDefined();
      expect(lastMock().sendCalls.length).toBe(1);
      expect(lastMock().sendCalls[0].message).toBe('hello world');
    });

    it('throws for unknown session', async () => {
      await expect(mgr.sendMessage('nope', 'hi')).rejects.toThrow("Session 'nope' not found");
    });

    // agy resolves with `stop_reason: 'error'` while carrying a usable reply. That
    // must NOT become `SendResult.error`: openai-compat answers 502 on a non-empty
    // error and drops the text, and ultraplan discards the plan. The outcome is
    // recorded in the ledger instead, from the session's own counter.
    it('records a turn the engine did not count as ok:false without failing the caller', async () => {
      await mgr.startSession({ name: 'sr-error', cwd: '/tmp' });
      lastMock().nextEvent = { type: 'result', result: 'agy stopped early', stop_reason: 'error' };
      lastMock().turnsSucceededOverride = 0;
      const ledger = vi.mocked((await import('node:fs')).default.appendFileSync);
      ledger.mockClear();

      const result = await mgr.sendMessage('sr-error', 'hello');

      // The reply still reaches the caller.
      expect(result.error).toBeUndefined();
      expect(result.output).toContain('hello');
      const rows = ledger.mock.calls.map((c) => JSON.parse(String(c[1]))).filter((r) => r.session === 'sr-error');
      expect(rows).toHaveLength(1);
      expect(rows[0].ok).toBe(false);
    });

    // The guard: a session whose getStats() throws leaves both snapshots empty, so the
    // counter cannot be read. A telemetry failure must not be recorded as a failed turn —
    // the row falls back to what it meant before, "nothing was thrown".
    it('keeps ok:true when the counter cannot be read at all', async () => {
      await mgr.startSession({ name: 'sr-blind', cwd: '/tmp' });
      lastMock().getStats = () => {
        throw new Error('engine torn down');
      };
      const ledger = vi.mocked((await import('node:fs')).default.appendFileSync);
      ledger.mockClear();

      await mgr.sendMessage('sr-blind', 'hello');

      const rows = ledger.mock.calls.map((c) => JSON.parse(String(c[1]))).filter((r) => r.session === 'sr-blind');
      expect(rows).toHaveLength(1);
      expect(rows[0].ok).toBe(true);
    });

    // Positive control for the assertion above: the default event still reads as a
    // success, so `ok: false` above is the classification and not a constant.
    it('records a clean turn as ok in the ledger', async () => {
      await mgr.startSession({ name: 'sr-ok', cwd: '/tmp' });
      const ledger = vi.mocked((await import('node:fs')).default.appendFileSync);
      ledger.mockClear();

      const result = await mgr.sendMessage('sr-ok', 'hello');

      expect(result.error).toBeUndefined();
      const rows = ledger.mock.calls.map((c) => JSON.parse(String(c[1]))).filter((r) => r.session === 'sr-ok');
      expect(rows).toHaveLength(1);
      expect(rows[0].ok).toBe(true);
    });

    it('passes effort and plan options through', async () => {
      await mgr.startSession({ name: 'opts-test', cwd: '/tmp' });
      await mgr.sendMessage('opts-test', 'plan this', { effort: 'max', plan: true });

      const sendOpts = lastMock().sendCalls[0].options;
      expect(sendOpts).toBeDefined();
      expect(sendOpts!.effort).toBe('max');
      expect(sendOpts!.plan).toBe(true);
    });

    it('calls onChunk and onEvent callbacks via stream callbacks', async () => {
      await mgr.startSession({ name: 'cb-test', cwd: '/tmp' });
      const chunks: string[] = [];
      const events: unknown[] = [];

      // Override the mock's send to call callbacks
      const mock = lastMock();
      mock.send = async (message, options) => {
        mock.sendCalls.push({ message, options });
        if (options?.callbacks?.onText) options.callbacks.onText('chunk1');
        if (options?.callbacks?.onToolUse) options.callbacks.onToolUse({ tool: 'Read' });
        if (options?.callbacks?.onToolResult) options.callbacks.onToolResult({ result: 'ok' });
        return { text: 'done', event: { type: 'result' } };
      };

      await mgr.sendMessage('cb-test', 'test', {
        onChunk: (c) => chunks.push(c),
        onEvent: (e) => events.push(e),
      });

      expect(chunks).toEqual(['chunk1']);
      // onEvent should receive text, tool_use, and tool_result events
      expect(events.length).toBe(3);
    });

    it('serializes concurrent sendMessage calls on the same session', async () => {
      // Two concurrent sends on the same session must NOT interleave —
      // PersistentClaudeSession's _streamCallbacks and TURN_COMPLETE listener
      // are single-slot, so without serialization the second caller would
      // receive the first caller's response.
      await mgr.startSession({ name: 'race-test', cwd: '/tmp' });
      const mock = lastMock();
      const log: string[] = [];

      // Replace send() with a slow, instrumented version that logs entry/exit.
      mock.send = async (message) => {
        const tag = String(message);
        log.push(`enter:${tag}`);
        // Yield to the event loop so any concurrent caller would race here
        // if no mutex was holding them off.
        await new Promise((r) => setTimeout(r, 20));
        log.push(`exit:${tag}`);
        return { text: `reply:${tag}`, event: { type: 'result' } };
      };

      // Fire two sends in parallel — both should resolve with the correct
      // matching reply, and the log must show no interleaving.
      const [r1, r2] = await Promise.all([mgr.sendMessage('race-test', 'one'), mgr.sendMessage('race-test', 'two')]);

      expect(r1.output).toBe('reply:one');
      expect(r2.output).toBe('reply:two');
      // No interleaving: every enter must be immediately followed by its exit
      expect(log).toEqual(['enter:one', 'exit:one', 'enter:two', 'exit:two']);
    });

    it('does not deadlock subsequent sends after a failed send', async () => {
      // If the prior link in the chain rejects, the next caller must still
      // proceed (we catch in the chain hand-off so failures don't poison it).
      await mgr.startSession({ name: 'recover-test', cwd: '/tmp' });
      const mock = lastMock();
      let failedOnce = false;
      mock.send = async (message) => {
        if (!failedOnce) {
          failedOnce = true;
          throw new Error('boom');
        }
        return { text: `ok:${message}`, event: { type: 'result' } };
      };

      await expect(mgr.sendMessage('recover-test', 'first')).rejects.toThrow('boom');
      const r2 = await mgr.sendMessage('recover-test', 'second');
      expect(r2.output).toBe('ok:second');
    });
  });

  // ─── Model Resolution ───────────────────────────────────────────────

  describe('model resolution (_resolveModel)', () => {
    it('resolves known aliases (opus -> claude-opus-5)', async () => {
      await mgr.startSession({ name: 'alias-test', model: 'opus', cwd: '/tmp' });
      const list = mgr.listSessions();
      expect(list[0].model).toBe('claude-opus-5');
    });

    it('resolves sonnet alias', async () => {
      await mgr.startSession({ name: 'sonnet-test', model: 'sonnet', cwd: '/tmp' });
      const list = mgr.listSessions();
      expect(list[0].model).toBe('claude-sonnet-5');
    });

    it('resolves haiku alias', async () => {
      await mgr.startSession({ name: 'haiku-test', model: 'haiku', cwd: '/tmp' });
      const list = mgr.listSessions();
      expect(list[0].model).toBe('claude-haiku-4-5');
    });

    it('passes through unknown model strings as-is', async () => {
      await mgr.startSession({ name: 'custom-test', model: 'my-custom-model', cwd: '/tmp' });
      const list = mgr.listSessions();
      expect(list[0].model).toBe('my-custom-model');
    });

    it('respects modelOverrides over default aliases', async () => {
      await mgr.startSession({
        name: 'override-test',
        model: 'opus',
        modelOverrides: { opus: 'claude-opus-custom-v2' },
        cwd: '/tmp',
      });
      const list = mgr.listSessions();
      expect(list[0].model).toBe('claude-opus-custom-v2');
    });

    it('setModel updates model for a session', async () => {
      await mgr.startSession({ name: 'model-set', model: 'opus', cwd: '/tmp' });
      mgr.setModel('model-set', 'sonnet');
      const list = mgr.listSessions();
      expect(list[0].model).toBe('claude-sonnet-5');
    });
  });

  // ─── Grep Session ───────────────────────────────────────────────────

  describe('grepSession', () => {
    it('filters history entries by regex pattern', async () => {
      await mgr.startSession({ name: 'grep-test', cwd: '/tmp' });
      const mock = lastMock();
      mock.addHistory([
        { time: '2025-01-01T00:00:00Z', type: 'user', event: { text: 'hello world' } },
        { time: '2025-01-01T00:01:00Z', type: 'assistant', event: { text: 'foo bar' } },
        { time: '2025-01-01T00:02:00Z', type: 'user', event: { text: 'hello again' } },
        { time: '2025-01-01T00:03:00Z', type: 'tool', event: { text: 'something else' } },
      ]);

      const results = await mgr.grepSession('grep-test', 'hello');
      expect(results.length).toBe(2);
      expect(results[0].type).toBe('user');
      expect(results[1].type).toBe('user');
    });

    it('respects limit parameter', async () => {
      await mgr.startSession({ name: 'grep-limit', cwd: '/tmp' });
      const mock = lastMock();
      mock.addHistory(
        Array.from({ length: 100 }, (_, i) => ({
          time: `2025-01-01T00:${String(i).padStart(2, '0')}:00Z`,
          type: 'user',
          event: { text: `message ${i}` },
        })),
      );

      const results = await mgr.grepSession('grep-limit', 'message', 5);
      expect(results.length).toBe(5);
    });

    it('is case-insensitive', async () => {
      await mgr.startSession({ name: 'grep-ci', cwd: '/tmp' });
      lastMock().addHistory([
        { time: '2025-01-01T00:00:00Z', type: 'user', event: { text: 'Hello World' } },
        { time: '2025-01-01T00:01:00Z', type: 'user', event: { text: 'HELLO AGAIN' } },
      ]);

      const results = await mgr.grepSession('grep-ci', 'hello');
      expect(results.length).toBe(2);
    });

    it('returns empty array when no matches', async () => {
      await mgr.startSession({ name: 'grep-empty', cwd: '/tmp' });
      lastMock().addHistory([{ time: '2025-01-01T00:00:00Z', type: 'user', event: { text: 'nothing here' } }]);

      const results = await mgr.grepSession('grep-empty', 'zzz_not_found');
      expect(results.length).toBe(0);
    });

    it('throws for unknown session', async () => {
      await expect(mgr.grepSession('nope', 'test')).rejects.toThrow("Session 'nope' not found");
    });
  });

  // ─── setEffort ──────────────────────────────────────────────────────

  describe('setEffort', () => {
    it('updates effort on the session', async () => {
      await mgr.startSession({ name: 'effort-test', cwd: '/tmp' });
      mgr.setEffort('effort-test', 'max');

      expect(lastMock().getEffort()).toBe('max');
    });

    it('throws for unknown session', () => {
      expect(() => mgr.setEffort('nope', 'high')).toThrow("Session 'nope' not found");
    });
  });

  // ─── compactSession ─────────────────────────────────────────────────

  describe('compactSession', () => {
    it('calls compact on the underlying session', async () => {
      await mgr.startSession({ name: 'compact-test', cwd: '/tmp' });
      await mgr.compactSession('compact-test', 'summarize this');
      expect(lastMock().compactCalls).toEqual(['summarize this']);
    });

    it('works without summary', async () => {
      await mgr.startSession({ name: 'compact-test2', cwd: '/tmp' });
      await mgr.compactSession('compact-test2');
      expect(lastMock().compactCalls).toEqual(['']);
    });
  });

  // ─── getCost ────────────────────────────────────────────────────────

  describe('getCost', () => {
    it('returns cost breakdown from session', async () => {
      await mgr.startSession({ name: 'cost-test', cwd: '/tmp' });
      const cost = mgr.getCost('cost-test');
      expect(cost.model).toBe('mock-model');
      expect(cost.totalUsd).toBeGreaterThan(0);
    });
  });

  // ─── Inbox (cross-session messaging) ────────────────────────────────

  describe('inbox / sessionSendTo', () => {
    it('delivers message directly to idle session', async () => {
      await mgr.startSession({ name: 'sender', cwd: '/tmp' });
      await mgr.startSession({ name: 'receiver', cwd: '/tmp' });

      const receiverMock = mockSessions[1]; // second session is receiver
      receiverMock.setBusy(false);
      receiverMock.setReady(true);

      const result = await mgr.sessionSendTo('sender', 'receiver', 'hello from sender');
      expect(result.delivered).toBe(true);
      expect(result.queued).toBe(false);

      // The receiver should have received a send call with cross-session XML wrapper
      expect(receiverMock.sendCalls.length).toBe(1);
      const msg = receiverMock.sendCalls[0].message as string;
      expect(msg).toContain('<cross-session-message');
      expect(msg).toContain('from="sender"');
      expect(msg).toContain('hello from sender');
    });

    it('queues message when target is busy', async () => {
      await mgr.startSession({ name: 'sender', cwd: '/tmp' });
      await mgr.startSession({ name: 'busy-recv', cwd: '/tmp' });

      const receiverMock = mockSessions[1];
      receiverMock.setBusy(true);

      const result = await mgr.sessionSendTo('sender', 'busy-recv', 'queued msg');
      expect(result.delivered).toBe(false);
      expect(result.queued).toBe(true);

      // Check inbox
      const inbox = mgr.sessionInbox('busy-recv');
      expect(inbox.length).toBe(1);
      expect(inbox[0].text).toBe('queued msg');
      expect(inbox[0].from).toBe('sender');
      expect(inbox[0].read).toBe(false);
    });

    it('queues message when target is not ready', async () => {
      await mgr.startSession({ name: 'sender', cwd: '/tmp' });
      await mgr.startSession({ name: 'notready', cwd: '/tmp' });

      const receiverMock = mockSessions[1];
      receiverMock.setBusy(false);
      receiverMock.setReady(false);

      const result = await mgr.sessionSendTo('sender', 'notready', 'queued msg');
      expect(result.delivered).toBe(false);
      expect(result.queued).toBe(true);
    });

    it('broadcast sends to all other sessions', async () => {
      await mgr.startSession({ name: 'broadcaster', cwd: '/tmp' });
      await mgr.startSession({ name: 'recv1', cwd: '/tmp' });
      await mgr.startSession({ name: 'recv2', cwd: '/tmp' });

      const result = await mgr.sessionSendTo('broadcaster', '*', 'broadcast msg');
      expect(result.delivered).toBe(true);

      // Both receivers should have gotten the message
      expect(mockSessions[1].sendCalls.length).toBe(1);
      expect(mockSessions[2].sendCalls.length).toBe(1);
      // Broadcaster should NOT have gotten its own message
      expect(mockSessions[0].sendCalls.length).toBe(0);
    });

    it('throws when sender session does not exist', async () => {
      await mgr.startSession({ name: 'target', cwd: '/tmp' });
      await expect(mgr.sessionSendTo('ghost', 'target', 'hi')).rejects.toThrow("Sender session 'ghost' not found");
    });

    it('throws when target session does not exist', async () => {
      await mgr.startSession({ name: 'sender', cwd: '/tmp' });
      await expect(mgr.sessionSendTo('sender', 'ghost', 'hi')).rejects.toThrow("Target session 'ghost' not found");
    });

    it('sessionInbox returns unread messages by default', async () => {
      await mgr.startSession({ name: 's1', cwd: '/tmp' });
      await mgr.startSession({ name: 's2', cwd: '/tmp' });
      mockSessions[1].setBusy(true);

      await mgr.sessionSendTo('s1', 's2', 'msg1');
      await mgr.sessionSendTo('s1', 's2', 'msg2');

      const unread = mgr.sessionInbox('s2');
      expect(unread.length).toBe(2);

      const all = mgr.sessionInbox('s2', false);
      expect(all.length).toBe(2);
    });

    it('sessionDeliverInbox delivers queued messages', async () => {
      await mgr.startSession({ name: 's1', cwd: '/tmp' });
      await mgr.startSession({ name: 's2', cwd: '/tmp' });
      mockSessions[1].setBusy(true);

      await mgr.sessionSendTo('s1', 's2', 'queued1');
      await mgr.sessionSendTo('s1', 's2', 'queued2');

      mockSessions[1].setBusy(false);

      const delivered = await mgr.sessionDeliverInbox('s2');
      expect(delivered).toBe(2);

      // Messages should be marked as read
      const unread = mgr.sessionInbox('s2');
      expect(unread.length).toBe(0);
    });

    it('sessionDeliverInbox returns 0 when inbox is empty', async () => {
      await mgr.startSession({ name: 'empty-inbox', cwd: '/tmp' });
      const delivered = await mgr.sessionDeliverInbox('empty-inbox');
      expect(delivered).toBe(0);
    });

    it('MAX_INBOX_SIZE eviction drops oldest read first, then oldest unread', async () => {
      await mgr.startSession({ name: 's1', cwd: '/tmp' });
      await mgr.startSession({ name: 's2', cwd: '/tmp' });
      mockSessions[1].setBusy(true);

      // Fill inbox to MAX_INBOX_SIZE (200)
      for (let i = 0; i < 200; i++) {
        await mgr.sessionSendTo('s1', 's2', `msg-${i}`);
      }

      let inbox = mgr.sessionInbox('s2', false);
      expect(inbox.length).toBe(200);

      // Mark some as read
      inbox[0].read = true;
      inbox[1].read = true;

      // Send one more — should evict the first read message
      await mgr.sessionSendTo('s1', 's2', 'overflow-msg');
      inbox = mgr.sessionInbox('s2', false);
      expect(inbox.length).toBe(200);
      // The evicted one should have been the first read message (msg-0)
      expect(inbox.find((m) => m.text === 'msg-0')).toBeUndefined();
      // msg-1 (also read) should still be there
      expect(inbox.find((m) => m.text === 'msg-1')).toBeDefined();
      // overflow should be the last
      expect(inbox[inbox.length - 1].text).toBe('overflow-msg');
    });

    it('evicts oldest unread if no read messages exist', async () => {
      await mgr.startSession({ name: 's1', cwd: '/tmp' });
      await mgr.startSession({ name: 's2', cwd: '/tmp' });
      mockSessions[1].setBusy(true);

      for (let i = 0; i < 200; i++) {
        await mgr.sessionSendTo('s1', 's2', `msg-${i}`);
      }

      // All unread — send one more
      await mgr.sessionSendTo('s1', 's2', 'overflow-unread');
      const inbox = mgr.sessionInbox('s2', false);
      expect(inbox.length).toBe(200);
      // First message should have been evicted
      expect(inbox.find((m) => m.text === 'msg-0')).toBeUndefined();
      expect(inbox[inbox.length - 1].text).toBe('overflow-unread');
    });

    it('includes summary in cross-session message when provided', async () => {
      await mgr.startSession({ name: 's1', cwd: '/tmp' });
      await mgr.startSession({ name: 's2', cwd: '/tmp' });

      await mgr.sessionSendTo('s1', 's2', 'detailed message', 'TL;DR summary');

      const msg = mockSessions[1].sendCalls[0].message as string;
      expect(msg).toContain('summary="TL;DR summary"');
    });

    it('escapes XML special characters in from and summary', async () => {
      await mgr.startSession({ name: 'a<b', cwd: '/tmp' });
      await mgr.startSession({ name: 'recv', cwd: '/tmp' });

      await mgr.sessionSendTo('a<b', 'recv', 'test', 'say "hi" & <bye>');

      const msg = mockSessions[1].sendCalls[0].message as string;
      expect(msg).toContain('from="a&lt;b"');
      expect(msg).toContain('summary="say &quot;hi&quot; &amp; &lt;bye&gt;"');
    });
  });

  // ─── Team tools (issue #48 regression guard) ────────────────────────────
  // Claude Code CLI does not expose `/team` or `@teammate` syntax. team_list
  // and team_send must use the virtual-team / inbox layer for every engine.

  describe('teamList / teamSend (virtual team across all engines)', () => {
    it('teamList returns virtual team list for claude engine (no /team command)', async () => {
      await mgr.startSession({ name: 'lead', cwd: '/tmp', engine: 'claude' });
      await mgr.startSession({ name: 'helper', cwd: '/tmp', engine: 'claude' });

      const out = await mgr.teamList('lead');

      expect(out).toContain('Virtual team');
      expect(out).toContain('helper');
      expect(out).toContain('claude');
      // Critically, the caller's session must NOT have been sent the literal '/team' string
      expect(mockSessions[0].sendCalls.find((c) => c.message === '/team')).toBeUndefined();
    });

    it('teamList omits the calling session and reports "No other active sessions" when alone', async () => {
      await mgr.startSession({ name: 'solo', cwd: '/tmp', engine: 'claude' });
      const out = await mgr.teamList('solo');
      expect(out).toBe('No other active sessions');
    });

    it('teamSend routes via cross-session inbox for claude engine (no @teammate command)', async () => {
      await mgr.startSession({ name: 'sender', cwd: '/tmp', engine: 'claude' });
      await mgr.startSession({ name: 'receiver', cwd: '/tmp', engine: 'claude' });

      const result = await mgr.teamSend('sender', 'receiver', 'please review');

      // Sender must NOT have been sent a literal '@receiver ...' string
      expect(
        mockSessions[0].sendCalls.find((c) => typeof c.message === 'string' && c.message.startsWith('@')),
      ).toBeUndefined();
      // Receiver got a cross-session-message envelope instead
      expect(mockSessions[1].sendCalls.length).toBe(1);
      const msg = mockSessions[1].sendCalls[0].message as string;
      expect(msg).toContain('<cross-session-message');
      expect(msg).toContain('from="sender"');
      expect(msg).toContain('please review');
      expect(result.output).toContain('delivered');
    });

    it('teamSend throws clearly when teammate session does not exist', async () => {
      await mgr.startSession({ name: 'lone', cwd: '/tmp', engine: 'claude' });
      await expect(mgr.teamSend('lone', 'ghost', 'hi')).rejects.toThrow("Target session 'ghost' not found");
    });
  });

  // ─── Ultraplan / Ultrareview ────────────────────────────────────────
  //
  // Both are kernel runs now, so these drive the real path — a temp run store
  // and stubbed node executors — instead of stubbing `fanoutStart`, which
  // ultrareview no longer calls. The assertions moved with them: what used to be
  // checked on the arguments handed to `fanoutStart` is now checked on the spec
  // that reached the kernel, which is the thing that actually gets executed.

  describe('ultraplan / ultrareview', () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let started: any[];

    beforeEach(() => {
      started = [];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const kernel = (mgr as any).kernel;
      for (const kind of ['agent', 'fanout'] as const) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        kernel.setExecutor(kind, async (nodeSpec: any) => {
          started.push(nodeSpec);
          // Park so the run stays `running` while the assertions look at it.
          await new Promise((r) => setTimeout(r, 50));
          return { ok: true, output: 'stub' };
        });
      }
    });

    it('ultraplanStart creates a result with running status', async () => {
      const result = await mgr.ultraplanStart('build a feature', { cwd: '/tmp' });
      expect(result.id).toMatch(/^ultraplan-/);
      expect(result.status).toBe('running');
      expect(result.sessionName).toContain('ultraplan-');
      expect(result.startTime).toBeDefined();
    });

    it('ultraplanStatus returns the result by id, from disk', async () => {
      const result = await mgr.ultraplanStart('plan task', { cwd: '/tmp' });
      const status = mgr.ultraplanStatus(result.id);
      expect(status).toBeDefined();
      expect(status!.id).toBe(result.id);
      expect(status!.status).toBe('running');
    });

    it('plans in plan mode at max effort', async () => {
      await mgr.ultraplanStart('plan task', { cwd: '/tmp' });
      expect(started[0]).toMatchObject({ kind: 'agent', permissionMode: 'plan', effort: 'max' });
    });

    it('ultraplanStatus returns undefined for unknown id', () => {
      expect(mgr.ultraplanStatus('nonexistent')).toBeUndefined();
    });

    it('ultrareviewStart creates result with running status', async () => {
      const result = await mgr.ultrareviewStart('/tmp', { agentCount: 3 });
      expect(result.id).toMatch(/^ultrareview-/);
      expect(result.status).toBe('running');
      expect(result.agentCount).toBe(3);
      // The fan-out id and the run id are the same thing now.
      expect(result.councilId).toBe(result.id);
    });

    it('runs reviewers read-only (plan mode) and fans out with synthesis', async () => {
      await mgr.ultrareviewStart('/tmp', { agentCount: 2, engines: ['claude', 'codex'] });
      const spec = started[0];
      expect(spec.kind).toBe('fanout');
      expect(spec.synthesize).toBe(true);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(spec.agents.every((a: any) => a.permissionMode === 'plan')).toBe(true);
      expect(spec.agents.map((a: { engine: string }) => a.engine)).toEqual(['claude', 'codex']);
    });

    it('ultrareviewStart clamps agentCount', async () => {
      // agentCount: 0 is falsy, so `0 || 5` defaults to 5
      expect((await mgr.ultrareviewStart('/tmp', { agentCount: 0 })).agentCount).toBe(5);
      expect((await mgr.ultrareviewStart('/tmp', { agentCount: 1 })).agentCount).toBe(1);
      expect((await mgr.ultrareviewStart('/tmp', { agentCount: 50 })).agentCount).toBe(20);
    });

    it('ultrareviewStatus returns undefined for unknown id', () => {
      expect(mgr.ultrareviewStatus('nonexistent')).toBeUndefined();
    });

    it('ultrareviewStatus returns the stored result', async () => {
      const result = await mgr.ultrareviewStart('/tmp');
      const status = mgr.ultrareviewStatus(result.id);
      expect(status).toBeDefined();
      expect(status!.id).toBe(result.id);
    });

    it('keeps results readable after the run finishes — no 30-minute eviction', async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const kernel = (mgr as any).kernel;
      kernel.setExecutor('fanout', async () => ({
        ok: true,
        output: 'done',
        data: { task: 't', agentCount: 1, results: [{ agent: 'a', ok: true, output: 'found a bug' }] },
      }));
      const result = await mgr.ultrareviewStart('/tmp', { agentCount: 1 });
      await kernel.wait(result.id);
      const status = mgr.ultrareviewStatus(result.id);
      expect(status!.status).toBe('completed');
      expect(status!.findings).toContain('found a bug');
    });
  });

  // ─── Health ─────────────────────────────────────────────────────────

  describe('health', () => {
    it('returns health with no sessions', () => {
      const h = mgr.health();
      expect(h.ok).toBe(true);
      expect(h.sessions).toBe(0);
      expect(h.sessionNames).toEqual([]);
      expect(h.details).toEqual([]);
    });

    it('returns health with active sessions', async () => {
      await mgr.startSession({ name: 'h1', cwd: '/tmp' });
      await mgr.startSession({ name: 'h2', cwd: '/tmp' });

      const h = mgr.health();
      expect(h.sessions).toBe(2);
      expect(h.sessionNames.sort()).toEqual(['h1', 'h2']);
      expect(h.details.length).toBe(2);
      expect(h.details[0].ready).toBe(true);
      expect(h.details[0].turns).toBeDefined();
    });
  });

  // ─── Shutdown ───────────────────────────────────────────────────────

  describe('shutdown', () => {
    it('stops all sessions', async () => {
      await mgr.startSession({ name: 'shutdown1', cwd: '/tmp' });
      await mgr.startSession({ name: 'shutdown2', cwd: '/tmp' });

      const mock1 = mockSessions[0];
      const mock2 = mockSessions[1];

      await mgr.shutdown();

      expect(mock1.stopCalled).toBe(1);
      expect(mock2.stopCalled).toBe(1);
      expect(mgr.listSessions().length).toBe(0);
    });

    it('clears cleanup timer', async () => {
      // After shutdown, the cleanup timer should be cleared
      // We can verify by checking that no cleanup runs after shutdown
      await mgr.shutdown();

      // Create a fresh manager to verify the timer cleanup logic path
      const mgr2 = createManager();
      // Access the private timer to verify it exists
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((mgr2 as any).cleanupTimer).not.toBeNull();
      await mgr2.shutdown();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((mgr2 as any).cleanupTimer).toBeNull();
    });

    it('cancels live kernel runs (there are no per-mode timers left to clear)', async () => {
      // Ultrareview used to keep a `setInterval` per review and a map of
      // results, both torn down here. Every mode is a kernel run now, so
      // shutdown has exactly one thing to stop.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const kernel = (mgr as any).kernel;
      let cancelled = false;
      kernel.setExecutor('fanout', async (_n: unknown, ctx: { signal: { aborted: boolean } }) => {
        for (let i = 0; i < 200; i++) {
          if (ctx.signal.aborted) {
            cancelled = true;
            return { ok: false, error: 'cancelled' };
          }
          await new Promise((r) => setTimeout(r, 5));
        }
        return { ok: true };
      });

      const review = await mgr.ultrareviewStart('/tmp');
      await vi.waitFor(() => expect(mgr.ultrareviewStatus(review.id)?.status).toBe('running'));
      await mgr.shutdown();
      expect(cancelled).toBe(true);
    });

    it('waits for kernel-triggered Autoloop reservation releases before closing the release fence', async () => {
      const runId = 'shutdown-agent-releases';
      const workspace = path.join(TEST_WF_DIR, 'workspaces', runId);
      fs.mkdirSync(workspace, { recursive: true });
      await mgr.autoloopStart({ runId, workspace });
      await mgr.getAutoloop(runId)!.dispatcher.spawnSubagents();

      await mgr.shutdown();

      const reservations = (
        mgr as unknown as {
          persistedSessions: Map<string, Record<string, unknown>>;
        }
      ).persistedSessions;
      for (const role of ['planner', 'coder', 'reviewer']) {
        const reservation = reservations.get(`autoloop-${runId}-${role}`);
        expect(reservation).toMatchObject({ agentReleasedGeneration: 1 });
        expect(reservation).not.toHaveProperty('agentGeneration');
        expect(reservation).not.toHaveProperty('agentReleasePending');
      }
    });

    it('keeps release admission open until natural Autoloop termination finishes dispatcher teardown', async () => {
      const runId = 'shutdown-overlapping-natural-autoloop-termination';
      const workspace = path.join(TEST_WF_DIR, 'workspaces', runId);
      fs.mkdirSync(workspace, { recursive: true });
      await mgr.autoloopStart({ runId, workspace });
      const handle = mgr.getAutoloop(runId)!;
      await handle.dispatcher.spawnSubagents();

      const originalDispatcherShutdown = handle.dispatcher.shutdown.bind(handle.dispatcher);
      let signalTeardownStarted!: () => void;
      const teardownStarted = new Promise<void>((resolve) => {
        signalTeardownStarted = resolve;
      });
      let allowTeardown!: () => void;
      const teardownBarrier = new Promise<void>((resolve) => {
        allowTeardown = resolve;
      });
      handle.dispatcher.shutdown = async (reason, options) => {
        signalTeardownStarted();
        await teardownBarrier;
        await originalDispatcherShutdown(reason, options);
      };

      let naturalTermination: Promise<boolean> | undefined;
      let managerShutdown: Promise<void> | undefined;
      try {
        naturalTermination = mgr.autoloopStop(runId, 'natural-test-termination');
        await teardownStarted;

        let managerShutdownSettled = false;
        managerShutdown = mgr.shutdown().then(() => {
          managerShutdownSettled = true;
        });
        await new Promise<void>((resolve) => nativeSetImmediate(resolve));

        expect(managerShutdownSettled).toBe(false);

        allowTeardown();
        await expect(naturalTermination).resolves.toBe(true);
        await managerShutdown;

        const reservations = (
          mgr as unknown as {
            persistedSessions: Map<string, Record<string, unknown>>;
          }
        ).persistedSessions;
        for (const role of ['planner', 'coder', 'reviewer']) {
          const reservation = reservations.get(`autoloop-${runId}-${role}`);
          expect(reservation).toMatchObject({ agentReleasedGeneration: 1 });
          expect(reservation).not.toHaveProperty('agentGeneration');
          expect(reservation).not.toHaveProperty('agentReleasePending');
        }
      } finally {
        allowTeardown();
        await Promise.allSettled([naturalTermination, managerShutdown].filter((value) => value !== undefined));
      }
    });

    it('is idempotent', async () => {
      await mgr.startSession({ name: 'idempotent', cwd: '/tmp' });
      await mgr.shutdown();
      // Second shutdown should not throw
      await mgr.shutdown();
    });

    it('cancels a pending debounced registry write before returning', async () => {
      await mgr.startSession({ name: 'debounced-shutdown', cwd: '/tmp' });
      const internals = mgr as unknown as {
        _persistRegistrySnapshot(): boolean;
      };
      const persist = internals._persistRegistrySnapshot.bind(mgr);
      let writesAfterSessionStart = 0;
      internals._persistRegistrySnapshot = () => {
        writesAfterSessionStart += 1;
        return persist();
      };

      await mgr.shutdown();
      expect(writesAfterSessionStart).toBe(1);

      await vi.advanceTimersByTimeAsync(5_001);
      expect(writesAfterSessionStart).toBe(1);
    });
  });

  // ─── TTL Cleanup ────────────────────────────────────────────────────

  describe('TTL cleanup', () => {
    it('cleans up sessions that exceed TTL', async () => {
      const shortTtlMgr = createManager({ sessionTtlMinutes: 1 });

      await shortTtlMgr.startSession({ name: 'ttl-test', cwd: '/tmp' });
      expect(shortTtlMgr.listSessions().length).toBe(1);

      // Advance time past the TTL (1 minute = 60_000ms) + cleanup interval (60_000ms)
      vi.advanceTimersByTime(2 * 60_000);

      expect(shortTtlMgr.listSessions().length).toBe(0);

      await shortTtlMgr.shutdown();
    });
  });

  // ─── Constructor Config ─────────────────────────────────────────────

  describe('constructor config', () => {
    it('uses defaults when no config provided', () => {
      const defaultMgr = new SessionManager();
      patchCreateSession(defaultMgr);

      const h = defaultMgr.health();
      expect(h.ok).toBe(true);

      // Clean up
      defaultMgr.shutdown();
    });

    it('applies pricing overrides', async () => {
      // This is tested indirectly — if pricingOverrides is passed,
      // overrideModelPricing should be called. We test the effect via getModelPricing:
      const { getModelPricing } = await import('../types.js');

      const overrideMgr = createManager({
        pricingOverrides: { 'claude-opus-4-6': { input: 999 } },
      });

      expect(getModelPricing('claude-opus-4-6').input).toBe(999);

      await overrideMgr.shutdown();
    });
  });

  // ─── switchModel ────────────────────────────────────────────────────

  describe('switchModel', () => {
    it('rejects when session is busy', async () => {
      await mgr.startSession({ name: 'busy-switch', cwd: '/tmp' });
      lastMock().setBusy(true);

      await expect(mgr.switchModel('busy-switch', 'sonnet')).rejects.toThrow('currently processing a message');
    });

    it('rejects when session has no session ID', async () => {
      await mgr.startSession({ name: 'no-id', cwd: '/tmp' });
      const mock = lastMock();
      mock.setBusy(false);
      mock.sessionId = undefined;
      // Also clear the managed session's claudeSessionId
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const managed = (mgr as any).sessions.get('no-id');
      managed.claudeSessionId = undefined;

      await expect(mgr.switchModel('no-id', 'sonnet')).rejects.toThrow('has no claude session ID');
    });

    // ── The guard checks the registry, not a frozen prefix list.
    //
    //    It was ['claude-','gemini-','gpt-','anthropic/','google/','openai/'],
    //    which rejects every model the registry has gained since — each of
    //    which `_createSession` can dispatch.
    it('accepts every model the registry actually knows', async () => {
      for (const model of ['grok-4.6', 'grok', 'composer-2', 'o3', 'o4-mini', 'codex-mini-latest']) {
        const name = `switch-${model}`;
        await mgr.startSession({ name, cwd: '/tmp' });
        lastMock().setBusy(false);
        await expect(mgr.switchModel(name, model)).resolves.toBeDefined();
        await mgr.stopSession(name); // the fixture caps concurrent sessions at 5
      }
    });

    it('still accepts a provider-qualified string, which the error message offers', async () => {
      await mgr.startSession({ name: 'switch-qualified', cwd: '/tmp' });
      lastMock().setBusy(false);
      await expect(mgr.switchModel('switch-qualified', 'someprovider/some-model')).resolves.toBeDefined();
    });

    it('rejects unknown model that does not match known patterns', async () => {
      await mgr.startSession({ name: 'bad-model', cwd: '/tmp' });
      lastMock().setBusy(false);

      await expect(mgr.switchModel('bad-model', 'totally-unknown')).rejects.toThrow("Unknown model 'totally-unknown'");
    });

    it('successfully switches model for a valid known-pattern model', async () => {
      await mgr.startSession({ name: 'switch-ok', cwd: '/tmp' });
      lastMock().setBusy(false);

      const info = await mgr.switchModel('switch-ok', 'sonnet');
      expect(info.name).toBe('switch-ok');
      // The session should have been recreated
      expect(mockSessions.length).toBe(2); // original + new
    });

    it('uses the agy conversation UUID, not the synthetic session ID, when switching models', async () => {
      await mgr.startSession({ name: 'agy-switch', cwd: '/tmp', engine: 'agy', model: 'gemini-3.5-flash' });
      lastMock().conversationId = '11111111-2222-3333-4444-555555555555';
      lastMock().setBusy(false);

      await mgr.sendMessage('agy-switch', 'hello');
      await mgr.switchModel('agy-switch', 'agy-pro');

      expect(createdConfigs[1].resumeSessionId).toBe('11111111-2222-3333-4444-555555555555');
      expect(createdConfigs[1].resumeSessionId).not.toMatch(/^mock-session-/);
    });
  });

  // ─── updateTools ────────────────────────────────────────────────────

  describe('updateTools', () => {
    it('rejects when session is busy', async () => {
      await mgr.startSession({ name: 'busy-tools', cwd: '/tmp' });
      lastMock().setBusy(true);

      await expect(mgr.updateTools('busy-tools', { allowedTools: ['Read'] })).rejects.toThrow(
        'currently processing a message',
      );
    });

    it('rejects when no session ID', async () => {
      await mgr.startSession({ name: 'no-id-tools', cwd: '/tmp' });
      const mock = lastMock();
      mock.setBusy(false);
      mock.sessionId = undefined;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (mgr as any).sessions.get('no-id-tools').claudeSessionId = undefined;

      await expect(mgr.updateTools('no-id-tools', { allowedTools: ['Read'] })).rejects.toThrow(
        'has no claude session ID',
      );
    });

    it('restarts session with new tools when merge is false', async () => {
      await mgr.startSession({
        name: 'tools-replace',
        cwd: '/tmp',
        allowedTools: ['Read', 'Write'],
      });
      lastMock().setBusy(false);

      await mgr.updateTools('tools-replace', { allowedTools: ['Bash'] });
      // A new session should have been created
      expect(mockSessions.length).toBe(2);
    });

    it('merges tools when merge is true', async () => {
      await mgr.startSession({
        name: 'tools-merge',
        cwd: '/tmp',
        allowedTools: ['Read'],
      });
      lastMock().setBusy(false);

      const info = await mgr.updateTools('tools-merge', {
        allowedTools: ['Write'],
        merge: true,
      });
      expect(info.name).toBe('tools-merge');
    });

    it('uses the agy conversation UUID, not the synthetic session ID, when updating tools', async () => {
      await mgr.startSession({ name: 'agy-tools', cwd: '/tmp', engine: 'agy', model: 'gemini-3.5-flash' });
      lastMock().conversationId = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
      lastMock().setBusy(false);

      await mgr.sendMessage('agy-tools', 'hello');
      await mgr.updateTools('agy-tools', { allowedTools: ['Read'] });

      expect(createdConfigs[1].resumeSessionId).toBe('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee');
      expect(createdConfigs[1].resumeSessionId).not.toMatch(/^mock-session-/);
    });
  });

  // ─── Autoloop role configuration ───────────────────────────────────

  describe('autoloop role configuration', () => {
    it.each(['claude', 'codex', 'codex-app', 'gemini', 'agy', 'cursor', 'grok', 'opencode'] as const)(
      'starts the %s Planner with an enforced read-only sandbox',
      async (plannerEngine) => {
        const runId = `readonly-planner-${plannerEngine}`;

        await mgr.autoloopStart({ runId, workspace: '/tmp', plannerEngine });

        expect(createdConfigs[0]).toMatchObject({
          name: `autoloop-${runId}-planner`,
          engine: plannerEngine,
          permissionMode: plannerEngine === 'claude' ? 'plan' : 'manual',
          sandboxMode: 'read-only',
        });
      },
    );

    it('starts a custom Planner with the same enforced read-only sandbox', async () => {
      const plannerCustomEngine = {
        name: 'readonly-custom',
        bin: 'readonly-custom',
        args: { permissionMode: '--permission-mode' },
        permissionModes: { manual: 'plan' },
      };

      await mgr.autoloopStart({
        runId: 'readonly-planner-custom',
        workspace: '/tmp',
        plannerEngine: 'custom',
        plannerCustomEngine,
      });

      expect(createdConfigs[0]).toMatchObject({
        engine: 'custom',
        permissionMode: 'manual',
        sandboxMode: 'read-only',
        customEngine: plannerCustomEngine,
      });
    });

    it('passes independent role engines, models, and custom configs into dispatcher sessions', async () => {
      const coderCustomEngine = { name: 'coder-cli', bin: 'coder-cli', args: {} };
      await mgr.autoloopStart({
        runId: 'multi-engine',
        workspace: '/tmp',
        plannerEngine: 'codex',
        coderEngine: 'custom',
        coderModel: 'coder-model',
        coderCustomEngine,
        reviewerEngine: 'gemini',
        reviewerModel: 'reviewer-model',
      });
      await mgr.getAutoloop('multi-engine')!.dispatcher.spawnSubagents();

      expect(createdConfigs[0]).toMatchObject({
        name: 'autoloop-multi-engine-planner',
        engine: 'codex',
        model: undefined,
      });
      expect(createdConfigs[1]).toMatchObject({
        name: 'autoloop-multi-engine-coder',
        engine: 'custom',
        model: 'coder-model',
        customEngine: coderCustomEngine,
      });
      expect(createdConfigs[2]).toMatchObject({
        name: 'autoloop-multi-engine-reviewer',
        engine: 'gemini',
        model: 'reviewer-model',
      });
    });

    it('suppresses a global default model for non-Claude roles with no explicit model', async () => {
      await mgr.shutdown();
      mgr = createManager({ defaultModel: 'global-claude-default' });

      await mgr.autoloopStart({ runId: 'no-global-model', workspace: '/tmp', plannerEngine: 'codex' });

      expect(createdConfigs[0]).toHaveProperty('model', undefined);
    });

    it('rejects an unknown role engine before creating a session', async () => {
      await expect(
        mgr.autoloopStart({
          runId: 'bad-engine',
          workspace: '/tmp',
          plannerEngine: 'not-real' as 'claude',
        }),
      ).rejects.toThrow("Planner engine 'not-real' is not supported");
      expect(createdConfigs).toEqual([]);
    });

    it('rejects a custom Planner without its trusted config before creating a session', async () => {
      await expect(
        mgr.autoloopStart({ runId: 'missing-custom', workspace: '/tmp', plannerEngine: 'custom' }),
      ).rejects.toThrow('Planner custom engine config is required');
      expect(createdConfigs).toEqual([]);
    });

    it('rejects malformed custom engine configs before creating a session', async () => {
      await expect(
        mgr.autoloopStart({
          runId: 'malformed-custom',
          workspace: '/tmp',
          plannerEngine: 'custom',
          plannerCustomEngine: {
            name: 'bad-custom',
            bin: 'custom-cli',
            args: null,
          } as unknown as NonNullable<SessionConfig['customEngine']>,
        }),
      ).rejects.toThrow('Planner custom engine config.args must be an object');
      expect(createdConfigs).toEqual([]);

      await expect(
        mgr.autoloopStart({
          runId: 'malformed-custom-flag',
          workspace: '/tmp',
          plannerEngine: 'custom',
          plannerCustomEngine: {
            name: 'bad-custom',
            bin: 'custom-cli',
            args: { permissionMode: 42 },
          } as unknown as NonNullable<SessionConfig['customEngine']>,
        }),
      ).rejects.toThrow('Planner custom engine config.args.permissionMode must be a string');
      expect(createdConfigs).toEqual([]);
    });

    it('rejects an autoloop whose reserved Planner session name is already active', async () => {
      await mgr.startSession({
        name: 'autoloop-name-collision-planner',
        cwd: '/tmp',
        engine: 'cursor',
        sandboxMode: 'workspace-write',
      });

      await expect(
        mgr.autoloopStart({ runId: 'name-collision', workspace: '/tmp', plannerEngine: 'codex' }),
      ).rejects.toThrow("Autoloop session name 'autoloop-name-collision-planner' is already in use");
      expect(mgr.getAutoloop('name-collision')).toBeUndefined();
    });

    it('rejects delete while an Autoloop Planner is still starting', async () => {
      let releaseStart!: () => void;
      const startGate = new Promise<void>((resolve) => {
        releaseStart = resolve;
      });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (mgr as any)._createSession = (): ISession => {
        const mock = new MockSession();
        mock.start = async () => {
          await startGate;
          mock.sessionId = 'slow-planner-session';
          return mock;
        };
        mockSessions.push(mock);
        return mock;
      };

      const starting = mgr.autoloopStart({ runId: 'slow-start', workspace: '/tmp' });
      // The live handle is published only once the engine is up, which is
      // exactly what this test blocks. "A start is in flight" is observable from
      // the run record existing while nothing has been published on it yet.
      await vi.waitFor(() => expect(mgr.workflowList({ workflow: 'autoloop' }).length).toBe(1));

      await expect(mgr.autoloopDelete('slow-start')).rejects.toThrow("Autoloop with id 'slow-start' is still starting");
      releaseStart();
      await expect(starting).resolves.toMatchObject({ runId: 'slow-start' });
    });

    it('removes a failed autoloop start so the same run id can be retried', async () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (mgr as any)._createSession = (): ISession => {
        const mock = new MockSession();
        mock.start = async () => {
          throw new Error('planner startup failed');
        };
        return mock;
      };

      await expect(mgr.autoloopStart({ runId: 'retry-start', workspace: '/tmp' })).rejects.toThrow(
        'planner startup failed',
      );
      expect(mgr.getAutoloop('retry-start')).toBeUndefined();

      patchCreateSession(mgr);
      await expect(mgr.autoloopStart({ runId: 'retry-start', workspace: '/tmp' })).resolves.toMatchObject({
        runId: 'retry-start',
      });
    });

    it('leaves the stored run intact when a resume fails to start', async () => {
      // The behaviour this protects: a resume that cannot bring the Planner up
      // must not destroy the record, or the run becomes unrecoverable. It used
      // to be phrased against an append-only registry file; the record is the
      // registry now, so that is what gets checked.
      await mgr.autoloopStart({ runId: 'resume-fail', workspace: '/tmp' });
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const kernel = (mgr as any).kernel;
      kernel.cancel('resume-fail');
      await kernel.wait('resume-fail');

      const before = mgr.workflowStatus('resume-fail');
      expect(before).toBeDefined();

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (mgr as any)._createSession = (): ISession => {
        const mock = new MockSession();
        mock.start = async () => {
          throw new Error('resume planner failed');
        };
        return mock;
      };
      await expect(mgr.autoloopResume('resume-fail')).rejects.toThrow('resume planner failed');

      const after = mgr.workflowStatus('resume-fail');
      expect(after).toBeDefined();
      expect(after.spec).toEqual(before!.spec);
    });

    describe('strict Planner turn success', () => {
      it('rejects transport success with an empty logical reply without advancing phase', async () => {
        const runId = 'planner-empty-reply';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: '   ',
          event: { type: 'result', result: '   ' },
        });
        const phaseErrors: PhaseErrorPayload[] = [];
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));

        await expect(mgr.autoloopChat(runId, 'return a reply')).rejects.toMatchObject({
          code: 'AUTOLOOP_EMPTY_REPLY',
        });
        expect(phaseErrors).toEqual([
          {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_EMPTY_REPLY',
            error: 'Planner transport completed without a non-empty logical reply',
          },
        ]);
        expect(handle.runner.state).toMatchObject({
          status: 'planning',
          iter: 0,
          subagents_spawned: false,
          consecutive_phase_errors: 1,
        });
      });

      it('rejects an empty logical reply when optional turn counters are unavailable', async () => {
        const runId = 'planner-empty-reply-no-counters';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: '   ',
          event: { type: 'result', result: '   ' },
        });
        vi.spyOn(mgr, 'getStatus').mockImplementation(() => {
          throw new Error('optional counters unavailable');
        });

        await expect(mgr.autoloopChat(runId, 'return a reply')).rejects.toMatchObject({
          code: 'AUTOLOOP_EMPTY_REPLY',
        });
      });

      it('normalizes a raw Planner boundary failure before one durable and one Runner phase-error path', async () => {
        const runId = 'planner-raw-boundary-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const cause = new Error('runtime liveness probe failed');
        vi.spyOn(mgr, 'inspect').mockRejectedValueOnce(cause);
        mockSessions[0].sendImplementation = async () => ({
          text: 'reply whose generation cannot be verified',
          event: { type: 'result', result: 'reply whose generation cannot be verified' },
        });

        const originalDeliver = handle.dispatcher.deliver.bind(handle.dispatcher);
        let boundaryFailure: unknown;
        vi.spyOn(handle.dispatcher, 'deliver').mockImplementation(async (env) => {
          try {
            return await originalDeliver(env);
          } catch (error) {
            boundaryFailure = error;
            throw error;
          }
        });
        const phaseErrors: PhaseErrorPayload[] = [];
        const pushes: Array<{ summary: string }> = [];
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));
        handle.runner.on('push', (payload: { summary: string }) => pushes.push(payload));

        let callerFailure: unknown;
        try {
          await mgr.autoloopChat(runId, 'exercise the raw dispatcher boundary');
        } catch (error) {
          callerFailure = error;
        }

        expect(boundaryFailure).toBeInstanceOf(AutoloopOperationError);
        expect(callerFailure).toBe(boundaryFailure);
        expect(callerFailure).toMatchObject({
          name: 'AutoloopOperationError',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          cause,
          message: 'Planner engine transport failed: runtime liveness probe failed',
        });
        expect(phaseErrors).toEqual([
          {
            agent: 'planner',
            phase: 'planner_turn',
            code: 'AUTOLOOP_ENGINE_FAILURE',
            error: 'Planner engine transport failed: runtime liveness probe failed',
          },
        ]);
        expect(pushes.map(({ summary }) => summary)).toEqual(['[on_phase_error] iter 0']);
        expect(handle.runner.state).toMatchObject({
          consecutive_phase_errors: 1,
          push_log_count: 1,
        });
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: PhaseErrorPayload });
        expect(
          decisions.filter(
            (row) =>
              row.kind === 'phase_error' &&
              row.payload.agent === 'planner' &&
              row.payload.code === 'AUTOLOOP_ENGINE_FAILURE',
          ),
        ).toEqual([
          expect.objectContaining({
            payload: expect.objectContaining({
              error: 'Planner engine transport failed: runtime liveness probe failed',
            }),
          }),
        ]);
      });

      it('rethrows the same specific typed Planner failure instead of normalizing it to engine failure', async () => {
        const runId = 'planner-specific-boundary-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
          event: { type: 'result', result: 'malformed control' },
        });
        const originalDeliver = handle.dispatcher.deliver.bind(handle.dispatcher);
        let boundaryFailure: unknown;
        vi.spyOn(handle.dispatcher, 'deliver').mockImplementation(async (env) => {
          try {
            return await originalDeliver(env);
          } catch (error) {
            boundaryFailure = error;
            throw error;
          }
        });

        let callerFailure: unknown;
        try {
          await mgr.autoloopChat(runId, 'preserve the specific failure');
        } catch (error) {
          callerFailure = error;
        }

        expect(boundaryFailure).toBeInstanceOf(AutoloopOperationError);
        expect(callerFailure).toBe(boundaryFailure);
        expect(callerFailure).toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
        });
      });

      it('creates the Planner control ledger owner-only and hardens pre-existing weak permissions', async () => {
        const runId = 'planner-private-control-ledger';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        fs.mkdirSync(ledgerDir, { recursive: true, mode: 0o777 });
        fs.chmodSync(ledgerDir, 0o775);
        fs.writeFileSync(decisionsPath, '', { mode: 0o666 });
        fs.chmodSync(decisionsPath, 0o664);

        await mgr.autoloopStart({ runId, workspace });

        expect(fs.statSync(ledgerDir).mode & 0o777).toBe(0o700);
        expect(fs.statSync(decisionsPath).mode & 0o777).toBe(0o600);
      });

      it('creates a new Planner ledger directory and decisions file with owner-only permissions', async () => {
        const runId = 'planner-new-private-control-ledger';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
          event: { type: 'result', result: 'invalid control' },
        });

        await expect(mgr.autoloopChat(runId, 'create private decision evidence')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
        });

        expect(fs.statSync(ledgerDir).mode & 0o777).toBe(0o700);
        expect(fs.statSync(decisionsPath).mode & 0o777).toBe(0o600);
      });

      it('does not weaken already owner-only Planner control ledger permissions', async () => {
        const runId = 'planner-private-control-ledger-preserved';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        fs.mkdirSync(ledgerDir, { recursive: true, mode: 0o700 });
        fs.chmodSync(ledgerDir, 0o700);
        fs.writeFileSync(decisionsPath, '', { mode: 0o600 });
        fs.chmodSync(decisionsPath, 0o600);

        await mgr.autoloopStart({ runId, workspace });

        expect(fs.statSync(ledgerDir).mode & 0o777).toBe(0o700);
        expect(fs.statSync(decisionsPath).mode & 0o777).toBe(0o600);
      });

      it('refuses a pre-planted Planner ledger-directory symlink without writing its target', async () => {
        const runId = 'planner-ledger-directory-symlink';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        const tasksDir = path.join(workspace, 'tasks');
        const external = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-external-`));
        fs.mkdirSync(tasksDir, { recursive: true });
        fs.symlinkSync(external, path.join(tasksDir, runId), 'dir');

        await expect(mgr.autoloopStart({ runId, workspace })).rejects.toThrow(/ledger.*symbolic link/i);
        expect(fs.readdirSync(external)).toEqual([]);
      });

      it('refuses a pre-planted decisions symlink without changing its external target', async () => {
        const runId = 'planner-decisions-symlink';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const external = path.join(TEST_WF_DIR, `${runId}-external.txt`);
        fs.mkdirSync(ledgerDir, { recursive: true });
        fs.writeFileSync(external, 'external decision sentinel\n');
        fs.symlinkSync(external, path.join(ledgerDir, 'decisions.jsonl'));

        await expect(mgr.autoloopStart({ runId, workspace })).rejects.toThrow(/decisions\.jsonl.*symbolic link/i);
        expect(fs.readFileSync(external, 'utf8')).toBe('external decision sentinel\n');
      });

      it.each(['unavailable', 'non-finite'] as const)(
        'fails closed before persisting a fenced AGY control when Planner success counters are %s',
        async (counterState) => {
          const runId = `planner-fenced-control-${counterState}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace, plannerEngine: 'agy' });
          const handle = mgr.getAutoloop(runId)!;
          mockSessions[0].sendImplementation = async () => ({
            text: ['starting agents', '```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
            event: { type: 'result', result: 'starting agents' },
          });
          const originalGetStatus = mgr.getStatus.bind(mgr);
          vi.spyOn(mgr, 'getStatus').mockImplementation((name) => {
            if (counterState === 'unavailable') throw new Error('authoritative Planner counters unavailable');
            const status = originalGetStatus(name);
            return { ...status, stats: { ...status.stats, turnsSucceeded: Number.NaN } };
          });
          const phaseErrors: PhaseErrorPayload[] = [];
          handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));

          await expect(mgr.autoloopChat(runId, 'start the approved implementation')).rejects.toMatchObject({
            code: 'AUTOLOOP_REQUIRED_TOOL_DENIED',
            retryable: true,
          });

          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: { code?: string } });
          expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toEqual([]);
          expect(decisions.filter((row) => row.kind === 'spawn_subagents')).toEqual([]);
          expect(mockSessions).toHaveLength(1);
          expect(phaseErrors.map(({ code }) => code)).toEqual(['AUTOLOOP_REQUIRED_TOOL_DENIED']);
          expect(handle.runner.state).toMatchObject({
            status: 'planning',
            iter: 0,
            subagents_spawned: false,
            consecutive_phase_errors: 1,
          });
        },
      );

      it('surfaces a verified fences-only control as an unambiguous logical result', async () => {
        const runId = 'planner-fences-only';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"update_push_policy","args":{"on_start":{"level":"info"}}}', '```'].join('\n'),
          event: { type: 'result', result: 'control only' },
        });

        await expect(mgr.autoloopChat(runId, 'apply the approved policy')).resolves.toEqual({
          reply: 'Planner controls persisted: update_push_policy',
        });
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> });
        expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload).toMatchObject({
          tools: ['update_push_policy'],
          controls: [{ tool: 'update_push_policy', args: { on_start: { level: 'info' } } }],
        });
      });

      it('normalizes Planner control property order before digest, persistence, comparison, and application', async () => {
        const runId = 'planner-control-semantic-order';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            '{"args":{"on_start":{"level":"info","channel":"auto"},"on_iter_done_ok":{"level":"warn","channel":"both"}},"tool":"update_push_policy"}',
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'control only' },
        });

        await expect(mgr.autoloopChat(runId, 'apply the approved policy')).resolves.toEqual({
          reply: 'Planner controls persisted: update_push_policy',
        });
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> });
        expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload).toMatchObject({
          controls: [
            {
              tool: 'update_push_policy',
              args: {
                on_iter_done_ok: { channel: 'both', level: 'warn' },
                on_start: { channel: 'auto', level: 'info' },
              },
            },
          ],
          controls_sha256: '3f044aad5fc0d2583e26bb8f235c0b8a7ce53f35c0d500e947a7bb3fb4b055f4',
        });
        expect(handle.runner.config.push_policy).toMatchObject({
          on_iter_done_ok: { channel: 'both', level: 'warn' },
          on_start: { channel: 'auto', level: 'info' },
        });
      });

      it('keeps every concurrent runner sender pending until the active drain reaches idle', async () => {
        const runId = 'planner-runner-drain-waiters';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        let releaseFirst!: () => void;
        const firstGate = new Promise<void>((resolve) => {
          releaseFirst = resolve;
        });
        let markFirstEntered!: () => void;
        const firstEntered = new Promise<void>((resolve) => {
          markFirstEntered = resolve;
        });
        let turn = 0;
        mockSessions[0].sendImplementation = async () => {
          turn += 1;
          if (turn === 1) {
            markFirstEntered();
            await firstGate;
          }
          return { text: `runner reply ${turn}`, event: { type: 'result', result: `runner reply ${turn}` } };
        };

        const first = handle.runner.send(AutoloopMsg.chat(0, { text: 'first direct runner chat' }));
        await firstEntered;
        let secondSettled = false;
        const second = handle.runner.send(AutoloopMsg.chat(0, { text: 'second direct runner chat' })).finally(() => {
          secondSettled = true;
        });
        await new Promise<void>((resolve) => nativeSetImmediate(resolve));

        expect(secondSettled).toBe(false);
        releaseFirst();
        await expect(Promise.all([first, second])).resolves.toEqual([undefined, undefined]);
        expect(mockSessions[0].sendCalls).toHaveLength(2);
      });

      it('publishes the active drain before synchronous message listeners can send again', async () => {
        const runId = 'planner-runner-reentrant-drain-waiter';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        let releasePlanner!: () => void;
        const plannerGate = new Promise<void>((resolve) => {
          releasePlanner = resolve;
        });
        let markPlannerEntered!: () => void;
        const plannerEntered = new Promise<void>((resolve) => {
          markPlannerEntered = resolve;
        });
        mockSessions[0].sendImplementation = async () => {
          markPlannerEntered();
          await plannerGate;
          return { text: 'planner reply', event: { type: 'result', result: 'planner reply' } };
        };
        let reentrantSettled = false;
        let reentrant: Promise<void> | undefined;
        handle.runner.on('message', (env: { type?: string }) => {
          if (env.type !== 'chat' || reentrant) return;
          reentrant = handle.runner.send(AutoloopMsg.pause(0, { reason: 'listener pause' })).finally(() => {
            reentrantSettled = true;
          });
        });

        const first = handle.runner.send(AutoloopMsg.chat(0, { text: 'trigger synchronous listener' }));
        await plannerEntered;
        await new Promise<void>((resolve) => nativeSetImmediate(resolve));

        expect(reentrant).toBeDefined();
        expect(reentrantSettled).toBe(false);
        releasePlanner();
        await expect(Promise.all([first, reentrant!])).resolves.toEqual([undefined, undefined]);
        expect(handle.runner.state).toMatchObject({ status: 'paused', status_reason: 'listener pause' });
      });

      it('serializes overlapping chats so each caller receives only its own ordered Planner reply', async () => {
        const runId = 'planner-overlapping-chat-replies';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        let releaseFirst!: () => void;
        const firstGate = new Promise<void>((resolve) => {
          releaseFirst = resolve;
        });
        let markFirstEntered!: () => void;
        const firstEntered = new Promise<void>((resolve) => {
          markFirstEntered = resolve;
        });
        let turn = 0;
        mockSessions[0].sendImplementation = async () => {
          turn += 1;
          if (turn === 1) {
            markFirstEntered();
            await firstGate;
          }
          return { text: `reply ${turn}`, event: { type: 'result', result: `reply ${turn}` } };
        };

        const first = mgr.autoloopChat(runId, 'first user chat');
        await firstEntered;
        let secondSettled = false;
        const second = mgr
          .autoloopChat(runId, 'second user chat')
          .then(
            (value) => ({ value }),
            (error: unknown) => ({ error }),
          )
          .finally(() => {
            secondSettled = true;
          });
        await new Promise<void>((resolve) => nativeSetImmediate(resolve));

        expect(secondSettled).toBe(false);
        expect(mockSessions[0].sendCalls).toHaveLength(1);
        releaseFirst();
        await expect(first).resolves.toEqual({ reply: 'reply 1' });
        await expect(second).resolves.toEqual({ value: { reply: 'reply 2' } });
        expect(mockSessions[0].sendCalls).toHaveLength(2);
        expect(
          (mgr as unknown as { _autoloopChatTransactions: Map<string, Promise<void>> })._autoloopChatTransactions.size,
        ).toBe(0);
      });

      it('does not let a rejected overlapping chat poison or consume the following chat reply', async () => {
        const runId = 'planner-overlapping-chat-rejection';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        let releaseFirst!: () => void;
        const firstGate = new Promise<void>((resolve) => {
          releaseFirst = resolve;
        });
        let markFirstEntered!: () => void;
        const firstEntered = new Promise<void>((resolve) => {
          markFirstEntered = resolve;
        });
        let turn = 0;
        mockSessions[0].sendImplementation = async () => {
          turn += 1;
          if (turn === 1) {
            markFirstEntered();
            await firstGate;
            return {
              text: ['```autoloop', '{"tool":"notify_user","args":{}}', '```'].join('\n'),
              event: { type: 'result', result: 'rejected first control' },
            };
          }
          return { text: 'fresh second reply', event: { type: 'result', result: 'fresh second reply' } };
        };

        const first = mgr.autoloopChat(runId, 'first rejected user chat');
        await firstEntered;
        let secondSettled = false;
        const second = mgr
          .autoloopChat(runId, 'second valid user chat')
          .then(
            (value) => ({ value }),
            (error: unknown) => ({ error }),
          )
          .finally(() => {
            secondSettled = true;
          });
        await new Promise<void>((resolve) => nativeSetImmediate(resolve));

        expect(secondSettled).toBe(false);
        expect(mockSessions[0].sendCalls).toHaveLength(1);
        releaseFirst();
        await expect(first).rejects.toMatchObject({ code: 'AUTOLOOP_CONTROL_MALFORMED' });
        await expect(second).resolves.toEqual({ value: { reply: 'fresh second reply' } });
        expect(mockSessions[0].sendCalls).toHaveLength(2);
        expect(
          (mgr as unknown as { _autoloopChatTransactions: Map<string, Promise<void>> })._autoloopChatTransactions.size,
        ).toBe(0);
      });

      it('rejects a Planner reply when its physical generation is absent after send', async () => {
        const runId = 'planner-session-absent';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        vi.spyOn(mgr, 'inspect').mockResolvedValue('absent');

        await expect(mgr.autoloopChat(runId, 'return a reply')).rejects.toMatchObject({
          code: 'AUTOLOOP_SESSION_NOT_CREATED',
        });
        expect(handle.runner.state).toMatchObject({
          status: 'planning',
          iter: 0,
          subagents_spawned: false,
        });
      });

      it('rejects a Planner generation whose post-send liveness is unknown', async () => {
        const runId = 'planner-session-unknown';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        vi.spyOn(mgr, 'inspect').mockResolvedValue('unknown');

        await expect(mgr.autoloopChat(runId, 'return a reply')).rejects.toMatchObject({
          code: 'AUTOLOOP_SESSION_NOT_CREATED',
        });
      });

      it('rejects a Planner turn whose required tool was denied without advancing phase', async () => {
        const runId = 'planner-required-tool-denied';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].turnsSucceededOverride = 0;

        await expect(mgr.autoloopChat(runId, 'inspect with the required tool')).rejects.toMatchObject({
          code: 'AUTOLOOP_REQUIRED_TOOL_DENIED',
        });
        expect(handle.runner.state).toMatchObject({
          status: 'planning',
          iter: 0,
          subagents_spawned: false,
        });
      });

      it('classifies an engine result failure separately from a required-tool denial', async () => {
        const runId = 'planner-engine-result-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: 'engine authentication failed',
          event: { type: 'result', result: 'engine authentication failed', is_error: true },
        });

        await expect(mgr.autoloopChat(runId, 'inspect before planning')).rejects.toMatchObject({
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
        });
      });

      it('wraps a thrown Planner transport failure in the engine failure taxonomy', async () => {
        const runId = 'planner-transport-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => {
          throw Object.assign(new Error('planner socket reset'), { code: 'ECONNRESET' });
        };

        await expect(mgr.autoloopChat(runId, 'inspect before planning')).rejects.toMatchObject({
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
        });
      });

      it('classifies malformed Planner control syntax without applying or persisting controls', async () => {
        const runId = 'planner-control-malformed';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
          event: { type: 'result', result: 'malformed control' },
        });

        await expect(mgr.autoloopChat(runId, 'start the implementation')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
        });
        expect(mockSessions).toHaveLength(1);
        const decisions = fs.readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8');
        expect(decisions).not.toContain('planner_turn_control');
      });

      it('rejects array spawn_subagents arguments before durable evidence or session effects', async () => {
        const runId = 'planner-control-array-arguments';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":[]}', '```'].join('\n'),
          event: { type: 'result', result: 'array arguments are invalid' },
        });

        await expect(mgr.autoloopChat(runId, 'start the implementation')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
        });
        expect(mockSessions).toHaveLength(1);
        const decisions = fs.readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8');
        expect(decisions).not.toContain('planner_turn_control');
        expect(decisions).not.toContain('spawn_subagents');
      });

      it('classifies invalid Planner control arguments as malformed before persistence', async () => {
        const runId = 'planner-control-invalid-arguments';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"notify_user","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'invalid control arguments' },
        });

        await expect(mgr.autoloopChat(runId, 'notify me')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
        });
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { code?: string } });
        expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toEqual([]);
        expect(decisions.filter((row) => row.kind === 'phase_error').at(-1)?.payload.code).toBe(
          'AUTOLOOP_CONTROL_MALFORMED',
        );
      });

      it('bounds every durable Planner metadata field by UTF-8 bytes and every directive array by count', () => {
        const metadataLimit = 8_192;
        const arrayItemLimit = 128;
        const oversizedAscii = 'x'.repeat(metadataLimit + 1);
        const oversizedUtf8 = '🚀'.repeat(metadataLimit / 4 + 1);
        const tooManyItems = Array.from({ length: arrayItemLimit + 1 }, () => 'bounded');
        const cases: Array<{ label: string; control: PlannerToolCall; expected: string }> = [
          {
            label: 'notify summary',
            control: { tool: 'notify_user', args: { summary: oversizedUtf8 } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'notify detail',
            control: { tool: 'notify_user', args: { summary: 'status', detail: oversizedAscii } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'directive goal',
            control: { tool: 'send_directive', args: { goal: oversizedAscii } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'directive constraint element',
            control: { tool: 'send_directive', args: { goal: 'ship', constraints: [oversizedUtf8] } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'directive success-criteria count',
            control: { tool: 'send_directive', args: { goal: 'ship', success_criteria: tooManyItems } },
            expected: `${arrayItemLimit}-item limit`,
          },
          {
            label: 'pause reason',
            control: { tool: 'pause_loop', args: { reason: oversizedAscii } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'terminate reason',
            control: { tool: 'terminate', args: { reason: oversizedUtf8 } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'spawn initial directive goal',
            control: {
              tool: 'spawn_subagents',
              args: { initial_directive: { goal: oversizedAscii } },
            },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'spawn initial directive constraint',
            control: {
              tool: 'spawn_subagents',
              args: { initial_directive: { goal: 'ship', constraints: [oversizedUtf8] } },
            },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'coder model',
            control: { tool: 'spawn_subagents', args: { coder_model: oversizedUtf8 } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'reviewer model',
            control: { tool: 'spawn_subagents', args: { reviewer_model: oversizedAscii } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'plan commit message',
            control: { tool: 'write_plan', args: { content: '# plan', commit_message: oversizedAscii } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
          {
            label: 'goal commit message',
            control: { tool: 'write_goal', args: { content: '{}', commit_message: oversizedUtf8 } },
            expected: `${metadataLimit}-byte UTF-8 limit`,
          },
        ];

        for (const { label, control, expected } of cases) {
          const validation = validatePlannerToolCalls([control]);
          expect(validation.calls, label).toEqual([]);
          expect(validation.errors[0]?.error, label).toContain(expected);
        }
      });

      it('accepts exact metadata, array-count, content, and normalized serialized-batch boundaries', () => {
        const metadataLimit = 8_192;
        const arrayItemLimit = 128;
        const contentLimit = 1_048_576;
        const batchLimit = 1_114_112;
        const exactUtf8 = '🚀'.repeat(metadataLimit / 4);
        const exactItems = Array.from({ length: arrayItemLimit }, () => 'bounded');

        expect(
          validatePlannerToolCalls([
            { tool: 'notify_user', args: { summary: exactUtf8, detail: 'x'.repeat(metadataLimit) } },
            {
              tool: 'send_directive',
              args: { goal: 'g'.repeat(metadataLimit), constraints: exactItems, success_criteria: exactItems },
            },
            {
              tool: 'spawn_subagents',
              args: { coder_model: 'c'.repeat(metadataLimit), reviewer_model: exactUtf8 },
            },
            {
              tool: 'write_goal',
              args: { content: '{}', commit_message: 'm'.repeat(metadataLimit) },
            },
          ]).errors,
        ).toEqual([]);

        // One content allocation is shared by both the exact and +1-byte batch
        // checks. The literal component lengths make the JSON boundary exact:
        // 1 MiB content + the write_plan envelope, then three full notify
        // envelopes and one shortened envelope. Canonical notify defaults are
        // included in every normalized row before this byte limit is applied.
        const content = 'p'.repeat(contentLimit);
        const exactBatch: PlannerToolCall[] = [
          { tool: 'write_plan', args: { content } },
          ...Array.from({ length: 3 }, () => ({
            tool: 'notify_user' as const,
            args: { summary: 's'.repeat(metadataLimit), detail: 'd'.repeat(metadataLimit) },
          })),
          {
            tool: 'notify_user',
            args: { summary: 's'.repeat(metadataLimit), detail: 'd'.repeat(7_739) },
          },
        ];
        const exact = validatePlannerToolCalls(exactBatch);
        expect(exact.errors).toEqual([]);
        expect(Buffer.byteLength(exact.controls_json ?? '', 'utf8')).toBe(batchLimit);

        const overBoundary = exactBatch.map((control, index) =>
          index === exactBatch.length - 1
            ? {
                tool: control.tool,
                args: { ...control.args, detail: `${String(control.args.detail)}x` },
              }
            : control,
        );
        const oversized = validatePlannerToolCalls(overBoundary);
        expect(oversized.calls).toEqual([]);
        expect(oversized.errors[0]?.error).toContain(`${batchLimit}-byte UTF-8 limit`);
      });

      it('rejects too many controls before allocating a durable batch', () => {
        const controls = Array.from<unknown, PlannerToolCall>({ length: 65 }, () => ({
          tool: 'notify_user',
          args: { summary: 'bounded' },
        }));

        const validation = validatePlannerToolCalls(controls);

        expect(validation.calls).toEqual([]);
        expect(validation.errors[0]?.error).toContain('64-control limit');
      });

      it('rejects an oversized mixed batch at the chat boundary without a durable control or earlier effect', async () => {
        const runId = 'planner-oversized-mixed-batch';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');
        const individuallyValidControls: PlannerToolCall[] = [
          { tool: 'spawn_subagents', args: {} },
          { tool: 'write_plan', args: { content: 'p'.repeat(1_048_576) } },
          ...Array.from({ length: 4 }, () => ({
            tool: 'notify_user' as const,
            args: { summary: 's'.repeat(8_192), detail: 'd'.repeat(8_192) },
          })),
        ];
        mockSessions[0].sendImplementation = async () => ({
          text: individuallyValidControls
            .flatMap((control) => ['```autoloop', JSON.stringify(control), '```'])
            .join('\n'),
          event: { type: 'result', result: 'oversized mixed batch' },
        });

        try {
          await expect(mgr.autoloopChat(runId, 'apply the oversized mixed batch')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_MALFORMED',
            retryable: false,
          });
          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string });
          expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toEqual([]);
          expect(decisions.filter((row) => row.kind === 'spawn_subagents')).toEqual([]);
          expect(spawn).not.toHaveBeenCalled();
          expect(mockSessions).toHaveLength(1);
          expect(fs.existsSync(path.join(workspace, 'plan.md'))).toBe(false);
          expect(handle.runner.state).toMatchObject({ status: 'planning', subagents_spawned: false });
        } finally {
          spawn.mockRestore();
        }
      });

      it('rejects a truly empty push-policy control without durable evidence or an effect', async () => {
        const runId = 'planner-empty-push-policy-control';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const policyBefore = JSON.stringify(handle.runner.config.push_policy);
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"update_push_policy","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'empty policy control' },
        });

        await expect(mgr.autoloopChat(runId, 'apply an empty policy update')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
        });
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string });
        expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toEqual([]);
        expect(decisions.filter((row) => row.kind === 'update_push_policy')).toEqual([]);
        expect(JSON.stringify(handle.runner.config.push_policy)).toBe(policyBefore);
      });

      it('rejects and sanitizes the complete control batch before persisting or applying an earlier spawn', async () => {
        const runId = 'planner-batch-prevalidation';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const secretMarker = 'R5_REJECTED_CUSTOM_ENGINE_SECRET';
        const pushes: Array<{ summary: string }> = [];
        handle.runner.on('push', (payload: { summary: string }) => pushes.push(payload));
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            '{"tool":"spawn_subagents","args":{}}',
            '```',
            '```autoloop',
            JSON.stringify({
              tool: 'spawn_subagents',
              args: { coder_custom_engine: { env: { TOKEN: secretMarker } } },
            }),
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'apply controls' },
        });

        await expect(mgr.autoloopChat(runId, 'apply the complete control batch')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
        });

        const decisionText = fs.readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8');
        const decisions = decisionText
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> });
        expect(decisionText).not.toContain(secretMarker);
        expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toEqual([]);
        expect(decisions.filter((row) => row.kind === 'spawn_subagents')).toEqual([]);
        expect(mockSessions).toHaveLength(1);
        expect(pushes.map(({ summary }) => summary)).toEqual(['[on_phase_error] iter 0']);
        expect(handle.runner.state).toMatchObject({
          status: 'planning',
          iter: 0,
          subagents_spawned: false,
          push_log_count: 1,
        });
      });

      const invalidNestedPlannerControls: Array<{ label: string; control: Record<string, unknown> }> = [
        {
          label: 'spawn initial_directive goal',
          control: { tool: 'spawn_subagents', args: { initial_directive: { goal: '   ' } } },
        },
        {
          label: 'spawn initial_directive constraints',
          control: {
            tool: 'spawn_subagents',
            args: { initial_directive: { goal: 'ship', constraints: ['safe', 7] } },
          },
        },
        {
          label: 'spawn initial_directive success criteria',
          control: {
            tool: 'spawn_subagents',
            args: { initial_directive: { goal: 'ship', success_criteria: ['green', false] } },
          },
        },
        {
          label: 'spawn initial_directive max attempts',
          control: {
            tool: 'spawn_subagents',
            args: { initial_directive: { goal: 'ship', max_attempts: 0 } },
          },
        },
        {
          label: 'send_directive string arrays',
          control: { tool: 'send_directive', args: { goal: 'ship', constraints: ['safe', null] } },
        },
        {
          label: 'send_directive positive integer max attempts',
          control: { tool: 'send_directive', args: { goal: 'ship', max_attempts: 1.5 } },
        },
        {
          label: 'notify_user level',
          control: { tool: 'notify_user', args: { summary: 'status', level: 'debug' } },
        },
        {
          label: 'notify_user channel',
          control: { tool: 'notify_user', args: { summary: 'status', channel: 'sms' } },
        },
      ];

      it.each(invalidNestedPlannerControls)(
        'rejects invalid $label before control persistence or effects',
        async ({ label, control }) => {
          const runId = `planner-invalid-${label.replaceAll(' ', '-').replaceAll('_', '-')}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const pushes: Array<{ summary: string }> = [];
          handle.runner.on('push', (payload: { summary: string }) => pushes.push(payload));
          mockSessions[0].sendImplementation = async () => ({
            text: ['```autoloop', JSON.stringify(control), '```'].join('\n'),
            event: { type: 'result', result: 'invalid nested control' },
          });

          await expect(mgr.autoloopChat(runId, 'apply invalid control')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_MALFORMED',
            retryable: false,
          });

          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string });
          expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toEqual([]);
          expect(decisions.filter((row) => row.kind === 'spawn_subagents')).toEqual([]);
          expect(mockSessions).toHaveLength(1);
          expect(pushes.map(({ summary }) => summary)).toEqual(['[on_phase_error] iter 0']);
        },
      );

      const invalidPushPolicies: Array<{ label: string; args: Record<string, unknown> }> = [
        { label: 'level', args: { on_start: { level: 'debug' } } },
        { label: 'channel', args: { on_start: { channel: 'sms' } } },
        { label: 'silent flag', args: { on_start: { silent: 'yes' } } },
        { label: 'rule value', args: { on_start: false } },
        { label: 'policy key', args: { on_unknown_event: { level: 'info' } } },
        { label: 'rule field', args: { on_start: { untrusted: true } } },
      ];

      it.each(invalidPushPolicies)(
        'rejects an invalid push-policy $label atomically without changing live policy',
        async ({ label, args }) => {
          const runId = `planner-invalid-push-policy-${label.replaceAll(' ', '-')}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const policyBefore = JSON.stringify(handle.runner.config.push_policy);
          mockSessions[0].sendImplementation = async () => ({
            text: [
              '```autoloop',
              '{"tool":"spawn_subagents","args":{}}',
              '```',
              '```autoloop',
              JSON.stringify({ tool: 'update_push_policy', args }),
              '```',
            ].join('\n'),
            event: { type: 'result', result: 'invalid push policy batch' },
          });

          await expect(mgr.autoloopChat(runId, 'apply the policy batch')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_MALFORMED',
            retryable: false,
          });

          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string });
          expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toEqual([]);
          expect(decisions.filter((row) => row.kind === 'spawn_subagents')).toEqual([]);
          expect(mockSessions).toHaveLength(1);
          expect(JSON.stringify(handle.runner.config.push_policy)).toBe(policyBefore);
        },
      );

      it('treats an explicitly empty push-policy rule as an intentional reset', async () => {
        const runId = 'planner-empty-push-policy-rule-reset';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"update_push_policy","args":{"on_start":{}}}', '```'].join('\n'),
          event: { type: 'result', result: 'reset push policy rule' },
        });

        await expect(mgr.autoloopChat(runId, 'reset the start policy')).resolves.toEqual({
          reply: 'Planner controls persisted: update_push_policy',
        });
        expect(handle.runner.config.push_policy?.on_start).toEqual({});
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { controls?: unknown } });
        expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload.controls).toEqual([
          { tool: 'update_push_policy', args: { on_start: {} } },
        ]);
      });

      it('persists and applies only allowlisted arguments from an accepted Planner control', async () => {
        const runId = 'planner-control-allowlist';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const secretMarker = 'R5_IGNORED_CONTROL_SECRET';
        const pushes: Array<{ level: string; summary: string; detail?: string; channel: string }> = [];
        handle.runner.on('push', (payload: { level: string; summary: string; detail?: string; channel: string }) =>
          pushes.push(payload),
        );
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            JSON.stringify({
              tool: 'notify_user',
              args: {
                level: 'info',
                summary: 'allowlisted status',
                channel: 'auto',
                ignored: { token: secretMarker },
              },
            }),
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'accepted sanitized control' },
        });

        await expect(mgr.autoloopChat(runId, 'apply sanitized control')).resolves.toEqual({
          reply: 'Planner controls persisted: notify_user',
        });

        const decisionText = fs.readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8');
        const decisions = decisionText
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> });
        expect(decisionText).not.toContain(secretMarker);
        expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload.controls).toEqual([
          {
            tool: 'notify_user',
            args: { channel: 'auto', level: 'info', summary: 'allowlisted status' },
          },
        ]);
        expect(pushes).toEqual([{ level: 'info', summary: 'allowlisted status', detail: undefined, channel: 'auto' }]);
      });

      it('round-trips legitimate multi-chunk plan and goal controls through durable persistence and application', async () => {
        const runId = 'planner-large-control-tail-round-trip';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const planContent = `# Approved plan\n${'plan detail\n'.repeat(900)}`;
        const goalContent = JSON.stringify({ goal: 'g'.repeat(9_000), success: ['tests pass'] });
        expect(Buffer.byteLength(planContent, 'utf8')).toBeGreaterThan(8_192);
        expect(Buffer.byteLength(goalContent, 'utf8')).toBeGreaterThan(8_192);
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            JSON.stringify({ tool: 'write_plan', args: { content: planContent } }),
            '```',
            '```autoloop',
            JSON.stringify({ tool: 'write_goal', args: { content: goalContent } }),
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'persist both large artifacts' },
        });

        await expect(mgr.autoloopChat(runId, 'persist the approved artifacts')).resolves.toEqual({
          reply: 'Planner controls persisted: write_plan, write_goal',
        });

        expect(fs.readFileSync(path.join(workspace, 'plan.md'), 'utf8')).toBe(planContent);
        expect(fs.readFileSync(path.join(workspace, 'goal.json'), 'utf8')).toBe(goalContent);
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { controls?: PlannerToolCall[] } });
        expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload.controls).toEqual([
          {
            tool: 'write_plan',
            args: { commit_message: 'autoloop: planner writes plan.md', content: planContent },
          },
          {
            tool: 'write_goal',
            args: { commit_message: 'autoloop: planner writes goal.json', content: goalContent },
          },
        ]);
      });

      it('crash-flushes a new control file and its parent directory before applying its first effect', async () => {
        const runId = 'planner-control-flush-order';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        expect(fs.existsSync(decisionsPath)).toBe(false);
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'spawn after flush' },
        });
        const order: string[] = [];
        const openedTargets = new Map<number, string>();
        const openFile = vi.mocked(fs.openSync);
        const openImplementation = openFile.getMockImplementation()!;
        openFile.mockImplementation(((target: unknown, ...args: unknown[]) => {
          const fd = (openImplementation as (...values: unknown[]) => number)(target, ...args);
          openedTargets.set(fd, String(target));
          return fd;
        }) as typeof fs.openSync);
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          const target = openedTargets.get(fd);
          if (target === decisionsPath) order.push('control-flushed');
          if (target === ledgerDir) order.push('directory-flushed');
          return flushImplementation(fd);
        });
        const spawnImplementation = handle.dispatcher.spawnSubagents.bind(handle.dispatcher);
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');
        spawn.mockImplementation(async (args) => {
          order.push('effect-started');
          return await spawnImplementation(args);
        });

        try {
          await expect(mgr.autoloopChat(runId, 'spawn after durable evidence')).resolves.toEqual({
            reply: 'Planner controls persisted: spawn_subagents',
          });
          expect(order.slice(0, 3)).toEqual(['control-flushed', 'directory-flushed', 'effect-started']);
        } finally {
          openFile.mockImplementation(openImplementation);
          flush.mockImplementation(flushImplementation);
          spawn.mockRestore();
        }
      });

      it('crash-flushes the parent directory after a rejected turn already created the control file', async () => {
        const runId = 'planner-control-flush-after-rejected-audit';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
          event: { type: 'result', result: 'rejected control' },
        });

        await expect(mgr.autoloopChat(runId, 'reject this control')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
        });
        expect(fs.existsSync(decisionsPath)).toBe(true);

        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'spawn after prior audit' },
        });
        const order: string[] = [];
        const openedTargets = new Map<number, string>();
        const openFile = vi.mocked(fs.openSync);
        const openImplementation = openFile.getMockImplementation()!;
        openFile.mockImplementation(((target: unknown, ...args: unknown[]) => {
          const fd = (openImplementation as (...values: unknown[]) => number)(target, ...args);
          openedTargets.set(fd, String(target));
          return fd;
        }) as typeof fs.openSync);
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          const target = openedTargets.get(fd);
          if (target === decisionsPath) order.push('control-flushed');
          if (target === ledgerDir) order.push('directory-flushed');
          return flushImplementation(fd);
        });
        const spawnImplementation = handle.dispatcher.spawnSubagents.bind(handle.dispatcher);
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');
        spawn.mockImplementation(async (args) => {
          order.push('effect-started');
          return await spawnImplementation(args);
        });

        try {
          await expect(mgr.autoloopChat(runId, 'spawn after durable evidence')).resolves.toEqual({
            reply: 'Planner controls persisted: spawn_subagents',
          });
          expect(order.slice(0, 3)).toEqual(['control-flushed', 'directory-flushed', 'effect-started']);
        } finally {
          openFile.mockImplementation(openImplementation);
          flush.mockImplementation(flushImplementation);
          spawn.mockRestore();
        }
      });

      it.each([
        { barrier: 'file', target: 'decisions.jsonl' },
        { barrier: 'directory', target: '' },
      ] as const)(
        'finishes a matching committed control after one $barrier-sync interruption and applies it once',
        async ({ barrier, target }) => {
          const runId = `planner-control-${barrier}-sync-reconcile`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const ledgerDir = path.join(workspace, 'tasks', runId);
          const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
          const failedTarget = target ? path.join(ledgerDir, target) : ledgerDir;
          mockSessions[0].sendImplementation = async () => ({
            text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
            event: { type: 'result', result: 'commit exactly one control' },
          });
          const flush = vi.mocked(fs.fsyncSync);
          const flushImplementation = flush.getMockImplementation()!;
          let injectedFailures = 0;
          flush.mockImplementation((fd) => {
            if (persistenceFsState.openPaths.get(fd) === failedTarget && injectedFailures === 0) {
              injectedFailures++;
              throw new Error(`injected Planner ${barrier} sync interruption`);
            }
            return flushImplementation(fd);
          });
          const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');

          try {
            await expect(mgr.autoloopChat(runId, 'apply one committed control')).resolves.toEqual({
              reply: 'Planner controls persisted: spawn_subagents',
            });
            expect(injectedFailures).toBe(1);
            expect(spawn).toHaveBeenCalledTimes(1);
            expect(mockSessions).toHaveLength(3);
            const controls = fs
              .readFileSync(decisionsPath, 'utf8')
              .trim()
              .split('\n')
              .map((line) => JSON.parse(line) as { kind?: string })
              .filter((row) => row.kind === 'planner_turn_control');
            expect(controls).toHaveLength(1);
            expect(handle.runner.state).toMatchObject({ status: 'running', subagents_spawned: true });
          } finally {
            flush.mockImplementation(flushImplementation);
            spawn.mockRestore();
          }
        },
      );

      it('preserves a committed directory-incomplete control outcome without applying its effect', async () => {
        const runId = 'planner-control-directory-flush-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const openedTargets = new Map<number, string>();
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'directory flush must succeed' },
        });
        const openFile = vi.mocked(fs.openSync);
        const openImplementation = openFile.getMockImplementation()!;
        openFile.mockImplementation(((target: unknown, ...args: unknown[]) => {
          const fd = (openImplementation as (...values: unknown[]) => number)(target, ...args);
          openedTargets.set(fd, String(target));
          return fd;
        }) as typeof fs.openSync);
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          if (openedTargets.get(fd) === ledgerDir) throw new Error('directory entry flush failed');
          return flushImplementation(fd);
        });
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');
        const phaseErrors: PhaseErrorPayload[] = [];
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));

        try {
          await expect(mgr.autoloopChat(runId, 'require directory durability')).rejects.toMatchObject({
            code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
            committed: true,
            retryable: false,
            effectsApplied: false,
            operation: 'secure_ledger_append',
          });
          expect(spawn).not.toHaveBeenCalled();
          expect(mockSessions).toHaveLength(1);
          expect(handle.runner.state).toMatchObject({
            status: 'planning',
            subagents_spawned: false,
            consecutive_phase_errors: 1,
            recent_phase_errors: [
              expect.objectContaining({
                code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
                committed: true,
                retryable: false,
              }),
            ],
          });
          expect(phaseErrors).toEqual([
            expect.objectContaining({
              code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
              committed: true,
              retryable: false,
            }),
          ]);
          const controls = fs
            .readFileSync(path.join(ledgerDir, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'planner_turn_control');
          expect(controls).toHaveLength(1);
          expect(
            fs
              .readFileSync(path.join(ledgerDir, 'decisions.jsonl'), 'utf8')
              .trim()
              .split('\n')
              .map((line) => JSON.parse(line) as { kind?: string })
              .filter((row) => row.kind === 'phase_error'),
          ).toHaveLength(0);
        } finally {
          openFile.mockImplementation(openImplementation);
          flush.mockImplementation(flushImplementation);
          spawn.mockRestore();
        }
      });

      it('preserves a committed directory-incomplete control outcome after a prior rejected audit', async () => {
        const runId = 'planner-existing-control-directory-flush-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
          event: { type: 'result', result: 'rejected control' },
        });

        await expect(mgr.autoloopChat(runId, 'reject this control')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
        });
        expect(fs.existsSync(decisionsPath)).toBe(true);

        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'directory flush must succeed' },
        });
        const openedTargets = new Map<number, string>();
        const openFile = vi.mocked(fs.openSync);
        const openImplementation = openFile.getMockImplementation()!;
        openFile.mockImplementation(((target: unknown, ...args: unknown[]) => {
          const fd = (openImplementation as (...values: unknown[]) => number)(target, ...args);
          openedTargets.set(fd, String(target));
          return fd;
        }) as typeof fs.openSync);
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          if (openedTargets.get(fd) === ledgerDir) throw new Error('existing directory entry flush failed');
          return flushImplementation(fd);
        });
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');

        try {
          await expect(mgr.autoloopChat(runId, 'require existing directory durability')).rejects.toMatchObject({
            code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
            committed: true,
          });
          expect(spawn).not.toHaveBeenCalled();
          expect(mockSessions).toHaveLength(1);
          expect(handle.runner.state).toMatchObject({
            status: 'planning',
            subagents_spawned: false,
            consecutive_phase_errors: 2,
          });
          const controls = fs
            .readFileSync(decisionsPath, 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'planner_turn_control');
          expect(controls).toHaveLength(1);
        } finally {
          openFile.mockImplementation(openImplementation);
          flush.mockImplementation(flushImplementation);
          spawn.mockRestore();
        }
      });

      it('flushes the control file without directory fsync and warns explicitly on win32', async () => {
        const runId = 'planner-control-win32-durability';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        const warn = vi.fn();
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (mgr as any).logger.warn = warn;
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'spawn on win32' },
        });
        const order: string[] = [];
        const openHistory: Array<{ fd: number; target: string; flags: unknown }> = [];
        const currentOpens = new Map<number, { target: string; flags: unknown }>();
        const controlFlushFlags: unknown[] = [];
        const flushedTargets: string[] = [];
        const openFile = vi.mocked(fs.openSync);
        const openImplementation = openFile.getMockImplementation()!;
        openFile.mockImplementation(((target: unknown, ...args: unknown[]) => {
          const fd = (openImplementation as (...values: unknown[]) => number)(target, ...args);
          const opened = { target: String(target), flags: args[0] };
          openHistory.push({ fd, ...opened });
          currentOpens.set(fd, opened);
          return fd;
        }) as typeof fs.openSync);
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          const currentOpen = currentOpens.get(fd);
          if (currentOpen) flushedTargets.push(currentOpen.target);
          if (currentOpen?.target === decisionsPath) {
            order.push('control-flushed');
            controlFlushFlags.push(currentOpen.flags);
          }
          return flushImplementation(fd);
        });
        const spawnImplementation = handle.dispatcher.spawnSubagents.bind(handle.dispatcher);
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');
        spawn.mockImplementation(async (args) => {
          order.push('effect-started');
          return await spawnImplementation(args);
        });
        const platform = vi.spyOn(process, 'platform', 'get').mockReturnValue('win32');

        try {
          await expect(mgr.autoloopChat(runId, 'spawn with Windows durability semantics')).resolves.toEqual({
            reply: 'Planner controls persisted: spawn_subagents',
          });
          expect(order).toEqual(['control-flushed', 'effect-started']);
          expect(openHistory.map(({ target }) => target)).toContain(ledgerDir);
          expect(flushedTargets).not.toContain(ledgerDir);
          expect(controlFlushFlags).toHaveLength(1);
          expect((controlFlushFlags[0] as number) & fs.constants.O_RDWR).toBe(fs.constants.O_RDWR);
          expect(warn).toHaveBeenCalledWith(
            '[autoloop] parent-directory fsync is unavailable on win32; control file contents were flushed without a POSIX directory-entry guarantee',
          );
        } finally {
          platform.mockRestore();
          openFile.mockImplementation(openImplementation);
          flush.mockImplementation(flushImplementation);
          spawn.mockRestore();
        }
      });

      it('preserves a committed file-incomplete control outcome without applying its effect', async () => {
        const runId = 'planner-control-flush-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'flush must succeed' },
        });
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        const phaseErrors: PhaseErrorPayload[] = [];
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));
        flush.mockImplementation(() => {
          throw new Error('stable-storage flush failed');
        });

        try {
          await expect(mgr.autoloopChat(runId, 'require a durable control row')).rejects.toMatchObject({
            code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
            committed: true,
            retryable: false,
            effectsApplied: false,
            operation: 'secure_ledger_append',
          });
          expect(mockSessions).toHaveLength(1);
          expect(handle.runner.state).toMatchObject({
            status: 'planning',
            subagents_spawned: false,
            consecutive_phase_errors: 1,
            recent_phase_errors: [
              expect.objectContaining({
                code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
                committed: true,
                retryable: false,
              }),
            ],
          });
          expect(phaseErrors).toEqual([
            expect.objectContaining({
              code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
              committed: true,
              retryable: false,
            }),
          ]);
          const controls = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'planner_turn_control');
          expect(controls).toHaveLength(1);
          expect(
            fs
              .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
              .trim()
              .split('\n')
              .map((line) => JSON.parse(line) as { kind?: string })
              .filter((row) => row.kind === 'phase_error'),
          ).toHaveLength(0);
        } finally {
          flush.mockImplementation(flushImplementation);
        }
      });

      it('keeps a recovered matching control authoritative after later valid decision rows', async () => {
        const runId = 'planner-control-committed-recovery-reentry';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'same committed logical control' },
        });
        const envelope = AutoloopMsg.chat(0, { text: 'recover this exact logical control' });
        const dispatchState = handle.dispatcher as unknown as {
          logicalDispatches: Map<string, unknown>;
          settledDispatches: Set<string>;
        };
        const forgetProcessLocalDispatch = () => {
          dispatchState.logicalDispatches.clear();
          dispatchState.settledDispatches.clear();
        };
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          if (persistenceFsState.openPaths.get(fd) === ledgerDir) {
            throw new Error('persistent control directory sync interruption');
          }
          return flushImplementation(fd);
        });
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');

        try {
          await expect(handle.dispatcher.deliver(envelope)).rejects.toMatchObject({
            code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
            committed: true,
          });
          flush.mockImplementation(flushImplementation);
          fs.appendFileSync(
            decisionsPath,
            `${JSON.stringify({
              ts: '2026-09-06T00:00:00.000Z',
              kind: 'phase_error',
              actor: 'dispatcher',
              payload: {
                agent: 'planner',
                phase: 'planner_turn',
                code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
                error: 'later valid audit row',
              },
            })}\n`,
          );

          forgetProcessLocalDispatch();
          await expect(handle.dispatcher.deliver(envelope)).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
            retryable: false,
          });
          forgetProcessLocalDispatch();
          await expect(handle.dispatcher.deliver(envelope)).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
            retryable: false,
          });

          expect(spawn).not.toHaveBeenCalled();
          expect(mockSessions).toHaveLength(1);
          const controls = fs
            .readFileSync(decisionsPath, 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'planner_turn_control');
          expect(controls).toHaveLength(1);
        } finally {
          flush.mockImplementation(flushImplementation);
          spawn.mockRestore();
        }
      });

      it('rejects a conflicting control for the same committed logical dispatch after later rows', async () => {
        const runId = 'planner-control-committed-conflict-after-later-row';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const ledgerDir = path.join(workspace, 'tasks', runId);
        const decisionsPath = path.join(ledgerDir, 'decisions.jsonl');
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'first logical claim' },
        });
        const envelope = AutoloopMsg.chat(0, { text: 'one immutable logical dispatch' });
        const dispatchState = handle.dispatcher as unknown as {
          logicalDispatches: Map<string, unknown>;
          settledDispatches: Set<string>;
        };
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          if (persistenceFsState.openPaths.get(fd) === ledgerDir) {
            throw new Error('persistent control directory sync interruption');
          }
          return flushImplementation(fd);
        });
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');
        const pushes: Array<{ summary: string }> = [];
        handle.runner.on('push', (payload: { summary: string }) => pushes.push(payload));

        try {
          await expect(handle.dispatcher.deliver(envelope)).rejects.toMatchObject({
            code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
            committed: true,
          });
          flush.mockImplementation(flushImplementation);
          fs.appendFileSync(
            decisionsPath,
            `${JSON.stringify({
              ts: '2026-09-06T00:00:01.000Z',
              kind: 'phase_error',
              actor: 'dispatcher',
              payload: { agent: 'planner', phase: 'planner_turn', error: 'later valid audit row' },
            })}\n`,
          );
          mockSessions[0].sendImplementation = async () => ({
            text: [
              '```autoloop',
              '{"tool":"notify_user","args":{"level":"info","summary":"must not run","channel":"auto"}}',
              '```',
            ].join('\n'),
            event: { type: 'result', result: 'conflicting logical claim' },
          });
          dispatchState.logicalDispatches.clear();
          dispatchState.settledDispatches.clear();

          await expect(handle.dispatcher.deliver(envelope)).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
            retryable: false,
          });

          expect(spawn).not.toHaveBeenCalled();
          expect(pushes).toEqual([]);
          expect(mockSessions).toHaveLength(1);
          const controls = fs
            .readFileSync(decisionsPath, 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'planner_turn_control');
          expect(controls).toHaveLength(1);
        } finally {
          flush.mockImplementation(flushImplementation);
          spawn.mockRestore();
        }
      });

      it.each([
        {
          label: 'malformed',
          arrange: (decisionsPath: string) => fs.writeFileSync(decisionsPath, 'not-json\n'),
        },
        {
          label: 'oversized',
          arrange: (decisionsPath: string) => {
            const maximumLedgerBytes = 64 * 1024 * 1024;
            fs.writeFileSync(decisionsPath, '{"kind":"phase_error"}\n');
            fs.truncateSync(decisionsPath, maximumLedgerBytes + 1);
            const fd = fs.openSync(decisionsPath, 'r+');
            try {
              fs.writeSync(fd, Buffer.from('\n'), 0, 1, maximumLedgerBytes);
            } finally {
              fs.closeSync(fd);
            }
          },
        },
      ])(
        'fails closed before applying a Planner control when the decision ledger is $label',
        async ({ label, arrange }) => {
          const runId = `planner-control-ledger-${label}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
          arrange(decisionsPath);
          const sizeBefore = fs.statSync(decisionsPath).size;
          mockSessions[0].sendImplementation = async () => ({
            text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
            event: { type: 'result', result: 'must fail closed' },
          });
          const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents');

          try {
            await expect(mgr.autoloopChat(runId, 'do not trust an invalid ledger')).rejects.toMatchObject({
              code: 'AUTOLOOP_CONTROL_NOT_PERSISTED',
              retryable: true,
            });
            expect(spawn).not.toHaveBeenCalled();
            expect(mockSessions).toHaveLength(1);
            expect(fs.statSync(decisionsPath).size).toBe(sizeBefore);
          } finally {
            spawn.mockRestore();
          }
        },
      );

      it('records only accepted Planner turns in replay history for non-native engines', async () => {
        const runId = 'planner-replay-accepted-only';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace, plannerEngine: 'gemini' });
        const planner = mockSessions[0];
        const rejectedMarker = 'R5_REJECTED_HISTORY_MARKER';
        const acceptedUserMarker = 'R5_ACCEPTED_USER_MARKER';
        const acceptedAgentMarker = 'R5_ACCEPTED_AGENT_MARKER';
        let turn = 0;
        planner.sendImplementation = async () => {
          turn += 1;
          if (turn === 1) {
            return {
              text: [rejectedMarker, '```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
              event: { type: 'result', result: 'rejected control' },
            };
          }
          return {
            text: turn === 2 ? acceptedAgentMarker : 'third accepted reply',
            event: { type: 'result', result: 'accepted turn' },
          };
        };

        await expect(mgr.autoloopChat(runId, 'R5_REJECTED_USER_MARKER')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
        });
        await expect(mgr.autoloopChat(runId, acceptedUserMarker)).resolves.toEqual({ reply: acceptedAgentMarker });
        expect(String(planner.sendCalls[1].message)).not.toContain(rejectedMarker);

        await expect(mgr.autoloopChat(runId, 'third user turn')).resolves.toEqual({ reply: 'third accepted reply' });
        const thirdPrompt = String(planner.sendCalls[2].message);
        expect(thirdPrompt).not.toContain(rejectedMarker);
        expect(thirdPrompt.match(new RegExp(acceptedUserMarker, 'g'))).toHaveLength(1);
        expect(thirdPrompt.match(new RegExp(acceptedAgentMarker, 'g'))).toHaveLength(1);
      });

      it('marks a durably verified successful spawn at its committed effect boundary', async () => {
        const runId = 'planner-spawn-committed';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'spawn agents' },
        });

        const order: string[] = [];
        const spawnImplementation = handle.dispatcher.spawnSubagents.bind(handle.dispatcher);
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents').mockImplementation(async (args) => {
          order.push('spawn-start');
          await spawnImplementation(args);
          order.push('spawn-finish');
        });
        const markImplementation = handle.runner.markSubagentsSpawned.bind(handle.runner);
        const mark = vi.spyOn(handle.runner, 'markSubagentsSpawned').mockImplementation(() => {
          order.push('mark-committed');
          markImplementation();
        });

        try {
          await expect(mgr.autoloopChat(runId, 'start the approved implementation')).resolves.toEqual({
            reply: 'Planner controls persisted: spawn_subagents',
          });

          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string });
          expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toHaveLength(1);
          expect(decisions.filter((row) => row.kind === 'spawn_subagents')).toHaveLength(1);
          expect(order).toEqual(['spawn-start', 'spawn-finish', 'mark-committed']);
          expect(mockSessions).toHaveLength(3);
          expect(handle.runner.state).toMatchObject({
            status: 'running',
            iter: 0,
            subagents_spawned: true,
          });
        } finally {
          spawn.mockRestore();
          mark.mockRestore();
        }
      });

      it('keeps planning truth and does not mark a failed spawn as committed', async () => {
        const runId = 'planner-spawn-fails-before-commit';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'spawn agents' },
        });
        const order: string[] = [];
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents').mockImplementation(async () => {
          order.push('spawn-start');
          throw new Error('spawn failed before completion');
        });
        const markImplementation = handle.runner.markSubagentsSpawned.bind(handle.runner);
        const mark = vi.spyOn(handle.runner, 'markSubagentsSpawned').mockImplementation(() => {
          order.push('mark-committed');
          markImplementation();
        });

        try {
          await expect(mgr.autoloopChat(runId, 'start the approved implementation')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
            retryable: false,
          });
          expect(order).toEqual(['spawn-start']);
          expect(mark).not.toHaveBeenCalled();
          expect(mockSessions).toHaveLength(1);
          expect(handle.runner.state).toMatchObject({
            status: 'planning',
            iter: 0,
            subagents_spawned: false,
          });
        } finally {
          spawn.mockRestore();
          mark.mockRestore();
        }
      });

      it('keeps a committed spawn marked when a later operational control fails', async () => {
        const runId = 'planner-spawn-before-operational-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            '{"tool":"spawn_subagents","args":{}}',
            '```',
            '```autoloop',
            '{"tool":"write_plan","args":{"content":"# Approved plan"}}',
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'apply controls' },
        });
        const planPath = path.join(workspace, 'plan.md');
        const rename = vi.mocked(fs.renameSync);
        const renameImplementation = rename.getMockImplementation()!;
        rename.mockImplementation(((from: unknown, to: unknown) => {
          if (String(to) === planPath) throw new Error('operational plan write failed');
          return (renameImplementation as (...values: unknown[]) => unknown)(from, to);
        }) as typeof fs.renameSync);

        try {
          await expect(mgr.autoloopChat(runId, 'apply both approved controls')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
            retryable: false,
          });

          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: Record<string, unknown> });
          expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload.controls).toEqual([
            { tool: 'spawn_subagents', args: {} },
            {
              tool: 'write_plan',
              args: {
                content: '# Approved plan',
                commit_message: 'autoloop: planner writes plan.md',
              },
            },
          ]);
          expect(decisions.filter((row) => row.kind === 'spawn_subagents')).toHaveLength(1);
          expect(mockSessions).toHaveLength(3);
          expect(handle.runner.state).toMatchObject({
            status: 'running',
            iter: 0,
            subagents_spawned: true,
            consecutive_phase_errors: 1,
          });
        } finally {
          rename.mockImplementation(renameImplementation);
        }
      });

      const runnerMediatedPlannerFailures: Array<{
        label: string;
        code: NonNullable<PhaseErrorPayload['code']>;
        retryable: boolean;
        configure: (context: { runId: string; workspace: string }) => void | (() => void);
      }> = [
        {
          label: 'missing live generation',
          code: 'AUTOLOOP_SESSION_NOT_CREATED',
          retryable: true,
          configure: () => {
            vi.spyOn(mgr, 'inspect').mockResolvedValue('absent');
          },
        },
        {
          label: 'engine result failure',
          code: 'AUTOLOOP_ENGINE_FAILURE',
          retryable: true,
          configure: () => {
            mockSessions[0].sendImplementation = async () => ({
              text: 'engine authentication failed',
              event: { type: 'result', result: 'engine authentication failed', is_error: true },
            });
          },
        },
        {
          label: 'required tool denial',
          code: 'AUTOLOOP_REQUIRED_TOOL_DENIED',
          retryable: true,
          configure: () => {
            mockSessions[0].turnsSucceededOverride = 0;
          },
        },
        {
          label: 'malformed control',
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
          configure: () => {
            mockSessions[0].sendImplementation = async () => ({
              text: ['```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
              event: { type: 'result', result: 'malformed control' },
            });
          },
        },
        {
          label: 'control application failure',
          code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
          retryable: false,
          configure: ({ workspace }) => {
            mockSessions[0].sendImplementation = async () => ({
              text: ['```autoloop', '{"tool":"write_plan","args":{"content":"# Validated plan"}}', '```'].join('\n'),
              event: { type: 'result', result: 'validated control with operational failure' },
            });
            const planPath = path.join(workspace, 'plan.md');
            const rename = vi.mocked(fs.renameSync);
            const renameImplementation = rename.getMockImplementation()!;
            rename.mockImplementation(((from: unknown, to: unknown) => {
              if (String(to) === planPath) throw new Error('operational plan write failed');
              return (renameImplementation as (...values: unknown[]) => unknown)(from, to);
            }) as typeof fs.renameSync);
            return () => rename.mockImplementation(renameImplementation);
          },
        },
        {
          label: 'control persistence failure',
          code: 'AUTOLOOP_CONTROL_NOT_PERSISTED',
          retryable: true,
          configure: ({ runId, workspace }) => {
            mockSessions[0].sendImplementation = async () => ({
              text: ['```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
              event: { type: 'result', result: 'control persistence must commit' },
            });
            const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
            const appendFile = vi.mocked(fs.appendFileSync);
            const appendImplementation = appendFile.getMockImplementation()!;
            appendFile.mockImplementation(((file: unknown, data: unknown, ...args: unknown[]) => {
              if (isOpenPath(file, decisionsPath) && String(data).includes('"kind":"planner_turn_control"')) return;
              return (appendImplementation as (...values: unknown[]) => unknown)(file, data, ...args);
            }) as typeof fs.appendFileSync);
            return () => appendFile.mockImplementation(appendImplementation);
          },
        },
      ];

      it.each(runnerMediatedPlannerFailures)(
        'routes $label through exactly one typed Runner phase-error path',
        async ({ label, code, retryable, configure }) => {
          const runId = `planner-runner-phase-error-${label.replaceAll(' ', '-')}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const cleanup = configure({ runId, workspace });
          const phaseErrors: PhaseErrorPayload[] = [];
          handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));

          try {
            await expect(mgr.autoloopChat(runId, 'exercise the typed Planner failure')).rejects.toMatchObject({
              code,
              retryable,
            });

            expect(phaseErrors).toHaveLength(1);
            expect(phaseErrors[0]).toMatchObject({ agent: 'planner', phase: 'planner_turn', code });
            expect(handle.runner.state).toMatchObject({
              consecutive_phase_errors: 1,
              recent_phase_errors: [expect.objectContaining({ agent: 'planner', phase: 'planner_turn', code })],
            });
            expect(handle.runner.state.recent_phase_errors[0]).not.toHaveProperty('committed');
            expect(handle.runner.state.recent_phase_errors[0]).not.toHaveProperty('retryable');
            const decisions = fs
              .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
              .trim()
              .split('\n')
              .map((line) => JSON.parse(line) as { kind: string; payload: { code?: string } });
            expect(decisions.filter((row) => row.kind === 'phase_error' && row.payload.code === code)).toHaveLength(1);
          } finally {
            cleanup?.();
          }
        },
      );

      it('preserves the original typed Planner failure when its synthetic phase-error drain also fails', async () => {
        const runId = 'planner-original-error-survives-secondary-drain-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
          event: { type: 'result', result: 'invalid control' },
        });
        handle.runner.config.notifyUser = async () => {
          throw new Error('secondary notification transport failed');
        };
        const phaseErrors: PhaseErrorPayload[] = [];
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));

        let caught: unknown;
        try {
          await mgr.autoloopChat(runId, 'exercise post-catch failure');
        } catch (error) {
          caught = error;
        }

        expect(caught).toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
          secondaryErrors: [expect.objectContaining({ message: 'secondary notification transport failed' })],
        });
        expect(phaseErrors).toHaveLength(1);
        expect(handle.runner.state).toMatchObject({
          consecutive_phase_errors: 1,
          recent_phase_errors: [expect.objectContaining({ code: 'AUTOLOOP_CONTROL_MALFORMED' })],
        });
      });

      it('keeps an empty Planner reply primary without recording agent progress when notification fails', async () => {
        const runId = 'planner-empty-reply-survives-secondary-notification-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: '   ',
          event: { type: 'result', result: '   ' },
        });
        handle.runner.config.notifyUser = async () => {
          throw new Error('secondary empty-reply notification failed');
        };
        const activity = vi.spyOn(handle.runner, 'recordActivity');
        const phaseErrors: PhaseErrorPayload[] = [];
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));

        let caught: unknown;
        try {
          await mgr.autoloopChat(runId, 'return a reply');
        } catch (error) {
          caught = error;
        }

        expect(caught).toMatchObject({
          name: 'AutoloopOperationError',
          code: 'AUTOLOOP_EMPTY_REPLY',
          retryable: true,
          secondaryErrors: [expect.objectContaining({ message: 'secondary empty-reply notification failed' })],
        });
        expect(activity.mock.calls.map(([kind]) => kind)).toEqual(['queue_message_accepted']);
        expect(phaseErrors).toEqual([
          expect.objectContaining({ agent: 'planner', phase: 'planner_turn', code: 'AUTOLOOP_EMPTY_REPLY' }),
        ]);
        expect(handle.runner.state).toMatchObject({
          status: 'planning',
          consecutive_phase_errors: 1,
          recent_phase_errors: [expect.objectContaining({ code: 'AUTOLOOP_EMPTY_REPLY' })],
        });
      });

      it('preserves the original typed Planner failure when max dispatch depth stops its synthetic phase-error', async () => {
        const runId = 'planner-original-error-survives-max-dispatch-depth';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        handle.runner.config.maxDispatchDepth = 0;
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"spawn_subagents"}', '```'].join('\n'),
          event: { type: 'result', result: 'invalid control' },
        });
        const phaseErrors: PhaseErrorPayload[] = [];
        const pushes: Array<{ summary: string }> = [];
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));
        handle.runner.on('push', (payload: { summary: string }) => pushes.push(payload));

        let caught: unknown;
        try {
          await mgr.autoloopChat(runId, 'exercise max-depth after typed Planner failure');
        } catch (error) {
          caught = error;
        }

        expect(caught).toMatchObject({
          name: 'AutoloopOperationError',
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
          secondaryErrors: [
            expect.objectContaining({
              name: 'AutoloopRoutingError',
              message: expect.stringContaining("dispatch depth exceeded 0 at iter 0 (next='phase_error' to 'runner')"),
            }),
          ],
        });
        expect(phaseErrors).toEqual([]);
        expect(handle.runner.state.consecutive_phase_errors).toBe(0);
        const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
        const phaseErrorRowsBeforeRetry = fs
          .readFileSync(decisionsPath, 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string })
          .filter((row) => row.kind === 'phase_error').length;

        mockSessions[0].sendImplementation = async () => ({
          text: 'fresh Planner reply',
          event: { type: 'result', result: 'fresh Planner reply' },
        });
        await expect(mgr.autoloopChat(runId, 'retry after the rejected control')).resolves.toEqual({
          reply: 'fresh Planner reply',
        });

        const decisions = fs
          .readFileSync(decisionsPath, 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string });
        expect(phaseErrors).toEqual([]);
        expect(pushes).toEqual([]);
        expect(decisions.filter((row) => row.kind === 'phase_error')).toHaveLength(phaseErrorRowsBeforeRetry);
        expect(handle.runner.state).toMatchObject({
          consecutive_phase_errors: 0,
          recent_phase_errors: [],
          push_log_count: 0,
        });
      });

      it('keeps max dispatch depth as the primary error without a pending Planner failure', async () => {
        const runId = 'planner-max-dispatch-depth-without-pending-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        handle.runner.config.maxDispatchDepth = 0;
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            '{"tool":"notify_user","args":{"level":"info","summary":"queued push","channel":"auto"}}',
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'valid queued control' },
        });

        await expect(mgr.autoloopChat(runId, 'exercise ordinary max-depth guard')).rejects.toMatchObject({
          name: 'AutoloopRoutingError',
          message: expect.stringContaining("dispatch depth exceeded 0 at iter 0 (next='push_user' to 'user')"),
        });
      });

      it('rejects an empty Planner success even when phase-error event plumbing drops the code', async () => {
        const runId = 'planner-empty-reply-lost-event';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: '',
          event: { type: 'result', result: '' },
        });
        const originalEmit = handle.runner.emit;
        vi.spyOn(handle.runner, 'emit').mockImplementation(function (eventName, ...args) {
          if (eventName === 'phase_error') return false;
          return originalEmit.call(handle.runner, eventName, ...args);
        });

        await expect(mgr.autoloopChat(runId, 'return a reply')).rejects.toMatchObject({
          code: 'AUTOLOOP_EMPTY_REPLY',
          retryable: true,
        });
        expect(handle.runner.state.consecutive_phase_errors).toBe(1);
        expect(handle.runner.state.recent_phase_errors).toEqual([
          expect.objectContaining({ code: 'AUTOLOOP_EMPTY_REPLY' }),
        ]);
      });

      it('rejects a claimed control with no matching persisted event without advancing phase', async () => {
        const runId = 'planner-control-not-persisted';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const planner = mockSessions[0];
        planner.sendImplementation = async () => ({
          text: ['starting agents', '```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'starting agents' },
        });
        const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
        const appendFile = vi.mocked(fs.appendFileSync);
        const appendImplementation = appendFile.getMockImplementation()!;
        appendFile.mockImplementation(((file: unknown, ...args: unknown[]) => {
          if (isOpenPath(file, decisionsPath)) return;
          return (appendImplementation as (...values: unknown[]) => unknown)(file, ...args);
        }) as typeof fs.appendFileSync);

        try {
          await expect(mgr.autoloopChat(runId, 'start the approved implementation')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_NOT_PERSISTED',
          });
          expect(handle.runner.state).toMatchObject({
            status: 'planning',
            iter: 0,
            subagents_spawned: false,
          });
          expect(mockSessions).toHaveLength(1);
        } finally {
          appendFile.mockImplementation(appendImplementation);
        }
      });

      it('rejects a same-id durable control whose persisted generation does not match the Planner turn', async () => {
        const runId = 'planner-control-wrong-payload';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const planner = mockSessions[0];
        planner.sendImplementation = async () => ({
          text: ['starting agents', '```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          event: { type: 'result', result: 'starting agents' },
        });
        const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
        const appendFile = vi.mocked(fs.appendFileSync);
        const appendImplementation = appendFile.getMockImplementation()!;
        appendFile.mockImplementation(((file: unknown, data: unknown, ...args: unknown[]) => {
          if (isOpenPath(file, decisionsPath) && String(data).includes('"kind":"planner_turn_control"')) {
            const row = JSON.parse(String(data)) as { payload: { generation: number } };
            row.payload.generation = 999;
            return (appendImplementation as (...values: unknown[]) => unknown)(
              file,
              `${JSON.stringify(row)}\n`,
              ...args,
            );
          }
          return (appendImplementation as (...values: unknown[]) => unknown)(file, data, ...args);
        }) as typeof fs.appendFileSync);

        try {
          await expect(mgr.autoloopChat(runId, 'start the approved implementation')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_NOT_PERSISTED',
          });
          expect(mockSessions).toHaveLength(1);
        } finally {
          appendFile.mockImplementation(appendImplementation);
        }
      });

      const durableControlCorruptions: Array<{
        label: string;
        mutate: (payload: Record<string, unknown>) => void;
      }> = [
        {
          label: 'complete controls',
          mutate: (payload) => {
            payload.controls = [{ tool: 'spawn_subagents', args: { coder_model: 'tampered-model' } }];
          },
        },
        {
          label: 'controls digest',
          mutate: (payload) => {
            payload.controls_sha256 = '0'.repeat(64);
          },
        },
        {
          label: 'owner identity',
          mutate: (payload) => {
            payload.owner_instance_id = 'session-manager:999999:00000000-0000-4000-8000-000000000000';
          },
        },
        {
          label: 'session identity',
          mutate: (payload) => {
            payload.session_id = '00000000-0000-4000-8000-000000000000';
          },
        },
        {
          label: 'dispatch identity',
          mutate: (payload) => {
            payload.dispatch_id = 'tampered-dispatch';
          },
        },
        {
          label: 'message identity',
          mutate: (payload) => {
            payload.message_id = 'tampered-message';
          },
        },
        {
          label: 'iteration identity',
          mutate: (payload) => {
            payload.iter = 99;
          },
        },
        {
          label: 'tools sequence only',
          mutate: (payload) => {
            payload.tools = ['write_plan'];
          },
        },
      ];

      it.each(durableControlCorruptions)(
        'rejects durable Planner control corruption in $label before effects',
        async ({ label, mutate }) => {
          const runId = `planner-control-corrupt-${label.replaceAll(' ', '-')}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          mockSessions[0].sendImplementation = async () => ({
            text: ['starting agents', '```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
            event: { type: 'result', result: 'starting agents' },
          });
          const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
          const appendFile = vi.mocked(fs.appendFileSync);
          const appendImplementation = appendFile.getMockImplementation()!;
          appendFile.mockImplementation(((file: unknown, data: unknown, ...args: unknown[]) => {
            if (isOpenPath(file, decisionsPath) && String(data).includes('"kind":"planner_turn_control"')) {
              const row = JSON.parse(String(data)) as { payload: Record<string, unknown> };
              mutate(row.payload);
              return (appendImplementation as (...values: unknown[]) => unknown)(
                file,
                `${JSON.stringify(row)}\n`,
                ...args,
              );
            }
            return (appendImplementation as (...values: unknown[]) => unknown)(file, data, ...args);
          }) as typeof fs.appendFileSync);

          try {
            await expect(mgr.autoloopChat(runId, 'start the approved implementation')).rejects.toMatchObject({
              code: 'AUTOLOOP_CONTROL_NOT_PERSISTED',
              retryable: true,
            });
            expect(mockSessions).toHaveLength(1);
            expect(handle.runner.state).toMatchObject({
              status: 'planning',
              iter: 0,
              subagents_spawned: false,
            });
          } finally {
            appendFile.mockImplementation(appendImplementation);
          }
        },
      );

      it('returns a structured reset failure when the exact reservation remains occupied', async () => {
        const runId = 'reset-reservation-occupied';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const plannerName = handle.dispatcher.sessionNames.planner;
        const reservations = (
          mgr as unknown as {
            persistedSessions: Map<string, Record<string, unknown>>;
          }
        ).persistedSessions;
        vi.spyOn(mgr, 'releaseReservation').mockImplementation(
          async (_name, _generation, options: AgentReservationReleaseOptions) => {
            if (!options.rollbackUncommittedReservation) {
              options.beforeRelease?.();
              options.persistReleaseEvidence?.();
            }
            return true;
          },
        );

        const result = await handle.dispatcher.resetAgent('planner', { force: true });

        expect(result).toMatchObject({
          ok: false,
          code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
          agent: 'planner',
          previous_generation: 1,
        });
        expect(reservations.get(plannerName)).toMatchObject({ agentGeneration: 1 });
      });

      it('does not map a failed reset postcondition to legacy boolean success', async () => {
        const runId = 'reset-legacy-false';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        vi.spyOn(mgr, 'releaseReservation').mockImplementation(
          async (_name, _generation, options: AgentReservationReleaseOptions) => {
            if (!options.rollbackUncommittedReservation) {
              options.beforeRelease?.();
              options.persistReleaseEvidence?.();
            }
            return true;
          },
        );

        await expect(mgr.autoloopResetAgent(runId, 'planner', { force: true })).resolves.toBe(false);
      });

      it.each(['coder', 'reviewer'] as const)(
        'carries a typed reset postcondition failure through the %s fatal send path without retrying',
        async (role) => {
          const runId = `reset-code-${role}-fatal-send`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          await handle.dispatcher.spawnSubagents();
          const roleIndex = role === 'coder' ? 1 : 2;
          mockSessions[roleIndex].sendImplementation = async () => {
            throw new Error(`${role} subprocess failed`);
          };
          const reset = vi.spyOn(handle.dispatcher, 'resetAgent').mockResolvedValue({
            ok: false,
            code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
            agent: role,
            previous_generation: 1,
            message: `${role} reset could not prove reuse`,
            retryable: false,
          });
          const phaseErrors: PhaseErrorPayload[] = [];
          const phaseEnvelopes: PhaseErrorPayload[] = [];
          handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));
          handle.runner.on('message', (env: { type?: string; payload?: PhaseErrorPayload }) => {
            if (env.type === 'phase_error' && env.payload) phaseEnvelopes.push(env.payload);
          });

          if (role === 'coder') {
            await handle.runner.send(
              AutoloopMsg.directive(0, {
                goal: 'exercise coder fatal recovery',
                constraints: [],
                success_criteria: [],
                max_attempts: 1,
              }),
            );
          } else {
            seedCompleteLegacyReviewArtifacts(workspace, runId, 0);
            await handle.runner.send(
              AutoloopMsg.reviewRequest(0, {
                iter: 0,
                ledger_path: path.join(workspace, 'tasks', runId),
                prior_metrics: [],
              }),
            );
          }

          expect(mockSessions[roleIndex].sendCalls).toHaveLength(1);
          expect(reset).toHaveBeenCalledTimes(1);
          expect(phaseErrors).toEqual([
            {
              agent: role,
              phase: 'send',
              code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
              error: `${role} reset could not prove reuse`,
            },
          ]);
          expect(phaseEnvelopes).toEqual(phaseErrors);
          expect(handle.runner.state.recent_phase_errors).toEqual([
            expect.objectContaining({ agent: role, phase: 'send', code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' }),
          ]);
          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: { agent?: string; code?: string } });
          expect(
            decisions.filter(
              (row) =>
                row.kind === 'phase_error' &&
                row.payload.agent === role &&
                row.payload.code === 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
            ),
          ).toHaveLength(1);
        },
      );

      it.each(['coder', 'reviewer'] as const)(
        'classifies an exhausted %s reset-and-retry send as AUTOLOOP_ENGINE_FAILURE',
        async (role) => {
          const runId = `engine-code-${role}-exhausted-send`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          await handle.dispatcher.spawnSubagents();
          const roleIndex = role === 'coder' ? 1 : 2;
          mockSessions[roleIndex].sendImplementation = async () => {
            throw new Error(`${role} engine unavailable`);
          };
          const reset = vi.spyOn(handle.dispatcher, 'resetAgent').mockResolvedValue({
            ok: true,
            agent: role,
            previous_generation: 1,
            active_generation: 2,
            reusable: true,
          });
          const phaseErrors: PhaseErrorPayload[] = [];
          handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));

          const dispatch = (() => {
            if (role === 'coder') {
              return handle.runner.send(
                AutoloopMsg.directive(0, {
                  goal: 'exercise exhausted coder recovery',
                  constraints: [],
                  success_criteria: [],
                  max_attempts: 1,
                }),
              );
            }
            seedCompleteLegacyReviewArtifacts(workspace, runId, 0);
            return handle.runner.send(
              AutoloopMsg.reviewRequest(0, {
                iter: 0,
                ledger_path: path.join(workspace, 'tasks', runId),
                prior_metrics: [],
              }),
            );
          })();
          await vi.advanceTimersByTimeAsync(1_000);
          await dispatch;

          expect(reset).toHaveBeenCalledTimes(1);
          expect(mockSessions[roleIndex].sendCalls).toHaveLength(2);
          expect(phaseErrors).toEqual([
            {
              agent: role,
              phase: 'send',
              code: 'AUTOLOOP_ENGINE_FAILURE',
              error: `${role} engine unavailable`,
            },
          ]);
          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: PhaseErrorPayload });
          expect(
            decisions.filter(
              (row) =>
                row.kind === 'phase_error' &&
                row.payload.agent === role &&
                row.payload.code === 'AUTOLOOP_ENGINE_FAILURE',
            ),
          ).toHaveLength(1);
        },
      );

      it('preserves Reviewer started state and its frozen prompt when exact-generation release fails', async () => {
        const runId = 'reset-release-preserves-reviewer';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        await handle.dispatcher.spawnSubagents();
        const roleState = handle.dispatcher as unknown as {
          reviewerStarted: boolean;
          reviewerSessionPrompt: string | null;
        };
        const priorPrompt = roleState.reviewerSessionPrompt;
        vi.spyOn(mgr, 'releaseReservation').mockResolvedValue(false);

        const result = await handle.dispatcher.resetAgent('reviewer');

        expect(result).toMatchObject({ ok: false, code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' });
        expect(roleState.reviewerStarted).toBe(true);
        expect(roleState.reviewerSessionPrompt).toBe(priorPrompt);
      });

      it('finishes a real pending registry release exactly once before creating the next generation', async () => {
        const runId = 'reset-release-evidence-then-registry-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const plannerName = handle.dispatcher.sessionNames.planner;
        const roleState = handle.dispatcher as unknown as { plannerStarted: boolean };
        const generationLedgerPath = path.join(workspace, 'tasks', runId, 'agent-generations.jsonl');
        const readRegistryReservation = () =>
          (JSON.parse(persistenceFsState.files.get(SESSION_REGISTRY_FILE)!) as Array<Record<string, unknown>>).find(
            (reservation) => reservation.name === plannerName,
          );
        const readGenerationRows = () =>
          fs
            .readFileSync(generationLedgerPath, 'utf8')
            .trim()
            .split('\n')
            .map(
              (line) =>
                JSON.parse(line) as {
                  kind: string;
                  payload: PhysicalAgentGeneration;
                },
            );
        const persistedRename = vi.mocked(fs.renameSync).getMockImplementation()!;
        let completionFailureInjected = false;
        vi.mocked(fs.renameSync).mockImplementation(((from: unknown, to: unknown) => {
          if (!completionFailureInjected && String(to) === SESSION_REGISTRY_FILE) {
            const pendingSnapshot = persistenceFsState.files.get(String(from));
            const plannerReservation = pendingSnapshot
              ? (JSON.parse(pendingSnapshot) as Array<Record<string, unknown>>).find(
                  (reservation) => reservation.name === plannerName,
                )
              : undefined;
            if (
              plannerReservation?.agentReleasedGeneration === 1 &&
              plannerReservation.agentGeneration === undefined &&
              plannerReservation.agentReleasePending !== true
            ) {
              completionFailureInjected = true;
              throw new Error('registry finalization failed after release evidence');
            }
          }
          return (persistedRename as unknown as (source: unknown, destination: unknown) => void)(from, to);
        }) as typeof fs.renameSync);

        try {
          await expect(handle.dispatcher.resetAgent('planner', { force: true })).resolves.toMatchObject({
            ok: false,
            code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
            previous_generation: 1,
          });

          expect(completionFailureInjected).toBe(true);
          expect(roleState.plannerStarted).toBe(false);
          expect(readRegistryReservation()).toMatchObject({
            agentGeneration: 1,
            agentReleasePending: true,
            agentReleaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          });
          expect(mgr.probeAgentNameReusable(plannerName, readGenerationRows().at(-1)!.payload)).toBe(false);
          expect(readGenerationRows()).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                kind: 'agent_generation_released',
                payload: expect.objectContaining({ generation: 1, state: 'released' }),
              }),
            ]),
          );
          expect(readGenerationRows().some(({ payload }) => payload.generation === 2)).toBe(false);

          const blockedGeneration: PhysicalAgentGeneration = {
            ...readGenerationRows().at(-1)!.payload,
            generation: 2,
            session_id: 'blocked-before-release-completion',
            state: 'stale',
          };
          expect(mgr.reserveAgentGeneration(blockedGeneration, workspace)).toBe(false);
          await expect(
            mgr.startSession({ name: plannerName, cwd: workspace }, blockedGeneration),
          ).rejects.toMatchObject({ code: 'AUTOLOOP_AGENT_GENERATION_CONFLICT' });
          expect(mockSessions).toHaveLength(1);

          await expect(handle.dispatcher.resetAgent('planner', { force: true })).resolves.toMatchObject({
            ok: true,
            previous_generation: 1,
            reusable: true,
          });
          const reusableReservation = readRegistryReservation();
          expect(reusableReservation).toMatchObject({ agentReleasedGeneration: 1 });
          expect(reusableReservation).not.toHaveProperty('agentGeneration');
          expect(reusableReservation).not.toHaveProperty('agentReleasePending');
          expect(
            readGenerationRows().filter(
              ({ kind, payload }) => kind === 'agent_generation_released' && payload.generation === 1,
            ),
          ).toHaveLength(1);
          expect(mockSessions).toHaveLength(1);

          await expect(mgr.autoloopChat(runId, 'continue after registry recovery')).resolves.toMatchObject({
            reply: expect.any(String),
          });

          expect(roleState.plannerStarted).toBe(true);
          expect(mockSessions).toHaveLength(2);
          expect(mgr.listSessions().filter(({ name }) => name === plannerName)).toHaveLength(1);
          const generationRows = readGenerationRows();
          expect(
            generationRows.filter(
              ({ kind, payload }) => kind === 'agent_generation_released' && payload.generation === 1,
            ),
          ).toHaveLength(1);
          expect(
            generationRows.filter(({ payload }) => payload.generation === 2 && payload.state === 'live'),
          ).toHaveLength(1);
          expect(generationRows.some(({ payload }) => payload.generation > 2)).toBe(false);
          expect(generationRows.at(-1)).toMatchObject({
            kind: 'agent_generation_started',
            payload: { generation: 2, state: 'live' },
          });
        } finally {
          vi.mocked(fs.renameSync).mockImplementation(persistedRename);
        }
      });

      it('finishes a real pending registry release on direct chat before starting exactly one successor', async () => {
        const runId = 'chat-reconciles-pending-planner-release';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const plannerName = handle.dispatcher.sessionNames.planner;
        const generationLedgerPath = path.join(workspace, 'tasks', runId, 'agent-generations.jsonl');
        const readRegistryReservation = () =>
          (JSON.parse(persistenceFsState.files.get(SESSION_REGISTRY_FILE)!) as Array<Record<string, unknown>>).find(
            (reservation) => reservation.name === plannerName,
          );
        const readGenerationRows = () =>
          fs
            .readFileSync(generationLedgerPath, 'utf8')
            .trim()
            .split('\n')
            .map(
              (line) =>
                JSON.parse(line) as {
                  kind: string;
                  payload: PhysicalAgentGeneration;
                },
            );
        const persistedRename = vi.mocked(fs.renameSync).getMockImplementation()!;
        let completionFailureInjected = false;
        let releaseCompletionCommitted = false;
        vi.mocked(fs.renameSync).mockImplementation(((from: unknown, to: unknown) => {
          if (String(to) === SESSION_REGISTRY_FILE) {
            const pendingSnapshot = persistenceFsState.files.get(String(from));
            const plannerReservation = pendingSnapshot
              ? (JSON.parse(pendingSnapshot) as Array<Record<string, unknown>>).find(
                  (reservation) => reservation.name === plannerName,
                )
              : undefined;
            if (
              plannerReservation?.agentReleasedGeneration === 1 &&
              plannerReservation.agentGeneration === undefined &&
              plannerReservation.agentReleasePending !== true
            ) {
              if (!completionFailureInjected) {
                completionFailureInjected = true;
                throw new Error('registry finalization failed after release evidence');
              }
              const result = (persistedRename as unknown as (source: unknown, destination: unknown) => void)(from, to);
              releaseCompletionCommitted = true;
              return result;
            }
          }
          return (persistedRename as unknown as (source: unknown, destination: unknown) => void)(from, to);
        }) as typeof fs.renameSync);

        try {
          await expect(handle.dispatcher.resetAgent('planner', { force: true })).resolves.toMatchObject({
            ok: false,
            code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
            previous_generation: 1,
          });

          expect(completionFailureInjected).toBe(true);
          expect(releaseCompletionCommitted).toBe(false);
          expect(readRegistryReservation()).toMatchObject({
            agentGeneration: 1,
            agentReleasePending: true,
            agentReleaseOwnerInstanceId: mgr.autoloopOwnerInstanceId,
          });
          expect(readGenerationRows().some(({ payload }) => payload.generation === 2)).toBe(false);
          expect(mockSessions).toHaveLength(1);

          const originalStartSession = mgr.startSession.bind(mgr);
          const successorStarts = vi.spyOn(mgr, 'startSession').mockImplementation(async (config, generation) => {
            if (config.name === plannerName) expect(releaseCompletionCommitted).toBe(true);
            return await originalStartSession(config, generation);
          });

          await expect(
            mgr.autoloopChat(runId, 'continue through pending release reconciliation'),
          ).resolves.toMatchObject({ reply: expect.any(String) });

          expect(releaseCompletionCommitted).toBe(true);
          expect(successorStarts).toHaveBeenCalledTimes(1);
          expect(readRegistryReservation()).toMatchObject({
            agentGeneration: 2,
            agentReleasedGeneration: 1,
          });
          expect(readRegistryReservation()).not.toHaveProperty('agentReleasePending');
          expect(mockSessions).toHaveLength(2);
          expect(mgr.listSessions().filter(({ name }) => name === plannerName)).toHaveLength(1);
          const generationRows = readGenerationRows();
          expect(
            generationRows.filter(
              ({ kind, payload }) => kind === 'agent_generation_released' && payload.generation === 1,
            ),
          ).toHaveLength(1);
          expect(
            generationRows.filter(
              ({ kind, payload }) => kind === 'agent_generation_reserved' && payload.generation === 2,
            ),
          ).toHaveLength(1);
          expect(
            generationRows.filter(
              ({ kind, payload }) => kind === 'agent_generation_started' && payload.generation === 2,
            ),
          ).toHaveLength(1);
          expect(generationRows.some(({ payload }) => payload.generation > 2)).toBe(false);
        } finally {
          vi.mocked(fs.renameSync).mockImplementation(persistedRename);
        }
      });

      it('fails a post-release reusability probe without resurrecting the released Planner generation', async () => {
        const runId = 'reset-probe-preserves-state';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const plannerName = handle.dispatcher.sessionNames.planner;
        const roleState = handle.dispatcher as unknown as { plannerStarted: boolean };
        const managerWithProbe = mgr as unknown as {
          probeAgentNameReusable: (name: string, generation?: PhysicalAgentGeneration) => boolean;
          persistedSessions: Map<string, Record<string, unknown>>;
        };
        managerWithProbe.probeAgentNameReusable = vi.fn(() => false);

        const result = await handle.dispatcher.resetAgent('planner', { force: true });
        const reservation = managerWithProbe.persistedSessions.get(plannerName);

        expect(result).toMatchObject({ ok: false, code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' });
        expect(reservation).toMatchObject({
          agentGeneration: undefined,
          agentReleasePending: undefined,
          agentReleasedGeneration: 1,
        });
        expect(roleState.plannerStarted).toBe(false);
      });

      it('clears released Reviewer state and its frozen prompt when a post-release probe fails', async () => {
        const runId = 'reset-reviewer-post-release-probe';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        await handle.dispatcher.spawnSubagents();
        const roleState = handle.dispatcher as unknown as {
          reviewerStarted: boolean;
          reviewerSessionPrompt: string | null;
        };
        const managerWithProbe = mgr as unknown as {
          probeAgentNameReusable: (name: string, generation?: PhysicalAgentGeneration) => boolean;
        };
        managerWithProbe.probeAgentNameReusable = vi.fn(() => false);

        const result = await handle.dispatcher.resetAgent('reviewer');

        expect(result).toMatchObject({ ok: false, code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' });
        expect(roleState.reviewerStarted).toBe(false);
        expect(roleState.reviewerSessionPrompt).toBeNull();
      });

      it('leaves a failed eager replacement released and permits a later exact-generation recovery', async () => {
        const runId = 'reset-eager-restart-fails';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const roleState = handle.dispatcher as unknown as { plannerStarted: boolean };
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (mgr as any)._createSession = (): ISession => {
          const mock = new MockSession();
          mock.start = async () => {
            throw new Error('replacement Planner failed to start');
          };
          mockSessions.push(mock);
          return mock;
        };

        const result = await handle.dispatcher.resetAgent('planner', { force: true, eagerRestart: true });

        expect(result).toMatchObject({ ok: false, code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' });
        expect(roleState.plannerStarted).toBe(false);
        let generationRows = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'agent-generations.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { generation: number; state: string } });
        expect(generationRows.at(-1)).toMatchObject({
          kind: 'agent_generation_released',
          payload: { generation: 2, state: 'released' },
        });

        patchCreateSession(mgr);
        await expect(mgr.autoloopChat(runId, 'continue after replacement recovery')).resolves.toMatchObject({
          reply: expect.any(String),
        });
        expect(roleState.plannerStarted).toBe(true);
        generationRows = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'agent-generations.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { generation: number; state: string } });
        expect(generationRows.at(-1)).toMatchObject({
          kind: 'agent_generation_started',
          payload: { generation: 3, state: 'live' },
        });
      });

      it('keeps the Planner started flag when reset liveness is unknown', async () => {
        const runId = 'reset-liveness-unknown';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const roleState = handle.dispatcher as unknown as { plannerStarted: boolean };
        vi.spyOn(mgr, 'inspect').mockResolvedValue('unknown');

        const result = await handle.dispatcher.resetAgent('planner', { force: true });

        expect(result).toMatchObject({ ok: false, code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' });
        expect(roleState.plannerStarted).toBe(true);
      });

      it('proves production name reuse and creates the next exact generation after reset', async () => {
        const runId = 'reset-production-reuse';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;

        await expect(handle.dispatcher.resetAgent('planner', { force: true })).resolves.toMatchObject({
          ok: true,
          previous_generation: 1,
          reusable: true,
        });
        await expect(mgr.autoloopChat(runId, 'continue')).resolves.toMatchObject({ reply: expect.any(String) });

        const generationRows = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'agent-generations.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { generation: number; state: string } });
        expect(generationRows.at(-1)).toMatchObject({
          kind: 'agent_generation_started',
          payload: { generation: 2, state: 'live' },
        });
      });

      it('returns the live replacement generation after a successful eager reset', async () => {
        const runId = 'reset-eager-restart-success';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;

        await expect(
          handle.dispatcher.resetAgent('planner', { force: true, eagerRestart: true }),
        ).resolves.toMatchObject({
          ok: true,
          previous_generation: 1,
          active_generation: 2,
          reusable: true,
        });
        await expect(mgr.inspect(handle.dispatcher.sessionNames.planner)).resolves.toBe('live');
        const generationRows = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'agent-generations.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { generation: number; state: string } });
        expect(generationRows.at(-1)).toMatchObject({
          kind: 'agent_generation_started',
          payload: { generation: 2, state: 'live' },
        });
      });

      it('retains an unproven eager replacement without duplicate startup and recovers when it proves live', async () => {
        const runId = 'reset-eager-restart-liveness-unknown';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const roleState = handle.dispatcher as unknown as { plannerStarted: boolean };
        const inspect = vi
          .spyOn(mgr, 'inspect')
          .mockResolvedValueOnce('absent')
          .mockResolvedValueOnce('absent')
          .mockResolvedValueOnce('unknown');

        await expect(
          handle.dispatcher.resetAgent('planner', { force: true, eagerRestart: true }),
        ).resolves.toMatchObject({
          ok: false,
          code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
          previous_generation: 1,
        });
        expect(roleState.plannerStarted).toBe(true);
        expect(mockSessions).toHaveLength(2);
        let generationRows = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'agent-generations.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { generation: number; state: string } });
        expect(generationRows.at(-1)).toMatchObject({
          kind: 'agent_generation_started',
          payload: { generation: 2, state: 'live' },
        });

        inspect.mockResolvedValue('live');
        await expect(mgr.autoloopChat(runId, 'continue on the retained replacement')).resolves.toMatchObject({
          reply: expect.any(String),
        });
        expect(mockSessions).toHaveLength(2);
        generationRows = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'agent-generations.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { generation: number; state: string } });
        expect(generationRows.at(-1)).toMatchObject({
          kind: 'agent_generation_started',
          payload: { generation: 2, state: 'live' },
        });
      });
    });

    describe('Autoloop send-timeout resume migration', () => {
      type ResumeOverride = {
        sendTimeoutMs?: unknown;
        pendingDispatchId?: string;
      };

      const resumeWithOverride = (runId: string, opts: ResumeOverride = {}) =>
        mgr.autoloopResume(
          runId,
          opts as unknown as Parameters<InstanceType<typeof SessionManager>['autoloopResume']>[1],
        );

      const workspaceFor = (runId: string): string => {
        const workspace = path.join(TEST_WF_DIR, 'workspaces', runId);
        fs.mkdirSync(workspace, { recursive: true });
        return workspace;
      };

      const auditPathFor = (workspace: string, runId: string): string =>
        path.join(workspace, 'tasks', runId, 'decisions.jsonl');

      const readIfPresent = (file: string): string => (fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '');

      const storedSpecPath = (runId: string): string => path.join(TEST_WF_DIR, runId, 'spec.json');

      const pendingTimeout = (dispatchId: string, timeoutMs: number) => ({
        status: 'awaiting_resume' as const,
        dispatch_id: dispatchId,
        agent: 'planner' as const,
        message_id: `message-${dispatchId}`,
        message_type: 'chat' as const,
        iter: 0,
        timeout_ms: timeoutMs,
        error: `Timed out after ${timeoutMs}ms`,
      });

      const pauseForTimeout = async (runId: string, workspace: string, timeoutMs: number, dispatchId: string) => {
        await mgr.autoloopStart({ runId, workspace, sendTimeoutMs: timeoutMs });
        const handle = mgr.getAutoloop(runId)!;
        const pending = pendingTimeout(dispatchId, timeoutMs);
        await handle.runner.send(AutoloopMsg.sendTimeout(0, pending));
        expect(handle.runner.state).toMatchObject({
          status: 'paused',
          pending_dispatch: pending,
        });
        return { handle, pending };
      };

      const terminateAndReconstructManager = async (runId: string): Promise<void> => {
        await mgr.autoloopStop(runId, 'test-restart');
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await (mgr as any).kernel.wait(runId);
        await mgr.shutdown();
        mgr = createManager();
      };

      it('increases a live recoverable timeout through the matching dispatch without replaying it', async () => {
        const runId = 'resume-timeout-live';
        const workspace = workspaceFor(runId);
        const dispatchId = 'dispatch-live-planner-0';
        const { handle, pending } = await pauseForTimeout(runId, workspace, 600_000, dispatchId);
        const auditPath = auditPathFor(workspace, runId);
        const historyPath = path.join(workspace, 'tasks', runId, 'chat.jsonl');
        const historicAudit = `${JSON.stringify({ ts: '2026-01-01T00:00:00.000Z', kind: 'existing' })}\n`;
        const historicChat = `${JSON.stringify({ who: 'user', text: 'keep me', ts: '2026-01-01T00:00:00.000Z' })}\n`;
        fs.writeFileSync(auditPath, historicAudit);
        fs.writeFileSync(historyPath, historicChat);
        const originalSpec = fs.readFileSync(storedSpecPath(runId), 'utf8');
        const sendsBefore = mockSessions[0].sendCalls.length;

        // No override retains the old public behaviour: a live handle is only
        // observed, and its recoverable pause is not discarded.
        await expect(mgr.autoloopResume(runId)).resolves.toBe(handle.runner.state);
        expect(handle.runner.state).toMatchObject({ status: 'paused', pending_dispatch: pending });
        expect(readIfPresent(auditPath)).toBe(historicAudit);

        const resumed = await resumeWithOverride(runId, {
          sendTimeoutMs: 7_200_000,
          pendingDispatchId: dispatchId,
        });

        expect(resumed).toBe(handle.runner.state);
        expect(resumed).toMatchObject({
          run_id: runId,
          status: 'running',
          status_reason: null,
          pending_dispatch: null,
        });
        expect(handle.dispatcher.config.sendTimeoutMs).toBe(7_200_000);
        expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(originalSpec);
        expect(fs.readFileSync(historyPath, 'utf8')).toBe(historicChat);

        const auditAfter = fs.readFileSync(auditPath, 'utf8');
        expect(auditAfter.startsWith(historicAudit)).toBe(true);
        const migrations = auditAfter
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as Record<string, unknown>)
          .filter((row) => row.kind === 'timeout_migration');
        expect(migrations).toHaveLength(1);
        expect(migrations[0]).toMatchObject({
          kind: 'timeout_migration',
          runId,
          field: 'sendTimeoutMs',
          oldValue: 600_000,
          newValue: 7_200_000,
          reason: 'recoverable_send_timeout_resume',
          pendingDispatchId: dispatchId,
        });
        expect(Date.parse(String(migrations[0].timestamp))).not.toBeNaN();

        // Replaying the already-resolved timeout result is ignored. In
        // particular it neither starts another Planner turn nor pauses again.
        await handle.runner.send(AutoloopMsg.sendTimeout(0, pending));
        expect(handle.runner.state).toMatchObject({ status: 'running', pending_dispatch: null });
        expect(mockSessions[0].sendCalls).toHaveLength(sendsBefore);
        await mgr.autoloopChat(runId, 'a distinct logical dispatch after resume');
        expect(mockSessions[0].sendCalls.at(-1)?.options?.timeout).toBe(7_200_000);
        await expect(
          resumeWithOverride(runId, { sendTimeoutMs: 7_200_000, pendingDispatchId: dispatchId }),
        ).rejects.toThrow(/not awaiting.*send timeout/i);
        expect(fs.readFileSync(auditPath, 'utf8')).toBe(auditAfter);
      });

      it.each([
        { barrier: 'file', target: 'decisions.jsonl' },
        { barrier: 'directory', target: '' },
      ] as const)(
        'reconciles a committed live migration after one $barrier-sync interruption',
        async ({ barrier, target }) => {
          const runId = `resume-timeout-live-${barrier}-sync-incomplete`;
          const workspace = workspaceFor(runId);
          const dispatchId = `dispatch-live-${barrier}-sync-incomplete`;
          const { handle } = await pauseForTimeout(runId, workspace, 600_000, dispatchId);
          const auditPath = auditPathFor(workspace, runId);
          const failedTarget = target ? path.join(workspace, 'tasks', runId, target) : path.dirname(auditPath);
          const flush = vi.mocked(fs.fsyncSync);
          const flushImplementation = flush.getMockImplementation()!;
          let injectedFailures = 0;
          flush.mockImplementation((fd) => {
            if (persistenceFsState.openPaths.get(fd) === failedTarget && injectedFailures === 0) {
              injectedFailures++;
              throw new Error(`injected live ${barrier} sync interruption`);
            }
            return flushImplementation(fd);
          });

          try {
            await expect(
              resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
            ).resolves.toBe(handle.runner.state);
          } finally {
            flush.mockImplementation(flushImplementation);
          }

          expect(injectedFailures).toBe(1);
          expect(handle.dispatcher.effectiveSendTimeoutMs).toBe(700_000);
          expect(handle.runner.state).toMatchObject({ status: 'running', pending_dispatch: null });
          const migrations = fs
            .readFileSync(auditPath, 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'timeout_migration');
          expect(migrations).toHaveLength(1);

          await expect(
            resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
          ).rejects.toThrow(/not awaiting.*send timeout/i);
          expect(
            fs
              .readFileSync(auditPath, 'utf8')
              .trim()
              .split('\n')
              .map((line) => JSON.parse(line) as { kind?: string })
              .filter((row) => row.kind === 'timeout_migration'),
          ).toHaveLength(1);
        },
      );

      it.each([
        {
          barrier: 'file',
          target: 'decisions.jsonl',
          code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
        },
        {
          barrier: 'directory',
          target: '',
          code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        },
      ] as const)(
        'reports applied live migration effects after a persistent $barrier-sync failure without replay',
        async ({ barrier, target, code }) => {
          const runId = `resume-timeout-live-${barrier}-sync-persistent`;
          const workspace = workspaceFor(runId);
          const dispatchId = `dispatch-live-${barrier}-sync-persistent`;
          const { handle } = await pauseForTimeout(runId, workspace, 600_000, dispatchId);
          const auditPath = auditPathFor(workspace, runId);
          const failedTarget = target ? path.join(workspace, 'tasks', runId, target) : path.dirname(auditPath);
          const flush = vi.mocked(fs.fsyncSync);
          const flushImplementation = flush.getMockImplementation()!;
          let injectedFailures = 0;
          flush.mockImplementation((fd) => {
            if (persistenceFsState.openPaths.get(fd) === failedTarget) {
              injectedFailures++;
              throw new Error(`persistent live ${barrier} sync failure`);
            }
            return flushImplementation(fd);
          });

          try {
            await expect(
              resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
            ).rejects.toMatchObject({
              code,
              committed: true,
              retryable: false,
              effectsApplied: true,
              operation: 'send_timeout_migration',
            });
          } finally {
            flush.mockImplementation(flushImplementation);
          }

          expect(injectedFailures).toBe(2);
          expect(handle.dispatcher.effectiveSendTimeoutMs).toBe(700_000);
          expect(handle.runner.state).toMatchObject({ status: 'running', pending_dispatch: null });
          const auditAfter = fs.readFileSync(auditPath, 'utf8');
          expect(
            auditAfter
              .trim()
              .split('\n')
              .map((line) => JSON.parse(line) as { kind?: string })
              .filter((row) => row.kind === 'timeout_migration'),
          ).toHaveLength(1);
          const sendsAfterMigration = mockSessions[0].sendCalls.length;

          await expect(
            resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
          ).rejects.toThrow(/not awaiting.*send timeout/i);
          expect(mockSessions[0].sendCalls).toHaveLength(sendsAfterMigration);
          expect(handle.dispatcher.effectiveSendTimeoutMs).toBe(700_000);
          expect(handle.runner.state).toMatchObject({ status: 'running', pending_dispatch: null });
          expect(fs.readFileSync(auditPath, 'utf8')).toBe(auditAfter);
        },
      );

      it('contains a live timeout migration inside its pinned run capability after a run-directory swap', async () => {
        const runId = 'resume-timeout-live-pinned-run-swap';
        const workspace = workspaceFor(runId);
        const dispatchId = 'dispatch-live-pinned-run-swap';
        const { handle, pending } = await pauseForTimeout(runId, workspace, 600_000, dispatchId);
        const runDir = path.join(workspace, 'tasks', runId);
        const originalDir = `${runDir}.saved`;
        fs.renameSync(runDir, originalDir);
        fs.mkdirSync(runDir);

        await expect(
          resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
        ).rejects.toThrow(/identity|changed|replaced/i);

        expect(handle.dispatcher.effectiveSendTimeoutMs).toBe(600_000);
        expect(handle.runner.state).toMatchObject({ status: 'paused', pending_dispatch: pending });
        expect(fs.readdirSync(runDir)).toEqual([]);
        expect(
          readIfPresent(path.join(originalDir, 'decisions.jsonl'))
            .trim()
            .split('\n')
            .filter(Boolean)
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'timeout_migration'),
        ).toHaveLength(0);
      });

      it('rejects equal, decreased, malformed, and out-of-range live overrides atomically', async () => {
        const runId = 'resume-timeout-invalid';
        const workspace = workspaceFor(runId);
        const dispatchId = 'dispatch-invalid-planner-0';
        const { handle } = await pauseForTimeout(runId, workspace, 600_000, dispatchId);
        const auditPath = auditPathFor(workspace, runId);
        const historyPath = path.join(workspace, 'tasks', runId, 'chat.jsonl');
        fs.writeFileSync(auditPath, '{"kind":"existing"}\n');
        fs.writeFileSync(historyPath, '{"who":"user","text":"history"}\n');
        const before = {
          state: JSON.stringify(handle.runner.state),
          timeout: handle.dispatcher.config.sendTimeoutMs,
          sessions: mgr.listSessions().map((session) => session.name),
          audit: fs.readFileSync(auditPath, 'utf8'),
          history: fs.readFileSync(historyPath, 'utf8'),
          spec: fs.readFileSync(storedSpecPath(runId), 'utf8'),
        };
        const invalidValues: unknown[] = [
          600_000,
          599_999,
          4_999,
          7_200_001,
          Number.NaN,
          Number.POSITIVE_INFINITY,
          '700000',
        ];

        for (const sendTimeoutMs of invalidValues) {
          await expect(resumeWithOverride(runId, { sendTimeoutMs, pendingDispatchId: dispatchId })).rejects.toThrow(
            /sendTimeoutMs/,
          );
          expect(JSON.stringify(handle.runner.state)).toBe(before.state);
          expect(handle.dispatcher.config.sendTimeoutMs).toBe(before.timeout);
          expect(mgr.listSessions().map((session) => session.name)).toEqual(before.sessions);
          expect(fs.readFileSync(auditPath, 'utf8')).toBe(before.audit);
          expect(fs.readFileSync(historyPath, 'utf8')).toBe(before.history);
          expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(before.spec);
        }
      });

      it('rejects a stale pending dispatch identity before changing timeout or audit state', async () => {
        const runId = 'resume-timeout-stale';
        const workspace = workspaceFor(runId);
        const dispatchId = 'dispatch-current-planner-0';
        const { handle } = await pauseForTimeout(runId, workspace, 600_000, dispatchId);
        const auditPath = auditPathFor(workspace, runId);
        const auditBefore = readIfPresent(auditPath);
        const stateBefore = JSON.stringify(handle.runner.state);

        await expect(resumeWithOverride(runId, { sendTimeoutMs: 700_000 })).rejects.toThrow(
          /pendingDispatchId is required/i,
        );
        await expect(
          resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: 'dispatch-stale-planner-0' }),
        ).rejects.toThrow(/pending dispatch.*does not match/i);

        expect(JSON.stringify(handle.runner.state)).toBe(stateBefore);
        expect(handle.dispatcher.config.sendTimeoutMs).toBe(600_000);
        expect(readIfPresent(auditPath)).toBe(auditBefore);
      });

      it('persists increases across manager reconstruction while leaving the original spec and evidence untouched', async () => {
        const runId = 'resume-timeout-reconstructed';
        const workspace = workspaceFor(runId);
        await mgr.autoloopStart({ runId, workspace, sendTimeoutMs: 650_000 });
        const auditPath = auditPathFor(workspace, runId);
        const historyPath = path.join(workspace, 'tasks', runId, 'chat.jsonl');
        const evidencePath = path.join(workspace, 'tasks', runId, 'iter', '0', 'verdict.json');
        fs.mkdirSync(path.dirname(evidencePath), { recursive: true });
        fs.writeFileSync(historyPath, '{"who":"planner","text":"historic chat"}\n');
        fs.writeFileSync(evidencePath, '{"decision":"hold","historic":true}\n');
        const original = {
          spec: fs.readFileSync(storedSpecPath(runId), 'utf8'),
          history: fs.readFileSync(historyPath, 'utf8'),
          evidence: fs.readFileSync(evidencePath, 'utf8'),
        };

        await terminateAndReconstructManager(runId);
        const first = await resumeWithOverride(runId, { sendTimeoutMs: 700_000 });
        expect(first.run_id).toBe(runId);
        expect(mgr.getAutoloop(runId)!.dispatcher.config.sendTimeoutMs).toBe(700_000);
        const afterFirstAudit = fs.readFileSync(auditPath, 'utf8');
        expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(original.spec);
        expect(fs.readFileSync(historyPath, 'utf8')).toBe(original.history);
        expect(fs.readFileSync(evidencePath, 'utf8')).toBe(original.evidence);

        // A plain resume after another process reconstruction carries forward
        // the latest effective increase but does not append another migration.
        await terminateAndReconstructManager(runId);
        const beforePlainResume = fs.readFileSync(auditPath, 'utf8');
        expect(beforePlainResume.startsWith(afterFirstAudit)).toBe(true);
        await mgr.autoloopResume(runId);
        expect(mgr.getAutoloop(runId)!.dispatcher.config.sendTimeoutMs).toBe(700_000);
        expect(fs.readFileSync(auditPath, 'utf8')).toBe(beforePlainResume);

        await terminateAndReconstructManager(runId);
        const beforeRejectedEqual = fs.readFileSync(auditPath, 'utf8');
        await expect(resumeWithOverride(runId, { sendTimeoutMs: 700_000 })).rejects.toThrow(/strictly greater/i);
        expect(mgr.getAutoloop(runId)).toBeUndefined();
        expect(fs.readFileSync(auditPath, 'utf8')).toBe(beforeRejectedEqual);

        await resumeWithOverride(runId, { sendTimeoutMs: 800_000 });
        expect(mgr.getAutoloop(runId)!.dispatcher.config.sendTimeoutMs).toBe(800_000);
        const migrations = fs
          .readFileSync(auditPath, 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as Record<string, unknown>)
          .filter((row) => row.kind === 'timeout_migration');
        expect(migrations).toMatchObject([
          { runId, oldValue: 650_000, newValue: 700_000 },
          { runId, oldValue: 700_000, newValue: 800_000 },
        ]);
        expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(original.spec);
        expect(fs.readFileSync(historyPath, 'utf8')).toBe(original.history);
        expect(fs.readFileSync(evidencePath, 'utf8')).toBe(original.evidence);
      });

      it.each([
        { barrier: 'file', target: 'decisions.jsonl' },
        { barrier: 'directory', target: '' },
      ] as const)(
        'recovers a committed stored migration after a $barrier-sync interruption without appending it again',
        async ({ barrier, target }) => {
          const runId = `resume-timeout-stored-${barrier}-sync-incomplete`;
          const workspace = workspaceFor(runId);
          const dispatchId = `dispatch-stored-${barrier}-sync-incomplete`;
          await pauseForTimeout(runId, workspace, 600_000, dispatchId);

          // Simulate process loss while retaining the recoverable timeout state.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const oldKernel = (mgr as any).kernel;
          oldKernel.cancel(runId);
          await oldKernel.wait(runId);
          await mgr.shutdown();
          mgr = createManager();

          const auditPath = auditPathFor(workspace, runId);
          const ledgerDir = path.dirname(auditPath);
          const failedTarget = target ? path.join(ledgerDir, target) : ledgerDir;
          const appendFile = vi.mocked(fs.appendFileSync);
          const appendImplementation = appendFile.getMockImplementation()!;
          const flush = vi.mocked(fs.fsyncSync);
          const flushImplementation = flush.getMockImplementation()!;
          let migrationBytesAppended = false;
          let injectedFailures = 0;
          appendFile.mockImplementation(((file: unknown, data: unknown, ...args: unknown[]) => {
            const result = (appendImplementation as (...values: unknown[]) => unknown)(file, data, ...args);
            if (isOpenPath(file, auditPath) && String(data).includes('"kind":"timeout_migration"')) {
              migrationBytesAppended = true;
            }
            return result;
          }) as typeof fs.appendFileSync);
          flush.mockImplementation((fd) => {
            if (
              migrationBytesAppended &&
              persistenceFsState.openPaths.get(fd) === failedTarget &&
              injectedFailures === 0
            ) {
              injectedFailures++;
              throw new Error(`injected stored ${barrier} sync interruption`);
            }
            return flushImplementation(fd);
          });

          try {
            await expect(
              resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
            ).resolves.toMatchObject({ run_id: runId });
          } finally {
            appendFile.mockImplementation(appendImplementation);
            flush.mockImplementation(flushImplementation);
          }

          expect(injectedFailures).toBe(1);
          expect(mgr.getAutoloop(runId)!.dispatcher.effectiveSendTimeoutMs).toBe(700_000);
          let migrations = fs
            .readFileSync(auditPath, 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'timeout_migration');
          expect(migrations).toHaveLength(1);

          await terminateAndReconstructManager(runId);
          const beforePlainRecovery = fs.readFileSync(auditPath, 'utf8');
          await expect(mgr.autoloopResume(runId)).resolves.toMatchObject({ run_id: runId });
          expect(mgr.getAutoloop(runId)!.dispatcher.effectiveSendTimeoutMs).toBe(700_000);
          expect(fs.readFileSync(auditPath, 'utf8')).toBe(beforePlainRecovery);
          migrations = beforePlainRecovery
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind?: string })
            .filter((row) => row.kind === 'timeout_migration');
          expect(migrations).toHaveLength(1);
        },
      );

      it.each([
        {
          barrier: 'file',
          target: 'decisions.jsonl',
          code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
        },
        {
          barrier: 'directory',
          target: '',
          code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        },
      ] as const)(
        'reports applied stored migration effects after a persistent $barrier-sync failure across restart',
        async ({ barrier, target, code }) => {
          const runId = `resume-timeout-stored-${barrier}-sync-persistent`;
          const workspace = workspaceFor(runId);
          const dispatchId = `dispatch-stored-${barrier}-sync-persistent`;
          await pauseForTimeout(runId, workspace, 600_000, dispatchId);

          // Simulate process loss while retaining the recoverable timeout state.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const oldKernel = (mgr as any).kernel;
          oldKernel.cancel(runId);
          await oldKernel.wait(runId);
          await mgr.shutdown();
          mgr = createManager();

          const auditPath = auditPathFor(workspace, runId);
          const ledgerDir = path.dirname(auditPath);
          const failedTarget = target ? path.join(ledgerDir, target) : ledgerDir;
          const appendFile = vi.mocked(fs.appendFileSync);
          const appendImplementation = appendFile.getMockImplementation()!;
          const flush = vi.mocked(fs.fsyncSync);
          const flushImplementation = flush.getMockImplementation()!;
          let migrationBytesAppended = false;
          let injectedFailures = 0;
          appendFile.mockImplementation(((file: unknown, data: unknown, ...args: unknown[]) => {
            const result = (appendImplementation as (...values: unknown[]) => unknown)(file, data, ...args);
            if (isOpenPath(file, auditPath) && String(data).includes('"kind":"timeout_migration"')) {
              migrationBytesAppended = true;
            }
            return result;
          }) as typeof fs.appendFileSync);
          flush.mockImplementation((fd) => {
            if (migrationBytesAppended && persistenceFsState.openPaths.get(fd) === failedTarget) {
              injectedFailures++;
              throw new Error(`persistent stored ${barrier} sync failure`);
            }
            return flushImplementation(fd);
          });

          try {
            await expect(
              resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
            ).rejects.toMatchObject({
              code,
              committed: true,
              retryable: false,
              effectsApplied: true,
              operation: 'send_timeout_migration',
            });
          } finally {
            appendFile.mockImplementation(appendImplementation);
            flush.mockImplementation(flushImplementation);
          }

          expect(injectedFailures).toBe(2);
          const liveAfterError = mgr.getAutoloop(runId)!;
          expect(liveAfterError.dispatcher.effectiveSendTimeoutMs).toBe(700_000);
          expect(liveAfterError.runner.state).toMatchObject({ status: 'planning', pending_dispatch: null });
          const auditAfter = fs.readFileSync(auditPath, 'utf8');
          expect(
            auditAfter
              .trim()
              .split('\n')
              .map((line) => JSON.parse(line) as { kind?: string })
              .filter((row) => row.kind === 'timeout_migration'),
          ).toHaveLength(1);

          // A fresh manager reconstructs the committed row as authoritative.
          // The identical outer request cannot append again or revive the
          // resolved pending dispatch; a plain recovery keeps the new timeout.
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const resumedKernel = (mgr as any).kernel;
          resumedKernel.cancel(runId);
          await resumedKernel.wait(runId);
          await mgr.shutdown();
          mgr = createManager();
          const auditBeforeOuterRetry = fs.readFileSync(auditPath, 'utf8');
          expect(auditBeforeOuterRetry.startsWith(auditAfter)).toBe(true);
          expect(
            auditBeforeOuterRetry
              .trim()
              .split('\n')
              .map((line) => JSON.parse(line) as { kind?: string })
              .filter((row) => row.kind === 'timeout_migration'),
          ).toHaveLength(1);

          await expect(
            resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
          ).rejects.toThrow(/no pending dispatch/i);
          expect(mgr.getAutoloop(runId)).toBeUndefined();
          expect(fs.readFileSync(auditPath, 'utf8')).toBe(auditBeforeOuterRetry);

          await expect(mgr.autoloopResume(runId)).resolves.toMatchObject({ run_id: runId });
          expect(mgr.getAutoloop(runId)!.dispatcher.effectiveSendTimeoutMs).toBe(700_000);
          expect(mgr.getAutoloop(runId)!.runner.state).toMatchObject({ status: 'planning', pending_dispatch: null });
          expect(fs.readFileSync(auditPath, 'utf8')).toBe(auditBeforeOuterRetry);
        },
      );

      it('uses the 600000ms compatibility default for a legacy stored run', async () => {
        const runId = 'resume-timeout-legacy';
        const workspace = workspaceFor(runId);
        await mgr.autoloopStart({ runId, workspace });
        const originalSpec = fs.readFileSync(storedSpecPath(runId), 'utf8');
        expect(JSON.parse(originalSpec).nodes[0].config).not.toHaveProperty('sendTimeoutMs');

        await terminateAndReconstructManager(runId);
        await resumeWithOverride(runId, { sendTimeoutMs: 600_001 });

        expect(mgr.getAutoloop(runId)!.dispatcher.config.sendTimeoutMs).toBe(600_001);
        expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(originalSpec);
        const migrations = fs
          .readFileSync(auditPathFor(workspace, runId), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as Record<string, unknown>)
          .filter((row) => row.kind === 'timeout_migration');
        expect(migrations).toMatchObject([{ oldValue: 600_000, newValue: 600_001 }]);
      });

      it('does not persist an increase or disturb pending evidence when a reconstructed resume fails', async () => {
        const runId = 'resume-timeout-failed';
        const workspace = workspaceFor(runId);
        const dispatchId = 'dispatch-failed-planner-0';
        await pauseForTimeout(runId, workspace, 600_000, dispatchId);
        const historyPath = path.join(workspace, 'tasks', runId, 'chat.jsonl');
        const evidencePath = path.join(workspace, 'tasks', runId, 'iter', '0', 'verdict.json');
        fs.mkdirSync(path.dirname(evidencePath), { recursive: true });
        fs.writeFileSync(historyPath, '{"who":"user","text":"preserve"}\n');
        fs.writeFileSync(evidencePath, '{"decision":"hold"}\n');

        // Simulate loss of the owning process. Unlike an operator terminate,
        // cancellation checkpoints the recoverable pending-dispatch metadata.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const oldKernel = (mgr as any).kernel;
        oldKernel.cancel(runId);
        await oldKernel.wait(runId);
        await mgr.shutdown();
        mgr = createManager();

        const auditPath = auditPathFor(workspace, runId);
        const before = {
          spec: fs.readFileSync(storedSpecPath(runId), 'utf8'),
          audit: readIfPresent(auditPath),
          history: fs.readFileSync(historyPath, 'utf8'),
          evidence: fs.readFileSync(evidencePath, 'utf8'),
          sessions: mgr.listSessions().map((session) => session.name),
          pending: JSON.stringify(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (mgr.workflowStatus(runId).nodes.main.data as any).state.pending_dispatch,
          ),
        };
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        (mgr as any)._createSession = (): ISession => {
          const mock = new MockSession();
          mock.start = async () => {
            throw new Error('resume planner failed');
          };
          return mock;
        };

        await expect(
          resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
        ).rejects.toThrow('resume planner failed');

        expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(before.spec);
        expect(readIfPresent(auditPath)).toBe(before.audit);
        expect(fs.readFileSync(historyPath, 'utf8')).toBe(before.history);
        expect(fs.readFileSync(evidencePath, 'utf8')).toBe(before.evidence);
        expect(mgr.listSessions().map((session) => session.name)).toEqual(before.sessions);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        expect(JSON.stringify((mgr.workflowStatus(runId).nodes.main.data as any).state.pending_dispatch)).toBe(
          before.pending,
        );
      });

      it('does not start a reconstructed migration when its audit append cannot be prepared', async () => {
        const runId = 'resume-timeout-audit-unavailable';
        const workspace = workspaceFor(runId);
        const dispatchId = 'dispatch-audit-unavailable-planner-0';
        await pauseForTimeout(runId, workspace, 600_000, dispatchId);
        const historyPath = path.join(workspace, 'tasks', runId, 'chat.jsonl');
        const evidencePath = path.join(workspace, 'tasks', runId, 'iter', '0', 'verdict.json');
        fs.mkdirSync(path.dirname(evidencePath), { recursive: true });
        fs.writeFileSync(historyPath, '{"who":"user","text":"preserve"}\n');
        fs.writeFileSync(evidencePath, '{"decision":"hold"}\n');

        // Reconstruct the manager while preserving the recoverable timeout
        // checkpoint, as if the owning process disappeared mid-dispatch.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const oldKernel = (mgr as any).kernel;
        oldKernel.cancel(runId);
        await oldKernel.wait(runId);
        await mgr.shutdown();
        mgr = createManager();

        const auditPath = auditPathFor(workspace, runId);
        const before = {
          spec: fs.readFileSync(storedSpecPath(runId), 'utf8'),
          audit: readIfPresent(auditPath),
          history: fs.readFileSync(historyPath, 'utf8'),
          evidence: fs.readFileSync(evidencePath, 'utf8'),
          sessions: mgr.listSessions().map((session) => session.name),
          pending: JSON.stringify(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (mgr.workflowStatus(runId).nodes.main.data as any).state.pending_dispatch,
          ),
        };
        const auditOpen = vi.mocked((await import('node:fs')).openSync);
        const openImplementation = auditOpen.getMockImplementation()!;
        auditOpen.mockImplementation(((file, flags, mode) => {
          if (
            String(file) === auditPath &&
            typeof flags === 'number' &&
            (flags & fs.constants.O_APPEND) === fs.constants.O_APPEND
          ) {
            throw new Error('audit append unavailable');
          }
          return openImplementation(file, flags, mode);
        }) as typeof fs.openSync);

        try {
          await expect(
            resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
          ).rejects.toThrow('audit append unavailable');
        } finally {
          auditOpen.mockImplementation(openImplementation);
        }

        expect(mgr.getAutoloop(runId)).toBeUndefined();
        expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(before.spec);
        expect(readIfPresent(auditPath)).toBe(before.audit);
        expect(fs.readFileSync(historyPath, 'utf8')).toBe(before.history);
        expect(fs.readFileSync(evidencePath, 'utf8')).toBe(before.evidence);
        expect(mgr.listSessions().map((session) => session.name)).toEqual(before.sessions);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        expect(JSON.stringify((mgr.workflowStatus(runId).nodes.main.data as any).state.pending_dispatch)).toBe(
          before.pending,
        );
      });

      it('rolls back reconstructed startup when the prepared audit append fails', async () => {
        const runId = 'resume-timeout-audit-write-failed';
        const workspace = workspaceFor(runId);
        const dispatchId = 'dispatch-audit-write-failed-planner-0';
        await pauseForTimeout(runId, workspace, 600_000, dispatchId);
        const historyPath = path.join(workspace, 'tasks', runId, 'chat.jsonl');
        const evidencePath = path.join(workspace, 'tasks', runId, 'iter', '0', 'verdict.json');
        fs.mkdirSync(path.dirname(evidencePath), { recursive: true });
        fs.writeFileSync(historyPath, '{"who":"user","text":"preserve"}\n');
        fs.writeFileSync(evidencePath, '{"decision":"hold"}\n');

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const oldKernel = (mgr as any).kernel;
        oldKernel.cancel(runId);
        await oldKernel.wait(runId);
        await mgr.shutdown();
        mgr = createManager();

        const auditPath = auditPathFor(workspace, runId);
        const before = {
          spec: fs.readFileSync(storedSpecPath(runId), 'utf8'),
          audit: readIfPresent(auditPath),
          history: fs.readFileSync(historyPath, 'utf8'),
          evidence: fs.readFileSync(evidencePath, 'utf8'),
          sessions: mgr.listSessions().map((session) => session.name),
          pending: JSON.stringify(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (mgr.workflowStatus(runId).nodes.main.data as any).state.pending_dispatch,
          ),
        };
        const actualFs = await vi.importActual<typeof import('node:fs')>('node:fs');
        const auditWrite = vi.mocked((await import('node:fs')).writeSync);
        auditWrite.mockImplementation((() => {
          throw new Error('audit append failed');
        }) as typeof fs.writeSync);

        try {
          await expect(
            resumeWithOverride(runId, { sendTimeoutMs: 700_000, pendingDispatchId: dispatchId }),
          ).rejects.toThrow('audit append failed');
        } finally {
          auditWrite.mockImplementation(((fd, ...args) =>
            (actualFs.writeSync as (...values: unknown[]) => number)(fd, ...args)) as typeof fs.writeSync);
        }

        expect(mgr.getAutoloop(runId)).toBeUndefined();
        expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(before.spec);
        expect(readIfPresent(auditPath)).toBe(before.audit);
        expect(fs.readFileSync(historyPath, 'utf8')).toBe(before.history);
        expect(fs.readFileSync(evidencePath, 'utf8')).toBe(before.evidence);
        expect(mgr.listSessions().map((session) => session.name)).toEqual(before.sessions);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        expect(JSON.stringify((mgr.workflowStatus(runId).nodes.main.data as any).state.pending_dispatch)).toBe(
          before.pending,
        );
      });

      describe('Autoloop timeout resilience integration', () => {
        it('carries a genuine timed-out send through an atomic increase and a distinct later send', async () => {
          const runId = 'timeout-integration-lifecycle';
          const workspace = workspaceFor(runId);
          await mgr.autoloopStart({
            runId,
            workspace,
            sendTimeoutMs: 600_000,
            activityLeaseMs: 60_000,
            autoloopHardTimeoutMs: 86_400_000,
          });
          const handle = mgr.getAutoloop(runId)!;
          const planner = mockSessions[0];
          const auditPath = auditPathFor(workspace, runId);
          const historyPath = path.join(workspace, 'tasks', runId, 'chat.jsonl');
          const evidencePath = path.join(workspace, 'tasks', runId, 'iter', '0', 'verdict.json');
          fs.mkdirSync(path.dirname(evidencePath), { recursive: true });
          fs.writeFileSync(evidencePath, '{"decision":"historic-hold"}\n');
          const originalSpec = fs.readFileSync(storedSpecPath(runId), 'utf8');
          const evidenceBefore = fs.readFileSync(evidencePath, 'utf8');
          const observedTimeouts: unknown[] = [];
          handle.runner.on('send_timeout', (event) => observedTimeouts.push(event));

          planner.sendImplementation = async () => {
            planner.sendImplementation = undefined;
            throw new Error('Timeout waiting for response');
          };
          await expect(mgr.autoloopChat(runId, 'one logical send that reaches its deadline')).rejects.toMatchObject({
            code: 'AUTOLOOP_SEND_TIMEOUT',
            retryable: true,
            pending_dispatch: {
              status: 'awaiting_resume',
              agent: 'planner',
              message_type: 'chat',
              timeout_ms: 600_000,
              dispatch_id: expect.stringMatching(/^dispatch_[a-f0-9]{64}$/),
            },
          });

          expect(planner.sendCalls).toHaveLength(1);
          expect(planner.sendCalls[0].options?.timeout).toBe(600_000);
          expect(observedTimeouts).toHaveLength(1);
          expect(handle.runner.state).toMatchObject({
            status: 'paused',
            pending_dispatch: {
              status: 'awaiting_resume',
              agent: 'planner',
              message_type: 'chat',
              timeout_ms: 600_000,
            },
          });
          const pending = handle.runner.state.pending_dispatch!;
          expect(pending.dispatch_id).toMatch(/^dispatch_[a-f0-9]{64}$/);
          expect(handle.runner.state.status_reason).toBe(`awaiting_resume:send_timeout:planner:${pending.dispatch_id}`);

          // Even qualified progress cannot replace or conceal the unresolved
          // dispatch identity. The renewable lease remains suspended here.
          expect(handle.runner.recordActivity('agent_progress')).toBe(true);
          expect(handle.runner.state).toMatchObject({
            status: 'paused',
            status_reason: `awaiting_resume:send_timeout:planner:${pending.dispatch_id}`,
            pending_dispatch: pending,
          });

          const beforeResume = {
            state: JSON.stringify(handle.runner.state),
            timeout: handle.dispatcher.effectiveSendTimeoutMs,
            sessions: mgr.listSessions().map((session) => session.name),
            spec: fs.readFileSync(storedSpecPath(runId), 'utf8'),
            audit: fs.readFileSync(auditPath, 'utf8'),
            history: fs.readFileSync(historyPath, 'utf8'),
            evidence: fs.readFileSync(evidencePath, 'utf8'),
          };
          const rejected: ResumeOverride[] = [
            { sendTimeoutMs: 650_000, pendingDispatchId: 'dispatch_stale' },
            { sendTimeoutMs: 600_000, pendingDispatchId: pending.dispatch_id },
            { sendTimeoutMs: 599_999, pendingDispatchId: pending.dispatch_id },
            { sendTimeoutMs: 7_200_001, pendingDispatchId: pending.dispatch_id },
            { sendTimeoutMs: Number.NaN, pendingDispatchId: pending.dispatch_id },
          ];
          for (const override of rejected) {
            await expect(resumeWithOverride(runId, override)).rejects.toThrow();
            expect(JSON.stringify(handle.runner.state)).toBe(beforeResume.state);
            expect(handle.dispatcher.effectiveSendTimeoutMs).toBe(beforeResume.timeout);
            expect(mgr.listSessions().map((session) => session.name)).toEqual(beforeResume.sessions);
            expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(beforeResume.spec);
            expect(fs.readFileSync(auditPath, 'utf8')).toBe(beforeResume.audit);
            expect(fs.readFileSync(historyPath, 'utf8')).toBe(beforeResume.history);
            expect(fs.readFileSync(evidencePath, 'utf8')).toBe(beforeResume.evidence);
          }

          await resumeWithOverride(runId, {
            sendTimeoutMs: 700_000,
            pendingDispatchId: pending.dispatch_id,
          });
          expect(handle.runner.state).toMatchObject({
            status: 'running',
            status_reason: null,
            pending_dispatch: null,
          });
          expect(handle.dispatcher.effectiveSendTimeoutMs).toBe(700_000);
          expect(planner.sendCalls).toHaveLength(1);
          expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(originalSpec);
          expect(fs.readFileSync(historyPath, 'utf8')).toBe(beforeResume.history);
          expect(fs.readFileSync(evidencePath, 'utf8')).toBe(evidenceBefore);

          const auditAfterResume = fs.readFileSync(auditPath, 'utf8');
          expect(auditAfterResume.startsWith(beforeResume.audit)).toBe(true);
          const appendedRows = auditAfterResume
            .slice(beforeResume.audit.length)
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as Record<string, unknown>);
          expect(appendedRows).toMatchObject([
            {
              kind: 'timeout_migration',
              runId,
              field: 'sendTimeoutMs',
              oldValue: 600_000,
              newValue: 700_000,
              reason: 'recoverable_send_timeout_resume',
              pendingDispatchId: pending.dispatch_id,
            },
          ]);

          await mgr.autoloopChat(runId, 'a later and distinct logical send');
          expect(planner.sendCalls).toHaveLength(2);
          expect(planner.sendCalls[1].options?.timeout).toBe(700_000);
          expect(observedTimeouts).toHaveLength(1);
          expect(fs.readFileSync(storedSpecPath(runId), 'utf8')).toBe(originalSpec);
          expect(fs.readFileSync(evidencePath, 'utf8')).toBe(evidenceBefore);
        });

        it('keeps all three defaults when public start omits timeout configuration', async () => {
          const runId = 'timeout-integration-defaults';
          const workspace = workspaceFor(runId);
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const runtime = handle.runner as unknown as {
            timeouts: {
              sendTimeoutMs: number;
              activityLeaseMs: number;
              autoloopHardTimeoutMs: number;
            };
          };

          expect(runtime.timeouts).toEqual({
            sendTimeoutMs: 600_000,
            activityLeaseMs: 1_800_000,
            autoloopHardTimeoutMs: 86_400_000,
          });
          await mgr.autoloopChat(runId, 'default timeout dispatch');
          expect(mockSessions[0].sendCalls[0].options?.timeout).toBe(600_000);
        });

        it.each(['hard timeout', 'operator stop'] as const)(
          'keeps %s terminal when an in-flight send reports its timeout late',
          async (terminalCause) => {
            const runId = `timeout-integration-${terminalCause.replace(' ', '-')}`;
            const workspace = workspaceFor(runId);
            await mgr.autoloopStart({
              runId,
              workspace,
              sendTimeoutMs: 7_200_000,
              activityLeaseMs: 7_200_000,
              autoloopHardTimeoutMs: 600_000,
            });
            const handle = mgr.getAutoloop(runId)!;
            const planner = mockSessions[0];
            const observedTimeouts: unknown[] = [];
            handle.runner.on('send_timeout', (event) => observedTimeouts.push(event));
            let rejectSend!: (reason?: unknown) => void;
            planner.sendImplementation = () =>
              new Promise((_resolve, reject) => {
                rejectSend = reject;
              });

            const chat = mgr.autoloopChat(runId, 'send still running at terminal transition');
            await vi.waitFor(() => expect(planner.sendCalls).toHaveLength(1));

            if (terminalCause === 'hard timeout') {
              // Qualified progress can renew the lease, but cannot move the
              // absolute deadline anchored at run construction.
              await vi.advanceTimersByTimeAsync(300_000);
              expect(handle.runner.recordActivity('agent_progress')).toBe(true);
              await vi.advanceTimersByTimeAsync(300_000);
            } else {
              await mgr.autoloopStop(runId, 'operator-stop-during-send');
            }

            rejectSend(new Error('Timeout waiting for response'));
            await expect(chat).rejects.toMatchObject({
              code: 'AUTOLOOP_RUN_TERMINAL',
              retryable: false,
              status_reason: terminalCause === 'hard timeout' ? 'hard_timeout_exceeded' : 'operator-stop-during-send',
            });

            expect(handle.runner.state).toMatchObject({
              status: 'terminated',
              status_reason: terminalCause === 'hard timeout' ? 'hard_timeout_exceeded' : 'operator-stop-during-send',
              pending_dispatch: null,
            });
            expect(observedTimeouts).toHaveLength(0);
            expect(planner.sendCalls).toHaveLength(1);
          },
        );
      });
    });

    describe('Task 3A round 11 recovery boundaries', () => {
      it('returns the initiating chat reply when the same drain contains a later Planner turn', async () => {
        const runId = 'planner-turn-bound-reply';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        let turn = 0;
        mockSessions[0].sendImplementation = async () => {
          turn += 1;
          return {
            text: `turn-bound reply ${turn}`,
            event: { type: 'result', result: `turn-bound reply ${turn}` },
          };
        };
        let followUp: Promise<void> | undefined;
        handle.dispatcher.on('planner_reply', () => {
          if (followUp) return;
          followUp = handle.runner.send(AutoloopMsg.chat(0, { text: 'later internal Planner turn' }));
        });

        await expect(mgr.autoloopChat(runId, 'initiating user turn')).resolves.toEqual({
          reply: 'turn-bound reply 1',
        });
        await expect(followUp).resolves.toBeUndefined();
        expect(mockSessions[0].sendCalls).toHaveLength(2);
      });

      it('reports an ordinary paused chat as non-retryable and resumes the parked turn exactly once', async () => {
        const runId = 'planner-chat-ordinary-pause';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        await handle.runner.send(AutoloopMsg.pause(0, { reason: 'operator-review' }));

        await expect(mgr.autoloopChat(runId, 'park this exact turn')).rejects.toMatchObject({
          code: 'AUTOLOOP_RUN_PAUSED',
          retryable: false,
          pending_dispatch: undefined,
          status_reason: 'operator-review',
        });
        expect(
          (handle.runner as unknown as { pausedBuffer: Array<{ payload: { text?: string } }> }).pausedBuffer,
        ).toEqual([expect.objectContaining({ payload: { text: 'park this exact turn' } })]);
        expect(mockSessions[0].sendCalls).toHaveLength(0);

        await handle.runner.send(AutoloopMsg.resume(0));
        await vi.waitFor(() => expect(mockSessions[0].sendCalls).toHaveLength(1));
        expect(mockSessions[0].sendCalls[0].message).toContain('park this exact turn');
        expect(
          (handle.runner as unknown as { pausedBuffer: Array<{ payload: { text?: string } }> }).pausedBuffer,
        ).toEqual([]);
      });

      it('contains a soft-resume parked Planner delivery failure without an error listener', async () => {
        const runId = 'planner-resumed-parked-delivery-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        await handle.runner.send(AutoloopMsg.pause(0, { reason: 'operator-review' }));

        await expect(mgr.autoloopChat(runId, 'park before failing on resume')).rejects.toMatchObject({
          code: 'AUTOLOOP_RUN_PAUSED',
          retryable: false,
        });
        const phaseErrors: PhaseErrorPayload[] = [];
        const pushes: Array<{ summary: string }> = [];
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown) => unhandled.push(reason);
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));
        handle.runner.on('push', (payload: { summary: string }) => pushes.push(payload));
        process.on('unhandledRejection', onUnhandled);
        expect(handle.runner.listenerCount('error')).toBe(0);
        const delivery = vi
          .spyOn(handle.dispatcher, 'deliver')
          .mockRejectedValueOnce(new Error('resumed parked chat delivery failed'));

        try {
          await expect(handle.runner.send(AutoloopMsg.resume(0))).resolves.toBeUndefined();
          await new Promise<void>((resolve) => nativeSetImmediate(resolve));
          expect(delivery).toHaveBeenCalledTimes(1);
          expect(unhandled).toEqual([]);
          expect(phaseErrors).toEqual([
            {
              agent: 'planner',
              phase: 'planner_turn',
              code: 'AUTOLOOP_ENGINE_FAILURE',
              error: 'Planner engine transport failed: resumed parked chat delivery failed',
            },
          ]);
          expect(pushes.map(({ summary }) => summary)).toEqual(['[on_phase_error] iter 0']);
          expect(handle.runner.state).toMatchObject({
            status: 'running',
            consecutive_phase_errors: 1,
            recent_phase_errors: [expect.objectContaining({ code: 'AUTOLOOP_ENGINE_FAILURE' })],
            push_log_count: 1,
          });
          const pushRows = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'push_log.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { summary: string });
          expect(pushRows).toEqual([expect.objectContaining({ summary: '[on_phase_error] iter 0' })]);
        } finally {
          process.off('unhandledRejection', onUnhandled);
          delivery.mockRestore();
        }
      });

      it('contains a soft-resume Planner failure when its mandatory phase-error notification also fails', async () => {
        const runId = 'planner-soft-resume-notification-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        await handle.runner.send(AutoloopMsg.pause(0, { reason: 'operator-review' }));
        await expect(mgr.autoloopChat(runId, 'park before both failures')).rejects.toMatchObject({
          code: 'AUTOLOOP_RUN_PAUSED',
          retryable: false,
        });
        mockSessions[0].sendImplementation = async () => ({
          text: 'reply whose generation probe will fail',
          event: { type: 'result', result: 'reply whose generation probe will fail' },
        });
        vi.spyOn(mgr, 'inspect').mockRejectedValueOnce(new Error('soft-resume liveness probe failed'));
        handle.runner.config.notifyUser = async () => {
          throw new Error('mandatory Planner phase-error notification failed');
        };

        const originalDeliver = handle.dispatcher.deliver.bind(handle.dispatcher);
        let boundaryFailure: unknown;
        vi.spyOn(handle.dispatcher, 'deliver').mockImplementation(async (env) => {
          try {
            return await originalDeliver(env);
          } catch (error) {
            boundaryFailure = error;
            throw error;
          }
        });
        const phaseErrors: PhaseErrorPayload[] = [];
        const pushes: Array<{ summary: string }> = [];
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown) => unhandled.push(reason);
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));
        handle.runner.on('push', (payload: { summary: string }) => pushes.push(payload));
        process.on('unhandledRejection', onUnhandled);
        expect(handle.runner.listenerCount('error')).toBe(0);

        try {
          await expect(handle.runner.send(AutoloopMsg.resume(0))).resolves.toBeUndefined();
          await new Promise<void>((resolve) => nativeSetImmediate(resolve));

          expect(boundaryFailure).toBeInstanceOf(AutoloopOperationError);
          expect(phaseErrors).toEqual([
            {
              agent: 'planner',
              phase: 'planner_turn',
              code: 'AUTOLOOP_ENGINE_FAILURE',
              error: 'Planner engine transport failed: soft-resume liveness probe failed',
            },
          ]);
          expect(pushes.map(({ summary }) => summary)).toEqual(['[on_phase_error] iter 0']);
          expect(unhandled).toEqual([]);
          expect(handle.runner.state).toMatchObject({
            status: 'running',
            consecutive_phase_errors: 1,
            push_log_count: 1,
          });
          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: PhaseErrorPayload });
          expect(
            decisions.filter(
              (row) =>
                row.kind === 'phase_error' &&
                row.payload.agent === 'planner' &&
                row.payload.code === 'AUTOLOOP_ENGINE_FAILURE',
            ),
          ).toHaveLength(1);
        } finally {
          process.off('unhandledRejection', onUnhandled);
        }
      });

      it('contains a timeout-resume parked Planner delivery failure without an unhandled rejection', async () => {
        const runId = 'planner-timeout-resumed-parked-delivery-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({
          runId,
          workspace,
          sendTimeoutMs: 600_000,
          activityLeaseMs: 1_800_000,
          autoloopHardTimeoutMs: 86_400_000,
        });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => {
          mockSessions[0].sendImplementation = undefined;
          throw new Error('Timeout waiting for response');
        };
        await expect(mgr.autoloopChat(runId, 'establish the timed-out Planner dispatch')).rejects.toMatchObject({
          code: 'AUTOLOOP_SEND_TIMEOUT',
          retryable: true,
        });
        const pending = handle.runner.state.pending_dispatch!;
        await expect(mgr.autoloopChat(runId, 'park after the timed-out dispatch')).rejects.toMatchObject({
          code: 'AUTOLOOP_RUN_PAUSED',
          retryable: false,
        });

        const phaseErrors: PhaseErrorPayload[] = [];
        const pushes: Array<{ summary: string }> = [];
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown) => unhandled.push(reason);
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));
        handle.runner.on('push', (payload: { summary: string }) => pushes.push(payload));
        process.on('unhandledRejection', onUnhandled);
        expect(handle.runner.listenerCount('error')).toBe(0);
        const delivery = vi
          .spyOn(handle.dispatcher, 'deliver')
          .mockRejectedValueOnce(new Error('timeout-resumed parked chat delivery failed'));

        try {
          await expect(
            mgr.autoloopResume(runId, {
              sendTimeoutMs: 700_000,
              pendingDispatchId: pending.dispatch_id,
            }),
          ).resolves.toMatchObject({ status: 'running', pending_dispatch: null });
          await vi.waitFor(() => expect(handle.runner.state.push_log_count).toBe(1));
          await new Promise<void>((resolve) => nativeSetImmediate(resolve));

          expect(delivery).toHaveBeenCalledTimes(1);
          expect(unhandled).toEqual([]);
          expect(phaseErrors).toEqual([
            {
              agent: 'planner',
              phase: 'planner_turn',
              code: 'AUTOLOOP_ENGINE_FAILURE',
              error: 'Planner engine transport failed: timeout-resumed parked chat delivery failed',
            },
          ]);
          expect(pushes.map(({ summary }) => summary)).toEqual(['[on_phase_error] iter 0']);
          expect(handle.runner.state).toMatchObject({
            status: 'running',
            consecutive_phase_errors: 1,
            recent_phase_errors: [expect.objectContaining({ code: 'AUTOLOOP_ENGINE_FAILURE' })],
            push_log_count: 1,
          });
          const pushRows = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'push_log.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { summary: string });
          expect(pushRows).toEqual([expect.objectContaining({ summary: '[on_phase_error] iter 0' })]);
        } finally {
          process.off('unhandledRejection', onUnhandled);
          delivery.mockRestore();
        }
      });

      it('contains a detached timeout-resume failure for a parked non-Planner message without an error listener', async () => {
        const runId = 'coder-timeout-resume-parked-delivery-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({
          runId,
          workspace,
          sendTimeoutMs: 600_000,
          activityLeaseMs: 1_800_000,
          autoloopHardTimeoutMs: 86_400_000,
        });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => {
          throw new Error('Timeout waiting for response');
        };
        await expect(mgr.autoloopChat(runId, 'establish timeout before parking Coder work')).rejects.toMatchObject({
          code: 'AUTOLOOP_SEND_TIMEOUT',
          retryable: true,
        });
        const pending = handle.runner.state.pending_dispatch!;
        await handle.runner.send(
          AutoloopMsg.directive(0, {
            goal: 'park this Coder delivery',
            constraints: [],
            success_criteria: [],
            max_attempts: 1,
          }),
        );

        const originalDeliver = handle.dispatcher.deliver.bind(handle.dispatcher);
        const delivery = vi.spyOn(handle.dispatcher, 'deliver').mockImplementation(async (env) => {
          if (env.to === 'coder') throw new Error('parked Coder delivery failed');
          return await originalDeliver(env);
        });
        const phaseErrors: PhaseErrorPayload[] = [];
        const unhandled: unknown[] = [];
        const onUnhandled = (reason: unknown) => unhandled.push(reason);
        handle.runner.on('phase_error', (payload: PhaseErrorPayload) => phaseErrors.push(payload));
        process.on('unhandledRejection', onUnhandled);
        expect(handle.runner.listenerCount('error')).toBe(0);

        try {
          await expect(
            mgr.autoloopResume(runId, {
              sendTimeoutMs: 700_000,
              pendingDispatchId: pending.dispatch_id,
            }),
          ).resolves.toMatchObject({ status: 'running', pending_dispatch: null });
          await vi.waitFor(() => expect(delivery.mock.calls.some(([env]) => env.to === 'coder')).toBe(true));
          await new Promise<void>((resolve) => nativeSetImmediate(resolve));

          expect(unhandled).toEqual([]);
          expect(phaseErrors).toEqual([]);
          expect(handle.runner.state).toMatchObject({
            status: 'running',
            consecutive_phase_errors: 0,
          });
        } finally {
          process.off('unhandledRejection', onUnhandled);
        }
      });

      it('does not attribute an older send-timeout dispatch to a newly parked chat', async () => {
        const runId = 'planner-chat-behind-timeout';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        let plannerTurns = 0;
        mockSessions[0].sendImplementation = async () => {
          plannerTurns += 1;
          if (plannerTurns === 1) throw Object.assign(new Error('Planner deadline'), { code: 'ETIMEDOUT' });
          return { text: 'exact parked chat delivered', event: { type: 'result', result: 'delivered after resume' } };
        };
        let originalPending: Record<string, unknown> | undefined;
        try {
          await mgr.autoloopChat(runId, 'the dispatch that times out');
        } catch (error) {
          originalPending = (error as { pending_dispatch?: Record<string, unknown> }).pending_dispatch;
        }
        expect(originalPending).toMatchObject({ agent: 'planner', message_type: 'chat' });

        await expect(mgr.autoloopChat(runId, 'a distinct parked chat')).rejects.toMatchObject({
          code: 'AUTOLOOP_RUN_PAUSED',
          retryable: false,
          pending_dispatch: undefined,
        });
        expect(handle.runner.state.pending_dispatch).toEqual(originalPending);
        expect(mockSessions[0].sendCalls).toHaveLength(1);
        const parked = (
          handle.runner as unknown as {
            pausedBuffer: Array<{ msg_id: string; type: string; payload: { text?: string } }>;
          }
        ).pausedBuffer;
        expect(parked).toEqual([
          expect.objectContaining({
            msg_id: expect.stringMatching(/^m_/),
            type: 'chat',
            payload: { text: 'a distinct parked chat' },
          }),
        ]);
        const parkedMessageId = parked[0].msg_id;
        const deliveredEnvelopes: string[] = [];
        handle.runner.on('message', (env: { msg_id?: string }) => {
          if (env.msg_id === parkedMessageId) deliveredEnvelopes.push(env.msg_id);
        });

        await mgr.autoloopResume(runId, {
          sendTimeoutMs: 700_000,
          pendingDispatchId: String(originalPending?.dispatch_id),
        });
        await vi.waitFor(() => expect(mockSessions[0].sendCalls).toHaveLength(2));

        expect(String(mockSessions[0].sendCalls[1].message)).toContain('a distinct parked chat');
        expect((handle.runner as unknown as { pausedBuffer: Array<{ msg_id: string }> }).pausedBuffer).toEqual([]);
        expect(deliveredEnvelopes).toEqual([parkedMessageId]);
        await vi.advanceTimersByTimeAsync(1_000);
        expect(mockSessions[0].sendCalls).toHaveLength(2);
        expect(deliveredEnvelopes).toEqual([parkedMessageId]);
      });

      it('isolates a failed Planner sender from a later queued sender', async () => {
        const runId = 'planner-drain-sender-isolation';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        let releaseFirst!: () => void;
        const firstGate = new Promise<void>((resolve) => {
          releaseFirst = resolve;
        });
        let markFirstEntered!: () => void;
        const firstEntered = new Promise<void>((resolve) => {
          markFirstEntered = resolve;
        });
        let turn = 0;
        mockSessions[0].sendImplementation = async () => {
          turn += 1;
          if (turn === 1) {
            markFirstEntered();
            await firstGate;
            return {
              text: ['```autoloop', '{"tool":"notify_user","args":{}}', '```'].join('\n'),
              event: { type: 'result', result: 'bad first turn' },
            };
          }
          return { text: 'independent second reply', event: { type: 'result', result: 'independent second reply' } };
        };

        const first = handle.runner.send(AutoloopMsg.chat(0, { text: 'failing sender' }));
        await firstEntered;
        const second = handle.runner.send(AutoloopMsg.chat(0, { text: 'independent sender' }));
        releaseFirst();

        await expect(first).rejects.toMatchObject({ code: 'AUTOLOOP_CONTROL_MALFORMED' });
        await expect(second).resolves.toBeUndefined();
        expect(mockSessions[0].sendCalls).toHaveLength(2);
      });

      it.each([
        { label: 'plain reply', output: 'late plain Planner success', control: false },
        {
          label: 'spawn control',
          output: ['late spawn', '```autoloop', '{"tool":"spawn_subagents","args":{}}', '```'].join('\n'),
          control: true,
        },
      ])('fences a late Planner $label after pre-emptive termination', async ({ label, output, control }) => {
        const runId = `planner-terminal-fence-${label.replace(' ', '-')}`;
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        let releaseTurn!: () => void;
        let markTurnEntered!: () => void;
        const turnEntered = new Promise<void>((resolve) => {
          markTurnEntered = resolve;
        });
        const turnGate = new Promise<void>((resolve) => {
          releaseTurn = resolve;
        });
        mockSessions[0].sendImplementation = async () => {
          markTurnEntered();
          await turnGate;
          return { text: output, event: { type: 'result', result: output } };
        };
        let releasePlannerStop!: () => void;
        let markPlannerStopEntered!: () => void;
        const plannerStopEntered = new Promise<void>((resolve) => {
          markPlannerStopEntered = resolve;
        });
        const plannerStopGate = new Promise<void>((resolve) => {
          releasePlannerStop = resolve;
        });
        const stopImplementation = mgr.stopSession.bind(mgr);
        const stop = vi.spyOn(mgr, 'stopSession').mockImplementation(async (name, options) => {
          if (name === handle.dispatcher.sessionNames.planner) {
            markPlannerStopEntered();
            await plannerStopGate;
          }
          return await stopImplementation(name, options);
        });
        const replies: string[] = [];
        handle.dispatcher.on('planner_reply', (reply: string) => replies.push(reply));

        try {
          const chat = mgr.autoloopChat(runId, 'turn racing terminal state');
          await turnEntered;
          const stopping = mgr.autoloopStop(runId, 'operator-terminal-fence');
          await plannerStopEntered;
          expect(handle.runner.state.status).toBe('terminated');
          releaseTurn();

          await expect(chat).rejects.toMatchObject({
            code: 'AUTOLOOP_RUN_TERMINAL',
            retryable: false,
            status_reason: 'operator-terminal-fence',
          });
          releasePlannerStop();
          await expect(stopping).resolves.toBe(true);
          expect(replies).toEqual([]);
          expect(handle.runner.state).toMatchObject({ status: 'terminated', subagents_spawned: false });
          expect(mockSessions).toHaveLength(1);
          const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
          const decisions = fs
            .readFileSync(decisionsPath, 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string });
          expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toHaveLength(0);
          if (control) expect(decisions.filter((row) => row.kind === 'spawn_subagents')).toHaveLength(0);
        } finally {
          releasePlannerStop();
          stop.mockRestore();
        }
      });

      it('returns a structured internal failure when Planner reset omits force', async () => {
        const runId = 'planner-reset-force-required';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });

        await expect(mgr.getAutoloop(runId)!.dispatcher.resetAgent('planner')).resolves.toMatchObject({
          ok: false,
          code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
          agent: 'planner',
          retryable: false,
          message: expect.stringContaining('force=true'),
        });
      });

      it.each(['coder', 'reviewer'] as const)(
        'does not retain a fatal rejected one-shot %s send in replay history or the next prompt',
        async (role) => {
          const runId = `fatal-${role}-history-exclusion`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          if (role === 'coder') initializeGitWorkspace(workspace);
          await mgr.autoloopStart({
            runId,
            workspace,
            ...(role === 'coder' ? { coderEngine: 'cursor' as const } : { reviewerEngine: 'cursor' as const }),
          });
          const handle = mgr.getAutoloop(runId)!;
          await handle.dispatcher.spawnSubagents();
          const roleIndex = role === 'coder' ? 1 : 2;
          const rejectedMarker = role === 'coder' ? 'FATAL_CODER_TURN_MUST_NOT_REPLAY' : '987654321';
          mockSessions[roleIndex].sendImplementation = async () => {
            throw new Error(`${role} fatal turn`);
          };
          const reset = vi.spyOn(handle.dispatcher, 'resetAgent').mockResolvedValue({
            ok: false,
            code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
            agent: role,
            previous_generation: 1,
            message: `${role} cannot recover`,
            retryable: false,
          });

          if (role === 'coder') {
            await handle.dispatcher.deliver(
              AutoloopMsg.directive(0, {
                goal: rejectedMarker,
                constraints: [],
                success_criteria: [],
                max_attempts: 1,
              }),
            );
          } else {
            seedCompleteLegacyReviewArtifacts(workspace, runId, 0);
            await handle.dispatcher.deliver(
              AutoloopMsg.reviewRequest(0, {
                iter: 0,
                ledger_path: path.join(workspace, 'tasks', runId),
                prior_metrics: [Number(rejectedMarker)],
              }),
            );
          }

          expect(
            (
              handle.dispatcher as unknown as {
                transcripts: Record<'coder' | 'reviewer', Array<{ who: string; text: string }>>;
              }
            ).transcripts[role],
          ).toEqual([]);

          mockSessions[roleIndex].sendImplementation = async (message) => {
            const text = successfulRoleReplyFromDeliveryPrompt(
              role,
              message,
              role === 'coder' ? 'next coder reply' : 'next reviewer reply',
            );
            return { text, event: { type: 'result', result: text } };
          };
          if (role === 'coder') {
            await handle.dispatcher.deliver(
              AutoloopMsg.directive(1, {
                goal: 'accepted next coder turn',
                constraints: [],
                success_criteria: [],
                max_attempts: 1,
              }),
            );
          } else {
            seedCompleteLegacyReviewArtifacts(workspace, runId, 1);
            await handle.dispatcher.deliver(
              AutoloopMsg.reviewRequest(1, {
                iter: 1,
                ledger_path: path.join(workspace, 'tasks', runId),
                prior_metrics: [123],
              }),
            );
          }

          expect(String(mockSessions[roleIndex].sendCalls.at(-1)?.message)).not.toContain(rejectedMarker);
          reset.mockRestore();
        },
      );

      it.each(['coder', 'reviewer'] as const)(
        'excludes a fatal %s one-shot after a real successful reset and failed retry',
        async (role) => {
          const runId = `real-reset-fatal-${role}-history-exclusion`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          if (role === 'coder') initializeGitWorkspace(workspace);
          let targetGenerationsStarted = 0;
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (mgr as any)._createSession = (_engine: string, config: SessionConfig): ISession => {
            const mock = new MockSession();
            if (config.name === `autoloop-${runId}-${role}`) {
              targetGenerationsStarted += 1;
              if (targetGenerationsStarted === 1) {
                mock.sendImplementation = async () => {
                  throw new Error(`${role} first generation failed`);
                };
              } else {
                let replacementTurns = 0;
                mock.sendImplementation = async (message) => {
                  replacementTurns += 1;
                  if (replacementTurns === 1) throw new Error(`${role} replacement retry failed`);
                  const text = successfulRoleReplyFromDeliveryPrompt(role, message, `${role} later success`);
                  return { text, event: { type: 'result', result: text } };
                };
              }
            }
            mockSessions.push(mock);
            createdConfigs.push(config);
            return mock;
          };
          await mgr.autoloopStart({
            runId,
            workspace,
            ...(role === 'coder' ? { coderEngine: 'cursor' as const } : { reviewerEngine: 'cursor' as const }),
          });
          const handle = mgr.getAutoloop(runId)!;
          await handle.dispatcher.spawnSubagents();
          const rejectedMarker = role === 'coder' ? 'REAL_RESET_CODER_MUST_NOT_REPLAY' : '135791113';
          const rejectedEnvelope =
            role === 'coder'
              ? AutoloopMsg.directive(0, {
                  goal: rejectedMarker,
                  constraints: [],
                  success_criteria: [],
                  max_attempts: 1,
                })
              : AutoloopMsg.reviewRequest(0, {
                  iter: 0,
                  ledger_path: path.join(workspace, 'tasks', runId),
                  prior_metrics: [Number(rejectedMarker)],
                });

          if (role === 'reviewer') seedCompleteLegacyReviewArtifacts(workspace, runId, 0);

          const rejected = handle.dispatcher.deliver(rejectedEnvelope);
          await vi.advanceTimersByTimeAsync(1_000);
          await expect(rejected).resolves.toEqual([
            expect.objectContaining({
              type: 'phase_error',
              payload: expect.objectContaining({ agent: role, code: 'AUTOLOOP_ENGINE_FAILURE' }),
            }),
          ]);

          expect(targetGenerationsStarted).toBe(2);
          expect(
            (
              handle.dispatcher as unknown as {
                transcripts: Record<'coder' | 'reviewer', Array<{ who: string; text: string }>>;
              }
            ).transcripts[role],
          ).toEqual([]);
          const generations = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'agent-generations.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: { role?: string; generation?: number } });
          expect(
            generations.filter(
              (row) =>
                row.payload.role === role && row.kind === 'agent_generation_started' && row.payload.generation === 2,
            ),
          ).toHaveLength(1);

          const acceptedEnvelope =
            role === 'coder'
              ? AutoloopMsg.directive(1, {
                  goal: 'accepted coder turn',
                  constraints: [],
                  success_criteria: [],
                  max_attempts: 1,
                })
              : AutoloopMsg.reviewRequest(1, {
                  iter: 1,
                  ledger_path: path.join(workspace, 'tasks', runId),
                  prior_metrics: [2468],
                });
          if (role === 'reviewer') seedCompleteLegacyReviewArtifacts(workspace, runId, 1);
          await handle.dispatcher.deliver(acceptedEnvelope);
          const replacementIndex = createdConfigs.map((config) => config.name).lastIndexOf(`autoloop-${runId}-${role}`);
          const replacementSession = mockSessions[replacementIndex];
          expect(String(replacementSession?.sendCalls.at(-1)?.message)).not.toContain(rejectedMarker);
        },
      );

      it.each([
        { key: 'on_phase_error' as const, channel: 'wechat' as const },
        { key: 'on_phase_error' as const, channel: 'webchat' as const },
        { key: 'on_phase_error' as const, channel: 'email' as const },
        { key: 'on_decision_needed' as const, channel: 'wechat' as const },
        { key: 'on_decision_needed' as const, channel: 'webchat' as const },
        { key: 'on_decision_needed' as const, channel: 'email' as const },
      ])(
        'rejects $key diversion to the $channel channel with the fallback-chain diagnostic',
        async ({ key, channel }) => {
          const runId = `planner-critical-policy-channel-${key}-${channel}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const before = JSON.stringify(handle.runner.config.push_policy);
          mockSessions[0].sendImplementation = async () => ({
            text: [
              '```autoloop',
              JSON.stringify({ tool: 'update_push_policy', args: { [key]: { channel } } }),
              '```',
            ].join('\n'),
            event: { type: 'result', result: 'unsafe critical channel update' },
          });

          await expect(mgr.autoloopChat(runId, 'do not divert the critical channel')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_MALFORMED',
            retryable: false,
            message: expect.stringContaining(
              `update_push_policy ${key} channel '${channel}' bypasses the required fallback chain`,
            ),
          });
          expect(JSON.stringify(handle.runner.config.push_policy)).toBe(before);
        },
      );

      it.each([
        {
          key: 'on_phase_error' as const,
          state: 'missing',
          legacyRule: null,
          expectedLevel: 'error',
          expectedChannel: 'both',
        },
        {
          key: 'on_decision_needed' as const,
          state: 'missing',
          legacyRule: null,
          expectedLevel: 'decision',
          expectedChannel: 'both',
        },
        {
          key: 'on_phase_error' as const,
          state: 'weak-info',
          legacyRule: { level: 'info', channel: 'both' },
          expectedLevel: 'error',
          expectedChannel: 'both',
        },
        {
          key: 'on_decision_needed' as const,
          state: 'weak-warn',
          legacyRule: { level: 'warn', channel: 'both' },
          expectedLevel: 'decision',
          expectedChannel: 'both',
        },
        ...(['wechat', 'webchat', 'email'] as const).flatMap((channel) => [
          {
            key: 'on_phase_error' as const,
            state: `unsafe-${channel}`,
            legacyRule: { level: 'error' as const, channel },
            expectedLevel: 'error' as const,
            expectedChannel: 'both' as const,
          },
          {
            key: 'on_decision_needed' as const,
            state: `unsafe-${channel}`,
            legacyRule: { level: 'decision' as const, channel },
            expectedLevel: 'decision' as const,
            expectedChannel: 'both' as const,
          },
        ]),
        {
          key: 'on_phase_error' as const,
          state: 'silent-partial',
          legacyRule: { silent: true },
          expectedLevel: 'error',
          expectedChannel: 'both',
        },
        {
          key: 'on_decision_needed' as const,
          state: 'silent-partial',
          legacyRule: { silent: true },
          expectedLevel: 'decision',
          expectedChannel: 'both',
        },
        {
          key: 'on_decision_needed' as const,
          state: 'stronger-error',
          legacyRule: { level: 'error', channel: 'auto', silent: true },
          expectedLevel: 'error',
          expectedChannel: 'auto',
        },
      ])(
        'emits a safe critical policy final emission for $key with a $state legacy rule',
        async ({ key, state, legacyRule, expectedLevel, expectedChannel }) => {
          const runId = `planner-critical-policy-emission-${key}-${state}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const policy = handle.runner.config.push_policy as unknown as Record<string, unknown>;
          if (legacyRule === null) delete policy[key];
          else policy[key] = legacyRule;
          const pushes: Array<{ level: string; summary: string; channel: string }> = [];
          handle.runner.on('push', (payload: { level: string; summary: string; channel: string }) =>
            pushes.push(payload),
          );

          await (
            handle.runner as unknown as {
              firePolicyPush(rule: typeof key, iter: number): Promise<void>;
            }
          ).firePolicyPush(key, 7);

          expect(pushes).toEqual([{ level: expectedLevel, summary: `[${key}] iter 7`, channel: expectedChannel }]);
        },
      );

      it('does not let an ordinary push deduplicate a mandatory critical policy emission', async () => {
        const runId = 'planner-critical-policy-dedup-origin';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const pushes: Array<{ level: string; summary: string; channel: string }> = [];
        handle.runner.on('push', (payload: { level: string; summary: string; channel: string }) =>
          pushes.push(payload),
        );

        await handle.runner.send(
          AutoloopMsg.pushUser(7, {
            level: 'error',
            summary: '[on_phase_error] iter 7',
            channel: 'auto',
          }),
        );
        await (
          handle.runner as unknown as {
            firePolicyPush(rule: 'on_phase_error', iter: number): Promise<void>;
          }
        ).firePolicyPush('on_phase_error', 7);

        expect(pushes).toEqual([
          { level: 'error', summary: '[on_phase_error] iter 7', channel: 'auto' },
          { level: 'error', summary: '[on_phase_error] iter 7', channel: 'both' },
        ]);
        expect(handle.runner.state.push_log_count).toBe(2);
      });

      it.each([
        {
          key: 'on_start' as const,
          delta: { level: 'warn' as const },
          expected: { level: 'warn', channel: 'wechat' },
        },
        {
          key: 'on_iter_done_ok' as const,
          delta: { channel: 'auto' as const },
          expected: { channel: 'auto', silent: true },
        },
      ])('merges a noncritical $key PATCH with its current policy rule', async ({ key, delta, expected }) => {
        const runId = `planner-noncritical-policy-patch-${key}`;
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', JSON.stringify({ tool: 'update_push_policy', args: { [key]: delta } }), '```'].join(
            '\n',
          ),
          event: { type: 'result', result: 'partial noncritical update' },
        });

        await expect(mgr.autoloopChat(runId, 'patch only the supplied policy fields')).resolves.toMatchObject({
          reply: expect.stringContaining('update_push_policy'),
        });

        expect(handle.runner.config.push_policy?.[key]).toEqual(expected);
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { applied?: Record<string, unknown> } });
        expect(decisions.find((row) => row.kind === 'update_push_policy')?.payload.applied).toEqual({
          [key]: expected,
        });
      });

      it.each(['on_phase_error', 'on_decision_needed'] as const)(
        'repairs a legacy non-fallback $key channel during an allowed partial update',
        async (key) => {
          const runId = `planner-critical-policy-defensive-${key}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const requiredLevel = key === 'on_phase_error' ? 'error' : 'decision';
          handle.runner.config.push_policy![key] = { level: requiredLevel, channel: 'webchat' };
          mockSessions[0].sendImplementation = async () => ({
            text: [
              '```autoloop',
              JSON.stringify({ tool: 'update_push_policy', args: { [key]: { level: requiredLevel } } }),
              '```',
            ].join('\n'),
            event: { type: 'result', result: 'repair legacy critical channel' },
          });

          await expect(mgr.autoloopChat(runId, 'retain a fallback-capable channel')).resolves.toMatchObject({
            reply: expect.stringContaining('update_push_policy'),
          });
          expect(handle.runner.config.push_policy?.[key]).toEqual({ level: requiredLevel, channel: 'both' });
        },
      );

      it('preserves critical push-policy severity across an allowed fallback-capable partial update', async () => {
        const runId = 'planner-critical-policy-partial-update';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            '{"tool":"update_push_policy","args":{"on_phase_error":{"channel":"auto"}}}',
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'partial critical update' },
        });

        await expect(mgr.autoloopChat(runId, 'narrow only the channel')).resolves.toMatchObject({
          reply: expect.stringContaining('update_push_policy'),
        });
        expect(handle.runner.config.push_policy?.on_phase_error).toEqual({ level: 'error', channel: 'auto' });
      });

      it('rejects a weakening critical push-policy severity atomically', async () => {
        const runId = 'planner-critical-policy-weakening';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const before = JSON.stringify(handle.runner.config.push_policy);
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            '{"tool":"update_push_policy","args":{"on_decision_needed":{"level":"info"}}}',
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'weaken critical severity' },
        });

        await expect(mgr.autoloopChat(runId, 'reject the weakening update')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
        });
        expect(JSON.stringify(handle.runner.config.push_policy)).toBe(before);
      });

      it.each(['on_phase_error', 'on_decision_needed'] as const)(
        'refuses prose plus a silence-only $key attempt without policy or successful-control audit mutation',
        async (key) => {
          const runId = `planner-critical-silence-with-text-${key}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const policyBefore = JSON.stringify(handle.runner.config.push_policy);
          mockSessions[0].sendImplementation = async () => ({
            text: [
              'I silenced the critical notification.',
              '```autoloop',
              JSON.stringify({ tool: 'update_push_policy', args: { [key]: { silent: true } } }),
              '```',
            ].join('\n'),
            event: { type: 'result', result: 'silence critical policy' },
          });

          await expect(mgr.autoloopChat(runId, 'do not accept prose as a control result')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_MALFORMED',
            retryable: false,
          });

          expect(JSON.stringify(handle.runner.config.push_policy)).toBe(policyBefore);
          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: { keys?: string[] } });
          expect(decisions.filter((row) => row.kind === 'planner_turn_control')).toEqual([]);
          expect(decisions.filter((row) => row.kind === 'update_push_policy')).toEqual([]);
          expect(decisions.filter((row) => row.kind === 'policy_silence_blocked')).toEqual([]);
        },
      );

      it('retains a valid non-weakening control beside a blocked critical silence attempt', async () => {
        const runId = 'planner-critical-silence-mixed-batch';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            '{"tool":"update_push_policy","args":{"on_phase_error":{"silent":true}}}',
            '```',
            '```autoloop',
            '{"tool":"update_push_policy","args":{"on_start":{"level":"warn"}}}',
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'mixed policy controls' },
        });

        await expect(mgr.autoloopChat(runId, 'apply only the valid policy control')).resolves.toMatchObject({
          reply: expect.stringContaining('update_push_policy'),
        });
        expect(handle.runner.config.push_policy?.on_phase_error).toEqual({ level: 'error', channel: 'both' });
        expect(handle.runner.config.push_policy?.on_start).toEqual({ level: 'warn', channel: 'wechat' });
      });

      it('rejects an oversized artifact batch before materializing every large body', () => {
        const body = 'x'.repeat(1_048_576);
        const reads: number[] = [];
        const controls = Array.from({ length: 8 }, (_, index) => {
          const args: Record<string, unknown> = {};
          Object.defineProperty(args, 'content', {
            enumerable: true,
            get: () => {
              reads.push(index);
              return body;
            },
          });
          return { tool: 'write_plan' as const, args };
        });

        const validation = validatePlannerToolCalls(controls);

        expect(validation.calls).toEqual([]);
        expect(validation.errors[0]?.error).toContain('1114112-byte UTF-8 limit');
        expect(reads).toEqual([0, 1]);
      });

      it('applies an already validated batch through the trusted path without a second boundary pass', async () => {
        const validation = validatePlannerToolCalls([{ tool: 'notify_user', args: { summary: 'one effect' } }]);
        const updatePushPolicy = vi.fn();

        const result = await applyValidatedPlannerToolCalls(
          validation,
          {
            spawnSubagents: async () => undefined,
            updatePushPolicy,
            writePlanFile: async () => undefined,
          },
          0,
        );

        expect(result.errors).toEqual([]);
        expect(result.emitted_messages).toEqual([
          expect.objectContaining({ type: 'push_user', payload: expect.objectContaining({ summary: 'one effect' }) }),
        ]);
        expect(updatePushPolicy).not.toHaveBeenCalled();
      });

      it('materializes deterministic effect defaults in the canonical validated controls', () => {
        const validation = validatePlannerToolCalls([
          { tool: 'notify_user', args: { summary: 'default notification' } },
          { tool: 'send_directive', args: { goal: 'default directive' } },
          {
            tool: 'spawn_subagents',
            args: { initial_directive: { goal: 'default initial directive' } },
          },
          { tool: 'write_plan', args: { content: '# Canonical plan' } },
          { tool: 'write_goal', args: { content: '{"gates":[]}' } },
        ]);

        expect(validation.errors).toEqual([]);
        expect(validation.calls).toEqual([
          {
            tool: 'notify_user',
            args: { channel: 'auto', level: 'info', summary: 'default notification' },
          },
          {
            tool: 'send_directive',
            args: { constraints: [], goal: 'default directive', max_attempts: 1, success_criteria: [] },
          },
          {
            tool: 'spawn_subagents',
            args: {
              initial_directive: {
                constraints: [],
                goal: 'default initial directive',
                max_attempts: 1,
                success_criteria: [],
              },
            },
          },
          {
            tool: 'write_plan',
            args: { commit_message: 'autoloop: planner writes plan.md', content: '# Canonical plan' },
          },
          {
            tool: 'write_goal',
            args: { commit_message: 'autoloop: planner writes goal.json', content: '{"gates":[]}' },
          },
        ]);
        expect(JSON.parse(validation.controls_json ?? 'null')).toEqual(validation.calls);
      });

      it('persists the same omitted defaults that real Planner chat emits and applies', async () => {
        const runId = 'planner-durable-default-effect-equality';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        initializeGitWorkspace(workspace);
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const expectedControls: PlannerToolCall[] = [
          {
            tool: 'notify_user',
            args: { channel: 'auto', level: 'info', summary: 'defaulted notification' },
          },
          {
            tool: 'spawn_subagents',
            args: {},
          },
          {
            tool: 'send_directive',
            args: {
              constraints: [],
              goal: 'defaulted follow-up directive',
              max_attempts: 1,
              success_criteria: [],
            },
          },
        ];
        let plannerTurn = 0;
        mockSessions[0].sendImplementation = async () => {
          plannerTurn += 1;
          const text =
            plannerTurn === 1
              ? [
                  '```autoloop',
                  '{"tool":"notify_user","args":{"summary":"defaulted notification"}}',
                  '```',
                  '```autoloop',
                  '{"tool":"spawn_subagents","args":{}}',
                  '```',
                  '```autoloop',
                  '{"tool":"send_directive","args":{"goal":"defaulted follow-up directive"}}',
                  '```',
                ].join('\n')
              : 'follow-up acknowledgement';
          return { text, event: { type: 'result', result: text } };
        };
        const pushes: Array<Record<string, unknown>> = [];
        const directives: Array<Record<string, unknown>> = [];
        handle.runner.on('push', (payload: Record<string, unknown>) => pushes.push(payload));
        handle.runner.on('message', (message: { type?: string; from?: string; payload?: Record<string, unknown> }) => {
          if (message.type === 'directive' && message.from === 'planner' && message.payload) {
            directives.push(message.payload);
          }
        });
        const spawnImplementation = handle.dispatcher.spawnSubagents.bind(handle.dispatcher);
        let spawnEffect: unknown;
        const spawn = vi.spyOn(handle.dispatcher, 'spawnSubagents').mockImplementation(async (args) => {
          spawnEffect = JSON.parse(JSON.stringify(args));
          await spawnImplementation(args);
          mockSessions[1].sendImplementation = async (message) => {
            const text = successfulRoleReplyFromDeliveryPrompt('coder', message, 'follow-up acknowledgement');
            return { text, event: { type: 'result', result: text } };
          };
          mockSessions[2].sendImplementation = async (message) => {
            const text = successfulRoleReplyFromDeliveryPrompt('reviewer', message, 'follow-up review');
            return { text, event: { type: 'result', result: text } };
          };
        });

        try {
          await expect(mgr.autoloopChat(runId, 'apply every deterministic default')).resolves.toMatchObject({
            reply: expect.stringContaining('notify_user'),
          });
          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: { controls?: PlannerToolCall[] } });
          expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload.controls).toEqual(
            expectedControls,
          );
          expect(spawnEffect).toEqual(expectedControls[1].args);
          expect(pushes).toEqual([
            { level: 'info', summary: 'defaulted notification', detail: undefined, channel: 'auto' },
          ]);
          expect(directives).toEqual([expectedControls[2].args]);
        } finally {
          spawn.mockRestore();
        }
      });

      it('persists omitted artifact commit defaults identical to the real git effects', async () => {
        const runId = 'planner-durable-artifact-commit-defaults';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        initializeGitWorkspace(workspace);
        await mgr.autoloopStart({ runId, workspace });
        const planContent = '# Durable default plan';
        const goalContent = '{"gates":[]}';
        mockSessions[0].sendImplementation = async () => ({
          text: [
            '```autoloop',
            JSON.stringify({ tool: 'write_plan', args: { content: planContent } }),
            '```',
            '```autoloop',
            JSON.stringify({ tool: 'write_goal', args: { content: goalContent } }),
            '```',
          ].join('\n'),
          event: { type: 'result', result: 'persist artifact defaults' },
        });

        await expect(mgr.autoloopChat(runId, 'persist deterministic artifact defaults')).resolves.toMatchObject({
          reply: expect.stringContaining('write_plan, write_goal'),
        });

        const expectedControls: PlannerToolCall[] = [
          {
            tool: 'write_plan',
            args: {
              commit_message: 'autoloop: planner writes plan.md',
              content: planContent,
            },
          },
          {
            tool: 'write_goal',
            args: {
              commit_message: 'autoloop: planner writes goal.json',
              content: goalContent,
            },
          },
        ];
        const decisions = fs
          .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line) as { kind: string; payload: { controls?: PlannerToolCall[] } });
        expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload.controls).toEqual(
          expectedControls,
        );
        expect(runGit(workspace, 'log', '-2', '--format=%s').trim().split('\n')).toEqual([
          String(expectedControls[1].args.commit_message),
          String(expectedControls[0].args.commit_message),
        ]);
      });

      it.each(['pause_loop', 'terminate'] as const)(
        'persists and applies the canonical omitted %s reason through Planner chat',
        async (tool) => {
          const runId = `planner-canonical-default-${tool}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          mockSessions[0].sendImplementation = async () => ({
            text: ['```autoloop', JSON.stringify({ tool, args: {} }), '```'].join('\n'),
            event: { type: 'result', result: 'default lifecycle reason' },
          });

          await expect(mgr.autoloopChat(runId, 'use the deterministic lifecycle default')).resolves.toEqual({
            reply: `Planner controls persisted: ${tool}`,
          });

          const reason = tool === 'pause_loop' ? 'planner-pause' : 'planner-terminate';
          expect(handle.runner.state).toMatchObject({
            status: tool === 'pause_loop' ? 'paused' : 'terminated',
            status_reason: reason,
          });
          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload: { controls?: PlannerToolCall[] } });
          expect(decisions.find((row) => row.kind === 'planner_turn_control')?.payload.controls).toEqual([
            { tool, args: { reason } },
          ]);
        },
      );

      it.each(['pause_loop', 'terminate'] as const)(
        'rejects a supplied blank %s reason at the complete Planner chat boundary',
        async (tool) => {
          const runId = `planner-blank-chat-reason-${tool}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          mockSessions[0].sendImplementation = async () => ({
            text: ['```autoloop', JSON.stringify({ tool, args: { reason: '   ' } }), '```'].join('\n'),
            event: { type: 'result', result: 'blank lifecycle reason' },
          });

          await expect(mgr.autoloopChat(runId, 'reject the blank reason')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_MALFORMED',
            retryable: false,
            message: expect.stringContaining(`${tool} reason must be a non-empty string`),
          });
          expect(handle.runner.state).toMatchObject({ status: 'planning', status_reason: null });
          const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
          const decisions = fs.existsSync(decisionsPath) ? fs.readFileSync(decisionsPath, 'utf8') : '';
          expect(decisions).not.toContain('planner_turn_control');
        },
      );

      it.each(['pause_loop', 'terminate'] as const)(
        'rejects a non-final %s control before any later direct effect or durable control event',
        async (tool) => {
          const runId = `planner-lifecycle-order-${tool}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const policyBefore = JSON.stringify(handle.runner.config.push_policy);
          mockSessions[0].sendImplementation = async () => ({
            text: [
              '```autoloop',
              JSON.stringify({ tool, args: {} }),
              '```',
              '```autoloop',
              '{"tool":"update_push_policy","args":{"on_start":{"level":"warn"}}}',
              '```',
            ].join('\n'),
            event: { type: 'result', result: 'invalid lifecycle ordering' },
          });

          await expect(mgr.autoloopChat(runId, 'reject effects after lifecycle control')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_MALFORMED',
            retryable: false,
            message: expect.stringContaining(`${tool} must be the final Planner control in its batch`),
          });
          expect(handle.runner.state).toMatchObject({ status: 'planning', status_reason: null });
          expect(JSON.stringify(handle.runner.config.push_policy)).toBe(policyBefore);
          const decisionsPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
          const decisions = fs.existsSync(decisionsPath) ? fs.readFileSync(decisionsPath, 'utf8') : '';
          expect(decisions).not.toContain('planner_turn_control');
          expect(decisions).not.toContain('update_push_policy');
        },
      );

      it.each(['pause_loop', 'terminate'] as const)(
        'rejects a supplied blank %s reason while preserving the omitted default',
        async (tool) => {
          const blank = validatePlannerToolCalls([{ tool, args: { reason: '   ' } }]);
          expect(blank.calls).toEqual([]);
          expect(blank.errors).toEqual([
            expect.objectContaining({ tool, error: expect.stringContaining('must be a non-empty string') }),
          ]);

          const omitted = validatePlannerToolCalls([{ tool, args: {} }]);
          expect(omitted.errors).toEqual([]);
          const applied = await applyValidatedPlannerToolCalls(
            omitted,
            {
              spawnSubagents: async () => undefined,
              updatePushPolicy: () => undefined,
              writePlanFile: async () => undefined,
            },
            0,
          );
          expect(applied.errors).toEqual([]);
          expect(applied.emitted_messages).toEqual([
            expect.objectContaining({
              type: tool === 'pause_loop' ? 'pause' : 'terminate',
              payload: { reason: tool === 'pause_loop' ? 'planner-pause' : 'planner-terminate' },
            }),
          ]);
        },
      );

      it.each([
        { tool: 'write_plan' as const, file: 'plan.md', content: '# blank commit message' },
        { tool: 'write_goal' as const, file: 'goal.json', content: '{"blank":true}' },
      ])('rejects a blank $tool commit_message before writing $file', async ({ tool, file, content }) => {
        const runId = `planner-blank-commit-${tool}`;
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        initializeGitWorkspace(workspace);
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', JSON.stringify({ tool, args: { content, commit_message: '   ' } }), '```'].join('\n'),
          event: { type: 'result', result: 'blank commit message' },
        });

        await expect(mgr.autoloopChat(runId, 'reject the blank commit message')).rejects.toMatchObject({
          code: 'AUTOLOOP_CONTROL_MALFORMED',
          retryable: false,
          message: expect.stringContaining(`${tool} commit_message must be a non-empty string`),
        });
        expect(fs.existsSync(path.join(workspace, file))).toBe(false);
      });

      it.each([
        { tool: 'write_plan' as const, file: 'plan.md' as const, content: '# must not be written' },
        { tool: 'write_goal' as const, file: 'goal.json' as const, content: '{"must_not":"be written"}' },
      ])(
        'aborts $tool after termination begins during an earlier persisted spawn effect',
        async ({ tool, file, content }) => {
          const runId = `planner-terminal-batch-${tool}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          const originalHead = initializeGitWorkspace(workspace);
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          handle.dispatcher.config.onSpawnSubagents = async () => {
            await handle.runner.send(AutoloopMsg.terminate(0, { reason: 'terminal-during-spawn-effect' }));
          };
          mockSessions[0].sendImplementation = async () => ({
            text: [
              '```autoloop',
              '{"tool":"spawn_subagents","args":{}}',
              '```',
              '```autoloop',
              JSON.stringify({ tool, args: { content } }),
              '```',
            ].join('\n'),
            event: { type: 'result', result: 'persisted multi-control batch' },
          });

          await expect(mgr.autoloopChat(runId, 'terminate between persisted effects')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
            retryable: false,
          });
          expect(handle.runner.state).toMatchObject({
            status: 'terminated',
            status_reason: 'terminal-during-spawn-effect',
          });
          expect(fs.existsSync(path.join(workspace, file))).toBe(false);
          expect(runGit(workspace, 'rev-parse', 'HEAD').trim()).toBe(originalHead);

          const decisions = fs
            .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line) as { kind: string; payload?: { tools?: string[] } });
          expect(decisions).toContainEqual(
            expect.objectContaining({
              kind: 'planner_turn_control',
              payload: expect.objectContaining({ tools: ['spawn_subagents', tool] }),
            }),
          );
        },
      );

      it('commits only the exact Planner control artifact and leaves unrelated dirty state untouched', async () => {
        const runId = 'planner-exact-control-commit-scope';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        fs.mkdirSync(workspace, { recursive: true });
        initializeGitWorkspace(workspace);
        fs.writeFileSync(path.join(workspace, 'dirty.txt'), 'baseline dirty\n');
        fs.writeFileSync(path.join(workspace, 'staged.txt'), 'baseline staged\n');
        runGit(workspace, 'add', '--', 'dirty.txt', 'staged.txt');
        runGit(workspace, 'commit', '--quiet', '-m', 'unrelated baselines');
        fs.writeFileSync(path.join(workspace, 'dirty.txt'), 'unstaged user change\n');
        fs.writeFileSync(path.join(workspace, 'staged.txt'), 'staged user change\n');
        runGit(workspace, 'add', '--', 'staged.txt');
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"write_plan","args":{"content":"# Exact plan"}}', '```'].join('\n'),
          event: { type: 'result', result: 'write exact plan' },
        });

        await expect(mgr.autoloopChat(runId, 'commit only plan.md')).resolves.toMatchObject({
          reply: expect.stringContaining('write_plan'),
        });

        expect(
          runGit(workspace, 'show', '--pretty=format:', '--name-only', 'HEAD').trim().split('\n').filter(Boolean),
        ).toEqual(['plan.md']);
        expect(runGit(workspace, 'diff', '--name-only')).toContain('dirty.txt');
        expect(runGit(workspace, 'diff', '--cached', '--name-only')).toContain('staged.txt');
        expect(runGit(workspace, 'status', '--porcelain', '--', 'tasks')).toContain('?? tasks/');
      });

      it.each(['status', 'add', 'commit'] as const)(
        'surfaces a Planner git %s failure instead of claiming control success',
        async (failedStep) => {
          const runId = `planner-git-${failedStep}-failure`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          initializeGitWorkspace(workspace);
          await mgr.autoloopStart({ runId, workspace });
          if (failedStep === 'status') {
            fs.writeFileSync(path.join(workspace, '.git', 'index'), 'corrupt index');
          } else if (failedStep === 'add') {
            fs.writeFileSync(path.join(workspace, '.git', 'index.lock'), 'locked');
          } else {
            const hook = path.join(workspace, '.git', 'hooks', 'pre-commit');
            fs.writeFileSync(hook, '#!/bin/sh\necho intentional commit failure >&2\nexit 1\n');
            fs.chmodSync(hook, 0o755);
          }
          mockSessions[0].sendImplementation = async () => ({
            text: ['```autoloop', '{"tool":"write_plan","args":{"content":"# Failing plan"}}', '```'].join('\n'),
            event: { type: 'result', result: 'git operation should fail' },
          });

          await expect(mgr.autoloopChat(runId, `surface ${failedStep} failure`)).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
            retryable: false,
            message: expect.stringContaining(`git ${failedStep}`),
          });
        },
      );

      it.each([
        { tool: 'write_plan' as const, file: 'plan.md', content: '# unsafe overwrite' },
        { tool: 'write_goal' as const, file: 'goal.json', content: '{"unsafe":true}' },
      ])(
        'refuses a pre-planted $file symlink without touching its external target',
        async ({ tool, file, content }) => {
          const runId = `planner-${tool}-symlink-refusal`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          const external = path.join(TEST_WF_DIR, `${runId}-external.txt`);
          fs.writeFileSync(external, 'external sentinel');
          fs.symlinkSync(external, path.join(workspace, file));
          await mgr.autoloopStart({ runId, workspace });
          mockSessions[0].sendImplementation = async () => ({
            text: ['```autoloop', JSON.stringify({ tool, args: { content } }), '```'].join('\n'),
            event: { type: 'result', result: 'write through symlink' },
          });

          await expect(mgr.autoloopChat(runId, 'persist safely')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
          });
          expect(fs.readFileSync(external, 'utf8')).toBe('external sentinel');
          expect(fs.lstatSync(path.join(workspace, file)).isSymbolicLink()).toBe(true);
        },
      );

      it.each(['absent', 'staged'] as const)(
        'restores the exact $prior Planner artifact index state after a failed commit',
        async (prior) => {
          const runId = `planner-index-restore-${prior}`;
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          initializeGitWorkspace(workspace);
          fs.writeFileSync(path.join(workspace, 'dirty.txt'), 'dirty baseline\n');
          fs.writeFileSync(path.join(workspace, 'staged.txt'), 'staged baseline\n');
          runGit(workspace, 'add', '--', 'dirty.txt', 'staged.txt');
          runGit(workspace, 'commit', '--quiet', '-m', 'unrelated baselines');
          fs.writeFileSync(path.join(workspace, 'dirty.txt'), 'unrelated unstaged change\n');
          fs.writeFileSync(path.join(workspace, 'staged.txt'), 'unrelated staged change\n');
          runGit(workspace, 'add', '--', 'staged.txt');
          if (prior === 'staged') {
            fs.writeFileSync(path.join(workspace, 'plan.md'), '# prior staged plan\n');
            runGit(workspace, 'add', '--', 'plan.md');
          }
          const artifactIndexBefore = runGit(workspace, 'ls-files', '--stage', '--', 'plan.md');
          const unrelatedIndexBefore = runGit(workspace, 'diff', '--cached', '--', 'staged.txt');
          const unrelatedWorktreeBefore = runGit(workspace, 'diff', '--', 'dirty.txt');
          const hook = path.join(workspace, '.git', 'hooks', 'pre-commit');
          fs.writeFileSync(hook, '#!/bin/sh\necho intentional commit failure >&2\nexit 1\n');
          fs.chmodSync(hook, 0o755);
          await mgr.autoloopStart({ runId, workspace });
          mockSessions[0].sendImplementation = async () => ({
            text: ['```autoloop', '{"tool":"write_plan","args":{"content":"# replacement plan"}}', '```'].join('\n'),
            event: { type: 'result', result: 'commit should fail' },
          });

          await expect(mgr.autoloopChat(runId, 'exercise exact index restoration')).rejects.toMatchObject({
            code: 'AUTOLOOP_CONTROL_APPLICATION_FAILED',
            message: expect.stringContaining('git commit failed for plan.md'),
          });

          expect(runGit(workspace, 'ls-files', '--stage', '--', 'plan.md')).toBe(artifactIndexBefore);
          expect(runGit(workspace, 'diff', '--cached', '--', 'staged.txt')).toBe(unrelatedIndexBefore);
          expect(runGit(workspace, 'diff', '--', 'dirty.txt')).toBe(unrelatedWorktreeBefore);
          expect(fs.readFileSync(path.join(workspace, 'plan.md'), 'utf8')).toBe('# replacement plan');
          expect(runGit(workspace, 'status', '--porcelain', '--', 'plan.md').trim()).toBe(
            prior === 'absent' ? '?? plan.md' : 'AM plan.md',
          );
        },
      );

      it('materializes a regular goal atomically through a same-directory rename', async () => {
        const runId = 'planner-goal-atomic-replace';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        const goalPath = path.join(workspace, 'goal.json');
        fs.writeFileSync(goalPath, '{"old":true}');
        await mgr.autoloopStart({ runId, workspace });
        mockSessions[0].sendImplementation = async () => ({
          text: ['```autoloop', '{"tool":"write_goal","args":{"content":"{\\"new\\":true}"}}', '```'].join('\n'),
          event: { type: 'result', result: 'replace goal atomically' },
        });
        const rename = vi.mocked(fs.renameSync);
        const renameImplementation = rename.getMockImplementation()!;
        const replacements: Array<{ from: string; to: string }> = [];
        rename.mockImplementation(((from: unknown, to: unknown) => {
          if (String(to) === goalPath) replacements.push({ from: String(from), to: String(to) });
          return (renameImplementation as (...values: unknown[]) => unknown)(from, to);
        }) as typeof fs.renameSync);

        try {
          await expect(mgr.autoloopChat(runId, 'replace the goal')).resolves.toMatchObject({
            reply: expect.stringContaining('write_goal'),
          });
          expect(fs.readFileSync(goalPath, 'utf8')).toBe('{"new":true}');
          expect(replacements).toHaveLength(1);
          expect(path.dirname(replacements[0].from)).toBe(workspace);
          expect(replacements[0].to).toBe(goalPath);
        } finally {
          rename.mockImplementation(renameImplementation);
        }
      });

      it('flushes the registry temp file, rename, and parent directory in order', () => {
        const sessionName = 'durable-registry-order';
        const generation = managerGeneration(sessionName, {
          owner_instance_id: mgr.autoloopOwnerInstanceId,
          session_id: 'durable-registry-session',
        });
        const tmpPath = `${SESSION_REGISTRY_FILE}.tmp`;
        const registryDir = path.dirname(SESSION_REGISTRY_FILE);
        const openedTargets = new Map<number, string>();
        const order: string[] = [];
        const openFile = vi.mocked(fs.openSync);
        const openImplementation = openFile.getMockImplementation()!;
        openFile.mockImplementation(((target: unknown, ...args: unknown[]) => {
          const fd = (openImplementation as (...values: unknown[]) => number)(target, ...args);
          openedTargets.set(fd, String(target));
          return fd;
        }) as typeof fs.openSync);
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          if (openedTargets.get(fd) === tmpPath) order.push('registry-file-flushed');
          if (openedTargets.get(fd) === registryDir) order.push('registry-directory-flushed');
          return flushImplementation(fd);
        });
        const rename = vi.mocked(fs.renameSync);
        const renameImplementation = rename.getMockImplementation()!;
        rename.mockImplementation(((from: unknown, to: unknown) => {
          if (String(from) === tmpPath && String(to) === SESSION_REGISTRY_FILE) order.push('registry-renamed');
          return (renameImplementation as (...values: unknown[]) => unknown)(from, to);
        }) as typeof fs.renameSync);

        try {
          expect(mgr.reserveAgentGeneration(generation, '/tmp')).toBe(true);
          expect(order).toEqual(['registry-file-flushed', 'registry-renamed', 'registry-directory-flushed']);
        } finally {
          openFile.mockImplementation(openImplementation);
          flush.mockImplementation(flushImplementation);
          rename.mockImplementation(renameImplementation);
        }
      });

      it('fails closed when the registry temp-file flush fails before replacement', () => {
        const sessionName = 'durable-registry-flush-failure';
        const generation = managerGeneration(sessionName, {
          owner_instance_id: mgr.autoloopOwnerInstanceId,
          session_id: 'durable-registry-flush-session',
        });
        const tmpPath = `${SESSION_REGISTRY_FILE}.tmp`;
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          if (persistenceFsState.descriptors.get(fd) === tmpPath) throw new Error('registry temp flush failed');
          return flushImplementation(fd);
        });

        try {
          expect(() => mgr.reserveAgentGeneration(generation, '/tmp')).toThrow(
            expect.objectContaining({ code: 'AUTOLOOP_AGENT_REGISTRY_PERSIST_FAILED' }),
          );
          expect(persistenceFsState.files.has(SESSION_REGISTRY_FILE)).toBe(false);
        } finally {
          flush.mockImplementation(flushImplementation);
        }
      });

      it('fails reset closed when generation release evidence cannot be flushed', async () => {
        const runId = 'generation-release-flush-failure';
        const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
        await mgr.autoloopStart({ runId, workspace });
        const handle = mgr.getAutoloop(runId)!;
        const generationPath = path.join(workspace, 'tasks', runId, 'agent-generations.jsonl');
        const openedTargets = new Map<number, string>();
        const openFile = vi.mocked(fs.openSync);
        const openImplementation = openFile.getMockImplementation()!;
        openFile.mockImplementation(((target: unknown, ...args: unknown[]) => {
          const fd = (openImplementation as (...values: unknown[]) => number)(target, ...args);
          openedTargets.set(fd, String(target));
          return fd;
        }) as typeof fs.openSync);
        const flush = vi.mocked(fs.fsyncSync);
        const flushImplementation = flush.getMockImplementation()!;
        flush.mockImplementation((fd) => {
          if (openedTargets.get(fd) === generationPath) throw new Error('generation evidence flush failed');
          return flushImplementation(fd);
        });

        try {
          await expect(handle.dispatcher.resetAgent('planner', { force: true })).resolves.toMatchObject({
            ok: false,
            code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
            message: expect.stringContaining('generation evidence flush failed'),
          });
        } finally {
          openFile.mockImplementation(openImplementation);
          flush.mockImplementation(flushImplementation);
        }
      });

      it.skipIf(process.platform === 'win32')(
        'fails reset closed when the generation-ledger parent directory cannot be flushed on POSIX',
        async () => {
          const runId = 'generation-release-directory-flush-failure';
          const workspace = fs.mkdtempSync(path.join(TEST_WF_DIR, `${runId}-`));
          await mgr.autoloopStart({ runId, workspace });
          const handle = mgr.getAutoloop(runId)!;
          const ledgerDir = path.join(workspace, 'tasks', runId);
          const openedTargets = new Map<number, string>();
          const openFile = vi.mocked(fs.openSync);
          const openImplementation = openFile.getMockImplementation()!;
          openFile.mockImplementation(((target: unknown, ...args: unknown[]) => {
            const fd = (openImplementation as (...values: unknown[]) => number)(target, ...args);
            openedTargets.set(fd, String(target));
            return fd;
          }) as typeof fs.openSync);
          const flush = vi.mocked(fs.fsyncSync);
          const flushImplementation = flush.getMockImplementation()!;
          flush.mockImplementation((fd) => {
            if (openedTargets.get(fd) === ledgerDir) throw new Error('generation directory flush failed');
            return flushImplementation(fd);
          });

          try {
            await expect(handle.dispatcher.resetAgent('planner', { force: true })).resolves.toMatchObject({
              ok: false,
              code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED',
              message: expect.stringContaining('generation directory flush failed'),
            });
          } finally {
            openFile.mockImplementation(openImplementation);
            flush.mockImplementation(flushImplementation);
          }
        },
      );

      it('keeps Planner reply listeners isolated across simultaneous runs', async () => {
        const firstWorkspace = fs.mkdtempSync(path.join(TEST_WF_DIR, 'cross-run-first-'));
        const secondWorkspace = fs.mkdtempSync(path.join(TEST_WF_DIR, 'cross-run-second-'));
        await mgr.autoloopStart({ runId: 'cross-run-first', workspace: firstWorkspace });
        await mgr.autoloopStart({ runId: 'cross-run-second', workspace: secondWorkspace });
        mockSessions[0].sendImplementation = async () => ({
          text: 'first run reply',
          event: { type: 'result', result: 'first run reply' },
        });
        mockSessions[1].sendImplementation = async () => ({
          text: 'second run reply',
          event: { type: 'result', result: 'second run reply' },
        });

        await expect(
          Promise.all([
            mgr.autoloopChat('cross-run-first', 'first run turn'),
            mgr.autoloopChat('cross-run-second', 'second run turn'),
          ]),
        ).resolves.toEqual([{ reply: 'first run reply' }, { reply: 'second run reply' }]);
      });
    });

    it('records the engines and models spawn_subagents actually chose', async () => {
      // Used to be written as a row into autoloop-registry.jsonl. It lands on
      // the run record now, which is what `autoloop_status` and a later resume
      // read — and unlike the registry, it survives alongside the rest of the
      // run's state rather than in a parallel file with its own lifecycle.
      await mgr.autoloopStart({ runId: 'spawn-persist', workspace: '/tmp' });
      await mgr.getAutoloop('spawn-persist')!.dispatcher.spawnSubagents({
        coder_engine: 'codex',
        coder_model: 'gpt-coder',
        reviewer_engine: 'gemini',
      });

      await vi.waitFor(() => {
        const run = mgr.workflowStatus('spawn-persist');
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const data = run.nodes.main?.data as any;
        expect(data?.roleSelection).toMatchObject({
          coder: { engine: 'codex', model: 'gpt-coder' },
          reviewer: { engine: 'gemini' },
        });
      });
    });
  });

  // ─── Persisted Sessions ─────────────────────────────────────────────

  describe('persisted sessions', () => {
    it('listPersistedSessions returns persisted entries', async () => {
      await mgr.startSession({ name: 'persist-test', cwd: '/tmp' });

      // After starting, the session should be persisted (since mock has sessionId)
      const persisted = mgr.listPersistedSessions();
      expect(persisted.length).toBeGreaterThanOrEqual(1);
      const entry = persisted.find((p) => p.name === 'persist-test');
      expect(entry).toBeDefined();
      expect(entry!.claudeSessionId).toBeDefined();
    });

    it('stopSession removes from persisted sessions', async () => {
      await mgr.startSession({ name: 'persist-remove', cwd: '/tmp' });
      expect(mgr.listPersistedSessions().find((p) => p.name === 'persist-remove')).toBeDefined();

      await mgr.stopSession('persist-remove');
      expect(mgr.listPersistedSessions().find((p) => p.name === 'persist-remove')).toBeUndefined();
    });

    it('persists and restores sandboxMode for non-Claude sessions', async () => {
      await mgr.startSession({
        name: 'readonly-persist',
        cwd: '/tmp',
        engine: 'cursor',
        sandboxMode: 'read-only',
      });
      await mgr.stopSession('readonly-persist', { keepPersisted: true });
      await mgr.startSession({ name: 'readonly-persist', cwd: '/tmp' });

      expect(createdConfigs.at(-1)).toMatchObject({
        engine: 'cursor',
        sandboxMode: 'read-only',
      });
    });

    it('persists and restores the real Codex thread ID', async () => {
      await mgr.startSession({ name: 'codex-persist', cwd: '/tmp', engine: 'codex', sandboxMode: 'read-only' });
      lastMock().threadId = '019c6dcb-93ad-7dc1-b531-418d213b8761';
      await mgr.sendMessage('codex-persist', 'hello');
      await mgr.stopSession('codex-persist', { keepPersisted: true });

      await mgr.startSession({ name: 'codex-persist', cwd: '/tmp' });

      expect(createdConfigs.at(-1)).toMatchObject({
        engine: 'codex',
        sandboxMode: 'read-only',
        resumeSessionId: '019c6dcb-93ad-7dc1-b531-418d213b8761',
      });
    });

    it('persists the agy conversation UUID after first send and never the synthetic session ID', async () => {
      const info = await mgr.startSession({ name: 'agy-persist', cwd: '/tmp', engine: 'agy' });
      expect(info.claudeSessionId).toBeUndefined();
      expect(mgr.listPersistedSessions().find((p) => p.name === 'agy-persist')).toBeUndefined();

      lastMock().conversationId = '99999999-8888-7777-6666-555555555555';
      const result = await mgr.sendMessage('agy-persist', 'hello');

      expect(result.sessionId).toBe('99999999-8888-7777-6666-555555555555');
      const entry = mgr.listPersistedSessions().find((p) => p.name === 'agy-persist');
      expect(entry?.claudeSessionId).toBe('99999999-8888-7777-6666-555555555555');
      expect(entry?.claudeSessionId).not.toMatch(/^mock-session-/);
    });
  });

  // ─── Council ────────────────────────────────────────────────────────

  describe('council', () => {
    it('councilStatus returns undefined for unknown council', () => {
      expect(mgr.councilStatus('unknown-council')).toBeUndefined();
    });

    it('councilAbort throws for unknown council', () => {
      expect(() => mgr.councilAbort('unknown')).toThrow("Council 'unknown' not found");
    });

    it('councilInject throws for unknown council', () => {
      expect(() => mgr.councilInject('unknown', 'msg')).toThrow("Council 'unknown' not found");
    });

    it('councilReview throws for unknown council', async () => {
      await expect(mgr.councilReview('unknown')).rejects.toThrow("Council 'unknown' not found");
    });

    it('councilAccept throws for unknown council', async () => {
      await expect(mgr.councilAccept('unknown')).rejects.toThrow("Council 'unknown' not found");
    });

    it('councilReject throws for unknown council', async () => {
      await expect(mgr.councilReject('unknown', 'bad work')).rejects.toThrow("Council 'unknown' not found");
    });
  });

  // ─── Input Validation ─────────────────────────────────────────────────

  describe('input validation', () => {
    it('createAgent rejects path-traversal names', () => {
      expect(() => mgr.createAgent('../../etc/evil', '/tmp')).toThrow('Invalid name');
    });

    it('createAgent rejects names with dots', () => {
      expect(() => mgr.createAgent('evil.md', '/tmp')).toThrow('Invalid name');
    });

    it('createSkill rejects path-traversal names', () => {
      expect(() => mgr.createSkill('../../etc/evil', '/tmp')).toThrow('Invalid name');
    });

    it('createRule rejects path-traversal names', () => {
      expect(() => mgr.createRule('../../etc/evil', '/tmp')).toThrow('Invalid name');
    });

    it('listAgents rejects unsafe cwd', () => {
      expect(() => mgr.listAgents('/etc')).toThrow('Unsafe working directory');
    });

    it('listSkills rejects unsafe cwd', () => {
      expect(() => mgr.listSkills('/etc')).toThrow('Unsafe working directory');
    });

    it('listRules rejects unsafe cwd', () => {
      expect(() => mgr.listRules('/etc')).toThrow('Unsafe working directory');
    });

    it('getVersion returns a version string', () => {
      const version = mgr.getVersion();
      expect(typeof version).toBe('string');
      expect(version.length).toBeGreaterThan(0);
    });
  });
});
