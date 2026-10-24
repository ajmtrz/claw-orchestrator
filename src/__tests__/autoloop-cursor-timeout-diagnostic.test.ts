/**
 * Phase A offline diagnostic — Cursor send-timeout / exact resume on the real
 * SessionManager → AutoloopRunner → ClaudeAgentDispatcher → PersistentCursorSession
 * → BaseOneShotSession.send path. Substitutes only spawnEngine.
 *
 * Diagnostic only: no production edits. Oracles O1–O9 assert cited DOC/TEST/COMMENT
 * contracts. Observations D1–D5 are recorded, never RED.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { Logger } from '../logger.js';

const NL = String.fromCharCode(10);

const OWNED_ROOT =
  process.env.CLAWO_PHASEA_OWNED_ROOT ||
  fs.mkdtempSync(path.join(os.tmpdir().startsWith('/dev/shm') ? os.tmpdir() : '/dev/shm', 'clawo-cursor-phaseA-'));
const PHASEA_DIR =
  process.env.CLAWO_PHASEA_DIR || path.join(process.cwd(), 'tasks', 'cursor-timeout-reconciliation-20261024', 'phaseA');

fs.mkdirSync(path.join(OWNED_ROOT, 'home'), { recursive: true });
fs.mkdirSync(path.join(OWNED_ROOT, 'tmp'), { recursive: true });
fs.mkdirSync(path.join(OWNED_ROOT, 'cache'), { recursive: true });
fs.mkdirSync(path.join(OWNED_ROOT, 'npm-cache'), { recursive: true });
fs.mkdirSync(PHASEA_DIR, { recursive: true });

process.env.HOME = path.join(OWNED_ROOT, 'home');
process.env.USERPROFILE = process.env.HOME;
process.env.TMPDIR = path.join(OWNED_ROOT, 'tmp');
process.env.TMP = process.env.TMPDIR;
process.env.TEMP = process.env.TMPDIR;
process.env.XDG_CACHE_HOME = path.join(OWNED_ROOT, 'cache');
process.env.npm_config_cache = path.join(OWNED_ROOT, 'npm-cache');
process.env.npm_config_offline = 'true';

const TEST_WF_DIR = fs.mkdtempSync(path.join(process.env.TMPDIR, 'clawo-phaseA-wf-'));
const TEST_SESSIONS_DIR = fs.mkdtempSync(path.join(process.env.TMPDIR, 'clawo-phaseA-sessions-'));
process.env.CLAWO_WF_DIR = TEST_WF_DIR;
process.env.CLAWO_SESSIONS_DIR = TEST_SESSIONS_DIR;

const mockSpawn = vi.fn();
vi.mock('../engine-spawn.js', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../engine-spawn.js')>()),
  spawnEngine: (...args: unknown[]) => mockSpawn(...args),
}));

vi.mock('node:fs', async () => {
  const actual = await vi.importActual<typeof import('node:fs')>('node:fs');
  const PROTECTED = ['claude-sessions.json', 'session-pids.json'];
  const isProtected = (p: unknown): boolean => typeof p === 'string' && PROTECTED.some((name) => p.includes(name));
  const existsSync = vi.fn((p: string) => (isProtected(p) ? false : actual.existsSync(p)));
  const readFileSync = vi.fn((p: string, enc?: string) =>
    isProtected(p) ? '[]' : actual.readFileSync(p, enc as BufferEncoding),
  );
  const writeFileSync = vi.fn((p: unknown, ...rest: unknown[]) => {
    if (isProtected(p)) return;
    (actual.writeFileSync as (...a: unknown[]) => void)(p, ...rest);
  });
  const appendFileSync = vi.fn((p: unknown, ...rest: unknown[]) => {
    if (isProtected(p)) return;
    (actual.appendFileSync as (...a: unknown[]) => void)(p, ...rest);
  });
  const openSync = vi.fn((p: unknown, ...rest: unknown[]) =>
    (actual.openSync as (...a: unknown[]) => number)(p, ...rest),
  );
  const writeSync = vi.fn((fd: unknown, ...rest: unknown[]) =>
    (actual.writeSync as (...a: unknown[]) => number)(fd, ...rest),
  );
  const mkdirSync = vi.fn((p: unknown, ...rest: unknown[]) => {
    if (isProtected(p)) return undefined;
    return (actual.mkdirSync as (...a: unknown[]) => string | undefined)(p, ...rest);
  });
  const renameSync = vi.fn((from: unknown, to: unknown) => {
    if (isProtected(from) || isProtected(to)) return;
    (actual.renameSync as (...a: unknown[]) => void)(from, to);
  });
  const shim = {
    existsSync,
    readFileSync,
    writeFileSync,
    appendFileSync,
    openSync,
    writeSync,
    mkdirSync,
    renameSync,
    writeFile: vi.fn((_p: unknown, _d: unknown, cb: (err: null) => void) => cb(null)),
    rename: vi.fn((_o: unknown, _n: unknown, cb: (err: null) => void) => cb(null)),
    mkdir: vi.fn((_p: unknown, _opts: unknown, cb: (err: null) => void) => cb(null)),
    unlink: vi.fn((_p: unknown, cb: () => void) => cb()),
  };
  return { ...actual, ...shim, default: { ...actual, ...shim } };
});

const { SessionManager } = await import('../session-manager.js');

type MockProc = EventEmitter & {
  stdout: Readable & { destroy: ReturnType<typeof vi.fn> };
  stderr: EventEmitter & { destroy: ReturnType<typeof vi.fn> };
  stdin: { end: ReturnType<typeof vi.fn> };
  kill: ReturnType<typeof vi.fn>;
  pid: number;
  exitCode: number | null;
};

type TurnKind = 'planner' | 'coder' | 'reviewer' | 'other';

type ScriptedReply =
  | { action: 'complete'; text: string }
  | { action: 'hang' }
  | { action: 'hang-then-late'; text: string }
  | { action: 'custom'; run: (proc: MockProc, prompt: string) => void };

interface SpawnRecord {
  kind: TurnKind;
  prompt: string;
  proc: MockProc;
  killSignal: string | null;
  closed: boolean;
  closeCode: number | null;
}

type SentinelDigestEntry = {
  caseId: string;
  logicalPath: 'work-sentinel.txt' | 'evidence-sentinel.txt';
  stage: 'before' | 'after';
  sha256: string;
};

const observations: Record<string, unknown> = {
  D1: null,
  D2: null,
  D3: null,
  D4: null,
  D5: null,
  /** Per-case actual before/after SHA-256 of both sentinel files (Finding 2). */
  sentinels: [] as SentinelDigestEntry[],
};

let spawnRecords: SpawnRecord[] = [];
let scriptedByKind: Partial<Record<TurnKind, ScriptedReply[]>> = {};
let pidSeq = 40_000;

function fence(tool: string, args: Record<string, unknown>): string {
  return ['```autoloop', JSON.stringify({ tool, args }), '```'].join(NL);
}

function sha256File(file: string): string {
  return createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function createMockProcess(): MockProc {
  const proc = new EventEmitter() as MockProc;
  proc.stdout = new Readable({ read() {} }) as MockProc['stdout'];
  proc.stdout.destroy = vi.fn();
  const stderr = new EventEmitter() as MockProc['stderr'];
  stderr.destroy = vi.fn();
  proc.stderr = stderr;
  proc.stdin = { end: vi.fn() };
  proc.kill = vi.fn();
  proc.pid = ++pidSeq;
  proc.exitCode = null;
  return proc;
}

function feedText(proc: MockProc, text: string): void {
  proc.stdout.push(text + NL);
}

/** One NDJSON assistant event so fence JSON is not mistaken for stream-json. */
function feedAssistant(proc: MockProc, text: string): void {
  feedText(
    proc,
    JSON.stringify({
      type: 'assistant',
      message: { role: 'assistant', content: [{ type: 'text', text }] },
    }),
  );
}

function closeProc(proc: MockProc, code: number): void {
  proc.exitCode = code;
  proc.stdout.push(null);
  proc.emit('close', code);
}

function classifyPrompt(args: string[]): { kind: TurnKind; prompt: string } {
  const pIdx = args.indexOf('-p');
  const prompt = pIdx >= 0 && pIdx + 1 < args.length ? String(args[pIdx + 1]) : '';
  if (prompt.includes('[directive iter=')) return { kind: 'coder', prompt };
  if (prompt.includes('[review_request iter=')) return { kind: 'reviewer', prompt };
  // Planner prompts carry role instructions / chat / system notices.
  if (
    prompt.includes('send_directive') ||
    prompt.includes('spawn_subagents') ||
    prompt.includes('[system]') ||
    prompt.includes('directive_ack') ||
    prompt.includes('You are the') ||
    prompt.length > 0
  ) {
    // Heuristic: coder/reviewer already matched; remaining Cursor -p sends are planner.
    return { kind: 'planner', prompt };
  }
  return { kind: 'other', prompt };
}

function takeScript(kind: TurnKind): ScriptedReply {
  const q = scriptedByKind[kind];
  if (q && q.length > 0) return q.shift()!;
  if (kind === 'planner') {
    return {
      action: 'complete',
      text: fence('spawn_subagents', {
        initial_directive: {
          goal: 'phaseA diagnostic',
          constraints: ['offline'],
          success_criteria: ['oracle'],
          max_attempts: 1,
        },
      }),
    };
  }
  if (kind === 'coder') {
    return {
      action: 'complete',
      text: fence('iter_complete', {
        summary: 'phaseA coder default',
        eval_output: { metric: 1, gates: [] },
      }),
    };
  }
  if (kind === 'reviewer') {
    return {
      action: 'complete',
      text: fence('review_complete', {
        decision: 'advance',
        metric: 1,
        audit_notes: 'phaseA reviewer default',
      }),
    };
  }
  return { action: 'complete', text: 'ack' };
}

function installSpawnMock(): void {
  mockSpawn.mockReset();
  mockSpawn.mockImplementation((_bin: string, args: string[]) => {
    const proc = createMockProcess();
    const { kind, prompt } = classifyPrompt(args);
    const record: SpawnRecord = {
      kind,
      prompt,
      proc,
      killSignal: null,
      closed: false,
      closeCode: null,
    };
    spawnRecords.push(record);
    proc.kill = vi.fn((signal?: string) => {
      record.killSignal = signal ?? 'SIGTERM';
    });
    const origEmit = proc.emit.bind(proc);
    proc.emit = ((event: string | symbol, ...eventArgs: unknown[]) => {
      if (event === 'close') {
        record.closed = true;
        record.closeCode = typeof eventArgs[0] === 'number' ? eventArgs[0] : null;
      }
      return origEmit(event, ...eventArgs);
    }) as typeof proc.emit;

    const script = takeScript(kind);
    const runScript = () => {
      if (script.action === 'complete') {
        feedAssistant(proc, script.text);
        closeProc(proc, 0);
      } else if (script.action === 'hang') {
        // leave open until adapter timeout kill
      } else if (script.action === 'hang-then-late') {
        const late = script.text;
        const prevKill = proc.kill;
        proc.kill = vi.fn((signal?: string) => {
          prevKill(signal);
          setTimeout(() => {
            feedAssistant(proc, late);
            closeProc(proc, 0);
          }, 1);
        });
      } else if (script.action === 'custom') {
        script.run(proc, prompt);
      }
    };
    // Defer past createInterface attachment; keep delay << sendTimeoutMs.
    setTimeout(runScript, 1);
    return proc;
  });
}

async function flushMicrotasks(times = 8): Promise<void> {
  for (let i = 0; i < times; i++) await Promise.resolve();
}

async function tick(ms = 1): Promise<void> {
  await vi.advanceTimersByTimeAsync(ms);
  await new Promise<void>((r) => setImmediate(r));
  await flushMicrotasks();
}

/** Wait while advancing tiny fake slices; keep total << sendTimeoutMs. */
async function waitUntil(check: () => void, steps = 2000, stepMs = 1): Promise<void> {
  let last: unknown;
  for (let i = 0; i < steps; i++) {
    try {
      check();
      return;
    } catch (err) {
      last = err;
    }
    await tick(stepMs);
  }
  throw last instanceof Error ? last : new Error(`waitUntil exhausted: ${String(last)}`);
}

/** Drain a promise under fake timers without sleeping on the wall clock. */
async function drainPromise<T>(pending: Promise<T>, maxSteps = 20_000, stepMs = 1): Promise<T> {
  let settled = false;
  let value: T | undefined;
  let error: unknown;
  void pending.then(
    (v) => {
      settled = true;
      value = v;
    },
    (e) => {
      settled = true;
      error = e;
    },
  );
  for (let i = 0; i < maxSteps && !settled; i++) await tick(stepMs);
  if (!settled) throw new Error(`drainPromise exhausted after ${maxSteps} steps`);
  if (error !== undefined) throw error;
  return value as T;
}

function createManager(logger?: Logger): InstanceType<typeof SessionManager> {
  return new SessionManager(
    {
      claudeBin: 'mock-claude',
      maxConcurrentSessions: 8,
      sessionTtlMinutes: 120,
      defaultPermissionMode: 'acceptEdits',
      defaultEffort: 'auto',
    },
    logger,
  );
}

function initGitWorkspace(workspace: string): { workSha: string; evidenceSha: string } {
  fs.mkdirSync(workspace, { recursive: true });
  execFileSync('git', ['init', '-b', 'main'], { cwd: workspace });
  execFileSync('git', ['config', 'user.email', 'phasea@example.com'], { cwd: workspace });
  execFileSync('git', ['config', 'user.name', 'phaseA'], { cwd: workspace });
  execFileSync('git', ['config', 'commit.gpgsign', 'false'], { cwd: workspace });
  const workPath = path.join(workspace, 'work-sentinel.txt');
  const evidencePath = path.join(workspace, 'evidence-sentinel.txt');
  fs.writeFileSync(workPath, 'work-sentinel-v1' + NL);
  fs.writeFileSync(evidencePath, 'evidence-sentinel-v1' + NL);
  execFileSync('git', ['add', 'work-sentinel.txt', 'evidence-sentinel.txt'], { cwd: workspace });
  execFileSync('git', ['commit', '-m', 'phaseA init sentinels'], { cwd: workspace });
  return { workSha: sha256File(workPath), evidenceSha: sha256File(evidencePath) };
}

function sentinelPaths(workspace: string): { work: string; evidence: string } {
  return {
    work: path.join(workspace, 'work-sentinel.txt'),
    evidence: path.join(workspace, 'evidence-sentinel.txt'),
  };
}

/** Read actual sentinel bytes, persist digests, return them. No duplicate framework. */
function captureSentinelDigests(
  caseId: string,
  workspace: string,
  stage: 'before' | 'after',
): { work: string; evidence: string } {
  const paths = sentinelPaths(workspace);
  const work = sha256File(paths.work);
  const evidence = sha256File(paths.evidence);
  const list = observations.sentinels as SentinelDigestEntry[];
  list.push({ caseId, logicalPath: 'work-sentinel.txt', stage, sha256: work });
  list.push({ caseId, logicalPath: 'evidence-sentinel.txt', stage, sha256: evidence });
  return { work, evidence };
}

function assertSentinelsUnchanged(caseId: string, workspace: string, before: { work: string; evidence: string }): void {
  const after = captureSentinelDigests(caseId, workspace, 'after');
  expect(after.work).toBe(before.work);
  expect(after.evidence).toBe(before.evidence);
}

function ledgerDir(workspace: string, runId: string): string {
  return path.join(workspace, 'tasks', runId);
}

function readDecisionRows(workspace: string, runId: string): Array<Record<string, unknown>> {
  const p = path.join(ledgerDir(workspace, runId), 'decisions.jsonl');
  if (!fs.existsSync(p)) return [];
  return fs
    .readFileSync(p, 'utf8')
    .trim()
    .split(NL)
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function coderSpawns(): SpawnRecord[] {
  return spawnRecords.filter((r) => r.kind === 'coder');
}

function reviewerSpawns(): SpawnRecord[] {
  return spawnRecords.filter((r) => r.kind === 'reviewer');
}

function countGitLog(workspace: string, grep: string): number {
  const out = execFileSync('git', ['log', '--oneline', '--grep', grep], {
    cwd: workspace,
    encoding: 'utf8',
  }).trim();
  return out ? out.split(NL).length : 0;
}

async function settleShutdown(target: InstanceType<typeof SessionManager>): Promise<void> {
  const done = target.shutdown();
  let settled = false;
  void done.finally(() => {
    settled = true;
  });
  for (let step = 0; step < 60 && !settled; step += 1) {
    await tick(1_000);
  }
  if (!settled) throw new Error('shutdown did not settle after advancing fake timers');
  await done;
}

function persistObservations(): void {
  fs.writeFileSync(path.join(PHASEA_DIR, 'observations.json'), JSON.stringify(observations, null, 2) + NL);
  // Dedicated sentinel evidence file (same digests); sealed after G4, not rewritten by G5–G10.
  fs.writeFileSync(
    path.join(PHASEA_DIR, 'sentinel-digests.json'),
    JSON.stringify(
      {
        note: 'Per-case actual before/after SHA-256 of work-sentinel.txt and evidence-sentinel.txt',
        cases: observations.sentinels,
      },
      null,
      2,
    ) + NL,
  );
  fs.writeFileSync(
    path.join(PHASEA_DIR, 'spawn-summary.json'),
    JSON.stringify(
      {
        ownedRoot: OWNED_ROOT,
        phaseADir: PHASEA_DIR,
        wfDir: TEST_WF_DIR,
        sessionsDir: TEST_SESSIONS_DIR,
        spawnCount: spawnRecords.length,
        byKind: {
          planner: spawnRecords.filter((r) => r.kind === 'planner').length,
          coder: coderSpawns().length,
          reviewer: reviewerSpawns().length,
          other: spawnRecords.filter((r) => r.kind === 'other').length,
        },
      },
      null,
      2,
    ) + NL,
  );
}

describe('Phase A Cursor timeout/resume diagnostic (real path, spawnEngine only)', () => {
  let mgr: InstanceType<typeof SessionManager>;
  const SEND_MS = 5_000;
  const RAISED_MS = 10_000;

  beforeEach(() => {
    vi.useFakeTimers({
      shouldAdvanceTime: false,
      toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date'],
    });
    spawnRecords = [];
    scriptedByKind = {};
    installSpawnMock();
    fs.rmSync(TEST_WF_DIR, { recursive: true, force: true });
    fs.mkdirSync(TEST_WF_DIR, { recursive: true });
    mgr = createManager();
  });

  afterEach(async () => {
    persistObservations();
    await settleShutdown(mgr);
    vi.useRealTimers();
  });

  const startCursorAutoloop = async (
    runId: string,
    workspace: string,
    timeouts?: {
      sendTimeoutMs?: number;
      activityLeaseMs?: number;
      autoloopHardTimeoutMs?: number;
    },
  ) => {
    await mgr.autoloopStart({
      runId,
      workspace,
      plannerEngine: 'cursor',
      coderEngine: 'cursor',
      reviewerEngine: 'cursor',
      sendTimeoutMs: timeouts?.sendTimeoutMs ?? SEND_MS,
      activityLeaseMs: timeouts?.activityLeaseMs ?? 3_600_000,
      autoloopHardTimeoutMs: timeouts?.autoloopHardTimeoutMs ?? 86_400_000,
    });
    return mgr.getAutoloop(runId)!;
  };

  it('O1 control: success before deadline is accepted once', { timeout: 60_000 }, async () => {
    const runId = 'phaseA-o1';
    const workspace = path.join(TEST_WF_DIR, 'ws-o1');
    initGitWorkspace(workspace);
    const before = captureSentinelDigests('O1', workspace, 'before');

    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: {
              goal: 'O1 control',
              constraints: [],
              success_criteria: ['accepted once'],
              max_attempts: 1,
            },
          }),
        },
        { action: 'complete', text: 'ack after review' },
      ],
      coder: [
        {
          action: 'custom',
          run: (proc) => {
            // Close just before the Coder send deadline (margin avoids racing SIGTERM).
            setTimeout(() => {
              fs.writeFileSync(path.join(workspace, 'o1-proof.txt'), 'o1-proof' + NL);
              feedAssistant(
                proc,
                fence('iter_complete', {
                  summary: 'O1 before deadline',
                  eval_output: { metric: 1, gates: [] },
                }),
              );
              closeProc(proc, 0);
            }, SEND_MS - 10);
          },
        },
      ],
      reviewer: [
        {
          action: 'complete',
          text: fence('review_complete', {
            decision: 'advance',
            metric: 1,
            audit_notes: 'O1 advance',
          }),
        },
      ],
    };

    const handle = await startCursorAutoloop(runId, workspace);
    const chatP = mgr.autoloopChat(runId, 'O1 start');
    const iterDir = path.join(ledgerDir(workspace, runId), 'iter', '0');
    await waitUntil(() => expect(coderSpawns().length).toBe(1));
    await tick(1); // installSpawnMock defers script by 1ms
    // Boundary: child still open before deadline.
    expect(coderSpawns()[0].closed).toBe(false);
    expect(coderSpawns()[0].killSignal).toBeNull();
    await vi.advanceTimersByTimeAsync(SEND_MS - 10);
    await waitUntil(() => expect(coderSpawns()[0].closed).toBe(true), 500, 0);
    expect(coderSpawns()[0].killSignal).toBeNull();
    expect(coderSpawns()[0].closeCode).toBe(0);
    // Coder already closed before SEND_MS; small advances finish reviewer/git without racing the open child.
    await drainPromise(chatP, 20_000, 1);
    expect(fs.existsSync(path.join(iterDir, 'eval_output.json'))).toBe(true);
    expect(fs.existsSync(path.join(iterDir, 'coder_summary.txt'))).toBe(true);
    expect(fs.existsSync(path.join(iterDir, 'diff.patch'))).toBe(true);
    expect(fs.existsSync(path.join(iterDir, 'verdict.json'))).toBe(true);

    expect(coderSpawns()).toHaveLength(1);
    expect(countGitLog(workspace, 'autoloop/iter-0')).toBe(1);
    expect(reviewerSpawns()).toHaveLength(1);
    const chat = fs.readFileSync(path.join(ledgerDir(workspace, runId), 'chat.jsonl'), 'utf8');
    expect(chat).toMatch(/review_request|Reviewer|review/i);
    assertSentinelsUnchanged('O1', workspace, before);
    expect(handle.runner.state.status).not.toBe('paused');
    expect(readDecisionRows(workspace, runId).filter((r) => r.kind === 'send_timeout')).toHaveLength(0);
  });

  it('O2: timeout classification pauses once without recovery effects', async () => {
    const runId = 'phaseA-o2';
    const workspace = path.join(TEST_WF_DIR, 'ws-o2');
    initGitWorkspace(workspace);
    const before = captureSentinelDigests('O2', workspace, 'before');

    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: {
              goal: 'O2 timeout',
              constraints: [],
              success_criteria: [],
              max_attempts: 1,
            },
          }),
        },
      ],
      coder: [{ action: 'hang' }],
    };

    const handle = await startCursorAutoloop(runId, workspace);
    const timeouts: unknown[] = [];
    handle.runner.on('send_timeout', (e) => timeouts.push(e));

    const chatP = mgr.autoloopChat(runId, 'O2 hang coder');
    await waitUntil(() => expect(coderSpawns().length).toBe(1));
    await vi.advanceTimersByTimeAsync(SEND_MS);
    await chatP;

    expect(handle.runner.state.status).toBe('paused');
    const pending = handle.runner.state.pending_dispatch!;
    expect(pending).toMatchObject({
      status: 'awaiting_resume',
      agent: 'coder',
      timeout_ms: SEND_MS,
    });
    expect(handle.runner.state.status_reason).toBe(`awaiting_resume:send_timeout:coder:${pending.dispatch_id}`);
    expect(timeouts).toHaveLength(1);
    const rows = readDecisionRows(workspace, runId);
    expect(rows.filter((r) => r.kind === 'send_timeout')).toHaveLength(1);
    expect(rows.filter((r) => r.kind === 'reset_agent')).toHaveLength(0);
    expect(coderSpawns()).toHaveLength(1);

    const killed = coderSpawns()[0];
    observations.D1 = {
      killSignal: killed.killSignal,
      closedAfterKill: killed.closed,
      closeCode: killed.closeCode,
      note: 'recorded only; no DOC/TEST/COMMENT contract on kill/close pairing',
    };
    observations.D5 = {
      coderSessions: mgr.listSessions().filter((s) => s.name.endsWith('-coder')),
      historyNote: 'whether timed-out one-shot leaves listed session stats — observation only',
    };
    assertSentinelsUnchanged('O2', workspace, before);
  });

  it('O3: late post-deadline effects are not accepted twice (kill-then-close)', async () => {
    const runId = 'phaseA-o3';
    const workspace = path.join(TEST_WF_DIR, 'ws-o3');
    initGitWorkspace(workspace);
    const before = captureSentinelDigests('O3', workspace, 'before');

    const lateText = fence('iter_complete', {
      summary: 'late after deadline',
      eval_output: { metric: 99, gates: [] },
    });

    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: {
              goal: 'O3 late',
              constraints: [],
              success_criteria: [],
              max_attempts: 1,
            },
          }),
        },
      ],
      // hang-then-late: after adapter kill, emit late iter_complete and close (same helper).
      coder: [{ action: 'hang-then-late', text: lateText }],
    };

    const handle = await startCursorAutoloop(runId, workspace);
    const timeouts: unknown[] = [];
    handle.runner.on('send_timeout', (e) => timeouts.push(e));

    const chatP = mgr.autoloopChat(runId, 'O3');
    await waitUntil(() => expect(coderSpawns().length).toBe(1));
    await vi.advanceTimersByTimeAsync(SEND_MS);
    await chatP;
    // Fixture precondition: kill happened, then late output+close actually ran.
    await waitUntil(() => expect(coderSpawns()[0].killSignal).toBe('SIGTERM'));
    await tick(1);
    await waitUntil(() => {
      expect(coderSpawns()[0].closed).toBe(true);
      expect(coderSpawns()[0].closeCode).toBe(0);
    });

    const pending = handle.runner.state.pending_dispatch!;
    // Public no-duplicate contract only (settled promise stays rejected; no fabricated resolve).
    expect(handle.runner.state.status).toBe('paused');
    expect(timeouts).toHaveLength(1);
    expect(coderSpawns()).toHaveLength(1);

    const { Msg } = await import('../autoloop/messages.js');
    await handle.runner.send(Msg.sendTimeout(0, pending));
    expect(handle.runner.state.status).toBe('paused');
    expect(timeouts).toHaveLength(1);
    expect(coderSpawns()).toHaveLength(1);
    expect(readDecisionRows(workspace, runId).filter((r) => r.kind === 'send_timeout')).toHaveLength(1);

    const acceptedIters = countGitLog(workspace, 'autoloop/iter-0');
    const evalPresent = fs.existsSync(path.join(ledgerDir(workspace, runId), 'iter', '0', 'eval_output.json'));
    observations.D2 = {
      lateAcceptedIterations: acceptedIters,
      evalPresent,
      killThenClose: true,
      note: 'D2 records 0-or-1 acceptance of late post-deadline output; observed ' + String(acceptedIters),
    };
    expect(acceptedIters === 0 || acceptedIters === 1).toBe(true);
    assertSentinelsUnchanged('O3', workspace, before);
  });

  it('O4: no-option resume is no-op; invalid overrides refuse atomically', async () => {
    const runId = 'phaseA-o4';
    const workspace = path.join(TEST_WF_DIR, 'ws-o4');
    initGitWorkspace(workspace);
    const beforeSent = captureSentinelDigests('O4', workspace, 'before');

    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: { goal: 'O4', constraints: [], success_criteria: [], max_attempts: 1 },
          }),
        },
      ],
      coder: [{ action: 'hang' }],
    };

    const handle = await startCursorAutoloop(runId, workspace);
    const chatP = mgr.autoloopChat(runId, 'O4');
    await waitUntil(() => expect(coderSpawns().length).toBe(1));
    await vi.advanceTimersByTimeAsync(SEND_MS);
    await chatP;

    const pending = handle.runner.state.pending_dispatch!;
    const auditPath = path.join(ledgerDir(workspace, runId), 'decisions.jsonl');
    const chatPath = path.join(ledgerDir(workspace, runId), 'chat.jsonl');
    const specPath = path.join(TEST_WF_DIR, runId, 'spec.json');
    const sentinels = sentinelPaths(workspace);
    const before = {
      status: handle.runner.state.status,
      status_reason: handle.runner.state.status_reason,
      pending: JSON.stringify(handle.runner.state.pending_dispatch),
      timeout: handle.dispatcher.config.sendTimeoutMs,
      sends: coderSpawns().length,
      audit: fs.readFileSync(auditPath, 'utf8'),
      chat: fs.readFileSync(chatPath, 'utf8'),
      spec: fs.existsSync(specPath) ? fs.readFileSync(specPath, 'utf8') : '',
      work: sha256File(sentinels.work),
      evidence: sha256File(sentinels.evidence),
      activity: handle.runner.state.last_activity_at,
    };

    await expect(mgr.autoloopResume(runId)).resolves.toBe(handle.runner.state);
    expect(handle.runner.state.status).toBe(before.status);
    expect(handle.runner.state.status_reason).toBe(before.status_reason);
    expect(JSON.stringify(handle.runner.state.pending_dispatch)).toBe(before.pending);
    expect(handle.runner.state.last_activity_at).toBe(before.activity);
    expect(coderSpawns()).toHaveLength(before.sends);
    expect(fs.readFileSync(auditPath, 'utf8')).toBe(before.audit);

    const resumeWith = (opts: Record<string, unknown>) =>
      mgr.autoloopResume(
        runId,
        opts as unknown as Parameters<InstanceType<typeof SessionManager>['autoloopResume']>[1],
      );

    for (const override of [
      { sendTimeoutMs: SEND_MS, pendingDispatchId: pending.dispatch_id },
      { sendTimeoutMs: SEND_MS - 1, pendingDispatchId: pending.dispatch_id },
      { sendTimeoutMs: 4_999, pendingDispatchId: pending.dispatch_id },
      { sendTimeoutMs: 7_200_001, pendingDispatchId: pending.dispatch_id },
      { sendTimeoutMs: RAISED_MS, pendingDispatchId: 'dispatch_stale_identity' },
    ]) {
      await expect(resumeWith(override)).rejects.toThrow();
      expect(handle.runner.state.status).toBe(before.status);
      expect(handle.runner.state.status_reason).toBe(before.status_reason);
      expect(JSON.stringify(handle.runner.state.pending_dispatch)).toBe(before.pending);
      expect(handle.dispatcher.config.sendTimeoutMs).toBe(before.timeout);
      expect(coderSpawns()).toHaveLength(before.sends);
      expect(fs.readFileSync(auditPath, 'utf8')).toBe(before.audit);
      expect(fs.readFileSync(chatPath, 'utf8')).toBe(before.chat);
      if (before.spec) expect(fs.readFileSync(specPath, 'utf8')).toBe(before.spec);
      expect(sha256File(sentinels.work)).toBe(before.work);
      expect(sha256File(sentinels.evidence)).toBe(before.evidence);
    }
    assertSentinelsUnchanged('O4', workspace, beforeSent);
  });

  it('O5: exact resume raises timeout, migrates once, does not re-send', async () => {
    const runId = 'phaseA-o5';
    const workspace = path.join(TEST_WF_DIR, 'ws-o5');
    initGitWorkspace(workspace);
    const beforeSent = captureSentinelDigests('O5', workspace, 'before');

    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: { goal: 'O5', constraints: [], success_criteria: [], max_attempts: 1 },
          }),
        },
      ],
      coder: [{ action: 'hang' }],
    };

    const LEASE_MS = 60_000;
    const handle = await startCursorAutoloop(runId, workspace, {
      sendTimeoutMs: SEND_MS,
      activityLeaseMs: LEASE_MS,
      autoloopHardTimeoutMs: 86_400_000,
    });
    const chatP = mgr.autoloopChat(runId, 'O5');
    await waitUntil(() => expect(coderSpawns().length).toBe(1));
    await vi.advanceTimersByTimeAsync(SEND_MS);
    await chatP;

    const pending = handle.runner.state.pending_dispatch!;
    const auditPath = path.join(ledgerDir(workspace, runId), 'decisions.jsonl');
    const chatPath = path.join(ledgerDir(workspace, runId), 'chat.jsonl');
    const specPath = path.join(TEST_WF_DIR, runId, 'spec.json');
    const auditBefore = fs.readFileSync(auditPath, 'utf8');
    const chatBefore = fs.readFileSync(chatPath, 'utf8');
    const specBefore = fs.readFileSync(specPath, 'utf8');
    const coderBefore = coderSpawns().length;
    const iter0 = path.join(ledgerDir(workspace, runId), 'iter', '0');
    const hashTree = (dir: string): Record<string, string> => {
      const out: Record<string, string> = {};
      if (!fs.existsSync(dir)) return out;
      for (const name of fs.readdirSync(dir)) {
        const full = path.join(dir, name);
        if (fs.statSync(full).isFile()) out[name] = sha256File(full);
      }
      return out;
    };
    const evidenceBefore = hashTree(iter0);
    const directiveBefore = fs.existsSync(path.join(iter0, 'directive.json'))
      ? fs.readFileSync(path.join(iter0, 'directive.json'), 'utf8')
      : '';

    const resumed = await mgr.autoloopResume(runId, {
      sendTimeoutMs: RAISED_MS,
      pendingDispatchId: pending.dispatch_id,
    } as never);

    expect(resumed).toMatchObject({
      status: 'running',
      status_reason: null,
      pending_dispatch: null,
    });
    expect(handle.dispatcher.config.sendTimeoutMs).toBe(RAISED_MS);
    expect(coderSpawns()).toHaveLength(coderBefore);

    const auditAfter = fs.readFileSync(auditPath, 'utf8');
    expect(auditAfter.startsWith(auditBefore)).toBe(true);
    const migrations = auditAfter
      .trim()
      .split(NL)
      .map((l) => JSON.parse(l) as Record<string, unknown>)
      .filter((r) => r.kind === 'timeout_migration');
    expect(migrations).toHaveLength(1);
    expect(migrations[0]).toMatchObject({
      kind: 'timeout_migration',
      field: 'sendTimeoutMs',
      oldValue: SEND_MS,
      newValue: RAISED_MS,
      reason: 'recoverable_send_timeout_resume',
      pendingDispatchId: pending.dispatch_id,
    });
    expect(fs.readFileSync(chatPath, 'utf8')).toBe(chatBefore);
    expect(fs.readFileSync(specPath, 'utf8')).toBe(specBefore);
    expect(hashTree(iter0)).toEqual(evidenceBefore);
    if (directiveBefore) {
      expect(fs.readFileSync(path.join(iter0, 'directive.json'), 'utf8')).toBe(directiveBefore);
    }

    // D3: advance past activity lease expiry and record outcome (observation only).
    await vi.advanceTimersByTimeAsync(LEASE_MS + 1);
    await tick(0);
    observations.D3 = {
      statusAfterLease: handle.runner.state.status,
      status_reason: handle.runner.state.status_reason,
      pending_dispatch: handle.runner.state.pending_dispatch,
      leaseMs: LEASE_MS,
      note: 'advanced past activityLeaseMs after exact resume; observation only',
    };
    assertSentinelsUnchanged('O5', workspace, beforeSent);
  });

  it('O6: replay resolved timeout ignored; repeat exact resume rejected', async () => {
    const runId = 'phaseA-o6';
    const workspace = path.join(TEST_WF_DIR, 'ws-o6');
    initGitWorkspace(workspace);
    const beforeSent = captureSentinelDigests('O6', workspace, 'before');

    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: { goal: 'O6', constraints: [], success_criteria: [], max_attempts: 1 },
          }),
        },
      ],
      coder: [{ action: 'hang' }],
    };

    const handle = await startCursorAutoloop(runId, workspace);
    const chatP = mgr.autoloopChat(runId, 'O6');
    await waitUntil(() => expect(coderSpawns().length).toBe(1));
    await vi.advanceTimersByTimeAsync(SEND_MS);
    await chatP;
    const pending = handle.runner.state.pending_dispatch!;
    await mgr.autoloopResume(runId, {
      sendTimeoutMs: RAISED_MS,
      pendingDispatchId: pending.dispatch_id,
    } as never);
    const auditPath = path.join(ledgerDir(workspace, runId), 'decisions.jsonl');
    const auditAfter = fs.readFileSync(auditPath, 'utf8');

    const { Msg } = await import('../autoloop/messages.js');
    await handle.runner.send(Msg.sendTimeout(0, pending));
    expect(handle.runner.state).toMatchObject({ status: 'running', pending_dispatch: null });

    await expect(
      mgr.autoloopResume(runId, {
        sendTimeoutMs: RAISED_MS,
        pendingDispatchId: pending.dispatch_id,
      } as never),
    ).rejects.toThrow(/not awaiting.*send timeout/i);
    expect(fs.readFileSync(auditPath, 'utf8')).toBe(auditAfter);
    assertSentinelsUnchanged('O6', workspace, beforeSent);
  });

  it('O7: distinct follow-up after resume uses raised timeout', { timeout: 30_000 }, async () => {
    const runId = 'phaseA-o7';
    const workspace = path.join(TEST_WF_DIR, 'ws-o7');
    initGitWorkspace(workspace);
    const beforeSent = captureSentinelDigests('O7', workspace, 'before');

    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: { goal: 'O7 first', constraints: [], success_criteria: [], max_attempts: 1 },
          }),
        },
        {
          action: 'complete',
          text: fence('send_directive', {
            goal: 'O7 follow-up',
            constraints: [],
            success_criteria: [],
            max_attempts: 1,
          }),
        },
        { action: 'complete', text: 'ack' },
      ],
      coder: [
        { action: 'hang' },
        {
          action: 'custom',
          run: (proc) => {
            (proc as MockProc & { __o7?: string }).__o7 = 'waiting';
          },
        },
      ],
      reviewer: [
        {
          action: 'complete',
          text: fence('review_complete', {
            decision: 'advance',
            metric: 2,
            audit_notes: 'O7',
          }),
        },
      ],
    };

    const handle = await startCursorAutoloop(runId, workspace);
    const first = mgr.autoloopChat(runId, 'O7 timeout');
    await waitUntil(() => expect(coderSpawns().length).toBe(1));
    await vi.advanceTimersByTimeAsync(SEND_MS);
    await first;
    const pending = handle.runner.state.pending_dispatch!;
    await mgr.autoloopResume(runId, {
      sendTimeoutMs: RAISED_MS,
      pendingDispatchId: pending.dispatch_id,
    } as never);

    const follow = mgr.autoloopChat(runId, 'O7 distinct follow-up');
    await waitUntil(() => expect(coderSpawns().length).toBe(2));
    // Past old deadline, still before raised: must not pause again.
    await vi.advanceTimersByTimeAsync(SEND_MS + 500);
    expect(handle.runner.state.status).toBe('running');
    expect(handle.runner.state.pending_dispatch).toBeNull();

    const second = coderSpawns()[1];
    fs.writeFileSync(path.join(workspace, 'o7-proof.txt'), 'o7-proof' + NL);
    feedAssistant(
      second.proc,
      fence('iter_complete', {
        summary: 'O7 accepted under raised timeout',
        eval_output: { metric: 2, gates: [] },
      }),
    );
    closeProc(second.proc, 0);
    await drainPromise(follow);

    // First directive timed out; follow-up send_directive reuses iter 0.
    const acceptedIter = path.join(ledgerDir(workspace, runId), 'iter', '0');
    expect(fs.existsSync(path.join(acceptedIter, 'eval_output.json'))).toBe(true);
    expect(countGitLog(workspace, 'autoloop/iter-0')).toBe(1);
    expect(coderSpawns()).toHaveLength(2);
    expect(readDecisionRows(workspace, runId).filter((r) => r.kind === 'send_timeout')).toHaveLength(1);
    assertSentinelsUnchanged('O7', workspace, beforeSent);
  });

  it.each(['operator stop', 'hard timeout'] as const)(
    'O8: %s stays terminal when adapter timeout arrives late',
    { timeout: 30_000 },
    async (terminalCause) => {
      scriptedByKind = {
        planner: [
          {
            action: 'complete',
            text: fence('spawn_subagents', {
              initial_directive: {
                goal: `O8 ${terminalCause}`,
                constraints: [],
                success_criteria: [],
                max_attempts: 1,
              },
            }),
          },
        ],
        coder: [{ action: 'hang' }],
      };

      const runId = `phaseA-o8-${terminalCause.replace(' ', '-')}`;
      const workspace = path.join(TEST_WF_DIR, `ws-${runId}`);
      initGitWorkspace(workspace);
      const caseId = `O8:${terminalCause}`;
      const beforeSent = captureSentinelDigests(caseId, workspace, 'before');
      const sendTimeoutMs = 7_200_000;
      const handle = await startCursorAutoloop(runId, workspace, {
        sendTimeoutMs,
        activityLeaseMs: 7_200_000,
        autoloopHardTimeoutMs: terminalCause === 'hard timeout' ? 600_000 : 86_400_000,
      });
      const timeouts: unknown[] = [];
      handle.runner.on('send_timeout', (e) => timeouts.push(e));

      const chatP = mgr.autoloopChat(runId, `O8 ${terminalCause}`);
      await waitUntil(() => expect(coderSpawns().length).toBe(1));

      if (terminalCause === 'hard timeout') {
        await vi.advanceTimersByTimeAsync(600_000);
        // Late adapter deadline after the hard terminal transition.
        await vi.advanceTimersByTimeAsync(sendTimeoutMs - 600_000);
      } else {
        await mgr.autoloopStop(runId, 'operator-stop-during-send');
        // Late adapter deadline after operator stop.
        await vi.advanceTimersByTimeAsync(sendTimeoutMs);
      }
      await drainPromise(chatP);

      expect(handle.runner.state.status).toBe('terminated');
      expect(handle.runner.state.pending_dispatch).toBeNull();
      expect(timeouts).toHaveLength(0);
      expect(coderSpawns()).toHaveLength(1);
      expect(handle.runner.state.status_reason).toBe(
        terminalCause === 'hard timeout' ? 'hard_timeout_exceeded' : 'operator-stop-during-send',
      );
      assertSentinelsUnchanged(caseId, workspace, beforeSent);
    },
  );

  it('D4: stop while awaiting_resume then late child close', async () => {
    const runId = 'phaseA-d4';
    const workspace = path.join(TEST_WF_DIR, 'ws-d4');
    initGitWorkspace(workspace);
    const beforeSent = captureSentinelDigests('D4', workspace, 'before');
    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: { goal: 'D4', constraints: [], success_criteria: [], max_attempts: 1 },
          }),
        },
      ],
      coder: [
        {
          action: 'custom',
          run: (proc) => {
            // After kill, delay close so stop can run while awaiting_resume.
            const prevKill = proc.kill;
            proc.kill = vi.fn((signal?: string) => {
              prevKill(signal);
              setTimeout(() => {
                feedAssistant(proc, 'late close after awaiting_resume stop');
                closeProc(proc, 0);
              }, 50);
            });
          },
        },
      ],
    };
    const handle = await startCursorAutoloop(runId, workspace);
    const chatP = mgr.autoloopChat(runId, 'D4');
    await waitUntil(() => expect(coderSpawns().length).toBe(1));
    await vi.advanceTimersByTimeAsync(SEND_MS);
    await chatP;
    expect(handle.runner.state.status).toBe('paused');
    expect(handle.runner.state.status_reason).toMatch(/^awaiting_resume:send_timeout:coder:/);
    expect(coderSpawns()[0].closed).toBe(false);
    await mgr.autoloopStop(runId, 'stop-while-awaiting-resume');
    expect(handle.runner.state.status).toBe('terminated');
    await tick(50);
    await waitUntil(() => expect(coderSpawns()[0].closed).toBe(true));
    observations.D4 = {
      stoppedWhileAwaitingResume: true,
      lateClosed: coderSpawns()[0].closed,
      closeCode: coderSpawns()[0].closeCode,
      killSignal: coderSpawns()[0].killSignal,
      finalStatus: handle.runner.state.status,
      note: 'stop WHILE awaiting_resume, then late close — distinct from O8 stop-during-send',
    };
    assertSentinelsUnchanged('D4', workspace, beforeSent);
  });

  it('O9: Reviewer timeout/resume variants on Cursor engine', async () => {
    const runId = 'phaseA-o9';
    const workspace = path.join(TEST_WF_DIR, 'ws-o9');
    initGitWorkspace(workspace);
    const beforeSent = captureSentinelDigests('O9', workspace, 'before');

    // First complete a coder iter so a review_request is issued, then hang reviewer.
    scriptedByKind = {
      planner: [
        {
          action: 'complete',
          text: fence('spawn_subagents', {
            initial_directive: {
              goal: 'O9 reviewer timeout',
              constraints: [],
              success_criteria: [],
              max_attempts: 1,
            },
          }),
        },
      ],
      coder: [
        {
          action: 'custom',
          run: (proc) => {
            fs.writeFileSync(path.join(workspace, 'o9-proof.txt'), 'o9-proof' + NL);
            feedAssistant(
              proc,
              fence('iter_complete', {
                summary: 'O9 coder done',
                eval_output: { metric: 1, gates: [] },
              }),
            );
            closeProc(proc, 0);
          },
        },
      ],
      reviewer: [{ action: 'hang' }],
    };

    const handle = await startCursorAutoloop(runId, workspace);
    const timeouts: unknown[] = [];
    handle.runner.on('send_timeout', (e) => timeouts.push(e));

    const chatP = mgr.autoloopChat(runId, 'O9');
    await waitUntil(() => expect(reviewerSpawns().length).toBe(1));
    await vi.advanceTimersByTimeAsync(SEND_MS);
    await chatP;

    expect(handle.runner.state.status).toBe('paused');
    const pending = handle.runner.state.pending_dispatch!;
    expect(pending.agent).toBe('reviewer');
    expect(handle.runner.state.status_reason).toBe(`awaiting_resume:send_timeout:reviewer:${pending.dispatch_id}`);
    expect(timeouts).toHaveLength(1);
    expect(readDecisionRows(workspace, runId).filter((r) => r.kind === 'send_timeout')).toHaveLength(1);
    expect(readDecisionRows(workspace, runId).filter((r) => r.kind === 'reset_agent')).toHaveLength(0);

    // O4a for reviewer: no-option resume is no-op
    const auditPath = path.join(ledgerDir(workspace, runId), 'decisions.jsonl');
    const chatPath = path.join(ledgerDir(workspace, runId), 'chat.jsonl');
    const specPath = path.join(TEST_WF_DIR, runId, 'spec.json');
    const sentinels = sentinelPaths(workspace);
    const before = {
      status: handle.runner.state.status,
      reason: handle.runner.state.status_reason,
      pending: JSON.stringify(handle.runner.state.pending_dispatch),
      timeout: handle.dispatcher.config.sendTimeoutMs,
      sends: reviewerSpawns().length,
      audit: fs.readFileSync(auditPath, 'utf8'),
      chat: fs.readFileSync(chatPath, 'utf8'),
      spec: fs.existsSync(specPath) ? fs.readFileSync(specPath, 'utf8') : '',
      work: sha256File(sentinels.work),
      evidence: sha256File(sentinels.evidence),
      activity: handle.runner.state.last_activity_at,
    };
    await expect(mgr.autoloopResume(runId)).resolves.toBe(handle.runner.state);
    expect(handle.runner.state.status).toBe(before.status);
    expect(handle.runner.state.status_reason).toBe(before.reason);
    expect(JSON.stringify(handle.runner.state.pending_dispatch)).toBe(before.pending);
    expect(handle.runner.state.last_activity_at).toBe(before.activity);
    expect(fs.readFileSync(auditPath, 'utf8')).toBe(before.audit);

    const resumeWith = (opts: Record<string, unknown>) =>
      mgr.autoloopResume(
        runId,
        opts as unknown as Parameters<InstanceType<typeof SessionManager>['autoloopResume']>[1],
      );
    // O4b refusals with byte-level atomicity (equal/decreased/out-of-range/stale-id).
    for (const override of [
      { sendTimeoutMs: SEND_MS, pendingDispatchId: pending.dispatch_id },
      { sendTimeoutMs: SEND_MS - 1, pendingDispatchId: pending.dispatch_id },
      { sendTimeoutMs: 4_999, pendingDispatchId: pending.dispatch_id },
      { sendTimeoutMs: 7_200_001, pendingDispatchId: pending.dispatch_id },
      { sendTimeoutMs: RAISED_MS, pendingDispatchId: 'dispatch_stale_identity' },
    ]) {
      await expect(resumeWith(override)).rejects.toThrow();
      expect(handle.runner.state.status).toBe(before.status);
      expect(handle.runner.state.status_reason).toBe(before.reason);
      expect(JSON.stringify(handle.runner.state.pending_dispatch)).toBe(before.pending);
      expect(handle.dispatcher.config.sendTimeoutMs).toBe(before.timeout);
      expect(reviewerSpawns()).toHaveLength(before.sends);
      expect(fs.readFileSync(auditPath, 'utf8')).toBe(before.audit);
      expect(fs.readFileSync(chatPath, 'utf8')).toBe(before.chat);
      if (before.spec) expect(fs.readFileSync(specPath, 'utf8')).toBe(before.spec);
      expect(sha256File(sentinels.work)).toBe(before.work);
      expect(sha256File(sentinels.evidence)).toBe(before.evidence);
    }

    // O5 for reviewer
    const auditBefore = before.audit;
    const reviewerBefore = before.sends;
    await mgr.autoloopResume(runId, {
      sendTimeoutMs: RAISED_MS,
      pendingDispatchId: pending.dispatch_id,
    } as never);
    expect(handle.runner.state).toMatchObject({ status: 'running', status_reason: null, pending_dispatch: null });
    expect(handle.dispatcher.config.sendTimeoutMs).toBe(RAISED_MS);
    expect(reviewerSpawns()).toHaveLength(reviewerBefore);
    const migrations = fs
      .readFileSync(path.join(ledgerDir(workspace, runId), 'decisions.jsonl'), 'utf8')
      .trim()
      .split(NL)
      .map((l) => JSON.parse(l) as Record<string, unknown>)
      .filter((r) => r.kind === 'timeout_migration');
    expect(migrations).toHaveLength(1);
    expect(migrations[0]).toMatchObject({
      reason: 'recoverable_send_timeout_resume',
      pendingDispatchId: pending.dispatch_id,
      oldValue: SEND_MS,
      newValue: RAISED_MS,
    });
    expect(
      fs.readFileSync(path.join(ledgerDir(workspace, runId), 'decisions.jsonl'), 'utf8').startsWith(auditBefore),
    ).toBe(true);

    // O6 for reviewer
    const { Msg } = await import('../autoloop/messages.js');
    const auditAfter = fs.readFileSync(path.join(ledgerDir(workspace, runId), 'decisions.jsonl'), 'utf8');
    await handle.runner.send(Msg.sendTimeout(0, pending));
    expect(handle.runner.state).toMatchObject({ status: 'running', pending_dispatch: null });
    await expect(
      mgr.autoloopResume(runId, {
        sendTimeoutMs: RAISED_MS,
        pendingDispatchId: pending.dispatch_id,
      } as never),
    ).rejects.toThrow(/not awaiting.*send timeout/i);
    expect(fs.readFileSync(path.join(ledgerDir(workspace, runId), 'decisions.jsonl'), 'utf8')).toBe(auditAfter);
    assertSentinelsUnchanged('O9', workspace, beforeSent);
  });
});
