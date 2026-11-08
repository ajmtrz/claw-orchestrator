/**
 * Council preflight, modes, manifest, persistence, post-processing refusals,
 * non-opted byte-identity, and dashboard reload of escaped failures.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import vm from 'node:vm';
import { EventEmitter } from 'node:events';
import { execFileSync } from 'node:child_process';
import type { ISession, SessionConfig } from '../types.js';
import {
  validateManifest,
  validateInputPathForm,
  selectMode,
  normalizeEffectiveMembers,
  classifyCapability,
  needsPreflight,
  PREFLIGHT_REASONS,
} from '../council-preflight.js';

const HOME = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'clawo-pf-home-'));
process.env.HOME = HOME;
process.env.USERPROFILE = HOME;
process.env.CLAWO_WF_DIR = path.join(HOME, 'wf');
process.env.CLAWO_NO_EMBEDDED_SERVER = '1';

const { SessionManager } = await import('../session-manager.js');
type SessionManager = InstanceType<typeof SessionManager>;

interface Observed {
  config: Partial<SessionConfig>;
  messages: string[];
}

let observed: Observed[] = [];
let mgr: SessionManager;

class FakeSession extends EventEmitter implements Partial<ISession> {
  sessionId = `fake-${Math.random().toString(36).slice(2, 8)}`;
  private stats = {
    turns: 0,
    turnsSucceeded: 0,
    toolCalls: 0,
    toolErrors: 0,
    tokensIn: 0,
    tokensOut: 0,
    cachedTokens: 0,
    costUsd: 0,
    isReady: true,
    startTime: new Date().toISOString(),
    lastActivity: new Date().toISOString(),
    contextPercent: 0,
    retries: 0,
  };
  constructor(private readonly config: Partial<SessionConfig>) {
    super();
  }
  async start() {
    return this;
  }
  async send(message: string) {
    this.stats.turns++;
    this.stats.turnsSucceeded++;
    observed.find((o) => o.config.name === this.config.name)?.messages.push(message);
    // sendMessage only forwards result.text (session-manager.ts:1383-1420); {output} is dropped.
    const text = `[${this.config.name}] ok\n[CONSENSUS: YES]`;
    return { text, event: { type: 'result', subtype: 'success', result: text, is_error: false } };
  }
  stop() {}
  getStats() {
    return { ...this.stats };
  }
  getCost() {
    return { totalCostUsd: 0, totalUsd: 0 };
  }
  getHistory() {
    return [];
  }
  isReady() {
    return true;
  }
}

function makeManager(): SessionManager {
  const m = new SessionManager({ logger: { info() {}, warn() {}, error() {}, debug() {} } as never });
  (m as unknown as { _createSession: (e: string, c: Partial<SessionConfig>) => ISession })._createSession = (
    _e,
    config,
  ) => {
    observed.push({ config, messages: [] });
    return new FakeSession(config) as unknown as ISession;
  };
  return m;
}

function gitRepo(): string {
  const dir = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'clawo-pf-repo-'));
  const git = (...a: string[]) => execFileSync('git', ['-C', dir, ...a], { stdio: 'pipe' });
  git('init', '-b', 'main');
  git('config', 'user.email', 't@example.com');
  git('config', 'user.name', 't');
  fs.writeFileSync(path.join(dir, 'tracked.txt'), 'tracked\n');
  git('add', '-A');
  git('commit', '-m', 'base');
  return dir;
}

beforeEach(() => {
  observed = [];
  process.env.CLAWO_WF_DIR = path.join(HOME, `wf-${Date.now()}`);
  mgr = makeManager();
});

afterEach(async () => {
  await mgr.shutdown();
});

describe('preflight unit helpers', () => {
  it('rejects invalid input path forms without echoing them', () => {
    expect(validateInputPathForm('../x')).toBe(false);
    expect(validateInputPathForm('/abs')).toBe(false);
    expect(validateInputPathForm('ok/file.txt')).toBe(true);
    const fails = validateManifest([{ path: '../x', sha256: 'a'.repeat(64) }]);
    expect(fails[0]?.reason).toBe('input-invalid');
    expect(JSON.stringify(fails)).not.toContain('../');
  });

  it('uses only the fixed reason vocabulary', () => {
    for (const r of PREFLIGHT_REASONS) {
      expect(typeof r).toBe('string');
    }
  });

  it('selects read-only when all members are read-only-effective', () => {
    const members = normalizeEffectiveMembers([
      { name: 'a', emoji: '', persona: 'p', engine: 'claude', permissionMode: 'plan' },
      { name: 'b', emoji: '', persona: 'p', engine: 'codex', sandboxMode: 'read-only' },
    ]);
    expect(selectMode(members).mode).toBe('read-only');
  });

  it('fails contract-mixed when RO and writable are combined', () => {
    const members = normalizeEffectiveMembers([
      { name: 'a', emoji: '', persona: 'p', engine: 'claude', sandboxMode: 'read-only' },
      { name: 'b', emoji: '', persona: 'p', engine: 'agy' },
    ]);
    expect(selectMode(members).failure?.reason).toBe('contract-mixed');
  });

  it('classifies dontAsk in git mode as unknown capability', () => {
    const [m] = normalizeEffectiveMembers([
      { name: 'a', emoji: '', persona: 'p', engine: 'claude', permissionMode: 'dontAsk' },
    ]);
    expect(classifyCapability(m!, 'git', '/tmp')).toBe('unknown');
  });

  it('does not opt into preflight when nothing is strict and inputs absent', () => {
    expect(needsPreflight([{ name: 'a', emoji: '', persona: 'p', engine: 'claude' }], undefined)).toBe(false);
  });
});

describe('council preflight via workflowStart', () => {
  it('forwards sandboxMode read-only and uses read-only prompts', { timeout: 60_000 }, async () => {
    const cwd = gitRepo();
    const agents = [
      { name: 'agy-ro', engine: 'agy' as const, persona: 'r', sandboxMode: 'read-only' as const },
      { name: 'codex-ro', engine: 'codex' as const, persona: 'r', sandboxMode: 'read-only' as const },
      { name: 'cursor-ro', engine: 'cursor' as const, persona: 'r', sandboxMode: 'read-only' as const },
      { name: 'claude-ro', engine: 'claude' as const, persona: 'r', sandboxMode: 'read-only' as const },
    ];
    const rec = await mgr.workflowStart({
      name: 'pf-ro',
      cwd,
      nodes: [
        { id: 'c', kind: 'council', task: 'review the tracked file carefully', projectDir: cwd, maxRounds: 1, agents },
      ],
    });
    const done = await (
      mgr as unknown as { kernel: { wait(id: string): Promise<{ state: string }> } }
    ).kernel.wait(rec.runId);
    expect(done.state).toBe('completed');
    for (const a of agents) {
      const start = observed.find((o) => String(o.config.name).includes(a.name))!;
      expect(start.config.sandboxMode).toBe('read-only');
      expect(start.messages[0]).toMatch(/read-only/i);
      expect(start.messages[0]).not.toMatch(/\bmerge\b|\bcommit\b|\bpush\b|plan\.md/i);
    }
    expect(observed).toHaveLength(agents.length);
    const { loadRun } = await import('../kernel/store.js');
    const record = loadRun(rec.runId)!;
    expect(record.state).toBe('completed');
    const node = Object.values(record.nodes)[0]!;
    expect(node.state).toBe('succeeded');
    const data = node.data as {
      preflight?: { mode: string; ok: boolean };
      councilStatus?: string;
      responses?: Array<{ agent: string; content?: string }>;
    };
    expect(data.preflight?.mode).toBe('read-only');
    expect(data.preflight?.ok).toBe(true);
    expect(data.responses).toHaveLength(agents.length);
    for (const r of data.responses ?? []) {
      expect(r.content?.trim().length).toBeGreaterThan(0);
      expect(r.content).toMatch(/CONSENSUS:\s*YES/i);
    }
    expect(data.councilStatus).toBe('awaiting_user');
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('refuses dontAsk before any session (capability-unknown)', { timeout: 60_000 }, async () => {
    const cwd = gitRepo();
    const rec = await mgr.workflowStart({
      name: 'pf-dontask',
      cwd,
      nodes: [
        {
          id: 'c',
          kind: 'council',
          task: 'review the tracked file carefully',
          projectDir: cwd,
          maxRounds: 1,
          agents: [{ name: 'da', engine: 'claude', persona: 'r', permissionMode: 'dontAsk' }],
        },
      ],
    });
    const done = await (
      mgr as unknown as { kernel: { wait(id: string): Promise<{ state: string }> } }
    ).kernel.wait(rec.runId);
    expect(done.state).toBe('failed');
    expect(observed.length).toBe(0);
    const { loadRun } = await import('../kernel/store.js');
    const record = loadRun(rec.runId)!;
    expect(record.state).toBe('failed');
    const node = Object.values(record.nodes)[0]!;
    expect(node.state).toBe('failed');
    const data = node.data as {
      preflight?: { ok: boolean; failures: Array<{ reason: string; correction?: string }>; members?: unknown[] };
      councilStatus?: string;
      responses?: unknown[];
    };
    expect(data.preflight?.ok).toBe(false);
    expect(data.preflight?.failures.some((f) => f.reason === 'capability-unknown')).toBe(true);
    expect(data.preflight?.failures.some((f) => f.correction === 'sandboxMode read-only')).toBe(true);
    expect(data.councilStatus).toBe('error');
    expect(data.responses ?? []).toHaveLength(0);
    expect(data.preflight?.members ?? []).toHaveLength(0);
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('delivers a declared input from source and verifies hash', { timeout: 60_000 }, async () => {
    const cwd = gitRepo();
    const external = path.join(fs.realpathSync(os.tmpdir()), `clawo-pf-ext-${Date.now()}.txt`);
    const body = 'snapshot-bytes\n';
    fs.writeFileSync(external, body);
    const sha256 = crypto.createHash('sha256').update(body).digest('hex');
    const rec = await mgr.workflowStart({
      name: 'pf-input',
      cwd,
      nodes: [
        {
          id: 'c',
          kind: 'council',
          task: 'review the tracked file carefully',
          projectDir: cwd,
          maxRounds: 1,
          agents: [{ name: 'ro', engine: 'codex', persona: 'r', sandboxMode: 'read-only' }],
          inputs: [{ path: 'docs/in.txt', sha256, source: external }],
        },
      ],
    });
    const done = await (
      mgr as unknown as { kernel: { wait(id: string): Promise<{ state: string }> } }
    ).kernel.wait(rec.runId);
    expect(done.state).toBe('completed');
    const start = observed[0]!;
    expect(start.config.sandboxMode).toBe('read-only');
    expect(start.messages[0]?.length).toBeGreaterThan(0);
    const { loadRun } = await import('../kernel/store.js');
    const record = loadRun(rec.runId)!;
    const node = Object.values(record.nodes)[0]!;
    expect(node.state).toBe('succeeded');
    const data = node.data as {
      preflight?: {
        ok: boolean;
        members: Array<{ inputs: Array<{ path: string; sha256: string; ok: boolean }>; view: string }>;
      };
      responses?: Array<{ content?: string }>;
      councilStatus?: string;
    };
    expect(data.preflight?.ok).toBe(true);
    expect(data.preflight?.members[0]?.inputs[0]).toMatchObject({ path: 'docs/in.txt', sha256, ok: true });
    expect(data.responses).toHaveLength(1);
    expect(data.responses![0]?.content).toMatch(/CONSENSUS:\s*YES/i);
    expect(data.councilStatus).toBe('awaiting_user');
    // Session was bound to the preflight view path (views may be removed when unchanged after the run).
    expect(String(start.config.cwd)).toBe(data.preflight?.members[0]?.view);
    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(external, { force: true });
  });

  it('returns unsupported-operation for accept/reject/review on read-only councils', { timeout: 60_000 }, async () => {
    const cwd = gitRepo();
    const rec = await mgr.workflowStart({
      name: 'pf-post',
      cwd,
      nodes: [
        {
          id: 'c',
          kind: 'council',
          task: 'review the tracked file carefully',
          projectDir: cwd,
          maxRounds: 1,
          agents: [{ name: 'ro', engine: 'codex', persona: 'r', sandboxMode: 'read-only' }],
        },
      ],
    });
    const done = await (
      mgr as unknown as { kernel: { wait(id: string): Promise<{ state: string }> } }
    ).kernel.wait(rec.runId);
    expect(done.state).toBe('completed');
    const { loadRun } = await import('../kernel/store.js');
    const record = loadRun(rec.runId)!;
    const node = Object.values(record.nodes)[0]!;
    expect(node.state).toBe('succeeded');
    const data = node.data as { councilStatus?: string; responses?: Array<{ content?: string }>; preflight?: { mode: string } };
    expect(data.preflight?.mode).toBe('read-only');
    expect(data.responses?.[0]?.content).toMatch(/CONSENSUS:\s*YES/i);
    expect(data.councilStatus).toBe('awaiting_user');

    const accept = await mgr.councilAccept(rec.runId);
    expect(accept.unsupportedOperation).toBe(true);
    expect(accept.reason).toBe('read-only-council');
    expect(accept.branchesDeleted).toEqual([]);
    const reject = await mgr.councilReject(rec.runId, 'nope');
    expect(reject.unsupportedOperation).toBe(true);
    expect(reject.planRewritten).toBe(false);
    const review = await mgr.councilReview(rec.runId);
    expect(review.unsupportedOperation).toBe(true);

    // Reconstructed session (no live handle): same fixed unsupported-operation result.
    const { Council } = await import('../council.js');
    const { toCouncilSession } = await import('../kernel/projections.js');
    const reconstructed = new Council(toCouncilSession(record).config, mgr, {
      info() {},
      warn() {},
      error() {},
      debug() {},
    } as never);
    reconstructed.adoptSession(toCouncilSession(record));
    const accept2 = await reconstructed.accept();
    expect(accept2.unsupportedOperation).toBe(true);
    expect(accept2.reason).toBe('read-only-council');
    fs.rmSync(cwd, { recursive: true, force: true });
  });

  it('keeps non-opted councils on the git worktree path (byte-compatible defaults)', { timeout: 60_000 }, async () => {
    const cwd = gitRepo();
    const rec = await mgr.workflowStart({
      name: 'pf-legacy',
      cwd,
      nodes: [
        {
          id: 'c',
          kind: 'council',
          task: 'review the tracked file carefully',
          projectDir: cwd,
          maxRounds: 1,
          agents: [{ name: 'A', engine: 'codex', persona: 'reviewer' }],
        },
      ],
    });
    const done = await (
      mgr as unknown as { kernel: { wait(id: string): Promise<{ state: string }> } }
    ).kernel.wait(rec.runId);
    expect(done.state).toBe('completed');
    expect(observed[0]?.config.sandboxMode).toBeUndefined();
    expect(observed[0]?.config.permissionMode).toBe('bypassPermissions');
    expect(String(observed[0]?.config.cwd)).toContain(path.join('.worktrees', 'A'));
    expect(observed[0]?.messages[0]?.length).toBeGreaterThan(0);
    const { loadRun } = await import('../kernel/store.js');
    const record = loadRun(rec.runId)!;
    const node = Object.values(record.nodes)[0]!;
    expect(node.state).toBe('succeeded');
    const data = node.data as { preflight?: unknown; responses?: Array<{ content?: string }>; councilStatus?: string };
    expect(data.preflight).toBeUndefined();
    expect(data.responses).toHaveLength(1);
    expect(data.responses![0]?.content).toMatch(/CONSENSUS:\s*YES/i);
    expect(data.councilStatus).toBe('awaiting_user');
    fs.rmSync(cwd, { recursive: true, force: true });
  });
});

describe('dashboard openCouncil preflight reload', () => {
  it('renders escaped failure entries with zero responses', () => {
    const html = fs.readFileSync(path.join(process.cwd(), 'src/dashboard/index.html'), 'utf8');
    const start = html.indexOf('async function openCouncil(id)');
    const end = html.indexOf('function paintCouncilState()');
    expect(start).toBeGreaterThan(0);
    const snippet = html.slice(start, end);
    // Extract escapeHtml + openCouncil + paintCouncilStream helpers into a stub DOM
    const escapeStart = html.indexOf('function escapeHtml');
    const escapeFn = html.slice(escapeStart, html.indexOf('\n      function', escapeStart + 10));
    const paintStart = html.indexOf('function paintCouncilStream()');
    const paintFn = html.slice(paintStart, html.indexOf('\n      // ─── Modal', paintStart));

    const events: Array<{ who: string; text: string }> = [];
    const store: Record<string, { innerHTML: string; textContent: string }> = {};
    const sandbox = {
      state: { council: { session: null as unknown, events, eventSource: null } },
      fetch: async () => ({
        json: async () => ({
          ok: true,
          session: {
            status: 'error',
            responses: [],
            config: { agents: [] },
            task: 't',
            startTime: '2026-01-01T00:00:00.000Z',
            preflight: {
              ok: false,
              failures: [
                {
                  check: 'capability',
                  subject: 'git',
                  reason: 'capability-unknown',
                  correction: 'sandboxMode read-only',
                },
              ],
            },
          },
        }),
      }),
      EventSource: class {
        addEventListener() {}
      },
      encodeURIComponent,
      $: (id: string) => {
        if (!store[id]) store[id] = { innerHTML: '', textContent: '' };
        return store[id];
      },
      renderCouncilShell: () => {},
      paintCouncilState: () => {},
      fmtTime: () => '',
      console,
    };
    vm.createContext(sandbox);
    vm.runInContext(`${escapeFn}\n${paintFn}\n${snippet}\nopenCouncil('x');`, sandbox);
    // Allow the async openCouncil to settle
    return new Promise<void>((resolve) => {
      setImmediate(() => {
        expect(events.length).toBe(1);
        expect(events[0]?.who).toBe('system');
        expect(events[0]?.text).toContain('capability-unknown');
        expect(events[0]?.text).not.toMatch(/<script/);
        resolve();
      });
    });
  });
});
