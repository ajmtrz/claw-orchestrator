/**
 * RED — council sandboxMode drop on the real workflowStart → council route.
 *
 * At base, FanoutAgentSpec declares sandboxMode and a council node accepts it,
 * but executeCouncilNode and Council.runSingleAgent drop it before startSession.
 * Exact assertion: every recorded startSession config has sandboxMode 'read-only'.
 * Expected at base: `expected undefined to be 'read-only'`.
 *
 * Base APIs only: SessionManager.workflowStart with a council node. Fake engines
 * via the _createSession seam (same pattern as invariants.test.ts).
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { execFileSync } from 'node:child_process';
import type { ISession, SessionConfig } from '../types.js';

const HOME = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-council-sandbox-red-'));
process.env.HOME = HOME;
process.env.USERPROFILE = HOME;
process.env.CLAWO_WF_DIR = path.join(HOME, 'wf');
const { SessionManager } = await import('../session-manager.js');
type SessionManager = InstanceType<typeof SessionManager>;

interface Observed {
  config: Partial<SessionConfig>;
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
  async start(): Promise<unknown> {
    return this;
  }
  async send(_message: string): Promise<{ output: string; events: never[] }> {
    this.stats.turns++;
    this.stats.turnsSucceeded++;
    return { output: `[${this.config.name}] ok\n[CONSENSUS: YES]`, events: [] };
  }
  stop(): void {}
  getStats(): typeof this.stats {
    return { ...this.stats };
  }
  getCost(): { totalCostUsd: number; totalUsd: number } {
    return { totalCostUsd: 0, totalUsd: 0 };
  }
  getHistory(): never[] {
    return [];
  }
  isReady(): boolean {
    return true;
  }
}

function makeManager(): SessionManager {
  const m = new SessionManager({ logger: { info() {}, warn() {}, error() {}, debug() {} } as never });
  (m as unknown as { _createSession: (engine: string, c: Partial<SessionConfig>) => ISession })._createSession = (
    _engine,
    config,
  ) => {
    observed.push({ config });
    return new FakeSession(config) as unknown as ISession;
  };
  return m;
}

function gitRepo(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-council-sandbox-repo-'));
  const git = (...a: string[]): void => {
    execFileSync('git', ['-C', dir, ...a], { stdio: 'pipe' });
  };
  git('init', '-b', 'main');
  git('config', 'user.email', 't@example.com');
  git('config', 'user.name', 't');
  fs.writeFileSync(path.join(dir, 'a.txt'), 'one\n');
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

describe('council sandboxMode forwarding (workflowStart route)', () => {
  it(
    'forwards sandboxMode read-only on every startSession for agy, codex, cursor, claude',
    { timeout: 60_000 },
    async () => {
      const cwd = gitRepo();
      const agents = [
        { name: 'agy-ro', engine: 'agy' as const, persona: 'review', sandboxMode: 'read-only' as const },
        { name: 'codex-ro', engine: 'codex' as const, persona: 'review', sandboxMode: 'read-only' as const },
        { name: 'cursor-ro', engine: 'cursor' as const, persona: 'review', sandboxMode: 'read-only' as const },
        { name: 'claude-ro', engine: 'claude' as const, persona: 'review', sandboxMode: 'read-only' as const },
      ];

      const rec = await mgr.workflowStart({
        name: 'council-sandbox-red',
        cwd,
        nodes: [
          {
            id: 'c',
            kind: 'council',
            task: 'review the tree',
            projectDir: cwd,
            maxRounds: 1,
            agents,
          },
        ],
      });
      await (mgr as unknown as { kernel: { wait(id: string): Promise<unknown> } }).kernel.wait(rec.runId);

      const memberStarts = observed.filter((o) =>
        agents.some((a) => String(o.config.name ?? '').includes(a.name)),
      );
      expect(memberStarts.length).toBe(agents.length);

      for (const start of memberStarts) {
        expect(start.config.sandboxMode).toBe('read-only');
      }

      fs.rmSync(cwd, { recursive: true, force: true });
    },
  );
});
