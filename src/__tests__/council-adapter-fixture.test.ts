/**
 * Controlled actual-adapter council round: real wrapper classes, no-LLM fixture
 * executables via binary overrides. Host compares fixture-observed opens/hashes
 * and argv to the manifest and wrapper read-only mappings. Native policy denial
 * is not claimed — view-content evidence only.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';

const FIX = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'clawo-adapter-fix-'));
const HOME = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'clawo-adapter-home-'));
process.env.HOME = HOME;
process.env.USERPROFILE = HOME;
process.env.CLAWO_WF_DIR = path.join(HOME, 'wf');
process.env.CLAWO_NO_EMBEDDED_SERVER = '1';
process.env.CODEX_HOME = path.join(HOME, 'codex-home');
fs.mkdirSync(process.env.CODEX_HOME, { recursive: true });

type FixtureKind = 'claude' | 'codex' | 'cursor' | 'agy';

/**
 * Bake wire output into each fixture from the shapes the real wrappers parse
 * (persistent-session / codex-session / cursor-session / agy-session tests).
 * No shared FIXTURE_KIND env. Claude is persistent (stdin protocol); others are one-shot.
 */
function writeFixture(name: string, kind: FixtureKind): string {
  const bin = path.join(FIX, name);
  const log = path.join(FIX, `${name}.jsonl`);
  const script = `#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const readline = require('readline');
const kind = ${JSON.stringify(kind)};
const logPath = ${JSON.stringify(log)};
const cwd = process.cwd();
const argv = process.argv.slice(2);
const reply = 'adapter-ok [CONSENSUS: YES]';

function snapshot() {
  // Cursor RO spawns with throwaway cwd + --workspace <view> (persistent-cursor-session.ts:138-156).
  const wi = argv.indexOf('--workspace');
  const workspace = wi >= 0 && argv[wi + 1] ? argv[wi + 1] : cwd;
  const opens = [];
  const hashes = {};
  const seen = [];
  function walk(dir, base) {
    for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        if (ent.name === '.git' || ent.name === '.worktrees') continue;
        walk(full, base);
      } else if (ent.isFile()) {
        const rel = path.relative(base, full).split(path.sep).join('/');
        seen.push(rel);
        if (rel === 'docs/in.txt' || rel.startsWith('docs/')) {
          opens.push(rel);
          hashes[rel] = crypto.createHash('sha256').update(fs.readFileSync(full)).digest('hex');
        }
      }
    }
  }
  try { walk(workspace, workspace); } catch (e) {}
  fs.appendFileSync(logPath, JSON.stringify({ kind, argv, cwd, workspace, opens, hashes, seen, ts: Date.now() }) + '\\n');
}

const li = argv.indexOf('--log-file');
if (li >= 0 && argv[li + 1]) {
  try {
    fs.writeFileSync(argv[li + 1], 'I0705 server.go:825] Created conversation aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee\\n');
  } catch (e) {}
}

if (kind === 'claude') {
  // Persistent Claude stream-json: emit init, then one result per stdin user message
  // (persistent-session.ts send payload + persistent-session.test.ts result shape).
  console.log(JSON.stringify({ type: 'system', subtype: 'init', session_id: 'fixture-claude-sess' }));
  const rl = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  rl.on('line', (line) => {
    let uuid = null;
    try {
      const msg = JSON.parse(line);
      if (msg && msg.type === 'user' && typeof msg.uuid === 'string') uuid = msg.uuid;
    } catch (e) {}
    snapshot();
    console.log(JSON.stringify({
      type: 'result',
      subtype: 'success',
      result: reply,
      is_error: false,
      total_cost_usd: 0,
      user_message_uuids: uuid ? [uuid] : [],
    }));
  });
  // Stay alive until the wrapper stops the process.
  setInterval(() => {}, 1 << 30);
} else {
  snapshot();
  if (kind === 'codex') {
    console.log(JSON.stringify({ type: 'thread.started', thread_id: 'fixture-thread-1' }));
    console.log(JSON.stringify({ type: 'item.completed', item: { type: 'agent_message', text: reply } }));
    console.log(JSON.stringify({ type: 'turn.completed', usage: { input_tokens: 1, output_tokens: 1 } }));
  } else if (kind === 'cursor') {
    console.log(JSON.stringify({ type: 'result', result: reply, usage: { inputTokens: 1, outputTokens: 1 } }));
  } else if (kind === 'agy') {
    console.log(JSON.stringify({ event: 'init', conversation_id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' }));
    console.log(JSON.stringify({
      event: 'result',
      result: { conversation_id: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', status: 'SUCCESS', response: reply },
    }));
  }
  process.exit(0);
}
`;
  fs.writeFileSync(bin, script, { mode: 0o755 });
  return bin;
}

const claudeBin = writeFixture('fake-claude', 'claude');
const codexBin = writeFixture('fake-codex', 'codex');
const cursorBin = writeFixture('fake-cursor', 'cursor');
const agyBin = writeFixture('fake-agy', 'agy');

process.env.CODEX_BIN = codexBin;
process.env.CURSOR_BIN = cursorBin;
process.env.AGY_BIN = agyBin;

const { SessionManager } = await import('../session-manager.js');

function gitRepo(): string {
  const dir = fs.mkdtempSync(path.join(FIX, 'repo-'));
  const git = (...a: string[]) => execFileSync('git', ['-C', dir, ...a], { stdio: 'pipe' });
  git('init', '-b', 'main');
  git('config', 'user.email', 't@example.com');
  git('config', 'user.name', 't');
  fs.writeFileSync(path.join(dir, 'tracked.txt'), 'tracked\n');
  git('add', '-A');
  git('commit', '-m', 'base');
  return dir;
}

function readRows(name: string): Array<{
  kind: string;
  argv: string[];
  cwd: string;
  workspace: string;
  opens: string[];
  hashes: Record<string, string>;
  seen: string[];
}> {
  const p = path.join(FIX, `${name}.jsonl`);
  if (!fs.existsSync(p)) return [];
  return fs
    .readFileSync(p, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

describe('council controlled adapter fixture round', () => {
  let mgr: InstanceType<typeof SessionManager>;

  beforeAll(() => {
    // SessionManager(Partial<PluginConfig>, logger?) — claudeBin is top-level.
    mgr = new SessionManager(
      { claudeBin, maxConcurrentSessions: 8 },
      { info() {}, warn() {}, error() {}, debug() {} } as never,
    );
  });

  afterAll(async () => {
    await mgr.shutdown();
    fs.rmSync(FIX, { recursive: true, force: true });
    fs.rmSync(HOME, { recursive: true, force: true });
  });

  it(
    'four engines complete a read-only round with host-verified inputs and RO argv',
    { timeout: 180_000 },
    async () => {
      const cwd = gitRepo();
      const external = path.join(FIX, 'external-snapshot.txt');
      const body = 'adapter-fixture-bytes\n';
      fs.writeFileSync(external, body);
      const sha256 = crypto.createHash('sha256').update(body).digest('hex');
      // Undeclared sibling next to the source (must not appear in any view).
      fs.writeFileSync(path.join(FIX, 'undeclared-sibling.txt'), 'secret-sibling\n');
      // Undeclared untracked file inside projectDir, not present at the bound base.
      fs.writeFileSync(path.join(cwd, 'undeclared-untracked.txt'), 'secret-untracked\n');

      const agents = [
        { name: 'claude-m', engine: 'claude' as const, persona: 'r', sandboxMode: 'read-only' as const },
        { name: 'codex-m', engine: 'codex' as const, persona: 'r', sandboxMode: 'read-only' as const },
        { name: 'cursor-m', engine: 'cursor' as const, persona: 'r', sandboxMode: 'read-only' as const },
        { name: 'agy-m', engine: 'agy' as const, persona: 'r', sandboxMode: 'read-only' as const },
      ];
      const fixtureByMember: Record<string, string> = {
        'claude-m': 'fake-claude',
        'codex-m': 'fake-codex',
        'cursor-m': 'fake-cursor',
        'agy-m': 'fake-agy',
      };

      for (const n of Object.values(fixtureByMember)) {
        fs.writeFileSync(path.join(FIX, `${n}.jsonl`), '');
      }

      const rec = await mgr.workflowStart({
        name: 'adapter-round',
        cwd,
        nodes: [
          {
            id: 'c',
            kind: 'council',
            task: 'review the declared input carefully now',
            projectDir: cwd,
            maxRounds: 1,
            agents,
            inputs: [{ path: 'docs/in.txt', sha256, source: external }],
          },
        ],
      });
      const done = await (
        mgr as unknown as { kernel: { wait(id: string): Promise<{ state: string }> } }
      ).kernel.wait(rec.runId);

      expect(done.state).toBe('completed');

      const { loadRun } = await import('../kernel/store.js');
      const record = loadRun(rec.runId)!;
      expect(record.state).toBe('completed');
      const node = Object.values(record.nodes)[0]!;
      expect(node.state).toBe('succeeded');
      expect(node.error).toBeUndefined();
      const data = node.data as {
        councilStatus?: string;
        responses?: Array<{ agent: string; content?: string }>;
        preflight?: {
          ok: boolean;
          mode: string;
          members: Array<{
            member: string;
            view: string;
            inputs: Array<{ path: string; sha256: string; ok: boolean }>;
          }>;
        };
      };
      expect(data.preflight?.ok).toBe(true);
      expect(data.preflight?.mode).toBe('read-only');
      expect(data.preflight?.members).toHaveLength(4);
      expect(data.responses).toHaveLength(4);
      for (const a of agents) {
        const resp = data.responses!.find((r) => r.agent === a.name);
        expect(resp?.content?.trim().length).toBeGreaterThan(0);
        expect(resp?.content).toMatch(/CONSENSUS:\s*YES/i);
      }
      // Empty-after-retries / Votes 0/N must not count as round evidence.
      // All-YES sets awaiting_user (council.ts:811-813), not max_rounds/error.
      expect(data.councilStatus).toBe('awaiting_user');

      const membersByName = new Map(data.preflight!.members.map((m) => [m.member, m]));
      for (const a of agents) {
        const m = membersByName.get(a.name)!;
        expect(m.inputs).toEqual([{ path: 'docs/in.txt', sha256, ok: true }]);
        const rows = readRows(fixtureByMember[a.name]!);
        expect(rows).toHaveLength(1);
        const row = rows[0]!;
        // Member view is the project tree the fixture walked (cwd for most; --workspace for cursor RO).
        expect(row.workspace).toBe(m.view);
        if (a.engine !== 'cursor') {
          expect(row.cwd).toBe(m.view);
        } else {
          // persistent-cursor-session.ts:143-156 — RO spawn cwd is throwaway config dir
          expect(row.cwd).not.toBe(m.view);
          expect(row.argv.includes('--workspace') && row.argv[row.argv.indexOf('--workspace') + 1]).toBe(m.view);
        }
        expect(row.hashes['docs/in.txt']).toBe(sha256);
        expect(row.opens).toContain('docs/in.txt');
        // View-content evidence only: undeclared paths absent from what the fixture saw in the view.
        expect(row.seen).not.toContain('undeclared-sibling.txt');
        expect(row.seen).not.toContain('undeclared-untracked.txt');
        expect(row.opens).not.toContain('undeclared-sibling.txt');
        expect(row.opens).not.toContain('undeclared-untracked.txt');

        // Read-only argv mappings emitted by the wrappers themselves:
        if (a.engine === 'claude') {
          // persistent-session.ts:209-215 — sandboxMode read-only → --permission-mode plan
          const i = row.argv.indexOf('--permission-mode');
          expect(i).toBeGreaterThanOrEqual(0);
          expect(row.argv[i + 1]).toBe('plan');
        } else if (a.engine === 'codex') {
          // persistent-codex-session.ts:270-284 — --sandbox <sandboxMode>
          const i = row.argv.indexOf('--sandbox');
          expect(i).toBeGreaterThanOrEqual(0);
          expect(row.argv[i + 1]).toBe('read-only');
        } else if (a.engine === 'cursor') {
          // persistent-cursor-session.ts:119-127 — read-only → --mode plan (not --force)
          const i = row.argv.indexOf('--mode');
          expect(i).toBeGreaterThanOrEqual(0);
          expect(row.argv[i + 1]).toBe('plan');
          expect(row.argv).not.toContain('--force');
        } else if (a.engine === 'agy') {
          // persistent-agy-session.ts:168-169 — sandboxMode read-only → --mode plan
          const i = row.argv.indexOf('--mode');
          expect(i).toBeGreaterThanOrEqual(0);
          expect(row.argv[i + 1]).toBe('plan');
        }
      }
    },
  );
});
