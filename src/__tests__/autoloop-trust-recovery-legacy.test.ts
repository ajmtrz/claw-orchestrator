import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const project = fileURLToPath(new URL('../../', import.meta.url));
const artifacts = path.join(project, '.artifacts', 'CLAWO-AUTOLOOP-DURABLE-RECOVERY-TRUST-RECOVERY-R1');
const helper = fileURLToPath(new URL('./helpers/autoloop-trust-recovery.ts', import.meta.url));

function attempt(name: string): string {
  const root = process.env.CLAWO_TRUST_CASE_ROOT ?? path.join(artifacts, 'evidence', 'candidate');
  fs.mkdirSync(root, { recursive: true });
  const directory = fs.mkdtempSync(path.join(root, `legacy-${name}-`));
  fs.writeFileSync(
    path.join(directory, 'case.json'),
    JSON.stringify({ title: expect.getState().currentTestName?.replaceAll(' > ', ' ') }),
    { flag: 'wx' },
  );
  return directory;
}

const parentSequences = new Map<string, number>();
function parentObserve(directory: string, value: unknown) {
  const sequence = parentSequences.get(directory) ?? 0;
  parentSequences.set(directory, sequence + 1);
  fs.appendFileSync(
    path.join(directory, 'parent.jsonl'),
    JSON.stringify({ sequence, process_id: process.pid, observer_id: `parent:${process.pid}`, value }) + '\n',
  );
}

const created = Date.parse('2026-09-05T10:00:00.000Z');
const ttl = 7 * 24 * 60 * 60 * 1000;
const runId = 'legacy-probe';
const sessionName = `autoloop-${runId}-planner`;
const legacy = {
  name: sessionName,
  claudeSessionId: 'legacy-session-id',
  cwd: project,
  originalCreated: new Date(created).toISOString(),
  lastResumed: new Date(created).toISOString(),
  lastActivity: created,
};

function workerEnvironment(action: Record<string, unknown>, directory: string) {
  return {
    ...process.env,
    NODE_OPTIONS: '',
    TSX_DISABLE_CACHE: '1',
    CLAWO_TRUST_SCRATCH: directory,
    CLAWO_TRUST_SHARED_HOME: path.join(directory, 'shared-home'),
    CLAWO_TRUST_ACTION: JSON.stringify({ runId, sessionName, now: created + 1_000, ...action }),
  };
}

function runWorker(action: Record<string, unknown>, directory = attempt('worker')) {
  const startedAt = new Date().toISOString();
  const child = spawnSync(process.execPath, ['--import', 'tsx', helper], {
    cwd: project,
    env: workerEnvironment(action, directory),
    encoding: 'utf8',
    timeout: 20_000,
  });
  parentObserve(directory, {
    kind: 'execution',
    pid: child.pid,
    action,
    argv: [process.execPath, '--import', 'tsx', helper],
    started_at: startedAt,
    ended_at: new Date().toISOString(),
    status: child.status,
    signal: child.signal,
    error: child.error?.message,
  });
  fs.writeFileSync(path.join(directory, `worker-${child.pid}-stdout.txt`), child.stdout ?? '');
  fs.writeFileSync(path.join(directory, `worker-${child.pid}-stderr.txt`), child.stderr ?? '');
  fs.writeFileSync(
    path.join(directory, `worker-${child.pid}-exit.json`),
    JSON.stringify({
      pid: child.pid,
      status: child.status,
      signal: child.signal,
      error: child.error?.message,
    }),
  );
  expect(child.status, child.stderr).toBe(0);
  const result = JSON.parse(child.stdout.trim());
  return { ...result, directory };
}

function pausedWorker(action: Record<string, unknown>, directory: string) {
  const startedAt = new Date().toISOString();
  const child = spawn(process.execPath, ['--import', 'tsx', helper], {
    cwd: project,
    env: workerEnvironment(action, directory),
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  parentObserve(directory, {
    kind: 'spawn',
    pid: child.pid,
    action,
    argv: [process.execPath, '--import', 'tsx', helper],
    started_at: startedAt,
  });
  const stdout: Buffer[] = [],
    stderr: Buffer[] = [];
  child.stdout.on('data', (bytes) => stdout.push(bytes));
  child.stderr.on('data', (bytes) => stderr.push(bytes));
  let reached!: (value: any) => void;
  let missed!: (error: Error) => void;
  const barrier = new Promise<any>((resolve, reject) => {
    reached = resolve;
    missed = reject;
  });
  child.on('message', (message) => {
    const event = message as { type: string };
    if (event.type === 'barrier') {
      parentObserve(directory, { kind: 'barrier', pid: child.pid, receipt: message });
      reached(message);
    }
  });
  const timer = setTimeout(() => child.kill('SIGKILL'), 15_000);
  const exit = new Promise<{ status: number | null; signal: string | null; result?: any }>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (status, signal) => {
      clearTimeout(timer);
      const out = Buffer.concat(stdout).toString();
      const err = Buffer.concat(stderr).toString();
      parentObserve(directory, { kind: 'exit', pid: child.pid, status, signal, ended_at: new Date().toISOString() });
      fs.writeFileSync(path.join(directory, `worker-${child.pid}-stdout.txt`), out);
      fs.writeFileSync(path.join(directory, `worker-${child.pid}-stderr.txt`), err);
      fs.writeFileSync(
        path.join(directory, `worker-${child.pid}-exit.json`),
        JSON.stringify({
          sequence: 0,
          process_id: process.pid,
          observer_id: `parent:${process.pid}`,
          value: { child_pid: child.pid, status, signal },
        }),
      );
      missed(new Error(`Worker exited before its barrier: ${status}/${signal}: ${err}`));
      resolve({ status, signal, result: out.trim() ? JSON.parse(out) : undefined });
    });
  });
  return {
    child,
    barrier,
    exit,
    resume: (receipt: { gate: string }) => fs.writeFileSync(receipt.gate, 'continue', { flag: 'wx' }),
  };
}

function bytes(reference: { path: string; sha256: string }): Buffer {
  const value = fs.readFileSync(reference.path);
  expect(createHash('sha256').update(value).digest('hex')).toBe(reference.sha256);
  return value;
}

function registryOf(snapshot: any) {
  return JSON.parse(bytes(snapshot.registry).toString());
}
function rowsOf(snapshot: any): any[] {
  return snapshot.ledger
    ? bytes(snapshot.ledger)
        .toString()
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line))
    : [];
}

function expectCanonicalSuccessor(snapshot: any) {
  const rows = rowsOf(snapshot);
  expect(rows.map((row) => [row.kind, row.payload.generation])).toEqual([
    ['agent_generation_orphaned', 0],
    ['agent_generation_released', 0],
    ['agent_generation_reserved', 1],
  ]);
  expect(rows.every((row) => row.schema_version === 1 && row.actor === 'dispatcher')).toBe(true);
  const successor = rows[2].payload;
  expect(registryOf(snapshot)[0]).toMatchObject({
    agentGeneration: 1,
    agentReleasedGeneration: 0,
    agentSessionId: successor.session_id,
    agentOwnerInstanceId: successor.owner_instance_id,
  });
}

describe('trust recovery legacy persisted state', () => {
  it('isolates registry and subprocess scratch paths before import without changing HOME or CODEX_HOME', () => {
    const directory = attempt('isolation');
    const child = spawnSync(
      process.execPath,
      [
        '--import',
        helper,
        '--input-type=module',
        '-e',
        `import os from 'node:os'; console.log(JSON.stringify({ home: os.homedir(), tmp: os.tmpdir(), HOME: process.env.HOME, CODEX_HOME: process.env.CODEX_HOME, wf: process.env.CLAWO_WF_DIR }));`,
      ],
      { cwd: project, env: { ...process.env, CLAWO_TRUST_SCRATCH: directory }, encoding: 'utf8' },
    );
    parentObserve(directory, { kind: 'isolation', pid: child.pid, status: child.status, signal: child.signal });
    fs.writeFileSync(path.join(directory, `worker-${child.pid}-stdout.txt`), child.stdout);
    fs.writeFileSync(path.join(directory, `worker-${child.pid}-stderr.txt`), child.stderr);
    expect(child.status, child.stderr).toBe(0);
    const observed = JSON.parse(child.stdout.trim());
    expect(observed.home.startsWith(directory + path.sep)).toBe(true);
    expect(observed.tmp.startsWith(directory + path.sep)).toBe(true);
    expect(observed.wf.startsWith(directory + path.sep)).toBe(true);
    expect(observed.HOME).toBe(process.env.HOME);
    expect(observed.CODEX_HOME).toBe(process.env.CODEX_HOME);
    expect(os.homedir()).not.toBe(observed.home);
  });

  for (const subject of ['upstream', 'start', 'candidate']) {
    for (const delta of [-1, 0, 1]) {
      it(`${subject}: reads ordinary legacy metadata at TTL ${delta < 0 ? '-1' : '+' + delta} ms from real registry bytes`, () => {
        const root = subject === 'candidate' ? project : path.join(project, '.worktrees/trust-recovery-r1', subject);
        const result = runWorker({ action: 'load', subject: root, seed: [legacy], now: created + ttl + delta });
        expect(JSON.parse(fs.readFileSync(result.snapshots.seed.registry.path, 'utf8'))).toEqual([legacy]);
        expect(result.loaded.map((entry: { name: string }) => entry.name)).toEqual(delta < 0 ? [sessionName] : []);
        expect(result.apis).toEqual({ reserve: subject !== 'upstream', release: subject !== 'upstream' });
        expect(result.imports[0].path).toBe(path.join(root, 'src/session-manager.ts'));
      });
    }
  }

  for (const delta of [-1, 0, 1]) {
    it(`distinguishes durable pending and released fences from ordinary TTL at ${delta} ms`, () => {
      const seed = [
        {
          ...legacy,
          agentGeneration: 0,
          agentOwnerInstanceId: 'legacy-registry',
          agentReleasePending: true,
          agentReleaseOwnerInstanceId: 'unclassifiable-old-owner',
        },
        { ...legacy, name: 'released-legacy', agentReleasedGeneration: 0 },
        { ...legacy, name: 'ordinary' },
      ];
      const result = runWorker({ action: 'load', seed, now: created + ttl + delta });
      expect(result.loaded.map((entry: { name: string }) => entry.name)).toEqual(
        delta < 0 ? [sessionName, 'released-legacy', 'ordinary'] : [sessionName, 'released-legacy'],
      );
      expect(JSON.parse(fs.readFileSync(result.snapshots.final.registry.path, 'utf8'))).toEqual(
        expect.arrayContaining(seed.slice(0, 2)),
      );
    });
  }

  it('refuses a competing process inside real legacy evidence persistence, then admits one successor', async () => {
    const directory = attempt('competing');
    const owner = pausedWorker({ action: 'release', seed: [legacy], barrier: 'before-file-fsync' }, directory);
    try {
      const receipt = await owner.barrier;
      const pending = registryOf(receipt.snapshot)[0];
      expect(pending).toMatchObject({
        agentGeneration: 0,
        agentReleasePending: true,
        agentOwnerInstanceId: 'legacy-registry',
        agentReleaseOwnerInstanceId: receipt.owner,
      });
      expect(pending).not.toHaveProperty('agentSessionId');
      const competitor = runWorker({ action: 'reserve' }, directory);
      expect(competitor.value).toBe(false);
      expect(bytes(competitor.snapshots.final.registry)).toEqual(bytes(receipt.snapshot.registry));
      const thief = runWorker({ action: 'prepare' }, directory);
      expect(thief.failure.code).toBe('AUTOLOOP_AGENT_GENERATION_CONFLICT');
      expect(bytes(thief.snapshots.final.registry)).toEqual(bytes(receipt.snapshot.registry));
      owner.resume(receipt);
      const released = await owner.exit;
      expect(released.status).toBe(0);
      expect(released.result.responses).toEqual([true]);
      expect(registryOf(released.result.snapshots.final)[0]).toMatchObject({ agentReleasedGeneration: 0 });
      const retry = runWorker({ action: 'release' }, directory);
      expect(retry.responses).toEqual([true]);
      expect(bytes(retry.snapshots.final.ledger)).toEqual(bytes(released.result.snapshots.final.ledger));
      const next = runWorker({ action: 'prepare' }, directory);
      expectCanonicalSuccessor(next.snapshots.final);
      expect(bytes(next.snapshots.final.ledger).subarray(0, bytes(receipt.snapshot.ledger).length)).toEqual(
        bytes(receipt.snapshot.ledger),
      );
      const duplicate = runWorker({ action: 'reserve' }, directory);
      expect(duplicate.value).toBe(false);
      expect(bytes(duplicate.snapshots.final.registry)).toEqual(bytes(next.snapshots.final.registry));
    } finally {
      if (owner.child.exitCode === null) owner.child.kill('SIGKILL');
      await owner.exit;
    }
  }, 20_000);

  for (const barrier of ['before-release-write', 'before-file-fsync', 'after-durable-release']) {
    it(`cold-recovers once after witnessed process loss at ${barrier}`, async () => {
      const directory = attempt(`crash-${barrier}`);
      const owner = pausedWorker({ action: 'prepare', seed: [legacy], barrier }, directory);
      try {
        const receipt = await owner.barrier;
        expect(registryOf(receipt.snapshot)[0].agentReleasePending).toBe(true);
        expect(runWorker({ action: 'reserve' }, directory).value).toBe(false);
        expect(rowsOf(receipt.snapshot).map((row) => row.kind)).toEqual(
          barrier === 'before-release-write'
            ? ['agent_generation_orphaned']
            : ['agent_generation_orphaned', 'agent_generation_released'],
        );
        owner.child.kill('SIGKILL');
        expect((await owner.exit).signal).toBe('SIGKILL');
        const cold = runWorker({ action: 'prepare' }, directory);
        expect(cold.failure).toBeUndefined();
        expectCanonicalSuccessor(cold.snapshots.final);
        expect(bytes(cold.snapshots.final.ledger).subarray(0, bytes(receipt.snapshot.ledger).length)).toEqual(
          bytes(receipt.snapshot.ledger),
        );
        expect(runWorker({ action: 'reserve' }, directory).value).toBe(false);
      } finally {
        if (owner.child.exitCode === null) owner.child.kill('SIGKILL');
        await owner.exit;
      }
    }, 20_000);
  }

  it('keeps unknown legacy release ownership blocked and rejects mismatched tuples without writes', () => {
    const directory = attempt('unknown-owner');
    const pending = {
      ...legacy,
      agentGeneration: 0,
      agentOwnerInstanceId: 'legacy-registry',
      agentReleasePending: true,
      agentReleaseOwnerInstanceId: 'unclassifiable-old-owner',
    };
    const initial = runWorker({ action: 'prepare', seed: [pending] }, directory);
    expect(initial.failure.code).toBe('AUTOLOOP_AGENT_GENERATION_CONFLICT');
    expect(rowsOf(initial.snapshots.final)).toEqual([]);
    for (const options of [
      { generation: 1 },
      { owner: 'wrong-owner' },
      { session: 'wrong-session' },
      { omitSession: true },
    ]) {
      const wrong = runWorker({ action: 'release-options', options }, directory);
      expect(wrong.value).toBe(false);
      expect(registryOf(wrong.snapshots.final)).toEqual([pending]);
      expect(rowsOf(wrong.snapshots.final)).toEqual([]);
    }
  }, 20_000);

  it('flushes a retained release row after a real file-sync failure without duplicating history', () => {
    const directory = attempt('fsync-failure');
    const failed = runWorker({ action: 'prepare', seed: [legacy], fault: 'release-fsync' }, directory);
    expect(failed.failure.code).toBe('AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE');
    expect(registryOf(failed.snapshots.final)[0].agentReleasePending).toBe(true);
    expect(rowsOf(failed.snapshots.final).map((row) => row.kind)).toEqual([
      'agent_generation_orphaned',
      'agent_generation_released',
    ]);
    const cold = runWorker({ action: 'prepare' }, directory);
    expect(cold.failure).toBeUndefined();
    expectCanonicalSuccessor(cold.snapshots.final);
    expect(bytes(cold.snapshots.final.ledger).subarray(0, bytes(failed.snapshots.final.ledger).length)).toEqual(
      bytes(failed.snapshots.final.ledger),
    );
  });

  it('blocks cold recovery of a partially written release row while preserving historical bytes', () => {
    const directory = attempt('short-write');
    const failed = runWorker({ action: 'prepare', seed: [legacy], fault: 'release-short-write' }, directory);
    expect(failed.failure.message).toContain('complete agent-generations.jsonl record');
    expect(registryOf(failed.snapshots.final)[0].agentReleasePending).toBe(true);
    const cold = runWorker({ action: 'prepare' }, directory);
    expect(cold.failure.code).toBe('AUTOLOOP_AGENT_LEDGER_INVALID');
    expect(bytes(cold.snapshots.final.ledger)).toEqual(bytes(failed.snapshots.final.ledger));
    expect(runWorker({ action: 'reserve' }, directory).value).toBe(false);
  });

  it('waits for durable release during shutdown and refuses later releases from the closed manager', async () => {
    const directory = attempt('shutdown');
    const owner = pausedWorker({ action: 'shutdown', seed: [legacy], barrier: 'after-durable-release' }, directory);
    try {
      const receipt = await owner.barrier;
      expect(runWorker({ action: 'reserve' }, directory).value).toBe(false);
      owner.resume(receipt);
      const completed = await owner.exit;
      expect(completed.status).toBe(0);
      expect(completed.result.value).toEqual({ closedRelease: false });
      expect(registryOf(completed.result.snapshots.final)[0]).toHaveProperty('agentReleasedGeneration', 0);
      const next = runWorker({ action: 'prepare' }, directory);
      expectCanonicalSuccessor(next.snapshots.final);
    } finally {
      if (owner.child.exitCode === null) owner.child.kill('SIGKILL');
      await owner.exit;
    }
  }, 20_000);

  it('does not let shutdown publish a stale legacy snapshot over the successor fence', async () => {
    const directory = attempt('stale-snapshot');
    const stale = pausedWorker({ action: 'load', seed: [legacy], barrier: 'loaded' }, directory);
    try {
      const receipt = await stale.barrier;
      const next = runWorker({ action: 'prepare' }, directory);
      expectCanonicalSuccessor(next.snapshots.final);
      stale.resume(receipt);
      const completed = await stale.exit;
      expect(completed.status).toBe(0);
      expect(bytes(completed.result.snapshots.final.registry)).toEqual(bytes(next.snapshots.final.registry));
      expect(bytes(completed.result.snapshots.final.ledger)).toEqual(bytes(next.snapshots.final.ledger));
    } finally {
      if (stale.child.exitCode === null) stale.child.kill('SIGKILL');
      await stale.exit;
    }
  }, 20_000);

  for (const delta of [-1, 0, 1]) {
    it(`traces the legacy release hooks and persisted ledger at TTL ${delta} ms`, async () => {
      const result = runWorker({ action: 'release', seed: [legacy], now: created + ttl + delta });
      expect(result.responses).toEqual([delta < 0]);
      const rows = result.snapshots.final.ledger
        ? fs.readFileSync(result.snapshots.final.ledger.path, 'utf8').trim().split('\n').map(JSON.parse)
        : [];
      expect(rows.map((row: { kind: string }) => row.kind)).toEqual(
        delta < 0 ? ['agent_generation_orphaned', 'agent_generation_released'] : [],
      );
      if (delta < 0) {
        expect(rows.map((row: { payload: { generation: number } }) => row.payload.generation)).toEqual([0, 0]);
        expect(JSON.parse(fs.readFileSync(result.snapshots.final.registry.path, 'utf8'))[0]).toMatchObject({
          agentReleasedGeneration: 0,
          agentReleasedOwnerInstanceId: 'legacy-registry',
        });
        const { verifyLegacyCase } = await import('../../scripts/autoloop-trust-recovery/verify.mjs');
        expect(() => verifyLegacyCase(result.directory)).not.toThrow();
        const sensitivity = path.join(artifacts, 'evidence', 'sensitivity');
        fs.mkdirSync(sensitivity, { recursive: true });
        const copy = fs.mkdtempSync(path.join(sensitivity, 'legacy-evidence-substitution-'));
        fs.cpSync(result.directory, copy, { recursive: true });
        for (const entry of fs.readdirSync(copy, { recursive: true, withFileTypes: true })) {
          if (!entry.isFile()) continue;
          const target = path.join(entry.parentPath, entry.name);
          fs.writeFileSync(target, fs.readFileSync(target, 'utf8').replaceAll(result.directory, copy));
        }
        expect(() => verifyLegacyCase(copy)).not.toThrow();
        const stdoutPath = path.join(copy, `worker-${result.process_id}-stdout.txt`);
        const substituted = JSON.parse(fs.readFileSync(stdoutPath, 'utf8'));
        const registryPath = substituted.snapshots.final.registry.path;
        const changed = JSON.parse(fs.readFileSync(registryPath, 'utf8'));
        changed[0].agentReleasedGeneration = 9;
        fs.writeFileSync(registryPath, JSON.stringify(changed));
        substituted.snapshots.final.registry.sha256 = createHash('sha256')
          .update(fs.readFileSync(registryPath))
          .digest('hex');
        fs.writeFileSync(stdoutPath, JSON.stringify(substituted));
        // The successful API boolean remains true and the substituted file's
        // hash is correct. The durable postcondition must still reject it.
        expect(() => verifyLegacyCase(copy)).toThrow(/postcondition|release|registry/i);
      }
    });
  }
});
