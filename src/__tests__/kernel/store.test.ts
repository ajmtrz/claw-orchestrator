/**
 * The durable store. Real temp directories, no fs mock — the whole point of the
 * layer is what survives the process that wrote it, so the assertions are on
 * what actually lands on disk.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import {
  acquireLease,
  atomicWriteJson,
  commit,
  createAndAcquire,
  deleteRunDir,
  listRunIds,
  listRuns,
  loadRun,
  loadSpec,
  nodeArtifactPath,
  readEvents,
  replayRun,
  runDir,
} from '../../kernel/store.js';
import { RunKernel } from '../../kernel/engine.js';
import type { KernelEvent, RunRecord, WorkflowSpec } from '../../kernel/types.js';

/**
 * There is no unguarded way to write to a run, by design — the raw checkpoint
 * and append helpers are module-private. Tests take a real claim like the kernel
 * does, which also means every assertion below runs against the same code path
 * production uses.
 */
function owner(runId: string, ownerId = 'test-owner'): ReturnType<typeof acquireLease> {
  return acquireLease(runId, ownerId);
}

function createRunDir(runId: string, spec: WorkflowSpec): void {
  createAndAcquire(runId, spec, 'test-owner');
}

function saveRun(record: RunRecord, ownerId = 'test-owner'): string {
  return commit(owner(record.runId, ownerId), { record }).outcome;
}

function appendEvent(runId: string, event: KernelEvent, ownerId = 'test-owner'): string {
  return commit(owner(runId, ownerId), { events: [event] }).outcome;
}

function writeNodeArtifact(runId: string, nodeId: string, name: string, body: string): string {
  commit(owner(runId), { artifacts: [{ nodeId, name, body }] });
  return nodeArtifactPath(runId, nodeId, name);
}

let tmp: string;
const saved = process.env.CLAWO_WF_DIR;

const spec: WorkflowSpec = {
  name: 'demo',
  nodes: [
    { id: 'a', kind: 'agent', prompt: 'do a' },
    { id: 'b', kind: 'agent', prompt: 'do b' },
  ],
};

function baseRecord(runId: string): RunRecord {
  return {
    runId,
    workflow: spec.name,
    spec,
    state: 'running',
    outcome: 'unverified',
    cwd: '/tmp',
    createdAt: '2026-08-23T00:00:00.000Z',
    updatedAt: '2026-08-23T00:00:00.000Z',
    nodes: {
      a: { id: 'a', kind: 'agent', state: 'pending', attempts: 0, visits: 0 },
      b: { id: 'b', kind: 'agent', state: 'pending', attempts: 0, visits: 0 },
    },
  };
}

beforeEach(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-wf-'));
  process.env.CLAWO_WF_DIR = tmp;
});

afterEach(() => {
  if (saved === undefined) delete process.env.CLAWO_WF_DIR;
  else process.env.CLAWO_WF_DIR = saved;
  fs.rmSync(tmp, { recursive: true, force: true });
});

describe('atomicWriteJson', () => {
  it('leaves no temp file behind on success', () => {
    const file = path.join(tmp, 'x.json');
    atomicWriteJson(file, { a: 1 });
    expect(JSON.parse(fs.readFileSync(file, 'utf8'))).toEqual({ a: 1 });
    expect(fs.readdirSync(tmp).filter((f) => f.includes('.tmp.'))).toHaveLength(0);
  });

  it('creates missing parent directories', () => {
    const file = path.join(tmp, 'deep', 'nested', 'x.json');
    atomicWriteJson(file, { ok: true });
    expect(fs.existsSync(file)).toBe(true);
  });

  it('replaces an existing file wholesale rather than appending', () => {
    const file = path.join(tmp, 'x.json');
    atomicWriteJson(file, { long: 'a'.repeat(500) });
    atomicWriteJson(file, { short: 1 });
    expect(JSON.parse(fs.readFileSync(file, 'utf8'))).toEqual({ short: 1 });
  });
});

describe('run directory', () => {
  it('clears the lease heartbeat on terminal cleanup failure before releasing the run once', async () => {
    // This catches the interval surviving a terminal release failure and
    // renewing the lease again after the run has already stood down.
    vi.useFakeTimers();
    const runId = 'heartbeat-cleanup-failed';
    const lockPath = path.join(runDir(runId), 'lease.lock');
    const releaseFailure = new Error('injected heartbeat lock release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    let failRelease = false;
    let rmSpy: ReturnType<typeof vi.spyOn> | undefined;
    let releaseNode: (() => void) | undefined;
    const kernel = new RunKernel();
    kernel.setExecutor('agent', async () => {
      await new Promise<void>((resolve) => {
        releaseNode = resolve;
      });
      return { ok: true };
    });
    const kernelInternal = kernel as unknown as {
      _scheduleRelease: (guard: unknown, reason?: string, attempt?: number) => void;
    };
    const scheduleSpy = vi.spyOn(kernelInternal, '_scheduleRelease').mockImplementation(() => undefined);

    try {
      await kernel.start(
        { name: 'heartbeat cleanup failure', nodes: [{ id: 'a', kind: 'agent', prompt: 'a' }] },
        { runId },
      );
      const originalRmSync = fs.rmSync;
      rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
        if (failRelease && target === lockPath) throw releaseFailure;
        return originalRmSync(target, options);
      }) as typeof fs.rmSync);
      syncBuiltinESMExports();
      failRelease = true;
      await vi.advanceTimersByTimeAsync(31_000);
      const afterFailure = JSON.parse(fs.readFileSync(path.join(runDir(runId), 'lease.json'), 'utf8')).renewedAt;
      const rmCallsAfterFailure = rmSpy.mock.calls.filter(([target]) => target === lockPath).length;

      await vi.advanceTimersByTimeAsync(5 * 60_000);

      expect(JSON.parse(fs.readFileSync(path.join(runDir(runId), 'lease.json'), 'utf8')).renewedAt).toBe(afterFailure);
      expect(rmSpy.mock.calls.filter(([target]) => target === lockPath)).toHaveLength(rmCallsAfterFailure);
      expect(scheduleSpy).toHaveBeenCalledTimes(1);
      expect(releaseNode).toBeTypeOf('function');
      releaseNode!();
      await vi.advanceTimersByTimeAsync(0);
      await kernel.wait(runId);
    } finally {
      releaseNode?.();
      scheduleSpy.mockRestore();
      rmSpy?.mockRestore();
      syncBuiltinESMExports();
      vi.useRealTimers();
      fs.rmSync(runDir(runId), { recursive: true, force: true });
    }
  });

  it('stalls without retrying when a kernel transaction receives cleanup_failed', async () => {
    // This catches RunTxn rejecting the terminal cleanup_failed CommitOutcome
    // even though it must take the same stalled/stop path as other failed
    // non-superseded commits.
    const runId = 'kernel-cleanup-failed';
    const lockPath = path.join(runDir(runId), 'lease.lock');
    const reclaimPath = `${lockPath}.reclaim`;
    const attempts: string[] = [];
    const kernel = new RunKernel();
    kernel.setExecutor('agent', async (node) => {
      attempts.push(node.id);
      fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
      fs.utimesSync(reclaimPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));
      return { ok: true };
    });
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error(
      'injected adopted transaction-lock reclaim release failure',
    ) as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (String(target).startsWith(`${reclaimPath}.adopt-`)) throw releaseFailure;
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    let done: RunRecord | undefined;
    try {
      const started = await kernel.start(
        {
          name: 'cleanup-failed-stop',
          nodes: [{ id: 'a', kind: 'agent', prompt: 'a', retry: { max: 3, backoffMs: 1 } }],
        },
        { runId },
      );
      done = await kernel.wait(started.runId);
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
      fs.rmSync(lockPath, { force: true });
      fs.rmSync(reclaimPath, { force: true });
    }

    expect(attempts).toEqual(['a']);
    expect(done).toMatchObject({
      state: 'running',
      nodes: { a: { state: 'running', attempts: 1 } },
    });
  });

  it('returns terminal cleanup_failed when an adopted run-lock reclaim cannot be released', () => {
    // This catches commit collapsing a terminal lock-cleanup failure into a
    // retryable blocked outcome after it has not entered the transaction.
    const guard = createAndAcquire('cleanup-failed', spec, 'test-owner');
    const lockPath = path.join(runDir('cleanup-failed'), 'lease.lock');
    const reclaimPath = `${lockPath}.reclaim`;
    fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    fs.utimesSync(reclaimPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error('injected adopted run-lock reclaim release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (String(target).startsWith(`${reclaimPath}.adopt-`)) throw releaseFailure;
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    let result: ReturnType<typeof commit>;
    try {
      result = commit(guard, { record: baseRecord('cleanup-failed') });
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
      fs.rmSync(lockPath, { force: true });
      fs.rmSync(reclaimPath, { force: true });
    }

    expect(result!).toMatchObject({
      outcome: 'cleanup_failed',
      reason: expect.stringContaining('Could not safely release owned lock'),
    });
    expect(fs.existsSync(path.join(runDir('cleanup-failed'), 'run.json'))).toBe(false);
  });

  it('writes the spec once and keeps it separate from the mutable checkpoint', () => {
    createRunDir('r1', spec);
    expect(loadSpec('r1')).toEqual(spec);
    expect(fs.existsSync(path.join(runDir('r1'), 'run.json'))).toBe(false);
  });

  it('round-trips a checkpoint', () => {
    createRunDir('r1', spec);
    const rec = baseRecord('r1');
    rec.nodes.a.state = 'succeeded';
    saveRun(rec);
    expect(loadRun('r1')?.nodes.a.state).toBe('succeeded');
  });

  it('stores node artifacts under the run', () => {
    createRunDir('r1', spec);
    const rel = writeNodeArtifact('r1', 'a', 'out.txt', 'hello');
    expect(fs.readFileSync(path.join(runDir('r1'), rel), 'utf8')).toBe('hello');
  });

  it('deletes only on an explicit request', () => {
    createRunDir('r1', spec);
    expect(listRunIds()).toContain('r1');
    deleteRunDir('r1');
    expect(listRunIds()).not.toContain('r1');
  });
});

describe('events', () => {
  it('appends and reads back in order', () => {
    createRunDir('r1', spec);
    appendEvent('r1', { ts: '2026-08-23T00:00:01.000Z', type: 'run_created', runId: 'r1', workflow: 'demo' });
    appendEvent('r1', { ts: '2026-08-23T00:00:02.000Z', type: 'run_state', state: 'running' });
    const events = readEvents('r1');
    expect(events.map((e) => e.type)).toEqual(['run_created', 'run_state']);
  });

  it('skips one corrupt line rather than failing the whole stream', () => {
    createRunDir('r1', spec);
    appendEvent('r1', { ts: 't1', type: 'run_state', state: 'running' });
    fs.appendFileSync(path.join(runDir('r1'), 'events.jsonl'), '{not json\n');
    appendEvent('r1', { ts: 't2', type: 'run_state', state: 'completed' });
    expect(readEvents('r1')).toHaveLength(2);
  });

  it('returns empty for a run with no event log', () => {
    createRunDir('r1', spec);
    expect(readEvents('r1')).toEqual([]);
  });
});

describe('crash recovery', () => {
  it('replays state from the event log when run.json is missing', () => {
    createRunDir('r1', spec);
    appendEvent('r1', { ts: 't0', type: 'run_created', runId: 'r1', workflow: 'demo' });
    appendEvent('r1', { ts: 't1', type: 'run_state', state: 'running' });
    appendEvent('r1', { ts: 't2', type: 'node_state', node: 'a', state: 'running', attempt: 1 });
    appendEvent('r1', { ts: 't3', type: 'node_state', node: 'a', state: 'succeeded' });

    const replayed = replayRun('r1', spec)!;
    expect(replayed.nodes.a.state).toBe('succeeded');
    expect(replayed.nodes.b.state).toBe('pending');
    expect(replayed.nodes.a.visits).toBe(1);
  });

  // ── A retry is not a visit.
  //
  //    `_run` commits the visit counter with no event; `_runWithRetry` emits a
  //    `running` event per ATTEMPT. Counting every one of them replayed a single
  //    visit with K retries as K visits, so a resume from a lost run.json could
  //    trip the visit bound on its first turn while the real count was 1.
  it('counts one visit however many attempts it took', () => {
    createRunDir('r1', spec);
    appendEvent('r1', { ts: 't0', type: 'run_created', runId: 'r1', workflow: 'demo' });
    appendEvent('r1', { ts: 't1', type: 'node_state', node: 'a', state: 'running', attempt: 1 });
    appendEvent('r1', { ts: 't2', type: 'node_state', node: 'a', state: 'running', attempt: 2 });
    appendEvent('r1', { ts: 't3', type: 'node_state', node: 'a', state: 'running', attempt: 3 });
    appendEvent('r1', { ts: 't4', type: 'node_state', node: 'a', state: 'succeeded' });

    const replayed = replayRun('r1', spec)!;
    expect(replayed.nodes.a.visits).toBe(1);
    expect(replayed.nodes.a.attempts).toBe(3);
  });

  it('counts a genuine revisit, which is what the bound is for', () => {
    createRunDir('r1', spec);
    appendEvent('r1', { ts: 't0', type: 'run_created', runId: 'r1', workflow: 'demo' });
    appendEvent('r1', { ts: 't1', type: 'node_state', node: 'a', state: 'running', attempt: 1 });
    appendEvent('r1', { ts: 't2', type: 'node_state', node: 'a', state: 'failed' });
    appendEvent('r1', { ts: 't3', type: 'node_state', node: 'a', state: 'running', attempt: 1 });
    appendEvent('r1', { ts: 't4', type: 'node_state', node: 'a', state: 'succeeded' });

    expect(replayRun('r1', spec)!.nodes.a.visits).toBe(2);
  });

  it('falls back to a replay when run.json is half-written', () => {
    createRunDir('r1', spec);
    appendEvent('r1', { ts: 't0', type: 'run_created', runId: 'r1', workflow: 'demo' });
    appendEvent('r1', { ts: 't1', type: 'node_state', node: 'a', state: 'succeeded' });
    // Simulate the torn write the atomic rename is there to prevent.
    fs.writeFileSync(path.join(runDir('r1'), 'run.json'), '{"runId":"r1","nod');

    const loaded = loadRun('r1');
    expect(loaded).toBeDefined();
    expect(loaded!.nodes.a.state).toBe('succeeded');
  });

  it('says a replayed mid-flight run did not reach a terminal state', () => {
    createRunDir('r1', spec);
    appendEvent('r1', { ts: 't0', type: 'run_created', runId: 'r1', workflow: 'demo' });
    appendEvent('r1', { ts: 't1', type: 'run_state', state: 'running' });
    const replayed = replayRun('r1', spec)!;
    expect(replayed.state).toBe('running');
    expect(replayed.error).toContain('process ended');
  });

  it('carries the verdict through a replay', () => {
    createRunDir('r1', spec);
    appendEvent('r1', { ts: 't0', type: 'run_created', runId: 'r1', workflow: 'demo' });
    appendEvent('r1', { ts: 't1', type: 'evidence', node: 'a', evidenceId: 'a-01', passed: true });
    appendEvent('r1', { ts: 't2', type: 'run_state', state: 'completed', outcome: 'verified' });
    const replayed = replayRun('r1', spec)!;
    expect(replayed.outcome).toBe('verified');
    expect(replayed.evidenceId).toBe('a-01');
  });

  it('returns undefined for an unknown run', () => {
    expect(loadRun('nope')).toBeUndefined();
  });
});

describe('listRuns', () => {
  it('lists newest first and filters', () => {
    for (const [id, ts, state] of [
      ['old', '2026-08-01T00:00:00.000Z', 'completed'],
      ['new', '2026-08-20T00:00:00.000Z', 'failed'],
    ] as const) {
      createRunDir(id, spec);
      const rec = baseRecord(id);
      rec.createdAt = ts;
      rec.state = state;
      saveRun(rec);
    }
    expect(listRuns().map((r) => r.runId)).toEqual(['new', 'old']);
    expect(listRuns({ state: 'completed' }).map((r) => r.runId)).toEqual(['old']);
    expect(listRuns({ workflow: 'nothing' })).toEqual([]);
    expect(listRuns({ limit: 1 })).toHaveLength(1);
  });

  it('is empty when nothing has run', () => {
    expect(listRuns()).toEqual([]);
  });

  it('skips a directory whose spec is gone rather than failing the listing', () => {
    // Inherited from the enumerators this replaced: a workspace that was moved
    // or deleted out from under a run used to be dropped from the autoloop
    // registry listing. One unreadable run must not make the rest invisible.
    createRunDir('good', spec);
    saveRun(baseRecord('good'));
    fs.mkdirSync(path.join(tmp, 'half-deleted'), { recursive: true });
    expect(listRuns().map((r) => r.runId)).toEqual(['good']);
  });

  it('ignores directories whose names are not valid run ids', () => {
    fs.mkdirSync(path.join(tmp, '..hidden'), { recursive: true });
    fs.mkdirSync(path.join(tmp, '.tmp-junk'), { recursive: true });
    createRunDir('real', spec);
    saveRun(baseRecord('real'));
    expect(listRuns().map((r) => r.runId)).toEqual(['real']);
  });
});
