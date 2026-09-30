import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import { UltraappBuildQueue } from '../../ultraapp/build.js';
import type { BuildEvent } from '../../ultraapp/build-events.js';

describe('UltraappBuildQueue', () => {
  it('runs queued builds serially', async () => {
    const order: string[] = [];
    const worker = vi.fn().mockImplementation(async (runId: string) => {
      order.push(`start ${runId}`);
      await new Promise((r) => setTimeout(r, 20));
      order.push(`end ${runId}`);
    });
    const q = new UltraappBuildQueue({ worker });
    await Promise.all([q.enqueue('a'), q.enqueue('b'), q.enqueue('c')]);
    await q.idle();
    expect(order).toEqual(['start a', 'end a', 'start b', 'end b', 'start c', 'end c']);
  });

  it('reports queue position', async () => {
    const releases: Array<() => void> = [];
    const worker = vi.fn().mockImplementation(() => new Promise<void>((r) => releases.push(r)));
    const q = new UltraappBuildQueue({ worker });
    await q.enqueue('a');
    await q.enqueue('b');
    await q.enqueue('c');
    // a is in flight (position 0), b/c pending
    expect(q.position('a')).toBe(0);
    expect(q.position('b')).toBe(1);
    expect(q.position('c')).toBe(2);
    // Release all in order so the queue can drain
    while (releases.length || q.position('a') === 0) {
      const r = releases.shift();
      if (!r) {
        await new Promise((res) => setTimeout(res, 5));
        continue;
      }
      r();
      await new Promise((res) => setTimeout(res, 5));
    }
    await q.idle();
  });

  it('emits queued event with position when enqueued behind another build', async () => {
    const releases: Array<() => void> = [];
    const worker = vi.fn().mockImplementation(() => new Promise<void>((r) => releases.push(r)));
    const events: BuildEvent[] = [];
    const q = new UltraappBuildQueue({ worker });
    q.subscribe((e) => events.push(e));
    await q.enqueue('a');
    await q.enqueue('b');
    expect(events.find((e) => e.type === 'queued' && e.runId === 'b')).toBeTruthy();
    while (releases.length) releases.shift()!();
    // Drain
    for (let i = 0; i < 10 && releases.length === 0; i++) {
      await new Promise((res) => setTimeout(res, 5));
    }
    while (releases.length) releases.shift()!();
    await q.idle();
  });

  it('cancel removes pending', async () => {
    const releases: Array<() => void> = [];
    const worker = vi.fn().mockImplementation(() => new Promise<void>((r) => releases.push(r)));
    const q = new UltraappBuildQueue({ worker });
    await q.enqueue('a');
    await q.enqueue('b');
    q.cancel('b');
    expect(q.position('b')).toBe(-1);
    while (releases.length) releases.shift()!();
    await q.idle();
    expect(worker).toHaveBeenCalledTimes(1);
  });

  it('emits build-failed when worker throws', async () => {
    const worker = vi.fn().mockRejectedValue(new Error('boom'));
    const events: BuildEvent[] = [];
    const q = new UltraappBuildQueue({ worker });
    q.subscribe((e) => events.push(e));
    await q.enqueue('a');
    await q.idle();
    const failed = events.find((e) => e.type === 'build-failed');
    expect(failed).toBeTruthy();
    expect(failed!.type === 'build-failed' && failed.reason).toMatch(/boom/);
  });

  it('subscribe returns unsubscribe fn', async () => {
    const events: BuildEvent[] = [];
    const q = new UltraappBuildQueue({ worker: vi.fn().mockResolvedValue(undefined) });
    const off = q.subscribe((e) => events.push(e));
    off();
    await q.enqueue('a');
    await q.idle();
    expect(events).toEqual([]);
  });
});

// ─── Durability (6.0.0) ─────────────────────────────────────────────────────

describe('durable queue', () => {
  let dir: string;
  let statePath: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-bq-'));
    statePath = path.join(dir, 'build-queue.json');
  });

  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('survives a process that died, restoring the in-flight build first', async () => {
    // The old queue kept pending builds in an array and nothing else: a restart
    // dropped every queued build with no record it had been asked for.
    //
    // The dead owner is simulated by writing a state file whose pid cannot
    // exist. Constructing a live queue and then a second one in the SAME
    // process would prove the opposite of what this test is for — that two
    // owners can run the same builds concurrently.
    fs.writeFileSync(
      statePath,
      JSON.stringify({
        pending: ['run-b'],
        current: 'run-a',
        owner: { ownerId: 'gone', pid: 2 ** 30, renewedAt: new Date().toISOString() },
      }),
    );

    const seen: string[] = [];
    const restored: string[][] = [];
    const q = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
      },
      onRestore: (ids) => restored.push(ids),
    });
    await q.idle();

    expect(q.ownsQueue()).toBe(true);
    // The in-flight build comes back first: it was asked for first, and the
    // user has been waiting on it longest.
    expect(restored[0]).toEqual(['run-a', 'run-b']);
    expect(seen).toEqual(['run-a', 'run-b']);
  });

  it('surfaces adopted lock cleanup failure without retrying or dispatching the queued build', async () => {
    // This catches persist treating terminal cleanup failure as transient lock
    // contention and retrying queue ownership.
    const seen: string[] = [];
    const q = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
      },
    });
    const reclaimPath = `${statePath}.lock.reclaim`;
    fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    fs.utimesSync(reclaimPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error('injected adopted queue-lock reclaim release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (String(target).startsWith(`${reclaimPath}.adopt-`)) throw releaseFailure;
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    try {
      await expect(q.enqueue('cleanup-failed-build')).rejects.toThrow(/lock cleanup failed/i);
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
      q.stop();
      fs.rmSync(`${statePath}.lock`, { force: true });
      fs.rmSync(reclaimPath, { force: true });
    }

    expect(seen).toEqual([]);
    expect(q.ownsQueue()).toBe(false);
  });

  it('returns committed terminal provenance after enqueue writes but its lock release fails, and a successor runs it once', async () => {
    // This catches enqueue treating a post-callback release failure as though
    // the durable row did not exist, which let a caller retry a build a
    // successor would also restore.
    const seen: string[] = [];
    const q = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
      },
    });
    const lockPath = `${statePath}.lock`;
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error('injected post-enqueue lock release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (target === lockPath) throw releaseFailure;
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    let failure: unknown;
    try {
      await q.enqueue('post-enqueue-release-failure').catch((error: unknown) => {
        failure = error;
      });
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(failure).toMatchObject({
      code: 'ULTRAAPP_ENQUEUE_LOCK_CLEANUP_FAILED',
      committed: true,
      retryable: false,
      cause: { name: 'FileLockReleaseError', cause: releaseFailure },
      identity: { runId: 'post-enqueue-release-failure', ownerId: q.ownerId },
    });
    expect(JSON.parse(fs.readFileSync(statePath, 'utf8'))).toMatchObject({
      pending: ['post-enqueue-release-failure'],
      current: null,
      owner: { ownerId: q.ownerId },
    });
    expect(q.ownsQueue()).toBe(false);
    expect(seen).toEqual([]);

    // Model the failed process dying: its unreleased lock and its live pid no
    // longer fence the durable row. A fresh owner must recover exactly this
    // one recorded logical build, not a retry plus the restored copy.
    fs.rmSync(lockPath, { force: true });
    const persisted = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    fs.writeFileSync(
      statePath,
      JSON.stringify({ ...persisted, owner: { ...persisted.owner, ownerId: 'dead-owner', pid: 2 ** 30 } }),
    );
    const successor = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
      },
    });
    await successor.idle();

    expect(seen).toEqual(['post-enqueue-release-failure']);
    q.stop();
    successor.stop();
  });

  it('keeps active-work provenance while proving a post-release-failure successor durable exactly once', async () => {
    // This catches the proof treating the active current row as evidence that
    // the separately queued successor was not committed.  Retrying that
    // successor would create a second logical submission for the row a fresh
    // owner restores.
    const seen: string[] = [];
    let releaseActive!: () => void;
    let activeEntered!: () => void;
    const activeStarted = new Promise<void>((resolve) => {
      activeEntered = resolve;
    });
    const activeDone = new Promise<void>((resolve) => {
      releaseActive = resolve;
    });
    const q = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
        if (runId === 'active-build') {
          activeEntered();
          await activeDone;
        }
      },
    });
    await q.enqueue('active-build');
    await activeStarted;

    const lockPath = `${statePath}.lock`;
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error('injected active-queue release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (target === lockPath) throw releaseFailure;
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    let failure: unknown;
    try {
      await q.enqueue('durable-successor').catch((error: unknown) => {
        failure = error;
      });
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(failure).toMatchObject({
      code: 'ULTRAAPP_ENQUEUE_LOCK_CLEANUP_FAILED',
      committed: true,
      retryable: false,
      cause: { name: 'FileLockReleaseError', cause: releaseFailure },
      identity: { runId: 'durable-successor', ownerId: q.ownerId },
    });
    expect(JSON.parse(fs.readFileSync(statePath, 'utf8'))).toMatchObject({
      current: 'active-build',
      pending: ['durable-successor'],
      owner: { ownerId: q.ownerId },
    });
    expect(seen).toEqual(['active-build']);

    releaseActive();
    await new Promise((resolve) => setTimeout(resolve, 0));

    // The failed owner never dispatches the durable successor locally.
    expect(seen).toEqual(['active-build']);
    fs.rmSync(lockPath, { force: true });
    const persisted = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    fs.writeFileSync(
      statePath,
      JSON.stringify({ ...persisted, owner: { ...persisted.owner, ownerId: 'dead-owner', pid: 2 ** 30 } }),
    );
    const successor = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
      },
    });
    await successor.idle();

    expect(seen.filter((runId) => runId === 'durable-successor')).toEqual(['durable-successor']);
    q.stop();
    successor.stop();
  });

  it('settles idle after an active worker exits following cleanup stand-down without dispatching its successor', async () => {
    // This catches the terminal persist path returning superseded after the
    // active worker clears currentRunId but forgetting to settle idle.
    const seen: string[] = [];
    let releaseActive!: () => void;
    let activeEntered!: () => void;
    const activeStarted = new Promise<void>((resolve) => {
      activeEntered = resolve;
    });
    const activeDone = new Promise<void>((resolve) => {
      releaseActive = resolve;
    });
    const q = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
        if (runId === 'idle-active-build') {
          activeEntered();
          await activeDone;
        }
      },
    });
    await q.enqueue('idle-active-build');
    await activeStarted;
    const idle = q.idle();

    const lockPath = `${statePath}.lock`;
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error('injected idle-settlement release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (target === lockPath) throw releaseFailure;
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();
    try {
      await expect(q.enqueue('idle-successor')).rejects.toMatchObject({ committed: true, retryable: false });
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
    }

    releaseActive();
    await expect(
      Promise.race([
        idle.then(() => 'settled'),
        new Promise<'timed-out'>((resolve) => setTimeout(() => resolve('timed-out'), 100)),
      ]),
    ).resolves.toBe('settled');
    expect(seen).toEqual(['idle-active-build']);
    q.stop();
  });

  it('stands down after restore writes durable work but its lock release fails before heartbeat setup', async () => {
    // This catches restore starting a heartbeat after a post-callback cleanup
    // failure, which stranded the row under a live owner that never dispatches.
    fs.writeFileSync(
      statePath,
      JSON.stringify({
        pending: ['restore-post-release-failure'],
        current: null,
        owner: { ownerId: 'gone', pid: 2 ** 30, renewedAt: new Date().toISOString() },
      }),
    );
    const seen: string[] = [];
    const lockPath = `${statePath}.lock`;
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error('injected post-restore lock release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (target === lockPath) throw releaseFailure;
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();
    vi.useFakeTimers();

    let q: UltraappBuildQueue;
    try {
      q = new UltraappBuildQueue({
        statePath,
        worker: async (runId) => {
          seen.push(runId);
        },
      });
      await vi.advanceTimersByTimeAsync(45_000);
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(q!.ownsQueue()).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
    expect(seen).toEqual([]);
    expect(JSON.parse(fs.readFileSync(statePath, 'utf8'))).toMatchObject({
      pending: ['restore-post-release-failure'],
      current: null,
    });

    fs.rmSync(lockPath, { force: true });
    const persisted = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    fs.writeFileSync(
      statePath,
      JSON.stringify({ ...persisted, owner: { ...persisted.owner, ownerId: 'dead-owner', pid: 2 ** 30 } }),
    );
    const successor = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
      },
    });
    await successor.idle();

    expect(seen).toEqual(['restore-post-release-failure']);
    q!.stop();
    successor.stop();
    vi.useRealTimers();
  });

  it('refuses to take builds a live OTHER process already owns', async () => {
    // Two owners restoring the same file would each run every build — every side
    // effect twice.
    //
    // The owner has to be a different live process, so pid 1 stands in: it
    // always exists, it is never us, and `kill(1, 0)` reports it as alive.
    // Constructing two queues in this process would not test this at all —
    // same-pid re-entrancy is allowed on purpose, so a manager can rebuild its
    // own queue.
    fs.writeFileSync(
      statePath,
      JSON.stringify({
        pending: ['run-a'],
        current: null,
        owner: { ownerId: 'other', pid: 1, renewedAt: new Date().toISOString() },
      }),
    );

    const seen: string[] = [];
    let refusedTo: { pid: number } | undefined;
    const second = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
      },
      onNotOwner: (o) => {
        refusedTo = o;
      },
    });

    await new Promise((r) => setTimeout(r, 20));
    expect(second.ownsQueue()).toBe(false);
    expect(refusedTo?.ownerId).toBe('other');
    expect(seen).toEqual([]);
  });

  it('takes over from an owner whose heartbeat has gone stale', async () => {
    fs.writeFileSync(
      statePath,
      JSON.stringify({
        pending: ['run-a'],
        current: null,
        owner: { ownerId: 'other', pid: 1, renewedAt: new Date(Date.now() - 10 * 60_000).toISOString() },
      }),
    );
    const seen: string[] = [];
    const q = new UltraappBuildQueue({
      statePath,
      worker: async (runId) => {
        seen.push(runId);
      },
    });
    await q.idle();
    expect(q.ownsQueue()).toBe(true);
    expect(seen).toEqual(['run-a']);
  });

  it('clears the state once the queue drains', async () => {
    const q = new UltraappBuildQueue({ statePath, worker: async () => undefined });
    await q.enqueue('run-a');
    await q.idle();
    expect(JSON.parse(fs.readFileSync(statePath, 'utf8'))).toMatchObject({ pending: [], current: null });
  });

  it('forgets a cancelled build', async () => {
    const blocked = new Promise<void>(() => undefined);
    const q = new UltraappBuildQueue({ statePath, worker: () => blocked });
    await q.enqueue('run-a');
    await q.enqueue('run-b');
    await new Promise((r) => setTimeout(r, 10));
    q.cancel('run-b');
    expect(JSON.parse(fs.readFileSync(statePath, 'utf8')).pending).toEqual([]);
  });

  it('ignores an unreadable or corrupt state file rather than refusing to start', () => {
    fs.writeFileSync(statePath, '{not json');
    const restored: string[][] = [];
    new UltraappBuildQueue({ statePath, worker: async () => undefined, onRestore: (ids) => restored.push(ids) });
    expect(restored).toEqual([]);
  });

  it('stays ephemeral when no statePath is given', async () => {
    const q = new UltraappBuildQueue({ worker: async () => undefined });
    await q.enqueue('run-a');
    await q.idle();
    expect(fs.existsSync(statePath)).toBe(false);
  });
});
