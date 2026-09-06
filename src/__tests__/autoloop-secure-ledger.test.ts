import { afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import {
  openPrivateAutoloopDecisions,
  SecureAutoloopLedger,
  SecureAutoloopLedgerCommitError,
  type SecureAutoloopIterationArtifact,
  type SecureAutoloopFlatFile,
} from '../autoloop/secure-ledger.js';
import { Msg } from '../autoloop/messages.js';
import { AutoloopRunner } from '../autoloop/runner.js';
import { appendPushLog } from '../autoloop/notify.js';
import { SessionManager } from '../session-manager.js';
import type { AgentReservationReleaseOptions, AutoloopState, PhysicalAgentGeneration } from '../autoloop/types.js';

const roots: string[] = [];

function tempWorkspace(): string {
  const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-secure-ledger-'));
  roots.push(workspace);
  return workspace;
}

function permissions(target: string): number {
  return fs.statSync(target).mode & 0o777;
}

function countRows(target: string, kind?: string): number {
  if (!fs.existsSync(target)) return 0;
  return fs
    .readFileSync(target, 'utf8')
    .split('\n')
    .filter((line) => line.trim())
    .filter((line) => !kind || (JSON.parse(line) as { kind?: unknown }).kind === kind).length;
}

function seedCompleteReviewerArtifacts(ledger: SecureAutoloopLedger, iter: number): void {
  ledger.writeIterationArtifact(iter, 'directive.json', `directive-${iter}\n`);
  ledger.writeIterationArtifact(iter, 'eval_output.json', `eval-${iter}\n`);
  ledger.writeIterationArtifact(iter, 'coder_summary.txt', `summary-${iter}\n`);
  ledger.writeIterationArtifact(iter, 'diff.patch', `diff-${iter}\n`);
}

function replaceWithSameBytes(target: string): { before: fs.Stats; after: fs.Stats } {
  const before = fs.lstatSync(target);
  const replacement = `${target}.same-content-replacement`;
  fs.writeFileSync(replacement, fs.readFileSync(target), { mode: 0o600 });
  fs.renameSync(replacement, target);
  return { before, after: fs.lstatSync(target) };
}

function lstatIfPresentForTest(target: string): fs.Stats | undefined {
  try {
    return fs.lstatSync(target);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
    throw error;
  }
}

const TEST_OWNER_INSTANCE_ID = `session-manager:${process.pid}:00000000-0000-4000-8000-000000000099`;

function stubGenerationManager() {
  const reservations = new Map<string, PhysicalAgentGeneration>();
  const releaseReservation = vi.fn(
    async (name: string, generation: number, options: AgentReservationReleaseOptions): Promise<boolean> => {
      if (!options.rollbackUncommittedReservation) return false;
      const current = reservations.get(name);
      if (
        current?.generation !== generation ||
        current.owner_instance_id !== options.expectedOwnerInstanceId ||
        current.session_id !== options.expectedSessionId
      ) {
        return false;
      }
      reservations.delete(name);
      return true;
    },
  );
  const manager = {
    autoloopOwnerInstanceId: TEST_OWNER_INSTANCE_ID,
    reserveAgentGeneration: vi.fn((generation: PhysicalAgentGeneration) => {
      if (reservations.has(generation.session_name)) return false;
      reservations.set(generation.session_name, generation);
      return true;
    }),
    releaseReservation,
    inspect: vi.fn(async () => 'absent' as const),
    startSession: vi.fn(async () => undefined),
    stopSession: vi.fn(async () => undefined),
    sendMessage: vi.fn(async () => ({ output: '', error: undefined })),
    getStatus: vi.fn(() => ({ stats: { turns: 0, turnsSucceeded: 0, contextPercent: 0 } })),
    compactSession: vi.fn(async () => undefined),
  };
  return { manager, reservations, releaseReservation };
}

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe('SecureAutoloopLedger', () => {
  it('hardens pre-existing tasks and run directories without adding owner permissions', () => {
    const workspace = tempWorkspace();
    const tasksDir = path.join(workspace, 'tasks');
    const runDir = path.join(tasksDir, 'run-1');
    fs.mkdirSync(runDir, { recursive: true });
    const chatPath = path.join(runDir, 'chat.jsonl');
    fs.writeFileSync(chatPath, '{"preserved":true}\n', { mode: 0o444 });
    fs.chmodSync(tasksDir, 0o577);
    fs.chmodSync(runDir, 0o577);

    try {
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });

      expect(ledger.directory).toBe(runDir);
      expect(permissions(tasksDir)).toBe(0o500);
      expect(permissions(runDir)).toBe(0o500);
      expect(permissions(chatPath)).toBe(0o400);
      expect(ledger.readFlatFile('chat.jsonl')).toBe('{"preserved":true}\n');
    } finally {
      fs.chmodSync(tasksDir, 0o700);
      fs.chmodSync(runDir, 0o700);
    }
  });

  it.each(['symlink', 'file'] as const)('rejects a %s tasks parent', (kind) => {
    const workspace = tempWorkspace();
    const tasksDir = path.join(workspace, 'tasks');
    if (kind === 'symlink') {
      const external = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-ledger-external-'));
      roots.push(external);
      fs.symlinkSync(external, tasksDir, 'dir');
    } else {
      fs.writeFileSync(tasksDir, 'not a directory');
    }

    expect(() => SecureAutoloopLedger.open(workspace, 'run-1', { create: true })).toThrow(/tasks parent|directory/i);
  });

  it.each(['symlink', 'file'] as const)('rejects a %s run directory', (kind) => {
    const workspace = tempWorkspace();
    const tasksDir = path.join(workspace, 'tasks');
    const runDir = path.join(tasksDir, 'run-1');
    fs.mkdirSync(tasksDir);
    if (kind === 'symlink') {
      const external = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-ledger-external-'));
      roots.push(external);
      fs.symlinkSync(external, runDir, 'dir');
    } else {
      fs.writeFileSync(runDir, 'not a directory');
    }

    expect(() => SecureAutoloopLedger.open(workspace, 'run-1', { create: true })).toThrow(/run directory|directory/i);
  });

  it('pins tasks and run identities and rejects a post-start parent swap', () => {
    const workspace = tempWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"before":true}\n');

    const tasksDir = path.join(workspace, 'tasks');
    fs.renameSync(tasksDir, path.join(workspace, 'tasks.saved'));
    fs.mkdirSync(path.join(tasksDir, 'run-1'), { recursive: true });

    const replacement = path.join(tasksDir, 'run-1');

    expect(() => ledger.readFlatFile('decisions.jsonl')).toThrow(/identity|changed|replaced/i);
    expect(() => ledger.appendFlatFile('chat.jsonl', '{"after":true}\n')).toThrow(/identity|changed|replaced/i);
    expect(fs.readdirSync(replacement)).toEqual([]);
  });

  it('pins the run identity and rejects a post-start run-directory swap', () => {
    const workspace = tempWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
    const runDir = ledger.directory;
    fs.renameSync(runDir, `${runDir}.saved`);
    fs.mkdirSync(runDir);

    expect(() => ledger.appendFlatFile('agent-generations.jsonl', '{}\n')).toThrow(/identity|changed|replaced/i);
    expect(fs.readdirSync(runDir)).toEqual([]);
  });

  it.each(['decisions.jsonl', 'agent-generations.jsonl', 'chat.jsonl', 'push_log.jsonl'] as const)(
    'rejects a final symbolic link for %s without mutating its target',
    (name) => {
      const workspace = tempWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
      const external = path.join(workspace, `${name}.external`);
      fs.writeFileSync(external, 'protected');
      fs.symlinkSync(external, path.join(ledger.directory, name));

      expect(() => ledger.readFlatFile(name)).toThrow(/symbolic link|unsafe/i);
      expect(() => ledger.appendFlatFile(name, 'mutated\n')).toThrow(/symbolic link|unsafe/i);
      expect(fs.readFileSync(external, 'utf8')).toBe('protected');
    },
  );

  it.each(['decisions.jsonl', 'agent-generations.jsonl', 'chat.jsonl', 'push_log.jsonl'] as const)(
    'rejects a hardlink for %s before changing bytes or permissions',
    (name) => {
      const workspace = tempWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
      const external = path.join(workspace, `${name}.external`);
      fs.writeFileSync(external, 'protected', { mode: 0o664 });
      fs.chmodSync(external, 0o664);
      fs.linkSync(external, path.join(ledger.directory, name));
      const modeBefore = permissions(external);

      expect(() => ledger.readFlatFile(name)).toThrow(/hardlink|link count|unsafe/i);
      expect(() => ledger.appendFlatFile(name, 'mutated\n')).toThrow(/hardlink|link count|unsafe/i);
      expect(fs.readFileSync(external, 'utf8')).toBe('protected');
      expect(permissions(external)).toBe(modeBefore);
    },
  );

  it('reads and appends through checked descriptors and creates private files', () => {
    const workspace = tempWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });

    ledger.appendFlatFile('chat.jsonl', '{"n":1}\n');
    ledger.appendFlatFile('chat.jsonl', '{"n":2}\n');

    expect(ledger.readFlatFile('chat.jsonl')).toBe('{"n":1}\n{"n":2}\n');
    expect(permissions(path.join(ledger.directory, 'chat.jsonl'))).toBe(0o600);
  });

  it('rejects a hardlink inserted immediately before permission mutation', () => {
    const workspace = tempWorkspace();
    const runDir = path.join(workspace, 'tasks', 'run-1');
    fs.mkdirSync(runDir, { recursive: true });
    const ledgerPath = path.join(runDir, 'decisions.jsonl');
    const external = path.join(workspace, 'external-hardlink');
    fs.writeFileSync(ledgerPath, 'protected\n', { mode: 0o664 });
    fs.chmodSync(ledgerPath, 0o664);

    expect(() =>
      SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeFileMutation: (event: { name: string; operation: string; filePath: string }) => {
            if (event.name === 'decisions.jsonl' && event.operation === 'chmod') {
              fs.linkSync(event.filePath, external);
            }
          },
        },
      } as never),
    ).toThrow(/hardlink|link count|unsafe/i);
    expect(fs.readFileSync(external, 'utf8')).toBe('protected\n');
    expect(permissions(external)).toBe(0o664);
  });

  it('rejects a hardlink inserted immediately before append mutation', () => {
    const workspace = tempWorkspace();
    const external = path.join(workspace, 'external-hardlink');
    let armHardlink = false;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      testHooks: {
        beforeFileMutation: (event: { name: string; operation: string; filePath: string }) => {
          if (armHardlink && event.name === 'decisions.jsonl' && event.operation === 'append') {
            fs.linkSync(event.filePath, external);
          }
        },
      },
    } as never);
    ledger.appendFlatFile('decisions.jsonl', 'protected\n');
    armHardlink = true;

    expect(() => ledger.appendFlatFile('decisions.jsonl', 'mutated\n')).toThrow(/hardlink|link count|unsafe/i);
    expect(fs.readFileSync(external, 'utf8')).toBe('protected\n');
    expect(permissions(external)).toBe(0o600);
  });

  it('flushes both the file and its parent directory', () => {
    const workspace = tempWorkspace();
    const events: string[] = [];
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      testHooks: {
        beforeFileMutation: (event: { operation: string }) => events.push(event.operation),
        beforeDirectorySync: () => events.push('directory-sync'),
      },
    } as never);
    ledger.appendFlatFile('decisions.jsonl', '{"row":1}\n');
    events.length = 0;

    ledger.flushFlatFile('decisions.jsonl');

    expect(events).toEqual(['flush', 'directory-sync']);
  });

  it('reports the explicit win32 parent-directory durability limitation', () => {
    const workspace = tempWorkspace();
    const warn = vi.fn();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      platform: 'win32',
      logger: { warn },
    } as never);
    ledger.appendFlatFile('decisions.jsonl', '{"row":1}\n');

    ledger.flushFlatFile('decisions.jsonl');

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/parent-directory fsync.*win32/i));
  });

  it.each([
    {
      barrier: 'file',
      code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
    },
    {
      barrier: 'directory',
      code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
    },
  ] as const)(
    'classifies a post-write $barrier-sync failure and resumes durability without appending twice',
    ({ barrier, code }) => {
      const workspace = tempWorkspace();
      let failBarrier = true;
      let appendChecks = 0;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeFileMutation: (event: { operation: string }) => {
            if (event.operation === 'append') appendChecks++;
            if (barrier === 'file' && event.operation === 'flush' && failBarrier) {
              failBarrier = false;
              throw new Error('injected file fsync failure');
            }
          },
          beforeDirectorySync: () => {
            if (barrier === 'directory' && failBarrier) {
              failBarrier = false;
              throw new Error('injected directory fsync failure');
            }
          },
        },
      } as never);
      const prepared = (
        ledger as unknown as {
          prepareFlatFileAppend(
            name: SecureAutoloopFlatFile,
            content: string,
          ): {
            committed: boolean;
            commitDurable(): void;
            close(): void;
          };
        }
      ).prepareFlatFileAppend('decisions.jsonl', '{"kind":"prepared"}\n');

      try {
        expect(() => prepared.commitDurable()).toThrow(
          expect.objectContaining({
            code,
            committed: true,
          }),
        );
        expect(prepared.committed).toBe(true);
        expect(() => prepared.commitDurable()).not.toThrow();
        expect(() => prepared.commitDurable()).not.toThrow();
      } finally {
        prepared.close();
      }
      expect(appendChecks).toBe(1);
      expect(countRows(path.join(ledger.directory, 'decisions.jsonl'), 'prepared')).toBe(1);
    },
  );

  it('does not roll back a generation reservation after its row was committed', async () => {
    const workspace = tempWorkspace();
    const { manager, releaseReservation } = stubGenerationManager();
    let failGenerationDirectorySync = true;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      testHooks: {
        beforeDirectorySync: (event: { name?: string }) => {
          if (event.name === 'agent-generations.jsonl' && failGenerationDirectorySync) {
            failGenerationDirectorySync = false;
            throw new Error('injected generation directory fsync failure');
          }
        },
      },
    } as never);
    const dispatcher = new ClaudeAgentDispatcher({
      manager: manager as never,
      runId: 'run-1',
      workspace,
      secureLedger: ledger,
      ownerInstanceId: TEST_OWNER_INSTANCE_ID,
    });

    await expect(dispatcher.init({} as AutoloopState)).rejects.toMatchObject({
      code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
      committed: true,
    });
    expect(releaseReservation).not.toHaveBeenCalledWith(
      'autoloop-run-1-planner',
      1,
      expect.objectContaining({ rollbackUncommittedReservation: true }),
    );
    expect(countRows(path.join(ledger.directory, 'agent-generations.jsonl'), 'agent_generation_reserved')).toBe(1);
  });

  it('keeps one pinned capability across staged resume work and rejects a run swap before commit', () => {
    const workspace = tempWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      validateExistingFlatFiles: ['decisions.jsonl'],
    } as never);
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"existing"}\n');
    expect(ledger.readFlatFile('decisions.jsonl')).toContain('existing');
    const prepared = (
      ledger as unknown as {
        prepareFlatFileAppend(
          name: SecureAutoloopFlatFile,
          content: string,
        ): {
          commitDurable(): void;
          close(): void;
        };
      }
    ).prepareFlatFileAppend('decisions.jsonl', '{"kind":"timeout_migration"}\n');
    const runDir = ledger.directory;
    const originalDir = `${runDir}.saved`;
    fs.renameSync(runDir, originalDir);
    fs.mkdirSync(runDir);

    try {
      expect(() => prepared.commitDurable()).toThrow(/identity|changed|replaced/i);
    } finally {
      prepared.close();
    }
    expect(countRows(path.join(originalDir, 'decisions.jsonl'), 'timeout_migration')).toBe(0);
    expect(fs.readdirSync(runDir)).toEqual([]);
  });

  it('refuses to boot runtime sessions when a supplied resume capability no longer matches the run path', async () => {
    const workspace = tempWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      validateExistingFlatFiles: ['decisions.jsonl'],
    } as never);
    const runDir = ledger.directory;
    fs.renameSync(runDir, `${runDir}.saved`);
    fs.mkdirSync(runDir);
    const manager = new SessionManager({ maxConcurrentSessions: 1 });
    const startSession = vi.spyOn(manager, 'startSession').mockRejectedValue(new Error('runtime boot attempted'));

    await expect(
      (
        manager as unknown as {
          _bootAutoloop(options: Record<string, unknown>): Promise<unknown>;
        }
      )._bootAutoloop({ runId: 'run-1', workspace, _secureLedger: ledger }),
    ).rejects.toThrow(/identity|changed|replaced/i);
    expect(startSession).not.toHaveBeenCalled();
    expect(fs.readdirSync(runDir)).toEqual([]);
    await manager.shutdown();
  });

  it('rejects an unsafe unrelated flat sibling during full boot before starting a physical session', async () => {
    const workspace = tempWorkspace();
    const runDir = path.join(workspace, 'tasks', 'run-1');
    const external = path.join(workspace, 'external-chat');
    fs.mkdirSync(runDir, { recursive: true });
    fs.writeFileSync(path.join(runDir, 'decisions.jsonl'), '{"kind":"existing"}\n');
    fs.writeFileSync(external, 'protected');
    fs.symlinkSync(external, path.join(runDir, 'chat.jsonl'));
    const manager = new SessionManager({ maxConcurrentSessions: 1 });
    const startSession = vi.spyOn(manager, 'startSession').mockRejectedValue(new Error('runtime boot attempted'));

    await expect(
      (
        manager as unknown as {
          _bootAutoloop(options: Record<string, unknown>): Promise<unknown>;
        }
      )._bootAutoloop({ runId: 'run-1', workspace }),
    ).rejects.toThrow(/symbolic link|unsafe/i);
    expect(startSession).not.toHaveBeenCalled();
    expect(fs.readFileSync(external, 'utf8')).toBe('protected');
    await manager.shutdown();
  });

  it('opens decisions lazily without validating an unrelated unsafe flat sibling', () => {
    const workspace = tempWorkspace();
    const runDir = path.join(workspace, 'tasks', 'run-1');
    fs.mkdirSync(runDir, { recursive: true });
    fs.writeFileSync(path.join(runDir, 'decisions.jsonl'), '{"kind":"safe"}\n');
    fs.mkdirSync(path.join(runDir, 'chat.jsonl'));

    const handle = openPrivateAutoloopDecisions(workspace, 'run-1', 'read');
    try {
      expect(fs.readFileSync(handle.fd, 'utf8')).toContain('safe');
    } finally {
      fs.closeSync(handle.fd);
    }
  });

  it('rejects read plus create before creating tasks or run directories', () => {
    const workspace = tempWorkspace();

    expect(() => openPrivateAutoloopDecisions(workspace, 'run-1', 'read', true)).toThrow(/read.*create/i);
    expect(fs.existsSync(path.join(workspace, 'tasks'))).toBe(false);
  });

  it('rejects a non-regular pre-existing flat ledger target', () => {
    const workspace = tempWorkspace();
    const runDir = path.join(workspace, 'tasks', 'run-1');
    fs.mkdirSync(path.join(runDir, 'chat.jsonl'), { recursive: true });

    expect(() => SecureAutoloopLedger.open(workspace, 'run-1')).toThrow(/non-regular/i);
  });

  it('preserves missing owner write permission and fails a later append closed', () => {
    const workspace = tempWorkspace();
    const runDir = path.join(workspace, 'tasks', 'run-1');
    fs.mkdirSync(runDir, { recursive: true });
    const decisionsPath = path.join(runDir, 'decisions.jsonl');
    fs.writeFileSync(decisionsPath, '{"preserved":true}\n', { mode: 0o400 });
    fs.chmodSync(decisionsPath, 0o400);
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1');

    expect(() => ledger.appendFlatFile('decisions.jsonl', '{"must_not_append":true}\n')).toThrow();
    expect(fs.readFileSync(decisionsPath, 'utf8')).toBe('{"preserved":true}\n');
    expect(permissions(decisionsPath)).toBe(0o400);
  });

  it('keeps the same protections when optional POSIX open flags are unavailable', () => {
    const workspace = tempWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      platformFlags: { noFollow: 0, directory: 0 },
    });

    ledger.appendFlatFile('decisions.jsonl', '{"safe":true}\n');
    expect(ledger.readFlatFile('decisions.jsonl')).toBe('{"safe":true}\n');

    const external = path.join(workspace, 'external-chat');
    fs.writeFileSync(external, 'protected');
    fs.symlinkSync(external, path.join(ledger.directory, 'chat.jsonl'));
    expect(() => ledger.appendFlatFile('chat.jsonl', 'mutated\n')).toThrow(/symbolic link|unsafe/i);
    expect(fs.readFileSync(external, 'utf8')).toBe('protected');
  });

  it('keeps legacy recursive push-log callers compatible while reporting an unsafe final target', () => {
    const workspace = tempWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
    const entry = {
      ts: '2026-09-06T00:00:00.000Z',
      level: 'info' as const,
      summary: 'secure push',
      channel_requested: 'auto' as const,
      channel_used: 'none',
    };

    appendPushLog(ledger, entry);
    expect(ledger.readFlatFile('push_log.jsonl')).toContain('secure push');

    const legacyRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-pushlog-legacy-'));
    roots.push(legacyRoot);
    const legacyDir = path.join(legacyRoot, 'nested', 'run');
    appendPushLog(legacyDir, entry);
    expect(fs.readFileSync(path.join(legacyDir, 'push_log.jsonl'), 'utf8')).toContain('secure push');

    const external = path.join(legacyRoot, 'external-push');
    fs.writeFileSync(external, 'protected');
    fs.unlinkSync(path.join(legacyDir, 'push_log.jsonl'));
    fs.symlinkSync(external, path.join(legacyDir, 'push_log.jsonl'));
    const warn = vi.fn();
    appendPushLog(legacyDir, entry, { warn } as never);
    expect(fs.readFileSync(external, 'utf8')).toBe('protected');
    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn).toHaveBeenCalledWith(expect.stringMatching(/push log.*symbolic link/i));
  });

  it('preserves committed-state-invalid classification through the runner boundary', async () => {
    const primary = new SecureAutoloopLedgerCommitError(
      'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
      'Reviewer sandbox committed state diverged',
      {
        cause: new Error('staged identity changed'),
        operation: 'secure_reviewer_sandbox_stage',
      },
    );
    const runner = new AutoloopRunner({
      run_id: 'run-1',
      workspace: tempWorkspace(),
      ledger_dir: '/unused',
      dispatcher: {
        deliver: vi.fn(async () => {
          throw primary;
        }),
      },
      notifyUser: vi.fn(async () => undefined),
      stallCheckIntervalMs: 24 * 60 * 60 * 1000,
    });
    let observed: unknown;
    runner.on('phase_error', (payload) => {
      observed = payload;
    });

    try {
      await runner.start();
      await expect(runner.chat('exercise committed classification')).rejects.toBe(primary);
      expect(observed).toMatchObject({
        code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
        committed: true,
        retryable: false,
      });
    } finally {
      runner.stop();
    }
  });

  describe('nested iteration artifacts', () => {
    it('creates private iteration artifacts and makes identical writes idempotent but conflicts immutable', () => {
      const workspace = tempWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
      const bytes = Buffer.from('{"goal":"bounded"}\n');

      expect(ledger.writeIterationArtifact(0, 'directive.json', bytes)).toBe('created');
      expect(ledger.readIterationArtifact(0, 'directive.json')).toEqual(bytes);
      expect(ledger.writeIterationArtifact(0, 'directive.json', Buffer.from(bytes))).toBe('unchanged');
      expect(() => ledger.writeIterationArtifact(0, 'directive.json', 'conflicting')).toThrow(/conflicting|immutable/i);

      const iterRoot = path.join(ledger.directory, 'iter');
      const iterDir = path.join(iterRoot, '0');
      expect(permissions(iterRoot)).toBe(0o700);
      expect(permissions(iterDir)).toBe(0o700);
      expect(permissions(path.join(iterDir, 'directive.json'))).toBe(0o600);
      expect(fs.readFileSync(path.join(iterDir, 'directive.json'))).toEqual(bytes);
    });

    it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
      'rejects invalid iteration %s before creating nested paths',
      (iter) => {
        const workspace = tempWorkspace();
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });

        expect(() => ledger.writeIterationArtifact(iter, 'directive.json', 'x')).toThrow(/nonnegative integer/i);
        expect(fs.existsSync(path.join(ledger.directory, 'iter'))).toBe(false);
      },
    );

    it('rejects unapproved artifact path components before creating nested paths', () => {
      const workspace = tempWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });

      expect(() =>
        ledger.writeIterationArtifact(0, '../directive.json' as SecureAutoloopIterationArtifact, 'x'),
      ).toThrow(/unsupported|path component/i);
      expect(() => ledger.readIterationArtifact(0, 'nested/verdict.json' as SecureAutoloopIterationArtifact)).toThrow(
        /unsupported|path component/i,
      );
      expect(fs.existsSync(path.join(ledger.directory, 'iter'))).toBe(false);
    });

    it.each(['symlink', 'hardlink'] as const)(
      'refuses a pre-planted %s artifact without mutating its external target',
      (kind) => {
        const workspace = tempWorkspace();
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
        ledger.writeIterationArtifact(0, 'directive.json', 'safe');
        const artifact = path.join(ledger.directory, 'iter', '0', 'eval_output.json');
        const external = path.join(workspace, `external-${kind}`);
        fs.writeFileSync(external, 'sentinel', { mode: 0o664 });
        if (kind === 'symlink') fs.symlinkSync(external, artifact);
        else fs.linkSync(external, artifact);

        expect(() => ledger.readIterationArtifact(0, 'eval_output.json')).toThrow(
          /symbolic link|hardlink|link count|unsafe/i,
        );
        expect(() => ledger.writeIterationArtifact(0, 'eval_output.json', 'mutated')).toThrow(
          /symbolic link|hardlink|link count|unsafe/i,
        );
        expect(fs.readFileSync(external, 'utf8')).toBe('sentinel');
        expect(permissions(external)).toBe(0o664);
      },
    );

    it('fails closed when the pinned iteration directory is replaced in the checked window', () => {
      const workspace = tempWorkspace();
      let armed = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (!armed || event.relativePath !== 'iter/0/diff.patch') return;
            const iterDir = path.join(ledger.directory, 'iter', '0');
            fs.renameSync(iterDir, `${iterDir}.saved`);
            fs.mkdirSync(iterDir, { mode: 0o700 });
          },
        },
      });
      ledger.writeIterationArtifact(0, 'directive.json', 'safe');
      armed = true;

      expect(() => ledger.writeIterationArtifact(0, 'diff.patch', 'must-not-land')).toThrow(
        /identity|changed|replaced/i,
      );
      expect(fs.readdirSync(path.join(ledger.directory, 'iter', '0'))).toEqual([]);
      expect(fs.readFileSync(path.join(ledger.directory, 'iter', '0.saved', 'directive.json'), 'utf8')).toBe('safe');
    });

    it('does not overwrite a target planted immediately before the atomic commit', () => {
      const workspace = tempWorkspace();
      const external = path.join(workspace, 'external-sentinel');
      fs.writeFileSync(external, 'sentinel');
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (event.operation === 'artifact-commit' && event.relativePath === 'iter/0/verdict.json') {
              fs.symlinkSync(external, event.filePath);
            }
          },
        },
      });

      expect(() => ledger.writeIterationArtifact(0, 'verdict.json', '{"decision":"advance"}')).toThrow(
        /symbolic link|unsafe/i,
      );
      expect(fs.readFileSync(external, 'utf8')).toBe('sentinel');
      expect(fs.readdirSync(path.join(ledger.directory, 'iter', '0')).filter((name) => name.includes('.tmp-'))).toEqual(
        [],
      );
    });

    it('preserves and rejects a conflicting regular target planted after the final absence check', () => {
      const workspace = tempWorkspace();
      const targetBytes = Buffer.from('approved\n');
      const conflictingBytes = Buffer.from('concurrent-writer\n');
      let plantedIdentity: fs.Stats | undefined;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedPublish: (event) => {
            if (event.relativePath !== 'iter/0/verdict.json') return;
            fs.writeFileSync(event.filePath, conflictingBytes, { mode: 0o600 });
            plantedIdentity = fs.lstatSync(event.filePath);
          },
        },
      });

      expect(() => ledger.writeIterationArtifact(0, 'verdict.json', targetBytes)).toThrow(/conflicting|immutable/i);

      const target = path.join(ledger.directory, 'iter', '0', 'verdict.json');
      const preserved = fs.lstatSync(target);
      expect({ dev: preserved.dev, ino: preserved.ino }).toEqual({
        dev: plantedIdentity?.dev,
        ino: plantedIdentity?.ino,
      });
      expect(fs.readFileSync(target)).toEqual(conflictingBytes);
      expect(fs.readdirSync(path.dirname(target)).filter((entry) => entry.startsWith('.verdict.json.tmp-'))).toEqual(
        [],
      );
    });

    it('converges unchanged on an identical regular target planted after the final absence check', () => {
      const workspace = tempWorkspace();
      const bytes = Buffer.from('same-winner\n');
      let plantedIdentity: fs.Stats | undefined;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedPublish: (event) => {
            if (event.relativePath !== 'iter/0/eval_output.json') return;
            fs.writeFileSync(event.filePath, bytes, { mode: 0o600 });
            plantedIdentity = fs.lstatSync(event.filePath);
          },
        },
      });

      expect(ledger.writeIterationArtifact(0, 'eval_output.json', Buffer.from(bytes))).toBe('unchanged');

      const target = path.join(ledger.directory, 'iter', '0', 'eval_output.json');
      const preserved = fs.lstatSync(target);
      expect({ dev: preserved.dev, ino: preserved.ino }).toEqual({
        dev: plantedIdentity?.dev,
        ino: plantedIdentity?.ino,
      });
      expect(fs.readFileSync(target)).toEqual(bytes);
      expect(
        fs.readdirSync(path.dirname(target)).filter((entry) => entry.startsWith('.eval_output.json.tmp-')),
      ).toEqual([]);
    });

    it('fails closed on an unsafe target planted after the final absence check', () => {
      const workspace = tempWorkspace();
      const external = path.join(workspace, 'external-race-target');
      fs.writeFileSync(external, 'external-sentinel\n');
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedPublish: (event) => {
            if (event.relativePath === 'iter/0/diff.patch') fs.symlinkSync(external, event.filePath);
          },
        },
      });

      expect(() => ledger.writeIterationArtifact(0, 'diff.patch', 'must-not-land\n')).toThrow(/symbolic link|unsafe/i);
      expect(fs.readFileSync(external, 'utf8')).toBe('external-sentinel\n');
      expect(fs.lstatSync(path.join(ledger.directory, 'iter', '0', 'diff.patch')).isSymbolicLink()).toBe(true);
    });

    it.each(['EPERM', 'ENOTSUP'] as const)(
      'fails closed without a replacement fallback when exclusive link publish returns %s',
      (code) => {
        const workspace = tempWorkspace();
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
          create: true,
          testHooks: {
            publishNestedTemporary: () => {
              throw Object.assign(new Error(`injected ${code} link failure`), { code });
            },
          },
        });

        expect(() => ledger.writeIterationArtifact(0, 'directive.json', 'must-not-land\n')).toThrow(
          new RegExp(`injected ${code} link failure`, 'i'),
        );
        const directory = path.join(ledger.directory, 'iter', '0');
        expect(fs.existsSync(path.join(directory, 'directive.json'))).toBe(false);
        expect(fs.readdirSync(directory).filter((entry) => entry.startsWith('.directive.json.tmp-'))).toEqual([]);
      },
    );

    it('publishes by exclusive link, removes the private alias, and only then syncs the parent directory', () => {
      const workspace = tempWorkspace();
      const relativePath = 'iter/0/coder_summary.txt';
      const sequence: string[] = [];
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          afterNestedPublish: (event) => {
            if (event.relativePath !== relativePath) return;
            const target = fs.lstatSync(event.filePath);
            const temporary = fs.lstatSync(event.temporaryPath);
            expect(target.nlink).toBe(2);
            expect(temporary.nlink).toBe(2);
            expect({ dev: target.dev, ino: target.ino }).toEqual({ dev: temporary.dev, ino: temporary.ino });
            sequence.push('published');
          },
          afterNestedTemporaryUnlink: (event) => {
            if (event.relativePath !== relativePath) return;
            expect(fs.existsSync(event.temporaryPath)).toBe(false);
            expect(fs.lstatSync(event.filePath).nlink).toBe(1);
            sequence.push('temporary-unlinked');
          },
          beforeDirectorySync: (event) => {
            const target = path.join(event.filePath, 'coder_summary.txt');
            if (!fs.existsSync(target)) return;
            expect(sequence).toEqual(['published', 'temporary-unlinked']);
            expect(
              fs.readdirSync(event.filePath).filter((entry) => entry.startsWith('.coder_summary.txt.tmp-')),
            ).toEqual([]);
            sequence.push('parent-sync');
          },
        },
      });

      expect(ledger.writeIterationArtifact(0, 'coder_summary.txt', 'ordered\n')).toBe('created');
      expect(sequence).toEqual(['published', 'temporary-unlinked', 'parent-sync']);
    });

    it('classifies a temporary-unlink failure after exclusive publication and safely reconciles its exact alias', () => {
      const workspace = tempWorkspace();
      const bytes = Buffer.from('published-before-unlink\n');
      let failUnlink = true;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          unlinkNestedTemporary: (temporaryPath) => {
            if (failUnlink) throw Object.assign(new Error('injected temporary unlink failure'), { code: 'EIO' });
            fs.unlinkSync(temporaryPath);
          },
        },
      });

      expect(() => ledger.writeIterationArtifact(0, 'coder_summary.txt', bytes)).toThrow(
        expect.objectContaining({
          name: 'SecureAutoloopLedgerCommitError',
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          committed: true,
          retryable: false,
          operation: 'secure_nested_artifact_write',
          cause: expect.objectContaining({ message: 'injected temporary unlink failure' }),
        }),
      );

      const directory = path.join(ledger.directory, 'iter', '0');
      const target = path.join(directory, 'coder_summary.txt');
      const aliases = fs.readdirSync(directory).filter((entry) => entry.startsWith('.coder_summary.txt.tmp-'));
      expect(aliases).toHaveLength(1);
      const targetBefore = fs.lstatSync(target);
      const aliasBefore = fs.lstatSync(path.join(directory, aliases[0]));
      expect(targetBefore.nlink).toBe(2);
      expect({ dev: aliasBefore.dev, ino: aliasBefore.ino }).toEqual({ dev: targetBefore.dev, ino: targetBefore.ino });

      failUnlink = false;
      expect(ledger.writeIterationArtifact(0, 'coder_summary.txt', Buffer.from(bytes))).toBe('unchanged');
      expect(fs.readdirSync(directory)).toEqual(['coder_summary.txt']);
      const targetAfter = fs.lstatSync(target);
      expect(targetAfter.nlink).toBe(1);
      expect({ dev: targetAfter.dev, ino: targetAfter.ino }).toEqual({ dev: targetBefore.dev, ino: targetBefore.ino });
      expect(fs.readFileSync(target)).toEqual(bytes);
    });

    it('preserves an exact published alias when retry bytes do not match', () => {
      const workspace = tempWorkspace();
      let failUnlink = true;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          unlinkNestedTemporary: (temporaryPath) => {
            if (failUnlink) throw new Error('leave exact published alias');
            fs.unlinkSync(temporaryPath);
          },
        },
      });

      expect(() => ledger.writeIterationArtifact(0, 'eval_output.json', 'original\n')).toThrow(
        expect.objectContaining({ code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID', committed: true }),
      );
      failUnlink = false;
      const directory = path.join(ledger.directory, 'iter', '0');
      const before = fs.readdirSync(directory).sort();

      expect(() => ledger.writeIterationArtifact(0, 'eval_output.json', 'conflicting\n')).toThrow(
        /conflicting|immutable/i,
      );
      expect(fs.readdirSync(directory).sort()).toEqual(before);
      expect(fs.readFileSync(path.join(directory, 'eval_output.json'), 'utf8')).toBe('original\n');
    });

    it('does not reconcile a published alias when another temp-shaped entry makes provenance ambiguous', () => {
      const workspace = tempWorkspace();
      const bytes = Buffer.from('ambiguous-alias\n');
      let failUnlink = true;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          unlinkNestedTemporary: (temporaryPath) => {
            if (failUnlink) throw new Error('leave published alias for ambiguity test');
            fs.unlinkSync(temporaryPath);
          },
        },
      });

      expect(() => ledger.writeIterationArtifact(0, 'eval_output.json', bytes)).toThrow(
        expect.objectContaining({ code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID', committed: true }),
      );
      failUnlink = false;
      const directory = path.join(ledger.directory, 'iter', '0');
      const unknownTemp = path.join(directory, '.eval_output.json.tmp-1-00000000-0000-4000-8000-000000000001');
      fs.writeFileSync(unknownTemp, bytes, { mode: 0o600 });
      const before = fs.readdirSync(directory).sort();

      expect(() => ledger.writeIterationArtifact(0, 'eval_output.json', Buffer.from(bytes))).toThrow(
        /hardlink|link count|ambiguous|unproven|unsafe/i,
      );
      expect(fs.readdirSync(directory).sort()).toEqual(before);
      expect(fs.lstatSync(path.join(directory, 'eval_output.json')).nlink).toBe(2);
    });

    it('does not reconcile an unknown hardlink alias even when target bytes match', () => {
      const workspace = tempWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
      ledger.writeIterationArtifact(0, 'directive.json', 'seed\n');
      const directory = path.join(ledger.directory, 'iter', '0');
      const target = path.join(directory, 'verdict.json');
      const unknownAlias = path.join(directory, '.verdict.json.tmp-not-an-internal-alias');
      fs.writeFileSync(target, 'same\n', { mode: 0o600 });
      fs.linkSync(target, unknownAlias);

      expect(() => ledger.writeIterationArtifact(0, 'verdict.json', 'same\n')).toThrow(/hardlink|link count|unsafe/i);
      expect(fs.lstatSync(target).nlink).toBe(2);
      expect(fs.existsSync(unknownAlias)).toBe(true);
    });

    it('classifies a post-publish verification failure after temporary unlink as committed and non-retryable', () => {
      const workspace = tempWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          afterNestedTemporaryUnlink: (event) => {
            if (event.relativePath === 'iter/0/diff.patch') fs.unlinkSync(event.filePath);
          },
        },
      });

      expect(() => ledger.writeIterationArtifact(0, 'diff.patch', 'published-then-removed\n')).toThrow(
        expect.objectContaining({
          name: 'SecureAutoloopLedgerCommitError',
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          committed: true,
          retryable: false,
          operation: 'secure_nested_artifact_write',
          cause: expect.objectContaining({ message: expect.stringMatching(/removed|missing|incomplete/i) }),
        }),
      );
      const directory = path.join(ledger.directory, 'iter', '0');
      expect(fs.existsSync(path.join(directory, 'diff.patch'))).toBe(false);
      expect(fs.readdirSync(directory).filter((entry) => entry.startsWith('.diff.patch.tmp-'))).toEqual([]);
    });

    it.each([
      ['same', Buffer.from('stable\n')],
      ['different', Buffer.from('replacement\n')],
    ] as const)(
      'reports an explicit identity change when a nested child is replaced with %s bytes between lstat and open',
      (_kind, replacementBytes) => {
        const workspace = tempWorkspace();
        let armed = false;
        let replaced = false;
        const target = path.join(workspace, 'tasks', 'run-1', 'iter', '0', 'coder_summary.txt');
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
          create: true,
          testHooks: {
            afterNestedChildLstat: (event) => {
              if (!armed || replaced || event.filePath !== target) return;
              replaced = true;
              const replacement = `${target}.replacement`;
              fs.writeFileSync(replacement, replacementBytes, { mode: 0o600 });
              fs.renameSync(replacement, target);
            },
          },
        });
        ledger.writeIterationArtifact(0, 'coder_summary.txt', 'stable\n');
        armed = true;

        let observed: unknown;
        try {
          ledger.readIterationArtifact(0, 'coder_summary.txt');
        } catch (error) {
          observed = error;
        }
        expect(observed).toBeInstanceOf(Error);
        expect((observed as Error).message).toMatch(/identity|replaced|changed/i);
        expect((observed as Error).message).not.toMatch(/hardlink with link count 1/i);
        expect(replaced).toBe(true);
        expect(fs.lstatSync(target).nlink).toBe(1);
        expect(fs.readFileSync(target)).toEqual(replacementBytes);
      },
    );

    it('preserves the primary identity failure and reports an unreachable private temp after parent replacement', () => {
      const workspace = tempWorkspace();
      const warn = vi.fn();
      let armed = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        logger: { warn },
        testHooks: {
          beforeNestedMutation: (event) => {
            if (!armed || event.operation !== 'artifact-commit') return;
            const iterDir = path.join(ledger.directory, 'iter', '0');
            fs.renameSync(iterDir, `${iterDir}.saved`);
            fs.mkdirSync(iterDir, { mode: 0o700 });
          },
        },
      });
      ledger.writeIterationArtifact(0, 'directive.json', 'safe');
      armed = true;

      expect(() => ledger.writeIterationArtifact(0, 'diff.patch', 'must-not-land')).toThrow(
        /identity|changed|replaced/i,
      );
      expect(fs.readdirSync(path.join(ledger.directory, 'iter', '0'))).toEqual([]);
      const stale = fs
        .readdirSync(path.join(ledger.directory, 'iter', '0.saved'))
        .filter((name) => name.includes('.diff.patch.tmp-'));
      expect(stale).toHaveLength(1);
      expect(permissions(path.join(ledger.directory, 'iter', '0.saved', stale[0]))).toBe(0o600);
      expect(warn).toHaveBeenCalledWith(expect.stringMatching(/incomplete nested artifact.*could not be located/i));
    });

    it('classifies a post-rename parent-directory sync failure as committed and converges on identical retry', () => {
      const workspace = tempWorkspace();
      const relativePath = 'iter/0/coder_summary.txt';
      let failDirectorySync = true;
      let artifactCommits = 0;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (event.operation === 'artifact-commit' && event.relativePath === relativePath) artifactCommits++;
          },
          beforeDirectorySync: (event) => {
            const committedTarget = path.join(event.filePath, 'coder_summary.txt');
            if (failDirectorySync && fs.existsSync(committedTarget)) {
              failDirectorySync = false;
              throw new Error('injected nested directory fsync failure');
            }
          },
        },
      });
      const bytes = Buffer.from('complete\n');

      let commitError: unknown;
      try {
        ledger.writeIterationArtifact(0, 'coder_summary.txt', bytes);
      } catch (error) {
        commitError = error;
      }

      expect(commitError).toMatchObject({
        name: 'SecureAutoloopLedgerCommitError',
        code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
        operation: 'secure_nested_artifact_write',
        cause: expect.objectContaining({ message: 'injected nested directory fsync failure' }),
      });
      const target = path.join(ledger.directory, 'iter', '0', 'coder_summary.txt');
      expect(fs.readFileSync(target)).toEqual(bytes);
      const committedIdentity = fs.lstatSync(target);

      expect(ledger.writeIterationArtifact(0, 'coder_summary.txt', Buffer.from(bytes))).toBe('unchanged');
      const retriedIdentity = fs.lstatSync(target);
      expect({ dev: retriedIdentity.dev, ino: retriedIdentity.ino }).toEqual({
        dev: committedIdentity.dev,
        ino: committedIdentity.ino,
      });
      expect(fs.readdirSync(path.dirname(target))).toEqual(['coder_summary.txt']);
      expect(artifactCommits).toBe(1);
    });

    it('reports a committed failure when the created child is deleted during its parent-directory barrier', () => {
      const workspace = tempWorkspace();
      const bytes = Buffer.from('created-child\n');
      let armed = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeDirectorySync: (event) => {
            const target = path.join(event.filePath, 'coder_summary.txt');
            if (!armed || !fs.existsSync(target)) return;
            armed = false;
            fs.unlinkSync(target);
          },
        },
      });
      armed = true;

      expect(() => ledger.writeIterationArtifact(0, 'coder_summary.txt', bytes)).toThrow(
        expect.objectContaining({
          name: 'SecureAutoloopLedgerCommitError',
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          committed: true,
          retryable: false,
          operation: 'secure_nested_artifact_write',
          cause: expect.objectContaining({ message: expect.stringMatching(/removed|missing|incomplete|changed/i) }),
        }),
      );
      expect(fs.existsSync(path.join(ledger.directory, 'iter', '0', 'coder_summary.txt'))).toBe(false);
    });

    it('reports a committed failure when an existing identical child is replaced during its durability barrier', () => {
      const workspace = tempWorkspace();
      const bytes = Buffer.from('existing-child\n');
      let armed = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeDirectorySync: (event) => {
            const target = path.join(event.filePath, 'coder_summary.txt');
            if (!armed || !fs.existsSync(target)) return;
            armed = false;
            fs.unlinkSync(target);
            fs.writeFileSync(target, bytes, { mode: 0o600 });
          },
        },
      });
      ledger.writeIterationArtifact(0, 'coder_summary.txt', bytes);
      const original = fs.lstatSync(path.join(ledger.directory, 'iter', '0', 'coder_summary.txt'));
      armed = true;

      expect(() => ledger.writeIterationArtifact(0, 'coder_summary.txt', Buffer.from(bytes))).toThrow(
        expect.objectContaining({
          name: 'SecureAutoloopLedgerCommitError',
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          committed: true,
          retryable: false,
          operation: 'secure_nested_artifact_write',
          cause: expect.objectContaining({ message: expect.stringMatching(/identity|replaced|changed/i) }),
        }),
      );
      const replacement = fs.lstatSync(path.join(ledger.directory, 'iter', '0', 'coder_summary.txt'));
      expect({ dev: replacement.dev, ino: replacement.ino }).not.toEqual({ dev: original.dev, ino: original.ino });
      expect(fs.readFileSync(path.join(ledger.directory, 'iter', '0', 'coder_summary.txt'))).toEqual(bytes);
    });

    it('reports a committed failure when a planted identical child changes during its durability barrier', () => {
      const workspace = tempWorkspace();
      const bytes = Buffer.from('planted-child\n');
      let planted = false;
      let armed = true;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (event.operation !== 'artifact-commit' || event.relativePath !== 'iter/0/coder_summary.txt') return;
            fs.writeFileSync(event.filePath, bytes, { mode: 0o600 });
            planted = true;
          },
          beforeDirectorySync: (event) => {
            const target = path.join(event.filePath, 'coder_summary.txt');
            if (!armed || !planted || !fs.existsSync(target)) return;
            armed = false;
            fs.writeFileSync(target, 'mismatched-child\n');
          },
        },
      });

      expect(() => ledger.writeIterationArtifact(0, 'coder_summary.txt', bytes)).toThrow(
        expect.objectContaining({
          name: 'SecureAutoloopLedgerCommitError',
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          committed: true,
          retryable: false,
          operation: 'secure_nested_artifact_write',
          cause: expect.objectContaining({ message: expect.stringMatching(/contents|mismatch|incomplete|changed/i) }),
        }),
      );
      expect(fs.readFileSync(path.join(ledger.directory, 'iter', '0', 'coder_summary.txt'), 'utf8')).toBe(
        'mismatched-child\n',
      );
    });

    it('preserves a typed committed-state failure when descriptor close also fails', () => {
      const workspace = tempWorkspace();
      const warn = vi.fn();
      const bytes = Buffer.from('close-masking\n');
      let armed = false;
      let closeFailureInjected = false;
      const target = path.join(workspace, 'tasks', 'run-1', 'iter', '0', 'coder_summary.txt');
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        logger: { warn },
        testHooks: {
          beforeDirectorySync: (event) => {
            if (!armed || closeFailureInjected || event.filePath !== path.dirname(target)) return;
            replaceWithSameBytes(target);
          },
          closeDescriptor: (fd) => {
            fs.closeSync(fd);
            if (!armed || closeFailureInjected) return;
            closeFailureInjected = true;
            throw new Error('injected descriptor close failure');
          },
        },
      });
      ledger.writeIterationArtifact(0, 'coder_summary.txt', bytes);
      armed = true;

      expect(() => ledger.writeIterationArtifact(0, 'coder_summary.txt', Buffer.from(bytes))).toThrow(
        expect.objectContaining({
          name: 'SecureAutoloopLedgerCommitError',
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          committed: true,
          retryable: false,
          secondaryErrors: [expect.objectContaining({ message: 'injected descriptor close failure' })],
        }),
      );
      expect(closeFailureInjected).toBe(true);
      expect(warn).toHaveBeenCalledWith(expect.stringMatching(/descriptor.*close.*injected descriptor close failure/i));
    });

    it('reports a descriptor close failure directly when durability otherwise succeeded', () => {
      const workspace = tempWorkspace();
      const bytes = Buffer.from('close-only\n');
      let armed = false;
      let closeFailureInjected = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          closeDescriptor: (fd) => {
            fs.closeSync(fd);
            if (!armed || closeFailureInjected) return;
            closeFailureInjected = true;
            throw new Error('injected close-only failure');
          },
        },
      });
      ledger.writeIterationArtifact(0, 'coder_summary.txt', bytes);
      armed = true;

      expect(() => ledger.writeIterationArtifact(0, 'coder_summary.txt', Buffer.from(bytes))).toThrow(
        /injected close-only failure/,
      );
      expect(closeFailureInjected).toBe(true);
    });

    it.each(['reviewer_memory.md', 'reviewer_log.jsonl'] as const)(
      'rejects a %s byte change at the sandbox reset seam',
      (persistentName) => {
        const workspace = tempWorkspace();
        let armed = false;
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
          create: true,
          testHooks: {
            beforeNestedMutation: (event) => {
              if (!armed || event.operation !== 'sandbox-reset') return;
              fs.writeFileSync(path.join(event.filePath, persistentName), 'changed-at-reset\n');
            },
          },
        });
        seedCompleteReviewerArtifacts(ledger, 0);
        const sandbox = path.join(ledger.directory, 'reviewer_sandbox');
        fs.mkdirSync(sandbox);
        fs.writeFileSync(path.join(sandbox, persistentName), 'captured-before-reset\n');
        armed = true;

        expect(() => ledger.stageReviewerSandbox(0)).toThrow(/contents|changed|mismatch|reset seam/i);
      },
    );

    it.each(['reviewer_memory.md', 'reviewer_log.jsonl'] as const)(
      'rejects a same-content %s inode replacement at the sandbox reset seam',
      (persistentName) => {
        const workspace = tempWorkspace();
        let armed = false;
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
          create: true,
          testHooks: {
            beforeNestedMutation: (event) => {
              if (!armed || event.operation !== 'sandbox-reset') return;
              replaceWithSameBytes(path.join(event.filePath, persistentName));
            },
          },
        });
        seedCompleteReviewerArtifacts(ledger, 0);
        const sandbox = path.join(ledger.directory, 'reviewer_sandbox');
        fs.mkdirSync(sandbox);
        fs.writeFileSync(path.join(sandbox, persistentName), `persistent-${persistentName}\n`);
        armed = true;

        expect(() => ledger.stageReviewerSandbox(0)).toThrow(/identity|replaced|changed|reset seam/i);
      },
    );

    it('rejects a removable file byte change at the sandbox reset seam', () => {
      const workspace = tempWorkspace();
      let armed = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (!armed || event.operation !== 'sandbox-reset') return;
            fs.writeFileSync(path.join(event.filePath, 'scratch.txt'), 'changed-before-removal\n');
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      const sandbox = path.join(ledger.directory, 'reviewer_sandbox');
      fs.mkdirSync(sandbox);
      fs.writeFileSync(path.join(sandbox, 'scratch.txt'), 'captured-before-reset\n');
      armed = true;

      expect(() => ledger.stageReviewerSandbox(0)).toThrow(/contents|changed|mismatch|reset seam/i);
    });

    it('retains an earlier staged sibling identity through later child barriers', () => {
      const workspace = tempWorkspace();
      let armed = false;
      let replaced = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeDirectorySync: (event) => {
            const directive = path.join(event.filePath, 'directive.json');
            const laterSibling = path.join(event.filePath, 'eval_output.json');
            if (!armed || replaced || !fs.existsSync(directive) || !fs.existsSync(laterSibling)) return;
            replaced = true;
            replaceWithSameBytes(directive);
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      armed = true;

      expect(() => ledger.stageReviewerSandbox(0)).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          committed: true,
          retryable: false,
          effectsApplied: false,
          operation: 'secure_reviewer_sandbox_stage',
          cause: expect.objectContaining({ message: expect.stringMatching(/identity|replaced|changed/i) }),
        }),
      );
      expect(replaced).toBe(true);
    });

    it.each(['reviewer_memory.md', 'reviewer_log.jsonl'] as const)(
      'retains the post-reset %s identity through staged-child barriers',
      (persistentName) => {
        const workspace = tempWorkspace();
        let armed = false;
        let replaced = false;
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
          create: true,
          testHooks: {
            beforeDirectorySync: (event) => {
              const firstStagedChild = path.join(event.filePath, 'directive.json');
              if (!armed || replaced || !fs.existsSync(firstStagedChild)) return;
              replaced = true;
              replaceWithSameBytes(path.join(path.dirname(event.filePath), persistentName));
            },
          },
        });
        seedCompleteReviewerArtifacts(ledger, 0);
        const sandbox = path.join(ledger.directory, 'reviewer_sandbox');
        fs.mkdirSync(sandbox);
        fs.writeFileSync(path.join(sandbox, persistentName), `persistent-${persistentName}\n`);
        armed = true;

        expect(() => ledger.stageReviewerSandbox(0)).toThrow(
          expect.objectContaining({
            code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
            operation: 'secure_reviewer_sandbox_stage',
            cause: expect.objectContaining({ message: expect.stringMatching(/identity|replaced|changed/i) }),
          }),
        );
        expect(replaced).toBe(true);
      },
    );

    it('revalidates authoritative iteration source bytes before returning a staged sandbox', () => {
      const workspace = tempWorkspace();
      let armed = false;
      let mutated = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (
              !armed ||
              mutated ||
              event.operation !== 'sandbox-stage' ||
              event.relativePath !== 'reviewer_sandbox/iter-0/directive.json'
            ) {
              return;
            }
            mutated = true;
            fs.writeFileSync(path.join(ledger.directory, 'iter', '0', 'directive.json'), 'source-drifted\n');
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      armed = true;

      expect(() => ledger.stageReviewerSandbox(0)).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          operation: 'secure_reviewer_sandbox_stage',
          cause: expect.objectContaining({ message: expect.stringMatching(/source.*contents|source.*changed/i) }),
        }),
      );
      expect(mutated).toBe(true);
      expect(fs.readFileSync(path.join(ledger.directory, 'reviewer_sandbox', 'iter-0', 'directive.json'), 'utf8')).toBe(
        'directive-0\n',
      );
    });

    it('rejects same-content source replacement on the steady-state Reviewer delivery path', async () => {
      const workspace = tempWorkspace();
      let armed = false;
      let replaced = false;
      const secureLedger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (
              !armed ||
              replaced ||
              event.operation !== 'sandbox-stage' ||
              event.relativePath !== 'reviewer_sandbox/iter-0/directive.json'
            ) {
              return;
            }
            replaced = true;
            replaceWithSameBytes(path.join(secureLedger.directory, 'iter', '0', 'directive.json'));
          },
        },
      });
      seedCompleteReviewerArtifacts(secureLedger, 0);
      const { manager } = stubGenerationManager();
      manager.sendMessage.mockResolvedValue({
        output: [
          '```autoloop',
          JSON.stringify({
            tool: 'review_complete',
            args: { decision: 'advance', metric: 1, audit_notes: 'must not be delivered' },
          }),
          '```',
        ].join('\n'),
        error: undefined,
      });
      const dispatcher = new ClaudeAgentDispatcher({
        manager: manager as never,
        runId: 'run-1',
        workspace,
        secureLedger,
        ownerInstanceId: TEST_OWNER_INSTANCE_ID,
      });
      (dispatcher as unknown as { reviewerStarted: boolean }).reviewerStarted = true;
      armed = true;

      await expect(
        dispatcher.deliver(Msg.reviewRequest(0, { iter: 0, ledger_path: secureLedger.directory, prior_metrics: [] })),
      ).rejects.toMatchObject({
        code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
        operation: 'secure_reviewer_sandbox_stage',
      });
      expect(replaced).toBe(true);
      expect(manager.sendMessage).not.toHaveBeenCalled();
    });

    it('revalidates the prior-verdict source identity after staging', () => {
      const workspace = tempWorkspace();
      let armed = false;
      let replaced = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (!armed || replaced || event.operation !== 'sandbox-stage') return;
            replaced = true;
            replaceWithSameBytes(path.join(ledger.directory, 'iter', '0', 'verdict.json'));
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 1);
      ledger.writeIterationArtifact(0, 'verdict.json', '{"decision":"hold"}\n');
      armed = true;

      expect(() => ledger.stageReviewerSandbox(1)).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          operation: 'secure_reviewer_sandbox_stage',
          cause: expect.objectContaining({ message: expect.stringMatching(/prior.*identity|source.*changed/i) }),
        }),
      );
      expect(replaced).toBe(true);
    });

    it('does not adopt a replacement between sandbox byte validation and identity capture', () => {
      const workspace = tempWorkspace();
      let armed = false;
      let replaced = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          afterReviewerSandboxEntryRead: (event) => {
            if (!armed || replaced || event.relativePath !== 'iter-0/directive.json') return;
            replaced = true;
            replaceWithSameBytes(event.filePath);
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      armed = true;

      expect(() => ledger.stageReviewerSandbox(0)).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          operation: 'secure_reviewer_sandbox_stage',
          cause: expect.objectContaining({ message: expect.stringMatching(/identity|replaced|changed/i) }),
        }),
      );
      expect(replaced).toBe(true);
    });

    it('rejects an artifact directory masquerading as a required regular file at the final sandbox seam', () => {
      const workspace = tempWorkspace();
      let armed = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (!armed || event.operation !== 'sandbox-stage' || event.relativePath !== 'reviewer_sandbox/iter-0') {
              return;
            }
            const target = path.join(event.filePath, 'directive.json');
            fs.unlinkSync(target);
            fs.mkdirSync(target);
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      armed = true;

      expect(() => ledger.stageReviewerSandbox(0)).toThrow(/regular|directory|type|staged iteration/i);
    });

    it('rejects a same-content directive inode replacement at the final sandbox seam', () => {
      const workspace = tempWorkspace();
      let armed = false;
      let identities: ReturnType<typeof replaceWithSameBytes> | undefined;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (!armed || event.operation !== 'sandbox-stage' || event.relativePath !== 'reviewer_sandbox/iter-0') {
              return;
            }
            identities = replaceWithSameBytes(path.join(event.filePath, 'directive.json'));
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      armed = true;

      expect(() => ledger.stageReviewerSandbox(0)).toThrow(/identity|replaced|changed/i);
      expect({ dev: identities?.after.dev, ino: identities?.after.ino }).not.toEqual({
        dev: identities?.before.dev,
        ino: identities?.before.ino,
      });
    });

    it.each(['reviewer_memory.md', 'reviewer_log.jsonl'] as const)(
      'rejects a same-content %s inode replacement at the final sandbox seam',
      (persistentName) => {
        const workspace = tempWorkspace();
        let armed = false;
        let identities: ReturnType<typeof replaceWithSameBytes> | undefined;
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
          create: true,
          testHooks: {
            beforeNestedMutation: (event) => {
              if (!armed || event.operation !== 'sandbox-stage' || event.relativePath !== 'reviewer_sandbox/iter-0') {
                return;
              }
              identities = replaceWithSameBytes(path.join(path.dirname(event.filePath), persistentName));
            },
          },
        });
        seedCompleteReviewerArtifacts(ledger, 0);
        const sandbox = path.join(ledger.directory, 'reviewer_sandbox');
        fs.mkdirSync(sandbox);
        fs.writeFileSync(path.join(sandbox, persistentName), `persistent-${persistentName}\n`);
        armed = true;

        expect(() => ledger.stageReviewerSandbox(0)).toThrow(/identity|replaced|changed/i);
        expect({ dev: identities?.after.dev, ino: identities?.after.ino }).not.toEqual({
          dev: identities?.before.dev,
          ino: identities?.before.ino,
        });
      },
    );

    it.each([
      'iter-1/directive.json',
      'iter-1/eval_output.json',
      'iter-1/coder_summary.txt',
      'iter-1/diff.patch',
      'plan.md',
      'goal.json',
      'prior_verdict.json',
      'reviewer_memory.md',
      'reviewer_log.jsonl',
    ] as const)('rejects a byte change to final staged file %s', (relativeTarget) => {
      const workspace = tempWorkspace();
      let armed = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeNestedMutation: (event) => {
            if (!armed || event.operation !== 'sandbox-stage' || event.relativePath !== 'reviewer_sandbox/iter-1') {
              return;
            }
            fs.writeFileSync(path.join(path.dirname(event.filePath), relativeTarget), 'changed-at-final-seam\n');
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 1);
      ledger.writeIterationArtifact(0, 'verdict.json', '{"decision":"hold"}\n');
      const sandbox = path.join(ledger.directory, 'reviewer_sandbox');
      fs.mkdirSync(sandbox);
      fs.writeFileSync(path.join(sandbox, 'reviewer_memory.md'), 'persistent-memory\n');
      fs.writeFileSync(path.join(sandbox, 'reviewer_log.jsonl'), '{"persistent":true}\n');
      armed = true;

      expect(() =>
        ledger.stageReviewerSandbox(1, {
          plan: Buffer.from('# approved plan\n'),
          goal: Buffer.from('{"goal":"approved"}\n'),
        }),
      ).toThrow(/contents|changed|mismatch|final stage/i);
    });

    it('revalidates authoritative source identity after the final sandbox barrier', () => {
      const workspace = tempWorkspace();
      const sandbox = path.join(workspace, 'tasks', 'run-1', 'reviewer_sandbox');
      let armed = false;
      let replaced = false;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeDirectorySync: (event) => {
            if (
              !armed ||
              replaced ||
              event.filePath !== sandbox ||
              !fs.existsSync(path.join(sandbox, 'iter-0', 'diff.patch'))
            ) {
              return;
            }
            replaced = true;
            replaceWithSameBytes(path.join(ledger.directory, 'iter', '0', 'directive.json'));
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      armed = true;

      expect(() => ledger.stageReviewerSandbox(0)).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          operation: 'secure_reviewer_sandbox_stage',
          cause: expect.objectContaining({ message: expect.stringMatching(/source.*identity|source.*changed/i) }),
        }),
      );
      expect(replaced).toBe(true);
    });

    it('rejects a final-barrier replacement of the staged iteration directory even when child inodes stay fixed', () => {
      const workspace = tempWorkspace();
      const sandbox = path.join(workspace, 'tasks', 'run-1', 'reviewer_sandbox');
      let armed = false;
      let before: fs.Stats | undefined;
      let after: fs.Stats | undefined;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeDirectorySync: (event) => {
            const staged = path.join(sandbox, 'iter-0');
            if (!armed || before || event.filePath !== sandbox || !fs.existsSync(path.join(staged, 'diff.patch'))) {
              return;
            }
            const saved = `${staged}.saved`;
            before = fs.lstatSync(staged);
            fs.renameSync(staged, saved);
            fs.mkdirSync(staged, { mode: 0o700 });
            for (const entry of fs.readdirSync(saved)) {
              fs.renameSync(path.join(saved, entry), path.join(staged, entry));
            }
            fs.rmdirSync(saved);
            after = fs.lstatSync(staged);
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      armed = true;

      expect(() => ledger.stageReviewerSandbox(0)).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          committed: true,
          retryable: false,
          effectsApplied: false,
          operation: 'secure_reviewer_sandbox_stage',
          cause: expect.objectContaining({ message: expect.stringMatching(/directory.*identity|replaced|changed/i) }),
        }),
      );
      expect({ dev: after?.dev, ino: after?.ino }).not.toEqual({ dev: before?.dev, ino: before?.ino });
      expect(ledger.stageReviewerSandbox(0)).toEqual({ directory: sandbox, priorVerdict: false });
    });

    it.each(['symlink', 'hardlink', 'directory', 'delete', 'replace-with-same-content'] as const)(
      'reports a committed failure when a staged child is subject to %s during the final sandbox barrier',
      (mutation) => {
        const workspace = tempWorkspace();
        const sandbox = path.join(workspace, 'tasks', 'run-1', 'reviewer_sandbox');
        const external = path.join(workspace, `external-${mutation}`);
        fs.writeFileSync(external, 'directive-0\n', { mode: 0o600 });
        let armed = false;
        let mutated = false;
        let original: fs.Stats | undefined;
        const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
          create: true,
          testHooks: {
            beforeDirectorySync: (event) => {
              const target = path.join(sandbox, 'iter-0', 'directive.json');
              if (mutated || !armed || event.filePath !== sandbox || !fs.existsSync(target)) return;
              if (!fs.existsSync(path.join(sandbox, 'iter-0', 'diff.patch'))) return;
              mutated = true;
              original = fs.lstatSync(target);
              if (mutation === 'replace-with-same-content') {
                replaceWithSameBytes(target);
                return;
              }
              fs.unlinkSync(target);
              if (mutation === 'symlink') fs.symlinkSync(external, target);
              else if (mutation === 'hardlink') fs.linkSync(external, target);
              else if (mutation === 'directory') fs.mkdirSync(target);
            },
          },
        });
        seedCompleteReviewerArtifacts(ledger, 0);
        armed = true;

        expect(() => ledger.stageReviewerSandbox(0)).toThrow(
          expect.objectContaining({
            name: 'SecureAutoloopLedgerCommitError',
            code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
            committed: true,
            retryable: false,
            effectsApplied: false,
            operation: 'secure_reviewer_sandbox_stage',
            cause: expect.objectContaining({
              message: expect.stringMatching(
                /symbolic link|hardlink|link count|regular|directory|identity|removed|membership|changed/i,
              ),
            }),
          }),
        );
        expect(mutated).toBe(true);

        const target = path.join(sandbox, 'iter-0', 'directive.json');
        const postState = lstatIfPresentForTest(target);
        if (mutation === 'delete') expect(postState).toBeUndefined();
        else if (mutation === 'symlink') expect(postState?.isSymbolicLink()).toBe(true);
        else if (mutation === 'hardlink') {
          expect(postState?.nlink).toBe(2);
          const externalIdentity = fs.lstatSync(external);
          expect({ dev: postState?.dev, ino: postState?.ino }).toEqual({
            dev: externalIdentity.dev,
            ino: externalIdentity.ino,
          });
        } else if (mutation === 'directory') expect(postState?.isDirectory()).toBe(true);
        else {
          expect(fs.readFileSync(target, 'utf8')).toBe('directive-0\n');
          expect({ dev: postState?.dev, ino: postState?.ino }).not.toEqual({
            dev: original?.dev,
            ino: original?.ino,
          });
        }

        if (mutation === 'symlink' || mutation === 'hardlink') {
          expect(() => ledger.stageReviewerSandbox(0)).toThrow(/symbolic link|hardlink|link count|unsafe/i);
          fs.rmSync(path.join(sandbox, 'iter-0'), { recursive: true, force: false });
        }
        expect(ledger.stageReviewerSandbox(0)).toEqual({ directory: sandbox, priorVerdict: false });
        expect(fs.readFileSync(path.join(sandbox, 'iter-0', 'directive.json'), 'utf8')).toBe('directive-0\n');
        expect(fs.readFileSync(external, 'utf8')).toBe('directive-0\n');
      },
    );

    it('safely stages and restages an unchanged Reviewer sandbox', () => {
      const workspace = tempWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
      const controls = { plan: Buffer.from('# plan\n'), goal: Buffer.from('{"goal":true}\n') };
      seedCompleteReviewerArtifacts(ledger, 0);

      const first = ledger.stageReviewerSandbox(0, controls);
      const second = ledger.stageReviewerSandbox(0, controls);

      expect(second).toEqual(first);
      expect(fs.readFileSync(path.join(second.directory, 'iter-0', 'directive.json'), 'utf8')).toBe('directive-0\n');
      expect(fs.readFileSync(path.join(second.directory, 'plan.md'), 'utf8')).toBe('# plan\n');
      expect(fs.readFileSync(path.join(second.directory, 'goal.json'), 'utf8')).toBe('{"goal":true}\n');
    });

    it('classifies the final sandbox barrier as committed and converges on safe restage', () => {
      const workspace = tempWorkspace();
      let completeSandboxSyncs = 0;
      let failFinalBarrier = true;
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        testHooks: {
          beforeDirectorySync: (event) => {
            const staged = path.join(event.filePath, 'iter-0');
            if (
              !failFinalBarrier ||
              !fs.existsSync(path.join(staged, 'diff.patch')) ||
              !fs.existsSync(path.join(event.filePath, 'plan.md')) ||
              !fs.existsSync(path.join(event.filePath, 'goal.json'))
            ) {
              return;
            }
            completeSandboxSyncs++;
            if (completeSandboxSyncs === 2) {
              failFinalBarrier = false;
              throw new Error('injected final Reviewer sandbox sync failure');
            }
          },
        },
      });
      seedCompleteReviewerArtifacts(ledger, 0);
      const controls = { plan: Buffer.from('# plan\n'), goal: Buffer.from('{"goal":true}\n') };

      expect(() => ledger.stageReviewerSandbox(0, controls)).toThrow(
        expect.objectContaining({
          name: 'SecureAutoloopLedgerCommitError',
          code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
          committed: true,
          retryable: false,
          effectsApplied: false,
          operation: 'secure_reviewer_sandbox_stage',
          cause: expect.objectContaining({ message: 'injected final Reviewer sandbox sync failure' }),
        }),
      );

      expect(ledger.stageReviewerSandbox(0, controls)).toEqual({
        directory: path.join(ledger.directory, 'reviewer_sandbox'),
        priorVerdict: false,
      });
      expect(fs.readFileSync(path.join(ledger.directory, 'reviewer_sandbox', 'iter-0', 'directive.json'), 'utf8')).toBe(
        'directive-0\n',
      );
      expect(fs.readFileSync(path.join(ledger.directory, 'reviewer_sandbox', 'plan.md'), 'utf8')).toBe('# plan\n');
    });

    it('reports the win32 directory-entry durability limitation for nested artifacts', () => {
      const workspace = tempWorkspace();
      const warn = vi.fn();
      const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
        create: true,
        platform: 'win32',
        logger: { warn },
      });

      ledger.writeIterationArtifact(0, 'coder_summary.txt', 'complete');

      expect(warn).toHaveBeenCalledWith(expect.stringMatching(/directory fsync.*win32/i));
    });
  });
});
