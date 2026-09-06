import { afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import {
  openPrivateAutoloopDecisions,
  SecureAutoloopLedger,
  type SecureAutoloopFlatFile,
} from '../autoloop/secure-ledger.js';
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

  it('classifies a post-write directory-sync failure as committed and never appends twice', () => {
    const workspace = tempWorkspace();
    let failDirectorySync = true;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      testHooks: {
        beforeDirectorySync: () => {
          if (failDirectorySync) {
            failDirectorySync = false;
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
          code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
          committed: true,
        }),
      );
      expect(prepared.committed).toBe(true);
      expect(() => prepared.commitDurable()).toThrow(
        expect.objectContaining({
          code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
          committed: true,
        }),
      );
    } finally {
      prepared.close();
    }
    expect(countRows(path.join(ledger.directory, 'decisions.jsonl'), 'prepared')).toBe(1);
  });

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
});
