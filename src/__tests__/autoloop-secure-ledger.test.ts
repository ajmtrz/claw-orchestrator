import { afterEach, describe, expect, it } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { SecureAutoloopLedger } from '../autoloop/secure-ledger.js';
import { appendPushLog } from '../autoloop/notify.js';

const roots: string[] = [];

function tempWorkspace(): string {
  const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-secure-ledger-'));
  roots.push(workspace);
  return workspace;
}

function permissions(target: string): number {
  return fs.statSync(target).mode & 0o777;
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

    expect(() => ledger.readFlatFile('decisions.jsonl')).toThrow(/identity|changed|replaced/i);
    expect(() => ledger.appendFlatFile('chat.jsonl', '{"after":true}\n')).toThrow(/identity|changed|replaced/i);
  });

  it('pins the run identity and rejects a post-start run-directory swap', () => {
    const workspace = tempWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', { create: true });
    const runDir = ledger.directory;
    fs.renameSync(runDir, `${runDir}.saved`);
    fs.mkdirSync(runDir);

    expect(() => ledger.appendFlatFile('agent-generations.jsonl', '{}\n')).toThrow(/identity|changed|replaced/i);
  });

  it.each(['agent-generations.jsonl', 'chat.jsonl', 'push_log.jsonl'] as const)(
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

  it('keeps legacy push-log callers compatible while rejecting an unsafe final target', () => {
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

    const legacyDir = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-pushlog-legacy-'));
    roots.push(legacyDir);
    appendPushLog(legacyDir, entry);
    expect(fs.readFileSync(path.join(legacyDir, 'push_log.jsonl'), 'utf8')).toContain('secure push');
  });
});
