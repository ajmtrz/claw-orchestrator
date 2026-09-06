import * as fs from 'node:fs';
import * as path from 'node:path';

const PRIVATE_DIRECTORY_MODE = 0o700;
const PRIVATE_FILE_MODE = 0o600;

export const SECURE_AUTOLOOP_FLAT_FILES = [
  'decisions.jsonl',
  'agent-generations.jsonl',
  'chat.jsonl',
  'push_log.jsonl',
] as const;

export type SecureAutoloopFlatFile = (typeof SECURE_AUTOLOOP_FLAT_FILES)[number];

export interface SecureLedgerPlatformFlags {
  noFollow?: number;
  directory?: number;
}

export interface SecureLedgerMutationEvent {
  name?: SecureAutoloopFlatFile;
  operation: 'chmod' | 'append' | 'flush' | 'directory-sync';
  filePath: string;
  fd?: number;
}

export interface SecureAutoloopLedgerOptions {
  create?: boolean;
  platformFlags?: SecureLedgerPlatformFlags;
  /** Validate only these compatibility files at construction. Full startup omits this. */
  validateExistingFlatFiles?: readonly SecureAutoloopFlatFile[];
  /** Explicit platform seam for testing the documented win32 durability limitation. */
  platform?: NodeJS.Platform;
  logger?: { warn?: (message: string) => void };
  /** Deterministic checked-window seam. Production callers never provide it. */
  testHooks?: {
    beforeFileMutation?: (event: SecureLedgerMutationEvent) => void;
    beforeDirectorySync?: (event: SecureLedgerMutationEvent) => void;
  };
}

export interface SecureAutoloopFileHandle {
  fd: number;
  filePath: string;
  created: boolean;
  name: SecureAutoloopFlatFile;
  stat: fs.Stats;
}

export interface SecureAutoloopPreparedAppend {
  readonly committed: boolean;
  commitDurable(): void;
  readLastNonEmptyLine(): string;
  close(): void;
}

export class SecureAutoloopLedgerCommitError extends Error {
  readonly committed = true;

  constructor(
    readonly code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE' | 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
    message: string,
    options: { cause: unknown },
  ) {
    super(message, options);
    this.name = 'SecureAutoloopLedgerCommitError';
  }
}

export function isCommittedSecureLedgerError(error: unknown): error is SecureAutoloopLedgerCommitError {
  return error instanceof SecureAutoloopLedgerCommitError && error.committed;
}

interface PinnedDirectory {
  path: string;
  stat: fs.Stats;
  label: string;
}

function lstatIfPresent(target: string): fs.Stats | undefined {
  try {
    return fs.lstatSync(target);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
    throw error;
  }
}

function missingPath(target: string): NodeJS.ErrnoException {
  return Object.assign(new Error(`ENOENT: no such file or directory, open '${target}'`), { code: 'ENOENT' });
}

function sameIdentity(left: fs.Stats, right: fs.Stats): boolean {
  return left.dev === right.dev && left.ino === right.ino;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function rejectDirectory(target: string, label: string, observed: fs.Stats): never {
  if (observed.isSymbolicLink()) throw new Error(`Refusing ${label} symbolic link '${target}'`);
  throw new Error(`Refusing non-directory ${label} '${target}'`);
}

function rejectFlatFile(target: string, observed: fs.Stats): never {
  if (observed.isSymbolicLink()) {
    throw new Error(`Refusing Autoloop ledger path '${target}' because it is a symbolic link`);
  }
  if (!observed.isFile()) {
    throw Object.assign(new Error(`Refusing non-regular Autoloop ledger file '${target}'`), {
      code: observed.isDirectory() ? 'EISDIR' : 'EINVAL',
    });
  }
  throw new Error(`Refusing Autoloop ledger hardlink with link count ${observed.nlink} at '${target}'`);
}

function normalizedFlags(flags?: SecureLedgerPlatformFlags): Required<SecureLedgerPlatformFlags> {
  return {
    noFollow: flags?.noFollow ?? fs.constants.O_NOFOLLOW ?? 0,
    directory: flags?.directory ?? fs.constants.O_DIRECTORY ?? 0,
  };
}

function validateRunId(runId: string): void {
  if (!runId || runId === '.' || runId === '..' || path.basename(runId) !== runId) {
    throw new Error(`Autoloop run id must be one path component`);
  }
}

/**
 * A capability for the private, append-only files at the root of one Autoloop
 * run. Directory device/inode identities are fixed at construction and checked
 * before and after every descendant open, so replacing `tasks` or the run
 * directory cannot redirect a later read or write.
 */
export class SecureAutoloopLedger {
  readonly directory: string;

  private constructor(
    private readonly tasksParent: PinnedDirectory | undefined,
    private readonly runDirectory: PinnedDirectory,
    private readonly flags: Required<SecureLedgerPlatformFlags>,
    private readonly platform: NodeJS.Platform | undefined,
    private readonly logger: { warn?: (message: string) => void },
    private readonly testHooks: NonNullable<SecureAutoloopLedgerOptions['testHooks']>,
  ) {
    this.directory = runDirectory.path;
  }

  static open(workspace: string, runId: string, options: SecureAutoloopLedgerOptions = {}): SecureAutoloopLedger {
    validateRunId(runId);
    const flags = normalizedFlags(options.platformFlags);
    const create = options.create ?? false;
    const tasksDir = path.join(workspace, 'tasks');
    const runDir = path.join(tasksDir, runId);

    const tasks = this.openDirectory(tasksDir, 'Autoloop ledger tasks parent', create, flags, false);
    let run: { pinned: PinnedDirectory; fd: number; created: boolean } | undefined;
    try {
      run = this.openDirectory(runDir, 'Autoloop ledger run directory', create, flags, false);
      this.assertOpenDirectory(tasks.pinned, tasks.fd);
      this.hardenDirectory(tasks.fd, tasks.created);
      this.hardenDirectory(run.fd, run.created);
      const pinnedTasks = { ...tasks.pinned, stat: fs.fstatSync(tasks.fd) };
      const pinnedRun = { ...run.pinned, stat: fs.fstatSync(run.fd) };
      const ledger = new SecureAutoloopLedger(
        pinnedTasks,
        pinnedRun,
        flags,
        options.platform,
        options.logger ?? {},
        options.testHooks ?? {},
      );
      ledger.validateExistingFlatFiles(options.validateExistingFlatFiles);
      return ledger;
    } finally {
      if (run) fs.closeSync(run.fd);
      fs.closeSync(tasks.fd);
    }
  }

  /** Compatibility adapter for older push-log callers that pass a run path. */
  static forLedgerDirectory(ledgerDir: string, options: SecureAutoloopLedgerOptions = {}): SecureAutoloopLedger {
    const flags = normalizedFlags(options.platformFlags);
    const create = options.create ?? false;
    const run = this.openDirectory(ledgerDir, 'Autoloop ledger run directory', create, flags, true);
    try {
      this.hardenDirectory(run.fd, run.created);
      const ledger = new SecureAutoloopLedger(
        undefined,
        { ...run.pinned, stat: fs.fstatSync(run.fd) },
        flags,
        options.platform,
        options.logger ?? {},
        options.testHooks ?? {},
      );
      ledger.validateExistingFlatFiles(options.validateExistingFlatFiles);
      return ledger;
    } finally {
      fs.closeSync(run.fd);
    }
  }

  private static openDirectory(
    target: string,
    label: string,
    create: boolean,
    flags: Required<SecureLedgerPlatformFlags>,
    recursiveCreate: boolean,
  ): { pinned: PinnedDirectory; fd: number; created: boolean } {
    let observed = lstatIfPresent(target);
    const created = !observed;
    if (!observed) {
      if (!create) throw missingPath(target);
      fs.mkdirSync(target, { mode: PRIVATE_DIRECTORY_MODE, recursive: recursiveCreate });
      observed = fs.lstatSync(target);
    }
    if (observed.isSymbolicLink() || !observed.isDirectory()) rejectDirectory(target, label, observed);

    const fd = fs.openSync(target, fs.constants.O_RDONLY | flags.directory | flags.noFollow);
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isDirectory() || !sameIdentity(observed, opened)) {
        throw new Error(`${label} identity changed while it was being secured: '${target}'`);
      }
      return { pinned: { path: target, stat: opened, label }, fd, created };
    } catch (error) {
      fs.closeSync(fd);
      throw error;
    }
  }

  private static assertOpenDirectory(pinned: PinnedDirectory, fd: number): void {
    const opened = fs.fstatSync(fd);
    const current = fs.lstatSync(pinned.path);
    if (current.isSymbolicLink() || !current.isDirectory() || !opened.isDirectory() || !sameIdentity(opened, current)) {
      throw new Error(`${pinned.label} identity changed while the ledger was being opened`);
    }
  }

  private static hardenDirectory(fd: number, created: boolean): void {
    const current = fs.fstatSync(fd).mode & 0o777;
    const secure = created ? PRIVATE_DIRECTORY_MODE : current & PRIVATE_DIRECTORY_MODE;
    if (current !== secure) fs.fchmodSync(fd, secure);
  }

  private assertPinnedDirectory(pinned: PinnedDirectory): void {
    const observed = lstatIfPresent(pinned.path);
    if (!observed) throw new Error(`${pinned.label} identity changed or was removed: '${pinned.path}'`);
    if (observed.isSymbolicLink() || !observed.isDirectory()) rejectDirectory(pinned.path, pinned.label, observed);
    if (!sameIdentity(pinned.stat, observed)) {
      throw new Error(`${pinned.label} identity changed or was replaced: '${pinned.path}'`);
    }

    const fd = fs.openSync(pinned.path, fs.constants.O_RDONLY | this.flags.directory | this.flags.noFollow);
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isDirectory() || !sameIdentity(pinned.stat, opened)) {
        throw new Error(`${pinned.label} identity changed while it was being verified: '${pinned.path}'`);
      }
    } finally {
      fs.closeSync(fd);
    }
  }

  assertIdentity(): void {
    if (this.tasksParent) this.assertPinnedDirectory(this.tasksParent);
    this.assertPinnedDirectory(this.runDirectory);
  }

  validateExistingFlatFiles(names: readonly SecureAutoloopFlatFile[] = SECURE_AUTOLOOP_FLAT_FILES): void {
    this.assertIdentity();
    for (const name of names) {
      if (!lstatIfPresent(path.join(this.directory, name))) continue;
      const handle = this.openFlatFile(name, 'read');
      fs.closeSync(handle.fd);
    }
  }

  openFlatFile(name: SecureAutoloopFlatFile, mode: 'read' | 'append', create = false): SecureAutoloopFileHandle {
    if (!SECURE_AUTOLOOP_FLAT_FILES.includes(name)) throw new Error(`Unsupported Autoloop ledger file '${name}'`);
    if (mode === 'read' && create) throw new Error(`Cannot combine read mode with create for '${name}'`);
    this.assertIdentity();
    const filePath = path.join(this.directory, name);
    const observed = lstatIfPresent(filePath);
    if (observed && (observed.isSymbolicLink() || !observed.isFile() || observed.nlink !== 1)) {
      rejectFlatFile(filePath, observed);
    }
    if (!observed && !create) throw missingPath(filePath);

    const flags =
      mode === 'append'
        ? fs.constants.O_RDWR |
          fs.constants.O_APPEND |
          this.flags.noFollow |
          (observed ? 0 : fs.constants.O_CREAT | fs.constants.O_EXCL)
        : fs.constants.O_RDONLY | this.flags.noFollow;
    const fd = fs.openSync(filePath, flags, PRIVATE_FILE_MODE);
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.nlink !== 1 || (observed !== undefined && !sameIdentity(observed, opened))) {
        rejectFlatFile(filePath, opened);
      }

      const handle: SecureAutoloopFileHandle = {
        fd,
        filePath,
        created: observed === undefined,
        name,
        stat: opened,
      };
      const current = opened.mode & 0o777;
      const secure = observed ? current & PRIVATE_FILE_MODE : PRIVATE_FILE_MODE;
      if (current !== secure) {
        this.beforeFileMutation(handle, 'chmod');
        fs.fchmodSync(fd, secure);
        handle.stat = fs.fstatSync(fd);
      }
      return handle;
    } catch (error) {
      fs.closeSync(fd);
      throw error;
    }
  }

  private assertFileHandle(handle: SecureAutoloopFileHandle): void {
    this.assertIdentity();
    const opened = fs.fstatSync(handle.fd);
    const observed = lstatIfPresent(handle.filePath);
    if (!observed) throw new Error(`Autoloop ledger file identity changed or was removed: '${handle.filePath}'`);
    if (observed.isSymbolicLink() || !observed.isFile() || observed.nlink !== 1)
      rejectFlatFile(handle.filePath, observed);
    if (!opened.isFile() || opened.nlink !== 1) rejectFlatFile(handle.filePath, opened);
    if (!sameIdentity(handle.stat, opened) || !sameIdentity(opened, observed)) {
      throw new Error(`Autoloop ledger file identity changed or was replaced: '${handle.filePath}'`);
    }
  }

  private beforeFileMutation(
    handle: SecureAutoloopFileHandle,
    operation: SecureLedgerMutationEvent['operation'],
  ): void {
    this.testHooks.beforeFileMutation?.({
      name: handle.name,
      operation,
      filePath: handle.filePath,
      fd: handle.fd,
    });
    this.assertFileHandle(handle);
  }

  readFlatFile(name: SecureAutoloopFlatFile): string | undefined {
    let handle: SecureAutoloopFileHandle;
    try {
      handle = this.openFlatFile(name, 'read');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
      throw error;
    }
    try {
      return fs.readFileSync(handle.fd, 'utf8');
    } finally {
      fs.closeSync(handle.fd);
    }
  }

  prepareFlatFileAppend(name: SecureAutoloopFlatFile, content: string): SecureAutoloopPreparedAppend {
    const handle = this.openFlatFile(name, 'append', true);
    let committed = false;
    let fileSynced = false;
    let directorySynced = false;
    let closed = false;
    let appendError: unknown;
    const close = (): void => {
      if (closed) return;
      closed = true;
      fs.closeSync(handle.fd);
    };
    const commitDurable = (): void => {
      if (directorySynced) return;
      if (appendError !== undefined) throw appendError;
      if (closed) throw new Error(`Cannot commit closed Autoloop ledger append for '${name}'`);
      if (!committed) {
        try {
          this.beforeFileMutation(handle, 'append');
          fs.appendFileSync(handle.fd, content, { encoding: 'utf8' });
          committed = true;
        } catch (error) {
          appendError = error;
          throw error;
        }
      }
      if (!fileSynced) {
        try {
          this.beforeFileMutation(handle, 'flush');
          fs.fsyncSync(handle.fd);
          fileSynced = true;
        } catch (error) {
          throw new SecureAutoloopLedgerCommitError(
            'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
            `Autoloop ledger row was committed to ${name}, but its file durability barrier failed: ${errorMessage(error)}`,
            { cause: error },
          );
        }
      }
      if (!directorySynced) {
        try {
          this.syncDirectory(name);
          directorySynced = true;
        } catch (error) {
          throw new SecureAutoloopLedgerCommitError(
            'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
            `Autoloop ledger row was committed to ${name}, but its parent-directory durability barrier failed: ${errorMessage(error)}`,
            { cause: error },
          );
        }
      }
    };
    return {
      get committed() {
        return committed;
      },
      commitDurable,
      readLastNonEmptyLine: () => {
        if (closed) throw new Error(`Cannot read closed Autoloop ledger append for '${name}'`);
        this.assertFileHandle(handle);
        let end = fs.fstatSync(handle.fd).size;
        const byte = Buffer.allocUnsafe(1);
        while (end > 0) {
          fs.readSync(handle.fd, byte, 0, 1, end - 1);
          if (byte[0] !== 0x0a && byte[0] !== 0x0d) break;
          end--;
        }
        const chunks: Buffer[] = [];
        let cursor = end;
        while (cursor > 0) {
          const start = Math.max(0, cursor - 8_192);
          const chunk = Buffer.allocUnsafe(cursor - start);
          fs.readSync(handle.fd, chunk, 0, chunk.length, start);
          const newline = chunk.lastIndexOf(0x0a);
          if (newline >= 0) {
            chunks.push(chunk.subarray(newline + 1));
            break;
          }
          chunks.push(chunk);
          cursor = start;
        }
        return Buffer.concat(chunks.reverse()).toString('utf8');
      },
      close,
    };
  }

  appendFlatFile(name: SecureAutoloopFlatFile, content: string, durable = false): void {
    const handle = this.openFlatFile(name, 'append', true);
    try {
      this.beforeFileMutation(handle, 'append');
      const expected = Buffer.byteLength(content);
      const written = fs.writeSync(handle.fd, content, null, 'utf8');
      if (written !== expected) throw new Error(`Could not append the complete ${name} record`);
      if (durable) {
        try {
          this.beforeFileMutation(handle, 'flush');
          fs.fsyncSync(handle.fd);
        } catch (error) {
          throw new SecureAutoloopLedgerCommitError(
            'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
            `Autoloop ledger row was committed to ${name}, but its file durability barrier failed: ${errorMessage(error)}`,
            { cause: error },
          );
        }
      }
    } finally {
      fs.closeSync(handle.fd);
    }
    if (durable) {
      try {
        this.syncDirectory(name);
      } catch (error) {
        throw new SecureAutoloopLedgerCommitError(
          'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
          `Autoloop ledger row was committed to ${name}, but its parent-directory durability barrier failed: ${errorMessage(error)}`,
          { cause: error },
        );
      }
    }
  }

  flushFlatFile(name: SecureAutoloopFlatFile): void {
    const handle = this.openFlatFile(name, 'append');
    try {
      try {
        this.beforeFileMutation(handle, 'flush');
        fs.fsyncSync(handle.fd);
      } catch (error) {
        throw new SecureAutoloopLedgerCommitError(
          'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
          `Autoloop ledger row was committed to ${name}, but its file durability barrier failed: ${errorMessage(error)}`,
          { cause: error },
        );
      }
    } finally {
      fs.closeSync(handle.fd);
    }
    try {
      this.syncDirectory(name);
    } catch (error) {
      throw new SecureAutoloopLedgerCommitError(
        'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        `Autoloop ledger row was committed to ${name}, but its parent-directory durability barrier failed: ${errorMessage(error)}`,
        { cause: error },
      );
    }
  }

  syncDirectory(name?: SecureAutoloopFlatFile): void {
    this.assertIdentity();
    const event: SecureLedgerMutationEvent = {
      ...(name ? { name } : {}),
      operation: 'directory-sync',
      filePath: this.directory,
    };
    this.testHooks.beforeDirectorySync?.(event);
    this.assertIdentity();
    if ((this.platform ?? process.platform) === 'win32') {
      this.logger.warn?.(
        name === 'decisions.jsonl'
          ? '[autoloop] parent-directory fsync is unavailable on win32; control file contents were flushed without a POSIX directory-entry guarantee'
          : `[autoloop] ${name ?? 'ledger'} was flushed, but parent-directory fsync is unavailable on win32`,
      );
      return;
    }
    const fd = fs.openSync(this.directory, fs.constants.O_RDONLY | this.flags.directory | this.flags.noFollow);
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isDirectory() || !sameIdentity(opened, this.runDirectory.stat)) {
        throw new Error(`Autoloop ledger run directory identity changed before directory sync`);
      }
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  }
}

/** Compatibility handle used by the existing timeout-migration paths. */
export interface PrivateAutoloopDecisionsHandle {
  fd: number;
  filePath: string;
}

export function openPrivateAutoloopDecisions(
  workspace: string,
  runId: string,
  mode: 'read' | 'append',
  create = false,
): PrivateAutoloopDecisionsHandle {
  if (mode === 'read' && create) throw new Error(`Cannot combine read mode with create for decisions.jsonl`);
  const ledger = SecureAutoloopLedger.open(workspace, runId, {
    create,
    validateExistingFlatFiles: ['decisions.jsonl'],
  });
  return ledger.openFlatFile('decisions.jsonl', mode, create);
}

export function securePrivateAutoloopDecisionLedger(workspace: string, runId: string): string {
  const ledger = SecureAutoloopLedger.open(workspace, runId, {
    create: true,
    validateExistingFlatFiles: ['decisions.jsonl'],
  });
  ledger.readFlatFile('decisions.jsonl');
  return ledger.directory;
}
