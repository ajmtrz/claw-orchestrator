import * as fs from 'node:fs';
import * as path from 'node:path';
import { randomUUID } from 'node:crypto';

const PRIVATE_DIRECTORY_MODE = 0o700;
const PRIVATE_FILE_MODE = 0o600;

export const SECURE_AUTOLOOP_FLAT_FILES = [
  'decisions.jsonl',
  'agent-generations.jsonl',
  'chat.jsonl',
  'push_log.jsonl',
] as const;

export type SecureAutoloopFlatFile = (typeof SECURE_AUTOLOOP_FLAT_FILES)[number];

export const SECURE_AUTOLOOP_ITERATION_ARTIFACTS = [
  'directive.json',
  'eval_output.json',
  'coder_summary.txt',
  'diff.patch',
  'verdict.json',
] as const;

export type SecureAutoloopIterationArtifact = (typeof SECURE_AUTOLOOP_ITERATION_ARTIFACTS)[number];
export type SecureAutoloopReviewerPersistentFile = 'reviewer_memory.md' | 'reviewer_log.jsonl';

export interface SecureReviewerControlFiles {
  plan?: Buffer;
  goal?: Buffer;
}

export interface SecureReviewerStageResult {
  directory: string;
  priorVerdict: boolean;
}

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

export interface SecureLedgerNestedMutationEvent {
  operation: 'artifact-write' | 'artifact-commit' | 'sandbox-reset' | 'sandbox-stage';
  relativePath: string;
  filePath: string;
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
    beforeNestedMutation?: (event: SecureLedgerNestedMutationEvent) => void;
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
  readonly retryable = false;
  readonly effectsApplied: boolean;
  readonly operation: 'secure_ledger_append' | 'secure_nested_artifact_write' | 'send_timeout_migration';

  constructor(
    readonly code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE' | 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
    message: string,
    options: {
      cause: unknown;
      effectsApplied?: boolean;
      operation?: 'secure_ledger_append' | 'secure_nested_artifact_write' | 'send_timeout_migration';
    },
  ) {
    super(message, options);
    this.name = 'SecureAutoloopLedgerCommitError';
    this.effectsApplied = options.effectsApplied ?? false;
    this.operation = options.operation ?? 'secure_ledger_append';
  }

  withAppliedOutcome(operation: 'send_timeout_migration'): SecureAutoloopLedgerCommitError {
    const error = new SecureAutoloopLedgerCommitError(this.code, this.message, {
      cause: this.cause,
      effectsApplied: true,
      operation,
    });
    error.stack = this.stack;
    return error;
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

interface RegularChildSnapshot {
  content: Buffer;
  stat: fs.Stats;
}

type ReviewerSandboxEntrySnapshot =
  | { kind: 'file'; content: Buffer; stat: fs.Stats }
  | { kind: 'directory'; entries: ReviewerSandboxSnapshot };

type ReviewerSandboxSnapshot = Map<string, ReviewerSandboxEntrySnapshot>;

type ReviewerSandboxEntryContents =
  | { kind: 'file'; content: Buffer }
  | { kind: 'directory'; entries: ReviewerSandboxContents };

type ReviewerSandboxContents = Map<string, ReviewerSandboxEntryContents>;

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

function rejectNestedFile(target: string, label: string, observed: fs.Stats): never {
  if (observed.isSymbolicLink()) throw new Error(`Refusing ${label} symbolic link '${target}'`);
  if (!observed.isFile()) throw new Error(`Refusing non-regular ${label} '${target}'`);
  throw new Error(`Refusing ${label} hardlink with link count ${observed.nlink} at '${target}'`);
}

function validatePathComponent(component: string, label: string): void {
  if (!component || component === '.' || component === '..' || path.basename(component) !== component) {
    throw new Error(`${label} must be one path component`);
  }
}

function validateIteration(iter: number): void {
  if (!Number.isSafeInteger(iter) || iter < 0) {
    throw new Error(`Autoloop iteration must be a nonnegative integer`);
  }
}

function validateIterationArtifact(name: SecureAutoloopIterationArtifact): void {
  if (!SECURE_AUTOLOOP_ITERATION_ARTIFACTS.includes(name)) {
    throw new Error(`Unsupported Autoloop iteration artifact '${String(name)}'`);
  }
  validatePathComponent(name, 'Autoloop iteration artifact');
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
  private iterationRoot: PinnedDirectory | undefined;
  private readonly iterationDirectories = new Map<number, PinnedDirectory>();
  private reviewerSandbox: PinnedDirectory | undefined;

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

  private openPinnedChildDirectory(
    parent: PinnedDirectory,
    name: string,
    label: string,
    create: boolean,
  ): PinnedDirectory {
    validatePathComponent(name, label);
    this.assertIdentity();
    this.assertPinnedDirectory(parent);
    const target = path.join(parent.path, name);
    let observed = lstatIfPresent(target);
    const created = !observed;
    if (!observed) {
      if (!create) throw missingPath(target);
      fs.mkdirSync(target, { mode: PRIVATE_DIRECTORY_MODE });
      observed = fs.lstatSync(target);
    }
    if (observed.isSymbolicLink() || !observed.isDirectory()) rejectDirectory(target, label, observed);

    const fd = fs.openSync(target, fs.constants.O_RDONLY | this.flags.directory | this.flags.noFollow);
    try {
      const opened = fs.fstatSync(fd);
      this.assertPinnedDirectory(parent);
      if (!opened.isDirectory() || !sameIdentity(observed, opened)) {
        throw new Error(`${label} identity changed while it was being secured: '${target}'`);
      }
      SecureAutoloopLedger.hardenDirectory(fd, created);
      const pinned = { path: target, stat: fs.fstatSync(fd), label };
      if (created) this.syncPinnedDirectory(parent, `${label} creation`);
      return pinned;
    } finally {
      fs.closeSync(fd);
    }
  }

  private syncPinnedDirectory(pinned: PinnedDirectory, label: string): void {
    this.assertIdentity();
    this.assertPinnedDirectory(pinned);
    this.testHooks.beforeDirectorySync?.({
      operation: 'directory-sync',
      filePath: pinned.path,
    });
    this.assertIdentity();
    this.assertPinnedDirectory(pinned);
    if ((this.platform ?? process.platform) === 'win32') {
      this.logger.warn?.(`[autoloop] ${label} was flushed, but directory fsync is unavailable on win32`);
      return;
    }
    const fd = fs.openSync(pinned.path, fs.constants.O_RDONLY | this.flags.directory | this.flags.noFollow);
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isDirectory() || !sameIdentity(opened, pinned.stat)) {
        throw new Error(`${pinned.label} identity changed before directory sync`);
      }
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }
  }

  private syncCommittedNestedArtifact(
    parent: PinnedDirectory,
    name: string,
    relativePath: string,
    expected: RegularChildSnapshot,
  ): void {
    const target = path.join(parent.path, name);
    let fd: number | undefined;
    try {
      const observed = lstatIfPresent(target);
      if (!observed || observed.isSymbolicLink() || !observed.isFile() || observed.nlink !== 1) {
        if (!observed)
          throw new Error(`Autoloop nested artifact was removed before its durability barrier: '${relativePath}'`);
        rejectNestedFile(target, 'Autoloop nested artifact', observed);
      }
      fd = fs.openSync(target, fs.constants.O_RDONLY | this.flags.noFollow);
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.nlink !== 1 || !sameIdentity(expected.stat, opened)) {
        throw new Error(`Autoloop nested artifact identity changed before its durability barrier: '${relativePath}'`);
      }
      this.syncPinnedDirectory(parent, relativePath);
      const verified = this.openRegularChildSnapshot(parent, name, 'Autoloop nested artifact');
      if (!verified) {
        throw new Error(`Autoloop nested artifact was removed after its durability barrier: '${relativePath}'`);
      }
      if (!sameIdentity(expected.stat, verified.stat)) {
        throw new Error(`Autoloop nested artifact identity changed after its durability barrier: '${relativePath}'`);
      }
      if (!verified.content.equals(expected.content)) {
        throw new Error(`Autoloop nested artifact contents changed after its durability barrier: '${relativePath}'`);
      }
    } catch (error) {
      throw new SecureAutoloopLedgerCommitError(
        'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        `Autoloop nested artifact was committed to ${relativePath}, but its parent-directory durability barrier failed: ${errorMessage(error)}`,
        { cause: error, operation: 'secure_nested_artifact_write' },
      );
    } finally {
      if (fd !== undefined) fs.closeSync(fd);
    }
  }

  private syncCommittedReviewerSandbox(
    sandbox: PinnedDirectory,
    iter: number,
    expected: ReviewerSandboxSnapshot,
  ): void {
    try {
      this.syncPinnedDirectory(sandbox, `Reviewer sandbox iteration ${iter}`);
      this.assertReviewerSandboxMatches(sandbox, expected, 'Reviewer sandbox final durability barrier');
    } catch (error) {
      throw new SecureAutoloopLedgerCommitError(
        'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        `Reviewer sandbox iteration ${iter} was staged, but its final parent-directory durability barrier failed: ${errorMessage(error)}`,
        { cause: error, operation: 'secure_nested_artifact_write' },
      );
    }
  }

  private getIterationRoot(create: boolean): PinnedDirectory {
    if (this.iterationRoot) {
      this.assertIdentity();
      this.assertPinnedDirectory(this.iterationRoot);
      return this.iterationRoot;
    }
    this.iterationRoot = this.openPinnedChildDirectory(this.runDirectory, 'iter', 'Autoloop iteration root', create);
    return this.iterationRoot;
  }

  private getIterationDirectory(iter: number, create: boolean): PinnedDirectory {
    validateIteration(iter);
    const existing = this.iterationDirectories.get(iter);
    if (existing) {
      this.assertIdentity();
      this.assertPinnedDirectory(this.getIterationRoot(false));
      this.assertPinnedDirectory(existing);
      return existing;
    }
    const root = this.getIterationRoot(create);
    const pinned = this.openPinnedChildDirectory(root, String(iter), `Autoloop iteration ${iter} directory`, create);
    this.iterationDirectories.set(iter, pinned);
    return pinned;
  }

  private openRegularChildSnapshot(
    parent: PinnedDirectory,
    name: string,
    label: string,
  ): RegularChildSnapshot | undefined {
    validatePathComponent(name, label);
    this.assertIdentity();
    this.assertPinnedDirectory(parent);
    const target = path.join(parent.path, name);
    const observed = lstatIfPresent(target);
    if (!observed) return undefined;
    if (observed.isSymbolicLink() || !observed.isFile() || observed.nlink !== 1) {
      rejectNestedFile(target, label, observed);
    }
    const fd = fs.openSync(target, fs.constants.O_RDONLY | this.flags.noFollow);
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.nlink !== 1 || !sameIdentity(observed, opened)) {
        rejectNestedFile(target, label, opened);
      }
      this.assertPinnedDirectory(parent);
      const content = fs.readFileSync(fd);
      const afterOpen = fs.fstatSync(fd);
      const current = lstatIfPresent(target);
      if (
        !current ||
        current.isSymbolicLink() ||
        !current.isFile() ||
        current.nlink !== 1 ||
        !sameIdentity(opened, afterOpen) ||
        !sameIdentity(afterOpen, current) ||
        afterOpen.size !== opened.size ||
        afterOpen.mtimeMs !== opened.mtimeMs
      ) {
        throw new Error(`${label} identity or contents changed while it was being read: '${target}'`);
      }
      this.assertPinnedDirectory(parent);
      return { content, stat: afterOpen };
    } finally {
      fs.closeSync(fd);
    }
  }

  private openRegularChild(parent: PinnedDirectory, name: string, label: string): Buffer | undefined {
    return this.openRegularChildSnapshot(parent, name, label)?.content;
  }

  private writeAtomicChild(
    parent: PinnedDirectory,
    name: string,
    content: string | Buffer,
    relativePath: string,
    operation: SecureLedgerNestedMutationEvent['operation'],
  ): 'created' | 'unchanged' {
    validatePathComponent(name, 'Autoloop nested artifact');
    const target = path.join(parent.path, name);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content, 'utf8');
    const existing = this.openRegularChildSnapshot(parent, name, 'Autoloop nested artifact');
    if (existing) {
      if (!existing.content.equals(bytes)) {
        throw new Error(`Refusing to overwrite conflicting immutable Autoloop artifact '${target}'`);
      }
      this.syncCommittedNestedArtifact(parent, name, relativePath, existing);
      return 'unchanged';
    }

    this.testHooks.beforeNestedMutation?.({ operation: 'artifact-write', relativePath, filePath: target });
    this.assertIdentity();
    this.assertPinnedDirectory(parent);

    const temporary = path.join(parent.path, `.${name}.tmp-${process.pid}-${randomUUID()}`);
    let fd: number | undefined;
    let temporaryCreated = false;
    let renamed = false;
    try {
      fd = fs.openSync(
        temporary,
        fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | this.flags.noFollow,
        PRIVATE_FILE_MODE,
      );
      temporaryCreated = true;
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.nlink !== 1) rejectNestedFile(temporary, 'Autoloop temporary artifact', opened);
      let offset = 0;
      while (offset < bytes.length) {
        const written = fs.writeSync(fd, bytes, offset, bytes.length - offset, null);
        if (written <= 0) throw new Error(`Could not write complete Autoloop artifact '${target}'`);
        offset += written;
      }
      fs.fsyncSync(fd);
      fs.closeSync(fd);
      fd = undefined;

      this.testHooks.beforeNestedMutation?.({ operation, relativePath, filePath: target });
      this.assertIdentity();
      this.assertPinnedDirectory(parent);
      const planted = lstatIfPresent(target);
      if (planted) {
        if (planted.isSymbolicLink() || !planted.isFile() || planted.nlink !== 1) {
          rejectNestedFile(target, 'Autoloop nested artifact', planted);
        }
        const plantedSnapshot = this.openRegularChildSnapshot(parent, name, 'Autoloop nested artifact');
        if (!plantedSnapshot?.content.equals(bytes)) {
          throw new Error(`Refusing to overwrite conflicting immutable Autoloop artifact '${target}'`);
        }
        this.syncCommittedNestedArtifact(parent, name, relativePath, plantedSnapshot);
        return 'unchanged';
      }

      fs.renameSync(temporary, target);
      renamed = true;
      const committed = this.openRegularChildSnapshot(parent, name, 'Autoloop nested artifact');
      if (!committed?.content.equals(bytes)) throw new Error(`Autoloop artifact commit was incomplete: '${target}'`);
      this.syncCommittedNestedArtifact(parent, name, relativePath, committed);
      return 'created';
    } finally {
      if (fd !== undefined) {
        try {
          fs.closeSync(fd);
        } catch (error) {
          this.logger.warn?.(`[autoloop] failed to close incomplete nested artifact: ${errorMessage(error)}`);
        }
      }
      if (!renamed) {
        try {
          fs.unlinkSync(temporary);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === 'ENOENT' && temporaryCreated) {
            this.logger.warn?.(
              `[autoloop] incomplete nested artifact '${relativePath}' could not be located during cleanup`,
            );
          } else if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
            this.logger.warn?.(`[autoloop] failed to remove incomplete nested artifact: ${errorMessage(error)}`);
          }
        }
      }
    }
  }

  validateExistingFlatFiles(names: readonly SecureAutoloopFlatFile[] = SECURE_AUTOLOOP_FLAT_FILES): void {
    this.assertIdentity();
    for (const name of names) {
      if (!lstatIfPresent(path.join(this.directory, name))) continue;
      const handle = this.openFlatFile(name, 'read');
      fs.closeSync(handle.fd);
    }
  }

  readIterationArtifact(iter: number, name: SecureAutoloopIterationArtifact): Buffer | undefined {
    validateIteration(iter);
    validateIterationArtifact(name);
    let directory: PinnedDirectory;
    try {
      directory = this.getIterationDirectory(iter, false);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
      throw error;
    }
    return this.openRegularChild(directory, name, `Autoloop iteration ${iter} artifact`);
  }

  writeIterationArtifact(
    iter: number,
    name: SecureAutoloopIterationArtifact,
    content: string | Buffer,
  ): 'created' | 'unchanged' {
    validateIteration(iter);
    validateIterationArtifact(name);
    const directory = this.getIterationDirectory(iter, true);
    return this.writeAtomicChild(directory, name, content, `iter/${iter}/${name}`, 'artifact-commit');
  }

  private getReviewerSandbox(create: boolean): PinnedDirectory {
    if (this.reviewerSandbox) {
      this.assertIdentity();
      this.assertPinnedDirectory(this.reviewerSandbox);
      return this.reviewerSandbox;
    }
    this.reviewerSandbox = this.openPinnedChildDirectory(
      this.runDirectory,
      'reviewer_sandbox',
      'Autoloop Reviewer sandbox',
      create,
    );
    return this.reviewerSandbox;
  }

  ensureReviewerSandbox(): string {
    const sandbox = this.getReviewerSandbox(true);
    this.assertReviewerSandboxBoundary(sandbox);
    return sandbox.path;
  }

  readReviewerPersistentFile(name: SecureAutoloopReviewerPersistentFile): string | undefined {
    if (name !== 'reviewer_memory.md' && name !== 'reviewer_log.jsonl') {
      throw new Error(`Unsupported Reviewer persistent file '${String(name)}'`);
    }
    let sandbox: PinnedDirectory;
    try {
      sandbox = this.getReviewerSandbox(false);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
      throw error;
    }
    return this.openRegularChild(sandbox, name, 'Reviewer persistent file')?.toString('utf8');
  }

  private assertSafeRemovableSandboxEntry(target: string): void {
    const observed = fs.lstatSync(target);
    if (observed.isSymbolicLink()) throw new Error(`Refusing unsafe Reviewer sandbox symbolic link '${target}'`);
    if (observed.isFile()) {
      if (observed.nlink !== 1) {
        throw new Error(`Refusing unsafe Reviewer sandbox hardlink with link count ${observed.nlink} at '${target}'`);
      }
      return;
    }
    if (!observed.isDirectory()) throw new Error(`Refusing unsafe Reviewer sandbox entry '${target}'`);
    for (const entry of fs.readdirSync(target)) {
      validatePathComponent(entry, 'Reviewer sandbox entry');
      this.assertSafeRemovableSandboxEntry(path.join(target, entry));
    }
  }

  private readReviewerSandboxEntries(directory: PinnedDirectory): string[] {
    this.assertIdentity();
    this.assertPinnedDirectory(directory);
    const entries = fs.readdirSync(directory.path).sort();
    for (const entry of entries) validatePathComponent(entry, 'Reviewer sandbox entry');
    this.assertPinnedDirectory(directory);
    return entries;
  }

  private snapshotReviewerSandbox(directory: PinnedDirectory, label: string): ReviewerSandboxSnapshot {
    const snapshot: ReviewerSandboxSnapshot = new Map();
    for (const entry of this.readReviewerSandboxEntries(directory)) {
      const target = path.join(directory.path, entry);
      const observed = lstatIfPresent(target);
      if (!observed) throw new Error(`${label} membership changed unexpectedly; '${target}' was removed`);
      if (observed.isSymbolicLink()) {
        throw new Error(`Refusing unsafe Reviewer sandbox symbolic link '${target}'`);
      }
      if (observed.isFile()) {
        const file = this.openRegularChildSnapshot(directory, entry, `${label} regular file`);
        if (!file) throw new Error(`${label} membership changed unexpectedly; '${target}' was removed`);
        snapshot.set(entry, { kind: 'file', content: Buffer.from(file.content), stat: file.stat });
        continue;
      }
      if (!observed.isDirectory()) throw new Error(`Refusing unsafe Reviewer sandbox entry '${target}'`);
      const child = this.openPinnedChildDirectory(directory, entry, `${label} directory '${entry}'`, false);
      snapshot.set(entry, { kind: 'directory', entries: this.snapshotReviewerSandbox(child, `${label}/${entry}`) });
    }
    this.assertPinnedDirectory(directory);
    return snapshot;
  }

  private assertReviewerSandboxMatches(
    directory: PinnedDirectory,
    expected: ReviewerSandboxSnapshot,
    label: string,
  ): void {
    const observed = this.readReviewerSandboxEntries(directory);
    const expectedEntries = [...expected.keys()].sort();
    if (
      observed.length !== expectedEntries.length ||
      observed.some((entry, index) => entry !== expectedEntries[index])
    ) {
      throw new Error(
        `${label} membership changed unexpectedly; expected [${expectedEntries.join(', ')}], found [${observed.join(', ')}]`,
      );
    }
    for (const entry of observed) {
      const expectedEntry = expected.get(entry)!;
      const target = path.join(directory.path, entry);
      if (expectedEntry.kind === 'file') {
        const file = this.openRegularChildSnapshot(directory, entry, `${label} expected regular file`);
        if (!file) throw new Error(`${label} expected regular file was removed: '${target}'`);
        if (!sameIdentity(file.stat, expectedEntry.stat)) {
          throw new Error(`${label} regular file identity changed unexpectedly: '${target}'`);
        }
        if (!file.content.equals(expectedEntry.content)) {
          throw new Error(`${label} regular file contents changed unexpectedly: '${target}'`);
        }
        continue;
      }
      const child = this.openPinnedChildDirectory(directory, entry, `${label} expected directory`, false);
      this.assertReviewerSandboxMatches(child, expectedEntry.entries, `${label}/${entry}`);
    }
    this.assertPinnedDirectory(directory);
  }

  private assertReviewerSandboxContentsMatch(
    directory: PinnedDirectory,
    expected: ReviewerSandboxContents,
    label: string,
  ): void {
    const observed = this.readReviewerSandboxEntries(directory);
    const expectedEntries = [...expected.keys()].sort();
    if (
      observed.length !== expectedEntries.length ||
      observed.some((entry, index) => entry !== expectedEntries[index])
    ) {
      throw new Error(
        `${label} membership changed unexpectedly; expected [${expectedEntries.join(', ')}], found [${observed.join(', ')}]`,
      );
    }
    for (const entry of observed) {
      const expectedEntry = expected.get(entry)!;
      const target = path.join(directory.path, entry);
      if (expectedEntry.kind === 'file') {
        const file = this.openRegularChildSnapshot(directory, entry, `${label} expected regular file`);
        if (!file) throw new Error(`${label} expected regular file was removed: '${target}'`);
        if (!file.content.equals(expectedEntry.content)) {
          throw new Error(`${label} regular file contents changed unexpectedly: '${target}'`);
        }
        continue;
      }
      const child = this.openPinnedChildDirectory(directory, entry, `${label} expected directory`, false);
      this.assertReviewerSandboxContentsMatch(child, expectedEntry.entries, `${label}/${entry}`);
    }
    this.assertPinnedDirectory(directory);
  }

  private assertReviewerSandboxBoundary(sandbox: PinnedDirectory): void {
    const persistentOrControl = new Set([
      'reviewer_memory.md',
      'reviewer_log.jsonl',
      'plan.md',
      'goal.json',
      'prior_verdict.json',
    ]);
    let stagedIterationSeen = false;
    for (const entry of this.readReviewerSandboxEntries(sandbox)) {
      if (persistentOrControl.has(entry)) {
        this.openRegularChild(sandbox, entry, 'Reviewer sandbox file');
        continue;
      }
      const match = /^iter-(0|[1-9]\d*)$/.exec(entry);
      if (!match || stagedIterationSeen) {
        throw new Error(`Refusing unapproved Reviewer sandbox entry '${path.join(sandbox.path, entry)}'`);
      }
      const iter = Number(match[1]);
      validateIteration(iter);
      const staged = this.openPinnedChildDirectory(sandbox, entry, `Reviewer staged iteration ${iter}`, false);
      const expectedArtifacts: ReviewerSandboxContents = new Map();
      for (const artifact of ['directive.json', 'eval_output.json', 'coder_summary.txt', 'diff.patch'] as const) {
        const content = this.readIterationArtifact(iter, artifact);
        if (content === undefined) {
          throw new Error(`Reviewer staged iteration ${iter} has no authoritative iter/${iter}/${artifact}`);
        }
        expectedArtifacts.set(artifact, { kind: 'file', content });
      }
      this.assertReviewerSandboxContentsMatch(staged, expectedArtifacts, `Reviewer staged iteration ${iter}`);
      stagedIterationSeen = true;
    }
    this.assertPinnedDirectory(sandbox);
  }

  stageReviewerSandbox(iter: number, controls: SecureReviewerControlFiles = {}): SecureReviewerStageResult {
    validateIteration(iter);
    const artifacts = new Map<SecureAutoloopIterationArtifact, Buffer>();
    for (const name of ['directive.json', 'eval_output.json', 'coder_summary.txt', 'diff.patch'] as const) {
      const content = this.readIterationArtifact(iter, name);
      if (content === undefined) {
        throw new Error(`Reviewer stage requires a complete artifact set; missing iter/${iter}/${name}`);
      }
      artifacts.set(name, content);
    }
    const prior = iter > 0 ? this.readIterationArtifact(iter - 1, 'verdict.json') : undefined;

    const plan = controls.plan === undefined ? undefined : Buffer.from(controls.plan);
    const goal = controls.goal === undefined ? undefined : Buffer.from(controls.goal);
    const sandbox = this.getReviewerSandbox(true);
    const persistent = new Set<SecureAutoloopReviewerPersistentFile>(['reviewer_memory.md', 'reviewer_log.jsonl']);
    const persistentEntries: ReviewerSandboxSnapshot = new Map();
    const removable: string[] = [];
    const initialSnapshot = this.snapshotReviewerSandbox(sandbox, 'Reviewer sandbox before reset');
    for (const [entry, snapshot] of initialSnapshot) {
      const target = path.join(sandbox.path, entry);
      if (persistent.has(entry as SecureAutoloopReviewerPersistentFile)) {
        if (snapshot.kind !== 'file') {
          throw new Error(`Reviewer persistent file must be a regular file: '${target}'`);
        }
        persistentEntries.set(entry, snapshot);
        continue;
      }
      removable.push(target);
    }

    this.testHooks.beforeNestedMutation?.({
      operation: 'sandbox-reset',
      relativePath: 'reviewer_sandbox',
      filePath: sandbox.path,
    });
    this.assertIdentity();
    this.assertPinnedDirectory(sandbox);
    this.assertReviewerSandboxMatches(sandbox, initialSnapshot, 'Reviewer sandbox reset seam');
    for (const target of removable) {
      this.assertPinnedDirectory(sandbox);
      this.assertSafeRemovableSandboxEntry(target);
      fs.rmSync(target, { recursive: true, force: false });
    }
    this.assertReviewerSandboxMatches(sandbox, persistentEntries, 'Reviewer sandbox after reset');

    const destination = this.openPinnedChildDirectory(
      sandbox,
      `iter-${iter}`,
      `Reviewer staged iteration ${iter}`,
      true,
    );
    for (const [name, content] of artifacts) {
      this.writeAtomicChild(destination, name, content, `reviewer_sandbox/iter-${iter}/${name}`, 'sandbox-stage');
    }
    if (plan) {
      this.writeAtomicChild(sandbox, 'plan.md', plan, 'reviewer_sandbox/plan.md', 'sandbox-stage');
    }
    if (goal) {
      this.writeAtomicChild(sandbox, 'goal.json', goal, 'reviewer_sandbox/goal.json', 'sandbox-stage');
    }

    if (prior) {
      this.writeAtomicChild(
        sandbox,
        'prior_verdict.json',
        prior,
        'reviewer_sandbox/prior_verdict.json',
        'sandbox-stage',
      );
    }
    const expectedArtifacts: ReviewerSandboxContents = new Map();
    for (const [name, content] of artifacts) {
      expectedArtifacts.set(name, { kind: 'file', content: Buffer.from(content) });
    }
    const expectedSandboxEntries: ReviewerSandboxContents = new Map();
    for (const [entry, snapshot] of persistentEntries) {
      if (snapshot.kind !== 'file') {
        throw new Error(`Reviewer persistent file must be a regular file: '${path.join(sandbox.path, entry)}'`);
      }
      expectedSandboxEntries.set(entry, { kind: 'file', content: snapshot.content });
    }
    expectedSandboxEntries.set(`iter-${iter}`, { kind: 'directory', entries: expectedArtifacts });
    if (plan) expectedSandboxEntries.set('plan.md', { kind: 'file', content: plan });
    if (goal) expectedSandboxEntries.set('goal.json', { kind: 'file', content: goal });
    if (prior) expectedSandboxEntries.set('prior_verdict.json', { kind: 'file', content: prior });
    this.assertReviewerSandboxContentsMatch(sandbox, expectedSandboxEntries, 'Reviewer sandbox authoritative files');
    const stagedSnapshot = this.snapshotReviewerSandbox(sandbox, 'Reviewer sandbox staged snapshot');
    this.testHooks.beforeNestedMutation?.({
      operation: 'sandbox-stage',
      relativePath: `reviewer_sandbox/iter-${iter}`,
      filePath: destination.path,
    });
    this.assertPinnedDirectory(sandbox);
    this.assertPinnedDirectory(destination);
    this.assertReviewerSandboxMatches(sandbox, stagedSnapshot, 'Reviewer sandbox final stage');
    this.syncCommittedReviewerSandbox(sandbox, iter, stagedSnapshot);
    return { directory: sandbox.path, priorVerdict: prior !== undefined };
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
