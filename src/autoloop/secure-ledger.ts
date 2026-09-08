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

const REVIEW_EVIDENCE_LIMIT_BYTES = 4 * 1024 * 1024;

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

export interface SecureLedgerNestedPublishEvent {
  relativePath: string;
  filePath: string;
  temporaryPath: string;
}

export interface SecureReviewerSandboxReadEvent {
  relativePath: string;
  filePath: string;
  kind: 'file' | 'directory';
}

export interface SecureReviewerSandboxResidueRemovalEvent {
  relativePath: string;
  filePath: string;
  kind: 'file' | 'symlink' | 'directory' | 'special';
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
    beforeNestedTemporaryIo?: (
      event: SecureLedgerNestedPublishEvent & {
        phase: 'after-create' | 'before-write' | 'before-flush';
        fd: number;
      },
    ) => void;
    beforeNestedPublish?: (event: SecureLedgerNestedPublishEvent) => void;
    publishNestedTemporary?: (temporaryPath: string, targetPath: string) => void;
    afterNestedPublish?: (event: SecureLedgerNestedPublishEvent) => void;
    closeNestedTemporary?: (fd: number) => void;
    unlinkNestedTemporary?: (temporaryPath: string) => void;
    afterNestedTemporaryUnlink?: (event: SecureLedgerNestedPublishEvent) => void;
    afterNestedChildLstat?: (event: { filePath: string; label: string }) => void;
    beforeNestedChildContentRead?: (event: { filePath: string; label: string; fd: number; size: number }) => void;
    beforeNestedChildDescriptorRead?: (event: {
      filePath: string;
      label: string;
      fd: number;
      phase: 'content' | 'growth-probe';
      bufferLength: number;
      offset: number;
      length: number;
    }) => void;
    afterReviewerSandboxEntryRead?: (event: SecureReviewerSandboxReadEvent) => void;
    beforeReviewerSandboxResidueRemoval?: (event: SecureReviewerSandboxResidueRemovalEvent) => void;
    closeFlatFileDescriptor?: (fd: number) => void;
    closeDescriptor?: (fd: number) => void;
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
  readonly secondaryErrors: Error[] = [];
  readonly effectsApplied: boolean;
  readonly operation:
    | 'secure_ledger_append'
    | 'secure_nested_artifact_write'
    | 'secure_reviewer_sandbox_stage'
    | 'send_timeout_migration';

  constructor(
    readonly code:
      | 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE'
      | 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE'
      | 'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE'
      | 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
    message: string,
    options: {
      cause: unknown;
      effectsApplied?: boolean;
      operation?:
        | 'secure_ledger_append'
        | 'secure_nested_artifact_write'
        | 'secure_reviewer_sandbox_stage'
        | 'send_timeout_migration';
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

interface AtomicChildWriteResult {
  outcome: 'created' | 'unchanged';
  snapshot: RegularChildSnapshot;
}

type ReviewerSandboxEntrySnapshot =
  | { kind: 'file'; content: Buffer; stat: fs.Stats; maxBytes?: number }
  | { kind: 'directory'; entries: ReviewerSandboxSnapshot; stat: fs.Stats };

type ReviewerSandboxSnapshot = Map<string, ReviewerSandboxEntrySnapshot>;

type ReviewerSandboxResidueSnapshot =
  | { kind: 'file'; content: Buffer; stat: fs.Stats; maxBytes?: number }
  | { kind: 'symlink'; linkTarget: string; stat: fs.Stats }
  | { kind: 'directory'; entries: ReviewerSandboxResidueMap; stat: fs.Stats }
  | { kind: 'special'; stat: fs.Stats };

type ReviewerSandboxResidueMap = Map<string, ReviewerSandboxResidueSnapshot>;

type ReviewerSandboxEntryExpectation =
  | { kind: 'file'; content: Buffer; stat?: fs.Stats; maxBytes?: number }
  | { kind: 'directory'; entries: ReviewerSandboxExpectation; stat?: fs.Stats };

type ReviewerSandboxExpectation = ReadonlyMap<string, ReviewerSandboxEntryExpectation>;

interface ReviewerSourceSnapshot {
  directory: PinnedDirectory;
  name: SecureAutoloopIterationArtifact;
  relativePath: string;
  snapshot: RegularChildSnapshot;
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

function reviewerEvidenceLimit(name: string, requested?: number): number | undefined {
  if (requested !== undefined && (!Number.isSafeInteger(requested) || requested < 0)) {
    throw new Error('Autoloop iteration artifact byte limit must be a nonnegative safe integer');
  }
  const mandatory =
    name === 'directive.json' || name === 'eval_output.json' || name === 'coder_summary.txt' || name === 'diff.patch'
      ? REVIEW_EVIDENCE_LIMIT_BYTES
      : undefined;
  if (mandatory === undefined) return requested;
  return requested === undefined ? mandatory : Math.min(mandatory, requested);
}

function reviewerSandboxEvidenceLimit(relativePath: string): number | undefined {
  const match = /^iter-(?:0|[1-9]\d*)\/([^/]+)$/.exec(relativePath);
  return match ? reviewerEvidenceLimit(match[1]) : undefined;
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
    private readonly readOnly: boolean,
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
        false,
      );
      ledger.validateExistingFlatFiles(options.validateExistingFlatFiles);
      return ledger;
    } finally {
      if (run) fs.closeSync(run.fd);
      fs.closeSync(tasks.fd);
    }
  }

  /**
   * Pin an existing foreign run as a strictly read-only capability. Unlike
   * `open`, this path never creates or hardens directories/files: importing
   * evidence must not repair or chmod another run's ledger.
   */
  static openReadOnly(
    workspace: string,
    runId: string,
    options: Omit<SecureAutoloopLedgerOptions, 'create'> = {},
  ): SecureAutoloopLedger {
    validateRunId(runId);
    const flags = normalizedFlags(options.platformFlags);
    const tasksDir = path.join(workspace, 'tasks');
    const runDir = path.join(tasksDir, runId);
    const tasks = this.openDirectory(tasksDir, 'Autoloop ledger tasks parent', false, flags, false);
    let run: { pinned: PinnedDirectory; fd: number; created: boolean } | undefined;
    try {
      run = this.openDirectory(runDir, 'Autoloop ledger run directory', false, flags, false);
      this.assertOpenDirectory(tasks.pinned, tasks.fd);
      const ledger = new SecureAutoloopLedger(
        { ...tasks.pinned, stat: fs.fstatSync(tasks.fd) },
        { ...run.pinned, stat: fs.fstatSync(run.fd) },
        flags,
        options.platform,
        options.logger ?? {},
        options.testHooks ?? {},
        true,
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
        false,
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

  private assertMutable(operation: string): void {
    if (this.readOnly) throw new Error(`Cannot ${operation} through a read-only Autoloop ledger capability`);
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
      this.assertMutable(`create ${label}`);
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
      if (!this.readOnly) SecureAutoloopLedger.hardenDirectory(fd, created);
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
    maxBytes?: number,
  ): RegularChildSnapshot {
    const target = path.join(parent.path, name);
    let fd: number | undefined;
    let verified: RegularChildSnapshot | undefined;
    let primaryFailure: unknown;
    try {
      try {
        const observed = lstatIfPresent(target);
        if (!observed || observed.isSymbolicLink() || !observed.isFile() || observed.nlink !== 1) {
          if (!observed) {
            throw new Error(`Autoloop nested artifact was removed before its durability barrier: '${relativePath}'`);
          }
          rejectNestedFile(target, 'Autoloop nested artifact', observed);
        }
        fd = fs.openSync(target, fs.constants.O_RDONLY | this.flags.noFollow);
        const opened = fs.fstatSync(fd);
        if (!opened.isFile() || opened.nlink !== 1 || !sameIdentity(expected.stat, opened)) {
          throw new Error(`Autoloop nested artifact identity changed before its durability barrier: '${relativePath}'`);
        }
      } catch (error) {
        throw new SecureAutoloopLedgerCommitError(
          'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          `Autoloop nested artifact committed state is invalid at ${relativePath}: ${errorMessage(error)}`,
          { cause: error, operation: 'secure_nested_artifact_write' },
        );
      }

      try {
        this.syncPinnedDirectory(parent, relativePath);
      } catch (error) {
        throw new SecureAutoloopLedgerCommitError(
          'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
          `Autoloop nested artifact was committed to ${relativePath}, but its parent-directory durability barrier failed: ${errorMessage(error)}`,
          { cause: error, operation: 'secure_nested_artifact_write' },
        );
      }

      try {
        verified = this.openRegularChildSnapshot(parent, name, 'Autoloop nested artifact', 1, maxBytes);
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
          'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
          `Autoloop nested artifact committed state is invalid after the durability barrier at ${relativePath}: ${errorMessage(error)}`,
          { cause: error, operation: 'secure_nested_artifact_write' },
        );
      }
    } catch (error) {
      primaryFailure = error;
    } finally {
      if (fd !== undefined) {
        try {
          (this.testHooks.closeDescriptor ?? fs.closeSync)(fd);
        } catch (error) {
          if (primaryFailure) {
            const secondary = error instanceof Error ? error : new Error(String(error));
            if (primaryFailure instanceof SecureAutoloopLedgerCommitError) {
              primaryFailure.secondaryErrors.push(secondary);
            }
            this.logger.warn?.(
              `[autoloop] nested artifact descriptor close failed after the primary error: ${errorMessage(error)}`,
            );
          } else {
            primaryFailure = new SecureAutoloopLedgerCommitError(
              'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE',
              `Autoloop nested artifact was committed to ${relativePath}, but its verification descriptor close failed: ${errorMessage(error)}`,
              { cause: error, operation: 'secure_nested_artifact_write' },
            );
          }
        }
      }
    }
    if (primaryFailure) throw primaryFailure;
    if (!verified) throw new Error(`Autoloop nested artifact verification produced no snapshot: '${relativePath}'`);
    return verified;
  }

  private syncCommittedReviewerSandbox(
    sandbox: PinnedDirectory,
    iter: number,
    expected: ReviewerSandboxSnapshot,
    assertAuthoritativeSources: () => void,
  ): void {
    try {
      this.syncPinnedDirectory(sandbox, `Reviewer sandbox iteration ${iter}`);
    } catch (error) {
      throw new SecureAutoloopLedgerCommitError(
        'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        `Reviewer sandbox iteration ${iter} was staged, but its final parent-directory durability barrier failed: ${errorMessage(error)}`,
        { cause: error, operation: 'secure_reviewer_sandbox_stage' },
      );
    }
    try {
      this.assertReviewerSandboxMatches(sandbox, expected, 'Reviewer sandbox final durability barrier');
      assertAuthoritativeSources();
    } catch (error) {
      throw new SecureAutoloopLedgerCommitError(
        'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
        `Reviewer sandbox iteration ${iter} committed state is invalid after its final durability barrier: ${errorMessage(error)}`,
        { cause: error, operation: 'secure_reviewer_sandbox_stage' },
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
    expectedLinkCount = 1,
    maxBytes?: number,
  ): RegularChildSnapshot | undefined {
    if (maxBytes !== undefined && (!Number.isSafeInteger(maxBytes) || maxBytes < 0)) {
      throw new Error(`${label} byte limit must be a nonnegative safe integer`);
    }
    validatePathComponent(name, label);
    this.assertIdentity();
    this.assertPinnedDirectory(parent);
    const target = path.join(parent.path, name);
    const observed = lstatIfPresent(target);
    if (!observed) return undefined;
    if (observed.isSymbolicLink() || !observed.isFile() || observed.nlink !== expectedLinkCount) {
      rejectNestedFile(target, label, observed);
    }
    this.testHooks.afterNestedChildLstat?.({ filePath: target, label });
    const fd = fs.openSync(target, fs.constants.O_RDONLY | this.flags.noFollow);
    try {
      const opened = fs.fstatSync(fd);
      if (opened.isFile() && !sameIdentity(observed, opened)) {
        throw new Error(`${label} identity changed between lstat and open: '${target}'`);
      }
      if (!opened.isFile() || opened.nlink !== expectedLinkCount) {
        rejectNestedFile(target, label, opened);
      }
      this.assertPinnedDirectory(parent);
      if (maxBytes !== undefined && opened.size > maxBytes) {
        throw new Error(`${label} '${name}' exceeds the ${maxBytes}-byte limit`);
      }
      this.testHooks.beforeNestedChildContentRead?.({ filePath: target, label, fd, size: opened.size });
      const content =
        maxBytes === undefined
          ? fs.readFileSync(fd)
          : this.readBoundedRegularChildContent(fd, target, name, label, opened.size, maxBytes);
      const afterOpen = fs.fstatSync(fd);
      const current = lstatIfPresent(target);
      if (current?.isFile() && !sameIdentity(afterOpen, current)) {
        throw new Error(`${label} identity changed while it was being read: '${target}'`);
      }
      if (
        !current ||
        current.isSymbolicLink() ||
        !current.isFile() ||
        current.nlink !== expectedLinkCount ||
        afterOpen.nlink !== expectedLinkCount ||
        !sameIdentity(opened, afterOpen) ||
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

  private readBoundedRegularChildContent(
    fd: number,
    filePath: string,
    name: string,
    label: string,
    openedSize: number,
    maxBytes: number,
  ): Buffer {
    const content = Buffer.alloc(openedSize);
    let offset = 0;
    while (offset < openedSize) {
      const length = openedSize - offset;
      this.testHooks.beforeNestedChildDescriptorRead?.({
        filePath,
        label,
        fd,
        phase: 'content',
        bufferLength: content.length,
        offset,
        length,
      });
      const bytesRead = fs.readSync(fd, content, offset, length, null);
      if (bytesRead === 0) break;
      offset += bytesRead;
    }

    const growthProbe = Buffer.alloc(1);
    this.testHooks.beforeNestedChildDescriptorRead?.({
      filePath,
      label,
      fd,
      phase: 'growth-probe',
      bufferLength: growthProbe.length,
      offset: 0,
      length: 1,
    });
    if (fs.readSync(fd, growthProbe, 0, 1, null) !== 0) {
      const boundary = openedSize === maxBytes ? `the ${maxBytes}-byte limit` : `its ${openedSize}-byte opened size`;
      throw new Error(`${label} '${name}' grew beyond ${boundary} while it was being read`);
    }
    if (offset !== openedSize) {
      throw new Error(
        `${label} '${name}' became shorter than its ${openedSize}-byte opened size while it was being read`,
      );
    }
    return content;
  }

  private openRegularChild(
    parent: PinnedDirectory,
    name: string,
    label: string,
    maxBytes?: number,
  ): Buffer | undefined {
    return this.openRegularChildSnapshot(parent, name, label, 1, maxBytes)?.content;
  }

  private isInternalNestedTemporaryName(name: string, entry: string): boolean {
    const prefix = `.${name}.tmp-`;
    if (!entry.startsWith(prefix)) return false;
    return /^(0|[1-9]\d*)-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
      entry.slice(prefix.length),
    );
  }

  private committedNestedArtifactError(relativePath: string, error: unknown): SecureAutoloopLedgerCommitError {
    if (error instanceof SecureAutoloopLedgerCommitError) return error;
    return new SecureAutoloopLedgerCommitError(
      'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
      `Autoloop nested artifact committed state is invalid at ${relativePath}: ${errorMessage(error)}`,
      { cause: error, operation: 'secure_nested_artifact_write' },
    );
  }

  private unlinkNestedTemporary(temporaryPath: string): void {
    (this.testHooks.unlinkNestedTemporary ?? fs.unlinkSync)(temporaryPath);
  }

  private unlinkOwnedNestedTemporary(
    temporaryPath: string,
    expectedIdentity: fs.Stats | undefined,
  ): 'removed' | 'missing' | 'foreign' {
    const observed = lstatIfPresent(temporaryPath);
    if (!observed) return 'missing';
    if (
      !expectedIdentity ||
      observed.isSymbolicLink() ||
      !observed.isFile() ||
      !sameIdentity(expectedIdentity, observed)
    ) {
      return 'foreign';
    }
    this.unlinkNestedTemporary(temporaryPath);
    if (lstatIfPresent(temporaryPath)) {
      throw new Error(`Autoloop temporary artifact remained after unlink: '${temporaryPath}'`);
    }
    return 'removed';
  }

  private openExpectedNestedAlias(
    parent: PinnedDirectory,
    name: string,
    label: string,
    expectedIdentity: fs.Stats,
    expectedBytes: Buffer,
    expectedLinkCount: number,
    maxBytes?: number,
  ): RegularChildSnapshot {
    const snapshot = this.openRegularChildSnapshot(parent, name, label, expectedLinkCount, maxBytes);
    if (!snapshot) throw new Error(`${label} was removed before it could be verified`);
    if (!sameIdentity(expectedIdentity, snapshot.stat)) {
      throw new Error(`${label} identity changed before it could be verified`);
    }
    if (!snapshot.content.equals(expectedBytes)) {
      throw new Error(`${label} contents changed before it could be verified`);
    }
    return snapshot;
  }

  private verifyPublishedNestedAliases(
    parent: PinnedDirectory,
    name: string,
    temporaryName: string,
    relativePath: string,
    expectedIdentity: fs.Stats,
    expectedBytes: Buffer,
    maxBytes?: number,
  ): RegularChildSnapshot {
    const targetSnapshot = this.openExpectedNestedAlias(
      parent,
      name,
      `Autoloop published nested artifact '${relativePath}'`,
      expectedIdentity,
      expectedBytes,
      2,
      maxBytes,
    );
    const temporarySnapshot = this.openExpectedNestedAlias(
      parent,
      temporaryName,
      `Autoloop published temporary alias '${relativePath}'`,
      expectedIdentity,
      expectedBytes,
      2,
      maxBytes,
    );
    if (!sameIdentity(targetSnapshot.stat, temporarySnapshot.stat)) {
      throw new Error(`Autoloop published aliases do not share one identity: '${relativePath}'`);
    }
    return targetSnapshot;
  }

  private reconcileExistingAtomicChild(
    parent: PinnedDirectory,
    name: string,
    bytes: Buffer,
    relativePath: string,
    maxBytes?: number,
  ): AtomicChildWriteResult | undefined {
    const target = path.join(parent.path, name);
    const observed = lstatIfPresent(target);
    if (!observed) return undefined;
    if (observed.isSymbolicLink() || !observed.isFile()) {
      rejectNestedFile(target, 'Autoloop nested artifact', observed);
    }

    if (observed.nlink === 1) {
      const existing = this.openRegularChildSnapshot(parent, name, 'Autoloop nested artifact', 1, maxBytes);
      if (!existing?.content.equals(bytes)) {
        throw new Error(`Refusing to overwrite conflicting immutable Autoloop artifact '${target}'`);
      }
      return {
        outcome: 'unchanged',
        snapshot: this.syncCommittedNestedArtifact(parent, name, relativePath, existing, maxBytes),
      };
    }

    if (observed.nlink !== 2) rejectNestedFile(target, 'Autoloop nested artifact', observed);
    this.assertIdentity();
    this.assertPinnedDirectory(parent);
    const aliases = fs.readdirSync(parent.path).filter((entry) => this.isInternalNestedTemporaryName(name, entry));
    this.assertPinnedDirectory(parent);
    if (aliases.length !== 1) rejectNestedFile(target, 'Autoloop nested artifact', observed);

    const aliasName = aliases[0];
    const aliasPath = path.join(parent.path, aliasName);
    const aliasObserved = lstatIfPresent(aliasPath);
    if (!aliasObserved?.isFile() || aliasObserved.nlink !== 2 || !sameIdentity(observed, aliasObserved)) {
      rejectNestedFile(target, 'Autoloop nested artifact', observed);
    }
    const targetSnapshot = this.openRegularChildSnapshot(
      parent,
      name,
      'Autoloop published nested artifact',
      2,
      maxBytes,
    );
    const aliasSnapshot = this.openRegularChildSnapshot(
      parent,
      aliasName,
      'Autoloop published temporary alias',
      2,
      maxBytes,
    );
    if (
      !targetSnapshot ||
      !aliasSnapshot ||
      !sameIdentity(targetSnapshot.stat, aliasSnapshot.stat) ||
      !targetSnapshot.content.equals(aliasSnapshot.content)
    ) {
      throw new Error(`Refusing to reconcile an unproven Autoloop temporary alias '${aliasPath}'`);
    }
    if (!targetSnapshot.content.equals(bytes)) {
      throw new Error(`Refusing to overwrite conflicting immutable Autoloop artifact '${target}'`);
    }

    try {
      this.unlinkNestedTemporary(aliasPath);
      this.testHooks.afterNestedTemporaryUnlink?.({
        relativePath,
        filePath: target,
        temporaryPath: aliasPath,
      });
      const reconciled = this.openRegularChildSnapshot(
        parent,
        name,
        'Autoloop reconciled nested artifact',
        1,
        maxBytes,
      );
      if (!reconciled || !sameIdentity(targetSnapshot.stat, reconciled.stat) || !reconciled.content.equals(bytes)) {
        throw new Error(`Autoloop published temporary alias reconciliation was incomplete: '${relativePath}'`);
      }
      return {
        outcome: 'unchanged',
        snapshot: this.syncCommittedNestedArtifact(parent, name, relativePath, reconciled, maxBytes),
      };
    } catch (error) {
      throw this.committedNestedArtifactError(relativePath, error);
    }
  }

  private writeAtomicChild(
    parent: PinnedDirectory,
    name: string,
    content: string | Buffer,
    relativePath: string,
    operation: SecureLedgerNestedMutationEvent['operation'],
    maxBytes?: number,
  ): AtomicChildWriteResult {
    validatePathComponent(name, 'Autoloop nested artifact');
    const target = path.join(parent.path, name);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content, 'utf8');
    if (maxBytes !== undefined && bytes.length > maxBytes) {
      throw new Error(`Autoloop nested artifact '${name}' exceeds the ${maxBytes}-byte limit`);
    }
    const existing = this.reconcileExistingAtomicChild(parent, name, bytes, relativePath, maxBytes);
    if (existing) {
      return existing;
    }

    this.testHooks.beforeNestedMutation?.({ operation: 'artifact-write', relativePath, filePath: target });
    this.assertIdentity();
    this.assertPinnedDirectory(parent);

    const temporaryName = `.${name}.tmp-${process.pid}-${randomUUID()}`;
    const temporary = path.join(parent.path, temporaryName);
    let fd: number | undefined;
    let temporaryCreated = false;
    let published = false;
    let temporaryCleanupAttempted = false;
    let temporaryIdentity: fs.Stats | undefined;
    try {
      fd = fs.openSync(
        temporary,
        fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | this.flags.noFollow,
        PRIVATE_FILE_MODE,
      );
      temporaryCreated = true;
      const temporaryIoEvent = { relativePath, filePath: target, temporaryPath: temporary, fd };
      this.testHooks.beforeNestedTemporaryIo?.({ ...temporaryIoEvent, phase: 'after-create' });
      const opened = fs.fstatSync(fd);
      if (!opened.isFile() || opened.nlink !== 1) rejectNestedFile(temporary, 'Autoloop temporary artifact', opened);
      let offset = 0;
      this.testHooks.beforeNestedTemporaryIo?.({ ...temporaryIoEvent, phase: 'before-write' });
      while (offset < bytes.length) {
        const written = fs.writeSync(fd, bytes, offset, bytes.length - offset, null);
        if (written <= 0) throw new Error(`Could not write complete Autoloop artifact '${target}'`);
        offset += written;
      }
      this.testHooks.beforeNestedTemporaryIo?.({ ...temporaryIoEvent, phase: 'before-flush' });
      fs.fsyncSync(fd);
      temporaryIdentity = fs.fstatSync(fd);
      const temporaryFd = fd;
      fd = undefined;
      (this.testHooks.closeNestedTemporary ?? fs.closeSync)(temporaryFd);

      this.testHooks.beforeNestedMutation?.({ operation, relativePath, filePath: target });
      this.assertIdentity();
      this.assertPinnedDirectory(parent);
      const publishEvent = { relativePath, filePath: target, temporaryPath: temporary };
      this.testHooks.beforeNestedPublish?.(publishEvent);
      this.assertIdentity();
      this.assertPinnedDirectory(parent);
      if (!temporaryIdentity) {
        throw new Error(`Autoloop temporary artifact identity was not captured: '${relativePath}'`);
      }
      this.openExpectedNestedAlias(
        parent,
        temporaryName,
        `Autoloop temporary artifact '${relativePath}'`,
        temporaryIdentity,
        bytes,
        1,
        maxBytes,
      );
      try {
        (this.testHooks.publishNestedTemporary ?? fs.linkSync)(temporary, target);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
        temporaryCleanupAttempted = true;
        const cleanup = this.unlinkOwnedNestedTemporary(temporary, temporaryIdentity);
        if (cleanup !== 'removed') {
          throw new Error(
            `Autoloop temporary artifact could not be safely removed after publish conflict: '${relativePath}'`,
          );
        }
        temporaryCreated = false;
        const raced = this.reconcileExistingAtomicChild(parent, name, bytes, relativePath, maxBytes);
        if (!raced) {
          throw new Error(`Autoloop nested artifact disappeared after exclusive publish conflict: '${relativePath}'`);
        }
        return raced;
      }
      published = true;
      this.testHooks.afterNestedPublish?.(publishEvent);
      this.verifyPublishedNestedAliases(parent, name, temporaryName, relativePath, temporaryIdentity, bytes, maxBytes);
      const cleanup = this.unlinkOwnedNestedTemporary(temporary, temporaryIdentity);
      if (cleanup !== 'removed') {
        throw new Error(`Autoloop published temporary alias could not be safely removed: '${relativePath}'`);
      }
      temporaryCreated = false;
      this.testHooks.afterNestedTemporaryUnlink?.(publishEvent);
      const committed = this.openRegularChildSnapshot(parent, name, 'Autoloop nested artifact', 1, maxBytes);
      if (
        !committed ||
        !temporaryIdentity ||
        !sameIdentity(temporaryIdentity, committed.stat) ||
        !committed.content.equals(bytes)
      ) {
        throw new Error(`Autoloop artifact commit was incomplete: '${target}'`);
      }
      return {
        outcome: 'created',
        snapshot: this.syncCommittedNestedArtifact(parent, name, relativePath, committed, maxBytes),
      };
    } catch (error) {
      if (published) throw this.committedNestedArtifactError(relativePath, error);
      throw error;
    } finally {
      if (fd !== undefined) {
        if (!temporaryIdentity) {
          try {
            const incompleteIdentity = fs.fstatSync(fd);
            if (incompleteIdentity.isFile()) temporaryIdentity = incompleteIdentity;
          } catch (error) {
            this.logger.warn?.(`[autoloop] failed to identify incomplete nested artifact: ${errorMessage(error)}`);
          }
        }
        try {
          fs.closeSync(fd);
        } catch (error) {
          this.logger.warn?.(`[autoloop] failed to close incomplete nested artifact: ${errorMessage(error)}`);
        }
      }
      if (!published && temporaryCreated && !temporaryCleanupAttempted) {
        try {
          const cleanup = this.unlinkOwnedNestedTemporary(temporary, temporaryIdentity);
          if (cleanup === 'missing') {
            this.logger.warn?.(
              `[autoloop] incomplete nested artifact '${relativePath}' could not be located during cleanup`,
            );
          } else if (cleanup === 'foreign') {
            this.logger.warn?.(
              `[autoloop] preserved an unowned incomplete nested artifact at '${temporary}' during cleanup`,
            );
          }
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
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

  readIterationArtifact(iter: number, name: SecureAutoloopIterationArtifact, maxBytes?: number): Buffer | undefined {
    validateIteration(iter);
    validateIterationArtifact(name);
    let directory: PinnedDirectory;
    try {
      directory = this.getIterationDirectory(iter, false);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
      throw error;
    }
    return this.openRegularChild(
      directory,
      name,
      `Autoloop iteration ${iter} artifact`,
      reviewerEvidenceLimit(name, maxBytes),
    );
  }

  private snapshotReviewerSource(
    iter: number,
    name: SecureAutoloopIterationArtifact,
  ): ReviewerSourceSnapshot | undefined {
    validateIteration(iter);
    validateIterationArtifact(name);
    let directory: PinnedDirectory;
    try {
      directory = this.getIterationDirectory(iter, false);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined;
      throw error;
    }
    const snapshot = this.openRegularChildSnapshot(
      directory,
      name,
      `Reviewer authoritative source iter/${iter}/${name}`,
      1,
      reviewerEvidenceLimit(name),
    );
    if (!snapshot) return undefined;
    return { directory, name, relativePath: `iter/${iter}/${name}`, snapshot };
  }

  private assertReviewerSourcesMatch(sources: readonly ReviewerSourceSnapshot[]): void {
    for (const source of sources) {
      const observed = this.openRegularChildSnapshot(
        source.directory,
        source.name,
        `Reviewer authoritative source ${source.relativePath}`,
        1,
        reviewerEvidenceLimit(source.name),
      );
      if (!observed) throw new Error(`Reviewer authoritative source was removed: '${source.relativePath}'`);
      if (!sameIdentity(observed.stat, source.snapshot.stat)) {
        throw new Error(`Reviewer authoritative source identity changed: '${source.relativePath}'`);
      }
      if (!observed.content.equals(source.snapshot.content)) {
        throw new Error(`Reviewer authoritative source contents changed: '${source.relativePath}'`);
      }
    }
  }

  writeIterationArtifact(
    iter: number,
    name: SecureAutoloopIterationArtifact,
    content: string | Buffer,
  ): 'created' | 'unchanged' {
    validateIteration(iter);
    validateIterationArtifact(name);
    this.assertMutable(`write iter/${iter}/${name}`);
    const directory = this.getIterationDirectory(iter, true);
    return this.writeAtomicChild(
      directory,
      name,
      content,
      `iter/${iter}/${name}`,
      'artifact-commit',
      reviewerEvidenceLimit(name),
    ).outcome;
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
    this.assertMutable('create or validate the Reviewer sandbox');
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

  private readReviewerSandboxEntries(directory: PinnedDirectory): string[] {
    this.assertIdentity();
    this.assertPinnedDirectory(directory);
    const entries = fs.readdirSync(directory.path).sort();
    for (const entry of entries) validatePathComponent(entry, 'Reviewer sandbox entry');
    this.assertPinnedDirectory(directory);
    return entries;
  }

  private assertReviewerSandboxResidueNode(
    parent: PinnedDirectory,
    name: string,
    expected: ReviewerSandboxResidueSnapshot,
    label: string,
  ): fs.Stats {
    validatePathComponent(name, 'Reviewer sandbox residue');
    this.assertIdentity();
    this.assertPinnedDirectory(parent);
    const target = path.join(parent.path, name);
    const observed = lstatIfPresent(target);
    if (!observed) throw new Error(`${label} membership changed unexpectedly; '${target}' was removed`);
    if (observed.dev !== parent.stat.dev) {
      throw new Error(`${label} crossed a filesystem boundary at '${target}'`);
    }
    if (
      !sameIdentity(observed, expected.stat) ||
      (observed.mode & fs.constants.S_IFMT) !== (expected.stat.mode & fs.constants.S_IFMT) ||
      (expected.kind !== 'directory' && observed.nlink !== expected.stat.nlink)
    ) {
      if (observed.isSymbolicLink()) throw new Error(`Refusing ${label} symbolic link '${target}'`);
      if (observed.isFile() && observed.nlink > 1) {
        throw new Error(`Refusing ${label} hardlink with link count ${observed.nlink} at '${target}'`);
      }
      throw new Error(`${label} type or identity changed unexpectedly: '${target}'`);
    }
    this.assertPinnedDirectory(parent);
    return observed;
  }

  private snapshotReviewerSandboxResidue(
    parent: PinnedDirectory,
    name: string,
    label: string,
    relativePath = name,
  ): ReviewerSandboxResidueSnapshot {
    validatePathComponent(name, 'Reviewer sandbox residue');
    const target = path.join(parent.path, name);
    const observed = lstatIfPresent(target);
    if (!observed) throw new Error(`${label} membership changed unexpectedly; '${target}' was removed`);
    if (observed.dev !== parent.stat.dev) {
      throw new Error(`${label} crossed a filesystem boundary at '${target}'`);
    }
    if (observed.isFile()) {
      const maxBytes = reviewerSandboxEvidenceLimit(relativePath);
      const file = this.openRegularChildSnapshot(parent, name, `${label} regular file`, observed.nlink, maxBytes);
      if (!file || !sameIdentity(observed, file.stat)) {
        throw new Error(`${label} regular file identity changed unexpectedly: '${target}'`);
      }
      return { kind: 'file', content: Buffer.from(file.content), stat: file.stat, maxBytes };
    }
    if (observed.isSymbolicLink()) {
      const linkTarget = fs.readlinkSync(target);
      const current = this.assertReviewerSandboxResidueNode(
        parent,
        name,
        { kind: 'symlink', linkTarget, stat: observed },
        label,
      );
      return { kind: 'symlink', linkTarget, stat: current };
    }
    if (observed.isDirectory()) {
      const child = this.openPinnedChildDirectory(parent, name, `${label} directory`, false);
      if (!sameIdentity(observed, child.stat) || child.stat.dev !== parent.stat.dev) {
        throw new Error(`${label} directory identity or filesystem changed unexpectedly: '${target}'`);
      }
      const entries: ReviewerSandboxResidueMap = new Map();
      for (const entry of this.readReviewerSandboxEntries(child)) {
        entries.set(
          entry,
          this.snapshotReviewerSandboxResidue(child, entry, `${label}/${entry}`, `${relativePath}/${entry}`),
        );
      }
      this.assertPinnedDirectory(child);
      this.assertPinnedDirectory(parent);
      return { kind: 'directory', entries, stat: child.stat };
    }
    const current = this.assertReviewerSandboxResidueNode(parent, name, { kind: 'special', stat: observed }, label);
    return { kind: 'special', stat: current };
  }

  private assertReviewerSandboxResidueMatches(
    parent: PinnedDirectory,
    name: string,
    expected: ReviewerSandboxResidueSnapshot,
    label: string,
  ): void {
    const target = path.join(parent.path, name);
    this.assertReviewerSandboxResidueNode(parent, name, expected, label);
    if (expected.kind === 'file') {
      const file = this.openRegularChildSnapshot(
        parent,
        name,
        `${label} regular file`,
        expected.stat.nlink,
        expected.maxBytes,
      );
      if (!file || !sameIdentity(file.stat, expected.stat)) {
        throw new Error(`${label} regular file identity changed unexpectedly: '${target}'`);
      }
      if (!file.content.equals(expected.content)) {
        throw new Error(`${label} regular file contents changed unexpectedly: '${target}'`);
      }
      return;
    }
    if (expected.kind === 'symlink') {
      if (fs.readlinkSync(target) !== expected.linkTarget) {
        throw new Error(`${label} symbolic link target changed unexpectedly: '${target}'`);
      }
      this.assertReviewerSandboxResidueNode(parent, name, expected, label);
      return;
    }
    if (expected.kind === 'special') return;

    const child = this.openPinnedChildDirectory(parent, name, `${label} directory`, false);
    if (!sameIdentity(child.stat, expected.stat) || child.stat.dev !== parent.stat.dev) {
      throw new Error(`${label} directory identity or filesystem changed unexpectedly: '${target}'`);
    }
    const observedEntries = this.readReviewerSandboxEntries(child);
    const expectedEntries = [...expected.entries.keys()].sort();
    if (
      observedEntries.length !== expectedEntries.length ||
      observedEntries.some((entry, index) => entry !== expectedEntries[index])
    ) {
      throw new Error(
        `${label} membership changed unexpectedly; expected [${expectedEntries.join(', ')}], found [${observedEntries.join(', ')}]`,
      );
    }
    for (const entry of observedEntries) {
      this.assertReviewerSandboxResidueMatches(child, entry, expected.entries.get(entry)!, `${label}/${entry}`);
    }
    this.assertPinnedDirectory(child);
    this.assertPinnedDirectory(parent);
  }

  private removeReviewerSandboxResidue(
    parent: PinnedDirectory,
    name: string,
    expected: ReviewerSandboxResidueSnapshot,
    relativePath: string,
  ): void {
    const target = path.join(parent.path, name);
    this.assertReviewerSandboxResidueMatches(parent, name, expected, `Reviewer sandbox reset seam/${relativePath}`);
    if (expected.kind === 'directory') {
      const child = this.openPinnedChildDirectory(
        parent,
        name,
        `Reviewer sandbox residue directory '${relativePath}'`,
        false,
      );
      if (!sameIdentity(child.stat, expected.stat) || child.stat.dev !== parent.stat.dev) {
        throw new Error(`Reviewer sandbox residue directory identity changed unexpectedly: '${target}'`);
      }
      for (const [entry, childExpected] of expected.entries) {
        this.removeReviewerSandboxResidue(child, entry, childExpected, `${relativePath}/${entry}`);
      }
      if (this.readReviewerSandboxEntries(child).length !== 0) {
        throw new Error(`Reviewer sandbox residue directory membership changed unexpectedly: '${target}'`);
      }
      this.syncPinnedDirectory(child, `Reviewer sandbox residue directory '${relativePath}' cleanup`);
      this.testHooks.beforeReviewerSandboxResidueRemoval?.({
        relativePath,
        filePath: target,
        kind: 'directory',
      });
      this.assertReviewerSandboxResidueNode(parent, name, expected, `Reviewer sandbox residue '${relativePath}'`);
      this.assertPinnedDirectory(child);
      fs.rmdirSync(target);
    } else {
      this.testHooks.beforeReviewerSandboxResidueRemoval?.({
        relativePath,
        filePath: target,
        kind: expected.kind,
      });
      this.assertReviewerSandboxResidueMatches(parent, name, expected, `Reviewer sandbox residue '${relativePath}'`);
      fs.unlinkSync(target);
    }
    if (lstatIfPresent(target)) {
      throw new Error(`Reviewer sandbox residue remained after removal: '${target}'`);
    }
    this.syncPinnedDirectory(parent, `Reviewer sandbox residue '${relativePath}' removal`);
    this.assertPinnedDirectory(parent);
  }

  private snapshotReviewerSandboxForReset(sandbox: PinnedDirectory): {
    persistent: ReviewerSandboxSnapshot;
    removable: ReviewerSandboxResidueMap;
  } {
    const persistentNames = new Set<SecureAutoloopReviewerPersistentFile>(['reviewer_memory.md', 'reviewer_log.jsonl']);
    const persistent: ReviewerSandboxSnapshot = new Map();
    const removable: ReviewerSandboxResidueMap = new Map();
    for (const entry of this.readReviewerSandboxEntries(sandbox)) {
      if (entry.startsWith('iter-')) {
        const match = /^iter-(0|[1-9]\d*)$/.exec(entry);
        if (!match) throw new Error(`Refusing unsafe Reviewer sandbox iteration alias '${entry}'`);
        validateIteration(Number(match[1]));
        const target = path.join(sandbox.path, entry);
        const observed = lstatIfPresent(target);
        if (!observed) throw new Error(`Reviewer sandbox iteration alias was removed: '${target}'`);
        if (observed.isSymbolicLink()) {
          throw new Error(`Refusing unsafe Reviewer sandbox symbolic link '${target}'`);
        }
        if (!observed.isDirectory()) {
          throw new Error(`Refusing non-directory Reviewer sandbox iteration alias '${target}'`);
        }
      }
      if (persistentNames.has(entry as SecureAutoloopReviewerPersistentFile)) {
        const file = this.openRegularChildSnapshot(sandbox, entry, 'Reviewer persistent file');
        if (!file) throw new Error(`Reviewer persistent file was removed while being secured: '${entry}'`);
        persistent.set(entry, { kind: 'file', content: Buffer.from(file.content), stat: file.stat });
        continue;
      }
      removable.set(entry, this.snapshotReviewerSandboxResidue(sandbox, entry, 'Reviewer sandbox before reset'));
    }
    this.assertPinnedDirectory(sandbox);
    return { persistent, removable };
  }

  private assertReviewerSandboxResetSnapshot(
    sandbox: PinnedDirectory,
    persistent: ReviewerSandboxSnapshot,
    removable: ReviewerSandboxResidueMap,
    label: string,
  ): void {
    const observedEntries = this.readReviewerSandboxEntries(sandbox);
    const expectedEntries = [...persistent.keys(), ...removable.keys()].sort();
    if (
      observedEntries.length !== expectedEntries.length ||
      observedEntries.some((entry, index) => entry !== expectedEntries[index])
    ) {
      throw new Error(
        `${label} membership changed unexpectedly; expected [${expectedEntries.join(', ')}], found [${observedEntries.join(', ')}]`,
      );
    }
    for (const entry of observedEntries) {
      const persistentEntry = persistent.get(entry);
      if (persistentEntry) {
        if (persistentEntry.kind !== 'file') {
          throw new Error(`${label} persistent entry was not a regular file: '${path.join(sandbox.path, entry)}'`);
        }
        const file = this.openRegularChildSnapshot(sandbox, entry, `${label} expected regular file`);
        if (!file) throw new Error(`${label} expected regular file was removed: '${path.join(sandbox.path, entry)}'`);
        if (!sameIdentity(file.stat, persistentEntry.stat)) {
          throw new Error(`${label} regular file identity changed unexpectedly: '${path.join(sandbox.path, entry)}'`);
        }
        if (!file.content.equals(persistentEntry.content)) {
          throw new Error(`${label} regular file contents changed unexpectedly: '${path.join(sandbox.path, entry)}'`);
        }
        continue;
      }
      this.assertReviewerSandboxResidueMatches(sandbox, entry, removable.get(entry)!, label);
    }
    this.assertPinnedDirectory(sandbox);
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
        const file = this.openRegularChildSnapshot(
          directory,
          entry,
          `${label} expected regular file`,
          1,
          expectedEntry.maxBytes,
        );
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
      if (!sameIdentity(child.stat, expectedEntry.stat)) {
        throw new Error(`${label} directory identity changed unexpectedly: '${target}'`);
      }
      this.assertReviewerSandboxMatches(child, expectedEntry.entries, `${label}/${entry}`);
    }
    this.assertPinnedDirectory(directory);
  }

  private validateAndSnapshotReviewerSandbox(
    directory: PinnedDirectory,
    expected: ReviewerSandboxExpectation,
    label: string,
    relativeDirectory = '',
  ): ReviewerSandboxSnapshot {
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
    const snapshot: ReviewerSandboxSnapshot = new Map();
    for (const entry of observed) {
      const expectedEntry = expected.get(entry)!;
      const target = path.join(directory.path, entry);
      const relativePath = relativeDirectory ? `${relativeDirectory}/${entry}` : entry;
      if (expectedEntry.kind === 'file') {
        const file = this.openRegularChildSnapshot(
          directory,
          entry,
          `${label} expected regular file`,
          1,
          expectedEntry.maxBytes,
        );
        if (!file) throw new Error(`${label} expected regular file was removed: '${target}'`);
        if (expectedEntry.stat && !sameIdentity(file.stat, expectedEntry.stat)) {
          throw new Error(`${label} regular file identity changed unexpectedly: '${target}'`);
        }
        if (!file.content.equals(expectedEntry.content)) {
          throw new Error(`${label} regular file contents changed unexpectedly: '${target}'`);
        }
        this.testHooks.afterReviewerSandboxEntryRead?.({ relativePath, filePath: target, kind: 'file' });
        snapshot.set(entry, {
          kind: 'file',
          content: Buffer.from(file.content),
          stat: file.stat,
          maxBytes: expectedEntry.maxBytes,
        });
        continue;
      }
      const child = this.openPinnedChildDirectory(directory, entry, `${label} expected directory`, false);
      if (expectedEntry.stat && !sameIdentity(child.stat, expectedEntry.stat)) {
        throw new Error(`${label} directory identity changed unexpectedly: '${target}'`);
      }
      this.testHooks.afterReviewerSandboxEntryRead?.({ relativePath, filePath: target, kind: 'directory' });
      snapshot.set(entry, {
        kind: 'directory',
        entries: this.validateAndSnapshotReviewerSandbox(
          child,
          expectedEntry.entries,
          `${label}/${entry}`,
          relativePath,
        ),
        stat: child.stat,
      });
    }
    this.assertPinnedDirectory(directory);
    return snapshot;
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
      const expectedArtifacts = new Map<string, ReviewerSandboxEntryExpectation>();
      for (const artifact of ['directive.json', 'eval_output.json', 'coder_summary.txt', 'diff.patch'] as const) {
        const content = this.readIterationArtifact(iter, artifact);
        if (content === undefined) {
          throw new Error(`Reviewer staged iteration ${iter} has no authoritative iter/${iter}/${artifact}`);
        }
        expectedArtifacts.set(artifact, { kind: 'file', content, maxBytes: reviewerEvidenceLimit(artifact) });
      }
      this.validateAndSnapshotReviewerSandbox(staged, expectedArtifacts, `Reviewer staged iteration ${iter}`);
      stagedIterationSeen = true;
    }
    this.assertPinnedDirectory(sandbox);
  }

  stageReviewerSandbox(iter: number, controls: SecureReviewerControlFiles = {}): SecureReviewerStageResult {
    validateIteration(iter);
    this.assertMutable(`stage Reviewer sandbox for iteration ${iter}`);
    const artifacts = new Map<SecureAutoloopIterationArtifact, ReviewerSourceSnapshot>();
    for (const name of ['directive.json', 'eval_output.json', 'coder_summary.txt', 'diff.patch'] as const) {
      const source = this.snapshotReviewerSource(iter, name);
      if (!source) {
        throw new Error(`Reviewer stage requires a complete artifact set; missing iter/${iter}/${name}`);
      }
      artifacts.set(name, source);
    }
    const prior = iter > 0 ? this.snapshotReviewerSource(iter - 1, 'verdict.json') : undefined;
    const authoritativeSources = [...artifacts.values(), ...(prior ? [prior] : [])];

    const plan = controls.plan === undefined ? undefined : Buffer.from(controls.plan);
    const goal = controls.goal === undefined ? undefined : Buffer.from(controls.goal);
    const sandbox = this.getReviewerSandbox(true);
    const { persistent: persistentEntries, removable } = this.snapshotReviewerSandboxForReset(sandbox);

    this.testHooks.beforeNestedMutation?.({
      operation: 'sandbox-reset',
      relativePath: 'reviewer_sandbox',
      filePath: sandbox.path,
    });
    this.assertIdentity();
    this.assertPinnedDirectory(sandbox);
    this.assertReviewerSandboxResetSnapshot(sandbox, persistentEntries, removable, 'Reviewer sandbox reset seam');
    for (const [entry, snapshot] of removable) {
      this.removeReviewerSandboxResidue(sandbox, entry, snapshot, entry);
    }
    this.syncPinnedDirectory(sandbox, 'Reviewer sandbox reset cleanup');
    this.assertPinnedDirectory(sandbox);
    this.assertReviewerSandboxMatches(sandbox, persistentEntries, 'Reviewer sandbox after reset');

    const destination = this.openPinnedChildDirectory(
      sandbox,
      `iter-${iter}`,
      `Reviewer staged iteration ${iter}`,
      true,
    );
    const expectedArtifacts: ReviewerSandboxSnapshot = new Map();
    for (const [name, source] of artifacts) {
      const staged = this.writeAtomicChild(
        destination,
        name,
        source.snapshot.content,
        `reviewer_sandbox/iter-${iter}/${name}`,
        'sandbox-stage',
        reviewerEvidenceLimit(name),
      );
      expectedArtifacts.set(name, { kind: 'file', ...staged.snapshot, maxBytes: reviewerEvidenceLimit(name) });
    }
    const expectedSandboxEntries: ReviewerSandboxSnapshot = new Map(persistentEntries);
    expectedSandboxEntries.set(`iter-${iter}`, {
      kind: 'directory',
      entries: expectedArtifacts,
      stat: destination.stat,
    });
    if (plan) {
      const staged = this.writeAtomicChild(sandbox, 'plan.md', plan, 'reviewer_sandbox/plan.md', 'sandbox-stage');
      expectedSandboxEntries.set('plan.md', { kind: 'file', ...staged.snapshot });
    }
    if (goal) {
      const staged = this.writeAtomicChild(sandbox, 'goal.json', goal, 'reviewer_sandbox/goal.json', 'sandbox-stage');
      expectedSandboxEntries.set('goal.json', { kind: 'file', ...staged.snapshot });
    }

    if (prior) {
      const staged = this.writeAtomicChild(
        sandbox,
        'prior_verdict.json',
        prior.snapshot.content,
        'reviewer_sandbox/prior_verdict.json',
        'sandbox-stage',
      );
      expectedSandboxEntries.set('prior_verdict.json', { kind: 'file', ...staged.snapshot });
    }

    let stagedSnapshot: ReviewerSandboxSnapshot;
    try {
      stagedSnapshot = this.validateAndSnapshotReviewerSandbox(
        sandbox,
        expectedSandboxEntries,
        'Reviewer sandbox authoritative files',
      );
      this.testHooks.beforeNestedMutation?.({
        operation: 'sandbox-stage',
        relativePath: `reviewer_sandbox/iter-${iter}`,
        filePath: destination.path,
      });
      this.assertPinnedDirectory(sandbox);
      this.assertPinnedDirectory(destination);
      this.assertReviewerSandboxMatches(sandbox, stagedSnapshot, 'Reviewer sandbox final stage');
      this.assertReviewerSourcesMatch(authoritativeSources);
    } catch (error) {
      if (error instanceof SecureAutoloopLedgerCommitError) throw error;
      throw new SecureAutoloopLedgerCommitError(
        'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
        `Reviewer sandbox iteration ${iter} committed state is invalid before its final durability barrier: ${errorMessage(error)}`,
        { cause: error, operation: 'secure_reviewer_sandbox_stage' },
      );
    }
    this.syncCommittedReviewerSandbox(sandbox, iter, stagedSnapshot, () => {
      this.assertReviewerSourcesMatch(authoritativeSources);
    });
    return { directory: sandbox.path, priorVerdict: prior !== undefined };
  }

  openFlatFile(name: SecureAutoloopFlatFile, mode: 'read' | 'append', create = false): SecureAutoloopFileHandle {
    if (!SECURE_AUTOLOOP_FLAT_FILES.includes(name)) throw new Error(`Unsupported Autoloop ledger file '${name}'`);
    if (mode === 'read' && create) throw new Error(`Cannot combine read mode with create for '${name}'`);
    if (mode === 'append' || create) this.assertMutable(`open ${name} for append`);
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
      if (!this.readOnly && current !== secure) {
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
    this.assertMutable(`prepare append to ${name}`);
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
    this.assertMutable(`append to ${name}`);
    const handle = this.openFlatFile(name, 'append', true);
    let committed = false;
    let primaryFailure: unknown;
    try {
      try {
        this.beforeFileMutation(handle, 'append');
        const expected = Buffer.byteLength(content);
        const written = fs.writeSync(handle.fd, content, null, 'utf8');
        if (written !== expected) throw new Error(`Could not append the complete ${name} record`);
        committed = true;
        if (durable) {
          this.beforeFileMutation(handle, 'flush');
          fs.fsyncSync(handle.fd);
        }
      } catch (error) {
        primaryFailure =
          committed && durable && !(error instanceof SecureAutoloopLedgerCommitError)
            ? new SecureAutoloopLedgerCommitError(
                'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
                `Autoloop ledger row was committed to ${name}, but its file durability barrier failed: ${errorMessage(error)}`,
                { cause: error },
              )
            : error;
      }
    } finally {
      try {
        (this.testHooks.closeFlatFileDescriptor ?? fs.closeSync)(handle.fd);
      } catch (error) {
        if (primaryFailure) {
          if (primaryFailure instanceof SecureAutoloopLedgerCommitError) {
            primaryFailure.secondaryErrors.push(error instanceof Error ? error : new Error(String(error)));
          }
          this.logger.warn?.(
            `[autoloop] ledger descriptor close failed after the primary error: ${errorMessage(error)}`,
          );
        } else if (committed) {
          primaryFailure = new SecureAutoloopLedgerCommitError(
            'AUTOLOOP_LEDGER_DESCRIPTOR_CLOSE_INCOMPLETE',
            `Autoloop ledger row was committed to ${name}, but its descriptor close failed: ${errorMessage(error)}`,
            { cause: error, operation: 'secure_ledger_append' },
          );
        } else {
          primaryFailure = error;
        }
      }
    }
    if (primaryFailure) throw primaryFailure;
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
    this.assertMutable(`flush ${name}`);
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
    this.assertMutable(`sync ${name ?? 'ledger directory'}`);
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
