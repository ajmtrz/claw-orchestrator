/**
 * A file lock with a bounded wait.
 *
 * Two callers need exactly this — the run store and the ultraapp build queue —
 * and both had their own copy, which is how they also had the same bug twice:
 * a lock that was merely *busy* was reported the same way as a lock we had lost
 * the right to. Those are different facts. Contention is transient and the
 * correct response is to wait; losing ownership is permanent and the correct
 * response is to stop. Collapsing them into one boolean meant a run that hit a
 * millisecond of contention stopped forever while still holding its lease, so
 * nothing else could take it over either.
 *
 * So this returns a result, not a boolean, and it waits before giving up. The
 * critical sections it guards are a few small writes, so real contention is
 * measured in microseconds; the default wait is three orders of magnitude more
 * than that, and reaching the end of it means something is genuinely wrong
 * rather than merely busy.
 */

import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';

export type LockResult<T> =
  | { ok: true; value: T }
  | { ok: false; reason: 'contended'; error: string }
  | { ok: false; reason: 'cleanup_failed'; error: string; cause: Error };

export interface FileLockOptions {
  /** A lock older than this may be recovered only after its recorded owner is proven dead. */
  staleMs?: number;
  /** How long to keep trying before reporting contention. */
  waitMs?: number;
  /**
   * Create the lock file's directory if it is missing.
   *
   * Off by default, and that default matters: locking inside a directory that
   * has been deleted must not recreate it. A run whose teardown raced its own
   * last write came back as an empty directory, which then made the run id
   * permanently unusable — the id looked taken by a run that did not exist.
   */
  createParent?: boolean;
}

export const DEFAULT_LOCK_STALE_MS = 60_000;
export const DEFAULT_LOCK_WAIT_MS = 250;

interface LockOwnerIdentity {
  pid: number;
  processStartTicks?: string;
  bootId?: string;
}

interface FileIdentity {
  dev: number;
  ino: number;
}

/** Test-only seam for platforms that cannot safely inspect contended entries. */
let testPosixInspectionFlagsAvailable: boolean | undefined;

export function __setFileLockPosixInspectionFlagsForTests(available: boolean | undefined): void {
  testPosixInspectionFlagsAvailable = available;
}

function posixInspectionFlags(): { noFollow: number; nonBlocking: number } | undefined {
  if (testPosixInspectionFlagsAvailable === false) return undefined;
  const noFollow = fs.constants.O_NOFOLLOW;
  const nonBlocking = fs.constants.O_NONBLOCK;
  if (typeof noFollow !== 'number' || typeof nonBlocking !== 'number') return undefined;
  return { noFollow, nonBlocking };
}

function sameIdentity(left: FileIdentity, right: FileIdentity): boolean {
  return left.dev === right.dev && left.ino === right.ino;
}

function readSmallUtf8File(filePath: string, maxBytes: number): string {
  const fd = fs.openSync(filePath, 'r');
  try {
    const buffer = Buffer.alloc(maxBytes);
    const bytesRead = fs.readSync(fd, buffer, 0, buffer.length, 0);
    return buffer.subarray(0, bytesRead).toString('utf8');
  } finally {
    fs.closeSync(fd);
  }
}

function linuxProcessIdentity(pid: number): Pick<LockOwnerIdentity, 'processStartTicks' | 'bootId'> | undefined {
  try {
    const stat = readSmallUtf8File(`/proc/${pid}/stat`, 16 * 1024);
    const fields = stat
      .slice(stat.lastIndexOf(')') + 1)
      .trim()
      .split(/\s+/);
    const processStartTicks = fields[19];
    if (!processStartTicks) return undefined;
    const bootId = readSmallUtf8File('/proc/sys/kernel/random/boot_id', 128).trim();
    return { processStartTicks, ...(bootId ? { bootId } : {}) };
  } catch {
    return undefined;
  }
}

function lockOwnerIdentity(): LockOwnerIdentity {
  return { pid: process.pid, ...(linuxProcessIdentity(process.pid) ?? {}) };
}

const MAX_LOCK_OWNER_BYTES = 16 * 1024;

function parseLockOwner(lockPath: string): { owner: LockOwnerIdentity; stat: fs.Stats } | undefined {
  // Treat a lock pathname as hostile while it is contended.  This must pin the
  // inode before parsing: stat/readFile can follow a link, block forever on a
  // FIFO, or inspect a replacement that appeared between the two calls.
  const flags = posixInspectionFlags();
  if (!flags) return undefined;
  let fd: number | undefined;
  try {
    fd = fs.openSync(lockPath, fs.constants.O_RDONLY | flags.noFollow | flags.nonBlocking);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > MAX_LOCK_OWNER_BYTES) return undefined;
    const buffer = Buffer.alloc(Math.min(MAX_LOCK_OWNER_BYTES, stat.size));
    const bytesRead = fs.readSync(fd, buffer, 0, buffer.length, 0);
    // A descriptor may still point at a regular file whose contents changed
    // concurrently.  Parse only the bounded bytes observed through that same
    // descriptor; malformed or short records are simply not recoverable.
    if (bytesRead !== stat.size) return undefined;
    const parsed: unknown = JSON.parse(buffer.subarray(0, bytesRead).toString('utf8'));
    if (
      typeof parsed !== 'object' ||
      parsed === null ||
      Array.isArray(parsed) ||
      !Number.isSafeInteger((parsed as { pid?: unknown }).pid) ||
      (parsed as { pid: number }).pid <= 0
    ) {
      return undefined;
    }
    const owner = parsed as Partial<LockOwnerIdentity>;
    if (
      (owner.processStartTicks !== undefined && typeof owner.processStartTicks !== 'string') ||
      (owner.bootId !== undefined && typeof owner.bootId !== 'string')
    ) {
      return undefined;
    }
    return {
      owner: {
        pid: owner.pid!,
        ...(owner.processStartTicks ? { processStartTicks: owner.processStartTicks } : {}),
        ...(owner.bootId ? { bootId: owner.bootId } : {}),
      },
      stat,
    };
  } catch {
    return undefined;
  } finally {
    if (fd !== undefined) {
      try {
        fs.closeSync(fd);
      } catch {
        // Descriptor close failure cannot make an untrusted owner record valid.
      }
    }
  }
}

function ownerIsProvenDead(owner: LockOwnerIdentity): boolean {
  try {
    process.kill(owner.pid, 0);
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'ESRCH';
  }
  if (!owner.processStartTicks) return false;
  const current = linuxProcessIdentity(owner.pid);
  if (!current?.processStartTicks) return false;
  // Optional fields are evidence only when both records have the field.  A
  // legacy child that predates boot-id recording is still the same live owner
  // when its PID and start ticks match; absence cannot prove it died.
  return (
    current.processStartTicks !== owner.processStartTicks ||
    (owner.bootId !== undefined && current.bootId !== undefined && current.bootId !== owner.bootId)
  );
}

function lockPathExistsSafely(lockPath: string): boolean {
  const flags = posixInspectionFlags();
  if (!flags) {
    // Without POSIX no-follow/nonblocking opens, this is only an existence
    // probe. It never reads, follows, or trusts the object; publication still
    // uses the atomic hard-link below, which rejects any replacement race.
    try {
      fs.lstatSync(lockPath);
      return true;
    } catch (error) {
      // Only proven absence admits the atomic publication attempt. Permission,
      // reparse, and every other lookup failure remain contended.
      return (error as NodeJS.ErrnoException).code !== 'ENOENT';
    }
  }
  let fd: number | undefined;
  try {
    fd = fs.openSync(lockPath, fs.constants.O_RDONLY | flags.noFollow | flags.nonBlocking);
    return true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'ENOENT') return false;
    // ELOOP is the expected O_NOFOLLOW rejection for a symlink: it still
    // occupies the pathname and must never be followed or replaced.
    if (code === 'ELOOP') return true;
    throw error;
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
  }
}

function createOwnedLock(
  lockPath: string,
  reclamationPath?: string,
  adoptedReclamationIdentity?: FileIdentity,
): { fd: number; identity: FileIdentity } | undefined {
  // A stale breaker owns this separate, short-lived claim before it can rename
  // the shared pathname.  Cooperating acquirers must not create in the gap
  // between that rename and its release.
  if (lockPathExistsSafely(lockPath)) return undefined;
  if (reclamationPath !== undefined && lockPathExistsSafely(reclamationPath)) {
    // A crashed stale breaker can leave its claim behind forever.  The only
    // exception is an adopter that holds an exact hard-link reference to the
    // same regular-file inode: the durable claim remains in place as a fence,
    // but the adopter may advance the interrupted reclamation.  Do not turn a
    // stale liveness observation into an unlink of the shared pathname.
    if (adoptedReclamationIdentity === undefined) return undefined;
    let current: fs.Stats;
    try {
      current = fs.lstatSync(reclamationPath);
    } catch {
      return undefined;
    }
    if (!current.isFile() || !sameIdentity(current, adoptedReclamationIdentity)) return undefined;
  } else if (adoptedReclamationIdentity !== undefined) {
    // The claimed pathname disappeared after it was adopted.  A normal retry
    // may acquire after re-evaluating state, but this recovery attempt has no
    // longer proven exclusion and must fail closed.
    return undefined;
  }

  const temporaryPath = `${lockPath}.owner-${process.pid}-${randomUUID()}`;
  let fd: number | undefined;
  try {
    fd = fs.openSync(temporaryPath, 'wx', 0o600);
    fs.writeFileSync(fd, JSON.stringify(lockOwnerIdentity()));
    fs.fsyncSync(fd);
    // Establish the descriptor identity while the file is still private.  Any
    // fallible operation after the hard link publishes the primary pathname;
    // it must therefore have enough evidence to release that exact inode.
    const temporaryStat = fs.fstatSync(fd);
    const identity = { dev: temporaryStat.dev, ino: temporaryStat.ino };
    try {
      fs.linkSync(temporaryPath, lockPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'EEXIST') return undefined;
      throw error;
    }
    const ownedFd = fd;
    fd = undefined;
    try {
      fs.unlinkSync(temporaryPath);
      return { fd: ownedFd, identity };
    } catch (error) {
      // Link publication succeeded, but the private alias could not be
      // removed.  Do not leave a live owner record behind when reporting the
      // acquisition failure; release only the inode this descriptor created.
      const cleanupError = releaseOwnedLock(lockPath, ownedFd, identity);
      if (cleanupError) throw cleanupError;
      throw error;
    }
  } finally {
    if (fd !== undefined) fs.closeSync(fd);
    try {
      fs.unlinkSync(temporaryPath);
    } catch {
      // The temporary name was already linked away or cleaned after a failed acquisition.
    }
  }
}

interface ReclamationAdoption {
  readonly path: string;
  readonly fd: number;
  readonly identity: FileIdentity;
}

/**
 * Pin a stale reclamation claim without ever deleting its shared pathname.
 *
 * Node does not expose a portable crash-released kernel lock primitive.  A
 * hard link is the smallest available exact-identity adoption mechanism: it
 * binds this recovery attempt to the inode that was named by `.reclaim` at
 * link time.  The original pathname remains as the cooperative exclusion
 * fence, so another caller cannot mistake the inspection-to-acquire gap for
 * an unlocked state.
 */
function adoptStaleReclamationClaim(reclamationPath: string, staleMs: number): ReclamationAdoption | undefined {
  const flags = posixInspectionFlags();
  if (!flags) return undefined;

  const adoptionPath = `${reclamationPath}.adopt-${process.pid}-${randomUUID()}`;
  let fd: number | undefined;
  let adopted = false;
  try {
    try {
      fs.linkSync(reclamationPath, adoptionPath);
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code === 'ENOENT' || code === 'EEXIST') return undefined;
      throw error;
    }

    const parsed = parseLockOwner(adoptionPath);
    if (parsed === undefined || Date.now() - parsed.stat.mtimeMs < staleMs || !ownerIsProvenDead(parsed.owner)) {
      return undefined;
    }

    fd = fs.openSync(adoptionPath, fs.constants.O_RDONLY | flags.noFollow | flags.nonBlocking);
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || !sameIdentity(stat, parsed.stat)) return undefined;

    // A link pins the inspected inode; this check additionally proves that
    // the shared fence still names exactly that inode.  If it was replaced,
    // never bypass it and never unlink it.
    const currentClaim = fs.lstatSync(reclamationPath);
    if (!currentClaim.isFile() || !sameIdentity(currentClaim, stat)) return undefined;

    const result = { path: adoptionPath, fd, identity: { dev: stat.dev, ino: stat.ino } };
    fd = undefined;
    adopted = true;
    return result;
  } catch {
    return undefined;
  } finally {
    if (fd !== undefined) {
      try {
        fs.closeSync(fd);
      } catch {
        // The failed adoption cannot authorize recovery.
      }
    }
    if (!adopted) {
      // This is our random private hard link, never the shared `.reclaim`
      // name.  A failed cleanup is harmless to mutual exclusion because no
      // acquisition path consults private adoption names.
      try {
        fs.unlinkSync(adoptionPath);
      } catch {
        // Best-effort cleanup of a failed private adoption.
      }
    }
  }
}

class FileLockReleaseError extends Error {
  constructor(lockPath: string, cause: unknown) {
    super(`Could not safely release owned lock '${lockPath}'`, { cause });
    this.name = 'FileLockReleaseError';
  }
}

export function isFileLockReleaseError(error: unknown): error is Error {
  return error instanceof FileLockReleaseError;
}

/** A stable carrier when a primary throw cannot safely retain cleanup evidence. */
class FileLockCompositeError extends Error {
  readonly secondaryErrors: Error[];

  constructor(operationError: unknown, cleanupError: Error) {
    super('Critical section failed and its owned lock could not be safely released', { cause: operationError });
    this.name = 'FileLockCompositeError';
    this.secondaryErrors = [cleanupError];
  }
}

function releaseOwnedLock(lockPath: string, fd: number, identity: FileIdentity): Error | undefined {
  let releaseError: unknown;
  let closeError: unknown;
  let released = false;
  try {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      let current: fs.Stats;
      try {
        current = fs.lstatSync(lockPath);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
          released = true;
          break;
        }
        releaseError ??= error;
        sleepSync(1);
        continue;
      }
      // A stale-breaker or new caller has replaced this pathname. We released
      // our ownership of the shared name already; never remove their lock.
      if (!sameIdentity(current, identity)) {
        released = true;
        break;
      }
      try {
        fs.rmSync(lockPath);
        released = true;
        break;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
          released = true;
          break;
        }
        releaseError ??= error;
        sleepSync(1);
      }
    }
  } finally {
    try {
      fs.closeSync(fd);
    } catch (error) {
      closeError = error;
    }
  }
  return released && closeError === undefined
    ? undefined
    : new FileLockReleaseError(lockPath, closeError ?? releaseError);
}

function retainCleanupEvidence(operationError: unknown, cleanupError: Error): boolean {
  if (!(operationError instanceof Error)) return false;
  try {
    const withSecondary = operationError as Error & { secondaryErrors?: Error[] };
    if (Array.isArray(withSecondary.secondaryErrors)) {
      withSecondary.secondaryErrors.push(cleanupError);
    } else {
      Object.defineProperty(operationError, 'secondaryErrors', {
        configurable: true,
        value: [cleanupError],
        writable: true,
      });
    }
    return true;
  } catch {
    return false;
  }
}

/**
 * Block this thread for `ms`.
 *
 * Deliberately synchronous: every caller of `withFileLock` is a synchronous
 * write path (a checkpoint, a queue persist) reached from callbacks that cannot
 * be made async without changing the node-context API. The waits are single
 * -digit milliseconds and only happen under real contention.
 */
function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Run `fn` while holding an exclusive lock file, waiting briefly for it.
 *
 * `ok: false` means only "could not enter the critical section" — never "you no
 * longer own this". Callers must not treat it as a loss.
 */
export function withFileLock<T>(lockPath: string, fn: () => T, opts: FileLockOptions = {}): LockResult<T> {
  const staleMs = opts.staleMs ?? DEFAULT_LOCK_STALE_MS;
  const waitMs = opts.waitMs ?? DEFAULT_LOCK_WAIT_MS;
  const deadline = Date.now() + waitMs;

  let fd: number | undefined;
  /**
   * Which file we created, so releasing can never remove someone else's.
   *
   * Unlinking by path is not release, it is "delete whatever is called that" —
   * and the two stop being the same file the moment anything else can break a
   * lock.
   */
  let identity: FileIdentity | undefined;
  const reclamationPath = `${lockPath}.reclaim`;
  let lastError = 'lock is held by another process';
  for (;;) {
    // Checked once per iteration, before anything can `continue` past it. Two
    // of the retry paths below used to skip it, which made this loop unbounded
    // in exactly the case it exists for: another process churning the lock file
    // meant the vanished-lock branch retried forever, with no deadline and no
    // yield. On a two-core CI runner that pegged a core and starved the test
    // runner for fourteen minutes — a busy-wait that a comment described as
    // "waits before giving up".
    if (Date.now() >= deadline) return { ok: false, reason: 'contended', error: lastError };

    try {
      if (opts.createParent) fs.mkdirSync(path.dirname(lockPath), { recursive: true });
      const owned = createOwnedLock(lockPath, reclamationPath);
      if (owned) {
        fd = owned.fd;
        identity = owned.identity;
        break;
      }
    } catch (err) {
      if (isFileLockReleaseError(err)) {
        // A published primary lock remains when cleanup could not prove its
        // exact release. This is terminal for this caller, not contention that
        // it can safely retry in-process.
        return { ok: false, reason: 'cleanup_failed', error: err.message, cause: err };
      }
      const code = (err as NodeJS.ErrnoException).code;
      if (code !== 'EEXIST') return { ok: false, reason: 'contended', error: (err as Error).message };
    }

    const adoptedClaim = adoptStaleReclamationClaim(reclamationPath, staleMs);
    if (adoptedClaim !== undefined) {
      let adoptedLock: { fd: number; identity: FileIdentity } | undefined;
      let adoptedCleanupError: Error | undefined;
      try {
        const heldPrimary = parseLockOwner(lockPath);
        if (heldPrimary === undefined) {
          if (lockPathExistsSafely(lockPath)) {
            lastError = 'lock lacks a verifiable owner identity';
          } else {
            // This is the post-rename crash case.  The still-present shared
            // claim fences competing normal acquirers; `createOwnedLock`
            // admits only this exact adoption and re-checks that fence.
            adoptedLock = createOwnedLock(lockPath, reclamationPath, adoptedClaim.identity);
            if (!adoptedLock) lastError = 'reclamation claim changed during recovery';
          }
        } else {
          const primaryAge = Date.now() - heldPrimary.stat.mtimeMs;
          if (
            primaryAge >= staleMs &&
            ownerIsProvenDead(heldPrimary.owner) &&
            sameIdentity(fs.lstatSync(reclamationPath), adoptedClaim.identity)
          ) {
            // This is the pre-rename crash case.  The adoption pins the exact
            // stale claim, and the shared claim remains installed throughout
            // the final primary identity check and rename.
            const current = fs.lstatSync(lockPath);
            if (!sameIdentity(current, heldPrimary.stat)) {
              lastError = 'lock identity changed before adopted stale reclamation';
            } else {
              fs.renameSync(lockPath, `${lockPath}.stale.${process.pid}.${Date.now().toString(36)}`);
              lastError = `broke a lock abandoned for ${Math.round(primaryAge)}ms`;
            }
          } else {
            lastError = `lock held by another process for ${Math.round(primaryAge)}ms`;
          }
        }
      } catch {
        // The exact-identity checks are deliberately all-or-nothing.  A
        // replacement, special file, or failed pathname operation retries as
        // contention instead of deleting or bypassing an unknown claim.
        lastError = 'could not prove reclaimed lock ownership';
      } finally {
        const cleanupError = releaseOwnedLock(adoptedClaim.path, adoptedClaim.fd, adoptedClaim.identity);
        if (cleanupError) adoptedCleanupError = cleanupError;
      }
      if (adoptedCleanupError) {
        return {
          ok: false,
          reason: 'cleanup_failed',
          error: adoptedCleanupError.message,
          cause: adoptedCleanupError,
        };
      }
      if (adoptedLock !== undefined) {
        fd = adoptedLock.fd;
        identity = adoptedLock.identity;
        break;
      }
      sleepSync(1);
      continue;
    }

    const held = parseLockOwner(lockPath);
    if (!held) {
      // Legacy zero-byte locks have no durable owner identity.  A procfs scan
      // is only a point-in-time observation: a holder can open the inode after
      // enumeration, so it cannot authorize reclamation.  Keep such records
      // contended until an owner with a verifiable identity can be recovered.
      lastError = 'lock lacks a verifiable owner identity';
      sleepSync(1);
      continue;
    }
    const age = Date.now() - held.stat.mtimeMs;

    if (age >= staleMs && ownerIsProvenDead(held.owner)) {
      // Serialize stale reclamation separately from normal acquisition.  The
      // claim remains present until after the rename, so a cooperating caller
      // cannot acquire a new owner in the inspection-to-rename window.
      const claim = createOwnedLock(reclamationPath);
      if (!claim) {
        sleepSync(1);
        continue;
      }
      let reclaimCleanupError: Error | undefined;
      try {
        const claimed = parseLockOwner(lockPath);
        if (
          claimed !== undefined &&
          sameIdentity(claimed.stat, held.stat) &&
          Date.now() - claimed.stat.mtimeMs >= staleMs &&
          ownerIsProvenDead(claimed.owner)
        ) {
          // This is the final identity proof made while the cooperative claim
          // excludes both other breakers and new acquirers.  If it changed,
          // leave the pathname untouched and fail closed by retrying.
          const current = fs.lstatSync(lockPath);
          if (!sameIdentity(current, held.stat)) {
            lastError = 'lock identity changed before stale reclamation';
          } else {
            fs.renameSync(lockPath, `${lockPath}.stale.${process.pid}.${Date.now().toString(36)}`);
            lastError = `broke a lock abandoned for ${Math.round(age)}ms`;
          }
        }
      } catch {
        // Someone else got there first; just retry the create.
      } finally {
        const cleanupError = releaseOwnedLock(reclamationPath, claim.fd, claim.identity);
        if (cleanupError) reclaimCleanupError = cleanupError;
      }
      if (reclaimCleanupError) {
        return {
          ok: false,
          reason: 'cleanup_failed',
          error: reclaimCleanupError.message,
          cause: reclaimCleanupError,
        };
      }
      sleepSync(1);
      continue;
    }

    lastError = `lock held by another process for ${Math.round(age)}ms`;
    sleepSync(5);
  }

  let value: T | undefined;
  let operationThrew = false;
  let operationError: unknown;
  try {
    value = fn();
  } catch (error) {
    operationThrew = true;
    operationError = error;
  }
  const cleanupError = releaseOwnedLock(lockPath, fd!, identity!);
  if (operationThrew) {
    if (cleanupError && !retainCleanupEvidence(operationError, cleanupError)) {
      throw new FileLockCompositeError(operationError, cleanupError);
    }
    throw operationError;
  }
  if (cleanupError) throw cleanupError;
  return { ok: true, value: value! };
}
