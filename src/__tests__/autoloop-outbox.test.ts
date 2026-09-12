import { spawn, type ChildProcess } from 'node:child_process';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import * as os from 'node:os';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { SecureAutoloopLedger } from '../autoloop/secure-ledger.js';
import { __setFileLockPosixInspectionFlagsForTests, withFileLock } from '../kernel/file-lock.js';

const workspaces: string[] = [];

function makeWorkspace(): string {
  const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-outbox-'));
  workspaces.push(workspace);
  return workspace;
}

function persistedDeliveryIntents(ledgerDir: string): Array<Record<string, unknown>> {
  const contents = fs.readFileSync(path.join(ledgerDir, 'decisions.jsonl'), 'utf8');
  return contents
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line) as Record<string, unknown>)
    .filter((row) => typeof row.delivery_id === 'string' && typeof row.idempotency_key === 'string');
}

function writeLegacyDecisionLedgerOfSize(filePath: string, targetBytes: number): void {
  const prefix = '{"kind":"legacy_seed","padding":"';
  const suffix = '"}\n';
  const minimumRowBytes = Buffer.byteLength(prefix + suffix);
  const maximumRowBytes = 1_300_000;
  const fd = fs.openSync(filePath, 'w', 0o600);
  try {
    let remaining = targetBytes;
    while (remaining > 0) {
      let rowBytes = Math.min(maximumRowBytes, remaining);
      const nextRemainder = remaining - rowBytes;
      if (nextRemainder > 0 && nextRemainder < minimumRowBytes) {
        rowBytes -= minimumRowBytes - nextRemainder;
      }
      if (rowBytes < minimumRowBytes) throw new Error('legacy decision-ledger fixture cannot fit a complete row');
      const row = prefix + 'x'.repeat(rowBytes - minimumRowBytes) + suffix;
      const written = fs.writeSync(fd, row, null, 'utf8');
      if (written !== rowBytes) throw new Error('legacy decision-ledger fixture write was incomplete');
      remaining -= rowBytes;
    }
  } finally {
    fs.closeSync(fd);
  }
}

async function nextWorkerMessage(child: ChildProcess): Promise<Record<string, unknown>> {
  const [message] = (await Promise.race([
    once(child, 'message'),
    once(child, 'error').then(([error]) => Promise.reject(error)),
    once(child, 'exit').then(([code]) => Promise.reject(new Error(`outbox worker exited early with code ${code}`))),
  ])) as [unknown];
  if (typeof message !== 'object' || message === null || Array.isArray(message)) {
    throw new Error('outbox worker returned a malformed IPC message');
  }
  return message as Record<string, unknown>;
}

interface PrepareWorkerOptions {
  attemptedMarker?: string;
  heldMarker?: string;
  holdAppendUntil?: string;
  ageHeldLock?: boolean;
}

interface InvalidInputFixture {
  input: Record<PropertyKey, unknown>;
  observations: () => number;
}

interface OutboxFailure extends Error {
  code?: unknown;
  committed?: unknown;
  retryable?: unknown;
  secondaryErrors?: Error[];
}

function captureFailure(operation: () => unknown): OutboxFailure {
  try {
    operation();
  } catch (error) {
    if (error instanceof Error) return error as OutboxFailure;
    throw new Error(`expected an Error failure, received ${String(error)}`);
  }
  throw new Error('expected operation to fail');
}

function nestedPayload(edges: number): unknown {
  let value: unknown = 'leaf';
  for (let index = 0; index < edges; index += 1) value = { child: value };
  return value;
}

async function waitForPath(filePath: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (!fs.existsSync(filePath)) {
    if (Date.now() >= deadline) throw new Error(`timed out waiting for '${filePath}'`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function spawnPrepareWorker(
  workspace: string,
  runId: string,
  createdAt: string,
  options: PrepareWorkerOptions = {},
): ChildProcess {
  const secureLedgerUrl = pathToFileURL(path.resolve('src/autoloop/secure-ledger.ts')).href;
  const outboxUrl = pathToFileURL(path.resolve('src/autoloop/outbox.ts')).href;
  const script = `
    import fs from 'node:fs';
    import { SecureAutoloopLedger } from ${JSON.stringify(secureLedgerUrl)};
    import { prepareDelivery } from ${JSON.stringify(outboxUrl)};
    const attemptedMarker = ${JSON.stringify(options.attemptedMarker ?? null)};
    const heldMarker = ${JSON.stringify(options.heldMarker ?? null)};
    const holdAppendUntil = ${JSON.stringify(options.holdAppendUntil ?? null)};
    const ageHeldLock = ${JSON.stringify(options.ageHeldLock ?? false)};
    let held = false;
    const ledger = SecureAutoloopLedger.open(${JSON.stringify(workspace)}, ${JSON.stringify(runId)}, {
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (held || name !== 'decisions.jsonl' || operation !== 'append' || holdAppendUntil === null) return;
          held = true;
          if (ageHeldLock) {
            const staleAt = new Date(Date.now() - 120_000);
            fs.utimesSync(path.join(ledger.directory, '.delivery-outbox.lock'), staleAt, staleAt);
          }
          if (heldMarker !== null) fs.writeFileSync(heldMarker, 'held');
          const deadline = Date.now() + 15_000;
          while (!fs.existsSync(holdAppendUntil) && Date.now() < deadline) {
            try {
              process.kill(process.ppid, 0);
            } catch {
              throw new Error('outbox worker parent exited while its append hook was held');
            }
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
          }
          if (!fs.existsSync(holdAppendUntil)) throw new Error('outbox append hook release timed out');
        },
      },
    });
    process.send?.({ type: 'ready' });
    process.once('message', (message) => {
      if (message !== 'go') process.exit(2);
      try {
        if (attemptedMarker !== null) fs.writeFileSync(attemptedMarker, 'attempted');
        const intent = prepareDelivery(ledger, {
          idempotency_key: 'concurrent-coder-attempt',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 8,
          payload: { directive: 'x'.repeat(512 * 1024), sequence: 12 },
        }, { now: () => new Date(${JSON.stringify(createdAt)}) });
        process.send?.({
          type: 'result',
          delivery_id: intent.delivery_id,
          created_at: intent.created_at,
          payload_sha256: intent.payload_sha256,
        });
        process.exit(0);
      } catch (error) {
        const failure = typeof error === 'object' && error !== null ? error : {};
        process.send?.({
          type: 'error',
          message: error instanceof Error ? error.message : String(error),
          code: failure.code,
          committed: failure.committed,
          retryable: failure.retryable,
        });
        process.exit(1);
      }
    });
  `;

  return spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '--eval', script], {
    cwd: process.cwd(),
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  });
}

function spawnLookupWorker(workspace: string, runId: string, idempotencyKey: string): ChildProcess {
  const secureLedgerUrl = pathToFileURL(path.resolve('src/autoloop/secure-ledger.ts')).href;
  const outboxUrl = pathToFileURL(path.resolve('src/autoloop/outbox.ts')).href;
  const script = `
    import { SecureAutoloopLedger } from ${JSON.stringify(secureLedgerUrl)};
    import { lookupByIdempotencyKey } from ${JSON.stringify(outboxUrl)};
    try {
      const ledger = SecureAutoloopLedger.open(${JSON.stringify(workspace)}, ${JSON.stringify(runId)});
      const intent = lookupByIdempotencyKey(ledger, ${JSON.stringify(idempotencyKey)});
      process.send?.({ type: 'result', intent });
      process.exit(0);
    } catch (error) {
      const failure = typeof error === 'object' && error !== null ? error : {};
      process.send?.({
        type: 'error',
        message: error instanceof Error ? error.message : String(error),
        code: failure.code,
        committed: failure.committed,
        retryable: failure.retryable,
      });
      process.exit(1);
    }
  `;

  return spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '--eval', script], {
    cwd: process.cwd(),
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  });
}

function spawnAcknowledgementWorker(
  workspace: string,
  runId: string,
  deliveryId: string,
  payloadSha256: string,
): ChildProcess {
  const secureLedgerUrl = pathToFileURL(path.resolve('src/autoloop/secure-ledger.ts')).href;
  const outboxUrl = pathToFileURL(path.resolve('src/autoloop/outbox.ts')).href;
  const script = `
    import { SecureAutoloopLedger } from ${JSON.stringify(secureLedgerUrl)};
    import { acknowledgeDelivery } from ${JSON.stringify(outboxUrl)};
    const ledger = SecureAutoloopLedger.open(${JSON.stringify(workspace)}, ${JSON.stringify(runId)});
    process.send?.({ type: 'ready' });
    process.once('message', (message) => {
      if (message !== 'go') process.exit(2);
      try {
        const acknowledgement = acknowledgeDelivery(ledger, ${JSON.stringify(deliveryId)}, ${JSON.stringify(payloadSha256)});
        process.send?.({ type: 'result', acknowledgement });
        process.exit(0);
      } catch (error) {
        const failure = typeof error === 'object' && error !== null ? error : {};
        process.send?.({
          type: 'error',
          message: error instanceof Error ? error.message : String(error),
          code: failure.code,
          committed: failure.committed,
          retryable: failure.retryable,
        });
        process.exit(1);
      }
    });
  `;
  return spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '--eval', script], {
    cwd: process.cwd(),
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  });
}

function spawnReclamationRaceWorker(lockPath: string, effectPath: string, role: string): ChildProcess {
  const fileLockUrl = pathToFileURL(path.resolve('src/kernel/file-lock.ts')).href;
  const script = `
    import fs from 'node:fs';
    import { withFileLock } from ${JSON.stringify(fileLockUrl)};
    const lockPath = ${JSON.stringify(lockPath)};
    const effectPath = ${JSON.stringify(effectPath)};
    const role = ${JSON.stringify(role)};
    process.send?.({ type: 'ready', role });
    process.once('message', (message) => {
      if (message !== 'go') process.exit(2);
      try {
        const result = withFileLock(lockPath, () => {
          const criticalPath = effectPath + '.critical';
          fs.mkdirSync(criticalPath);
          try {
            let wroteEffect = false;
            try {
              fs.writeFileSync(effectPath, 'exactly-one-durable-effect', { flag: 'wx', mode: 0o600 });
              wroteEffect = true;
            } catch (error) {
              if (error?.code !== 'EEXIST') throw error;
            }
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);
            return { role, wroteEffect };
          } finally {
            fs.rmdirSync(criticalPath);
          }
        }, { staleMs: 1, waitMs: 2_000 });
        process.send?.({ type: 'result', role, result });
        process.exit(result.ok ? 0 : 1);
      } catch (error) {
        process.send?.({ type: 'error', role, message: error instanceof Error ? error.message : String(error) });
        process.exit(1);
      }
    });
  `;
  return spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '--eval', script], {
    cwd: process.cwd(),
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  });
}

afterEach(() => {
  __setFileLockPosixInspectionFlagsForTests(undefined);
  for (const workspace of workspaces.splice(0)) {
    fs.rmSync(workspace, { force: true, recursive: true });
  }
});

describe('Autoloop delivery outbox', () => {
  it('rebinds one unacknowledged delivery to a strictly newer generation without changing its identity', async () => {
    // Production break caught: recovery either rejects a new physical generation
    // or mints a second delivery identity after the original send crashed.
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-generation-rebind', { create: true });
    const original = prepareDelivery(ledger, {
      idempotency_key: 'generation-safe-retry',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 4,
      payload: { directive: 'retry only after the replacement generation is live' },
    });

    const rebound = prepareDelivery(ledger, {
      idempotency_key: 'generation-safe-retry',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 5,
      payload: { directive: 'retry only after the replacement generation is live' },
    });

    expect(rebound).toMatchObject({
      delivery_id: original.delivery_id,
      idempotency_key: original.idempotency_key,
      kind: original.kind,
      target_role: original.target_role,
      target_generation: 5,
      payload_sha256: original.payload_sha256,
    });
    expect(lookupByIdempotencyKey(ledger, original.idempotency_key)).toEqual(rebound);
    const records = fs
      .readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(records).toContainEqual(
      expect.objectContaining({
        record_type: 'delivery_generation_rebind',
        delivery_id: original.delivery_id,
        idempotency_key: original.idempotency_key,
        from_generation: 4,
        to_generation: 5,
        payload_sha256: original.payload_sha256,
      }),
    );
  });

  it('keeps an acknowledged delivery a byte-for-byte no-op when a newer generation appears', async () => {
    // Production break caught: an already delivered message is rebound and sent
    // again after its in-memory dispatcher cache is lost.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-acknowledged-generation-noop', { create: true });
    const original = prepareDelivery(ledger, {
      idempotency_key: 'acknowledged-generation-noop',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 2,
      payload: { request: 'do not resend this completed review' },
    });
    acknowledgeDelivery(ledger, original.delivery_id, original.payload_sha256);
    const before = fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8');

    expect(
      prepareDelivery(ledger, {
        idempotency_key: original.idempotency_key,
        kind: original.kind,
        target_role: original.target_role,
        target_generation: 3,
        payload: original.payload,
      }),
    ).toEqual(original);
    expect(fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')).toEqual(before);
  });

  it('rejects forked, stale, and post-acknowledgement generation evidence globally', async () => {
    // Production break caught: an unrelated forged rebind can make a valid
    // delivery lookup silently select an ambiguous or already completed route.
    const { acknowledgeDelivery, lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-forked-generation-evidence', { create: true });
    const original = prepareDelivery(ledger, {
      idempotency_key: 'forked-generation-evidence',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'validate every generation chain' },
    });
    prepareDelivery(ledger, {
      idempotency_key: original.idempotency_key,
      kind: original.kind,
      target_role: original.target_role,
      target_generation: 2,
      payload: original.payload,
    });
    const forged = {
      schema_version: 1,
      record_type: 'delivery_generation_rebind',
      delivery_id: original.delivery_id,
      idempotency_key: original.idempotency_key,
      kind: original.kind,
      target_role: original.target_role,
      from_generation: 1,
      to_generation: 3,
      payload_sha256: original.payload_sha256,
      rebound_at: '2026-09-11T12:00:00.000Z',
    };
    ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(forged)}\n`, true);

    expect(captureFailure(() => lookupByIdempotencyKey(ledger, original.idempotency_key))).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });

    const cleanLedger = SecureAutoloopLedger.open(workspace, 'run-post-ack-generation-evidence', { create: true });
    const acknowledged = prepareDelivery(cleanLedger, {
      idempotency_key: 'post-ack-generation-evidence',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 1,
      payload: { request: 'completed' },
    });
    acknowledgeDelivery(cleanLedger, acknowledged.delivery_id, acknowledged.payload_sha256);
    cleanLedger.appendFlatFile(
      'decisions.jsonl',
      `${JSON.stringify({ ...forged, delivery_id: acknowledged.delivery_id, idempotency_key: acknowledged.idempotency_key, kind: acknowledged.kind, target_role: acknowledged.target_role, payload_sha256: acknowledged.payload_sha256 })}\n`,
      true,
    );
    expect(captureFailure(() => lookupByIdempotencyKey(cleanLedger, acknowledged.idempotency_key))).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
  });

  it('serializes concurrent newer-generation retries into one rebind record', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-concurrent-generation-rebind', { create: true });
    const original = prepareDelivery(ledger, {
      idempotency_key: 'concurrent-generation-rebind',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 8,
      payload: { request: 'one durable retry route' },
    });

    const attempts = await Promise.all(
      Array.from({ length: 20 }, () =>
        Promise.resolve().then(() =>
          prepareDelivery(ledger, {
            idempotency_key: original.idempotency_key,
            kind: original.kind,
            target_role: original.target_role,
            target_generation: 9,
            payload: original.payload,
          }),
        ),
      ),
    );
    expect(attempts).toEqual(
      Array.from({ length: 20 }, () =>
        expect.objectContaining({ delivery_id: original.delivery_id, target_generation: 9 }),
      ),
    );
    const rebinds = fs
      .readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as Record<string, unknown>)
      .filter((row) => row.record_type === 'delivery_generation_rebind');
    expect(rebinds).toHaveLength(1);
  });

  it('latches a post-commit rebind ambiguity instead of manufacturing another retry route', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    let remainingDecisionBarriers = -1;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-generation-rebind-barrier-ambiguity', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name === 'decisions.jsonl' && remainingDecisionBarriers-- === 0) {
            throw new Error('injected rebind directory barrier failure');
          }
        },
      },
    });
    const original = prepareDelivery(ledger, {
      idempotency_key: 'generation-rebind-barrier-ambiguity',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'fail closed after ambiguous generation append' },
    });
    // The retry first flushes its old durable intent; fail the next directory
    // barrier, after the new rebind row has been committed.
    remainingDecisionBarriers = 1;

    const first = captureFailure(() =>
      prepareDelivery(ledger, {
        idempotency_key: original.idempotency_key,
        kind: original.kind,
        target_role: original.target_role,
        target_generation: 2,
        payload: original.payload,
      }),
    );
    const retry = captureFailure(() =>
      prepareDelivery(ledger, {
        idempotency_key: original.idempotency_key,
        kind: original.kind,
        target_role: original.target_role,
        target_generation: 2,
        payload: original.payload,
      }),
    );
    expect(first).toMatchObject({ code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED', committed: true });
    expect(retry).toBe(first);
    expect(
      fs
        .readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')
        .split('\n')
        .filter((line) => line.includes('delivery_generation_rebind')),
    ).toHaveLength(1);
  });

  it('persists the matching acknowledgement before returning it (break: acknowledgement API returns success without a durable row)', async () => {
    // Production break caught: acknowledgeDelivery returns before its acknowledgement is durable in decisions.jsonl.
    const outbox = (await import('../autoloop/outbox.js')) as typeof import('../autoloop/outbox.js') & {
      acknowledgeDelivery: (
        ledger: SecureAutoloopLedger,
        deliveryId: string,
        payloadSha256: string,
        options?: { now?: () => Date },
      ) => unknown;
    };
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-persist-before-success', { create: true });
    const intent = outbox.prepareDelivery(ledger, {
      idempotency_key: 'ack-persist-before-success',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'durably acknowledge this exact delivery' },
    });

    const acknowledgement = outbox.acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
      now: () => new Date('2026-09-11T06:00:00.000Z'),
    });

    expect(acknowledgement).toEqual({
      schema_version: 1,
      delivery_id: intent.delivery_id,
      payload_sha256: intent.payload_sha256,
      acknowledged_at: '2026-09-11T06:00:00.000Z',
    });
    expect(fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')).toContain(
      `${JSON.stringify(acknowledgement)}\n`,
    );
  });

  it('replays an identical acknowledgement without appending or changing its first timestamp (break: retry writes a second acknowledgement)', async () => {
    // Production break caught: an acknowledgement retry appends another record or replaces the first timestamp.
    const outbox = (await import('../autoloop/outbox.js')) as typeof import('../autoloop/outbox.js') & {
      acknowledgeDelivery: (
        ledger: SecureAutoloopLedger,
        deliveryId: string,
        payloadSha256: string,
        options?: { now?: () => Date },
      ) => unknown;
    };
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-idempotent-replay', { create: true });
    const intent = outbox.prepareDelivery(ledger, {
      idempotency_key: 'ack-idempotent-replay',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 2,
      payload: { scope: 'preserve the first acknowledgement' },
    });
    const first = outbox.acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
      now: () => new Date('2026-09-11T06:01:00.000Z'),
    });
    const beforeReplay = fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8');

    const replay = outbox.acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
      now: () => new Date('2026-09-11T07:01:00.000Z'),
    });

    expect(replay).toEqual(first);
    expect(fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')).toEqual(beforeReplay);
  });

  it('replays an identical acknowledgement when its later clock throws or is invalid (break: replay validates a clock it does not need)', async () => {
    // Production break caught: a durable acknowledgement retry must be a no-op
    // even when a later caller clock cannot supply a new timestamp.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-idempotent-replay-clock-failure', { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-idempotent-replay-clock-failure',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 2,
      payload: { scope: 'preserve the first acknowledgement without a second clock' },
    });
    const first = acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
      now: () => new Date('2026-09-11T06:01:00.000Z'),
    });
    const beforeReplay = fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8');

    expect(
      acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
        now: () => {
          throw new Error('second acknowledgement clock must not be used for replay');
        },
      }),
    ).toEqual(first);
    expect(
      acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
        now: () => new Date('not-a-real-acknowledgement-time'),
      }),
    ).toEqual(first);
    expect(fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')).toEqual(beforeReplay);
  });

  it('rejects a throwing or invalid acknowledgement clock before mutating an unacknowledged delivery', async () => {
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-new-clock-failure', { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-new-clock-failure',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 2,
      payload: { scope: 'validate a new acknowledgement clock before append' },
    });
    const before = fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8');

    for (const now of [
      () => {
        throw new Error('acknowledgement clock failure');
      },
      () => new Date('not-a-real-acknowledgement-time'),
    ]) {
      expect(
        captureFailure(() => acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, { now })),
      ).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_INPUT_INVALID',
        retryable: false,
      });
      expect(fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')).toEqual(before);
    }
  });

  it('classifies hostile acknowledgement clock accessors without invalidating a durable replay (break: options.now getter errors escape the typed input boundary)', async () => {
    // Production break caught: resolving options.now can execute a hostile getter or
    // Proxy trap. A new acknowledgement must report typed invalid input, while an
    // existing acknowledgement remains a durable no-op regardless of that later clock.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const replayLedger = SecureAutoloopLedger.open(workspace, 'run-ack-hostile-now-replay', { create: true });
    const replayIntent = prepareDelivery(replayLedger, {
      idempotency_key: 'ack-hostile-now-replay',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 2,
      payload: { scope: 'preserve a durable acknowledgement despite hostile clock property access' },
    });
    const first = acknowledgeDelivery(replayLedger, replayIntent.delivery_id, replayIntent.payload_sha256, {
      now: () => new Date('2026-09-11T06:01:00.000Z'),
    });
    const replayPath = path.join(replayLedger.directory, 'decisions.jsonl');
    const beforeReplay = fs.readFileSync(replayPath, 'utf8');

    const newLedger = SecureAutoloopLedger.open(workspace, 'run-ack-hostile-now-new', { create: true });
    const newIntent = prepareDelivery(newLedger, {
      idempotency_key: 'ack-hostile-now-new',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 2,
      payload: { scope: 'reject hostile clock property access before acknowledgement append' },
    });
    const newPath = path.join(newLedger.directory, 'decisions.jsonl');
    const beforeNewAcknowledgement = fs.readFileSync(newPath, 'utf8');

    const hostileOptions = [
      {
        label: 'own getter',
        create(failure: Error): { now?: () => Date } {
          const options = {};
          Object.defineProperty(options, 'now', {
            enumerable: true,
            get: () => {
              throw failure;
            },
          });
          return options;
        },
      },
      {
        label: 'inherited getter',
        create(failure: Error): { now?: () => Date } {
          return Object.create({
            get now() {
              throw failure;
            },
          }) as { now?: () => Date };
        },
      },
      {
        label: 'Proxy get trap',
        create(failure: Error): { now?: () => Date } {
          return new Proxy(
            {},
            {
              get(_target, key) {
                if (key === 'now') throw failure;
                return undefined;
              },
            },
          ) as { now?: () => Date };
        },
      },
    ];

    for (const hostile of hostileOptions) {
      const replayFailure = new Error(`replay ${hostile.label} failure`);
      expect(
        acknowledgeDelivery(
          replayLedger,
          replayIntent.delivery_id,
          replayIntent.payload_sha256,
          hostile.create(replayFailure),
        ),
      ).toEqual(first);
      expect(fs.readFileSync(replayPath, 'utf8')).toEqual(beforeReplay);

      const newFailure = new Error(`new ${hostile.label} failure`);
      const failure = captureFailure(() =>
        acknowledgeDelivery(newLedger, newIntent.delivery_id, newIntent.payload_sha256, hostile.create(newFailure)),
      );
      expect(failure).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_INPUT_INVALID',
        retryable: false,
        cause: newFailure,
      });
      expect(fs.readFileSync(newPath, 'utf8')).toEqual(beforeNewAcknowledgement);
    }
  });

  it('fails closed when the acknowledgement clock rewrites the matching intent before its first acknowledgement (break: caller-controlled acknowledgement time can leave stale intent proof)', async () => {
    // Production break caught: acknowledgeDelivery reads an intent, then lets its
    // caller-controlled clock replace that same-length intent before success.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-clock-prefix-rewrite', { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-clock-prefix-rewrite',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'reject stale acknowledgement proof' },
    });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const replacementId = `${intent.delivery_id[0] === 'a' ? 'b' : 'a'}${intent.delivery_id.slice(1)}`;
    let rewritten = false;

    const failure = captureFailure(() =>
      acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
        now: () => {
          const contents = fs.readFileSync(ledgerPath, 'utf8');
          const replacement = contents.replace(intent.delivery_id, replacementId);
          expect(Buffer.byteLength(replacement, 'utf8')).toBe(Buffer.byteLength(contents, 'utf8'));
          fs.writeFileSync(ledgerPath, replacement, { mode: 0o600 });
          rewritten = true;
          return new Date('2026-09-11T08:00:00.000Z');
        },
      }),
    );

    expect(rewritten).toBe(true);
    expect(failure).toMatchObject({ code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID', retryable: false });
    const cold = SecureAutoloopLedger.open(workspace, 'run-ack-clock-prefix-rewrite');
    expect(captureFailure(() => acknowledgeDelivery(cold, intent.delivery_id, intent.payload_sha256))).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_ACKNOWLEDGEMENT_CONFLICT',
      retryable: false,
    });
  });

  it('fails closed when replay durability rewrites an intent outside the cached tail (break: replay returns acknowledgement without freshly proving its intent)', async () => {
    // Production break caught: replay validates only its acknowledgement after
    // durability, so an older same-length intent rewrite can return stale proof.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-replay-prefix-rewrite', { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-replay-prefix-rewrite',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'freshly prove intent on replay' },
    });
    acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
      now: () => new Date('2026-09-11T08:01:00.000Z'),
    });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    ledger.appendFlatFile('decisions.jsonl', `${'{"kind":"legacy_seed","padding":"'}${'x'.repeat(8 * 1024)}"}\n`, true);
    const replacementId = `${intent.delivery_id[0] === 'a' ? 'b' : 'a'}${intent.delivery_id.slice(1)}`;
    const flush = ledger.flushFlatFile.bind(ledger);
    let rewritten = false;
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile').mockImplementation((name) => {
      const result = flush(name);
      if (name === 'decisions.jsonl' && !rewritten) {
        const contents = fs.readFileSync(ledgerPath, 'utf8');
        const replacement = contents.replace(intent.delivery_id, replacementId);
        expect(Buffer.byteLength(replacement, 'utf8')).toBe(Buffer.byteLength(contents, 'utf8'));
        fs.writeFileSync(ledgerPath, replacement, { mode: 0o600 });
        rewritten = true;
      }
      return result;
    });

    try {
      const failure = captureFailure(() => acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256));
      expect(rewritten).toBe(true);
      expect(failure).toMatchObject({ code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID', retryable: false });
    } finally {
      flushSpy.mockRestore();
    }

    const cold = SecureAutoloopLedger.open(workspace, 'run-ack-replay-prefix-rewrite');
    expect(captureFailure(() => acknowledgeDelivery(cold, intent.delivery_id, intent.payload_sha256))).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
  });

  it('fails closed when a fresh acknowledgement pathname is identically replaced after its barrier before final observation', async () => {
    // Production break caught: the final acknowledgement observation can prove
    // identical bytes from a new pathname inode that the barrier never flushed.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-fresh-post-barrier-inode-replacement', {
      create: true,
    });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-fresh-post-barrier-inode-replacement',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'bind final acknowledgement proof to its flushed inode' },
    });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const flush = ledger.flushFlatFile.bind(ledger);
    const open = ledger.openFlatFile.bind(ledger);
    let barrierComplete = false;
    let readsAfterBarrier = 0;
    let swapped = false;
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile').mockImplementation((name) => {
      const flushed = flush(name);
      if (name === 'decisions.jsonl') barrierComplete = true;
      return flushed;
    });
    const openSpy = vi.spyOn(ledger, 'openFlatFile').mockImplementation((name, mode, create) => {
      if (name === 'decisions.jsonl' && mode === 'read' && barrierComplete) {
        readsAfterBarrier += 1;
      }
      if (name === 'decisions.jsonl' && mode === 'read' && readsAfterBarrier === 2 && !swapped) {
        const replacementPath = path.join(ledger.directory, 'decisions.fresh-post-barrier-replacement.jsonl');
        fs.writeFileSync(replacementPath, fs.readFileSync(ledgerPath), { mode: 0o600 });
        fs.renameSync(replacementPath, ledgerPath);
        swapped = true;
      }
      return open(name, mode, create);
    });

    try {
      const first = captureFailure(() =>
        acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
          now: () => new Date('2026-09-11T10:00:00.000Z'),
        }),
      );
      const retry = captureFailure(() => acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256));

      expect(first).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
        committed: true,
        retryable: false,
      });
      expect(retry).toBe(first);
    } finally {
      openSpy.mockRestore();
      flushSpy.mockRestore();
    }

    expect(swapped).toBe(true);
  });

  it('fails closed when an acknowledgement replay pathname is identically replaced after its barrier before final observation', async () => {
    // Production break caught: replay can return an acknowledgement from a
    // replacement inode after flushing the old pathname inode.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-replay-post-barrier-inode-replacement', {
      create: true,
    });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-replay-post-barrier-inode-replacement',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 1,
      payload: { scope: 'bind replay proof to its flushed inode' },
    });
    acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
      now: () => new Date('2026-09-11T10:01:00.000Z'),
    });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const flush = ledger.flushFlatFile.bind(ledger);
    const open = ledger.openFlatFile.bind(ledger);
    let barrierComplete = false;
    let swapped = false;
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile').mockImplementation((name) => {
      const flushed = flush(name);
      if (name === 'decisions.jsonl') barrierComplete = true;
      return flushed;
    });
    const openSpy = vi.spyOn(ledger, 'openFlatFile').mockImplementation((name, mode, create) => {
      if (name === 'decisions.jsonl' && mode === 'read' && barrierComplete && !swapped) {
        const replacementPath = path.join(ledger.directory, 'decisions.replay-post-barrier-replacement.jsonl');
        fs.writeFileSync(replacementPath, fs.readFileSync(ledgerPath), { mode: 0o600 });
        fs.renameSync(replacementPath, ledgerPath);
        swapped = true;
      }
      return open(name, mode, create);
    });

    try {
      const failure = captureFailure(() => acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256));

      expect(failure).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
        retryable: false,
      });
      expect(failure).not.toHaveProperty('committed');
    } finally {
      openSpy.mockRestore();
      flushSpy.mockRestore();
    }

    expect(swapped).toBe(true);
  });

  it('latches committed recovery failure when its final acknowledgement observation sees an identical replacement inode', async () => {
    // Production break caught: committed recovery accepts identical replacement
    // bytes after its recovery barrier, despite never flushing that inode.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const originalBarrier = new Error('original acknowledgement directory barrier');
    let failCommitBarrier = false;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-recovery-post-barrier-inode-replacement', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name === 'decisions.jsonl' && failCommitBarrier) {
            failCommitBarrier = false;
            throw originalBarrier;
          }
        },
      },
    });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-recovery-post-barrier-inode-replacement',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'bind recovered proof to its flushed inode' },
    });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const flush = ledger.flushFlatFile.bind(ledger);
    const open = ledger.openFlatFile.bind(ledger);
    let barrierComplete = false;
    let swapped = false;
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile').mockImplementation((name) => {
      const flushed = flush(name);
      if (name === 'decisions.jsonl') barrierComplete = true;
      return flushed;
    });
    const openSpy = vi.spyOn(ledger, 'openFlatFile').mockImplementation((name, mode, create) => {
      if (name === 'decisions.jsonl' && mode === 'read' && barrierComplete && !swapped) {
        const replacementPath = path.join(ledger.directory, 'decisions.recovery-post-barrier-replacement.jsonl');
        fs.writeFileSync(replacementPath, fs.readFileSync(ledgerPath), { mode: 0o600 });
        fs.renameSync(replacementPath, ledgerPath);
        swapped = true;
      }
      return open(name, mode, create);
    });
    failCommitBarrier = true;

    try {
      const first = captureFailure(() =>
        acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, {
          now: () => new Date('2026-09-11T10:02:00.000Z'),
        }),
      );
      const retry = captureFailure(() => acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256));

      expect(first).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
        committed: true,
        retryable: false,
        cause: expect.objectContaining({
          code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
          cause: originalBarrier,
        }),
      });
      expect(first.secondaryErrors).toEqual(expect.arrayContaining([expect.any(Error)]));
      expect(retry).toBe(first);
    } finally {
      openSpy.mockRestore();
      flushSpy.mockRestore();
    }

    expect(swapped).toBe(true);
  });

  it('stops a mismatched acknowledgement before changing the ledger (break: digest mismatch is accepted)', async () => {
    // Production break caught: acknowledgeDelivery accepts a digest different from the persisted delivery intent.
    const outbox = (await import('../autoloop/outbox.js')) as typeof import('../autoloop/outbox.js') & {
      acknowledgeDelivery: (ledger: SecureAutoloopLedger, deliveryId: string, payloadSha256: string) => unknown;
    };
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-digest-mismatch', { create: true });
    const intent = outbox.prepareDelivery(ledger, {
      idempotency_key: 'ack-digest-mismatch',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'require exact digest' },
    });
    const before = fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'));

    const failure = captureFailure(() => outbox.acknowledgeDelivery(ledger, intent.delivery_id, 'b'.repeat(64)));

    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_ACKNOWLEDGEMENT_CONFLICT',
      retryable: false,
    });
    expect(fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'))).toEqual(before);
  });

  it('stops an acknowledgement for an unknown delivery before changing the ledger (break: orphan acknowledgement is persisted)', async () => {
    // Production break caught: acknowledgeDelivery creates durable evidence for a delivery that was never prepared.
    const outbox = (await import('../autoloop/outbox.js')) as typeof import('../autoloop/outbox.js') & {
      acknowledgeDelivery: (ledger: SecureAutoloopLedger, deliveryId: string, payloadSha256: string) => unknown;
    };
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-unknown-delivery', { create: true });

    const failure = captureFailure(() => outbox.acknowledgeDelivery(ledger, 'unknown-delivery', 'a'.repeat(64)));

    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_ACKNOWLEDGEMENT_CONFLICT',
      retryable: false,
    });
    expect(fs.existsSync(path.join(ledger.directory, 'decisions.jsonl'))).toBe(false);
  });

  it('fails closed on ambiguous acknowledgement evidence (break: duplicate acknowledgement rows are silently accepted)', async () => {
    // Production break caught: a duplicate acknowledgement in decisions.jsonl is treated as an idempotent replay.
    const outbox = (await import('../autoloop/outbox.js')) as typeof import('../autoloop/outbox.js') & {
      acknowledgeDelivery: (ledger: SecureAutoloopLedger, deliveryId: string, payloadSha256: string) => unknown;
    };
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-ambiguous-evidence', { create: true });
    const intent = outbox.prepareDelivery(ledger, {
      idempotency_key: 'ack-ambiguous-evidence',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'reject ambiguous proof' },
    });
    const acknowledgement = {
      schema_version: 1,
      delivery_id: intent.delivery_id,
      payload_sha256: intent.payload_sha256,
      acknowledged_at: '2026-09-11T06:02:00.000Z',
    };
    ledger.appendFlatFile(
      'decisions.jsonl',
      `${JSON.stringify(acknowledgement)}\n${JSON.stringify(acknowledgement)}\n`,
      true,
    );
    const before = fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'));

    const failure = captureFailure(() => outbox.acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256));

    expect(failure).toMatchObject({ code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID', retryable: false });
    expect(fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'))).toEqual(before);
  });

  it('rejects an acknowledgement that precedes its matching intent without mutating the durable ledger (break: row-order loss accepts causal inversion)', async () => {
    // Production break caught: rebuilding lookup maps without row order accepts an
    // acknowledgement as proof even when its intent is appended only afterwards.
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-before-intent', { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-before-intent',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'require causal durable evidence' },
    });
    const acknowledgement = {
      schema_version: 1,
      delivery_id: intent.delivery_id,
      payload_sha256: intent.payload_sha256,
      acknowledged_at: '2026-09-11T06:03:00.000Z',
    };
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    fs.writeFileSync(ledgerPath, `${JSON.stringify(acknowledgement)}\n${JSON.stringify(intent)}\n`, { mode: 0o600 });
    const before = fs.readFileSync(ledgerPath);

    const coldLedger = SecureAutoloopLedger.open(workspace, 'run-ack-before-intent');
    const failure = captureFailure(() => lookupByIdempotencyKey(coldLedger, intent.idempotency_key));

    expect(failure).toMatchObject({ code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID', retryable: false });
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('rejects an unrelated orphan acknowledgement anywhere in the durable ledger without mutation (break: lookup ignores invalid acknowledgement evidence for another delivery)', async () => {
    // Production break caught: the requested intent is returned although another
    // acknowledgement in the same durable graph has no causal intent.
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-global-orphan-acknowledgement', { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'global-orphan-acknowledgement',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 1,
      payload: { scope: 'validate every acknowledgement' },
    });
    const orphan = {
      schema_version: 1,
      delivery_id: 'unrelated-orphan-delivery',
      payload_sha256: 'a'.repeat(64),
      acknowledged_at: '2026-09-11T06:04:00.000Z',
    };
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(orphan)}\n`, true);
    const before = fs.readFileSync(ledgerPath);

    const failure = captureFailure(() => lookupByIdempotencyKey(ledger, intent.idempotency_key));

    expect(failure).toMatchObject({ code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID', retryable: false });
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('rejects duplicate delivery IDs globally even when the requested idempotency key is otherwise unique (break: lookup validates only the queried map entry)', async () => {
    // Production break caught: two distinct intent rows can claim one delivery ID
    // while a lookup of an unrelated, unique idempotency key succeeds.
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-global-duplicate-delivery-id', { create: true });
    const first = prepareDelivery(ledger, {
      idempotency_key: 'first-delivery-id-claim',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'first claim' },
    });
    const clean = prepareDelivery(ledger, {
      idempotency_key: 'unrelated-unique-delivery-id-key',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 1,
      payload: { scope: 'must not hide graph corruption' },
    });
    const duplicateDeliveryId = { ...first, idempotency_key: 'second-delivery-id-claim' };
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(duplicateDeliveryId)}\n`, true);
    const before = fs.readFileSync(ledgerPath);

    const failure = captureFailure(() => lookupByIdempotencyKey(ledger, clean.idempotency_key));

    expect(failure).toMatchObject({ code: 'AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT', retryable: false });
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('rejects duplicate idempotency keys globally even when the requested key is otherwise unique (break: lookup leaves unrelated duplicate intent claims unchecked)', async () => {
    // Production break caught: two distinct intent rows can share an idempotency
    // key without invalidating a lookup for another delivery.
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-global-duplicate-idempotency-key', { create: true });
    const first = prepareDelivery(ledger, {
      idempotency_key: 'duplicated-idempotency-key',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'first idempotency claim' },
    });
    const clean = prepareDelivery(ledger, {
      idempotency_key: 'unrelated-unique-idempotency-key',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 1,
      payload: { scope: 'must not hide duplicate key corruption' },
    });
    const duplicateKey = { ...first, delivery_id: 'different-delivery-for-same-idempotency-key' };
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(duplicateKey)}\n`, true);
    const before = fs.readFileSync(ledgerPath);

    const failure = captureFailure(() => lookupByIdempotencyKey(ledger, clean.idempotency_key));

    expect(failure).toMatchObject({ code: 'AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT', retryable: false });
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('reconciles a committed acknowledgement after its first directory barrier fails (break: committed acknowledgement returns false success or appends twice)', async () => {
    // Production break caught: a post-append durability ambiguity loses or duplicates a committed acknowledgement.
    const outbox = (await import('../autoloop/outbox.js')) as typeof import('../autoloop/outbox.js') & {
      acknowledgeDelivery: (ledger: SecureAutoloopLedger, deliveryId: string, payloadSha256: string) => unknown;
    };
    const workspace = makeWorkspace();
    let failOnce = false;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-committed-reconciliation', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name === 'decisions.jsonl' && failOnce) {
            failOnce = false;
            throw new Error('injected acknowledgement directory barrier failure');
          }
        },
      },
    });
    const intent = outbox.prepareDelivery(ledger, {
      idempotency_key: 'ack-committed-reconciliation',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'reconcile after commit' },
    });
    failOnce = true;

    const acknowledgement = outbox.acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256);

    const rows = fs
      .readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as Record<string, unknown>)
      .filter((row) => row.delivery_id === intent.delivery_id && Object.hasOwn(row, 'acknowledged_at'));
    expect(rows).toEqual([acknowledgement]);
  });

  it('latches committed observation failure when recovery finds a same-length rewritten acknowledgement timestamp', async () => {
    // Production break caught: recovery accepted a timestamp reread from disk
    // rather than the timestamp serialized by this invocation.
    const { acknowledgeDelivery, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const originalBarrier = new Error('original acknowledgement directory barrier');
    const originalTime = '2026-09-11T08:00:00.000Z';
    const replacementTime = '2026-09-11T09:00:00.000Z';
    let failOnce = false;
    let rewritten = false;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-ack-committed-timestamp-rewrite', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || !failOnce) return;
          failOnce = false;
          const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
          const contents = fs.readFileSync(ledgerPath, 'utf8');
          const replacement = contents.replace(originalTime, replacementTime);
          expect(Buffer.byteLength(replacement, 'utf8')).toBe(Buffer.byteLength(contents, 'utf8'));
          fs.writeFileSync(ledgerPath, replacement, { mode: 0o600 });
          rewritten = true;
          throw originalBarrier;
        },
      },
    });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'ack-committed-timestamp-rewrite',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'reject rewritten committed acknowledgement timestamp' },
    });
    failOnce = true;

    const first = captureFailure(() =>
      acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, { now: () => new Date(originalTime) }),
    );
    const retry = captureFailure(() =>
      acknowledgeDelivery(ledger, intent.delivery_id, intent.payload_sha256, { now: () => new Date(originalTime) }),
    );

    expect(rewritten).toBe(true);
    expect(first).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
      committed: true,
      retryable: false,
      cause: expect.objectContaining({
        code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        cause: originalBarrier,
      }),
    });
    expect(first.secondaryErrors).toEqual(expect.arrayContaining([expect.any(Error)]));
    expect(retry).toBe(first);
  });

  it('gives concurrent identical acknowledgements one durable record (break: parallel acknowledgement callers append duplicates)', async () => {
    // Production break caught: separate processes both observe an unacknowledged intent and append duplicate acknowledgements.
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const runId = 'run-concurrent-identical-acknowledgements';
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'concurrent-identical-acknowledgements',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'only one acknowledgement may become durable' },
    });
    const first = spawnAcknowledgementWorker(workspace, runId, intent.delivery_id, intent.payload_sha256);
    const second = spawnAcknowledgementWorker(workspace, runId, intent.delivery_id, intent.payload_sha256);
    await Promise.all([nextWorkerMessage(first), nextWorkerMessage(second)]);
    first.send('go');
    second.send('go');
    const results = await Promise.all([nextWorkerMessage(first), nextWorkerMessage(second)]);
    const exitCodes = await Promise.all(
      [first, second].map(async (worker) =>
        worker.exitCode === null ? (await once(worker, 'exit'))[0] : worker.exitCode,
      ),
    );
    const rows = fs
      .readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as Record<string, unknown>)
      .filter((row) => row.delivery_id === intent.delivery_id && Object.hasOwn(row, 'acknowledged_at'));

    expect(exitCodes).toEqual([0, 0]);
    expect(results).toEqual([
      { type: 'result', acknowledgement: rows[0] },
      { type: 'result', acknowledgement: rows[0] },
    ]);
    expect(rows).toHaveLength(1);
  });

  it('makes a complete versioned intent durable before the caller can begin transport', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const durabilityOrder: string[] = [];
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-1', {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name === 'decisions.jsonl' && (operation === 'append' || operation === 'flush')) {
            durabilityOrder.push(operation);
          }
        },
        beforeDirectorySync: ({ name }) => {
          if (name === 'decisions.jsonl') durabilityOrder.push('directory-sync');
        },
      },
    });
    const payload = { z: ['last', { b: true, a: null }], a: 'first' };

    const intent = prepareDelivery(
      ledger,
      {
        idempotency_key: 'coder-attempt-7',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 3,
        payload,
      },
      { now: () => new Date('2026-09-10T12:34:56.789Z') },
    );

    const rowsVisibleBeforeTransport = persistedDeliveryIntents(ledger.directory);
    durabilityOrder.push('transport');

    expect(durabilityOrder).toEqual(['append', 'flush', 'directory-sync', 'flush', 'directory-sync', 'transport']);
    expect(rowsVisibleBeforeTransport).toHaveLength(1);
    expect(rowsVisibleBeforeTransport[0]).toEqual({
      schema_version: 1,
      delivery_id: expect.stringMatching(/\S/),
      idempotency_key: 'coder-attempt-7',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 3,
      payload,
      payload_sha256: '4ac509439001ae8063a5cdb0c3bc35a62a96886106906eedabb75da25d270105',
      created_at: '2026-09-10T12:34:56.789Z',
    });
    expect(intent).toEqual(rowsVisibleBeforeTransport[0]);
  });

  it('rejects a non-canonical clock timestamp before changing the decision ledger', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-invalid-created-at', { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);
    const hostileClock = new Date('2026-09-10T12:34:56.789Z');
    Object.defineProperty(hostileClock, 'toISOString', {
      configurable: true,
      value: () => 'not-an-iso-timestamp',
    });

    expect(() =>
      prepareDelivery(
        ledger,
        {
          idempotency_key: 'invalid-created-at-attempt',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 1,
          payload: { directive: 'do not poison recovery' },
        },
        { now: () => hostileClock },
      ),
    ).toThrow(/created_at|timestamp|ISO/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('classifies a throwing clock as invalid input without changing the decision ledger', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-throwing-clock', { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"clock_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);
    const clockFailure = new Error('injected clock failure');

    const failure = captureFailure(() =>
      prepareDelivery(
        ledger,
        {
          idempotency_key: 'throwing-clock-attempt',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 1,
          payload: { directive: 'preserve the ledger' },
        },
        {
          now: () => {
            throw clockFailure;
          },
        },
      ),
    );

    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_INPUT_INVALID',
      retryable: false,
      cause: clockFailure,
    });
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it.each(['file', 'directory'] as const)(
    're-establishes a fresh durability barrier after a one-shot %s sync failure without appending twice',
    async (barrier) => {
      const { prepareDelivery } = await import('../autoloop/outbox.js');
      const workspace = makeWorkspace();
      let failOnce = true;
      let appendChecks = 0;
      let flushChecks = 0;
      let directoryChecks = 0;
      const ledger = SecureAutoloopLedger.open(workspace, `run-one-shot-${barrier}-barrier`, {
        create: true,
        testHooks: {
          beforeFileMutation: ({ name, operation }) => {
            if (name !== 'decisions.jsonl') return;
            if (operation === 'append') appendChecks += 1;
            if (operation === 'flush') {
              flushChecks += 1;
              if (barrier === 'file' && failOnce) {
                failOnce = false;
                throw new Error('injected one-shot file barrier failure');
              }
            }
          },
          beforeDirectorySync: ({ name }) => {
            if (name !== 'decisions.jsonl') return;
            directoryChecks += 1;
            if (barrier === 'directory' && failOnce) {
              failOnce = false;
              throw new Error('injected one-shot directory barrier failure');
            }
          },
        },
      });

      const intent = prepareDelivery(ledger, {
        idempotency_key: `one-shot-${barrier}-barrier`,
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 2,
        payload: { directive: 'recover the committed row' },
      });

      expect(persistedDeliveryIntents(ledger.directory)).toEqual([intent]);
      expect(appendChecks).toBe(1);
      expect(flushChecks).toBe(2);
      expect(directoryChecks).toBe(barrier === 'directory' ? 2 : 1);
    },
  );

  it('re-establishes file and directory durability barriers before returning an existing intent', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    let appendChecks = 0;
    let flushChecks = 0;
    let directoryChecks = 0;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-existing-intent-durability', {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name !== 'decisions.jsonl') return;
          if (operation === 'append') appendChecks += 1;
          if (operation === 'flush') flushChecks += 1;
        },
        beforeDirectorySync: ({ name }) => {
          if (name === 'decisions.jsonl') directoryChecks += 1;
        },
      },
    });
    const input = {
      idempotency_key: 'existing-intent-durability',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 2,
      payload: { directive: 'confirm the persisted durability barrier' },
    } as const;
    const original = prepareDelivery(ledger, input);
    const afterFirst = { appendChecks, flushChecks, directoryChecks };

    const recovered = prepareDelivery(ledger, input);

    expect(recovered).toEqual(original);
    expect(appendChecks).toBe(afterFirst.appendChecks);
    expect(flushChecks).toBe(afterFirst.flushChecks + 1);
    expect(directoryChecks).toBe(afterFirst.directoryChecks + 1);
  });

  it('rejects an existing intent when its pathname rotates during the retry durability barrier', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-existing-intent-rotates-before-flush', { create: true });
    const input = {
      idempotency_key: 'existing-intent-rotates-before-flush',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 2,
      payload: { directive: 'require the same durable inode on retry' },
    } as const;
    const original = prepareDelivery(ledger, input);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const flush = ledger.flushFlatFile.bind(ledger);
    let rotated = false;
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile').mockImplementation((name) => {
      const result = flush(name);
      if (name === 'decisions.jsonl' && !rotated) {
        fs.renameSync(ledgerPath, path.join(ledger.directory, 'decisions.rotated.jsonl'));
        fs.writeFileSync(ledgerPath, '{"kind":"rotation_seed"}\n', { mode: 0o600 });
        rotated = true;
      }
      return result;
    });

    let failure: OutboxFailure;
    try {
      failure = captureFailure(() => prepareDelivery(ledger, input));
    } finally {
      flushSpy.mockRestore();
    }

    expect(rotated).toBe(true);
    expect(failure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
    expect(fs.readFileSync(ledgerPath, 'utf8')).not.toContain(original.delivery_id);
  });

  it.each(['new', 'existing'] as const)(
    'preserves committed outbox semantics when the %s-intent callback cannot release its lock',
    async (branch) => {
      const { prepareDelivery } = await import('../autoloop/outbox.js');
      const workspace = makeWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, `run-${branch}-intent-release-failure`, { create: true });
      const input = {
        idempotency_key: `${branch}-intent-release-failure`,
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 2,
        payload: { directive: 'retain committed identity through cleanup failure' },
      } as const;
      const original = branch === 'existing' ? prepareDelivery(ledger, input) : undefined;
      const lockPath = path.join(ledger.directory, '.delivery-outbox.lock');
      const rmSync = fs.rmSync.bind(fs);
      const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
        if (target === lockPath) throw new Error('injected permanent lock release failure');
        return rmSync(target, options);
      }) as typeof fs.rmSync);
      syncBuiltinESMExports();

      let failure: OutboxFailure;
      try {
        failure = captureFailure(() => prepareDelivery(ledger, input));
      } finally {
        rmSpy.mockRestore();
        syncBuiltinESMExports();
      }

      expect(failure).toMatchObject({
        name: 'AutoloopDeliveryOutboxError',
        code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
        committed: true,
        retryable: false,
      });
      expect(failure.cause).toBeInstanceOf(Error);
      const persisted = persistedDeliveryIntents(ledger.directory);
      expect(persisted).toHaveLength(1);
      if (original) expect(persisted[0].delivery_id).toBe(original.delivery_id);
      else expect(persisted[0].delivery_id).toEqual(expect.any(String));
    },
  );

  it('removes a published primary lock when its temporary alias cleanup fails', () => {
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'published-alias-unlink-failure.lock');
    const originalUnlinkSync = fs.unlinkSync;
    const injectedFailure = new Error('injected temporary alias unlink failure');
    let injected = false;
    let entries = 0;
    const unlinkSpy = vi.spyOn(fs, 'unlinkSync').mockImplementation(((target: fs.PathLike) => {
      if (!injected && String(target).startsWith(`${lockPath}.owner-`) && fs.existsSync(lockPath)) {
        injected = true;
        throw injectedFailure;
      }
      return originalUnlinkSync(target);
    }) as typeof fs.unlinkSync);
    syncBuiltinESMExports();

    let result: ReturnType<typeof withFileLock<string>>;
    try {
      result = withFileLock(
        lockPath,
        () => {
          entries += 1;
          return 'must-not-enter-after-failed-publication-cleanup';
        },
        { waitMs: 25 },
      );
    } finally {
      unlinkSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(injected).toBe(true);
    expect(result!).toMatchObject({ ok: false, reason: 'contended' });
    expect(entries).toBe(0);
    expect(fs.existsSync(lockPath)).toBe(false);
  });

  it('returns a terminal non-committed failure when alias and exact primary lock cleanup both fail', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-published-alias-and-primary-cleanup-failure', {
      create: true,
    });
    const lockPath = path.join(ledger.directory, '.delivery-outbox.lock');
    const input = {
      idempotency_key: 'published-alias-and-primary-cleanup-failure',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 2,
      payload: { directive: 'must not run after terminal lock cleanup failure' },
    } as const;
    const originalUnlinkSync = fs.unlinkSync;
    const originalRmSync = fs.rmSync;
    const aliasFailure = new Error('injected temporary alias unlink failure');
    const releaseFailure = new Error('injected exact primary lock release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    let aliasUnlinkFailed = false;
    let primaryReleaseAttempts = 0;
    const unlinkSpy = vi.spyOn(fs, 'unlinkSync').mockImplementation(((target: fs.PathLike) => {
      if (!aliasUnlinkFailed && String(target).startsWith(`${lockPath}.owner-`) && fs.existsSync(lockPath)) {
        aliasUnlinkFailed = true;
        throw aliasFailure;
      }
      return originalUnlinkSync(target);
    }) as typeof fs.unlinkSync);
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (target === lockPath) {
        primaryReleaseAttempts += 1;
        throw releaseFailure;
      }
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    let failure: OutboxFailure;
    try {
      failure = captureFailure(() => prepareDelivery(ledger, input));
    } finally {
      rmSpy.mockRestore();
      unlinkSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(aliasUnlinkFailed).toBe(true);
    expect(primaryReleaseAttempts).toBe(3);
    expect(failure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CLEANUP_FAILED',
      retryable: false,
    });
    expect(failure).not.toHaveProperty('committed');
    expect(failure.cause).toMatchObject({ name: 'FileLockReleaseError', cause: releaseFailure });
    expect(fs.existsSync(lockPath)).toBe(true);
    expect(fs.existsSync(path.join(ledger.directory, 'decisions.jsonl'))).toBe(false);
  });

  it('does not perform a fallible descriptor identity read after publishing the primary lock', () => {
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'published-fstat-failure.lock');
    const originalFstatSync = fs.fstatSync;
    const injectedFailure = new Error('injected post-publication descriptor identity failure');
    let injected = false;
    let entries = 0;
    const fstatSpy = vi.spyOn(fs, 'fstatSync').mockImplementation(((fd: number) => {
      const stat = originalFstatSync(fd);
      if (!injected && fs.existsSync(lockPath)) {
        injected = true;
        throw injectedFailure;
      }
      return stat;
    }) as typeof fs.fstatSync);
    syncBuiltinESMExports();

    let result: ReturnType<typeof withFileLock<string>>;
    try {
      result = withFileLock(lockPath, () => {
        entries += 1;
        return 'entered-after-prepublication-identity-capture';
      });
    } finally {
      fstatSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(injected).toBe(false);
    expect(result!).toEqual({ ok: true, value: 'entered-after-prepublication-identity-capture' });
    expect(entries).toBe(1);
    expect(fs.existsSync(lockPath)).toBe(false);
  });

  it('does not reclaim an aged lock held by a live child whose record omits boot identity', async () => {
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'partial-owner.lock');
    const child = spawn(
      process.execPath,
      [
        '--input-type=module',
        '--eval',
        `import fs from 'node:fs'; const stat = fs.readFileSync('/proc/self/stat', 'utf8'); const fields = stat.slice(stat.lastIndexOf(')') + 1).trim().split(/\\s+/); fs.writeFileSync(${JSON.stringify(lockPath)}, JSON.stringify({ pid: process.pid, processStartTicks: fields[19] })); process.send?.({ type: 'ready' }); setInterval(() => {}, 1_000);`,
      ],
      { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] },
    );
    try {
      await nextWorkerMessage(child);
      const staleAt = new Date(Date.now() - 120_000);
      fs.utimesSync(lockPath, staleAt, staleAt);
      const result = withFileLock(lockPath, () => 'entered', { staleMs: 1, waitMs: 25 });

      expect(result).toMatchObject({ ok: false, reason: 'contended' });
      expect(fs.existsSync(lockPath)).toBe(true);
    } finally {
      child.kill();
    }
  });

  it('does not let a second stale breaker acquire while a cooperating reclaim claim fences the pathname', () => {
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'two-breaker.lock');
    const reclaimPath = `${lockPath}.reclaim`;
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    fs.writeFileSync(reclaimPath, JSON.stringify({ pid: process.pid }), { mode: 0o600 });
    const staleAt = new Date(Date.now() - 120_000);
    fs.utimesSync(lockPath, staleAt, staleAt);
    const result = withFileLock(lockPath, () => 'second-breaker-entered', { staleMs: 1, waitMs: 25 });

    expect(result).toMatchObject({ ok: false, reason: 'contended' });
    expect(fs.existsSync(lockPath)).toBe(true);
    expect(fs.existsSync(reclaimPath)).toBe(true);
  });

  it('serializes two reclaimers and a normal acquirer after a stale reclamation crash', async () => {
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'three-process-reclamation.lock');
    const reclaimPath = `${lockPath}.reclaim`;
    const effectPath = path.join(workspace, 'three-process-effect');
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    const staleAt = new Date(Date.now() - 120_000);
    fs.utimesSync(lockPath, staleAt, staleAt);
    fs.utimesSync(reclaimPath, staleAt, staleAt);
    const workers = [
      spawnReclamationRaceWorker(lockPath, effectPath, 'reclaimer-a'),
      spawnReclamationRaceWorker(lockPath, effectPath, 'reclaimer-b'),
      spawnReclamationRaceWorker(lockPath, effectPath, 'normal-acquirer'),
    ];

    try {
      const ready = await Promise.all(workers.map((worker) => nextWorkerMessage(worker)));
      expect(ready.map((message) => message.role).sort()).toEqual(['normal-acquirer', 'reclaimer-a', 'reclaimer-b']);

      for (const worker of workers) worker.send('go');
      const results = await Promise.all(workers.map((worker) => nextWorkerMessage(worker)));
      expect(results.every((message) => message.type === 'result')).toBe(true);
      const durableWriters = results.filter(
        (message) =>
          typeof message.result === 'object' &&
          message.result !== null &&
          !Array.isArray(message.result) &&
          (message.result as { ok?: unknown; value?: { wroteEffect?: unknown } }).ok === true &&
          (message.result as { value?: { wroteEffect?: unknown } }).value?.wroteEffect === true,
      );
      expect(durableWriters).toHaveLength(1);
      expect(fs.readFileSync(effectPath, 'utf8')).toBe('exactly-one-durable-effect');
      expect(fs.existsSync(`${effectPath}.critical`)).toBe(false);
    } finally {
      for (const worker of workers) {
        if (worker.exitCode === null) worker.kill();
      }
      await Promise.all(workers.map((worker) => (worker.exitCode === null ? once(worker, 'exit') : undefined)));
    }
  });

  it.each(['before renaming the dead primary', 'after renaming the dead primary'] as const)(
    'recovers a dead stale reclamation claim left by a crash %s',
    (crashPoint) => {
      const workspace = makeWorkspace();
      const lockPath = path.join(workspace, 'crashed-reclaimer.lock');
      const reclaimPath = `${lockPath}.reclaim`;
      fs.writeFileSync(lockPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
      fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
      const staleAt = new Date(Date.now() - 120_000);
      fs.utimesSync(lockPath, staleAt, staleAt);
      fs.utimesSync(reclaimPath, staleAt, staleAt);
      if (crashPoint === 'after renaming the dead primary') {
        fs.renameSync(lockPath, `${lockPath}.stale.crashed-reclaimer`);
      }

      let entries = 0;
      const result = withFileLock(
        lockPath,
        () => {
          entries += 1;
          return 'recovered-after-crashed-reclaimer';
        },
        { staleMs: 1, waitMs: 120 },
      );

      expect(result).toEqual({ ok: true, value: 'recovered-after-crashed-reclaimer' });
      expect(entries).toBe(1);
    },
  );

  it('does not evict a live primary while a separate dead reclamation claim is recoverable', () => {
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'live-primary-with-dead-reclaim.lock');
    const reclaimPath = `${lockPath}.reclaim`;
    fs.writeFileSync(lockPath, JSON.stringify({ pid: process.pid }), { mode: 0o600 });
    fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    const staleAt = new Date(Date.now() - 120_000);
    fs.utimesSync(lockPath, staleAt, staleAt);
    fs.utimesSync(reclaimPath, staleAt, staleAt);
    const primaryIdentity = fs.statSync(lockPath);

    const result = withFileLock(lockPath, () => 'must-not-enter', { staleMs: 1, waitMs: 80 });

    expect(result).toMatchObject({ ok: false, reason: 'contended' });
    expect(fs.statSync(lockPath)).toMatchObject({ dev: primaryIdentity.dev, ino: primaryIdentity.ino });
  });

  it('returns cleanup_failed without entering the callback when releasing an adopted reclaim claim fails', () => {
    // This catches the branch that promotes an adopted reclaim claim's primary
    // lock even after it can no longer prove cleanup of the adopted claim.
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'adopted-reclaim-cleanup-failure.lock');
    const reclaimPath = `${lockPath}.reclaim`;
    fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    fs.utimesSync(reclaimPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error('injected adopted reclaim release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (String(target).startsWith(`${reclaimPath}.adopt-`)) throw releaseFailure;
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    let entries = 0;
    let result: ReturnType<typeof withFileLock<string>>;
    try {
      result = withFileLock(
        lockPath,
        () => {
          entries += 1;
          return 'must-not-enter-after-adopted-reclaim-cleanup-failure';
        },
        { staleMs: 1, waitMs: 120 },
      );
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
    }

    const cleanupResult = result!;
    expect(cleanupResult).toMatchObject({ ok: false, reason: 'cleanup_failed' });
    if (cleanupResult.ok || cleanupResult.reason !== 'cleanup_failed') {
      throw new Error('expected adopted reclaim cleanup failure');
    }
    expect(cleanupResult.cause.cause).toBe(releaseFailure);
    expect(entries).toBe(0);
  });

  it('recovers a stale claim left when the prior reclaimer could not release it', () => {
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'reclaim-release-failure.lock');
    const reclaimPath = `${lockPath}.reclaim`;
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    fs.utimesSync(lockPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));
    const originalRmSync = fs.rmSync;
    const releaseFailure = new Error('injected reclaim release failure') as NodeJS.ErrnoException;
    releaseFailure.code = 'EBUSY';
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (target === reclaimPath) {
        throw releaseFailure;
      }
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    try {
      const result = withFileLock(lockPath, () => 'must-not-enter-before-recovery', { staleMs: 1, waitMs: 120 });
      expect(result).toMatchObject({ ok: false, reason: 'cleanup_failed' });
      if (result.ok || result.reason !== 'cleanup_failed') throw new Error('expected terminal reclaim cleanup failure');
      expect(result.cause.cause).toBe(releaseFailure);
      expect(fs.existsSync(reclaimPath)).toBe(true);
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
    }

    // The failed cleanup owner's process has now crashed.  Its claim is no
    // longer live and must be recovered by a later caller without deleting a
    // replacement claim belonging to another owner.
    fs.writeFileSync(reclaimPath, JSON.stringify({ pid: 999_999_999 }), { mode: 0o600 });
    fs.utimesSync(reclaimPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));

    expect(withFileLock(lockPath, () => 'recovered-effect', { staleMs: 1, waitMs: 120 })).toEqual({
      ok: true,
      value: 'recovered-effect',
    });
  });

  it('latches a committed row when recovery durability proof remains unavailable', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    let failFlush = true;
    let appendChecks = 0;
    let flushChecks = 0;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-persistent-barrier', {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name !== 'decisions.jsonl') return;
          if (operation === 'append') appendChecks += 1;
          if (operation === 'flush') {
            flushChecks += 1;
            if (failFlush) throw new Error('injected persistent file barrier failure');
          }
        },
      },
    });
    const input = {
      idempotency_key: 'persistent-barrier-attempt',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 2,
      payload: { directive: 'wait for proven durability' },
    } as const;
    const prepare = () => prepareDelivery(ledger, input, { now: () => new Date('2026-09-10T15:00:00.000Z') });

    const firstFailure = captureFailure(prepare);
    expect(firstFailure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
      committed: true,
      retryable: false,
      cause: expect.objectContaining({
        code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
      }),
    });
    const committedRows = persistedDeliveryIntents(ledger.directory);
    expect(committedRows).toHaveLength(1);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const afterFirstFailure = fs.readFileSync(ledgerPath);

    expect(captureFailure(prepare)).toBe(firstFailure);
    expect(fs.readFileSync(ledgerPath)).toEqual(afterFirstFailure);
    expect(appendChecks).toBe(1);

    failFlush = false;
    expect(captureFailure(prepare)).toBe(firstFailure);
    expect(persistedDeliveryIntents(ledger.directory)).toEqual(committedRows);
    expect(appendChecks).toBe(1);
    expect(flushChecks).toBe(2);
  });

  it('wraps the original committed failure when reconciliation itself fails', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    let injected = false;
    const ledgerHolder: { current?: SecureAutoloopLedger } = {};
    const ledger = SecureAutoloopLedger.open(workspace, 'run-committed-reconcile-failure', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || injected) return;
          injected = true;
          ledgerHolder.current!.appendFlatFile('decisions.jsonl', '{malformed-reconcile-row\n');
          throw new Error('injected committed directory barrier failure');
        },
      },
    });
    ledgerHolder.current = ledger;

    const caught = captureFailure(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'committed-reconcile-attempt',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 2,
        payload: { directive: 'preserve committed state' },
      }),
    );

    expect(caught).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
      committed: true,
      retryable: false,
      cause: expect.objectContaining({
        code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
      }),
    });
    expect(caught.secondaryErrors?.map((error) => error.message)).toEqual([
      expect.stringMatching(/record 2 is malformed/i),
    ]);
  });

  it('wraps the original committed failure when reconciliation finds a different persisted intent', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    let injected = false;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-committed-reconcile-mismatch', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || injected) return;
          injected = true;
          const filePath = path.join(ledger.directory, 'decisions.jsonl');
          const row = JSON.parse(fs.readFileSync(filePath, 'utf8')) as Record<string, unknown>;
          row.delivery_id = 'different-persisted-delivery-id';
          fs.writeFileSync(filePath, `${JSON.stringify(row)}\n`, { mode: 0o600 });
          throw new Error('injected committed directory barrier failure');
        },
      },
    });

    const failure = captureFailure(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'committed-reconcile-mismatch',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 2,
        payload: { directive: 'preserve the committed classification' },
      }),
    );

    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
      committed: true,
      retryable: false,
      cause: expect.objectContaining({
        code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
      }),
    });
    expect(failure.secondaryErrors).toEqual([
      expect.objectContaining({
        code: 'AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT',
        retryable: false,
      }),
    ]);
  });

  it('latches the original committed failure when recovery flush proof fails', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const originalCommitDirectoryBarrier = new Error('ORIGINAL_COMMIT_DIRECTORY_BARRIER');
    const recoveryFlushBarrier = new Error('RECOVERY_FLUSH_BARRIER');
    let originalCommitInterrupted = false;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-committed-recovery-flush-failure', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || originalCommitInterrupted) return;
          originalCommitInterrupted = true;
          throw originalCommitDirectoryBarrier;
        },
        beforeFileMutation: ({ name, operation }) => {
          if (name === 'decisions.jsonl' && operation === 'flush' && originalCommitInterrupted) {
            throw recoveryFlushBarrier;
          }
        },
      },
    });
    const prepare = () =>
      prepareDelivery(
        ledger,
        {
          idempotency_key: 'committed-recovery-flush-failure',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 2,
          payload: { directive: 'prove first-call recovery observation failure is terminal' },
        },
        { now: () => new Date('2026-09-10T15:00:00.000Z') },
      );

    const firstFailure = captureFailure(prepare);

    expect(firstFailure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
      committed: true,
      retryable: false,
      cause: expect.objectContaining({
        code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
        cause: originalCommitDirectoryBarrier,
      }),
    });
    expect(firstFailure.secondaryErrors).toEqual([
      expect.objectContaining({
        code: 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
        cause: recoveryFlushBarrier,
      }),
    ]);
    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(1);

    const retryFailure = captureFailure(prepare);
    expect(retryFailure).toBe(firstFailure);
    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(1);
  });

  it('rejects a replaced decision ledger before the committed recovery flush can expose its intent', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const barrierFailure = new Error('injected committed directory barrier failure');
    let injected = false;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-committed-recovery-flush-replacement', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || injected) return;
          injected = true;
          throw barrierFailure;
        },
      },
    });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const flush = ledger.flushFlatFile.bind(ledger);
    let replaced = false;
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile').mockImplementation((name) => {
      if (name === 'decisions.jsonl' && !replaced) {
        fs.renameSync(ledgerPath, path.join(ledger.directory, 'decisions.before-recovery-flush.jsonl'));
        fs.writeFileSync(ledgerPath, '{"kind":"recovery_flush_replacement"}\n', { mode: 0o600 });
        replaced = true;
      }
      return flush(name);
    });
    let returned: unknown;
    let failure: OutboxFailure;
    try {
      failure = captureFailure(() => {
        returned = prepareDelivery(ledger, {
          idempotency_key: 'committed-recovery-flush-replacement',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 2,
          payload: { directive: 'never expose D1 after a recovery-path replacement' },
        });
      });
    } finally {
      flushSpy.mockRestore();
    }

    expect(injected).toBe(true);
    expect(replaced).toBe(true);
    expect(returned).toBeUndefined();
    expect(failure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
      committed: true,
      retryable: false,
      cause: expect.objectContaining({
        code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
        cause: barrierFailure,
      }),
    });
    expect(persistedDeliveryIntents(ledger.directory)).toEqual([]);
  });

  it('finds a persisted delivery intent by idempotency key through a fresh ledger capability', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const writer = SecureAutoloopLedger.open(workspace, 'run-lookup', { create: true });
    const intent = prepareDelivery(writer, {
      idempotency_key: 'review-attempt-4',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 2,
      payload: { source_iter: 4, scope: ['durability', 'identity'] },
    });
    const reopened = SecureAutoloopLedger.open(workspace, 'run-lookup');

    expect(lookupByIdempotencyKey(reopened, 'review-attempt-4')).toEqual(intent);
    expect(lookupByIdempotencyKey(reopened, 'unknown-attempt')).toBeUndefined();
  });

  it('finds a persisted delivery intent in a cold process with no in-memory outbox index', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const runId = 'run-cold-process-lookup';
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'cold-process-review-attempt',
      kind: 'review_request',
      target_role: 'reviewer',
      target_generation: 4,
      payload: { source_iter: 7, scope: ['cold-start'] },
    });
    const worker = spawnLookupWorker(workspace, runId, intent.idempotency_key);

    const result = await nextWorkerMessage(worker);
    const [exitCode] = worker.exitCode === null ? await once(worker, 'exit') : [worker.exitCode];

    expect(result).toEqual({ type: 'result', intent });
    expect(exitCode).toBe(0);
  });

  it.each([
    ['blank', '   '],
    ['padded', ' review-attempt-4'],
    ['oversized', 'k'.repeat(8_193)],
  ])('validates a %s lookup key before reading the decision ledger', async (_label, idempotencyKey) => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, `run-invalid-lookup-key-${_label}`, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{malformed-json\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);
    let caught: unknown;

    try {
      lookupByIdempotencyKey(ledger, idempotencyKey);
    } catch (error) {
      caught = error;
    }

    expect(caught).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_INPUT_INVALID',
      retryable: false,
    });
    expect((caught as Error).message).toMatch(/idempotency_key/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('classifies invalid public input with a stable non-retryable outbox error', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-typed-invalid-input', { create: true });

    const failure = captureFailure(() => lookupByIdempotencyKey(ledger, ' padded-key'));

    expect(failure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_INPUT_INVALID',
      retryable: false,
    });
    expect(failure).not.toHaveProperty('committed');
  });

  it('reads only newly appended decision-ledger bytes after warming a validated index', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-incremental-index-scale', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    writeLegacyDecisionLedgerOfSize(ledgerPath, 512 * 1024);
    lookupByIdempotencyKey(ledger, 'warm-the-ledger-index');
    const target = fs.statSync(ledgerPath);
    const originalReadSync = fs.readSync;
    let ledgerBytesRead = 0;
    const readSpy = vi.spyOn(fs, 'readSync').mockImplementation(((
      fd: number,
      buffer: NodeJS.ArrayBufferView,
      offset: number,
      length: number,
      position: number | null,
    ) => {
      const count = originalReadSync(fd, buffer, offset, length, position);
      const observed = fs.fstatSync(fd);
      if (observed.dev === target.dev && observed.ino === target.ino) ledgerBytesRead += count;
      return count;
    }) as typeof fs.readSync);
    syncBuiltinESMExports();

    try {
      for (let index = 0; index < 32; index += 1) {
        prepareDelivery(ledger, {
          idempotency_key: `incremental-index-attempt-${index}`,
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: index,
          payload: { directive: `bounded-${index}` },
        });
      }
    } finally {
      readSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(32);
    expect(ledgerBytesRead).toBeLessThan(256 * 1024);
  });

  it('revalidates a warm cache when a same-length rewrite changes an older row outside its cached tail', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-cached-prefix-rewrite', { create: true });
    const input = {
      idempotency_key: 'cached-prefix-rewrite',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 8,
      payload: { directive: 'old durable identity' },
    } as const;
    const original = prepareDelivery(ledger, input);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    ledger.appendFlatFile('decisions.jsonl', `${'{"kind":"legacy_seed","padding":"'}${'x'.repeat(8 * 1024)}"}\n`, true);
    expect(lookupByIdempotencyKey(ledger, input.idempotency_key)).toEqual(original);

    const rewritten = fs.readFileSync(ledgerPath, 'utf8');
    const replacementId = `${original.delivery_id[0] === 'a' ? 'b' : 'a'}${original.delivery_id.slice(1)}`;
    const replacement = rewritten.replace(original.delivery_id, replacementId);
    expect(Buffer.byteLength(replacement, 'utf8')).toBe(Buffer.byteLength(rewritten, 'utf8'));
    fs.writeFileSync(ledgerPath, replacement, { mode: 0o600 });
    fs.utimesSync(ledgerPath, new Date('2026-09-10T20:00:00.000Z'), new Date('2026-09-10T20:00:00.000Z'));

    const cold = SecureAutoloopLedger.open(workspace, 'run-cached-prefix-rewrite');
    const coldIntent = lookupByIdempotencyKey(cold, input.idempotency_key);
    const warmIntent = lookupByIdempotencyKey(ledger, input.idempotency_key);

    expect(coldIntent?.delivery_id).toBe(replacementId);
    expect(warmIntent).toEqual(coldIntent);
  });

  it('does not bypass a committed-observation terminal latch through a lexical ledger-directory alias', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const runId = 'run-committed-observation-alias';
    let removed = false;
    let appends = 0;
    const ledger = SecureAutoloopLedger.open(workspace, runId, {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name === 'decisions.jsonl' && operation === 'append') appends += 1;
        },
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || removed) return;
          removed = true;
          fs.unlinkSync(path.join(ledger.directory, 'decisions.jsonl'));
        },
      },
    });
    const input = {
      idempotency_key: 'committed-observation-alias',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 8,
      payload: { directive: 'do not mint D2 through a lexical alias' },
    } as const;

    const initialFailure = captureFailure(() => prepareDelivery(ledger, input));
    const alias = path.relative(process.cwd(), workspace);
    expect(alias).not.toBe('');
    const reopened = SecureAutoloopLedger.open(alias, runId);
    const retryFailure = captureFailure(() => prepareDelivery(reopened, input));

    for (const failure of [initialFailure, retryFailure]) {
      expect(failure).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
        committed: true,
        retryable: false,
      });
    }
    expect(appends).toBe(1);
  });

  it('establishes a fresh durability barrier for an identical-byte pathname inode replacement after pinned observation', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-post-pinned-inode-replacement', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const originalPrepare = ledger.prepareFlatFileAppend.bind(ledger);
    let swapped = false;
    const prepareSpy = vi.spyOn(ledger, 'prepareFlatFileAppend').mockImplementation((name, content) => {
      const prepared = originalPrepare(name, content);
      const readLastLine = prepared.readLastNonEmptyLine.bind(prepared);
      vi.spyOn(prepared, 'readLastNonEmptyLine').mockImplementation(() => {
        const line = readLastLine();
        const bytes = fs.readFileSync(ledgerPath);
        const replacementPath = path.join(ledger.directory, 'decisions.replacement.jsonl');
        fs.writeFileSync(replacementPath, bytes, { mode: 0o600 });
        fs.renameSync(replacementPath, ledgerPath);
        swapped = true;
        return line;
      });
      return prepared;
    });
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile');

    try {
      const intent = prepareDelivery(ledger, {
        idempotency_key: 'post-pinned-inode-replacement',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 8,
        payload: { directive: 'make the replacement durable before return' },
      });

      expect(intent.idempotency_key).toBe('post-pinned-inode-replacement');
    } finally {
      prepareSpy.mockRestore();
    }

    expect(swapped).toBe(true);
    expect(flushSpy).toHaveBeenCalledWith('decisions.jsonl');
  });

  it('rejects an identical-byte pathname inode replacement after the durability barrier', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-post-barrier-inode-replacement', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const originalFlush = ledger.flushFlatFile.bind(ledger);
    let swapped = false;
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile').mockImplementation((name) => {
      const flushed = originalFlush(name);
      if (name !== 'decisions.jsonl' || swapped) return flushed;
      const replacementPath = path.join(ledger.directory, 'decisions.post-barrier-replacement.jsonl');
      fs.writeFileSync(replacementPath, fs.readFileSync(ledgerPath), { mode: 0o600 });
      fs.renameSync(replacementPath, ledgerPath);
      swapped = true;
      return flushed;
    });

    try {
      const failure = captureFailure(() =>
        prepareDelivery(ledger, {
          idempotency_key: 'post-barrier-inode-replacement',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 8,
          payload: { directive: 'reject an inode that was not flushed' },
        }),
      );

      expect(failure).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
        committed: true,
        retryable: false,
      });
    } finally {
      flushSpy.mockRestore();
    }

    expect(swapped).toBe(true);
  });

  it('rejects ABA pathname restoration when the durability barrier flushed a different descriptor', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-aba-barrier-descriptor', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const originalFlush = ledger.flushFlatFile.bind(ledger);
    let swapped = false;
    const flushSpy = vi.spyOn(ledger, 'flushFlatFile').mockImplementation((name) => {
      if (name !== 'decisions.jsonl' || swapped) return originalFlush(name);
      const originalPath = path.join(ledger.directory, 'decisions.before-barrier.jsonl');
      const flushedPath = path.join(ledger.directory, 'decisions.flushed-barrier.jsonl');
      fs.renameSync(ledgerPath, originalPath);
      fs.writeFileSync(ledgerPath, fs.readFileSync(originalPath), { mode: 0o600 });
      const flushed = originalFlush(name);
      fs.renameSync(ledgerPath, flushedPath);
      fs.renameSync(originalPath, ledgerPath);
      swapped = true;
      return flushed;
    });

    try {
      const failure = captureFailure(() =>
        prepareDelivery(ledger, {
          idempotency_key: 'aba-barrier-descriptor',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 8,
          payload: { directive: 'reject a durable barrier for another descriptor' },
        }),
      );

      expect(failure).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
        committed: true,
        retryable: false,
      });
    } finally {
      flushSpy.mockRestore();
    }

    expect(swapped).toBe(true);
  });

  it('invalidates a warm cached prefix when metadata changes before decision-ledger growth', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-cached-prefix-rewrite-before-growth', { create: true });
    const input = {
      idempotency_key: 'cached-prefix-rewrite-before-growth',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 8,
      payload: { directive: 'old durable identity before growth' },
    } as const;
    const original = prepareDelivery(ledger, input);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    ledger.appendFlatFile('decisions.jsonl', `${'{"kind":"legacy_seed","padding":"'}${'x'.repeat(8 * 1024)}"}\n`, true);
    expect(lookupByIdempotencyKey(ledger, input.idempotency_key)).toEqual(original);

    const rewritten = fs.readFileSync(ledgerPath, 'utf8');
    const replacementId = `${original.delivery_id[0] === 'a' ? 'b' : 'a'}${original.delivery_id.slice(1)}`;
    const replacement = rewritten.replace(original.delivery_id, replacementId);
    expect(Buffer.byteLength(replacement, 'utf8')).toBe(Buffer.byteLength(rewritten, 'utf8'));
    fs.writeFileSync(ledgerPath, replacement, { mode: 0o600 });
    fs.utimesSync(ledgerPath, new Date('2026-09-10T20:00:00.000Z'), new Date('2026-09-10T20:00:00.000Z'));
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_growth"}\n', true);

    const cold = SecureAutoloopLedger.open(workspace, 'run-cached-prefix-rewrite-before-growth');
    const coldIntent = lookupByIdempotencyKey(cold, input.idempotency_key);
    const warmIntent = lookupByIdempotencyKey(ledger, input.idempotency_key);

    expect(coldIntent?.delivery_id).toBe(replacementId);
    expect(warmIntent).toEqual(coldIntent);
  });

  it('latches a committed directory-sync failure whose reconciliation finds no delivery row', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    let removed = false;
    let appends = 0;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-committed-missing-row-latch', {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name === 'decisions.jsonl' && operation === 'append') appends += 1;
        },
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || removed) return;
          removed = true;
          fs.unlinkSync(path.join(ledger.directory, 'decisions.jsonl'));
          throw new Error('injected committed directory sync failure after row removal');
        },
      },
    });
    const input = {
      idempotency_key: 'committed-missing-row-latch',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 8,
      payload: { directive: 'do not mint D2 after missing-row reconciliation' },
    } as const;

    const initialFailure = captureFailure(() => prepareDelivery(ledger, input));
    const retryFailure = captureFailure(() => prepareDelivery(ledger, input));

    expect(initialFailure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
      committed: true,
      retryable: false,
      cause: expect.objectContaining({
        code: 'AUTOLOOP_LEDGER_DIRECTORY_SYNC_INCOMPLETE',
        committed: true,
        retryable: false,
      }),
    });
    expect(retryFailure).toBe(initialFailure);
    expect(appends).toBe(1);
  });

  it('observes a first durable append before rejecting a same-key retry after deletion', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-first-write-history-observation', { create: true });
    const input = {
      idempotency_key: 'first-write-history-observation',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 8,
      payload: { directive: 'first durable identity' },
    } as const;

    const original = prepareDelivery(ledger, input);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    fs.unlinkSync(ledgerPath);
    const appendSpy = vi.spyOn(ledger, 'appendFlatFile');

    const failure = captureFailure(() => prepareDelivery(ledger, input));

    expect(original.delivery_id).toEqual(expect.any(String));
    expect(failure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
    expect(appendSpy).not.toHaveBeenCalled();
    expect(fs.existsSync(ledgerPath)).toBe(false);
  });

  it('fails closed when a same-size valid row replaces the durable append before observation', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    let replaced = false;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-post-append-same-size-rewrite', {
      create: true,
      testHooks: {
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || replaced) return;
          replaced = true;
          const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
          const original = fs.readFileSync(ledgerPath, 'utf8');
          const replacement = JSON.parse(original) as Record<string, unknown>;
          const deliveryId = replacement.delivery_id as string;
          replacement.delivery_id = `${deliveryId[0] === 'a' ? 'b' : 'a'}${deliveryId.slice(1)}`;
          const rewritten = `${JSON.stringify(replacement)}\n`;
          expect(Buffer.byteLength(rewritten, 'utf8')).toBe(Buffer.byteLength(original, 'utf8'));
          fs.writeFileSync(ledgerPath, rewritten, { mode: 0o600 });
        },
      },
    });
    const input = {
      idempotency_key: 'post-append-same-size-rewrite',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 8,
      payload: { directive: 'prove the exact committed range' },
    } as const;

    const failure = captureFailure(() => prepareDelivery(ledger, input));
    const cold = SecureAutoloopLedger.open(workspace, 'run-post-append-same-size-rewrite');

    expect(failure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
      committed: true,
      retryable: false,
    });
    expect(lookupByIdempotencyKey(cold, input.idempotency_key)?.delivery_id).not.toBeUndefined();
  });

  it('preserves a terminal committed observation failure after deletion before observation', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    let deleted = false;
    let appends = 0;
    const ledger = SecureAutoloopLedger.open(workspace, 'run-post-append-deletion', {
      create: true,
      testHooks: {
        beforeFileMutation: ({ name, operation }) => {
          if (name === 'decisions.jsonl' && operation === 'append') appends += 1;
        },
        beforeDirectorySync: ({ name }) => {
          if (name !== 'decisions.jsonl' || deleted) return;
          deleted = true;
          fs.unlinkSync(path.join(ledger.directory, 'decisions.jsonl'));
        },
      },
    });
    const input = {
      idempotency_key: 'post-append-deletion',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 8,
      payload: { directive: 'do not mint D2 after a committed write' },
    } as const;

    const firstFailure = captureFailure(() => prepareDelivery(ledger, input));
    const reopened = SecureAutoloopLedger.open(workspace, 'run-post-append-deletion');
    const retryFailure = captureFailure(() => prepareDelivery(reopened, input));

    for (const failure of [firstFailure, retryFailure]) {
      expect(failure).toMatchObject({
        name: 'AutoloopDeliveryOutboxError',
        code: 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED',
        committed: true,
        retryable: false,
      });
    }
    expect(appends).toBe(1);
    expect(fs.existsSync(path.join(ledger.directory, 'decisions.jsonl'))).toBe(false);
  });

  it.each(['deletion', 'rotation', 'truncation', 'same-size tail rewrite'] as const)(
    'does not mint a second delivery identity after observed ledger %s',
    async (destructiveChange) => {
      const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
      const workspace = makeWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, `run-observed-history-${destructiveChange}`, {
        create: true,
      });
      const input = {
        idempotency_key: `observed-history-${destructiveChange}`,
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 8,
        payload: { directive: 'initial durable identity' },
      } as const;
      const original = prepareDelivery(ledger, input);
      expect(lookupByIdempotencyKey(ledger, input.idempotency_key)).toEqual(original);

      const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
      if (destructiveChange === 'deletion') {
        fs.unlinkSync(ledgerPath);
      } else if (destructiveChange === 'rotation') {
        fs.renameSync(ledgerPath, path.join(ledger.directory, 'decisions.rotated.jsonl'));
        fs.writeFileSync(ledgerPath, '{"kind":"rotation_seed"}\n', { mode: 0o600 });
      } else if (destructiveChange === 'truncation') {
        fs.truncateSync(ledgerPath, 0);
        fs.appendFileSync(ledgerPath, '{"kind":"truncation_seed"}\n');
      } else {
        const rewritten = fs.readFileSync(ledgerPath);
        const marker = Buffer.from('initial durable identity');
        const markerOffset = rewritten.indexOf(marker);
        expect(markerOffset).toBeGreaterThanOrEqual(0);
        rewritten[markerOffset] = 'I'.charCodeAt(0);
        const digestOffset = rewritten.indexOf(Buffer.from(original.payload_sha256));
        expect(digestOffset).toBeGreaterThanOrEqual(0);
        const rewrittenDigest = createHash('sha256')
          .update('{"directive":"Initial durable identity"}', 'utf8')
          .digest('hex');
        rewritten.write(rewrittenDigest, digestOffset, 'utf8');
        fs.writeFileSync(ledgerPath, rewritten, { mode: 0o600 });
      }
      const beforeRetry = fs.existsSync(ledgerPath) ? fs.readFileSync(ledgerPath) : undefined;

      const failure = captureFailure(() => prepareDelivery(ledger, input));

      expect(failure).toMatchObject({
        name: 'AutoloopDeliveryOutboxError',
        code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
        retryable: false,
      });
      expect(fs.existsSync(ledgerPath) ? fs.readFileSync(ledgerPath) : undefined).toEqual(beforeRetry);
    },
  );

  it('does not let a returned lookup value mutate a later indexed lookup', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-index-result-isolation', { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'index-result-isolation',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'persisted value' },
    });
    const first = lookupByIdempotencyKey(ledger, intent.idempotency_key)!;
    (first.payload as { directive: string }).directive = 'caller mutation';

    const second = lookupByIdempotencyKey(ledger, intent.idempotency_key)!;

    expect(second.payload).toEqual({ directive: 'persisted value' });
    expect(second.payload_sha256).toBe(intent.payload_sha256);
  });

  it('rejects a mixed-writer append that changes its stable read snapshot', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const writer = SecureAutoloopLedger.open(workspace, 'run-mixed-writer-snapshot', { create: true });
    writer.appendFlatFile('decisions.jsonl', '{"kind":"snapshot_seed"}\n', true);
    const reader = SecureAutoloopLedger.open(workspace, 'run-mixed-writer-snapshot');
    const ledgerPath = path.join(reader.directory, 'decisions.jsonl');
    const target = fs.statSync(ledgerPath);
    const originalReadSync = fs.readSync;
    let appended = false;
    const readSpy = vi.spyOn(fs, 'readSync').mockImplementation(((
      fd: number,
      buffer: NodeJS.ArrayBufferView,
      offset: number,
      length: number,
      position: number | null,
    ) => {
      const count = originalReadSync(fd, buffer, offset, length, position);
      const observed = fs.fstatSync(fd);
      if (!appended && count > 0 && observed.dev === target.dev && observed.ino === target.ino) {
        appended = true;
        writer.appendFlatFile('decisions.jsonl', '{"kind":"concurrent_mixed_writer"}\n', true);
      }
      return count;
    }) as typeof fs.readSync);
    syncBuiltinESMExports();

    try {
      expect(() => lookupByIdempotencyKey(reader, 'missing')).toThrow(/changed while.*snapshot|stable snapshot/i);
    } finally {
      readSpy.mockRestore();
      syncBuiltinESMExports();
    }
    expect(appended).toBe(true);
    expect(fs.readFileSync(ledgerPath, 'utf8')).toBe('{"kind":"snapshot_seed"}\n{"kind":"concurrent_mixed_writer"}\n');
  });

  it('fails closed with a stable typed STOP on invalid UTF-8 without changing the ledger', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-invalid-utf8', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const bytes = Buffer.from([0x7b, 0x22, 0x6b, 0x22, 0x3a, 0xff, 0x7d, 0x0a]);
    fs.writeFileSync(ledgerPath, bytes, { mode: 0o600 });

    const failure = captureFailure(() => lookupByIdempotencyKey(ledger, 'missing'));

    expect(failure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
    expect(failure).not.toHaveProperty('committed');
    expect(fs.readFileSync(ledgerPath)).toEqual(bytes);
  });

  it('fails closed with a stable typed STOP when a validated snapshot read returns zero bytes', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-zero-byte-short-read', { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"short_read_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const target = fs.statSync(ledgerPath);
    const originalReadSync = fs.readSync;
    let injected = false;
    const readSpy = vi.spyOn(fs, 'readSync').mockImplementation(((
      fd: number,
      buffer: NodeJS.ArrayBufferView,
      offset: number,
      length: number,
      position: number | null,
    ) => {
      const observed = fs.fstatSync(fd);
      if (!injected && observed.dev === target.dev && observed.ino === target.ino && length > 0) {
        injected = true;
        return 0;
      }
      return originalReadSync(fd, buffer, offset, length, position);
    }) as typeof fs.readSync);
    syncBuiltinESMExports();

    let failure: OutboxFailure;
    try {
      failure = captureFailure(() => lookupByIdempotencyKey(ledger, 'missing'));
    } finally {
      readSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(injected).toBe(true);
    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
  });

  it('fails closed when the decision-ledger device or inode changes across one read snapshot', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-device-inode-change', { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"identity_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const target = fs.statSync(ledgerPath);
    const originalFstatSync = fs.fstatSync;
    let targetCalls = 0;
    const fstatSpy = vi.spyOn(fs, 'fstatSync').mockImplementation(((fd: number) => {
      const observed = originalFstatSync(fd);
      if (observed.dev !== target.dev || observed.ino !== target.ino) return observed;
      targetCalls += 1;
      if (targetCalls < 3) return observed;
      return Object.assign(Object.create(Object.getPrototypeOf(observed)), observed, { ino: observed.ino + 1 });
    }) as typeof fs.fstatSync);
    syncBuiltinESMExports();

    let failure: OutboxFailure;
    try {
      failure = captureFailure(() => lookupByIdempotencyKey(ledger, 'missing'));
    } finally {
      fstatSpy.mockRestore();
      syncBuiltinESMExports();
    }

    expect(targetCalls).toBeGreaterThanOrEqual(3);
    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
    expect(failure.message).toMatch(/changed while.*snapshot|stable snapshot/i);
  });

  it('leaves a torn trailing row untouched and returns a stable typed STOP', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-torn-trailing-row', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"complete_seed"}\n', true);
    expect(lookupByIdempotencyKey(ledger, 'warm-before-torn-row')).toBeUndefined();
    fs.appendFileSync(ledgerPath, '{"kind":"coder_directive","delivery_id":"torn');
    const torn = fs.readFileSync(ledgerPath);

    const failure = captureFailure(() => lookupByIdempotencyKey(ledger, 'missing'));

    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
    expect(failure.message).toMatch(/incomplete final record/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(torn);
  });

  it('treats one reserved delivery-shaped field as a malformed intent instead of a legacy row', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-single-delivery-shaped-field', { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"delivery_id":"ambiguous-reserved-row"}\n', true);

    const failure = captureFailure(() => lookupByIdempotencyKey(ledger, 'missing'));

    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
      retryable: false,
    });
    expect(failure.message).toMatch(/record 1.*(?:delivery intent|non-empty kind)/i);
  });

  it.each([
    ['reserved kind', { kind: 'coder_directive', idempotency_key: 'malformed-delivery' }],
    [
      'reserved fields',
      {
        schema_version: 1,
        idempotency_key: 'malformed-delivery',
        target_role: 'coder',
        target_generation: 1,
        payload: { directive: 'missing identity' },
      },
    ],
  ])('fails closed on a delivery-shaped row with %s', async (_label, malformedRow) => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, `run-malformed-delivery-${_label.replace(' ', '-')}`, {
      create: true,
    });
    ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(malformedRow)}\n`, true);

    expect(() => lookupByIdempotencyKey(ledger, 'malformed-delivery')).toThrow(
      /delivery intent|delivery-shaped|non-empty kind/i,
    );
  });

  it('validates later delivery-shaped rows even after finding an earlier matching intent', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-later-poison-row', { create: true });
    const input = {
      idempotency_key: 'early-valid-attempt',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'do not hide later poison' },
    } as const;
    prepareDelivery(ledger, input);
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"review_request","idempotency_key":"later-poison"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    expect(() => lookupByIdempotencyKey(ledger, input.idempotency_key)).toThrow(/record 2.*delivery intent/i);
    expect(() => prepareDelivery(ledger, input)).toThrow(/record 2.*delivery intent/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it.each([
    ['null', null],
    ['array', []],
    ['empty object', {}],
    ['empty kind', { kind: '' }],
    ['blank kind', { kind: '   ' }],
    ['non-string kind', { kind: 42 }],
    [
      'malformed reserved acknowledgement',
      { schema_version: 1, delivery_id: 'malformed-ack', payload_sha256: 'a'.repeat(64) },
    ],
  ])(
    'fails closed on a shared decision-ledger row with %s before lookup or prepare can append',
    async (_label, row) => {
      const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
      const workspace = makeWorkspace();
      const ledger = SecureAutoloopLedger.open(
        workspace,
        `run-invalid-shared-envelope-${_label.replaceAll(' ', '-')}`,
        {
          create: true,
        },
      );
      ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(row)}\n`, true);
      const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
      const before = fs.readFileSync(ledgerPath);
      const input = {
        idempotency_key: `invalid-shared-envelope-${_label.replaceAll(' ', '-')}`,
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: { directive: 'fail closed before append' },
      } as const;

      const lookupFailure = captureFailure(() => lookupByIdempotencyKey(ledger, input.idempotency_key));
      const prepareFailure = captureFailure(() => prepareDelivery(ledger, input));

      for (const failure of [lookupFailure, prepareFailure]) {
        expect(failure).toMatchObject({
          name: 'AutoloopDeliveryOutboxError',
          code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
          retryable: false,
        });
      }
      expect(fs.readFileSync(ledgerPath)).toEqual(before);
    },
  );

  it('does not let a matching intent hide a later invalid shared decision-ledger row', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-later-invalid-shared-envelope', { create: true });
    const input = {
      idempotency_key: 'early-valid-shared-envelope',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'do not hide later invalid row' },
    } as const;
    const original = prepareDelivery(ledger, input);
    ledger.appendFlatFile('decisions.jsonl', '{}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    const lookupFailure = captureFailure(() => lookupByIdempotencyKey(ledger, original.idempotency_key));
    const prepareFailure = captureFailure(() => prepareDelivery(ledger, input));

    for (const failure of [lookupFailure, prepareFailure]) {
      expect(failure).toMatchObject({
        code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID',
        retryable: false,
      });
    }
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('rejects an orphan reserved acknowledgement among otherwise compatible legacy decisions', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-compatible-rows', { create: true });
    const compatibleRows = [
      { ts: '2026-09-10T12:00:00.000Z', kind: 'legacy_seed', payload: { retained: true } },
      {
        ts: '2026-09-10T12:00:01.000Z',
        kind: 'request_review',
        actor: 'planner',
        payload: { idempotency_key: 'legacy-review' },
      },
      {
        schema_version: 1,
        delivery_id: 'delivery-future-ack',
        payload_sha256: 'a'.repeat(64),
        acknowledged_at: '2026-09-10T12:00:02.000Z',
      },
    ];
    ledger.appendFlatFile('decisions.jsonl', `${compatibleRows.map((row) => JSON.stringify(row)).join('\n')}\n`, true);

    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    const failure = captureFailure(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'coder-after-compatible-rows',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: { directive: 'continue' },
      }),
    );

    expect(failure).toMatchObject({ code: 'AUTOLOOP_DELIVERY_LEDGER_INVALID', retryable: false });
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('rejects duplicate persisted idempotency keys instead of returning the first claim', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-duplicate-key', { create: true });
    const intent = prepareDelivery(ledger, {
      idempotency_key: 'duplicate-coder-attempt',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: { directive: 'once' },
    });
    ledger.appendFlatFile('decisions.jsonl', `${JSON.stringify(intent)}\n`, true);

    const failure = captureFailure(() => lookupByIdempotencyKey(ledger, 'duplicate-coder-attempt'));
    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT',
      retryable: false,
    });
    expect(failure.message).toMatch(/duplicate|conflict/i);
  });

  it.each([
    ['blank', '   '],
    ['padded', ' coder-attempt-7'],
    ['oversized', 'k'.repeat(8_193)],
  ])('rejects a %s idempotency key without changing the decision ledger', async (_label, idempotencyKey) => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, `run-invalid-key-${_label}`, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    expect(() =>
      prepareDelivery(ledger, {
        idempotency_key: idempotencyKey,
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: { directive: 'preserve the ledger' },
      }),
    ).toThrow(/idempotency_key/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('accepts idempotency-key and generation values exactly at their public bounds', async () => {
    const { lookupByIdempotencyKey, prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-exact-input-bounds', { create: true });
    const exactKey = 'k'.repeat(8_192);

    const intent = prepareDelivery(ledger, {
      idempotency_key: exactKey,
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: Number.MAX_SAFE_INTEGER,
      payload: { directive: 'accept the exact input bounds' },
    });

    expect(intent.idempotency_key).toBe(exactKey);
    expect(intent.target_generation).toBe(Number.MAX_SAFE_INTEGER);
    expect(lookupByIdempotencyKey(ledger, exactKey)).toEqual(intent);
  });

  it.each(['idempotency_key', 'kind', 'target_role', 'target_generation', 'payload'] as const)(
    'rejects a top-level %s accessor without invoking it or changing the decision ledger',
    async (field) => {
      const { prepareDelivery } = await import('../autoloop/outbox.js');
      const workspace = makeWorkspace();
      const ledger = SecureAutoloopLedger.open(workspace, `run-input-accessor-${field}`, { create: true });
      ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
      const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
      const before = fs.readFileSync(ledgerPath);
      const input: Record<string, unknown> = {
        idempotency_key: 'accessor-coder-attempt',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: { directive: 'must not be observed through an accessor' },
      };
      const value = input[field];
      let observations = 0;
      Object.defineProperty(input, field, {
        configurable: true,
        enumerable: true,
        get: () => {
          observations += 1;
          return value;
        },
      });

      expect(() => prepareDelivery(ledger, input as never)).toThrow(/input|envelope|own.*data|accessor/i);
      expect(observations).toBe(0);
      expect(fs.readFileSync(ledgerPath)).toEqual(before);
    },
  );

  it.each([
    [
      'inherited field',
      (): InvalidInputFixture => {
        const input = Object.create({ idempotency_key: 'inherited-coder-attempt' }) as Record<string, unknown>;
        Object.assign(input, {
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 1,
          payload: { directive: 'reject inherited values' },
        });
        return { input, observations: () => 0 };
      },
    ],
    [
      'extra field',
      (): InvalidInputFixture => ({
        input: {
          idempotency_key: 'extra-field-coder-attempt',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 1,
          payload: { directive: 'reject extras' },
          extra: true,
        },
        observations: () => 0,
      }),
    ],
    [
      'symbol field',
      (): InvalidInputFixture => {
        const input: Record<PropertyKey, unknown> = {
          idempotency_key: 'symbol-field-coder-attempt',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 1,
          payload: { directive: 'reject symbols' },
        };
        input[Symbol('extra')] = true;
        return { input, observations: () => 0 };
      },
    ],
    [
      'proxy',
      (): InvalidInputFixture => {
        let observations = 0;
        const input = new Proxy(
          {
            idempotency_key: 'proxy-coder-attempt',
            kind: 'coder_directive',
            target_role: 'coder',
            target_generation: 1,
            payload: { directive: 'reject proxies before their traps run' },
          },
          {
            get(target, key, receiver) {
              observations += 1;
              return Reflect.get(target, key, receiver);
            },
            getOwnPropertyDescriptor(target, key) {
              observations += 1;
              return Reflect.getOwnPropertyDescriptor(target, key);
            },
            getPrototypeOf(target) {
              observations += 1;
              return Reflect.getPrototypeOf(target);
            },
            ownKeys(target) {
              observations += 1;
              return Reflect.ownKeys(target);
            },
          },
        );
        return { input, observations: () => observations };
      },
    ],
  ] as const)('rejects a top-level input with a %s without changing the decision ledger', async (label, makeInput) => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const runId = `run-invalid-input-${label.replace(/[^a-z]+/gi, '-')}`;
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);
    const { input, observations } = makeInput();

    expect(() => prepareDelivery(ledger, input as never)).toThrow(/input|envelope|plain|field|proxy/i);
    expect(observations()).toBe(0);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('rejects a delivery row above the shared decision-ledger ceiling before append', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-oversized-delivery-row', { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    expect(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'oversized-delivery-row',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: { directive: 'x'.repeat(1_376_257) },
      }),
    ).toThrow(/row|byte limit|oversized|exceeds/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('accepts a serialized delivery row exactly at the shared decision-ledger row ceiling', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-exact-delivery-row-bound', { create: true });

    const intent = prepareDelivery(
      ledger,
      {
        idempotency_key: 'exact-delivery-row-bound',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: { directive: 'x'.repeat(1_375_918) },
      },
      { now: () => new Date('2026-09-10T12:34:56.789Z') },
    );
    const serialized = fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8').slice(0, -1);

    expect(intent.idempotency_key).toBe('exact-delivery-row-bound');
    expect(Buffer.byteLength(serialized, 'utf8')).toBe(1_376_256);
  });

  it('rejects a decision-ledger snapshot above 64 MiB before reading it', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-oversized-decision-ledger', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    fs.writeFileSync(ledgerPath, '', { mode: 0o600 });
    fs.truncateSync(ledgerPath, 64 * 1024 * 1024 + 1);

    expect(() => lookupByIdempotencyKey(ledger, 'missing')).toThrow(/67108864-byte|64 MiB|recovery limit/i);
  });

  it('refuses an append that would grow the shared decision ledger beyond 64 MiB', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-full-decision-ledger', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const maximumLedgerBytes = 64 * 1024 * 1024;
    writeLegacyDecisionLedgerOfSize(ledgerPath, maximumLedgerBytes);

    expect(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'beyond-ledger-capacity',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: { directive: 'must not append' },
      }),
    ).toThrow(/67108864-byte|ledger.*limit|exceed/i);
    expect(fs.statSync(ledgerPath).size).toBe(maximumLedgerBytes);
  }, 15_000);

  it('rejects an existing decision-ledger row above the dispatcher-compatible byte ceiling', async () => {
    const { lookupByIdempotencyKey } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-oversized-existing-row', { create: true });
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const oversizedLegacyRow = JSON.stringify({ kind: 'legacy_seed', padding: 'x'.repeat(1_376_257) });
    fs.writeFileSync(ledgerPath, `${oversizedLegacyRow}\n`, { mode: 0o600 });

    expect(() => lookupByIdempotencyKey(ledger, 'missing')).toThrow(/record 1.*byte limit/i);
  });

  it.each([
    ['non-numeric', '1'],
    ['non-finite', Number.POSITIVE_INFINITY],
    ['fractional', 1.5],
    ['unsafe', Number.MAX_SAFE_INTEGER + 1],
    ['negative', -1],
  ])('rejects a %s target generation without changing the decision ledger', async (_label, targetGeneration) => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, `run-invalid-generation-${_label}`, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    expect(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'coder-attempt-invalid-generation',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: targetGeneration as never,
        payload: { directive: 'preserve the ledger' },
      }),
    ).toThrow(/target_generation|generation/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it.each([
    ['unknown kind', 'unknown_delivery', 'coder'],
    ['unknown role', 'coder_directive', 'planner'],
    ['Coder directive routed to Reviewer', 'coder_directive', 'reviewer'],
    ['review request routed to Coder', 'review_request', 'coder'],
  ])('rejects an %s without changing the decision ledger', async (_label, kind, targetRole) => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, `run-invalid-route-${kind}-${targetRole}`, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    expect(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'invalid-delivery-route',
        kind,
        target_role: targetRole,
        target_generation: 1,
        payload: { directive: 'preserve the ledger' },
      } as never),
    ).toThrow(/kind|role|route/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it.each([
    ['non-finite number', () => ({ value: Number.NaN })],
    ['undefined object value', () => ({ value: undefined })],
    ['undefined array value', () => [undefined]],
    [
      'sparse array',
      () => {
        const value = ['kept', 'removed'];
        Reflect.deleteProperty(value, '1');
        return value;
      },
    ],
    ['function value', () => ({ value: () => 'not JSON' })],
    ['symbol value', () => ({ value: Symbol('not JSON') })],
    ['bigint value', () => ({ value: 1n })],
    [
      'custom toJSON',
      () => ({
        safe: true,
        toJSON: () => ({ forged: true }),
      }),
    ],
    ['non-plain object', () => new Date('2026-09-10T00:00:00.000Z')],
    [
      'cyclic object',
      () => {
        const value: { self?: unknown } = {};
        value.self = value;
        return value;
      },
    ],
  ] as const)('rejects a %s payload without changing the decision ledger', async (label, makePayload) => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const runId = `run-invalid-payload-${label.replace(/[^a-z]+/gi, '-')}`;
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    expect(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'coder-attempt-invalid-payload',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: makePayload(),
      }),
    ).toThrow(/JSON|payload|serializ/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
  });

  it('defines the payload nesting bound as 64 edges and returns a typed error above it', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-payload-depth-bound', { create: true });

    const accepted = prepareDelivery(ledger, {
      idempotency_key: 'payload-depth-64',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload: nestedPayload(64),
    });
    const failure = captureFailure(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'payload-depth-65',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: nestedPayload(65),
      }),
    );

    expect(accepted.idempotency_key).toBe('payload-depth-64');
    expect(failure).toMatchObject({
      name: 'AutoloopDeliveryOutboxError',
      code: 'AUTOLOOP_DELIVERY_INPUT_INVALID',
      retryable: false,
    });
    expect(failure).not.toBeInstanceOf(RangeError);
    expect(failure.message).toMatch(/payload.*nesting|nesting.*payload|depth/i);
  });

  it('rejects a nested payload Proxy before invoking any of its traps', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-nested-payload-proxy', { create: true });
    let observations = 0;
    const nested = new Proxy(
      { directive: 'forged' },
      {
        get(target, key, receiver) {
          observations += 1;
          return Reflect.get(target, key, receiver);
        },
        getOwnPropertyDescriptor(target, key) {
          observations += 1;
          return Reflect.getOwnPropertyDescriptor(target, key);
        },
        getPrototypeOf(target) {
          observations += 1;
          return Reflect.getPrototypeOf(target);
        },
        ownKeys(target) {
          observations += 1;
          return Reflect.ownKeys(target);
        },
      },
    );

    const failure = captureFailure(() =>
      prepareDelivery(ledger, {
        idempotency_key: 'nested-payload-proxy',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 1,
        payload: { nested },
      }),
    );

    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_INPUT_INVALID',
      retryable: false,
    });
    expect(failure.message).toMatch(/proxies are unsupported/i);
    expect(observations).toBe(0);
  });

  it('accepts an array whose inert own toJSON shadow cannot alter serialization', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-safe-array-to-json-shadow', { create: true });
    const payload = ['safe', { order: 2 }];
    Object.defineProperty(payload, 'toJSON', {
      configurable: false,
      enumerable: false,
      value: undefined,
      writable: false,
    });

    const intent = prepareDelivery(ledger, {
      idempotency_key: 'safe-array-to-json-shadow',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 1,
      payload,
    });

    expect(intent.payload).toEqual(['safe', { order: 2 }]);
    expect(intent.payload_sha256).toBe('ceac701d8f3a547feac2cb3c352eb3dde1b3c829c453aa09b10821f15d1efc6a');
  });

  it.each([
    [
      'object field getter',
      () => {
        let observations = 0;
        const payload = {};
        Object.defineProperty(payload, 'directive', {
          enumerable: true,
          get: () => {
            observations += 1;
            return 'forged';
          },
        });
        return { payload, observations: () => observations };
      },
    ],
    [
      'object toJSON getter',
      () => {
        let observations = 0;
        const payload = { directive: 'safe' };
        Object.defineProperty(payload, 'toJSON', {
          enumerable: true,
          get: () => {
            observations += 1;
            return () => ({ directive: 'forged' });
          },
        });
        return { payload, observations: () => observations };
      },
    ],
    [
      'array element getter',
      () => {
        let observations = 0;
        const payload = ['safe'];
        Object.defineProperty(payload, '0', {
          configurable: true,
          enumerable: true,
          get: () => {
            observations += 1;
            return 'forged';
          },
        });
        return { payload, observations: () => observations };
      },
    ],
    [
      'array toJSON getter',
      () => {
        let observations = 0;
        const payload = ['safe'];
        Object.defineProperty(payload, 'toJSON', {
          enumerable: false,
          get: () => {
            observations += 1;
            return () => ['forged'];
          },
        });
        return { payload, observations: () => observations };
      },
    ],
  ] as const)(
    'rejects a payload %s without invoking it or changing the decision ledger',
    async (label, makePayload) => {
      const { prepareDelivery } = await import('../autoloop/outbox.js');
      const workspace = makeWorkspace();
      const runId = `run-payload-accessor-${label.replace(/[^a-z]+/gi, '-')}`;
      const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
      ledger.appendFlatFile('decisions.jsonl', '{"kind":"legacy_seed"}\n', true);
      const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
      const before = fs.readFileSync(ledgerPath);
      const { payload, observations } = makePayload();

      expect(() =>
        prepareDelivery(ledger, {
          idempotency_key: 'payload-accessor-coder-attempt',
          kind: 'coder_directive',
          target_role: 'coder',
          target_generation: 1,
          payload,
        }),
      ).toThrow(/JSON|payload|serializ/i);
      expect(observations()).toBe(0);
      expect(fs.readFileSync(ledgerPath)).toEqual(before);
    },
  );

  it('reuses the original logical dispatch identity when a caller watchdog resumes the attempt', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-resume', { create: true });
    const first = prepareDelivery(
      ledger,
      {
        idempotency_key: 'coder-watchdog-attempt-9',
        kind: 'coder_directive',
        target_role: 'coder',
        target_generation: 5,
        payload: { sequence: 9, directive: { goal: 'repair', constraints: ['preserve identity'] } },
      },
      { now: () => new Date('2026-09-10T13:00:00.000Z') },
    );

    const resumed = prepareDelivery(
      ledger,
      {
        payload: { directive: { constraints: ['preserve identity'], goal: 'repair' }, sequence: 9 },
        target_generation: 5,
        target_role: 'coder',
        kind: 'coder_directive',
        idempotency_key: 'coder-watchdog-attempt-9',
      },
      { now: () => new Date('2026-09-10T13:10:00.000Z') },
    );

    expect(resumed).toEqual(first);
    expect(resumed.delivery_id).toBe(first.delivery_id);
    expect(resumed.created_at).toBe('2026-09-10T13:00:00.000Z');
    expect(persistedDeliveryIntents(ledger.directory)).toEqual([first]);
  });

  it.each([
    ['payload', { payload: { directive: 'changed' } }],
    ['kind', { kind: 'review_request' }],
    ['role', { target_role: 'reviewer' }],
  ] as const)('rejects a same-key delivery whose %s conflicts with the persisted intent', async (_field, changed) => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, `run-conflicting-${_field}`, { create: true });
    const original = {
      idempotency_key: 'coder-conflicting-attempt',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 5,
      payload: { directive: 'original' },
    } as const;
    prepareDelivery(ledger, original);
    const ledgerPath = path.join(ledger.directory, 'decisions.jsonl');
    const before = fs.readFileSync(ledgerPath);

    const failure = captureFailure(() => prepareDelivery(ledger, { ...original, ...changed } as never));
    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_IDEMPOTENCY_CONFLICT',
      retryable: false,
    });
    expect(failure.message).toMatch(/idempotency key.*conflicts with its persisted intent/i);
    expect(fs.readFileSync(ledgerPath)).toEqual(before);
    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(1);
  });

  it('rejects a stale or lower generation rebind without changing the ledger', async () => {
    const { prepareDelivery } = await import('../autoloop/outbox.js');
    const workspace = makeWorkspace();
    const ledger = SecureAutoloopLedger.open(workspace, 'run-stale-generation-rebind', { create: true });
    const original = prepareDelivery(ledger, {
      idempotency_key: 'stale-generation-rebind',
      kind: 'coder_directive',
      target_role: 'coder',
      target_generation: 5,
      payload: { directive: 'never route an old physical generation' },
    });
    const before = fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8');

    const failure = captureFailure(() =>
      prepareDelivery(ledger, {
        idempotency_key: original.idempotency_key,
        kind: original.kind,
        target_role: original.target_role,
        target_generation: 4,
        payload: original.payload,
      }),
    );
    expect(failure).toMatchObject({
      code: 'AUTOLOOP_DELIVERY_GENERATION_REBIND_CONFLICT',
      retryable: false,
    });
    expect(fs.readFileSync(path.join(ledger.directory, 'decisions.jsonl'), 'utf8')).toEqual(before);
  });

  it('gives two concurrent callers one durable intent and one delivery identity', async () => {
    const workspace = makeWorkspace();
    const runId = 'run-concurrent';
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"concurrency_seed"}\n', true);
    const workers = [
      spawnPrepareWorker(workspace, runId, '2026-09-10T14:00:00.000Z'),
      spawnPrepareWorker(workspace, runId, '2026-09-10T14:00:01.000Z'),
    ];

    const ready = await Promise.all(workers.map((worker) => nextWorkerMessage(worker)));
    expect(ready).toEqual([{ type: 'ready' }, { type: 'ready' }]);
    const results = workers.map((worker) => nextWorkerMessage(worker));
    for (const worker of workers) worker.send('go');
    const completed = await Promise.all(results);

    expect(completed[0]).toEqual(completed[1]);
    expect(completed[0].type).toBe('result');
    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(1);
    await Promise.all(
      workers.map(async (worker) => {
        if (worker.exitCode !== null) return expect(worker.exitCode).toBe(0);
        const [code] = await once(worker, 'exit');
        expect(code).toBe(0);
      }),
    );
  }, 15_000);

  it('keeps one identity through deterministic slow durable lock contention beyond 250 ms', async () => {
    const workspace = makeWorkspace();
    const runId = 'run-slow-lock-contention';
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"slow_lock_seed"}\n', true);
    const release = path.join(workspace, 'release-slow-outbox-lock');
    const held = path.join(workspace, 'held-slow-outbox-lock');
    const attempted = path.join(workspace, 'attempted-contended-outbox-lock');
    const workers = [
      spawnPrepareWorker(workspace, runId, '2026-09-10T14:10:00.000Z', {
        heldMarker: held,
        holdAppendUntil: release,
      }),
      spawnPrepareWorker(workspace, runId, '2026-09-10T14:10:01.000Z', { attemptedMarker: attempted }),
    ];

    const ready = await Promise.all(workers.map((worker) => nextWorkerMessage(worker)));
    expect(ready).toEqual([{ type: 'ready' }, { type: 'ready' }]);
    const firstResult = nextWorkerMessage(workers[0]);
    workers[0].send('go');
    await waitForPath(held);
    const secondResult = nextWorkerMessage(workers[1]);
    workers[1].send('go');
    await waitForPath(attempted);
    await new Promise((resolve) => setTimeout(resolve, 400));
    fs.writeFileSync(release, 'release');
    const completed = await Promise.all([firstResult, secondResult]);
    await Promise.all(
      workers.map(async (worker) => {
        if (worker.exitCode !== null) return;
        await once(worker, 'exit');
      }),
    );

    expect(completed[0]).toEqual(completed[1]);
    expect(completed[0].type).toBe('result');
    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(1);
    expect(workers.map((worker) => worker.exitCode)).toEqual([0, 0]);
  }, 20_000);

  it('does not evict a proven-live aged outbox lock to mint a second delivery identity', async () => {
    const workspace = makeWorkspace();
    const runId = 'run-live-aged-outbox-lock';
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"live_aged_lock_seed"}\n', true);
    const release = path.join(workspace, 'release-live-aged-outbox-lock');
    const held = path.join(workspace, 'held-live-aged-outbox-lock');
    const holder = spawnPrepareWorker(workspace, runId, '2026-09-10T14:20:00.000Z', {
      heldMarker: held,
      holdAppendUntil: release,
      ageHeldLock: true,
    });
    const contender = spawnPrepareWorker(workspace, runId, '2026-09-10T14:20:01.000Z');
    await Promise.all([nextWorkerMessage(holder), nextWorkerMessage(contender)]);
    const holderResult = nextWorkerMessage(holder);
    holder.send('go');
    await waitForPath(held);
    const contenderResult = nextWorkerMessage(contender);
    contender.send('go');

    let contention: Record<string, unknown>;
    try {
      contention = await contenderResult;
    } finally {
      fs.writeFileSync(release, 'release');
    }
    const completed = await holderResult;
    await Promise.all(
      [holder, contender].map(async (worker) => {
        if (worker.exitCode !== null) return;
        await once(worker, 'exit');
      }),
    );

    expect(contention).toMatchObject({
      type: 'error',
      code: 'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CONTENDED',
      retryable: true,
    });
    expect(completed.type).toBe('result');
    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(1);
    expect(holder.exitCode).toBe(0);
    expect(contender.exitCode).toBe(1);
  }, 20_000);

  it('recovers a dead aged outbox lock within its bounded wait', async () => {
    const workspace = makeWorkspace();
    const runId = 'run-dead-aged-outbox-lock';
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"dead_aged_lock_seed"}\n', true);
    const release = path.join(workspace, 'release-dead-aged-outbox-lock');
    const held = path.join(workspace, 'held-dead-aged-outbox-lock');
    const holder = spawnPrepareWorker(workspace, runId, '2026-09-10T14:30:00.000Z', {
      heldMarker: held,
      holdAppendUntil: release,
      ageHeldLock: true,
    });
    await nextWorkerMessage(holder);
    holder.send('go');
    await waitForPath(held);
    holder.kill('SIGKILL');
    await once(holder, 'exit');

    const contender = spawnPrepareWorker(workspace, runId, '2026-09-10T14:30:01.000Z');
    await nextWorkerMessage(contender);
    const startedAt = Date.now();
    const result = nextWorkerMessage(contender);
    contender.send('go');
    const completed = await result;
    const elapsed = Date.now() - startedAt;
    if (contender.exitCode === null) await once(contender, 'exit');

    expect(completed.type).toBe('result');
    expect(elapsed).toBeLessThan(2_000);
    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(1);
    expect(contender.exitCode).toBe(0);
  }, 20_000);

  it('does not report a successful critical section until a transient release failure has released its owned lock', async () => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-release-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    const originalRmSync = fs.rmSync;
    let injected = false;
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (!injected && target === lockPath) {
        injected = true;
        const error = new Error('injected release-side busy lock') as NodeJS.ErrnoException;
        error.code = 'EBUSY';
        throw error;
      }
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    try {
      const result = withFileLock(lockPath, () => 'durable result');

      expect(injected).toBe(true);
      expect(result).toEqual({ ok: true, value: 'durable result' });
      expect(fs.existsSync(lockPath)).toBe(false);
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it('preserves a critical-section failure while retaining evidence when its owned lock cannot be released', async () => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-release-failure-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    const originalRmSync = fs.rmSync;
    const operationFailure = new Error('critical section failed first');
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (target === lockPath) {
        const error = new Error('injected permanent release failure') as NodeJS.ErrnoException;
        error.code = 'EBUSY';
        throw error;
      }
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();

    try {
      expect(() =>
        withFileLock(lockPath, () => {
          throw operationFailure;
        }),
      ).toThrow(operationFailure);

      expect((operationFailure as OutboxFailure).secondaryErrors).toHaveLength(1);
      expect((operationFailure as OutboxFailure).secondaryErrors![0]).toMatchObject({ name: 'FileLockReleaseError' });
      expect(fs.existsSync(lockPath)).toBe(true);
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it('fails closed for an aged zero-byte baseline lock because a proc snapshot cannot prove ownership absence', async () => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-legacy-empty-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    fs.writeFileSync(lockPath, '');
    const staleAt = new Date(Date.now() - 120_000);
    fs.utimesSync(lockPath, staleAt, staleAt);
    const originalReaddirSync = fs.readdirSync;
    const readdirSpy = vi.spyOn(fs, 'readdirSync').mockImplementation(((
      directoryPath: fs.PathLike,
      options?: unknown,
    ) => {
      if (directoryPath === '/proc') return [];
      return originalReaddirSync(directoryPath, options as never);
    }) as typeof fs.readdirSync);
    syncBuiltinESMExports();

    try {
      const result = withFileLock(lockPath, () => 'recovered', { staleMs: 1, waitMs: 80 });

      expect(result).toMatchObject({ ok: false, reason: 'contended' });
      expect(fs.statSync(lockPath).size).toBe(0);
    } finally {
      readdirSpy.mockRestore();
      syncBuiltinESMExports();
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it('does not reclaim a legacy zero-byte lock when a holder starts after the proc snapshot', async () => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-snapshot-legacy-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    const heldPath = path.join(directory, 'held');
    const releasePath = path.join(directory, 'release');
    fs.writeFileSync(lockPath, '');
    fs.utimesSync(lockPath, new Date(Date.now() - 120_000), new Date(Date.now() - 120_000));
    // This is the exact gap a one-shot proc scan cannot close: its process
    // list is already complete before a process opens this legacy inode.
    const procSnapshot: number[] = [];
    expect(procSnapshot).toEqual([]);
    const holderSource = `
      const fs = await import('node:fs');
      const fd = fs.openSync(${JSON.stringify(lockPath)}, 'r');
      fs.writeFileSync(${JSON.stringify(heldPath)}, 'held');
      while (!fs.existsSync(${JSON.stringify(releasePath)})) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2);
      fs.closeSync(fd);
    `;
    const holder = spawn(process.execPath, ['--input-type=module', '--eval', holderSource], { stdio: 'ignore' });

    try {
      await waitForPath(heldPath);
      const result = withFileLock(lockPath, () => 'must not enter', { staleMs: 1, waitMs: 100 });

      expect(result).toMatchObject({ ok: false, reason: 'contended' });
      expect(fs.statSync(lockPath).size).toBe(0);
    } finally {
      fs.writeFileSync(releasePath, 'release');
      if (holder.exitCode === null) await once(holder, 'exit');
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it('pins owner inspection to a non-following descriptor before a pathname replacement', async () => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-pinned-owner-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    const replacementPath = path.join(directory, 'replacement.lock');
    fs.writeFileSync(lockPath, JSON.stringify({ pid: process.pid }));
    fs.writeFileSync(replacementPath, JSON.stringify({ pid: process.pid }));
    const originalReadSync = fs.readSync;
    let swapped = false;
    const readSpy = vi.spyOn(fs, 'readSync').mockImplementation(((
      fd: number,
      buffer: NodeJS.ArrayBufferView,
      offset: number,
      length: number,
      position: number | null,
    ) => {
      if (!swapped) {
        swapped = true;
        fs.renameSync(replacementPath, lockPath);
      }
      return originalReadSync(fd, buffer, offset, length, position);
    }) as typeof fs.readSync);
    syncBuiltinESMExports();

    try {
      const result = withFileLock(lockPath, () => 'must not enter', { waitMs: 80 });

      expect(swapped).toBe(true);
      expect(result).toMatchObject({ ok: false, reason: 'contended' });
      expect(fs.existsSync(lockPath)).toBe(true);
    } finally {
      readSpy.mockRestore();
      syncBuiltinESMExports();
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it('opens a symlink lock path with no-follow and nonblocking descriptor flags', async () => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-symlink-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    const targetPath = path.join(directory, 'target.lock');
    fs.writeFileSync(targetPath, JSON.stringify({ pid: process.pid }));
    fs.symlinkSync(targetPath, lockPath);
    const originalOpenSync = fs.openSync;
    const flags: Array<string | number> = [];
    const openSpy = vi.spyOn(fs, 'openSync').mockImplementation(((
      target: fs.PathLike,
      flag: string | number,
      mode?: string | number,
    ) => {
      if (target === lockPath) flags.push(flag);
      return originalOpenSync(target, flag, mode);
    }) as typeof fs.openSync);
    syncBuiltinESMExports();

    try {
      const startedAt = Date.now();
      const result = withFileLock(lockPath, () => 'must not enter', { waitMs: 80 });

      expect(result).toMatchObject({ ok: false, reason: 'contended' });
      expect(Date.now() - startedAt).toBeLessThan(500);
      expect(
        flags.some(
          (flag) =>
            typeof flag === 'number' &&
            (flag & fs.constants.O_NOFOLLOW) !== 0 &&
            (flag & fs.constants.O_NONBLOCK) !== 0,
        ),
      ).toBe(true);
    } finally {
      openSpy.mockRestore();
      syncBuiltinESMExports();
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it('atomically acquires an absent lock when POSIX inspection flags are unavailable', () => {
    // This catches treating lack of safe *inspection* flags as proof that an
    // absent pathname is occupied, rather than attempting atomic publication.
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'win32-absent.lock');
    __setFileLockPosixInspectionFlagsForTests(false);

    expect(withFileLock(lockPath, () => 'atomically-owned', { waitMs: 40 })).toEqual({
      ok: true,
      value: 'atomically-owned',
    });
    expect(fs.existsSync(lockPath)).toBe(false);
  });

  it.each([
    [
      'an existing regular file',
      (lockPath: string) => fs.writeFileSync(lockPath, JSON.stringify({ pid: process.pid })),
    ],
    [
      'a symlink or reparse-risk entry',
      (lockPath: string) => {
        const target = `${lockPath}.target`;
        fs.writeFileSync(target, 'external sentinel');
        fs.symlinkSync(target, lockPath);
      },
    ],
    ['a special untrusted directory entry', (lockPath: string) => fs.mkdirSync(lockPath)],
  ])('fails closed for %s when POSIX inspection flags are unavailable', (_label, plant) => {
    // This catches an unsafe fallback that follows, parses, or replaces a
    // contended path merely because POSIX descriptor flags are unavailable.
    const workspace = makeWorkspace();
    const lockPath = path.join(workspace, 'win32-contended.lock');
    plant(lockPath);
    __setFileLockPosixInspectionFlagsForTests(false);
    let entries = 0;

    const result = withFileLock(
      lockPath,
      () => {
        entries += 1;
        return 'must-not-enter';
      },
      { waitMs: 40 },
    );

    expect(result).toMatchObject({ ok: false, reason: 'contended' });
    expect(entries).toBe(0);
    expect(fs.lstatSync(lockPath).isSymbolicLink() || fs.existsSync(lockPath)).toBe(true);
  });

  it.each([
    ['undefined', undefined],
    ['null', null],
    ['string', 'critical section failure'],
    ['symbol', Symbol('critical section failure')],
    ['Error', new Error('critical section failure')],
  ])('rethrows an exact %s primary failure after successful cleanup', async (_label, primary) => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-primary-failure-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    let observed: unknown = Symbol('no throw');

    try {
      try {
        withFileLock(lockPath, () => {
          throw primary;
        });
      } catch (error) {
        observed = error;
      }

      expect(observed).toBe(primary);
      expect(fs.existsSync(lockPath)).toBe(false);
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it.each([
    ['frozen Error', Object.freeze(new Error('frozen primary'))],
    ['non-extensible Error', Object.preventExtensions(new Error('non-extensible primary'))],
    [
      'conflicting secondaryErrors Error',
      Object.defineProperty(new Error('conflicting primary'), 'secondaryErrors', {
        value: 'reserved',
        configurable: false,
        writable: false,
      }),
    ],
    ['undefined primitive', undefined],
    ['null primitive', null],
    ['string primitive', 'primitive primary'],
    ['symbol primitive', Symbol('primitive primary')],
  ])('preserves permanent release evidence for a %s primary failure', async (_label, primary) => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-composite-release-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    const originalRmSync = fs.rmSync;
    const rmSpy = vi.spyOn(fs, 'rmSync').mockImplementation(((target: fs.PathLike, options?: fs.RmDirOptions) => {
      if (target === lockPath) {
        const error = new Error('injected permanent release failure') as NodeJS.ErrnoException;
        error.code = 'EBUSY';
        throw error;
      }
      return originalRmSync(target, options);
    }) as typeof fs.rmSync);
    syncBuiltinESMExports();
    let observed: unknown = Symbol('no throw');

    try {
      try {
        withFileLock(lockPath, () => {
          throw primary;
        });
      } catch (error) {
        observed = error;
      }

      expect(observed).toBeInstanceOf(Error);
      expect(observed).not.toBe(primary);
      expect((observed as Error).cause).toBe(primary);
      expect((observed as OutboxFailure).secondaryErrors).toHaveLength(1);
      expect((observed as OutboxFailure).secondaryErrors![0]).toMatchObject({ name: 'FileLockReleaseError' });
    } finally {
      rmSpy.mockRestore();
      syncBuiltinESMExports();
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it('does not evict an aged zero-byte baseline lock while its exact inode remains open by a live holder', async () => {
    const { withFileLock } = await import('../kernel/file-lock.js');
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'clawo-live-legacy-empty-lock-'));
    const lockPath = path.join(directory, 'outbox.lock');
    const releasePath = path.join(directory, 'release');
    const holderSource = `
      const fs = await import('node:fs');
      const lockPath = ${JSON.stringify(lockPath)};
      const releasePath = ${JSON.stringify(releasePath)};
      const fd = fs.openSync(lockPath, 'wx', 0o600);
      fs.fsyncSync(fd);
      console.log('HELD');
      while (!fs.existsSync(releasePath)) {
        await new Promise((resolve) => setTimeout(resolve, 2));
      }
      fs.closeSync(fd);
      fs.rmSync(lockPath, { force: true });
    `;
    const holder = spawn(process.execPath, ['--input-type=module', '--eval', holderSource], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    try {
      await new Promise<void>((resolve, reject) => {
        let output = '';
        holder.stdout!.on('data', (chunk: Buffer) => {
          output += chunk.toString();
          if (output.includes('HELD')) resolve();
        });
        holder.on('error', reject);
        holder.on('exit', (code) => reject(new Error(`legacy holder exited early with code ${code}`)));
      });
      const staleAt = new Date(Date.now() - 120_000);
      fs.utimesSync(lockPath, staleAt, staleAt);
      const originalReaddirSync = fs.readdirSync;
      const originalStatSync = fs.statSync;
      const holderPid = String(holder.pid);
      const readdirSpy = vi.spyOn(fs, 'readdirSync').mockImplementation(((
        directoryPath: fs.PathLike,
        options?: unknown,
      ) => {
        if (directoryPath === '/proc') {
          return [{ name: holderPid, isDirectory: () => true }] as unknown as ReturnType<typeof fs.readdirSync>;
        }
        if (directoryPath === `/proc/${holderPid}/fd`) {
          return [{ name: 'legacy-lock', isDirectory: () => false }] as unknown as ReturnType<typeof fs.readdirSync>;
        }
        return originalReaddirSync(directoryPath, options as never);
      }) as typeof fs.readdirSync);
      const statSpy = vi.spyOn(fs, 'statSync').mockImplementation(((target: fs.PathLike, options?: unknown) => {
        if (target === `/proc/${holderPid}/fd/legacy-lock`) return originalStatSync(lockPath);
        return originalStatSync(target, options as never);
      }) as typeof fs.statSync);
      syncBuiltinESMExports();

      try {
        const result = withFileLock(lockPath, () => 'must not enter', { staleMs: 1, waitMs: 150 });

        expect(result).toMatchObject({ ok: false, reason: 'contended' });
        expect(fs.statSync(lockPath).size).toBe(0);
      } finally {
        statSpy.mockRestore();
        readdirSpy.mockRestore();
        syncBuiltinESMExports();
      }
    } finally {
      fs.writeFileSync(releasePath, 'release');
      if (holder.exitCode === null) await once(holder, 'exit');
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  it('returns a stable retryable failure after a real delivery-outbox lock timeout', async () => {
    const workspace = makeWorkspace();
    const runId = 'run-lock-timeout';
    const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
    ledger.appendFlatFile('decisions.jsonl', '{"kind":"lock_timeout_seed"}\n', true);
    const release = path.join(workspace, 'release-timeout-outbox-lock');
    const held = path.join(workspace, 'held-timeout-outbox-lock');
    const attempted = path.join(workspace, 'attempted-timeout-outbox-lock');
    const holder = spawnPrepareWorker(workspace, runId, '2026-09-10T18:00:00.000Z', {
      heldMarker: held,
      holdAppendUntil: release,
    });
    const contender = spawnPrepareWorker(workspace, runId, '2026-09-10T18:00:01.000Z', {
      attemptedMarker: attempted,
    });
    await Promise.all([nextWorkerMessage(holder), nextWorkerMessage(contender)]);
    const holderResult = nextWorkerMessage(holder);
    holder.send('go');
    await waitForPath(held);
    const contenderResult = nextWorkerMessage(contender);
    contender.send('go');
    await waitForPath(attempted);

    let contention: Record<string, unknown>;
    try {
      contention = await contenderResult;
    } finally {
      fs.writeFileSync(release, 'release');
    }
    const completed = await holderResult;
    await Promise.all(
      [holder, contender].map(async (worker) => {
        if (worker.exitCode !== null) return;
        await once(worker, 'exit');
      }),
    );

    expect(contention).toMatchObject({
      type: 'error',
      code: 'AUTOLOOP_DELIVERY_OUTBOX_LOCK_CONTENDED',
      retryable: true,
    });
    expect(contention).not.toHaveProperty('committed');
    expect(completed.type).toBe('result');
    expect(holder.exitCode).toBe(0);
    expect(contender.exitCode).toBe(1);
    expect(persistedDeliveryIntents(ledger.directory)).toHaveLength(1);
  }, 25_000);
});
