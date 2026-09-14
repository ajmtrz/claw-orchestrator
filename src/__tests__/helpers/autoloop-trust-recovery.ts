import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { syncBuiltinESMExports } from 'node:module';
import { createHash } from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';

export const trustProject = fileURLToPath(new URL('../../../', import.meta.url));
export const trustArtifacts = path.join(
  trustProject,
  '.artifacts',
  'CLAWO-AUTOLOOP-DURABLE-RECOVERY-TRUST-RECOVERY-R1',
);
const rawRead = fs.readFileSync.bind(fs);
const rawAppend = fs.appendFileSync.bind(fs);
let sequence = 0;
let observationFile: string | undefined;

export function trustDigest(bytes: string | Buffer): string {
  return createHash('sha256').update(bytes).digest('hex');
}

/** External CLI fixture behavior only: it never writes an Autoloop ledger. */
export function trustNativeResponse(
  config: { directory: string; reply: string; scenario?: string; boundary?: string; cold?: boolean },
  prompt: string,
): string {
  if (config.scenario !== 'delivery' && config.scenario !== 'review') return config.reply;
  const match = /<autoloop_delivery delivery_id="([^"]+)" payload_sha256="([a-f0-9]{64})">/.exec(prompt);
  if (!match) return prompt.includes('[system]') ? 'receipt acknowledged' : config.reply;
  let reviewInspection;
  if (config.scenario === 'review') {
    const request =
      /\[review_request iter=(\d+)\][\s\S]*?Artifacts staged from run (\S+) iter (\d+) at: (iter-\d+)\//.exec(prompt);
    const checkpoint = /^checkpoint_sha: ([a-f0-9]{40})$/m.exec(prompt);
    const scope = /^scope: (.+)$/m.exec(prompt);
    const prior = /^prior_verdict: (prior_verdict\.json|\(none\))$/m.exec(prompt);
    if (!request || !checkpoint || !scope || !prior) throw new Error('Incomplete review request identity');
    // Cursor deliberately runs from a separate permissions cwd; its real
    // artifact workspace is the explicit native --workspace argument.
    const workspaceArg = process.argv.indexOf('--workspace');
    const workspace = workspaceArg >= 0 ? process.argv[workspaceArg + 1] : process.cwd();
    const readArtifact = (relative: string) => {
      const bytes = fs.readFileSync(path.join(workspace, relative));
      return { bytes_base64: bytes.toString('base64'), bytes: bytes.length, sha256: trustDigest(bytes) };
    };
    reviewInspection = {
      cwd: process.cwd(),
      workspace,
      request: {
        target_iter: Number(request[1]),
        source_run_id: request[2],
        source_iter: Number(request[3]),
        checkpoint_sha: checkpoint[1],
        scope: JSON.parse(scope[1]),
      },
      artifacts: Object.fromEntries(
        ['directive.json', 'diff.patch', 'eval_output.json', 'coder_summary.txt'].map((name) => [
          name,
          readArtifact(request[4] + '/' + name),
        ]),
      ),
      prior_verdict: prior[1] === '(none)' ? null : readArtifact(prior[1]),
    };
  }
  const received = {
    delivery_id: match[1],
    payload_sha256: match[2],
    prompt,
    pid: process.pid,
    ...(reviewInspection ? { review_inspection: reviewInspection } : {}),
  };
  const receiptFd = fs.openSync(path.join(config.directory, 'recipient.jsonl'), 'a');
  try {
    fs.writeSync(receiptFd, JSON.stringify(received) + '\n');
    fs.fsyncSync(receiptFd);
  } finally {
    fs.closeSync(receiptFd);
  }
  const effects = path.join(config.directory, 'receiver-effects.jsonl');
  const prior = fs.existsSync(effects)
    ? fs
        .readFileSync(effects, 'utf8')
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line))
    : [];
  if (!prior.some((row) => row.delivery_id === match[1])) {
    const fd = fs.openSync(effects, 'a');
    fs.writeSync(fd, JSON.stringify(received) + '\n');
    fs.fsyncSync(fd);
    fs.closeSync(fd);
  }
  if (!config.cold && config.boundary === 'after-capture') {
    fs.writeFileSync(
      path.join(config.directory, 'barrier.json'),
      JSON.stringify({ boundary: config.boundary, pid: process.pid, delivery_id: match[1] }),
      { flag: 'wx' },
    );
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  }
  const identity = { delivery_id: match[1], payload_sha256: match[2] };
  const call =
    config.scenario === 'review'
      ? {
          tool: 'review_complete',
          args: { decision: 'hold', metric: null, audit_notes: 'existing checkpoint inspected', ...identity },
        }
      : { tool: 'request_clarification', args: { question: 'clarify the exact directive', ...identity } };
  return '```autoloop\n' + JSON.stringify(call) + '\n```';
}

export function trustDirectory(directory: string): string {
  const absolute = path.resolve(directory);
  const relative = path.relative(trustArtifacts, absolute);
  if (
    !/^evidence\/(upstream|start|candidate|sensitivity)\/[^/]+(?:\/.*)?$/.test(relative) ||
    relative.split('/').includes('..')
  ) {
    throw new Error('Trust recovery scratch must be inside a run-owned artifact attempt');
  }
  let current = trustProject;
  for (const segment of path.relative(trustProject, absolute).split(path.sep)) {
    current = path.join(current, segment);
    try {
      const stat = fs.lstatSync(current);
      if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error('Unsafe trust recovery scratch directory');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      fs.mkdirSync(current, { mode: 0o700 });
    }
  }
  return absolute;
}

export function trustObserve(kind: string, value: unknown): void {
  if (!observationFile) throw new Error('Trust recovery observation boundary is not initialized');
  rawAppend(
    observationFile,
    JSON.stringify({
      sequence: sequence++,
      process_id: process.pid,
      observer_id: `trust-observer:${process.pid}`,
      value: kind === 'source-import' ? value : { kind, data: value },
    }) + '\n',
  );
}

// Preload through NODE_OPTIONS before Vitest, tsx or SessionManager is imported.
// Patch only test-boundary OS paths; HOME and CODEX_HOME remain unchanged.
if (process.env.CLAWO_TRUST_SCRATCH) {
  const scratch = trustDirectory(process.env.CLAWO_TRUST_SCRATCH);
  const processRoot = trustDirectory(path.join(scratch, `process-${process.pid}`));
  const home = trustDirectory(process.env.CLAWO_TRUST_SHARED_HOME ?? path.join(processRoot, 'home'));
  const tmp = trustDirectory(path.join(processRoot, 'tmp'));
  observationFile = path.join(processRoot, 'observations.jsonl');
  fs.writeFileSync(observationFile, '', { flag: 'wx', mode: 0o600 });
  os.homedir = () => home;
  os.tmpdir = () => tmp;
  process.env.CLAWO_WF_DIR = trustDirectory(path.join(processRoot, 'wf'));
  const subject = process.env.CLAWO_TRUST_SUBJECT_ROOT;
  if (subject) {
    fs.readFileSync = ((...args: Parameters<typeof fs.readFileSync>) => {
      const result = rawRead(...args);
      const file = args[0] instanceof URL ? fileURLToPath(args[0]) : String(args[0]);
      if (
        file.startsWith(path.resolve(subject) + path.sep) &&
        /\/(session-manager(?:\.test)?|constants|dispatcher|secure-ledger)\.ts$/.test(file)
      ) {
        trustObserve('source-read', { path: file, sha256: trustDigest(result) });
      }
      return result;
    }) as typeof fs.readFileSync;
  }
  syncBuiltinESMExports();
  trustObserve('isolation', { home, tmp, wf: process.env.CLAWO_WF_DIR, subject });
}

interface TrustAction {
  action: 'load' | 'release' | 'prepare' | 'reserve' | 'release-options' | 'shutdown';
  subject?: string;
  runId: string;
  sessionName: string;
  now: number;
  seed?: Record<string, unknown>[];
  barrier?: 'loaded' | 'before-release-write' | 'before-file-fsync' | 'after-durable-release';
  fault?: 'release-fsync' | 'release-short-write';
  options?: { generation?: number; owner?: string; session?: string; omitSession?: boolean };
}

async function runTrustAction(action: TrustAction): Promise<void> {
  if (!observationFile || !process.env.CLAWO_TRUST_SCRATCH) throw new Error('Worker requires isolated scratch');
  const scratch = trustDirectory(process.env.CLAWO_TRUST_SCRATCH);
  const output = path.dirname(observationFile);
  trustObserve('invocation', action);
  const subject = path.resolve(action.subject ?? trustProject);
  const workspace = trustDirectory(path.join(scratch, 'workspace'));
  const registry = path.join(trustDirectory(path.join(os.homedir(), '.openclaw')), 'claude-sessions.json');
  const ledgerPath = path.join(workspace, 'tasks', action.runId, 'agent-generations.jsonl');
  const snapshots: Record<string, unknown> = {};
  function snapshot(label: string) {
    const files = Object.fromEntries(
      Object.entries({ registry, ledger: ledgerPath }).map(([kind, file]) => {
        if (!fs.existsSync(file)) return [kind, null];
        const bytes = rawRead(file);
        const target = path.join(output, `${sequence}-${label}-${kind}.${kind === 'registry' ? 'json' : 'jsonl'}`);
        fs.writeFileSync(target, bytes, { flag: 'wx', mode: 0o600 });
        return [kind, { path: target, sha256: trustDigest(bytes) }];
      }),
    );
    snapshots[label] = files;
    trustObserve('snapshot', { label, ...files });
    return files;
  }
  if (action.seed) {
    fs.writeFileSync(registry, JSON.stringify(action.seed), { flag: 'wx', mode: 0o600 });
    snapshot('seed');
  }
  // Clock injection is at the test boundary. The production TTL and registry
  // loader are untouched, and dispatcher lease dates use its existing seam.
  Date.now = () => action.now;
  const originalSync = fs.fsyncSync;
  const originalWrite = fs.writeSync;
  let appendingKind: string | undefined;
  let faultUsed = false;
  function descriptorPath(fd: number): string {
    return fs.readlinkSync(`/proc/self/fd/${fd}`);
  }
  fs.fsyncSync = (fd) => {
    originalSync(fd);
    const target = descriptorPath(fd);
    if (target === ledgerPath || target === path.dirname(ledgerPath)) {
      trustObserve('fsync-return', { target, fd, appendingKind });
    }
  };
  fs.writeSync = ((...args: Parameters<typeof fs.writeSync>) => {
    if (
      !faultUsed &&
      action.fault === 'release-short-write' &&
      appendingKind === 'agent_generation_released' &&
      descriptorPath(args[0]) === ledgerPath &&
      typeof args[1] === 'string'
    ) {
      faultUsed = true;
      const requested = Buffer.byteLength(args[1]);
      const written = originalWrite(args[0], args[1].slice(0, 23), null, 'utf8');
      trustObserve('short-write', { requested, written, target: ledgerPath });
      return written;
    }
    return Reflect.apply(originalWrite, fs, args);
  }) as typeof fs.writeSync;
  syncBuiltinESMExports();
  const managerPath = path.join(subject, 'src/session-manager.ts');
  const imports = [{ path: managerPath, sha256: trustDigest(rawRead(managerPath)) }];
  const { SessionManager } = await import(pathToFileURL(managerPath).href);
  trustObserve('source-import', imports[0]);
  const logger = Object.fromEntries(
    ['debug', 'info', 'warn', 'error'].map((level) => [
      level,
      (message: unknown) => trustObserve('log', { level, message }),
    ]),
  );
  const manager = new SessionManager({}, logger);
  const loaded = manager.listPersistedSessions();
  trustObserve('loaded', loaded);
  snapshot('loaded');
  const apis = {
    reserve: typeof manager.reserveAgentGeneration === 'function',
    release: typeof manager.releaseReservation === 'function',
  };
  const responses: boolean[] = [];
  let value: unknown;
  let failure: { name: string; code?: string; message: string } | undefined;
  let barrierUsed = false;
  function barrier(stage: TrustAction['barrier']) {
    if (barrierUsed || action.barrier !== stage) return;
    barrierUsed = true;
    const gate = path.join(output, `continue-${stage}`);
    const receipt = {
      type: 'barrier',
      stage,
      gate,
      owner: manager.autoloopOwnerInstanceId,
      process_id: process.pid,
      snapshot: snapshot(`barrier-${stage}`),
    };
    trustObserve('barrier-reached', receipt);
    process.send?.(receipt);
    const start = performance.now();
    const wait = new Int32Array(new SharedArrayBuffer(4));
    // A filesystem handshake also works while the real synchronous fsync
    // boundary has the child event loop blocked. The clock under test cannot
    // affect this bounded wait.
    while (!fs.existsSync(gate)) {
      if (performance.now() - start > 15_000) throw new Error(`Unreleased test barrier: ${stage}`);
      Atomics.wait(wait, 0, 0, 10);
    }
    trustObserve('barrier-resumed', { stage });
  }
  try {
    barrier('loaded');
    if (action.action !== 'load') {
      if (!apis.release) throw new Error('Subject has no generation release API; this is NOT_PROVEN');
      const release = manager.releaseReservation.bind(manager);
      manager.releaseReservation = async (...args: unknown[]) => {
        const result = await release(...args);
        responses.push(result);
        trustObserve('release-response', { result });
        snapshot(`release-return-${responses.length}`);
        return result;
      };
      const dispatcherPath = path.join(subject, 'src/autoloop/dispatcher.ts');
      const { ClaudeAgentDispatcher } = await import(pathToFileURL(dispatcherPath).href);
      imports.push({ path: dispatcherPath, sha256: trustDigest(rawRead(dispatcherPath)) });
      trustObserve('source-import', imports.at(-1));
      const securePath = path.join(subject, 'src/autoloop/secure-ledger.ts');
      const { SecureAutoloopLedger } = await import(pathToFileURL(securePath).href);
      imports.push({ path: securePath, sha256: trustDigest(rawRead(securePath)) });
      trustObserve('source-import', imports.at(-1));
      const secureLedger = SecureAutoloopLedger.open(workspace, action.runId, {
        create: true,
        testHooks: {
          beforeFileMutation: (event: { operation: string; filePath: string }) => {
            if (appendingKind !== 'agent_generation_released' || event.filePath !== ledgerPath) return;
            if (event.operation === 'append') barrier('before-release-write');
            if (event.operation === 'flush') {
              barrier('before-file-fsync');
              if (!faultUsed && action.fault === 'release-fsync') {
                faultUsed = true;
                trustObserve('file-sync-fault', { target: ledgerPath });
                throw new Error('Injected release file-sync failure');
              }
            }
          },
        },
      });
      const append = secureLedger.appendFlatFile.bind(secureLedger);
      secureLedger.appendFlatFile = (name: string, content: string, durable: boolean) => {
        appendingKind = JSON.parse(content).kind;
        trustObserve('append-enter', { name, kind: appendingKind, durable });
        try {
          append(name, content, durable);
          trustObserve('append-return', { name, kind: appendingKind, durable });
          if (appendingKind === 'agent_generation_released') barrier('after-durable-release');
        } finally {
          appendingKind = undefined;
        }
      };
      const dispatcher = new ClaudeAgentDispatcher({
        manager,
        runId: action.runId,
        workspace,
        plannerEngine: 'codex',
        plannerModel: 'gpt-6-astra',
        coderEngine: 'codex',
        coderModel: 'gpt-6-astra',
        reviewerEngine: 'codex',
        reviewerModel: 'gpt-6-astra',
        now: () => new Date(action.now),
        logger,
        secureLedger,
      });
      // Exercise the real dispatcher's reservation path without starting an
      // external adapter. These methods call the real registry and ledger.
      if (action.action === 'release') value = await dispatcher.releaseLegacyReservation('planner');
      else if (action.action === 'prepare') value = await dispatcher.prepareGeneration('planner');
      else if (action.action === 'reserve') {
        value = manager.reserveAgentGeneration(
          {
            role: 'planner',
            generation: 1,
            session_name: action.sessionName,
            session_id: `competing-session:${process.pid}`,
            owner_instance_id: manager.autoloopOwnerInstanceId,
            created_at: new Date(action.now).toISOString(),
            last_activity_at: new Date(action.now).toISOString(),
            lease_expires_at: new Date(action.now + 60_000).toISOString(),
            state: 'stale',
          },
          workspace,
        );
        trustObserve('reserve-response', { result: value });
      } else if (action.action === 'release-options') {
        const options: Record<string, unknown> = {
          expectedOwnerInstanceId: action.options?.owner ?? 'legacy-registry',
          expectedSessionId: action.options?.session,
          releaseOwnerInstanceId: manager.autoloopOwnerInstanceId,
        };
        if (action.options?.omitSession) delete options.expectedSessionId;
        value = await manager.releaseReservation(action.sessionName, action.options?.generation ?? 0, options);
      } else if (action.action === 'shutdown') {
        const releasing = dispatcher.releaseLegacyReservation('planner');
        trustObserve('shutdown-requested', {});
        const shuttingDown = manager.shutdown();
        await releasing;
        await shuttingDown;
        trustObserve('shutdown-return', {});
        value = {
          closedRelease: await manager.releaseReservation(action.sessionName, 0, {
            expectedOwnerInstanceId: 'legacy-registry',
            expectedSessionId: undefined,
            releaseOwnerInstanceId: manager.autoloopOwnerInstanceId,
          }),
        };
      }
    }
  } catch (error) {
    const observed = error as Error & { code?: string };
    failure = { name: observed.name, code: observed.code, message: observed.message };
    trustObserve('failure', failure);
  } finally {
    await manager.shutdown();
    snapshot('final');
    fs.fsyncSync = originalSync;
    fs.writeSync = originalWrite;
  }
  const result = {
    loaded,
    apis,
    responses,
    snapshots,
    imports,
    value,
    failure,
    owner: manager.autoloopOwnerInstanceId,
    process_id: process.pid,
  };
  trustObserve('result', result);
  process.stdout.write(JSON.stringify(result) + '\n');
  process.disconnect?.();
}

if (
  process.env.CLAWO_TRUST_ACTION &&
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  await runTrustAction(JSON.parse(process.env.CLAWO_TRUST_ACTION) as TrustAction);
}
