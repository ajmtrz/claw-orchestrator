import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { fileURLToPath } from 'node:url';
import Ajv from 'ajv';

export const RUN_ID = 'CLAWO-AUTOLOOP-DURABLE-RECOVERY-TRUST-RECOVERY-R1';
export const SOURCE_URL = import.meta.url;
export const PROJECT = fileURLToPath(new URL('../../', import.meta.url));
export const ARTIFACT_ROOT = path.join(PROJECT, '.artifacts', RUN_ID);
export const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const schema = JSON.parse(fs.readFileSync(new URL('./evidence.schema.json', import.meta.url), 'utf8'));
const ajv = new Ajv({ allErrors: true, strict: false });
const validateSchema = ajv.compile(schema);
const validateManifest = ajv.compile({ $ref: '#/definitions/manifest', definitions: schema.definitions });
const requireThat = (condition, message) => {
  if (!condition) throw new Error(message);
};
const same = (a, b, message) => requireThat(isDeepStrictEqual(a, b), message);

/** Reject traversal and symlinks, including ancestors of the attempt directory. */
export function containedPath(directory, relative) {
  requireThat(
    typeof relative === 'string' &&
      relative.length > 0 &&
      !path.isAbsolute(relative) &&
      !relative.includes('\\') &&
      relative.split('/').every((s) => s && s !== '.' && s !== '..'),
    'Invalid artifact path',
  );
  const absolute = path.resolve(directory, relative);
  let current = path.parse(absolute).root;
  for (const segment of absolute.slice(current.length).split(path.sep)) {
    current = path.join(current, segment);
    const stat = fs.lstatSync(current);
    requireThat(!stat.isSymbolicLink(), 'Symlinked artifact path');
  }
  return absolute;
}

export function readArtifact(directory, reference) {
  const absolute = containedPath(directory, reference.path);
  const fd = fs.openSync(absolute, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    requireThat(fs.fstatSync(fd).isFile(), 'Artifact is not a regular file');
    const bytes = fs.readFileSync(fd);
    requireThat(sha256(bytes) === reference.sha256, `Artifact hash mismatch: ${reference.path}`);
    return bytes;
  } finally {
    fs.closeSync(fd);
  }
}

function pointerValue(value, pointer) {
  if (pointer === '') return value;
  requireThat(pointer.startsWith('/') && !/~(?:[^01]|$)/.test(pointer), 'Invalid observation JSON pointer');
  for (const escaped of pointer.slice(1).split('/')) {
    const key = escaped.replace(/~1/g, '/').replace(/~0/g, '~');
    requireThat(
      value !== null && typeof value === 'object' && Object.hasOwn(value, key),
      'Missing observation JSON pointer',
    );
    value = value[key];
  }
  return value;
}

function duplicates(values) {
  return new Set(values).size !== values.length;
}
const diagnosticFailure =
  /^(?:#\s*)?(?:Error:.*(?:unhandled|uncaught)|Unhandled(?:Promise)?Rejection|uncaughtException|.*generated asynchronous activity after the test ended)/im;
const failedTestDiagnostic = /^\s*(?:not ok \d+\b|FAIL\s|[×✗]\s|.*⎯.*Failed Tests\s+\d+)/m;

/** Parse complete machine reports, retaining failure information for RED captures. */
export function inspectTestReport(bytes, format) {
  const text = bytes.toString('utf8');
  const result = { discovered: [], executed: [], skipped: [], errors: [] };
  if (format === 'node-tap') {
    const rows = [...text.matchAll(/^(not ok|ok) \d+ - (.*?)(?: # (SKIP|TODO)(?: .*)?)?$/gm)];
    for (const row of rows) {
      result.discovered.push(row[2]);
      (row[3] ? result.skipped : result.executed).push(row[2]);
      if (row[1] === 'not ok') result.errors.push(`TAP failed test: ${row[2]}`);
    }
    const count = (name) => {
      const matches = [...text.matchAll(new RegExp(`^# ${name} (\\d+)$`, 'gm'))];
      if (matches.length !== 1) {
        result.errors.push(`Missing or duplicate TAP ${name} summary`);
        return -1;
      }
      return Number(matches[0][1]);
    };
    const plans = [...text.matchAll(/^1\.\.(\d+)$/gm)];
    if (plans.length !== 1 || Number(plans[0][1]) !== rows.length) result.errors.push('Incomplete TAP plan');
    if (count('tests') !== rows.length || count('pass') !== result.executed.length || count('suites') !== 0)
      result.errors.push('TAP test inventory mismatch or unsupported nested suite');
    for (const key of ['fail', 'cancelled', 'skipped', 'todo'])
      if (count(key) !== 0) result.errors.push(`TAP ${key} is nonzero`);
    if (diagnosticFailure.test(text)) result.errors.push('Unhandled TAP diagnostic');
  } else if (format === 'vitest-json') {
    const report = JSON.parse(text);
    requireThat(Array.isArray(report.testResults), 'Missing Vitest report testResults');
    for (const suite of report.testResults) {
      requireThat(Array.isArray(suite.assertionResults), 'Missing Vitest assertions');
      if (suite.status !== 'passed' || suite.message || suite.testExecError) result.errors.push('Failed Vitest suite');
      for (const row of suite.assertionResults) {
        const id = `${suite.name} > ${row.fullName}`;
        result.discovered.push(id);
        if (row.status === 'passed' || row.status === 'failed') result.executed.push(id);
        else result.skipped.push(id);
        if (row.status !== 'passed' || row.failureMessages?.length) result.errors.push(`Nonpassing Vitest test: ${id}`);
      }
    }
    if (
      report.success !== true ||
      report.numFailedTests !== 0 ||
      report.numFailedTestSuites !== 0 ||
      report.numTotalTests !== result.discovered.length ||
      report.numPassedTests !== result.executed.length ||
      report.numPendingTests !== 0 ||
      (report.numTodoTests ?? 0) !== 0 ||
      report.unhandledErrors?.length ||
      (report.numUnhandledErrors ?? 0) !== 0
    )
      result.errors.push('Failed or inconsistent Vitest report');
  } else throw new Error('Unsupported report format');
  if (!result.executed.length || duplicates(result.discovered)) result.errors.push('Empty or ambiguous test inventory');
  return result;
}

function verifyExecution(execution, directory, expected) {
  same(execution.argv, expected.argv, 'Execution argv differs from fixed command');
  same(execution.cwd, expected.cwd, 'Execution cwd differs from fixed command');
  requireThat(
    execution.exit_code === 0 &&
      execution.signal === null &&
      !execution.timed_out &&
      !execution.spawn_error &&
      execution.process_id > 0,
    'Failed execution exit, spawn, process, signal or timeout',
  );
  const start = Date.parse(execution.started_at),
    end = Date.parse(execution.ended_at);
  requireThat(Number.isFinite(start) && Number.isFinite(end) && end >= start, 'Invalid execution times');
  const stdout = readArtifact(directory, execution.stdout);
  const stderr = readArtifact(directory, execution.stderr);
  const rawReport = readArtifact(directory, execution.report);
  const report = inspectTestReport(rawReport, execution.report_format);
  requireThat(report.errors.length === 0, `Failed report: ${report.errors.join('; ')}`);
  if (execution.report_format === 'node-tap') {
    // A separately rewritten green report must not conceal original failure output.
    same(stdout, rawReport, 'TAP stdout differs from retained raw report');
  }
  requireThat(
    !diagnosticFailure.test(stderr.toString('utf8')) && !diagnosticFailure.test(stdout.toString('utf8')),
    'Unhandled execution diagnostic',
  );
  requireThat(
    !failedTestDiagnostic.test(stderr.toString('utf8')) && !failedTestDiagnostic.test(stdout.toString('utf8')),
    'Failed test diagnostic in raw stdout/stderr',
  );
  same(execution.discovered_test_ids, report.discovered, 'Discovered test inventory differs from report');
  same(execution.executed_test_ids, report.executed, 'Executed test inventory differs from report');
  same(execution.skipped_test_ids, report.skipped, 'Skipped test inventory differs from report');
  requireThat(execution.skipped_test_ids.length === 0, 'Required tests skipped');
  for (const id of expected.required_test_ids)
    requireThat(report.executed.includes(id), `Missing required test: ${id}`);
}

function applyPredicate(assertion, values, observations) {
  const args = assertion.observations.map((id) => {
    requireThat(values.has(id), `Missing assertion observation: ${id}`);
    return values.get(id);
  });
  let valid = false;
  switch (assertion.predicate) {
    case 'equal':
      valid = args.length === 2 && !args.some((v) => typeof v === 'boolean') && isDeepStrictEqual(args[0], args[1]);
      break;
    case 'literal':
      requireThat(
        Object.hasOwn(assertion, 'expected') && typeof assertion.expected !== 'boolean',
        'Boolean or missing literal predicate',
      );
      valid = args.length === 1 && isDeepStrictEqual(args[0], assertion.expected);
      break;
    case 'digest-equal':
      valid = args.length === 2 && args.every((v) => typeof v === 'string') && sha256(args[0]) === args[1];
      break;
    case 'ordered': {
      const events = assertion.observations.map((id) => observations.get(id));
      valid =
        events.length > 1 &&
        events.every(
          (e, i) =>
            i === 0 ||
            (e.execution_id === events[0].execution_id &&
              e.observer_id === events[0].observer_id &&
              e.sequence > events[i - 1].sequence),
        );
      break;
    }
    case 'row-count':
      valid =
        args.length === 1 &&
        Array.isArray(args[0]) &&
        Number.isSafeInteger(assertion.expected) &&
        assertion.expected >= 0 &&
        args[0].length === assertion.expected;
      break;
    case 'prefix-preserved':
      valid =
        args.length === 2 &&
        args.every((v) => typeof v === 'string') &&
        Buffer.from(args[1]).subarray(0, Buffer.byteLength(args[0])).equals(Buffer.from(args[0]));
      break;
    default:
      throw new Error('Unsupported predicate');
  }
  requireThat(valid, `Failed predicate ${assertion.id} (${assertion.predicate})`);
}

/**
 * Contract comes from reviewed caller code, never bundle fields. This checks
 * evidence integrity and predicates, not authenticity or independent review.
 */
export function readInputManifest(directory, reference) {
  const manifest = JSON.parse(readArtifact(directory, reference));
  requireThat(validateManifest(manifest), 'Invalid input manifest schema');
  for (const entries of Object.values(manifest))
    requireThat(!duplicates(entries.map((e) => e.path)), 'Duplicate manifest path');
  return manifest;
}

export function verifyBundle(bundle, directory, contract) {
  requireThat(validateSchema(bundle), `Evidence schema: ${ajv.errorsText(validateSchema.errors)}`);
  requireThat(contract && Array.isArray(contract.assertions), 'A separate fixed contract is required');
  for (const key of [
    'run_id',
    'case_id',
    'subject_kind',
    'base',
    'head',
    'tree',
    'frozen',
    'requirement_ids',
    'harness_sha256',
    'dependency_sha256',
    'tool_sha256',
  ])
    same(bundle[key], contract[key], `Wrong ${key}`);
  same(bundle.input_manifest.sha256, contract.input_manifest_sha256, 'Wrong input manifest');
  const manifest = readInputManifest(directory, bundle.input_manifest);
  for (const [field, key] of [
    ['harness_sha256', 'harness'],
    ['dependency_sha256', 'dependencies'],
    ['tool_sha256', 'tools'],
  ])
    same(bundle[field], sha256(JSON.stringify(manifest[key])), `Manifest ${field} mismatch`);
  if (!bundle.frozen)
    requireThat(
      bundle.patch !== null && bundle.test_sources.length > 0,
      'Unfrozen evidence requires exact patch and test bytes',
    );
  else requireThat(bundle.patch === null, 'Frozen evidence cannot contain an uncommitted patch');
  same(bundle.patch?.sha256 ?? null, contract.patch_sha256, 'Patch differs from exact input snapshot');
  same(
    bundle.test_sources.map((source) => source.sha256),
    contract.test_source_sha256s,
    'Test source bytes differ from exact input snapshot',
  );
  if (bundle.patch) readArtifact(directory, bundle.patch);
  for (const source of bundle.test_sources) readArtifact(directory, source);
  same(bundle.assertions, contract.assertions, 'Assertions differ from fixed contract');
  requireThat(bundle.executions.length === contract.executions.length, 'Execution count differs from contract');
  requireThat(!duplicates(bundle.executions.map((e) => e.id)), 'Duplicate execution identity');
  bundle.executions.forEach((e, i) => verifyExecution(e, directory, contract.executions[i]));
  const executionIds = new Set(bundle.executions.map((e) => e.id));
  const values = new Map(),
    observations = new Map(),
    sequences = new Map();
  for (const observation of bundle.observations) {
    requireThat(!values.has(observation.id), 'Duplicate observation identity');
    requireThat(executionIds.has(observation.execution_id), 'Unknown observation execution');
    const bytes = readArtifact(directory, observation.artifact);
    let event;
    if (Object.hasOwn(observation, 'pointer')) event = pointerValue(JSON.parse(bytes), observation.pointer);
    else {
      const [start, end] = observation.byte_range;
      requireThat(start < end && end <= bytes.length, 'Invalid observation byte range');
      event = JSON.parse(bytes.subarray(start, end));
    }
    for (const field of ['sequence', 'process_id', 'observer_id', 'value'])
      same(event?.[field], observation[field], `Observation ${field} differs from artifact`);
    const stream = JSON.stringify([observation.execution_id, observation.observer_id]);
    requireThat(
      observation.sequence > (sequences.get(stream) ?? -1),
      'Observation sequence/order is not strictly increasing',
    );
    sequences.set(stream, observation.sequence);
    values.set(observation.id, event.value);
    observations.set(observation.id, observation);
  }
  for (const id of [...contract.required_observations, ...contract.required_fault_observations])
    requireThat(values.has(id), `Missing required/fault observation: ${id}`);
  requireThat(!duplicates(bundle.source_imports.map((s) => s.path)), 'Duplicate source import');
  for (const source of bundle.source_imports) {
    const receipt = values.get(source.observation_id);
    requireThat(
      receipt?.path === source.path && receipt?.sha256 === source.sha256,
      'Source import differs from observed import receipt',
    );
    const input = [...manifest.tracked, ...manifest.harness].find((entry) => entry.path === source.path);
    requireThat(input?.sha256 === source.sha256, 'Source import does not match input manifest');
  }
  for (const sourcePath of contract.required_source_paths)
    requireThat(
      bundle.source_imports.some((s) => s.path === sourcePath),
      'Missing required source import',
    );
  for (const assertion of bundle.assertions) applyPredicate(assertion, values, observations);
  return values;
}

export function verifySeries(entries, contract, repetitions) {
  requireThat(
    Number.isSafeInteger(repetitions) && repetitions > 0 && entries.length === repetitions,
    'Incomplete repetitions',
  );
  const seen = new Set();
  let previousEnd = -Infinity;
  return entries.map(({ bundle, directory }) => {
    const values = verifyBundle(bundle, directory, contract);
    for (const execution of bundle.executions) {
      requireThat(!seen.has(execution.id), 'Duplicate repetition execution');
      requireThat(Date.parse(execution.started_at) >= previousEnd, 'Repetitions are out of order or overlap');
      seen.add(execution.id);
      previousEnd = Date.parse(execution.ended_at);
    }
    return values;
  });
}

export const LEGACY_TITLES = [
  'isolates registry and subprocess scratch paths before import without changing HOME or CODEX_HOME',
  ...['upstream', 'start', 'candidate'].flatMap((subject) =>
    [-1, 0, 1].map(
      (delta) =>
        `${subject}: reads ordinary legacy metadata at TTL ${delta < 0 ? '-1' : '+' + delta} ms from real registry bytes`,
    ),
  ),
  ...[-1, 0, 1].map((delta) => `distinguishes durable pending and released fences from ordinary TTL at ${delta} ms`),
  ...[-1, 0, 1].map((delta) => `traces the legacy release hooks and persisted ledger at TTL ${delta} ms`),
  'refuses a competing process inside real legacy evidence persistence, then admits one successor',
  ...['before-release-write', 'before-file-fsync', 'after-durable-release'].map(
    (stage) => `cold-recovers once after witnessed process loss at ${stage}`,
  ),
  'keeps unknown legacy release ownership blocked and rejects mismatched tuples without writes',
  'flushes a retained release row after a real file-sync failure without duplicating history',
  'blocks cold recovery of a partially written release row while preserving historical bytes',
  'waits for durable release during shutdown and refuses later releases from the closed manager',
  'does not let shutdown publish a stale legacy snapshot over the successor fence',
].map((title) => `trust recovery legacy persisted state ${title}`);

// Independent scenario definitions: neither labels nor supplied descriptors
// select arbitrary clocks, seeds, subjects, barriers or invocation sequences.
function legacyScenario(title) {
  const created = 1788602400000;
  const legacy = {
    name: 'autoloop-legacy-probe-planner',
    claudeSessionId: 'legacy-session-id',
    cwd: PROJECT,
    originalCreated: '2026-09-05T10:00:00.000Z',
    lastResumed: '2026-09-05T10:00:00.000Z',
    lastActivity: created,
  };
  const pending = {
    ...legacy,
    agentGeneration: 0,
    agentOwnerInstanceId: 'legacy-registry',
    agentReleasePending: true,
    agentReleaseOwnerInstanceId: 'unclassifiable-old-owner',
  };
  const step = (action, rest = {}) => ({ action, ...rest });
  const seeded = (action, rest = {}) => step(action, { seed: [legacy], ...rest });
  const definitions = [
    { outcome: 'isolation', steps: [] },
    ...['upstream', 'start', 'candidate'].flatMap((subjectKind) =>
      [-1, 0, 1].map((delta) => ({
        outcome: 'ordinary',
        subjectKind,
        delta,
        steps: [
          seeded('load', {
            subject:
              subjectKind === 'candidate' ? PROJECT : path.join(PROJECT, '.worktrees/trust-recovery-r1', subjectKind),
            now: created + 604800000 + delta,
          }),
        ],
      })),
    ),
    ...[-1, 0, 1].map((delta) => ({
      outcome: 'fences',
      delta,
      steps: [
        step('load', {
          seed: [
            pending,
            { ...legacy, name: 'released-legacy', agentReleasedGeneration: 0 },
            { ...legacy, name: 'ordinary' },
          ],
          now: created + 604800000 + delta,
        }),
      ],
    })),
    ...[-1, 0, 1].map((delta) => ({
      outcome: 'release-ttl',
      delta,
      steps: [seeded('release', { now: created + 604800000 + delta })],
    })),
    {
      outcome: 'competition',
      steps: [
        seeded('release', { barrier: 'before-file-fsync' }),
        step('reserve'),
        step('prepare'),
        step('release'),
        step('prepare'),
        step('reserve'),
      ],
    },
    ...['before-release-write', 'before-file-fsync', 'after-durable-release'].map((barrier) => ({
      outcome: 'crash',
      steps: [seeded('prepare', { barrier }), step('reserve'), step('prepare'), step('reserve')],
    })),
    {
      outcome: 'unknown',
      steps: [
        step('prepare', { seed: [pending] }),
        ...[{ generation: 1 }, { owner: 'wrong-owner' }, { session: 'wrong-session' }, { omitSession: true }].map(
          (options) => step('release-options', { options }),
        ),
      ],
    },
    { outcome: 'sync', steps: [seeded('prepare', { fault: 'release-fsync' }), step('prepare')] },
    {
      outcome: 'partial',
      steps: [seeded('prepare', { fault: 'release-short-write' }), step('prepare'), step('reserve')],
    },
    {
      outcome: 'shutdown',
      steps: [seeded('shutdown', { barrier: 'after-durable-release' }), step('reserve'), step('prepare')],
    },
    { outcome: 'stale', steps: [seeded('load', { barrier: 'loaded' }), step('prepare')] },
  ];
  const index = LEGACY_TITLES.indexOf(title);
  requireThat(index >= 0 && definitions.length === LEGACY_TITLES.length, 'Unknown legacy scenario identity');
  return { title: LEGACY_TITLES[index], subjectKind: 'candidate', ...definitions[index] };
}

/** Slice-specific predicates over actual files; API booleans alone never suffice. */
export function verifyLegacyCase(directory, candidateHead) {
  const read = (relative) => fs.readFileSync(containedPath(directory, relative), 'utf8');
  const lines = (relative) =>
    read(relative)
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  const title = JSON.parse(read('case.json')).title;
  const scenario = legacyScenario(title);
  const journal = lines('parent.jsonl');
  journal.forEach((event, i) =>
    requireThat(event.sequence === i && event.process_id > 0, 'Invalid parent observation order'),
  );
  const events = journal.map((event) => event.value);
  if (scenario.outcome === 'isolation') {
    const [event] = events;
    requireThat(
      events.length === 1 && event.kind === 'isolation' && event.status === 0 && event.signal === null,
      'Isolation child failed',
    );
    const observed = JSON.parse(read(`worker-${event.pid}-stdout.txt`));
    for (const key of ['home', 'tmp', 'wf']) containedPath(directory, path.relative(directory, observed[key]));
    requireThat(observed.home !== observed.HOME, 'Isolation changed the effective user home');
    return title;
  }
  const runs = events
    .filter((event) => ['execution', 'spawn'].includes(event.kind))
    .map((event, index) => {
      const exit = event.kind === 'execution' ? event : events.find((e) => e.kind === 'exit' && e.pid === event.pid);
      requireThat(exit && !exit.error, 'Missing/failed worker exit observation');
      const trace = lines(`process-${event.pid}/observations.jsonl`);
      trace.forEach((entry, i) =>
        requireThat(entry.sequence === i && entry.process_id === event.pid, 'Misattributed worker trace'),
      );
      const values = trace.map((entry) => entry.value);
      const input = values.find((entry) => entry.kind === 'invocation')?.data;
      requireThat(input, 'Missing worker invocation');
      const expected = scenario.steps[index];
      same(event.action, expected, 'Legacy scenario descriptor: parent action, clock, seed or barrier mismatch');
      same(
        input,
        { runId: 'legacy-probe', sessionName: 'autoloop-legacy-probe-planner', now: 1788602401000, ...expected },
        'Legacy scenario descriptor: worker invocation mismatch',
      );
      const subject = path.resolve(input.subject ?? PROJECT);
      const expectedSubject =
        scenario.subjectKind === 'candidate'
          ? PROJECT
          : path.join(PROJECT, '.worktrees/trust-recovery-r1', scenario.subjectKind);
      same(subject, path.resolve(expectedSubject), 'Legacy scenario descriptor: wrong subject');
      const head =
        scenario.subjectKind === 'upstream'
          ? '3e09b032a2f95fa520648f959f4ac9cdc7393350'
          : scenario.subjectKind === 'start'
            ? '2694c0babcf16030278e58d829a7c71bcaa0f7a2'
            : (candidateHead ??
              execFileSync('rtk', ['proxy', 'git', '-C', PROJECT, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim());
      requireThat(/^[a-f0-9]{40}$/.test(head), 'Legacy scenario descriptor: invalid subject head');
      const source = values.find((entry) => entry.path === path.join(subject, 'src/session-manager.ts'));
      // Historical receipt verification uses its independently checked Git
      // object. Fresh collection binds the worktree through the input manifest
      // and complete patch; later worktree edits cannot invalidate old receipts.
      const expectedSource =
        scenario.subjectKind === 'candidate' && candidateHead === undefined
          ? fs.readFileSync(path.join(subject, 'src/session-manager.ts'))
          : execFileSync('rtk', ['proxy', 'git', '-C', PROJECT, 'cat-file', 'blob', `${head}:src/session-manager.ts`]);
      requireThat(
        source?.sha256 === sha256(expectedSource),
        'Legacy scenario descriptor: missing/stale imported subject head',
      );
      if (expected.seed) {
        const seed = values.find((entry) => entry.kind === 'snapshot' && entry.data?.label === 'seed')?.data.registry;
        // Original worker captures also expose this reference in stdout.
        const stdoutSeed =
          exit.signal === null && JSON.parse(read(`worker-${event.pid}-stdout.txt`)).snapshots?.seed?.registry;
        const reference = seed ?? stdoutSeed;
        requireThat(reference, 'Legacy scenario descriptor: missing persisted seed');
        same(
          JSON.parse(readArtifact(directory, { ...reference, path: path.relative(directory, reference.path) })),
          expected.seed,
          'Legacy scenario descriptor: persisted seed mismatch',
        );
      }
      const barrier = values.find((entry) => entry.kind === 'barrier-reached')?.data;
      if (exit.signal === 'SIGKILL') {
        requireThat(
          scenario.outcome === 'crash' && barrier?.stage === expected.barrier,
          'Unwitnessed or unexpected process loss',
        );
        const receipt = events.find((e) => e.kind === 'barrier' && e.pid === event.pid);
        same(receipt?.receipt, barrier, 'Barrier was not observed by the parent');
        requireThat(events.indexOf(receipt) < events.indexOf(exit), 'Process loss preceded its barrier');
        return { event, exit, values, input, barrier };
      }
      requireThat(exit.status === 0 && exit.signal === null, 'Hidden worker failure');
      const result = JSON.parse(read(`worker-${event.pid}-stdout.txt`));
      requireThat(result.process_id === event.pid, 'Wrong result process');
      return { event, exit, values, input, barrier, result };
    });
  requireThat(
    runs.length > 0 && runs.length === scenario.steps.length,
    'Legacy scenario descriptor: wrong execution count',
  );
  const raw = (ref) => readArtifact(directory, { ...ref, path: path.relative(directory, ref.path) });
  const registry = (snap) => JSON.parse(raw(snap.registry));
  const ledger = (snap) => (snap.ledger ? raw(snap.ledger) : Buffer.alloc(0));
  const rows = (snap) =>
    ledger(snap)
      .toString()
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  const final = (run) => run.result.snapshots.final;
  const pending = (snap) => {
    const [entry] = registry(snap);
    requireThat(
      entry?.agentGeneration === 0 &&
        entry.agentReleasePending === true &&
        entry.agentOwnerInstanceId === 'legacy-registry' &&
        !Object.hasOwn(entry, 'agentSessionId'),
      'Legacy pending registry postcondition',
    );
  };
  const released = (snap) => {
    const [entry] = registry(snap);
    requireThat(
      entry?.agentReleasedGeneration === 0 &&
        entry.agentReleasedOwnerInstanceId === 'legacy-registry' &&
        !Object.hasOwn(entry, 'agentGeneration') &&
        !entry.agentReleasePending,
      'Legacy release registry postcondition',
    );
  };
  const history = (snap, successor) => {
    const parsed = rows(snap);
    same(
      parsed.map((row) => [row.kind, row.payload.generation]),
      [
        ['agent_generation_orphaned', 0],
        ['agent_generation_released', 0],
        ...(successor ? [['agent_generation_reserved', 1]] : []),
      ],
      'Wrong legacy ledger history or repeated release effect',
    );
    requireThat(
      parsed.every((row) => row.schema_version === 1 && row.actor === 'dispatcher'),
      'Noncanonical generation write',
    );
    if (successor) {
      const [entry] = registry(snap),
        next = parsed[2].payload;
      requireThat(
        entry.agentGeneration === 1 &&
          entry.agentReleasedGeneration === 0 &&
          entry.agentOwnerInstanceId === next.owner_instance_id &&
          entry.agentSessionId === next.session_id &&
          !entry.agentReleasePending,
        'Successor registry/ledger identity mismatch',
      );
    }
  };
  const unchanged = (a, b) => same(raw(a.registry), raw(b.registry), 'Registry changed while fenced');
  const prefix = (a, b) => same(ledger(b).subarray(0, ledger(a).length), ledger(a), 'Historical ledger prefix changed');
  const refused = (run) => {
    requireThat(run.result.value === false && !run.result.failure, 'Competing reservation was not refused');
    pending(final(run));
  };
  const flushed = (run) =>
    requireThat(
      run.values.some((v) => v.kind === 'fsync-return' && v.data.target.endsWith('/agent-generations.jsonl')),
      'Missing real ledger fsync return',
    );
  const [first] = runs;
  if (scenario.outcome === 'ordinary') {
    requireThat(runs.length === 1 && first.input.action === 'load', 'Wrong TTL execution');
    const delta = scenario.delta;
    same(first.result.loaded, delta < 0 ? first.input.seed : [], 'Ordinary metadata TTL postcondition');
    same(registry(first.result.snapshots.seed), first.input.seed, 'Seed differs from persisted bytes');
    same(
      first.result.apis,
      { reserve: scenario.subjectKind !== 'upstream', release: scenario.subjectKind !== 'upstream' },
      'Missing baseline API cannot be a release PASS',
    );
  } else if (scenario.outcome === 'fences') {
    const delta = scenario.delta;
    same(
      first.result.loaded,
      delta < 0 ? first.input.seed : first.input.seed.slice(0, 2),
      'Durable fences expired with ordinary TTL',
    );
    requireThat(
      registry(final(first)).some((entry) => entry.agentReleasePending) &&
        registry(final(first)).some((entry) => entry.agentReleasedGeneration === 0),
      'Lost durable TTL fences',
    );
  } else if (scenario.outcome === 'release-ttl') {
    const young = scenario.delta < 0;
    same(first.result.responses, [young], 'Wrong observed legacy release response');
    if (young) {
      released(final(first));
      history(final(first), false);
      flushed(first);
    } else {
      same(rows(final(first)), [], 'Expired fixture reached evidence hooks');
      same(registry(final(first)), [], 'Expired ordinary fixture retained');
    }
  } else if (scenario.outcome === 'competition') {
    requireThat(
      runs.length === 6 && first.barrier?.stage === 'before-file-fsync',
      'Incomplete competing reservation interval',
    );
    pending(first.barrier.snapshot);
    refused(runs[1]);
    unchanged(first.barrier.snapshot, final(runs[1]));
    requireThat(runs[2].result.failure?.code === 'AUTOLOOP_AGENT_GENERATION_CONFLICT', 'Live release owner was stolen');
    unchanged(first.barrier.snapshot, final(runs[2]));
    released(final(first));
    history(final(first), false);
    flushed(first);
    same(runs[3].result.responses, [true], 'Released retry failed');
    same(ledger(final(first)), ledger(final(runs[3])), 'Retry duplicated evidence');
    history(final(runs[4]), true);
    prefix(first.barrier.snapshot, final(runs[4]));
    requireThat(runs[5].result.value === false, 'Duplicate successor accepted');
    unchanged(final(runs[4]), final(runs[5]));
  } else if (scenario.outcome === 'crash') {
    requireThat(runs.length === 4 && first.exit.signal === 'SIGKILL', 'Missing crash/cold execution');
    pending(first.barrier.snapshot);
    refused(runs[1]);
    unchanged(first.barrier.snapshot, final(runs[1]));
    history(final(runs[2]), true);
    flushed(runs[2]);
    prefix(first.barrier.snapshot, final(runs[2]));
    requireThat(
      runs[2].result.owner !== first.barrier.owner && !runs[2].result.failure && runs[3].result.value === false,
      'Cold owner/single successor postcondition',
    );
  } else if (scenario.outcome === 'unknown') {
    requireThat(
      runs.length === 5 && first.result.failure?.code === 'AUTOLOOP_AGENT_GENERATION_CONFLICT',
      'Unknown ownership was not BLOCKED',
    );
    for (const run of runs) {
      pending(final(run));
      same(rows(final(run)), [], 'Ambiguous state wrote generation evidence');
      same(registry(final(run)), first.input.seed, 'Wrong tuple changed ownership');
    }
    requireThat(
      runs.slice(1).every((run) => run.result.value === false),
      'Mismatched tuple accepted',
    );
  } else if (scenario.outcome === 'sync') {
    requireThat(
      runs.length === 2 &&
        first.result.failure?.code === 'AUTOLOOP_LEDGER_FILE_SYNC_INCOMPLETE' &&
        first.values.some((v) => v.kind === 'file-sync-fault'),
      'Missing persistence failure',
    );
    pending(final(first));
    history(final(first), false);
    history(final(runs[1]), true);
    flushed(runs[1]);
    prefix(final(first), final(runs[1]));
  } else if (scenario.outcome === 'partial') {
    requireThat(
      runs.length === 3 &&
        first.values.some((v) => v.kind === 'short-write' && v.data.written > 0 && v.data.written < v.data.requested) &&
        first.result.failure,
      'Missing actual partial write',
    );
    pending(final(first));
    requireThat(
      runs[1].result.failure?.code === 'AUTOLOOP_AGENT_LEDGER_INVALID',
      'Ambiguous partial ledger did not BLOCK',
    );
    same(ledger(final(first)), ledger(final(runs[1])), 'Partial history rewritten');
    refused(runs[2]);
  } else if (scenario.outcome === 'shutdown') {
    requireThat(
      runs.length === 3 &&
        first.barrier?.stage === 'after-durable-release' &&
        first.result.value.closedRelease === false,
      'Shutdown did not close release capability',
    );
    const start = first.values.findIndex((v) => v.kind === 'shutdown-requested'),
      end = first.values.findIndex((v) => v.kind === 'shutdown-return');
    requireThat(
      start >= 0 && end > first.values.findIndex((v) => v.kind === 'barrier-resumed') && end > start,
      'Shutdown completed before release',
    );
    refused(runs[1]);
    unchanged(first.barrier.snapshot, final(runs[1]));
    released(final(first));
    flushed(first);
    history(final(runs.at(-1)), true);
  } else if (scenario.outcome === 'stale') {
    requireThat(runs.length === 2 && first.barrier?.stage === 'loaded', 'Missing stale snapshot barrier');
    history(final(runs[1]), true);
    unchanged(final(first), final(runs[1]));
    same(ledger(final(first)), ledger(final(runs[1])), 'Stale shutdown changed ledger');
  } else throw new Error('Unsupported legacy predicate');
  return title;
}

export const DURABILITY_CASES = [
  'before-send',
  'after-capture',
  'after-ack',
  'target',
  'torn',
  'intent-fsync',
  'digest-mismatch',
  'review-checkpoint',
];
const DELIVERY_SOURCE = 'src/__tests__/autoloop-trust-recovery-delivery.test.ts';
const DELIVERY_IMPORTS = [
  'session-manager.ts',
  'autoloop/dispatcher.ts',
  'autoloop/runner.ts',
  'autoloop/messages.ts',
  'logger.ts',
  'autoloop/secure-ledger.ts',
].map((file) => `src/${file}`);
const gitSource = (head, file) => {
  if (head === undefined) return fs.readFileSync(path.join(PROJECT, file));
  requireThat(/^[a-f0-9]{40}$/.test(head), 'durability: invalid source head');
  return execFileSync('rtk', ['proxy', 'git', 'show', `${head}:${file}`], {
    cwd: PROJECT,
    maxBuffer: 16 * 1024 * 1024,
  });
};
const canonical = (value) =>
  JSON.stringify(value, function (key, item) {
    return item && typeof item === 'object' && !Array.isArray(item)
      ? Object.fromEntries(
          Object.keys(item)
            .sort()
            .map((k) => [k, item[k]]),
        )
      : item;
  });

/** Recompute one independently selected scenario from the worker's actual files.
 * readBytes can enforce an outer hashed inventory; it cannot supply predicates.
 * A copied scenario may keep its historical absolute paths, but not its identity.
 */
export function verifyDurabilityCase(directory, caseId, sourceHead, readBytes) {
  const check = (value, label) => requireThat(value, `durability ${caseId}: ${label}`);
  const eq = (a, b, label) => check(isDeepStrictEqual(a, b), label);
  check(DURABILITY_CASES.includes(caseId), 'unknown scenario');
  const read = readBytes ?? ((name) => fs.readFileSync(containedPath(directory, name)));
  const json = (name) => JSON.parse(read(name));
  const rows = (name) => {
    const text = read(name).toString();
    check(text.endsWith('\n'), `torn artifact ${name}`);
    return text.trim().split('\n').filter(Boolean).map(JSON.parse);
  };
  const optionalRows = (name) => {
    try {
      return rows(name);
    } catch (error) {
      if (error.code === 'ENOENT') return [];
      throw error;
    }
  };
  const source = gitSource(sourceHead, DELIVERY_SOURCE).toString();
  const worker = source.match(/const worker = String.raw`([\s\S]*?)`;\n/);
  check(worker, 'missing committed worker');
  eq(read('worker.mjs').toString(), worker[1], 'worker differs from committed scenario');
  const review = caseId === 'review-checkpoint',
    negative = ['target', 'torn'].includes(caseId);
  const fault = caseId === 'intent-fsync' ? 'fsync' : caseId === 'digest-mismatch' ? 'digest' : undefined;
  const boundary = review ? 'after-ack' : negative ? 'before-send' : fault ? 'none' : caseId;
  const ids = fault ? ['failed'] : ['crashed', 'cold'];
  const processes = ids.map((id) => {
    const e = json(`${id}.execution.json`),
      events = rows(`${id}/events.jsonl`);
    eq(
      { id: e.input.id, boundary: e.input.boundary, cold: e.input.cold, fault: e.input.fault, review: e.input.review },
      { id, boundary: id === 'cold' && (review || negative) ? 'none' : boundary, cold: id === 'cold', fault, review },
      'scenario descriptor mismatch',
    );
    eq(path.resolve(e.input.project), path.resolve(PROJECT), 'wrong source subject');
    check(!e.timedOut && Date.parse(e.ended_at) >= Date.parse(e.started_at), 'invalid process interval');
    eq(e.argv.slice(0, 2), ['rtk', 'proxy'], 'unexpected launcher');
    eq(e.argv.slice(3), ['--import', 'tsx', '--input-type=module', '-'], 'unexpected process command');
    check(path.basename(e.argv[2]) === 'node', 'unexpected executable');
    const out = read(`${id}.stdout.txt`),
      err = read(`${id}.stderr.txt`);
    eq(sha256(out), e.stdout_sha256, 'stdout hash');
    eq(sha256(err), e.stderr_sha256, 'stderr hash');
    check(events.length > 1 && Number.isInteger(events[0].process_id), 'missing process events');
    events.forEach((event, i) => {
      eq(event.sequence, i, 'event ordering');
      eq(event.process_id, events[0].process_id, 'mixed process');
    });
    eq(events[0].kind, 'source-imports', 'missing source observations');
    eq(
      events[0].value,
      DELIVERY_IMPORTS.map((file) => ({ path: path.join(PROJECT, file), sha256: sha256(gitSource(sourceHead, file)) })),
      'fabricated source hashes',
    );
    if (id === 'crashed') {
      eq(e.signal, 'SIGKILL', 'missing witnessed death');
      eq(e.code, null, 'crash exit');
      check(
        out
          .toString()
          .split('\n')
          .some((line) => line === JSON.stringify({ barrier: boundary })),
        'missing barrier stdout',
      );
      eq(events.at(-1).kind, 'barrier', 'missing last barrier');
      eq(events.at(-1).value, boundary, 'wrong barrier');
    } else {
      eq(e.signal, null, 'unexpected process loss');
      eq(e.code, fault || negative ? 2 : 0, 'hidden process failure');
      check(out.toString().includes(JSON.stringify({ done: true, exit: e.code })), 'missing process completion');
      check(
        events.some((x) => x.kind === (fault || negative ? 'failure' : 'final-state')),
        'missing API terminal outcome',
      );
      if (!fault && !negative)
        check(!events.some((x) => ['failure', 'runner-error'].includes(x.kind)), 'hidden API failure');
    }
    return { id, e, events, pid: events[0].process_id };
  });
  if (processes.length === 2) {
    check(processes[0].pid !== processes[1].pid, 'warm process replay');
    eq(processes[0].e.input.directory, processes[1].e.input.directory, 'different persisted workspace');
    check(Date.parse(processes[1].e.started_at) >= Date.parse(processes[0].e.ended_at), 'cold process preceded death');
  }
  const message = json('message-A.json');
  eq(
    { iter: message.iter, from: message.from, to: message.to, type: message.type },
    review
      ? { iter: 1, from: 'runner', to: 'reviewer', type: 'review_request' }
      : { iter: 0, from: 'planner', to: 'coder', type: 'directive' },
    'wrong logical message',
  );
  if (!review)
    eq(
      message.payload,
      {
        goal: 'directive A: preserve these exact bytes',
        constraints: ['immutable A'],
        success_criteria: ['one logical effect'],
        max_attempts: 1,
      },
      'changed A payload',
    );
  const initialName = fault ? 'failed/failure-decisions.jsonl' : `crashed/${boundary}-decisions.jsonl`;
  const initial = rows(initialName),
    intents = initial.filter((x) => x.delivery_id && x.kind && !x.record_type);
  eq(intents.length, 1, 'missing unique intent');
  const intent = intents[0];
  eq(intent.schema_version, 1, 'noncanonical intent');
  eq(intent.target_role, message.to, 'wrong target');
  eq(intent.target_generation, 1, 'wrong initial generation');
  eq(intent.kind, review ? 'review_request' : 'coder_directive', 'wrong intent kind');
  eq(
    intent.idempotency_key,
    'dispatch_' +
      sha256(JSON.stringify(['trust-delivery', message.msg_id, message.iter, message.from, message.to, message.type])),
    'changed idempotency key',
  );
  eq(intent.payload_sha256, sha256(canonical(intent.payload)), 'payload digest mismatch');
  const envelope = {
    msg_id: message.msg_id,
    iter: message.iter,
    from: message.from,
    to: message.to,
    type: message.type,
    ts: message.ts,
    payload: message.payload,
  };
  eq(intent.payload.logical_message_sha256, sha256(JSON.stringify(envelope)), 'logical envelope digest mismatch');
  const checkpoint = review ? read('checkpoint.txt').toString().trim() : null;
  const prompt = review
    ? `[review_request iter=1]\nArtifacts staged from run source-checkpoint iter 0 at: iter-1/ (directive.json, diff.patch, eval_output.json)\ncheckpoint_sha: ${checkpoint}\nscope: ["durable-delivery"]\nprior_verdict: (none)\nprior_metrics: []\n\nAudit and emit \`review_complete\`.`
    : '[directive iter=0]\ngoal: directive A: preserve these exact bytes\nconstraints:\n  - immutable A\nsuccess_criteria:\n  - one logical effect\nmax_attempts: 1\nRead plan.md / goal.json, make the change, run the evaluator, then emit `iter_complete`.';
  eq(intent.payload.prompt, prompt, 'changed rendered payload');
  const suffix = `\n\n<autoloop_delivery delivery_id="${intent.delivery_id}" payload_sha256="${intent.payload_sha256}">\nEcho both fields unchanged in iter_complete, request_clarification, or review_complete.\n</autoloop_delivery>`;
  const recipients = optionalRows('recipient.jsonl'),
    effects = optionalRows('receiver-effects.jsonl');
  const count = negative || fault === 'fsync' ? 0 : boundary === 'after-capture' ? 2 : 1;
  eq(recipients.length, count, 'recipient inventory');
  eq(effects, count ? [recipients[0]] : [], 'duplicate/altered receiver effects');
  const captures = processes.flatMap((p) =>
    p.events.filter((x) => x.kind === 'recipient-captured').map((x) => x.value),
  );
  eq(captures, recipients, 'recipient event/file disagreement');
  recipients.forEach((recipient) => {
    eq(recipient.prompt, prompt + suffix, 'changed recipient payload');
    eq(recipient.role, message.to, 'changed recipient role');
    eq(recipient.delivery_id, intent.delivery_id, 'changed delivery id');
    eq(recipient.payload_sha256, intent.payload_sha256, 'changed recipient digest');
    const p = processes.find((p) => p.pid === recipient.process_id);
    check(p, 'unknown recipient process');
    check(
      p.events.some(
        (x) =>
          x.kind === 'engine-created' &&
          x.value.session_id === recipient.session_id &&
          x.value.name.endsWith('-' + message.to),
      ),
      'unproved receiver session',
    );
  });
  for (const p of processes) {
    for (const transport of p.events.filter((x) => x.kind === 'transport-enter')) {
      const snapshot = rows(`${p.id}/transport-1-decisions.jsonl`);
      check(
        snapshot.some((x) => isDeepStrictEqual(x, intent)),
        'transport before durable complete intent',
      );
      const preceding = p.events.slice(0, transport.sequence);
      const lastMessage = preceding.findLastIndex(
        (x) => x.kind === 'runner-message' && x.value.msg_id === message.msg_id,
      );
      const syncs = preceding.slice(lastMessage + 1).filter((x) => x.kind === 'fsync-return');
      check(
        syncs.some((x) => x.value.endsWith('/decisions.jsonl')) &&
          syncs.some((x) => x.value.endsWith('/tasks/trust-delivery')),
        'transport before file/directory fsync',
      );
    }
  }
  if (negative) {
    const altered = read('altered-input.jsonl');
    if (caseId === 'torn')
      eq(altered, Buffer.concat([read(initialName), Buffer.from('{"schema_version":')]), 'wrong torn fault');
    else {
      const expected = structuredClone(initial);
      expected.find((x) => x.kind === 'coder_directive').target_role = 'reviewer';
      eq(altered.toString(), expected.map((x) => JSON.stringify(x) + '\n').join(''), 'wrong target fault');
    }
    eq(read('cold/failure-decisions.jsonl'), altered, 'blocked ledger changed');
    check(
      !processes[1].events.some((x) => ['transport-enter', 'ack-consumed'].includes(x.kind)),
      'blocked case performed effect',
    );
    return { case_id: caseId };
  }
  if (fault) {
    eq(
      initial.filter((x) => x.acknowledged_at),
      [],
      'failed delivery was acknowledged',
    );
    check(!processes[0].events.some((x) => x.kind === 'ack-consumed'), 'failed phase advanced');
    const failure = processes[0].events.find((x) => x.kind === 'failure').value;
    eq(
      failure.code,
      fault === 'fsync' ? 'AUTOLOOP_DELIVERY_COMMITTED_OBSERVATION_FAILED' : 'AUTOLOOP_CONTROL_MALFORMED',
      'wrong fault outcome',
    );
    if (fault === 'fsync')
      check(
        processes[0].events.some((x) => x.kind === 'injected-fsync-failure'),
        'missing persistence fault',
      );
    return { case_id: caseId };
  }
  const finalBytes = read('cold/final-decisions.jsonl'),
    final = rows('cold/final-decisions.jsonl');
  eq(finalBytes.subarray(0, read(initialName).length), read(initialName), 'historical prefix rewritten');
  eq(
    final.filter((x) => x.delivery_id && x.kind && !x.record_type),
    [intent],
    'replayed intent changed',
  );
  check(
    final.slice(initial.length).every((x) => x.schema_version === 1),
    'noncanonical appended delivery',
  );
  const acks = final.filter((x) => x.acknowledged_at);
  eq(acks.length, 1, 'missing unique durable ACK');
  eq(acks[0].delivery_id, intent.delivery_id, 'ACK identity');
  eq(acks[0].payload_sha256, intent.payload_sha256, 'ACK digest');
  eq(initial.filter((x) => x.acknowledged_at).length, boundary === 'after-ack' ? 1 : 0, 'wrong crash/ACK boundary');
  const cold = processes[1];
  const messages = cold.events.filter((x) => x.kind === 'runner-message').map((x) => x.value);
  check(messages.filter((x) => isDeepStrictEqual(x, message)).length >= 2, 'missing concurrent exact retries');
  check(
    cold.events.some((x) => x.kind === 'B-rejected'),
    'conflicting directive not refused',
  );
  const consumed = cold.events.filter((x) => x.kind === 'ack-consumed');
  if (!review) {
    check(consumed.length >= 1, 'missing ACK phase consumption');
    consumed.forEach((x) => eq(x.value.ack_rows, acks, 'phase before matching durable ACK'));
  }
  for (const p of processes) {
    const captured = p.events.find((x) => x.kind === 'recipient-captured');
    if (captured && !(p.id === 'crashed' && boundary === 'after-capture')) {
      const end =
        p.id === 'crashed'
          ? p.events.length
          : (p.events.find((x) => x.kind === 'ack-consumed')?.sequence ?? p.events.length);
      check(
        p.events
          .slice(captured.sequence + 1, end)
          .some((x) => x.kind === 'fsync-return' && x.value.endsWith('/decisions.jsonl')),
        'ACK before captured receiver or durable fsync',
      );
    }
  }
  const rebinds = final.filter((x) => x.record_type === 'delivery_generation_rebind');
  eq(rebinds.length, boundary === 'after-ack' ? 0 : 1, 'wrong rebind count');
  for (const rebind of rebinds) {
    for (const key of ['delivery_id', 'idempotency_key', 'kind', 'target_role', 'payload_sha256'])
      eq(rebind[key], intent[key], 'rebind identity');
    eq([rebind.from_generation, rebind.to_generation], [1, 2], 'rebind generation');
    const generations = rows('cold/final-generations.jsonl'),
      old = rows(`crashed/${boundary}-generations.jsonl`);
    eq(
      read('cold/final-generations.jsonl').subarray(0, read(`crashed/${boundary}-generations.jsonl`).length),
      read(`crashed/${boundary}-generations.jsonl`),
      'generation history changed',
    );
    const live = old.find((x) => x.kind === 'agent_generation_started' && x.payload.role === message.to);
    check(live, 'missing original owner');
    const role = generations.filter((x) => x.payload.role === message.to);
    eq(
      role.map((x) => x.kind),
      [
        'agent_generation_reserved',
        'agent_generation_started',
        'agent_generation_orphaned',
        'agent_generation_released',
        'agent_generation_reserved',
        'agent_generation_started',
      ],
      'missing release before replacement',
    );
    for (const row of role.slice(2, 4))
      for (const key of ['generation', 'session_id', 'session_name', 'owner_instance_id'])
        eq(row.payload[key], live.payload[key], 'release ownership mismatch');
    eq(role[4].payload.generation, 2, 'nonmonotonic successor');
    check(role[4].payload.owner_instance_id !== live.payload.owner_instance_id, 'same owner replacement');
    eq(Date.parse(role[4].ts) - Date.parse(live.ts), 300000, 'wrong controlled cold clock');
    check(Date.parse(role[4].ts) > Date.parse(live.payload.lease_expires_at), 'lease not crossed');
  }
  if (review) {
    check(/^[a-f0-9]{40}$/.test(checkpoint), 'invalid checkpoint');
    eq(
      read('source.patch'),
      execFileSync(
        'rtk',
        [
          'proxy',
          'git',
          'show',
          '--no-ext-diff',
          '--no-textconv',
          '--format=',
          '--unified=3',
          '--no-renames',
          checkpoint,
          '--',
        ],
        { cwd: PROJECT, maxBuffer: 16 * 1024 * 1024 },
      ),
      'wrong checkpoint patch',
    );
    eq(message.payload.checkpoint_sha, checkpoint, 'wrong review subject');
    const verdict = json('workspace/tasks/trust-delivery/iter/1/verdict.json');
    eq(verdict.iter, 1, 'wrong review iteration');
    eq(verdict.decision, 'hold', 'wrong observed verdict');
    check(
      !processes.some((p) => p.events.some((x) => x.kind === 'engine-created' && x.value.name.endsWith('-coder'))),
      'review started Coder',
    );
    eq(final.filter((x) => x.kind === 'request_review').length, 1, 'duplicate review request');
  }
  return { case_id: caseId };
}

export const RESUME_CASES = [
  'generation-reset',
  'terminated-generation',
  'missing-legacy',
  'missing-release',
  'same-owner',
  'wrong-session',
  'wrong-generation',
  'late-start',
  'future-created',
  'torn',
  'decreasing',
  'wrong-dispatch',
]
  .map((name) => `trust-timeout-${name}`)
  .concat(['trust-real-uncertain-recovery-claim']);

export function verifyResumeCase(directory, caseId, sourceHead, readBytes) {
  const check = (value, label) => requireThat(value, `durability ${caseId}: ${label}`);
  const eq = (a, b, label) => check(isDeepStrictEqual(a, b), label);
  check(RESUME_CASES.includes(caseId), 'unknown resume scenario');
  const read = readBytes ?? ((name) => fs.readFileSync(containedPath(directory, name)));
  const parse = (bytes) => bytes.toString().trim().split('\n').filter(Boolean).map(JSON.parse);
  const api = JSON.parse(read('api.json'));
  eq(api.schema_version, 1, 'API observation version');
  eq(api.scenario, caseId, 'relabelled API scenario');
  eq(
    api.source_imports,
    ['src/session-manager.ts', 'src/__tests__/session-manager.test.ts'].map((file) => ({
      path: path.join(PROJECT, file),
      sha256: sha256(gitSource(sourceHead, file)),
    })),
    'wrong API source identity',
  );
  check(Number.isInteger(api.process_id) && api.process_id > 0, 'missing observer process');
  eq(api.before.handle_present, false, 'not cold reconstruction');
  const before = read('decisions.input.jsonl'),
    after = read('decisions.after.jsonl');
  const generations = read('generations.input.jsonl'),
    generationAfter = read('generations.after.jsonl');
  eq(after, before, 'resume rewrote decision history');
  if (caseId === 'trust-real-uncertain-recovery-claim') {
    eq(api.request.method, 'autoloopRecover', 'wrong recovery API');
    eq(api.request.run_id, caseId, 'wrong recovery run');
    eq(api.request.options.apply, true, 'not an apply operation');
    check(/^[a-f0-9]{64}$/.test(api.request.options.recovery_token), 'missing actual recovery token');
    const receipts = parse(before).filter((x) => x.record_type === 'autoloop_recovery_receipt');
    eq(receipts.length, 1, 'uncertain claim inventory');
    const claim = receipts[0];
    eq(claim.schema_version, 1, 'noncanonical claim');
    eq(claim.status, 'prepared', 'not uncertain prepared effect');
    eq(claim.run_id, caseId, 'wrong claim subject');
    check(
      claim.claim_id && claim.action_snapshot && claim.action_sha256 && claim.recovery_token,
      'incomplete prepared claim',
    );
    eq(claim.action_sha256, sha256(canonical(claim.action_snapshot)), 'altered uncertain action');
    const original = read('before.jsonl');
    eq(before.subarray(0, original.length), original, 'preparation changed history');
    eq(before, read('prepared-and-cold-rejected.jsonl'), 'uncertain bytes changed');
    eq(api.response.error?.code, 'AUTOLOOP_RECOVERY_INCOMPLETE', 'uncertainty was not blocked');
    eq(api.response.error?.retryable, false, 'unsafe retry permitted');
    eq(api.after, api.before, 'uncertain retry created an effect');
    eq(generationAfter, generations, 'uncertain retry changed ownership');
    return { case_id: caseId };
  }
  const runId = caseId === 'trust-timeout-missing-legacy' ? RUN_ID : caseId;
  eq(api.request, { method: 'autoloopResume', run_id: runId }, 'wrong resume request');
  eq(read('spec.after.jsonl'), read('spec.input.jsonl'), 'immutable configuration rewritten');
  const success = ['trust-timeout-generation-reset', 'trust-timeout-terminated-generation'].includes(caseId);
  if (!success) {
    check(
      api.response.error &&
        typeof api.response.error.message === 'string' &&
        /timeout.*(chain|generation)|malformed/i.test(api.response.error.message),
      'missing blocking API error',
    );
    eq(api.after.handle_present, false, 'blocked resume created a handle');
    eq(generationAfter, generations, 'blocked resume changed generation ledger');
    if (caseId === 'trust-timeout-missing-legacy') {
      eq(
        sha256(before),
        'c09281dc6c5bca5f5cc2ddae128b7532c89542506bda026afd3dad5c86d1a55f',
        'wrong historical incident bytes',
      );
      eq(generations.toString(), 'null\n', 'missing generation scenario invented evidence');
      return { case_id: caseId };
    }
    // Independent fault descriptors specify the exact mutation, not an API
    // status label. The unmodified, actually produced history remains retained.
    const clean = read('generations.before.jsonl'),
      cleanDecisions = read('decisions.before.jsonl');
    let expected = parse(clean);
    const first = expected.find((x) => x.kind === 'agent_generation_started').payload;
    const fault = caseId.slice('trust-timeout-'.length);
    if (fault === 'missing-release')
      expected = expected.filter((x) => !(x.kind === 'agent_generation_released' && x.payload.generation === 1));
    if (fault === 'same-owner')
      expected.forEach((x) => {
        x.payload.owner_instance_id = first.owner_instance_id;
      });
    if (fault === 'wrong-session')
      expected.find((x) => x.kind === 'agent_generation_released').payload.session_id = 'different-physical-session';
    if (fault === 'wrong-generation')
      expected.forEach((x) => {
        if (x.payload.generation === 2) x.payload.generation = 3;
      });
    const actual = fault === 'torn' ? null : parse(generations);
    if (fault === 'late-start' || fault === 'future-created') {
      const field = fault === 'late-start' ? 'ts' : 'created_at';
      const reference = actual.find((x) => x.kind === 'agent_generation_started' && x.payload.generation === 2);
      const timestamp = field === 'ts' ? reference.ts : reference.payload.created_at;
      check(
        Date.parse(timestamp) > Math.max(...parse(cleanDecisions).map((x) => Date.parse(x.ts))),
        'fault timestamp did not cross timeout',
      );
      expected.forEach((x) => {
        if (x.payload.generation === 2) {
          if (field === 'created_at') x.payload.created_at = timestamp;
          else if (x.kind === 'agent_generation_started') x.ts = timestamp;
        }
      });
    }
    eq(
      generations.toString(),
      expected.map((x) => JSON.stringify(x) + '\n').join('') + (fault === 'torn' ? '{' + '"kind":' : ''),
      'wrong lifecycle fault',
    );
    const decisions = parse(cleanDecisions),
      migration = decisions.filter((x) => x.kind === 'timeout_migration').at(-1);
    if (fault === 'decreasing') migration.newValue = 599999;
    if (fault === 'wrong-dispatch') migration.pendingDispatchId = 'unrelated-dispatch';
    eq(before.toString(), decisions.map((x) => JSON.stringify(x) + '\n').join(''), 'wrong timeout fault');
    return { case_id: caseId };
  }
  check(api.response.value && !api.response.error, 'missing successful public outcome');
  eq(api.response.value.run_id, runId, 'wrong returned run');
  eq(api.after.handle_present, true, 'no resumed handle');
  eq(api.after.effective_timeout_ms, 1200000, 'wrong effective timeout');
  const decisions = parse(before),
    migrations = decisions.filter((x) => x.kind === 'timeout_migration');
  eq(
    migrations.map((x) => [x.oldValue, x.newValue]),
    [
      [600000, 1200000],
      [1200000, 2400000],
      [2400000, 3600000],
      [600000, 1200000],
    ],
    'wrong migration chain',
  );
  for (const migration of migrations) {
    eq(migration.schema_version, 1, 'noncanonical migration');
    eq(migration.runId, runId, 'wrong migration subject');
    if (caseId === 'trust-timeout-terminated-generation' && migration === migrations.at(-1)) {
      eq(migration.reason, 'stored_run_resume', 'wrong terminated migration reason');
      eq(migration.pendingDispatchId, undefined, 'invented terminated pending dispatch');
      const firstApi = JSON.parse(read('first-resume/api.json'));
      eq(firstApi.source_imports, api.source_imports, 'first resume source mismatch');
      eq(
        firstApi.request,
        { method: 'autoloopResume', run_id: runId, options: { sendTimeoutMs: 1200000 } },
        'wrong first resume request',
      );
      eq(firstApi.before.handle_present, false, 'first resume was warm');
      eq(firstApi.after.effective_timeout_ms, 1200000, 'first migration ineffective');
      eq(firstApi.response.value?.run_id, runId, 'missing first resume result');
      const firstBefore = read('first-resume/decisions.input.jsonl'),
        firstAfter = read('first-resume/decisions.after.jsonl');
      eq(firstAfter.subarray(0, firstBefore.length), firstBefore, 'first resume rewrote history');
      eq(parse(firstAfter.subarray(firstBefore.length)), [migration], 'wrong persisted first migration');
      eq(before.subarray(0, firstAfter.length), firstAfter, 'cold retry lost first migration');
      eq(parse(firstBefore).at(-1).kind, 'terminate', 'reset not terminated');
      check(
        parse(firstBefore).some((x) => x.kind === 'send_timeout' && x.payload.timeout_ms === 600000),
        'missing reset timeout',
      );
      continue;
    }
    check(
      decisions.some(
        (x) =>
          x.kind === 'send_timeout' &&
          x.payload.dispatch_id === migration.pendingDispatchId &&
          x.payload.timeout_ms === migration.oldValue &&
          Date.parse(x.ts) <= Date.parse(migration.ts),
      ),
      'migration has no matching timeout',
    );
  }
  const lifecycle = parse(generations).filter((x) => x.payload.role === 'planner');
  const started = lifecycle.filter((x) => x.kind === 'agent_generation_started');
  check(started.length >= 2, 'missing generations');
  eq(
    started.slice(0, 2).map((x) => x.payload.generation),
    [1, 2],
    'generation gap',
  );
  check(started[0].payload.owner_instance_id !== started[1].payload.owner_instance_id, 'generation owner reused');
  const released = lifecycle.find((x) => x.kind === 'agent_generation_released' && x.payload.generation === 1);
  check(released, 'missing durable release');
  for (const key of ['session_id', 'owner_instance_id', 'session_name'])
    eq(released.payload[key], started[0].payload[key], 'release identity');
  const reset = decisions.filter((x) => x.kind === 'send_timeout').at(-1);
  check(
    Date.parse(released.ts) < Date.parse(started[1].ts) &&
      Date.parse(started[1].ts) <= Date.parse(reset.ts) &&
      Date.parse(started[1].payload.created_at) <= Date.parse(reset.ts),
    'reset lacks preceding lifecycle proof',
  );
  eq(generationAfter.subarray(0, generations.length), generations, 'resume changed historical generation prefix');
  const suffix = parse(generationAfter.subarray(generations.length));
  check(suffix.length >= 2 && suffix.every((x) => x.schema_version === 1), 'missing canonical replacement events');
  const successor = suffix.filter((x) => x.kind === 'agent_generation_started' && x.payload.role === 'planner');
  eq(successor.length, 1, 'duplicate/missing resumed generation');
  eq(successor[0].payload.generation, Math.max(...started.map((x) => x.payload.generation)) + 1, 'nonmonotonic resume');
  return { case_id: caseId };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  // Finish module evaluation before loading collect, which imports this API.
  setImmediate(async () => {
    try {
      const [mode, directory, ...extra] = process.argv.slice(2);
      if (mode === 'durability' && extra.length === 0) {
        const { verifyDurability } = await import('./collect.mjs');
        process.stdout.write(JSON.stringify({ ...verifyDurability(directory), verified: true }) + '\n');
        return;
      }
      if (mode === 'legacy' && extra.length === 0) {
        const { verifyLegacy } = await import('./collect.mjs');
        const result = verifyLegacy(directory && path.resolve(directory));
        process.stdout.write(JSON.stringify({ ...result, verified: true }) + '\n');
        return;
      }
      requireThat(
        mode === 'preparation' && directory && extra.length === 0,
        'No acceptance proof: use preparation <attempt-directory>. Incident/final gates require their later reviewed scenario contracts; this harness cannot certify them.',
      );
      const { verifyPreparation } = await import('./collect.mjs');
      verifyPreparation(path.resolve(directory));
      process.stdout.write(JSON.stringify({ scope: 'preparation', verified: true }) + '\n');
    } catch (error) {
      process.stderr.write(JSON.stringify({ verified: false, error: error.message }) + '\n');
      process.exitCode = 1;
    }
  });
}
