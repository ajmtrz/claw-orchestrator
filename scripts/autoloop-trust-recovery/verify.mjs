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
  const manifest = JSON.parse(readArtifact(directory, bundle.input_manifest));
  requireThat(validateManifest(manifest), 'Invalid input manifest schema');
  for (const entries of Object.values(manifest))
    requireThat(!duplicates(entries.map((e) => e.path)), 'Duplicate manifest path');
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

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  // Finish module evaluation before loading collect, which imports this API.
  setImmediate(async () => {
    try {
      const [mode, directory, ...extra] = process.argv.slice(2);
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
