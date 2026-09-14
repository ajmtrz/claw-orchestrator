import { createHash } from 'node:crypto';
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

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const [mode, directory, ...extra] = process.argv.slice(2);
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
}
