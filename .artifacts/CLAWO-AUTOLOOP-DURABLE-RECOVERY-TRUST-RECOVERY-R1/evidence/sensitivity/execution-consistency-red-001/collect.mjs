import { execFileSync, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  ARTIFACT_ROOT, PROJECT, RUN_ID, containedPath, inspectTestReport, readArtifact, sha256, verifyBundle,
} from './verify.mjs';

export const SOURCE_URL = import.meta.url;
const require = createRequire(import.meta.url);
const BASE = '2694c0babcf16030278e58d829a7c71bcaa0f7a2';
const FILES = ['evidence.schema.json', 'collect.mjs', 'verify.mjs', 'verify.test.mjs']
  .map((name) => `scripts/autoloop-trust-recovery/${name}`);
const SOURCES = ['verify.test.mjs', 'verify.mjs', 'collect.mjs']
  .map((name) => path.join(PROJECT, 'scripts/autoloop-trust-recovery', name));
const COMMAND = ['rtk', 'proxy', 'node', '--test', '--test-reporter=tap', FILES[3]];
const LATER_GROUPS = ['baselines', 'legacy', 'delivery', 'boundaries', 'adapters', 'focused', 'format', 'lint',
  'build', 'typecheck', 'full', 'coverage', 'e2e', 'concurrency', 'nonregression', 'live'];
const REQUIREMENTS = ['HARNESS-VALIDATION'];
const REQUIRED_TESTS = ['rejects fabricated invariant booleans', 'rejects missing artifacts',
  'rejects changed artifact hashes', 'rejects wrong subject kind', 'rejects hidden process failure',
  'a green report cannot hide a failed raw stdout stream', 'rejects Coder-authored review claims'];
const ensure = (condition, message) => { if (!condition) throw new Error(message); };

/** Every directory is checked before mkdir; a symlink cannot redirect writes. */
function outputDirectory(directory) {
  const absolute = path.resolve(directory);
  const relative = path.relative(ARTIFACT_ROOT, absolute);
  ensure(/^evidence\/(upstream|start|candidate|sensitivity)\/[^/]+(?:\/.*)?$/.test(relative) &&
    !relative.split('/').includes('..'), 'Output directory must be a run-owned evidence attempt path');
  let current = PROJECT;
  for (const segment of path.relative(PROJECT, absolute).split(path.sep)) {
    current = path.join(current, segment);
    try {
      const stat = fs.lstatSync(current);
      ensure(!stat.isSymbolicLink() && stat.isDirectory(), 'Symlink or non-directory output path');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      fs.mkdirSync(current, { mode: 0o700 });
    }
  }
  return absolute;
}

function writeArtifact(directory, name, bytes) {
  const fd = fs.openSync(path.join(directory, name), 'wx', 0o600);
  try { fs.writeFileSync(fd, bytes); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  return { path: name, sha256: sha256(bytes) };
}

const REDACTIONS = [
  ['bearer-token', /(\bAuthorization:\s*Bearer\s+)[A-Za-z0-9._~+/=-]+/gi],
  ['api-key-assignment', /(\b(?:OPENAI_API_KEY|ANTHROPIC_API_KEY|API_KEY|ACCESS_TOKEN)=)[^\s'";\\]+/g],
];

function redact(text, artifact, redactions) {
  for (const [rule, pattern] of REDACTIONS) {
    let occurrences = 0;
    text = text.replace(pattern, (_match, prefix) => { occurrences++; return `${prefix}[REDACTED:${rule}]`; });
    if (occurrences) redactions.push({ artifact, rule, occurrences });
  }
  return text;
}

/**
 * Internal process boundary: callers supply literal argv in reviewed code.
 * The CLI never loads a command, cwd, or environment from an evidence record.
 * Failure records are retained as failure records and are not acceptance proof.
 */
export async function captureExecution({ directory, id, argv, cwd, timeout_ms = 30000, env = {} }) {
  ensure(/^[A-Za-z0-9_-]+$/.test(id), 'Invalid execution identity/path');
  ensure(Number.isSafeInteger(timeout_ms) && timeout_ms > 0 && timeout_ms <= 60000, 'Invalid bounded timeout');
  ensure(Array.isArray(argv) && argv.length > 0 && argv.every((arg) => typeof arg === 'string'), 'Invalid argv');
  directory = outputDirectory(directory);
  const names = [`${id}.stdout.txt`, `${id}.stderr.txt`, `${id}.execution.json`];
  for (const name of names) ensure(!fs.existsSync(path.join(directory, name)), 'Refusing to overwrite existing capture');
  // Reserve all outputs before spawning so concurrent collectors cannot reuse an ID.
  const handles = [];
  try {
    for (const name of names) handles.push(fs.openSync(path.join(directory, name), 'wx', 0o600));
    const started_at = new Date().toISOString();
    const stdout = [], stderr = [];
    let spawn_error = null, timed_out = false;
    const child = spawn(argv[0], argv.slice(1), {
      cwd, env: { ...process.env, ...env }, shell: false,
      detached: process.platform !== 'win32', stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', (bytes) => stdout.push(bytes));
    child.stderr.on('data', (bytes) => stderr.push(bytes));
    child.on('error', (error) => { spawn_error = error.message; });
    const timer = setTimeout(() => {
      timed_out = true;
      if (child.pid) {
        try { process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL'); }
        catch (error) { if (error.code !== 'ESRCH') spawn_error = `Owned process cleanup failed: ${error.message}`; }
      }
    }, timeout_ms);
    const [exit_code, signal] = await new Promise((resolve) => child.once('close', (...args) => resolve(args)));
    clearTimeout(timer);
    const ended_at = new Date().toISOString();
    const redactions = [];
    const cleanOut = redact(Buffer.concat(stdout).toString('utf8'), 'stdout', redactions);
    const cleanErr = redact(Buffer.concat(stderr).toString('utf8'), 'stderr', redactions);
    const cleanArgv = argv.map((arg) => redact(arg, 'argv', redactions));
    const report = inspectTestReport(Buffer.from(cleanOut), 'node-tap');
    const execution = {
      id, argv: cleanArgv, cwd, started_at, ended_at, process_id: child.pid ?? null,
      exit_code, signal, timed_out, spawn_error,
      stdout: { path: names[0], sha256: sha256(cleanOut) },
      stderr: { path: names[1], sha256: sha256(cleanErr) },
      report: { path: names[0], sha256: sha256(cleanOut) }, report_format: 'node-tap',
      discovered_test_ids: report.discovered, executed_test_ids: report.executed, skipped_test_ids: report.skipped,
      redactions,
    };
    for (const [index, bytes] of [cleanOut, cleanErr, JSON.stringify(execution, null, 2) + '\n'].entries()) {
      fs.writeFileSync(handles[index], bytes); fs.fsyncSync(handles[index]);
    }
    return execution;
  } finally { for (const fd of handles) fs.closeSync(fd); }
}

function git(...args) {
  return execFileSync('rtk', ['proxy', 'git', ...args], { cwd: PROJECT, encoding: 'utf8' });
}

function inputSnapshot() {
  const tracked = git('ls-files', '-z').split('\0').filter((p) =>
    /^(src|scripts|bin|docs|skills)\//.test(p) || /^(package(?:-lock)?\.json|tsconfig.*\.json|vitest\.config\.ts)$/.test(p));
  const entry = (p) => ({ path: path.resolve(PROJECT, p), sha256: sha256(fs.readFileSync(path.resolve(PROJECT, p))) });
  const inputs = [...new Set([...tracked, ...FILES])].sort();
  const dependencies = [...new Set([
    path.join(PROJECT, 'package-lock.json'),
    ...Object.keys(require.cache).filter((p) => p.includes('/node_modules/')),
  ])].sort();
  const rtk = execFileSync('rtk', ['proxy', 'which', 'rtk'], { cwd: PROJECT, encoding: 'utf8' }).trim();
  const manifest = {
    tracked: inputs.map(entry), harness: FILES.toSorted().map(entry),
    dependencies: dependencies.map(entry), tools: [process.execPath, rtk].sort().map(entry),
  };
  const head = git('rev-parse', 'HEAD').trim(), tree = git('rev-parse', 'HEAD^{tree}').trim();
  return { manifest, head, tree };
}

function patchBytes() {
  const patches = [git('diff', '--binary', 'HEAD', '--', ...FILES)];
  const tracked = new Set(git('ls-files', '-z', '--', ...FILES).split('\0'));
  for (const file of FILES) {
    if (tracked.has(file)) continue;
    try { patches.push(git('diff', '--no-index', '--binary', '--', '/dev/null', file)); }
    catch (error) { if (error.status !== 1) throw error; patches.push(error.stdout); }
  }
  return patches.join('');
}

function preparationContract(snapshot) {
  return {
    run_id: RUN_ID, case_id: 'preparation-validator', requirement_ids: REQUIREMENTS,
    subject_kind: 'candidate', base: BASE, head: snapshot.head, tree: snapshot.tree,
    input_manifest_sha256: sha256(JSON.stringify(snapshot.manifest, null, 2) + '\n'),
    harness_sha256: sha256(JSON.stringify(snapshot.manifest.harness)),
    dependency_sha256: sha256(JSON.stringify(snapshot.manifest.dependencies)),
    tool_sha256: sha256(JSON.stringify(snapshot.manifest.tools)),
    executions: [{ argv: COMMAND, cwd: PROJECT, required_test_ids: REQUIRED_TESTS }],
    assertions: SOURCES.map((source, i) => ({ id: `import-${i}`, predicate: 'literal', observations: [`source-${i}`],
      expected: snapshot.manifest.harness.find((e) => e.path === source) })),
    required_observations: SOURCES.map((_, i) => `source-${i}`),
    required_fault_observations: [], required_source_paths: SOURCES,
  };
}

function importObservations(directory, execution) {
  const stdout = readArtifact(directory, execution.stdout);
  const marker = Buffer.from('TRUST_RECOVERY_OBSERVATION ');
  const observations = [];
  let offset = 0;
  while ((offset = stdout.indexOf(marker, offset)) >= 0) {
    const start = offset + marker.length, end = stdout.indexOf('\n', start);
    ensure(end >= 0, 'Incomplete source import receipt');
    const event = JSON.parse(stdout.subarray(start, end));
    ensure(event.execution_id === execution.id, 'Wrong source import execution identity');
    observations.push({ id: `source-${observations.length}`, artifact: execution.stdout, byte_range: [start, end],
      sequence: event.sequence, process_id: event.process_id, observer_id: event.observer_id,
      execution_id: execution.id, value: event.value });
    offset = end;
  }
  return observations;
}

export async function collect(group) {
  ensure(group === 'validator' || LATER_GROUPS.includes(group), 'Unknown collector group');
  // Preparation is the only reviewed execution policy in this iteration. In
  // particular, no final group is allowed to silently run without isolation.
  ensure(group === 'validator', `Group ${group} requires a frozen candidate and its later reviewed scenario/isolation contract; no command launched`);
  const snapshot = inputSnapshot();
  const directory = outputDirectory(path.join(ARTIFACT_ROOT, 'evidence', 'candidate', `validator-${Date.now()}-${randomUUID()}`));
  const input_manifest = writeArtifact(directory, 'inputs.json', JSON.stringify(snapshot.manifest, null, 2) + '\n');
  const patch = writeArtifact(directory, 'inputs.patch', patchBytes());
  const test_sources = FILES.map((file) => writeArtifact(directory, path.basename(file), fs.readFileSync(path.join(PROJECT, file))));
  const id = `validator-${randomUUID()}`;
  const execution = await captureExecution({ directory, id, argv: COMMAND, cwd: PROJECT,
    env: { CLAWO_TRUST_CAPTURE_ID: id }, timeout_ms: 60000 });
  const contract = preparationContract(snapshot);
  const observations = importObservations(directory, execution);
  const bundle = {
    schema_version: 1, run_id: RUN_ID, case_id: contract.case_id, requirement_ids: REQUIREMENTS,
    subject_kind: 'candidate', base: BASE, head: snapshot.head, tree: snapshot.tree,
    // This records an uncommitted Preparation snapshot, never a final candidate.
    frozen: false, input_manifest, harness_sha256: contract.harness_sha256,
    dependency_sha256: contract.dependency_sha256, tool_sha256: contract.tool_sha256,
    patch, test_sources, source_imports: observations.map((o) => ({ ...o.value, observation_id: o.id })),
    executions: [execution], observations, assertions: contract.assertions,
  };
  writeArtifact(directory, 'bundle.json', JSON.stringify(bundle, null, 2) + '\n');
  ensure(JSON.stringify(inputSnapshot()) === JSON.stringify(snapshot), 'Candidate inputs changed during collection; capture is not acceptance proof');
  verifyBundle(bundle, directory, contract);
  return { directory, bundle_sha256: sha256(fs.readFileSync(path.join(directory, 'bundle.json'))), execution };
}

export function verifyPreparation(directory) {
  // Read-only: unlike collection this must never mkdir, write, or launch tests.
  const relative = path.relative(ARTIFACT_ROOT, path.resolve(directory));
  ensure(/^evidence\/candidate\/[^/]+$/.test(relative), 'Invalid Preparation attempt directory');
  const bundle = JSON.parse(fs.readFileSync(containedPath(directory, 'bundle.json')));
  return verifyBundle(bundle, directory, preparationContract(inputSnapshot()));
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    ensure(process.argv.length === 3, 'Usage: collect.mjs <group>');
    const result = await collect(process.argv[2]);
    process.stdout.write(JSON.stringify({ directory: result.directory, bundle_sha256: result.bundle_sha256, exit_code: result.execution.exit_code }) + '\n');
  } catch (error) {
    process.stderr.write(JSON.stringify({ collected: false, error: error.message }) + '\n');
    process.exitCode = 1;
  }
}
