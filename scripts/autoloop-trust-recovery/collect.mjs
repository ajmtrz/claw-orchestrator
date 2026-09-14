import { execFileSync, spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual } from 'node:util';
import {
  ARTIFACT_ROOT,
  PROJECT,
  RUN_ID,
  containedPath,
  inspectTestReport,
  readArtifact,
  readInputManifest,
  sha256,
  verifyBundle,
  LEGACY_TITLES,
  verifyLegacyCase,
  DURABILITY_CASES,
  RESUME_CASES,
  verifyDurabilityCase,
  verifyResumeCase,
} from './verify.mjs';

export const SOURCE_URL = import.meta.url;
const require = createRequire(import.meta.url);
const BASE = '2694c0babcf16030278e58d829a7c71bcaa0f7a2';
const FILES = ['evidence.schema.json', 'collect.mjs', 'verify.mjs', 'verify.test.mjs'].map(
  (name) => `scripts/autoloop-trust-recovery/${name}`,
);
const SOURCES = ['verify.test.mjs', 'verify.mjs', 'collect.mjs'].map((name) =>
  path.join(PROJECT, 'scripts/autoloop-trust-recovery', name),
);
const COMMAND = ['rtk', 'proxy', 'node', '--test', '--test-reporter=tap', FILES[3]];
const LATER_GROUPS = [
  'baselines',
  'legacy',
  'delivery',
  'boundaries',
  'adapters',
  'focused',
  'format',
  'lint',
  'build',
  'typecheck',
  'full',
  'coverage',
  'e2e',
  'concurrency',
  'nonregression',
  'live',
];
const REQUIREMENTS = ['HARNESS-VALIDATION'];
const REQUIRED_TESTS = [
  'rejects fabricated invariant booleans',
  'rejects missing artifacts',
  'rejects changed artifact hashes',
  'rejects wrong subject kind',
  'rejects hidden process failure',
  'a green report cannot hide a failed raw stdout stream',
  'rejects Coder-authored review claims',
];
const ensure = (condition, message) => {
  if (!condition) throw new Error(message);
};

/** Every directory is checked before mkdir; a symlink cannot redirect writes. */
function outputDirectory(directory) {
  const absolute = path.resolve(directory);
  const relative = path.relative(ARTIFACT_ROOT, absolute);
  ensure(
    /^evidence\/(upstream|start|candidate|sensitivity)\/[^/]+(?:\/.*)?$/.test(relative) &&
      !relative.split('/').includes('..'),
    'Output directory must be a run-owned evidence attempt path',
  );
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
  try {
    fs.writeFileSync(fd, bytes);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  return { path: name, sha256: sha256(bytes) };
}

const REDACTIONS = [
  ['bearer-token', /(\bAuthorization:\s*Bearer\s+)[A-Za-z0-9._~+/=-]+/gi],
  ['api-key-assignment', /(\b(?:OPENAI_API_KEY|ANTHROPIC_API_KEY|API_KEY|ACCESS_TOKEN)=)[^\s'";\\]+/g],
];

function redact(text, artifact, redactions) {
  for (const [rule, pattern] of REDACTIONS) {
    let occurrences = 0;
    text = text.replace(pattern, (_match, prefix) => {
      occurrences++;
      return `${prefix}[REDACTED:${rule}]`;
    });
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
  ensure(Number.isSafeInteger(timeout_ms) && timeout_ms > 0 && timeout_ms <= 600000, 'Invalid bounded timeout');
  ensure(Array.isArray(argv) && argv.length > 0 && argv.every((arg) => typeof arg === 'string'), 'Invalid argv');
  directory = outputDirectory(directory);
  const names = [`${id}.stdout.txt`, `${id}.stderr.txt`, `${id}.execution.json`];
  for (const name of names)
    ensure(!fs.existsSync(path.join(directory, name)), 'Refusing to overwrite existing capture');
  // Reserve all outputs before spawning so concurrent collectors cannot reuse an ID.
  const handles = [];
  try {
    for (const name of names) handles.push(fs.openSync(path.join(directory, name), 'wx', 0o600));
    const started_at = new Date().toISOString();
    const stdout = [],
      stderr = [];
    let spawn_error = null,
      timed_out = false;
    const child = spawn(argv[0], argv.slice(1), {
      cwd,
      env: { ...process.env, ...env },
      shell: false,
      detached: process.platform !== 'win32',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', (bytes) => stdout.push(bytes));
    child.stderr.on('data', (bytes) => stderr.push(bytes));
    child.on('error', (error) => {
      spawn_error = error.message;
    });
    const timer = setTimeout(() => {
      timed_out = true;
      if (child.pid) {
        try {
          process.kill(process.platform === 'win32' ? child.pid : -child.pid, 'SIGKILL');
        } catch (error) {
          if (error.code !== 'ESRCH') spawn_error = `Owned process cleanup failed: ${error.message}`;
        }
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
      id,
      argv: cleanArgv,
      cwd,
      started_at,
      ended_at,
      process_id: child.pid ?? null,
      exit_code,
      signal,
      timed_out,
      spawn_error,
      stdout: { path: names[0], sha256: sha256(cleanOut) },
      stderr: { path: names[1], sha256: sha256(cleanErr) },
      report: { path: names[0], sha256: sha256(cleanOut) },
      report_format: 'node-tap',
      discovered_test_ids: report.discovered,
      executed_test_ids: report.executed,
      skipped_test_ids: report.skipped,
      redactions,
    };
    for (const [index, bytes] of [cleanOut, cleanErr, JSON.stringify(execution, null, 2) + '\n'].entries()) {
      fs.writeFileSync(handles[index], bytes);
      fs.fsyncSync(handles[index]);
    }
    return execution;
  } finally {
    for (const fd of handles) fs.closeSync(fd);
  }
}

function git(...args) {
  return execFileSync('rtk', ['proxy', 'git', ...args], { cwd: PROJECT, encoding: 'utf8' });
}

function inputSnapshot(files = FILES) {
  const tracked = git('ls-files', '-z')
    .split('\0')
    .filter(
      (p) =>
        /^(src|scripts|bin|docs|skills)\//.test(p) ||
        /^(package(?:-lock)?\.json|tsconfig.*\.json|vitest\.config\.ts)$/.test(p),
    );
  const entry = (p) => ({ path: path.resolve(PROJECT, p), sha256: sha256(fs.readFileSync(path.resolve(PROJECT, p))) });
  const inputs = [...new Set([...tracked, ...files])].sort();
  const dependencies = [
    ...new Set([
      path.join(PROJECT, 'package-lock.json'),
      ...Object.keys(require.cache).filter((p) => p.includes('/node_modules/')),
    ]),
  ].sort();
  const rtk = execFileSync('rtk', ['proxy', 'which', 'rtk'], { cwd: PROJECT, encoding: 'utf8' }).trim();
  const manifest = {
    tracked: inputs.map(entry),
    harness: files.toSorted().map(entry),
    dependencies: dependencies.map(entry),
    tools: [process.execPath, rtk].sort().map(entry),
  };
  const head = git('rev-parse', 'HEAD').trim(),
    tree = git('rev-parse', 'HEAD^{tree}').trim();
  return { manifest, head, tree };
}

function patchBytes(files = FILES) {
  const patches = [git('diff', '--binary', 'HEAD', '--', ...files)];
  const tracked = new Set(git('ls-files', '-z', '--', ...files).split('\0'));
  for (const file of files) {
    if (tracked.has(file)) continue;
    try {
      patches.push(git('diff', '--no-index', '--binary', '--', '/dev/null', file));
    } catch (error) {
      if (error.status !== 1) throw error;
      patches.push(error.stdout);
    }
  }
  return patches.join('');
}

function preparationContract(snapshot) {
  return {
    run_id: RUN_ID,
    case_id: 'preparation-validator',
    requirement_ids: REQUIREMENTS,
    subject_kind: 'candidate',
    base: BASE,
    head: snapshot.head,
    tree: snapshot.tree,
    frozen: false,
    patch_sha256: sha256(patchBytes()),
    test_source_sha256s: FILES.map(
      (file) => snapshot.manifest.harness.find((e) => e.path === path.join(PROJECT, file)).sha256,
    ),
    input_manifest_sha256: sha256(JSON.stringify(snapshot.manifest, null, 2) + '\n'),
    harness_sha256: sha256(JSON.stringify(snapshot.manifest.harness)),
    dependency_sha256: sha256(JSON.stringify(snapshot.manifest.dependencies)),
    tool_sha256: sha256(JSON.stringify(snapshot.manifest.tools)),
    executions: [{ argv: COMMAND, cwd: PROJECT, required_test_ids: REQUIRED_TESTS }],
    assertions: SOURCES.map((source, i) => ({
      id: `import-${i}`,
      predicate: 'literal',
      observations: [`source-${i}`],
      expected: snapshot.manifest.harness.find((e) => e.path === source),
    })),
    required_observations: SOURCES.map((_, i) => `source-${i}`),
    required_fault_observations: [],
    required_source_paths: SOURCES,
  };
}

function importObservations(directory, execution) {
  const stdout = readArtifact(directory, execution.stdout);
  const marker = Buffer.from('TRUST_RECOVERY_OBSERVATION ');
  const observations = [];
  let offset = 0;
  while ((offset = stdout.indexOf(marker, offset)) >= 0) {
    const start = offset + marker.length,
      end = stdout.indexOf('\n', start);
    ensure(end >= 0, 'Incomplete source import receipt');
    const event = JSON.parse(stdout.subarray(start, end));
    ensure(event.execution_id === execution.id, 'Wrong source import execution identity');
    observations.push({
      id: `source-${observations.length}`,
      artifact: execution.stdout,
      byte_range: [start, end],
      sequence: event.sequence,
      process_id: event.process_id,
      observer_id: event.observer_id,
      execution_id: execution.id,
      value: event.value,
    });
    offset = end;
  }
  return observations;
}

export async function collect(group) {
  ensure(group === 'validator' || LATER_GROUPS.includes(group), 'Unknown collector group');
  if (group === 'legacy') return await collectLegacy();
  if (group === 'delivery') return await collectDurability();
  // Preparation is the only reviewed execution policy in this iteration. In
  // particular, no final group is allowed to silently run without isolation.
  ensure(
    group === 'validator',
    `Group ${group} requires a frozen candidate and its later reviewed scenario/isolation contract; no command launched`,
  );
  const snapshot = inputSnapshot();
  const directory = outputDirectory(
    path.join(ARTIFACT_ROOT, 'evidence', 'candidate', `validator-${Date.now()}-${randomUUID()}`),
  );
  const input_manifest = writeArtifact(directory, 'inputs.json', JSON.stringify(snapshot.manifest, null, 2) + '\n');
  const patch = writeArtifact(directory, 'inputs.patch', patchBytes());
  const test_sources = FILES.map((file) =>
    writeArtifact(directory, path.basename(file), fs.readFileSync(path.join(PROJECT, file))),
  );
  const id = `validator-${randomUUID()}`;
  const execution = await captureExecution({
    directory,
    id,
    argv: COMMAND,
    cwd: PROJECT,
    env: { CLAWO_TRUST_CAPTURE_ID: id },
    timeout_ms: 60000,
  });
  const contract = preparationContract(snapshot);
  const observations = importObservations(directory, execution);
  const bundle = {
    schema_version: 1,
    run_id: RUN_ID,
    case_id: contract.case_id,
    requirement_ids: REQUIREMENTS,
    subject_kind: 'candidate',
    base: BASE,
    head: snapshot.head,
    tree: snapshot.tree,
    // This records an uncommitted Preparation snapshot, never a final candidate.
    frozen: false,
    input_manifest,
    harness_sha256: contract.harness_sha256,
    dependency_sha256: contract.dependency_sha256,
    tool_sha256: contract.tool_sha256,
    patch,
    test_sources,
    source_imports: observations.map((o) => ({ ...o.value, observation_id: o.id })),
    executions: [execution],
    observations,
    assertions: contract.assertions,
  };
  writeArtifact(directory, 'bundle.json', JSON.stringify(bundle, null, 2) + '\n');
  ensure(
    JSON.stringify(inputSnapshot()) === JSON.stringify(snapshot),
    'Candidate inputs changed during collection; capture is not acceptance proof',
  );
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

const LEGACY_TEST = 'src/__tests__/autoloop-trust-recovery-legacy.test.ts';
const LEGACY_HELPER = 'src/__tests__/helpers/autoloop-trust-recovery.ts';
const LEGACY_FILES = [...FILES, 'src/__tests__/session-manager.test.ts', LEGACY_TEST, LEGACY_HELPER];
const LEGACY_BASELINES = { upstream: '3e09b032a2f95fa520648f959f4ac9cdc7393350', start: BASE };
const LEGACY_SOURCES = [
  'src/session-manager.ts',
  'src/autoloop/dispatcher.ts',
  'src/autoloop/secure-ledger.ts',
  ...Object.keys(LEGACY_BASELINES).map((kind) => `.worktrees/trust-recovery-r1/${kind}/src/session-manager.ts`),
].map((file) => path.join(PROJECT, file));

export function legacySnapshot() {
  const snapshot = inputSnapshot(LEGACY_FILES);
  const entry = (file) => ({ path: file, sha256: sha256(fs.readFileSync(file)) });
  for (const [kind, head] of Object.entries(LEGACY_BASELINES)) {
    const root = path.join(PROJECT, '.worktrees/trust-recovery-r1', kind);
    ensure(
      git('-C', root, 'rev-parse', 'HEAD').trim() === head && git('-C', root, 'diff', 'HEAD', '--', 'src') === '',
      'Baseline source checkout changed',
    );
  }
  const tracked = new Map(snapshot.manifest.tracked.map((row) => [row.path, row]));
  for (const source of LEGACY_SOURCES) tracked.set(source, entry(source));
  snapshot.manifest.tracked = [...tracked.values()].sort((a, b) => a.path.localeCompare(b.path));
  const dependencies = new Map(snapshot.manifest.dependencies.map((row) => [row.path, row]));
  for (const dependency of ['vitest', 'vite', 'tsx', 'esbuild']) {
    for (const file of [require.resolve(dependency), require.resolve(`${dependency}/package.json`)])
      dependencies.set(file, entry(file));
  }
  snapshot.manifest.dependencies = [...dependencies.values()].sort((a, b) => a.path.localeCompare(b.path));
  return snapshot;
}

function legacyCommand(directory) {
  return [
    'rtk',
    'proxy',
    'npm',
    'test',
    '--',
    LEGACY_TEST,
    '--config',
    path.join(directory, 'vitest.config.mjs'),
    '--configLoader',
    'native',
    '--reporter=json',
    `--outputFile=${path.join(directory, 'report.json')}`,
  ];
}

function legacyContract(snapshot, directory, exactInputs) {
  const hashList = (key) => sha256(JSON.stringify(snapshot.manifest[key]));
  return {
    run_id: RUN_ID,
    case_id: 'slice1-legacy',
    requirement_ids: ['LEGACY-LIFETIME', 'LEGACY-FENCING', 'LEGACY-PERSISTENCE'],
    subject_kind: 'candidate',
    base: BASE,
    head: snapshot.head,
    tree: snapshot.tree,
    frozen: false,
    patch_sha256: exactInputs?.patch_sha256 ?? sha256(patchBytes(LEGACY_FILES)),
    test_source_sha256s:
      exactInputs?.test_source_sha256s ?? LEGACY_FILES.map((file) => sha256(fs.readFileSync(path.join(PROJECT, file)))),
    input_manifest_sha256: sha256(JSON.stringify(snapshot.manifest, null, 2) + '\n'),
    harness_sha256: hashList('harness'),
    dependency_sha256: hashList('dependencies'),
    tool_sha256: hashList('tools'),
    executions: [
      {
        argv: legacyCommand(directory),
        cwd: PROJECT,
        required_test_ids: LEGACY_TITLES.map((title) => `${path.join(PROJECT, LEGACY_TEST)} > ${title}`),
      },
    ],
    assertions: [
      { id: 'complete-cases', predicate: 'row-count', observations: ['case-index'], expected: LEGACY_TITLES.length },
      ...LEGACY_SOURCES.map((source, i) => ({
        id: `import-${i}`,
        predicate: 'literal',
        observations: [`source-${i}`],
        expected: snapshot.manifest.tracked.find((row) => row.path === source),
      })),
    ],
    required_observations: ['case-index', 'evidence-files', ...LEGACY_SOURCES.map((_, i) => `source-${i}`)],
    required_fault_observations: [],
    required_source_paths: LEGACY_SOURCES,
  };
}

function artifactFiles(directory, relative) {
  const files = [];
  for (const entry of fs.readdirSync(containedPath(directory, relative), { withFileTypes: true })) {
    const file = `${relative}/${entry.name}`;
    if (entry.isDirectory()) files.push(...artifactFiles(directory, file));
    else {
      ensure(entry.isFile() && !entry.isSymbolicLink(), 'Unexpected evidence filesystem entry');
      files.push(file);
    }
  }
  return files.sort();
}

async function collectLegacy() {
  const snapshot = legacySnapshot();
  const directory = outputDirectory(
    path.join(ARTIFACT_ROOT, 'evidence', 'candidate', `legacy-${Date.now()}-${randomUUID()}`),
  );
  writeArtifact(
    directory,
    'vitest.config.mjs',
    `import original from ${JSON.stringify(path.join(PROJECT, 'vitest.config.ts'))};\nexport default {...original,root:${JSON.stringify(PROJECT)},cacheDir:${JSON.stringify(path.join(directory, 'cache'))},test:{...original.test,pool:'forks',maxWorkers:1,minWorkers:1}};\n`,
  );
  const input_manifest = writeArtifact(directory, 'inputs.json', JSON.stringify(snapshot.manifest, null, 2) + '\n');
  const patch = writeArtifact(directory, 'inputs.patch', patchBytes(LEGACY_FILES));
  const test_sources = LEGACY_FILES.map((file) =>
    writeArtifact(directory, path.basename(file), fs.readFileSync(path.join(PROJECT, file))),
  );
  const id = `legacy-${randomUUID()}`;
  const execution = await captureExecution({
    directory,
    id,
    argv: legacyCommand(directory),
    cwd: PROJECT,
    timeout_ms: 60000,
    env: {
      NODE_OPTIONS: `--import=${path.join(PROJECT, LEGACY_HELPER)}`,
      CLAWO_TRUST_SCRATCH: path.join(directory, 'scratch'),
      CLAWO_TRUST_CASE_ROOT: path.join(directory, 'cases'),
      NPM_CONFIG_CACHE: path.join(directory, 'npm-cache'),
      NPM_CONFIG_UPDATE_NOTIFIER: 'false',
    },
  });
  // Keep the original capture intact, including RED stdout, even if a report
  // is absent. Report interpretation is an additional immutable receipt.
  const report = { path: 'report.json', sha256: sha256(fs.readFileSync(containedPath(directory, 'report.json'))) };
  const inventory = inspectTestReport(readArtifact(directory, report), 'vitest-json');
  Object.assign(execution, {
    report,
    report_format: 'vitest-json',
    discovered_test_ids: inventory.discovered,
    executed_test_ids: inventory.executed,
    skipped_test_ids: inventory.skipped,
  });
  writeArtifact(directory, 'interpreted-execution.json', JSON.stringify(execution, null, 2) + '\n');
  const cases = fs
    .readdirSync(path.join(directory, 'cases'))
    .sort()
    .map((name) => `cases/${name}`);
  for (const kind of Object.keys(LEGACY_BASELINES)) {
    outputDirectory(path.join(directory, 'baseline', kind));
    const original = path.join(ARTIFACT_ROOT, 'evidence', kind, 'slice1-original-001');
    for (const name of ['report.json', 'execution.json', 'stdout.txt', 'stderr.txt', 'source-imports.jsonl'])
      writeArtifact(directory, `baseline/${kind}/${name}`, fs.readFileSync(containedPath(original, name)));
  }
  const files = [...artifactFiles(directory, 'cases'), ...artifactFiles(directory, 'baseline')].map((file) => ({
    path: file,
    sha256: sha256(fs.readFileSync(containedPath(directory, file))),
  }));
  const index = [
    { sequence: 0, process_id: process.pid, observer_id: 'legacy-collector', value: cases },
    { sequence: 1, process_id: process.pid, observer_id: 'legacy-collector', value: files },
  ];
  const indexRef = writeArtifact(directory, 'observed-files.json', JSON.stringify(index, null, 2) + '\n');
  const observations = index.map((event, i) => ({
    id: i ? 'evidence-files' : 'case-index',
    artifact: indexRef,
    pointer: `/${i}`,
    ...event,
    execution_id: id,
  }));
  for (const [i, source] of LEGACY_SOURCES.entries()) {
    let observation;
    for (const ref of files.filter((file) => file.path.endsWith('/observations.jsonl'))) {
      const bytes = readArtifact(directory, ref);
      let offset = 0;
      for (const line of bytes.toString().split('\n')) {
        const end = offset + Buffer.byteLength(line);
        if (line) {
          const event = JSON.parse(line);
          if (event.value?.path === source) {
            observation = { id: `source-${i}`, artifact: ref, byte_range: [offset, end], ...event, execution_id: id };
            break;
          }
        }
        offset = end + 1;
      }
      if (observation) break;
    }
    ensure(observation, `Missing actual source import: ${source}`);
    observations.push(observation);
  }
  // A worker can provide several source receipts. Keep its original order.
  observations.sort((a, b) => a.observer_id.localeCompare(b.observer_id) || a.sequence - b.sequence);
  const contract = legacyContract(snapshot, directory);
  const bundle = {
    schema_version: 1,
    run_id: RUN_ID,
    case_id: contract.case_id,
    requirement_ids: contract.requirement_ids,
    subject_kind: 'candidate',
    base: BASE,
    head: snapshot.head,
    tree: snapshot.tree,
    frozen: false,
    input_manifest,
    harness_sha256: contract.harness_sha256,
    dependency_sha256: contract.dependency_sha256,
    tool_sha256: contract.tool_sha256,
    patch,
    test_sources,
    source_imports: observations
      .filter((o) => o.id.startsWith('source-'))
      .map((o) => ({ ...o.value, observation_id: o.id })),
    executions: [execution],
    observations,
    assertions: contract.assertions,
  };
  writeArtifact(directory, 'bundle.json', JSON.stringify(bundle, null, 2) + '\n');
  ensure(JSON.stringify(legacySnapshot()) === JSON.stringify(snapshot), 'Legacy inputs changed during collection');
  verifyLegacy(directory);
  // Repeated collection on the same source must preserve each attempt's report.
  const pointer = path.join(ARTIFACT_ROOT, 'reports', `legacy-${snapshot.head}-${contract.patch_sha256}-${id}.json`);
  const bundle_sha256 = sha256(fs.readFileSync(path.join(directory, 'bundle.json')));
  fs.writeFileSync(pointer, JSON.stringify({ directory, bundle_sha256 }) + '\n', { flag: 'wx', mode: 0o600 });
  return { directory, bundle_sha256, execution };
}

export function verifyLegacy(directory) {
  if (!directory) return verifyLegacyReceipt();
  ensure(/^evidence\/candidate\/[^/]+$/.test(path.relative(ARTIFACT_ROOT, directory)), 'Wrong legacy attempt scope');
  const bundle = JSON.parse(fs.readFileSync(containedPath(directory, 'bundle.json')));
  const snapshot = authenticatedPrecommitSnapshot(legacySnapshot(), directory, bundle);
  const values = verifyBundle(bundle, directory, legacyContract(snapshot, directory));
  return verifyLegacyObservations(values, directory);
}

function verifyLegacyObservations(values, directory, candidateHead) {
  for (const ref of values.get('evidence-files')) readArtifact(directory, ref);
  const titles = values
    .get('case-index')
    .map((relative) => verifyLegacyCase(containedPath(directory, relative), candidateHead));
  ensure(
    JSON.stringify(titles.toSorted()) === JSON.stringify(LEGACY_TITLES.toSorted()),
    'Missing, duplicated or misattributed legacy case',
  );
  for (const [kind, head] of Object.entries(LEGACY_BASELINES)) {
    const report = JSON.parse(fs.readFileSync(containedPath(directory, `baseline/${kind}/report.json`)));
    const execution = JSON.parse(fs.readFileSync(containedPath(directory, `baseline/${kind}/execution.json`)));
    ensure(execution.head === head, 'Wrong original-test subject');
    const selected = report.testResults
      .flatMap((suite) => suite.assertionResults)
      .filter((test) => ['passed', 'failed'].includes(test.status));
    ensure(
      kind === 'upstream'
        ? selected.length === 0 && execution.exit_code === 0
        : selected.length === 1 &&
            selected[0].status === 'failed' &&
            selected[0].fullName.endsWith(
              'keeps a legacy generation-zero tombstone fenced until release evidence is durable',
            ) &&
            execution.exit_code !== 0,
      'Original baseline failure/absence not preserved',
    );
  }
  return { scope: 'slice1', cases: titles.length, final_candidate: false };
}

// Immutable anchor from the approved Slice 1 review. It is historical evidence,
// never rewritten as a claim that a newer commit produced these observations.
const ORIGINAL_COMMIT = '1559595d688fd91e2d9d9073c395f66d222afd83';
const FIRST_CORRECTION = 'a0a6fdaf188d107d9eacd760984685ada813da1e';
const FIRST_CORRECTION_BUNDLE = {
  path: 'evidence/candidate/legacy-1789359636920-7c9e02d9-4e71-4c01-a83b-087501a6396d/bundle.json',
  sha256: '8f28667ee4ee9db372d767ea08178bbd0823ac87763fff849381f907cf240c99',
};
const ORIGINAL_BUNDLE = {
  path: 'evidence/candidate/legacy-1789355466078-105035dd-ce89-4fb4-a876-56d398fb81d4/bundle.json',
  sha256: 'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d',
};
const equal = (actual, expected, label) => ensure(isDeepStrictEqual(actual, expected), label);
const gitBytes = (...args) =>
  execFileSync('rtk', ['proxy', 'git', ...args], { cwd: PROJECT, maxBuffer: 64 * 1024 * 1024 });
const objectTrees = new Map();
const objectBytes = new Map();
function commitTree(commit) {
  ensure(/^[a-f0-9]{40}$/.test(commit), 'Invalid committed head');
  if (!objectTrees.has(commit)) {
    const entries = git('ls-tree', '-r', '-z', commit)
      .split('\0')
      .filter(Boolean)
      .map((row) => {
        const tab = row.indexOf('\t');
        const [mode, type, oid] = row.slice(0, tab).split(' ');
        return [row.slice(tab + 1), { mode, type, oid }];
      });
    objectTrees.set(commit, new Map(entries));
  }
  return objectTrees.get(commit);
}

function commitFile(commit, file, seen = new Set()) {
  ensure(!seen.has(file), 'Cyclic Git source symlink');
  const entry = commitTree(commit).get(file);
  ensure(entry?.type === 'blob', `Missing committed file: ${file}`);
  if (!objectBytes.has(entry.oid)) {
    // Reading a matching worktree blob saves subprocesses; its Git object
    // digest must match the immutable tree entry before its bytes are used.
    let bytes;
    if (entry.mode !== '120000') {
      try {
        const current = fs.readFileSync(path.join(PROJECT, file));
        const oid = createHash('sha1').update(`blob ${current.length}\0`).update(current).digest('hex');
        if (oid === entry.oid) bytes = current;
      } catch {
        /* A historical file may no longer exist in the worktree. */
      }
    }
    objectBytes.set(entry.oid, bytes ?? gitBytes('cat-file', 'blob', entry.oid));
  }
  const bytes = objectBytes.get(entry.oid);
  if (entry.mode !== '120000') return bytes;
  const target = path.posix.normalize(path.posix.join(path.posix.dirname(file), bytes.toString()));
  ensure(!path.isAbsolute(bytes.toString()) && !target.startsWith('../'), 'External Git source symlink');
  return commitFile(commit, target, new Set([...seen, file]));
}

function historicalTools(manifest) {
  ensure(Array.isArray(manifest.tools) && manifest.tools.length === 2, 'Wrong historical tool inventory');
  equal(manifest.tools.map((tool) => path.basename(tool.path)).sort(), ['node', 'rtk'], 'Wrong historical tool names');
  return manifest.tools.map((tool) => {
    ensure(path.isAbsolute(tool.path) && fs.statSync(tool.path).isFile(), 'Missing historical tool file');
    // The execution path belongs to the historical process. Recompute its
    // executable bytes independently; the verifier's PATH is not that process.
    const actual = sha256(fs.readFileSync(tool.path));
    equal(actual, tool.sha256, `Historical tool bytes differ: ${tool.path}`);
    return { path: tool.path, sha256: actual };
  });
}

// Verification executes in a different process from collection. Preserve the
// historical executable paths, but authenticate their bytes before using them
// in the expected contract. All other expected inputs remain source-derived.
export function authenticatedPrecommitSnapshot(snapshot, directory, bundle) {
  const manifest = readInputManifest(directory, bundle.input_manifest);
  const tools = historicalTools(manifest);
  equal(bundle.tool_sha256, sha256(JSON.stringify(tools)), 'Wrong tool_sha256');
  return { ...snapshot, manifest: { ...snapshot.manifest, tools } };
}

function committedSnapshot(commit, parent, historicalManifest, inventoryCommit = commit) {
  const current = legacySnapshot();
  const names = [...commitTree(inventoryCommit).keys()].filter(
    (file) =>
      /^(src|scripts|bin|docs|skills)\//.test(file) ||
      /^(package(?:-lock)?\.json|tsconfig.*\.json|vitest\.config\.ts)$/.test(file),
  );
  const entry = (file) => ({ path: path.join(PROJECT, file), sha256: sha256(commitFile(commit, file)) });
  const tracked = new Map(
    [...new Set([...names, ...LEGACY_FILES])].map((file) => [path.join(PROJECT, file), entry(file)]),
  );
  for (const [kind, head] of Object.entries(LEGACY_BASELINES)) {
    const file = path.join(PROJECT, '.worktrees/trust-recovery-r1', kind, 'src/session-manager.ts');
    tracked.set(file, { path: file, sha256: sha256(commitFile(head, 'src/session-manager.ts')) });
  }
  return {
    head: parent,
    tree: git('rev-parse', `${parent}^{tree}`).trim(),
    manifest: {
      tracked: [...tracked.values()].sort((a, b) => a.path.localeCompare(b.path)),
      harness: LEGACY_FILES.toSorted().map(entry),
      dependencies: current.manifest.dependencies,
      tools: historicalTools(historicalManifest),
    },
  };
}

function changedCommitFiles(parent, commit, fence = LEGACY_FILES) {
  const names = git('diff', '--no-renames', '--name-only', parent, commit).trim().split('\n').filter(Boolean);
  ensure(
    names.length && names.every((file) => fence.includes(file)),
    'Commit changed files outside the legacy source fence',
  );
  return names
    .map((file) => {
      const bytes = commitFile(commit, file),
        entry = commitTree(commit).get(file);
      ensure(entry.mode === '100644', 'Unexpected changed file mode');
      return { path: file, bytes: bytes.length, mode: entry.mode, sha256: sha256(bytes), git_blob_sha1: entry.oid };
    })
    .sort((a, b) => a.path.localeCompare(b.path));
}

function patchSections(bytes) {
  const text = bytes.toString();
  ensure(text.startsWith('diff --git '), 'Missing complete commit patch');
  const sections = text.split(/(?=^diff --git )/m);
  ensure(
    new Set(sections.map((section) => section.split('\n')[0])).size === sections.length,
    'Duplicate patch section',
  );
  return sections.sort();
}

/** Read-only proof of one precommit bundle -> one existing immutable commit. */
export function verifyLegacyCommitLink(link, expectedCommit) {
  equal(link.commit, expectedCommit, 'Wrong committed head');
  equal(git('rev-parse', `${expectedCommit}^{commit}`).trim(), expectedCommit, 'Wrong Git head object');
  const parents = git('show', '-s', '--format=%P', expectedCommit).trim().split(' ');
  ensure(parents.length === 1, 'Expected one exact commit parent');
  equal(link.parent, parents[0], 'Wrong commit parent');
  equal(link.base, BASE, 'Wrong evidence base');
  equal(link.tree, git('rev-parse', `${expectedCommit}^{tree}`).trim(), 'Wrong committed tree');
  const bundle = JSON.parse(readArtifact(ARTIFACT_ROOT, link.bundle));
  equal(bundle.head, link.parent, 'Wrong precommit bundle head');
  equal(bundle.base, BASE, 'Wrong precommit bundle base');
  equal(bundle.tree, git('rev-parse', `${link.parent}^{tree}`).trim(), 'Wrong precommit bundle tree');
  const directory = path.dirname(containedPath(ARTIFACT_ROOT, link.bundle.path));
  const patch = readArtifact(ARTIFACT_ROOT, link.patch);
  equal(patch, readArtifact(directory, bundle.patch), 'Receipt patch differs from preserved bundle patch');
  const actualPatch = gitBytes(
    '-c',
    'core.abbrev=7',
    'diff',
    '--binary',
    '--no-ext-diff',
    '--no-textconv',
    '--no-renames',
    '--no-color',
    '--src-prefix=a/',
    '--dst-prefix=b/',
    link.parent,
    expectedCommit,
  );
  // The original collector appended new-file sections after tracked sections.
  // Preserve its bytes/hash, while comparing every complete section to Git.
  equal(patchSections(patch), patchSections(actualPatch), 'Complete patch differs from actual Git objects');
  equal(link.files, changedCommitFiles(link.parent, expectedCommit), 'Changed file bytes or Git objects differ');
  const manifest = JSON.parse(readArtifact(directory, bundle.input_manifest));
  const snapshot = committedSnapshot(expectedCommit, link.parent, manifest);
  const contract = legacyContract(snapshot, directory, {
    patch_sha256: sha256(patch),
    test_source_sha256s: LEGACY_FILES.map((file) => sha256(commitFile(expectedCommit, file))),
  });
  const values = verifyBundle(bundle, directory, contract);
  return verifyLegacyObservations(values, directory, expectedCommit);
}

function makeCommitLink(commit, bundleRef) {
  const bundle = JSON.parse(readArtifact(ARTIFACT_ROOT, bundleRef));
  const parent = git('rev-parse', `${commit}^`).trim();
  return {
    base: BASE,
    parent,
    commit,
    tree: git('rev-parse', `${commit}^{tree}`).trim(),
    bundle: { ...bundleRef },
    patch: {
      path: path.posix.join(path.posix.dirname(bundleRef.path), bundle.patch.path),
      sha256: bundle.patch.sha256,
    },
    files: changedCommitFiles(parent, commit),
  };
}

// The accepted Slice 1 receipt is an immutable predecessor, not a template
// to rename for later source candidates. Subsequent links are actual Git objects.
const SLICE1_HEAD = '21ded46e6d94be1ce11d0eb8336134c4a2dea29d';
const SLICE2_HEAD = 'e3e235b280a570e1bafef57be13e5fad8f6d7ce2';
const SLICE1_RECEIPT = {
  path: 'evidence/candidate/legacy-postcommit-21ded46e6d94be1ce11d0eb8336134c4a2dea29d-3d8841cd-bb98-40f0-8e15-3c7d2ee194aa/receipt.json',
  sha256: '1c53338fc95b6811859cbebe726e0ae2e06a928534bb2ecdd4bbee676b8cbd79',
};
const SLICE2_BUNDLE = {
  path: 'evidence/candidate/legacy-1789368805539-5f6a9d04-5b17-4327-b1ce-1ff61eb7c012/bundle.json',
  sha256: '7ccdcc14a115e225a3c0e43a37a79eb0c6dcecbb0b81dbea05156b966efa7f60',
};
const SLICE2_FILES = [
  ...FILES,
  'src/session-manager.ts',
  'src/__tests__/session-manager.test.ts',
  'src/__tests__/autoloop-durable-recovery-e2e.test.ts',
  'src/__tests__/autoloop-trust-recovery-delivery.test.ts',
];
const commitPatch = (parent, head, files = []) =>
  gitBytes(
    '-c',
    'core.abbrev=7',
    'diff',
    '--binary',
    '--no-ext-diff',
    '--no-textconv',
    '--no-renames',
    '--no-color',
    '--src-prefix=a/',
    '--dst-prefix=b/',
    parent,
    head,
    '--',
    ...files,
  );
function extensionHeads(head) {
  const heads = git('rev-list', '--reverse', '--ancestry-path', `${SLICE1_HEAD}..${head}`).trim().split('\n');
  ensure(heads[0] === SLICE2_HEAD && heads.at(-1) === head, 'Wrong receipt ancestry');
  let parent = SLICE1_HEAD;
  for (const commit of heads) {
    equal(git('show', '-s', '--format=%P', commit).trim(), parent, 'Wrong chain parent');
    parent = commit;
  }
  return heads;
}
function extensionLink(commit) {
  const parent = git('rev-parse', `${commit}^`).trim();
  const patch = commitPatch(parent, commit);
  return {
    parent,
    commit,
    tree: git('rev-parse', `${commit}^{tree}`).trim(),
    patch_base64: patch.toString('base64'),
    patch_sha256: sha256(patch),
    files: changedCommitFiles(
      parent,
      commit,
      commit === SLICE2_HEAD ? SLICE2_FILES : [...FILES, 'src/__tests__/session-manager.test.ts'],
    ),
  };
}
function verifyExtendedReceipt(receipt, head) {
  equal(receipt.schema_version, 2, 'Wrong receipt schema');
  equal(receipt.run_id, RUN_ID, 'Wrong receipt run');
  equal(receipt.kind, 'legacy-postcommit', 'Wrong receipt kind');
  equal(receipt.commit, head, 'Wrong receipt head');
  equal(receipt.tree, git('rev-parse', `${head}^{tree}`).trim(), 'Wrong receipt tree');
  equal(receipt.parent, git('show', '-s', '--format=%P', head).trim(), 'Wrong receipt parent');
  equal(receipt.base, SLICE1_HEAD, 'Wrong receipt base');
  equal(receipt.previous, SLICE1_RECEIPT, 'Wrong previous receipt hash/path');
  verifyReceiptData(JSON.parse(readArtifact(ARTIFACT_ROOT, receipt.previous)), SLICE1_HEAD);
  const heads = extensionHeads(head);
  ensure(Array.isArray(receipt.chain) && receipt.chain.length === heads.length, 'Incomplete receipt chain');
  receipt.chain.forEach((link, i) => {
    const expected = extensionLink(heads[i]);
    for (const key of ['parent', 'commit', 'tree', 'patch_base64', 'patch_sha256', 'files'])
      equal(link[key], expected[key], `Wrong chain ${key}`);
  });
  // Retained observations were executed on the Slice 2 bytes. Every later
  // allowed link changes only the verifier harness; runtime/test evidence is
  // not silently rebound to different production or scenario implementations.
  equal(receipt.candidate_bundle, SLICE2_BUNDLE, 'Wrong candidate bundle hash/path');
  const bundle = JSON.parse(readArtifact(ARTIFACT_ROOT, receipt.candidate_bundle));
  const directory = path.dirname(containedPath(ARTIFACT_ROOT, receipt.candidate_bundle.path));
  equal(bundle.head, SLICE1_HEAD, 'Wrong precommit head');
  equal(bundle.tree, git('rev-parse', `${SLICE1_HEAD}^{tree}`).trim(), 'Wrong precommit tree');
  const patch = readArtifact(directory, bundle.patch);
  equal(
    patchSections(patch),
    patchSections(commitPatch(SLICE1_HEAD, SLICE2_HEAD, LEGACY_FILES)),
    'Wrong retained legacy patch',
  );
  const manifest = JSON.parse(readArtifact(directory, bundle.input_manifest));
  const snapshot = committedSnapshot(SLICE2_HEAD, SLICE1_HEAD, manifest, SLICE1_HEAD);
  for (const key of ['tracked', 'harness', 'dependencies', 'tools']) {
    const actual = new Map(manifest[key].map((row) => [row.path, row.sha256]));
    const expected = new Map(snapshot.manifest[key].map((row) => [row.path, row.sha256]));
    const differing = [...new Set([...actual.keys(), ...expected.keys()])].filter(
      (file) => actual.get(file) !== expected.get(file),
    );
    ensure(!differing.length, `Historical ${key} mismatch: ${differing.join(', ')}`);
    equal(manifest[key], snapshot.manifest[key], `Historical ${key} inventory order mismatch`);
  }
  const contract = legacyContract(snapshot, directory, {
    patch_sha256: sha256(patch),
    test_source_sha256s: LEGACY_FILES.map((file) => sha256(commitFile(SLICE2_HEAD, file))),
  });
  const result = verifyLegacyObservations(verifyBundle(bundle, directory, contract), directory, SLICE2_HEAD);
  return { ...result, committed_head: head, committed_tree: receipt.tree, postcommit_verified: true };
}

function receiptParent(head) {
  ensure(head !== ORIGINAL_COMMIT, 'Corrective controller commit does not exist yet');
  if (![FIRST_CORRECTION, SLICE1_HEAD].includes(head)) {
    extensionHeads(head);
    return git('rev-parse', `${head}^`).trim();
  }
  const parent = head === FIRST_CORRECTION ? ORIGINAL_COMMIT : FIRST_CORRECTION;
  equal(git('show', '-s', '--format=%P', head).trim(), parent, 'Wrong corrective commit parent');
  return parent;
}

function frozenHead() {
  ensure(
    git('status', '--porcelain=v1', '--untracked-files=all') === '',
    'Postcommit verification requires clean, frozen source',
  );
  const head = git('rev-parse', 'HEAD').trim();
  receiptParent(head);
  // Check actual current bytes as well as Git status (which can hide files
  // marked assume-unchanged). A receipt cannot certify an uncommitted module.
  for (const file of [...commitTree(head).keys()].filter((file) => /^(src|scripts)\//.test(file)))
    equal(
      fs.readFileSync(path.join(PROJECT, file)),
      commitFile(head, file),
      'Loaded source bytes differ from committed HEAD',
    );
  return head;
}

export function verifyReceiptData(receipt, head) {
  if (receipt.schema_version === 2) return verifyExtendedReceipt(receipt, head);
  equal(receipt.schema_version, 1, 'Wrong receipt schema');
  equal(receipt.run_id, RUN_ID, 'Wrong receipt run');
  equal(receipt.kind, 'legacy-postcommit', 'Wrong receipt kind');
  equal(receipt.commit, head, 'Wrong receipt head');
  equal(receipt.parent, receiptParent(head), 'Wrong receipt parent');
  equal(receipt.base, BASE, 'Wrong receipt base');
  equal(receipt.tree, git('rev-parse', `${head}^{tree}`).trim(), 'Wrong receipt tree');
  equal(receipt.original.bundle, ORIGINAL_BUNDLE, 'Wrong original bundle hash or path');
  verifyLegacyCommitLink(receipt.original, ORIGINAL_COMMIT);
  const extended = head !== FIRST_CORRECTION;
  ensure(Array.isArray(receipt.history) && receipt.history.length === (extended ? 1 : 0), 'Wrong receipt history');
  if (extended) {
    equal(receipt.history[0].bundle, FIRST_CORRECTION_BUNDLE, 'Wrong first correction bundle hash or path');
    equal(receipt.history[0].parent, ORIGINAL_COMMIT, 'Wrong first correction parent');
    verifyLegacyCommitLink(receipt.history[0], FIRST_CORRECTION);
  }
  const result = verifyLegacyCommitLink(receipt.candidate, head);
  return { ...result, committed_head: head, committed_tree: receipt.tree, postcommit_verified: true };
}

export function verifyLegacyReceipt() {
  const head = frozenHead();
  const pointer = JSON.parse(fs.readFileSync(containedPath(ARTIFACT_ROOT, `reports/legacy-committed-${head}.json`)));
  const receipt = JSON.parse(readArtifact(ARTIFACT_ROOT, pointer.receipt));
  return { ...verifyReceiptData(receipt, head), receipt: pointer.receipt };
}

/** Controller-only ordering: execute this committed CLI after its atomic commit. */
export function finalizeLegacy(directory, bundleSha256, originalSha256) {
  const head = frozenHead();
  ensure(
    /^evidence\/candidate\/[^/]+$/.test(path.relative(ARTIFACT_ROOT, path.resolve(directory))),
    'Wrong candidate bundle scope',
  );
  const { receipt } = auditLegacyFinalization(head, directory, bundleSha256, originalSha256);
  const pointerPath = path.join(ARTIFACT_ROOT, 'reports', `legacy-committed-${head}.json`);
  if (fs.existsSync(pointerPath)) {
    const existing = verifyLegacyReceipt();
    const previous = JSON.parse(readArtifact(ARTIFACT_ROOT, existing.receipt));
    equal(previous, receipt, 'Existing immutable receipt binds another bundle');
    return existing;
  }
  const attempt = outputDirectory(
    path.join(ARTIFACT_ROOT, 'evidence', 'candidate', `legacy-postcommit-${head}-${randomUUID()}`),
  );
  const reference = writeArtifact(attempt, 'receipt.json', JSON.stringify(receipt, null, 2) + '\n');
  reference.path = path.relative(ARTIFACT_ROOT, path.join(attempt, reference.path));
  containedPath(ARTIFACT_ROOT, 'reports');
  writeArtifact(path.dirname(pointerPath), path.basename(pointerPath), JSON.stringify({ receipt: reference }) + '\n');
  return verifyLegacyReceipt();
}

/** Read-only audit of the same complete pipeline, against an existing commit.
 * This never publishes a receipt or claims that the current worktree is frozen.
 */
export function auditLegacyFinalization(head, directory, bundleSha256, originalSha256) {
  equal(originalSha256, ORIGINAL_BUNDLE.sha256, 'Wrong original bundle hash');
  const relative = path.relative(ARTIFACT_ROOT, path.resolve(directory));
  ensure(/^evidence\/(candidate|sensitivity)\/[^/]+$/.test(relative), 'Wrong audit bundle scope');
  const bundleRef = { path: `${relative}/bundle.json`, sha256: bundleSha256 };
  if (![ORIGINAL_COMMIT, FIRST_CORRECTION, SLICE1_HEAD].includes(head)) {
    const receipt = {
      schema_version: 2,
      run_id: RUN_ID,
      kind: 'legacy-postcommit',
      base: SLICE1_HEAD,
      parent: receiptParent(head),
      commit: head,
      tree: git('rev-parse', `${head}^{tree}`).trim(),
      previous: { ...SLICE1_RECEIPT },
      candidate_bundle: bundleRef,
      chain: extensionHeads(head).map(extensionLink),
    };
    return { receipt, result: verifyReceiptData(receipt, head) };
  }
  const receipt = {
    schema_version: 1,
    run_id: RUN_ID,
    kind: 'legacy-postcommit',
    base: BASE,
    parent: receiptParent(head),
    commit: head,
    tree: git('rev-parse', `${head}^{tree}`).trim(),
    original: makeCommitLink(ORIGINAL_COMMIT, ORIGINAL_BUNDLE),
    history: head === FIRST_CORRECTION ? [] : [makeCommitLink(FIRST_CORRECTION, FIRST_CORRECTION_BUNDLE)],
    candidate: makeCommitLink(head, bundleRef),
  };
  return { receipt, result: verifyReceiptData(receipt, head) };
}

const DELIVERY_MATRIX = [
  'autoloop-dispatcher',
  'autoloop-durable-recovery-e2e',
  'autoloop-outbox',
  'autoloop-recovery',
  'autoloop-runner',
  'autoloop-secure-ledger',
  'autoloop-trust-recovery-delivery',
  'autoloop-trust-recovery-legacy',
  'session-manager-pidfile',
  'session-manager',
].map((name) => `src/__tests__/${name}.test.ts`);
const DELIVERY_FILES = [...FILES, ...DELIVERY_MATRIX, LEGACY_HELPER];
function durabilitySnapshot() {
  const snapshot = legacySnapshot();
  snapshot.manifest.harness = DELIVERY_FILES.toSorted().map((file) => ({
    path: path.join(PROJECT, file),
    sha256: sha256(fs.readFileSync(path.join(PROJECT, file))),
  }));
  return snapshot;
}
function durabilityCommand(directory) {
  return [
    'rtk',
    'proxy',
    'npm',
    'test',
    '--',
    ...DELIVERY_MATRIX,
    '--config',
    path.join(directory, 'vitest.config.mjs'),
    '--configLoader',
    'native',
    '--reporter=verbose',
    '--reporter=json',
    `--outputFile=${path.join(directory, 'report.json')}`,
  ];
}
const deliveryConfig = (directory) =>
  `import original from ${JSON.stringify(path.join(PROJECT, 'vitest.config.ts'))};\nexport default {...original,root:${JSON.stringify(PROJECT)},cacheDir:${JSON.stringify(path.join(directory, 'cache'))},test:{...original.test,pool:'forks',maxWorkers:4,minWorkers:1}};\n`;
// Preserve real Unix sockets under the long authorized artifact root. An open
// directory fd shortens only the bind address, not its filesystem destination.
const SOCKET_PRELOAD = `import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';
const original=net.Server.prototype.listen;
net.Server.prototype.listen=function(...args){
 const socket=args[0],root=${JSON.stringify(ARTIFACT_ROOT)};
 if(typeof socket!=='string'||!socket.startsWith(root+'/')||Buffer.byteLength(socket)<104)return Reflect.apply(original,this,args);
 const fd=fs.openSync(path.dirname(socket),fs.constants.O_RDONLY|fs.constants.O_DIRECTORY);
 let closed=false;const close=()=>{if(!closed){closed=true;fs.closeSync(fd);}};
 const address='/proc/self/fd/'+fd+'/'+path.basename(socket);
 if(Buffer.byteLength(address)>=104){close();throw new Error('Isolated socket address still too long');}
 this.once('close',close);this.once('error',close);args[0]=address;
 try{return Reflect.apply(original,this,args);}catch(e){close();throw e;}
};\n`;
function durabilityContract(snapshot, directory, exactInputs) {
  const hashList = (key) => sha256(JSON.stringify(snapshot.manifest[key]));
  return {
    run_id: RUN_ID,
    case_id: 'slice2-durability',
    requirement_ids: ['DURABLE-DELIVERY', 'COLD-RECOVERY', 'GENERATION-RESUME'],
    subject_kind: 'candidate',
    base: SLICE2_HEAD,
    head: snapshot.head,
    tree: snapshot.tree,
    frozen: false,
    patch_sha256: exactInputs?.patch_sha256 ?? sha256(patchBytes(DELIVERY_FILES)),
    test_source_sha256s:
      exactInputs?.test_source_sha256s ??
      DELIVERY_FILES.map((file) => sha256(fs.readFileSync(path.join(PROJECT, file)))),
    input_manifest_sha256: sha256(JSON.stringify(snapshot.manifest, null, 2) + '\n'),
    harness_sha256: hashList('harness'),
    dependency_sha256: hashList('dependencies'),
    tool_sha256: hashList('tools'),
    executions: [{ argv: durabilityCommand(directory), cwd: PROJECT, required_test_ids: [] }],
    assertions: [
      {
        id: 'complete-cases',
        predicate: 'row-count',
        observations: ['case-index'],
        expected: DURABILITY_CASES.length + RESUME_CASES.length,
      },
    ],
    required_observations: ['case-index', 'evidence-files'],
    required_fault_observations: [],
    required_source_paths: [],
  };
}
export function durabilityArtifactReader(directory, files) {
  const refs = new Map(files.map((ref) => [ref.path, ref]));
  equal(refs.size, files.length, 'Duplicate durability artifacts');
  return (name) => {
    if (!refs.has(name)) {
      // The negative scenarios deliberately produce no recipient file. Check
      // its actual absence; an existing file without a retained hash is an error.
      containedPath(directory, name);
      throw new Error(`Missing durability artifact reference: ${name}`);
    }
    return readArtifact(directory, refs.get(name));
  };
}

function durabilityObservations(bundle, directory, values, head) {
  const files = values.get('evidence-files'),
    cases = values.get('case-index');
  ensure(Array.isArray(files) && Array.isArray(cases), 'Missing durability artifact index');
  const read = durabilityArtifactReader(directory, files);
  for (const file of files) read(file.path);
  equal(
    cases.map((row) => row.case_id).sort(),
    [...DURABILITY_CASES, ...RESUME_CASES].sort(),
    'Wrong durability scenario inventory',
  );
  ensure(new Set(cases.map((row) => row.path)).size === cases.length, 'Replayed durability case directory');
  const results = cases.map((row) => {
    ensure(/^cases\/[^/]+$/.test(row.path), 'Wrong durability case path');
    const readCase = (name) => read(`${row.path}/${name}`);
    if (DURABILITY_CASES.includes(row.case_id))
      return verifyDurabilityCase(path.join(directory, row.path), row.case_id, head, readCase);
    return verifyResumeCase(path.join(directory, row.path), row.case_id, head, readCase);
  });
  // Full matrix inventory is independently fixed by source filenames. A green
  // aggregate cannot conceal a missing suite or an unhandled failed assertion.
  const execution = bundle.executions[0],
    report = JSON.parse(readArtifact(directory, execution.report));
  ensure(execution.executed_test_ids.length >= 1376, 'Missing previously passing focused tests');
  equal(
    report.testResults.map((row) => row.name).sort(),
    DELIVERY_MATRIX.map((file) => path.join(PROJECT, file)).sort(),
    'Incomplete focused matrix',
  );
  equal(
    fs.readFileSync(containedPath(directory, 'vitest.config.mjs')).toString(),
    deliveryConfig(directory),
    'Altered test configuration',
  );
  equal(
    fs.readFileSync(containedPath(directory, 'socket-preload.mjs')).toString(),
    SOCKET_PRELOAD,
    'Altered isolation preload',
  );
  return { scope: 'slice2', cases: results.length, tests: execution.executed_test_ids.length, final_candidate: false };
}
export function verifyDurability(directory) {
  if (!directory) return verifyDurabilityReceipt();
  directory = path.resolve(directory);
  const bundle = JSON.parse(fs.readFileSync(containedPath(directory, 'bundle.json')));
  const contract = durabilityContract(
    authenticatedPrecommitSnapshot(durabilitySnapshot(), directory, bundle),
    directory,
  );
  return durabilityObservations(bundle, directory, verifyBundle(bundle, directory, contract));
}
async function collectDurability() {
  const snapshot = durabilitySnapshot();
  const directory = outputDirectory(
    path.join(ARTIFACT_ROOT, 'evidence', 'candidate', `durability-${Date.now()}-${randomUUID()}`),
  );
  outputDirectory(path.join(directory, 'cases'));
  writeArtifact(directory, 'vitest.config.mjs', deliveryConfig(directory));
  writeArtifact(directory, 'socket-preload.mjs', SOCKET_PRELOAD);
  const input_manifest = writeArtifact(directory, 'inputs.json', JSON.stringify(snapshot.manifest, null, 2) + '\n');
  const patch = writeArtifact(directory, 'inputs.patch', patchBytes(DELIVERY_FILES));
  const test_sources = DELIVERY_FILES.map((file) =>
    writeArtifact(directory, path.basename(file), fs.readFileSync(path.join(PROJECT, file))),
  );
  const execution = await captureExecution({
    directory,
    id: `durability-${randomUUID()}`,
    argv: durabilityCommand(directory),
    cwd: PROJECT,
    timeout_ms: 600000,
    env: {
      NODE_OPTIONS: `--import=${path.join(PROJECT, LEGACY_HELPER)} --import=${path.join(directory, 'socket-preload.mjs')}`,
      CLAWO_TRUST_SCRATCH: path.join(directory, 'scratch'),
      CLAWO_TRUST_CASE_ROOT: path.join(directory, 'cases'),
      NPM_CONFIG_CACHE: path.join(directory, 'npm-cache'),
      NPM_CONFIG_UPDATE_NOTIFIER: 'false',
    },
  });
  const report = { path: 'report.json', sha256: sha256(fs.readFileSync(containedPath(directory, 'report.json'))) };
  const inventory = inspectTestReport(readArtifact(directory, report), 'vitest-json');
  Object.assign(execution, {
    report,
    report_format: 'vitest-json',
    discovered_test_ids: inventory.discovered,
    executed_test_ids: inventory.executed,
    skipped_test_ids: inventory.skipped,
  });
  writeArtifact(directory, 'interpreted-execution.json', JSON.stringify(execution, null, 2) + '\n');
  const entries = fs.readdirSync(path.join(directory, 'cases'));
  const cases = [...DURABILITY_CASES, ...RESUME_CASES].map((case_id) => {
    const found = entries.filter((name) => name.startsWith(case_id + '-'));
    equal(found.length, 1, `Missing/duplicate observed scenario: ${case_id}`);
    return { case_id, path: `cases/${found[0]}` };
  });
  // Capture only actual relevant process/API files, not the disposable review
  // checkout's .git objects or symlinks. The semantic reader requires every
  // consumed file to occur in this independently hashed inventory.
  const evidenceNames = [];
  function walk(relative) {
    for (const entry of fs.readdirSync(containedPath(directory, relative), { withFileTypes: true })) {
      const file = `${relative}/${entry.name}`;
      if (entry.isSymbolicLink()) continue;
      if (entry.isDirectory()) {
        if (['.git', 'node_modules', 'home'].includes(entry.name)) continue;
        if (
          relative.includes('/workspace') &&
          !['workspace', 'tasks', 'trust-delivery', 'source-checkpoint', 'iter', '0', '1'].includes(entry.name)
        )
          continue;
        walk(file);
      } else if (entry.isFile() && /\.(jsonl|json|txt|mjs|patch)$/.test(entry.name)) evidenceNames.push(file);
    }
  }
  cases.forEach((row) => walk(row.path));
  const files = evidenceNames
    .sort()
    .map((file) => ({ path: file, sha256: sha256(fs.readFileSync(containedPath(directory, file))) }));
  const index = [cases, files].map((value, sequence) => ({
    sequence,
    process_id: process.pid,
    observer_id: 'durability-collector',
    value,
  }));
  const ref = writeArtifact(directory, 'observed-files.json', JSON.stringify(index, null, 2) + '\n');
  const observations = index.map((event, i) => ({
    id: i ? 'evidence-files' : 'case-index',
    artifact: ref,
    pointer: `/${i}`,
    ...event,
    execution_id: execution.id,
  }));
  const contract = durabilityContract(snapshot, directory);
  const bundle = {
    schema_version: 1,
    run_id: RUN_ID,
    case_id: contract.case_id,
    requirement_ids: contract.requirement_ids,
    subject_kind: 'candidate',
    base: SLICE2_HEAD,
    head: snapshot.head,
    tree: snapshot.tree,
    frozen: false,
    input_manifest,
    harness_sha256: contract.harness_sha256,
    dependency_sha256: contract.dependency_sha256,
    tool_sha256: contract.tool_sha256,
    patch,
    test_sources,
    source_imports: [],
    executions: [execution],
    observations,
    assertions: contract.assertions,
  };
  writeArtifact(directory, 'bundle.json', JSON.stringify(bundle, null, 2) + '\n');
  equal(durabilitySnapshot(), snapshot, 'Source changed during durability collection');
  verifyDurability(directory);
  return { directory, bundle_sha256: sha256(fs.readFileSync(path.join(directory, 'bundle.json'))), execution };
}

export function auditDurabilityFinalization(head, directory, bundleSha256) {
  const parent = git('rev-parse', `${head}^`).trim();
  ensure(head !== SLICE2_HEAD, 'Durability corrective commit does not exist yet');
  const chain = extensionHeads(head).map(extensionLink);
  const relative = path.relative(ARTIFACT_ROOT, path.resolve(directory));
  ensure(/^evidence\/candidate\/[^/]+$/.test(relative), 'Wrong durability bundle scope');
  const receipt = {
    schema_version: 1,
    kind: 'durability-postcommit',
    run_id: RUN_ID,
    base: SLICE2_HEAD,
    parent,
    commit: head,
    tree: git('rev-parse', `${head}^{tree}`).trim(),
    bundle: { path: `${relative}/bundle.json`, sha256: bundleSha256 },
    chain,
  };
  return { receipt, result: verifyDurabilityReceiptData(receipt, head) };
}
export function verifyDurabilityReceiptData(receipt, head) {
  equal(receipt.schema_version, 1, 'Wrong durability receipt schema');
  equal(receipt.kind, 'durability-postcommit', 'Wrong durability receipt kind');
  equal(receipt.run_id, RUN_ID, 'Wrong durability run');
  equal(receipt.base, SLICE2_HEAD, 'Wrong durability base');
  equal(receipt.commit, head, 'Wrong durability head');
  equal(receipt.tree, git('rev-parse', `${head}^{tree}`).trim(), 'Wrong durability tree');
  equal(receipt.parent, git('show', '-s', '--format=%P', head).trim(), 'Wrong durability parent');
  equal(receipt.chain, extensionHeads(head).map(extensionLink), 'Wrong durability ancestry/patch/files');
  const directory = path.dirname(containedPath(ARTIFACT_ROOT, receipt.bundle.path));
  const bundle = JSON.parse(readArtifact(ARTIFACT_ROOT, receipt.bundle));
  equal(bundle.head, receipt.parent, 'Wrong durability precommit head');
  equal(bundle.base, SLICE2_HEAD, 'Wrong durability bundle base');
  const manifest = JSON.parse(readArtifact(directory, bundle.input_manifest));
  const snapshot = committedSnapshot(head, receipt.parent, manifest, receipt.parent);
  snapshot.manifest.harness = DELIVERY_FILES.toSorted().map((file) => ({
    path: path.join(PROJECT, file),
    sha256: sha256(commitFile(head, file)),
  }));
  const patch = readArtifact(directory, bundle.patch);
  equal(patchSections(patch), patchSections(commitPatch(receipt.parent, head)), 'Wrong durability complete patch');
  const contract = durabilityContract(snapshot, directory, {
    patch_sha256: sha256(patch),
    test_source_sha256s: DELIVERY_FILES.map((file) => sha256(commitFile(head, file))),
  });
  return {
    ...durabilityObservations(bundle, directory, verifyBundle(bundle, directory, contract), head),
    committed_head: head,
    committed_tree: receipt.tree,
    postcommit_verified: true,
  };
}
export function verifyDurabilityReceipt() {
  const head = frozenHead();
  const pointer = JSON.parse(
    fs.readFileSync(containedPath(ARTIFACT_ROOT, `reports/durability-committed-${head}.json`)),
  );
  return {
    ...verifyDurabilityReceiptData(JSON.parse(readArtifact(ARTIFACT_ROOT, pointer.receipt)), head),
    receipt: pointer.receipt,
  };
}
export function finalizeDurability(directory, bundleSha256) {
  const head = frozenHead();
  // G4's committed chain must be verified before G5 can be finalized.
  verifyLegacyReceipt();
  const { receipt } = auditDurabilityFinalization(head, directory, bundleSha256);
  const pointer = path.join(ARTIFACT_ROOT, 'reports', `durability-committed-${head}.json`);
  if (fs.existsSync(pointer)) {
    const existing = verifyDurabilityReceipt();
    equal(JSON.parse(readArtifact(ARTIFACT_ROOT, existing.receipt)), receipt, 'Immutable durability receipt differs');
    return existing;
  }
  const directoryOut = outputDirectory(
    path.join(ARTIFACT_ROOT, 'evidence', 'candidate', `durability-postcommit-${head}-${randomUUID()}`),
  );
  const ref = writeArtifact(directoryOut, 'receipt.json', JSON.stringify(receipt, null, 2) + '\n');
  ref.path = path.relative(ARTIFACT_ROOT, path.join(directoryOut, ref.path));
  writeArtifact(path.dirname(pointer), path.basename(pointer), JSON.stringify({ receipt: ref }) + '\n');
  return verifyDurabilityReceipt();
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv[2] === 'finalize-durability') {
      ensure(process.argv.length === 5, 'Usage: collect.mjs finalize-durability <attempt> <bundle-sha256>');
      process.stdout.write(JSON.stringify(finalizeDurability(...process.argv.slice(3))) + '\n');
    } else if (process.argv[2] === 'finalize-legacy') {
      ensure(
        process.argv.length === 6,
        'Usage: collect.mjs finalize-legacy <candidate-attempt> <bundle-sha256> <original-bundle-sha256>',
      );
      process.stdout.write(JSON.stringify(finalizeLegacy(...process.argv.slice(3))) + '\n');
    } else {
      ensure(process.argv.length === 3, 'Usage: collect.mjs <group>');
      const result = await collect(process.argv[2]);
      process.stdout.write(
        JSON.stringify({
          directory: result.directory,
          bundle_sha256: result.bundle_sha256,
          exit_code: result.execution.exit_code,
        }) + '\n',
      );
    }
  } catch (error) {
    process.stderr.write(JSON.stringify({ collected: false, error: error.message }) + '\n');
    process.exitCode = 1;
  }
}
