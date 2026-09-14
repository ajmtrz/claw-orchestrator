import { execFileSync, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  ARTIFACT_ROOT,
  PROJECT,
  RUN_ID,
  containedPath,
  inspectTestReport,
  readArtifact,
  sha256,
  verifyBundle,
  LEGACY_TITLES,
  verifyLegacyCase,
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
  ensure(Number.isSafeInteger(timeout_ms) && timeout_ms > 0 && timeout_ms <= 60000, 'Invalid bounded timeout');
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

function legacySnapshot() {
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

function legacyContract(snapshot, directory) {
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
    patch_sha256: sha256(patchBytes(LEGACY_FILES)),
    test_source_sha256s: LEGACY_FILES.map((file) => sha256(fs.readFileSync(path.join(PROJECT, file)))),
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
  const pointer = path.join(ARTIFACT_ROOT, 'reports', `legacy-${snapshot.head}-${contract.patch_sha256}.json`);
  const bundle_sha256 = sha256(fs.readFileSync(path.join(directory, 'bundle.json')));
  fs.writeFileSync(pointer, JSON.stringify({ directory, bundle_sha256 }) + '\n', { flag: 'wx', mode: 0o600 });
  return { directory, bundle_sha256, execution };
}

export function verifyLegacy(directory) {
  const snapshot = legacySnapshot();
  if (!directory) {
    const pointer = path.join(
      ARTIFACT_ROOT,
      'reports',
      `legacy-${snapshot.head}-${sha256(patchBytes(LEGACY_FILES))}.json`,
    );
    const receipt = JSON.parse(fs.readFileSync(containedPath(ARTIFACT_ROOT, path.relative(ARTIFACT_ROOT, pointer))));
    directory = receipt.directory;
    ensure(
      sha256(fs.readFileSync(containedPath(directory, 'bundle.json'))) === receipt.bundle_sha256,
      'Legacy bundle changed',
    );
  }
  ensure(/^evidence\/candidate\/[^/]+$/.test(path.relative(ARTIFACT_ROOT, directory)), 'Wrong legacy attempt scope');
  const bundle = JSON.parse(fs.readFileSync(containedPath(directory, 'bundle.json')));
  const values = verifyBundle(bundle, directory, legacyContract(snapshot, directory));
  for (const ref of values.get('evidence-files')) readArtifact(directory, ref);
  const titles = values.get('case-index').map((relative) => verifyLegacyCase(containedPath(directory, relative)));
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

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    ensure(process.argv.length === 3, 'Usage: collect.mjs <group>');
    const result = await collect(process.argv[2]);
    process.stdout.write(
      JSON.stringify({
        directory: result.directory,
        bundle_sha256: result.bundle_sha256,
        exit_code: result.execution.exit_code,
      }) + '\n',
    );
  } catch (error) {
    process.stderr.write(JSON.stringify({ collected: false, error: error.message }) + '\n');
    process.exitCode = 1;
  }
}
