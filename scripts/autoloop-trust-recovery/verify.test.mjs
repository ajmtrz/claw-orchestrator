import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { verifyBundle, verifySeries, verifyLegacyCase, LEGACY_TITLES, SOURCE_URL as verifySource } from './verify.mjs';
import { captureExecution, collect, SOURCE_URL as collectorSource } from './collect.mjs';
import * as collector from './collect.mjs';

// These deliberately synthetic documents test the validator, never recovery.
// A missing hash/identity/predicate/report check must make a negative test fail.
const project = fileURLToPath(new URL('../../', import.meta.url));
const run = 'CLAWO-AUTOLOOP-DURABLE-RECOVERY-TRUST-RECOVERY-R1';
const scratch = path.join(project, '.artifacts', run, 'evidence', 'sensitivity');
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
const sha = 'a'.repeat(40);
const tap =
  'TAP version 13\n# Subtest: fixture command\nok 1 - fixture command\n1..1\n# tests 1\n# suites 0\n# pass 1\n# fail 0\n# cancelled 0\n# skipped 0\n# todo 0\n';

// This immutable, real Git commit and its retained precommit evidence are the
// historical anchor approved for this run. Tests never create Git commits.
const originalCommit = '1559595d688fd91e2d9d9073c395f66d222afd83';
const originalBundle = 'evidence/candidate/legacy-1789355466078-105035dd-ce89-4fb4-a876-56d398fb81d4/bundle.json';
const artifactRoot = path.join(project, '.artifacts', run);
const firstCorrection = 'a0a6fdaf188d107d9eacd760984685ada813da1e';
const firstCorrectionDirectory = path.join(
  artifactRoot,
  'evidence/candidate/legacy-1789359636920-7c9e02d9-4e71-4c01-a83b-087501a6396d',
);
const firstCorrectionDigest = '8f28667ee4ee9db372d767ea08178bbd0823ac87763fff849381f907cf240c99';

test('audits controller commit then receipt verification under the controller Node path', () => {
  fs.mkdirSync(scratch, { recursive: true });
  const directory = fs.mkdtempSync(path.join(scratch, 'controller-finalization-'));
  const code = `import { auditLegacyFinalization } from ${JSON.stringify(collectorSource)};
    const result = auditLegacyFinalization(${JSON.stringify(firstCorrection)}, ${JSON.stringify(firstCorrectionDirectory)}, ${JSON.stringify(firstCorrectionDigest)}, 'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d');
    console.log(JSON.stringify({runtime:process.execPath, ...result.result}));`;
  // This is an already-installed controller executable, not a copied binary or
  // changed runtime configuration. The audit executes the complete receipt
  // construction/verification path using actual committed Git objects.
  const argv = ['proxy', '/home/openclaw/.openclaw/tools/node-v26.7.0/bin/node', '--input-type=module', '-'];
  const result = spawnSync('rtk', argv, { cwd: project, input: code, encoding: 'utf8' });
  fs.writeFileSync(path.join(directory, 'stdout.txt'), result.stdout);
  fs.writeFileSync(path.join(directory, 'stderr.txt'), result.stderr);
  fs.writeFileSync(
    path.join(directory, 'execution.json'),
    JSON.stringify({ argv: ['rtk', ...argv], status: result.status, signal: result.signal, commit: firstCorrection }),
  );
  assert.equal(result.status, 0, result.stderr);
  const observed = JSON.parse(result.stdout);
  assert.equal(observed.committed_head, firstCorrection);
  assert.equal(observed.committed_tree, 'e4f0fdbe301d8b6843447d4dae12505ce6ca0c73');
  assert.equal(observed.postcommit_verified, true);
  assert.equal(observed.cases, 25);
});

test('rejects altered historical tool bytes despite refreshed manifest and bundle hashes', () => {
  const directory = fs.mkdtempSync(path.join(scratch, 'controller-tool-tamper-'));
  fs.cpSync(firstCorrectionDirectory, directory, { recursive: true });
  const inputFile = path.join(directory, 'inputs.json');
  const manifest = JSON.parse(fs.readFileSync(inputFile));
  manifest.tools[1].sha256 = '0'.repeat(64);
  const inputBytes = JSON.stringify(manifest, null, 2) + '\n';
  fs.writeFileSync(inputFile, inputBytes);
  const bundleFile = path.join(directory, 'bundle.json');
  const bundle = JSON.parse(fs.readFileSync(bundleFile));
  bundle.input_manifest.sha256 = digest(inputBytes);
  bundle.tool_sha256 = digest(JSON.stringify(manifest.tools));
  const bytes = JSON.stringify(bundle, null, 2) + '\n';
  fs.writeFileSync(bundleFile, bytes);
  assert.throws(
    () =>
      collector.auditLegacyFinalization(
        firstCorrection,
        directory,
        digest(bytes),
        'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d',
      ),
    /historical tool|tool.*bytes/i,
  );
});
function originalCommitLink() {
  const files = JSON.parse(
    fs.readFileSync(path.join(artifactRoot, 'evidence/candidate/slice1-finish-002/candidate-files.json')),
  );
  return {
    base: '2694c0babcf16030278e58d829a7c71bcaa0f7a2',
    parent: '1af92f4e1a8d6f82bfe8e1175310f672606c5e55',
    commit: originalCommit,
    tree: '10dc9a4a7c1c23795cd8cb998beb4b35fc9b13e2',
    bundle: { path: originalBundle, sha256: 'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d' },
    patch: {
      path: originalBundle.replace('bundle.json', 'inputs.patch'),
      sha256: '9f31883089e7488c25e15f7cab13e1f9ae975471f4ad46706ac2f49628c85e36',
    },
    files: files.sort((a, b) => a.path.localeCompare(b.path)),
  };
}

for (const [name, change, expected] of [
  [
    'head',
    (x) => {
      x.commit = originalCommit;
    },
    /head/i,
  ],
  [
    'tree',
    (x) => {
      x.tree = originalCommitLink().tree;
    },
    /tree/i,
  ],
  [
    'parent',
    (x) => {
      x.parent = firstCorrection;
    },
    /parent/i,
  ],
  [
    'base',
    (x) => {
      x.base = originalCommit;
    },
    /base/i,
  ],
  [
    'missing history',
    (x) => {
      delete x.history;
    },
    /history/i,
  ],
  [
    'replayed history',
    (x) => {
      x.history = [x.original];
    },
    /history/i,
  ],
  [
    'original anchor hash',
    (x) => {
      x.original.bundle.sha256 = '0'.repeat(64);
    },
    /bundle/i,
  ],
  [
    'candidate bundle hash',
    (x) => {
      x.candidate.bundle.sha256 = '0'.repeat(64);
    },
    /hash/i,
  ],
]) {
  test(`rejects complete controller receipt with wrong ${name}`, () => {
    const { receipt } = collector.auditLegacyFinalization(
      firstCorrection,
      firstCorrectionDirectory,
      firstCorrectionDigest,
      'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d',
    );
    change(receipt);
    assert.throws(() => collector.verifyReceiptData(receipt, firstCorrection), expected);
  });
}

test('binds preserved precommit evidence to the actual immutable Slice 1 Git commit', () => {
  const before = fs.readFileSync(path.join(artifactRoot, originalBundle));
  assert.equal(collector.verifyLegacyCommitLink(originalCommitLink(), originalCommit).cases, 25);
  assert.deepEqual(fs.readFileSync(path.join(artifactRoot, originalBundle)), before);
});

for (const [name, change, expected] of [
  [
    'head',
    (x) => {
      x.commit = '2694c0babcf16030278e58d829a7c71bcaa0f7a2';
    },
    /head/i,
  ],
  [
    'tree',
    (x) => {
      x.tree = '4b7b5561919457ad1193467b3caa7e709d53db0f';
    },
    /tree/i,
  ],
  [
    'parent',
    (x) => {
      x.parent = x.base;
    },
    /parent/i,
  ],
  [
    'base',
    (x) => {
      x.base = x.parent;
    },
    /base/i,
  ],
  [
    'patch hash',
    (x) => {
      x.patch.sha256 = '0'.repeat(64);
    },
    /hash|patch/i,
  ],
  [
    'file bytes',
    (x) => {
      x.files[0].sha256 = '0'.repeat(64);
    },
    /file|bytes/i,
  ],
  [
    'file Git object',
    (x) => {
      x.files[0].git_blob_sha1 = '0'.repeat(40);
    },
    /file|object/i,
  ],
  [
    'omitted changed file',
    (x) => {
      x.files.pop();
    },
    /file/i,
  ],
  [
    'bundle hash',
    (x) => {
      x.bundle.sha256 = '0'.repeat(64);
    },
    /hash|bundle/i,
  ],
]) {
  test(`rejects a committed evidence link with wrong ${name}`, () => {
    const link = originalCommitLink();
    change(link);
    assert.throws(() => collector.verifyLegacyCommitLink(link, originalCommit), expected);
  });
}

test('rejects changed patch bytes even when the supplied patch hash is refreshed', () => {
  const link = originalCommitLink();
  fs.mkdirSync(scratch, { recursive: true });
  const dir = fs.mkdtempSync(path.join(scratch, 'postcommit-patch-'));
  const bytes = fs
    .readFileSync(path.join(artifactRoot, link.patch.path), 'utf8')
    .replace('@@ -13,6 +13,8 @@', '@@ -14,6 +14,8 @@');
  const file = path.join(dir, 'changed.patch');
  fs.writeFileSync(file, bytes);
  link.patch = { path: path.relative(artifactRoot, file), sha256: digest(bytes) };
  assert.throws(() => collector.verifyLegacyCommitLink(link, originalCommit), /patch/i);
});

test('rejects a rewritten historical bundle identity even with a refreshed bundle hash', () => {
  const link = originalCommitLink();
  fs.mkdirSync(scratch, { recursive: true });
  const dir = fs.mkdtempSync(path.join(scratch, 'postcommit-bundle-'));
  const b = JSON.parse(fs.readFileSync(path.join(artifactRoot, link.bundle.path)));
  b.head = link.base;
  const bytes = JSON.stringify(b);
  const file = path.join(dir, 'bundle.json');
  fs.writeFileSync(file, bytes);
  link.bundle = { path: path.relative(artifactRoot, file), sha256: digest(bytes) };
  assert.throws(() => collector.verifyLegacyCommitLink(link, originalCommit), /head/i);
});

test('cannot finalize a receipt before the corrective controller commit exists', () => {
  assert.throws(
    () =>
      collector.finalizeLegacy(
        path.join(artifactRoot, path.dirname(originalBundle)),
        'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d',
        'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d',
      ),
    /commit|frozen|clean/i,
  );
});

// Synthetic validator inputs, not runtime evidence. Expectations intentionally
// do not import the verifier's scenario builder or derive clocks from titles.
function legacyLabelFixture(change = () => {}) {
  fs.mkdirSync(scratch, { recursive: true });
  const directory = fs.mkdtempSync(path.join(scratch, 'legacy-label-unit-'));
  const title =
    'trust recovery legacy persisted state candidate: reads ordinary legacy metadata at TTL -1 ms from real registry bytes';
  const action = {
    action: 'load',
    subject: project,
    now: 1789207199999,
    seed: [
      {
        name: 'autoloop-legacy-probe-planner',
        claudeSessionId: 'legacy-session-id',
        cwd: project,
        originalCreated: '2026-09-05T10:00:00.000Z',
        lastResumed: '2026-09-05T10:00:00.000Z',
        lastActivity: 1788602400000,
      },
    ],
  };
  change(action);
  const input = { runId: 'legacy-probe', sessionName: 'autoloop-legacy-probe-planner', ...action };
  const seedBytes = JSON.stringify(action.seed);
  fs.writeFileSync(path.join(directory, 'seed.json'), seedBytes);
  const snapshot = { registry: { path: path.join(directory, 'seed.json'), sha256: digest(seedBytes) } };
  fs.mkdirSync(path.join(directory, 'process-42'));
  const source = path.join(action.subject, 'src/session-manager.ts');
  const values = [
    { kind: 'invocation', data: input },
    { path: source, sha256: digest(fs.readFileSync(source)) },
  ];
  fs.writeFileSync(
    path.join(directory, 'process-42/observations.jsonl'),
    values
      .map((value, sequence) => JSON.stringify({ sequence, process_id: 42, observer_id: 'unit-worker', value }) + '\n')
      .join(''),
  );
  fs.writeFileSync(
    path.join(directory, 'parent.jsonl'),
    JSON.stringify({
      sequence: 0,
      process_id: 43,
      observer_id: 'unit-parent',
      value: { kind: 'execution', pid: 42, action, status: 0, signal: null },
    }) + '\n',
  );
  fs.writeFileSync(
    path.join(directory, 'worker-42-stdout.txt'),
    JSON.stringify({
      process_id: 42,
      loaded: action.seed,
      apis: { reserve: true, release: true },
      snapshots: { seed: snapshot, final: snapshot },
    }),
  );
  fs.writeFileSync(path.join(directory, 'case.json'), JSON.stringify({ title }));
  return { directory, title };
}

test('accepts independently specified below-TTL validator inputs', () => {
  const f = legacyLabelFixture();
  assert.equal(verifyLegacyCase(f.directory), f.title);
});

for (const [name, replacement] of [
  [
    'TTL-plus-1 relabel',
    'trust recovery legacy persisted state candidate: reads ordinary legacy metadata at TTL +1 ms from real registry bytes',
  ],
  [
    'TTL-boundary replay',
    'trust recovery legacy persisted state candidate: reads ordinary legacy metadata at TTL +0 ms from real registry bytes',
  ],
  [
    'starting-subject relabel',
    'trust recovery legacy persisted state start: reads ordinary legacy metadata at TTL -1 ms from real registry bytes',
  ],
]) {
  test(`rejects ${name} even with the complete required title inventory`, () => {
    const f = legacyLabelFixture();
    assert.equal(verifyLegacyCase(f.directory), f.title);
    // A correct-looking supplied descriptor cannot substitute for fixed code.
    fs.writeFileSync(
      path.join(f.directory, 'case.json'),
      JSON.stringify({ title: replacement, descriptor: { title: replacement, delta: 1, subject: 'candidate' } }),
    );
    const titles = LEGACY_TITLES.map((t) => (t === replacement ? replacement : t));
    assert.deepEqual(titles, LEGACY_TITLES);
    assert.throws(() => verifyLegacyCase(f.directory), /scenario|descriptor/i);
  });
}

for (const [name, change] of [
  [
    'seed name',
    (a) => {
      a.seed[0].name = 'another-legacy-session';
    },
  ],
  [
    'seed clock',
    (a) => {
      a.seed[0].lastActivity -= 1;
    },
  ],
  [
    'extra precondition',
    (a) => {
      a.seed[0].agentGeneration = 0;
    },
  ],
]) {
  test(`rejects self-consistent legacy evidence with a wrong ${name}`, () => {
    const f = legacyLabelFixture(change);
    assert.throws(() => verifyLegacyCase(f.directory), /scenario|descriptor/i);
  });
}

// Actual worker import receipts. Unit-test fixture events below are separate.
if (process.env.CLAWO_TRUST_CAPTURE_ID) {
  for (const [index, url] of [import.meta.url, verifySource, collectorSource].entries()) {
    const source = fileURLToPath(url);
    console.log(
      'TRUST_RECOVERY_OBSERVATION ' +
        JSON.stringify({
          sequence: index,
          process_id: process.pid,
          observer_id: `node-test:${process.pid}`,
          execution_id: process.env.CLAWO_TRUST_CAPTURE_ID,
          value: { path: source, sha256: digest(fs.readFileSync(source)) },
        }),
    );
  }
}

function fixture() {
  fs.mkdirSync(scratch, { recursive: true });
  const directory = fs.mkdtempSync(path.join(scratch, 'validator-fixture-'));
  const artifact = (name, bytes) => {
    fs.writeFileSync(path.join(directory, name), bytes, { flag: 'wx' });
    return { path: name, sha256: digest(bytes) };
  };
  const manifest = {
    tracked: [{ path: 'src/example.ts', sha256: digest('example source') }],
    harness: [{ path: 'verify.test.mjs', sha256: digest('fixture harness') }],
    dependencies: [{ path: 'package-lock.json', sha256: digest('fixture dependencies') }],
    tools: [{ path: '/fixture/node', sha256: digest('fixture node') }],
  };
  const events = [
    { sequence: 1, process_id: 42, observer_id: 'fixture-observer', value: 'directive A\n' },
    { sequence: 2, process_id: 42, observer_id: 'fixture-observer', value: digest('directive A\n') },
    { sequence: 3, process_id: 42, observer_id: 'fixture-observer', value: [{ type: 'ack' }] },
  ];
  const eventArtifact = artifact('events.json', JSON.stringify(events));
  const assertions = [
    { id: 'payload-digest', predicate: 'digest-equal', observations: ['payload', 'digest'] },
    { id: 'durable-order', predicate: 'ordered', observations: ['payload', 'digest', 'rows'] },
    { id: 'one-ack', predicate: 'row-count', observations: ['rows'], expected: 1 },
  ];
  const bundle = {
    schema_version: 1,
    run_id: run,
    case_id: 'validator-fixture',
    requirement_ids: ['HARNESS-VALIDATION'],
    subject_kind: 'sensitivity',
    base: sha,
    head: sha,
    tree: sha,
    frozen: false,
    input_manifest: artifact('inputs.json', JSON.stringify(manifest)),
    harness_sha256: digest(JSON.stringify(manifest.harness)),
    dependency_sha256: digest(JSON.stringify(manifest.dependencies)),
    tool_sha256: digest(JSON.stringify(manifest.tools)),
    patch: artifact('tests.patch', 'fixture-only patch\n'),
    test_sources: [artifact('test-source.txt', 'synthetic validator input, not an incident test\n')],
    source_imports: [],
    executions: [
      {
        id: 'execution-1',
        argv: ['node', '--test', 'fixture.test.mjs'],
        cwd: '/fixture',
        started_at: '2026-09-14T01:00:00.000Z',
        ended_at: '2026-09-14T01:00:01.000Z',
        process_id: 42,
        exit_code: 0,
        signal: null,
        timed_out: false,
        stdout: artifact('stdout.txt', tap),
        stderr: artifact('stderr.txt', ''),
        report: artifact('report.tap', tap),
        report_format: 'node-tap',
        discovered_test_ids: ['fixture command'],
        executed_test_ids: ['fixture command'],
        skipped_test_ids: [],
        redactions: [],
      },
    ],
    observations: events.map((event, index) => ({
      id: ['payload', 'digest', 'rows'][index],
      artifact: eventArtifact,
      pointer: `/${index}`,
      sequence: event.sequence,
      process_id: event.process_id,
      observer_id: event.observer_id,
      execution_id: 'execution-1',
      value: event.value,
    })),
    assertions,
  };
  // Expectations are supplied by reviewed caller code, never read from a bundle.
  const contract = {
    run_id: run,
    case_id: 'validator-fixture',
    subject_kind: 'sensitivity',
    base: sha,
    head: sha,
    tree: sha,
    frozen: false,
    patch_sha256: bundle.patch.sha256,
    test_source_sha256s: bundle.test_sources.map((source) => source.sha256),
    input_manifest_sha256: bundle.input_manifest.sha256,
    harness_sha256: bundle.harness_sha256,
    dependency_sha256: bundle.dependency_sha256,
    tool_sha256: bundle.tool_sha256,
    requirement_ids: ['HARNESS-VALIDATION'],
    executions: [
      { argv: ['node', '--test', 'fixture.test.mjs'], cwd: '/fixture', required_test_ids: ['fixture command'] },
    ],
    assertions: structuredClone(assertions),
    required_observations: ['payload', 'digest', 'rows'],
    required_fault_observations: [],
    required_source_paths: [],
  };
  return { directory, artifact, bundle, contract, events };
}

test('recomputes digest, durable event order and row count from referenced bytes', () => {
  const f = fixture();
  const values = verifyBundle(f.bundle, f.directory, f.contract);
  assert.equal(values.get('payload'), 'directive A\n');
  assert.deepEqual(values.get('rows'), [{ type: 'ack' }]);
});

for (const [name, mutate, error] of [
  [
    'fabricated invariant booleans',
    (f) => {
      f.bundle.invariants = { exactly_once: true };
    },
    /schema|additional/i,
  ],
  [
    'missing artifacts',
    (f) => {
      fs.unlinkSync(path.join(f.directory, 'events.json'));
    },
    /artifact|ENOENT/i,
  ],
  [
    'changed artifact hashes',
    (f) => {
      fs.appendFileSync(path.join(f.directory, 'events.json'), '\n');
    },
    /hash/i,
  ],
  [
    'wrong subject kind',
    (f) => {
      f.bundle.subject_kind = 'candidate';
    },
    /subject/i,
  ],
  [
    'wrong candidate head',
    (f) => {
      f.bundle.head = 'b'.repeat(40);
    },
    /head/i,
  ],
  [
    'wrong input manifest',
    (f) => {
      f.contract.input_manifest_sha256 = 'b'.repeat(64);
    },
    /manifest/i,
  ],
  [
    'hidden process failure',
    (f) => {
      f.bundle.executions[0].exit_code = 1;
    },
    /execution|exit/i,
  ],
  [
    'a recorded spawn error with exit zero',
    (f) => {
      f.bundle.executions[0].spawn_error = 'ENOENT';
    },
    /execution|spawn/i,
  ],
  [
    'success without an observed process',
    (f) => {
      f.bundle.executions[0].process_id = null;
    },
    /execution|process/i,
  ],
  [
    'a terminating signal',
    (f) => {
      f.bundle.executions[0].exit_code = null;
      f.bundle.executions[0].signal = 'SIGKILL';
    },
    /execution|signal/i,
  ],
  [
    'a timeout',
    (f) => {
      f.bundle.executions[0].timed_out = true;
    },
    /execution|timeout/i,
  ],
  [
    'zero selected tests',
    (f) => {
      f.bundle.executions[0].executed_test_ids = [];
    },
    /test/i,
  ],
  [
    'required skips',
    (f) => {
      f.bundle.executions[0].skipped_test_ids = ['fixture command'];
    },
    /skip|test/i,
  ],
  [
    'fabricated parsed values',
    (f) => {
      f.bundle.observations[0].value = 'directive B\n';
    },
    /value/i,
  ],
  [
    'reordered observation declarations',
    (f) => {
      f.bundle.observations.reverse();
    },
    /order|sequence/i,
  ],
  [
    'fabricated event sequence',
    (f) => {
      f.bundle.observations[0].sequence = 20;
    },
    /sequence/i,
  ],
  [
    'a missing fault observation',
    (f) => {
      f.contract.required_fault_observations = ['process-loss'];
    },
    /fault|observation/i,
  ],
  [
    'removed predicates',
    (f) => {
      f.bundle.assertions = [];
    },
    /assertion|schema/i,
  ],
  [
    'weakened row-count predicate',
    (f) => {
      f.bundle.assertions[2].expected = 0;
    },
    /assertion|contract/i,
  ],
  [
    'Coder-authored review claims',
    (f) => {
      f.bundle.review = { author: 'coder', decision: 'advance' };
    },
    /schema|additional/i,
  ],
  [
    'unrecorded dirty test bytes',
    (f) => {
      f.bundle.test_sources = [];
    },
    /patch|test|unfrozen/i,
  ],
  [
    'missing RED patch',
    (f) => {
      f.bundle.patch = null;
    },
    /patch|unfrozen/i,
  ],
  [
    'an unfrozen snapshot relabeled as frozen',
    (f) => {
      f.bundle.frozen = true;
      f.bundle.patch = null;
    },
    /frozen/i,
  ],
  [
    'substituted RED patch bytes',
    (f) => {
      f.bundle.patch = f.artifact('wrong.patch', 'different patch\n');
    },
    /patch/i,
  ],
  [
    'substituted test source bytes',
    (f) => {
      f.bundle.test_sources = [f.artifact('wrong-test.txt', 'different test\n')];
    },
    /test.*source/i,
  ],
  [
    'incorrect command identity',
    (f) => {
      f.bundle.executions[0].argv = ['node', '-e', 'process.exit(0)'];
    },
    /command|argv/i,
  ],
  [
    'duplicate execution identity',
    (f) => {
      f.bundle.executions.push(structuredClone(f.bundle.executions[0]));
    },
    /execution|duplicate/i,
  ],
  [
    'unknown observation execution',
    (f) => {
      f.bundle.observations[0].execution_id = 'invented';
    },
    /execution/i,
  ],
  [
    'fabricated observer identity',
    (f) => {
      f.bundle.observations[0].observer_id = 'invented';
    },
    /observer/i,
  ],
  [
    'out-of-run path traversal',
    (f) => {
      f.bundle.observations[0].artifact.path = '../events.json';
    },
    /path|artifact/i,
  ],
  [
    'absolute artifact paths',
    (f) => {
      f.bundle.observations[0].artifact.path = path.join(f.directory, 'events.json');
    },
    /path|artifact/i,
  ],
]) {
  test(`rejects ${name}`, () => {
    const f = fixture();
    mutate(f);
    assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), error);
  });
}

test('rejects symlinked artifacts even when their target has the expected bytes', () => {
  const f = fixture();
  fs.renameSync(path.join(f.directory, 'events.json'), path.join(f.directory, 'target.json'));
  fs.symlinkSync('target.json', path.join(f.directory, 'events.json'));
  assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /symlink|artifact/i);
});

test('a recomputed hash cannot hide a failed test in raw TAP', () => {
  const f = fixture();
  const failed = tap.replace('\nok 1', '\nnot ok 1').replace('# fail 0', '# fail 1').replace('# pass 1', '# pass 0');
  f.bundle.executions[0].report = f.artifact('failed.tap', failed);
  assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /report|fail|TAP/i);
});

test('a green report cannot hide a failed raw stdout stream', () => {
  const f = fixture();
  f.bundle.executions[0].stdout = f.artifact('hidden-failure.txt', tap + 'not ok 2 - hidden failure\n');
  assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /stdout|report|fail|TAP/i);
});

test('rejects cancellation and unhandled errors even with exit zero', () => {
  for (const output of [tap.replace('# cancelled 0', '# cancelled 1'), tap + '# Error: unhandledRejection\n']) {
    const f = fixture();
    f.bundle.executions[0].stdout = f.artifact('unhandled.txt', output);
    f.bundle.executions[0].report = f.bundle.executions[0].stdout;
    assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /report|cancel|unhandled|TAP/i);
  }
});

test('recomputes predicates after both the event bytes and declared values change', () => {
  const f = fixture();
  f.events[2].value.push({ type: 'ack' });
  const changed = f.artifact('duplicate-ack.json', JSON.stringify(f.events));
  for (const o of f.bundle.observations) o.artifact = changed;
  f.bundle.observations[2].value = f.events[2].value;
  assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /one-ack|row-count/i);
});

test('detects source import misattribution from the import receipt', () => {
  const f = fixture();
  f.bundle.source_imports = [{ path: '/upstream/src/a.ts', sha256: 'a'.repeat(64), observation_id: 'payload' }];
  f.contract.required_source_paths = ['/upstream/src/a.ts'];
  assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /import|source/i);
});

test('refuses synthetic booleans as fixed literal predicates', () => {
  const f = fixture();
  f.bundle.assertions.push({ id: 'invented-pass', predicate: 'literal', observations: ['payload'], expected: true });
  f.contract.assertions = structuredClone(f.bundle.assertions);
  assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /boolean|predicate|schema/i);
});

test('accepts only complete ordered repetitions with distinct execution identities', () => {
  const first = fixture();
  const second = fixture();
  second.bundle.executions[0].id = 'execution-2';
  second.bundle.executions[0].started_at = '2026-09-14T01:00:02.000Z';
  second.bundle.executions[0].ended_at = '2026-09-14T01:00:03.000Z';
  for (const o of second.bundle.observations) o.execution_id = 'execution-2';
  const entries = [first, second].map((f) => ({ bundle: f.bundle, directory: f.directory }));
  assert.equal(verifySeries(entries, first.contract, 2).length, 2);
  assert.throws(() => verifySeries(entries.slice(0, 1), first.contract, 2), /repetition/i);
  assert.throws(() => verifySeries(entries.toReversed(), first.contract, 2), /order/i);
  assert.throws(() => verifySeries([entries[0], entries[0]], first.contract, 2), /duplicate/i);
});

test('reads a JSON event from an exact byte range and rejects an invalid range', () => {
  const f = fixture();
  const row = JSON.stringify(f.events[0]);
  f.bundle.observations[0].artifact = f.artifact('event.jsonl', row + '\n');
  delete f.bundle.observations[0].pointer;
  f.bundle.observations[0].byte_range = [0, Buffer.byteLength(row)];
  assert.equal(verifyBundle(f.bundle, f.directory, f.contract).get('payload'), 'directive A\n');
  f.bundle.observations[0].byte_range[1] += 100;
  assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /byte range/i);
});

test('collector retains real child stdout, stderr, argv, cwd, exit and test inventory', async () => {
  const f = fixture();
  const argv = [
    process.execPath,
    '-e',
    `process.stdout.write(${JSON.stringify(tap)}); process.stderr.write('diagnostic\\n'); process.exitCode = 7;`,
  ];
  const execution = await captureExecution({
    directory: f.directory,
    id: 'child-capture',
    argv,
    cwd: f.directory,
    timeout_ms: 3000,
  });
  assert.equal(execution.exit_code, 7);
  assert.equal(execution.signal, null);
  assert.equal(execution.timed_out, false);
  assert.deepEqual(execution.argv, argv);
  assert.equal(execution.cwd, f.directory);
  assert.ok(execution.process_id > 0);
  assert.deepEqual(execution.executed_test_ids, ['fixture command']);
  assert.equal(fs.readFileSync(path.join(f.directory, execution.stdout.path), 'utf8'), tap);
  assert.equal(fs.readFileSync(path.join(f.directory, execution.stderr.path), 'utf8'), 'diagnostic\n');
  assert.equal(execution.stdout.sha256, digest(tap));
  assert.ok(Date.parse(execution.ended_at) >= Date.parse(execution.started_at));
});

test('collector kills its timed-out process and retains its terminating signal', async () => {
  const f = fixture();
  const execution = await captureExecution({
    directory: f.directory,
    id: 'timeout-capture',
    argv: [process.execPath, '-e', 'setInterval(() => {}, 1000)'],
    cwd: f.directory,
    timeout_ms: 100,
  });
  assert.equal(execution.timed_out, true);
  assert.equal(execution.signal, 'SIGKILL');
  assert.equal(execution.exit_code, null);
  assert.throws(() => process.kill(execution.process_id, 0), { code: 'ESRCH' });
});

test('collector preserves spawn failure rather than inventing a successful process', async () => {
  const f = fixture();
  const execution = await captureExecution({
    directory: f.directory,
    id: 'spawn-failure',
    argv: [path.join(f.directory, 'missing-command')],
    cwd: f.directory,
    timeout_ms: 1000,
  });
  assert.equal(execution.process_id, null);
  assert.match(execution.spawn_error, /ENOENT/);
  assert.notEqual(execution.exit_code, 0);
});

test('collector redacts known credential forms and records the applied rules', async () => {
  const f = fixture();
  const secret = 'fixture-secret-12345';
  const execution = await captureExecution({
    directory: f.directory,
    id: 'redaction-capture',
    argv: [
      process.execPath,
      '-e',
      `process.stdout.write('Authorization: Bearer ${secret}\\n'); process.stderr.write('OPENAI_API_KEY=${secret}\\n')`,
    ],
    cwd: f.directory,
    timeout_ms: 3000,
  });
  const stdout = fs.readFileSync(path.join(f.directory, execution.stdout.path), 'utf8');
  const stderr = fs.readFileSync(path.join(f.directory, execution.stderr.path), 'utf8');
  assert.ok(!stdout.includes(secret));
  assert.ok(!stderr.includes(secret));
  assert.match(stdout, /REDACTED/);
  assert.match(stderr, /REDACTED/);
  assert.equal(execution.redactions.filter((r) => r.artifact !== 'argv').length, 2);
  assert.equal(execution.redactions.filter((r) => r.artifact === 'argv').length, 2);
  assert.ok(!JSON.stringify(execution.argv).includes(secret), 'argv must also be sanitized');
});

test('collector refuses overwrites and output paths outside the run artifact root before spawning', async () => {
  const f = fixture();
  const marker = path.join(f.directory, 'must-not-run');
  const argv = [process.execPath, '-e', `require('node:fs').writeFileSync(${JSON.stringify(marker)}, 'ran')`];
  for (const directory of [path.join(project, 'tasks', run), path.dirname(scratch)]) {
    await assert.rejects(
      captureExecution({ directory, id: 'protected-path', argv, cwd: project, timeout_ms: 1000 }),
      /path|directory/i,
    );
  }
  fs.writeFileSync(path.join(f.directory, 'existing.stdout.txt'), 'preserved');
  await assert.rejects(
    captureExecution({ directory: f.directory, id: 'existing', argv, cwd: project, timeout_ms: 1000 }),
    /exist|overwrite/i,
  );
  assert.ok(!fs.existsSync(marker));
  assert.equal(fs.readFileSync(path.join(f.directory, 'existing.stdout.txt'), 'utf8'), 'preserved');
});

test('collector cannot use a symlink to redirect output into a protected directory', async () => {
  const f = fixture();
  fs.symlinkSync(path.join(project, 'tasks', run), path.join(f.directory, 'redirect'));
  await assert.rejects(
    captureExecution({
      directory: path.join(f.directory, 'redirect'),
      id: 'escape',
      argv: [process.execPath, '-e', 'process.exit(0)'],
      cwd: project,
      timeout_ms: 1000,
    }),
    /symlink/i,
  );
});

test('collector rejects unknown groups and refuses heavy groups without a frozen candidate', async () => {
  await assert.rejects(collect('arbitrary-command'), /group/i);
  await assert.rejects(collect('full'), /frozen/i);
});

test('requires an independent contract instead of inferring acceptance from a bundle', () => {
  const f = fixture();
  assert.throws(() => verifyBundle(f.bundle, f.directory), /contract/i);
});

test('validates literal, equality and byte-preserving prefix predicates against event values', () => {
  for (const [predicate, left, right, expected, changed] of [
    ['literal', 'directive A\n', '', 'directive A\n', 'directive B\n'],
    ['equal', 'α\n', 'α\n', undefined, 'β\n'],
    ['prefix-preserved', 'α\n', 'α\nnew row\n', undefined, 'α changed\n'],
  ]) {
    const f = fixture();
    f.events[0].value = left;
    f.events[1].value = right;
    const replaceEvents = (name) => {
      const ref = f.artifact(name, JSON.stringify(f.events));
      for (const [i, o] of f.bundle.observations.entries()) {
        o.artifact = ref;
        o.value = f.events[i].value;
      }
    };
    replaceEvents('predicate-events.json');
    const assertion = {
      id: 'contract',
      predicate,
      observations: predicate === 'literal' ? ['payload'] : ['payload', 'digest'],
    };
    if (expected !== undefined) assertion.expected = expected;
    f.bundle.assertions = [assertion];
    f.contract.assertions = structuredClone(f.bundle.assertions);
    verifyBundle(f.bundle, f.directory, f.contract);
    f.events[predicate === 'literal' ? 0 : 1].value = changed;
    replaceEvents('changed-events.json');
    assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /predicate/i);
  }
});

test('validates Vitest inventory and rejects hidden failures even when success remains true', () => {
  const base = {
    success: true,
    numTotalTests: 1,
    numPassedTests: 1,
    numFailedTests: 0,
    numFailedTestSuites: 0,
    numPendingTests: 0,
    numTodoTests: 0,
    testResults: [
      {
        name: '/fixture/test.ts',
        status: 'passed',
        message: '',
        assertionResults: [{ fullName: 'a case', status: 'passed', failureMessages: [] }],
      },
    ],
  };
  for (const mutation of [
    null,
    (r) => {
      r.numFailedTests = 1;
    },
    (r) => {
      r.numPendingTests = 1;
    },
    (r) => {
      r.unhandledErrors = ['unhandled rejection'];
    },
    (r) => {
      r.testResults[0].assertionResults[0].failureMessages = ['hidden failure'];
    },
  ]) {
    const f = fixture();
    const report = structuredClone(base);
    if (mutation) mutation(report);
    f.bundle.executions[0].report = f.artifact('vitest.json', JSON.stringify(report));
    f.bundle.executions[0].report_format = 'vitest-json';
    f.bundle.executions[0].discovered_test_ids = ['/fixture/test.ts > a case'];
    f.bundle.executions[0].executed_test_ids = ['/fixture/test.ts > a case'];
    f.contract.executions[0].required_test_ids = ['/fixture/test.ts > a case'];
    if (mutation) assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /report|Vitest/i);
    else verifyBundle(f.bundle, f.directory, f.contract);
  }
});

test('a green Vitest report cannot hide a failing test in raw stdout', () => {
  const f = fixture();
  const report = {
    success: true,
    numTotalTests: 1,
    numPassedTests: 1,
    numFailedTests: 0,
    numFailedTestSuites: 0,
    numPendingTests: 0,
    testResults: [
      {
        name: '/fixture/test.ts',
        status: 'passed',
        message: '',
        assertionResults: [{ fullName: 'a case', status: 'passed', failureMessages: [] }],
      },
    ],
  };
  const e = f.bundle.executions[0];
  e.report_format = 'vitest-json';
  e.report = f.artifact('vitest.json', JSON.stringify(report));
  e.discovered_test_ids = e.executed_test_ids = ['/fixture/test.ts > a case'];
  f.contract.executions[0].required_test_ids = ['/fixture/test.ts > a case'];
  e.stdout = f.artifact('failed-stdout.txt', 'not ok 1 - hidden failure\n');
  assert.throws(() => verifyBundle(f.bundle, f.directory, f.contract), /diagnostic|stdout|fail/i);
});

test('Preparation CLI resolves its imports and reports missing evidence without writing', async () => {
  const f = fixture();
  const missing = path.join(project, '.artifacts', run, 'evidence', 'candidate', path.basename(f.directory));
  assert.equal(fs.existsSync(missing), false);
  const execution = await captureExecution({
    directory: f.directory,
    id: 'preparation-cli',
    argv: [process.execPath, 'scripts/autoloop-trust-recovery/verify.mjs', 'preparation', missing],
    cwd: project,
    timeout_ms: 3000,
  });
  assert.equal(execution.exit_code, 1);
  const result = JSON.parse(fs.readFileSync(path.join(f.directory, execution.stderr.path), 'utf8'));
  assert.equal(result.verified, false);
  assert.match(result.error, /ENOENT/);
  assert.equal(fs.existsSync(missing), false);
});
