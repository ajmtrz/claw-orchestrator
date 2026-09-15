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
import * as verifier from './verify.mjs';

test('G6 requires one isolated report per fixed file without missing or duplicated shards', () => {
  const names = [
    'autoloop-trust-recovery-boundaries',
    'autoloop-trust-recovery-adapters',
    'autoloop-planner-tools',
    'autoloop-agent-tools',
    'agy-planner-e2e',
    'tool-registration',
    'embedded-server-launcher',
    'autoloop-documentation-contract',
  ];
  const reports = names.map((name) => ({
    testResults: [{ name: path.join(verifier.PROJECT, `src/__tests__/${name}.test.ts`) }],
  }));
  assert.equal(collector.verifySlice3ShardInventory('controls', reports), true);
  assert.throws(() => collector.verifySlice3ShardInventory('controls', reports.slice(1)), /shard inventory/);
  assert.throws(
    () => collector.verifySlice3ShardInventory('controls', [...reports.slice(0, -1), reports[0]]),
    /shard inventory/,
  );
  assert.throws(
    () => collector.verifySlice3ShardInventory('controls', [{ testResults: reports.flatMap((r) => r.testResults) }]),
    /shard inventory/,
  );
});

// G6/G7 unit fixtures are deliberately synthetic validator inputs, never gate
// evidence. A parser which trusts a label rather than native bytes fails these.
function nativeProof() {
  return {
    engine: 'codex',
    model: 'gpt-6-astra',
    invocations: [{ engine: 'codex', pid: 901, argv: ['exec', '--model', 'gpt-6-astra'] }],
    protocol: [
      { pid: 901, event: { type: 'thread.started', thread_id: 'test-thread' } },
      { pid: 901, event: { type: 'item.completed', item: { type: 'agent_message', text: 'hello' } } },
      { pid: 901, event: { type: 'turn.completed' } },
    ],
  };
}
test('G6 indexed report retains two real same-title assertions without denominator shrinking', () => {
  const report = {
    success: true,
    numFailedTests: 0,
    numFailedTestSuites: 0,
    numTotalTests: 2,
    numPassedTests: 2,
    numPendingTests: 0,
    testResults: [
      {
        name: '/source/public.test.ts',
        status: 'passed',
        assertionResults: [
          { fullName: 'same', status: 'passed', failureMessages: [] },
          { fullName: 'same', status: 'passed', failureMessages: [] },
        ],
      },
    ],
  };
  const result = verifier.inspectTestReport(Buffer.from(JSON.stringify(report)), 'vitest-json-indexed');
  assert.equal(result.errors.length, 0);
  assert.equal(result.executed.length, 2);
  assert.equal(new Set(result.executed).size, 2);
  report.testResults[0].assertionResults[1].status = 'pending';
  assert.ok(verifier.inspectTestReport(Buffer.from(JSON.stringify(report)), 'vitest-json-indexed').errors.length > 0);
});
function publicProof() {
  const publicWorkerSource = fs.readFileSync(
    path.join(project, 'src/__tests__/autoloop-trust-recovery-boundaries.test.ts'),
  );
  const publicWorker = Buffer.from(
    publicWorkerSource.toString().match(/const worker = String\.raw`([\s\S]*?)`;\n\nasync function publicCase/)?.[1] ??
      '',
  );
  assert.ok(publicWorker.length > 0, 'synthetic control proof must use the candidate-owned worker');
  const nodeTool = { path: process.execPath, sha256: digest(fs.readFileSync(process.execPath)) };
  const controlSources = [
    'src/session-manager.ts',
    'src/index.ts',
    'src/embedded-server.ts',
    'src/autoloop/dispatcher.ts',
    'src/autoloop/planner-tools.ts',
    'src/persistent-codex-session.ts',
  ].map((name) => project + name);
  const bytes = new Map([
    [
      'execution.json',
      Buffer.from(
        JSON.stringify({
          input: { surface: 'mcp', directory: '/observed', project, reply: 'conversation without a control' },
          exit: { code: 0, signal: null },
          timedOut: false,
          started: '2026-09-14T00:00:00Z',
          ended: '2026-09-14T00:00:01Z',
          argv: ['rtk', 'proxy', process.execPath, '--import', 'tsx', '--input-type=module', '-'],
          fixture_sha256: digest('fixture'),
          worker_sha256: digest(publicWorker),
          stdout_sha256: digest(''),
          stderr_sha256: digest(''),
          source_inputs: controlSources.map((source) => ({ path: source, sha256: digest('source:' + source) })),
        }),
      ),
    ],
    ['codex.mjs', Buffer.from('fixture')],
    ['worker.mjs', publicWorker],
    ['stdout.txt', Buffer.from('')],
    ['stderr.txt', Buffer.from('')],
    ['outcome.json', Buffer.from(JSON.stringify({ response: { ok: true, reply: 'conversation without a control' } }))],
  ]);
  return {
    bytes,
    context: {
      head: sha,
      sourceHashes: new Map([
        ...controlSources.map((source) => [source, digest('source:' + source)]),
        [project + 'src/__tests__/fixtures/autoloop-trust-recovery/codex.mjs', digest('fixture')],
        [project + 'src/__tests__/autoloop-trust-recovery-boundaries.test.ts', digest(publicWorkerSource)],
      ]),
      requiredSources: [project + 'src/index.ts'],
      nodeTool,
    },
  };
}
test('G6 validates a captured public conversational compatibility outcome', () => {
  const { bytes, context } = publicProof();
  assert.equal(
    verifier.verifySlice3RawCase(
      'controls',
      (name) => {
        if (!bytes.has(name)) throw new Error('missing');
        return bytes.get(name);
      },
      context,
    ),
    'mcp:conversation',
  );
});
for (const [name, mutate] of [
  ['missing outcome', (p) => p.bytes.delete('outcome.json')],
  ['changed stdout hash', (p) => p.bytes.set('stdout.txt', Buffer.from('changed'))],
  [
    'hidden process exit',
    (p) => {
      const e = JSON.parse(p.bytes.get('execution.json'));
      e.exit.code = 23;
      p.bytes.set('execution.json', Buffer.from(JSON.stringify(e)));
    },
  ],
  [
    'unbound public source',
    (p) => {
      p.context.sourceHashes.set(project + 'src/index.ts', digest('other'));
    },
  ],
  ['Coder authored Reviewer claim', (p) => p.bytes.set('outcome.json', Buffer.from('{"reviewer":"ADVANCE"}'))],
  [
    'NVM Node substitution',
    (p) => {
      const e = JSON.parse(p.bytes.get('execution.json'));
      e.argv[2] = '/home/openclaw/.nvm/versions/node/v26.7.0/bin/node';
      p.bytes.set('execution.json', Buffer.from(JSON.stringify(e)));
    },
  ],
  [
    'shell worker substitution',
    (p) => {
      const e = JSON.parse(p.bytes.get('execution.json'));
      e.argv = ['/bin/sh', '-c', 'true'];
      p.bytes.set('execution.json', Buffer.from(JSON.stringify(e)));
    },
  ],
])
  test(`G6 rejects ${name}`, () => {
    const p = publicProof();
    mutate(p);
    assert.throws(
      () =>
        verifier.verifySlice3RawCase(
          'controls',
          (n) => {
            if (!p.bytes.has(n)) throw new Error('missing');
            return p.bytes.get(n);
          },
          p.context,
        ),
      /Slice3 proof|missing/,
    );
  });
test('G7 accepts observed native model and complete terminal protocol', () => {
  assert.equal(verifier.verifyNativeProtocol(nativeProof()).engine, 'codex');
});
test('G7 recovery requires an explicit spawn phase observation', () => {
  const proof = nativeProof();
  assert.throws(
    () =>
      verifier.verifyNativeProtocol({
        ...proof,
        childExits: [{ engine: 'codex', pid: 901, argv: proof.invocations[0].argv, code: 0, signal: null }],
        completedRecovery: true,
      }),
    /spawn observation/,
  );
});
test('G7 recovery accepts Claude logical turns sharing one bound physical worker', () => {
  const proof = recoveryNativeProof();
  proof.invocations.push({
    engine: 'claude',
    pid: 901,
    argv: ['exec', '--model', 'claude-haiku-4-5'],
  });
  proof.protocol.push(
    { pid: 901, event: { type: 'user', message: { content: 'second turn' } } },
    { pid: 901, event: { type: 'assistant', message: { content: [{ type: 'text', text: 'again' }] } } },
    { pid: 901, event: { type: 'result', subtype: 'success', is_error: false, result: 'again' } },
  );
  proof.witnessedWarmCleanupPids = [901];
  assert.equal(verifier.verifyNativeProtocol(proof).processes, 1);
});
test('G7 recovery rejects duplicate physical spawns for a shared Claude worker', () => {
  const proof = recoveryNativeProof();
  proof.invocations.push({
    engine: 'claude',
    pid: 901,
    argv: ['exec', '--model', 'claude-haiku-4-5'],
  });
  proof.spawnObservations.push({ ...proof.spawnObservations[0] });
  proof.witnessedWarmCleanupPids = [901];
  assert.throws(() => verifier.verifyNativeProtocol(proof), /duplicate spawn observation/);
});
test('G7 recovery permits an authenticated warm Claude process without a close observation', () => {
  assert.equal(verifier.verifyNativeProtocol(recoveryNativeProof()).processes, 1);
});
test('G7 recovery permits each independently authenticated warm Claude cleanup without a close observation', () => {
  const proof = recoveryNativeProof();
  proof.invocations.push({ engine: 'claude', pid: 902, argv: ['exec', '--model', 'claude-haiku-4-5'] });
  proof.protocol.push(
    { pid: 902, event: { type: 'system', subtype: 'init', session_id: 'other-session', model: 'claude-haiku-4-5' } },
    { pid: 902, event: { type: 'user', message: { content: 'other turn' } } },
    { pid: 902, event: { type: 'assistant', message: { content: [{ type: 'text', text: 'other' }] } } },
    { pid: 902, event: { type: 'result', subtype: 'success', is_error: false, result: 'other' } },
  );
  proof.spawnObservations.push({
    phase: 'warm',
    worker_pid: 8001,
    engine: 'claude',
    pid: 902,
    argv: ['exec', '--model', 'claude-haiku-4-5'],
  });
  proof.witnessedWarmCleanupPids = [901, 902];
  assert.equal(verifier.verifyNativeProtocol(proof).processes, 2);
});
test('G7 recovery permits an unsent warm Claude spawn while binding every logical invocation', () => {
  const proof = recoveryNativeProof();
  proof.allowUnsentStarts = true;
  proof.spawnObservations.push({
    phase: 'warm',
    worker_pid: 8001,
    engine: 'claude',
    pid: 902,
    argv: ['exec', '--model', 'claude-haiku-4-5'],
  });
  assert.equal(verifier.verifyNativeProtocol(proof).processes, 1);
});
function recoveryNativeProof() {
  return {
    engine: 'claude',
    model: 'haiku',
    invocations: [{ engine: 'claude', pid: 901, argv: ['exec', '--model', 'claude-haiku-4-5'] }],
    protocol: [
      { pid: 901, event: { type: 'system', subtype: 'init', session_id: 'test-session', model: 'claude-haiku-4-5' } },
      { pid: 901, event: { type: 'user', message: { content: 'turn' } } },
      { pid: 901, event: { type: 'assistant', message: { content: [{ type: 'text', text: 'hello' }] } } },
      { pid: 901, event: { type: 'result', subtype: 'success', is_error: false, result: 'hello' } },
    ],
    childExits: [],
    spawnObservations: [
      {
        phase: 'warm',
        worker_pid: 8001,
        engine: 'claude',
        pid: 901,
        argv: ['exec', '--model', 'claude-haiku-4-5'],
      },
    ],
    completedRecovery: true,
    witnessedWarmCleanupPids: [901],
    expectedWorkerPids: { warm: 8001 },
  };
}
for (const [name, mutate] of [
  ['relabelled warm cleanup phase', (p) => (p.spawnObservations[0].phase = 'cold')],
  ['duplicate spawn observation', (p) => p.spawnObservations.push({ ...p.spawnObservations[0] })],
  ['spawn observation PID mismatch', (p) => (p.spawnObservations[0].pid = 902)],
  ['spawn observation argv mismatch', (p) => (p.spawnObservations[0].argv = ['wrong'])],
  ['cleanup PID relabel', (p) => (p.witnessedWarmCleanupPids = [902])],
])
  test(`G7 rejects ${name}`, () => {
    const proof = recoveryNativeProof();
    proof.witnessedWarmCleanupPids = [901];
    mutate(proof);
    assert.throws(() => verifier.verifyNativeProtocol(proof), /Native proof/);
  });
for (const [name, mutate] of [
  [
    'relabelled adapter',
    (p) => {
      p.invocations[0].engine = 'agy';
    },
  ],
  [
    'missing requested model argv',
    (p) => {
      p.invocations[0].argv = ['exec'];
    },
  ],
  [
    'silently substituted model',
    (p) => {
      p.invocations[0].argv[2] = 'other';
    },
  ],
  [
    'missing terminal lifecycle',
    (p) => {
      p.protocol.pop();
    },
  ],
  [
    'unattributed protocol process',
    (p) => {
      p.protocol[2].pid = 902;
    },
  ],
  [
    'empty native protocol',
    (p) => {
      p.protocol = [];
    },
  ],
])
  test(`G7 rejects ${name}`, () => {
    const proof = nativeProof();
    mutate(proof);
    assert.throws(() => verifier.verifyNativeProtocol(proof), /Native proof/);
  });

// These deliberately synthetic documents test the validator, never recovery.
// A missing hash/identity/predicate/report check must make a negative test fail.
const project = fileURLToPath(new URL('../../', import.meta.url));
const run = 'CLAWO-AUTOLOOP-DURABLE-RECOVERY-TRUST-RECOVERY-R1';
const scratch = path.join(project, '.artifacts', run, 'evidence', 'sensitivity');

test('Slice3 launches Vitest directly under the authenticated Node executable', () => {
  // Break caught: PATH resolves npm or a Vitest worker through a different
  // Node installation than the executable hashed in inputs.json.
  const command = collector.slice3Command('adapters', '/tmp/slice3-command-contract', 0);
  assert.deepEqual(command.slice(0, 5), [
    'rtk',
    'proxy',
    process.execPath,
    path.join(project, 'node_modules/vitest/vitest.mjs'),
    'run',
  ]);
});
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
const sha = 'a'.repeat(40);
const tap =
  'TAP version 13\n# Subtest: fixture command\nok 1 - fixture command\n1..1\n# tests 1\n# suites 0\n# pass 1\n# fail 0\n# cancelled 0\n# skipped 0\n# todo 0\n';

// This immutable, real Git commit and its retained precommit evidence are the
// historical anchor approved for this run. Tests never create Git commits.
const originalCommit = '1559595d688fd91e2d9d9073c395f66d222afd83';
const originalBundle = 'evidence/candidate/legacy-1789355466078-105035dd-ce89-4fb4-a876-56d398fb81d4/bundle.json';
const artifactRoot = path.join(project, '.artifacts', run);
// Retained *diagnostic* capture exercises only semantic validator sensitivity.
// It is not an acceptance receipt and cannot satisfy verify.mjs controls/adapters.
const slice3Diagnostic = (() => {
  const candidate = path.join(artifactRoot, 'evidence/candidate');
  const fresh = fs
    .readdirSync(candidate)
    .filter((name) => name.startsWith('adapters-'))
    .map((name) => path.join(candidate, name))
    .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs)
    .find((directory) => {
      if (!fs.existsSync(path.join(directory, 'cases')) || !fs.existsSync(path.join(directory, 'inputs.json')))
        return false;
      const manifest = JSON.parse(fs.readFileSync(path.join(directory, 'inputs.json')));
      const nodeTools = manifest.tools.filter(
        (entry) => entry.path === process.execPath && path.basename(entry.path) === 'node',
      );
      if (
        nodeTools.length !== 1 ||
        !fs.statSync(nodeTools[0].path).isFile() ||
        digest(fs.readFileSync(nodeTools[0].path)) !== nodeTools[0].sha256
      )
        return false;
      const cases = fs
        .readdirSync(path.join(directory, 'cases'))
        .filter((name) => fs.existsSync(path.join(directory, 'cases', name, 'native-spawns.jsonl')));
      return (
        cases.length > 0 &&
        cases.every((name) => {
          const execution = JSON.parse(fs.readFileSync(path.join(directory, 'cases', name, 'execution.json')));
          return execution.argv?.[2] === nodeTools[0].path;
        })
      );
    });
  assert.ok(fresh, 'fresh authenticated G7 phase-ledger capture is required');
  return fresh;
})();
function rawReviewFixture(
  select = (e) => e.input.engine === 'codex' && e.input.scenario === 'review' && e.input.boundary === 'before-send',
) {
  const manifest = JSON.parse(fs.readFileSync(path.join(slice3Diagnostic, 'inputs.json')));
  const nodeTool = manifest.tools.find(
    (entry) => entry.path === process.execPath && path.basename(entry.path) === 'node',
  );
  assert.ok(nodeTool, 'diagnostic fixture must authenticate the current Node path');
  assert.equal(digest(fs.readFileSync(nodeTool.path)), nodeTool.sha256, 'diagnostic Node bytes changed');
  // The retained capture is diagnostic audit history.  Build the portable
  // mutation fixture around the candidate-owned worker template so failures
  // below are attributable to the mutated invariant, rather than to the
  // historical capture predating a legitimate worker change.
  const adapterSourcePath = path.join(project, 'src/__tests__/autoloop-trust-recovery-adapters.test.ts');
  const adapterSource = fs.readFileSync(adapterSourcePath);
  const workerMatch = adapterSource
    .toString()
    .match(/const worker = String\.raw`([\s\S]*?)`;\n\nasync function native/);
  assert.ok(workerMatch, 'candidate adapter worker template must be extractable');
  const candidateWorker = Buffer.from(workerMatch[1]);
  const sourceHashes = new Map(manifest.tracked.concat(manifest.harness).map((x) => [x.path, x.sha256]));
  sourceHashes.set(adapterSourcePath, digest(adapterSource));
  for (const role of ['planner', 'coder', 'reviewer']) {
    const template = path.join(project, `configs/autoloop-${role}-prompt.md`);
    sourceHashes.set(template, digest(fs.readFileSync(template)));
  }
  const entries = JSON.parse(fs.readFileSync(path.join(slice3Diagnostic, 'observed-files.json')))[0].value;
  const row = entries.find((r) => {
    const e = JSON.parse(fs.readFileSync(path.join(slice3Diagnostic, r.path, 'execution.json')));
    return select(e);
  });
  const normalized = new Map();
  const warmExecution = JSON.parse(fs.readFileSync(path.join(slice3Diagnostic, row.path, 'execution.json')));
  warmExecution.worker_pid = 700001;
  warmExecution.worker_sha256 = digest(candidateWorker);
  normalized.set('execution.json', Buffer.from(JSON.stringify(warmExecution)));
  normalized.set('worker-process.json', Buffer.from(JSON.stringify({ pid: warmExecution.worker_pid })));
  normalized.set('worker.mjs', candidateWorker);
  if (warmExecution.input.boundary === 'before-send')
    normalized.set(
      'barrier.json',
      Buffer.from(JSON.stringify({ boundary: 'before-send', pid: warmExecution.worker_pid })),
    );
  if (warmExecution.input.boundary) {
    const coldExecution = JSON.parse(fs.readFileSync(path.join(slice3Diagnostic, row.path, 'cold-execution.json')));
    coldExecution.worker_pid = 700002;
    coldExecution.worker_sha256 = digest(candidateWorker);
    normalized.set('cold-execution.json', Buffer.from(JSON.stringify(coldExecution)));
    normalized.set('cold-worker-process.json', Buffer.from(JSON.stringify({ pid: coldExecution.worker_pid })));
    normalized.set('cold-worker.mjs', candidateWorker);
    // This is an explicitly synthetic, internally consistent unit fixture,
    // never an acceptance receipt.  Rebind both the observer-owned crash
    // prefix and its final extension to the portable warm/cold worker PIDs so
    // owner authentication remains meaningful after the historical worker
    // processes are gone.
    const generationPath = 'tasks/native-boundary/agent-generations.jsonl';
    const generationRows = fs
      .readFileSync(path.join(slice3Diagnostic, row.path, generationPath), 'utf8')
      .trim()
      .split('\n')
      .filter(Boolean)
      .map(JSON.parse);
    const portableOwner = (owner, pid) => owner.replace(/^session-manager:\d+:/, `session-manager:${pid}:`);
    for (const generation of generationRows)
      generation.payload.owner_instance_id = portableOwner(
        generation.payload.owner_instance_id,
        generation.payload.generation === 1 ? warmExecution.worker_pid : coldExecution.worker_pid,
      );
    const crashRows = fs
      .readFileSync(path.join(slice3Diagnostic, row.path, 'crash-agent-generations.jsonl'), 'utf8')
      .trim()
      .split('\n')
      .filter(Boolean)
      .map(JSON.parse);
    for (const generation of crashRows)
      generation.payload.owner_instance_id = portableOwner(
        generation.payload.owner_instance_id,
        warmExecution.worker_pid,
      );
    normalized.set(generationPath, Buffer.from(generationRows.map(JSON.stringify).join('\n') + '\n'));
    normalized.set('crash-agent-generations.jsonl', Buffer.from(crashRows.map(JSON.stringify).join('\n') + '\n'));
    for (const name of ['crash-decisions.jsonl', 'tasks/native-boundary/decisions.jsonl']) {
      const decisions = fs
        .readFileSync(path.join(slice3Diagnostic, row.path, name), 'utf8')
        .trim()
        .split('\n')
        .map(JSON.parse);
      for (const decision of decisions)
        if (decision.kind === 'planner_turn_control')
          decision.payload.owner_instance_id = portableOwner(
            decision.payload.owner_instance_id,
            warmExecution.worker_pid,
          );
      normalized.set(name, Buffer.from(decisions.map(JSON.stringify).join('\n') + '\n'));
    }
  }
  const spawnFile = path.join(slice3Diagnostic, row.path, 'native-spawns.jsonl');
  if (fs.existsSync(spawnFile)) {
    const spawns = fs.readFileSync(spawnFile, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
    for (const spawn of spawns) spawn.worker_pid = spawn.phase === 'cold' ? 700002 : 700001;
    if (warmExecution.input.engine === 'claude') {
      const exits = fs
        .readFileSync(path.join(slice3Diagnostic, row.path, 'native-exits.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .filter(Boolean)
        .map(JSON.parse);
      const exited = new Set(exits.map((exit) => exit.pid));
      const witnessedCleanup = new Set(warmExecution.cleaned_fixture_pids ?? []);
      // Old diagnostic capture stopped some persistent Claude children before
      // their observer callback flushed.  This portable baseline is not an
      // acceptance receipt, so normalize those old observer gaps into complete
      // successful lifecycles. Dedicated unit tests below retain the only
      // permitted missing-exit shape: an exact witnessed warm cleanup PID.
      for (const spawn of spawns.filter(
        (spawn) => spawn.phase === 'warm' && !exited.has(spawn.pid) && !witnessedCleanup.has(spawn.pid),
      ))
        exits.push({ engine: spawn.engine, pid: spawn.pid, argv: spawn.argv, code: 0, signal: null, shutdown: false });
      const spawnOrder = new Map(spawns.map((spawn, index) => [spawn.pid, index]));
      exits.sort((left, right) => spawnOrder.get(left.pid) - spawnOrder.get(right.pid));
      normalized.set('execution.json', Buffer.from(JSON.stringify(warmExecution)));
      normalized.set('native-exits.jsonl', Buffer.from(exits.map(JSON.stringify).join('\n') + '\n'));
    }
    normalized.set('native-spawns.jsonl', Buffer.from(spawns.map(JSON.stringify).join('\n') + '\n'));
  }
  return {
    read: (name) => normalized.get(name) ?? fs.readFileSync(path.join(slice3Diagnostic, row.path, name)),
    names: JSON.parse(fs.readFileSync(path.join(slice3Diagnostic, 'observed-files.json')))[1]
      .value.filter((entry) => entry.path.startsWith(row.path + '/'))
      .map((entry) => entry.path.slice(row.path.length + 1)),
    context: {
      head: 'c79c0991f28955fe7731fb643472cd7a4dff1f9e',
      sourceHashes,
      nodeTool,
    },
  };
}

// Prompt mutations preserve native spawn/invocation/exit agreement. Full bundle
// RED/GREEN additionally rehashes each containing artifact and observation.
function mutateRoleArgv(files, role, variant, onlyPid) {
  const heading = `# ${role[0].toUpperCase() + role.slice(1)} — Autoloop`;
  let changes = 0;
  for (const name of ['native.jsonl', 'native-spawns.jsonl', 'native-exits.jsonl']) {
    const rows = files.jsonl(name);
    for (const row of rows) {
      if (onlyPid !== undefined && row.pid !== onlyPid) continue;
      const position = row.argv.findIndex((arg) => arg.includes(heading));
      if (position < 0) continue;
      const original = row.argv[position];
      if (variant === 'prefix') row.argv[position] = 'UNREQUESTED PREFIX\n' + original;
      else if (variant === 'suffix') row.argv[position] += '\nUNREQUESTED EXTRA INSTRUCTION';
      else if (variant === 'body') row.argv[position] = heading + '\nREPLACED ENTIRE AUTHENTICATED ROLE BODY';
      else if (variant === 'header-only') row.argv[position] = heading;
      else if (variant === 'no-heading') row.argv[position] = 'WHOLLY SUBSTITUTED ROLE';
      else if (variant === 'missing') row.argv.splice(position - 1, 1);
      else if (variant === 'duplicate') row.argv.push('--system-prompt', 'UNREQUESTED OVERRIDE');
      else if (variant === 'duplicate-identical') row.argv.push('--system-prompt', original);
      else if (variant === 'attached') row.argv.push('--system-prompt=UNREQUESTED OVERRIDE');
      else if (variant === 'attached-replacement') row.argv.splice(position - 1, 2, '--system-prompt=' + original);
      else if (variant === 'append') row.argv.push('--append-system-prompt', 'UNREQUESTED ADDITION');
      else if (variant === 'append-attached') row.argv.push('--append-system-prompt=UNREQUESTED ADDITION');
      else if (variant === 'file-option') row.argv.push('--system-prompt-file', '/unrequested/prompt');
      else if (variant === 'extra-positional') row.argv.push(original);
      else throw new Error('unknown role argv mutation');
      changes++;
    }
    files.putJsonl(name, rows);
  }
  assert.ok(changes > 0, 'role argv mutation must change native evidence');
}

for (const scenario of ['review', 'delivery'])
  for (const boundary of ['before-send', 'after-capture']) {
    for (const variant of [
      'prefix',
      'suffix',
      'body',
      'header-only',
      'no-heading',
      'missing',
      'duplicate',
      'duplicate-identical',
      'attached',
      'attached-replacement',
      'append',
      'append-attached',
      'file-option',
    ])
      test(`Round10 repair3 rejects Claude recipient ${scenario} ${boundary} ${variant}`, () => {
        const p = rawReviewFixture(
          (e) => e.input.engine === 'claude' && e.input.scenario === scenario && e.input.boundary === boundary,
        );
        assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
        const read = changedRawRead(p, (files) =>
          mutateRoleArgv(files, scenario === 'review' ? 'reviewer' : 'coder', variant),
        );
        assert.throws(
          () => verifier.verifySlice3RawCase('adapters', read, p.context),
          /recipient.*role.*(prompt|option)/,
        );
      });
  }
for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const boundary of ['before-send', 'after-capture']) {
    const fixture = () =>
      rawReviewFixture(
        (e) => e.input.engine === engine && e.input.scenario === 'delivery' && e.input.boundary === boundary,
      );
    for (const variant of [
      'prefix',
      'suffix',
      'body',
      'header-only',
      'no-heading',
      'extra-positional',
      'duplicate',
      'attached',
      'append',
    ])
      test(`Round10 repair3 rejects source Planner ${engine} ${boundary} ${variant}`, () => {
        const p = fixture();
        assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
        const read = changedRawRead(p, (files) => mutateRoleArgv(files, 'planner', variant));
        assert.throws(
          () => verifier.verifySlice3RawCase('adapters', read, p.context),
          /Planner.*role.*(prompt|option)/,
        );
      });
    test(`Round10 repair3 rejects unauthenticated Planner template ${engine} ${boundary}`, () => {
      const p = fixture();
      p.context.sourceHashes.delete(path.join(project, 'configs/autoloop-planner-prompt.md'));
      assert.throws(
        () => verifier.verifySlice3RawCase('adapters', p.read, p.context),
        /Planner role template is not authenticated/,
      );
    });
  }
for (const scenario of ['review', 'delivery'])
  for (const variant of ['positive', 'suffix', 'no-heading', 'missing', 'duplicate', 'attached', 'append'])
    test(`Round10 repair3 Claude unsent no-exit recipient ${scenario} ${variant}`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === 'claude' && e.input.scenario === scenario && e.input.boundary === 'before-send',
      );
      const read = changedRawRead(p, (files) => {
        const calls = files.jsonl('native.jsonl');
        const spawn = files
          .jsonl('native-spawns.jsonl')
          .find((row) => row.phase === 'warm' && !calls.some((call) => call.pid === row.pid));
        assert.ok(spawn, 'unsent warm recipient');
        const warm = files.json('execution.json');
        warm.cleaned_fixture_pids = [...new Set([...warm.cleaned_fixture_pids, spawn.pid])];
        files.put('execution.json', warm);
        files.putJsonl(
          'native-exits.jsonl',
          files.jsonl('native-exits.jsonl').filter((row) => row.pid !== spawn.pid),
        );
        if (variant !== 'positive')
          mutateRoleArgv(files, scenario === 'review' ? 'reviewer' : 'coder', variant, spawn.pid);
      });
      if (variant === 'positive') assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', read, p.context));
      else
        assert.throws(
          () => verifier.verifySlice3RawCase('adapters', read, p.context),
          /recipient.*role.*(prompt|option)/,
        );
    });

// Breaks caught: a self-consistent bundle must not certify impossible Claude
// initialization, a substituted effective model/durable request digest, or a
// recovered Planner executing without the authenticated role and reply message.
for (const family of ['init', 'model', 'digest', 'planner'])
  for (const engine of family === 'init' || family === 'model' ? ['claude'] : ['codex', 'claude', 'agy', 'cursor'])
    for (const scenario of family === 'digest'
      ? ['review']
      : family === 'planner'
        ? ['delivery']
        : ['review', 'delivery'])
      for (const boundary of ['before-send', 'after-capture'])
        test(`Round11 rejects ${family} ${engine} ${scenario} ${boundary}`, () => {
          const p = rawReviewFixture(
            (e) => e.input.engine === engine && e.input.scenario === scenario && e.input.boundary === boundary,
          );
          assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
          const read = changedRawRead(p, (files) => {
            if (family === 'init') {
              const rows = files.jsonl('protocol.jsonl');
              const pid = files.jsonl('recipient.jsonl').at(-1).pid;
              const at = rows.findIndex(
                (row) => row.pid === pid && row.event.type === 'system' && row.event.subtype === 'init',
              );
              assert.ok(at >= 0);
              const [init] = rows.splice(at, 1);
              rows.push(init);
              files.putJsonl('protocol.jsonl', rows);
            } else if (family === 'model') {
              const rows = files.jsonl('protocol.jsonl');
              let changed = 0;
              for (const row of rows)
                if (row.event.type === 'system' && row.event.subtype === 'init') {
                  assert.equal(row.event.model, 'claude-haiku-4-5');
                  row.event.model = 'claude-opus-4-1';
                  changed++;
                }
              assert.ok(changed > 0);
              files.putJsonl('protocol.jsonl', rows);
            } else if (family === 'digest') {
              for (const name of ['crash-decisions.jsonl', 'tasks/native-boundary/decisions.jsonl']) {
                const rows = files.jsonl(name),
                  prepared = rows.filter((row) => row.kind === 'request_review');
                assert.equal(prepared.length, 1);
                prepared[0].payload.request_digest = '0'.repeat(64);
                files.putJsonl(name, rows);
              }
              const outcome = files.json('cold-outcome.json');
              outcome.rows = files.bytes('tasks/native-boundary/decisions.jsonl').toString();
              files.put('cold-outcome.json', outcome);
            } else {
              const planner = files
                .jsonl('native-spawns.jsonl')
                .filter((row) => row.phase === 'cold' && row.argv.some((arg) => arg.includes('# Planner — Autoloop')));
              assert.equal(planner.length, 1);
              mutateRoleArgv(files, 'planner', 'body', planner[0].pid);
            }
          });
          const expected = {
            init: /native start|initial lifecycle/,
            model: /effective model/,
            digest: /review request digest/,
            planner: /Planner.*role.*(prompt|option)/,
          };
          assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), expected[family]);
        });

for (const variant of ['before-user', 'after-result', 'missing-model', 'unsent-model'])
  test(`Round11 adjacent Claude ${variant}`, () => {
    const proof = recoveryNativeProof();
    assert.doesNotThrow(() => verifier.verifyNativeProtocol(proof));
    if (variant === 'before-user') proof.protocol.splice(1, 0, { pid: 901, event: { type: 'unexpected' } });
    else if (variant === 'after-result') proof.protocol.push({ pid: 901, event: { type: 'unexpected' } });
    else if (variant === 'missing-model') delete proof.protocol[0].event.model;
    else {
      proof.allowUnsentStarts = true;
      proof.spawnObservations.push({ ...proof.spawnObservations[0], pid: 902 });
      proof.protocol.push({
        pid: 902,
        event: { type: 'system', subtype: 'init', session_id: 'unsent', model: 'claude-opus-4-1' },
      });
    }
    assert.throws(() => verifier.verifyNativeProtocol(proof), /unconsumed.*event|effective model/);
  });
for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const boundary of ['before-send', 'after-capture'])
    test(`Round11 adjacent cold Planner message ${engine} ${boundary}`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === engine && e.input.scenario === 'delivery' && e.input.boundary === boundary,
      );
      assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
      const read = changedRawRead(p, (files) =>
        replaceRawStrings(
          files,
          [['[system] coder directive_ack iter=0:', '[system] substituted recovery message:']],
          ['native.jsonl', 'native-spawns.jsonl', 'native-exits.jsonl', 'protocol.jsonl'],
        ),
      );
      assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Planner.*role.*(prompt|option)/);
    });

function changedRawRead(p, mutate) {
  const changed = new Map();
  const files = {
    json: (name) => JSON.parse(changed.get(name) ?? p.read(name)),
    jsonl: (name) => (changed.get(name) ?? p.read(name)).toString().trim().split('\n').filter(Boolean).map(JSON.parse),
    bytes: (name) => changed.get(name) ?? p.read(name),
    put: (name, value) => changed.set(name, Buffer.from(JSON.stringify(value))),
    putJsonl: (name, value) => changed.set(name, Buffer.from(value.map(JSON.stringify).join('\n') + '\n')),
    putBytes: (name, value) => changed.set(name, Buffer.from(value)),
    names: p.names,
  };
  mutate(files);
  return (name) => changed.get(name) ?? p.read(name);
}

// Round11 F1/F2/F3: removing the independent checkpoint or preparation
// comparison must make these semantically inconsistent raw captures pass.
// The complete-bundle audit separately refreshes every enclosing hash.
const sourceDescriptor = (bytes) => ({
  bytes_base64: bytes.toString('base64'),
  bytes: bytes.length,
  sha256: digest(bytes),
});
// Regression: coordinated replacement of every observation must not turn an
// unrelated patch or prior verdict into evidence for the requested checkpoint.
for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const boundary of ['before-send', 'after-capture'])
    for (const family of boundary === 'after-capture' ? ['patch', 'prior'] : ['patch'])
      test(`Round11 provenance rejects coordinated ${family} ${engine} ${boundary}`, () => {
        const p = rawReviewFixture(
          (e) => e.input.engine === engine && e.input.scenario === 'review' && e.input.boundary === boundary,
        );
        assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
        const read = changedRawRead(p, (files) => {
          const bytes = Buffer.from(
            family === 'patch'
              ? 'diff --git a/UNRELATED b/UNRELATED\n--- a/UNRELATED\n+++ b/UNRELATED\n@@ -1 +1 @@\n-old\n+UNRELATED_CHECKPOINT\n'
              : '{"iter":999,"decision":"advance","audit_notes":"unrelated"}\n',
          );
          files.putBytes(
            family === 'patch'
              ? 'tasks/source-checkpoint/iter/2/diff.patch'
              : 'tasks/native-boundary/iter/0/verdict.json',
            bytes,
          );
          const observed = files.json('independent-source-observation.json');
          if (family === 'patch') observed.sourceArtifacts['diff.patch'] = sourceDescriptor(bytes);
          else observed.prior = sourceDescriptor(bytes);
          files.put('independent-source-observation.json', observed);
          for (const name of ['receiver-effects.jsonl', 'recipient.jsonl']) {
            const rows = files.jsonl(name);
            for (const row of rows) {
              if (family === 'patch') row.review_inspection.artifacts['diff.patch'] = sourceDescriptor(bytes);
              else row.review_inspection.prior_verdict = sourceDescriptor(bytes);
            }
            files.putJsonl(name, rows);
          }
        });
        assert.throws(
          () => verifier.verifySlice3RawCase('adapters', read, p.context),
          family === 'patch' ? /source patch.*checkpoint/ : /prior verdict.*provenance/,
        );
      });

for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const boundary of ['before-send', 'after-capture']) {
    const mutations = [
      [
        'prior-verdict',
        (files) => {
          const prior = boundary === 'after-capture' ? null : sourceDescriptor(Buffer.from('{"decision":"advance"}\n'));
          for (const name of ['receiver-effects.jsonl', 'recipient.jsonl']) {
            const rows = files.jsonl(name);
            for (const row of rows) row.review_inspection.prior_verdict = prior;
            files.putJsonl(name, rows);
          }
        },
      ],
      [
        'joint-source-substitution',
        (files) => {
          const bytes = Buffer.from('{"goal":"unrelated checkpoint"}\n');
          files.putBytes('tasks/source-checkpoint/iter/2/directive.json', bytes);
          for (const name of ['receiver-effects.jsonl', 'recipient.jsonl']) {
            const rows = files.jsonl(name);
            for (const row of rows) row.review_inspection.artifacts['directive.json'] = sourceDescriptor(bytes);
            files.putJsonl(name, rows);
          }
        },
      ],
      [
        'source-oracle-removed',
        (files) => files.put('independent-source-observation.json', { sourceArtifacts: {}, prior: null }),
      ],
      [
        'prepared-scope-substitution',
        (files) => {
          const rows = files.json('review-preparations.json');
          rows.find((row) => row.status === 'prepared').payload.scope = ['unrequested-scope'];
          files.put('review-preparations.json', rows);
        },
      ],
    ];
    for (const artifact of ['directive.json', 'diff.patch', 'eval_output.json', 'coder_summary.txt'])
      for (const field of ['bytes_base64', 'bytes', 'sha256'])
        mutations.push([
          `source-observer-${artifact}-${field}`,
          (files) => {
            const observed = files.json('independent-source-observation.json');
            observed.sourceArtifacts[artifact][field] = field === 'bytes' ? 0 : 'substituted';
            files.put('independent-source-observation.json', observed);
          },
        ]);
    for (const field of [
      'iter',
      'checkpoint_sha',
      'source_run_id',
      'source_iter',
      'idempotency_key',
      'ledger_path',
      'prior_metrics',
    ])
      mutations.push([
        `prepared-${field}`,
        (files) => {
          const rows = files.json('review-preparations.json');
          rows.find((row) => row.status === 'prepared').payload[field] = 'substituted';
          files.put('review-preparations.json', rows);
        },
      ]);
    for (const status of ['prepared', 'duplicate']) {
      for (const field of ['target', 'idempotency_key'])
        mutations.push([
          `${status}-${field}`,
          (files) => {
            const rows = files.json('review-preparations.json');
            rows.find((row) => row.status === status)[field] = 'substituted';
            files.put('review-preparations.json', rows);
          },
        ]);
      for (const action of ['drop', 'repeat'])
        mutations.push([
          `${status}-${action}`,
          (files) => {
            const rows = files.json('review-preparations.json');
            files.put(
              'review-preparations.json',
              action === 'drop'
                ? rows.filter((row) => row.status !== status)
                : [...rows, rows.find((row) => row.status === status)],
            );
          },
        ]);
    }
    mutations.push([
      'observer-prior',
      (files) => {
        const observed = files.json('independent-source-observation.json');
        observed.prior = boundary === 'after-capture' ? null : sourceDescriptor(Buffer.from('fabricated'));
        files.put('independent-source-observation.json', observed);
      },
    ]);
    mutations.push([
      'receiver-extra-artifact',
      (files) => {
        for (const name of ['receiver-effects.jsonl', 'recipient.jsonl']) {
          const rows = files.jsonl(name);
          for (const row of rows)
            row.review_inspection.artifacts['unrequested.txt'] = sourceDescriptor(Buffer.from('other'));
          files.putJsonl(name, rows);
        }
      },
    ]);
    mutations.push([
      'retained-prior-substitution',
      (files) => files.putBytes('tasks/native-boundary/iter/0/verdict.json', Buffer.from('{"decision":"advance"}\n')),
    ]);
    for (const [name, mutate] of mutations)
      test(`Round11 F1F3 rejects ${engine} review ${boundary} ${name}`, () => {
        const p = rawReviewFixture(
          (e) => e.input.engine === engine && e.input.scenario === 'review' && e.input.boundary === boundary,
        );
        assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
        const read = changedRawRead(p, mutate);
        assert.throws(
          () => verifier.verifySlice3RawCase('adapters', read, p.context),
          /Slice3 proof: (independent source|prior verdict|review preparation|receiver source)/,
        );
      });
    for (const name of ['independent-source-observation.json', 'review-preparations.json'])
      test(`Round11 F1F3 rejects ${engine} review ${boundary} absent ${name}`, () => {
        const p = rawReviewFixture(
          (e) => e.input.engine === engine && e.input.scenario === 'review' && e.input.boundary === boundary,
        );
        const read = (file) => {
          if (file === name) throw Object.assign(new Error(`absent ${name}`), { code: 'ENOENT' });
          return p.read(file);
        };
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), { message: `absent ${name}` });
      });
  }

// Consistently mutate the entire transport chain, including JSON nested inside
// protocol and outcome strings. Expectations remain the independently captured
// logical message and source Planner reply, not a hash of the changed prompt.
function replaceRawStrings(files, pairs, names = files.names) {
  const deep = (value) => {
    if (typeof value === 'string') {
      if (value.startsWith('{') || value.startsWith('[')) {
        try {
          const parsed = JSON.parse(value),
            changed = deep(parsed);
          return JSON.stringify(changed) === JSON.stringify(parsed) ? value : JSON.stringify(changed);
        } catch {
          try {
            const lines = value.trimEnd().split('\n');
            lines.forEach((line) => JSON.parse(line));
            return lines.map(deep).join('\n') + (value.endsWith('\n') ? '\n' : '');
          } catch {}
        }
      }
      let text = value;
      for (const [before, after] of pairs) text = text.replaceAll(before, after);
      return text;
    }
    if (Array.isArray(value)) return value.map(deep);
    if (value && typeof value === 'object')
      return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, deep(entry)]));
    return value;
  };
  for (const name of names) {
    if (name.endsWith('.json')) {
      let value;
      try {
        value = files.json(name);
      } catch (error) {
        // A review sandbox also contains unchanged repository JSONC configs.
        if (error instanceof SyntaxError) continue;
        throw error;
      }
      const changed = deep(value);
      if (JSON.stringify(changed) !== JSON.stringify(value)) files.put(name, changed);
    } else if (name.endsWith('.jsonl')) {
      const rows = files.jsonl(name),
        changed = rows.map(deep);
      if (JSON.stringify(changed) !== JSON.stringify(rows)) files.putJsonl(name, changed);
    }
  }
}

for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const scenario of ['review', 'delivery'])
    for (const boundary of ['before-send', 'after-capture']) {
      const fixture = () =>
        rawReviewFixture(
          (e) => e.input.engine === engine && e.input.scenario === scenario && e.input.boundary === boundary,
        );
      for (const chronology of ['before warm start', 'overlapping warm execution'])
        test(`Round10 repair2 rejects ${engine} ${scenario} ${boundary} cold ${chronology}`, () => {
          const p = fixture();
          assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
          const read = changedRawRead(p, (files) => {
            const cold = files.json('cold-execution.json'),
              warm = files.json('execution.json');
            cold.started = new Date(
              Date.parse(chronology === 'before warm start' ? warm.started : warm.ended) - 1,
            ).toISOString();
            files.put('cold-execution.json', cold);
          });
          assert.throws(
            () => verifier.verifySlice3RawCase('adapters', read, p.context),
            /warm\/cold execution chronology/,
          );
        });
      test(`Round10 repair2 accepts ${engine} ${scenario} ${boundary} adjacent execution intervals`, () => {
        const p = fixture();
        const read = changedRawRead(p, (files) => {
          const cold = files.json('cold-execution.json');
          cold.started = files.json('execution.json').ended;
          files.put('cold-execution.json', cold);
        });
        assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', read, p.context));
      });
      for (const alteration of ['suffix', 'prefix', 'message body'])
        test(`Round10 repair2 rejects ${engine} ${scenario} ${boundary} consistently rebound prompt ${alteration}`, () => {
          const p = fixture();
          assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
          const read = changedRawRead(p, (files) => {
            const intent = files.jsonl('crash-decisions.jsonl').find((row) => row.delivery_id);
            const before = intent.payload.prompt,
              oldDigest = intent.payload_sha256;
            intent.payload.prompt =
              alteration === 'suffix'
                ? before + '\nUNREQUESTED EXTRA INSTRUCTION'
                : alteration === 'prefix'
                  ? 'UNREQUESTED PREFIX\n' + before
                  : before.replace(
                      scenario === 'review' ? '[review_request iter=1]' : '[directive iter=0]',
                      'UNREQUESTED BODY\n' + (scenario === 'review' ? '[review_request iter=1]' : '[directive iter=0]'),
                    );
            replaceRawStrings(files, [
              [before, intent.payload.prompt],
              [oldDigest, digest(JSON.stringify(intent.payload))],
            ]);
          });
          assert.throws(
            () => verifier.verifySlice3RawCase('adapters', read, p.context),
            /transport prompt differs from original logical message/,
          );
        });
      test(`Round10 repair2 rejects ${engine} ${scenario} ${boundary} unauthenticated role template`, () => {
        const p = fixture();
        p.context.sourceHashes.delete(
          path.join(project, `configs/autoloop-${scenario === 'review' ? 'reviewer' : 'coder'}-prompt.md`),
        );
        assert.throws(
          () => verifier.verifySlice3RawCase('adapters', p.read, p.context),
          /role template is not authenticated/,
        );
      });
      if (scenario === 'delivery') {
        for (const fields of [['owner_instance_id'], ['session_id'], ['owner_instance_id', 'session_id']])
          test(`Round10 repair2 rejects ${engine} ${boundary} foreign prepared ${fields.join('+')}`, () => {
            const p = fixture();
            assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
            const read = changedRawRead(p, (files) => {
              for (const name of ['crash-decisions.jsonl', 'tasks/native-boundary/decisions.jsonl']) {
                const rows = files.jsonl(name);
                for (const key of fields)
                  rows[0].payload[key] = key === 'session_id' ? 'foreign-session' : 'session-manager:1234:foreign';
                files.putJsonl(name, rows);
              }
            });
            assert.throws(
              () => verifier.verifySlice3RawCase('adapters', read, p.context),
              /prepared control source Planner generation/,
            );
          });
        test(`Round10 repair2 rejects ${engine} ${boundary} substituted source Planner reply`, () => {
          const p = fixture();
          assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
          const read = changedRawRead(p, (files) => {
            const reply = files.json('execution.json').input.reply;
            replaceRawStrings(files, [[reply, reply + '\nUNREQUESTED PLANNER PROSE']], ['protocol.jsonl']);
          });
          assert.throws(
            () => verifier.verifySlice3RawCase('adapters', read, p.context),
            /source Planner response differs/,
          );
        });
      }
    }
for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const scenario of ['review', 'delivery'])
    test(`Round10 repair2 rejects ${engine} ${scenario} attached model override with rebound spawn and exit`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === engine && e.input.scenario === scenario && e.input.boundary === 'before-send',
      );
      assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
      const read = changedRawRead(p, (files) => {
        for (const name of ['native.jsonl', 'native-spawns.jsonl', 'native-exits.jsonl']) {
          const rows = files.jsonl(name);
          for (const row of rows) row.argv.push('--model=wrong-model');
          files.putJsonl(name, rows);
        }
      });
      assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /missing model argv/);
    });
for (const scenario of ['review', 'delivery'])
  for (const variant of ['wrong', 'missing', 'duplicate', 'attached'])
    test(`Round10 repair2 rejects Claude ${scenario} unsent no-exit ${variant} model`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === 'claude' && e.input.scenario === scenario && e.input.boundary === 'before-send',
      );
      const read = changedRawRead(p, (files) => {
        const calls = files.jsonl('native.jsonl'),
          spawns = files.jsonl('native-spawns.jsonl');
        const unsent = spawns.find((spawn) => !calls.some((call) => call.pid === spawn.pid));
        assert.ok(unsent);
        const warm = files.json('execution.json');
        warm.cleaned_fixture_pids = [...new Set([...warm.cleaned_fixture_pids, unsent.pid])];
        files.put('execution.json', warm);
        files.putJsonl(
          'native-exits.jsonl',
          files.jsonl('native-exits.jsonl').filter((exit) => exit.pid !== unsent.pid),
        );
        const positive = changedRawRead(p, (positiveFiles) => {
          positiveFiles.put('execution.json', warm);
          positiveFiles.putJsonl('native-exits.jsonl', files.jsonl('native-exits.jsonl'));
        });
        assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', positive, p.context));
        if (variant === 'wrong') unsent.argv[unsent.argv.indexOf('--model') + 1] = 'wrong-model';
        else if (variant === 'missing') unsent.argv.splice(unsent.argv.indexOf('--model'), 2);
        else if (variant === 'attached') unsent.argv.push('--model=wrong-model');
        else unsent.argv.push('--model', 'wrong-model');
        files.putJsonl('native-spawns.jsonl', spawns);
      });
      assert.throws(
        () => verifier.verifySlice3RawCase('adapters', read, p.context),
        variant === 'wrong' ? /substituted spawn model/ : /missing spawn model argv/,
      );
    });

// Round 10 derives 24 mutation shapes from the four accepted Round-9 HOLD
// families; the prior review did not preserve individual variant identifiers.
// Mutate both crash and final prefixes where applicable so rejection must be
// semantic, not merely an unrehashed or byte-inconsistent artifact check.
const round10DecisionPath = 'tasks/native-boundary/decisions.jsonl';
function round10Decisions(files, mutate) {
  for (const name of ['crash-decisions.jsonl', round10DecisionPath]) {
    const rows = files.jsonl(name);
    mutate(rows);
    files.putJsonl(name, rows);
  }
}
const round10Mutations = [
  ...['appended unobserved native start', 'duplicated unobserved native start'].map((name, index) => ({
    name,
    boundary: 'before-send',
    mutate(files) {
      const rows = files.jsonl('protocol.jsonl');
      const start = rows.find(
        (row) => row.event.type === 'thread.started' || row.event.type === 'system' || row.event.event === 'init',
      );
      assert.ok(start);
      const extra = { ...structuredClone(start), pid: 987654321 };
      rows.push(extra);
      if (index) rows.push(structuredClone(extra));
      files.putJsonl('protocol.jsonl', rows);
    },
  })),
  ...[
    ['original intent role substitution', 'target_role', 'planner'],
    ['original intent generation substitution', 'target_generation', 999],
    ['joint intent and rebind kind substitution', 'kind', 'unrelated_delivery'],
  ].map(([name, key, value]) => ({
    name,
    mutate(files) {
      round10Decisions(files, (rows) => {
        for (const row of rows)
          if (
            (row.delivery_id && row.payload && !row.record_type) ||
            (key === 'kind' && row.record_type === 'delivery_generation_rebind')
          )
            row[key] = value;
      });
    },
  })),
  ...[
    ['original message ID substitution', 'msg_id', 'different-message'],
    ['original message timestamp substitution', 'ts', '2001-01-01T00:00:00.000Z'],
    ['original message sender substitution', 'from', 'reviewer'],
  ].map(([name, key, value]) => ({
    name,
    mutate(files) {
      const message = files.json('message-A.json');
      message[key] = value;
      files.put('message-A.json', message);
    },
  })),
  ...[
    ['result delivery ID substitution', 'delivery_id', 'different-delivery'],
    ['result payload digest substitution', 'payload_sha256', '0'.repeat(64)],
    ['result kind substitution', 'result_kind', 'iter_complete'],
    ['result payload substitution', 'result_payload', { understood: false, clarification: 'invented native reply' }],
  ].map(([name, key, value]) => ({
    name,
    scenario: 'delivery',
    mutate(files) {
      const rows = files.jsonl(round10DecisionPath);
      rows.find((row) => row.record_type === 'delivery_result')[key] = value;
      files.putJsonl(round10DecisionPath, rows);
    },
  })),
  ...[
    [
      'unknown post-terminal decision',
      (rows) => rows.push({ ts: rows.at(-1).ts, actor: 'planner', kind: 'unrecognized', payload: {} }),
    ],
    ['valid decision appended after termination', (rows) => rows.push(structuredClone(rows[0]))],
    ['duplicate termination', (rows) => rows.push(structuredClone(rows.at(-1)))],
    [
      'termination actor substitution',
      (rows) => {
        rows.at(-1).actor = 'coder';
      },
    ],
    [
      'prepared decision actor substitution',
      (rows) => {
        rows[0].actor = 'coder';
      },
    ],
    [
      'prepared decision unsupported schema',
      (rows) => {
        rows[0].schema_version = 2;
      },
    ],
  ].map(([name, mutate]) => ({
    name,
    mutate(files) {
      if (name.startsWith('prepared')) round10Decisions(files, mutate);
      else {
        const rows = files.jsonl(round10DecisionPath);
        mutate(rows);
        files.putJsonl(round10DecisionPath, rows);
      }
    },
  })),
  ...[
    [
      'ACK timestamp before intent',
      (rows) => {
        rows.find((row) => row.acknowledged_at).acknowledged_at = '2001-01-01T00:00:00.000Z';
      },
    ],
    [
      'rebind timestamp before intent',
      (rows) => {
        rows.find((row) => row.record_type === 'delivery_generation_rebind').rebound_at = '2001-01-01T00:00:00.000Z';
      },
    ],
    [
      'termination timestamp before ACK',
      (rows) => {
        rows.at(-1).ts = '2001-01-01T00:00:00.000Z';
      },
    ],
    [
      'prepared timestamp after intent',
      (rows) => {
        rows[0].ts = '2099-01-01T00:00:00.000Z';
      },
    ],
  ].map(([name, mutate]) => ({
    name,
    mutate(files) {
      if (name.startsWith('prepared')) round10Decisions(files, mutate);
      else {
        const rows = files.jsonl(round10DecisionPath);
        mutate(rows);
        files.putJsonl(round10DecisionPath, rows);
      }
    },
  })),
  {
    name: 'successor timestamps before predecessor release',
    mutate(files) {
      const name = 'tasks/native-boundary/agent-generations.jsonl';
      const rows = files.jsonl(name);
      for (const row of rows.filter((row) => row.payload.generation === 2)) {
        row.ts = '2001-01-01T00:00:00.000Z';
        for (const key of ['created_at', 'last_activity_at', 'lease_expires_at']) row.payload[key] = row.ts;
      }
      files.putJsonl(name, rows);
    },
  },
  {
    name: 'intent timestamp before original message',
    mutate(files) {
      round10Decisions(files, (rows) => {
        rows.find((row) => row.delivery_id && row.payload && !row.record_type).created_at = new Date(
          Date.parse(files.json('message-A.json').ts) - 1,
        ).toISOString();
      });
    },
  },
];
for (const [scenario, key, value] of [
  ...['control_id', 'dispatch_id', 'message_id', 'owner_instance_id', 'session_id'].map((key) => ['delivery', key, 0]),
  ['delivery', 'iter', '0'],
  ['delivery', 'generation', '1'],
  ['review', 'request_digest', 42],
])
  test(`Round10 supplemental rejects ${scenario} prepared ${key} invalid type`, () => {
    const p = rawReviewFixture(
      (e) => e.input.engine === 'codex' && e.input.scenario === scenario && e.input.boundary === 'before-send',
    );
    assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
    const read = changedRawRead(p, (files) => {
      for (const name of ['crash-decisions.jsonl', 'tasks/native-boundary/decisions.jsonl']) {
        const rows = files.jsonl(name);
        rows[0].payload[key] = value;
        files.putJsonl(name, rows);
      }
    });
    assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
  });
for (const scenario of ['review', 'delivery'])
  for (const variant of ['extra observer-linked unsent start', 'unsent spawn model substitution'])
    test(`Round10 supplemental rejects Claude ${scenario} ${variant}`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === 'claude' && e.input.scenario === scenario && e.input.boundary === 'before-send',
      );
      assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
      const read = changedRawRead(p, (files) => {
        const calls = files.jsonl('native.jsonl');
        const spawns = files.jsonl('native-spawns.jsonl');
        const unsent = spawns.find((spawn) => !calls.some((call) => call.pid === spawn.pid));
        assert.ok(unsent);
        if (variant.startsWith('extra')) {
          const protocol = files.jsonl('protocol.jsonl');
          spawns.push({ ...structuredClone(unsent), pid: 987654322 });
          const start = protocol.find((row) => row.pid === unsent.pid);
          protocol.push({ ...structuredClone(start), pid: 987654322 });
          files.putJsonl('protocol.jsonl', protocol);
        } else unsent.argv[unsent.argv.indexOf('--model') + 1] = 'different-model';
        files.putJsonl('native-spawns.jsonl', spawns);
      });
      assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Native proof/);
    });
assert.equal(round10Mutations.length, 24);
for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const scenario of ['review', 'delivery'])
    for (const boundary of ['before-send', 'after-capture'])
      for (const mutation of round10Mutations) {
        if (
          (mutation.scenario && mutation.scenario !== scenario) ||
          (mutation.boundary && mutation.boundary !== boundary)
        )
          continue;
        test(`Round10 rejects ${engine} ${scenario} ${boundary}: ${mutation.name}`, () => {
          const p = rawReviewFixture(
            (e) => e.input.engine === engine && e.input.scenario === scenario && e.input.boundary === boundary,
          );
          assert.doesNotThrow(() => verifier.verifySlice3RawCase('adapters', p.read, p.context));
          const read = changedRawRead(p, mutation.mutate);
          assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof|Native proof/);
        });
      }

for (const engine of ['codex', 'claude', 'agy', 'cursor']) {
  for (const boundary of ['before-send', 'after-capture'])
    for (const defect of ['terminal', 'missing exit', 'changed exit'])
      test(`G7 rejects ${engine} ${boundary} cold ${defect}`, () => {
        const p = rawReviewFixture(
          (e) => e.input.engine === engine && e.input.scenario === 'review' && e.input.boundary === boundary,
        );
        assert.throws(() =>
          verifier.verifySlice3RawCase(
            'adapters',
            (name) => {
              const bytes = p.read(name);
              if (name !== (defect === 'terminal' ? 'protocol.jsonl' : 'native-exits.jsonl')) return bytes;
              const rows = bytes.toString().trim().split('\n').map(JSON.parse);
              if (defect === 'missing exit') rows.pop();
              else if (defect === 'changed exit') {
                rows.at(-1).code = 23;
                rows.at(-1).signal = null;
              } else {
                const e = rows
                  .map((r) => r.event)
                  .findLast((e) => e.type === 'turn.completed' || e.type === 'result' || e.event === 'result');
                if (engine === 'codex') e.type = 'turn.failed';
                else if (engine === 'agy') e.result.status = 'ERROR';
                else {
                  e.is_error = true;
                  e.subtype = 'error_during_execution';
                }
              }
              return Buffer.from(rows.map(JSON.stringify).join('\n') + '\n');
            },
            p.context,
          ),
        );
      });
  for (const suffix of ['\n```autoloop\n{broken\n```', '\nVISIBLE TRAILING PROSE'])
    test(`G7 rejects ${engine} unbound control suffix ${suffix}`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === engine && !e.input.boundary && e.input.modes?.join() === 'success',
      );
      assert.throws(() =>
        verifier.verifySlice3RawCase(
          'adapters',
          (name) => {
            const bytes = p.read(name);
            if (name !== 'protocol.jsonl') return bytes;
            const rows = bytes.toString().trim().split('\n').map(JSON.parse);
            for (const { event: e } of rows) {
              if (e.item?.type === 'agent_message') e.item.text += suffix;
              if (e.message?.role === 'assistant')
                for (const c of e.message.content) if (c.type === 'text') c.text += suffix;
              if (typeof e.result === 'string') e.result += suffix;
              if (e.result?.response !== undefined) e.result.response += suffix;
            }
            return Buffer.from(rows.map(JSON.stringify).join('\n') + '\n');
          },
          p.context,
        ),
      );
    });
  test(`G7 accepts ${engine} exactly surfaced trailing prose with persisted plan`, () => {
    const p = rawReviewFixture(
      (e) => e.input.engine === engine && !e.input.boundary && e.input.modes?.join() === 'success',
    );
    const read = (name) => {
      const bytes = p.read(name);
      if (name === 'outcome.json') {
        const row = JSON.parse(bytes);
        row.replies = ['VISIBLE TRAILING PROSE'];
        return Buffer.from(JSON.stringify(row));
      }
      if (name !== 'protocol.jsonl') return bytes;
      const rows = bytes.toString().trim().split('\n').map(JSON.parse);
      for (const { event: e } of rows) {
        if (e.item?.type === 'agent_message') e.item.text += '\nVISIBLE TRAILING PROSE';
        if (e.message?.role === 'assistant')
          for (const c of e.message.content) if (c.type === 'text') c.text += '\nVISIBLE TRAILING PROSE';
        if (typeof e.result === 'string') e.result += '\nVISIBLE TRAILING PROSE';
        if (e.result?.response !== undefined) e.result.response += '\nVISIBLE TRAILING PROSE';
      }
      return Buffer.from(rows.map(JSON.stringify).join('\n') + '\n');
    };
    assert.equal(typeof verifier.verifySlice3RawCase('adapters', read, p.context), 'string');
  });
  for (const mode of ['success', 'process'])
    test(`G7 accepts retained ${engine} control/process ${mode}`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === engine && !e.input.boundary && e.input.modes?.join() === mode,
      );
      assert.equal(typeof verifier.verifySlice3RawCase('adapters', p.read, p.context), 'string');
    });
  for (const defect of ['missing', 'code', 'signal', 'pid', 'engine', 'argv'])
    test(`G7 rejects ${engine} child exit ${defect}`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === engine && !e.input.boundary && e.input.modes?.join() === 'success,success',
      );
      assert.throws(
        () =>
          verifier.verifySlice3RawCase(
            'adapters',
            (name) => {
              const b = p.read(name);
              if (name !== 'native-exits.jsonl') return b;
              const rows = b.toString().trim().split('\n').map(JSON.parse);
              if (defect === 'missing') rows.pop();
              else if (defect === 'code') rows[0].code = 23;
              else if (defect === 'signal') rows[0].signal = 'SIGKILL';
              else if (defect === 'pid') rows[0].pid = 1;
              else if (defect === 'engine') rows[0].engine = 'wrong';
              else rows[0].argv = ['wrong'];
              return Buffer.from(rows.map(JSON.stringify).join('\n') + '\n');
            },
            p.context,
          ),
        /Native proof|Slice3 proof/,
      );
    });
  test(`G7 accepts genuine ${engine} empty response as failure`, () => {
    const p = rawReviewFixture(
      (e) => e.input.engine === engine && !e.input.boundary && e.input.modes?.join() === 'empty',
    );
    assert.equal(typeof verifier.verifySlice3RawCase('adapters', p.read, p.context), 'string');
  });
  test(`G7 accepts witnessed ${engine} after-capture interruption before native completion`, () => {
    const p = rawReviewFixture(
      (e) => e.input.engine === engine && e.input.scenario === 'review' && e.input.boundary === 'after-capture',
    );
    assert.equal(verifier.verifySlice3RawCase('adapters', p.read, p.context), `${engine}:review:after-capture`);
    for (const defect of ['barrier PID', 'barrier boundary', 'crash signal']) {
      assert.throws(
        () =>
          verifier.verifySlice3RawCase(
            'adapters',
            (name) => {
              const bytes = p.read(name);
              if (name === 'barrier.json') {
                const row = JSON.parse(bytes);
                if (defect === 'barrier PID') row.pid = 1;
                if (defect === 'barrier boundary') row.boundary = 'ordinary';
                return Buffer.from(JSON.stringify(row));
              }
              if (name === 'execution.json' && defect === 'crash signal') {
                const row = JSON.parse(bytes);
                row.signal = null;
                return Buffer.from(JSON.stringify(row));
              }
              return bytes;
            },
            p.context,
          ),
        /Slice3 proof|Native proof/,
      );
    }
  });
  for (const defect of [
    'second terminal',
    'continuation identity',
    'failed second terminal',
    'empty second response',
  ]) {
    test(`G7 rejects real ${engine} multi-turn ${defect}`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === engine && !e.input.boundary && e.input.modes?.join() === 'success,success',
      );
      assert.equal(typeof verifier.verifySlice3RawCase('adapters', p.read, p.context), 'string');
      assert.throws(
        () =>
          verifier.verifySlice3RawCase(
            'adapters',
            (name) => {
              const bytes = p.read(name);
              if (name !== 'protocol.jsonl') return bytes;
              const rows = bytes.toString().trim().split('\n').map(JSON.parse);
              const ends = rows
                .map((r, i) => [r, i])
                .filter(
                  ([r]) => r.event.type === 'turn.completed' || r.event.type === 'result' || r.event.event === 'result',
                );
              if (defect === 'second terminal') rows.splice(ends[1][1], 1);
              else if (defect === 'failed second terminal') {
                const e = ends[1][0].event;
                if (engine === 'codex') e.type = 'turn.failed';
                else if (engine === 'agy') e.result.status = 'ERROR';
                else {
                  e.is_error = true;
                  e.subtype = 'error_during_execution';
                }
              } else if (defect === 'empty second response') {
                const from = ends[0][1] + 1;
                for (const { event: e } of rows.slice(from)) {
                  if (e.item?.type === 'agent_message') e.item.text = '';
                  if (e.message?.role === 'assistant') e.message.content = [{ type: 'text', text: '' }];
                  if (typeof e.result === 'string') e.result = '';
                  if (e.result?.response !== undefined) e.result.response = '';
                }
              } else {
                const r = engine === 'claude' ? ends[1][0] : rows.find((r) => r.pid !== rows[0].pid);
                const key = engine === 'codex' ? 'thread_id' : engine === 'agy' ? 'conversation_id' : 'session_id';
                r.event[key] = 'wrong-session';
              }
              return Buffer.from(rows.map(JSON.stringify).join('\n') + '\n');
            },
            p.context,
          ),
        /Native proof/,
      );
    });
  }
}
test('G7 recomputes independent review from retained diagnostic raw bytes', () => {
  const p = rawReviewFixture();
  assert.equal(verifier.verifySlice3RawCase('adapters', p.read, p.context), 'codex:review:before-send');
});
test('G7 rejects jointly substituted ACK effect and recipient digest against original intent', () => {
  const p = rawReviewFixture();
  assert.throws(
    () =>
      verifier.verifySlice3RawCase(
        'adapters',
        (name) => {
          const bytes = p.read(name);
          if (!['tasks/native-boundary/decisions.jsonl', 'receiver-effects.jsonl', 'recipient.jsonl'].includes(name))
            return bytes;
          return Buffer.from(
            bytes
              .toString()
              .trim()
              .split('\n')
              .map((line) => {
                const row = JSON.parse(line);
                if (name !== 'tasks/native-boundary/decisions.jsonl' || row.acknowledged_at)
                  row.payload_sha256 = '0'.repeat(64);
                return JSON.stringify(row);
              })
              .join('\n') + '\n',
          );
        },
        p.context,
      ),
    /Slice3 proof/,
  );
});
test('G7 rejects a cold worker importing a substituted source', () => {
  const p = rawReviewFixture();
  assert.throws(
    () =>
      verifier.verifySlice3RawCase(
        'adapters',
        (name) => {
          const bytes = p.read(name);
          if (name !== 'cold-execution.json') return bytes;
          const e = JSON.parse(bytes);
          e.source_inputs[0].sha256 = '0'.repeat(64);
          return Buffer.from(JSON.stringify(e));
        },
        p.context,
      ),
    /unbound source/,
  );
});
// Recovery is a 4-engine × review/delivery × boundary matrix.  These are
// deliberately hostile mutations of retained authentic raw bytes: each must
// be rejected by the validator, rather than trusted because a fixture label
// says it is a recovery run.
for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const scenario of ['review', 'delivery'])
    for (const boundary of ['before-send', 'after-capture'])
      for (const [defect, mutate] of [
        [
          'arbitrary warm cleanup PID cannot exempt a cold exit',
          (files) => {
            const exits = files.jsonl('native-exits.jsonl');
            // The fixture's shutdown flag is observer timing, not recovery
            // identity; append order is the retained cold reconstruction.
            const cold = exits.at(-1);
            assert.ok(cold, 'fixture must expose the cold child exit');
            const warm = files.json('execution.json');
            warm.cleaned_fixture_pids = [cold.pid];
            files.put('execution.json', warm);
            files.putJsonl(
              'native-exits.jsonl',
              exits.filter((row) => row.pid !== cold.pid),
            );
          },
        ],
        [
          'cold source inventory cannot be incomplete',
          (files) => {
            const cold = files.json('cold-execution.json');
            cold.source_inputs = cold.source_inputs.slice(0, 1);
            files.put('cold-execution.json', cold);
          },
        ],
        [
          'cold worker digest cannot be synthetic',
          (files) => {
            const cold = files.json('cold-execution.json');
            cold.worker_sha256 = '0'.repeat(64);
            files.put('cold-execution.json', cold);
          },
        ],
        [
          'cold failed or empty outcome cannot complete recovery',
          (files) => {
            const outcome = files.json('cold-outcome.json');
            outcome.outcomes = [{ ok: false }];
            outcome.replies = [''];
            files.put('cold-outcome.json', outcome);
          },
        ],
      ])
        test(`G7 rejects ${engine} ${scenario} ${boundary} ${defect}`, () => {
          const p = rawReviewFixture(
            (e) => e.input.engine === engine && e.input.scenario === scenario && e.input.boundary === boundary,
          );
          const files = {
            json: (name) => JSON.parse(p.read(name)),
            jsonl: (name) => p.read(name).toString().trim().split('\n').filter(Boolean).map(JSON.parse),
            put: (name, value) => changed.set(name, Buffer.from(JSON.stringify(value))),
            putJsonl: (name, value) => changed.set(name, Buffer.from(value.map(JSON.stringify).join('\n') + '\n')),
          };
          const changed = new Map();
          mutate(files);
          assert.throws(
            () => verifier.verifySlice3RawCase('adapters', (name) => changed.get(name) ?? p.read(name), p.context),
            /Slice3 proof|Native proof/,
          );
        });

// Round-5 adversarial mutations reproduce complete-bundle false accepts.  Each
// mutation keeps the surrounding authentic attempt bytes intact and changes
// only the evidence family named by the test.
for (const scenario of ['review', 'delivery'])
  for (const boundary of ['before-send', 'after-capture'])
    test(`G7 rejects Claude ${scenario} ${boundary} cold phase relabel plus deleted exit`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === 'claude' && e.input.scenario === scenario && e.input.boundary === boundary,
      );
      const read = changedRawRead(p, (files) => {
        const spawns = files.jsonl('native-spawns.jsonl');
        const exits = files.jsonl('native-exits.jsonl');
        const cold = spawns.find((row) => row.phase === 'cold' && exits.some((exit) => exit.pid === row.pid));
        assert.ok(cold, 'fixture must have a cold process with a retained exit');
        cold.phase = 'warm';
        files.putJsonl('native-spawns.jsonl', spawns);
        files.putJsonl(
          'native-exits.jsonl',
          exits.filter((row) => row.pid !== cold.pid),
        );
      });
      assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof|Native proof/);
    });

for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const scenario of ['review', 'delivery'])
    for (const boundary of ['before-send', 'after-capture']) {
      const select = (e) => e.input.engine === engine && e.input.scenario === scenario && e.input.boundary === boundary;
      for (const [defect, mutate] of [
        [
          'unrelated crash generation ledger',
          (files) => files.putBytes('crash-agent-generations.jsonl', Buffer.from('{"unrelated":true}\n')),
        ],
        [
          'missing predecessor orphan transition',
          (files) =>
            files.putJsonl(
              'tasks/native-boundary/agent-generations.jsonl',
              files.jsonl('tasks/native-boundary/agent-generations.jsonl').filter((row) => {
                const targetRole = scenario === 'review' ? 'reviewer' : 'coder';
                return !(row.kind === 'agent_generation_orphaned' && row.payload.role === targetRole);
              }),
            ),
        ],
        [
          'cold successor owner rewritten to warm owner',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            const targetRole = scenario === 'review' ? 'reviewer' : 'coder';
            const warmOwner = rows.find(
              (row) =>
                row.kind === 'agent_generation_started' &&
                row.payload.role === targetRole &&
                row.payload.generation === 1,
            ).payload.owner_instance_id;
            for (const row of rows)
              if (row.payload.role === targetRole && row.payload.generation === 2)
                row.payload.owner_instance_id = warmOwner;
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
        [
          'predecessor release moved after successor startup',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            const targetRole = scenario === 'review' ? 'reviewer' : 'coder';
            const releaseIndex = rows.findIndex(
              (row) =>
                row.kind === 'agent_generation_released' &&
                row.payload.role === targetRole &&
                row.payload.generation === 1,
            );
            const [release] = rows.splice(releaseIndex, 1);
            const successorStart = rows.findIndex(
              (row) =>
                row.kind === 'agent_generation_started' &&
                row.payload.role === targetRole &&
                row.payload.generation === 2,
            );
            rows.splice(successorStart + 1, 0, release);
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
      ])
        test(`G7 rejects ${engine} ${scenario} ${boundary} ${defect}`, () => {
          const p = rawReviewFixture(select);
          const read = changedRawRead(p, mutate);
          assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
        });
      test(`G7 rejects ${engine} ${scenario} ${boundary} substituted cold input descriptor`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          const cold = files.json('cold-execution.json');
          cold.input.project = '/substituted/project';
          files.put('cold-execution.json', cold);
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
      });
      test(`G7 rejects ${engine} ${scenario} ${boundary} jointly replaced worker bodies`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          const replacement = Buffer.from('// unrelated worker\n');
          const replacementHash = digest(replacement);
          for (const prefix of ['', 'cold-']) {
            files.putBytes(prefix + 'worker.mjs', replacement);
            const execution = files.json(prefix + 'execution.json');
            execution.worker_sha256 = replacementHash;
            files.put(prefix + 'execution.json', execution);
          }
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
      });
      test(`G7 rejects ${engine} ${scenario} ${boundary} malformed completed recovery fence`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          const spawns = files.jsonl('native-spawns.jsonl');
          const coldPids = new Set(spawns.filter((row) => row.phase === 'cold').map((row) => row.pid));
          const rows = files.jsonl('protocol.jsonl');
          const outcome = files.json('cold-outcome.json');
          const suffix = '\n```autoloop\n{broken\n```';
          const terminalIndex = rows.findLastIndex(
            (row) =>
              coldPids.has(row.pid) &&
              (row.event.type === 'turn.completed' || row.event.type === 'result' || row.event.event === 'result'),
          );
          assert.notEqual(terminalIndex, -1, 'fixture must expose a cold terminal');
          const terminal = rows[terminalIndex].event;
          let original;
          if (engine === 'agy') {
            original = terminal.result.response;
            terminal.result.response += suffix;
          } else if (engine === 'codex') {
            const message = rows
              .slice(0, terminalIndex)
              .findLast((row) => row.pid === rows[terminalIndex].pid && row.event.item?.type === 'agent_message');
            original = message.event.item.text;
            message.event.item.text += suffix;
          } else {
            original = terminal.result;
            terminal.result += suffix;
            const assistant = rows
              .slice(0, terminalIndex)
              .findLast((row) => row.pid === rows[terminalIndex].pid && row.event.type === 'assistant');
            for (const content of assistant.event.message.content) if (content.type === 'text') content.text += suffix;
          }
          const replyIndex = outcome.replies.lastIndexOf(original);
          assert.notEqual(replyIndex, -1, 'fixture must surface the changed cold reply');
          outcome.replies[replyIndex] += suffix;
          files.putJsonl('protocol.jsonl', rows);
          files.put('cold-outcome.json', outcome);
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof|Native proof/);
      });
      test(`G7 rejects ${engine} ${scenario} ${boundary} missing generation release`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          files.putJsonl(
            'tasks/native-boundary/agent-generations.jsonl',
            files
              .jsonl('tasks/native-boundary/agent-generations.jsonl')
              .filter((row) => row.kind !== 'agent_generation_released'),
          );
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
      });
      test(`G7 rejects ${engine} ${scenario} ${boundary} fabricated delivery generation rebind`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          const rows = files.jsonl('tasks/native-boundary/decisions.jsonl');
          const rebind = rows.find((row) => row.record_type === 'delivery_generation_rebind');
          assert.ok(rebind, 'fixture must expose a generation rebind');
          rebind.to_generation = 999;
          files.putJsonl('tasks/native-boundary/decisions.jsonl', rows);
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
      });

      for (const [record, selector] of [
        ['intent', (row) => row.payload && !row.record_type && row.delivery_id],
        ['rebind', (row) => row.record_type === 'delivery_generation_rebind'],
        ['ACK', (row) => row.acknowledged_at],
        ...(scenario === 'delivery' ? [['result', (row) => row.record_type === 'delivery_result']] : []),
      ])
        test(`G7 rejects ${engine} ${scenario} ${boundary} unsupported ${record} schema`, () => {
          const p = rawReviewFixture(select);
          const read = changedRawRead(p, (files) => {
            const rows = files.jsonl('tasks/native-boundary/decisions.jsonl');
            const row = rows.find(selector);
            assert.ok(row, `fixture must expose ${record}`);
            row.schema_version = 2;
            files.putJsonl('tasks/native-boundary/decisions.jsonl', rows);
          });
          assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
        });

      test(`G7 rejects ${engine} ${scenario} ${boundary} ACK before generation rebind`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          const rows = files.jsonl('tasks/native-boundary/decisions.jsonl');
          const ackIndex = rows.findIndex((row) => row.acknowledged_at);
          const rebindIndex = rows.findIndex((row) => row.record_type === 'delivery_generation_rebind');
          assert.ok(ackIndex > rebindIndex, 'fixture must ACK after rebind');
          const [ack] = rows.splice(ackIndex, 1);
          rows.splice(rebindIndex, 0, ack);
          files.putJsonl('tasks/native-boundary/decisions.jsonl', rows);
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
      });

      for (const [defect, mutate] of [
        [
          'substituted generation state',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            const started = rows.find((row) => row.kind === 'agent_generation_started');
            started.payload.state = 'stale';
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
        [
          'generation timestamp before creation',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            const started = rows.find((row) => row.kind === 'agent_generation_started');
            started.ts = '2000-01-01T00:00:00.000Z';
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
        [
          'extra complete generation three lifecycle',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            const role = scenario === 'review' ? 'reviewer' : 'coder';
            const generationTwo = rows.filter((row) => row.payload.role === role && row.payload.generation === 2);
            const releaseIndex = rows.findIndex(
              (row) =>
                row.kind === 'agent_generation_released' && row.payload.role === role && row.payload.generation === 2,
            );
            assert.ok(generationTwo.length === 3 && releaseIndex >= 0, 'fixture must expose complete generation two');
            const orphan = structuredClone(generationTwo.find((row) => row.kind === 'agent_generation_started'));
            orphan.kind = 'agent_generation_orphaned';
            orphan.payload.state = 'orphaned';
            const generationThree = generationTwo.map((row) => {
              const clone = structuredClone(row);
              clone.payload.generation = 3;
              clone.payload.session_id += '-extra';
              clone.payload.owner_instance_id += '-extra';
              return clone;
            });
            rows.splice(releaseIndex, 0, orphan);
            rows.splice(releaseIndex + 2, 0, ...generationThree);
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
        [
          'duplicate generation start',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            const startIndex = rows.findIndex((row) => row.kind === 'agent_generation_started');
            rows.splice(startIndex + 1, 0, structuredClone(rows[startIndex]));
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
      ])
        test(`G7 rejects ${engine} ${scenario} ${boundary} ${defect}`, () => {
          const p = rawReviewFixture(select);
          const read = changedRawRead(p, mutate);
          assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
        });

      test(`G7 rejects ${engine} ${scenario} ${boundary} duplicate native start`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          const receipts = files.jsonl('recipient.jsonl');
          const recipientPid = receipts.at(-1).pid;
          const rows = files.jsonl('protocol.jsonl');
          const startIndex = rows.findIndex(
            (row) =>
              row.pid === recipientPid &&
              (row.event.type === 'thread.started' ||
                (row.event.type === 'system' && row.event.subtype === 'init') ||
                row.event.event === 'init'),
          );
          assert.notEqual(startIndex, -1, 'fixture must expose recipient native start');
          rows.splice(startIndex + 1, 0, structuredClone(rows[startIndex]));
          files.putJsonl('protocol.jsonl', rows);
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Native proof/);
      });

      test(`G7 rejects ${engine} ${scenario} ${boundary} recipient process relabel`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          const receipts = files.jsonl('recipient.jsonl');
          const target = receipts.at(-1);
          const replacement = files
            .jsonl('native-spawns.jsonl')
            .find((row) => row.phase === 'cold' && row.pid !== target.pid);
          assert.ok(replacement, 'fixture must expose another cold native process');
          const priorPid = target.pid;
          target.pid = replacement.pid;
          files.putJsonl('recipient.jsonl', receipts);
          if (boundary === 'before-send') {
            const effects = files.jsonl('receiver-effects.jsonl');
            assert.equal(effects[0].pid, priorPid, 'fixture effect must bind the original recipient');
            effects[0].pid = replacement.pid;
            files.putJsonl('receiver-effects.jsonl', effects);
          }
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
      });

      test(`G7 rejects ${engine} ${scenario} ${boundary} recovered recipient session relabel`, () => {
        const p = rawReviewFixture(select);
        const read = changedRawRead(p, (files) => {
          const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
          const role = scenario === 'review' ? 'reviewer' : 'coder';
          const otherSession = rows.find((row) => !(row.payload.role === role && row.payload.generation === 2)).payload
            .session_id;
          for (const row of rows)
            if (row.payload.role === role && row.payload.generation === 2) row.payload.session_id = otherSession;
          files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
        });
        assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof/);
      });

      if (engine === 'claude' || engine === 'cursor') {
        for (const defect of ['terminal differs from assistant', 'missing assistant'])
          test(`G7 rejects ${engine} ${scenario} ${boundary} completed recovery ${defect}`, () => {
            const p = rawReviewFixture(select);
            const read = changedRawRead(p, (files) => {
              const recipientPid = files.jsonl('recipient.jsonl').at(-1).pid;
              const rows = files.jsonl('protocol.jsonl');
              const terminalIndex = rows.findIndex((row) => row.pid === recipientPid && row.event.type === 'result');
              assert.notEqual(terminalIndex, -1, 'fixture must expose recipient terminal');
              if (defect === 'terminal differs from assistant') rows[terminalIndex].event.result += ' altered-terminal';
              else {
                const assistantIndex = rows.findIndex(
                  (row, index) => index < terminalIndex && row.pid === recipientPid && row.event.type === 'assistant',
                );
                assert.notEqual(assistantIndex, -1, 'fixture must expose recipient assistant');
                rows.splice(assistantIndex, 1);
              }
              files.putJsonl('protocol.jsonl', rows);
            });
            assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Native proof/);
          });
      }

      for (const [defect, mutate] of [
        [
          'final decision ledger substituted for the witnessed crash cut',
          (files) => files.putBytes('crash-decisions.jsonl', files.bytes('tasks/native-boundary/decisions.jsonl')),
        ],
        [
          'final generation ledger substituted for the witnessed crash cut',
          (files) =>
            files.putBytes(
              'crash-agent-generations.jsonl',
              files.bytes('tasks/native-boundary/agent-generations.jsonl'),
            ),
        ],
        [
          'duplicate predecessor orphan transition',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            const role = scenario === 'review' ? 'reviewer' : 'coder';
            const index = rows.findIndex(
              (row) =>
                row.kind === 'agent_generation_orphaned' && row.payload.role === role && row.payload.generation === 1,
            );
            assert.notEqual(index, -1, 'fixture must expose predecessor orphaning');
            rows.splice(index + 1, 0, structuredClone(rows[index]));
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
        [
          'predecessor orphan transition before predecessor start',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            const role = scenario === 'review' ? 'reviewer' : 'coder';
            const orphanIndex = rows.findIndex(
              (row) =>
                row.kind === 'agent_generation_orphaned' && row.payload.role === role && row.payload.generation === 1,
            );
            const startIndex = rows.findIndex(
              (row) =>
                row.kind === 'agent_generation_started' && row.payload.role === role && row.payload.generation === 1,
            );
            assert.ok(orphanIndex > startIndex, 'fixture must start predecessor before orphaning it');
            const [orphan] = rows.splice(orphanIndex, 1);
            rows.splice(startIndex, 0, orphan);
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
        [
          'unsupported generation ledger schema version',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            rows[0].schema_version = 2;
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
        [
          'incomplete generation ledger row schema',
          (files) => {
            const rows = files.jsonl('tasks/native-boundary/agent-generations.jsonl');
            delete rows[0].payload.lease_expires_at;
            files.putJsonl('tasks/native-boundary/agent-generations.jsonl', rows);
          },
        ],
        [
          'post-terminal native response before the next authenticated turn',
          (files) => {
            const spawns = files.jsonl('native-spawns.jsonl');
            const coldPid = spawns.filter((row) => row.phase === 'cold').at(-1)?.pid;
            assert.ok(coldPid, 'fixture must expose a cold native process');
            const rows = files.jsonl('protocol.jsonl');
            const terminalIndex = rows.findLastIndex(
              (row) =>
                row.pid === coldPid &&
                (row.event.type === 'turn.completed' || row.event.type === 'result' || row.event.event === 'result'),
            );
            assert.notEqual(terminalIndex, -1, 'fixture must expose a cold terminal');
            const suffix = ' post-terminal-response';
            const terminal = rows[terminalIndex].event;
            if (engine === 'agy') terminal.result.response += suffix;
            else if (engine !== 'codex') terminal.result += suffix;
            rows.splice(terminalIndex + 1, 0, {
              pid: coldPid,
              event:
                engine === 'codex'
                  ? { type: 'item.completed', item: { type: 'agent_message', text: suffix } }
                  : {
                      type: 'assistant',
                      message: { role: 'assistant', content: [{ type: 'text', text: suffix }] },
                    },
            });
            const outcome = files.json('cold-outcome.json');
            outcome.replies[outcome.replies.length - 1] += suffix;
            files.putJsonl('protocol.jsonl', rows);
            files.put('cold-outcome.json', outcome);
          },
        ],
        [
          'unexpected interruption barrier field',
          (files) => {
            const barrier = files.json('barrier.json');
            barrier.unrelated = true;
            files.put('barrier.json', barrier);
          },
        ],
      ])
        test(`G7 rejects ${engine} ${scenario} ${boundary} ${defect}`, () => {
          const p = rawReviewFixture(select);
          const read = changedRawRead(p, mutate);
          assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof|Native proof/);
        });

      if (boundary === 'before-send') {
        for (const [defect, mutate] of [
          [
            'wrong before-send barrier boundary',
            (files) => {
              const barrier = files.json('barrier.json');
              barrier.boundary = 'after-capture';
              files.put('barrier.json', barrier);
            },
          ],
          [
            'unrelated before-send barrier worker PID',
            (files) => {
              const barrier = files.json('barrier.json');
              barrier.pid += 1000;
              files.put('barrier.json', barrier);
            },
          ],
          [
            'before-send barrier with fabricated delivery identity',
            (files) => {
              const barrier = files.json('barrier.json');
              barrier.delivery_id = 'fabricated';
              files.put('barrier.json', barrier);
            },
          ],
        ])
          test(`G7 rejects ${engine} ${scenario} ${defect}`, () => {
            const p = rawReviewFixture(select);
            const read = changedRawRead(p, mutate);
            assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof|Native proof/);
          });

        test(`G7 rejects ${engine} ${scenario} missing before-send barrier`, () => {
          const p = rawReviewFixture(select);
          const read = (name) => {
            if (name === 'barrier.json') throw Object.assign(new Error('missing barrier'), { code: 'ENOENT' });
            return p.read(name);
          };
          assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /missing barrier|ENOENT/);
        });
      }
    }

for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const boundary of ['before-send', 'after-capture'])
    test(`G7 rejects ${engine} Reviewer ${boundary} dropped nonempty surfaced reply`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === engine && e.input.scenario === 'review' && e.input.boundary === boundary,
      );
      const read = changedRawRead(p, (files) => {
        const outcome = files.json('cold-outcome.json');
        assert.ok(outcome.replies.length > 1, 'fixture must expose multiple surfaced replies');
        outcome.replies.pop();
        files.put('cold-outcome.json', outcome);
      });
      assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof|Native proof/);
    });

for (const engine of ['codex', 'claude', 'agy', 'cursor'])
  for (const defect of ['fabricated', 'duplicate'])
    test(`G7 rejects ${engine} ordinary ${defect} physical spawn ledger`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === engine && !e.input.boundary && e.input.modes?.join() === 'success',
      );
      const read = changedRawRead(p, (files) => {
        const spawns = files.jsonl('native-spawns.jsonl');
        if (defect === 'fabricated') {
          spawns[0].pid = 1;
          spawns[0].argv = ['/bin/sh', '-c', 'true'];
        } else spawns.push(structuredClone(spawns[0]));
        files.putJsonl('native-spawns.jsonl', spawns);
      });
      assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Slice3 proof|Native proof/);
    });

for (const scenario of ['review', 'delivery'])
  for (const boundary of ['before-send', 'after-capture'])
    test(`G7 rejects Claude ${scenario} ${boundary} unconsumed completed protocol turn`, () => {
      const p = rawReviewFixture(
        (e) => e.input.engine === 'claude' && e.input.scenario === scenario && e.input.boundary === boundary,
      );
      const read = changedRawRead(p, (files) => {
        const rows = files.jsonl('protocol.jsonl');
        const coldPid = files.jsonl('native-spawns.jsonl').find((row) => row.phase === 'cold').pid;
        rows.push(
          {
            pid: coldPid,
            event: { type: 'user', message: { role: 'user', content: [{ type: 'text', text: 'extra' }] } },
          },
          {
            pid: coldPid,
            event: { type: 'assistant', message: { role: 'assistant', content: [{ type: 'text', text: 'ignored' }] } },
          },
          {
            pid: coldPid,
            event: { type: 'result', subtype: 'error_during_execution', is_error: true, result: 'ignored' },
          },
        );
        files.putJsonl('protocol.jsonl', rows);
      });
      assert.throws(() => verifier.verifySlice3RawCase('adapters', read, p.context), /Native proof/);
    });
for (const [name, file, mutate] of [
  ['missing protocol', 'protocol.jsonl', () => Buffer.alloc(0)],
  ['torn ledger', 'tasks/native-boundary/decisions.jsonl', (b) => b.subarray(0, b.length - 1)],
  [
    'wrong requested review scope',
    'independent-caller-request.json',
    (b) => Buffer.from(b.toString().replace('durable-delivery', 'other-scope')),
  ],
  [
    'wrong checkpoint HEAD',
    'collector-repo-state.json',
    (b) => Buffer.from(b.toString().replace('c79c0991f28955fe7731fb643472cd7a4dff1f9e', 'a'.repeat(40))),
  ],
  [
    'Reviewer repository writes',
    'collector-repo-state.json',
    (b) => {
      const r = JSON.parse(b);
      r.tracked_status = ' M src/index.ts';
      return Buffer.from(JSON.stringify(r));
    },
  ],
  [
    'changed staged artifact',
    'tasks/source-checkpoint/iter/2/directive.json',
    (b) => Buffer.concat([b, Buffer.from(' ')]),
  ],
  [
    'Coder substituted for Reviewer',
    'tasks/native-boundary/agent-generations.jsonl',
    (b) => Buffer.from(b.toString().replaceAll('"role":"reviewer"', '"role":"coder"')),
  ],
])
  test(`G7 rejects raw ${name}`, () => {
    const p = rawReviewFixture();
    assert.throws(
      () => verifier.verifySlice3RawCase('adapters', (n) => (n === file ? mutate(p.read(n)) : p.read(n)), p.context),
      /Slice3 proof|Native proof/,
    );
  });
const firstCorrection = 'a0a6fdaf188d107d9eacd760984685ada813da1e';
const firstCorrectionDirectory = path.join(
  artifactRoot,
  'evidence/candidate/legacy-1789359636920-7c9e02d9-4e71-4c01-a83b-087501a6396d',
);
const firstCorrectionDigest = '8f28667ee4ee9db372d767ea08178bbd0823ac87763fff849381f907cf240c99';

const slice2Commit = 'e3e235b280a570e1bafef57be13e5fad8f6d7ce2';
const slice2Directory = path.join(
  artifactRoot,
  'evidence/candidate/legacy-1789368805539-5f6a9d04-5b17-4327-b1ce-1ff61eb7c012',
);
const slice2Digest = '7ccdcc14a115e225a3c0e43a37a79eb0c6dcecbb0b81dbea05156b966efa7f60';
// Immutable receipts are source fixtures, not new successful audit results.
// Keep structural mutation tests independent of whether their old executable
// still exists; a valid old receipt must still fail closed on executable drift.
function historicalReceipt(head) {
  const reference =
    head === slice2Commit
      ? [
          'legacy-postcommit-e6d1eadd636ef9f20e97d3b1c4b1145d5bfca374-ed9ecbfc-83d9-4d8e-acc6-1b5c9ba87495',
          'b927b7fdaf9d9b11fc494b034052213b7e29b71eac721346c10b3ebcad90985e',
        ]
      : [
          'legacy-postcommit-21ded46e6d94be1ce11d0eb8336134c4a2dea29d-3d8841cd-bb98-40f0-8e15-3c7d2ee194aa',
          '1c53338fc95b6811859cbebe726e0ae2e06a928534bb2ecdd4bbee676b8cbd79',
        ];
  const bytes = fs.readFileSync(path.join(artifactRoot, 'evidence/candidate', reference[0], 'receipt.json'));
  assert.equal(digest(bytes), reference[1], 'immutable receipt fixture changed');
  const receipt = JSON.parse(bytes);
  if (head === slice2Commit) {
    receipt.chain = receipt.chain.slice(0, 1);
    Object.assign(receipt, ...['commit', 'parent', 'tree'].map((key) => ({ [key]: receipt.chain[0][key] })));
  } else {
    receipt.candidate = receipt.history[0];
    receipt.history = [];
    Object.assign(receipt, ...['commit', 'parent', 'tree'].map((key) => ({ [key]: receipt.candidate[key] })));
  }
  assert.equal(receipt.commit, head);
  return receipt;
}
function historicalToolMismatch(directory = path.join(artifactRoot, path.dirname(originalBundle))) {
  const bundle = JSON.parse(fs.readFileSync(path.join(directory, 'bundle.json')));
  const manifest = verifier.readInputManifest(directory, bundle.input_manifest);
  for (const tool of manifest.tools) {
    assert.ok(fs.statSync(tool.path).isFile(), 'historical executable path must remain inspectable');
    if (digest(fs.readFileSync(tool.path)) !== tool.sha256) return `Historical tool bytes differ: ${tool.path}`;
  }
  return null;
}

const slice2Audit = () =>
  collector.auditLegacyFinalization(
    slice2Commit,
    slice2Directory,
    slice2Digest,
    'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d',
  );

test('authenticates the Slice 2 receipt chain or rejects historical tool drift', () => {
  const before = fs.readFileSync(path.join(slice2Directory, 'bundle.json'));
  const mismatch = historicalToolMismatch();
  if (mismatch) {
    assert.throws(slice2Audit, { message: mismatch });
    assert.deepEqual(fs.readFileSync(path.join(slice2Directory, 'bundle.json')), before);
    return;
  }
  const audit = slice2Audit();
  assert.equal(audit.result.committed_head, slice2Commit);
  assert.equal(audit.result.committed_tree, '64a2450d65b29e0eb71e5c6729bb4958c099df2f');
  assert.equal(audit.result.cases, 25);
  assert.deepEqual(fs.readFileSync(path.join(slice2Directory, 'bundle.json')), before);
  assert.equal(audit.receipt.chain[0].parent, '21ded46e6d94be1ce11d0eb8336134c4a2dea29d');
  assert.equal(audit.receipt.chain[0].files.length, 6);
});

for (const [name, mutate, pattern] of [
  [
    'head',
    (r) => {
      r.commit = firstCorrection;
    },
    /head/i,
  ],
  [
    'tree',
    (r) => {
      r.tree = '0'.repeat(40);
    },
    /tree/i,
  ],
  [
    'parent',
    (r) => {
      r.parent = firstCorrection;
    },
    /parent/i,
  ],
  [
    'base',
    (r) => {
      r.base = firstCorrection;
    },
    /base/i,
  ],
  [
    'prior receipt',
    (r) => {
      r.previous.sha256 = '0'.repeat(64);
    },
    /prior|previous/i,
  ],
  [
    'bundle',
    (r) => {
      r.candidate_bundle.sha256 = '0'.repeat(64);
    },
    /hash/i,
  ],
  [
    'missing ancestry',
    (r) => {
      r.chain = [];
    },
    /chain|ancestry/i,
  ],
  [
    'link parent',
    (r) => {
      r.chain[0].parent = firstCorrection;
    },
    /parent/i,
  ],
  [
    'patch',
    (r) => {
      r.chain[0].patch_base64 = Buffer.from('fake patch').toString('base64');
    },
    /patch/i,
  ],
  [
    'file bytes',
    (r) => {
      r.chain[0].files[0].sha256 = '0'.repeat(64);
    },
    /file/i,
  ],
]) {
  test(`rejects Slice 2 receipt ${name} substitution`, () => {
    const receipt = historicalReceipt(slice2Commit);
    mutate(receipt);
    assert.throws(() => collector.verifyReceiptData(receipt, slice2Commit), pattern);
  });
}

const retainedDelivery = path.join(artifactRoot, 'evidence/candidate/slice2-combined-final-correction-001/cases');
function deliveryCase(id) {
  return path.join(
    retainedDelivery,
    fs.readdirSync(retainedDelivery).find((name) => name.startsWith(id + '-')),
  );
}
for (const id of [
  'before-send',
  'after-capture',
  'after-ack',
  'target',
  'torn',
  'intent-fsync',
  'digest-mismatch',
  'review-checkpoint',
]) {
  test(`recomputes retained ${id} durability observations from real process files`, () => {
    assert.equal(verifier.verifyDurabilityCase(deliveryCase(id), id, slice2Commit).case_id, id);
  });
}
for (const [name, id, change] of [
  ['relabeled crash', 'before-send', (d) => {}],
  [
    'payload bytes',
    'before-send',
    (d) => {
      const f = path.join(d, 'cold/final-decisions.jsonl');
      fs.writeFileSync(
        f,
        fs.readFileSync(f, 'utf8').replace('directive A: preserve these exact bytes', 'substituted payload'),
      );
    },
  ],
  [
    'missing durable ACK',
    'before-send',
    (d) => {
      const f = path.join(d, 'cold/final-decisions.jsonl');
      fs.writeFileSync(
        f,
        fs
          .readFileSync(f, 'utf8')
          .split('\n')
          .filter((l) => !l.includes('acknowledged_at'))
          .join('\n'),
      );
    },
  ],
  [
    'changed recipient',
    'after-capture',
    (d) => {
      const f = path.join(d, 'recipient.jsonl');
      fs.writeFileSync(f, fs.readFileSync(f, 'utf8').replace('immutable A', 'mutated B'));
    },
  ],
  [
    'duplicate logical effect',
    'after-capture',
    (d) => {
      const f = path.join(d, 'receiver-effects.jsonl');
      fs.appendFileSync(f, fs.readFileSync(f));
    },
  ],
  [
    'missing death',
    'after-ack',
    (d) => {
      const f = path.join(d, 'crashed.execution.json');
      const v = JSON.parse(fs.readFileSync(f));
      v.signal = null;
      fs.writeFileSync(f, JSON.stringify(v));
    },
  ],
  [
    'missing release',
    'before-send',
    (d) => {
      const f = path.join(d, 'cold/final-generations.jsonl');
      fs.writeFileSync(
        f,
        fs
          .readFileSync(f, 'utf8')
          .split('\n')
          .filter((l) => !l.includes('agent_generation_released'))
          .join('\n'),
      );
    },
  ],
  [
    'fabricated source',
    'before-send',
    (d) => {
      const f = path.join(d, 'cold/events.jsonl');
      const rows = fs.readFileSync(f, 'utf8').trim().split('\n').map(JSON.parse);
      rows[0].value[0].sha256 = '0'.repeat(64);
      fs.writeFileSync(f, rows.map((r) => JSON.stringify(r) + '\n').join(''));
    },
  ],
  [
    'hidden failure',
    'before-send',
    (d) => {
      const f = path.join(d, 'cold.execution.json');
      const v = JSON.parse(fs.readFileSync(f));
      v.code = 2;
      fs.writeFileSync(f, JSON.stringify(v));
    },
  ],
  ['missing artifact', 'before-send', (d) => fs.unlinkSync(path.join(d, 'message-A.json'))],
  [
    'rebind target',
    'before-send',
    (d) => {
      const f = path.join(d, 'cold/final-decisions.jsonl');
      const rows = fs.readFileSync(f, 'utf8').trim().split('\n').map(JSON.parse);
      rows.find((x) => x.record_type === 'delivery_generation_rebind').to_generation = 3;
      fs.writeFileSync(f, rows.map((x) => JSON.stringify(x) + '\n').join(''));
    },
  ],
  [
    'ACK digest',
    'after-ack',
    (d) => {
      const f = path.join(d, 'cold/final-decisions.jsonl');
      const rows = fs.readFileSync(f, 'utf8').trim().split('\n').map(JSON.parse);
      rows.find((x) => x.acknowledged_at).payload_sha256 = '0'.repeat(64);
      fs.writeFileSync(f, rows.map((x) => JSON.stringify(x) + '\n').join(''));
    },
  ],
  [
    'missing sync',
    'before-send',
    (d) => {
      const f = path.join(d, 'cold/events.jsonl');
      const rows = fs
        .readFileSync(f, 'utf8')
        .trim()
        .split('\n')
        .map(JSON.parse)
        .filter((x) => x.kind !== 'fsync-return');
      fs.writeFileSync(f, rows.map((x, sequence) => JSON.stringify({ ...x, sequence }) + '\n').join(''));
    },
  ],
]) {
  test(`rejects durability ${name} even with internally refreshed references`, () => {
    fs.mkdirSync(scratch, { recursive: true });
    const d = fs.mkdtempSync(path.join(scratch, 'durability-tamper-'));
    fs.cpSync(deliveryCase(id), d, { recursive: true });
    change(d);
    assert.throws(
      () => verifier.verifyDurabilityCase(d, name === 'relabeled crash' ? 'after-capture' : id, slice2Commit),
      /durability|artifact|ENOENT/i,
    );
  });
}

test('retained delivery cannot certify a stale source subject', () => {
  assert.throws(
    () => verifier.verifyDurabilityCase(deliveryCase('before-send'), 'before-send', originalCommit),
    /durability|not in/i,
  );
});

test('the actual resume regression retains API outcomes and before/after files for G5', async () => {
  const directory = fs.mkdtempSync(path.join(scratch, 'resume-observer-'));
  fs.mkdirSync(path.join(directory, 'cases'));
  fs.writeFileSync(
    path.join(directory, 'vitest.config.mjs'),
    `import original from ${JSON.stringify(path.join(project, 'vitest.config.ts'))};
export default {...original,root:${JSON.stringify(project)},cacheDir:${JSON.stringify(path.join(directory, 'cache'))},test:{...original.test,pool:'forks',maxWorkers:1,minWorkers:1}};`,
  );
  const execution = await captureExecution({
    directory,
    id: 'resume',
    cwd: project,
    timeout_ms: 60000,
    argv: [
      'rtk',
      'proxy',
      'npm',
      'test',
      '--',
      'src/__tests__/session-manager.test.ts',
      '-t',
      'cold-resumes a timeout reset proved',
      '--config',
      path.join(directory, 'vitest.config.mjs'),
      '--configLoader',
      'native',
    ],
    env: {
      NODE_OPTIONS: `--import=${path.join(project, 'src/__tests__/helpers/autoloop-trust-recovery.ts')}`,
      CLAWO_TRUST_SCRATCH: path.join(directory, 'scratch'),
      CLAWO_TRUST_CASE_ROOT: path.join(directory, 'cases'),
    },
  });
  assert.equal(execution.exit_code, 0);
  const found = fs
    .readdirSync(path.join(directory, 'cases'))
    .filter((name) => name.startsWith('trust-timeout-generation-reset-'));
  assert.equal(found.length, 1);
  const observation = path.join(directory, 'cases', found[0]);
  assert.ok(fs.existsSync(path.join(observation, 'api.json')), 'G5 lacks actual resume API outcome');
  assert.ok(fs.existsSync(path.join(observation, 'decisions.after.jsonl')), 'G5 lacks post-resume durable bytes');
  assert.equal(
    verifier.verifyResumeCase(observation, 'trust-timeout-generation-reset').case_id,
    'trust-timeout-generation-reset',
  );
  for (const [name, change] of [
    [
      'API failure',
      (d) => {
        const f = path.join(d, 'api.json'),
          v = JSON.parse(fs.readFileSync(f));
        v.response = { error: { message: 'failure' } };
        fs.writeFileSync(f, JSON.stringify(v));
      },
    ],
    [
      'forged source',
      (d) => {
        const f = path.join(d, 'api.json'),
          v = JSON.parse(fs.readFileSync(f));
        v.source_imports[0].sha256 = '0'.repeat(64);
        fs.writeFileSync(f, JSON.stringify(v));
      },
    ],
    [
      'no replacement',
      (d) =>
        fs.writeFileSync(
          path.join(d, 'generations.after.jsonl'),
          fs.readFileSync(path.join(d, 'generations.input.jsonl')),
        ),
    ],
    ['rewrite history', (d) => fs.appendFileSync(path.join(d, 'decisions.after.jsonl'), '{}\n')],
    [
      'missing release',
      (d) => {
        const f = path.join(d, 'generations.input.jsonl');
        fs.writeFileSync(
          f,
          fs
            .readFileSync(f, 'utf8')
            .split('\n')
            .filter((x) => !x.includes('agent_generation_released'))
            .join('\n'),
        );
      },
    ],
  ]) {
    const copy = fs.mkdtempSync(path.join(scratch, 'resume-tamper-'));
    fs.cpSync(observation, copy, { recursive: true });
    change(copy);
    assert.throws(() => verifier.verifyResumeCase(copy, 'trust-timeout-generation-reset'), /durability/i, name);
  }
});

// Retained real API observations: refresh every file reference after mutation so
// hash validation cannot mask the JSONL completeness predicate under test.
const resumeEvidence = path.join(
  artifactRoot,
  'evidence/candidate/durability-1789373923054-c3982300-b930-45ab-9e49-9d1057afecf0/cases',
);
for (const [label, scenario, names, mutation] of [
  ['decisions input and after', 'generation-reset', ['decisions.input.jsonl', 'decisions.after.jsonl']],
  ['generation input with matching after prefix', 'generation-reset', ['generations.input.jsonl'], 'prefix'],
  ['generation after suffix', 'generation-reset', ['generations.after.jsonl']],
  ['first decisions input', 'terminated-generation', ['first-resume/decisions.input.jsonl']],
  ['first decisions after suffix', 'terminated-generation', ['first-resume/decisions.after.jsonl']],
  ['first generations input', 'terminated-generation', ['first-resume/generations.input.jsonl']],
  ['first generations after', 'terminated-generation', ['first-resume/generations.after.jsonl']],
  ['clean generation baseline', 'missing-release', ['generations.before.jsonl']],
  ['clean decision baseline', 'missing-release', ['decisions.before.jsonl']],
  ['empty decisions', 'generation-reset', ['decisions.input.jsonl', 'decisions.after.jsonl'], 'empty'],
  ['blank decisions', 'generation-reset', ['decisions.input.jsonl', 'decisions.after.jsonl'], 'blank'],
  ['empty generation suffix', 'generation-reset', ['generations.after.jsonl'], 'empty-suffix'],
]) {
  test(`resume JSONL rejects ${label} with refreshed artifact references`, () => {
    const id = `trust-timeout-${scenario}`;
    const original = path.join(
      resumeEvidence,
      fs.readdirSync(resumeEvidence).find((name) => name.startsWith(id + '-')),
    );
    const head = '8469e68821acbca313d9e442970cabffb082779a';
    assert.equal(verifier.verifyResumeCase(original, id, head).case_id, id);
    const directory = fs.mkdtempSync(path.join(scratch, 'resume-jsonl-'));
    fs.cpSync(original, directory, { recursive: true });
    for (const name of names) {
      const file = path.join(directory, name);
      const bytes = fs.readFileSync(file);
      assert.equal(bytes.at(-1), 10);
      let changed = bytes.subarray(0, bytes.length - 1);
      if (mutation === 'empty') changed = Buffer.alloc(0);
      if (mutation === 'blank') changed = Buffer.from('\n');
      if (mutation === 'empty-suffix') changed = fs.readFileSync(path.join(directory, 'generations.input.jsonl'));
      fs.writeFileSync(file, changed);
      if (mutation === 'prefix') {
        const after = path.join(directory, 'generations.after.jsonl');
        fs.writeFileSync(after, Buffer.concat([changed, fs.readFileSync(after).subarray(bytes.length)]));
      }
    }
    const files = fs
      .readdirSync(directory, { recursive: true })
      .filter((name) => fs.statSync(path.join(directory, name)).isFile())
      .map((name) => ({ path: name, sha256: digest(fs.readFileSync(path.join(directory, name))) }));
    fs.writeFileSync(path.join(directory, 'refreshed-references.json'), JSON.stringify(files));
    const read = collector.durabilityArtifactReader(directory, files);
    for (const file of files) read(file.path);
    let error;
    try {
      verifier.verifyResumeCase(directory, id, head, read);
    } catch (caught) {
      error = caught;
    }
    fs.writeFileSync(
      path.join(directory, 'observed-verdict.json'),
      JSON.stringify({ accepted: !error, error: error?.message }),
    );
    assert.match(error?.message ?? 'accepted torn ledger', /line-complete JSONL/);
  });
}

test('durability reference reader distinguishes observed absence from an omitted artifact hash', () => {
  const directory = deliveryCase('target');
  const read = collector.durabilityArtifactReader(directory, []);
  assert.throws(
    () => read('recipient.jsonl'),
    (error) => error.code === 'ENOENT',
  );
  assert.throws(() => read('message-A.json'), /Missing durability artifact reference/);
  const wrong = collector.durabilityArtifactReader(directory, [{ path: 'message-A.json', sha256: '0'.repeat(64) }]);
  assert.throws(() => wrong('message-A.json'), /hash/i);
});

test('durability CLI resolves its evidence mode and fails closed on missing bytes', async () => {
  const directory = fs.mkdtempSync(path.join(scratch, 'durability-cli-'));
  const execution = await captureExecution({
    directory,
    id: 'missing',
    cwd: project,
    timeout_ms: 3000,
    argv: [
      'rtk',
      'proxy',
      'node',
      'scripts/autoloop-trust-recovery/verify.mjs',
      'durability',
      path.join(directory, 'absent'),
    ],
  });
  assert.equal(execution.exit_code, 1);
  const error = JSON.parse(fs.readFileSync(path.join(directory, execution.stderr.path))).error;
  assert.match(error, /ENOENT/);
});

const relocationBundles = {
  legacy: 'legacy-1789372963181-b79dd8e3-885f-43dc-86d3-fac75cbdbf27',
  durability: 'durability-1789372963192-2d24f5a3-e1c0-4c60-a7f2-68f219efcad9',
};
test('authenticates current executable bytes in a synthetic precommit tool contract', () => {
  // This is a new unit fixture, never a replacement for a historical receipt.
  // Keep positive authentication exercised even when old tool bytes are gone.
  const source = path.join(artifactRoot, 'evidence/candidate', relocationBundles.legacy);
  const bundle = JSON.parse(fs.readFileSync(path.join(source, 'bundle.json')));
  const manifest = verifier.readInputManifest(source, bundle.input_manifest);
  for (const tool of manifest.tools) tool.sha256 = digest(fs.readFileSync(tool.path));
  const directory = fs.mkdtempSync(path.join(scratch, 'current-tool-contract-'));
  const bytes = JSON.stringify(manifest, null, 2) + '\n';
  fs.writeFileSync(path.join(directory, 'inputs.json'), bytes);
  bundle.input_manifest = { path: 'inputs.json', sha256: digest(bytes) };
  bundle.tool_sha256 = digest(JSON.stringify(manifest.tools));
  const snapshot = { head: bundle.head, tree: bundle.tree, manifest: structuredClone(manifest) };
  assert.deepEqual(collector.authenticatedPrecommitSnapshot(snapshot, directory, bundle), snapshot);
  manifest.tools[0].sha256 = '0'.repeat(64);
  const changed = JSON.stringify(manifest, null, 2) + '\n';
  fs.writeFileSync(path.join(directory, 'inputs.json'), changed);
  bundle.input_manifest.sha256 = digest(changed);
  bundle.tool_sha256 = digest(JSON.stringify(manifest.tools));
  assert.throws(() => collector.authenticatedPrecommitSnapshot(snapshot, directory, bundle), {
    message: `Historical tool bytes differ: ${manifest.tools[0].path}`,
  });
});
for (const [mode, attempt] of Object.entries(relocationBundles)) {
  for (const node of [
    '/home/openclaw/.nvm/versions/node/v26.7.0/bin/node',
    '/home/openclaw/.openclaw/tools/node-v26.7.0/bin/node',
  ]) {
    test(`${mode} precommit tool contract authenticates paths or rejects drift under ${node}`, () => {
      const directory = path.join(artifactRoot, 'evidence/candidate', attempt);
      const mismatch = historicalToolMismatch(directory);
      const code = `import assert from 'node:assert/strict';import fs from 'node:fs';
        import {authenticatedPrecommitSnapshot} from ${JSON.stringify(collectorSource)};
        const directory=${JSON.stringify(directory)};
        const bundle=JSON.parse(fs.readFileSync(directory+'/bundle.json'));
        const manifest=JSON.parse(fs.readFileSync(directory+'/inputs.json'));
        const before={head:bundle.head,tree:bundle.tree,manifest:structuredClone(manifest)};
        before.manifest.tools[0].path=process.execPath;
        const expectedMismatch=${JSON.stringify(mismatch)};
        if (expectedMismatch) {
          assert.throws(()=>authenticatedPrecommitSnapshot(before,directory,bundle),{message:expectedMismatch});
          console.log(JSON.stringify({authenticated:false,error:expectedMismatch,runtime:process.execPath}));
        } else {
        const actual=authenticatedPrecommitSnapshot(before,directory,bundle);
        assert.deepEqual(actual.manifest.tools,manifest.tools);
        for(const key of ['tracked','harness','dependencies']) assert.deepEqual(actual.manifest[key],before.manifest[key]);
        assert.equal(actual.head,before.head);assert.equal(actual.tree,before.tree);
        console.log(JSON.stringify({mode:${JSON.stringify(mode)},runtime:process.execPath,tools:actual.manifest.tools}));}`;
      const output = fs.mkdtempSync(path.join(scratch, 'precommit-relocation-'));
      const argv = ['rtk', 'proxy', node, '--input-type=module', '-'];
      const result = spawnSync(argv[0], argv.slice(1), { input: code, cwd: project, encoding: 'utf8' });
      fs.writeFileSync(path.join(output, 'stdout.txt'), result.stdout);
      fs.writeFileSync(path.join(output, 'stderr.txt'), result.stderr);
      fs.writeFileSync(path.join(output, 'execution.json'), JSON.stringify({ argv, exit_code: result.status }));
      assert.equal(result.status, 0, result.stderr);
    });
  }
}
for (const fault of [
  'digest',
  'manifest-reference',
  'tool-digest',
  'relative-path',
  'wrong-name',
  'missing-path',
  'non-file',
  'changed-path-bytes',
  'schema',
]) {
  test(`precommit historical tool validation rejects ${fault}`, () => {
    const original = path.join(artifactRoot, 'evidence/candidate', relocationBundles.legacy);
    const directory = fs.mkdtempSync(path.join(scratch, 'precommit-tool-tamper-'));
    const bundle = JSON.parse(fs.readFileSync(path.join(original, 'bundle.json')));
    const manifest = JSON.parse(fs.readFileSync(path.join(original, 'inputs.json')));
    const snapshot = { head: bundle.head, tree: bundle.tree, manifest: structuredClone(manifest) };
    if (fault === 'digest') manifest.tools[0].sha256 = '0'.repeat(64);
    if (fault === 'relative-path') manifest.tools[0].path = 'node';
    if (fault === 'wrong-name') manifest.tools[0].path = path.join(directory, 'pretend-node');
    if (['missing-path', 'non-file', 'changed-path-bytes'].includes(fault)) {
      manifest.tools[0].path = path.join(directory, 'node');
      if (fault === 'non-file') fs.mkdirSync(manifest.tools[0].path);
      if (fault === 'changed-path-bytes') fs.writeFileSync(manifest.tools[0].path, 'different executable bytes');
    }
    if (fault === 'schema') manifest.tools[0].extra = 'unapproved';
    const bytes = JSON.stringify(manifest, null, 2) + '\n';
    fs.writeFileSync(path.join(directory, 'inputs.json'), bytes);
    bundle.input_manifest = {
      path: 'inputs.json',
      sha256: fault === 'manifest-reference' ? '0'.repeat(64) : digest(bytes),
    };
    bundle.tool_sha256 = fault === 'tool-digest' ? '0'.repeat(64) : digest(JSON.stringify(manifest.tools));
    assert.throws(
      () => collector.authenticatedPrecommitSnapshot(snapshot, directory, bundle),
      /hash|tool|schema|ENOENT/i,
    );
  });
}

test('controller receipt audit authenticates history or rejects tool drift under its Node path', () => {
  fs.mkdirSync(scratch, { recursive: true });
  const directory = fs.mkdtempSync(path.join(scratch, 'controller-finalization-'));
  const mismatch = historicalToolMismatch();
  const code = `import assert from 'node:assert/strict';
    import { auditLegacyFinalization } from ${JSON.stringify(collectorSource)};
    const audit = () => auditLegacyFinalization(${JSON.stringify(firstCorrection)}, ${JSON.stringify(firstCorrectionDirectory)}, ${JSON.stringify(firstCorrectionDigest)}, 'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d');
    if (${JSON.stringify(mismatch)}) {
      assert.throws(audit,{message:${JSON.stringify(mismatch)}});
      console.log(JSON.stringify({runtime:process.execPath,postcommit_verified:false,error:${JSON.stringify(mismatch)}}));
    } else console.log(JSON.stringify({runtime:process.execPath, ...audit().result}));`;
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
  if (mismatch) {
    assert.equal(observed.postcommit_verified, false);
    assert.equal(observed.error, mismatch);
    return;
  }
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
    const receipt = historicalReceipt(firstCorrection);
    change(receipt);
    assert.throws(() => collector.verifyReceiptData(receipt, firstCorrection), expected);
  });
}

test('authenticates the Slice 1 Git link or rejects historical tool drift', () => {
  const before = fs.readFileSync(path.join(artifactRoot, originalBundle));
  const mismatch = historicalToolMismatch();
  if (mismatch)
    assert.throws(() => collector.verifyLegacyCommitLink(originalCommitLink(), originalCommit), { message: mismatch });
  else assert.equal(collector.verifyLegacyCommitLink(originalCommitLink(), originalCommit).cases, 25);
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

test('rejects the original Slice 1 bundle as a Slice 2 candidate', () => {
  assert.throws(
    () =>
      collector.auditLegacyFinalization(
        slice2Commit,
        path.join(artifactRoot, path.dirname(originalBundle)),
        'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d',
        'a89dc369113aaeaefb469625f4c4532d83e481a85dccde392e4f4f6967836f3d',
      ),
    { message: 'Wrong candidate bundle hash/path' },
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
