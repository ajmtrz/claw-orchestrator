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

function candidateWorkerBytes(kind, sourceHashes) {
  const file = path.join(
    PROJECT,
    kind === 'controls'
      ? 'src/__tests__/autoloop-trust-recovery-boundaries.test.ts'
      : 'src/__tests__/autoloop-trust-recovery-adapters.test.ts',
  );
  const source = fs.readFileSync(file);
  requireThat(sourceHashes.get(file) === sha256(source), 'Slice3 proof: candidate worker source is not authenticated');
  const marker = kind === 'controls' ? 'publicCase' : 'native';
  const match = source
    .toString()
    .match(new RegExp('const worker = String\\.raw`([\\s\\S]*?)`;\\n\\nasync function ' + marker));
  requireThat(match, 'Slice3 proof: candidate worker source template is missing');
  return Buffer.from(match[1]);
}

function parseNativeResponse(response, check) {
  check(typeof response === 'string' && response.trim().length > 0, 'empty completed native response');
  const controls = [];
  const cleaned = response
    .replace(/```autoloop\s*\n([\s\S]*?)\n```/g, (_match, body) => {
      let control;
      try {
        control = JSON.parse(body.trim());
      } catch {
        check(false, 'malformed native autoloop control');
      }
      check(
        control &&
          typeof control === 'object' &&
          !Array.isArray(control) &&
          typeof control.tool === 'string' &&
          control.args &&
          typeof control.args === 'object' &&
          !Array.isArray(control.args),
        'malformed native autoloop control',
      );
      controls.push(control);
      return '';
    })
    .trim();
  check(!cleaned.includes('```autoloop'), 'malformed native autoloop control inventory');
  return { response, controls, visible: cleaned };
}

export function verifyNativeProtocol({
  engine,
  model,
  invocations,
  protocol,
  terminalRequired = true,
  allowUnsentStarts = false,
  expectedModes,
  interruptedPid,
  observedOutcomes,
  observedReplies,
  childExits,
  spawnObservations,
  completedRecovery = false,
  witnessedWarmCleanupPids = [],
  expectedWorkerPids,
}) {
  const check = (condition, message) => requireThat(condition, `Native proof: ${message}`);
  const models = { codex: 'gpt-6-astra', claude: 'haiku', agy: 'gemini-3.8-flash-high', cursor: 'auto' };
  check(models[engine] === model, 'wrong requested engine/model');
  check(Array.isArray(invocations) && invocations.length > 0, 'missing invocation');
  check(Array.isArray(protocol) && protocol.length > 0, 'empty protocol');
  const pids = new Set();
  for (const call of invocations) {
    check(call.engine === engine && Number.isSafeInteger(call.pid) && call.pid > 0, 'relabelled invocation');
    check(
      Array.isArray(call.argv) &&
        call.argv.filter((x) => x === '--model').length === 1 &&
        !call.argv.some((arg) => typeof arg === 'string' && arg.startsWith('--model=')),
      'missing model argv',
    );
    check(
      call.argv[call.argv.indexOf('--model') + 1] === (engine === 'claude' ? 'claude-haiku-4-5' : model),
      'substituted model',
    );
    pids.add(call.pid);
  }
  const callsByPid = new Map();
  for (const call of invocations) {
    const calls = callsByPid.get(call.pid) ?? [];
    calls.push(call);
    callsByPid.set(call.pid, calls);
  }
  const observationByPid = new Map();
  const exitsByPid = new Map();
  if (spawnObservations !== undefined || completedRecovery) {
    check(Array.isArray(spawnObservations) && spawnObservations.length > 0, 'missing spawn observation');
    for (const observation of spawnObservations) {
      check(
        (observation.phase === 'warm' || observation.phase === 'cold') &&
          observation.engine === engine &&
          Number.isSafeInteger(observation.pid) &&
          observation.pid > 0 &&
          Array.isArray(observation.argv) &&
          Number.isSafeInteger(observation.worker_pid) &&
          observation.worker_pid > 0,
        'invalid spawn observation',
      );
      check(
        observation.argv.filter((arg) => arg === '--model').length === 1 &&
          !observation.argv.some((arg) => typeof arg === 'string' && arg.startsWith('--model=')),
        'missing spawn model argv',
      );
      check(
        observation.argv[observation.argv.indexOf('--model') + 1] ===
          (engine === 'claude' ? 'claude-haiku-4-5' : model),
        'substituted spawn model',
      );
      if (expectedWorkerPids)
        check(
          observation.worker_pid === expectedWorkerPids[observation.phase],
          'spawn phase differs from authenticated worker execution',
        );
      check(!observationByPid.has(observation.pid), 'duplicate spawn observation');
      const calls = callsByPid.get(observation.pid);
      check(
        (calls &&
          calls.every(
            (call) => call.engine === observation.engine && isDeepStrictEqual(observation.argv, call.argv),
          )) ||
          (allowUnsentStarts && engine === 'claude' && observation.phase === 'warm'),
        'spawn observation invocation binding',
      );
      observationByPid.set(observation.pid, observation);
    }
    check(
      [...pids].every((pid) => observationByPid.has(pid)),
      'missing spawn observation for invocation',
    );
    if (completedRecovery)
      check(
        [...observationByPid.keys()].filter((pid) => !callsByPid.has(pid)).length ===
          (allowUnsentStarts && engine === 'claude' ? 1 : 0),
        'unsent native process inventory differs from the single prepared target',
      );
    if (completedRecovery) check(Array.isArray(childExits), 'missing child exit observations');
    if (childExits !== undefined) {
      check(Array.isArray(childExits), 'invalid child exit observations');
      for (const exit of childExits) {
        check(!exitsByPid.has(exit.pid), 'duplicate child exit');
        const observation = observationByPid.get(exit.pid);
        check(
          observation && exit.engine === observation.engine && isDeepStrictEqual(exit.argv, observation.argv),
          'child exit spawn-observation binding',
        );
        exitsByPid.set(exit.pid, exit);
      }
    }
    check(Array.isArray(witnessedWarmCleanupPids), 'invalid witnessed warm cleanup');
    for (const pid of witnessedWarmCleanupPids) {
      const observation = observationByPid.get(pid);
      check(
        engine === 'claude' && observation?.phase === 'warm' && !exitsByPid.has(pid),
        'unobserved or non-warm cleanup exit exemption',
      );
    }
  }
  const events = protocol.map((x) => x.event);
  const start = {
    codex: (e) => e.type === 'thread.started' && typeof e.thread_id === 'string',
    claude: (e) => e.type === 'system' && e.subtype === 'init' && typeof e.session_id === 'string',
    agy: (e) => e.event === 'init' && typeof e.conversation_id === 'string',
    cursor: (e) => e.type === 'system' && typeof e.session_id === 'string',
  }[engine];
  for (const row of protocol) {
    check(
      row.event &&
        typeof row.event === 'object' &&
        (pids.has(row.pid) ||
          (allowUnsentStarts &&
            engine === 'claude' &&
            start(row.event) &&
            observationByPid.get(row.pid)?.phase === 'warm')),
      'unattributed event',
    );
    if (engine === 'claude' && start(row.event))
      check(row.event.model === 'claude-haiku-4-5', 'Claude effective model differs from authorized model');
  }
  const terminal = {
    codex: (e) => e.type === 'turn.completed' || e.type === 'turn.failed',
    claude: (e) => e.type === 'result',
    agy: (e) => e.event === 'result' && e.result,
    cursor: (e) => e.type === 'result',
  }[engine];
  check(events.some(start), 'missing native start');
  if (terminalRequired) check(events.some(terminal), 'missing terminal lifecycle');
  for (const [pid, calls] of callsByPid) {
    const pidRows = protocol.filter((row) => row.pid === pid);
    const starts = pidRows.filter((row) => start(row.event));
    if (pid === interruptedPid && pidRows.length === 0) continue;
    check(starts.length === 1, 'missing/duplicate physical native start');
    check(pidRows[0] === starts[0], 'terminal before native start');
    if (engine !== 'claude') check(calls.length === 1, 'reused non-persistent native process');
  }
  for (const [pid, observation] of observationByPid)
    if (!callsByPid.has(pid))
      check(
        allowUnsentStarts &&
          engine === 'claude' &&
          observation.phase === 'warm' &&
          protocol.filter((row) => row.pid === pid && start(row.event)).length <= 1,
        'unbound physical native start',
      );
  const identity = (e) => e.thread_id ?? e.session_id ?? e.conversation_id ?? e.result?.conversation_id;
  let previousIdentity;
  const offsets = new Map();
  const nativeReplies = [];
  const nativeTurns = [];
  for (const [i, call] of invocations.entries()) {
    const rows = protocol.filter((r) => r.pid === call.pid).map((r) => r.event);
    const init = rows.find(start);
    if (call.pid === interruptedPid && !init) {
      check(rows.length === 0, 'interrupted invocation has unexpected protocol');
      continue;
    }
    check(init && identity(init)?.length > 0, 'missing per-invocation start');
    const id = identity(init);
    const flag = { codex: 'resume', agy: '--conversation', cursor: '--resume', claude: '--resume' }[engine];
    const resumeIndex = call.argv.indexOf(flag);
    if (resumeIndex >= 0) {
      check(call.argv[resumeIndex + 1] === id, 'continuation identity differs from invocation');
      if (previousIdentity) check(id === previousIdentity, 'continuation identity changed');
    }
    let turnRows = rows;
    if (engine === 'claude') {
      const users = rows.map((e, j) => (e.type === 'user' ? j : -1)).filter((j) => j >= 0);
      const turn = offsets.get(call.pid) ?? 0;
      check(users[turn] !== undefined, 'missing stdin turn');
      check(users[0] === 1, 'unconsumed event before first Claude turn');
      if (call.stdin) check(isDeepStrictEqual(JSON.parse(call.stdin), rows[users[turn]]), 'stdin turn identity');
      turnRows = rows.slice(users[turn], users[turn + 1] ?? rows.length);
      offsets.set(call.pid, turn + 1);
    } else check(rows.indexOf(init) === 0, 'terminal before native start');
    const terminalIndexes = turnRows
      .map((event, index) => (terminal(event) ? index : -1))
      .filter((index) => index >= 0);
    const ends = terminalIndexes.map((index) => turnRows[index]);
    const required = expectedModes ? !['partial', 'protocol', 'process'].includes(expectedModes[i]) : terminalRequired;
    if (call.pid === interruptedPid) check(ends.length === 0, 'interrupted invocation already completed');
    if (required && call.pid !== interruptedPid)
      check(ends.length === 1, 'missing/duplicate per-turn terminal lifecycle');
    if (terminalIndexes.length > 0) {
      const responseAfterTerminal = turnRows
        .slice(terminalIndexes[0] + 1)
        .some((event) =>
          engine === 'codex'
            ? event.type === 'item.completed' && event.item?.type === 'agent_message'
            : event.type === 'assistant',
        );
      check(!responseAfterTerminal, 'native response after terminal lifecycle');
    }
    if (engine === 'claude')
      check(
        turnRows
          .slice(1)
          .every((event, index) => (index === 0 ? event.type === 'assistant' : index === 1 && terminal(event))),
        'unconsumed or out-of-order Claude native lifecycle event',
      );
    for (const end of ends)
      if (identity(end) !== undefined) check(identity(end) === id, 'terminal continuation identity');
    if ((observedOutcomes || completedRecovery) && call.pid !== interruptedPid) {
      const end = ends[0];
      const terminalOk =
        !!end &&
        (engine === 'codex'
          ? end.type === 'turn.completed'
          : engine === 'agy'
            ? end.result.status === 'SUCCESS'
            : end.subtype === 'success' && end.is_error === false);
      const responseRows = terminalIndexes.length > 0 ? turnRows.slice(0, terminalIndexes[0]) : turnRows;
      const messages = responseRows.flatMap((e) =>
        engine === 'codex'
          ? e.type === 'item.completed' && e.item?.type === 'agent_message'
            ? [e.item.text]
            : []
          : e.type === 'assistant'
            ? (e.message?.content ?? []).filter((c) => c.type === 'text').map((c) => c.text)
            : [],
      );
      const response = engine === 'codex' ? messages.join('') : engine === 'agy' ? end?.result.response : end?.result;
      const exits = childExits.filter((x) => x.pid === call.pid);
      const lostWarmObserver =
        completedRecovery &&
        exits.length === 0 &&
        engine === 'claude' &&
        observationByPid.get(call.pid)?.phase === 'warm' &&
        witnessedWarmCleanupPids.includes(call.pid) &&
        observationByPid.get(call.pid)?.worker_pid === expectedWorkerPids?.warm;
      check(exits.length === 1 || lostWarmObserver, 'missing/duplicate child exit');
      const exit = exits[0];
      if (exit)
        check(exit.engine === engine && isDeepStrictEqual(exit.argv, call.argv), 'child exit invocation binding');
      if (expectedModes?.[i] === 'process')
        check(exit.code === 23 && exit.signal === null, 'wrong injected process exit');
      const persistentShutdown =
        engine === 'claude' && exit?.shutdown === true && exit.code === null && exit.signal === 'SIGTERM';
      const processFailure = !lostWarmObserver && !(exit?.code === 0 && exit.signal === null) && !persistentShutdown;
      if (processFailure && !completedRecovery)
        check(
          ['AUTOLOOP_ENGINE_FAILURE', 'AUTOLOOP_REQUIRED_TOOL_DENIED'].includes(observedOutcomes[i]?.code),
          'missing process failure outcome',
        );
      const ok = !processFailure && terminalOk && typeof response === 'string' && response.trim().length > 0;
      check(
        (completedRecovery ? true : observedOutcomes[i]?.ok) === ok,
        'native terminal outcome differs from observed outcome',
      );
      const parsed = ok ? parseNativeResponse(response, check) : null;
      if (parsed) nativeTurns.push({ ...parsed, pid: call.pid, phase: observationByPid.get(call.pid)?.phase });
      if (ok && (engine === 'claude' || engine === 'cursor')) {
        check(messages.length > 0, 'missing native assistant response');
        check(messages.join('') === response, 'native assistant response differs from terminal');
      }
      if (completedRecovery) {
        previousIdentity = id;
        continue;
      }
      if (ok) {
        if (parsed.controls.length) {
          check(
            parsed.controls.length === 1 &&
              parsed.controls[0].tool === 'write_plan' &&
              parsed.controls[0].args?.content === '# verified plan\n\nExact bytes.\n',
            'wrong native plan control',
          );
          nativeReplies.push(
            parsed.visible || `Planner controls persisted: ${parsed.controls.map((x) => x.tool).join(', ')}`,
          );
        } else nativeReplies.push(response);
      }
    }
    previousIdentity = id;
  }
  if (engine === 'claude')
    for (const [pid, calls] of callsByPid) {
      const userTurns = protocol.filter((row) => row.pid === pid && row.event.type === 'user').length;
      check(userTurns === calls.length, 'unconsumed or missing persistent Claude turn');
    }
  if (observedOutcomes)
    check(isDeepStrictEqual(nativeReplies, observedReplies), 'native replies differ from observed replies');
  return { engine, processes: pids.size, turns: nativeTurns };
}

export function verifySlice3RawCase(kind, read, context) {
  const check = (ok, message) => requireThat(ok, `Slice3 proof: ${message}`);
  const eq = (a, b, message) => check(isDeepStrictEqual(a, b), message);
  const json = (name) => JSON.parse(read(name));
  const lines = (name) => {
    const text = read(name).toString();
    check(text.length > 0 && text.endsWith('\n'), 'incomplete JSONL');
    return text
      .slice(0, -1)
      .split('\n')
      .map((line) => {
        check(line.trim().length > 0, 'blank JSONL row');
        return JSON.parse(line);
      });
  };
  const execution = json('execution.json'),
    input = execution.input;
  check(
    context.nodeTool &&
      path.isAbsolute(context.nodeTool.path) &&
      path.basename(context.nodeTool.path) === 'node' &&
      sha256(fs.readFileSync(context.nodeTool.path)) === context.nodeTool.sha256,
    'missing or changed authenticated Node tool',
  );
  check(input.project === PROJECT.replace(/\/$/, '') || input.project === PROJECT, 'wrong imported project');
  const engine = kind === 'controls' ? 'codex' : input.engine;
  const fixture = path.join(PROJECT, `src/__tests__/fixtures/autoloop-trust-recovery/${engine}.mjs`);
  const nativeSources = [
    'src/session-manager.ts',
    'src/autoloop/dispatcher.ts',
    'src/autoloop/runner.ts',
    'src/autoloop/secure-ledger.ts',
    'src/autoloop/planner-tools.ts',
    'src/autoloop/agent-tools.ts',
    engine === 'claude' ? 'src/persistent-session.ts' : `src/persistent-${engine}-session.ts`,
    'src/__tests__/helpers/autoloop-trust-recovery.ts',
  ].map((name) => path.join(PROJECT, name));
  const controlSources = [
    'src/session-manager.ts',
    'src/index.ts',
    'src/embedded-server.ts',
    'src/autoloop/dispatcher.ts',
    'src/autoloop/planner-tools.ts',
    'src/persistent-codex-session.ts',
  ].map((name) => path.join(PROJECT, name));
  const authenticatedWorker = candidateWorkerBytes(kind, context.sourceHashes);
  eq(sha256(read(engine + '.mjs')), context.sourceHashes.get(fixture), 'relabelled fixture');
  eq(execution.fixture_sha256, context.sourceHashes.get(fixture), 'fixture source hash');
  for (const source of context.requiredSources ?? [])
    check(
      execution.source_inputs.some((x) => x.path === source),
      'missing public-path source',
    );
  for (const source of execution.source_inputs)
    eq(source.sha256, context.sourceHashes.get(source.path), 'unbound source');
  const nativeExecutions = [];
  for (const prefix of ['', ...(input.boundary ? ['cold-'] : [])]) {
    const e = json(prefix + 'execution.json');
    nativeExecutions.push(e);
    for (const source of e.source_inputs) eq(source.sha256, context.sourceHashes.get(source.path), 'unbound source');
    check(
      isDeepStrictEqual(
        e.source_inputs.map((source) => source.path),
        kind === 'adapters' ? nativeSources : controlSources,
      ),
      'incomplete or substituted native source inventory',
    );
    check(
      isDeepStrictEqual(e.argv, ['rtk', 'proxy', context.nodeTool.path, '--import', 'tsx', '--input-type=module', '-']),
      'substituted native worker invocation',
    );
    const worker = read(prefix + 'worker.mjs');
    eq(worker, authenticatedWorker, 'worker bytes differ from candidate-owned source');
    if (kind === 'adapters') {
      eq(json(prefix + 'worker-process.json'), { pid: e.worker_pid }, 'worker process identity');
      check(e.input.fixture === path.join(e.input.directory, `${engine}.mjs`), 'substituted native fixture path');
      check(
        worker.length > 0 && sha256(authenticatedWorker) === e.worker_sha256 && !/^0+$/.test(e.worker_sha256),
        'synthetic worker digest',
      );
    }
    eq(e.fixture_sha256, context.sourceHashes.get(fixture), 'cold fixture source hash');
    check(!e.timeout && !e.timedOut, 'worker timeout');
    const exit = e.exit ?? e;
    if (prefix === '' && input.boundary)
      check(e.witnessed === true && exit.signal === 'SIGKILL', 'missing witnessed crash');
    else check(exit.code === 0 && exit.signal === null, 'hidden worker exit');
    check(Number.isFinite(Date.parse(e.started)) && Date.parse(e.ended) >= Date.parse(e.started), 'worker time');
    for (const stream of ['stdout', 'stderr'])
      eq(sha256(read(prefix + stream + '.txt')), e[stream + '_sha256'], 'changed ' + stream);
  }
  if (input.boundary)
    check(Date.parse(nativeExecutions[1].started) >= Date.parse(execution.ended), 'warm/cold execution chronology');
  const outcome = json(input.boundary ? 'cold-outcome.json' : 'outcome.json');
  check(
    !Object.hasOwn(outcome, 'reviewer') && !Object.hasOwn(outcome, 'review_verdict'),
    'Coder authored Reviewer claim',
  );
  if (kind === 'controls') {
    const surface = input.surface;
    check(['manager', 'mcp', 'http'].includes(surface), 'wrong public surface');
    if (input.operation === 'reset') {
      check(
        outcome.eager?.ok === true && outcome.eager.previous_generation === 1 && outcome.eager.active_generation === 2,
        'eager replacement',
      );
      check(
        outcome.lazy === true && outcome.afterLazy?.reply === 'conversation without a control',
        'lazy compatibility',
      );
      check(
        outcome.fault === true &&
          outcome.failedEager?.ok === false &&
          outcome.failedEager.code === 'AUTOLOOP_RESET_POSTCONDITION_FAILED' &&
          outcome.failedWrapper === false,
        'failed reset success',
      );
      return 'manager:reset';
    }
    let tag = input.mode ?? 'conversation';
    if (input.fault) tag = 'persistence';
    else if (input.reply.includes('request_review')) {
      const blocks = [...input.reply.matchAll(/```autoloop\n([\s\S]*?)\n```/g)].map((x) => JSON.parse(x[1]));
      if (blocks.length === 2) tag = 'partial-batch';
      else {
        const args = blocks[0].args;
        tag =
          args.source_run_id === '../escape'
            ? 'source_run_id'
            : args.source_iter === -1
              ? 'source_iter'
              : Array.isArray(args.scope) && args.scope.length === 0
                ? 'scope'
                : args.idempotency_key === ''
                  ? 'idempotency_key'
                  : null;
        check(tag, 'unknown invalid request');
        const rows = lines('tasks/public-boundary/agent-generations.jsonl');
        check(
          rows.some((x) => x.payload.role === 'planner') &&
            !rows.some((x) => ['coder', 'reviewer'].includes(x.payload.role)),
          'invalid control spawned role',
        );
      }
    }
    if (tag === 'conversation')
      eq(outcome.response, { ok: true, reply: 'conversation without a control' }, 'conversational reply');
    else {
      if (surface === 'http') {
        eq(outcome.http, { status: 202, body: { ok: true, queued: true } }, 'queue acceptance');
        check(outcome.terminal?.recent_phase_errors?.length > 0, 'missing terminal error');
      } else check(outcome.response?.ok === false, 'false public success');
      if (['empty', 'denied', 'persistence', 'partial-batch'].includes(tag))
        eq(outcome.plan, null, 'unexpected plan effect');
      if (tag === 'persistence') check(outcome.fault === true, 'missing persistence fault');
      if (tag === 'partial-batch') eq(outcome.goal, null, 'partial goal effect');
    }
    return `${surface}:${tag}`;
  }
  const invocations = lines('native.jsonl'),
    protocol = lines('protocol.jsonl');
  let interruptedPid;
  const spawnObservations = lines('native-spawns.jsonl');
  let permittedWarmCleanup =
    input.boundary &&
    engine === 'claude' &&
    execution.witnessed === true &&
    execution.signal === 'SIGKILL' &&
    execution.cleaned_fixture_pids.length > 0 &&
    execution.cleaned_fixture_pids.every((pid) =>
      spawnObservations.some((spawn) => spawn.pid === pid && spawn.phase === 'warm'),
    )
      ? execution.cleaned_fixture_pids
      : [];
  if (input.boundary) {
    const barrier = json('barrier.json');
    const expectedBarrierKeys =
      input.boundary === 'after-capture' ? ['boundary', 'delivery_id', 'pid'] : ['boundary', 'pid'];
    eq(Object.keys(barrier).sort(), expectedBarrierKeys, 'interruption barrier schema');
    check(barrier.boundary === input.boundary, 'wrong interruption barrier');
    check(Number.isSafeInteger(barrier.pid) && barrier.pid > 0, 'invalid interruption PID');
    if (input.boundary === 'before-send') {
      eq(barrier.pid, execution.worker_pid, 'before-send interruption worker PID');
    } else {
      const recipient = lines('recipient.jsonl')[0];
      eq(barrier.pid, recipient.pid, 'interruption recipient PID');
      eq(barrier.delivery_id, recipient.delivery_id, 'interruption delivery identity');
      check(invocations.filter((call) => call.pid === barrier.pid).length === 1, 'interruption invocation identity');
      if (engine === 'claude') {
        check(execution.cleaned_fixture_pids.includes(barrier.pid), 'detached interrupted PID not terminated');
        check(permittedWarmCleanup.includes(barrier.pid), 'warm cleanup does not bind interrupted invocation');
      }
      check(
        isDeepStrictEqual(execution.cleaned_fixture_pids, permittedWarmCleanup),
        'arbitrary or cold PID claimed as warm cleanup',
      );
      interruptedPid = barrier.pid;
    }
  }
  if (input.boundary) {
    const expectedReply =
      input.scenario === 'review'
        ? 'review existing checkpoint'
        : '```autoloop\n{"tool":"send_directive","args":{"goal":"directive A exact bytes","constraints":["preserve A"],"success_criteria":["one effect"],"max_attempts":1}}\n```';
    const common = {
      project: PROJECT,
      directory: input.directory,
      fixture: path.join(input.directory, `${engine}.mjs`),
      engine,
      model: { codex: 'gpt-6-astra', claude: 'haiku', agy: 'gemini-3.8-flash-high', cursor: 'auto' }[engine],
      modes: [],
      reply: expectedReply,
      scenario: input.scenario,
      boundary: input.boundary,
    };
    const expectedWarm =
      input.scenario === 'review'
        ? {
            ...common,
            reviewRequest: {
              checkpoint_sha: context.head,
              source_run_id: 'source-checkpoint',
              source_iter: 2,
              scope: ['durable-delivery'],
              idempotency_key: 'native-checkpoint-review',
            },
          }
        : common;
    eq(input, expectedWarm, 'warm input descriptor differs from candidate contract');
    eq(nativeExecutions[1].input, { ...common, cold: true }, 'cold input descriptor differs from candidate contract');
    check(
      Number.isSafeInteger(execution.worker_pid) &&
        execution.worker_pid > 0 &&
        Number.isSafeInteger(nativeExecutions[1].worker_pid) &&
        nativeExecutions[1].worker_pid > 0 &&
        execution.worker_pid !== nativeExecutions[1].worker_pid,
      'warm/cold worker process identity',
    );
  }
  const nativeVerification = verifyNativeProtocol({
    engine,
    model: input.model,
    invocations,
    protocol,
    terminalRequired: !input.modes.some((x) => ['partial', 'protocol', 'process'].includes(x)),
    allowUnsentStarts: input.boundary === 'before-send',
    expectedModes: input.boundary || input.scenario === 'reset' ? undefined : input.modes,
    interruptedPid,
    observedOutcomes: input.boundary || input.scenario === 'reset' ? undefined : outcome.outcomes,
    observedReplies: outcome.replies,
    childExits: input.scenario === 'reset' ? undefined : lines('native-exits.jsonl'),
    spawnObservations,
    completedRecovery: Boolean(input.boundary),
    witnessedWarmCleanupPids: permittedWarmCleanup,
    expectedWorkerPids: input.boundary
      ? { warm: execution.worker_pid, cold: nativeExecutions[1].worker_pid }
      : { warm: execution.worker_pid },
  });
  if (input.boundary) {
    const cold = nativeExecutions[1];
    check(
      cold.input.cold === true && cold.witnessed === false && cold.cleaned_fixture_pids.length === 0,
      'cold recovery is not an independent completed worker',
    );
    check(isDeepStrictEqual(cold.source_inputs, execution.source_inputs), 'warm/cold source identities differ');
    check(cold.worker_sha256 === execution.worker_sha256, 'warm/cold worker identities differ');
    check(
      Array.isArray(outcome.outcomes) && outcome.outcomes.length === 1 && outcome.outcomes[0]?.ok === true,
      'cold recovery outcome is not successful',
    );
    check(
      Array.isArray(outcome.replies) &&
        outcome.replies.length > 0 &&
        outcome.replies.every((reply) => typeof reply === 'string' && reply.trim()),
      'cold recovery reply is empty',
    );
    const exits = lines('native-exits.jsonl');
    const coldCalls = spawnObservations
      .filter((spawn) => spawn.phase === 'cold')
      .map((spawn) => invocations.find((call) => call.pid === spawn.pid));
    check(coldCalls.every(Boolean), 'cold spawn observation lacks native invocation');
    const coldPids = new Set(coldCalls.map((call) => call.pid));
    check(coldPids.size > 0, 'cold recovery has no successful child exit');
    for (const pid of coldPids)
      check(exits.filter((exit) => exit.pid === pid).length === 1, 'cold recovery missing/duplicate bound child exit');
    const surfacedReplies = nativeVerification.turns
      .filter((turn) => turn.controls.length === 0)
      .map((turn) => turn.response);
    eq(surfacedReplies, outcome.replies, 'complete surfaced terminal/reply sequence differs');
  }
  if (engine === 'cursor' && !input.boundary)
    for (const call of invocations) {
      check(!call.argv.includes('--force'), 'Cursor force');
      eq(
        call.permission_config,
        { permissions: { allow: [], deny: ['Write(**)', 'Edit(**)', 'Shell(**)'] } },
        'Cursor permissions',
      );
    }
  if (input.scenario === 'reset') {
    check(
      outcome.first?.ok === true && outcome.first.previous_generation === 1 && outcome.first.active_generation === 2,
      'replacement identity',
    );
    check(
      outcome.second?.ok === false &&
        outcome.second.code === 'AUTOLOOP_RESET_POSTCONDITION_FAILED' &&
        outcome.fault === true,
      'replacement failure',
    );
    check(outcome.staleRelease === false && outcome.registryBefore === outcome.registryAfter, 'stale owner released');
    const rows = lines('tasks/native-boundary/agent-generations.jsonl');
    check(
      rows.filter((x) => x.kind === 'agent_generation_released' && x.payload.generation === 2).length === 1 &&
        !rows.some((x) => x.kind === 'agent_generation_started' && x.payload.generation === 3),
      'reset generation lifecycle',
    );
    return engine + ':reset';
  }
  if (input.boundary) {
    check(
      ['review', 'delivery'].includes(input.scenario) && ['before-send', 'after-capture'].includes(input.boundary),
      'unknown boundary',
    );
    const crash = read('crash-decisions.jsonl'),
      final = read('tasks/native-boundary/decisions.jsonl'),
      crashGenerationBytes = read('crash-agent-generations.jsonl'),
      finalGenerationBytes = read('tasks/native-boundary/agent-generations.jsonl');
    check(crash.length < final.length, 'decision crash cut is not a strict proper prefix');
    eq(final.subarray(0, crash.length), crash, 'rewritten crash prefix');
    // The crash observer owns the exact durable generation prefix.  Parsing
    // either ledger alone cannot prove that reconstruction extended that
    // history rather than replacing it with a self-consistent substitute.
    const crashGenerations = lines('crash-agent-generations.jsonl');
    check(
      crashGenerationBytes.length < finalGenerationBytes.length,
      'generation crash cut is not a strict proper prefix',
    );
    eq(
      finalGenerationBytes.subarray(0, crashGenerationBytes.length),
      crashGenerationBytes,
      'rewritten crash generation prefix',
    );
    const rows = lines('tasks/native-boundary/decisions.jsonl'),
      effects = lines('receiver-effects.jsonl'),
      receipts = lines('recipient.jsonl'),
      generations = lines('tasks/native-boundary/agent-generations.jsonl');
    check(effects.length === 1, 'duplicate logical effect');
    const ack = rows.filter((x) => x.acknowledged_at);
    check(ack.length === 1, 'missing/duplicate ACK');
    const intents = rows.filter((x) => x.delivery_id === effects[0].delivery_id && x.payload && !x.record_type);
    check(intents.length === 1, 'missing/duplicate original intent');
    const intent = intents[0];
    const rebinds = rows.filter((row) => row.record_type === 'delivery_generation_rebind');
    check(rebinds.length === 1, 'missing/duplicate delivery generation rebind');
    const rebind = rebinds[0];
    const results = rows.filter((row) => row.record_type === 'delivery_result');
    check(results.length === (input.scenario === 'delivery' ? 1 : 0), 'unexpected delivery result inventory');
    const exactKeys = (row, keys, message) => eq(Object.keys(row).sort(), [...keys].sort(), message);
    exactKeys(
      intent,
      [
        'schema_version',
        'delivery_id',
        'idempotency_key',
        'kind',
        'target_role',
        'target_generation',
        'payload',
        'payload_sha256',
        'created_at',
      ],
      'delivery intent schema',
    );
    exactKeys(
      rebind,
      [
        'schema_version',
        'record_type',
        'delivery_id',
        'idempotency_key',
        'kind',
        'target_role',
        'from_generation',
        'to_generation',
        'payload_sha256',
        'rebound_at',
      ],
      'delivery rebind schema',
    );
    exactKeys(
      ack[0],
      ['schema_version', 'delivery_id', 'payload_sha256', 'acknowledged_at'],
      'delivery acknowledgement schema',
    );
    if (results.length === 1)
      exactKeys(
        results[0],
        ['schema_version', 'record_type', 'delivery_id', 'payload_sha256', 'result_kind', 'result_payload'],
        'delivery result schema',
      );
    check(
      intent.schema_version === 1 &&
        rebind.schema_version === 1 &&
        ack[0].schema_version === 1 &&
        results.every((row) => row.schema_version === 1) &&
        Number.isFinite(Date.parse(intent.created_at)) &&
        Number.isFinite(Date.parse(rebind.rebound_at)) &&
        Number.isFinite(Date.parse(ack[0].acknowledged_at)),
      'invalid delivery record version or time',
    );
    const prepared = rows[0];
    const terminated = rows.at(-1);
    for (const decision of [prepared, terminated]) {
      exactKeys(decision, ['ts', 'kind', 'actor', 'payload'], 'decision ledger schema');
      check(decision.actor === 'planner' && Number.isFinite(Date.parse(decision.ts)), 'decision actor or time');
    }
    eq(
      prepared.kind,
      input.scenario === 'review' ? 'request_review' : 'planner_turn_control',
      'prepared decision kind',
    );
    eq(terminated.kind, 'terminate', 'missing terminal decision');
    eq(terminated.payload, { reason: 'native-fixture-end' }, 'termination payload');
    eq(
      rows,
      [prepared, intent, rebind, ...results, ack[0], terminated],
      'complete decision ledger order and cardinality',
    );
    exactKeys(
      prepared.payload,
      input.scenario === 'review'
        ? [
            'checkpoint_sha',
            'source_run_id',
            'source_iter',
            'target_iter',
            'scope',
            'idempotency_key',
            'request_digest',
          ]
        : [
            'control_id',
            'persisted_at',
            'dispatch_id',
            'message_id',
            'iter',
            'generation',
            'owner_instance_id',
            'session_id',
            'tools',
            'controls',
            'controls_sha256',
          ],
      'prepared decision payload schema',
    );
    if (input.scenario === 'delivery') {
      check(
        ['control_id', 'dispatch_id', 'message_id', 'owner_instance_id', 'session_id'].every(
          (key) => typeof prepared.payload[key] === 'string' && prepared.payload[key].length > 0,
        ) &&
          prepared.payload.iter === 0 &&
          prepared.payload.generation === 1,
        'prepared control identity field types',
      );
      eq(prepared.payload.persisted_at, prepared.ts, 'prepared control persistence timestamp');
      eq(prepared.payload.tools, ['send_directive'], 'prepared control tool inventory');
      eq(
        prepared.payload.controls_sha256,
        sha256(JSON.stringify(prepared.payload.controls)),
        'prepared control digest',
      );
    } else
      check(
        typeof prepared.payload.request_digest === 'string' && /^[a-f0-9]{64}$/.test(prepared.payload.request_digest),
        'prepared review digest type',
      );
    const message = json('message-A.json');
    exactKeys(message, ['msg_id', 'iter', 'from', 'to', 'type', 'ts', 'payload'], 'original message schema');
    const targetRole = input.scenario === 'review' ? 'reviewer' : 'coder';
    const messageType = input.scenario === 'review' ? 'review_request' : 'directive';
    check(
      typeof message.msg_id === 'string' &&
        message.msg_id.length > 0 &&
        message.iter === (input.scenario === 'review' ? 1 : 0) &&
        message.from === (input.scenario === 'review' ? 'runner' : 'planner') &&
        message.to === targetRole &&
        message.type === messageType &&
        Number.isFinite(Date.parse(message.ts)),
      'original message identity',
    );
    exactKeys(intent.payload, ['prompt', 'logical_message_sha256'], 'original intent payload schema');
    eq(intent.target_role, targetRole, 'original intent target role');
    eq(intent.target_generation, rebind.from_generation, 'original intent target generation');
    eq(intent.kind, input.scenario === 'review' ? 'review_request' : 'coder_directive', 'original intent kind');
    eq(
      intent.idempotency_key,
      'dispatch_' +
        sha256(
          JSON.stringify(['native-boundary', message.msg_id, message.iter, message.from, message.to, message.type]),
        ),
      'original message dispatch identity',
    );
    eq(
      intent.payload.logical_message_sha256,
      sha256(
        JSON.stringify({
          msg_id: message.msg_id,
          iter: message.iter,
          from: message.from,
          to: message.to,
          type: message.type,
          ts: message.ts,
          payload: message.payload,
        }),
      ),
      'original logical message digest',
    );
    // A payload self-hash proves consistency, not that its prompt implements
    // the independently captured message. Reconstruct the schema-v1 rendering
    // from that message and the authenticated role template (never intent).
    const templatePath = path.join(PROJECT, `configs/autoloop-${targetRole}-prompt.md`);
    const template = fs.readFileSync(templatePath);
    eq(context.sourceHashes.get(templatePath), sha256(template), 'role template is not authenticated');
    let messagePrompt;
    if (input.scenario === 'delivery') {
      const expectedControl = parseNativeResponse(input.reply, check).controls[0];
      eq(message.payload, expectedControl.args, 'original directive differs from authenticated Planner contract');
      messagePrompt = `[directive iter=${message.iter}]\ngoal: ${message.payload.goal}`;
      for (const key of ['constraints', 'success_criteria'])
        if (message.payload[key].length)
          messagePrompt += `\n${key}:` + message.payload[key].map((item) => `\n  - ${item}`).join('');
      messagePrompt += `\nmax_attempts: ${message.payload.max_attempts}`;
      messagePrompt += '\nRead plan.md / goal.json, make the change, run the evaluator, then emit `iter_complete`.';
    } else {
      eq(
        message.payload,
        {
          iter: 1,
          ledger_path: path.join(input.directory, 'tasks/native-boundary'),
          prior_metrics: [],
          ...input.reviewRequest,
        },
        'original review message differs from authenticated caller contract',
      );
      messagePrompt = [
        `[review_request iter=${message.iter}]`,
        `Artifacts staged from run ${message.payload.source_run_id} iter ${message.payload.source_iter} at: iter-${message.iter}/ (directive.json, diff.patch, eval_output.json)`,
        `checkpoint_sha: ${message.payload.checkpoint_sha}`,
        `scope: ${JSON.stringify(message.payload.scope)}`,
        `prior_verdict: ${input.boundary === 'after-capture' ? 'prior_verdict.json' : '(none)'}`,
        `prior_metrics: ${JSON.stringify(message.payload.prior_metrics)}`,
        '',
        'Audit and emit `review_complete`.',
      ].join('\n');
    }
    const transportPrompt =
      engine === 'claude'
        ? messagePrompt
        : `<autoloop_role_instructions>\n${template.toString().trim()}\n</autoloop_role_instructions>\n\n<autoloop_message>\n${messagePrompt}\n</autoloop_message>`;
    eq(intent.payload.prompt, transportPrompt, 'transport prompt differs from original logical message');
    const deliveryTimes = [
      prepared.ts,
      message.ts,
      intent.created_at,
      rebind.rebound_at,
      ack[0].acknowledged_at,
      terminated.ts,
    ].map(Date.parse);
    check(
      deliveryTimes.every((time, index) => index === 0 || time >= deliveryTimes[index - 1]),
      'cross-record delivery chronology',
    );
    const preparedIndex = rows.findIndex((row) =>
      input.scenario === 'review' ? row.kind === 'request_review' : row.kind === 'planner_turn_control',
    );
    const intentIndex = rows.indexOf(intent),
      rebindIndex = rows.indexOf(rebind),
      resultIndex = results.length === 1 ? rows.indexOf(results[0]) : rebindIndex,
      ackIndex = rows.indexOf(ack[0]);
    check(
      preparedIndex >= 0 &&
        preparedIndex < intentIndex &&
        intentIndex < rebindIndex &&
        rebindIndex <= resultIndex &&
        resultIndex < ackIndex,
      'delivery preparation/rebind/result/ACK order',
    );
    check(
      crashGenerations.every(
        (row) => row.kind === 'agent_generation_reserved' || row.kind === 'agent_generation_started',
      ) && crashGenerations.every((row) => row.payload.generation === 1),
      'generation crash cut contains recovery or post-crash rows',
    );
    const crashRows = lines('crash-decisions.jsonl');
    check(
      crashRows.length > 0 &&
        crashRows.at(-1)?.delivery_id === intent.delivery_id &&
        crashRows.at(-1)?.payload_sha256 === intent.payload_sha256 &&
        crashRows.every(
          (row) =>
            !row.acknowledged_at &&
            row.record_type !== 'delivery_generation_rebind' &&
            row.record_type !== 'delivery_result' &&
            row.kind !== 'terminate',
        ),
      'decision crash cut does not end at the witnessed prepared intent',
    );
    eq(sha256(JSON.stringify(intent.payload)), intent.payload_sha256, 'payload hash');
    eq(ack[0].delivery_id, intent.delivery_id, 'ACK intent delivery identity');
    eq(ack[0].payload_sha256, intent.payload_sha256, 'ACK intent digest');
    check(
      typeof intent.payload.prompt === 'string' && intent.payload.prompt.length > 0,
      'missing authoritative payload bytes',
    );
    const delivered =
      intent.payload.prompt +
      `\n\n<autoloop_delivery delivery_id="${intent.delivery_id}" payload_sha256="${intent.payload_sha256}">\nEcho both fields unchanged in iter_complete, request_clarification, or review_complete.\n</autoloop_delivery>`;
    for (const receipt of [effects[0], ...receipts]) {
      eq(receipt.delivery_id, ack[0].delivery_id, 'delivery identity');
      eq(receipt.payload_sha256, ack[0].payload_sha256, 'ACK digest');
      eq(receipt.prompt, effects[0].prompt, 'replay bytes');
      eq(receipt.prompt, delivered, 'authoritative delivered bytes');
    }
    eq(receipts.length, input.boundary === 'after-capture' ? 2 : 1, 'physical retransmit inventory');
    eq(effects[0].pid, receipts[0].pid, 'logical effect recipient process');
    const generationKeys = ['role', 'generation', 'session_name', 'session_id', 'owner_instance_id'];
    const generationRowKeys = ['actor', 'kind', 'payload', 'schema_version', 'ts'];
    const generationPayloadKeys = [
      'created_at',
      'generation',
      'last_activity_at',
      'lease_expires_at',
      'owner_instance_id',
      'role',
      'session_id',
      'session_name',
      'state',
    ];
    const generationKinds = new Set([
      'agent_generation_reserved',
      'agent_generation_started',
      'agent_generation_orphaned',
      'agent_generation_released',
    ]);
    for (const row of generations) {
      eq(Object.keys(row).sort(), generationRowKeys, 'generation row schema');
      eq(Object.keys(row.payload).sort(), generationPayloadKeys, 'generation payload schema');
      check(
        row.schema_version === 1 &&
          row.actor === 'dispatcher' &&
          generationKinds.has(row.kind) &&
          Number.isFinite(Date.parse(row.ts)) &&
          ['planner', 'coder', 'reviewer'].includes(row.payload.role) &&
          Number.isSafeInteger(row.payload.generation) &&
          row.payload.generation > 0 &&
          [
            'session_name',
            'session_id',
            'owner_instance_id',
            'created_at',
            'last_activity_at',
            'lease_expires_at',
          ].every((key) => typeof row.payload[key] === 'string' && row.payload[key].length > 0) &&
          ['created_at', 'last_activity_at', 'lease_expires_at'].every((key) =>
            Number.isFinite(Date.parse(row.payload[key])),
          ) &&
          Date.parse(row.ts) >= Date.parse(row.payload.created_at) &&
          Date.parse(row.ts) >= Date.parse(row.payload.last_activity_at),
        'invalid generation row',
      );
      const expectedState = {
        agent_generation_reserved: 'stale',
        agent_generation_started: 'live',
        agent_generation_orphaned: 'orphaned',
        agent_generation_released: 'released',
      }[row.kind];
      eq(row.payload.state, expectedState, 'generation state transition');
    }
    check(
      generations.every((row, index) => index === 0 || Date.parse(row.ts) >= Date.parse(generations[index - 1].ts)),
      'cross-generation chronology',
    );
    const ownerBelongsToWorker = (owner, workerPid) =>
      typeof owner === 'string' && owner.startsWith(`session-manager:${workerPid}:`) && owner.length > 20;
    const recoveredRoles = new Map();
    const expectedRoleGenerations =
      input.scenario === 'review'
        ? new Map([
            ['reviewer', [1, 2]],
            ['planner', [1]],
          ])
        : new Map([
            ['planner', [1, 2]],
            ['coder', [1, 2]],
          ]);
    eq(
      [...new Set(generations.map((row) => row.payload.role))].sort(),
      [...expectedRoleGenerations.keys()].sort(),
      'generation role inventory',
    );
    eq(generations.length, input.scenario === 'review' ? 10 : 14, 'generation lifecycle cardinality');
    const expectedPhysicalGenerations = [...expectedRoleGenerations.values()].reduce(
      (count, generationNumbers) => count + generationNumbers.length,
      0,
    );
    eq(
      new Set(generations.map((row) => row.payload.session_id)).size,
      expectedPhysicalGenerations,
      'generation session identity cardinality',
    );
    for (const role of expectedRoleGenerations.keys()) {
      const roleRows = generations.filter((row) => row.payload.role === role);
      const generationNumbers = [...new Set(roleRows.map((row) => row.payload.generation))].sort((a, b) => a - b);
      eq(generationNumbers, expectedRoleGenerations.get(role), 'unexpected generation history');
      for (const generation of generationNumbers) {
        const records = roleRows.filter((row) => row.payload.generation === generation);
        const reserved = records.filter((row) => row.kind === 'agent_generation_reserved');
        const started = records.filter((row) => row.kind === 'agent_generation_started');
        const orphaned = records.filter((row) => row.kind === 'agent_generation_orphaned');
        const released = records.filter((row) => row.kind === 'agent_generation_released');
        check(
          reserved.length === 1 &&
            started.length === 1 &&
            released.length === 1 &&
            orphaned.length === (generation < generationNumbers.at(-1) ? 1 : 0),
          'incomplete generation lifecycle',
        );
        for (const key of generationKeys) {
          eq(started[0].payload[key], reserved[0].payload[key], 'started generation ownership');
          eq(released[0].payload[key], reserved[0].payload[key], 'released generation ownership');
        }
        check(
          generations.indexOf(reserved[0]) < generations.indexOf(started[0]) &&
            generations.indexOf(started[0]) < generations.indexOf(released[0]) &&
            Date.parse(reserved[0].ts) <= Date.parse(started[0].ts) &&
            Date.parse(started[0].ts) <= Date.parse(released[0].ts),
          'generation lifecycle order',
        );
        if (orphaned.length === 1) {
          for (const key of generationKeys)
            eq(orphaned[0].payload[key], reserved[0].payload[key], 'orphaned generation ownership');
          check(
            orphaned[0].payload.state === 'orphaned' &&
              generations.indexOf(started[0]) < generations.indexOf(orphaned[0]) &&
              generations.indexOf(orphaned[0]) < generations.indexOf(released[0]) &&
              Date.parse(started[0].ts) <= Date.parse(orphaned[0].ts) &&
              Date.parse(orphaned[0].ts) <= Date.parse(released[0].ts),
            'orphan lifecycle order',
          );
        }
      }
      if (generationNumbers.includes(2)) {
        const predecessor = crashGenerations.find(
          (row) => row.kind === 'agent_generation_started' && row.payload.role === role && row.payload.generation === 1,
        );
        check(predecessor, 'missing authenticated crash predecessor');
        check(
          ownerBelongsToWorker(predecessor.payload.owner_instance_id, execution.worker_pid),
          'warm generation owner is not authenticated by the warm worker',
        );
        const orphaned = generations.find(
            (row) =>
              row.kind === 'agent_generation_orphaned' && row.payload.role === role && row.payload.generation === 1,
          ),
          released = generations.find(
            (row) =>
              row.kind === 'agent_generation_released' && row.payload.role === role && row.payload.generation === 1,
          ),
          reserved = generations.find(
            (row) =>
              row.kind === 'agent_generation_reserved' && row.payload.role === role && row.payload.generation === 2,
          ),
          started = generations.find(
            (row) =>
              row.kind === 'agent_generation_started' && row.payload.role === role && row.payload.generation === 2,
          );
        check(orphaned && released && reserved && started, 'missing recovered generation transition');
        for (const row of [orphaned, released])
          for (const key of generationKeys)
            eq(row.payload[key], predecessor.payload[key], 'predecessor recovery ownership');
        check(
          orphaned.payload.state === 'orphaned' && released.payload.state === 'released',
          'predecessor state transition',
        );
        check(
          generations.indexOf(orphaned) < generations.indexOf(released) &&
            generations.indexOf(released) < generations.indexOf(reserved) &&
            generations.indexOf(reserved) < generations.indexOf(started),
          'predecessor recovery must finish before successor startup',
        );
        check(
          ownerBelongsToWorker(reserved.payload.owner_instance_id, nativeExecutions[1].worker_pid),
          'cold generation owner is not authenticated by the cold worker',
        );
        check(
          reserved.payload.owner_instance_id !== predecessor.payload.owner_instance_id &&
            reserved.payload.session_id !== predecessor.payload.session_id &&
            reserved.payload.session_name === predecessor.payload.session_name,
          'cold successor did not replace the warm owner/session identity',
        );
        recoveredRoles.set(role, { predecessor, released, reserved, started });
      }
    }
    if (input.scenario === 'delivery') {
      const sourcePlanner = recoveredRoles.get('planner')?.predecessor;
      check(sourcePlanner, 'missing prepared control source Planner generation');
      for (const key of ['generation', 'owner_instance_id', 'session_id'])
        eq(prepared.payload[key], sourcePlanner.payload[key], 'prepared control source Planner generation ' + key);
      check(Date.parse(prepared.ts) >= Date.parse(sourcePlanner.ts), 'prepared control predates source Planner');
    }
    const targetRecovery = recoveredRoles.get(targetRole);
    check(targetRecovery, 'missing target role recovery lifecycle');
    check(
      rebind.delivery_id === intent.delivery_id &&
        rebind.idempotency_key === intent.idempotency_key &&
        rebind.kind === intent.kind &&
        rebind.target_role === targetRole &&
        rebind.payload_sha256 === intent.payload_sha256 &&
        rebind.from_generation + 1 === rebind.to_generation &&
        targetRecovery.released.payload.generation === rebind.from_generation &&
        targetRecovery.started.payload.generation === rebind.to_generation &&
        generations.indexOf(targetRecovery.released) < generations.indexOf(targetRecovery.started),
      'delivery generation rebind linkage',
    );
    check(
      Date.parse(targetRecovery.predecessor.ts) <= Date.parse(intent.created_at) &&
        Date.parse(intent.created_at) <= Date.parse(targetRecovery.released.ts) &&
        Date.parse(targetRecovery.started.ts) <= Date.parse(rebind.rebound_at),
      'delivery and target-generation chronology',
    );
    // Authenticate the effective prompt and the complete option inventory, not
    // a heading in an arbitrary argument. Each engine has its own schema-v1
    // adapter shape; no CLI option or role is inferred from another engine.
    const verifyRoleInvocation = (call, role, roleTemplate, prompt) => {
      const label = role === 'planner' ? 'source Planner role prompt' : 'recipient role prompt';
      check(call && Array.isArray(call.argv), label + ' missing invocation');
      const planner = role === 'planner';
      const workspace =
        role === 'reviewer' ? path.join(input.directory, 'tasks/native-boundary/reviewer_sandbox') : input.directory;
      let expected;
      if (engine === 'claude') {
        expected = [
          '-p',
          '--input-format',
          'stream-json',
          '--output-format',
          'stream-json',
          '--replay-user-messages',
          '--verbose',
          '--include-partial-messages',
          '--permission-mode',
          planner ? 'plan' : 'bypassPermissions',
          '--model',
          'claude-haiku-4-5',
          ...(planner ? ['--disallowed-tools', 'Write,Edit,MultiEdit,NotebookEdit'] : []),
          '--system-prompt',
          roleTemplate.toString(),
          '--permission-prompts',
          'none',
        ];
      } else if (engine === 'codex') {
        expected = [
          'exec',
          '--sandbox',
          planner ? 'read-only' : 'workspace-write',
          '--skip-git-repo-check',
          '--json',
          '-C',
          workspace,
          '--model',
          input.model,
          prompt,
        ];
      } else if (engine === 'agy') {
        const log = call.argv[5];
        check(
          typeof log === 'string' &&
            path.isAbsolute(log) &&
            /^process-[1-9][0-9]*\/tmp\/agy-agy-[0-9]+-[a-z0-9]+\.log$/.test(path.relative(input.directory, log)),
          label + ' AGY log option',
        );
        expected = [
          '-p',
          prompt,
          '--output-format',
          'stream-json',
          '--log-file',
          log,
          ...(planner ? ['--mode', 'plan'] : ['--dangerously-skip-permissions']),
          '--model',
          input.model,
          '--print-timeout',
          '605s',
        ];
      } else if (engine === 'cursor') {
        expected = [
          '-p',
          prompt,
          ...(planner ? ['--mode', 'plan'] : ['--force']),
          '--trust',
          '--output-format',
          'stream-json',
          '--model',
          input.model,
          '--workspace',
          workspace,
        ];
      }
      eq(call.argv, expected, label + ' differs from authenticated template or canonical option inventory');
      if (engine === 'claude' && prompt !== undefined) {
        eq(
          call.stdin,
          JSON.stringify({ type: 'user', message: { role: 'user', content: [{ type: 'text', text: prompt }] } }),
          label + ' rendered message differs from authenticated contract',
        );
      }
    };
    for (const [index, receipt] of receipts.entries()) {
      const expectedPhase = input.boundary === 'after-capture' && index === 0 ? 'warm' : 'cold';
      const spawn = spawnObservations.find((row) => row.pid === receipt.pid);
      check(spawn?.phase === expectedPhase, 'recipient phase differs from witnessed delivery worker');
      const calls = invocations.filter((call) => call.pid === receipt.pid);
      check(calls.length === 1, 'recipient process does not identify one logical delivery turn');
      const call = calls[0];
      verifyRoleInvocation(call, targetRole, template, delivered);
      check(
        spawn.worker_pid === (expectedPhase === 'warm' ? execution.worker_pid : nativeExecutions[1].worker_pid),
        'recipient process is not owned by the authenticated delivery worker',
      );
    }
    // Claude starts its persistent recipient before the before-send barrier.
    // It may have no invocation or exit after witnessed cleanup; authenticate
    // that exact spawn independently without requiring an invented close row.
    if (engine === 'claude' && input.boundary === 'before-send') {
      const unsent = spawnObservations.filter((spawn) => !invocations.some((call) => call.pid === spawn.pid));
      check(unsent.length === 1 && unsent[0].phase === 'warm', 'recipient role prompt unsent spawn inventory');
      verifyRoleInvocation(unsent[0], targetRole, template);
    }
    check(
      targetRecovery.started.payload.session_name === `autoloop-native-boundary-${targetRole}` &&
        /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
          targetRecovery.started.payload.session_id,
        ) &&
        ownerBelongsToWorker(targetRecovery.started.payload.owner_instance_id, nativeExecutions[1].worker_pid),
      'recovered recipient session/owner identity',
    );
    const controls = nativeVerification.turns.flatMap((turn) => turn.controls);
    if (input.scenario === 'review') {
      const caller = json('independent-caller-request.json');
      eq(
        caller,
        {
          target_iter: 1,
          request: {
            checkpoint_sha: context.head,
            source_run_id: 'source-checkpoint',
            source_iter: 2,
            scope: ['durable-delivery'],
            idempotency_key: 'native-checkpoint-review',
          },
        },
        'caller request identity',
      );
      const decisions = rows.filter((x) => x.kind === 'request_review');
      check(decisions.length === 1, 'review decision count');
      for (const [key, value] of Object.entries({ ...caller.request, target_iter: 1 }))
        eq(decisions[0].payload[key], value, 'review prepared caller ' + key);
      // request_review is the durable decision, not requestReview's returned
      // preparation. Authenticate that separately retained boundary as well.
      eq(
        json('review-preparations.json'),
        [
          {
            status: 'prepared',
            target: 'reviewer',
            idempotency_key: caller.request.idempotency_key,
            payload: message.payload,
          },
          { status: 'duplicate', target: 'reviewer', idempotency_key: caller.request.idempotency_key },
        ],
        'review preparation cardinality or caller/message identity',
      );
      // Reconstruct the canonical production serialization in its fixed field
      // order; an arbitrary 64-hex self-consistent digest is not request identity.
      eq(
        prepared.payload.request_digest,
        sha256(
          JSON.stringify({
            target_iter: caller.target_iter,
            request: {
              checkpoint_sha: caller.request.checkpoint_sha,
              source_run_id: caller.request.source_run_id,
              source_iter: caller.request.source_iter,
              scope: caller.request.scope,
              idempotency_key: caller.request.idempotency_key,
            },
          }),
        ),
        'review request digest differs from authenticated canonical request',
      );
      check(
        !generations.some((x) => x.payload.role === 'coder') &&
          generations.some((x) => x.payload.role === 'reviewer' && x.kind === 'agent_generation_started'),
        'Reviewer independence',
      );
      const inspection = effects[0].review_inspection;
      check(inspection, 'missing receiver inspection');
      eq(
        inspection.workspace,
        path.join(input.directory, 'tasks/native-boundary/reviewer_sandbox'),
        'Reviewer sandbox isolation',
      );
      if (engine === 'cursor') {
        const call = invocations.find((x) => x.pid === effects[0].pid);
        check(call, 'missing Cursor Reviewer process');
        eq(call.argv[call.argv.indexOf('--workspace') + 1], inspection.workspace, 'Cursor Reviewer workspace');
        eq(call.cwd, inspection.cwd, 'Cursor observed cwd');
      }
      const repo = json('collector-repo-state.json');
      eq(repo.head, context.head, 'Reviewer changed checkpoint HEAD');
      eq(repo.tracked_status, '', 'Reviewer changed tracked repository');
      eq(
        inspection.request,
        {
          target_iter: 1,
          source_run_id: 'source-checkpoint',
          source_iter: 2,
          checkpoint_sha: context.head,
          scope: ['durable-delivery'],
        },
        'receiver caller identity',
      );
      // This observation was captured by the parent before cold recovery,
      // independently of the receiver. Comparing only the retained source and
      // receiver would permit their coordinated replacement.
      const observedSource = json('independent-source-observation.json');
      exactKeys(observedSource, ['sourceArtifacts', 'prior'], 'independent source observation schema');
      const sourceNames = ['directive.json', 'diff.patch', 'eval_output.json', 'coder_summary.txt'];
      exactKeys(observedSource.sourceArtifacts, sourceNames, 'independent source artifact inventory');
      const descriptor = (bytes) => ({
        bytes_base64: bytes.toString('base64'),
        bytes: bytes.length,
        sha256: sha256(bytes),
      });
      for (const name of sourceNames) {
        const bytes = read('tasks/source-checkpoint/iter/2/' + name);
        eq(observedSource.sourceArtifacts[name], descriptor(bytes), 'independent source bytes or descriptor ' + name);
      }
      eq(inspection.artifacts, observedSource.sourceArtifacts, 'receiver source artifact inventory or bytes');
      // Agreement among mutable receipts is not checkpoint provenance. Rebuild
      // the requested patch from the repository's content-addressed commit,
      // using the same fixed, non-executable diff options as the capture.
      eq(
        read('tasks/source-checkpoint/iter/2/diff.patch'),
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
            caller.request.checkpoint_sha,
            '--',
          ],
          { cwd: PROJECT, maxBuffer: 16 * 1024 * 1024 },
        ),
        'source patch differs from requested checkpoint',
      );

      let prior = null;
      try {
        prior = descriptor(read('tasks/native-boundary/iter/0/verdict.json'));
      } catch (error) {
        // ENOENT proves actual absence through the contained artifact reader.
        // A present file omitted from its hash inventory is not absence.
        if (error.code !== 'ENOENT') throw error;
      }
      check((prior !== null) === (input.boundary === 'after-capture'), 'prior verdict presence at crash boundary');
      eq(observedSource.prior, prior, 'prior verdict differs from independent source observation');
      eq(inspection.prior_verdict, prior, 'prior verdict receiver bytes or absence');
      // The already-authenticated candidate worker seeds these exact bytes
      // before the after-capture crash. Bind to that scenario contract, not
      // another replaceable descriptor supplied by the evidence bundle.
      if (prior !== null)
        eq(
          prior,
          descriptor(
            Buffer.from('{"schema_version":1,"iter":0,"decision":"hold","audit_notes":"prior checkpoint review"}\n'),
          ),
          'prior verdict differs from authenticated seed provenance',
        );

      const verdict = json('tasks/native-boundary/iter/1/verdict.json');
      check(verdict.iter === 1 && verdict.decision === 'hold', 'source-bound verdict');
      const reviewControls = controls.filter((control) => control.tool === 'review_complete');
      check(controls.length === 1 && reviewControls.length === 1, 'unexpected recovery control inventory');
      eq(
        reviewControls[0].args,
        {
          decision: verdict.decision,
          metric: verdict.metric,
          audit_notes: verdict.audit_notes,
          delivery_id: intent.delivery_id,
          payload_sha256: intent.payload_sha256,
        },
        'review control differs from persisted verdict/delivery',
      );
      for (const receipt of receipts) eq(receipt.review_inspection, inspection, 'replayed inspection');
    } else {
      const sourceTurns = nativeVerification.turns.filter((turn) =>
        turn.controls.some((control) => control.tool === 'send_directive'),
      );
      check(sourceTurns.length === 1 && sourceTurns[0].phase === 'warm', 'source Planner native turn identity');
      eq(sourceTurns[0].response, input.reply, 'source Planner response differs from authenticated contract');
      const sourceCall = invocations.find((call) => call.pid === sourceTurns[0].pid);
      const plannerTemplatePath = path.join(PROJECT, 'configs/autoloop-planner-prompt.md');
      const plannerTemplate = fs.readFileSync(plannerTemplatePath);
      eq(
        context.sourceHashes.get(plannerTemplatePath),
        sha256(plannerTemplate),
        'Planner role template is not authenticated',
      );
      const plannerPrompt = (message) =>
        engine === 'claude'
          ? message
          : `<autoloop_role_instructions>\n${plannerTemplate.toString().trim()}\n</autoloop_role_instructions>\n\n<autoloop_message>\n${message}\n</autoloop_message>`;
      verifyRoleInvocation(sourceCall, 'planner', plannerTemplate, plannerPrompt('issue the exact directive'));
      const directive = controls.filter((control) => control.tool === 'send_directive');
      const clarification = controls.filter((control) => control.tool === 'request_clarification');
      check(
        controls.length === 2 && directive.length === 1 && clarification.length === 1,
        'unexpected delivery recovery control inventory',
      );
      eq(directive[0].args, message.payload, 'directive control differs from persisted message');
      eq(prepared.payload.controls, directive, 'prepared control differs from native directive');
      eq(
        clarification[0].args,
        {
          question: 'clarify the exact directive',
          delivery_id: intent.delivery_id,
          payload_sha256: intent.payload_sha256,
        },
        'clarification control differs from persisted delivery',
      );
      const recipientTurns = nativeVerification.turns.filter((turn) => turn.pid === receipts.at(-1).pid);
      check(
        recipientTurns.length === 1 && recipientTurns[0].controls.length === 1,
        'completed delivery result turn inventory',
      );
      eq(recipientTurns[0].controls[0], clarification[0], 'delivery result native control identity');
      const directiveAck = { understood: false, clarification: recipientTurns[0].visible.slice(0, 500) };
      eq(
        results[0],
        {
          schema_version: 1,
          record_type: 'delivery_result',
          delivery_id: intent.delivery_id,
          payload_sha256: intent.payload_sha256,
          result_kind: 'directive_ack',
          result_payload: directiveAck,
        },
        'delivery result differs from original intent and completed native reply',
      );
      // This schema-v1 crash worker reconstructs a fresh Planner generation to
      // consume the recovered ACK. It cannot borrow the warm role check or an
      // arbitrary resume option shape. Select by authenticated phase/PID, never
      // by a role heading that a substituted argv could simply remove.
      const recipientPids = new Set(receipts.map((receipt) => receipt.pid));
      const coldPlannerCalls = invocations.filter(
        (call) =>
          spawnObservations.some((spawn) => spawn.pid === call.pid && spawn.phase === 'cold') &&
          !recipientPids.has(call.pid),
      );
      check(coldPlannerCalls.length === 1, 'cold Planner role prompt invocation inventory');
      verifyRoleInvocation(
        coldPlannerCalls[0],
        'planner',
        plannerTemplate,
        plannerPrompt(`[system] coder directive_ack iter=${message.iter}: ${JSON.stringify(directiveAck)}`),
      );
    }
    return `${engine}:${input.scenario}:${input.boundary}`;
  }
  eq(outcome.outcomes.length, input.modes.length, 'turn inventory');
  for (const [i, mode] of input.modes.entries())
    check(outcome.outcomes[i].ok === (mode === 'success'), 'terminal outcome ' + mode);
  const successful = input.modes.filter((x) => x === 'success').length;
  check(outcome.replies.length === successful, 'nonempty replies inventory');
  check(
    outcome.replies.every((x) => typeof x === 'string' && x.trim().length > 0),
    'empty successful reply',
  );
  if (input.modes.length === 2) {
    if (engine === 'claude') check(invocations[0].pid === invocations[1].pid, 'stdin continuity');
    else
      check(
        invocations[1].argv.includes({ codex: 'resume', agy: '--conversation', cursor: '--resume' }[engine]),
        'resume continuity',
      );
  }
  if (input.reply.includes('write_plan')) {
    if (successful) {
      eq(outcome.plan, '# verified plan\n\nExact bytes.\n', 'real control bytes');
      eq(read('plan.md').toString(), outcome.plan, 'persisted plan');
    } else eq(outcome.plan, null, 'incomplete control persisted');
  }
  return `${engine}:${input.modes.join(',')}:${input.reply.includes('write_plan') ? 'control' : 'reply'}`;
}
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
  } else if (format === 'vitest-json' || format === 'vitest-json-indexed') {
    const report = JSON.parse(text);
    requireThat(Array.isArray(report.testResults), 'Missing Vitest report testResults');
    for (const suite of report.testResults) {
      requireThat(Array.isArray(suite.assertionResults), 'Missing Vitest assertions');
      if (suite.status !== 'passed' || suite.message || suite.testExecError) result.errors.push('Failed Vitest suite');
      for (const [offset, row] of suite.assertionResults.entries()) {
        const id = `${suite.name} > ${row.fullName}${format === 'vitest-json-indexed' ? ` [assertion ${offset}]` : ''}`;
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
  const parse = (bytes) => {
    const text = bytes.toString();
    check(text.endsWith('\n'), 'expected nonempty, line-complete JSONL');
    const lines = text.slice(0, -1).split('\n');
    check(
      lines.every((line) => line.trim().length > 0),
      'expected nonempty, line-complete JSONL',
    );
    try {
      return lines.map((line) => JSON.parse(line));
    } catch {
      check(false, 'expected valid, line-complete JSONL');
    }
  };
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
  parse(before);
  parse(after);
  // The torn negative scenario certifies rejection, never successful resume.
  // Its exact malformed bytes are checked against the clean baseline below.
  if (caseId !== 'trust-timeout-torn') {
    parse(generations);
    parse(generationAfter);
  }
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
    parse(original);
    parse(read('prepared-and-cold-rejected.jsonl'));
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
      parse(firstBefore);
      parse(firstAfter);
      parse(read('first-resume/generations.input.jsonl'));
      parse(read('first-resume/generations.after.jsonl'));
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
      if (['controls', 'adapters'].includes(mode) && extra.length === 0) {
        const { verifySlice3 } = await import('./collect.mjs');
        process.stdout.write(JSON.stringify({ ...verifySlice3(mode, directory), verified: true }) + '\n');
        return;
      }
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
