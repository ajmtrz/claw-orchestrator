import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const project = fileURLToPath(new URL('../../', import.meta.url));
const root = path.join(
  project,
  '.artifacts/CLAWO-AUTOLOOP-DURABLE-RECOVERY-TRUST-RECOVERY-R1/evidence/candidate/slice3-native',
);
const worker = String.raw`
import fs from 'node:fs';
import path from 'node:path';
const input=JSON.parse(process.env.CLAWO_TRUST_NATIVE_CONFIG_INPUT);
fs.writeFileSync(path.join(input.directory,(input.cold?'cold-':'')+'worker-process.json'),JSON.stringify({pid:process.pid}),{flag:'wx'});
const childProcess=await import('node:child_process');
const {syncBuiltinESMExports}=await import('node:module');
const originalSpawn=childProcess.default.spawn;
const observedChildren=[];
let shutdownStarted=false;
childProcess.default.spawn=function(command,args,options){
 const child=originalSpawn.call(this,command,args,options);
 if(command===input.fixture){
  const fd=fs.openSync(path.join(input.directory,'native-spawns.jsonl'),'a');
  try{fs.writeSync(fd,JSON.stringify({phase:input.cold?'cold':'warm',worker_pid:process.pid,engine:input.engine,pid:child.pid,argv:args})+'\n');fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
  const closed=new Promise(resolve=>child.once('close',(code,signal)=>{
   const exitFd=fs.openSync(path.join(input.directory,'native-exits.jsonl'),'a');
   try{fs.writeSync(exitFd,JSON.stringify({engine:input.engine,pid:child.pid,argv:args,code,signal,shutdown:shutdownStarted})+'\n');fs.fsyncSync(exitFd);}finally{fs.closeSync(exitFd);}
   resolve();
  }));observedChildren.push(closed);
 }
 return child;
};syncBuiltinESMExports();
// Keep lock ages in the real filesystem epoch and deadlines advancing.
// Cold reconstruction still advances exactly five minutes relative to wall time.
const WallDate=Date;
globalThis.Date=class extends WallDate {
 constructor(...args){super(...(args.length?args:[WallDate.now()+(input.cold?300000:0)]));}
 static now(){return WallDate.now()+(input.cold?300000:0);}
};
const {SessionManager}=await import(input.project+'/src/session-manager.ts');
const {ClaudeAgentDispatcher}=await import(input.project+'/src/autoloop/dispatcher.ts');
const {Msg}=await import(input.project+'/src/autoloop/messages.ts');
const {AutoloopRunner}=await import(input.project+'/src/autoloop/runner.ts');
const {nullLogger}=await import(input.project+'/src/logger.ts');
const manager=new SessionManager({maxConcurrentSessions:3,claudeBin:input.fixture},nullLogger);
const dispatcher=new ClaudeAgentDispatcher({manager,workspace:input.directory,runId:'native-boundary',plannerEngine:input.engine,plannerModel:input.model,coderEngine:input.engine,coderModel:input.model,reviewerEngine:input.engine,reviewerModel:input.model,agentLeaseMs:1000,now:()=>new Date(Date.now()),logger:nullLogger});
const replies=[];dispatcher.on('planner_reply',reply=>replies.push(reply));
const outcomes=[];
let runner;
try {
 if(input.scenario==='delivery'||input.scenario==='review') {
  fs.writeFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG,JSON.stringify({...input,mode:'success'}));
  const decisions=path.join(input.directory,'tasks/native-boundary/decisions.jsonl');
  if(!input.cold&&input.boundary==='before-send') {
   const sync=fs.fsyncSync;
   fs.fsyncSync=fd=>{sync(fd);if(fs.readlinkSync('/proc/self/fd/'+fd)===decisions&&fs.readFileSync(decisions,'utf8').split('\n').filter(Boolean).map(JSON.parse).some(row=>row.delivery_id&&row.kind===(input.scenario==='review'?'review_request':'coder_directive'))){fs.writeFileSync(path.join(input.directory,'barrier.json'),JSON.stringify({boundary:'before-send',pid:process.pid}),{flag:'wx'});Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0);}};
   syncBuiltinESMExports();
  }
  runner=new AutoloopRunner({run_id:'native-boundary',workspace:input.directory,ledger_dir:path.dirname(decisions),dispatcher,notifyUser:async()=>{}});
  runner.on('error',()=>{});
  runner.on('message',message=>{fs.appendFileSync(path.join(input.directory,input.cold?'cold-messages.jsonl':'messages.jsonl'),JSON.stringify(message)+'\n');if(message.type==='directive'&&!input.cold&&!fs.existsSync(path.join(input.directory,'message-A.json')))fs.writeFileSync(path.join(input.directory,'message-A.json'),JSON.stringify(message),{flag:'wx'});});
  if(input.cold){const original=JSON.parse(fs.readFileSync(path.join(input.directory,'message-A.json')));await Promise.all([runner.send(original,{requireRootDelivery:true}),runner.send(original,{requireRootDelivery:true})]);}
  else if(input.scenario==='review') {
   const {SecureAutoloopLedger}=await import(input.project+'/src/autoloop/secure-ledger.ts');
   const source=SecureAutoloopLedger.open(input.directory,'source-checkpoint',{create:true});
   for(const iter of [0,2]) {
    source.writeIterationArtifact(iter,'directive.json',JSON.stringify({schema_version:1,goal:'review checkpoint source '+iter})+'\n');
    source.writeIterationArtifact(iter,'eval_output.json',JSON.stringify({schema_version:1,iter,eval_output:{source:iter}})+'\n');
    source.writeIterationArtifact(iter,'coder_summary.txt','existing checkpoint source '+iter+'\n');
    source.writeIterationArtifact(iter,'diff.patch',fs.readFileSync(path.join(input.directory,'source.patch')));
   }
   if(input.boundary==='after-capture')SecureAutoloopLedger.open(input.directory,'native-boundary',{create:true}).writeIterationArtifact(0,'verdict.json','{"schema_version":1,"iter":0,"decision":"hold","audit_notes":"prior checkpoint review"}\n');
   const request=structuredClone(input.reviewRequest);
   const prepared=await Promise.all([dispatcher.requestReview(request,1),dispatcher.requestReview(request,1)]);
   fs.writeFileSync(path.join(input.directory,'review-preparations.json'),JSON.stringify(prepared),{flag:'wx'});
   const message=Msg.reviewRequest(1,prepared.find(row=>row.status==='prepared').payload);
   fs.writeFileSync(path.join(input.directory,'message-A.json'),JSON.stringify(message),{flag:'wx'});
   await runner.send(message,{requireRootDelivery:true});
  }
  else {await runner.start();await runner.send(Msg.chat(0,{text:'issue the exact directive'}),{requireRootDelivery:true});}
  fs.writeFileSync(path.join(input.directory,input.cold?'cold-outcome.json':'outcome.json'),JSON.stringify({outcomes:[{ok:true}],replies,state:runner.state,rows:fs.readFileSync(decisions,'utf8')}));
 } else if(input.scenario==='reset') {
  fs.writeFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG,JSON.stringify({...input,mode:'success'}));
  await dispatcher.deliver(Msg.chat(0,{text:'initial native turn'}));
  const first=await dispatcher.resetAgent('planner',{force:true,eagerRestart:true});
  await dispatcher.deliver(Msg.chat(0,{text:'replacement native turn'}));
  const prior=JSON.parse(fs.readFileSync(path.join(input.directory,'tasks/native-boundary/agent-generations.jsonl'),'utf8').trim().split('\n')[0]).payload;
  const registry=path.join((await import('node:os')).homedir(),'.openclaw/claude-sessions.json');
  const registryBefore=fs.readFileSync(registry,'utf8');
  const staleRelease=await manager.releaseReservation(dispatcher.sessionNames.planner,prior.generation,{expectedOwnerInstanceId:prior.owner_instance_id,expectedSessionId:prior.session_id,releaseOwnerInstanceId:manager.autoloopOwnerInstanceId});
  const registryAfter=fs.readFileSync(registry,'utf8');
  const sync=fs.fsyncSync;
  let fault=false;
  fs.fsyncSync=fd=>{const target=fs.readlinkSync('/proc/self/fd/'+fd);if(target.endsWith('/agent-generations.jsonl')&&fs.readFileSync(target,'utf8').split('\n').filter(Boolean).map(JSON.parse).some(row=>row.kind==='agent_generation_reserved'&&row.payload.generation===3)){fault=true;throw new Error('fixture replacement reservation fsync rejection');}sync(fd);};
  syncBuiltinESMExports();
  let second;
  try{second=await dispatcher.resetAgent('planner',{force:true,eagerRestart:true});}finally{fs.fsyncSync=sync;syncBuiltinESMExports();}
  fs.writeFileSync(path.join(input.directory,'outcome.json'),JSON.stringify({first,second,fault,replies,staleRelease,registryBefore,registryAfter,generations:fs.readFileSync(path.join(input.directory,'tasks/native-boundary/agent-generations.jsonl'),'utf8')}),{flag:'wx'});
 } else {
 for (const mode of input.modes) {
  fs.writeFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG,JSON.stringify({...input,mode}));
  try {const messages=await dispatcher.deliver(Msg.chat(0,{text:'native turn '+mode}));outcomes.push({ok:true,messages});}
  catch(e){outcomes.push({ok:false,code:e.code,message:e.message});}
 }
 fs.writeFileSync(path.join(input.directory,'outcome.json'),JSON.stringify({outcomes,replies,stats:manager.getStatus(dispatcher.sessionNames.planner).stats,plan:fs.existsSync(path.join(input.directory,'plan.md'))?fs.readFileSync(path.join(input.directory,'plan.md'),'utf8'):null}));
 }
} finally {shutdownStarted=true;runner?.stop();await dispatcher.shutdown('native-fixture-end',{purge:!input.scenario});await manager.shutdown();await Promise.all(observedChildren);}
`;

async function native(
  engine: string,
  model: string,
  modes: string[],
  reply = 'native reply',
  extra: { scenario?: string; boundary?: string; cold?: boolean; directory?: string } = {},
) {
  fs.mkdirSync(root, { recursive: true });
  const directory = extra.directory ?? fs.mkdtempSync(path.join(root, `${engine}-`));
  let reviewRequest;
  if (extra.scenario === 'review' && !extra.cold) {
    const before = execFileSync('rtk', ['proxy', 'git', 'rev-parse', 'HEAD'], {
      cwd: project,
      encoding: 'utf8',
    }).trim();
    const indexBefore = execFileSync('rtk', ['proxy', 'git', 'diff', '--cached', '--binary'], { cwd: project });
    execFileSync('rtk', ['proxy', 'git', 'clone', '--quiet', '--shared', '--no-checkout', project, directory]);
    execFileSync('rtk', ['proxy', 'git', 'checkout', '--quiet', '--detach', before], {
      cwd: directory,
      env: { ...process.env, GIT_CEILING_DIRECTORIES: path.dirname(directory) },
    });
    const patch = execFileSync(
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
        before,
        '--',
      ],
      { cwd: directory },
    );
    fs.writeFileSync(path.join(directory, 'source.patch'), patch, { flag: 'wx' });
    fs.writeFileSync(path.join(directory, 'checkpoint.txt'), before, { flag: 'wx' });
    // Observer-owned caller identity, persisted before any production request
    // preparation. The worker receives a serialized copy, never this oracle.
    reviewRequest = {
      checkpoint_sha: before,
      source_run_id: 'source-checkpoint',
      source_iter: 2,
      scope: ['durable-delivery'],
      idempotency_key: 'native-checkpoint-review',
    };
    const observation = fs.openSync(path.join(directory, 'independent-caller-request.json'), 'wx');
    try {
      fs.writeFileSync(observation, JSON.stringify({ target_iter: 1, request: reviewRequest }));
      fs.fsyncSync(observation);
    } finally {
      fs.closeSync(observation);
    }
    expect(execFileSync('rtk', ['proxy', 'git', 'rev-parse', 'HEAD'], { cwd: project, encoding: 'utf8' }).trim()).toBe(
      before,
    );
    expect(execFileSync('rtk', ['proxy', 'git', 'diff', '--cached', '--binary'], { cwd: project })).toEqual(
      indexBefore,
    );
  }
  const fixtureSource = path.join(project, `src/__tests__/fixtures/autoloop-trust-recovery/${engine}.mjs`);
  const fixture = path.join(directory, `${engine}.mjs`);
  if (!extra.cold) {
    fs.copyFileSync(fixtureSource, fixture);
    fs.chmodSync(fixture, 0o755);
  }
  const input = { project, directory, fixture, engine, model, modes, reply, reviewRequest, ...extra };
  const prefix = extra.cold ? 'cold-' : '';
  fs.writeFileSync(path.join(directory, prefix + 'worker.mjs'), worker, { flag: 'wx' });
  const env = {
    ...process.env,
    CLAWO_TRUST_SCRATCH: directory,
    CLAWO_TRUST_SHARED_HOME: path.join(directory, 'shared-home'),
    GIT_CEILING_DIRECTORIES: path.dirname(directory),
    CLAWO_TRUST_NATIVE_CONFIG: path.join(directory, 'config.json'),
    CLAWO_TRUST_NATIVE_CONFIG_INPUT: JSON.stringify(input),
    CODEX_BIN: fixture,
    AGY_BIN: fixture,
    CURSOR_BIN: fixture,
    NODE_OPTIONS: `--import tsx --import ${project}/src/__tests__/helpers/autoloop-trust-recovery.ts`,
  };
  const argv = ['proxy', process.execPath, '--import', 'tsx', '--input-type=module', '-'];
  const started = new Date().toISOString();
  const child = spawn('rtk', argv, { cwd: project, env, detached: true, stdio: ['pipe', 'pipe', 'pipe'] });
  const stdout: Buffer[] = [],
    stderr: Buffer[] = [];
  child.stdout.on('data', (bytes: Buffer) => stdout.push(bytes));
  child.stderr.on('data', (bytes: Buffer) => stderr.push(bytes));
  child.stdin.end(worker);
  let witnessed = false;
  const watch = setInterval(() => {
    if (!extra.cold && extra.boundary && fs.existsSync(path.join(directory, 'barrier.json'))) {
      witnessed = true;
      clearInterval(watch);
      process.kill(-child.pid!, 'SIGKILL');
    }
  }, 10);
  let timeout = false;
  const timer = setTimeout(() => {
    timeout = true;
    process.kill(-child.pid!, 'SIGKILL');
  }, 20000);
  const result = await new Promise<{ code: number | null; signal: string | null }>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ code, signal }));
  });
  clearTimeout(timer);
  clearInterval(watch);
  // Claude deliberately detaches its own process group. A worker-group crash
  // cannot terminate a CLI blocked inside the external receiver barrier.
  // Only signal PIDs whose captured invocation and live argv both prove that
  // they are this attempt's fixture, never another session or real provider.
  const cleaned: number[] = [];
  const nativeLog = path.join(directory, 'native.jsonl');
  if (fs.existsSync(nativeLog)) {
    const pids = new Set<number>(
      fs
        .readFileSync(nativeLog, 'utf8')
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line).pid),
    );
    const cleanupPids = pids;
    for (const pid of cleanupPids) {
      if (!pids.has(pid)) throw new Error(`Cleanup barrier PID ${pid} was not a captured fixture invocation`);
      let argv: string[];
      try {
        argv = fs.readFileSync(`/proc/${pid}/cmdline`, 'utf8').split('\0');
      } catch {
        continue;
      }
      if (argv.every((arg) => arg === '')) continue;
      if (!argv.includes(fixture)) throw new Error(`Fixture PID ${pid} no longer has the owned invocation`);
      try {
        process.kill(pid, 'SIGKILL');
        cleaned.push(pid);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
      }
    }
    const deadline = Date.now() + 5000;
    for (const pid of cleaned) {
      while (fs.existsSync(`/proc/${pid}/cmdline`)) {
        let bytes: Buffer;
        try {
          bytes = fs.readFileSync(`/proc/${pid}/cmdline`);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === 'ENOENT') break;
          throw error;
        }
        if (bytes.length === 0) break;
        if (Date.now() >= deadline) throw new Error(`Owned fixture ${pid} did not terminate`);
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
    }
  }
  const workerPid = JSON.parse(fs.readFileSync(path.join(directory, prefix + 'worker-process.json'), 'utf8')).pid;
  fs.writeFileSync(path.join(directory, prefix + 'stdout.txt'), Buffer.concat(stdout), { flag: 'wx' });
  fs.writeFileSync(path.join(directory, prefix + 'stderr.txt'), Buffer.concat(stderr), { flag: 'wx' });
  fs.writeFileSync(
    path.join(directory, prefix + 'execution.json'),
    JSON.stringify({
      input,
      argv: ['rtk', ...argv],
      started,
      ended: new Date().toISOString(),
      ...result,
      worker_pid: workerPid,
      timeout,
      witnessed,
      cleaned_fixture_pids: cleaned,
      fixture_sha256: createHash('sha256').update(fs.readFileSync(fixture)).digest('hex'),
      source_inputs: [
        'src/session-manager.ts',
        'src/autoloop/dispatcher.ts',
        'src/autoloop/runner.ts',
        'src/autoloop/secure-ledger.ts',
        'src/autoloop/planner-tools.ts',
        'src/autoloop/agent-tools.ts',
        engine === 'claude' ? 'src/persistent-session.ts' : `src/persistent-${engine}-session.ts`,
        'src/__tests__/helpers/autoloop-trust-recovery.ts',
      ].map((file) => ({
        path: path.join(project, file),
        sha256: createHash('sha256')
          .update(fs.readFileSync(path.join(project, file)))
          .digest('hex'),
      })),
      stdout_sha256: createHash('sha256').update(Buffer.concat(stdout)).digest('hex'),
      stderr_sha256: createHash('sha256').update(Buffer.concat(stderr)).digest('hex'),
      worker_sha256: createHash('sha256').update(worker).digest('hex'),
    }),
    { flag: 'wx' },
  );
  expect(timeout, directory).toBe(false);
  if (witnessed) {
    expect(result.signal).toBe('SIGKILL');
    for (const name of ['decisions.jsonl', 'agent-generations.jsonl'])
      fs.writeFileSync(
        path.join(directory, 'crash-' + name),
        fs.readFileSync(path.join(directory, 'tasks/native-boundary', name)),
        { flag: 'wx' },
      );
    return { directory, witnessed, cleaned_fixture_pids: cleaned };
  }
  expect(result.code, Buffer.concat(stderr).toString() + directory).toBe(0);
  return {
    directory,
    ...JSON.parse(fs.readFileSync(path.join(directory, prefix + 'outcome.json'), 'utf8')),
    invocations: fs
      .readFileSync(path.join(directory, 'native.jsonl'), 'utf8')
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line)),
  };
}

describe('Slice 3 authentic native protocol boundaries', () => {
  it('Cursor receiver reads --workspace artifacts when its permissions cwd is separate', () => {
    // Break caught: receiver opens relative files in Cursor's permission-only
    // cwd instead of the artifact workspace exposed by the native invocation.
    fs.mkdirSync(root, { recursive: true });
    const directory = fs.mkdtempSync(path.join(root, 'cursor-receiver-'));
    const workspace = path.join(directory, 'review-workspace');
    const cwd = path.join(directory, 'permissions-cwd');
    fs.mkdirSync(path.join(workspace, 'iter-1'), { recursive: true });
    fs.mkdirSync(cwd);
    const names = ['directive.json', 'diff.patch', 'eval_output.json', 'coder_summary.txt'];
    for (const name of names) fs.writeFileSync(path.join(workspace, 'iter-1', name), `source bytes for ${name}\n`);
    const config = path.join(directory, 'config.json');
    fs.writeFileSync(
      config,
      JSON.stringify({ project, directory, scenario: 'review', mode: 'success', reply: 'review' }),
    );
    const prompt = `<autoloop_delivery delivery_id="cursor-workspace" payload_sha256="${'a'.repeat(64)}">\n[review_request iter=1]\nArtifacts staged from run source-checkpoint iter 2 at: iter-1/\ncheckpoint_sha: ${'b'.repeat(40)}\nscope: ["durable-delivery"]\nprior_verdict: (none)\n`;
    execFileSync(
      'rtk',
      [
        'proxy',
        process.execPath,
        '--import',
        'tsx',
        path.join(project, 'src/__tests__/fixtures/autoloop-trust-recovery/cursor.mjs'),
        '--workspace',
        workspace,
        '-p',
        prompt,
      ],
      { cwd, env: { ...process.env, CLAWO_TRUST_NATIVE_CONFIG: config }, stdio: 'pipe' },
    );
    const receipt = JSON.parse(fs.readFileSync(path.join(directory, 'receiver-effects.jsonl'), 'utf8'));
    expect(receipt.review_inspection.cwd).toBe(cwd);
    expect(receipt.review_inspection.workspace).toBe(workspace);
    for (const name of names) {
      const bytes = Buffer.from(`source bytes for ${name}\n`);
      expect(receipt.review_inspection.artifacts[name]).toEqual({
        bytes_base64: bytes.toString('base64'),
        bytes: bytes.length,
        sha256: createHash('sha256').update(bytes).digest('hex'),
      });
    }
  });
  // Each case catches a lost physical identity/model selection or acceptance
  // of an incomplete/denied native turn by the real Planner dispatcher.
  for (const [engine, model, continuity] of [
    ['codex', 'gpt-6-astra', 'resume'],
    ['claude', 'haiku', 'stdin'],
    ['agy', 'gemini-3.8-flash-high', '--conversation'],
    ['cursor', 'auto', '--resume'],
  ]) {
    it(`${engine}: denied native turn retains continuity for a successful retry`, async () => {
      // Production break: denial destroys the physical conversation identity
      // or poisons the next turn rather than retaining retry context.
      const result = await native(engine, model, ['denied', 'success']);
      expect(result.outcomes[0]).toMatchObject({ ok: false });
      expect(result.outcomes[1]).toMatchObject({ ok: true });
      expect(result.replies).toEqual(['native reply']);
      if (continuity === 'stdin') expect(result.invocations[1].pid).toBe(result.invocations[0].pid);
      else expect(result.invocations[1].argv).toContain(continuity);
    });
    it(`${engine}: eager reset creates a replacement and rejects failed replacement persistence`, async () => {
      // Production break: eager reset reports success after only stopping the
      // prior generation, even though replacement reservation cannot persist.
      const result = await native(engine, model, [], 'native reply', { scenario: 'reset' });
      expect(result.first).toMatchObject({ ok: true, previous_generation: 1, active_generation: 2, reusable: true });
      expect(result.replies).toEqual(['native reply', 'native reply']);
      expect(result.fault).toBe(true);
      expect(result.staleRelease).toBe(false);
      expect(result.registryAfter).toBe(result.registryBefore);
      expect(result.second).toMatchObject({ ok: false, code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' });
      const rows = result.generations
        .trim()
        .split('\n')
        .map((line: string) => JSON.parse(line));
      expect(
        rows.filter(
          (row: { kind: string; payload: { generation: number } }) =>
            row.kind === 'agent_generation_released' && row.payload.generation === 2,
        ),
      ).toHaveLength(1);
      expect(
        rows.filter(
          (row: { kind: string; payload: { generation: number } }) =>
            row.kind === 'agent_generation_started' && row.payload.generation === 3,
        ),
      ).toHaveLength(0);
    });
    it(`${engine}: a completed Planner control persists real plan bytes`, async () => {
      // Production break: valid control claimed as successful but its effect
      // is discarded or content changed before durable artifact replacement.
      const result = await native(
        engine,
        model,
        ['success'],
        '```autoloop\n{"tool":"write_plan","args":{"content":"# verified plan\\n\\nExact bytes.\\n"}}\n```',
      );
      expect(result.outcomes[0]).toMatchObject({ ok: true });
      expect(result.plan).toBe('# verified plan\n\nExact bytes.\n');
    });
    for (const boundary of ['before-send', 'after-capture']) {
      it(`${engine}: independently recovers Reviewer-only ${boundary} into one source-bound verdict`, async () => {
        // Production break: reviewing by starting Coder, losing checkpoint
        // identity during preparation/recovery (including scope or source
        // iteration substitution), or duplicate request/ACK/verdict effects.
        const first = await native(engine, model, [], 'review existing checkpoint', { scenario: 'review', boundary });
        expect(first.witnessed).toBe(true);
        const callerObservationPath = path.join(first.directory, 'independent-caller-request.json');
        expect(fs.existsSync(callerObservationPath), 'caller identity must be captured before requestReview').toBe(
          true,
        );
        const caller = JSON.parse(fs.readFileSync(callerObservationPath, 'utf8'));
        expect(caller).toEqual({
          target_iter: 1,
          request: {
            checkpoint_sha: fs.readFileSync(path.join(first.directory, 'checkpoint.txt'), 'utf8').trim(),
            source_run_id: 'source-checkpoint',
            source_iter: 2,
            scope: ['durable-delivery'],
            idempotency_key: 'native-checkpoint-review',
          },
        });
        // Independent observer captures immutable source bytes before recovery,
        // never from the receiver or from its staged destination.
        const sourceArtifacts = Object.fromEntries(
          ['directive.json', 'diff.patch', 'eval_output.json', 'coder_summary.txt'].map((name) => {
            const bytes = fs.readFileSync(path.join(first.directory, 'tasks/source-checkpoint/iter/2', name));
            return [
              name,
              {
                bytes_base64: bytes.toString('base64'),
                bytes: bytes.length,
                sha256: createHash('sha256').update(bytes).digest('hex'),
              },
            ];
          }),
        );
        const priorBytes =
          boundary === 'after-capture'
            ? fs.readFileSync(path.join(first.directory, 'tasks/native-boundary/iter/0/verdict.json'))
            : undefined;
        const expectedPrior = priorBytes
          ? {
              bytes_base64: priorBytes.toString('base64'),
              bytes: priorBytes.length,
              sha256: createHash('sha256').update(priorBytes).digest('hex'),
            }
          : null;
        fs.writeFileSync(
          path.join(first.directory, 'independent-source-observation.json'),
          JSON.stringify({ sourceArtifacts, prior: expectedPrior }),
          { flag: 'wx' },
        );
        const result = await native(engine, model, [], 'review existing checkpoint', {
          scenario: 'review',
          boundary,
          cold: true,
          directory: first.directory,
        });
        const preparations = JSON.parse(
          fs.readFileSync(path.join(first.directory, 'review-preparations.json'), 'utf8'),
        );
        expect(preparations.map((row: { status: string }) => row.status).sort()).toEqual(['duplicate', 'prepared']);
        const prepared = preparations.find((row: { status: string }) => row.status === 'prepared');
        expect(prepared.payload, 'prepared identity must preserve the original caller request').toMatchObject({
          ...caller.request,
          iter: caller.target_iter,
        });
        for (const preparation of preparations)
          expect(preparation.idempotency_key).toBe(caller.request.idempotency_key);
        const rows = result.rows
          .trim()
          .split('\n')
          .map((line: string) => JSON.parse(line));
        expect(rows.filter((row: { acknowledged_at?: string }) => row.acknowledged_at)).toHaveLength(1);
        const decisions = rows.filter((row: { kind: string }) => row.kind === 'request_review');
        expect(decisions).toHaveLength(1);
        expect(decisions[0].payload).toMatchObject({ ...caller.request, target_iter: caller.target_iter });
        const generations = fs
          .readFileSync(path.join(first.directory, 'tasks/native-boundary/agent-generations.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line));
        const successor = generations.find(
          (row) =>
            row.payload.role === 'reviewer' && row.payload.generation === 2 && row.kind === 'agent_generation_started',
        );
        const rebind = rows.find((row: { record_type?: string }) => row.record_type === 'delivery_generation_rebind');
        expect(
          Date.parse(successor.ts),
          'outbox and generation timestamps must share the cold clock',
        ).toBeLessThanOrEqual(Date.parse(rebind.rebound_at));
        expect(generations.filter((row) => row.payload.role === 'coder')).toHaveLength(0);
        expect(
          generations.filter((row) => row.payload.role === 'reviewer' && row.kind === 'agent_generation_started')
            .length,
        ).toBeGreaterThan(0);
        const verdict = JSON.parse(
          fs.readFileSync(path.join(first.directory, 'tasks/native-boundary/iter/1/verdict.json'), 'utf8'),
        );
        expect(verdict).toMatchObject({ iter: 1, decision: 'hold' });
        const received = fs
          .readFileSync(path.join(first.directory, 'receiver-effects.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line));
        expect(received).toHaveLength(1);
        const inspection = received[0].review_inspection;
        expect(inspection, 'receiver must open staged artifacts before review_complete').toBeDefined();
        expect(inspection.artifacts).toEqual(sourceArtifacts);
        expect(inspection.prior_verdict).toEqual(expectedPrior);
        expect(inspection.request).toEqual({
          target_iter: caller.target_iter,
          source_run_id: caller.request.source_run_id,
          source_iter: caller.request.source_iter,
          checkpoint_sha: caller.request.checkpoint_sha,
          scope: caller.request.scope,
        });
        expect(inspection.workspace).toBe(path.join(first.directory, 'tasks/native-boundary/reviewer_sandbox'));
        const intent = rows.find((row: { delivery_id?: string }) => row.delivery_id === received[0].delivery_id);
        expect(intent).toBeDefined();
        expect(received[0].payload_sha256).toBe(intent.payload_sha256);
        // The durable transport payload must carry the caller identity, and
        // its ACK must bind that exact payload and the observed recipient.
        expect(intent.payload.prompt).toContain(`[review_request iter=${caller.target_iter}]`);
        expect(intent.payload.prompt).toContain(
          `Artifacts staged from run ${caller.request.source_run_id} iter ${caller.request.source_iter} at:`,
        );
        expect(intent.payload.prompt).toContain(`checkpoint_sha: ${caller.request.checkpoint_sha}\n`);
        expect(intent.payload.prompt).toContain(`scope: ${JSON.stringify(caller.request.scope)}\n`);
        expect(createHash('sha256').update(JSON.stringify(intent.payload)).digest('hex')).toBe(intent.payload_sha256);
        const ack = rows.find((row: { acknowledged_at?: string }) => row.acknowledged_at);
        expect(ack).toMatchObject({ delivery_id: received[0].delivery_id, payload_sha256: intent.payload_sha256 });
        const receipts = fs
          .readFileSync(path.join(first.directory, 'recipient.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line));
        expect(receipts).toHaveLength(boundary === 'after-capture' ? 2 : 1);
        for (const receipt of receipts) {
          expect(receipt.review_inspection.request).toEqual(inspection.request);
          expect(receipt.review_inspection.artifacts).toEqual(sourceArtifacts);
          expect(receipt.review_inspection.prior_verdict).toEqual(expectedPrior);
          expect(receipt.delivery_id).toBe(ack.delivery_id);
          expect(receipt.payload_sha256).toBe(ack.payload_sha256);
          expect(receipt.prompt).toBe(received[0].prompt);
        }
        const invocation = result.invocations.find((row: { pid: number }) => row.pid === received[0].pid);
        if (engine === 'cursor') {
          // Reviewer currently uses its sandbox cwd; read-only Cursor turns
          // may instead use a distinct permissions cwd and --workspace.
          expect(invocation.argv[invocation.argv.indexOf('--workspace') + 1]).toBe(inspection.workspace);
          expect(invocation.cwd).toBe(inspection.cwd);
        }
        expect(received[0].prompt).toContain(
          fs.readFileSync(path.join(first.directory, 'checkpoint.txt'), 'utf8').trim(),
        );
      });
    }
    for (const boundary of ['before-send', 'after-capture']) {
      it(`${engine}: cold replay after ${boundary} preserves native directive bytes and acknowledges one logical effect`, async () => {
        // Production break: transport before durable intent, losing original
        // bytes on cold reconstruction, or replay consuming a second effect.
        const reply =
          '```autoloop\n{"tool":"send_directive","args":{"goal":"directive A exact bytes","constraints":["preserve A"],"success_criteria":["one effect"],"max_attempts":1}}\n```';
        const first = await native(engine, model, [], reply, { scenario: 'delivery', boundary });
        expect(first.witnessed).toBe(true);
        if (engine === 'claude' && boundary === 'after-capture') {
          const barrier = JSON.parse(fs.readFileSync(path.join(first.directory, 'barrier.json'), 'utf8'));
          const warmPids = fs
            .readFileSync(path.join(first.directory, 'native-spawns.jsonl'), 'utf8')
            .trim()
            .split('\n')
            .map((line) => JSON.parse(line))
            .filter((row) => row.phase === 'warm')
            .map((row) => row.pid);
          // The worker-group crash cannot reach Claude's detached children.
          // Every captured warm child is therefore cleaned by exact PID after
          // its live argv is re-authenticated, including the barrier PID.
          expect(first.cleaned_fixture_pids).toEqual(warmPids);
          expect(first.cleaned_fixture_pids).toContain(barrier.pid);
        }
        const before = fs.readFileSync(path.join(first.directory, 'tasks/native-boundary/decisions.jsonl'));
        const final = await native(engine, model, [], reply, {
          scenario: 'delivery',
          boundary,
          cold: true,
          directory: first.directory,
        });
        expect(Buffer.from(final.rows).subarray(0, before.length)).toEqual(before);
        const rows = final.rows
          .trim()
          .split('\n')
          .map((line: string) => JSON.parse(line));
        expect(rows.filter((row: { acknowledged_at?: string }) => row.acknowledged_at)).toHaveLength(1);
        const effects = fs
          .readFileSync(path.join(first.directory, 'receiver-effects.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line));
        expect(effects).toHaveLength(1);
        expect(effects[0].prompt).toContain('directive A exact bytes');
        const received = fs
          .readFileSync(path.join(first.directory, 'recipient.jsonl'), 'utf8')
          .trim()
          .split('\n')
          .map((line) => JSON.parse(line));
        expect(new Set(received.map((row) => row.delivery_id)).size).toBe(1);
        expect(new Set(received.map((row) => row.prompt)).size).toBe(1);
      });
    }
    it(`${engine}: completes two nonempty turns with native continuity and selected model`, async () => {
      const result = await native(engine, model, ['success', 'success']);
      expect(result.outcomes).toEqual([
        { ok: true, messages: [] },
        { ok: true, messages: [] },
      ]);
      expect(result.replies).toEqual(['native reply', 'native reply']);
      expect(result.invocations).toHaveLength(2);
      const args = result.invocations[0].argv;
      expect(args[args.indexOf('--model') + 1]).toBe(engine === 'claude' ? 'claude-haiku-4-5' : model);
      if (continuity === 'stdin') {
        expect(result.invocations[1].pid).toBe(result.invocations[0].pid);
        expect(JSON.parse(result.invocations[1].stdin).type).toBe('user');
      } else expect(result.invocations[1].argv).toContain(continuity);
      if (engine === 'cursor') {
        for (const invocation of result.invocations) {
          expect(invocation.argv).not.toContain('--force');
          expect(invocation.permission_config).toEqual({
            permissions: { allow: [], deny: ['Write(**)', 'Edit(**)', 'Shell(**)'] },
          });
          expect(invocation.cwd).not.toBe(result.directory);
        }
      }
    });
    for (const mode of ['empty', 'partial', 'protocol', 'process', 'denied']) {
      it(`${engine}: ${mode} cannot execute a claimed Planner control`, async () => {
        const reply = '```autoloop\n{"tool":"write_plan","args":{"content":"# must not persist"}}\n```';
        const result = await native(engine, model, [mode], reply);
        expect(result.outcomes[0], result.directory).toMatchObject({ ok: false });
        expect(result.plan).toBe(null);
      });
    }
  }
  it('AGY empty soft denial retains its conversation for retry', async () => {
    // Production break: an empty STOPPED result is accepted or its ID lost.
    const result = await native('agy', 'gemini-3.8-flash-high', ['denied-empty', 'success']);
    expect(result.outcomes[0]).toMatchObject({ ok: false });
    expect(result.outcomes[1]).toMatchObject({ ok: true });
    expect(result.invocations[1].argv).toContain('--conversation');
  });
  it('does not persist a Codex partial control without a terminal completion, and retains its thread for retry', async () => {
    // Production break: exit zero plus assistant text, but no turn.completed,
    // is mistaken for a completed Planner turn and executes a partial control.
    const reply = '```autoloop\n{"tool":"write_plan","args":{"content":"# completed plan"}}\n```';
    const result = await native('codex', 'gpt-6-astra', ['partial'], reply);
    expect(result.outcomes[0], result.directory).toMatchObject({ ok: false });
    expect(result.plan).toBe(null);
    expect(result.stats.codexThreadId).toBe('019c6dcb-93ad-7dc1-b531-418d213b8761');
    expect(result.stats.turnsSucceeded).toBe(0);
  });
});
