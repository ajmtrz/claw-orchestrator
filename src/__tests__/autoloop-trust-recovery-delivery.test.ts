import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const project = fileURLToPath(new URL('../../', import.meta.url));
const artifactRoot = path.join(project, '.artifacts/CLAWO-AUTOLOOP-DURABLE-RECOVERY-TRUST-RECOVERY-R1');
const digest = (bytes: string | Buffer) => createHash('sha256').update(bytes).digest('hex');

// The external engine port records recipient bytes and echoes identity. The
// runner, manager, registry, dispatcher, outbox and sync barriers are real.
// No fixture writes a production intent, ACK, generation or recovery receipt.
const worker = String.raw`
import fs from 'node:fs';
import path from 'node:path';
import { EventEmitter } from 'node:events';
import { randomUUID, createHash } from 'node:crypto';
import { syncBuiltinESMExports } from 'node:module';
import { pathToFileURL } from 'node:url';
const input=JSON.parse(process.env.CLAWO_TRUST_DELIVERY_INPUT);
const sources=[];
const moduleAt=async(file)=>{
  const source=path.join(input.project,'src',file);
  const sha256=createHash('sha256').update(fs.readFileSync(source)).digest('hex');
  const imported=await import(pathToFileURL(source).href);
  sources.push({path:source,sha256});return imported;
};
const { SessionManager }=await moduleAt('session-manager.ts');
const { ClaudeAgentDispatcher }=await moduleAt('autoloop/dispatcher.ts');
const { AutoloopRunner }=await moduleAt('autoloop/runner.ts');
const { Msg }=await moduleAt('autoloop/messages.ts');
const { nullLogger }=await moduleAt('logger.ts');
const { SecureAutoloopLedger }=await moduleAt('autoloop/secure-ledger.ts');
const root=input.directory, workspace=path.join(root,'workspace'), runId='trust-delivery';
fs.mkdirSync(workspace,{recursive:true});
const now=Date.parse('2026-09-14T12:00:00.000Z')+(input.cold?300000:0);
Date.now=()=>now;
const output=path.join(root,input.id);fs.mkdirSync(output);
const decisions=path.join(workspace,'tasks',runId,'decisions.jsonl');
const events=[];
const record=(kind,value)=>{const event={sequence:events.length,process_id:process.pid,kind,value};events.push(event);fs.appendFileSync(path.join(output,'events.jsonl'),JSON.stringify(event)+'\n');};
record('source-imports',sources);
const rows=()=>fs.existsSync(decisions)?fs.readFileSync(decisions,'utf8').trim().split('\n').filter(Boolean).map(JSON.parse):[];
const snapshot=(label)=>{for(const [key,file] of Object.entries({decisions,generations:path.join(workspace,'tasks',runId,'agent-generations.jsonl')})){if(fs.existsSync(file))fs.writeFileSync(path.join(output,label+'-'+key+'.jsonl'),fs.readFileSync(file),{flag:'wx'});}};
const barrier=(label)=>{snapshot(label);record('barrier',label);process.stdout.write(JSON.stringify({barrier:label})+'\n');Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0);};
const originalSync=fs.fsyncSync;
fs.fsyncSync=(fd)=>{if(input.fault==='fsync'&&fs.readlinkSync('/proc/self/fd/'+fd)===decisions&&fs.readFileSync(decisions,'utf8').includes('coder_directive')){record('injected-fsync-failure',decisions);throw new Error('injected intent fsync failure');}originalSync(fd);let target;try{target=fs.readlinkSync('/proc/self/fd/'+fd);}catch{};if(target===decisions||target===path.dirname(decisions))record('fsync-return',target);};
syncBuiltinESMExports();
let issued=false;
class EnginePort extends EventEmitter {
  sessionId=randomUUID();isReady=true;isPaused=false;isBusy=false;turns=0;
  constructor(config){super();this.config=config;record('engine-created',{name:config.name,session_id:this.sessionId});}
  async start(){return this;}
  stop(){this.isReady=false;}
  getStats(){return {turns:this.turns,turnsSucceeded:this.turns,toolCalls:0,toolErrors:0,tokensIn:0,tokensOut:0,cachedTokens:0,costUsd:0,isReady:this.isReady,startTime:new Date(now).toISOString(),lastActivity:new Date(now).toISOString(),contextPercent:0,retries:0,sessionId:this.sessionId,uptime:0};}
  getCost(){return {model:'fixture',tokensIn:0,tokensOut:0,cachedTokens:0,totalUsd:0};}
  getHistory(){return [];}
  resolveModel(x){return x;}
  async send(prompt){
    this.turns++;
    const role=this.config.name.split('-').at(-1);
    let reply='acknowledged';
    if(role==='planner'&&!input.cold&&!issued){issued=true;reply='\x60\x60\x60autoloop\n'+JSON.stringify({tool:'send_directive',args:{goal:'directive A: preserve these exact bytes',constraints:['immutable A'],success_criteria:['one logical effect'],max_attempts:1}})+'\n\x60\x60\x60';}
    if(role==='coder'||role==='reviewer'){
      const match=/<autoloop_delivery delivery_id="([^"]+)" payload_sha256="([a-f0-9]{64})">/.exec(prompt);
      if(!match)throw new Error('Missing transport identity');
      record('transport-enter',{delivery_id:match[1],payload_sha256:match[2]});
      snapshot('transport-'+this.turns);
      if(!input.cold&&input.boundary==='before-send')barrier('before-send');
      const received={delivery_id:match[1],payload_sha256:match[2],prompt,role,session_id:this.sessionId,process_id:process.pid};
      fs.appendFileSync(path.join(root,'recipient.jsonl'),JSON.stringify(received)+'\n');
      // This fixture has an explicit durable idempotency store. Its logical
      // effect is separate from physical retransmission, which is retained.
      const effects=path.join(root,'receiver-effects.jsonl');
      const seen=fs.existsSync(effects)?fs.readFileSync(effects,'utf8').trim().split('\n').filter(Boolean).map(JSON.parse):[];
      if(!seen.some(x=>x.delivery_id===received.delivery_id)){
        const fd=fs.openSync(effects,'a');fs.writeSync(fd,JSON.stringify(received)+'\n');fs.fsyncSync(fd);fs.closeSync(fd);
      }
      record('recipient-captured',received);
      if(!input.cold&&input.boundary==='after-capture')barrier('after-capture');
      const identity={delivery_id:match[1],payload_sha256:input.fault==='digest'?'0'.repeat(64):match[2]};
      reply='clarification for A\n\x60\x60\x60autoloop\n'+JSON.stringify(role==='coder'?{tool:'request_clarification',args:{question:'clarify A',...identity}}:{tool:'review_complete',args:{decision:'hold',metric:null,audit_notes:'checkpoint inspected',...identity}})+'\n\x60\x60\x60';
    }
    return {text:reply,event:{type:'result',result:reply}};
  }
}
const manager=new SessionManager({maxConcurrentSessions:3},nullLogger);
manager._createSession=(_engine,config)=>new EnginePort(config);
const dispatcher=new ClaudeAgentDispatcher({manager,runId,workspace,logger:nullLogger,now:()=>new Date(now),agentLeaseMs:1000});
if(!input.cold&&input.boundary==='after-ack'){
  const acknowledge=dispatcher.acknowledgeDurableDelivery.bind(dispatcher);
  dispatcher.acknowledgeDurableDelivery=(intent)=>{const result=acknowledge(intent);barrier('after-ack');return result;};
}
const runner=new AutoloopRunner({run_id:runId,workspace,ledger_dir:path.dirname(decisions),dispatcher,notifyUser:async(...args)=>record('notification',args)});
runner.on('error',e=>record('runner-error',{message:e.message,code:e.code}));
runner.on('message',message=>{
  record('runner-message',message);
  if(message.type==='directive'&&!input.cold&&!fs.existsSync(path.join(root,'message-A.json')))fs.writeFileSync(path.join(root,'message-A.json'),JSON.stringify(message),{flag:'wx'});
  if(message.type==='directive_ack')record('ack-consumed',{message,ack_rows:rows().filter(row=>row.acknowledged_at)});
});
try{
 if(input.review&&!input.cold){
   const source=SecureAutoloopLedger.open(workspace,'source-checkpoint',{create:true});
   source.writeIterationArtifact(0,'directive.json','{"schema_version":1,"goal":"review checkpoint"}\n');
   source.writeIterationArtifact(0,'eval_output.json','{"schema_version":1,"iter":0,"eval_output":{}}\n');
   source.writeIterationArtifact(0,'coder_summary.txt','existing checkpoint\n');
   source.writeIterationArtifact(0,'diff.patch',fs.readFileSync(path.join(root,'source.patch')));
   const request={checkpoint_sha:input.checkpoint,source_run_id:'source-checkpoint',source_iter:0,scope:['durable-delivery'],idempotency_key:'review-existing-checkpoint'};
   const preparations=await Promise.all([dispatcher.requestReview(request,1),dispatcher.requestReview(request,1)]);
   record('review-preparations',preparations);
   const prepared=preparations.find(x=>x.status==='prepared');
   const message=Msg.reviewRequest(1,prepared.payload);
   fs.writeFileSync(path.join(root,'message-A.json'),JSON.stringify(message),{flag:'wx'});
   await runner.send(message,{requireRootDelivery:true});
 }else if(input.cold){
   const original=JSON.parse(fs.readFileSync(path.join(root,'message-A.json')));
   const conflicting={...original,msg_id:'distinct-directive-B',payload:{...original.payload,goal:'directive B: must never replace A'}};
   try{await runner.send(conflicting,{requireRootDelivery:true});}catch(e){record('B-rejected',{message:e.message,code:e.code});}
   await Promise.all([runner.send(original,{requireRootDelivery:true}),runner.send(original,{requireRootDelivery:true})]);
 }else{await runner.start();await runner.send(Msg.chat(0,{text:'Planner: issue directive A'}),{requireRootDelivery:true});}
 snapshot('final');record('final-state',runner.state);
}catch(e){snapshot('failure');record('failure',{message:e.message,code:e.code});process.exitCode=2;}
finally{runner.stop();await dispatcher.shutdown('fixture-end');await manager.shutdown();}
process.stdout.write(JSON.stringify({done:true,exit:process.exitCode??0})+'\n');
`;

async function execute(directory: string, id: string, boundary: string, cold = false, fault?: string, review = false) {
  const input = {
    project: process.env.CLAWO_TRUST_DELIVERY_SUBJECT ?? project,
    directory,
    id,
    boundary,
    cold,
    fault,
    review,
    checkpoint: review ? fs.readFileSync(path.join(directory, 'checkpoint.txt'), 'utf8').trim() : undefined,
  };
  const argv = ['proxy', process.execPath, '--import', 'tsx', '--input-type=module', '-'];
  const started = new Date().toISOString();
  const child = spawn('rtk', argv, {
    cwd: project,
    detached: true,
    env: {
      ...process.env,
      CLAWO_TRUST_DELIVERY_INPUT: JSON.stringify(input),
      CLAWO_TRUST_SHARED_HOME: path.join(directory, 'home'),
    },
    stdio: ['pipe', 'pipe', 'pipe'],
  });
  const stdout: Buffer[] = [],
    stderr: Buffer[] = [];
  let witnessed = false;
  child.stdout.on('data', (bytes: Buffer) => {
    stdout.push(bytes);
    if (!cold && Buffer.concat(stdout).toString().includes(`"barrier":"${boundary}"`)) {
      witnessed = true;
      process.kill(-child.pid!, 'SIGKILL');
    }
  });
  child.stderr.on('data', (bytes: Buffer) => stderr.push(bytes));
  child.stdin.end(worker);
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    process.kill(-child.pid!, 'SIGKILL');
  }, 25000);
  const result = await new Promise<{ code: number | null; signal: string | null }>((resolve, reject) => {
    child.on('error', reject);
    child.on('close', (code, signal) => resolve({ code, signal }));
  });
  clearTimeout(timer);
  const out = Buffer.concat(stdout),
    err = Buffer.concat(stderr);
  fs.writeFileSync(path.join(directory, `${id}.stdout.txt`), out, { flag: 'wx' });
  fs.writeFileSync(path.join(directory, `${id}.stderr.txt`), err, { flag: 'wx' });
  fs.writeFileSync(
    path.join(directory, `${id}.execution.json`),
    JSON.stringify({
      argv: ['rtk', ...argv],
      input,
      started_at: started,
      ended_at: new Date().toISOString(),
      ...result,
      witnessed,
      timedOut,
      stdout_sha256: digest(out),
      stderr_sha256: digest(err),
    }),
    { flag: 'wx' },
  );
  return { ...result, witnessed, stderr: err.toString() };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const readRows = (file: string): Record<string, any>[] =>
  fs.existsSync(file)
    ? fs
        .readFileSync(file, 'utf8')
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line))
    : [];

function scenario(name: string) {
  const root = process.env.CLAWO_TRUST_CASE_ROOT ?? path.join(artifactRoot, 'evidence/candidate/slice2-delivery-tests');
  if (!path.resolve(root).startsWith(artifactRoot + path.sep)) throw new Error('Unsafe delivery evidence path');
  fs.mkdirSync(root, { recursive: true });
  const directory = fs.mkdtempSync(path.join(root, `${name}-`));
  fs.writeFileSync(path.join(directory, 'worker.mjs'), worker, { flag: 'wx' });
  return directory;
}

describe('Slice 2 real-process delivery boundaries', () => {
  for (const boundary of ['before-send', 'after-capture', 'after-ack']) {
    it(`cold-recovers exact A after SIGKILL ${boundary} with one receiver effect and durable ACK before consumption`, async () => {
      const directory = scenario(boundary);
      const first = await execute(directory, 'crashed', boundary);
      expect(first.witnessed, first.stderr).toBe(true);
      expect(first.signal).toBe('SIGKILL');
      const saved = path.join(directory, 'crashed', `${boundary}-decisions.jsonl`);
      const before = fs.readFileSync(saved);
      const initial = readRows(saved);
      const intent = initial.find((row) => row.kind === 'coder_directive')!;
      expect(intent).toMatchObject({ schema_version: 1, target_role: 'coder', target_generation: 1 });
      expect(intent.payload_sha256).toMatch(/^[a-f0-9]{64}$/);
      expect(initial.filter((row) => row.acknowledged_at)).toHaveLength(boundary === 'after-ack' ? 1 : 0);
      const resumed = await execute(directory, 'cold', boundary, true);
      expect(resumed.code, resumed.stderr).toBe(0);
      expect(fs.readFileSync(saved)).toEqual(before);
      const final = readRows(path.join(directory, 'cold/final-decisions.jsonl'));
      expect(fs.readFileSync(path.join(directory, 'cold/final-decisions.jsonl')).subarray(0, before.length)).toEqual(
        before,
      );
      expect(final.filter((row) => row.kind === 'coder_directive' && !row.record_type)).toEqual([intent]);
      expect(final.filter((row) => row.acknowledged_at)).toHaveLength(1);
      const received = readRows(path.join(directory, 'recipient.jsonl'));
      expect(received.length).toBe(boundary === 'after-capture' ? 2 : 1);
      expect(new Set(received.map((row) => row.prompt)).size).toBe(1);
      expect(
        received.every((row) => row.delivery_id === intent.delivery_id && row.payload_sha256 === intent.payload_sha256),
      ).toBe(true);
      expect(received.every((row) => !row.prompt.includes('directive B:'))).toBe(true);
      expect(readRows(path.join(directory, 'receiver-effects.jsonl'))).toHaveLength(1);
      const events = readRows(path.join(directory, 'cold/events.jsonl'));
      const consumed = events.filter((row) => row.kind === 'ack-consumed');
      expect(consumed.length).toBeGreaterThan(0);
      expect(consumed.every((row) => row.value.ack_rows.length === 1)).toBe(true);
      const rebinds = final.filter((row) => row.record_type === 'delivery_generation_rebind');
      expect(rebinds).toHaveLength(boundary === 'after-ack' ? 0 : 1);
    }, 30000);
  }

  for (const fault of ['target', 'torn']) {
    it(`blocks cold delivery with ${fault} ledger evidence before recipient effects`, async () => {
      const directory = scenario(fault);
      expect((await execute(directory, 'crashed', 'before-send')).witnessed).toBe(true);
      const file = path.join(directory, 'workspace/tasks/trust-delivery/decisions.jsonl');
      if (fault === 'torn') fs.appendFileSync(file, '{"schema_version":');
      else {
        const rows = readRows(file);
        rows.find((row) => row.kind === 'coder_directive')!.target_role = 'reviewer';
        fs.writeFileSync(file, rows.map((row) => JSON.stringify(row) + '\n').join(''));
      }
      const before = fs.readFileSync(file);
      fs.writeFileSync(path.join(directory, 'altered-input.jsonl'), before, { flag: 'wx' });
      const cold = await execute(directory, 'cold', 'none', true);
      expect(cold.code, cold.stderr).toBe(2);
      expect(readRows(path.join(directory, 'recipient.jsonl'))).toEqual([]);
      expect(fs.readFileSync(file).subarray(0, before.length)).toEqual(before);
    }, 30000);
  }

  it('keeps a failed intent persistence from transport and ACK consumption', async () => {
    const directory = scenario('intent-fsync');
    await execute(directory, 'failed', 'none', false, 'fsync');
    expect(readRows(path.join(directory, 'recipient.jsonl'))).toEqual([]);
    expect(
      readRows(path.join(directory, 'failed/events.jsonl')).some((row) => row.kind === 'injected-fsync-failure'),
    ).toBe(true);
    const rows = readRows(path.join(directory, 'workspace/tasks/trust-delivery/decisions.jsonl'));
    expect(rows.some((row) => row.kind === 'coder_directive')).toBe(true);
    expect(rows.filter((row) => row.acknowledged_at)).toEqual([]);
    expect(readRows(path.join(directory, 'failed/events.jsonl')).filter((row) => row.kind === 'ack-consumed')).toEqual(
      [],
    );
  }, 30000);

  it('cold-replays an acknowledged existing checkpoint review without a Coder or another review effect', async () => {
    const directory = scenario('review-checkpoint');
    const workspace = path.join(directory, 'workspace');
    const checkpoint = execFileSync('rtk', ['proxy', 'git', 'rev-parse', 'HEAD'], {
      cwd: project,
      encoding: 'utf8',
    }).trim();
    // A local checkout of an existing immutable commit; tests create no commit.
    execFileSync('rtk', ['proxy', 'git', 'clone', '--quiet', '--shared', '--no-checkout', project, workspace]);
    execFileSync('rtk', ['proxy', 'git', 'checkout', '--quiet', '--detach', checkpoint], { cwd: workspace });
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
        checkpoint,
        '--',
      ],
      { cwd: workspace },
    );
    fs.writeFileSync(path.join(directory, 'source.patch'), patch, { flag: 'wx' });
    fs.writeFileSync(path.join(directory, 'checkpoint.txt'), checkpoint, { flag: 'wx' });
    const first = await execute(directory, 'crashed', 'after-ack', false, undefined, true);
    expect(first.witnessed, first.stderr).toBe(true);
    expect(first.signal).toBe('SIGKILL');
    const verdictPath = path.join(workspace, 'tasks/trust-delivery/iter/1/verdict.json');
    const before = fs.readFileSync(verdictPath);
    const result = await execute(directory, 'cold', 'none', true, undefined, true);
    expect(result.code, result.stderr).toBe(0);
    expect(fs.readFileSync(verdictPath)).toEqual(before);
    expect(JSON.parse(before.toString())).toMatchObject({ iter: 1, decision: 'hold' });
    const captured = readRows(path.join(directory, 'recipient.jsonl'));
    expect(captured).toHaveLength(1);
    expect(captured[0].role).toBe('reviewer');
    expect(captured[0].prompt).toContain(checkpoint);
    const events = ['crashed', 'cold'].flatMap((id) => readRows(path.join(directory, id, 'events.jsonl')));
    expect(events.filter((row) => row.kind === 'engine-created' && row.value.name.endsWith('-coder'))).toEqual([]);
    expect(
      events
        .find((row) => row.kind === 'review-preparations')!
        .value.map((item: { status: string }) => item.status)
        .sort(),
    ).toEqual(['duplicate', 'prepared']);
    const decisions = readRows(path.join(workspace, 'tasks/trust-delivery/decisions.jsonl'));
    expect(
      decisions.filter((row) => row.kind === 'review_request' && !row.record_type && row.delivery_id),
    ).toHaveLength(1);
    expect(decisions.filter((row) => row.acknowledged_at)).toHaveLength(1);
  }, 30000);

  it('rejects receiver digest mismatch without ACK or phase consumption', async () => {
    const directory = scenario('digest-mismatch');
    await execute(directory, 'failed', 'none', false, 'digest');
    const rows = readRows(path.join(directory, 'workspace/tasks/trust-delivery/decisions.jsonl'));
    expect(rows.some((row) => row.kind === 'coder_directive')).toBe(true);
    expect(rows.filter((row) => row.acknowledged_at)).toHaveLength(0);
    expect(
      readRows(path.join(directory, 'failed/events.jsonl')).filter((row) => row.kind === 'ack-consumed'),
    ).toHaveLength(0);
  }, 30000);
});
