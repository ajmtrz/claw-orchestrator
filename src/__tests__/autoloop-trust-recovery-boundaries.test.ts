import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

const project = fileURLToPath(new URL('../../', import.meta.url));
const root = path.join(
  project,
  '.artifacts/CLAWO-AUTOLOOP-DURABLE-RECOVERY-TRUST-RECOVERY-R1/evidence/candidate/slice3-public',
);
const worker = String.raw`
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {syncBuiltinESMExports} from 'node:module';
const input=JSON.parse(process.env.CLAWO_TRUST_NATIVE_CONFIG_INPUT);
const keepAlive=setInterval(()=>{},1000);
const {SessionManager}=await import(input.project+'/src/session-manager.ts');
const {nullLogger}=await import(input.project+'/src/logger.ts');
const manager=new SessionManager({maxConcurrentSessions:3},nullLogger);
const runId='public-boundary';
const outcome={};
let server,services=[];
const tools=new Map();
const unwrap=result=>result.structuredContent??JSON.parse(result.content[0].text);
const invoke=async(name,args)=>unwrap(await tools.get(name).execute('fixture-call',args));
fs.writeFileSync(process.env.CLAWO_TRUST_NATIVE_CONFIG,JSON.stringify({...input,mode:input.mode??'success'}));
const sync=fs.fsyncSync;
let fault=false;
if(input.fault==='persistence') {
 fs.fsyncSync=fd=>{const target=fs.readlinkSync('/proc/self/fd/'+fd);if(target.endsWith('/decisions.jsonl')&&fs.readFileSync(target,'utf8').includes('planner_control_')){fault=true;throw new Error('fixture control fsync rejection');}sync(fd);};
 syncBuiltinESMExports();
}
try {
 if(input.surface==='mcp') {
  const {default:plugin}=await import(input.project+'/src/index.ts');
  plugin.register({pluginConfig:{},logger:nullLogger,registerTool:def=>tools.set(def.name,def),registerHttpRoute:()=>{},registerService:def=>services.push(def),on:()=>{}});
  outcome.start=await invoke('autoloop_start',{run_id:runId,workspace:input.directory,planner_engine:'codex',planner_model:'gpt-6-astra'});
  outcome.response=await invoke('autoloop_chat',{run_id:runId,text:input.text??'perform the requested control'});
  outcome.state=await invoke('autoloop_status',{run_id:runId});
 } else {
  outcome.start=await manager.autoloopStart({runId,workspace:input.directory,plannerEngine:'codex',plannerModel:'gpt-6-astra'});
  if(input.surface==='http') {
   const {EmbeddedServer}=await import(input.project+'/src/embedded-server.ts');
   const net=await import('node:net');
   const free=await new Promise((resolve,reject)=>{const probe=net.createServer();probe.once('error',reject);probe.listen(0,'127.0.0.1',()=>{const port=probe.address().port;probe.close(()=>resolve(port));});});
   server=new EmbeddedServer(manager,free,'127.0.0.1');const port=await server.start();
   if(port!==free)throw new Error('Fixture HTTP listener did not bind its isolated port');
   const token=fs.readFileSync(path.join(os.homedir(),'.openclaw/server-token'),'utf8').trim();
   const response=await fetch('http://127.0.0.1:'+port+'/autoloop/'+runId+'/chat',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+token},body:JSON.stringify({text:input.text??'perform the requested control'})});
   outcome.http={status:response.status,body:await response.json()};
   const started=Date.now();
   while(Date.now()-started<5000){const state=manager.autoloopStatus(runId);if(state.recent_phase_errors.length||fs.existsSync(path.join(input.directory,'plan.md'))){outcome.terminal=state;break;}await new Promise(resolve=>setTimeout(resolve,10));}
  } else {
   try {outcome.response={ok:true,...await manager.autoloopChat(runId,input.text??'perform the requested control')};}
   catch(e){outcome.response={ok:false,code:e.code,message:e.message,retryable:e.retryable};}
  }
  outcome.state=manager.autoloopStatus(runId);
  if(input.operation==='reset') {
   outcome.eager=await manager.autoloopResetAgentResult(runId,'planner',{force:true,eagerRestart:true});
   outcome.lazy=await manager.autoloopResetAgent(runId,'planner',{force:true});
   outcome.afterLazy=await manager.autoloopChat(runId,'recreate the reusable Planner');
   fs.fsyncSync=fd=>{const target=fs.readlinkSync('/proc/self/fd/'+fd);if(target.endsWith('/agent-generations.jsonl')&&fs.readFileSync(target,'utf8').split('\n').filter(Boolean).map(JSON.parse).some(row=>row.kind==='agent_generation_reserved'&&row.payload.generation===4)){fault=true;throw new Error('fixture public replacement failure');}sync(fd);};
   syncBuiltinESMExports();
   outcome.failedEager=await manager.autoloopResetAgentResult(runId,'planner',{force:true,eagerRestart:true});
   outcome.failedWrapper=await manager.autoloopResetAgent(runId,'planner',{force:true,eagerRestart:true});
   fs.fsyncSync=sync;syncBuiltinESMExports();
  }
 }
 outcome.fault=fault;
 outcome.plan=fs.existsSync(path.join(input.directory,'plan.md'))?fs.readFileSync(path.join(input.directory,'plan.md'),'utf8'):null;
 outcome.goal=fs.existsSync(path.join(input.directory,'goal.json'))?fs.readFileSync(path.join(input.directory,'goal.json'),'utf8'):null;
 fs.writeFileSync(path.join(input.directory,'outcome.json'),JSON.stringify(outcome),{flag:'wx'});
} catch(e) {fs.writeFileSync(path.join(input.directory,'worker-error.json'),JSON.stringify({message:e.message,stack:e.stack}));throw e;}
finally {fs.fsyncSync=sync;syncBuiltinESMExports();if(server)await server.stop();if(tools.size)await invoke('autoloop_stop',{run_id:runId});for(const service of services)service.stop();await manager.shutdown();clearInterval(keepAlive);}
`;

async function publicCase(
  surface: string,
  options: { mode?: string; fault?: string; reply?: string; text?: string; operation?: string },
) {
  fs.mkdirSync(root, { recursive: true });
  const directory = fs.mkdtempSync(path.join(root, surface + '-'));
  const fixture = path.join(directory, 'codex.mjs');
  fs.copyFileSync(path.join(project, 'src/__tests__/fixtures/autoloop-trust-recovery/codex.mjs'), fixture);
  fs.chmodSync(fixture, 0o755);
  const input = { project, directory, surface, reply: 'conversation without a control', ...options };
  fs.writeFileSync(path.join(directory, 'worker.mjs'), worker, { flag: 'wx' });
  const started = new Date().toISOString();
  const child = spawn('rtk', ['proxy', process.execPath, '--import', 'tsx', '--input-type=module', '-'], {
    cwd: project,
    detached: true,
    stdio: ['pipe', 'pipe', 'pipe'],
    env: {
      ...process.env,
      OPENCLAW_SERVER_TOKEN: '',
      CODEX_BIN: fixture,
      CLAWO_NO_EMBEDDED_SERVER: '1',
      GIT_CEILING_DIRECTORIES: path.dirname(directory),
      CLAWO_TRUST_SCRATCH: directory,
      CLAWO_TRUST_NATIVE_CONFIG: path.join(directory, 'config.json'),
      CLAWO_TRUST_NATIVE_CONFIG_INPUT: JSON.stringify(input),
      NODE_OPTIONS: `--import tsx --import ${project}/src/__tests__/helpers/autoloop-trust-recovery.ts`,
    },
  });
  const stdout: Buffer[] = [],
    stderr: Buffer[] = [];
  child.stdout.on('data', (bytes: Buffer) => stdout.push(bytes));
  child.stderr.on('data', (bytes: Buffer) => stderr.push(bytes));
  child.stdin.end(worker);
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    process.kill(-child.pid!, 'SIGKILL');
  }, 15000);
  const exit = await new Promise<{ code: number | null; signal: string | null }>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ code, signal }));
  });
  clearTimeout(timer);
  fs.writeFileSync(path.join(directory, 'stdout.txt'), Buffer.concat(stdout), { flag: 'wx' });
  fs.writeFileSync(path.join(directory, 'stderr.txt'), Buffer.concat(stderr), { flag: 'wx' });
  fs.writeFileSync(
    path.join(directory, 'execution.json'),
    JSON.stringify({
      input,
      exit,
      timedOut,
      started,
      ended: new Date().toISOString(),
      argv: ['rtk', 'proxy', process.execPath, '--import', 'tsx', '--input-type=module', '-'],
      fixture_sha256: createHash('sha256').update(fs.readFileSync(fixture)).digest('hex'),
      stdout_sha256: createHash('sha256').update(Buffer.concat(stdout)).digest('hex'),
      stderr_sha256: createHash('sha256').update(Buffer.concat(stderr)).digest('hex'),
      source_inputs: [
        'src/session-manager.ts',
        'src/index.ts',
        'src/embedded-server.ts',
        'src/autoloop/dispatcher.ts',
        'src/autoloop/planner-tools.ts',
        'src/persistent-codex-session.ts',
      ].map((file) => ({
        path: path.join(project, file),
        sha256: createHash('sha256')
          .update(fs.readFileSync(path.join(project, file)))
          .digest('hex'),
      })),
    }),
    { flag: 'wx' },
  );
  expect(timedOut, directory).toBe(false);
  expect(exit.code, Buffer.concat(stderr).toString() + directory).toBe(0);
  return { directory, ...JSON.parse(fs.readFileSync(path.join(directory, 'outcome.json'), 'utf8')) };
}

describe('Slice 3 actual public terminal outcomes', () => {
  it('public eager reset verifies replacement, lazy boolean reset stays reusable, and failed recreation stays false', async () => {
    // Production break: the public wrapper maps failed eager replacement to
    // true, or the historical lazy contract no longer permits recreation.
    const result = await publicCase('manager', { operation: 'reset' });
    expect(result.eager).toMatchObject({ ok: true, previous_generation: 1, active_generation: 2 });
    expect(result.lazy).toBe(true);
    expect(result.afterLazy).toEqual({ reply: 'conversation without a control' });
    expect(result.fault).toBe(true);
    expect(result.failedEager).toMatchObject({ ok: false, code: 'AUTOLOOP_RESET_POSTCONDITION_FAILED' });
    expect(result.failedWrapper).toBe(false);
  });
  for (const [field, value] of [
    ['source_run_id', '../escape'],
    ['source_iter', -1],
    ['scope', []],
    ['idempotency_key', ''],
  ] as const) {
    it(`Planner rejects invalid ${field} independently without starting roles`, async () => {
      // Production break: a malformed review identifier crosses the public
      // Planner boundary and creates an independent Reviewer request.
      const args = {
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-checkpoint',
        source_iter: 0,
        scope: ['security'],
        idempotency_key: 'review-key',
        [field]: value,
      };
      const result = await publicCase('manager', {
        reply: '```autoloop\n' + JSON.stringify({ tool: 'request_review', args }) + '\n```',
      });
      expect(result.response.ok).toBe(false);
      expect(result.response.message).toContain(field);
      const rows = fs
        .readFileSync(path.join(result.directory, 'tasks/public-boundary/agent-generations.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line));
      expect(rows.filter((row) => row.payload.role === 'planner').length).toBeGreaterThan(0);
      expect(rows.filter((row) => row.payload.role === 'coder' || row.payload.role === 'reviewer')).toHaveLength(0);
    });
  }
  for (const surface of ['manager', 'mcp', 'http']) {
    for (const mode of ['empty', 'denied']) {
      it(`${surface}: ${mode} native turn is terminal failure, never public completion`, async () => {
        // Production break: queue acceptance or an empty/denied native turn
        // becomes a completed public control instead of a retained phase error.
        const result = await publicCase(surface, { mode });
        if (surface === 'http') {
          expect(result.http).toEqual({ status: 202, body: { ok: true, queued: true } });
          expect(result.terminal.recent_phase_errors.length).toBeGreaterThan(0);
        } else expect(result.response.ok, result.directory).toBe(false);
        expect(result.plan).toBe(null);
      });
    }
    it(`${surface}: rejected control persistence has no artifact effect`, async () => {
      // Production break: executing a valid claimed control before its intent
      // is durable, or reporting success after that persistence rejects.
      const result = await publicCase(surface, {
        fault: 'persistence',
        reply: '```autoloop\n{"tool":"write_plan","args":{"content":"# prohibited before durability"}}\n```',
      });
      expect(result.fault, result.directory).toBe(true);
      expect(result.plan).toBe(null);
      if (surface === 'http') expect(result.terminal.recent_phase_errors.length).toBeGreaterThan(0);
      else expect(result.response.ok).toBe(false);
    });
  }
  it('MCP preserves a non-control conversational reply', async () => {
    // Production break: requiring a control from every conversational turn.
    const result = await publicCase('mcp', {});
    expect(result.response).toMatchObject({ ok: true, reply: 'conversation without a control' });
  });
  it('valid first control plus malformed second control produces no partial batch effect', async () => {
    // Production break: applying controls incrementally before full validation.
    const result = await publicCase('manager', {
      reply:
        '```autoloop\n{"tool":"write_plan","args":{"content":"# no partial effect"}}\n```\n```autoloop\n{"tool":"request_review","args":{"checkpoint_sha":"invalid","source_run_id":"../escape","source_iter":-1,"scope":[],"idempotency_key":""}}\n```',
    });
    expect(result.response.ok).toBe(false);
    expect(result.plan).toBe(null);
    expect(result.goal).toBe(null);
  });
});
