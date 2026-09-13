import { execFileSync, spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { pathToFileURL } from 'node:url';

import { describe, expect, it } from 'vitest';

import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import { Msg } from '../autoloop/messages.js';
import { lookupByIdempotencyKey } from '../autoloop/outbox.js';
import { recoveryActionDispatchId } from '../autoloop/recovery.js';
import { SecureAutoloopLedger } from '../autoloop/secure-ledger.js';
import { nullLogger } from '../logger.js';
import { SessionManager } from '../session-manager.js';

const AGY_FIXTURE = `#!/usr/bin/env node
import fs from 'node:fs';
const args = process.argv.slice(2);
const prompt = args[args.indexOf('-p') + 1] ?? '';
const provenance = /<autoloop_delivery delivery_id="([^"]+)" payload_sha256="([a-f0-9]{64})">/.exec(prompt);
process.stdout.write(JSON.stringify({ event: 'init', conversation_id: '11111111-2222-4333-8444-555555555555' }) + '\\n');
if (process.env.AUTOLOOP_E2E_AGY_MODE === 'crash_after_lease') {
  fs.writeFileSync(process.env.AUTOLOOP_E2E_BARRIER, 'lease-acquired');
  process.exit(17);
}
if (process.env.AUTOLOOP_E2E_AGY_MODE === 'crash_after_delivery_attempt') {
  fs.writeFileSync(process.env.AUTOLOOP_E2E_BARRIER, JSON.stringify({
    delivery_id: provenance?.[1], payload_sha256: provenance?.[2],
  }));
  process.exit(23);
}
if (process.env.AUTOLOOP_E2E_AGY_MODE === 'capture_replayed_ack') {
  fs.writeFileSync(process.env.AUTOLOOP_E2E_BARRIER, JSON.stringify({
    delivery_id: provenance?.[1], payload_sha256: provenance?.[2],
  }));
}
process.stdout.write(JSON.stringify({
  event: 'result',
  result: {
    conversation_id: '11111111-2222-4333-8444-555555555555',
    status: 'SUCCESS',
    response: (process.env.AUTOLOOP_E2E_AGY_ROLE === 'coder' || prompt.includes('# Coder —'))
      ? ['needs no further work', '\`\`\`autoloop', JSON.stringify({ tool: 'request_clarification', args: {
          delivery_id: provenance?.[1], payload_sha256: provenance?.[2],
        }}), '\`\`\`'].join('\\n')
      : prompt.includes('# Planner —')
        ? process.env.AUTOLOOP_E2E_AGY_MODE === 'planner_control'
          ? ['Planner received the Coder acknowledgement.', '\`\`\`autoloop', JSON.stringify({ tool: 'update_push_policy', args: {
              on_start: { level: 'info', channel: 'both' },
            }}), '\`\`\`'].join('\\n')
          : 'Planner received the review verdict.'
      : ['review completed', '\`\`\`autoloop', JSON.stringify({ tool: 'review_complete', args: {
          decision: 'hold', metric: null, audit_notes: 'fixture review',
          delivery_id: provenance?.[1], payload_sha256: provenance?.[2],
        }}), '\`\`\`'].join('\\n'),
    usage: { input_tokens: 1, output_tokens: 1, cache_read_tokens: 0 },
  },
}) + '\\n');
`;

function seedReviewArtifacts(dispatcher: ClaudeAgentDispatcher): void {
  const ledger = dispatcher.secureLedgerCapability;
  ledger.writeIterationArtifact(0, 'directive.json', '{"schema_version":1}\n');
  ledger.writeIterationArtifact(0, 'coder_summary.txt', 'Coder completed the checkpoint.\n');
  ledger.writeIterationArtifact(0, 'eval_output.json', '{"passed":true}\n');
  ledger.writeIterationArtifact(0, 'diff.patch', 'diff --git a/a b/a\n');
}

async function persistThenCrash(
  workspace: string,
  runId: string,
  message: ReturnType<typeof Msg.directive>,
  barrierPath: string,
): Promise<void> {
  const dispatcherUrl = pathToFileURL(path.resolve('src/autoloop/dispatcher.ts')).href;
  const loggerUrl = pathToFileURL(path.resolve('src/logger.ts')).href;
  const sessionManagerUrl = pathToFileURL(path.resolve('src/session-manager.ts')).href;
  const script = `
    import fs from 'node:fs';
    import { ClaudeAgentDispatcher } from ${JSON.stringify(dispatcherUrl)};
    import { nullLogger } from ${JSON.stringify(loggerUrl)};
    import { SessionManager } from ${JSON.stringify(sessionManagerUrl)};
    const manager = new SessionManager({ maxConcurrentSessions: 3 }, nullLogger);
    manager.sendMessage = async () => {
      fs.writeFileSync(${JSON.stringify(barrierPath)}, 'persisted-before-delivery');
      throw new Error('simulated child crash before delivery');
    };
    const dispatcher = new ClaudeAgentDispatcher({
      manager,
      runId: ${JSON.stringify(runId)},
      workspace: ${JSON.stringify(workspace)},
      coderEngine: 'agy',
      logger: nullLogger,
    });
    await dispatcher.deliver(${JSON.stringify(message)});
    await dispatcher.shutdown('simulated-pre-delivery-crash', { purge: true });
    process.exit(23);
  `;
  const child = spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '--eval', script], {
    cwd: process.cwd(),
    stdio: 'ignore',
  });
  const [code] = await once(child, 'exit');
  expect(code).toBe(23);
}

function initializeCheckpointRepository(workspace: string): { sha: string; patch: Buffer } {
  execFileSync('git', ['init', '-q'], { cwd: workspace });
  execFileSync('git', ['config', 'user.email', 'autoloop-e2e@example.invalid'], { cwd: workspace });
  execFileSync('git', ['config', 'user.name', 'Autoloop E2E'], { cwd: workspace });
  fs.writeFileSync(path.join(workspace, 'checkpoint.txt'), 'base\n');
  execFileSync('git', ['add', '--', 'checkpoint.txt'], { cwd: workspace });
  execFileSync('git', ['commit', '-q', '-m', 'base checkpoint'], { cwd: workspace });
  fs.writeFileSync(path.join(workspace, 'checkpoint.txt'), 'review this checkpoint\n');
  execFileSync('git', ['add', '--', 'checkpoint.txt'], { cwd: workspace });
  execFileSync('git', ['commit', '-q', '-m', 'review checkpoint'], { cwd: workspace });
  const sha = execFileSync('git', ['rev-parse', '--verify', 'HEAD'], { cwd: workspace, encoding: 'utf8' }).trim();
  const patch = execFileSync('git', ['show', '--format=', '--unified=3', '--no-renames', sha, '--'], {
    cwd: workspace,
  });
  return { sha, patch };
}

function writeSourceReviewArtifacts(workspace: string, runId: string, iter: number, patch: Buffer): void {
  const ledger = SecureAutoloopLedger.open(workspace, runId, { create: true });
  ledger.writeIterationArtifact(iter, 'directive.json', '{"schema_version":1,"goal":"review existing checkpoint"}\n');
  ledger.writeIterationArtifact(iter, 'coder_summary.txt', 'Coder artifacts are complete.\n');
  ledger.writeIterationArtifact(iter, 'eval_output.json', '{"passed":true}\n');
  ledger.writeIterationArtifact(iter, 'diff.patch', patch);
}

describe.sequential('Autoloop durable recovery real-process E2E', () => {
  it('releases a stale Reviewer generation after its subprocess dies and recreates the name safely', async () => {
    // Production break caught: a crashed physical agent leaves its generation
    // reservation occupied, so reset cannot safely recreate that role.
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-e2e-reset-'));
    const fixturePath = path.join(workspace, 'agy-fixture.mjs');
    fs.writeFileSync(fixturePath, AGY_FIXTURE, { mode: 0o755 });
    const barrierPath = path.join(workspace, 'reviewer-started');
    const previousAgyBin = process.env.AGY_BIN;
    const previousMode = process.env.AUTOLOOP_E2E_AGY_MODE;
    const previousBarrier = process.env.AUTOLOOP_E2E_BARRIER;
    process.env.AGY_BIN = fixturePath;
    process.env.AUTOLOOP_E2E_AGY_MODE = 'crash_after_lease';
    process.env.AUTOLOOP_E2E_BARRIER = barrierPath;
    const runId = `durable-reset-${randomUUID()}`;
    const manager = new SessionManager({ maxConcurrentSessions: 2 }, nullLogger);
    const dispatcher = new ClaudeAgentDispatcher({
      manager,
      runId,
      workspace,
      reviewerEngine: 'agy',
      logger: nullLogger,
    });

    try {
      seedReviewArtifacts(dispatcher);
      await expect(
        dispatcher.deliver(
          Msg.reviewRequest(0, { iter: 0, ledger_path: path.join(workspace, 'tasks', runId), prior_metrics: [] }),
        ),
      ).resolves.toEqual([
        expect.objectContaining({ type: 'phase_error', payload: expect.objectContaining({ agent: 'reviewer' }) }),
      ]);
      expect(fs.readFileSync(barrierPath, 'utf8')).toBe('lease-acquired');

      const reset = await dispatcher.resetAgent('reviewer', { eagerRestart: true });
      expect(reset).toMatchObject({
        ok: true,
        agent: 'reviewer',
      });
      if (!reset.ok) throw new Error(`Reviewer reset failed: ${reset.message}`);
      expect(reset.previous_generation).toBeGreaterThanOrEqual(1);
      expect(reset.active_generation).toBe(reset.previous_generation! + 1);
    } finally {
      await dispatcher.shutdown('durable-reset-e2e-cleanup', { purge: true });
      await manager.shutdown();
      if (previousAgyBin === undefined) delete process.env.AGY_BIN;
      else process.env.AGY_BIN = previousAgyBin;
      if (previousMode === undefined) delete process.env.AUTOLOOP_E2E_AGY_MODE;
      else process.env.AUTOLOOP_E2E_AGY_MODE = previousMode;
      if (previousBarrier === undefined) delete process.env.AUTOLOOP_E2E_BARRIER;
      else process.env.AUTOLOOP_E2E_BARRIER = previousBarrier;
      fs.rmSync(workspace, { force: true, recursive: true });
    }
  });

  it('inspects a stored pre-change run through the public recovery boundary without mutating evidence', async () => {
    // Production break caught: public read-only recovery writes generation or
    // outbox defaults into a stored pre-change run and changes its evidence.
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-e2e-legacy-'));
    const runId = `legacy-${randomUUID()}`;
    const runStore = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-e2e-run-store-'));
    const fixturePath = path.join(workspace, 'agy-fixture.mjs');
    const previousRunStore = process.env.CLAWO_WF_DIR;
    const previousAgyBin = process.env.AGY_BIN;
    process.env.CLAWO_WF_DIR = runStore;
    process.env.AGY_BIN = fixturePath;
    fs.writeFileSync(fixturePath, AGY_FIXTURE, { mode: 0o755 });
    let original: SessionManager | undefined;
    let recovered: SessionManager | undefined;

    try {
      original = new SessionManager({ maxConcurrentSessions: 2 }, nullLogger);
      await original.autoloopStart({
        runId,
        workspace,
        plannerEngine: 'agy',
        coderEngine: 'agy',
        reviewerEngine: 'agy',
      });
      const ledger = original.getAutoloop(runId)!.dispatcher.secureLedgerCapability;
      ledger.appendFlatFile('chat.jsonl', '{"legacy":true}\n');
      const metadataPath = path.join(runStore, runId, 'run.json');
      const artifactPath = path.join(workspace, 'tasks', runId, 'chat.jsonl');
      const ledgerPath = path.join(workspace, 'tasks', runId, 'decisions.jsonl');
      const metadataBefore = fs.readFileSync(metadataPath);
      const artifactBefore = fs.readFileSync(artifactPath);
      const ledgerExistedBefore = fs.existsSync(ledgerPath);
      const ledgerBefore = ledgerExistedBefore ? fs.readFileSync(ledgerPath) : Buffer.alloc(0);
      expect(metadataBefore.toString('utf8')).not.toContain('generation');
      expect(metadataBefore.toString('utf8')).not.toContain('outbox');

      await original.shutdown();
      original = undefined;
      fs.writeFileSync(metadataPath, metadataBefore);
      if (ledgerExistedBefore) fs.writeFileSync(ledgerPath, ledgerBefore);
      else fs.rmSync(ledgerPath, { force: true });
      recovered = new SessionManager({ maxConcurrentSessions: 2 }, nullLogger);
      const { assessment } = await recovered.autoloopRecover(runId);

      expect(assessment).toMatchObject({ run_id: runId, phase: 'PLANNING' });
      expect(assessment.evidence).toContain('legacy:status:planning');
      expect(fs.readFileSync(metadataPath)).toEqual(metadataBefore);
      expect(fs.readFileSync(artifactPath)).toEqual(artifactBefore);
      expect(fs.existsSync(ledgerPath) ? fs.readFileSync(ledgerPath) : Buffer.alloc(0)).toEqual(ledgerBefore);
    } finally {
      await recovered?.shutdown();
      await original?.shutdown();
      if (previousRunStore === undefined) delete process.env.CLAWO_WF_DIR;
      else process.env.CLAWO_WF_DIR = previousRunStore;
      if (previousAgyBin === undefined) delete process.env.AGY_BIN;
      else process.env.AGY_BIN = previousAgyBin;
      fs.rmSync(workspace, { force: true, recursive: true });
      fs.rmSync(runStore, { force: true, recursive: true });
    }
  });

  it('recovers a child-persisted pre-delivery outbox intent through public autoloopRecover', async () => {
    // Production break caught: public recovery ignores a durable intent left
    // behind when the sender dies after persistence and before any delivery.
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-e2e-outbox-'));
    const runId = `outbox-${randomUUID()}`;
    const barrierPath = path.join(workspace, 'crash-barrier');
    const fixturePath = path.join(workspace, 'agy-fixture.mjs');
    const runStore = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-e2e-outbox-store-'));
    const previousAgyBin = process.env.AGY_BIN;
    const previousMode = process.env.AUTOLOOP_E2E_AGY_MODE;
    const previousRunStore = process.env.CLAWO_WF_DIR;
    const previousRole = process.env.AUTOLOOP_E2E_AGY_ROLE;
    process.env.AGY_BIN = fixturePath;
    process.env.CLAWO_WF_DIR = runStore;
    fs.writeFileSync(fixturePath, AGY_FIXTURE, { mode: 0o755 });
    const message = Msg.directive(0, {
      goal: 'recover the durable pre-delivery intent',
      constraints: [],
      success_criteria: [],
      max_attempts: 1,
    });
    const idempotencyKey = recoveryActionDispatchId(runId, message);
    let original: SessionManager | undefined;
    let recovered: SessionManager | undefined;

    try {
      original = new SessionManager({ maxConcurrentSessions: 3 }, nullLogger);
      await original.autoloopStart({
        runId,
        workspace,
        plannerEngine: 'agy',
        coderEngine: 'agy',
        reviewerEngine: 'agy',
      });
      const originalHandle = original.getAutoloop(runId)!;
      await originalHandle.dispatcher.spawnSubagents();
      const metadataPath = path.join(runStore, runId, 'run.json');
      const metadataBeforeOriginalShutdown = fs.readFileSync(metadataPath);
      await original.shutdown();
      original = undefined;
      fs.writeFileSync(metadataPath, metadataBeforeOriginalShutdown);

      await persistThenCrash(workspace, runId, message, barrierPath);
      expect(fs.readFileSync(barrierPath, 'utf8')).toBe('persisted-before-delivery');
      const ledger = SecureAutoloopLedger.open(workspace, runId);
      const intent = lookupByIdempotencyKey(ledger, idempotencyKey);
      expect(intent).toMatchObject({ idempotency_key: idempotencyKey, target_role: 'coder' });

      recovered = new SessionManager({ maxConcurrentSessions: 3 }, nullLogger);
      process.env.AUTOLOOP_E2E_AGY_MODE = 'planner_control';
      const inspection = await recovered.autoloopRecover(runId);
      const assessment = inspection.assessment;
      expect(assessment).toMatchObject({ phase: 'AWAITING_CODER', next_safe_action: 'dispatch_coder' });
      expect(assessment.pending_delivery_ids).toEqual([intent!.delivery_id]);
      await expect(
        recovered.autoloopRecover(runId, { apply: true, recovery_token: assessment.recovery_token }),
      ).resolves.toMatchObject({ receipt: { status: 'applied' } });
      const recoveredIntent = lookupByIdempotencyKey(
        recovered.getAutoloop(runId)!.dispatcher.secureLedgerCapability,
        idempotencyKey,
      );
      expect(recoveredIntent).toMatchObject({
        delivery_id: intent!.delivery_id,
        payload_sha256: intent!.payload_sha256,
      });
    } finally {
      await recovered?.shutdown();
      await original?.shutdown();
      if (previousAgyBin === undefined) delete process.env.AGY_BIN;
      else process.env.AGY_BIN = previousAgyBin;
      if (previousMode === undefined) delete process.env.AUTOLOOP_E2E_AGY_MODE;
      else process.env.AUTOLOOP_E2E_AGY_MODE = previousMode;
      if (previousRunStore === undefined) delete process.env.CLAWO_WF_DIR;
      else process.env.CLAWO_WF_DIR = previousRunStore;
      if (previousRole === undefined) delete process.env.AUTOLOOP_E2E_AGY_ROLE;
      else process.env.AUTOLOOP_E2E_AGY_ROLE = previousRole;
      fs.rmSync(workspace, { force: true, recursive: true });
      fs.rmSync(runStore, { force: true, recursive: true });
    }
  });

  it('recovers an unacknowledged intent after a real Coder delivery attempt crashes pre-acknowledgement', async () => {
    // Production break caught: the dispatcher either never reaches
    // SessionManager.sendMessage, loses its durable intent after a crashed
    // physical Coder, or cannot replay that exact unacknowledged intent.
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-e2e-delivery-attempt-'));
    const fixturePath = path.join(workspace, 'agy-fixture.mjs');
    const barrierPath = path.join(workspace, 'delivery-attempt.json');
    const replayBarrierPath = path.join(workspace, 'replayed-acknowledgement.json');
    const runId = `delivery-attempt-${randomUUID()}`;
    const previousAgyBin = process.env.AGY_BIN;
    const previousMode = process.env.AUTOLOOP_E2E_AGY_MODE;
    const previousBarrier = process.env.AUTOLOOP_E2E_BARRIER;
    const previousRole = process.env.AUTOLOOP_E2E_AGY_ROLE;
    fs.writeFileSync(fixturePath, AGY_FIXTURE, { mode: 0o755 });
    process.env.AGY_BIN = fixturePath;
    process.env.AUTOLOOP_E2E_AGY_MODE = 'crash_after_delivery_attempt';
    process.env.AUTOLOOP_E2E_BARRIER = barrierPath;
    process.env.AUTOLOOP_E2E_AGY_ROLE = 'coder';
    const message = Msg.directive(0, {
      goal: 'prove durable delivery recovery',
      constraints: [],
      success_criteria: [],
      max_attempts: 1,
    });
    let firstManager: SessionManager | undefined;
    let firstDispatcher: ClaudeAgentDispatcher | undefined;
    let replayManager: SessionManager | undefined;
    let replayDispatcher: ClaudeAgentDispatcher | undefined;

    try {
      firstManager = new SessionManager({ maxConcurrentSessions: 2 }, nullLogger);
      firstDispatcher = new ClaudeAgentDispatcher({
        manager: firstManager,
        runId,
        workspace,
        coderEngine: 'agy',
        logger: nullLogger,
      });
      await expect(firstDispatcher.deliver(message)).resolves.toEqual([
        expect.objectContaining({ type: 'phase_error', payload: expect.objectContaining({ agent: 'coder' }) }),
      ]);
      const attempted = JSON.parse(fs.readFileSync(barrierPath, 'utf8')) as {
        delivery_id?: string;
        payload_sha256?: string;
      };
      expect(attempted.delivery_id).toMatch(/^[0-9a-f-]{36}$/);
      expect(attempted.payload_sha256).toMatch(/^[a-f0-9]{64}$/);
      const beforeReplay = fs
        .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'), 'utf8')
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line) as { delivery_id?: string; idempotency_key?: string })
        .find((row) => row.delivery_id === attempted.delivery_id);
      expect(beforeReplay).toMatchObject({ delivery_id: attempted.delivery_id, idempotency_key: expect.any(String) });
      const persistedIntent = lookupByIdempotencyKey(
        firstDispatcher.secureLedgerCapability,
        beforeReplay!.idempotency_key!,
      );
      expect(persistedIntent).toMatchObject({ delivery_id: attempted.delivery_id });

      await firstDispatcher.shutdown('delivery-attempt-crash-cleanup', { purge: true });
      await firstManager.shutdown();
      firstDispatcher = undefined;
      firstManager = undefined;
      const decisionsBeforeReplay = fs.readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'));
      process.env.AUTOLOOP_E2E_AGY_MODE = 'capture_replayed_ack';
      process.env.AUTOLOOP_E2E_BARRIER = replayBarrierPath;

      replayManager = new SessionManager({ maxConcurrentSessions: 2 }, nullLogger);
      replayDispatcher = new ClaudeAgentDispatcher({
        manager: replayManager,
        runId,
        workspace,
        coderEngine: 'agy',
        logger: nullLogger,
      });
      await expect(replayDispatcher.deliver(message)).resolves.toEqual([
        expect.objectContaining({ type: 'directive_ack' }),
      ]);
      const replayed = lookupByIdempotencyKey(replayDispatcher.secureLedgerCapability, beforeReplay!.idempotency_key!);
      expect(replayed).toMatchObject({ delivery_id: attempted.delivery_id });
      const replayedReceiverProvenance = JSON.parse(fs.readFileSync(replayBarrierPath, 'utf8')) as {
        delivery_id?: string;
        payload_sha256?: string;
      };
      expect(replayedReceiverProvenance).toEqual(attempted);
      const postReplayRows = fs
        .readFileSync(path.join(workspace, 'tasks', runId, 'decisions.jsonl'))
        .subarray(decisionsBeforeReplay.length)
        .toString('utf8')
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(
        postReplayRows.find(
          (row) =>
            row.delivery_id === attempted.delivery_id &&
            row.payload_sha256 === attempted.payload_sha256 &&
            typeof row.acknowledged_at === 'string',
        ),
      ).toMatchObject({ delivery_id: attempted.delivery_id, payload_sha256: attempted.payload_sha256 });
    } finally {
      await replayDispatcher?.shutdown('delivery-attempt-replay-cleanup', { purge: true });
      await replayManager?.shutdown();
      await firstDispatcher?.shutdown('delivery-attempt-first-cleanup', { purge: true });
      await firstManager?.shutdown();
      if (previousAgyBin === undefined) delete process.env.AGY_BIN;
      else process.env.AGY_BIN = previousAgyBin;
      if (previousMode === undefined) delete process.env.AUTOLOOP_E2E_AGY_MODE;
      else process.env.AUTOLOOP_E2E_AGY_MODE = previousMode;
      if (previousBarrier === undefined) delete process.env.AUTOLOOP_E2E_BARRIER;
      else process.env.AUTOLOOP_E2E_BARRIER = previousBarrier;
      if (previousRole === undefined) delete process.env.AUTOLOOP_E2E_AGY_ROLE;
      else process.env.AUTOLOOP_E2E_AGY_ROLE = previousRole;
      fs.rmSync(workspace, { force: true, recursive: true });
    }
  });

  it('starts only a real Reviewer for a recovered checkpoint and persists one source-bound verdict', async () => {
    // Production break caught: public recovery starts a Coder or a continuation
    // run instead of reusing the original run/checkpoint and starting Reviewer.
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-e2e-reviewer-only-'));
    const runStore = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-e2e-review-store-'));
    const fixturePath = path.join(workspace, 'agy-fixture.mjs');
    fs.writeFileSync(fixturePath, AGY_FIXTURE, { mode: 0o755 });
    const previousAgyBin = process.env.AGY_BIN;
    const previousMode = process.env.AUTOLOOP_E2E_AGY_MODE;
    const previousRunStore = process.env.CLAWO_WF_DIR;
    process.env.AGY_BIN = fixturePath;
    delete process.env.AUTOLOOP_E2E_AGY_MODE;
    process.env.CLAWO_WF_DIR = runStore;
    const sourceRunId = `source-${randomUUID()}`;
    let initial: SessionManager | undefined;
    let recovered: SessionManager | undefined;

    try {
      const checkpoint = initializeCheckpointRepository(workspace);
      initial = new SessionManager({ maxConcurrentSessions: 3 }, nullLogger);
      await initial.autoloopStart({
        runId: sourceRunId,
        workspace,
        plannerEngine: 'agy',
        coderEngine: 'agy',
        reviewerEngine: 'agy',
      });
      const original = initial.getAutoloop(sourceRunId)!;
      await original.dispatcher.spawnSubagents();
      writeSourceReviewArtifacts(workspace, sourceRunId, 0, checkpoint.patch);
      const envelope = Msg.reviewRequest(0, {
        iter: 0,
        ledger_path: original.runner.state.ledger_dir,
        prior_metrics: [],
        checkpoint_sha: checkpoint.sha,
        source_run_id: sourceRunId,
        source_iter: 0,
        scope: ['durable-recovery'],
        idempotency_key: `review-${randomUUID()}`,
      });
      await original.runner.config.persistReviewEnvelope!(envelope);
      const decisionPath = path.join(workspace, 'tasks', sourceRunId, 'decisions.jsonl');
      const generationsPath = path.join(workspace, 'tasks', sourceRunId, 'agent-generations.jsonl');
      const decisionsBeforeRecovery = fs.readFileSync(decisionPath);
      expect(original.dispatcher.secureLedgerCapability.readIterationArtifact(0, 'verdict.json')).toBeUndefined();
      await initial.shutdown();
      initial = undefined;
      const generationsBeforeRecovery = fs.readFileSync(generationsPath, 'utf8');
      expect(generationsBeforeRecovery).toContain('"role":"coder"');
      expect(generationsBeforeRecovery).toContain('"state":"released"');

      recovered = new SessionManager({ maxConcurrentSessions: 3 }, nullLogger);
      const inspection = await recovered.autoloopRecover(sourceRunId);
      expect(inspection.assessment).toMatchObject({ phase: 'AWAITING_REVIEW', next_safe_action: 'request_review' });
      await expect(
        recovered.autoloopRecover(sourceRunId, {
          apply: true,
          recovery_token: inspection.assessment.recovery_token,
        }),
      ).resolves.toMatchObject({ receipt: { status: 'applied' } });
      const handle = recovered.getAutoloop(sourceRunId)!;
      const verdict = handle.dispatcher.secureLedgerCapability.readIterationArtifact(0, 'verdict.json');
      expect(JSON.parse(verdict!.toString('utf8'))).toMatchObject({ iter: 0, decision: 'hold' });
      const postRecoveryRows = fs
        .readFileSync(decisionPath)
        .subarray(decisionsBeforeRecovery.length)
        .toString('utf8')
        .trim()
        .split('\n')
        .filter(Boolean)
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      const recoveryReceipt = postRecoveryRows.find(
        (row) => row.record_type === 'autoloop_recovery_receipt' && row.status === 'applied',
      );
      expect(recoveryReceipt).toMatchObject({
        action_snapshot: {
          type: 'review_request',
          payload: {
            checkpoint_sha: checkpoint.sha,
            source_run_id: sourceRunId,
            source_iter: 0,
          },
        },
      });
      const reviewerIntent = postRecoveryRows.find(
        (row) => row.kind === 'review_request' && row.target_role === 'reviewer' && typeof row.delivery_id === 'string',
      );
      expect(reviewerIntent).toMatchObject({
        kind: 'review_request',
        target_role: 'reviewer',
        payload: expect.objectContaining({
          logical_message_sha256: expect.any(String),
          prompt: expect.stringContaining(`checkpoint_sha: ${checkpoint.sha}`),
        }),
      });
      expect((reviewerIntent!.payload as { prompt: string }).prompt).toContain(
        `Artifacts staged from run ${sourceRunId} iter 0`,
      );
      expect(
        postRecoveryRows.find(
          (row) =>
            row.delivery_id === reviewerIntent!.delivery_id &&
            row.payload_sha256 === reviewerIntent!.payload_sha256 &&
            typeof row.acknowledged_at === 'string',
        ),
      ).toMatchObject({ delivery_id: reviewerIntent!.delivery_id, payload_sha256: reviewerIntent!.payload_sha256 });
      const recoveryGenerations = fs.readFileSync(generationsPath, 'utf8').slice(generationsBeforeRecovery.length);
      expect(recoveryGenerations).toContain('"role":"reviewer"');
      expect(recoveryGenerations).not.toContain('"role":"coder"');
      const decisions = fs.readFileSync(decisionPath, 'utf8');
      expect(decisions).toContain(`"source_run_id":"${sourceRunId}"`);
      expect(decisions).toContain(`"source_iter":0`);
      expect(decisions).not.toContain('spawn_coder');
      expect(fs.readdirSync(runStore).filter((entry) => entry !== sourceRunId)).toEqual([]);
      expect(decisions).toContain('autoloop_recovery_review_envelope');
    } finally {
      await recovered?.shutdown();
      await initial?.shutdown();
      if (previousAgyBin === undefined) delete process.env.AGY_BIN;
      else process.env.AGY_BIN = previousAgyBin;
      if (previousMode === undefined) delete process.env.AUTOLOOP_E2E_AGY_MODE;
      else process.env.AUTOLOOP_E2E_AGY_MODE = previousMode;
      if (previousRunStore === undefined) delete process.env.CLAWO_WF_DIR;
      else process.env.CLAWO_WF_DIR = previousRunStore;
      fs.rmSync(workspace, { force: true, recursive: true });
      fs.rmSync(runStore, { force: true, recursive: true });
    }
  });
});
