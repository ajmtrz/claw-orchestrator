import { randomUUID } from 'node:crypto';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it, vi } from 'vitest';

import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import { Msg, type AnyAutoloopMessage } from '../autoloop/messages.js';
import type { SpawnSubagentsArgs } from '../autoloop/planner-tools.js';
import { nullLogger } from '../logger.js';
import { SessionManager } from '../session-manager.js';

const CONVERSATION_ID = 'a126a126-bbbb-4ccc-8ddd-eeeeeeeeeeee';
const FIXTURE_PATH = fileURLToPath(new URL('./fixtures/agy-planner-1.1.26.mjs', import.meta.url));
const DENIAL_ERROR =
  'Antigravity returned an empty response after a tool permission denial; the turn failed but the session remains available for retry';
const ORIGINAL_PLAN = Buffer.from('# original plan\r\nbyte stable\r\n');
const ORIGINAL_GOAL = Buffer.from('{"original":true}\n');
const RECOVERED_PLAN = '# recovered plan\n\nExact fixture bytes.\n';
const RECOVERED_GOAL = '{\n  "scalar": null,\n  "gates": []\n}\n';
const INITIAL_DIRECTIVE = {
  goal: 'execute the recovered plan',
  constraints: ['preserve exact artifact bytes'],
  success_criteria: ['run the synthetic gate'],
  max_attempts: 1,
};

function plannerTemps(workspace: string): string[] {
  return fs.readdirSync(workspace).filter((name) => name.startsWith('.plan.md.') || name.startsWith('.goal.json.'));
}

const AGY_FIXTURE = `#!/usr/bin/env node
const args = process.argv.slice(2);
const value = (flag) => {
  const index = args.indexOf(flag);
  return index < 0 ? undefined : args[index + 1];
};
const conversation = value('--conversation');
process.stdout.write(JSON.stringify({ event: 'init', conversation_id: '${CONVERSATION_ID}' }) + '\\n');
if (process.env.AUTOLOOP_E2E_AGY_EMPTY_REPLY === '1') {
  process.stdout.write(JSON.stringify({
    event: 'result',
    result: {
      conversation_id: '${CONVERSATION_ID}',
      status: 'SUCCESS',
      response: '',
      usage: { input_tokens: 1, output_tokens: 1, cache_read_tokens: 0 },
    },
  }) + '\\n');
} else if (conversation === undefined) {
  process.stdout.write(JSON.stringify({
    event: 'result',
    result: {
      conversation_id: '${CONVERSATION_ID}',
      status: 'STOPPED',
      response: 'required inspection tool was denied',
    },
  }) + '\\n');
} else {
  if (conversation !== '${CONVERSATION_ID}') process.exit(64);
  process.stdout.write(JSON.stringify({
    event: 'result',
    result: {
      conversation_id: '${CONVERSATION_ID}',
      status: 'SUCCESS',
      response: 'continued in the original conversation',
      usage: { input_tokens: 1, output_tokens: 1, cache_read_tokens: 0 },
    },
  }) + '\\n');
}
`;

describe('AGY Planner strict success contract', () => {
  it('rejects a successful AGY turn whose logical reply is empty', async () => {
    // Production break caught: an engine transport success with an empty
    // logical reply advances the Planner boundary as if it were a real turn.
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'agy-planner-empty-reply-'));
    const fixturePath = path.join(workspace, 'agy-fixture.mjs');
    fs.writeFileSync(fixturePath, AGY_FIXTURE, { mode: 0o755 });
    const previousAgyBin = process.env.AGY_BIN;
    const previousEmptyReply = process.env.AUTOLOOP_E2E_AGY_EMPTY_REPLY;
    process.env.AGY_BIN = fixturePath;
    process.env.AUTOLOOP_E2E_AGY_EMPTY_REPLY = '1';
    const manager = new SessionManager({ maxConcurrentSessions: 1 }, nullLogger);
    const dispatcher = new ClaudeAgentDispatcher({
      manager,
      runId: `agy-empty-reply-${randomUUID()}`,
      workspace,
      plannerEngine: 'agy',
      logger: nullLogger,
    });

    try {
      await expect(dispatcher.deliver(Msg.chat(0, { text: 'produce an empty logical reply' }))).rejects.toMatchObject({
        code: 'AUTOLOOP_ENGINE_FAILURE',
        retryable: true,
      });
    } finally {
      await dispatcher.shutdown('agy-empty-reply-cleanup', { purge: true });
      await manager.shutdown();
      if (previousAgyBin === undefined) delete process.env.AGY_BIN;
      else process.env.AGY_BIN = previousAgyBin;
      if (previousEmptyReply === undefined) delete process.env.AUTOLOOP_E2E_AGY_EMPTY_REPLY;
      else process.env.AUTOLOOP_E2E_AGY_EMPTY_REPLY = previousEmptyReply;
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });

  it('rejects a denied turn and reuses its conversation for a successful retry', async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'agy-planner-success-contract-'));
    const fixturePath = path.join(workspace, 'agy-fixture.mjs');
    fs.writeFileSync(fixturePath, AGY_FIXTURE, { mode: 0o755 });
    const previousAgyBin = process.env.AGY_BIN;
    process.env.AGY_BIN = fixturePath;
    const manager = new SessionManager({ maxConcurrentSessions: 1 }, nullLogger);
    const dispatcher = new ClaudeAgentDispatcher({
      manager,
      runId: `agy-success-contract-${randomUUID()}`,
      workspace,
      plannerEngine: 'agy',
      logger: nullLogger,
    });
    const plannerReplies: string[] = [];
    dispatcher.on('planner_reply', (reply) => plannerReplies.push(String(reply)));

    try {
      await expect(dispatcher.deliver(Msg.chat(0, { text: 'inspect before planning' }))).rejects.toMatchObject({
        code: 'AUTOLOOP_REQUIRED_TOOL_DENIED',
        retryable: true,
      });
      expect(manager.getStatus(dispatcher.sessionNames.planner).stats).toMatchObject({
        agyConversationId: CONVERSATION_ID,
        turns: 1,
        turnsSucceeded: 0,
      });
      expect(plannerReplies).toEqual([]);

      await expect(
        dispatcher.deliver(Msg.chat(0, { text: 'continue after correcting the permission' })),
      ).resolves.toEqual([]);
      expect(plannerReplies).toEqual(['continued in the original conversation']);
      expect(manager.getStatus(dispatcher.sessionNames.planner).stats).toMatchObject({
        agyConversationId: CONVERSATION_ID,
        turns: 2,
        turnsSucceeded: 1,
      });
    } finally {
      await dispatcher.shutdown('agy-success-contract-cleanup', { purge: true });
      await manager.shutdown();
      if (previousAgyBin === undefined) delete process.env.AGY_BIN;
      else process.env.AGY_BIN = previousAgyBin;
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });
});

describe('agy Planner subprocess recovery', () => {
  it('recovers a soft-denied empty turn in the same conversation before one atomic spawn', async () => {
    const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'agy-planner-e2e-'));
    const planPath = path.join(workspace, 'plan.md');
    const goalPath = path.join(workspace, 'goal.json');
    fs.writeFileSync(planPath, ORIGINAL_PLAN);
    fs.writeFileSync(goalPath, ORIGINAL_GOAL);

    const previousAgyBin = process.env.AGY_BIN;
    process.env.AGY_BIN = FIXTURE_PATH;
    const manager = new SessionManager({ maxConcurrentSessions: 1 }, nullLogger);
    const observedAtSpawn: Array<{
      args: SpawnSubagentsArgs;
      plan: Buffer;
      goal: Buffer;
    }> = [];
    const spawnSubagents = vi.fn(async (args: SpawnSubagentsArgs) => {
      observedAtSpawn.push({
        args,
        plan: fs.readFileSync(planPath),
        goal: fs.readFileSync(goalPath),
      });
    });
    const dispatcher = new ClaudeAgentDispatcher({
      manager,
      runId: `agy-e2e-${randomUUID()}`,
      workspace,
      plannerEngine: 'agy',
      onSpawnSubagents: spawnSubagents,
      logger: nullLogger,
    });

    try {
      let denial: unknown;
      let firstMessages: AnyAutoloopMessage[] = [];
      try {
        firstMessages = await dispatcher.deliver(Msg.chat(0, { text: 'inspect and propose a plan' }));
      } catch (error) {
        denial = error;
      }

      expect(denial).toBeInstanceOf(Error);
      expect((denial as Error).message).toContain(DENIAL_ERROR);
      expect(fs.readFileSync(planPath)).toEqual(ORIGINAL_PLAN);
      expect(fs.readFileSync(goalPath)).toEqual(ORIGINAL_GOAL);
      expect(plannerTemps(workspace)).toEqual([]);
      expect(spawnSubagents).not.toHaveBeenCalled();
      expect(firstMessages.filter(({ type }) => type === 'directive')).toEqual([]);
      expect(manager.getStatus(dispatcher.sessionNames.planner).stats).toMatchObject({
        agyConversationId: CONVERSATION_ID,
        turns: 1,
        turnsSucceeded: 0,
      });

      const recoveredMessages = await dispatcher.deliver(Msg.chat(0, { text: 'continue in the same conversation' }));

      expect(observedAtSpawn).toEqual([
        {
          args: { initial_directive: INITIAL_DIRECTIVE },
          plan: Buffer.from(RECOVERED_PLAN),
          goal: Buffer.from(RECOVERED_GOAL),
        },
      ]);
      expect(spawnSubagents).toHaveBeenCalledTimes(1);
      expect(fs.readFileSync(planPath)).toEqual(Buffer.from(RECOVERED_PLAN));
      expect(fs.readFileSync(goalPath)).toEqual(Buffer.from(RECOVERED_GOAL));
      expect(plannerTemps(workspace)).toEqual([]);
      expect(recoveredMessages.filter(({ type }) => type === 'directive')).toEqual([
        expect.objectContaining({ iter: 0, payload: INITIAL_DIRECTIVE }),
      ]);
      expect(manager.getStatus(dispatcher.sessionNames.planner).stats).toMatchObject({
        agyConversationId: CONVERSATION_ID,
        turns: 2,
        turnsSucceeded: 1,
      });
    } finally {
      await dispatcher.shutdown('agy-e2e-cleanup', { purge: true });
      await manager.shutdown();
      if (previousAgyBin === undefined) delete process.env.AGY_BIN;
      else process.env.AGY_BIN = previousAgyBin;
      fs.rmSync(workspace, { recursive: true, force: true });
    }
  });
});
