import { randomUUID } from 'node:crypto';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { describe, expect, it } from 'vitest';

import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import { Msg } from '../autoloop/messages.js';
import { nullLogger } from '../logger.js';
import { SessionManager } from '../session-manager.js';

const CONVERSATION_ID = 'a126a126-bbbb-4ccc-8ddd-eeeeeeeeeeee';

const AGY_FIXTURE = `#!/usr/bin/env node
const args = process.argv.slice(2);
const value = (flag) => {
  const index = args.indexOf(flag);
  return index < 0 ? undefined : args[index + 1];
};
const conversation = value('--conversation');
process.stdout.write(JSON.stringify({ event: 'init', conversation_id: '${CONVERSATION_ID}' }) + '\\n');
if (conversation === undefined) {
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
