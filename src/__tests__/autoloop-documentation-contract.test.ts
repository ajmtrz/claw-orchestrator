import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { describe, expect, it } from 'vitest';
import plugin from '../index.js';
import { validatePlannerToolCalls } from '../autoloop/planner-tools.js';

interface RegisteredTool {
  name: string;
  parameters: Record<string, unknown>;
}

function documentation(): string {
  const here = dirname(fileURLToPath(import.meta.url));
  return readFileSync(join(here, '../../skills/references/autoloop.md'), 'utf8');
}

function registeredTools(): RegisteredTool[] {
  const tools: RegisteredTool[] = [];
  (plugin as unknown as { register: (api: unknown) => void }).register({
    pluginConfig: {},
    logger: { info: () => {}, error: () => {}, warn: () => {} },
    on: () => {},
    registerTool: (tool: RegisteredTool) => tools.push(tool),
    registerHttpRoute: () => {},
    registerService: () => {},
  });
  return tools;
}

describe('Autoloop documentation contract', () => {
  it('documents recovery fields, errors, and HTTP routes that match the public registrations', () => {
    const reference = documentation();
    const tools = new Map(registeredTools().map((tool) => [tool.name, tool]));
    const recover = tools.get('autoloop_recover');
    const requestReview = tools.get('autoloop_request_review');

    expect(recover?.parameters).toMatchObject({
      additionalProperties: false,
      required: ['run_id'],
      properties: {
        run_id: { type: 'string' },
        apply: { type: 'boolean' },
        recovery_token: { type: 'string' },
      },
    });
    expect(requestReview?.parameters).toMatchObject({
      additionalProperties: false,
      required: ['run_id', 'checkpoint_sha', 'source_run_id', 'source_iter', 'scope', 'idempotency_key'],
    });
    expect(reference).toContain('`autoloop_recover`');
    expect(reference).toContain('`run_id`, `apply?`, `recovery_token?`');
    expect(reference).toContain('POST http://127.0.0.1:18789/autoloop/my-run/recover');
    expect(reference).toContain('POST http://127.0.0.1:18789/autoloop/my-run/request_review');
    for (const [code, status] of [
      ['AUTOLOOP_RECOVERY_TOKEN_REQUIRED', 'HTTP 400'],
      ['AUTOLOOP_RECOVERY_TOKEN_STALE', 'HTTP 409'],
      ['AUTOLOOP_RECOVERY_MANUAL_RESOLUTION_REQUIRED', 'HTTP 409'],
      ['AUTOLOOP_RECOVERY_INCOMPLETE', 'HTTP 409'],
    ]) {
      expect(reference).toContain(code);
      expect(reference).toContain(status);
    }
  });

  it('documents independent control examples that the Planner accepts, while rejected combinations remain rejected', () => {
    const reference = documentation();
    const validControls = [
      { tool: 'spawn_coder', args: { coder_engine: 'codex', coder_model: 'gpt-5.6-sol' } },
      { tool: 'spawn_reviewer', args: { reviewer_engine: 'codex', reviewer_model: 'gpt-5.6-sol' } },
      {
        tool: 'request_review',
        args: {
          checkpoint_sha: 'a'.repeat(40),
          source_run_id: 'source-run',
          source_iter: 7,
          scope: ['security'],
          idempotency_key: 'review-7',
        },
      },
    ] as const;

    for (const control of validControls) {
      expect(validatePlannerToolCalls([control])).toMatchObject({ errors: [], calls: [control] });
    }
    expect(
      validatePlannerToolCalls([
        validControls[0],
        { tool: 'notify_user', args: { level: 'info', summary: 'not allowed with a singleton control' } },
      ]),
    ).toMatchObject({
      calls: [],
      errors: [{ tool: 'spawn_coder', error: expect.stringMatching(/only Planner control/) }],
    });

    expect(reference).toContain('`spawn_coder`');
    expect(reference).toContain('`spawn_reviewer`');
    expect(reference).toContain('`request_review`');
    expect(reference).toContain('only Planner control in its batch');
  });
});
