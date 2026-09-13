import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { DEFAULT_SERVER_PORT } from '../constants.js';
import plugin from '../index.js';
import { parsePlannerReply, validatePlannerToolCalls, type PlannerToolCall } from '../autoloop/planner-tools.js';

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

function section(reference: string, heading: string): string {
  const start = reference.indexOf(heading);
  expect(start).toBeGreaterThanOrEqual(0);
  const nextHeading = reference.indexOf('\n## ', start + heading.length);
  return reference.slice(start, nextHeading === -1 ? undefined : nextHeading);
}

describe('Autoloop documentation contract', () => {
  it('documents production-derived embedded recovery and review routes, schemas, and error statuses', () => {
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
    const recovery = section(reference, '## Durable recovery and Reviewer-only requests');
    const recoverRoute = `POST http://127.0.0.1:${DEFAULT_SERVER_PORT}/autoloop/my-run/recover`;
    const reviewRoute = `POST http://127.0.0.1:${DEFAULT_SERVER_PORT}/autoloop/my-run/request_review`;

    expect(reference).toContain('`autoloop_recover`');
    expect(reference).toContain('`run_id`, `apply?`, `recovery_token?`');
    expect(recovery).toContain(recoverRoute);
    expect(recovery).toContain(reviewRoute);
    expect(recovery).toContain('"apply":true,"recovery_token":"<inspection-token>"');
    expect(recovery).toContain(
      '"checkpoint_sha":"<40-hex-sha>","source_run_id":"my-run","source_iter":7,"scope":["security"],"idempotency_key":"review-7"',
    );
    for (const [code, status] of [
      ['AUTOLOOP_RECOVERY_TOKEN_REQUIRED', 'HTTP 400'],
      ['AUTOLOOP_RECOVERY_TOKEN_STALE', 'HTTP 409'],
      ['AUTOLOOP_RECOVERY_MANUAL_RESOLUTION_REQUIRED', 'HTTP 409'],
      ['AUTOLOOP_RECOVERY_INCOMPLETE', 'HTTP 409'],
    ]) {
      expect(recovery).toContain(`| \`${code}\` | ${status} |`);
    }
  });

  it('parses and validates the documented independent Planner control examples', () => {
    const reference = documentation();
    const independentControls = section(reference, '### Independent controls and Reviewer-only delivery');
    const documented = parsePlannerReply(independentControls);
    const expected: PlannerToolCall[] = [
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
    ];

    expect(documented.parse_errors).toEqual([]);
    expect(documented.calls).toEqual(expected);
    for (const control of documented.calls) {
      const validation = validatePlannerToolCalls([control]);
      expect(validation.errors).toEqual([]);
      expect(validation.blocked_policy_silence).toEqual([]);
      expect(validation.calls).toEqual([control]);
    }
    expect(
      validatePlannerToolCalls([
        documented.calls[0],
        { tool: 'notify_user', args: { level: 'info', summary: 'not allowed with a singleton control' } },
      ]),
    ).toMatchObject({
      calls: [],
      errors: [{ tool: 'spawn_coder', error: expect.stringMatching(/only Planner control/) }],
    });

    expect(independentControls).toContain('only Planner control in its batch');
  });
});
