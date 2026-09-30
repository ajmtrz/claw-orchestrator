/**
 * Tests for the Planner tool-call parser + handler.
 */

import { describe, it, expect, expectTypeOf, vi } from 'vitest';
import {
  applyPlannerToolCalls,
  applyValidatedPlannerToolCalls,
  parsePlannerReply,
  validatePlannerToolCalls,
  type PreparedReviewRequest,
  type PlannerToolEffects,
} from '../autoloop/planner-tools.js';
import {
  Msg,
  validateMessage,
  type AnyAutoloopMessage,
  type CheckpointReviewRequestPayload,
} from '../autoloop/messages.js';

function makeMockEffects(): {
  fx: PlannerToolEffects;
  calls: string[];
  policyDelta: Record<string, unknown>;
  writes: Array<{ file: string; content: string; msg?: string }>;
} {
  const calls: string[] = [];
  const policyDelta: Record<string, unknown> = {};
  const writes: Array<{ file: string; content: string; msg?: string }> = [];
  const fx: PlannerToolEffects = {
    spawnCoder: async (args) => {
      calls.push(`spawnCoder:${JSON.stringify(args)}`);
    },
    spawnReviewer: async (args) => {
      calls.push(`spawnReviewer:${JSON.stringify(args)}`);
    },
    spawnSubagents: async (args) => {
      calls.push(`spawnSubagents:${JSON.stringify(args)}`);
    },
    requestReview: async (args, targetIter) => {
      calls.push(`requestReview:${JSON.stringify(args)}`);
      return {
        status: 'prepared' as const,
        target: 'reviewer' as const,
        idempotency_key: args.idempotency_key as string,
        payload: {
          iter: targetIter,
          ledger_path: '/trusted/run',
          prior_metrics: [],
          ...args,
        },
      };
    },
    releaseReviewRequest: () => undefined,
    updatePushPolicy: (delta) => {
      Object.assign(policyDelta, delta);
      calls.push(`updatePushPolicy:${JSON.stringify(delta)}`);
    },
    writePlanFiles: async (batch) => {
      for (const { file, content, commitMessage: msg } of batch) {
        writes.push({ file, content, msg });
        calls.push(`write:${file}:${msg ?? ''}`);
      }
    },
  };
  return { fx, calls, policyDelta, writes };
}

describe('parsePlannerReply', () => {
  it('extracts a single autoloop block and strips it from the reply', () => {
    const reply = `Sure, here's the plan.

\`\`\`autoloop
{"tool": "notify_user", "args": {"level": "info", "summary": "plan ready"}}
\`\`\`

Let me know if you want to adjust.`;
    const { calls, cleaned_reply, parse_errors } = parsePlannerReply(reply);
    expect(calls).toHaveLength(1);
    expect(calls[0].tool).toBe('notify_user');
    expect(calls[0].args.summary).toBe('plan ready');
    expect(parse_errors).toEqual([]);
    expect(cleaned_reply).not.toContain('autoloop');
    expect(cleaned_reply).toContain("Sure, here's the plan.");
    expect(cleaned_reply).toContain('Let me know if you want to adjust.');
  });

  it('extracts multiple blocks in order', () => {
    const reply = `\`\`\`autoloop
{"tool": "write_plan", "args": {"content": "# Plan\\n..."}}
\`\`\`

then

\`\`\`autoloop
{"tool": "spawn_subagents", "args": {"coder_model": "sonnet"}}
\`\`\``;
    const { calls } = parsePlannerReply(reply);
    expect(calls.map((c) => c.tool)).toEqual(['write_plan', 'spawn_subagents']);
  });

  it('records parse errors but keeps going on malformed blocks', () => {
    const reply = `\`\`\`autoloop
{not valid json
\`\`\`

\`\`\`autoloop
{"tool": "terminate", "args": {"reason": "ok"}}
\`\`\``;
    const { calls, parse_errors } = parsePlannerReply(reply);
    expect(parse_errors).toHaveLength(1);
    expect(calls).toHaveLength(1);
    expect(calls[0].tool).toBe('terminate');
  });

  it('rejects blocks missing tool/args fields', () => {
    const reply = `\`\`\`autoloop
{"args": {"x": 1}}
\`\`\``;
    const { calls, parse_errors } = parsePlannerReply(reply);
    expect(calls).toEqual([]);
    expect(parse_errors).toHaveLength(1);
  });
});

describe('applyPlannerToolCalls', () => {
  it('prevalidates the complete batch before applying any effect', async () => {
    const { fx, calls, writes } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [
        { tool: 'write_plan', args: { content: '# replacement plan' } },
        {
          tool: 'spawn_subagents',
          args: { initial_directive: { goal: 'must not run' } },
        },
        { tool: 'write_goal', args: { content: '{not valid json' } },
      ],
      fx,
      0,
    );

    expect(r.errors).toHaveLength(1);
    expect(r.errors[0]).toMatchObject({ tool: 'write_goal' });
    expect(writes).toEqual([]);
    expect(calls).toEqual([]);
    expect(r.emitted_messages).toEqual([]);
  });

  it('does not spawn or emit a directive when artifact materialization fails', async () => {
    const harness = makeMockEffects();
    harness.fx.writePlanFiles = async (batch) => {
      for (const { file } of batch) harness.calls.push(`write:${file}:`);
      throw new Error('simulated goal write failure');
    };

    const r = await applyPlannerToolCalls(
      [
        { tool: 'write_plan', args: { content: '# replacement plan' } },
        { tool: 'write_goal', args: { content: '{"scalar":null,"gates":[]}' } },
        {
          tool: 'spawn_subagents',
          args: { initial_directive: { goal: 'must not run' } },
        },
      ],
      harness.fx,
      0,
    );

    expect(r.errors).toHaveLength(1);
    expect(r.errors[0].error).toContain('simulated goal write failure');
    expect(harness.calls.filter((call) => call.startsWith('spawnSubagents:'))).toEqual([]);
    expect(r.emitted_messages).toEqual([]);
  });

  it('materializes all artifacts before spawning even when spawn appears first', async () => {
    const { fx, calls, writes } = makeMockEffects();
    const plan = '# exact plan\n';
    const goal = '{"scalar":null,"gates":[]}\n';
    const r = await applyPlannerToolCalls(
      [
        {
          tool: 'spawn_subagents',
          args: { initial_directive: { goal: 'run after writes' } },
        },
        { tool: 'write_goal', args: { content: goal } },
        { tool: 'write_plan', args: { content: plan } },
      ],
      fx,
      4,
    );

    expect(r.errors).toEqual([]);
    expect(writes).toEqual([
      { file: 'goal.json', content: goal, msg: 'autoloop: planner writes goal.json' },
      { file: 'plan.md', content: plan, msg: 'autoloop: planner writes plan.md' },
    ]);
    const spawnIndex = calls.findIndex((call) => call.startsWith('spawnSubagents:'));
    expect(spawnIndex).toBeGreaterThan(calls.findIndex((call) => call.startsWith('write:goal.json:')));
    expect(spawnIndex).toBeGreaterThan(calls.findIndex((call) => call.startsWith('write:plan.md:')));
    expect(r.emitted_messages.filter((message) => message.type === 'directive')).toHaveLength(1);
  });

  it('rejects duplicate spawn controls before starting roles or emitting directives', async () => {
    const { fx, calls } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [
        { tool: 'spawn_subagents', args: { initial_directive: { goal: 'first' } } },
        { tool: 'spawn_subagents', args: { initial_directive: { goal: 'second' } } },
      ],
      fx,
      0,
    );

    expect(r.errors).toHaveLength(1);
    expect(r.errors[0].error).toContain('duplicate');
    expect(calls).toEqual([]);
    expect(r.emitted_messages).toEqual([]);
  });

  it('notify_user becomes a push_user envelope', async () => {
    const { fx } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [{ tool: 'notify_user', args: { level: 'info', summary: 'hi', channel: 'wechat' } }],
      fx,
      0,
    );
    expect(r.errors).toEqual([]);
    expect(r.emitted_messages).toHaveLength(1);
    const env = r.emitted_messages[0];
    expect(env.type).toBe('push_user');
    expect(env.from).toBe('planner');
    expect(env.to).toBe('user');
    if (env.type === 'push_user') {
      expect(env.payload.summary).toBe('hi');
      expect(env.payload.channel).toBe('wechat');
    }
  });

  it('spawn_subagents calls effect AND emits initial directive when present', async () => {
    const { fx, calls } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [
        {
          tool: 'spawn_subagents',
          args: {
            coder_model: 'sonnet',
            initial_directive: { goal: 'ship it', constraints: ['no new deps'] },
          },
        },
      ],
      fx,
      3,
    );
    expect(r.errors).toEqual([]);
    expect(calls.some((c) => c.startsWith('spawnSubagents:'))).toBe(true);
    expect(r.emitted_messages).toHaveLength(1);
    const env = r.emitted_messages[0];
    expect(env.type).toBe('directive');
    if (env.type === 'directive') {
      expect(env.payload.goal).toBe('ship it');
      expect(env.payload.constraints).toEqual(['no new deps']);
    }
  });

  it('passes valid coder and reviewer engines to the spawn effect', async () => {
    const { fx, calls } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [
        {
          tool: 'spawn_subagents',
          args: { coder_engine: 'codex', reviewer_engine: 'gemini' },
        },
      ],
      fx,
      0,
    );

    expect(r.errors).toEqual([]);
    expect(calls).toEqual(['spawnSubagents:{"coder_engine":"codex","reviewer_engine":"gemini"}']);
  });

  it('keeps inherited spawn fields out of validated controls, effects, and emitted directives without invoking accessors', async () => {
    const inherited = {
      coder_engine: 'codex',
      coder_model: 'inherited-coder-model',
      reviewer_engine: 'gemini',
      reviewer_model: 'inherited-reviewer-model',
      initial_directive: { goal: 'inherited directive' },
    } as const;
    const originals = Object.keys(inherited).map((field) => ({
      field,
      descriptor: Object.getOwnPropertyDescriptor(Object.prototype, field),
    }));
    let accessorCalls = 0;
    let validation: ReturnType<typeof validatePlannerToolCalls> | undefined;
    let applied: Awaited<ReturnType<typeof applyValidatedPlannerToolCalls>> | undefined;
    const { fx, calls } = makeMockEffects();

    try {
      for (const [field, value] of Object.entries(inherited)) {
        Object.defineProperty(Object.prototype, field, {
          configurable: true,
          get() {
            accessorCalls += 1;
            return value;
          },
          set() {
            accessorCalls += 1;
          },
        });
      }
      validation = validatePlannerToolCalls([{ tool: 'spawn_subagents', args: {} }]);
      applied = await applyValidatedPlannerToolCalls(validation, fx, 4);
    } finally {
      for (const { field, descriptor } of originals) {
        if (descriptor) Object.defineProperty(Object.prototype, field, descriptor);
        else Reflect.deleteProperty(Object.prototype, field);
      }
    }

    expect(accessorCalls).toBe(0);
    expect(validation?.errors).toEqual([]);
    expect(validation?.controls_json).toBe('[{"tool":"spawn_subagents","args":{}}]');
    expect(calls).toEqual(['spawnSubagents:{}']);
    expect(applied).toEqual({ emitted_messages: [], errors: [] });
  });

  it('serializes normalized control arrays without consulting inherited Array.prototype.toJSON', () => {
    const original = Object.getOwnPropertyDescriptor(Array.prototype, 'toJSON');
    let inheritedToJsonHits = 0;
    let validation: ReturnType<typeof validatePlannerToolCalls> | undefined;
    try {
      Object.defineProperty(Array.prototype, 'toJSON', {
        configurable: true,
        value(this: unknown[]) {
          inheritedToJsonHits += 1;
          return this.length === 1 && this[0] === 'security' ? ['PWNED_SCOPE'] : this;
        },
      });
      validation = validatePlannerToolCalls([
        {
          tool: 'request_review' as never,
          args: {
            checkpoint_sha: 'a'.repeat(40),
            source_run_id: 'source-run',
            source_iter: 3,
            scope: ['security'],
            idempotency_key: 'prototype-safe-review',
          },
        },
      ]);
    } finally {
      if (original) Object.defineProperty(Array.prototype, 'toJSON', original);
      else Reflect.deleteProperty(Array.prototype, 'toJSON');
    }

    expect(inheritedToJsonHits).toBe(0);
    expect(validation?.errors).toEqual([]);
    expect(JSON.parse(validation?.controls_json ?? 'null')).toEqual(validation?.calls);
    expect(validation?.controls_json).not.toContain('PWNED_SCOPE');
  });

  it.each([
    ['coder_engine', 'codex'],
    ['customEngine', { name: 'inherited-custom-engine' }],
  ] as const)('ignores inherited spawn_subagents field %s', (field, value) => {
    const original = Object.getOwnPropertyDescriptor(Object.prototype, field);
    Object.defineProperty(Object.prototype, field, { configurable: true, value });
    try {
      const result = validatePlannerToolCalls([{ tool: 'spawn_subagents', args: {} }]);
      expect(result.errors).toEqual([]);
      expect(result.calls).toEqual([{ tool: 'spawn_subagents', args: {} }]);
    } finally {
      if (original) Object.defineProperty(Object.prototype, field, original);
      else Reflect.deleteProperty(Object.prototype, field);
    }
  });

  it.each([
    ['coder_engine', 'codex'],
    ['coder_model', 'gpt-coder'],
    ['reviewer_engine', 'gemini'],
    ['reviewer_model', 'gemini-review'],
    ['initial_directive', { goal: 'must not be read' }],
  ] as const)('rejects an own spawn_subagents accessor for %s without invoking it', (field, value) => {
    let getterCalls = 0;
    const args: Record<string, unknown> = {};
    Object.defineProperty(args, field, {
      enumerable: true,
      get() {
        getterCalls += 1;
        return value;
      },
    });

    const result = validatePlannerToolCalls([{ tool: 'spawn_subagents', args }]);

    expect(result.errors).toEqual([
      expect.objectContaining({
        tool: 'spawn_subagents',
        error: expect.stringMatching(new RegExp(`${field}.*own data property`, 'i')),
      }),
    ]);
    expect(result.calls).toEqual([]);
    expect(getterCalls).toBe(0);
  });

  it('accepts all valid own spawn_subagents data properties', () => {
    const result = validatePlannerToolCalls([
      {
        tool: 'spawn_subagents',
        args: {
          coder_engine: 'codex',
          coder_model: 'gpt-coder',
          reviewer_engine: 'gemini',
          reviewer_model: 'gemini-review',
          initial_directive: { goal: 'ship it' },
        },
      },
    ]);

    expect(result.errors).toEqual([]);
    expect(result.calls).toEqual([
      {
        tool: 'spawn_subagents',
        args: {
          coder_engine: 'codex',
          coder_model: 'gpt-coder',
          reviewer_engine: 'gemini',
          reviewer_model: 'gemini-review',
          initial_directive: {
            goal: 'ship it',
            constraints: [],
            success_criteria: [],
            max_attempts: 1,
          },
        },
      },
    ]);
  });

  it('does not inherit nested initial_directive fields from Object.prototype', () => {
    const fields = {
      goal: 'inherited goal',
      constraints: ['inherited constraint'],
      success_criteria: ['inherited success'],
      max_attempts: 9,
    } as const;
    const originals = Object.fromEntries(
      Object.keys(fields).map((field) => [field, Object.getOwnPropertyDescriptor(Object.prototype, field)]),
    );
    try {
      for (const [field, value] of Object.entries(fields)) {
        Object.defineProperty(Object.prototype, field, { configurable: true, value });
      }

      const missingGoal = validatePlannerToolCalls([{ tool: 'spawn_subagents', args: { initial_directive: {} } }]);
      const ownGoal = validatePlannerToolCalls([
        { tool: 'spawn_subagents', args: { initial_directive: { goal: 'own goal' } } },
      ]);

      expect(missingGoal.calls).toEqual([]);
      expect(missingGoal.errors).toEqual([
        expect.objectContaining({
          tool: 'spawn_subagents',
          error: expect.stringMatching(/initial_directive goal.*non-empty string/i),
        }),
      ]);
      expect(ownGoal.errors).toEqual([]);
      expect(ownGoal.calls).toEqual([
        {
          tool: 'spawn_subagents',
          args: {
            initial_directive: {
              goal: 'own goal',
              constraints: [],
              success_criteria: [],
              max_attempts: 1,
            },
          },
        },
      ]);
    } finally {
      for (const field of Object.keys(fields)) {
        const original = originals[field];
        if (original) Object.defineProperty(Object.prototype, field, original);
        else Reflect.deleteProperty(Object.prototype, field);
      }
    }
  });

  it.each([
    ['goal', 'accessor goal'],
    ['constraints', ['accessor constraint']],
    ['success_criteria', ['accessor success']],
    ['max_attempts', 4],
  ] as const)('rejects an own nested initial_directive accessor for %s without invoking it', (field, value) => {
    let getterCalls = 0;
    const initialDirective: Record<string, unknown> = { goal: 'own goal' };
    Object.defineProperty(initialDirective, field, {
      configurable: true,
      enumerable: true,
      get() {
        getterCalls += 1;
        return value;
      },
    });

    const result = validatePlannerToolCalls([
      { tool: 'spawn_subagents', args: { initial_directive: initialDirective } },
    ]);

    expect(result.calls).toEqual([]);
    expect(result.errors).toEqual([
      expect.objectContaining({
        tool: 'spawn_subagents',
        error: expect.stringMatching(new RegExp(`initial_directive ${field}.*own data property`, 'i')),
      }),
    ]);
    expect(getterCalls).toBe(0);
  });

  it('does not inherit send_directive fields from Object.prototype', () => {
    const fields = {
      goal: 'inherited goal',
      constraints: ['inherited constraint'],
      success_criteria: ['inherited success'],
      max_attempts: 9,
    } as const;
    const originals = Object.fromEntries(
      Object.keys(fields).map((field) => [field, Object.getOwnPropertyDescriptor(Object.prototype, field)]),
    );
    try {
      for (const [field, value] of Object.entries(fields)) {
        Object.defineProperty(Object.prototype, field, { configurable: true, value });
      }

      const missingGoal = validatePlannerToolCalls([{ tool: 'send_directive', args: {} }]);
      const ownGoal = validatePlannerToolCalls([{ tool: 'send_directive', args: { goal: 'own goal' } }]);

      expect(missingGoal.calls).toEqual([]);
      expect(missingGoal.errors).toEqual([
        expect.objectContaining({
          tool: 'send_directive',
          error: expect.stringMatching(/send_directive goal.*non-empty string/i),
        }),
      ]);
      expect(ownGoal.errors).toEqual([]);
      expect(ownGoal.calls).toEqual([
        {
          tool: 'send_directive',
          args: {
            goal: 'own goal',
            constraints: [],
            success_criteria: [],
            max_attempts: 1,
          },
        },
      ]);
    } finally {
      for (const field of Object.keys(fields)) {
        const original = originals[field];
        if (original) Object.defineProperty(Object.prototype, field, original);
        else Reflect.deleteProperty(Object.prototype, field);
      }
    }
  });

  it.each([
    ['goal', 'accessor goal'],
    ['constraints', ['accessor constraint']],
    ['success_criteria', ['accessor success']],
    ['max_attempts', 4],
  ] as const)('rejects an own send_directive accessor for %s without invoking it', (field, value) => {
    let getterCalls = 0;
    const args: Record<string, unknown> = { goal: 'own goal' };
    Object.defineProperty(args, field, {
      configurable: true,
      enumerable: true,
      get() {
        getterCalls += 1;
        return value;
      },
    });

    const result = validatePlannerToolCalls([{ tool: 'send_directive', args }]);

    expect(result.calls).toEqual([]);
    expect(result.errors).toEqual([
      expect.objectContaining({
        tool: 'send_directive',
        error: expect.stringMatching(new RegExp(`send_directive ${field}.*own data property`, 'i')),
      }),
    ]);
    expect(getterCalls).toBe(0);
  });

  it('accepts valid own send_directive data properties and preserves optional defaults', () => {
    const goalOnly = validatePlannerToolCalls([{ tool: 'send_directive', args: { goal: 'ship it' } }]);
    const allFields = validatePlannerToolCalls([
      {
        tool: 'send_directive',
        args: {
          goal: 'ship it',
          constraints: ['no new deps'],
          success_criteria: ['focused tests pass'],
          max_attempts: 3,
        },
      },
    ]);

    expect(goalOnly.errors).toEqual([]);
    expect(goalOnly.calls).toEqual([
      {
        tool: 'send_directive',
        args: {
          goal: 'ship it',
          constraints: [],
          success_criteria: [],
          max_attempts: 1,
        },
      },
    ]);
    expect(allFields.errors).toEqual([]);
    expect(allFields.calls).toEqual([
      {
        tool: 'send_directive',
        args: {
          goal: 'ship it',
          constraints: ['no new deps'],
          success_criteria: ['focused tests pass'],
          max_attempts: 3,
        },
      },
    ]);
  });

  it.each([
    ['send_directive', 'constraints'],
    ['send_directive', 'success_criteria'],
    ['spawn_subagents', 'constraints'],
    ['spawn_subagents', 'success_criteria'],
  ] as const)('rejects a %s %s element accessor without invoking it or effects', async (tool, field) => {
    let getterCalls = 0;
    const entries: string[] = [];
    Object.defineProperty(entries, '0', {
      configurable: true,
      enumerable: true,
      get() {
        getterCalls += 1;
        return 'must not be read';
      },
    });
    const directive = { goal: 'ship it', [field]: entries };
    const control =
      tool === 'send_directive' ? { tool, args: directive } : { tool, args: { initial_directive: directive } };
    const { fx, calls } = makeMockEffects();

    const result = await applyPlannerToolCalls([control], fx, 0);

    expect(result.errors).toEqual([
      expect.objectContaining({ tool, error: expect.stringMatching(new RegExp(`${field}.*own data property`, 'i')) }),
    ]);
    expect(result.emitted_messages).toEqual([]);
    expect(calls).toEqual([]);
    expect(getterCalls).toBe(0);
  });

  it.each([
    ['send_directive', 'constraints', 'hole'],
    ['send_directive', 'success_criteria', 'prototype index'],
    ['spawn_subagents', 'constraints', 'prototype index'],
    ['spawn_subagents', 'success_criteria', 'hole'],
  ] as const)('rejects a %s %s array with a %s before effects', async (tool, field, shape) => {
    const entries: string[] = [];
    entries.length = 1;
    const original = Object.getOwnPropertyDescriptor(Array.prototype, '0');
    if (shape === 'prototype index') {
      Object.defineProperty(Array.prototype, '0', {
        configurable: true,
        enumerable: false,
        value: 'inherited entry',
        writable: true,
      });
    }
    const directive = { goal: 'ship it', [field]: entries };
    const control =
      tool === 'send_directive' ? { tool, args: directive } : { tool, args: { initial_directive: directive } };
    const { fx, calls } = makeMockEffects();
    let result: Awaited<ReturnType<typeof applyPlannerToolCalls>> | undefined;

    try {
      result = await applyPlannerToolCalls([control], fx, 0);
    } finally {
      if (original) Object.defineProperty(Array.prototype, '0', original);
      else Reflect.deleteProperty(Array.prototype, '0');
    }

    expect(result?.errors).toEqual([
      expect.objectContaining({ tool, error: expect.stringMatching(new RegExp(`${field}.*own data property`, 'i')) }),
    ]);
    expect(result?.emitted_messages).toEqual([]);
    expect(calls).toEqual([]);
  });

  it.each([
    ['send_directive', 'constraints'],
    ['spawn_subagents', 'success_criteria'],
  ] as const)('rejects an over-limit %s %s array before visiting any element', async (tool, field) => {
    let getterCalls = 0;
    const entries: string[] = [];
    Object.defineProperty(entries, '0', {
      configurable: true,
      enumerable: true,
      get() {
        getterCalls += 1;
        throw new Error('over-limit array element must not be visited');
      },
    });
    entries.length = 0xffff_ffff;
    const directive = { goal: 'ship it', [field]: entries };
    const control =
      tool === 'send_directive' ? { tool, args: directive } : { tool, args: { initial_directive: directive } };
    const { fx, calls } = makeMockEffects();

    const result = await applyPlannerToolCalls([control], fx, 0);

    expect(result.errors).toEqual([
      expect.objectContaining({ tool, error: expect.stringMatching(new RegExp(`${field}.*128-item limit`, 'i')) }),
    ]);
    expect(result.emitted_messages).toEqual([]);
    expect(calls).toEqual([]);
    expect(getterCalls).toBe(0);
  });

  it.each([
    ['Coder then Reviewer', ['spawn_coder', 'spawn_reviewer']],
    ['Coder then notification', ['spawn_coder', 'notify_user']],
    ['notification then Reviewer', ['notify_user', 'spawn_reviewer']],
  ] as const)('rejects a non-atomic %s batch before either control has an effect', async (_label, order) => {
    const { fx, calls } = makeMockEffects();
    const controls = order.map((tool) => {
      if (tool === 'spawn_coder') {
        return { tool, args: { coder_engine: 'codex', coder_model: 'gpt-coder' } };
      }
      if (tool === 'spawn_reviewer') {
        return { tool, args: { reviewer_engine: 'gemini', reviewer_model: 'gemini-review' } };
      }
      return { tool, args: { summary: 'must not emit' } };
    });
    const result = await applyPlannerToolCalls(controls as never, fx, 4);

    expect(result.emitted_messages).toEqual([]);
    expect(result.errors).toEqual([
      expect.objectContaining({
        tool: expect.stringMatching(/^spawn_(coder|reviewer)$/),
        error: expect.stringMatching(/only|single|batch|atomic/i),
      }),
    ]);
    expect(calls).toEqual([]);
  });

  it('keeps each independent Coder and Reviewer spawn usable as a standalone control', async () => {
    const { fx, calls } = makeMockEffects();

    const coder = await applyPlannerToolCalls(
      [{ tool: 'spawn_coder' as never, args: { coder_engine: 'codex', coder_model: 'gpt-coder' } }],
      fx,
      4,
    );
    const reviewer = await applyPlannerToolCalls(
      [{ tool: 'spawn_reviewer' as never, args: { reviewer_engine: 'gemini', reviewer_model: 'gemini-review' } }],
      fx,
      4,
    );

    expect(coder).toEqual({ emitted_messages: [], errors: [] });
    expect(reviewer).toEqual({ emitted_messages: [], errors: [] });
    expect(calls).toEqual([
      'spawnCoder:{"coder_engine":"codex","coder_model":"gpt-coder"}',
      'spawnReviewer:{"reviewer_engine":"gemini","reviewer_model":"gemini-review"}',
    ]);
  });

  it('canonicalizes and applies a Reviewer-only request for an existing checkpoint', async () => {
    const { fx, calls } = makeMockEffects();
    const checkpoint = 'A'.repeat(40);
    const control = {
      tool: 'request_review' as never,
      args: {
        checkpoint_sha: checkpoint,
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security', 'regression'],
        idempotency_key: 'review-source-run-7',
      },
    };

    const validation = validatePlannerToolCalls([control]);
    expect(validation.errors).toEqual([]);
    expect(validation.calls).toEqual([
      {
        tool: 'request_review',
        args: {
          checkpoint_sha: checkpoint.toLowerCase(),
          idempotency_key: 'review-source-run-7',
          scope: ['security', 'regression'],
          source_iter: 7,
          source_run_id: 'source-run',
        },
      },
    ]);

    const result = await applyPlannerToolCalls([control], fx, 0);
    expect(result.errors).toEqual([]);
    expect(result.emitted_messages).toHaveLength(1);
    expect(validateMessage(result.emitted_messages[0])).toMatchObject({
      iter: 0,
      from: 'runner',
      to: 'reviewer',
      type: 'review_request',
      payload: {
        iter: 0,
        ledger_path: '/trusted/run',
        prior_metrics: [],
        checkpoint_sha: checkpoint.toLowerCase(),
        source_run_id: 'source-run',
        source_iter: 7,
        scope: ['security', 'regression'],
        idempotency_key: 'review-source-run-7',
      },
    });
    expect(calls).toEqual([
      `requestReview:{"checkpoint_sha":"${checkpoint.toLowerCase()}","idempotency_key":"review-source-run-7","scope":["security","regression"],"source_iter":7,"source_run_id":"source-run"}`,
    ]);
  });

  it('emits one canonical review_request after preparation and no message for a successful duplicate', async () => {
    const { fx } = makeMockEffects();
    const args = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'review-source-run-3',
    };

    const first = await applyPlannerToolCalls([{ tool: 'request_review' as never, args }], fx, 0);
    fx.requestReview = async (request) => ({
      status: 'duplicate' as const,
      target: 'reviewer' as const,
      idempotency_key: request.idempotency_key,
    });
    const duplicate = await applyPlannerToolCalls([{ tool: 'request_review' as never, args }], fx, 0);

    expect(first.errors).toEqual([]);
    expect(first.emitted_messages).toHaveLength(1);
    expect(validateMessage(first.emitted_messages[0])).toMatchObject({
      iter: 0,
      type: 'review_request',
      payload: expect.objectContaining({ iter: 0, source_iter: 3 }),
    });
    expect(duplicate).toEqual({ emitted_messages: [], errors: [] });
  });

  it('releases a prepared request when the post-preparation active fence rejects the handoff', async () => {
    const { fx } = makeMockEffects();
    const releaseReviewRequest = vi.fn();
    let activeChecks = 0;
    fx.assertActive = () => {
      activeChecks += 1;
      if (activeChecks === 2) throw new Error('run became terminal after preparation');
    };
    (
      fx as PlannerToolEffects & {
        releaseReviewRequest?: (idempotencyKey: string, payload: CheckpointReviewRequestPayload) => void;
      }
    ).releaseReviewRequest = releaseReviewRequest;
    const args = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'release-after-active-fence',
    };

    const result = await applyPlannerToolCalls([{ tool: 'request_review' as never, args }], fx, 0);

    expect(result.emitted_messages).toEqual([]);
    expect(result.errors).toEqual([{ tool: 'request_review', error: 'run became terminal after preparation' }]);
    expect(releaseReviewRequest).toHaveBeenCalledOnce();
    expect(releaseReviewRequest).toHaveBeenCalledWith(
      args.idempotency_key,
      expect.objectContaining({ idempotency_key: args.idempotency_key }),
    );
  });

  it('fails before durable review preparation when the reclaim hook is unavailable', async () => {
    const { fx } = makeMockEffects();
    const requestReview = vi.fn(fx.requestReview!);
    fx.requestReview = requestReview;
    delete fx.releaseReviewRequest;
    const args = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 3,
      scope: ['correctness'],
      idempotency_key: 'missing-release-hook',
    };

    const result = await applyPlannerToolCalls([{ tool: 'request_review' as never, args }], fx, 0);

    expect(result).toEqual({
      emitted_messages: [],
      errors: [{ tool: 'request_review', error: 'request_review release handler is not installed' }],
    });
    expect(requestReview).not.toHaveBeenCalled();
  });

  it('types a prepared review as the checkpoint-specific wire payload', () => {
    expectTypeOf<PreparedReviewRequest['payload']>().toEqualTypeOf<CheckpointReviewRequestPayload>();
  });

  it.each([
    ['before', ['request_review', 'notify_user']],
    ['after', ['notify_user', 'request_review']],
    ['another request_review', ['request_review', 'request_review']],
  ] as const)('rejects a request_review with a %s sibling before any batch effect', async (_label, order) => {
    const { fx, calls, policyDelta, writes } = makeMockEffects();
    const requestReview = {
      tool: 'request_review' as never,
      args: {
        checkpoint_sha: 'a'.repeat(40),
        source_run_id: 'source-run',
        source_iter: 3,
        scope: ['correctness'],
        idempotency_key: 'singleton-review',
      },
    };
    const notify = { tool: 'notify_user' as const, args: { summary: 'must not emit' } };
    const controls = order.map((tool, index) =>
      tool === 'request_review'
        ? {
            ...requestReview,
            args: { ...requestReview.args, idempotency_key: `singleton-review-${index}` },
          }
        : notify,
    );

    const validation = validatePlannerToolCalls(controls);
    const result = await applyPlannerToolCalls(controls, fx, 0);

    expect(validation.calls).toEqual([]);
    expect(validation.errors).toEqual([
      expect.objectContaining({
        tool: 'request_review',
        error: expect.stringMatching(/only|singleton|sibling/i),
      }),
    ]);
    expect(result).toEqual({ emitted_messages: [], errors: validation.errors });
    expect(calls).toEqual([]);
    expect(policyDelta).toEqual({});
    expect(writes).toEqual([]);
  });

  it.each([
    ['short checkpoint', { checkpoint_sha: 'abc' }, 'checkpoint_sha'],
    ['empty source run', { source_run_id: '   ' }, 'source_run_id'],
    ['parent traversal source run', { source_run_id: '../source-run' }, 'source_run_id'],
    ['forward-slash source run', { source_run_id: 'source/run' }, 'source_run_id'],
    ['backslash source run', { source_run_id: 'source\\run' }, 'source_run_id'],
    ['leading-whitespace source run', { source_run_id: ' source-run' }, 'source_run_id'],
    ['trailing-whitespace source run', { source_run_id: 'source-run ' }, 'source_run_id'],
    ['negative iteration', { source_iter: -1 }, 'source_iter'],
    ['unsafe iteration', { source_iter: Number.MAX_SAFE_INTEGER + 1 }, 'source_iter'],
    ['empty scope', { scope: [] }, 'scope'],
    ['blank scope member', { scope: ['security', '  '] }, 'scope'],
    ['leading-whitespace scope member', { scope: [' security'] }, 'scope'],
    ['trailing-whitespace scope member', { scope: ['security '] }, 'scope'],
    ['too many scope members', { scope: Array.from({ length: 129 }, () => 'security') }, 'scope'],
    ['oversized UTF-8 scope member', { scope: ['é'.repeat(4_097)] }, 'scope'],
    ['empty idempotency key', { idempotency_key: '' }, 'idempotency_key'],
    ['leading-whitespace idempotency key', { idempotency_key: ' review-source-run-7' }, 'idempotency_key'],
    ['trailing-whitespace idempotency key', { idempotency_key: 'review-source-run-7 ' }, 'idempotency_key'],
    ['oversized UTF-8 idempotency key', { idempotency_key: 'é'.repeat(4_097) }, 'idempotency_key'],
  ])('rejects request_review with %s before any effect', async (_label, override, expectedField) => {
    const { fx, calls } = makeMockEffects();
    const args = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-source-run-7',
      ...override,
    };

    const result = await applyPlannerToolCalls([{ tool: 'request_review' as never, args }], fx, 7);

    expect(result.emitted_messages).toEqual([]);
    expect(result.errors).toEqual([
      expect.objectContaining({ tool: 'request_review', error: expect.stringContaining(expectedField) }),
    ]);
    expect(calls).toEqual([]);
  });

  it('enforces the same request_review scope and UTF-8 metadata limits at the message boundary', () => {
    const base = {
      iter: 0,
      ledger_path: '/trusted/run',
      prior_metrics: [],
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: 7,
      scope: ['security'],
      idempotency_key: 'review-source-run-7',
    };
    const message = (payload: Record<string, unknown>) =>
      ({
        v: 2,
        msg_id: 'm-request-review-limits',
        run_id: 'r1',
        iter: 0,
        ts: new Date().toISOString(),
        from: 'runner',
        to: 'reviewer',
        type: 'review_request',
        payload,
      }) as unknown as AnyAutoloopMessage;

    expect(() => validateMessage(message({ ...base, scope: [' security'] }))).toThrow(/scope/i);
    expect(() => validateMessage(message({ ...base, scope: Array.from({ length: 129 }, () => 'security') }))).toThrow(
      /scope.*128|128.*scope/i,
    );
    expect(() => validateMessage(message({ ...base, scope: ['é'.repeat(4_097)] }))).toThrow(
      /scope.*8192|8192.*scope|scope.*oversized/i,
    );
    expect(() => validateMessage(message({ ...base, idempotency_key: 'é'.repeat(4_097) }))).toThrow(
      /idempotency_key.*8192|8192.*idempotency_key|idempotency_key.*bounded/i,
    );
    expect(() => validateMessage(message({ ...base, ledger_path: 'é'.repeat(4_097) }))).toThrow(
      /ledger_path.*8192|8192.*ledger_path|ledger_path.*bounded/i,
    );

    expect(() =>
      validateMessage(
        message({
          ...base,
          ledger_path: 'é'.repeat(4_096),
          scope: Array.from({ length: 128 }, () => 'x'.repeat(8_192)),
          idempotency_key: 'x'.repeat(8_192),
        }),
      ),
    ).not.toThrow();
  });

  it('names Planner request_review args separately from wire review_request payload errors', () => {
    const invalid = {
      checkpoint_sha: 'a'.repeat(40),
      source_run_id: 'source-run',
      source_iter: -1,
      scope: ['correctness'],
      idempotency_key: 'separate-validator-labels',
    };

    const planner = validatePlannerToolCalls([{ tool: 'request_review' as never, args: invalid }]);
    expect(planner.errors).toEqual([
      expect.objectContaining({ error: expect.stringMatching(/^Request_review payload is invalid:/) }),
    ]);
    expect(() =>
      validateMessage(
        Msg.reviewRequest(0, {
          iter: 0,
          ledger_path: '/trusted/run',
          prior_metrics: [],
          ...invalid,
        }),
      ),
    ).toThrow(/^Review_request payload is invalid:/);
  });

  it('rejects hostile request_review accessors and sparse scope arrays without invoking them or effects', async () => {
    let getterCalls = 0;
    const hostileScope: string[] = [];
    Object.defineProperty(hostileScope, '0', {
      enumerable: true,
      get: () => {
        getterCalls += 1;
        throw new Error('scope getter must not run');
      },
    });
    hostileScope.length = 1;
    const args = Object.create(null) as Record<string, unknown>;
    Object.defineProperties(args, {
      checkpoint_sha: { enumerable: true, value: 'a'.repeat(40) },
      source_run_id: { enumerable: true, value: 'source-run' },
      source_iter: { enumerable: true, value: 7 },
      scope: { enumerable: true, value: hostileScope },
      idempotency_key: { enumerable: true, value: 'review-source-run-7' },
    });
    const { fx, calls } = makeMockEffects();

    const result = await applyPlannerToolCalls([{ tool: 'request_review' as never, args }], fx, 7);

    expect(result.errors).toEqual([
      expect.objectContaining({
        tool: 'request_review',
        error: expect.stringMatching(/scope.*array|scope.*own|scope.*data/i),
      }),
    ]);
    expect(result.emitted_messages).toEqual([]);
    expect(getterCalls).toBe(0);
    expect(calls).toEqual([]);
  });

  it('only forwards the documented spawn fields to the effect', async () => {
    const { fx, calls } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [
        {
          tool: 'spawn_subagents',
          args: {
            coder_engine: 'codex',
            reviewer_model: 'reviewer-model',
            env: { SECRET: 'must-not-cross-boundary' },
            unknown_field: 'ignored',
          },
        },
      ],
      fx,
      0,
    );

    expect(r.errors).toEqual([]);
    expect(calls).toEqual(['spawnSubagents:{"coder_engine":"codex","reviewer_model":"reviewer-model"}']);
  });

  it('rejects unknown engines before spawning or emitting an initial directive', async () => {
    const { fx, calls } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [
        {
          tool: 'spawn_subagents',
          args: {
            coder_engine: 'not-real',
            initial_directive: { goal: 'must not run' },
          },
        },
      ],
      fx,
      0,
    );

    expect(r.errors).toHaveLength(1);
    expect(r.errors[0].error).toContain('coder_engine');
    expect(r.errors[0].error).toContain('not-real');
    expect(calls).toEqual([]);
    expect(r.emitted_messages).toEqual([]);
  });

  it('rejects custom engine configs emitted by the Planner', async () => {
    const { fx, calls } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [
        {
          tool: 'spawn_subagents',
          args: {
            coder_engine: 'custom',
            coder_custom_engine: { name: 'unsafe', bin: 'unsafe-cli', args: {} },
          },
        },
        {
          tool: 'spawn_subagents',
          args: {
            reviewer_engine: 'custom',
            reviewerCustomEngine: { name: 'unsafe', bin: 'unsafe-cli', args: {} },
          },
        },
      ],
      fx,
      0,
    );

    expect(r.errors).toHaveLength(2);
    expect(r.errors.every((entry) => entry.error.includes('custom engine config'))).toBe(true);
    expect(calls).toEqual([]);
  });

  it('rejects a non-final lifecycle control atomically and emits valid final lifecycle controls', async () => {
    const { fx } = makeMockEffects();
    const invalid = await applyPlannerToolCalls(
      [
        { tool: 'pause_loop', args: { reason: 'rethink' } },
        { tool: 'resume_loop', args: {} },
        { tool: 'terminate', args: { reason: 'done' } },
      ],
      fx,
      0,
    );
    expect(invalid.emitted_messages).toEqual([]);
    expect(invalid.errors).toEqual([
      { tool: 'pause_loop', error: 'pause_loop must be the final Planner control in its batch' },
    ]);

    const pause = await applyPlannerToolCalls([{ tool: 'pause_loop', args: { reason: 'rethink' } }], fx, 0);
    expect(pause.errors).toEqual([]);
    expect(pause.emitted_messages.map((message: AnyAutoloopMessage) => message.type)).toEqual(['pause']);

    const terminate = await applyPlannerToolCalls(
      [
        { tool: 'resume_loop', args: {} },
        { tool: 'terminate', args: { reason: 'done' } },
      ],
      fx,
      0,
    );
    expect(terminate.errors).toEqual([]);
    expect(terminate.emitted_messages.map((message: AnyAutoloopMessage) => message.type)).toEqual([
      'resume',
      'terminate',
    ]);
  });

  it('update_push_policy mutates via the effect', async () => {
    const { fx, policyDelta } = makeMockEffects();
    await applyPlannerToolCalls(
      [{ tool: 'update_push_policy', args: { on_iter_done_ok: { level: 'info', channel: 'wechat' } } }],
      fx,
      0,
    );
    expect(policyDelta.on_iter_done_ok).toEqual({ level: 'info', channel: 'wechat' });
  });

  it('rejects a silence-only critical policy control at the shared validate/apply boundary', async () => {
    const control = {
      tool: 'update_push_policy' as const,
      args: { on_phase_error: { silent: true } },
    };

    const validation = validatePlannerToolCalls([control]);
    expect(validation.calls).toEqual([]);
    expect(validation.errors).toEqual([
      expect.objectContaining({
        tool: 'update_push_policy',
        error: expect.stringContaining('critical policy silence'),
      }),
    ]);

    const { fx, calls, policyDelta } = makeMockEffects();
    const applied = await applyPlannerToolCalls([control], fx, 0);
    expect(applied.emitted_messages).toEqual([]);
    expect(applied.errors).toEqual(validation.errors);
    expect(calls).toEqual([]);
    expect(policyDelta).toEqual({});
  });

  it('records error for unknown tool names without throwing', async () => {
    const { fx } = makeMockEffects();
    const r = await applyPlannerToolCalls([{ tool: 'nonsense' as 'notify_user', args: {} }], fx, 0);
    expect(r.errors).toHaveLength(1);
    expect(r.errors[0].tool).toBe('nonsense');
  });

  it('records error when notify_user is missing summary', async () => {
    const { fx } = makeMockEffects();
    const r = await applyPlannerToolCalls([{ tool: 'notify_user', args: { level: 'info' } }], fx, 0);
    expect(r.errors).toHaveLength(1);
    expect(r.errors[0].error).toContain('summary');
  });

  it('write_plan writes plan.md content via the effect', async () => {
    const { fx, writes } = makeMockEffects();
    const r = await applyPlannerToolCalls(
      [{ tool: 'write_plan', args: { content: '# Plan\n\n## Goal\nbuild it.\n', commit_message: 'first plan' } }],
      fx,
      0,
    );
    expect(r.errors).toEqual([]);
    expect(writes).toHaveLength(1);
    expect(writes[0].file).toBe('plan.md');
    expect(writes[0].content).toContain('## Goal');
    expect(writes[0].msg).toBe('first plan');
  });

  it.each([
    {
      tool: 'write_plan' as const,
      file: 'plan.md' as const,
      content: '# Canonical plan',
      commitMessage: 'autoloop: planner writes plan.md',
    },
    {
      tool: 'write_goal' as const,
      file: 'goal.json' as const,
      content: '{"gates":[]}',
      commitMessage: 'autoloop: planner writes goal.json',
    },
  ])(
    'canonicalizes an omitted $tool commit_message before persistence and effect application',
    async ({ tool, file, content, commitMessage }) => {
      const control = { tool, args: { content } };
      const validation = validatePlannerToolCalls([control]);
      expect(validation.errors).toEqual([]);
      expect(validation.calls).toEqual([{ tool, args: { commit_message: commitMessage, content } }]);
      expect(JSON.parse(validation.controls_json ?? 'null')).toEqual(validation.calls);

      const { fx, writes } = makeMockEffects();
      const applied = await applyPlannerToolCalls([control], fx, 0);
      expect(applied.errors).toEqual([]);
      expect(writes).toEqual([{ file, content, msg: commitMessage }]);
    },
  );

  it('write_goal validates JSON before delegating to the effect', async () => {
    const { fx, writes } = makeMockEffects();
    const ok = await applyPlannerToolCalls(
      [{ tool: 'write_goal', args: { content: '{"scalar":null,"gates":[]}' } }],
      fx,
      0,
    );
    expect(ok.errors).toEqual([]);
    expect(writes).toHaveLength(1);
    expect(writes[0].file).toBe('goal.json');

    const bad = await applyPlannerToolCalls([{ tool: 'write_goal', args: { content: '{not json' } }], fx, 0);
    expect(bad.errors).toHaveLength(1);
    expect(bad.errors[0].error).toMatch(/not valid JSON/);
    expect(writes).toHaveLength(1); // unchanged — bad call must not write
  });

  it.each([
    { label: 'null', content: 'null' },
    { label: 'array', content: '[]' },
    { label: 'string', content: '"goal"' },
    { label: 'number', content: '42' },
    { label: 'boolean', content: 'true' },
  ])('rejects a $label write_goal payload before any effect', async ({ content }) => {
    const { fx, writes } = makeMockEffects();
    const result = await applyPlannerToolCalls([{ tool: 'write_goal', args: { content } }], fx, 0);

    expect(result.emitted_messages).toEqual([]);
    expect(result.errors).toEqual([
      expect.objectContaining({
        tool: 'write_goal',
        error: expect.stringContaining('plain JSON object'),
      }),
    ]);
    expect(writes).toEqual([]);
  });

  it('write_plan rejects empty content (would erase plan.md)', async () => {
    const { fx, writes } = makeMockEffects();
    const r = await applyPlannerToolCalls([{ tool: 'write_plan', args: { content: '   \n  ' } }], fx, 0);
    expect(r.errors).toHaveLength(1);
    expect(r.errors[0].error).toContain('non-empty');
    expect(writes).toEqual([]);
  });
});
