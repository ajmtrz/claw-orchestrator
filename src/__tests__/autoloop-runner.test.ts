/**
 * Unit tests for autoloop runner skeleton (S1 — no real LLM).
 *
 * Strategy: inject a scripted AgentDispatcher that produces canned replies, then
 * drive the runner through a representative iter and assert routing invariants.
 */

import { describe, it, expect, expectTypeOf, vi, afterEach } from 'vitest';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { ClaudeAgentDispatcher } from '../autoloop/dispatcher.js';
import {
  type AnyAutoloopMessage,
  type ReviewVerdictPayload,
  AutoloopRoutingError,
  Msg,
  canonicalizeMessage,
  deserialise,
  serialise,
  validateMessage,
} from '../autoloop/messages.js';
import { AutoloopRunner } from '../autoloop/runner.js';
import type { SessionManager } from '../session-manager.js';
import type { AgentDispatcher, AutoloopConfig, PhysicalAgentGeneration } from '../autoloop/types.js';

function makeRunner(
  dispatcher: AgentDispatcher,
  recordedPushes: AnyAutoloopMessage[] = [],
  overrides: Partial<AutoloopConfig> = {},
): {
  runner: AutoloopRunner;
  pushes: Array<{ level: string; summary: string }>;
} {
  const pushes: Array<{ level: string; summary: string }> = [];
  const config: AutoloopConfig = {
    run_id: 'test-run',
    workspace: '/tmp/test',
    ledger_dir: '/tmp/test/ledger',
    notifyUser: async (level, summary) => {
      pushes.push({ level, summary });
      void recordedPushes;
    },
    dispatcher,
    // Disable the real interval timer in tests by default.
    stallCheckIntervalMs: 24 * 60 * 60 * 1000,
    ...overrides,
  };
  const runner = new AutoloopRunner(config);
  return { runner, pushes };
}

function makeCanonicalValidationHarness(overrides: Partial<AutoloopConfig> = {}): {
  runner: AutoloopRunner;
  effects: {
    reserveAgentGeneration: ReturnType<typeof vi.fn>;
    startSession: ReturnType<typeof vi.fn>;
    sendMessage: ReturnType<typeof vi.fn>;
    stopSession: ReturnType<typeof vi.fn>;
    notifyUser: ReturnType<typeof vi.fn>;
  };
  ledgerDir: string;
  cleanup: () => void;
} {
  const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'autoloop-runner-routing-'));
  const effects = {
    reserveAgentGeneration: vi.fn((_generation: PhysicalAgentGeneration) => true),
    startSession: vi.fn(async () => ({ state: 'ready' })),
    sendMessage: vi.fn(async () => ({ output: 'must not be sent', error: undefined })),
    stopSession: vi.fn(async () => undefined),
    notifyUser: vi.fn(async () => undefined),
  };
  const manager = {
    autoloopOwnerInstanceId: `session-manager:${process.pid}:00000000-0000-4000-8000-000000000001`,
    ...effects,
    inspect: vi.fn(async () => 'absent'),
    releaseReservation: vi.fn(async () => true),
    getStatus: vi.fn(() => ({ stats: {} })),
    compactSession: vi.fn(async () => undefined),
  } as unknown as SessionManager;
  const dispatcher = new ClaudeAgentDispatcher({ manager, runId: 'runner-routing', workspace });
  const publicDispatcherBoundary: AgentDispatcher = {
    deliver: (env) => dispatcher.deliver(env),
  };
  const ledgerDir = path.join(workspace, 'tasks', 'runner-routing');
  const runner = new AutoloopRunner({
    run_id: 'runner-routing',
    workspace,
    ledger_dir: ledgerDir,
    dispatcher: publicDispatcherBoundary,
    phaseErrorCircuit: 3,
    notifyUser: effects.notifyUser,
    ...overrides,
  });
  return {
    runner,
    effects,
    ledgerDir,
    cleanup: () => {
      runner.stop();
      fs.rmSync(workspace, { recursive: true, force: true });
    },
  };
}

function exactMessageCases(): Array<readonly [string, AnyAutoloopMessage]> {
  return [
    ['chat', Msg.chat(0, { text: 'hello' })],
    [
      'directive',
      Msg.directive(1, {
        goal: 'ship safely',
        constraints: ['no regressions'],
        success_criteria: ['all gates green'],
        max_attempts: 3,
      }),
    ],
    ['directive_ack', Msg.directiveAck(1, { understood: false, clarification: 'Which gate?' })],
    [
      'iter_artifacts',
      Msg.iterArtifacts(1, {
        diff: 'diff --git a/a b/a',
        eval_output: {
          passed: true,
          metric: 0.75,
          note: 'clean',
          nullable: null,
          checks: [{ name: 'unit', exit_code: 0 }, false],
        },
        files_changed: ['src/a.ts'],
      }),
    ],
    ['review_request', Msg.reviewRequest(1, { iter: 1, ledger_path: '/run/ledger', prior_metrics: [0.5, 0.75] })],
    [
      'review_verdict',
      Msg.reviewVerdict(1, {
        decision: 'advance',
        metric: 0.75,
        audit_notes: 'verified',
        accepted: true,
        evidence_id: 'evidence-1',
      }),
    ],
    ['iter_done', Msg.iterDone(1, { iter: 1, verdict: 'advance', metric: 0.75, regression: false })],
    [
      'push_user',
      Msg.pushUser(1, {
        level: 'decision',
        summary: 'Input required',
        detail: 'Choose a path',
        channel: 'both',
      }),
    ],
    ['pause', Msg.pause(1, { reason: 'operator review' })],
    ['resume', Msg.resume(1)],
    ['terminate', Msg.terminate(1, { reason: 'complete' })],
    [
      'phase_error',
      Msg.phaseError(1, {
        agent: 'coder',
        phase: 'coder_turn',
        code: 'AUTOLOOP_LEDGER_COMMITTED_STATE_INVALID',
        committed: true,
        retryable: false,
        error: 'commit could not be verified',
      }),
    ],
    [
      'send_timeout',
      Msg.sendTimeout(1, {
        status: 'awaiting_resume',
        dispatch_id: 'dispatch-1',
        agent: 'reviewer',
        message_id: 'message-1',
        message_type: 'review_request',
        iter: 1,
        timeout_ms: 600_000,
        error: 'deadline exceeded',
      }),
    ],
  ];
}

const TEST_MAX_PRIMITIVE_ARRAY_ITEMS = 10_000;
const TEST_MAX_REPLY_BATCH_ITEMS = 10_000;
const TEST_MAX_MESSAGE_STRING_CODE_UNITS = 1_048_576;
const TEST_MAX_MESSAGE_TOTAL_STRING_CODE_UNITS = 4_194_304;
const TEST_MAX_ITER_ARTIFACT_DIFF_CODE_UNITS = 4_194_304;
const TEST_MAX_EVAL_OUTPUT_DEPTH = 64;
const TEST_MAX_EVAL_OUTPUT_CONTAINER_ITEMS = 10_000;
const TEST_MAX_EVAL_OUTPUT_NODES = 100_000;

function primitiveArrayMessage(
  field: 'constraints' | 'success_criteria' | 'files_changed' | 'prior_metrics' | 'flags',
  value: unknown[],
): AnyAutoloopMessage {
  switch (field) {
    case 'constraints':
      return Msg.directive(0, {
        goal: 'bounded',
        constraints: value as string[],
        success_criteria: [],
        max_attempts: 1,
      });
    case 'success_criteria':
      return Msg.directive(0, {
        goal: 'bounded',
        constraints: [],
        success_criteria: value as string[],
        max_attempts: 1,
      });
    case 'files_changed':
      return Msg.iterArtifacts(0, { diff: '', eval_output: null, files_changed: value as string[] });
    case 'prior_metrics':
      return Msg.reviewRequest(0, { iter: 0, ledger_path: '/run/ledger', prior_metrics: value as number[] });
    case 'flags':
      return Msg.reviewVerdict(0, {
        decision: 'hold',
        metric: null,
        audit_notes: 'compatibility flags',
        flags: value,
      } as unknown as Parameters<typeof Msg.reviewVerdict>[1]);
  }
}

function evalOutputMessage(evalOutput: unknown): AnyAutoloopMessage {
  return Msg.iterArtifacts(0, { diff: '', eval_output: evalOutput, files_changed: [] });
}

function nestedEvalOutput(depth: number): unknown {
  let value: unknown = null;
  for (let index = 0; index < depth; index += 1) value = { child: value };
  return value;
}

function evalOutputWithNodeCount(nodeCount: number): unknown {
  // The root and ten child arrays account for eleven nodes. Nine full child
  // arrays plus one remainder then make the requested exact total.
  const primitiveNodes = nodeCount - 11;
  const fullChildren = 9;
  const remainder = primitiveNodes - fullChildren * TEST_MAX_EVAL_OUTPUT_CONTAINER_ITEMS;
  return [
    ...Array.from({ length: fullChildren }, () =>
      Array.from({ length: TEST_MAX_EVAL_OUTPUT_CONTAINER_ITEMS }, () => null),
    ),
    Array.from({ length: remainder }, () => null),
  ];
}

function expectOrdinaryFrozenArray(actual: unknown[], expected: unknown[]): void {
  expect(Array.isArray(actual)).toBe(true);
  expect(Object.getPrototypeOf(actual)).toBe(Array.prototype);
  expect(actual.map((entry) => entry)).toEqual(expected);
  expect([...actual]).toEqual(expected);
  const iterated: unknown[] = [];
  for (const entry of actual) iterated.push(entry);
  expect(iterated).toEqual(expected);
  expect(Object.isFrozen(actual)).toBe(true);
}

describe('autoloop messages', () => {
  it('Msg constructors build well-formed envelopes', () => {
    const e = Msg.chat(0, { text: 'hello' });
    expect(e.from).toBe('user');
    expect(e.to).toBe('planner');
    expect(e.type).toBe('chat');
    expect(e.payload.text).toBe('hello');
    expect(typeof e.msg_id).toBe('string');
    expect(typeof e.ts).toBe('string');
  });

  it.each(exactMessageCases())('validateMessage returns an immutable exact-schema snapshot for %s', (_type, source) => {
    const canonical = validateMessage(source);

    expect(canonical).toEqual(source);
    expect(canonical).not.toBe(source);
    expect(canonical.payload).not.toBe(source.payload);
    expect(Object.getPrototypeOf(canonical)).toBeNull();
    expect(Object.getPrototypeOf(canonical.payload)).toBeNull();
    expect(Object.isFrozen(canonical)).toBe(true);
    expect(Object.isFrozen(canonical.payload)).toBe(true);
  });

  it.each(exactMessageCases())('rejects an additional own payload field for exact %s schemas', (_type, source) => {
    const forged = {
      ...source,
      payload: { ...source.payload, unexpected: 'must not cross the boundary' },
    } as unknown as AnyAutoloopMessage;

    expect(() => validateMessage(forged)).toThrow(AutoloopRoutingError);
  });

  it('serialise → deserialise returns a detached deeply immutable canonical snapshot', () => {
    const source = Msg.iterArtifacts(3, {
      diff: 'stable patch',
      eval_output: {
        summary: 'all green',
        checks: [{ id: 'unit', passed: true, metrics: [0, 0.5, 1] }],
      },
      files_changed: ['src/a.ts', 'src/b.ts'],
    });
    const { text, summary } = serialise(source);

    expect(summary).toBe('iter_artifacts');
    const back = deserialise(text);
    expect(back).toEqual(source);
    expect(back).not.toBe(source);
    expect(back.payload).not.toBe(source.payload);
    expect(Object.getPrototypeOf(back)).toBeNull();
    expect(Object.getPrototypeOf(back.payload)).toBeNull();
    expect(Object.isFrozen(back)).toBe(true);
    expect(Object.isFrozen(back.payload)).toBe(true);
    if (back.type !== 'iter_artifacts') throw new Error('expected iter_artifacts');
    const evalOutput = back.payload.eval_output as {
      checks: Array<{ metrics: number[] }>;
    };
    expect(Object.getPrototypeOf(evalOutput)).toBeNull();
    expect(Object.getPrototypeOf(evalOutput.checks)).toBe(Array.prototype);
    expect(Object.getPrototypeOf(evalOutput.checks[0])).toBeNull();
    expect(Object.getPrototypeOf(evalOutput.checks[0].metrics)).toBe(Array.prototype);
    expect(Object.isFrozen(evalOutput)).toBe(true);
    expect(Object.isFrozen(evalOutput.checks)).toBe(true);
    expect(Object.isFrozen(evalOutput.checks[0])).toBe(true);
    expect(Object.isFrozen(evalOutput.checks[0].metrics)).toBe(true);
    expect(Reflect.set(back.payload, 'diff', 'mutated')).toBe(false);
    expect(Reflect.set(evalOutput.checks[0].metrics, '0', 99)).toBe(false);
    expect(back.payload.diff).toBe('stable patch');
    expect(evalOutput.checks[0].metrics).toEqual([0, 0.5, 1]);
  });

  it.each([
    ['canonicalizeMessage', (message: AnyAutoloopMessage) => canonicalizeMessage(message)],
    ['validateMessage', (message: AnyAutoloopMessage) => validateMessage(message)],
    ['deserialise', (message: AnyAutoloopMessage) => deserialise(serialise(message).text)],
  ] as const)(
    '%s exposes frozen ordinary arrays for directive, artifact, review, and nested eval fields',
    (_boundary, snapshot) => {
      const directive = snapshot(
        Msg.directive(0, {
          goal: 'array compatibility',
          constraints: ['one writer'],
          success_criteria: ['all gates green'],
          max_attempts: 1,
        }),
      );
      const artifacts = snapshot(
        Msg.iterArtifacts(0, {
          diff: '',
          eval_output: { checks: [{ metrics: [0, 1] }] },
          files_changed: ['src/a.ts'],
        }),
      );
      const review = snapshot(
        Msg.reviewRequest(0, { iter: 0, ledger_path: '/run/ledger', prior_metrics: [0.25, 0.5] }),
      );
      if (directive.type !== 'directive' || artifacts.type !== 'iter_artifacts' || review.type !== 'review_request') {
        throw new Error('unexpected canonical message type');
      }
      const evalOutput = artifacts.payload.eval_output as { checks: Array<{ metrics: number[] }> };

      expectOrdinaryFrozenArray(directive.payload.constraints, ['one writer']);
      expectOrdinaryFrozenArray(directive.payload.success_criteria, ['all gates green']);
      expectOrdinaryFrozenArray(artifacts.payload.files_changed, ['src/a.ts']);
      expectOrdinaryFrozenArray(review.payload.prior_metrics, [0.25, 0.5]);
      expectOrdinaryFrozenArray(evalOutput.checks, [{ metrics: [0, 1] }]);
      expectOrdinaryFrozenArray(evalOutput.checks[0].metrics, [0, 1]);
      expect(Object.getPrototypeOf(evalOutput)).toBeNull();
      expect(Object.getPrototypeOf(evalOutput.checks[0])).toBeNull();
    },
  );

  it.each([
    [
      'push_user detail',
      Msg.pushUser(0, { level: 'info', summary: 'compatible', detail: undefined, channel: 'auto' }),
      'detail',
    ],
    [
      'directive_ack clarification',
      Msg.directiveAck(0, { understood: true, clarification: undefined }),
      'clarification',
    ],
    [
      'iter_done regression',
      Msg.iterDone(0, { iter: 0, verdict: 'hold', metric: null, regression: undefined }),
      'regression',
    ],
  ] as const)('accepts and omits own undefined optional %s', (_label, message, optionalField) => {
    const canonical = validateMessage(message);

    expect(Object.hasOwn(canonical.payload, optionalField)).toBe(false);
  });

  it.each([
    ['undefined', undefined],
    ['an exact string array', ['runtime-only-warning', 'second-warning']],
  ] as const)('accepts Reviewer flags as %s, validates them, and omits them', (_label, flags) => {
    const canonical = validateMessage(
      Msg.reviewVerdict(0, {
        decision: 'hold',
        metric: null,
        audit_notes: 'legacy-compatible flags',
        flags,
      } as unknown as Parameters<typeof Msg.reviewVerdict>[1]),
    );

    expect(canonical.type).toBe('review_verdict');
    expect(Object.hasOwn(canonical.payload, 'flags')).toBe(false);
  });

  it('accepts own undefined Reviewer acceptance fields and omits them', () => {
    const canonical = validateMessage(
      Msg.reviewVerdict(0, {
        decision: 'hold',
        metric: null,
        audit_notes: 'no acceptance contract',
        accepted: undefined,
        evidence_id: undefined,
      }),
    );

    expect(Object.hasOwn(canonical.payload, 'accepted')).toBe(false);
    expect(Object.hasOwn(canonical.payload, 'evidence_id')).toBe(false);
  });

  it.each([
    ['accepted false', { accepted: false }],
    ['accepted without evidence', { accepted: true }],
    ['evidence without accepted', { evidence_id: 'iter-0' }],
    ['empty acceptance evidence', { accepted: true, evidence_id: '' }],
  ] as const)('rejects review_verdict with %s', (_label, optionalFields) => {
    expect(() =>
      validateMessage(
        Msg.reviewVerdict(0, {
          decision: 'advance',
          metric: 1,
          audit_notes: 'invalid acceptance relationship',
          ...optionalFields,
        } as unknown as Parameters<typeof Msg.reviewVerdict>[1]),
      ),
    ).toThrow(AutoloopRoutingError);
  });

  it('accepts review_verdict acceptance only as a true/nonempty evidence pair', () => {
    const canonical = validateMessage(
      Msg.reviewVerdict(0, {
        decision: 'advance',
        metric: 1,
        audit_notes: 'measured',
        accepted: true,
        evidence_id: 'iter-0',
      }),
    );

    expect(canonical.payload).toMatchObject({ accepted: true, evidence_id: 'iter-0' });
  });

  it('exposes only the runtime-supported true marker in the public review verdict type', () => {
    expectTypeOf<ReviewVerdictPayload['accepted']>().toEqualTypeOf<true | undefined>();
  });

  it.each([
    ['standalone retryable:false', { retryable: false }],
    ['committed:true with retryable:false', { committed: true, retryable: false }],
  ] as const)('accepts phase_error %s', (_label, relationship) => {
    const canonical = validateMessage(
      Msg.phaseError(0, {
        agent: 'coder',
        phase: 'commit',
        error: 'failed',
        ...relationship,
      }),
    );

    expect(canonical.payload).toMatchObject(relationship);
  });

  it('rejects committed phase_error without retryable:false', () => {
    expect(() =>
      validateMessage(
        Msg.phaseError(0, {
          agent: 'coder',
          phase: 'commit',
          committed: true,
          error: 'commit outcome is not replayable',
        }),
      ),
    ).toThrow(AutoloopRoutingError);
  });

  it('requires send_timeout timeout_ms to be a positive safe integer', () => {
    const base = {
      status: 'awaiting_resume' as const,
      dispatch_id: 'dispatch-0',
      agent: 'coder' as const,
      message_id: 'message-0',
      message_type: 'directive' as const,
      iter: 0,
      error: 'deadline exceeded',
    };

    expect(() => validateMessage(Msg.sendTimeout(0, { ...base, timeout_ms: Number.MAX_SAFE_INTEGER }))).not.toThrow();
    expect(() => validateMessage(Msg.sendTimeout(0, { ...base, timeout_ms: 0.5 }))).toThrow(AutoloopRoutingError);
  });

  it.each(['constraints', 'success_criteria', 'files_changed', 'prior_metrics', 'flags'] as const)(
    'accepts exactly the primitive-array item cap and rejects cap + 1 for %s',
    (field) => {
      const value = field === 'prior_metrics' ? 1 : 'x';
      const exact = Array.from({ length: TEST_MAX_PRIMITIVE_ARRAY_ITEMS }, () => value);
      const oversized = Array.from({ length: TEST_MAX_PRIMITIVE_ARRAY_ITEMS + 1 }, () => value);

      expect(() => validateMessage(primitiveArrayMessage(field, exact))).not.toThrow();
      expect(() => validateMessage(primitiveArrayMessage(field, oversized))).toThrow(AutoloopRoutingError);
    },
  );

  it.each(['constraints', 'success_criteria', 'files_changed', 'prior_metrics', 'flags'] as const)(
    'rejects over-cap %s before consulting its expensive ownKeys trap',
    (field) => {
      let ownKeysHits = 0;
      const oversized = new Proxy(new Array(TEST_MAX_PRIMITIVE_ARRAY_ITEMS + 1), {
        ownKeys(target) {
          ownKeysHits += 1;
          return Reflect.ownKeys(target);
        },
      });

      expect(() => validateMessage(primitiveArrayMessage(field, oversized))).toThrow(AutoloopRoutingError);
      expect(ownKeysHits).toBe(0);
    },
  );

  it.each(['constraints', 'success_criteria', 'files_changed', 'prior_metrics', 'flags'] as const)(
    'rejects a non-enumerable own index in primitive-array field %s',
    (field) => {
      const values: unknown[] = [field === 'prior_metrics' ? 1 : 'x'];
      Object.defineProperty(values, '0', { configurable: true, enumerable: false, value: values[0], writable: true });

      expect(() => validateMessage(primitiveArrayMessage(field, values))).toThrow(AutoloopRoutingError);
    },
  );

  it.each(['constraints', 'success_criteria', 'files_changed', 'flags'] as const)(
    'enforces exact per-string and aggregate string budgets for %s',
    (field) => {
      const exactString = 'x'.repeat(TEST_MAX_MESSAGE_STRING_CODE_UNITS);

      expect(() => validateMessage(primitiveArrayMessage(field, [exactString]))).not.toThrow();
      expect(() => validateMessage(primitiveArrayMessage(field, [`${exactString}x`]))).toThrow(AutoloopRoutingError);
      expect(() =>
        validateMessage(primitiveArrayMessage(field, [exactString, exactString, exactString, exactString])),
      ).not.toThrow();
      expect(() =>
        validateMessage(primitiveArrayMessage(field, [exactString, exactString, exactString, exactString, 'x'])),
      ).toThrow(AutoloopRoutingError);
    },
  );

  it.each(['constraints', 'success_criteria', 'files_changed', 'flags'] as const)(
    'rejects oversized strings in %s before consulting ownKeys while retaining exact-limit inputs',
    (field) => {
      const exactString = 'x'.repeat(TEST_MAX_MESSAGE_STRING_CODE_UNITS);
      const cases = [
        { label: 'per-string', value: [`${exactString}x`] },
        { label: 'aggregate', value: [exactString, exactString, exactString, exactString, 'x'] },
      ] as const;

      for (const testCase of cases) {
        let ownKeysHits = 0;
        const guarded = new Proxy(testCase.value.slice(), {
          ownKeys(target) {
            ownKeysHits += 1;
            return Reflect.ownKeys(target);
          },
        });

        expect(() => validateMessage(primitiveArrayMessage(field, guarded)), testCase.label).toThrow(
          AutoloopRoutingError,
        );
        expect(ownKeysHits, testCase.label).toBe(0);
      }

      for (const [label, value] of [
        ['per-string exact', [exactString]],
        ['aggregate exact', [exactString, exactString, exactString, exactString]],
      ] as const) {
        let ownKeysHits = 0;
        const guarded = new Proxy(value.slice(), {
          ownKeys(target) {
            ownKeysHits += 1;
            return Reflect.ownKeys(target);
          },
        });

        expect(() => validateMessage(primitiveArrayMessage(field, guarded)), label).not.toThrow();
        expect(ownKeysHits, label).toBe(1);
      }
    },
  );

  it.each(['constraints', 'success_criteria', 'files_changed', 'prior_metrics', 'flags'] as const)(
    'rejects sparse, named, symbol, and accessor shapes for public primitive-array field %s',
    (field) => {
      const item = field === 'prior_metrics' ? 1 : 'x';
      for (const kind of ['sparse', 'named', 'symbol', 'accessor'] as const) {
        const candidate: unknown[] = [item];
        let getterHits = 0;
        if (kind === 'sparse') delete candidate[0];
        if (kind === 'named') Object.defineProperty(candidate, 'metadata', { value: 'unsupported' });
        if (kind === 'symbol') Object.defineProperty(candidate, Symbol('metadata'), { value: 'unsupported' });
        if (kind === 'accessor') {
          Object.defineProperty(candidate, '0', {
            configurable: true,
            enumerable: true,
            get() {
              getterHits += 1;
              return item;
            },
          });
        }

        expect(() => validateMessage(primitiveArrayMessage(field, candidate)), kind).toThrow(AutoloopRoutingError);
        expect(getterHits, kind).toBe(0);
      }
    },
  );

  it('keeps canonical arrays byte-stable and re-canonicalizable under Array.prototype.toJSON pollution', () => {
    const originalToJSON = Object.getOwnPropertyDescriptor(Array.prototype, 'toJSON');
    let getterHits = 0;
    let callHits = 0;
    let encoded = '';
    try {
      Object.defineProperty(Array.prototype, 'toJSON', {
        configurable: true,
        get() {
          getterHits += 1;
          return () => {
            callHits += 1;
            return ['polluted'];
          };
        },
      });
      const first = canonicalizeMessage(
        Msg.directive(0, {
          goal: 'stable arrays',
          constraints: ['one writer'],
          success_criteria: ['all gates green'],
          max_attempts: 1,
        }),
      );
      const second = canonicalizeMessage(first);
      if (first.type !== 'directive' || second.type !== 'directive') throw new Error('expected directive');

      for (const value of [
        first.payload.constraints,
        first.payload.success_criteria,
        second.payload.constraints,
        second.payload.success_criteria,
      ]) {
        expect(Object.getOwnPropertyDescriptor(value, 'toJSON')).toEqual({
          configurable: false,
          enumerable: false,
          value: undefined,
          writable: false,
        });
      }
      encoded = serialise(second).text;
    } finally {
      if (originalToJSON === undefined) Reflect.deleteProperty(Array.prototype, 'toJSON');
      else Object.defineProperty(Array.prototype, 'toJSON', originalToJSON);
    }

    expect(getterHits).toBe(0);
    expect(callHits).toBe(0);
    expect(JSON.parse(encoded)).toMatchObject({
      payload: { constraints: ['one writer'], success_criteria: ['all gates green'] },
    });
  });

  it.each([
    ['configurable shadow', { configurable: true, value: undefined }],
    ['writable shadow', { value: undefined, writable: true }],
    ['enumerable shadow', { enumerable: true, value: undefined }],
    ['callable shadow', { value: () => ['mutated'] }],
    ['accessor shadow', { configurable: true, get: () => ['mutated'] }],
  ] as const)('rejects primitive arrays with a near-miss own toJSON %s', (_label, descriptor) => {
    const candidate = ['one writer'];
    Object.defineProperty(candidate, 'toJSON', descriptor);

    expect(() => validateMessage(primitiveArrayMessage('constraints', candidate))).toThrow(AutoloopRoutingError);
  });

  it('enforces the exact iter_artifacts diff budget', () => {
    const exact = 'x'.repeat(TEST_MAX_ITER_ARTIFACT_DIFF_CODE_UNITS);

    expect(() =>
      validateMessage(Msg.iterArtifacts(0, { diff: exact, eval_output: null, files_changed: [] })),
    ).not.toThrow();
    expect(() =>
      validateMessage(Msg.iterArtifacts(0, { diff: `${exact}x`, eval_output: null, files_changed: [] })),
    ).toThrow(AutoloopRoutingError);
  });

  it('rejects an oversized diff before traversing files_changed or eval_output', () => {
    let filesOwnKeysHits = 0;
    let evalOwnKeysHits = 0;
    const filesChanged = new Proxy([], {
      ownKeys(target) {
        filesOwnKeysHits += 1;
        return Reflect.ownKeys(target);
      },
    });
    const evalOutput = new Proxy(
      {},
      {
        ownKeys(target) {
          evalOwnKeysHits += 1;
          return Reflect.ownKeys(target);
        },
      },
    );

    expect(() =>
      validateMessage(
        Msg.iterArtifacts(0, {
          diff: 'x'.repeat(TEST_MAX_ITER_ARTIFACT_DIFF_CODE_UNITS + 1),
          eval_output: evalOutput,
          files_changed: filesChanged,
        }),
      ),
    ).toThrow(AutoloopRoutingError);
    expect(filesOwnKeysHits).toBe(0);
    expect(evalOwnKeysHits).toBe(0);
  });

  it('accepts eval_output null and a null-prototype input record as frozen canonical JSON', () => {
    expect((validateMessage(evalOutputMessage(null)).payload as { eval_output: unknown }).eval_output).toBeNull();
    const input = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(input, 'passed', { enumerable: true, value: true });

    const canonical = validateMessage(evalOutputMessage(input));
    if (canonical.type !== 'iter_artifacts') throw new Error('expected iter_artifacts');
    expect(canonical.payload.eval_output).toEqual({ passed: true });
    expect(Object.getPrototypeOf(canonical.payload.eval_output)).toBeNull();
    expect(Object.isFrozen(canonical.payload.eval_output)).toBe(true);
  });

  it('defines eval_output maximum depth as 64 edges from the root value', () => {
    expect(() => validateMessage(evalOutputMessage(nestedEvalOutput(TEST_MAX_EVAL_OUTPUT_DEPTH)))).not.toThrow();
    expect(() => validateMessage(evalOutputMessage(nestedEvalOutput(TEST_MAX_EVAL_OUTPUT_DEPTH + 1)))).toThrow(
      AutoloopRoutingError,
    );
  });

  it('enforces exact eval_output per-string and aggregate key/value budgets', () => {
    const exactString = 'x'.repeat(TEST_MAX_MESSAGE_STRING_CODE_UNITS);
    const exactKeyValue = 'x'.repeat(TEST_MAX_MESSAGE_STRING_CODE_UNITS - 1);
    const exactKeyBudget = { a: exactKeyValue, b: exactKeyValue, c: exactKeyValue, d: exactKeyValue };
    const overKeyBudget = { ...exactKeyBudget, d: `${exactKeyValue}x` };

    expect(() => validateMessage(evalOutputMessage(exactString))).not.toThrow();
    expect(() => validateMessage(evalOutputMessage(`${exactString}x`))).toThrow(AutoloopRoutingError);
    expect(() => validateMessage(evalOutputMessage(exactKeyBudget))).not.toThrow();
    expect(() => validateMessage(evalOutputMessage(overKeyBudget))).toThrow(AutoloopRoutingError);
    expect(TEST_MAX_MESSAGE_TOTAL_STRING_CODE_UNITS).toBe(4 * TEST_MAX_MESSAGE_STRING_CODE_UNITS);
  });

  it('accepts the exact eval_output node cap and rejects cap + 1', () => {
    expect(() => validateMessage(evalOutputMessage(evalOutputWithNodeCount(TEST_MAX_EVAL_OUTPUT_NODES)))).not.toThrow();
    expect(() => validateMessage(evalOutputMessage(evalOutputWithNodeCount(TEST_MAX_EVAL_OUTPUT_NODES + 1)))).toThrow(
      AutoloopRoutingError,
    );
  });

  it('accepts the exact eval_output container-item cap and rejects cap + 1', () => {
    expect(() =>
      validateMessage(evalOutputMessage(Array.from({ length: TEST_MAX_EVAL_OUTPUT_CONTAINER_ITEMS }, () => null))),
    ).not.toThrow();
    expect(() =>
      validateMessage(evalOutputMessage(Array.from({ length: TEST_MAX_EVAL_OUTPUT_CONTAINER_ITEMS + 1 }, () => null))),
    ).toThrow(AutoloopRoutingError);
  });

  it('rejects an overlength eval_output array before consulting ownKeys', () => {
    let ownKeysHits = 0;
    const overlength = new Proxy(new Array(TEST_MAX_EVAL_OUTPUT_CONTAINER_ITEMS + 1), {
      ownKeys(target) {
        ownKeysHits += 1;
        return Reflect.ownKeys(target);
      },
    });

    expect(() => validateMessage(evalOutputMessage(overlength))).toThrow(AutoloopRoutingError);
    expect(ownKeysHits).toBe(0);
  });

  it.each([Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY])(
    'rejects non-finite eval_output number %s',
    (value) => {
      expect(() => validateMessage(evalOutputMessage({ nested: [value] }))).toThrow(AutoloopRoutingError);
    },
  );

  it.each([
    ['Date', new Date('2026-01-01T00:00:00.000Z')],
    ['boxed number', new Number(1)],
    ['boxed string', new String('value')],
    ['boxed boolean', new Boolean(true)],
  ] as const)('rejects %s in eval_output', (_label, value) => {
    expect(() => validateMessage(evalOutputMessage({ nested: value }))).toThrow(AutoloopRoutingError);
  });

  it('rejects a nested eval_output accessor without invoking it', () => {
    let getterHits = 0;
    const nested: Record<string, unknown> = {};
    Object.defineProperty(nested, 'value', {
      enumerable: true,
      get() {
        getterHits += 1;
        return 'attacker-controlled';
      },
    });

    expect(() => validateMessage(evalOutputMessage({ outer: [nested] }))).toThrow(AutoloopRoutingError);
    expect(getterHits).toBe(0);
  });

  it.each([
    [
      'review_verdict decision',
      () => {
        const payload = Object.assign(Object.create({ decision: 'rollback' }) as Record<string, unknown>, {
          metric: 0.5,
          audit_notes: 'inherited decision must not count',
        });
        return { ...Msg.reviewVerdict(0, { decision: 'advance', metric: 0.5, audit_notes: 'valid' }), payload };
      },
    ],
    [
      'terminate reason',
      () => {
        const payload = Object.create({ reason: 'attacker-controlled stop' }) as Record<string, unknown>;
        return { ...Msg.terminate(0, { reason: 'valid' }), payload };
      },
    ],
  ] as const)('rejects inherited required %s data', (_label, buildMessage) => {
    expect(() => validateMessage(buildMessage() as unknown as AnyAutoloopMessage)).toThrow(AutoloopRoutingError);
  });

  it('rejects an accessor send_timeout dispatch_id without invoking it', () => {
    let getterHits = 0;
    const valid = Msg.sendTimeout(2, {
      status: 'awaiting_resume',
      dispatch_id: 'dispatch-2',
      agent: 'coder',
      message_id: 'message-2',
      message_type: 'directive',
      iter: 2,
      timeout_ms: 600_000,
      error: 'deadline exceeded',
    });
    const payload = { ...valid.payload } as Record<string, unknown>;
    Object.defineProperty(payload, 'dispatch_id', {
      configurable: true,
      enumerable: true,
      get() {
        getterHits += 1;
        return 'attacker-dispatch';
      },
    });

    expect(() => validateMessage({ ...valid, payload } as unknown as AnyAutoloopMessage)).toThrow(AutoloopRoutingError);
    expect(getterHits).toBe(0);
  });

  it.each([
    [
      'object accessor',
      () => {
        const value: Record<string, unknown> = {};
        Object.defineProperty(value, 'metric', { enumerable: true, get: () => 1 });
        return value;
      },
    ],
    ['inherited object data', () => Object.assign(Object.create({ inherited: true }), { own: 'value' })],
    [
      'cycle',
      () => {
        const value: Record<string, unknown> = {};
        value.self = value;
        return value;
      },
    ],
    ['unsupported value', () => ({ callback: () => 'not JSON' })],
    [
      'sparse array',
      () => {
        const value = ['first', 'second'];
        delete value[0];
        return value;
      },
    ],
    [
      'named array field',
      () => {
        const value = ['first'] as string[] & { metadata?: string };
        value.metadata = 'unsupported';
        return value;
      },
    ],
    [
      'symbol array field',
      () => {
        const value = ['first'] as string[] & Record<symbol, string>;
        value[Symbol('metadata')] = 'unsupported';
        return value;
      },
    ],
    [
      'array accessor',
      () => {
        const value = ['first'];
        Object.defineProperty(value, '0', { configurable: true, enumerable: true, get: () => 'first' });
        return value;
      },
    ],
    [
      'unbounded nesting',
      () => {
        const root: Record<string, unknown> = {};
        let cursor = root;
        for (let depth = 0; depth < 200; depth += 1) {
          const child: Record<string, unknown> = {};
          cursor.child = child;
          cursor = child;
        }
        return root;
      },
    ],
    ['unbounded array', () => Array.from({ length: 100_001 }, () => 0)],
  ] as const)('rejects unsafe iter_artifacts eval_output: %s', (_label, buildEvalOutput) => {
    const message = Msg.iterArtifacts(0, {
      diff: '',
      eval_output: buildEvalOutput(),
      files_changed: [],
    });

    expect(() => validateMessage(message)).toThrow(AutoloopRoutingError);
  });

  it.each([
    [
      'non-string files_changed',
      Msg.iterArtifacts(0, { diff: '', eval_output: {}, files_changed: [7] as unknown as string[] }),
    ],
    ['invalid verdict enum', Msg.reviewVerdict(0, { decision: 'approve' as 'advance', metric: 1, audit_notes: '' })],
    [
      'non-finite verdict metric',
      Msg.reviewVerdict(0, { decision: 'advance', metric: Number.POSITIVE_INFINITY, audit_notes: '' }),
    ],
    ['invalid push level', Msg.pushUser(0, { level: 'debug' as 'info', summary: '', channel: 'auto' })],
    ['invalid push channel', Msg.pushUser(0, { level: 'info', summary: '', channel: 'sms' as 'auto' })],
    ['invalid pause reason', Msg.pause(0, { reason: 7 as unknown as string })],
    [
      'invalid phase_error code',
      Msg.phaseError(0, { agent: 'coder', phase: 'send', code: 'UNKNOWN' as 'AUTOLOOP_ENGINE_FAILURE', error: 'x' }),
    ],
    [
      'invalid phase_error committed flag',
      Msg.phaseError(0, { agent: 'coder', phase: 'send', committed: false as true, error: 'x' }),
    ],
    [
      'send_timeout iteration mismatch',
      Msg.sendTimeout(0, {
        status: 'awaiting_resume',
        dispatch_id: 'dispatch',
        agent: 'coder',
        message_id: 'message',
        message_type: 'directive',
        iter: 1,
        timeout_ms: 600_000,
        error: 'timeout',
      }),
    ],
    [
      'send_timeout non-finite timeout',
      Msg.sendTimeout(0, {
        status: 'awaiting_resume',
        dispatch_id: 'dispatch',
        agent: 'coder',
        message_id: 'message',
        message_type: 'directive',
        iter: 0,
        timeout_ms: Number.NaN,
        error: 'timeout',
      }),
    ],
  ] as const)('rejects %s', (_label, message) => {
    expect(() => validateMessage(message)).toThrow(AutoloopRoutingError);
  });

  it('does not retain an invalid-route attacker object for later error serialization', () => {
    let toJSONGetterHits = 0;
    let toJSONCallHits = 0;
    const attackerPrototype = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(attackerPrototype, 'toJSON', {
      enumerable: false,
      get() {
        toJSONGetterHits += 1;
        return () => {
          toJSONCallHits += 1;
          return { leaked: true };
        };
      },
    });
    const attacker = Object.create(attackerPrototype) as Record<string, unknown>;
    Object.assign(attacker, {
      msg_id: 'attacker-message',
      iter: 0,
      from: 'reviewer',
      to: 'coder',
      type: 'directive',
      ts: '2026-01-01T00:00:00.000Z',
      payload: { goal: 'x', constraints: [], success_criteria: [], max_attempts: 1 },
    });
    let observed: AutoloopRoutingError | undefined;

    try {
      validateMessage(attacker as unknown as AnyAutoloopMessage);
    } catch (error) {
      observed = error as AutoloopRoutingError;
    }

    expect(observed).toBeInstanceOf(AutoloopRoutingError);
    expect(observed?.envelope).not.toBe(attacker);
    expect(toJSONGetterHits).toBe(0);
    expect(toJSONCallHits).toBe(0);
    JSON.stringify(observed);
    expect(toJSONGetterHits).toBe(0);
    expect(toJSONCallHits).toBe(0);
    if (observed?.envelope) {
      expect(Object.getPrototypeOf(observed.envelope)).toBeNull();
      expect(Object.isFrozen(observed.envelope)).toBe(true);
      expect(Object.hasOwn(observed.envelope, 'payload')).toBe(false);
      expect(observed.envelope.type).toBe('directive');
    }
  });

  it('exports the canonicalizer and compatible validating snapshot API from the package entry point', async () => {
    const { AutoloopMsg, autoloopCanonicalize, autoloopValidate } = await import('../index.js');
    const source = AutoloopMsg.pause(0, { reason: 'public API' });

    const canonical = autoloopCanonicalize(source);
    const compatible = autoloopValidate(source);
    expect(canonical).toEqual(source);
    expect(compatible).toEqual(source);
    expect(canonical).not.toBe(source);
    expect(compatible).not.toBe(source);
    expect(Object.getPrototypeOf(canonical)).toBeNull();
    expect(Object.getPrototypeOf(compatible)).toBeNull();
    expect(Object.isFrozen(canonical)).toBe(true);
    expect(Object.isFrozen(compatible)).toBe(true);
  });

  it('validateMessage rejects bogus routes', () => {
    const bad: AnyAutoloopMessage = {
      msg_id: 'x',
      iter: 0,
      from: 'coder',
      to: 'user', // coder cannot talk to user directly
      type: 'iter_artifacts',
      ts: new Date().toISOString(),
      payload: { diff: '', eval_output: {}, files_changed: [] },
    } as AnyAutoloopMessage;
    expect(() => validateMessage(bad)).toThrow(AutoloopRoutingError);
  });
});

describe('AutoloopRunner', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('drains a full iter: chat → directive → ack → artifacts → verdict → iter_done', async () => {
    const observed: string[] = [];

    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        observed.push(`${env.from}->${env.to}:${env.type}`);
        // Planner receives chat → emits directive to coder.
        if (env.type === 'chat') {
          return [Msg.directive(env.iter, { goal: 'fix', constraints: [], success_criteria: [], max_attempts: 1 })];
        }
        // Coder receives directive → ack + artifacts.
        if (env.type === 'directive') {
          return [
            Msg.directiveAck(env.iter, { understood: true }),
            Msg.iterArtifacts(env.iter, { diff: 'patch', eval_output: { metric: 0.9 }, files_changed: ['a.py'] }),
          ];
        }
        // Reviewer receives review_request → verdict.
        if (env.type === 'review_request') {
          return [Msg.reviewVerdict(env.iter, { decision: 'advance', metric: 0.9, audit_notes: 'ok' })];
        }
        // Planner receives directive_ack and iter_done → no reply (terminal).
        return [];
      },
    };

    const { runner } = makeRunner(dispatcher);
    await runner.start();

    let iterDoneEvent: { iter: number; verdict: string; metric: number | null } | null = null;
    runner.on('iter_done', (p) => (iterDoneEvent = p));

    await runner.chat('do the thing');

    // Sequence we expect to have observed at the dispatcher boundary:
    expect(observed).toEqual([
      'user->planner:chat',
      'planner->coder:directive',
      'coder->planner:directive_ack',
      'runner->reviewer:review_request',
      'runner->planner:iter_done',
    ]);
    expect(iterDoneEvent).toEqual({ iter: 0, verdict: 'advance', metric: 0.9 });
  });

  it('terminate halts further dispatch', async () => {
    const dispatcher: AgentDispatcher = {
      async deliver() {
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher);
    await runner.start();

    let terminatedReason: string | null = null;
    runner.on('terminated', (r) => (terminatedReason = r));

    await runner.send(Msg.terminate(0, { reason: 'user-request' }));
    expect(terminatedReason).toBe('user-request');
    expect(runner.state.status).toBe('terminated');
  });

  it('drops all further messages once terminated (final-state contract)', async () => {
    const delivered: string[] = [];
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        delivered.push(env.type);
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher);
    await runner.start();

    await runner.send(Msg.terminate(0, { reason: 'done' }));
    // A chat after terminate must never reach the dispatcher.
    await runner.chat('should be ignored');
    expect(delivered).toEqual([]);
    expect(runner.state.status).toBe('terminated');
  });

  it('pause/resume flips status', async () => {
    const dispatcher: AgentDispatcher = {
      async deliver() {
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher);
    await runner.start();
    runner.markSubagentsSpawned();
    expect(runner.state.status).toBe('running');

    await runner.send(Msg.pause(0, { reason: 'user-pause' }));
    expect(runner.state.status).toBe('paused');

    await runner.send(Msg.resume(0));
    expect(runner.state.status).toBe('running');
  });

  it('push_user dedups identical events within 5 min', async () => {
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        if (env.type === 'chat') {
          return [
            Msg.pushUser(0, { level: 'info', summary: 'hello', channel: 'auto' }),
            Msg.pushUser(0, { level: 'info', summary: 'hello', channel: 'auto' }), // dup
            Msg.pushUser(0, { level: 'info', summary: 'different', channel: 'auto' }),
          ];
        }
        return [];
      },
    };
    const { runner, pushes } = makeRunner(dispatcher);
    await runner.start();
    await runner.chat('go');
    expect(pushes).toEqual([
      { level: 'info', summary: 'hello' },
      { level: 'info', summary: 'different' },
    ]);
    expect(runner.state.push_log_count).toBe(2);
  });

  it('two consecutive holds trigger reviewer-reject policy push', async () => {
    let iterCount = 0;
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        if (env.type === 'chat' || env.type === 'iter_done') {
          iterCount++;
          if (iterCount > 2) return []; // bail after 2 iters
          return [Msg.directive(iterCount, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 })];
        }
        if (env.type === 'directive') {
          return [Msg.iterArtifacts(env.iter, { diff: 'p', eval_output: {}, files_changed: [] })];
        }
        if (env.type === 'review_request') {
          return [Msg.reviewVerdict(env.iter, { decision: 'hold', metric: null, audit_notes: 'gate fail' })];
        }
        return [];
      },
    };
    const { runner, pushes } = makeRunner(dispatcher);
    await runner.start();
    await runner.chat('start');
    // After 2 holds, on_reviewer_reject_2 should fire.
    const rejectPush = pushes.find((p) => p.summary.includes('on_reviewer_reject_2'));
    expect(rejectPush).toBeDefined();
  });

  it('state.iter advances after each iter_done', async () => {
    let directiveCount = 0;
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        if (env.type === 'chat' || env.type === 'iter_done') {
          if (directiveCount >= 2) return [];
          // Mock Planner picks the next iter by advancing past the iter_done's
          // iter (matches the production dispatcher's iter-bump logic).
          const nextIter = env.type === 'iter_done' ? env.iter + 1 : 0;
          directiveCount += 1;
          return [Msg.directive(nextIter, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 })];
        }
        if (env.type === 'directive') {
          return [Msg.iterArtifacts(env.iter, { diff: '', eval_output: {}, files_changed: [] })];
        }
        if (env.type === 'review_request') {
          return [Msg.reviewVerdict(env.iter, { decision: 'advance', metric: 0.5, audit_notes: 'ok' })];
        }
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher);
    await runner.start();
    expect(runner.state.iter).toBe(0);
    await runner.chat('go');
    // After two iters of advance verdicts, state.iter should have advanced
    // from 0 → 1 → 2.
    expect(runner.state.iter).toBe(2);
    runner.stop();
  });

  it('pause parks agent-bound messages and resume replays them in order', async () => {
    const delivered: string[] = [];
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        delivered.push(env.type);
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher);
    await runner.start();
    runner.markSubagentsSpawned();

    await runner.send(Msg.pause(0, { reason: 'manual' }));
    expect(runner.state.status).toBe('paused');

    // While paused, agent-bound messages park.
    await runner.send(Msg.directive(0, { goal: 'g', constraints: [], success_criteria: [], max_attempts: 1 }));
    expect(delivered).toEqual([]);

    // Resume: parked messages drain in arrival order.
    await runner.send(Msg.resume(0));
    expect(runner.state.status).toBe('running');
    expect(delivered).toEqual(['directive']);
    runner.stop();
  });

  it('phase_error trips circuit after threshold (default 3) → auto-terminate', async () => {
    const dispatcher: AgentDispatcher = {
      async deliver() {
        return [];
      },
    };
    const { runner, pushes } = makeRunner(dispatcher, [], { phaseErrorCircuit: 3 });
    await runner.start();

    let terminatedReason: string | null = null;
    runner.on('terminated', (r) => (terminatedReason = r));

    for (let i = 0; i < 3; i++) {
      await runner.send(Msg.phaseError(0, { agent: 'coder', phase: 'send', error: `boom ${i}` }));
    }
    expect(runner.state.status).toBe('terminated');
    expect(terminatedReason).toBe('phase_error_circuit');
    expect(runner.state.consecutive_phase_errors).toBe(3);
    // A decision-level push should be emitted before terminate.
    const decisionPush = pushes.find((p) => p.level === 'decision' && p.summary.includes('phase-error circuit'));
    expect(decisionPush).toBeDefined();
    // Every mandatory on_phase_error policy push fires; ordinary dedup must not
    // collapse any of the three identical summaries.
    const errorPushes = pushes.filter((p) => p.summary.includes('on_phase_error'));
    expect(errorPushes).toHaveLength(3);
    runner.stop();
  });

  it('successful iter_done resets consecutive_phase_errors', async () => {
    const dispatcher: AgentDispatcher = {
      async deliver() {
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher, [], { phaseErrorCircuit: 5 });
    await runner.start();

    await runner.send(Msg.phaseError(0, { agent: 'coder', phase: 'send', error: 'x' }));
    expect(runner.state.consecutive_phase_errors).toBe(1);

    // Drive an advance verdict through the runner inbox.
    await runner.send(Msg.iterArtifacts(0, { diff: '', eval_output: {}, files_changed: [] }));
    await runner.send(Msg.reviewVerdict(0, { decision: 'advance', metric: 0.7, audit_notes: 'ok' }));
    expect(runner.state.consecutive_phase_errors).toBe(0);
    expect(runner.state.recent_phase_errors).toEqual([]);
    runner.stop();
  });

  it('prior_metrics accumulates from verdict metrics across iters', async () => {
    const requests: Array<number[]> = [];
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        if (env.type === 'review_request') {
          const priorMetrics = env.payload.prior_metrics ?? [];
          const snapshot: number[] = [];
          for (let index = 0; index < priorMetrics.length; index += 1) snapshot.push(priorMetrics[index]);
          requests.push(snapshot);
          return [];
        }
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher);
    await runner.start();

    // Iter 0 verdict (metric 0.1)
    await runner.send(Msg.iterArtifacts(0, { diff: '', eval_output: {}, files_changed: [] }));
    await runner.send(Msg.reviewVerdict(0, { decision: 'advance', metric: 0.1, audit_notes: '' }));
    // Iter 1 verdict (metric 0.3)
    await runner.send(Msg.iterArtifacts(1, { diff: '', eval_output: {}, files_changed: [] }));
    await runner.send(Msg.reviewVerdict(1, { decision: 'advance', metric: 0.3, audit_notes: '' }));
    // Iter 2 review_request should observe both prior metrics.
    await runner.send(Msg.iterArtifacts(2, { diff: '', eval_output: {}, files_changed: [] }));

    expect(requests).toHaveLength(3);
    expect(requests[0]).toEqual([]); // before any verdict
    expect(requests[1]).toEqual([0.1]); // after iter 0
    expect(requests[2]).toEqual([0.1, 0.3]); // after iter 1
    runner.stop();
  });

  it('stall detector fires on_stall_30min when idle past stallMs', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
    const dispatcher: AgentDispatcher = {
      async deliver() {
        return [];
      },
    };
    const { runner, pushes } = makeRunner(dispatcher, [], {
      stallMs: 1000,
      stallCheckIntervalMs: 100,
    });
    await runner.start();
    runner.markSubagentsSpawned();
    // Advance fake clock past stallMs so the interval observes a stalled run.
    await vi.advanceTimersByTimeAsync(1500);
    // Flush queued microtasks (firePolicyPush is async).
    await vi.advanceTimersByTimeAsync(0);
    const stallPush = pushes.find((p) => p.summary.includes('on_stall_30min'));
    expect(stallPush).toBeDefined();
    runner.stop();
    vi.useRealTimers();
  });

  describe('activity lease and absolute hard deadline', () => {
    it('pauses an idle run at the activity deadline and emits the expiration exactly once', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const dispatcher: AgentDispatcher = {
        async deliver() {
          return [];
        },
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const timeoutKinds: string[] = [];
      runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));

      await runner.start();
      runner.markSubagentsSpawned();
      await vi.advanceTimersByTimeAsync(60_000);

      expect(runner.state.status).toBe('paused');
      expect(runner.state.status_reason).toBe('activity_lease_expired');
      expect(timeoutKinds).toEqual(['activity_lease_expired']);

      await vi.advanceTimersByTimeAsync(120_000);
      expect(timeoutKinds).toEqual(['activity_lease_expired']);
      runner.stop();
    });

    it('renews only for accepted messages and validated forward-progress signals', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const dispatcher: AgentDispatcher = {
        async deliver() {
          return [];
        },
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const timeoutKinds: string[] = [];
      runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));

      await runner.start();
      runner.markSubagentsSpawned();
      await vi.advanceTimersByTimeAsync(59_000);
      await runner.chat('validated queue activity');
      await vi.advanceTimersByTimeAsync(59_000);
      expect(runner.recordActivity('agent_progress')).toBe(true);
      await vi.advanceTimersByTimeAsync(59_000);
      expect(runner.recordActivity('lifecycle_transition')).toBe(true);
      await vi.advanceTimersByTimeAsync(59_000);
      expect(runner.recordActivity('checkpoint_persisted')).toBe(true);

      await vi.advanceTimersByTimeAsync(59_999);
      expect(timeoutKinds).toEqual([]);
      await vi.advanceTimersByTimeAsync(1);
      expect(timeoutKinds).toEqual(['activity_lease_expired']);
      runner.stop();
    });

    it('does not renew for timer checks or runner bookkeeping', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const dispatcher: AgentDispatcher = {
        async deliver() {
          return [];
        },
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const timeoutKinds: string[] = [];
      runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));

      await runner.start();
      runner.markSubagentsSpawned();
      await vi.advanceTimersByTimeAsync(59_000);
      expect(runner.recordActivity('timer_check')).toBe(false);
      expect(runner.recordActivity('runner_bookkeeping')).toBe(false);
      await vi.advanceTimersByTimeAsync(1_000);

      expect(timeoutKinds).toEqual(['activity_lease_expired']);
      runner.stop();
    });

    it('enforces the absolute hard deadline despite repeated qualified activity', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const shutdown = vi.fn(async () => undefined);
      const dispatcher: AgentDispatcher = {
        async deliver() {
          return [];
        },
        shutdown,
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const timeoutKinds: string[] = [];
      const terminatedReasons: string[] = [];
      runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));
      runner.on('terminated', (reason: string) => terminatedReasons.push(reason));

      await runner.start();
      runner.markSubagentsSpawned();
      for (let elapsed = 50_000; elapsed < 600_000; elapsed += 50_000) {
        await vi.advanceTimersByTimeAsync(50_000);
        expect(runner.recordActivity('agent_progress')).toBe(true);
      }
      expect(timeoutKinds).toEqual([]);

      await vi.advanceTimersByTimeAsync(50_000);
      expect(runner.state.status).toBe('terminated');
      expect(runner.state.status_reason).toBe('hard_timeout_exceeded');
      expect(timeoutKinds).toEqual(['hard_timeout_exceeded']);
      expect(terminatedReasons).toEqual(['hard_timeout_exceeded']);
      expect(shutdown).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(0);
    });

    it('lets the hard deadline win when it coincides with activity-lease expiry', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const shutdown = vi.fn(async () => undefined);
      const dispatcher: AgentDispatcher = {
        async deliver() {
          return [];
        },
        shutdown,
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 600_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const timeoutKinds: string[] = [];
      runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));

      await runner.start();
      runner.markSubagentsSpawned();
      await vi.advanceTimersByTimeAsync(600_000);

      expect(runner.state.status).toBe('terminated');
      expect(timeoutKinds).toEqual(['hard_timeout_exceeded']);
      expect(shutdown).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(0);
    });

    it('keeps the hard deadline anchored while dispatcher initialization is pending', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      let finishInit: (() => void) | undefined;
      const shutdown = vi.fn(async () => undefined);
      const dispatcher: AgentDispatcher = {
        init: () =>
          new Promise<void>((resolve) => {
            finishInit = resolve;
          }),
        async deliver() {
          return [];
        },
        shutdown,
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 600_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const timeoutKinds: string[] = [];
      runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));

      const start = runner.start();
      await vi.advanceTimersByTimeAsync(600_000);
      finishInit?.();
      await start;

      expect(runner.state.status).toBe('terminated');
      expect(timeoutKinds).toEqual(['hard_timeout_exceeded']);
      expect(shutdown).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(0);
    });

    it('operator stop clears lifecycle timers before they can emit late timeouts', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const shutdown = vi.fn(async () => undefined);
      const dispatcher: AgentDispatcher = {
        async deliver() {
          return [];
        },
        shutdown,
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const timeoutKinds: string[] = [];
      runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));

      await runner.start();
      runner.markSubagentsSpawned();
      expect(vi.getTimerCount()).toBeGreaterThan(1);
      runner.stop();
      expect(vi.getTimerCount()).toBe(0);

      await vi.advanceTimersByTimeAsync(700_000);
      expect(timeoutKinds).toEqual([]);
      expect(runner.state.status).toBe('running');
      expect(shutdown).not.toHaveBeenCalled();
    });

    it('termination clears lifecycle timers and shuts down only once', async () => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00Z'));
      const shutdown = vi.fn(async () => undefined);
      const dispatcher: AgentDispatcher = {
        async deliver() {
          return [];
        },
        shutdown,
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const timeoutKinds: string[] = [];
      runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));

      await runner.start();
      runner.markSubagentsSpawned();
      expect(vi.getTimerCount()).toBeGreaterThan(1);
      await runner.send(Msg.terminate(0, { reason: 'operator-stop' }));
      expect(vi.getTimerCount()).toBe(0);

      await vi.advanceTimersByTimeAsync(700_000);
      expect(timeoutKinds).toEqual([]);
      expect(runner.state.status).toBe('terminated');
      expect(shutdown).toHaveBeenCalledTimes(1);
    });
  });

  it('rejects invalid envelopes via validateMessage', async () => {
    const dispatcher: AgentDispatcher = {
      async deliver() {
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher);
    await runner.start();
    const bogus = {
      msg_id: 'x',
      iter: 0,
      from: 'reviewer',
      to: 'coder',
      type: 'directive',
      ts: new Date().toISOString(),
      payload: { goal: 'x', constraints: [], success_criteria: [], max_attempts: 1 },
    } as AnyAutoloopMessage;
    await expect(runner.send(bogus)).rejects.toThrow(AutoloopRoutingError);
  });

  it.each([
    ['chat', () => Msg.chat(0, { text: 7 } as unknown as Parameters<typeof Msg.chat>[1])],
    [
      'directive_ack',
      () =>
        Msg.directiveAck(0, {
          understood: 'yes',
        } as unknown as Parameters<typeof Msg.directiveAck>[1]),
    ],
    [
      'iter_done',
      () =>
        Msg.iterDone(0, {
          iter: 0,
          verdict: 'advance',
          metric: Number.NaN,
        }),
    ],
    [
      'iter_done iteration mismatch',
      () =>
        Msg.iterDone(0, {
          iter: 1,
          verdict: 'advance',
          metric: 1,
        }),
    ],
    [
      'review_verdict inherited decision',
      () => {
        const payload = Object.assign(Object.create({ decision: 'rollback' }) as Record<string, unknown>, {
          metric: 0.4,
          audit_notes: 'the inherited decision must not advance runner state',
        });
        return {
          ...Msg.reviewVerdict(0, { decision: 'advance', metric: 0.4, audit_notes: 'valid source' }),
          payload,
        } as unknown as AnyAutoloopMessage;
      },
    ],
    [
      'terminate inherited reason',
      () => {
        const payload = Object.create({ reason: 'attacker-controlled stop' }) as Record<string, unknown>;
        return { ...Msg.terminate(0, { reason: 'valid stop' }), payload } as unknown as AnyAutoloopMessage;
      },
    ],
    [
      'send_timeout accessor dispatch_id',
      () => {
        const valid = Msg.sendTimeout(0, {
          status: 'awaiting_resume',
          dispatch_id: 'dispatch-0',
          agent: 'planner',
          message_id: 'message-0',
          message_type: 'chat',
          iter: 0,
          timeout_ms: 600_000,
          error: 'timeout',
        });
        const payload = { ...valid.payload } as Record<string, unknown>;
        Object.defineProperty(payload, 'dispatch_id', {
          configurable: true,
          enumerable: true,
          get: () => 'attacker-dispatch',
        });
        return { ...valid, payload } as unknown as AnyAutoloopMessage;
      },
    ],
  ] as const)(
    'rejects invalid %s before activity, message, failure, agent, or filesystem effects',
    async (_type, buildMessage) => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
      const { runner, effects, ledgerDir, cleanup } = makeCanonicalValidationHarness({
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const routedTypes: string[] = [];
      const phaseErrors: unknown[] = [];
      const timeoutEvents: unknown[] = [];
      const terminatedReasons: string[] = [];
      runner.on('message', (message: AnyAutoloopMessage) => routedTypes.push(message.type));
      runner.on('phase_error', (payload: unknown) => phaseErrors.push(payload));
      runner.on('timeout', (payload: unknown) => timeoutEvents.push(payload));
      runner.on('terminated', (reason: string) => terminatedReasons.push(reason));

      try {
        await runner.start();
        const initialActivity = runner.state.last_activity_at;
        await vi.advanceTimersByTimeAsync(59_000);
        for (let attempt = 0; attempt < 3; attempt += 1) {
          await expect(runner.send(buildMessage())).rejects.toBeInstanceOf(AutoloopRoutingError);
        }

        expect(runner.state.last_activity_at).toBe(initialActivity);
        expect(timeoutEvents).toEqual([]);
        expect(phaseErrors).toEqual([]);
        expect(routedTypes).toEqual([]);
        expect(runner.state.status).toBe('planning');
        expect(runner.state.consecutive_phase_errors).toBe(0);
        expect(runner.state.recent_phase_errors).toEqual([]);
        expect(runner.state.push_log_count).toBe(0);
        expect(terminatedReasons).toEqual([]);
        expect(effects.reserveAgentGeneration).toHaveBeenCalledTimes(0);
        expect(effects.startSession).toHaveBeenCalledTimes(0);
        expect(effects.sendMessage).toHaveBeenCalledTimes(0);
        expect(effects.stopSession).toHaveBeenCalledTimes(0);
        expect(effects.notifyUser).toHaveBeenCalledTimes(0);
        expect(fs.existsSync(path.join(ledgerDir, 'decisions.jsonl'))).toBe(false);
        expect(fs.existsSync(path.join(ledgerDir, 'chat.jsonl'))).toBe(false);
        expect(fs.existsSync(path.join(ledgerDir, 'reviewer_sandbox'))).toBe(false);

        await vi.advanceTimersByTimeAsync(1_000);
        expect(timeoutEvents).toEqual([
          expect.objectContaining({
            kind: 'activity_lease_expired',
            last_activity_at: initialActivity,
            deadline_at: initialActivity + 60_000,
          }),
        ]);
        expect(terminatedReasons).toEqual([]);
      } finally {
        cleanup();
      }
    },
  );

  it('renews and emits one immutable canonical snapshot for a valid public send', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
    const delivered: AnyAutoloopMessage[] = [];
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        delivered.push(env);
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher, [], {
      activityLeaseMs: 60_000,
      autoloopHardTimeoutMs: 600_000,
    });
    const emitted: AnyAutoloopMessage[] = [];
    const timeoutKinds: string[] = [];
    runner.on('message', (message: AnyAutoloopMessage) => emitted.push(message));
    runner.on('timeout', (event: { kind: string }) => timeoutKinds.push(event.kind));

    await runner.start();
    const source = Msg.chat(0, { text: 'stable' });
    await vi.advanceTimersByTimeAsync(59_000);
    await runner.send(source);

    expect(runner.state.last_activity_at).toBe(new Date('2026-01-01T00:00:59.000Z').getTime());
    expect(emitted).toHaveLength(1);
    expect(delivered).toHaveLength(1);
    expect(delivered[0]).toBe(emitted[0]);
    expect(emitted[0]).not.toBe(source);
    expect(Object.getPrototypeOf(emitted[0])).toBeNull();
    expect(Object.getPrototypeOf(emitted[0].payload)).toBeNull();
    expect(Object.isFrozen(emitted[0])).toBe(true);
    expect(Object.isFrozen(emitted[0].payload)).toBe(true);

    await vi.advanceTimersByTimeAsync(59_999);
    expect(timeoutKinds).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(timeoutKinds).toEqual(['activity_lease_expired']);
    runner.stop();
  });

  it.each([
    [
      'an inherited review_verdict decision',
      () => {
        const payload = Object.assign(Object.create({ decision: 'rollback' }) as Record<string, unknown>, {
          metric: 0.9,
          audit_notes: 'must be rejected',
        });
        return {
          message: {
            ...Msg.reviewVerdict(0, { decision: 'advance', metric: 0.9, audit_notes: 'valid' }),
            payload,
          } as unknown as AnyAutoloopMessage,
          getterHits: () => 0,
        };
      },
    ],
    [
      'an accessor send_timeout dispatch_id',
      () => {
        let hits = 0;
        const valid = Msg.sendTimeout(0, {
          status: 'awaiting_resume',
          dispatch_id: 'dispatch-0',
          agent: 'planner',
          message_id: 'chat-0',
          message_type: 'chat',
          iter: 0,
          timeout_ms: 600_000,
          error: 'deadline exceeded',
        });
        const payload = { ...valid.payload } as Record<string, unknown>;
        Object.defineProperty(payload, 'dispatch_id', {
          configurable: true,
          enumerable: true,
          get() {
            hits += 1;
            return 'attacker-dispatch';
          },
        });
        return {
          message: { ...valid, payload } as unknown as AnyAutoloopMessage,
          getterHits: () => hits,
        };
      },
    ],
  ] as const)(
    'rejects dispatcher reply with %s before progress renewal, message emission, or runner mutation',
    async (_label, buildInvalidReply) => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
      const invalid = buildInvalidReply();
      const dispatcher: AgentDispatcher = {
        async deliver(env) {
          if (env.type !== 'chat') return [];
          await vi.advanceTimersByTimeAsync(1_000);
          return [invalid.message];
        },
      };
      const { runner } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const emittedTypes: string[] = [];
      runner.on('message', (message: AnyAutoloopMessage) => emittedTypes.push(message.type));

      try {
        await runner.start();
        await vi.advanceTimersByTimeAsync(1_000);
        const acceptedAt = Date.now();
        let observed: unknown;
        try {
          await runner.send(Msg.chat(0, { text: 'valid input' }));
        } catch (error) {
          observed = error;
        }

        expect(observed).toBeInstanceOf(AutoloopRoutingError);
        expect(runner.state.last_activity_at).toBe(acceptedAt);
        expect(emittedTypes).toEqual(['chat']);
        expect(invalid.getterHits()).toBe(0);
        expect(runner.state.status).toBe('planning');
        expect(runner.state.iter).toBe(0);
        expect(runner.state.metric_history).toEqual([]);
        expect(runner.state.pending_dispatch).toBeNull();
        expect(runner.state.consecutive_phase_errors).toBe(0);
      } finally {
        runner.stop();
      }
    },
  );

  it.each([
    ['push_user', () => Msg.pushUser(0, { level: 'info', summary: 'must stay atomic', channel: 'auto' })],
    ['terminate', () => Msg.terminate(0, { reason: 'must-stay-atomic' })],
  ] as const)(
    'rejects a valid %s then invalid dispatcher reply batch without any partial reply effect',
    async (_validType, buildValidReply) => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
      const invalidReply = Msg.pushUser(0, {
        level: 'info',
        summary: 7 as unknown as string,
        channel: 'auto',
      });
      const dispatcher: AgentDispatcher = {
        async deliver(env) {
          if (env.type !== 'chat') return [];
          await vi.advanceTimersByTimeAsync(1_000);
          return [buildValidReply(), invalidReply];
        },
      };
      const { runner, pushes } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const emittedTypes: string[] = [];
      const stateEvents: string[] = [];
      const terminatedReasons: string[] = [];
      runner.on('message', (message: AnyAutoloopMessage) => emittedTypes.push(message.type));
      runner.on('state', () => stateEvents.push(runner.state.status));
      runner.on('terminated', (reason: string) => terminatedReasons.push(reason));

      try {
        await runner.start();
        stateEvents.length = 0;
        await vi.advanceTimersByTimeAsync(1_000);
        const acceptedAt = Date.now();

        await expect(runner.send(Msg.chat(0, { text: 'validate the whole reply batch' }))).rejects.toBeInstanceOf(
          AutoloopRoutingError,
        );

        if (_validType === 'push_user') expect(pushes).toEqual([]);
        else expect(runner.state.status).toBe('planning');
        expect(emittedTypes).toEqual(['chat']);
        expect(runner.state.last_activity_at).toBe(acceptedAt);
        expect(stateEvents).toEqual([]);
        expect(runner.state.push_log_count).toBe(0);
        expect(runner.state.status).toBe('planning');
        expect(runner.state.status_reason).toBeNull();
        expect(terminatedReasons).toEqual([]);
      } finally {
        runner.stop();
      }
    },
  );

  it.each([
    [
      'sparse',
      () => {
        const replies = [Msg.pushUser(0, { level: 'info', summary: 'must not escape', channel: 'auto' })];
        replies.length = 2;
        return { replies, getterHits: (): number => 0 };
      },
    ],
    [
      'named',
      () => {
        const replies = [
          Msg.pushUser(0, { level: 'info', summary: 'must not escape', channel: 'auto' }),
        ] as Array<AnyAutoloopMessage> & { metadata?: string };
        replies.metadata = 'unsupported';
        return { replies, getterHits: (): number => 0 };
      },
    ],
    [
      'symbol',
      () => {
        const replies = [
          Msg.pushUser(0, { level: 'info', summary: 'must not escape', channel: 'auto' }),
        ] as Array<AnyAutoloopMessage> & Record<symbol, string>;
        replies[Symbol('metadata')] = 'unsupported';
        return { replies, getterHits: (): number => 0 };
      },
    ],
    [
      'accessor',
      () => {
        const replies = [Msg.pushUser(0, { level: 'info', summary: 'must not escape', channel: 'auto' })];
        let hits = 0;
        const value = replies[0];
        Object.defineProperty(replies, '0', {
          configurable: true,
          enumerable: true,
          get() {
            hits += 1;
            return value;
          },
        });
        return { replies, getterHits: (): number => hits };
      },
    ],
  ] as const)('rejects a %s dispatcher reply container before any member effect', async (_shape, buildReplies) => {
    const candidate = buildReplies();
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        return env.type === 'chat' ? candidate.replies : [];
      },
    };
    const { runner, pushes } = makeRunner(dispatcher);
    const emittedTypes: string[] = [];
    runner.on('message', (message: AnyAutoloopMessage) => emittedTypes.push(message.type));

    try {
      await runner.start();
      await expect(runner.send(Msg.chat(0, { text: 'reject malformed batch shape' }))).rejects.toBeInstanceOf(
        AutoloopRoutingError,
      );
      expect(candidate.getterHits()).toBe(0);
      expect(pushes).toEqual([]);
      expect(emittedTypes).toEqual(['chat']);
      expect(runner.state.status).toBe('planning');
      expect(runner.state.push_log_count).toBe(0);
      expect(runner.state.consecutive_phase_errors).toBe(0);
    } finally {
      runner.stop();
    }
  });

  it.each(['named', 'symbol'] as const)(
    'rejects a dispatcher reply batch whose ownKeys substitutes index 0 with a %s key before every effect',
    async (substitutionKind) => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
      const source = [
        Msg.pushUser(0, { level: 'info', summary: 'must not be notified', channel: 'auto' }),
        Msg.sendTimeout(0, {
          status: 'awaiting_resume',
          dispatch_id: 'must-not-be-pending',
          agent: 'planner',
          message_id: 'chat-cardinality-bypass',
          message_type: 'chat',
          iter: 0,
          timeout_ms: 60_000,
          error: 'must not pause',
        }),
        Msg.phaseError(0, {
          agent: 'planner',
          phase: 'planner_turn',
          error: 'must not count',
        }),
        Msg.pause(0, { reason: 'must not pause' }),
        Msg.terminate(0, { reason: 'must not terminate' }),
      ];
      const replacementKey: PropertyKey = substitutionKind === 'named' ? 'metadata' : Symbol('reply-batch-metadata');
      Object.defineProperty(source, replacementKey, {
        configurable: true,
        enumerable: true,
        value: 'unsupported',
        writable: true,
      });
      let ownKeysHits = 0;
      let memberDescriptorReads = 0;
      const replies = new Proxy(source, {
        ownKeys(target) {
          ownKeysHits += 1;
          return Reflect.ownKeys(target).filter((key) => key !== '0');
        },
        getOwnPropertyDescriptor(target, key) {
          if (typeof key === 'string' && /^(?:0|[1-9]\d*)$/.test(key)) memberDescriptorReads += 1;
          return Reflect.getOwnPropertyDescriptor(target, key);
        },
      });
      const dispatcher: AgentDispatcher = {
        async deliver(env) {
          if (env.type !== 'chat') return [];
          await vi.advanceTimersByTimeAsync(1_000);
          return replies;
        },
      };
      const { runner, pushes } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const emittedTypes: string[] = [];
      const stateEvents: string[] = [];
      const timeoutEvents: unknown[] = [];
      const phaseErrors: unknown[] = [];
      const terminatedReasons: string[] = [];
      runner.on('message', (message: AnyAutoloopMessage) => emittedTypes.push(message.type));
      runner.on('state', () => stateEvents.push(runner.state.status));
      runner.on('send_timeout', (event) => timeoutEvents.push(event));
      runner.on('phase_error', (event) => phaseErrors.push(event));
      runner.on('terminated', (reason: string) => terminatedReasons.push(reason));

      try {
        await runner.start();
        stateEvents.length = 0;
        await vi.advanceTimersByTimeAsync(1_000);
        const acceptedAt = Date.now();
        let observed: unknown;
        try {
          await runner.send(Msg.chat(0, { text: 'reject substituted reply index atomically' }));
        } catch (error) {
          observed = error;
        }

        expect({
          routingError: observed instanceof AutoloopRoutingError,
          ownKeysHits,
          memberDescriptorReads,
          lastActivityAt: runner.state.last_activity_at,
          emittedTypes,
          pushes,
          stateEvents,
          timeoutEvents,
          phaseErrors,
          terminatedReasons,
          status: runner.state.status,
          statusReason: runner.state.status_reason,
          pendingDispatch: runner.state.pending_dispatch,
          phaseErrorCount: runner.state.consecutive_phase_errors,
          recentPhaseErrors: runner.state.recent_phase_errors,
          pushLogCount: runner.state.push_log_count,
        }).toEqual({
          routingError: true,
          ownKeysHits: 1,
          memberDescriptorReads: 0,
          lastActivityAt: acceptedAt,
          emittedTypes: ['chat'],
          pushes: [],
          stateEvents: [],
          timeoutEvents: [],
          phaseErrors: [],
          terminatedReasons: [],
          status: 'planning',
          statusReason: null,
          pendingDispatch: null,
          phaseErrorCount: 0,
          recentPhaseErrors: [],
          pushLogCount: 0,
        });
      } finally {
        runner.stop();
      }
    },
  );

  it('bounds a dispatcher reply container before enumerating or reading members', async () => {
    let ownKeysHits = 0;
    let indexDescriptorHits = 0;
    const replies = new Proxy(new Array<AnyAutoloopMessage>(TEST_MAX_REPLY_BATCH_ITEMS + 1), {
      ownKeys(target) {
        ownKeysHits += 1;
        return Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor(target, key) {
        if (key !== 'length') indexDescriptorHits += 1;
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        return env.type === 'chat' ? replies : [];
      },
    };
    const { runner } = makeRunner(dispatcher);

    try {
      await runner.start();
      let observed: unknown;
      try {
        await runner.send(Msg.chat(0, { text: 'bound the whole batch' }));
      } catch (error) {
        observed = error;
      }
      expect(observed).toBeInstanceOf(AutoloopRoutingError);
      expect((observed as Error).message).toMatch(/reply batch.*10000-item limit/i);
      expect(ownKeysHits).toBe(0);
      expect(indexDescriptorHits).toBe(0);
      expect(runner.state.consecutive_phase_errors).toBe(0);
    } finally {
      runner.stop();
    }
  });

  it('snapshots a valid dispatcher reply container through data descriptors exactly once', async () => {
    const source = [Msg.pushUser(0, { level: 'info', summary: 'one canonical effect', channel: 'auto' })];
    let ordinaryDataReads = 0;
    let ownKeysHits = 0;
    const descriptorReads = new Map<PropertyKey, number>();
    const replies = new Proxy(source, {
      get(target, key, receiver) {
        // Async return-value assimilation is allowed to inspect `then`; the
        // reply data itself must be captured exclusively via descriptors.
        if (key === 'length' || key === '0') ordinaryDataReads += 1;
        return Reflect.get(target, key, receiver);
      },
      ownKeys(target) {
        ownKeysHits += 1;
        return Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor(target, key) {
        descriptorReads.set(key, (descriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        return env.type === 'chat' ? replies : [];
      },
    };
    const { runner, pushes } = makeRunner(dispatcher);

    try {
      await runner.start();
      await runner.send(Msg.chat(0, { text: 'snapshot the batch' }));

      expect(ordinaryDataReads).toBe(0);
      expect(ownKeysHits).toBe(1);
      expect(descriptorReads.get('length')).toBe(1);
      expect(descriptorReads.get('0')).toBe(1);
      expect(pushes).toEqual([{ level: 'info', summary: 'one canonical effect' }]);
    } finally {
      runner.stop();
    }
  });

  it.each([
    ['pause', () => Msg.pause(0, { reason: 'must remain unapplied' })],
    [
      'send_timeout',
      () =>
        Msg.sendTimeout(0, {
          status: 'awaiting_resume',
          dispatch_id: 'must-remain-unapplied',
          agent: 'planner',
          message_id: 'chat-0',
          message_type: 'chat',
          iter: 0,
          timeout_ms: 600_000,
          error: 'must remain unapplied',
        }),
    ],
  ] as const)(
    'rejects a length-shifting batch containing valid %s plus an invalid sibling with zero partial effect',
    async (_validType, buildValidReply) => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'));
      const source = [
        buildValidReply(),
        Msg.pushUser(0, { level: 'info', summary: 7 as unknown as string, channel: 'auto' }),
      ];
      let ordinaryLengthReads = 0;
      let ownKeysHits = 0;
      const descriptorReads = new Map<PropertyKey, number>();
      const replies = new Proxy(source, {
        get(target, key, receiver) {
          if (key === 'length') {
            ordinaryLengthReads += 1;
            return ordinaryLengthReads === 1 ? 2 : 1;
          }
          return Reflect.get(target, key, receiver);
        },
        ownKeys(target) {
          ownKeysHits += 1;
          return Reflect.ownKeys(target);
        },
        getOwnPropertyDescriptor(target, key) {
          descriptorReads.set(key, (descriptorReads.get(key) ?? 0) + 1);
          return Reflect.getOwnPropertyDescriptor(target, key);
        },
      });
      const dispatcher: AgentDispatcher = {
        async deliver(env) {
          if (env.type !== 'chat') return [];
          await vi.advanceTimersByTimeAsync(1_000);
          return replies;
        },
      };
      const { runner, pushes } = makeRunner(dispatcher, [], {
        activityLeaseMs: 60_000,
        autoloopHardTimeoutMs: 600_000,
      });
      const stateEvents: string[] = [];
      const emittedTypes: string[] = [];
      const timeoutEvents: unknown[] = [];
      const phaseErrors: unknown[] = [];
      runner.on('state', () => stateEvents.push(runner.state.status));
      runner.on('message', (message: AnyAutoloopMessage) => emittedTypes.push(message.type));
      runner.on('send_timeout', (event) => timeoutEvents.push(event));
      runner.on('phase_error', (event) => phaseErrors.push(event));

      try {
        await runner.start();
        stateEvents.length = 0;
        const acceptedAt = Date.now();
        let observed: unknown;
        try {
          await runner.send(Msg.chat(0, { text: 'reject a shifting batch atomically' }));
        } catch (error) {
          observed = error;
        }

        expect(observed).toBeInstanceOf(AutoloopRoutingError);
        expect(ordinaryLengthReads).toBe(0);
        // Container shape is validated once before any member snapshot; the
        // malformed second member is then rejected without applying the first.
        expect(ownKeysHits).toBe(1);
        expect(descriptorReads.get('length')).toBe(1);
        expect(descriptorReads.get('0')).toBe(1);
        expect(descriptorReads.get('1')).toBe(1);
        expect(runner.state.last_activity_at).toBe(acceptedAt);
        expect(runner.state.status).toBe('planning');
        expect(runner.state.status_reason).toBeNull();
        expect(runner.state.pending_dispatch).toBeNull();
        expect(runner.state.consecutive_phase_errors).toBe(0);
        expect(runner.state.push_log_count).toBe(0);
        expect(stateEvents).toEqual([]);
        expect(emittedTypes).toEqual(['chat']);
        expect(timeoutEvents).toEqual([]);
        expect(phaseErrors).toEqual([]);
        expect(pushes).toEqual([]);
      } finally {
        runner.stop();
      }
    },
  );

  it('uses immutable one-snapshot boundaries for a valid reply and derived review messages', async () => {
    const delivered: AnyAutoloopMessage[] = [];
    const verdictTarget = {
      decision: 'advance' as const,
      metric: 0.9,
      audit_notes: 'verified',
      accepted: true as const,
      evidence_id: 'evidence-0',
    };
    const verdictDescriptorReads = new Map<PropertyKey, number>();
    const verdictPayload = new Proxy(verdictTarget, {
      getOwnPropertyDescriptor(target, key) {
        verdictDescriptorReads.set(key, (verdictDescriptorReads.get(key) ?? 0) + 1);
        return Reflect.getOwnPropertyDescriptor(target, key);
      },
    });
    let replySource: AnyAutoloopMessage | undefined;
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        delivered.push(env);
        if (env.type === 'review_request') {
          replySource = Msg.reviewVerdict(env.iter, verdictPayload);
          return [replySource];
        }
        return [];
      },
    };
    const { runner } = makeRunner(dispatcher);
    runner.state.metric_history.push(0.25);
    const emitted: AnyAutoloopMessage[] = [];
    const mutationResults: boolean[] = [];
    runner.on('message', (message: AnyAutoloopMessage) => {
      emitted.push(message);
      if (message.type === 'review_request') {
        mutationResults.push(Reflect.set(message.payload, 'ledger_path', '/listener-controlled'));
        mutationResults.push(Reflect.set(message.payload.prior_metrics, '0', 99));
      } else if (message.type === 'review_verdict') {
        mutationResults.push(Reflect.set(message.payload, 'decision', 'rollback'));
        mutationResults.push(Reflect.set(message.payload, 'metric', -1));
      } else if (message.type === 'iter_done') {
        mutationResults.push(Reflect.set(message.payload, 'verdict', 'rollback'));
        mutationResults.push(Reflect.set(message.payload, 'metric', -1));
      }
    });

    await runner.start();
    await runner.send(Msg.iterArtifacts(0, { diff: 'patch', eval_output: { passed: true }, files_changed: ['a.ts'] }));
    const verdictReadsAfterDelivery = new Map(verdictDescriptorReads);

    const emittedReviewRequest = emitted.find((message) => message.type === 'review_request');
    const emittedReviewVerdict = emitted.find((message) => message.type === 'review_verdict');
    const emittedIterDone = emitted.find((message) => message.type === 'iter_done');
    const deliveredReviewRequest = delivered.find((message) => message.type === 'review_request');
    const deliveredIterDone = delivered.find((message) => message.type === 'iter_done');
    expect(mutationResults).toEqual([false, false, false, false, false, false]);
    expect(deliveredReviewRequest).toBe(emittedReviewRequest);
    expect(deliveredIterDone).toBe(emittedIterDone);
    expect(emittedReviewVerdict).not.toBe(replySource);
    for (const message of [emittedReviewRequest, emittedReviewVerdict, emittedIterDone]) {
      expect(message).toBeDefined();
      expect(Object.getPrototypeOf(message!)).toBeNull();
      expect(Object.getPrototypeOf(message!.payload)).toBeNull();
      expect(Object.isFrozen(message)).toBe(true);
      expect(Object.isFrozen(message!.payload)).toBe(true);
    }
    for (const field of ['decision', 'metric', 'audit_notes', 'accepted', 'evidence_id']) {
      expect(verdictReadsAfterDelivery.get(field)).toBe(1);
    }
    expect(deliveredReviewRequest).toMatchObject({
      payload: { iter: 0, ledger_path: '/tmp/test/ledger', prior_metrics: [0.25] },
    });
    expect(deliveredIterDone).toMatchObject({
      payload: { iter: 0, verdict: 'advance', metric: 0.9, regression: false },
    });
    expect(runner.state.iter).toBe(1);
    expect(runner.state.metric_history).toEqual([0.25, 0.9]);
    runner.stop();
  });

  it('stores and emits one immutable canonical send_timeout reply without exposing runner state to listeners', async () => {
    let replySource: AnyAutoloopMessage | undefined;
    const dispatcher: AgentDispatcher = {
      async deliver(env) {
        if (env.type !== 'chat') return [];
        replySource = Msg.sendTimeout(env.iter, {
          status: 'awaiting_resume',
          dispatch_id: 'dispatch-stable',
          agent: 'planner',
          message_id: env.msg_id,
          message_type: env.type,
          iter: env.iter,
          timeout_ms: 600_000,
          error: 'deadline exceeded',
        });
        return [replySource];
      },
    };
    const { runner } = makeRunner(dispatcher);
    let messagePayload: object | undefined;
    let timeoutPayload: object | undefined;
    let mutationResult: boolean | undefined;
    runner.on('message', (message: AnyAutoloopMessage) => {
      if (message.type === 'send_timeout') messagePayload = message.payload;
    });
    runner.on('send_timeout', (payload: object) => {
      timeoutPayload = payload;
      mutationResult = Reflect.set(payload, 'dispatch_id', 'listener-controlled');
    });

    await runner.start();
    await runner.send(Msg.chat(0, { text: 'trigger timeout result' }));

    expect(mutationResult).toBe(false);
    expect(messagePayload).toBe(timeoutPayload);
    expect(timeoutPayload).not.toBe(replySource?.payload);
    expect(runner.state.pending_dispatch).toBe(timeoutPayload);
    expect(Object.getPrototypeOf(timeoutPayload!)).toBeNull();
    expect(Object.isFrozen(timeoutPayload)).toBe(true);
    expect(runner.state.pending_dispatch?.dispatch_id).toBe('dispatch-stable');
    expect(runner.state.status_reason).toBe('awaiting_resume:send_timeout:planner:dispatch-stable');
    runner.stop();
  });

  it('canonicalizes derived phase-error, policy, circuit, and terminate messages before listeners can mutate them', async () => {
    const dispatcher: AgentDispatcher = {
      async deliver() {
        throw new Error('planner transport exploded');
      },
    };
    const { runner, pushes } = makeRunner(dispatcher, [], { phaseErrorCircuit: 1 });
    const derived: AnyAutoloopMessage[] = [];
    const mutationResults: boolean[] = [];
    const terminatedReasons: string[] = [];
    runner.on('terminated', (reason: string) => terminatedReasons.push(reason));
    runner.on('message', (message: AnyAutoloopMessage) => {
      if (message.type === 'phase_error') {
        derived.push(message);
        mutationResults.push(Reflect.set(message.payload, 'agent', 'coder'));
        mutationResults.push(Reflect.set(message.payload, 'error', 'listener-controlled error'));
      } else if (message.type === 'push_user') {
        derived.push(message);
        mutationResults.push(Reflect.set(message.payload, 'summary', 'listener-controlled summary'));
      } else if (message.type === 'terminate') {
        derived.push(message);
        mutationResults.push(Reflect.set(message.payload, 'reason', 'listener-controlled stop'));
      }
    });

    await runner.start();
    await expect(runner.send(Msg.chat(0, { text: 'trigger failure' }))).rejects.toMatchObject({
      name: 'AutoloopOperationError',
      code: 'AUTOLOOP_ENGINE_FAILURE',
    });

    expect(mutationResults).toEqual([false, false, false, false, false]);
    expect(derived.map((message) => message.type)).toEqual(['phase_error', 'push_user', 'push_user', 'terminate']);
    for (const message of derived) {
      expect(Object.getPrototypeOf(message)).toBeNull();
      expect(Object.getPrototypeOf(message.payload)).toBeNull();
      expect(Object.isFrozen(message)).toBe(true);
      expect(Object.isFrozen(message.payload)).toBe(true);
    }
    expect(runner.state.recent_phase_errors).toEqual([
      expect.objectContaining({
        agent: 'planner',
        phase: 'planner_turn',
        code: 'AUTOLOOP_ENGINE_FAILURE',
        error: 'Planner engine transport failed: planner transport exploded',
      }),
    ]);
    expect(pushes).toEqual([
      { level: 'error', summary: '[on_phase_error] iter 0' },
      { level: 'decision', summary: 'phase-error circuit tripped (1 consecutive)' },
    ]);
    expect(terminatedReasons).toEqual(['phase_error_circuit']);
    expect(runner.state.status).toBe('terminated');
  });
});
