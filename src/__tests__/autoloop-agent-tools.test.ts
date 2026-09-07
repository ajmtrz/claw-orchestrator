/**
 * Tests for Coder/Reviewer reply parsers.
 */

import { describe, it, expect } from 'vitest';
import {
  parseAgentReply,
  extractIterComplete,
  extractReviewComplete,
  extractClarification,
} from '../autoloop/agent-tools.js';

describe('parseAgentReply', () => {
  it('extracts blocks from a coder reply', () => {
    const reply = `Fixed the off-by-one in add_two.

\`\`\`autoloop
{"tool": "iter_complete", "args": {"summary": "fixed add_two", "eval_output": {"metric": 0.95}, "files_changed": ["src/math.py"]}}
\`\`\``;
    const r = parseAgentReply(reply);
    expect(r.calls).toHaveLength(1);
    expect(r.calls[0].tool).toBe('iter_complete');
    expect(r.cleaned_reply).toContain('Fixed the off-by-one');
    expect(r.cleaned_reply).not.toContain('autoloop');
  });
});

describe('extractIterComplete', () => {
  it('returns null when no iter_complete block', () => {
    expect(extractIterComplete([])).toBeNull();
    expect(extractIterComplete([{ tool: 'coder_log', args: { message: 'hi' } }])).toBeNull();
  });

  it('returns the last iter_complete when multiple are present', () => {
    const calls = [
      { tool: 'iter_complete', args: { summary: 'first', eval_output: { metric: 0.5 } } },
      { tool: 'iter_complete', args: { summary: 'last', eval_output: { metric: 0.9 } } },
    ];
    const ic = extractIterComplete(calls);
    expect(ic?.summary).toBe('last');
  });

  it('parses files_changed when supplied as string array', () => {
    const ic = extractIterComplete([
      { tool: 'iter_complete', args: { summary: 's', eval_output: {}, files_changed: ['a.py', 42, 'b.py'] } },
    ]);
    expect(ic?.files_changed).toEqual(['a.py', 'b.py']);
  });
});

describe('extractReviewComplete', () => {
  it('parses a typical advance verdict', () => {
    const rc = extractReviewComplete([
      {
        tool: 'review_complete',
        args: { decision: 'advance', metric: 0.92, audit_notes: 'all gates green' },
      },
    ]);
    expect(rc).toEqual({ decision: 'advance', metric: 0.92, audit_notes: 'all gates green', flags: undefined });
  });

  it('returns null on invalid decision', () => {
    const rc = extractReviewComplete([
      { tool: 'review_complete', args: { decision: 'maybe', metric: 0.5, audit_notes: 'x' } },
    ]);
    expect(rc).toBeNull();
  });

  it('preserves flags array when present', () => {
    const rc = extractReviewComplete([
      {
        tool: 'review_complete',
        args: {
          decision: 'hold',
          metric: 0.6,
          audit_notes: 'gate B fail',
          flags: ['gate_B_fail', 'sus_metric_jump'],
        },
      },
    ]);
    expect(rc?.flags).toEqual(['gate_B_fail', 'sus_metric_jump']);
  });

  it('does not invoke a polluted Array.prototype.filter while snapshotting flags', () => {
    const original = Object.getOwnPropertyDescriptor(Array.prototype, 'filter');
    let filterCalls = 0;
    let rc: ReturnType<typeof extractReviewComplete>;
    try {
      Object.defineProperty(Array.prototype, 'filter', {
        configurable: true,
        value() {
          filterCalls += 1;
          throw new Error('polluted filter must not run');
        },
        writable: true,
      });
      rc = extractReviewComplete([
        {
          tool: 'review_complete',
          args: {
            decision: 'hold',
            metric: 0.6,
            audit_notes: 'bounded warning',
            flags: ['gate_B_fail'],
          },
        },
      ]);
    } finally {
      if (original === undefined) {
        Reflect.deleteProperty(Array.prototype, 'filter');
      } else {
        Object.defineProperty(Array.prototype, 'filter', original);
      }
    }

    expect(filterCalls).toBe(0);
    expect(rc?.flags).toEqual(['gate_B_fail']);
  });

  it('rejects an accessor-backed flag without invoking its getter', () => {
    let getterCalls = 0;
    const flags = ['placeholder'];
    Object.defineProperty(flags, '0', {
      configurable: true,
      enumerable: true,
      get() {
        getterCalls += 1;
        return 'attacker-selected';
      },
    });

    const rc = extractReviewComplete([
      {
        tool: 'review_complete',
        args: { decision: 'advance', metric: 1, audit_notes: 'unsafe flags', flags },
      },
    ]);

    expect(getterCalls).toBe(0);
    expect(rc).toBeNull();
  });

  it.each([
    [
      'a sparse array',
      () => {
        const flags = ['flag'];
        Reflect.deleteProperty(flags, '0');
        return flags;
      },
    ],
    [
      'an extra named property',
      () => {
        const flags = ['flag'];
        Object.defineProperty(flags, 'metadata', { value: 'unsupported' });
        return flags;
      },
    ],
    [
      'an extra symbol property',
      () => {
        const flags = ['flag'];
        Object.defineProperty(flags, Symbol('metadata'), { value: 'unsupported' });
        return flags;
      },
    ],
    [
      'a configurable toJSON shadow',
      () => {
        const flags = ['flag'];
        Object.defineProperty(flags, 'toJSON', { configurable: true, value: undefined });
        return flags;
      },
    ],
    [
      'a callable toJSON shadow',
      () => {
        const flags = ['flag'];
        Object.defineProperty(flags, 'toJSON', { value: () => ['attacker-selected'] });
        return flags;
      },
    ],
    ['a mixed-type array', () => ['flag', 7]],
    [
      'a proxy that substitutes an index with a named key',
      () => {
        const target = ['flag'];
        Object.defineProperty(target, '0', { configurable: true, enumerable: true, value: 'flag' });
        Object.defineProperty(target, 'metadata', { configurable: true, value: 'unsupported' });
        return new Proxy(target, {
          ownKeys() {
            return ['length', 'metadata'];
          },
        });
      },
    ],
  ] satisfies Array<[string, () => unknown]>)('fails closed on %s instead of filtering it', (_label, makeFlags) => {
    const rc = extractReviewComplete([
      {
        tool: 'review_complete',
        args: { decision: 'advance', metric: 1, audit_notes: 'unsafe flags', flags: makeFlags() },
      },
    ]);

    expect(rc).toBeNull();
  });

  it.each([
    ['an ordinary exact array', () => ['first', 'second']],
    [
      'an already-canonical exact array',
      () => {
        const flags = ['first', 'second'];
        Object.defineProperty(flags, 'toJSON', { value: undefined });
        return Object.freeze(flags);
      },
    ],
  ] satisfies Array<[string, () => readonly string[]]>)(
    'returns an inert ordered snapshot for %s',
    (_label, makeFlags) => {
      const source = makeFlags();
      const rc = extractReviewComplete([
        {
          tool: 'review_complete',
          args: { decision: 'hold', metric: 0.5, audit_notes: 'safe flags', flags: source },
        },
      ]);

      expect(rc?.flags).toEqual(['first', 'second']);
      expect(rc?.flags).not.toBe(source);
      expect(Object.isFrozen(rc?.flags)).toBe(true);
      expect(Object.getOwnPropertyDescriptor(rc?.flags, 'toJSON')).toEqual({
        configurable: false,
        enumerable: false,
        value: undefined,
        writable: false,
      });
    },
  );

  it('rejects an explicitly supplied undefined flags field while preserving absence compatibility', () => {
    const malformed = extractReviewComplete([
      {
        tool: 'review_complete',
        args: { decision: 'hold', metric: null, audit_notes: 'explicit malformed flags', flags: undefined },
      },
    ]);
    const absent = extractReviewComplete([
      {
        tool: 'review_complete',
        args: { decision: 'hold', metric: null, audit_notes: 'flags absent' },
      },
    ]);

    expect(malformed).toBeNull();
    expect(absent).toEqual({ decision: 'hold', metric: null, audit_notes: 'flags absent', flags: undefined });
  });

  it('coerces metric to null when non-numeric', () => {
    const rc = extractReviewComplete([
      { tool: 'review_complete', args: { decision: 'hold', metric: 'broken', audit_notes: 'x' } },
    ]);
    expect(rc?.metric).toBeNull();
  });
});

describe('extractClarification', () => {
  it('returns the question text', () => {
    const q = extractClarification([
      { tool: 'request_clarification', args: { question: 'should I touch the eval script?' } },
    ]);
    expect(q).toBe('should I touch the eval script?');
  });
  it('returns null when not present', () => {
    expect(extractClarification([])).toBeNull();
  });
});
