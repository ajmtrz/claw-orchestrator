import { describe, expect, it } from 'vitest';
import {
  assessRecovery,
  computeRecoveryToken,
  parseRecoveryReceipt,
  recoveryActionDigest,
} from '../autoloop/recovery.js';
import type {
  AutoloopPhase,
  PhysicalAgentGeneration,
  RecoveryAgentEvidence,
  RecoveryInput,
} from '../autoloop/types.js';
import {
  assessRecovery as assessRecoveryFromPublicApi,
  computeRecoveryToken as computeRecoveryTokenFromPublicApi,
} from '../index.js';

const NOW = '2026-09-05T12:00:00.000Z';

function generation(
  role: PhysicalAgentGeneration['role'],
  overrides: Partial<PhysicalAgentGeneration> = {},
): PhysicalAgentGeneration {
  return {
    role,
    generation: 1,
    session_name: `autoloop-run-1-${role}`,
    session_id: `${role}-session-1`,
    owner_instance_id: 'owner-1',
    created_at: '2026-09-05T10:00:00.000Z',
    last_activity_at: '2026-09-05T11:55:00.000Z',
    lease_expires_at: '2026-09-05T12:05:00.000Z',
    state: 'live',
    ...overrides,
  };
}

function input(overrides: Partial<RecoveryInput> = {}): RecoveryInput {
  return {
    run_id: 'run-1',
    observed_at: NOW,
    legacy_state: {
      status: 'planning',
      iter: 0,
      subagents_spawned: false,
    },
    iterations: [],
    deliveries: [],
    agents: [],
    ...overrides,
  };
}

describe('assessRecovery', () => {
  it('keeps a legacy planning run with no directive at the Planner boundary', () => {
    const assessment = assessRecovery(input());

    expect(assessment).toMatchObject({
      run_id: 'run-1',
      phase: 'PLANNING',
      pending_delivery_ids: [],
      next_safe_action: 'resume_planner',
    });
    expect(assessment.evidence).toContain('legacy:status:planning');
  });

  it('waits to dispatch a persisted directive until its delivery is acknowledged', () => {
    const assessment = assessRecovery(
      input({
        iterations: [{ iter: 0, artifacts: ['directive'] }],
        deliveries: [
          {
            delivery_id: 'delivery-coder-0',
            iter: 0,
            kind: 'coder_directive',
            acknowledged: false,
          },
        ],
      }),
    );

    expect(assessment.phase).toBe('AWAITING_CODER');
    expect(assessment.pending_delivery_ids).toEqual(['delivery-coder-0']);
    expect(assessment.next_safe_action).toBe('dispatch_coder');
  });

  it('recognizes an acknowledged directive owned by an active Coder generation', () => {
    const coder: RecoveryAgentEvidence = {
      generation: generation('coder'),
      matching_runtime: 'live',
    };
    const assessment = assessRecovery(
      input({
        iterations: [{ iter: 0, artifacts: ['directive'] }],
        deliveries: [
          {
            delivery_id: 'delivery-coder-0',
            iter: 0,
            kind: 'coder_directive',
            acknowledged: true,
          },
        ],
        agents: [coder],
      }),
    );

    expect(assessment.phase).toBe('CODER_RUNNING');
    expect(assessment.agents).toEqual([coder.generation]);
    expect(assessment.next_safe_action).toBe('none');
  });

  it('makes a disk-only acknowledged Coder directive recoverable instead of claiming a live owner', () => {
    const assessment = assessRecovery(
      input({
        iterations: [{ iter: 0, artifacts: ['directive'] }],
        deliveries: [
          {
            delivery_id: 'delivery-coder-0',
            iter: 0,
            kind: 'coder_directive',
            acknowledged: true,
          },
        ],
        agents: [],
      }),
    );

    expect(assessment.phase).toBe('PAUSED_RECOVERABLE');
    expect(assessment.next_safe_action).toBe('dispatch_coder');
    expect(assessment.evidence).toContain('recovery:coder:no_live_generation');
  });

  it('keeps a caller watchdog observation distinct from an absent Coder generation', () => {
    const assessment = assessRecovery(
      input({
        legacy_state: {
          status: 'paused',
          iter: 0,
          subagents_spawned: true,
          status_reason: 'caller_watchdog_timeout',
        },
        iterations: [{ iter: 0, artifacts: ['directive'] }],
        deliveries: [
          {
            delivery_id: 'delivery-coder-0',
            iter: 0,
            kind: 'coder_directive',
            acknowledged: true,
          },
        ],
        agents: [{ generation: generation('coder'), matching_runtime: 'live' }],
      }),
    );

    expect(assessment.phase).toBe('CODER_RUNNING');
    expect(assessment.next_safe_action).toBe('none');
    expect(assessment.evidence).toContain('legacy:status_reason:caller_watchdog_timeout');
  });

  it('reconstructs awaiting review from the complete Coder artifact set', () => {
    const assessment = assessRecovery(
      input({
        iterations: [
          {
            iter: 0,
            artifacts: ['directive', 'coder_summary', 'eval_output', 'diff'],
          },
        ],
      }),
    );

    expect(assessment.phase).toBe('AWAITING_REVIEW');
    expect(assessment.next_safe_action).toBe('request_review');
  });

  it('recognizes an acknowledged review request with no verdict as Reviewer running', () => {
    const reviewer: RecoveryAgentEvidence = {
      generation: generation('reviewer'),
      matching_runtime: 'live',
    };
    const assessment = assessRecovery(
      input({
        iterations: [
          {
            iter: 0,
            artifacts: ['directive', 'coder_summary', 'eval_output', 'diff'],
          },
        ],
        deliveries: [
          {
            delivery_id: 'delivery-reviewer-0',
            iter: 0,
            kind: 'review_request',
            acknowledged: true,
          },
        ],
        agents: [reviewer],
      }),
    );

    expect(assessment.phase).toBe('REVIEWER_RUNNING');
    expect(assessment.agents).toEqual([reviewer.generation]);
    expect(assessment.next_safe_action).toBe('none');
  });

  it('moves an advance verdict to the next Planner boundary', () => {
    const assessment = assessRecovery(
      input({
        legacy_state: {
          status: 'running',
          iter: 4,
          subagents_spawned: true,
        },
        iterations: [
          {
            iter: 3,
            artifacts: ['directive', 'coder_summary', 'eval_output', 'diff'],
            verdict: 'advance',
          },
        ],
      }),
    );

    expect(assessment.phase).toBe('PLANNING');
    expect(assessment.evidence).toContain('iteration:3:verdict:advance');
    expect(assessment.next_safe_action).toBe('resume_planner');
  });

  it('replays a same-iteration durable verdict that the Runner has not consumed', () => {
    const assessment = assessRecovery(
      input({
        legacy_state: {
          status: 'running',
          iter: 3,
          subagents_spawned: true,
        },
        iterations: [
          {
            iter: 3,
            artifacts: ['directive', 'coder_summary', 'eval_output', 'diff'],
            verdict: 'hold',
          },
        ],
        deliveries: [
          {
            delivery_id: 'delivery-reviewer-3',
            iter: 3,
            kind: 'review_request',
            acknowledged: false,
          },
        ],
      }),
    );

    expect(assessment.phase).toBe('AWAITING_REVIEW');
    expect(assessment.next_safe_action).toBe('request_review');
    expect(assessment.pending_delivery_ids).toEqual(['delivery-reviewer-3']);
    expect(assessment.evidence).toContain('recovery:review:verdict_unconsumed');
  });

  it('blocks partial legacy Coder artifacts instead of guessing a phase', () => {
    const assessment = assessRecovery(
      input({
        legacy_state: {
          status: 'running',
          iter: 0,
          subagents_spawned: true,
        },
        iterations: [{ iter: 0, artifacts: ['directive', 'coder_summary'] }],
      }),
    );

    expect(assessment.phase).toBe('BLOCKED');
    expect(assessment.evidence).toContain('ambiguity:iteration:0:partial_coder_artifacts');
    expect(assessment.next_safe_action).toBe('manual_resolution');
  });

  it('marks an expired Coder generation orphaned when its matching runtime is absent', () => {
    const expiredCoder = generation('coder', {
      lease_expires_at: '2026-09-05T11:59:59.000Z',
      state: 'stale',
    });
    const assessment = assessRecovery(
      input({
        iterations: [{ iter: 0, artifacts: ['directive'] }],
        deliveries: [
          {
            delivery_id: 'delivery-coder-0',
            iter: 0,
            kind: 'coder_directive',
            acknowledged: true,
          },
        ],
        agents: [{ generation: expiredCoder, matching_runtime: 'absent' }],
      }),
    );

    expect(assessment.phase).toBe('PAUSED_RECOVERABLE');
    expect(assessment.agents).toEqual([{ ...expiredCoder, state: 'orphaned' }]);
    expect(assessment.evidence).toContain('agent:coder:1:recoverable_orphan');
    expect(assessment.next_safe_action).toBe('dispatch_coder');
  });

  it('preserves an expired generation when the matching owner is still live', () => {
    const expiredCoder = generation('coder', {
      lease_expires_at: '2026-09-05T11:59:59.000Z',
      state: 'stale',
    });
    const assessment = assessRecovery(
      input({
        iterations: [{ iter: 0, artifacts: ['directive'] }],
        deliveries: [
          {
            delivery_id: 'delivery-coder-0',
            iter: 0,
            kind: 'coder_directive',
            acknowledged: true,
          },
        ],
        agents: [{ generation: expiredCoder, matching_runtime: 'live' }],
      }),
    );

    expect(assessment.phase).toBe('CODER_RUNNING');
    expect(assessment.agents).toEqual([{ ...expiredCoder, state: 'live' }]);
    expect(assessment.evidence).toContain('agent:coder:1:ownership_preserved');
    expect(assessment.next_safe_action).toBe('none');
  });

  it('recognizes explicit durable completion evidence', () => {
    const assessment = assessRecovery(input({ completed: true }));

    expect(assessment.phase).toBe('COMPLETED');
    expect(assessment.next_safe_action).toBe('none');
  });

  it('normalizes evidence order into a stable SHA-256 recovery token', () => {
    const coder = {
      generation: generation('coder'),
      matching_runtime: 'live' as const,
    };
    const reviewer = {
      generation: generation('reviewer', { generation: 2 }),
      matching_runtime: 'absent' as const,
    };
    const first = input({
      iterations: [
        {
          iter: 0,
          artifacts: ['eval_output', 'directive', 'diff', 'coder_summary'],
        },
      ],
      deliveries: [
        {
          delivery_id: 'z-review',
          iter: 0,
          kind: 'review_request',
          acknowledged: false,
        },
        {
          delivery_id: 'a-coder',
          iter: 0,
          kind: 'coder_directive',
          acknowledged: true,
        },
      ],
      agents: [reviewer, coder],
    });
    const reordered = input({
      ...first,
      iterations: [
        {
          iter: 0,
          artifacts: ['coder_summary', 'diff', 'directive', 'eval_output'],
        },
      ],
      deliveries: [...first.deliveries].reverse(),
      agents: [...first.agents].reverse(),
    });

    const assessment = assessRecovery(first);
    expect(assessment.evidence).toEqual([...assessment.evidence].sort());
    expect(assessment.recovery_token).toMatch(/^[a-f0-9]{64}$/);
    expect(computeRecoveryToken(first)).toBe(assessment.recovery_token);
    expect(computeRecoveryToken(reordered)).toBe(assessment.recovery_token);
    expect(
      computeRecoveryToken({
        ...first,
        deliveries: first.deliveries.map((delivery) =>
          delivery.delivery_id === 'z-review' ? { ...delivery, acknowledged: true } : delivery,
        ),
      }),
    ).not.toBe(assessment.recovery_token);
  });

  it('keeps recovery tokens stable when generation property insertion order differs', () => {
    const ordered = generation('coder');
    const reordered: PhysicalAgentGeneration = {
      state: ordered.state,
      lease_expires_at: ordered.lease_expires_at,
      last_activity_at: ordered.last_activity_at,
      created_at: ordered.created_at,
      owner_instance_id: ordered.owner_instance_id,
      session_id: ordered.session_id,
      session_name: ordered.session_name,
      generation: ordered.generation,
      role: ordered.role,
    };

    const tokenFor = (agent: PhysicalAgentGeneration): string =>
      computeRecoveryToken(
        input({
          agents: [{ generation: agent, matching_runtime: 'live' }],
        }),
      );

    expect(reordered).toEqual(ordered);
    expect(tokenFor(reordered)).toBe(tokenFor(ordered));
  });

  it('does not mutate caller-provided durable evidence', () => {
    const original = input({
      iterations: [{ iter: 0, artifacts: ['directive'] }],
      deliveries: [
        {
          delivery_id: 'delivery-coder-0',
          iter: 0,
          kind: 'coder_directive',
          acknowledged: false,
        },
      ],
      agents: [
        {
          generation: generation('coder', {
            lease_expires_at: '2026-09-05T11:59:59.000Z',
            state: 'stale',
          }),
          matching_runtime: 'absent',
        },
      ],
    });
    const before = structuredClone(original);

    assessRecovery(original);

    expect(original).toEqual(before);
  });

  it('exports the recovery API and phase types from the package entry point', () => {
    const phase: AutoloopPhase = 'AWAITING_REVIEW';

    expect(phase).toBe('AWAITING_REVIEW');
    expect(assessRecoveryFromPublicApi).toBe(assessRecovery);
    expect(computeRecoveryTokenFromPublicApi).toBe(computeRecoveryToken);
  });
});

describe('parseRecoveryReceipt', () => {
  it('rejects inherited, accessor, and unknown receipt fields', () => {
    const actionSnapshot = { type: 'resume_planner', run_id: 'run-1', iter: 0, phase: 'PLANNING' } as const;
    const receipt = {
      schema_version: 1,
      record_type: 'autoloop_recovery_receipt',
      kind: 'autoloop_recovery_receipt',
      run_id: 'run-1',
      recovery_token: 'a'.repeat(64),
      action_sha256: recoveryActionDigest(actionSnapshot),
      action_snapshot: actionSnapshot,
      claim_id: '12345678-1234-1234-1234-123456789abc',
      phase: 'PLANNING',
      next_safe_action: 'resume_planner',
      status: 'prepared',
      recorded_at: '2026-09-12T00:00:00.000Z',
    } as const;
    const inherited = Object.create(receipt);
    const accessor = Object.defineProperty({ ...receipt }, 'run_id', {
      enumerable: true,
      get: () => 'run-1',
    });

    expect(parseRecoveryReceipt(inherited)).toBeUndefined();
    expect(parseRecoveryReceipt(accessor)).toBeUndefined();
    expect(parseRecoveryReceipt({ ...receipt, extra: true })).toBeUndefined();
  });

  it.each([
    ['run_id', 'another-run'],
    ['phase', 'AWAITING_CODER'],
  ] as const)('rejects a %s mismatch between a resume_planner snapshot and its receipt', (field, value) => {
    const actionSnapshot = { type: 'resume_planner', run_id: 'run-1', iter: 0, phase: 'PLANNING' } as const;
    const receipt = {
      schema_version: 1,
      record_type: 'autoloop_recovery_receipt',
      kind: 'autoloop_recovery_receipt',
      run_id: 'run-1',
      recovery_token: 'a'.repeat(64),
      action_sha256: recoveryActionDigest(actionSnapshot),
      action_snapshot: actionSnapshot,
      claim_id: '12345678-1234-1234-1234-123456789abc',
      phase: 'PLANNING',
      next_safe_action: 'resume_planner',
      status: 'prepared',
      recorded_at: '2026-09-12T00:00:00.000Z',
    } as const;

    expect(parseRecoveryReceipt({ ...receipt, [field]: value })).toBeUndefined();
  });
});
