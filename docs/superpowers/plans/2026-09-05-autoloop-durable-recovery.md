# Autoloop Durable Recovery Implementation Plan

> **Execution requirement:** Run this plan through Claw Orchestrator Autoloop with a read-only Planner, a single Coder, and an independent read-only Reviewer. Implement every task test-first and apply `superpowers:test-driven-development`; before any completion claim apply `superpowers:verification-before-completion`.

**Goal:** Make Autoloop recover idempotently from interrupted physical sessions and partially completed iterations, reject false-success adapter results, and support review of an existing checkpoint without starting a Coder.

**Architecture:** Keep the append-only ledger and iteration artifacts authoritative. Add a pure recovery reconciler, generation-fenced physical-agent records, and a durable delivery outbox. Expose recovery and independent agent dispatch through the existing SessionManager/MCP/embedded-server layers while retaining legacy APIs as compatible wrappers.

**Tech stack:** TypeScript, Node.js 22+, Vitest, MCP/OpenClaw tool registration, embedded HTTP server, existing Autoloop runner/dispatcher/session manager.

**Design source:** `docs/superpowers/specs/2026-09-05-autoloop-durable-recovery-design.md`

---

## Global invariants

- Historical ledgers and iteration artifacts are append-only and are never rewritten during recovery.
- A logical Planner/Coder/Reviewer identity may have multiple physical generations, but only one generation may own a valid lease.
- A public success response proves the durable effect it claims; empty replies, missing sessions, missing acknowledgements, and digest mismatches are typed failures.
- Persist complete delivery intent and SHA-256 before sending; change phase only after a matching acknowledgement.
- All recovery, reset, spawn, directive, and review operations are idempotent under their idempotency/recovery token.
- Existing `spawn_subagents` and legacy state remain accepted.

## Task 1: Formalize effective phases and pure recovery assessment

**Files:**
- Create: `src/autoloop/recovery.ts`
- Modify: `src/autoloop/types.ts`
- Modify: `src/index.ts`
- Test: `src/__tests__/autoloop-recovery.test.ts`

**Step 1: Write failing reconstruction tests**

Cover at minimum:
- legacy planning state with no directive -> `PLANNING`;
- persisted directive without delivery acknowledgement -> `AWAITING_CODER`;
- active acknowledged directive -> `CODER_RUNNING`;
- `coder_summary.txt` + `eval_output.json` + `diff.patch`, but no verdict -> `AWAITING_REVIEW`;
- delivered review request without verdict -> `REVIEWER_RUNNING`;
- `advance` verdict -> next Planner boundary;
- ambiguous legacy evidence -> `BLOCKED` with evidence, never a guessed phase;
- expired lease with no live matching generation -> recoverable orphan;
- expired lease with live matching owner -> preserve ownership.

Run: `npx vitest run src/__tests__/autoloop-recovery.test.ts`
Expected: FAIL because the recovery API/types do not exist.

**Step 2: Add exact public recovery types**

In `src/autoloop/types.ts`, add:

```ts
export type AutoloopPhase =
  | 'PLANNING'
  | 'AWAITING_CODER'
  | 'CODER_RUNNING'
  | 'AWAITING_REVIEW'
  | 'REVIEWER_RUNNING'
  | 'PAUSED_RECOVERABLE'
  | 'BLOCKED'
  | 'COMPLETED';

export type AutoloopAgentRole = 'planner' | 'coder' | 'reviewer';

export interface PhysicalAgentGeneration {
  role: AutoloopAgentRole;
  generation: number;
  session_name: string;
  session_id?: string;
  owner_instance_id: string;
  created_at: string;
  last_activity_at: string;
  lease_expires_at: string;
  state: 'live' | 'stale' | 'orphaned' | 'released';
}

export interface RecoveryAssessment {
  run_id: string;
  phase: AutoloopPhase;
  evidence: string[];
  agents: PhysicalAgentGeneration[];
  pending_delivery_ids: string[];
  next_safe_action: 'none' | 'resume_planner' | 'dispatch_coder' | 'request_review' | 'manual_resolution';
  recovery_token: string;
}
```

Extend persisted state only with optional versioned fields so old JSON remains readable.

**Step 3: Implement a side-effect-free reconciler**

In `recovery.ts`, implement:

```ts
export function assessRecovery(input: RecoveryInput): RecoveryAssessment;
export function computeRecoveryToken(input: RecoveryInput): string;
```

Normalize/sort evidence before hashing so identical durable evidence yields an identical token. Do not inspect or mutate the filesystem inside this module; callers provide artifact/session facts.

**Step 4: Export types/functions and verify**

Export the public types from `src/index.ts` and run:

`npx vitest run src/__tests__/autoloop-recovery.test.ts && npm run typecheck:tests`

Expected: PASS.

**Step 5: Commit**

`git add src/autoloop/recovery.ts src/autoloop/types.ts src/index.ts src/__tests__/autoloop-recovery.test.ts && git commit -m "feat: reconstruct durable autoloop phase"`

## Task 2: Add generation-fenced leases and atomic orphan release

**Files:**
- Modify: `src/autoloop/dispatcher.ts`
- Modify: `src/session-manager.ts`
- Modify: `src/autoloop/types.ts`
- Test: `src/__tests__/autoloop-dispatcher.test.ts`
- Test: `src/__tests__/session-manager.test.ts`

**Step 1: Write failing lease/orphan tests**

Test live-owner collision, expired dead-owner cleanup, stale registry-only name, generation fencing, two concurrent recoverers, and append-only `agent_generation_orphaned`/`agent_generation_released` evidence. Assert that concurrent cleanup has one winner and one idempotent observation.

Run: `npx vitest run src/__tests__/autoloop-dispatcher.test.ts src/__tests__/session-manager.test.ts`
Expected: new tests FAIL.

**Step 2: Introduce a narrow runtime liveness boundary**

Add injectable callbacks to the dispatcher/session manager for:

```ts
interface AgentRuntimeProbe {
  inspect(sessionName: string, sessionId?: string): Promise<'live' | 'absent' | 'unknown'>;
  releaseReservation(sessionName: string, expectedGeneration: number): Promise<boolean>;
}
```

Production uses the existing kernel/session registry; tests use deterministic fakes.

**Step 3: Implement compare-and-release semantics**

Create/update the generation record before session creation. On recovery, release only when the expected generation and owner still match and the probe proves absence. Treat `unknown` as a hard stop. Persist orphan/release events before returning the name as reusable.

**Step 4: Replace raw name-collision checks**

Change `_bootAutoloop` and agent spawning to call the fenced reservation API. A genuinely live owner retains HTTP/MCP conflict behavior; an orphan is reclaimed once and audibly.

**Step 5: Verify and commit**

Run the focused tests plus `npm run typecheck:tests`, then commit:

`git commit -m "fix: fence autoloop sessions by generation"`

## Task 3: Enforce strict Planner success and reset postconditions

**Files:**
- Modify: `src/session-manager.ts`
- Modify: `src/autoloop/dispatcher.ts`
- Modify: `src/embedded-server.ts`
- Modify: `src/index.ts`
- Test: `src/__tests__/session-manager.test.ts`
- Test: `src/__tests__/agy-planner-e2e.test.ts`
- Test: `src/__tests__/embedded-server-launcher.test.ts`

**Step 1: Write failing false-success tests**

Cover transport success with empty logical reply, no physical session after send, denied required tool, missing persisted control action, and reset reporting success while a reservation remains occupied. Assert typed codes such as `AUTOLOOP_EMPTY_REPLY`, `AUTOLOOP_SESSION_NOT_CREATED`, `AUTOLOOP_CONTROL_NOT_PERSISTED`, and `AUTOLOOP_RESET_POSTCONDITION_FAILED`.

**Step 2: Centralize the success contract**

Add an internal validator:

```ts
function assertPlannerTurnSucceeded(result: PlannerTurnResult, expected: PlannerTurnExpectation): void;
```

Validate non-empty reply when required, live/reused physical generation, and matching persisted control event. Ensure failed turns do not advance phase.

**Step 3: Make reset prove reusable state**

`autoloopResetAgent` must stop/prove absence, release the exact generation, probe the reservation, and return a structured result. Preserve the legacy boolean response only at the compatibility boundary; internally never map a failed postcondition to `true`.

**Step 4: Map typed failures consistently**

Map typed errors to stable MCP errors and embedded HTTP status/body without hiding the code. Ensure async embedded chat records the terminal failure rather than silently accepting it.

**Step 5: Verify and commit**

Run focused tests, `npm run lint`, and `npm run typecheck:tests`; commit:

`git commit -m "fix: reject false-success autoloop turns"`

## Task 4: Split Coder/Reviewer lifecycle and add Reviewer-only requests

**Files:**
- Modify: `src/autoloop/planner-tools.ts`
- Modify: `src/autoloop/dispatcher.ts`
- Modify: `src/autoloop/messages.ts`
- Modify: `src/session-manager.ts`
- Modify: `src/index.ts`
- Modify: `src/embedded-server.ts`
- Test: `src/__tests__/autoloop-planner-tools.test.ts`
- Test: `src/__tests__/autoloop-dispatcher.test.ts`
- Test: `src/__tests__/tool-registration.test.ts`
- Test: `src/__tests__/embedded-server-launcher.test.ts`

**Step 1: Write failing independent-dispatch tests**

Prove `spawn_reviewer` creates no Coder, `spawn_coder` creates no Reviewer, `spawn_subagents` remains compatible, and `request_review` accepts an existing checkpoint/source iteration without a Coder session.

**Step 2: Add planner controls and dispatcher primitives**

Add:

```ts
spawnCoder(args?: SpawnCoderArgs): Promise<PhysicalAgentGeneration>;
spawnReviewer(args?: SpawnReviewerArgs): Promise<PhysicalAgentGeneration>;
requestReview(args: {
  checkpoint_sha: string;
  source_run_id: string;
  source_iter: number;
  scope: string[];
  idempotency_key: string;
}): Promise<DeliveryReceipt>;
```

Implement `spawnSubagents` as a compatibility wrapper over the two primitives with rollback/failure semantics matching existing behavior.

**Step 3: Add strict schema validation and public registration**

Validate full hexadecimal checkpoint SHA, non-negative iteration, non-empty scope, and idempotency key. Register the new public recovery/review tools only where the architecture exposes them; keep Planner-only controls inside Planner tool parsing.

**Step 4: Verify and commit**

Run all four focused test files and typecheck; commit:

`git commit -m "feat: support reviewer-only autoloop dispatch"`

## Task 5: Implement the durable delivery outbox

**Files:**
- Create: `src/autoloop/outbox.ts`
- Modify: `src/autoloop/types.ts`
- Modify: `src/autoloop/runner.ts`
- Modify: `src/autoloop/dispatcher.ts`
- Modify: `src/autoloop/messages.ts`
- Test: `src/__tests__/autoloop-outbox.test.ts`
- Test: `src/__tests__/autoloop-runner.test.ts`
- Test: `src/__tests__/autoloop-dispatcher.test.ts`

**Step 1: Write failing outbox/idempotency tests**

Cover persist-before-send, crash after persistence, crash after send before acknowledgement, exact acknowledgement digest, mismatched digest STOP, acknowledged retry as no-op, unacknowledged retry to the same logical role/new valid generation, and two concurrent callers producing one durable item.

**Step 2: Define versioned records**

```ts
interface DeliveryIntent {
  schema_version: 1;
  delivery_id: string;
  idempotency_key: string;
  kind: 'coder_directive' | 'review_request';
  target_role: 'coder' | 'reviewer';
  target_generation: number;
  payload: unknown;
  payload_sha256: string;
  created_at: string;
}

interface DeliveryAcknowledgement {
  schema_version: 1;
  delivery_id: string;
  payload_sha256: string;
  acknowledged_at: string;
}
```

**Step 3: Implement atomic append and lookup**

Provide `prepareDelivery`, `acknowledgeDelivery`, and `lookupByIdempotencyKey`. Use the repository's existing append/atomic-write primitives; do not introduce a second database. Canonicalize payload bytes deterministically before hashing.

**Step 4: Route directives and reviews through the outbox**

Persist intent before delivery and transition runner phase only after matching acknowledgement. Preserve existing message envelopes while adding delivery id/digest fields compatibly.

**Step 5: Verify and commit**

Run focused tests, concurrency tests repeatedly, and typecheck; commit:

`git commit -m "feat: make autoloop delivery durable and idempotent"`

## Task 6: Expose `autoloop_recover` and make resume idempotent

**Files:**
- Modify: `src/session-manager.ts`
- Modify: `src/index.ts`
- Modify: `src/embedded-server.ts`
- Modify: `src/autoloop/runner.ts`
- Modify: `src/autoloop/recovery.ts`
- Test: `src/__tests__/session-manager.test.ts`
- Test: `src/__tests__/tool-registration.test.ts`
- Test: `src/__tests__/embedded-server-launcher.test.ts`
- Test: `src/__tests__/autoloop-runner.test.ts`

**Step 1: Write failing inspect/apply tests**

Test read-only assessment, explicit apply with matching token, stale token rejection, repeated apply returning the same receipt, two simultaneous applies, reconstruction from disk when no live runner exists, and refusal when ownership is ambiguous.

**Step 2: Add SessionManager API**

```ts
autoloopRecover(runId: string, options?: {
  apply?: boolean;
  recovery_token?: string;
}): Promise<RecoveryResult>;
```

Default to inspection. Applying requires the current token and executes exactly the reconciler's next safe action. Persist a versioned recovery event/receipt.

**Step 3: Register MCP and embedded HTTP surfaces**

Add `autoloop_recover` schema and handler, plus an embedded endpoint consistent with existing routing. Return the effective phase, evidence, session classification, pending deliveries, next safe action, and token.

**Step 4: Route legacy resume through recovery**

Keep existing arguments, but reconstruct first and use a generated/stable idempotency token. Do not duplicate sessions, directives, commits, or notifications.

**Step 5: Verify and commit**

Run focused tests and commit:

`git commit -m "feat: add idempotent autoloop recovery"`

## Task 7: Add real-process recovery and legacy E2E coverage

**Files:**
- Create: `src/__tests__/autoloop-durable-recovery-e2e.test.ts`
- Modify: `src/__tests__/agy-planner-e2e.test.ts`
- Modify: test fixtures/helpers only as required

**Step 1: Build deterministic subprocess fixtures**

Fixtures must simulate: dead process after lease, stale name reservation, reset/recreation, empty adapter reply, crash after outbox persistence, crash after delivery, and Reviewer-only review. Avoid timing-only assertions; expose synchronization barriers/files or IPC signals.

**Step 2: Test legacy ledger compatibility**

Load representative pre-change run metadata with absent generation/outbox fields. Assert conservative phase reconstruction and no mutation in inspection mode.

**Step 3: Test the OG-GOV001-shaped case generically**

Create Coder artifacts and checkpoint with no verdict, terminate all physical sessions, recover, start only Reviewer, and persist one verdict bound to the original checkpoint/source iteration. Assert no Coder start and no continuation run.

**Step 4: Run reliability repetitions**

Run the E2E file at least five times and the concurrency-focused tests at least twenty times. Any flake is a failure requiring diagnosis.

**Step 5: Commit**

`git commit -m "test: cover interrupted autoloop recovery end to end"`

## Task 8: Document APIs, compatibility, and reviewer path

**Files:**
- Modify: `skills/references/autoloop.md`
- Modify: `README.md` if public tool inventory is documented there
- Create or modify: repository PR template material only if an existing convention supports it
- Test: documentation/tool registration consistency tests

**Step 1: Document operations and errors**

Document effective phases, `autoloop_recover` inspect/apply examples, recovery token semantics, independent spawn/review, reset postconditions, error codes, compatibility behavior, and operator guidance.

**Step 2: Add a reviewer-oriented change map**

Explain reproduced incidents -> root cause -> invariant -> implementation file -> regression test. Include non-goals, rollout, rollback, and remaining risks. Do not include private project payloads; describe OG-GOV001 as an anonymized live-recovery validation.

**Step 3: Verify generated/public documentation consistency**

Run tool registration tests and any documentation checks, then commit:

`git commit -m "docs: explain durable autoloop recovery"`

## Task 9: Final local verification and independent review

**Files:** all changed files

**Step 1: Run formatting and static checks**

```bash
npm run format:check
npm run lint
npm run typecheck:tests
npm run build
```

Expected: all exit 0.

**Step 2: Run full tests and coverage-sensitive suites**

```bash
npm test
npm run test:coverage
```

Expected: all tests pass with no new uncovered critical recovery branch.

**Step 3: Run repeated reliability gates**

Repeat concurrency suites 20 times and durable-recovery E2E 5 times; capture commands and summaries in Autoloop evidence.

**Step 4: Request independent Cursor review**

Reviewer must inspect the final candidate only, defect-first and read-only, against the approved design and this plan. Material findings return to the Coder for TDD fixes, full affected verification, and a fresh independent review.

**Step 5: Commit final corrections and verify clean tree**

Run `git status --short`, `git diff --check`, and the full gate again. Do not claim completion unless output is current and green.

## Task 10: Local install, isolated smoke, and controlled live recovery

**Files:** deployment evidence only; do not commit machine-specific paths/configuration

**Step 1: Preserve rollback state**

Record installed package version/build identity, Gateway status, relevant non-secret configuration hashes, and reversible reinstall command. Do not expose credentials or tokens.

**Step 2: Install candidate and restart Gateway**

Install the built candidate using the existing package/plugin mechanism. Restart the Gateway under the owner's explicit authorization for this task and verify health before proceeding.

**Step 3: Run isolated smoke**

Exercise orphan cleanup/name reuse, strict empty-response failure, repeated recovery idempotency, and Reviewer-only review. Assert zero duplicate sessions/deliveries and expected durable evidence.

**Step 4: Recover OG-GOV001 C6**

Only after smoke passes, inspect then apply recovery for C6. Prove checkpoint `caee5092beb8af184ebade942f0f2ea65daf7f24` reaches Cursor review without starting a Coder bridge and without creating C7. Preserve all project authorization boundaries and zero-push policy.

**Step 5: Roll back on any smoke failure**

Restore the prior installed package and restart the Gateway. Do not touch OG-GOV001 after a failed smoke. Diagnose and return to Task 9.

## Task 11: Draft PR, GitHub CI, and publication

**Files:** PR metadata and any fixes required by CI/review

**Step 1: Push the feature branch and open a draft PR**

Target current upstream main. The title and body must frame the work as one durable lifecycle correction. Include the incident/root-cause matrix, invariants, API compatibility, commit/file guide, local tests, repeated reliability evidence, isolated smoke, live recovery, rollout/rollback, non-goals, and risks.

**Step 2: Wait for every required GitHub check**

Inspect failures from their logs. Fix real failures test-first, rerun all affected local gates, reinstall/re-smoke if runtime behavior changed, and obtain a new independent Cursor review for material corrections.

**Step 3: Rebase/update safely if upstream moved**

Resolve conflicts without weakening tests or invariants; rerun the complete final gate and GitHub CI.

**Step 4: Convert to Ready for review only when green**

All required GitHub checks, final local verification, independent review, isolated smoke, and controlled live recovery must be green/current. Then convert the draft PR to Ready and report its URL and evidence summary.
