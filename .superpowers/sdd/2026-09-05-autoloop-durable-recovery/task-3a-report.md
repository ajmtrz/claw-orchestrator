# Task 3A Coder Report: strict Planner success and reset postconditions

## Status

Complete for the approved slice-A scope and review-fix round 1. The original
implementation commit is `e590475757ba0814fec9e3af8b48f366f68ffbec`
(`fix: reject false-success autoloop turns`). Review findings A1-A6 are resolved
in `45484d970d4b0ceb2b73b8088d909a6df8555581`
(`fix: harden Planner control and reset proofs`). No push was performed.

## Immutable inputs

- Instruction snapshot:
  `tasks/CLAWO-AUTOLOOP-DURABLE-RECOVERY-20260905-R1/instructions/global-AGENTS.md`
  at SHA-256 `9cc0b00ab3be15fb2d932dc5b5047d4285969fdd949df504cd64e262c72b0850`.
- The live global `AGENTS.md` had the same SHA-256 at the final stage boundary.
- Extracted Task 3 brief:
  `.superpowers/sdd/2026-09-05-autoloop-durable-recovery/task-3-brief.md`
  at SHA-256 `7e41fbce41fdf7654abb2db4880c2a733bf3adc23bdbf6f6a6df419e7d99dc06`.
- Approved design and full implementation plan were read before implementation.

## Exact scope

Four implementation/test files changed in the implementation commit:

1. `src/autoloop/dispatcher.ts`
2. `src/session-manager.ts`
3. `src/__tests__/session-manager.test.ts`
4. `src/__tests__/agy-planner-e2e.test.ts` (new)

This report is the fifth and final tracked file in slice A. No HTTP/MCP boundary
files or tests were modified.

## Implementation

- Added the centralized internal `assertPlannerTurnSucceeded` seam and typed
  retryable operation errors.
- A real Planner turn now requires a non-empty logical reply, the same live
  physical generation after send, no denied/failed required tool work, and an
  exact matching persisted control event when the reply claims controls.
- Control evidence records dispatch/message/iteration identity, exact
  generation-owner-session identity, the tool list, and a SHA-256 of the parsed
  calls. The append is re-read before the turn can pass.
- Runner phase advancement for `spawn_subagents` is delayed until the complete
  Planner turn contract succeeds; rejected turns remain in planning.
- Reset now proves physical absence, compare-and-releases the exact prior
  generation, proves the authoritative registry tombstone/name reusability,
  and (when requested) proves the replacement generation is live.
- Reset returns a structured internal success/failure union. The existing
  SessionManager boolean compatibility method returns `result.ok`, so a failed
  postcondition cannot become `true`.
- Existing Task 2 reservation, generation, lease, owner, and release fences are
  reused rather than bypassed.

## TDD evidence

### RED

Each required false-success case was introduced and observed failing before
the corresponding production change:

- Empty transport-success reply resolved successfully instead of rejecting.
- Missing post-send physical session resolved successfully instead of
  rejecting.
- A turn whose engine success counter did not advance resolved successfully.
- A claimed `spawn_subagents` control advanced the runner despite the control
  append being suppressed.
- Dispatcher reset returned `undefined` despite the exact registry reservation
  remaining occupied.
- The legacy SessionManager reset method returned `true` for that failed reset
  postcondition.

### GREEN

- `npx vitest run src/__tests__/session-manager.test.ts -t 'strict Planner turn success'`
  -> 6 passed.
- `npx vitest run src/__tests__/agy-planner-e2e.test.ts`
  -> 1 passed. A real AGY subprocess reports `STOPPED` on its first turn,
  preserves the conversation ID, and succeeds on a second turn in that same
  conversation. The denied turn emits no Planner reply.

## Final verification

- Focused plus Task 2 regressions:
  `npx vitest run src/__tests__/session-manager.test.ts src/__tests__/agy-planner-e2e.test.ts src/__tests__/autoloop-dispatcher.test.ts src/__tests__/autoloop-recovery.test.ts src/__tests__/autoloop-planner-tools.test.ts`
  -> 5 files passed, 251 tests passed.
- `npm run build` -> passed.
- `npm run lint` -> passed.
- `npm run format:check` -> passed.
- `git diff --check` -> passed.
- `npm run typecheck:tests` -> exit 2 with exactly 83 diagnostics, matching the
  documented pre-existing baseline exactly. There are zero new diagnostics in
  `src/autoloop/dispatcher.ts`, `src/session-manager.ts`, or the new AGY test;
  the three printed diagnostics in `session-manager.test.ts` are existing lines
  98, 3097, and 3111 outside this slice's added block.
- Bounded full `npm test` -> 79 files passed and 1 failed; 1,640 tests passed and
  1 failed. The sole failure is the proven baseline
  `src/__tests__/ultraapp/host-strategy.test.ts` case `hostBuild succeeds even
with no build script`, caused by its nested npm invocation. No Ultraapp file
  was modified.

## Deferred to Task 3B

- Stable typed MCP error mapping in `src/index.ts` and its tests.
- Stable embedded HTTP status/body mapping and async terminal-failure recording
  in `src/embedded-server.ts` and its tests.
- Any public adapter changes required to expose the structured reset result.

## Concerns and follow-on notes

- Review-fix round 1 closes the two concerns above: exact control intent is now
  persisted and re-read before any handler executes, and empty logical results
  fail closed even when optional engine counters are unavailable.
- The durable mailbox / outbox work in later approved tasks remains responsible
  for idempotent retry after a persisted intent has begun applying effects.
- Public HTTP/MCP callers retain their previous boundary behavior until slice B;
  internal typed failures and structured reset results are intentionally ready
  for those adapters.

## Review-fix round 1

### Inputs and scope

- Independently consolidated findings:
  `.superpowers/sdd/2026-09-05-autoloop-durable-recovery/task-3a-review-round-1-findings.md`
  at SHA-256
  `0353b753fc799ebaf30abdad0be9185881b255267f7c743ba023c075cb4421fb`.
- The immutable instruction snapshot remained
  `9cc0b00ab3be15fb2d932dc5b5047d4285969fdd949df504cd64e262c72b0850`.
- The review-fix implementation commit changes exactly three tracked files:
  `src/autoloop/dispatcher.ts`, `src/session-manager.ts`, and
  `src/__tests__/session-manager.test.ts`. This existing report is the fourth
  tracked file in the review-fix range. `src/__tests__/agy-planner-e2e.test.ts`
  was permitted but did not require modification. No other file was changed.

### Rulings and implementation

- **A1:** Complete Planner controls, including exact arguments, are appended and
  verified before `applyPlannerToolCalls`; append/re-read failure produces zero
  control effects.
- **A2:** Verification parses the just-appended non-empty ledger tail and
  compares its control ID, timestamp, dispatch/message/iteration identity,
  generation/owner/session identity, tool list, complete calls, and digest to
  the intended evidence. It no longer scans the whole ledger or returns the
  in-memory twin.
- **A3:** Logical success requires a non-empty cleaned reply or an exact verified
  control independently of optional counters. A fences-only control is surfaced
  as `Planner controls persisted: <tools>` after successful application.
- **A4:** Engine/transport failure, actual counter denial/non-advance, malformed
  control, control-application failure, and control-persistence failure retain
  distinct internal typed codes. Dispatcher phase-error decision evidence
  records the same typed code. Public HTTP/MCP mapping remains deferred to 3B.
- **A5:** Started flags and the frozen Reviewer prompt remain unchanged until
  physical absence, exact release, and reuse are proved. Every failure path,
  including eager replacement failure, restores the prior in-memory state.
- **A6:** SessionManager now provides a lock-protected, non-mutating production
  reusability probe. It checks live/pending state and the authoritative exact
  released tombstone without creating a reservation, so probe failure cannot
  worsen the registry. Reset uses it on every production success path.
- The review's two generation-ledger scan observations remain deferred as
  performance-only. Task 5 still owns durable delivery/outbox idempotence after
  an intent has been verified. Task 3B still owns public HTTP/MCP mappings.

### Deterministic TDD evidence

#### RED

- Suppressing the Planner control append still spawned Coder and Reviewer
  sessions (three sessions observed instead of the Planner-only one).
- A durable record with the expected control ID but generation `999` was
  accepted and `spawn_subagents` executed.
- An empty reply resolved when counter reads threw, and a fences-only successful
  control returned an ambiguous empty reply.
- Engine result failure, thrown transport failure, malformed fences, and
  handler failure did not produce their required distinct codes.
- Release failure cleared Reviewer state/prompt; the production reset path did
  not consult a reusability probe; eager restart failure cleared Planner state.
- The production `probeAgentNameReusable` primitive was absent.

#### GREEN

- The strict Planner block now passes 19 tests, covering persistence-before-
  effect, same-ID/wrong-payload rejection, missing counters, fences-only output,
  generation absence/unknown state, all five failure classes, reset failure
  state preservation, production probe failure, and exact generation reuse.
- The production registry probe tests pass for successful exact release,
  owner/session mismatch rejection, and byte-for-byte unchanged registry state.
- The original real-subprocess AGY denial/conversation-continuity test remains
  green.

### Review-fix verification

- Focused plus Task 2 regressions:
  `npx vitest run src/__tests__/session-manager.test.ts src/__tests__/agy-planner-e2e.test.ts src/__tests__/autoloop-dispatcher.test.ts src/__tests__/autoloop-recovery.test.ts src/__tests__/autoloop-planner-tools.test.ts --reporter=dot --silent`
  -> 5 files passed, 265 tests passed.
- `npm run build` -> passed.
- `npm run lint` -> passed.
- `npm run format:check` -> passed.
- `git diff --check 73f9d6a0cb6b4c6f3c7b70071bd8652a08cbbd6b`
  -> passed.
- `npm run typecheck:tests` -> exit 2 with exactly 83 diagnostics, exactly
  matching the documented baseline count. Production and AGY files add zero
  diagnostics. The touched SessionManager test retains only the same three
  pre-existing error shapes at line 98 and shifted lines 3131/3145; no
  suppression was added.
- Bounded `timeout 180s npm test` completed in 64.77 seconds -> 79 files passed,
  1 failed; 1,654 tests passed, 1 failed. The sole failure remains the proven
  unchanged Ultraapp nested-`npm` baseline in
  `src/__tests__/ultraapp/host-strategy.test.ts`: `hostBuild succeeds even with
  no build script`. No Ultraapp file was modified.
