# Task 3A Coder Report: strict Planner success and reset postconditions

## Status

Complete for the approved slice-A scope through final-review correction round 4.
The original implementation commit is `e590475757ba0814fec9e3af8b48f366f68ffbec`
(`fix: reject false-success autoloop turns`). Review findings A1-A6 are resolved
in `45484d970d4b0ceb2b73b8088d909a6df8555581`
(`fix: harden Planner control and reset proofs`). Review-fix round 2 is in
`90cf4a72fa42dc480612844aa79afc5557d74ee2`
(`fix: close durable Planner recovery gaps`). The owner-audit round-3 correction
is `4d8f157248eee8f76072c514ac4a8bdd157f7cd0`
(`fix: finish pending autoloop generation release`). Final-review round 4 is
`2ef58c0a680030ce5925c3f5e1f2bfc4a7626eae`
(`fix: make Planner control delivery failure-atomic`). No push was performed.

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

## Review-fix round 2

### Inputs, commit, and exact scope

- Independently consolidated findings:
  `.superpowers/sdd/2026-09-05-autoloop-durable-recovery/task-3a-review-round-2-findings.md`
  at SHA-256
  `1e8a3efb3de6cddd35c0b5a5e4d37e6042cb89c3a762901c4bc781c99dc946a9`.
- The immutable instruction snapshot remained
  `9cc0b00ab3be15fb2d932dc5b5047d4285969fdd949df504cd64e262c72b0850`
  at the final stage boundary.
- Implementation commit:
  `90cf4a72fa42dc480612844aa79afc5557d74ee2`
  (`fix: close durable Planner recovery gaps`).
- The implementation commit changes exactly four tracked files:
  `src/autoloop/dispatcher.ts`, `src/autoloop/messages.ts`,
  `src/session-manager.ts`, and `src/__tests__/session-manager.test.ts`.
  This existing report is the fifth and only other tracked file in the round-2
  range. The forbidden `src/__tests__/agy-planner-e2e.test.ts`, all Task 3B
  HTTP/MCP files, and all Ultraapp files remain unchanged.

### Technical rulings and implementation

- **B1:** In-memory ownership now changes at the exact durable release callback,
  including the case where registry finalization throws after the release
  tombstone is durable. Failures before that callback restore the prior started
  flag and frozen Reviewer prompt; failures after it never resurrect the old
  generation. Failed eager startup leaves its replacement durably released and
  permits the next exact generation. Unknown post-start liveness conservatively
  retains the new live-ledger generation and started flag, preventing duplicate
  startup until a later probe proves it live.
- **B2:** Planner control values are recursively canonicalized once with stable
  code-unit key ordering while array order is preserved. The same normalized
  complete controls feed tool extraction, SHA-256, persistence, comparison, and
  application, eliminating source-property-order false rejection.
- **B3 (partially rejected after regression validation):** The inference that
  every typed Planner failure must be converted inside `dispatcher.deliver()`
  was broader than the approved design and broke the established direct
  dispatcher rejection contract proven by the unchanged AGY test. Direct
  callers therefore continue to receive distinct thrown
  `AutoloopOperationError` values. The existing empty-reply runner route now
  carries its code through typed `PhaseErrorPayload` with no cast, while every
  typed thrown error still records exact-code decision evidence.
  `SessionManager.autoloopChat()` consumes typed routed errors and has a final
  non-empty guard, so lost event plumbing cannot return `{ reply: "" }`.
- Genuine Planner send deadlines are not relabeled as empty replies:
  `autoloopChat()` rejects with `AUTOLOOP_SEND_TIMEOUT`, `retryable: true`, and
  the exact `pending_dispatch`; a terminal race rejects with
  `AUTOLOOP_RUN_TERMINAL`, `retryable: false`, and the exact terminal reason.
  This preserves Task 2 timeout identity and state while closing empty success.
- **B4:** Deterministic corruption cases cover complete controls, digest,
  owner, session, dispatch, and message identity. Every case rejects before
  `spawn_subagents`, leaves only the Planner session, and preserves planning at
  iteration zero. The existing full evidence comparator was retained.
- **B5:** Deterministic coverage proves successful eager replacement returns
  live generation 2, failed eager startup releases generation 2 and later
  creates generation 3, and unknown replacement liveness retains generation 2
  without a second startup before later safe reuse. The routed empty-reply test
  also proves the exact typed phase-error payload and circuit increment.

### Explicit RED evidence

- B1 post-release/eager state:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'post-release|failed eager replacement' --reporter=verbose`
  -> 3 failed, 201 skipped; released Planner/Reviewer flags were restored and
  failed eager replacement claimed the old started state.
- B1 exact commit boundary:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'registry finalization fails after durable release' --reporter=verbose`
  -> 1 failed, 204 skipped; a durable generation-1 tombstone followed by a
  registry-finalization exception restored `plannerStarted: true`.
- B2 semantic ordering:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'normalizes Planner control property order' --reporter=verbose`
  -> 1 failed, 203 skipped with `AUTOLOOP_CONTROL_NOT_PERSISTED` for a valid
  args-before-tool control.
- B3 fail-closed exploration:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'typed Planner phase-error|phase-error event plumbing drops' --reporter=verbose`
  -> 7 failed, 1 passed, 196 skipped. The valid failure was the swallowed-code
  case resolving `{ reply: "" }`; the six circuit assertions for directly
  thrown non-empty codes were removed after the compatibility ruling above.
- B4 mutation proof: with the complete-control/digest and identity checks
  temporarily removed from both durable comparators,
  `npx vitest run src/__tests__/session-manager.test.ts -t 'rejects durable Planner control corruption' --reporter=dot --silent`
  -> 6 failed, 198 skipped because every tampered row wrongly resolved and
  applied effects. The comparator was restored immediately.
- B5 mutation proof: with the successful replacement generation deliberately
  misreported and unknown-liveness ownership deliberately cleared,
  `npx vitest run src/__tests__/session-manager.test.ts -t 'successful eager reset|unproven eager replacement' --reporter=verbose`
  -> 2 failed, 202 skipped. The mutation was restored immediately.
- Cross-contract regression RED:
  `npx vitest run src/__tests__/session-manager.test.ts src/__tests__/agy-planner-e2e.test.ts src/__tests__/autoloop-dispatcher.test.ts src/__tests__/autoloop-recovery.test.ts src/__tests__/autoloop-planner-tools.test.ts --reporter=dot --silent`
  -> 3 files passed, 2 failed; 280 tests passed, 4 failed. One failure exposed
  the overbroad direct-dispatch conversion and three exposed empty-reply
  relabeling of recoverable/terminal timeout outcomes. The final design split
  above resolved all four without changing the AGY test.

### GREEN and final verification

- B1/reset boundary set:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'registry finalization fails after durable release|post-release|failed eager replacement|successful eager reset|unproven eager replacement' --reporter=dot --silent`
  -> 6 passed, 192 skipped.
- B2 semantic control:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'normalizes Planner control property order' --reporter=dot --silent`
  -> 1 passed, 197 skipped.
- B3 routed/lost empty reply:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'rejects transport success with an empty logical reply|phase-error event plumbing drops the code' --reporter=dot --silent`
  -> 2 passed, 196 skipped.
- Direct dispatcher compatibility:
  `npx vitest run src/__tests__/agy-planner-e2e.test.ts --reporter=dot --silent`
  -> 1 passed.
- Task 2 timeout contract plus fail-closed chat outcome:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'Autoloop timeout resilience integration' --reporter=dot --silent`
  -> 4 passed, 194 skipped.
- B4 corruption set:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'rejects durable Planner control corruption' --reporter=dot --silent`
  -> 6 passed, 192 skipped.
- Final focused Task 3A plus Task 2 regression set:
  `npx vitest run src/__tests__/session-manager.test.ts src/__tests__/agy-planner-e2e.test.ts src/__tests__/autoloop-dispatcher.test.ts src/__tests__/autoloop-recovery.test.ts src/__tests__/autoloop-planner-tools.test.ts --reporter=dot --silent`
  -> 5 files passed, 277 tests passed.
- `npm run build` -> passed.
- `npm run lint` -> passed.
- `npm run format:check` -> passed.
- `git diff --check` -> passed.
- `npm run typecheck:tests` -> exit 2 with exactly 83 diagnostics, exactly the
  established baseline. The only diagnostics in touched files are the same
  three pre-existing `session-manager.test.ts` diagnostics at lines 99, 3132,
  and 3146; production files and all new test blocks add zero diagnostics.
- `timeout 180s npm test -- --reporter=dot --silent` completed in 64.78 seconds
  -> 79 files passed, 1 failed; 1,666 tests passed, 1 failed. The sole failure
  is exactly the unchanged baseline in
  `src/__tests__/ultraapp/host-strategy.test.ts`:
  `hostBuild succeeds even with no build script`. No Ultraapp file changed.

### Deferred ownership and remaining concern

- Full idempotent replay after a persisted intent begins applying effects
  remains Task 5 durable-outbox work.
- Public HTTP/MCP error and timeout mapping remains Task 3B; this round changes
  only internal SessionManager/dispatcher behavior and evidence.
- No unresolved Task 3A correctness concern remains. A fresh independent
  read-only review is still required by the parent workflow before advancement.

## Owner-audit correction round 3

### Inputs, ruling, and exact scope

- Owner audit:
  `.superpowers/sdd/2026-09-05-autoloop-durable-recovery/task-3a-owner-audit-round-3.md`
  at SHA-256
  `51683eb7973c716ee3e86dcb6f889d6e1df6cbbaad757a8fae8bef7439b846fe`.
- The immutable instruction snapshot remained
  `9cc0b00ab3be15fb2d932dc5b5047d4285969fdd949df504cd64e262c72b0850`
  at the implementation commit boundary.
- The finding is technically valid for repeated reset. The existing dispatcher
  test mocked `releaseReservation()` and appended release evidence without
  creating SessionManager's real durable `agentReleasePending` fence. With the
  production registry transaction, a failure in the final tombstone persistence
  leaves generation 1 released in the append-only ledger while the registry
  still authoritatively fences generation 1 as release-pending.
- Implementation commit:
  `4d8f157248eee8f76072c514ac4a8bdd157f7cd0`
  (`fix: finish pending autoloop generation release`). It changes exactly
  `src/autoloop/dispatcher.ts` and `src/__tests__/session-manager.test.ts`.
  This existing report is the only file in the separate evidence commit.

### Correction and safety invariants

- `resetAgent()` now retries `releaseGeneration()` for the exact previous
  generation even when the ledger already records it as released. The retry is
  still delegated to SessionManager's existing `releaseReservation()` operation;
  there is no second release-state store or bypass.
- SessionManager's exact generation/owner/session tuple, durable release-owner
  fence, typed registry lock/storage failures, and idempotent completed-hook
  tracking remain unchanged. A false compare-and-release remains a hard reset
  failure, and registry exceptions are not converted into ownership approval.
- Reset still proves runtime absence before the exact retry. `live` or `unknown`
  liveness exits before any release call, so a possibly-live generation is never
  released.
- A completed tombstone is an idempotent no-op. A pending matching tombstone is
  completed without appending a second `agent_generation_released` event.

### Deterministic TDD evidence

#### RED

- Replaced the mocked post-release test with a real SessionManager integration
  test. Its `renameSync` fault triggers only when the attempted registry snapshot
  is the final reusable generation-1 tombstone; by then the pending fence and the
  ledger release event both exist.
- Command:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'finishes a real pending registry release exactly once before creating the next generation' --reporter=verbose`
  -> 1 failed, 197 skipped. The authoritative registry remained
  `agentGeneration=1, agentReleasePending=true`, generation 2 reservation and
  physical startup were rejected, and the repeated reset returned `ok:false`
  where the required result was reusable success.

#### GREEN

- The same focused command with `--reporter=dot --silent` -> 1 passed, 197
  skipped.
- Adjacent release/liveness safety set:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'finishes a real pending registry release exactly once before creating the next generation|keeps a prepared tombstone fenced when completion persistence fails, then finishes idempotently|keeps the Planner started flag when reset liveness is unknown|preserves Reviewer started state and its frozen prompt when exact-generation release fails' --reporter=dot --silent`
  -> 4 passed, 194 skipped.
- The production test proves all intermediate and terminal effects: generation 1
  stays fenced before reconciliation; no generation-2 ledger row or second
  physical session exists; repeated reset completes the exact pending release;
  release evidence remains single; later chat creates exactly one live generation
  2, one active named session, and no generation greater than 2.

### Final verification after formatting

- Focused Task 3A plus Task 2 regressions:
  `npx vitest run src/__tests__/session-manager.test.ts src/__tests__/agy-planner-e2e.test.ts src/__tests__/autoloop-dispatcher.test.ts src/__tests__/autoloop-recovery.test.ts src/__tests__/autoloop-planner-tools.test.ts --reporter=dot --silent`
  -> 5 files passed, 277 tests passed.
- `npm run build` -> passed.
- `npm run lint` -> passed.
- `npm run format:check` -> passed.
- `git diff --check` -> passed.
- `npm run typecheck:tests` -> exit 2 with exactly 83 diagnostics, exactly the
  established baseline. The only diagnostics in touched files remain the same
  three pre-existing `session-manager.test.ts` diagnostics at lines 99, 3132,
  and 3146; dispatcher and the new test block add zero diagnostics.
- `timeout 180s npm test -- --reporter=dot --silent` completed in 64.80 seconds
  -> 79 files passed, 1 failed; 1,666 tests passed, 1 failed. The sole failure
  remains exactly the unchanged baseline in
  `src/__tests__/ultraapp/host-strategy.test.ts`:
  `hostBuild succeeds even with no build script`. No Ultraapp file changed.

### Remaining workflow state

- No Task 3B, outbox, runtime, Gateway, Ollama, or Ultraapp file was changed.
- No push was performed.
- A fresh independent read-only review of this final candidate remains required
  by the parent workflow before Task 3A can advance.

## Final-review correction round 4

### Inputs, commit, and exact scope

- Adjudicated findings:
  `.superpowers/sdd/2026-09-05-autoloop-durable-recovery/task-3a-review-round-4-findings.md`
  at SHA-256
  `c9b53a930841497a773908250986e42899cde9b0f924a7b8dbb8b7802083ea13`.
- The immutable instruction snapshot remained
  `9cc0b00ab3be15fb2d932dc5b5047d4285969fdd949df504cd64e262c72b0850`
  at the implementation commit boundary.
- Implementation commit:
  `2ef58c0a680030ce5925c3f5e1f2bfc4a7626eae`
  (`fix: make Planner control delivery failure-atomic`).
- The implementation commit changes exactly the five allowed files:
  `src/autoloop/dispatcher.ts`, `src/autoloop/planner-tools.ts`,
  `src/autoloop/runner.ts`, `src/session-manager.ts`, and
  `src/__tests__/session-manager.test.ts` (594 insertions, 155 deletions).
  This report is the only file in the separate evidence commit. No Task 3B,
  Ultraapp, runtime, Gateway, or Ollama file was modified.

### Technical rulings and implementation

- **C1:** AGY is the adapter whose usable non-empty denial is intentionally
  represented only by `turnsSucceeded`. Missing or non-finite before/after AGY
  counters now fail closed as `AUTOLOOP_REQUIRED_TOOL_DENIED` before control
  persistence or application. Other engines retain their existing
  `SendResult.error`/`is_error` and transport classifications. Pure empty output
  retains `AUTOLOOP_EMPTY_REPLY` precedence.
- **C2:** `applyPlannerToolCalls()` now prepares and validates the complete
  deterministic batch before invoking any prepared closure. Any validation
  error returns zero control messages and performs zero direct effects. A fully
  valid batch still applies in original order; unexpected operational errors
  retain the existing ordered partial-delivery behavior assigned to Task 5.
- **C3:** The SessionManager runner transition is now wired to
  `onSpawnSubagentsCommitted`, called immediately after the durably verified
  `spawn_subagents` effect returns. `markSubagentsSpawned()` remains idempotent.
  A prevalidation/spawn failure never marks state, while a later operational
  control failure leaves already-created Coder/Reviewer sessions truthfully
  represented by `subagents_spawned=true` and `status=running`.
- **C4:** Direct `dispatcher.deliver()` compatibility is preserved: empty
  Planner output remains a typed `phase_error` envelope and the other typed
  operation failures remain direct rejections. When Runner dispatches a Planner
  turn, it converts every thrown typed operation failure into the normal queued
  `phase_error` envelope, drains the policy/circuit path once, then rethrows the
  original typed failure. Decision evidence, Runner counters, emitted event,
  policy hook, and caller-visible code are therefore aligned without duplicate
  accounting.
- **C5:** A real SessionManager registry regression now enters the crash window
  with durable generation-1 release evidence and a pending registry tombstone,
  then calls `autoloopChat()` directly. Chat completes that exact release before
  starting one generation-2 Planner, with one release row, one reservation, one
  physical start, and no generation greater than 2.

### Explicit RED evidence

- New C1-C4 regressions were run before production changes with:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'fails closed before persisting a fenced control|prevalidates the complete Planner control batch|marks a durably verified successful spawn|keeps a committed spawn marked|routes .* through exactly one typed Runner phase-error path|finishes a real pending registry release on direct chat' --reporter=verbose`
  -> 8 failed, 2 passed, 198 skipped. The unavailable-counter turn wrongly
  resolved and spawned; the later-invalid batch created a spawn decision and
  sessions; a committed spawn followed by an operational write failure remained
  `planning/subagents_spawned=false`; and five non-empty typed failure classes
  emitted zero Runner phase errors and left the circuit count at zero. The two
  positive controls were the already-correct simple spawn and direct-chat
  pending-release paths.
- C5 received an explicit mutation proof before production correction: removing
  only the existing pending-release completion/retry block and running
  `npx vitest run src/__tests__/session-manager.test.ts -t 'finishes a real pending registry release on direct chat before starting exactly one successor' --reporter=verbose`
  -> 1 failed, 207 skipped with
  `AUTOLOOP_AGENT_GENERATION_CONFLICT` while reserving generation 2. The block
  was restored immediately; the test then proves the public chat path rather
  than a repeated-reset helper path.
- The initial five-file cross-contract gate exposed nine regressions in direct
  dispatcher compatibility (six direct empty-envelope cases and three
  non-empty non-AGY fake-manager cases). This evidence rejected an overbroad
  conversion/counter rule. Restoring the narrow direct empty envelope and
  limiting unavailable-counter denial to AGY resolved the gate without changing
  the direct-dispatcher or AGY tests.

### GREEN and final verification

- Focused round-4 regressions:
  `npx vitest run src/__tests__/session-manager.test.ts -t 'fails closed before persisting a fenced AGY control|prevalidates the complete Planner control batch|marks a durably verified successful spawn|keeps a committed spawn marked|routes .* through exactly one typed Runner phase-error path|finishes a real pending registry release on direct chat' --reporter=dot --silent`
  -> 11 passed, 198 skipped. The C1 cases cover both unavailable and non-finite
  AGY counter evidence.
- Direct dispatcher plus real AGY compatibility:
  `npx vitest run src/__tests__/autoloop-dispatcher.test.ts src/__tests__/agy-planner-e2e.test.ts --reporter=dot --silent`
  -> 2 files passed, 48 tests passed.
- Final focused Task 3A plus Task 2 regression gate:
  `npx vitest run src/__tests__/session-manager.test.ts src/__tests__/agy-planner-e2e.test.ts src/__tests__/autoloop-dispatcher.test.ts src/__tests__/autoloop-recovery.test.ts src/__tests__/autoloop-planner-tools.test.ts --reporter=dot --silent`
  -> 5 files passed, 288 tests passed.
- `npm run build` -> passed.
- `npm run lint` -> passed.
- `npm run format:check` -> passed.
- `git diff --check` -> passed.
- `npm run typecheck:tests` -> exit 2 with exactly 83 diagnostics, exactly the
  established baseline. The only diagnostics in touched files remain the same
  three pre-existing `session-manager.test.ts` diagnostics at lines 99, 3132,
  and 3146; all touched production files and all new test blocks add zero
  diagnostics.
- `timeout 180s npm test -- --reporter=dot --silent` completed in 64.84 seconds
  -> 79 files passed, 1 failed; 1,677 tests passed, 1 failed. The sole failure
  remains exactly the unchanged baseline in
  `src/__tests__/ultraapp/host-strategy.test.ts`:
  `hostBuild succeeds even with no build script`. No Ultraapp file changed.

### Remaining workflow state

- Task 5 still owns durable replay/rollback after a valid persisted batch has
  begun applying operational effects.
- Task 3B still owns public HTTP/MCP error/reset mapping and asynchronous
  embedded-chat terminal recording.
- No unresolved Task 3A implementation concern remains. A fresh final
  independent read-only review is required before advancement.
