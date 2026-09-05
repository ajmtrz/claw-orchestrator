# Task 3A Coder Report: strict Planner success and reset postconditions

## Status

Complete for the approved slice-A scope. The implementation commit is
`e590475757ba0814fec9e3af8b48f366f68ffbec` (`fix: reject false-success autoloop turns`).
No push was performed.

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

- Planner control effects are still executed through the existing tool-effect
  seam before the strict control-evidence append is proved. Phase advancement
  is gated correctly, but a tool such as `spawn_subagents` may have started its
  physical sessions before a later persistence failure. The durable mailbox /
  outbox work in later approved tasks remains responsible for broader
  side-effect delivery atomicity.
- The non-empty reply check uses real SessionManager turn counters to
  distinguish completed physical turns from legacy dispatcher test doubles
  that do not expose numeric counters. Production SessionManager paths expose
  those counters and are covered by both unit and real-subprocess tests.
- Public HTTP/MCP callers retain their previous boundary behavior until slice B;
  internal typed failures and structured reset results are intentionally ready
  for those adapters.
