# Autoloop Durable Recovery Design

## Purpose

Make Autoloop recover predictably from process loss, expired leases, stale session
registrations, empty adapter replies, and partially completed iterations without
duplicating work or requiring a new continuation run. Add a first-class way to
review an existing checkpoint without starting a Coder.

This design does not promise that Autoloop can never fail. It defines observable,
testable guarantees for every failure mode reproduced during OG-GOV001.

## Problem Statement

The current implementation mixes durable run state with ephemeral engine-session
state. This produced several distinct failures with similar symptoms:

- a dispatcher replayed a directive other than the one confirmed by the Planner;
- expired activity leases left runs paused despite durable artifacts identifying
  the next safe phase;
- dead sessions retained names and blocked recreation;
- a supported Planner reset returned success but did not create a replacement;
- `autoloop_chat` could resolve successfully with an empty reply and no persisted
  control action;
- the public protocol could start Coder and Reviewer together but could not start
  or request only a Reviewer for an already-created checkpoint.

## Design Principles

1. The append-only ledger and iteration artifacts are authoritative.
2. Physical engine sessions are replaceable resources, not run identity.
3. Every externally visible transition is failure-atomic and idempotent.
4. Persist intent and its digest before delivery; confirm the same digest after
   delivery.
5. A successful API response must prove its promised durable effect.
6. Recovery must not invent identifiers, rewrite historical ledgers, or broaden
   authority.
7. Existing run metadata and public APIs remain readable and compatible.

## Durable State Model

Autoloop will formalize these phases:

- `PLANNING`
- `AWAITING_CODER`
- `CODER_RUNNING`
- `AWAITING_REVIEW`
- `REVIEWER_RUNNING`
- `PAUSED_RECOVERABLE`
- `BLOCKED`
- `COMPLETED`

The persisted representation may retain legacy status fields, but one canonical
reconciler will derive the effective phase from ledger events and artifacts.
Examples:

- directive persisted, no acknowledged delivery: `AWAITING_CODER`;
- Coder summary, evaluation, and diff present, no verdict: `AWAITING_REVIEW`;
- verdict `advance` present: advance to the next Planner boundary;
- expired lease with a live matching session: preserve the session and refresh
  only after proving ownership;
- expired lease with no live matching session: mark the physical generation
  orphaned and make the logical agent recoverable.

Each logical agent has a durable identity. Each physical session has a generation,
session identifier, owner instance identifier, creation time, last activity time,
and lease expiry. Generation replacement is appended as an event.

## Recovery and Idempotency

Add a public `autoloop_recover` operation. It performs read-only inspection first,
then applies one atomic reconciliation plan when explicitly requested. Its result
includes:

- reconstructed phase and evidence;
- live, stale, and orphaned physical sessions;
- pending persisted deliveries and their digests;
- the next safe action;
- a stable recovery token.

Repeating recovery with the same token returns the same result or a typed stale
token error. It cannot create duplicate sessions, directives, review requests,
commits, or notifications.

Session-name collisions are resolved only after the runtime proves that the
registered owner is absent or no longer owns the generation. Cleanup removes the
registry entry, lease, and physical-name reservation atomically and records the
orphaning event. A genuinely live conflicting owner remains a hard stop.

`autoloop_reset_agent` must verify its postcondition. Success means the previous
generation is stopped or proven absent, its reservation is released, and a new
generation can be created. Failure returns a typed error and never reports
`ok=true`.

## Adapter Success Contract

All Planner adapters share a strict success contract:

- a physical session exists or was deliberately reused;
- the turn completed without a denied required tool;
- the reply is non-empty when a reply is required;
- every claimed control action was parsed, validated, and persisted;
- the returned generation and persisted event identify the same turn.

An empty response, missing session, missing ledger event, or denied required tool
is a recoverable typed error. Conversation history remains available for a safe
retry, but the runtime does not advance phase or report success.

## Independent Agent Dispatch

Add independent lifecycle controls for Coder and Reviewer while preserving
`spawn_subagents` as a compatibility wrapper:

- `spawn_coder`
- `spawn_reviewer`
- `request_review`

`request_review` requires a checkpoint SHA, source run/iteration, review scope,
and idempotency key. It persists a review request and digest before delivery. The
Reviewer acknowledges the same digest before reviewing. It can therefore review a
checkpoint imported from a prior continuation without creating or directing a
Coder.

The existing joint spawn path delegates to the independent primitives. Existing
Planner output remains valid.

## Delivery Protocol

Coder directives and review requests use the same durable outbox protocol:

1. validate authority, phase, scope, and target agent;
2. persist the complete payload, digest, idempotency key, and target generation;
3. attempt delivery;
4. persist acknowledgement containing the received digest;
5. transition phase only after matching acknowledgement.

Retries inspect the outbox first. An acknowledged item is a no-op; an unacknowledged
item is redelivered only to the intended logical agent under a valid generation.
Digest mismatch stops the run.

## Backward Compatibility

- Existing ledgers are never rewritten.
- Missing generation and outbox fields are derived conservatively.
- Existing `spawn_subagents`, `autoloop_chat`, `autoloop_reset_agent`, and resume
  inputs remain accepted.
- Recovery events use versioned schemas.
- A legacy state that cannot be reconstructed unambiguously becomes `BLOCKED`
  with explicit evidence rather than being guessed.

## Testing Strategy

### Unit and Property Tests

- every valid and invalid state transition;
- lease acquisition, renewal, expiry, orphan detection, and generation fencing;
- reset postconditions and name reuse;
- empty-response and missing-effect rejection;
- outbox persistence, acknowledgement, digest mismatch, and deduplication;
- backward reconstruction from legacy metadata.

Concurrency tests race duplicate resume, recover, directive, and review requests.
Each must produce exactly one durable effect.

### End-to-End Tests

Use real subprocess adapters where practical and deterministic crash injection to
cover:

1. process death with an expired lease;
2. stale session-name reservation;
3. reset followed by recreation;
4. transport success with an empty logical reply;
5. reconstruction of `AWAITING_REVIEW` from Coder artifacts;
6. Reviewer-only startup and review without a Coder session;
7. crash between persistence and delivery;
8. crash between delivery and acknowledgement;
9. mismatched payload digest;
10. legacy run recovery.

The full repository suite, build, lint, and formatting must pass. Cursor performs
an independent defect-first review after the final implementation and after any
material correction.

## Local Rollout and Acceptance

1. Preserve the currently installed package and its configuration for rollback.
2. Install the candidate build locally.
3. Restart the Gateway under the owner's explicit authorization.
4. Run an isolated smoke that exercises orphan cleanup, empty-response rejection,
   idempotent recovery, and Reviewer-only dispatch.
5. Only after the smoke passes, recover OG-GOV001 C6.
6. Prove that the existing `caee509` checkpoint reaches Cursor review without a
   Coder bridge and without creating C7.
7. If the smoke fails, restore the previous package and restart the Gateway before
   touching OG-GOV001.

## Upstream Publication

Develop on a new branch from the current upstream main. Open a separate draft PR
only after local implementation, tests, independent review, local installation,
isolated smoke, and the controlled OG-GOV001 recovery pass.

The draft PR activates GitHub's PR-only workflows. All required checks must pass
before conversion to Ready for review. Any material fix after review or CI requires
fresh affected tests and another independent review.

The PR description will give reviewers:

- the reproduced incidents and root-cause mapping;
- the state-machine and durable-outbox invariants;
- API and compatibility changes;
- a file/commit review guide;
- unit, concurrency, E2E, full-suite, smoke, and live-recovery evidence;
- rollout and rollback procedure;
- explicit non-goals and remaining risks.

## Non-Goals

- rewriting Autoloop as a new orchestration engine;
- changing Planner/Coder/Reviewer model policy;
- silently recovering ambiguous or concurrently owned runs;
- weakening single-writer, review, authorization, or push gates;
- claiming that all future defects are impossible.

## Acceptance Criteria

The work is complete only when:

1. all reproduced lifecycle failures have deterministic regression tests;
2. recovery is idempotent and generation-fenced under concurrency;
3. empty success cannot cross any adapter boundary;
4. orphan cleanup makes names safely reusable;
5. Reviewer-only review works for an existing checkpoint;
6. legacy run reconstruction is conservative and auditable;
7. local and GitHub-required CI are green;
8. independent review has no material findings;
9. isolated smoke passes on the installed candidate;
10. OG-GOV001 C6 reaches review of `caee509` without a Coder bridge or C7;
11. the PR is converted from draft to Ready only after all prior criteria pass.
