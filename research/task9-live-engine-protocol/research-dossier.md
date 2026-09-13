# Research Dossier — Task 9 live engine protocol

- Successor: `CLAWO-AUTOLOOP-DURABLE-RECOVERY-POST-UPSTREAM-TASK8-9-R1`
- Parent: `CLAWO-AUTOLOOP-DURABLE-RECOVERY-20260905-BOOTSTRAP`
- Retrieval date: 2026-09-13
- Candidate base: `fb913779225777b299749d8955bac60a0a940f60`
- Candidate tree: `e7fd39ae18c3bdf630aeb82a3ba0b5c58f9b76cf`
- Fan-out: `fanout-mtzx3hm8-d3a0d4e0`, `synthesize:false`, status `done`, five read-only roles

## Question

What already-authorized, effectively available protocol and bindings can validate the minimum real Codex, Claude, and AGY boundaries required by Task 9 without installation, runtime/Gateway/configuration/secret changes, ad-hoc engine/model substitution, or unnecessary live quota?

## Executive findings

1. The deterministic full-lifecycle matrix is implementable with the repository's real adapter-shaped fixtures and durable ledgers. It must not be described as live-provider evidence.
2. Real wrappers and binaries exist for Codex, Claude, and AGY. Presence and version do not authorize a model or prove the candidate's Autoloop lifecycle.
3. The immutable successor snapshot binds only Autoloop Codex roles:
   - Planner: `codex/gpt-6-astra`
   - Coder: `codex/gpt-5.6-terra`
   - Reviewer: `codex/gpt-5.6-sol`
4. The snapshot contains no Claude model. AGY `gemini-3.8-flash-high` appears only in the Fan-out/Council profiles, not the Autoloop profile.
5. Therefore the requested live Autoloop cells are:
   - Codex: executable in principle with the bound models, subject to fresh provider/quota evidence.
   - Claude: `BLOCKED_BINDING`.
   - AGY: `BLOCKED_BINDING`.
6. Claude/AGY cannot be marked `NOT_APPLICABLE`: the owner explicitly requires those boundaries, and `BLOCKED` or `NOT_EXECUTED` cannot count as completion.
7. Using a CLI default model, copying AGY's model from a different profile, changing the Autoloop binding, or treating a direct session smoke as Reviewer delivery would violate the governing configuration/no-substitution rules.

## Verified facts

### Immutable configuration

- `tasks/CLAWO-AUTOLOOP-DURABLE-RECOVERY-POST-UPSTREAM-TASK8-9-R1/checkpoints/council.yaml`
  - SHA-256: `2cb588ce434270b7db9ae24945699f36d1190785555e0d9c7a27d08ab4aa7db5`
  - Autoloop flat fields bind all three roles to Codex.
  - No Claude engine/model appears anywhere in the snapshot.
  - AGY is present only under Fan-out/Council.
- `checkpoints/successor-preflight-resolved.json`
  - SHA-256: `301957ac5031ddb1b122767abbf4db11fb77509db8e2110becea5e6dcab386a4`
  - Records the six effective engine/model fields, `nested_per_role_effort_effective:false`, and `ad_hoc_substitution:false`.

### Adapter and runtime identity

| Engine | Adapter source | Installed binary/version |
|---|---|---|
| Codex | `src/persistent-codex-session.ts` | `/home/openclaw/.nvm/versions/node/v26.7.0/bin/codex`, 0.153.0 |
| Claude | `src/persistent-session.ts` | `/home/openclaw/.local/bin/claude`, 2.1.267 |
| AGY | `src/persistent-agy-session.ts` | `/home/openclaw/.local/bin/agy`, 1.2.2 |

The repository documents newer tested pins for Codex (0.154.0) and Claude (2.1.269) in `CLAUDE.md:224-226`; this is version drift to record, not authority to install or upgrade.

### Physical protocols

- Codex: first send uses `codex exec ... --json --model <bound-model>`; later turns use `codex exec resume <thread_id>`. See `src/persistent-codex-session.ts:211`.
- Claude: persistent stream-JSON subprocess; restart continuity uses `--resume <session_id>`. See `src/persistent-session.ts:185`.
- AGY: `agy -p ... --output-format stream-json --conversation <conversation_id>`; empty replies are explicitly rejected. See `src/persistent-agy-session.ts:121`.
- Central engine selection is `SessionManager._createSession` at `src/session-manager.ts:3925`.

### Deterministic evidence boundary

- `src/__tests__/autoloop-durable-recovery-e2e.test.ts` uses an AGY-shaped executable fixture. It exercises subprocess and persistence behavior but consumes no AGY provider and is not live evidence.
- Existing tests cover reset/recreate, legacy reconstruction, crash-before-delivery, crash-after-delivery-before-ACK, reviewer-only recovery, generation fencing, delivery identity, and payload digests.
- `scripts/sweep.ts` performs one real turn through each wrapper, but it selects wrapper defaults and does not prove the complete Autoloop lifecycle or Reviewer delivery. It cannot solve the missing Claude/AGY Autoloop binding.

## Minimum protocol if bindings become available

For each engine, use one bounded lifecycle where possible:

1. Start/create with the exact bound model and record adapter/binary version.
2. Send a minimal nonce-bearing prompt; require a non-empty logical result and native thread/session/conversation id.
3. Resume using that native id and require continuity without repeating the nonce value in the second prompt.
4. Exercise Autoloop recovery inspect/apply with a stable token; verify inspect is byte-read-only and apply is idempotent.
5. Reset/recreate the relevant worker; verify the old generation is absent, reservation released, and a new generation owns the role.
6. Deliver a Reviewer request through durable intent/send/ACK; require matching delivery id and payload SHA-256, verdict before ACK, no Coder recreation in reviewer-only recovery, and no continuation run.

Destructive crash windows, empty/partial replies, stale generation/lease schedules, retry, and deduplication remain deterministic fixture tests. They should not be forced against live providers.

## Conflicting evidence and adjudication

- One Fan-out report classified live calls as belonging only to original Task 10. The current authenticated amendment explicitly moves minimum live engine validation into Task 9, so that historical plan ordering does not remove the live requirement.
- One report proposed Claude/AGY as `NOT_APPLICABLE` because the successor itself uses Codex. The amendment explicitly requires all three engines and says blocked/not-executed cannot count. The stricter and technically supported classification is `BLOCKED_BINDING`.
- Direct wrapper smokes prove transport/authentication only. They do not prove engine participation through Autoloop Reviewer delivery.

## Unresolved questions

1. Which immutable successor configuration authorizes an exact Claude model for the required Task 9 live boundary?
2. Does the owner intend AGY's Fan-out/Council binding to be duplicated into Autoloop validation? The current snapshot does not say so, and profile authority is non-transferable.
3. Once bindings exist, does current provider quota permit the minimum live calls? This requires a bounded live attempt; availability cannot be inferred read-only.

## Implementation implications

- Task 8 documentation can be reconciled from the preserved stash, but the current supervisor directive says not to dispatch while a governing conflict remains.
- Task 9 deterministic fixtures/gates may be planned, but terminal completion is impossible while Claude and AGY remain `BLOCKED_BINDING`.
- Do not apply stash `8ef468094d093d26b24b4e4c06902fd27cd8d3d1` wholesale.
- Do not change `council.yaml`, engine defaults, runtime, Gateway, or secrets.
- Keep the successor in planning and return this dossier to the same Planner.

## Provenance

- Canonical project root: `/home/openclaw/workspace/projects/claw-orchestrator-autoloop-durable-recovery`
- Instruction snapshot:
  - Global AGENTS: `d493facab0d036ad85f488464ab26a3cf5bc5355b21555664f1f0591054266f4`
  - RTK: `49c368c302c6f63d089f4c1085b242fc50fe22ce5bc34dada6478083000e7c6f`
  - Approved plan: `b8446ec85a0a6285db59cf6405583298fb099462a91304ca36302c876a27b328`
- Council snapshot: `2cb588ce434270b7db9ae24945699f36d1190785555e0d9c7a27d08ab4aa7db5`
- No instruction conflict or snapshot migration was detected.

## Read-only attestation

The research passes and controller verification made no tracked source/index/configuration/runtime/secret/external-state changes and executed no live provider calls. The dossier itself is the authorized project evidence artifact.
