# Slice 3 native CLI fixtures

These are four separate deterministic implementations of the protocol consumed
by the current production adapters. They do not launch providers or call an API.

| Fixture      | Parser and authentic adapter test provenance                                                                 | Native continuity                                                                       |
| ------------ | ------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `codex.mjs`  | `src/persistent-codex-session.ts` `_run`; `src/__tests__/codex-session.test.ts` `runTurn`                    | `thread.started`, `item.completed`, `turn.completed`; `exec resume <thread>`            |
| `claude.mjs` | `src/persistent-session.ts` event handler and `_waitForResponse`; `src/__tests__/persistent-session.test.ts` | `system/init`, replayed `user`, `assistant/result`; repeated stdin turns in one process |
| `agy.mjs`    | `src/persistent-agy-session.ts` `_run`; `src/__tests__/agy-session.test.ts` and `agy-planner-e2e.test.ts`    | `init/conversation_id`, nested `result`; `--conversation`                               |
| `cursor.mjs` | `src/persistent-cursor-session.ts` `_run` and `_handleStreamEvent`; `src/__tests__/cursor-session.test.ts`   | `system/session_id`, `assistant/result`; `--resume`                                     |

Requested models are Codex `gpt-6-astra`, Claude `haiku` through the production
alias resolver (`claude-haiku-4-5`), AGY `gemini-3.8-flash-high`, and Cursor `auto`.
Execution records retain the requested model and fixture hash. `native.jsonl`
contains actual argv/stdin; `protocol.jsonl` contains emitted structured events.
These observations do not claim provider-effective model identity.

Modes exercise success, empty response, partial stream, malformed stream,
nonzero process exit, and terminal failure/denial. AGY additionally exercises
empty STOPPED soft denial. Cursor captures the actual isolated CLI permission
configuration and working directory used for read-only Planner turns.

`trustNativeResponse` in the allowed test helper is shared **external receiver**
behavior. It records recipient bytes, maintains a fixture-owned idempotency
store, and emits native text containing Coder/Reviewer controls. It never writes
an Autoloop intent, generation, ACK, recovery receipt, or verdict. Those are
produced by the actual SessionManager, dispatcher, runner, and secure ledger.
The receiver idempotency store demonstrates the explicit external deduplication
assumption; it is not evidence that a live vendor CLI deduplicates tool effects.

Crash tests witness and SIGKILL the entire owned subprocess group after durable
intent fsync or after recipient capture, then reconstruct with a fresh process
and retained files. An injected cold clock crosses the activity lease; it does
not override PID liveness. Reviewer fixtures use isolated shared clones of a
real immutable project checkpoint and its exact Git diff. No project commit or
index operation is performed. Test scratch, raw streams, observations, and
fixture Git repositories remain under the run's ignored candidate artifacts.

Observed parser corrections reject Codex streams missing `turn.completed`, AGY
streams with init but no terminal result and nonempty partial output, and Cursor
structured streams missing/denying their terminal result. Legacy unstructured
AGY/Cursor output compatibility is retained; existing native tests cover it.

G6/G7 collector modes and live-provider capability gates remain pending Task 9
work. These fixture tests are not acceptance receipts for those gates.
