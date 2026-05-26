# Pi Phase 0 RPC Client Comparison

Upstream references:

- `packages/coding-agent/src/modes/rpc/rpc-client.ts`
- `packages/coding-agent/src/modes/rpc/rpc-types.ts`
- `packages/coding-agent/docs/rpc.md`

## Upstream Client Shape

The upstream `RpcClient`:

- spawns the CLI in RPC mode
- uses JSONL stdout parsing through `attachJsonlLineReader`
- writes JSONL commands to stdin
- correlates responses by optional `id`
- treats non-response JSON lines as events
- stores pending requests in a `Map`
- rejects pending requests on process exit, process error, stdin error, or timeout
- exposes typed helpers for `prompt`, `abort`, `get_state`, `get_available_models`, `set_model`, `set_thinking_level`, session operations, bash, and commands
- provides `waitForIdle` / `collectEvents` helpers that wait for `agent_end`

Important limitations for T3:

- It is Promise/EventEmitter-style TypeScript, not Effect-native.
- It spawns `node dist/cli.js` by default; T3 needs to spawn the configured `pi` binary path directly.
- It has a fixed request timeout and does not integrate with T3 `Scope`, `ChildProcessSpawner`, logging, or provider instance environment.
- It does not own T3-specific raw event privacy/redaction, event mapping, or stale extension UI behavior.
- It ignores non-JSON stdout lines; T3 should choose whether to ignore, warn, or raw-log parse failures.

## Bespoke Effect-Native Client Shape

Recommended Phase 2 implementation: **bespoke Effect-native Pi RPC client**, using upstream `rpc-types.ts` and captured fixtures as compatibility references.

The T3 client should:

- spawn configured `binaryPath` as `pi --mode rpc --no-session`
- set `cwd` from the T3 thread workspace path
- merge `ProviderInstanceEnvironment`
- use `ChildProcessSpawner` and `Scope` finalizers
- parse stdout as strict LF-delimited JSONL, stripping optional trailing CR
- correlate command responses by `id`
- expose an event stream for non-response JSON objects
- reject pending commands on process exit, stop, timeout, malformed dependent responses, and broken stdin
- write `extension_ui_response` records to stdin
- carry `pi --version` in diagnostics/snapshot metadata
- raw-log unknown events under `pi.rpc.${string}` with privacy policy enforcement

## Wrap vs Bespoke

| Option                       | Pros                                                                                                                                                                                                                   | Cons                                                                                                                                                                                                                     |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Wrap upstream `RpcClient`    | Faster start; tracks Pi helper API; already covers id correlation and basic process exit rejection.                                                                                                                    | Poor Effect/Scope integration; spawns `node dist/cli.js` by default; harder to enforce T3 process hygiene, timeouts, native logging, redaction, and Windows spawn policy; less control over fixture-driven parser tests. |
| Bespoke Effect-native client | Fits T3 runtime architecture; precise lifecycle ownership; easy fixture tests for JSONL framing and process failures; can share patterns with Codex/OpenCode/Cursor; supports T3 raw logging and stale request policy. | Must track upstream protocol drift manually; more code to maintain; needs explicit type/fixture review on Pi upgrades.                                                                                                   |

## Recommendation

Use a bespoke Effect-native client for Phase 2.

Mitigate drift by:

- copying the upstream command/response/event surface into narrowly scoped local types
- adding fixture tests from observed `pi 0.72.1` output
- documenting the upstream `rpc-types.ts` revision reviewed
- tolerating unknown stream events by raw-logging and ignoring
- failing fast on unknown response shapes for commands T3 depends on
- re-running the Phase 0 probe when bumping supported Pi versions
