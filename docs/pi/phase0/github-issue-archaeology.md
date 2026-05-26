# Pi Phase 0 GitHub Issue Archaeology

Scope: read-only review of upstream issues that affect Pi/provider runtime behavior. This reinforces the Phase 0 design; it does not reopen locked choices unless a blocker appears.

## Issues Reviewed

| Issue                  | Signal for Pi plan                                                                                                                                                                                                                                                      |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pingdotgg/t3code#402` | Pi integration discussion explicitly raises `AgentSession` vs RPC. Comments note that `AgentSession` would move T3 toward embedding Pi desktop/runtime behavior. The v1 RPC subprocess choice is reinforced for isolation and architectural fit.                        |
| `#2157`                | OpenCode health probes and idle subprocesses stacked up, with reports of multiple `opencode` processes and high memory. Reinforces scoped cleanup, bounded probes, and no disabled-provider probing.                                                                    |
| `#2294`                | OpenCode background processes were repeatedly created and not killed when T3 closed. Comments included stop routing failures and restart spawning without killing old process. Reinforces one owned process scope per Pi thread and finalizer-backed cleanup.           |
| `#2248`                | OpenCode probe blocked cold start for 45-75s; comment mentioned symlinked config causing enormous `package-lock.json`. Reinforces non-blocking backend readiness and short probe timeouts.                                                                              |
| `#2240`                | Claude provider probe hang was found in search results as a stdin/timeout class of failure. Reinforces closing/owning stdio for probe processes and rejecting pending commands on exit.                                                                                 |
| `#2537`                | Windows users saw repeated `cmd.exe` / `conhost` flashes from provider/VCS process paths. Reinforces avoiding unnecessary `shell: true`, using hidden windows where available, and testing Windows spawn/kill paths.                                                    |
| `#2495`                | Windows Codex probe timeout plus packaged backend readiness timeout. Reinforces that Pi probes must not block server readiness and need actionable timeout diagnostics.                                                                                                 |
| `#2644`                | OpenCode replies were saved to `opencode.db` but T3 stayed `working...`; a commenter identified an SSE/event subscription returning immediately, so the terminal event never reached the renderer. Reinforces a Pi turn stall watchdog with `get_state` reconciliation. |
| `#917`                 | Proposal for recovering stuck running turns when `turn/completed` is lost; comment also found a client-side send-state race. Reinforces server-side deterministic terminal events and UI fresh-state recovery signals.                                                  |
| `#2173`                | Codex sessions completed locally but still showed `Working` in T3. Reinforces that provider-native completion is not enough; T3 must emit/ingest terminal lifecycle events reliably.                                                                                    |
| `#2336`                | Dangling `resume_cursor_json` at turn count 0 can make a Claude thread permanently unusable after CLI session death. Reinforces Pi stale-cursor policy: ignore stale non-null cursors, start fresh, warn visibly.                                                       |
| `#313`                 | Transient reconnect could orphan pending user turns and leave thread stuck in error. Reinforces stale extension UI request handling and terminal failure on disconnect/restart.                                                                                         |
| `#2614`                | Orphaned `t3 serve` processes remain after app sessions. Reinforces process ownership and shutdown cleanup tests beyond the provider-specific subprocess.                                                                                                               |

## Plan Assumptions Changed Or Reinforced

- **RPC subprocess vs `AgentSession`: reinforced.** Issue `#402` includes an explicit `AgentSession` suggestion, but also a maintainer-style concern that this becomes a Pi desktop/runtime embedding. Phase 0 should keep RPC v1 and revisit SDK only if measured RPC resource use becomes unacceptable.
- **Probe policy: strengthened.** Existing bugs show probes can block startup, flash windows, load expensive config, and leak child processes. Pi probes should be tiered, async, jittered, bounded, and scoped.
- **Turn watchdog: required.** OpenCode/Codex stuck-working issues show that a provider can complete while T3 misses terminal events. Pi must not rely only on ideal stream delivery; it needs silence timeout plus `get_state` reconciliation and terminal `turn.failed`/`turn.completed`.
- **Ephemeral resume UX: required.** The stale cursor and split/lost-context issues show that silent fresh starts are dangerous. Pi v1 should show a visible "fresh runtime" warning when recovery starts a new `--no-session` process.
- **Stale interaction requests: required.** Reconnect and pending-turn issues imply Pi extension UI requests must be cancelled/failed deterministically if the browser disconnects, the server restarts, the provider/session changes, or Pi exits.
- **Windows process hygiene: required.** Spawn/kill policy and hidden-window behavior should be explicit in Phase 2/7 tests, even though Phase 0 runs on Linux/WSL.

No issue reviewed blocks the locked v1 choices.
