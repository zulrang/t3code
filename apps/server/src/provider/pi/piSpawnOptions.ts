import type * as Duration from "effect/Duration";
import type { CommandOptions } from "effect/unstable/process/ChildProcess";

const DEFAULT_FORCE_KILL_AFTER = "2 seconds" as const;

/**
 * Shared Pi CLI subprocess spawn options.
 *
 * v1 resource limits (see also `PiProvider` probe concurrency):
 * - One active turn per Pi thread RPC process (adapter command lock).
 * - Bounded probe/discovery RPC concurrency (`PI_PROBE_RPC_CONCURRENCY`).
 * - Per-command RPC timeouts in `piRpcProtocol`.
 *
 * v1 does **not** enforce a global semaphore across all Pi provider instances.
 */
export function piChildProcessSpawnOptions(input: {
  readonly cwd: string;
  readonly env: NodeJS.ProcessEnv;
  readonly forceKillAfter?: Duration.Input;
}): CommandOptions {
  return {
    cwd: input.cwd,
    env: input.env,
    forceKillAfter: input.forceKillAfter ?? DEFAULT_FORCE_KILL_AFTER,
    // Keep stdin open across multiple JSONL RPC commands (default endOnDone closes
    // stdin after the first write and tears down the Pi process).
    stdin: { stream: "pipe", endOnDone: false },
    // Match sibling providers: Windows needs shell for `.cmd` shims. Effect's
    // ChildProcess API does not expose `windowsHide`; rely on scoped kill cleanup.
    shell: process.platform === "win32",
  };
}
