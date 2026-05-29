import {
  ProviderDriverKind,
  type PiSettings,
  type ModelCapabilities,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { createModelCapabilities } from "@t3tools/shared/model";
import {
  makePiRpcClient,
  PiRpcClientSpawnError,
  type PiRpcClientShape,
} from "../pi/piRpcClient.ts";
import { piChildProcessSpawnOptions } from "../pi/piSpawnOptions.ts";
import { mapPiRpcModelsToServerModels } from "../pi/piModelMapping.ts";
import type { PiRpcAvailableModels } from "../pi/piRpcTypes.ts";
import {
  AUTH_PROBE_TIMEOUT_MS,
  DEFAULT_TIMEOUT_MS,
  buildServerProvider,
  detailFromResult,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

export class PiProbeError extends Data.TaggedError("PiProbeError")<{
  readonly detail: string;
  readonly cause?: unknown;
}> {}

const isPiRpcClientSpawnError = Schema.is(PiRpcClientSpawnError);

const isPiProbeError = (error: unknown): error is PiProbeError =>
  typeof error === "object" &&
  error !== null &&
  "_tag" in error &&
  (error as { readonly _tag: string })._tag === "PiProbeError";

const PROVIDER = ProviderDriverKind.make("pi");
const PI_PRESENTATION = {
  displayName: "Pi",
  showInteractionModeToggle: false,
} as const;

export const PI_SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);
export const PI_SNAPSHOT_REFRESH_JITTER_MS = 30_000;
export const PI_PROBE_REQUEST_TIMEOUT_MS = 8_000;
export const PI_PROBE_RPC_CONCURRENCY = 2;

/** v1 does not cap total Pi RPC processes across all enabled instances globally. */
export const PI_V1_NO_GLOBAL_PROCESS_SEMAPHORE = true as const;

const DEFAULT_PI_MODEL_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

const ZERO_MODELS_MESSAGE =
  "Pi reported no available models. Configure Pi provider authentication and ensure at least one upstream provider is connected in Pi.";

export interface PiRpcProbeResult {
  readonly version: string | null;
  readonly models: ReadonlyArray<ServerProviderModel>;
}

function hashString(input: string): number {
  let hash = 0;
  for (let index = 0; index < input.length; index += 1) {
    hash = (hash * 31 + input.charCodeAt(index)) | 0;
  }
  return Math.abs(hash);
}

export function piSnapshotRefreshInterval(instanceId: string): Duration.Duration {
  const jitterMs = hashString(instanceId) % (PI_SNAPSHOT_REFRESH_JITTER_MS + 1);
  return Duration.sum(PI_SNAPSHOT_REFRESH_INTERVAL, Duration.millis(jitterMs));
}

let cachedProbeRpcSemaphore: Semaphore.Semaphore | undefined;

const withProbeRpcPermit = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.gen(function* () {
    if (cachedProbeRpcSemaphore === undefined) {
      cachedProbeRpcSemaphore = yield* Semaphore.make(PI_PROBE_RPC_CONCURRENCY);
    }
    return yield* cachedProbeRpcSemaphore.withPermits(1)(effect);
  });

function normalizeProbeDetail(error: unknown): string {
  if (isPiRpcClientSpawnError(error)) {
    return error.message;
  }
  if (isPiProbeError(error)) {
    return error.detail;
  }
  if (error instanceof Error) {
    const trimmed = error.message.trim();
    if (
      trimmed.length > 0 &&
      trimmed !== "An error occurred in Effect.tryPromise" &&
      trimmed !== "An error occurred in Effect.try"
    ) {
      return trimmed;
    }
  }
  return "Pi RPC probe failed.";
}

function formatBinaryProbeFailure(error: unknown): {
  readonly installed: boolean;
  readonly message: string;
} {
  if (isCommandMissingCause(error instanceof Error ? error : { message: String(error) })) {
    return {
      installed: false,
      message: "Pi CLI (`pi`) is not installed or not on PATH.",
    };
  }

  const detail = error instanceof Error ? error.message.trim() : String(error);
  return {
    installed: true,
    message: detail
      ? `Failed to execute Pi CLI health check: ${detail}`
      : "Failed to execute Pi CLI health check.",
  };
}

function formatRpcProbeFailure(error: unknown): {
  readonly installed: boolean;
  readonly message: string;
} {
  if (isPiRpcClientSpawnError(error)) {
    const lower = error.message.toLowerCase();
    if (lower.includes("enoent") || lower.includes("not found")) {
      return {
        installed: false,
        message: "Pi CLI (`pi`) is not installed or not on PATH.",
      };
    }
    return {
      installed: true,
      message: `Failed to spawn Pi RPC process: ${normalizeProbeDetail(error)}`,
    };
  }

  return {
    installed: true,
    message: normalizeProbeDetail(error),
  };
}

const runPiBinaryVersionCheck = Effect.fn("runPiBinaryVersionCheck")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const command = ChildProcess.make(
    piSettings.binaryPath,
    ["--version"],
    piChildProcessSpawnOptions({
      cwd: process.cwd(),
      env: environment,
    }),
  );
  return yield* spawnAndCollect(piSettings.binaryPath, command);
});

export const probePiRpcDiscovery = Effect.fn("probePiRpcDiscovery")(function* (input: {
  readonly binaryPath: string;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly requestTimeoutMs?: number;
  readonly makeClient?: (
    options: Parameters<typeof makePiRpcClient>[0],
  ) => Effect.Effect<
    PiRpcClientShape,
    PiRpcClientSpawnError,
    ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
  >;
}) {
  const makeClient = input.makeClient ?? makePiRpcClient;
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const client = yield* makeClient({
        binaryPath: input.binaryPath,
        cwd: input.cwd,
        noSession: true,
        requestTimeoutMs: input.requestTimeoutMs ?? PI_PROBE_REQUEST_TIMEOUT_MS,
        ...(input.environment ? { spawnEnv: input.environment } : {}),
      });
      yield* client.getState().pipe(
        Effect.mapError(
          (cause) =>
            new PiProbeError({
              detail: normalizeProbeDetail(cause),
              cause,
            }),
        ),
      );
      const discovery = yield* client.getAvailableModels().pipe(
        Effect.mapError(
          (cause) =>
            new PiProbeError({
              detail: normalizeProbeDetail(cause),
              cause,
            }),
        ),
      );
      yield* client.close();
      return discovery;
    }),
  );
});

const defaultProbePiRpc = (input: {
  readonly piSettings: PiSettings;
  readonly cwd: string;
  readonly environment?: NodeJS.ProcessEnv;
  readonly makeClient?: (
    options: Parameters<typeof makePiRpcClient>[0],
  ) => Effect.Effect<
    PiRpcClientShape,
    PiRpcClientSpawnError,
    ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
  >;
}) =>
  probePiRpcDiscovery({
    binaryPath: input.piSettings.binaryPath,
    cwd: input.cwd,
    ...(input.environment ? { environment: input.environment } : {}),
    ...(input.makeClient ? { makeClient: input.makeClient } : {}),
  });

export const makePendingPiProvider = (piSettings: PiSettings): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = providerModelsFromSettings(
      [],
      PROVIDER,
      piSettings.customModels,
      DEFAULT_PI_MODEL_CAPABILITIES,
    );

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi provider status has not been checked in this session yet.",
      },
    });
  });

function mapDiscoveryToSnapshot(input: {
  readonly piSettings: PiSettings;
  readonly checkedAt: string;
  readonly version: string | null;
  readonly discovery: PiRpcAvailableModels;
}): ServerProviderDraft {
  const discoveredModels = mapPiRpcModelsToServerModels(input.discovery.models);
  const models = providerModelsFromSettings(
    discoveredModels,
    PROVIDER,
    input.piSettings.customModels,
    DEFAULT_PI_MODEL_CAPABILITIES,
  );

  if (discoveredModels.length === 0) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: input.piSettings.enabled,
      checkedAt: input.checkedAt,
      models,
      probe: {
        installed: true,
        version: input.version,
        status: "warning",
        auth: { status: "unknown" },
        message: ZERO_MODELS_MESSAGE,
      },
    });
  }

  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: input.piSettings.enabled,
    checkedAt: input.checkedAt,
    models,
    probe: {
      installed: true,
      version: input.version,
      status: "ready",
      auth: { status: "unknown" },
      message: `${discoveredModels.length} model${discoveredModels.length === 1 ? "" : "s"} discovered from Pi.`,
    },
  });
}

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  cwd: string,
  environment: NodeJS.ProcessEnv = process.env,
  probePi: (input: {
    readonly piSettings: PiSettings;
    readonly cwd: string;
    readonly environment?: NodeJS.ProcessEnv;
    readonly makeClient?: (
      options: Parameters<typeof makePiRpcClient>[0],
    ) => Effect.Effect<
      PiRpcClientShape,
      PiRpcClientSpawnError,
      ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
    >;
  }) => Effect.Effect<
    PiRpcAvailableModels,
    PiProbeError | PiRpcClientSpawnError,
    ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
  > = defaultProbePiRpc,
  runBinaryCheck: typeof runPiBinaryVersionCheck = runPiBinaryVersionCheck,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const emptyModels = providerModelsFromSettings(
    [],
    PROVIDER,
    piSettings.customModels,
    DEFAULT_PI_MODEL_CAPABILITIES,
  );

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: emptyModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionExit = yield* Effect.exit(
    runBinaryCheck(piSettings, environment).pipe(
      Effect.timeoutOption(Duration.millis(DEFAULT_TIMEOUT_MS)),
    ),
  );
  if (versionExit._tag === "Failure") {
    const failure = formatBinaryProbeFailure(Cause.squash(versionExit.cause));
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: emptyModels,
      probe: {
        installed: failure.installed,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: failure.message,
      },
    });
  }

  const versionResult = versionExit.value;
  if (Option.isNone(versionResult)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: emptyModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Timed out while checking Pi CLI availability.",
      },
    });
  }

  const versionDetail = detailFromResult(versionResult.value);
  if (versionResult.value.code !== 0) {
    const failure = formatBinaryProbeFailure(
      new Error(versionDetail ?? `Pi --version exited with code ${versionResult.value.code}.`),
    );
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: emptyModels,
      probe: {
        installed: failure.installed,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: failure.message,
      },
    });
  }

  const version = parseGenericCliVersion(versionResult.value.stdout) ?? null;

  const discoveryExit = yield* Effect.exit(
    withProbeRpcPermit(
      probePi({ piSettings, cwd, environment }).pipe(
        Effect.timeoutOption(Duration.millis(AUTH_PROBE_TIMEOUT_MS)),
      ),
    ),
  );
  if (discoveryExit._tag === "Failure") {
    const failure = formatRpcProbeFailure(Cause.squash(discoveryExit.cause));
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: emptyModels,
      probe: {
        installed: failure.installed,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: failure.message,
      },
    });
  }

  const discoveryResult = discoveryExit.value;
  if (Option.isNone(discoveryResult)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: emptyModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Timed out while discovering Pi models.",
      },
    });
  }

  return mapDiscoveryToSnapshot({
    piSettings,
    checkedAt,
    version,
    discovery: discoveryResult.value,
  });
});
