/**
 * PiDriver — `ProviderDriver` for the Pi CLI (`pi --mode rpc`) runtime.
 *
 * Phase 4 registers the driver, wires per-instance snapshot probes, and
 * stubs adapter/text-generation surfaces until Phase 5/7 land live turns
 * and RPC-backed text generation.
 *
 * @module provider/Drivers/PiDriver
 */
import {
  PiSettings,
  ProviderDriverKind,
  TextGenerationError,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { ServerConfig } from "../../config.ts";
import type { TextGenerationShape } from "../../textGeneration/TextGeneration.ts";
import { ProviderAdapterValidationError, ProviderDriverError } from "../Errors.ts";
import { makePiAdapter } from "../Layers/PiAdapter.ts";
import {
  checkPiProviderStatus,
  makePendingPiProvider,
  piSnapshotRefreshInterval,
} from "../Layers/PiProvider.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  type ProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  makeManualOnlyProviderMaintenanceCapabilities,
  normalizeCommandPath,
} from "../providerMaintenance.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const DRIVER_KIND = ProviderDriverKind.make("pi");
const LIVE_TURNS_ISSUE = "Pi live turns are not available until Phase 5 (PiAdapter).";
const TEXT_GENERATION_ISSUE =
  "Pi text generation is not available until Phase 7 (PiTextGeneration).";

export type PiDriverEnv =
  | ChildProcessSpawner.ChildProcessSpawner
  | ProviderEventLoggers
  | ServerConfig;

export function piContinuationIdentity(input: {
  readonly driverKind: ProviderDriverKind;
  readonly instanceId: ProviderInstance["instanceId"];
  readonly binaryPath: string;
}): ProviderContinuationIdentity {
  const normalizedBinaryPath = normalizeCommandPath(input.binaryPath);
  return {
    driverKind: input.driverKind,
    continuationKey: `${input.driverKind}:instance:${input.instanceId}:binary:${normalizedBinaryPath}`,
  };
}

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

const adapterValidationError = (operation: string) =>
  new ProviderAdapterValidationError({
    provider: DRIVER_KIND,
    operation,
    issue: LIVE_TURNS_ISSUE,
  });

export const makeStubPiAdapter = (): ProviderAdapterShape<ProviderAdapterValidationError> => ({
  provider: DRIVER_KIND,
  capabilities: { sessionModelSwitch: "in-session" },
  startSession: () => Effect.fail(adapterValidationError("startSession")),
  sendTurn: () => Effect.fail(adapterValidationError("sendTurn")),
  interruptTurn: () => Effect.fail(adapterValidationError("interruptTurn")),
  respondToRequest: () => Effect.fail(adapterValidationError("respondToRequest")),
  respondToUserInput: () => Effect.fail(adapterValidationError("respondToUserInput")),
  stopSession: () => Effect.fail(adapterValidationError("stopSession")),
  listSessions: () => Effect.succeed([]),
  hasSession: () => Effect.succeed(false),
  readThread: () => Effect.fail(adapterValidationError("readThread")),
  rollbackThread: () => Effect.fail(adapterValidationError("rollbackThread")),
  stopAll: () => Effect.void,
  streamEvents: Stream.never,
});

const textGenerationUnavailable = (
  operation:
    | "generateCommitMessage"
    | "generatePrContent"
    | "generateBranchName"
    | "generateThreadTitle",
) =>
  Effect.fail(
    new TextGenerationError({
      operation,
      detail: TEXT_GENERATION_ISSUE,
    }),
  );

export const makeStubPiTextGeneration = (): TextGenerationShape => ({
  generateCommitMessage: () => textGenerationUnavailable("generateCommitMessage"),
  generatePrContent: () => textGenerationUnavailable("generatePrContent"),
  generateBranchName: () => textGenerationUnavailable("generateBranchName"),
  generateThreadTitle: () => textGenerationUnavailable("generateThreadTitle"),
});

export const PiDriver: ProviderDriver<PiSettings, PiDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "Pi",
    supportsMultipleInstances: true,
  },
  configSchema: PiSettings,
  defaultConfig: (): PiSettings => decodePiSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const scope = yield* Scope.Scope;
      const serverConfig = yield* ServerConfig;
      const eventLoggers = yield* ProviderEventLoggers;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const effectiveConfig = { ...config, enabled } satisfies PiSettings;
      const continuationIdentity = piContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
        binaryPath: effectiveConfig.binaryPath,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const maintenanceCapabilities = makeManualOnlyProviderMaintenanceCapabilities({
        provider: DRIVER_KIND,
        packageName: null,
      });

      const checkProvider = checkPiProviderStatus(
        effectiveConfig,
        serverConfig.cwd,
        processEnv,
      ).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(Scope.Scope, scope),
      );

      const snapshot = yield* makeManagedServerProvider<PiSettings>({
        maintenanceCapabilities,
        getSettings: Effect.succeed(effectiveConfig),
        streamSettings: Stream.never,
        haveSettingsChanged: () => false,
        initialSnapshot: (settings) =>
          makePendingPiProvider(settings).pipe(Effect.map(stampIdentity)),
        checkProvider,
        refreshInterval: piSnapshotRefreshInterval(instanceId),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build Pi snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      const adapter = yield* makePiAdapter(effectiveConfig, {
        instanceId,
        environment: processEnv,
        ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
      }).pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration: makeStubPiTextGeneration(),
      } satisfies ProviderInstance;
    }),
};
