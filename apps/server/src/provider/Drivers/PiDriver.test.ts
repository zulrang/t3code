import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  ServerSettings,
  TextGenerationError,
  type PiSettings,
  type ProviderSendTurnInput,
  type ProviderSessionStartInput,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { assert, describe, expect } from "vitest";

import { ServerConfig } from "../../config.ts";
import { makeTextGenerationFromRegistry } from "../../textGeneration/TextGeneration.ts";
import type { ProviderInstance } from "../ProviderDriver.ts";
import type { ProviderInstanceRegistryShape } from "../Services/ProviderInstanceRegistry.ts";
import { BUILT_IN_DRIVERS } from "../builtInDrivers.ts";
import { deriveProviderInstanceConfigMap } from "../Layers/ProviderInstanceRegistryHydration.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { piSnapshotRefreshInterval } from "../Layers/PiProvider.ts";
import { makeProviderInstanceRegistry } from "../Layers/ProviderInstanceRegistryLive.ts";
import {
  makeStubPiAdapter,
  makeStubPiTextGeneration,
  PiDriver,
  piContinuationIdentity,
} from "./PiDriver.ts";

const decodePiSettings = Schema.decodeSync(PiDriver.configSchema);
const decodeServerSettings = Schema.decodeSync(ServerSettings);

const makePiConfig = (overrides?: Partial<PiSettings>): PiSettings =>
  decodePiSettings({
    enabled: false,
    binaryPath: "pi",
    customModels: [],
    ...overrides,
  });

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "pi-driver-test",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
);

const makeStubRegistry = (
  instances: ReadonlyArray<ProviderInstance>,
): ProviderInstanceRegistryShape => {
  const byId = new Map(instances.map((instance) => [instance.instanceId, instance] as const));
  return {
    getInstance: (id) => Effect.succeed(byId.get(id)),
    listInstances: Effect.succeed(instances),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.die("subscribeChanges stub not configured"),
  };
};

describe("PiDriver", () => {
  it("is registered in BUILT_IN_DRIVERS", () => {
    expect(BUILT_IN_DRIVERS.map((driver) => driver.driverKind)).toContain("pi");
    expect(BUILT_IN_DRIVERS.find((driver) => driver.driverKind === "pi")).toBe(PiDriver);
  });

  it("defaultConfig decodes to disabled Pi settings", () => {
    expect(PiDriver.defaultConfig()).toEqual({
      enabled: false,
      binaryPath: "pi",
      customModels: [],
    });
  });

  it("derives continuation identity from driver, instance, and normalized binary path", () => {
    const identity = piContinuationIdentity({
      driverKind: ProviderDriverKind.make("pi"),
      instanceId: ProviderInstanceId.make("pi_work"),
      binaryPath: String.raw`C:\Tools\pi.exe`,
    });
    expect(identity.continuationKey).toBe("pi:instance:pi_work:binary:c:/tools/pi.exe");
  });

  it("refresh interval is jittered per instance id", () => {
    const first = piSnapshotRefreshInterval("pi");
    const second = piSnapshotRefreshInterval("pi_alt");
    expect(Duration.toMillis(first)).not.toBe(Duration.toMillis(second));
  });

  it("hydrates legacy providers.pi into the default pi instance", () => {
    const settings = decodeServerSettings({
      providers: {
        pi: { enabled: true, binaryPath: "/opt/pi/bin/pi" },
      },
    });
    const piInstanceId = ProviderInstanceId.make("pi");
    const configMap = deriveProviderInstanceConfigMap(settings);
    expect(configMap[piInstanceId]).toEqual({
      driver: ProviderDriverKind.make("pi"),
      config: {
        enabled: true,
        binaryPath: "/opt/pi/bin/pi",
        customModels: [],
      },
    });
  });

  it("stub adapter rejects live turn/session operations with a clear validation error", () =>
    Effect.gen(function* () {
      const adapter = makeStubPiAdapter();
      const sendTurnResult = yield* adapter
        .sendTurn({} as ProviderSendTurnInput)
        .pipe(Effect.result);
      expect(Result.isFailure(sendTurnResult)).toBe(true);
      if (Result.isFailure(sendTurnResult)) {
        expect(sendTurnResult.failure._tag).toBe("ProviderAdapterValidationError");
        expect(sendTurnResult.failure.operation).toBe("sendTurn");
        expect(sendTurnResult.failure.issue).toMatch(/Phase 5/i);
      }

      const startSessionResult = yield* adapter
        .startSession({} as ProviderSessionStartInput)
        .pipe(Effect.result);
      expect(Result.isFailure(startSessionResult)).toBe(true);
      if (Result.isFailure(startSessionResult)) {
        expect(startSessionResult.failure.operation).toBe("startSession");
      }
    }));

  it("stub text generation fails with TextGenerationError", () =>
    Effect.gen(function* () {
      const textGeneration = makeStubPiTextGeneration();
      const result = yield* textGeneration
        .generateThreadTitle({
          cwd: process.cwd(),
          message: "hello",
          modelSelection: createModelSelection(ProviderInstanceId.make("pi"), "openai/gpt-4"),
        })
        .pipe(Effect.result);

      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.failure).toBeInstanceOf(TextGenerationError);
        expect(result.failure.operation).toBe("generateThreadTitle");
        expect(result.failure.detail).toMatch(/stub/i);
      }
    }));

  it.effect("registry routing resolves Pi text generation by instance id", () =>
    Effect.gen(function* () {
      const piId = ProviderInstanceId.make("pi");
      const piInstance = yield* PiDriver.create({
        instanceId: piId,
        displayName: "Pi",
        environment: [],
        enabled: false,
        config: makePiConfig({ binaryPath: "__missing_pi_binary__" }),
      }).pipe(Effect.provide(testLayer), Effect.scoped);

      const tg = makeTextGenerationFromRegistry(makeStubRegistry([piInstance]));
      const result = yield* tg
        .generateBranchName({
          cwd: process.cwd(),
          message: "Add Pi routing test",
          modelSelection: createModelSelection(piId, "openai%2Fgpt-4"),
        })
        .pipe(Effect.result);

      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.failure._tag).toBe("TextGenerationError");
        expect(result.failure.detail).not.toMatch(/Phase 7/i);
      }
    }),
  );

  it.live("materializes a disabled instance without unavailable downgrade", () =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("pi_disabled");
      const { registry } = yield* makeProviderInstanceRegistry({
        drivers: [PiDriver],
        configMap: {
          [instanceId]: {
            driver: ProviderDriverKind.make("pi"),
            enabled: false,
            config: makePiConfig({ enabled: false }),
          },
        },
      });

      const instance = yield* registry.getInstance(instanceId);
      assert(instance !== undefined);
      expect(instance.driverKind).toBe("pi");
      expect(instance.continuationIdentity.continuationKey).toBe(
        "pi:instance:pi_disabled:binary:pi",
      );

      const snapshot = yield* instance.snapshot.getSnapshot;
      expect(snapshot.status).toBe("disabled");
      expect(snapshot.message).toBe("Pi is disabled in T3 Code settings.");

      const unavailable = yield* registry.listUnavailable;
      expect(unavailable).toEqual([]);
    }).pipe(Effect.provide(testLayer)),
  );

  it.live("uses pending or error snapshot messaging for enabled instances", () =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("pi_enabled");
      const { registry } = yield* makeProviderInstanceRegistry({
        drivers: [PiDriver],
        configMap: {
          [instanceId]: {
            driver: ProviderDriverKind.make("pi"),
            enabled: true,
            config: makePiConfig({
              enabled: true,
              binaryPath: "__missing_pi_binary__",
            }),
          },
        },
      });

      const instance = yield* registry.getInstance(instanceId);
      assert(instance !== undefined);
      const snapshot = yield* instance.snapshot.getSnapshot;
      expect(snapshot.enabled).toBe(true);
      expect(snapshot.status === "warning" || snapshot.status === "error").toBe(true);
      expect(snapshot.message).toBeTruthy();
    }).pipe(Effect.provide(testLayer)),
  );
});
