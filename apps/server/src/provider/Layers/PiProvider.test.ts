// @effect-diagnostics nodeBuiltinImport:off
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { PiSettings } from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { assert, describe } from "vitest";

import {
  checkPiProviderStatus,
  makePendingPiProvider,
  PI_PROBE_RPC_CONCURRENCY,
  PiProbeError,
  piSnapshotRefreshInterval,
  probePiRpcDiscovery,
} from "./PiProvider.ts";
import { PiRpcClientSpawnError, type PiRpcClientOptions } from "../pi/piRpcClient.ts";
import { PiRpcAvailableModels } from "../pi/piRpcTypes.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturesDir = path.join(__dirname, "../../../../../docs/pi/phase0/fixtures");
const discoveryFixture = JSON.parse(
  fs.readFileSync(path.join(fixturesDir, "model-discovery.json"), "utf8"),
) as { data: PiRpcAvailableModels };

const makePiSettings = (overrides?: Partial<PiSettings>): PiSettings =>
  decodePiSettings({
    enabled: true,
    binaryPath: "pi",
    customModels: [],
    ...overrides,
  });

const binaryVersionOk = () =>
  Effect.succeed({
    stdout: "pi 0.72.1\n",
    stderr: "",
    code: 0,
  });

const testLayer = NodeServices.layer;

describe("PiProvider", () => {
  it.layer(testLayer)("makePendingPiProvider", (it) => {
    it.effect("returns disabled warning without probing", () =>
      Effect.gen(function* () {
        const snapshot = yield* makePendingPiProvider(makePiSettings({ enabled: false }));
        assert.equal(snapshot.status, "disabled");
        assert.equal(snapshot.message, "Pi is disabled in T3 Code settings.");
        assert.deepEqual(snapshot.models, []);
      }),
    );

    it.effect("returns unchecked warning when enabled", () =>
      Effect.gen(function* () {
        const snapshot = yield* makePendingPiProvider(makePiSettings({ enabled: true }));
        assert.equal(snapshot.status, "warning");
        assert.match(snapshot.message ?? "", /not been checked/i);
      }),
    );
  });

  it.layer(testLayer)("checkPiProviderStatus", (it) => {
    it.effect("does not probe RPC when disabled", () =>
      Effect.gen(function* () {
        let probeCalls = 0;
        const snapshot = yield* checkPiProviderStatus(
          makePiSettings({ enabled: false }),
          process.cwd(),
          process.env,
          () => {
            probeCalls += 1;
            return Effect.succeed({ models: { models: [] }, slashCommands: [] });
          },
        );

        assert.equal(probeCalls, 0);
        assert.equal(snapshot.status, "disabled");
      }),
    );

    it.effect("reports unavailable when binary is missing", () =>
      Effect.gen(function* () {
        const snapshot = yield* checkPiProviderStatus(
          makePiSettings({ binaryPath: "__missing_pi_binary__" }),
          process.cwd(),
          process.env,
          () => Effect.die("probe should not run"),
        );

        assert.equal(snapshot.status, "error");
        assert.equal(snapshot.installed, false);
        assert.match(snapshot.message ?? "", /not installed/i);
        assert.deepEqual(snapshot.models, []);
      }),
    );

    it.effect("reports spawn failure as error", () =>
      Effect.gen(function* () {
        const snapshot = yield* checkPiProviderStatus(
          makePiSettings(),
          process.cwd(),
          process.env,
          () =>
            Effect.fail(
              new PiRpcClientSpawnError({
                command: "pi --mode rpc --no-session",
              }),
            ),
          binaryVersionOk,
        );

        assert.equal(snapshot.status, "error");
        assert.equal(snapshot.installed, true);
        assert.match(snapshot.message ?? "", /spawn/i);
      }),
    );

    it.effect("reports liveness failure as error", () =>
      Effect.gen(function* () {
        const snapshot = yield* checkPiProviderStatus(
          makePiSettings(),
          process.cwd(),
          process.env,
          () =>
            Effect.fail(
              new PiProbeError({
                detail: "get_state failed",
              }),
            ),
          binaryVersionOk,
        );

        assert.equal(snapshot.status, "error");
        assert.match(snapshot.message ?? "", /get_state failed/i);
      }),
    );

    it.effect("maps discovery success to ready models with slugs and thinking", () =>
      Effect.gen(function* () {
        const snapshot = yield* checkPiProviderStatus(
          makePiSettings(),
          process.cwd(),
          process.env,
          () => Effect.succeed({ models: discoveryFixture.data, slashCommands: [] }),
          binaryVersionOk,
        );

        assert.equal(snapshot.status, "ready");
        assert.equal(snapshot.models.length, 3);
        const mini = snapshot.models.find((model) => model.slug === "openai-codex/gpt-5.4-mini");
        assert.ok(mini);
        const thinking = mini.capabilities?.optionDescriptors?.find(
          (descriptor) => descriptor.id === "thinkingLevel",
        );
        assert.ok(thinking && thinking.type === "select");
        assert.deepEqual(
          thinking.options.map((option) => option.id),
          ["minimal", "xhigh"],
        );
      }),
    );

    it.effect("returns warning without fake models when discovery is empty", () =>
      Effect.gen(function* () {
        const snapshot = yield* checkPiProviderStatus(
          makePiSettings(),
          process.cwd(),
          process.env,
          () => Effect.succeed({ models: { models: [] }, slashCommands: [] }),
          binaryVersionOk,
        );

        assert.equal(snapshot.status, "warning");
        assert.deepEqual(snapshot.models, []);
        assert.match(snapshot.message ?? "", /no available models/i);
      }),
    );

    it.effect("maps Pi RPC commands into slashCommands on the provider snapshot", () =>
      Effect.gen(function* () {
        const snapshot = yield* checkPiProviderStatus(
          makePiSettings(),
          process.cwd(),
          process.env,
          () =>
            Effect.succeed({
              models: discoveryFixture.data,
              slashCommands: [
                { name: "reload", description: "Reload extensions" },
                { name: "rpc-input" },
              ],
            }),
          binaryVersionOk,
        );

        assert.deepEqual(snapshot.slashCommands, [
          { name: "reload", description: "Reload extensions" },
          { name: "rpc-input" },
        ]);
      }),
    );

    it.effect("forwards provider environment to the default RPC probe", () =>
      Effect.gen(function* () {
        let capturedEnvironment: NodeJS.ProcessEnv | undefined;
        const customEnv = { ...process.env, PI_PROBE_ENV: "probe-token" };

        yield* checkPiProviderStatus(
          makePiSettings(),
          process.cwd(),
          customEnv,
          (input) => {
            capturedEnvironment = input.environment;
            return Effect.succeed({ models: discoveryFixture.data, slashCommands: [] });
          },
          binaryVersionOk,
        );

        assert.deepEqual(capturedEnvironment, customEnv);
      }),
    );

    it.effect("fetches slash commands during RPC discovery", () =>
      Effect.gen(function* () {
        let getCommandsCalls = 0;
        const discovery = yield* probePiRpcDiscovery({
          binaryPath: "pi",
          cwd: process.cwd(),
          makeClient: () =>
            Effect.succeed({
              getState: () => Effect.succeed({}),
              getAvailableModels: () => Effect.succeed(discoveryFixture.data),
              getCommands: () => {
                getCommandsCalls += 1;
                return Effect.succeed({
                  commands: [{ name: "reload", description: "Reload extensions" }],
                });
              },
              setModel: () => Effect.die("not used"),
              setThinkingLevel: () => Effect.void,
              prompt: () => Effect.void,
              abort: () => Effect.void,
              sendExtensionUiResponse: () => Effect.void,
              close: () => Effect.void,
              piVersion: undefined,
              streamEvents: Stream.empty,
              stderrLines: Stream.empty,
            }),
        });

        assert.equal(getCommandsCalls, 1);
        assert.deepEqual(discovery.slashCommands, [
          { name: "reload", description: "Reload extensions" },
        ]);
      }),
    );

    it.effect("passes probe environment through to Pi RPC client spawn", () =>
      Effect.gen(function* () {
        let capturedOptions: PiRpcClientOptions | undefined;
        const customEnv = { ...process.env, PI_PROBE_ENV: "probe-token" };

        yield* probePiRpcDiscovery({
          binaryPath: "pi",
          cwd: process.cwd(),
          environment: customEnv,
          makeClient: (options) => {
            capturedOptions = options;
            return Effect.succeed({
              getState: () => Effect.succeed({}),
              getAvailableModels: () => Effect.succeed({ models: [] }),
              getCommands: () => Effect.succeed({ commands: [] }),
              setModel: () => Effect.die("not used"),
              setThinkingLevel: () => Effect.void,
              prompt: () => Effect.void,
              abort: () => Effect.void,
              sendExtensionUiResponse: () => Effect.void,
              close: () => Effect.void,
              piVersion: undefined,
              streamEvents: Stream.empty,
              stderrLines: Stream.empty,
            });
          },
        });

        assert.deepEqual(capturedOptions?.spawnEnv, customEnv);
      }),
    );

    it.effect("limits concurrent probe RPC spawns", () =>
      Effect.gen(function* () {
        const active = yield* Ref.make(0);
        const maxActive = yield* Ref.make(0);
        const gate = yield* Deferred.make<void, never>();

        const probe = () =>
          Effect.gen(function* () {
            const next = yield* Ref.updateAndGet(active, (count) => count + 1);
            yield* Ref.update(maxActive, (current) => Math.max(current, next));
            yield* Deferred.await(gate);
            yield* Ref.update(active, (count) => count - 1);
            return { models: discoveryFixture.data, slashCommands: [] };
          });

        const fiber = yield* Effect.forkScoped(
          Effect.forEach(
            Array.from({ length: PI_PROBE_RPC_CONCURRENCY + 2 }, () =>
              checkPiProviderStatus(
                makePiSettings(),
                process.cwd(),
                process.env,
                probe,
                binaryVersionOk,
              ),
            ),
            (effect) => effect,
            { concurrency: "unbounded", discard: true },
          ),
        );

        yield* Effect.yieldNow;
        yield* Effect.yieldNow;
        const observedMax = yield* Ref.get(maxActive);
        assert.ok(
          observedMax <= PI_PROBE_RPC_CONCURRENCY,
          `expected max concurrency ${PI_PROBE_RPC_CONCURRENCY}, observed ${observedMax}`,
        );
        yield* Deferred.succeed(gate, undefined);
        yield* Fiber.join(fiber);
      }),
    );
  });

  it("adds deterministic jitter to refresh interval", () => {
    const first = piSnapshotRefreshInterval("pi");
    const second = piSnapshotRefreshInterval("pi-work");
    assert.ok(Duration.toMillis(first) >= Duration.toMillis(Duration.minutes(5)));
    assert.ok(Duration.toMillis(second) >= Duration.toMillis(Duration.minutes(5)));
    assert.notEqual(Duration.toMillis(first), Duration.toMillis(second));
  });
});
