// @effect-diagnostics nodeBuiltinImport:off
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { assert, describe } from "vitest";

import { makePiRpcClient } from "./piRpcClient.ts";

const PI_BINARY = process.env.PI_BINARY;

describe.skipIf(!PI_BINARY)("makePiRpcClient integration (PI_BINARY)", () => {
  it.layer(NodeServices.layer)("live binary", (it) => {
    it.effect("sends get_state then get_available_models on one RPC session", () =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const cwd = yield* Effect.sync(() => fs.mkdtempSync(path.join(os.tmpdir(), "pi-rpc-int-")));

        const client = yield* makePiRpcClient({
          binaryPath: PI_BINARY!,
          cwd,
          noSession: true,
          requestTimeoutMs: 30_000,
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

        const state = yield* client.getState();
        assert.equal(typeof state, "object");
        if (state.model !== undefined) {
          assert.equal(typeof state.model.provider, "string");
          assert.equal(typeof state.model.id, "string");
        }

        const models = yield* client.getAvailableModels();
        assert.ok(Array.isArray(models.models));
        if (models.models.length > 0) {
          assert.equal(typeof models.models[0]?.provider, "string");
          assert.equal(typeof models.models[0]?.id, "string");
        }

        yield* client.close();
      }).pipe(Effect.scoped),
    );
  });
});
