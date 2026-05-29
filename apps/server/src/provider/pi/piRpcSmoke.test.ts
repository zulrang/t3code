// @effect-diagnostics nodeBuiltinImport:off
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { assert, describe } from "vitest";

import { parseGenericCliVersion } from "../providerSnapshot.ts";
import { makePiRpcClient } from "./piRpcClient.ts";

const PI_BINARY = process.env.PI_BINARY;

describe.skipIf(!PI_BINARY)("Pi RPC smoke (PI_BINARY)", () => {
  it.layer(NodeServices.layer)("live binary", (it) => {
    it.effect("reports version and discovers models via ephemeral RPC", () =>
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const cwd = yield* Effect.sync(() => fs.mkdtempSync(path.join(os.tmpdir(), "pi-smoke-")));

        const client = yield* makePiRpcClient({
          binaryPath: PI_BINARY!,
          cwd,
          noSession: true,
          requestTimeoutMs: 30_000,
        }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));

        const state = yield* client.getState();
        assert.equal(typeof state, "object");

        const models = yield* client.getAvailableModels();
        assert.ok(Array.isArray(models.models));

        yield* client.close();

        const versionStdout = yield* Effect.tryPromise(() =>
          import("node:child_process").then(
            ({ execFile }) =>
              new Promise<string>((resolve, reject) => {
                execFile(PI_BINARY!, ["--version"], (error, stdout) => {
                  if (error) {
                    reject(error);
                    return;
                  }
                  resolve(String(stdout));
                });
              }),
          ),
        );
        const versionHint = parseGenericCliVersion(versionStdout);
        assert.ok(versionHint === null || versionHint.length > 0);
        assert.ok(models.models.length >= 0);
      }).pipe(Effect.scoped),
    );
  });
});
