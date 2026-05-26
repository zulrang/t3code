// @effect-diagnostics nodeBuiltinImport:off
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Duration from "effect/Duration";
import * as Stream from "effect/Stream";
import { it } from "@effect/vitest";
import * as TestClock from "effect/testing/TestClock";
import { assert, describe } from "vitest";

import { encodeJsonlLine, splitJsonlChunk } from "./piJsonl.ts";
import {
  makeInMemoryPiStdio,
  makePiRpcProtocol,
  PiRpcProtocolCommandFailedError,
  PiRpcProtocolProcessExitedError,
  PiRpcProtocolRequestTimeoutError,
} from "./piRpcProtocol.ts";
import { PiRpcAvailableModels, PiRpcResponseEnvelope } from "./piRpcTypes.ts";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturesDir = path.join(__dirname, "../../../../../docs/pi/phase0/fixtures");

const PiJsonObject = Schema.Record(Schema.String, Schema.Unknown);
const decodePiJsonObject = Schema.decodeSync(Schema.fromJsonString(PiJsonObject));

const readFixtureText = (name: string) => fs.readFileSync(path.join(fixturesDir, name), "utf8");

const readFixtureJsonlLines = (name: string) =>
  readFixtureText(name)
    .split("\n")
    .map((line) => line.replace(/\r$/, ""))
    .filter((line) => line.trim().length > 0);

const parseJsonObject = (text: string): Record<string, unknown> => decodePiJsonObject(text);

const offerStdoutLines = (
  input: Queue.Queue<Uint8Array, Cause.Done<void>>,
  lines: ReadonlyArray<string>,
) =>
  Effect.forEach(lines, (line) => Queue.offer(input, new TextEncoder().encode(`${line}\n`)), {
    discard: true,
  });

const isPiRpcProtocolCommandFailedError = Schema.is(PiRpcProtocolCommandFailedError);
const isPiRpcProtocolProcessExitedError = Schema.is(PiRpcProtocolProcessExitedError);
const isPiRpcProtocolRequestTimeoutError = Schema.is(PiRpcProtocolRequestTimeoutError);

describe("piJsonl framing", () => {
  it("splits partial chunks and strips trailing CR", () => {
    const first = splitJsonlChunk("", '{"type":');
    assert.deepEqual(first, { lines: [], remainder: '{"type":' });

    const second = splitJsonlChunk(first.remainder, '"response"}\r\n{"id":');
    assert.deepEqual(second, {
      lines: ['{"type":"response"}'],
      remainder: '{"id":',
    });

    const third = splitJsonlChunk(second.remainder, '"ok"}\n');
    assert.deepEqual(third, {
      lines: ['{"id":"ok"}'],
      remainder: "",
    });
  });
});

describe("Pi RPC protocol", () => {
  it.effect("correlates JSONL command responses by request id", () =>
    Effect.gen(function* () {
      const io = yield* makeInMemoryPiStdio();
      const protocol = yield* makePiRpcProtocol({ stdio: io.stdio });

      const commandFiber = yield* protocol
        .sendCommand("get_state", {}, (data) => Effect.succeed(data as { sessionId: string }))
        .pipe(Effect.forkScoped);

      const written = yield* Queue.take(io.output);
      const request = parseJsonObject(written.trim());

      yield* offerStdoutLines(io.input, [
        encodeJsonlLine({
          id: request.id,
          type: "response",
          command: "get_state",
          success: true,
          data: { sessionId: "session-1", messageCount: 0 },
        }).trimEnd(),
      ]);

      const data = yield* Fiber.join(commandFiber);
      assert.equal(data.sessionId, "session-1");
    }).pipe(Effect.scoped),
  );

  it.effect("collects non-response stdout JSON as stream events", () =>
    Effect.gen(function* () {
      const lines = readFixtureJsonlLines("normal-prompt-stream.jsonl").filter(
        (line) => !line.includes('"type":"response"'),
      );
      const io = yield* makeInMemoryPiStdio();
      const protocol = yield* makePiRpcProtocol({ stdio: io.stdio });
      const events: Array<Record<string, unknown>> = [];

      for (const line of lines) {
        yield* Queue.offer(io.input, new TextEncoder().encode(`${line}\n`));
        const batch = yield* Stream.runCollect(protocol.streamEvents.pipe(Stream.take(1)));
        events.push(...Array.from(batch));
      }

      assert.ok(events.length > 0);
      assert.equal(events[0]?.type, "thinking_level_changed");
      assert.ok(events.some((event) => event.type === "agent_start"));
      assert.ok(events.some((event) => event.type === "agent_end"));
    }).pipe(Effect.scoped),
  );

  it.effect("surfaces command failure responses", () =>
    Effect.gen(function* () {
      const io = yield* makeInMemoryPiStdio();
      const protocol = yield* makePiRpcProtocol({ stdio: io.stdio });
      const failureFiber = yield* protocol
        .sendCommand("set_model", { provider: "bad", modelId: "missing" }, Effect.succeed)
        .pipe(Effect.exit, Effect.forkScoped);

      const written = yield* Queue.take(io.output);
      const request = parseJsonObject(written.trim());
      const failurePayload = decodePiJsonObject(readFixtureJsonlLines("command-failure.jsonl")[0]!);

      yield* offerStdoutLines(io.input, [
        encodeJsonlLine({
          ...failurePayload,
          id: request.id,
        }).trimEnd(),
      ]);

      const exit = yield* Fiber.join(failureFiber);
      assert.strictEqual(Exit.isFailure(exit), true);
      if (Exit.isFailure(exit)) {
        const error = Cause.squash(exit.cause);
        assert.strictEqual(isPiRpcProtocolCommandFailedError(error), true);
        if (isPiRpcProtocolCommandFailedError(error)) {
          assert.match(error.error, /Model not found/);
        }
      }
    }).pipe(Effect.scoped),
  );

  it.effect("rejects pending commands when the process stream ends", () =>
    Effect.gen(function* () {
      const io = yield* makeInMemoryPiStdio();
      const protocol = yield* makePiRpcProtocol({ stdio: io.stdio });

      const pending = yield* protocol
        .sendCommand("prompt", { message: "blocked" }, Effect.succeed)
        .pipe(Effect.exit, Effect.forkScoped);

      yield* Queue.take(io.output);
      yield* protocol.failAllPending(
        new PiRpcProtocolProcessExitedError({
          detail: "Pi RPC stdout stream ended",
        }),
      );

      const exit = yield* Fiber.join(pending);
      assert.strictEqual(Exit.isFailure(exit), true);
      if (Exit.isFailure(exit)) {
        const error = Cause.squash(exit.cause);
        assert.strictEqual(isPiRpcProtocolProcessExitedError(error), true);
      }
    }).pipe(Effect.scoped),
  );

  it.effect("ignores malformed non-JSON stdout lines", () =>
    Effect.gen(function* () {
      const nonJsonLines = yield* Ref.make<Array<string>>([]);
      const io = yield* makeInMemoryPiStdio();
      const protocol = yield* makePiRpcProtocol({
        stdio: io.stdio,
        onNonJsonStdoutLine: (line) => Ref.update(nonJsonLines, (current) => [...current, line]),
      });

      const commandFiber = yield* protocol
        .sendCommand("abort", {}, () => Effect.void)
        .pipe(Effect.exit, Effect.forkScoped);

      yield* Queue.offer(io.input, new TextEncoder().encode("not-json {{\n"));
      const written = yield* Queue.take(io.output);
      const request = parseJsonObject(written.trim());

      yield* offerStdoutLines(io.input, [
        encodeJsonlLine({
          id: request.id,
          type: "response",
          command: "abort",
          success: true,
        }).trimEnd(),
      ]);

      const exit = yield* Fiber.join(commandFiber);
      assert.strictEqual(Exit.isSuccess(exit), true);
      assert.deepEqual(yield* Ref.get(nonJsonLines), ["not-json {{"]);
    }).pipe(Effect.scoped),
  );

  it.effect("rejects pending commands on request timeout", () =>
    Effect.gen(function* () {
      const io = yield* makeInMemoryPiStdio();
      const protocol = yield* makePiRpcProtocol({
        stdio: io.stdio,
        requestTimeoutMs: 30,
      });

      const fiber = yield* protocol
        .sendCommand("get_state", {}, Effect.succeed)
        .pipe(Effect.exit, Effect.forkScoped);

      yield* TestClock.adjust(Duration.millis(31));
      yield* Effect.yieldNow;

      const exit = yield* Fiber.join(fiber);
      assert.strictEqual(Exit.isFailure(exit), true);
      if (Exit.isFailure(exit)) {
        const error = Cause.squash(exit.cause);
        assert.strictEqual(isPiRpcProtocolRequestTimeoutError(error), true);
        if (isPiRpcProtocolRequestTimeoutError(error)) {
          assert.equal(error.command, "get_state");
        }
      }
    }).pipe(Effect.provide(TestClock.layer()), Effect.scoped),
  );

  it("accepts thinkingLevelMap on discovered models", () => {
    const fixture = Schema.decodeUnknownSync(PiRpcResponseEnvelope)(
      decodePiJsonObject(readFixtureText("model-discovery.json")),
    );
    const decoded = Schema.decodeUnknownSync(PiRpcAvailableModels)(fixture.data);
    const mini = decoded.models.find((model) => model.id === "gpt-5.4-mini");
    assert.ok(mini?.thinkingLevelMap);
    assert.equal(mini.thinkingLevelMap.xhigh, "xhigh");
    assert.equal(mini.thinkingLevelMap.minimal, "low");
  });

  it.effect("writes extension_ui_response records to stdin", () =>
    Effect.gen(function* () {
      const io = yield* makeInMemoryPiStdio();
      const protocol = yield* makePiRpcProtocol({ stdio: io.stdio });

      yield* protocol.sendRaw({
        type: "extension_ui_response",
        id: "uuid-select",
        value: "Allow",
      });

      const written = yield* Queue.take(io.output);
      const parsed = parseJsonObject(written.trim());
      assert.equal(parsed.type, "extension_ui_response");
      assert.equal(parsed.id, "uuid-select");
      assert.equal(parsed.value, "Allow");
    }).pipe(Effect.scoped),
  );
});
