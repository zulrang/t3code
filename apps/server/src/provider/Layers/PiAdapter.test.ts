// @effect-diagnostics nodeBuiltinImport:off
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ApprovalRequestId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { assert, beforeEach, describe, expect } from "vitest";

import { ServerConfig } from "../../config.ts";
import { ProviderAdapterValidationError, type ProviderAdapterError } from "../Errors.ts";
import { PiRpcProtocolCommandFailedError } from "../pi/piRpcProtocol.ts";
import { encodePiModelSlug } from "../pi/piModelSlug.ts";
import type { PiRpcClientShape, PiRpcClientError } from "../pi/piRpcClient.ts";
import type {
  PiRpcState,
  PiRpcStreamEvent,
  PiRpcAvailableModels,
  PiThinkingLevel,
} from "../pi/piRpcTypes.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { makePiAdapter, type PiRpcClientFactory } from "./PiAdapter.ts";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const fixturesDir = path.join(__dirname, "../../../../../docs/pi/phase0/fixtures");

class PiAdapter extends Context.Service<PiAdapter, ProviderAdapterShape<ProviderAdapterError>>()(
  "test/PiAdapter",
) {}

const decodePiSettings = Schema.decodeSync(
  Schema.Struct({
    enabled: Schema.Boolean,
    binaryPath: Schema.String,
    customModels: Schema.Array(Schema.String),
  }),
);

const PROVIDER = ProviderDriverKind.make("pi");
const MODEL_SLUG = encodePiModelSlug("openai-codex", "gpt-5.4-mini");
const INSTANCE_ID = ProviderInstanceId.make("pi");

const asThreadId = (value: string): ThreadId => ThreadId.make(value);

const readFixtureJsonlLines = (name: string) =>
  fs
    .readFileSync(path.join(fixturesDir, name), "utf8")
    .split("\n")
    .map((line) => line.replace(/\r$/, ""))
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as PiRpcStreamEvent);

class FakePiRpcClient implements PiRpcClientShape {
  readonly piVersion = "0.72.1-test";
  readonly streamInput: Queue.Queue<PiRpcStreamEvent, Cause.Done<void>>;
  readonly stderrInput: Queue.Queue<string, Cause.Done<void>>;
  readonly calls = {
    setModel: [] as Array<{ provider: string; modelId: string }>,
    setThinkingLevel: [] as Array<PiThinkingLevel>,
    prompt: [] as Array<{ message: string }>,
    abort: 0,
    extensionUiResponses: [] as Array<Record<string, unknown>>,
    close: 0,
    getState: 0,
    getAvailableModels: 0,
  };

  readonly streamEvents: Stream.Stream<PiRpcStreamEvent, never>;
  readonly stderrLines: Stream.Stream<string, never>;
  readonly spawnKey: string;
  readonly directDispatch: boolean;
  promptError: PiRpcClientError | undefined;
  private streamHandler: ((event: PiRpcStreamEvent) => Effect.Effect<void>) | undefined;
  private readonly isStreamingRef: Ref.Ref<boolean>;

  constructor(
    spawnKey: string,
    isStreamingRef: Ref.Ref<boolean>,
    options?: { readonly directDispatch?: boolean },
  ) {
    this.spawnKey = spawnKey;
    this.isStreamingRef = isStreamingRef;
    this.directDispatch = options?.directDispatch !== false;
    this.streamInput = Effect.runSync(Queue.unbounded<PiRpcStreamEvent, Cause.Done<void>>());
    this.stderrInput = Effect.runSync(Queue.unbounded<string, Cause.Done<void>>());
    this.streamEvents = Stream.fromQueue(this.streamInput);
    this.stderrLines = Stream.fromQueue(this.stderrInput);
  }

  setStreamEventHandler = (handler: (event: PiRpcStreamEvent) => Effect.Effect<void>) =>
    Effect.sync(() => {
      if (!this.directDispatch) {
        return;
      }
      this.streamHandler = handler;
    });

  pushStderr(lines: ReadonlyArray<string>) {
    return Effect.forEach(lines, (line) => Queue.offer(this.stderrInput, line), { discard: true });
  }

  pushEvents(events: ReadonlyArray<PiRpcStreamEvent>) {
    const self = this;
    return Effect.forEach(
      events,
      (event) =>
        Effect.gen(function* () {
          if (self.streamHandler) {
            yield* self.streamHandler(event);
            return;
          }
          yield* Queue.offer(self.streamInput, event);
        }),
      { discard: true },
    );
  }

  getState = () => {
    const self = this;
    return Effect.gen(function* () {
      self.calls.getState += 1;
      const isStreaming = yield* Ref.get(self.isStreamingRef);
      return {
        isStreaming,
        model: {
          id: "gpt-5.4-mini",
          provider: "openai-codex",
        },
      } satisfies PiRpcState;
    });
  };

  getAvailableModels = (): Effect.Effect<PiRpcAvailableModels, PiRpcClientError> => {
    const self = this;
    return Effect.sync(() => {
      self.calls.getAvailableModels += 1;
      return {
        models: [
          {
            id: "gpt-5.4-mini",
            provider: "openai-codex",
            reasoning: true,
            thinkingLevelMap: { minimal: "low", xhigh: "xhigh" },
          },
        ],
      } satisfies PiRpcAvailableModels;
    });
  };

  getCommands = () => Effect.succeed({ commands: [] });

  setModel = (input: { readonly provider: string; readonly modelId: string }) =>
    Effect.sync(() => {
      this.calls.setModel.push(input);
      return {
        id: input.modelId,
        provider: input.provider,
      };
    });

  setThinkingLevel = (level: PiThinkingLevel) =>
    Effect.sync(() => {
      this.calls.setThinkingLevel.push(level);
    });

  prompt = (input: { readonly message: string }) => {
    const self = this;
    return Effect.gen(function* () {
      if (self.promptError) {
        return yield* self.promptError;
      }
      self.calls.prompt.push(input);
      yield* Ref.set(self.isStreamingRef, true);
    });
  };

  abort = () =>
    Effect.sync(() => {
      this.calls.abort += 1;
    });

  sendExtensionUiResponse = (response: Record<string, unknown>) =>
    Effect.sync(() => {
      this.calls.extensionUiResponses.push(response);
    });

  close = () =>
    Effect.sync(() => {
      this.calls.close += 1;
    });
}

const fakeClients: FakePiRpcClient[] = [];
const isStreamingRef = Effect.runSync(Ref.make(false));

const fakeClientFactory: PiRpcClientFactory = (options) =>
  Effect.gen(function* () {
    yield* Scope.Scope;
    const directDispatch = !String(options.binaryPath).includes("no-direct-dispatch");
    const client = new FakePiRpcClient(`spawn-${fakeClients.length + 1}`, isStreamingRef, {
      directDispatch,
    });
    fakeClients.push(client);
    return client;
  });

const makePiAdapterLayer = (options?: {
  readonly nativeEventLogger?: {
    readonly filePath: string;
    readonly write: (event: unknown, threadId: ThreadId | null) => Effect.Effect<void>;
    readonly close: () => Effect.Effect<void>;
  };
  readonly turnSilenceHardMs?: number;
  readonly turnSilenceReconcileMs?: number;
  readonly sleep?: (duration: import("effect/Duration").Input) => Effect.Effect<void>;
}) =>
  Layer.effect(
    PiAdapter,
    makePiAdapter(
      decodePiSettings({
        enabled: true,
        binaryPath: "pi",
        customModels: [],
      }),
      {
        instanceId: INSTANCE_ID,
        makeRpcClient: fakeClientFactory,
        ...(options?.nativeEventLogger ? { nativeEventLogger: options.nativeEventLogger } : {}),
        ...(options?.turnSilenceHardMs !== undefined
          ? { turnSilenceHardMs: options.turnSilenceHardMs }
          : {}),
        ...(options?.turnSilenceReconcileMs !== undefined
          ? { turnSilenceReconcileMs: options.turnSilenceReconcileMs }
          : {}),
        ...(options?.sleep ? { sleep: options.sleep } : {}),
      },
    ),
  ).pipe(
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(NodeServices.layer),
  );

const PiAdapterTestLayer = makePiAdapterLayer();

const collectThreadEvents = (
  stream: Stream.Stream<ProviderRuntimeEvent, never>,
  threadId: ThreadId,
  count: number,
) =>
  stream.pipe(
    Stream.filter((event) => event.threadId === threadId),
    Stream.take(count),
    Stream.runCollect,
    Effect.forkChild,
  );

const collectThreadEventsUntil = (
  stream: Stream.Stream<ProviderRuntimeEvent, never>,
  threadId: ThreadId,
  predicate: (event: ProviderRuntimeEvent) => boolean,
) =>
  stream.pipe(
    Stream.filter((event) => event.threadId === threadId),
    Stream.takeUntil(predicate),
    Stream.runCollect,
    Effect.forkChild,
  );

const joinCollectedEvents = (
  eventsFiber: Fiber.Fiber<ReadonlyArray<ProviderRuntimeEvent>, never>,
) => Fiber.join(eventsFiber).pipe(Effect.timeout("10 seconds"));

const yieldToEventLoop = Effect.repeat(Effect.yieldNow, { times: 3, discard: true });

beforeEach(() => {
  fakeClients.length = 0;
  Effect.runSync(Ref.set(isStreamingRef, false));
});

describe("PiAdapter", () => {
  it.layer(PiAdapterTestLayer)("live adapter", (it) => {
    it.effect("starts one RPC client per thread and emits session/thread started events", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-a");
        const eventsFiber = yield* collectThreadEvents(adapter.streamEvents, threadId, 2);

        const session = yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
        });

        expect(session.provider).toBe(PROVIDER);
        expect(session.threadId).toBe(threadId);
        expect(yield* adapter.hasSession(threadId)).toBe(true);
        expect(fakeClients).toHaveLength(1);

        const events = yield* joinCollectedEvents(eventsFiber);
        expect(events.map((event) => (event as { type: string }).type)).toEqual([
          "session.started",
          "thread.started",
        ]);

        yield* adapter.stopSession(threadId);
        expect(yield* adapter.hasSession(threadId)).toBe(false);
        expect(fakeClients[0]?.calls.close).toBe(1);
      }),
    );

    it.effect("ignores stale resumeCursor and emits fresh-runtime warning", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-resume");
        const eventsFiber = yield* collectThreadEvents(adapter.streamEvents, threadId, 3);

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          resumeCursor: { sessionId: "stale-session" },
        });

        const events = yield* joinCollectedEvents(eventsFiber);
        expect(events.map((event) => (event as { type: string }).type)).toEqual([
          "session.started",
          "thread.started",
          "runtime.warning",
        ]);
        expect(
          events.some((event) =>
            (event as { type: string; payload?: { message?: string } }).payload?.message?.includes(
              "not resumed",
            ),
          ),
        ).toBe(true);
      }),
    );

    it.effect("streams assistant text and completes the turn from fixture events", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-stream");
        const eventsFiber = yield* collectThreadEvents(adapter.streamEvents, threadId, 6);

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        const turn = yield* adapter.sendTurn({
          threadId,
          input: "Reply with exactly: T3_PI_PHASE0_OK.",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        const fixtureEvents = readFixtureJsonlLines("normal-prompt-stream.jsonl").filter(
          (event) => event.type !== "response",
        );
        yield* Ref.set(isStreamingRef, false);
        yield* fakeClients[0]!.pushEvents(fixtureEvents);
        yield* yieldToEventLoop;

        const events = yield* joinCollectedEvents(eventsFiber);
        const typed = events as unknown as Array<{
          type: string;
          payload?: { streamKind?: string; delta?: string };
        }>;
        expect(typed.some((event) => event.type === "turn.started")).toBe(true);
        expect(
          typed.some(
            (event) =>
              event.type === "content.delta" &&
              event.payload?.streamKind === "assistant_text" &&
              event.payload.delta?.includes("T3"),
          ),
        ).toBe(true);
        expect(typed.some((event) => event.type === "turn.completed")).toBe(true);
        expect(yield* adapter.readThread(threadId)).toMatchObject({
          threadId,
          turns: [{ id: turn.turnId }],
        });
      }),
    );

    it.effect("rejects unknown model selections", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-model");

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
        });

        const result = yield* adapter
          .sendTurn({
            threadId,
            input: "hello",
            modelSelection: createModelSelection(
              INSTANCE_ID,
              encodePiModelSlug("missing-provider", "missing-model"),
            ),
          })
          .pipe(Effect.flip);
        expect(result).toBeInstanceOf(ProviderAdapterValidationError);
      }),
    );

    it.effect("interruptTurn aborts the active Pi turn", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-abort");
        const eventsFiber = yield* collectThreadEventsUntil(
          adapter.streamEvents,
          threadId,
          (event) => (event as { type: string }).type === "turn.aborted",
        );

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        const turn = yield* adapter.sendTurn({
          threadId,
          input: "long task",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        yield* adapter.interruptTurn(threadId, turn.turnId);
        expect(fakeClients[0]?.calls.abort).toBe(1);

        const events = yield* joinCollectedEvents(eventsFiber);
        expect(events.some((event) => (event as { type: string }).type === "turn.aborted")).toBe(
          true,
        );
      }),
    );

    it.effect("bridges extension UI select requests through respondToUserInput", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-ui");
        const eventsFiber = yield* collectThreadEvents(adapter.streamEvents, threadId, 3);

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        yield* adapter.sendTurn({
          threadId,
          input: "trigger ui",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        yield* fakeClients[0]!.pushEvents([
          {
            type: "extension_ui_request",
            id: "uuid-select",
            method: "select",
            title: "Allow dangerous command?",
            options: ["Allow", "Deny"],
            timeout: 30_000,
          },
        ]);
        yield* yieldToEventLoop;

        const requested = yield* joinCollectedEvents(eventsFiber);
        const uiRequest = requested.find(
          (event) => (event as { type: string }).type === "user-input.requested",
        ) as { requestId?: string } | undefined;
        assert(uiRequest?.requestId);

        yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make(uiRequest.requestId), {
          [uiRequest.requestId]: "Allow",
        });

        expect(fakeClients[0]?.calls.extensionUiResponses).toEqual([
          { id: "uuid-select", value: "Allow" },
        ]);
      }),
    );

    it.effect("preserves whitespace in streamed assistant text deltas", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-whitespace-delta");
        const eventsFiber = yield* collectThreadEventsUntil(
          adapter.streamEvents,
          threadId,
          (event) =>
            (event as { type: string; payload?: { delta?: string } }).type === "content.delta" &&
            (event as { payload?: { delta?: string } }).payload?.delta === "\n",
        );

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        yield* adapter.sendTurn({
          threadId,
          input: "format text",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        yield* fakeClients[0]!.pushEvents([
          { type: "turn_start" },
          {
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", delta: "hello", contentIndex: 0 },
          },
          {
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", delta: " world", contentIndex: 0 },
          },
          {
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", delta: "\n", contentIndex: 0 },
          },
        ]);
        yield* yieldToEventLoop;

        const events = yield* joinCollectedEvents(eventsFiber);
        const deltas = events
          .filter((event) => (event as { type: string }).type === "content.delta")
          .map((event) => (event as { payload?: { delta?: string } }).payload?.delta);
        expect(deltas).toEqual(["hello", " world", "\n"]);
      }),
    );

    it.effect("completes after compaction retry once the retried turn finishes", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-compaction-retry");
        const eventsFiber = yield* collectThreadEventsUntil(
          adapter.streamEvents,
          threadId,
          (event) => (event as { type: string }).type === "turn.completed",
        );

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        yield* adapter.sendTurn({
          threadId,
          input: "long context task",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        yield* fakeClients[0]!.pushEvents([
          { type: "turn_start" },
          {
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", delta: "partial", contentIndex: 0 },
          },
          { type: "compaction_end", willRetry: true },
          {
            type: "agent_end",
            message: { stopReason: "compacted" },
          },
          { type: "auto_retry_start" },
          {
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", delta: "done", contentIndex: 0 },
          },
          {
            type: "agent_end",
            message: { stopReason: "stop" },
          },
        ]);
        yield* Ref.set(isStreamingRef, false);
        yield* yieldToEventLoop;

        const events = yield* joinCollectedEvents(eventsFiber);
        const completed = events.filter(
          (event) => (event as { type: string }).type === "turn.completed",
        );
        expect(completed).toHaveLength(1);
        expect((completed[0] as { payload?: { state?: string } }).payload?.state).toBe("completed");
      }),
    );

    it.effect("rollbackThread is unsupported", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-rollback");

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
        });

        const error = yield* adapter.rollbackThread(threadId, 1).pipe(Effect.flip);
        expect(error).toBeInstanceOf(ProviderAdapterValidationError);
      }),
    );

    it.effect("writes redacted native RPC logs with pi.rpc.* sources", () =>
      Effect.gen(function* () {
        const nativeEvents: Array<{ event?: { source?: string; threadId?: string } }> = [];
        const adapterLayer = makePiAdapterLayer({
          nativeEventLogger: {
            filePath: "memory://pi-native-events",
            write: (event) => {
              nativeEvents.push(event as (typeof nativeEvents)[number]);
              return Effect.void;
            },
            close: () => Effect.void,
          },
        });

        const threadId = asThreadId("thread-native-log");
        yield* Effect.gen(function* () {
          const adapter = yield* PiAdapter;
          yield* adapter.startSession({
            threadId,
            runtimeMode: "full-access",
            modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
          });
          yield* adapter.sendTurn({
            threadId,
            input: "hello",
            modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
          });
          yield* fakeClients[0]!.pushEvents([{ type: "turn_start" }]);
          yield* yieldToEventLoop;
        }).pipe(Effect.provide(adapterLayer));

        expect(nativeEvents.length).toBeGreaterThan(0);
        expect(nativeEvents.some((record) => record.event?.source === "pi.rpc.turn_start")).toBe(
          true,
        );
        expect(nativeEvents.some((record) => record.event?.threadId === "thread-native-log")).toBe(
          true,
        );
      }),
    );

    it.effect("maps Pi stderr lines to runtime.warning", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-stderr");
        const eventsFiber = yield* collectThreadEventsUntil(
          adapter.streamEvents,
          threadId,
          (event) =>
            (event as { type: string }).type === "runtime.warning" &&
            (event as { payload?: { message?: string } }).payload?.message === "pi auth warning",
        );

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
        });
        yield* fakeClients[0]!.pushStderr(["pi auth warning"]);
        yield* yieldToEventLoop;

        const events = yield* joinCollectedEvents(eventsFiber);
        expect(
          events.some(
            (event) =>
              (event as { type: string; payload?: { message?: string } }).type ===
                "runtime.warning" &&
              (event as { payload?: { message?: string } }).payload?.message === "pi auth warning",
          ),
        ).toBe(true);
      }),
    );

    it.effect("fails the turn when prompt is rejected after acceptance", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-prompt-fail");
        const eventsFiber = yield* collectThreadEventsUntil(
          adapter.streamEvents,
          threadId,
          (event) =>
            (event as { type: string; payload?: { state?: string } }).type === "turn.completed" &&
            (event as { payload?: { state?: string } }).payload?.state === "failed",
        );

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        fakeClients[0]!.promptError = new PiRpcProtocolCommandFailedError({
          command: "prompt",
          error: "upstream provider auth failed",
        });

        const result = yield* adapter
          .sendTurn({
            threadId,
            input: "hello",
            modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
          })
          .pipe(Effect.result);
        expect(Result.isFailure(result)).toBe(true);

        const events = yield* joinCollectedEvents(eventsFiber);
        const failed = events.filter(
          (event) =>
            (event as { type: string; payload?: { state?: string } }).type === "turn.completed" &&
            (event as { payload?: { state?: string } }).payload?.state === "failed",
        );
        expect(failed).toHaveLength(1);
      }),
    );

    it.effect("fails stalled turns via watchdog timeout", () =>
      Effect.gen(function* () {
        const adapterLayer = makePiAdapterLayer({
          turnSilenceHardMs: 1_000,
          turnSilenceReconcileMs: 100,
        });
        const threadId = asThreadId("thread-watchdog");

        yield* Effect.gen(function* () {
          const adapter = yield* PiAdapter;
          const eventsFiber = yield* collectThreadEventsUntil(
            adapter.streamEvents,
            threadId,
            (event) =>
              (event as { type: string; payload?: { state?: string } }).type === "turn.completed" &&
              (event as { payload?: { state?: string } }).payload?.state === "failed",
          );

          yield* adapter.startSession({
            threadId,
            runtimeMode: "full-access",
            modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
          });
          yield* adapter.sendTurn({
            threadId,
            input: "stall",
            modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
          });
          yield* fakeClients[0]!.pushEvents([{ type: "turn_start" }]);
          yield* TestClock.adjust("2 seconds");
          yield* Effect.yieldNow;
          yield* Effect.yieldNow;

          const events = yield* joinCollectedEvents(eventsFiber);
          const failed = events.filter(
            (event) =>
              (event as { type: string; payload?: { state?: string } }).type === "turn.completed" &&
              (event as { payload?: { state?: string } }).payload?.state === "failed",
          );
          expect(failed).toHaveLength(1);
        }).pipe(Effect.provide(Layer.mergeAll(adapterLayer, TestClock.layer())));
      }),
    );

    it.effect("cancels stale extension UI when restarting a session", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-ui-restart");

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });
        yield* adapter.sendTurn({
          threadId,
          input: "trigger ui",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });
        yield* fakeClients[0]!.pushEvents([
          {
            type: "extension_ui_request",
            id: "uuid-stale",
            method: "confirm",
            title: "Continue?",
            message: "Allow?",
          },
        ]);
        yield* yieldToEventLoop;

        const firstClient = fakeClients[0]!;
        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        expect(firstClient.calls.close).toBe(1);
        expect(firstClient.calls.extensionUiResponses).toEqual([
          { id: "uuid-stale", cancelled: true },
        ]);
        expect(fakeClients).toHaveLength(2);
      }),
    );

    it.effect("emits only one turn.started per T3 turn", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-single-start");
        const eventsFiber = yield* collectThreadEventsUntil(
          adapter.streamEvents,
          threadId,
          (event) => (event as { type: string }).type === "turn.completed",
        );

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });
        yield* adapter.sendTurn({
          threadId,
          input: "hello",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });
        yield* fakeClients[0]!.pushEvents([
          { type: "turn_start" },
          { type: "turn_start" },
          { type: "agent_start" },
          { type: "agent_start" },
          {
            type: "message_update",
            assistantMessageEvent: { type: "text_delta", delta: "ok", contentIndex: 0 },
          },
          { type: "agent_end", message: { stopReason: "stop" } },
        ]);
        yield* Ref.set(isStreamingRef, false);
        yield* yieldToEventLoop;

        const events = yield* joinCollectedEvents(eventsFiber);
        const started = events.filter(
          (event) => (event as { type: string }).type === "turn.started",
        );
        const completed = events.filter(
          (event) => (event as { type: string }).type === "turn.completed",
        );
        expect(started).toHaveLength(1);
        expect(completed).toHaveLength(1);
      }),
    );

    it.effect("closes RPC clients when stopSession is called after failed prompt", () =>
      Effect.gen(function* () {
        const adapter = yield* PiAdapter;
        const threadId = asThreadId("thread-failed-prompt-cleanup");

        yield* adapter.startSession({
          threadId,
          runtimeMode: "full-access",
          modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
        });

        fakeClients[0]!.promptError = new PiRpcProtocolCommandFailedError({
          command: "prompt",
          error: "broken pipe",
        });
        yield* adapter
          .sendTurn({
            threadId,
            input: "hello",
            modelSelection: createModelSelection(INSTANCE_ID, MODEL_SLUG),
          })
          .pipe(Effect.result);

        yield* adapter.stopSession(threadId);
        expect(fakeClients[0]?.calls.close).toBe(1);
        expect(yield* adapter.hasSession(threadId)).toBe(false);
      }),
    );
  });
});
