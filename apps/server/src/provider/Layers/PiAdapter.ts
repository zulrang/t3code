/**
 * PiAdapter — live Pi RPC adapter (`pi --mode rpc --no-session`).
 *
 * Maps Pi JSONL RPC stream events to canonical `ProviderRuntimeEvent`s and
 * implements the full provider adapter SPI for per-thread Pi subprocesses.
 *
 * @module provider/Layers/PiAdapter
 */
import {
  ApprovalRequestId,
  EventId,
  type PiSettings,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSessionStartInput,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  RuntimeRequestId,
  type ProviderUserInputAnswers,
  ThreadId,
  TurnId,
  type UserInputQuestion,
  type CanonicalItemType,
  type ToolLifecycleItemType,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { PI_FRESH_RUNTIME_WARNING } from "@t3tools/shared/pi";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Random from "effect/Random";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { decodePiModelSlug, encodePiModelSlug } from "../pi/piModelSlug.ts";
import { resolvePiThinkingLevelForModel } from "../pi/piModelMapping.ts";
import { formatPiNativeLogRecord, piRpcRawSource } from "../pi/piNativeLogging.ts";
import {
  makePiRpcClient,
  PiRpcClientSpawnError,
  PiRpcProtocolCommandFailedError,
  type PiRpcClientShape,
} from "../pi/piRpcClient.ts";
import type { PiRpcClientOptions } from "../pi/piRpcClient.ts";
import type { PiRpcModel, PiRpcStreamEvent, PiThinkingLevel } from "../pi/piRpcTypes.ts";
import { evaluatePiWatchdogTick } from "../pi/piTurnWatchdogPolicy.ts";
import type {
  ProviderAdapterShape,
  ProviderThreadSnapshot,
  ProviderThreadTurnSnapshot,
} from "../Services/ProviderAdapter.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("pi");

const TURN_SILENCE_RECONCILE_MS = 30_000;
const TURN_SILENCE_HARD_MS = 120_000;
const EXTENSION_UI_DEFAULT_TIMEOUT_MS = 60_000;

const BLOCKING_EXTENSION_UI_METHODS = new Set(["select", "confirm", "input", "editor"]);
const FIRE_AND_FORGET_EXTENSION_UI_METHODS = new Set([
  "notify",
  "setStatus",
  "setWidget",
  "setTitle",
  "set_editor_text",
]);

type PiTurnPhase =
  | "idle"
  | "prompt_pending"
  | "accepted"
  | "running"
  | "streaming"
  | "waiting_for_user_input"
  | "completed"
  | "failed"
  | "aborted";

interface PiTurnSnapshot {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

interface PendingExtensionUiRequest {
  readonly piRequestId: string;
  readonly t3RequestId: ApprovalRequestId;
  readonly method: string;
  readonly timeoutFiber: Fiber.Fiber<void, never> | undefined;
}

interface PiSessionContext {
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly client: PiRpcClientShape;
  readonly commandLock: Semaphore.Semaphore;
  eventFiber: Fiber.Fiber<void, never>;
  stderrFiber: Fiber.Fiber<void, never> | undefined;
  readonly stopped: Ref.Ref<boolean>;
  activeTurnId: TurnId | undefined;
  turnPhase: PiTurnPhase;
  turnStartedEmitted: boolean;
  turnCompletedEmitted: boolean;
  hasAssistantText: boolean;
  hasReasoningText: boolean;
  hasToolActivity: boolean;
  activeModelSlug: string | undefined;
  readonly turns: Array<PiTurnSnapshot>;
  readonly turnItems: Map<TurnId, Array<unknown>>;
  readonly contentEmittedByKey: Map<string, string>;
  readonly toolExecutionIds: Set<string>;
  readonly toolcallIds: Set<string>;
  readonly pendingExtensionUi: Map<ApprovalRequestId, PendingExtensionUiRequest>;
  watchdogFiber: Fiber.Fiber<void, never> | undefined;
  lastEventAtMs: number;
  compactionWillRetry: boolean;
}

export interface PiAdapterLiveOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly makeRpcClient?: PiRpcClientFactory;
  readonly turnSilenceReconcileMs?: number;
  readonly turnSilenceHardMs?: number;
  readonly sleep?: (duration: Duration.Input) => Effect.Effect<void>;
}

export type PiRpcClientFactory = (
  options: PiRpcClientOptions,
) => Effect.Effect<
  PiRpcClientShape,
  PiRpcClientSpawnError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
>;

const isPiRpcClientSpawnError = Schema.is(PiRpcClientSpawnError);
const isPiRpcProtocolCommandFailedError = Schema.is(PiRpcProtocolCommandFailedError);

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function readString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/** Preserve whitespace for streamed LLM/tool payload text (deltas, stdout, etc.). */
function readContentString(value: unknown): string | undefined {
  if (typeof value !== "string" || value.length === 0) {
    return undefined;
  }
  return value;
}

function providerAdapterErrorDetail(error: ProviderAdapterError): string {
  if ("detail" in error && typeof error.detail === "string") {
    return error.detail;
  }
  if ("issue" in error && typeof error.issue === "string") {
    return error.issue;
  }
  return String(error);
}

function mapPiClientError(method: string, error: unknown): ProviderAdapterError {
  if (isPiRpcProtocolCommandFailedError(error)) {
    return new ProviderAdapterRequestError({
      provider: PROVIDER,
      method,
      detail: error.error,
      cause: error,
    });
  }
  if (isPiRpcClientSpawnError(error)) {
    return new ProviderAdapterProcessError({
      provider: PROVIDER,
      threadId: ThreadId.make("unknown"),
      detail: error.message,
      cause: error,
    });
  }
  if (error instanceof Error) {
    return new ProviderAdapterRequestError({
      provider: PROVIDER,
      method,
      detail: error.message,
      cause: error,
    });
  }
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: String(error),
    cause: error,
  });
}

function toToolLifecycleItemType(toolName: string): ToolLifecycleItemType {
  const normalized = toolName.toLowerCase();
  if (
    normalized.includes("bash") ||
    normalized.includes("command") ||
    normalized.includes("shell")
  ) {
    return "command_execution";
  }
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("patch") ||
    normalized.includes("multiedit")
  ) {
    return "file_change";
  }
  if (normalized.includes("web")) {
    return "web_search";
  }
  if (normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  if (normalized.includes("image")) {
    return "image_view";
  }
  if (
    normalized.includes("task") ||
    normalized.includes("agent") ||
    normalized.includes("subtask")
  ) {
    return "collab_agent_tool_call";
  }
  return "dynamic_tool_call";
}

function toCanonicalItemType(toolName: string): CanonicalItemType {
  return toToolLifecycleItemType(toolName);
}

function extensionUiQuestions(
  request: Record<string, unknown>,
): ReadonlyArray<UserInputQuestion> | null {
  const id = readString(request.id);
  const method = readString(request.method);
  const title = readString(request.title) ?? "Pi extension request";
  if (!id || !method) {
    return null;
  }

  switch (method) {
    case "select": {
      const options = Array.isArray(request.options)
        ? request.options
            .map((option) => readString(option))
            .filter((option): option is string => option !== undefined)
        : [];
      if (options.length === 0) {
        return null;
      }
      return [
        {
          id,
          header: title,
          question: title,
          options: options.map((option) => ({ label: option, description: option })),
        },
      ];
    }
    case "confirm": {
      const message = readString(request.message) ?? title;
      return [
        {
          id,
          header: title,
          question: message,
          options: [
            { label: "Yes", description: "Confirm" },
            { label: "No", description: "Decline" },
          ],
        },
      ];
    }
    case "input": {
      const placeholder = readString(request.placeholder) ?? "Enter a value";
      return [
        {
          id,
          header: title,
          question: placeholder,
          options: [{ label: "Submit", description: placeholder }],
        },
      ];
    }
    case "editor": {
      const prefill = readString(request.prefill);
      return [
        {
          id,
          header: title,
          question: prefill ?? "Edit text",
          options: [{ label: "Submit", description: "Submit edited text" }],
        },
      ];
    }
    default:
      return null;
  }
}

export function makePiAdapter(
  piSettings: PiSettings,
  options?: PiAdapterLiveOptions,
): Effect.Effect<
  ProviderAdapterShape<ProviderAdapterError>,
  never,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope | ServerConfig
> {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
    const turnSilenceReconcileMs = options?.turnSilenceReconcileMs ?? TURN_SILENCE_RECONCILE_MS;
    const turnSilenceHardMs = options?.turnSilenceHardMs ?? TURN_SILENCE_HARD_MS;
    const sleep = options?.sleep ?? ((duration: Duration.Input) => Effect.sleep(duration));
    const readWatchdogClockMs = Clock.currentTimeMillis;
    const serverConfig = yield* ServerConfig;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const makeRpcClient = options?.makeRpcClient ?? makePiRpcClient;
    const nativeEventLogger =
      options?.nativeEventLogger ??
      (options?.nativeEventLogPath !== undefined
        ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, { stream: "native" })
        : undefined);
    const managedNativeEventLogger =
      options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;

    const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, PiSessionContext>();

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const contexts = [...sessions.values()];
        sessions.clear();
        yield* Effect.forEach(contexts, (context) => stopPiContext(context), {
          concurrency: "unbounded",
          discard: true,
        }).pipe(Effect.ignoreCause);
        if (managedNativeEventLogger !== undefined) {
          yield* managedNativeEventLogger.close();
        }
      }).pipe(Effect.ensuring(Queue.shutdown(runtimeEvents))),
    );

    const buildEventBase = (input: {
      readonly threadId: ThreadId;
      readonly turnId?: TurnId | undefined;
      readonly itemId?: string | undefined;
      readonly requestId?: string | undefined;
      readonly raw?: unknown;
      readonly rawSource?: string;
    }): Effect.Effect<
      Pick<
        ProviderRuntimeEvent,
        | "eventId"
        | "provider"
        | "providerInstanceId"
        | "threadId"
        | "createdAt"
        | "turnId"
        | "itemId"
        | "requestId"
        | "raw"
      >
    > =>
      Effect.gen(function* () {
        const uuid = yield* Random.nextUUIDv4;
        const createdAt = yield* nowIso;
        return {
          eventId: EventId.make(uuid),
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          threadId: input.threadId,
          createdAt,
          ...(input.turnId ? { turnId: input.turnId } : {}),
          ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
          ...(input.requestId ? { requestId: RuntimeRequestId.make(input.requestId) } : {}),
          ...(input.raw !== undefined
            ? {
                raw: {
                  source: piRpcRawSource(input.rawSource ?? "event"),
                  payload: input.raw,
                },
              }
            : {}),
        };
      });

    const emit = (event: ProviderRuntimeEvent) =>
      Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);

    const writeNativeEvent = (
      threadId: ThreadId,
      event: Record<string, unknown>,
      meta?: { readonly turnId?: TurnId; readonly category?: string },
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        if (!nativeEventLogger) {
          return;
        }
        const observedAt = yield* nowIso;
        const eventType = readString(event.type) ?? "event";
        yield* nativeEventLogger.write(
          {
            observedAt,
            event: formatPiNativeLogRecord({
              provider: PROVIDER,
              providerInstanceId: boundInstanceId,
              threadId,
              ...(meta?.turnId ? { turnId: meta.turnId } : {}),
              category: meta?.category ?? eventType,
              type: eventType,
              payload: event,
            }),
          },
          threadId,
        );
      });

    const requireSession = (
      threadId: ThreadId,
      operation: string,
    ): Effect.Effect<PiSessionContext, ProviderAdapterError> =>
      Effect.gen(function* () {
        const context = sessions.get(threadId);
        if (!context) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        if (yield* Ref.get(context.stopped)) {
          return yield* new ProviderAdapterSessionClosedError({
            provider: PROVIDER,
            threadId,
            cause: new Error(`${operation} called after session stopped`),
          });
        }
        return context;
      });

    const updateSession = (
      context: PiSessionContext,
      patch: Partial<ProviderSession> & { readonly updatedAt?: string },
    ): Effect.Effect<void> =>
      Effect.gen(function* () {
        const updatedAt = patch.updatedAt ?? (yield* nowIso);
        context.session = {
          ...context.session,
          ...patch,
          updatedAt,
        };
      });

    const cancelPendingExtensionUi = Effect.fn("cancelPendingExtensionUi")(function* (
      context: PiSessionContext,
      reason: string,
    ) {
      if (context.pendingExtensionUi.size === 0) {
        return;
      }
      const pending = [...context.pendingExtensionUi.values()];
      context.pendingExtensionUi.clear();
      for (const request of pending) {
        if (request.timeoutFiber) {
          yield* Fiber.interrupt(request.timeoutFiber).pipe(Effect.ignore);
        }
        yield* context.client
          .sendExtensionUiResponse({ id: request.piRequestId, cancelled: true })
          .pipe(Effect.ignore);
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId: context.activeTurnId,
            requestId: request.t3RequestId,
            raw: { type: "extension_ui_cancelled", reason },
            rawSource: "extension_ui",
          })),
          type: "user-input.resolved",
          payload: { answers: {} },
        });
      }
    });

    const clearTurnState = (context: PiSessionContext): Effect.Effect<void> =>
      Effect.gen(function* () {
        yield* cancelPendingExtensionUi(context, "Pi turn state cleared.");
        context.activeTurnId = undefined;
        context.turnPhase = "idle";
        context.turnStartedEmitted = false;
        context.turnCompletedEmitted = false;
        context.hasAssistantText = false;
        context.hasReasoningText = false;
        context.hasToolActivity = false;
        context.compactionWillRetry = false;
        if (context.watchdogFiber) {
          yield* Fiber.interrupt(context.watchdogFiber).pipe(Effect.ignore);
          context.watchdogFiber = undefined;
        }
      });

    const failActiveTurn = Effect.fn("failActiveTurn")(function* (
      context: PiSessionContext,
      message: string,
      options?: { readonly aborted?: boolean },
    ) {
      if (context.turnCompletedEmitted) {
        yield* clearTurnState(context);
        return;
      }
      const turnId = context.activeTurnId;
      if (!turnId) {
        yield* clearTurnState(context);
        return;
      }
      context.turnCompletedEmitted = true;
      context.turnPhase = options?.aborted ? "aborted" : "failed";
      if (options?.aborted) {
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId,
          })),
          type: "turn.aborted",
          payload: { reason: message },
        });
      } else {
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId,
          })),
          type: "turn.completed",
          payload: {
            state: "failed",
            errorMessage: message,
          },
        });
      }
      yield* updateSession(context, {
        status: "ready",
        activeTurnId: undefined,
        lastError: message,
      });
      yield* clearTurnState(context);
    });

    const completeActiveTurn = Effect.fn("completeActiveTurn")(function* (
      context: PiSessionContext,
      input?: { readonly stopReason?: string | null; readonly aborted?: boolean },
    ) {
      if (context.turnCompletedEmitted || context.compactionWillRetry) {
        return;
      }
      const turnId = context.activeTurnId;
      if (!turnId) {
        return;
      }
      const stopReason = input?.stopReason ?? null;
      const isAborted =
        input?.aborted === true ||
        stopReason === "aborted" ||
        (typeof stopReason === "string" && stopReason.toLowerCase().includes("abort"));
      if (isAborted) {
        yield* failActiveTurn(context, stopReason ?? "Turn aborted.", { aborted: true });
        return;
      }
      const hasVisibleOutput =
        context.hasAssistantText || context.hasReasoningText || context.hasToolActivity;
      if (!hasVisibleOutput) {
        // Tool-only cycles should not emit blank assistant completions.
      }
      context.turnCompletedEmitted = true;
      context.turnPhase = "completed";
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
        })),
        type: "turn.completed",
        payload: {
          state: "completed",
          ...(stopReason ? { stopReason } : {}),
        },
      });
      yield* updateSession(context, {
        status: "ready",
        activeTurnId: undefined,
        lastError: undefined,
      });
      yield* clearTurnState(context);
    });

    const startWatchdog = Effect.fn("startWatchdog")(function* (context: PiSessionContext) {
      if (context.watchdogFiber) {
        yield* Fiber.interrupt(context.watchdogFiber).pipe(Effect.ignore);
      }
      context.lastEventAtMs = yield* readWatchdogClockMs;
      context.watchdogFiber = yield* Effect.gen(function* () {
        yield* Effect.yieldNow;
        while (true) {
          yield* sleep(Duration.millis(turnSilenceReconcileMs));
          if (context.turnCompletedEmitted || !context.activeTurnId) {
            return;
          }
          const now = yield* readWatchdogClockMs;
          const silenceMs = now - context.lastEventAtMs;
          const silenceAction = evaluatePiWatchdogTick({
            turnCompletedEmitted: context.turnCompletedEmitted,
            activeTurnId: context.activeTurnId,
            silenceMs,
            turnSilenceHardMs,
            isStreaming: undefined,
          });
          if (silenceAction === "stop") {
            return;
          }
          if (silenceAction === "fail") {
            yield* failActiveTurn(
              context,
              "Pi turn stalled: no RPC events received within the configured timeout.",
            );
            return;
          }
          const state = yield* context.client
            .getState()
            .pipe(
              Effect.catch((error: unknown) =>
                failActiveTurn(
                  context,
                  providerAdapterErrorDetail(mapPiClientError("get_state", error)),
                ).pipe(Effect.as(null)),
              ),
            );
          if (!state) {
            return;
          }
          const reconcileAction = evaluatePiWatchdogTick({
            turnCompletedEmitted: context.turnCompletedEmitted,
            activeTurnId: context.activeTurnId,
            silenceMs,
            turnSilenceHardMs,
            isStreaming: state.isStreaming,
          });
          if (reconcileAction === "stop") {
            return;
          }
          if (reconcileAction === "fail") {
            yield* failActiveTurn(
              context,
              "Pi turn stalled: no RPC events received within the configured timeout.",
            );
            return;
          }
          if (reconcileAction === "complete") {
            yield* completeActiveTurn(context);
          }
        }
      }).pipe(Effect.forkIn(context.scope));
    });

    const markTurnEvent = (context: PiSessionContext): Effect.Effect<void> =>
      Effect.gen(function* () {
        context.lastEventAtMs = yield* readWatchdogClockMs;
      });

    const handleStreamEvent = Effect.fn("handleStreamEvent")(function* (
      context: PiSessionContext,
      event: PiRpcStreamEvent,
    ) {
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      yield* markTurnEvent(context);
      const eventType = readString(event.type);
      if (!eventType) {
        return;
      }

      yield* writeNativeEvent(context.session.threadId, event, {
        ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
        category: eventType,
      }).pipe(Effect.ignore);

      if (eventType === "extension_ui_request") {
        yield* handleExtensionUiRequest(context, event);
        return;
      }

      if (
        context.compactionWillRetry &&
        (eventType === "agent_start" ||
          eventType === "turn_start" ||
          eventType === "auto_retry_start")
      ) {
        // Compaction may emit a terminal agent_end before the retried attempt starts.
        // Clear the guard once Pi signals the retry cycle so the final completion can land.
        context.compactionWillRetry = false;
      }

      if (context.activeTurnId && !context.turnStartedEmitted) {
        if (eventType === "agent_start" || eventType === "turn_start") {
          context.turnPhase = "running";
          context.turnStartedEmitted = true;
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId: context.activeTurnId,
              raw: event,
            })),
            type: "turn.started",
            payload: {
              ...(context.activeModelSlug ? { model: context.activeModelSlug } : {}),
            },
          });
        }
      }

      if (eventType === "message_update") {
        yield* handleMessageUpdate(context, event);
        return;
      }

      if (eventType === "tool_execution_start") {
        yield* handleToolExecutionStart(context, event);
        return;
      }
      if (eventType === "tool_execution_update") {
        yield* handleToolExecutionUpdate(context, event);
        return;
      }
      if (eventType === "tool_execution_end") {
        yield* handleToolExecutionEnd(context, event);
        return;
      }

      if (eventType === "compaction_start") {
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId: context.activeTurnId,
            raw: event,
          })),
          type: "runtime.warning",
          payload: { message: "Pi context compaction started." },
        });
        return;
      }

      if (eventType === "compaction_end") {
        const willRetry = event.willRetry === true;
        context.compactionWillRetry = willRetry;
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId: context.activeTurnId,
            raw: event,
          })),
          type: "runtime.warning",
          payload: {
            message: willRetry
              ? "Pi context compaction finished; turn will retry."
              : "Pi context compaction finished.",
          },
        });
        return;
      }

      if (eventType === "auto_retry_start" || eventType === "auto_retry_end") {
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId: context.activeTurnId,
            raw: event,
          })),
          type: "runtime.warning",
          payload: {
            message:
              eventType === "auto_retry_start"
                ? "Pi auto-retry started."
                : "Pi auto-retry finished.",
          },
        });
        return;
      }

      if (eventType === "agent_end") {
        const stopReason = extractAssistantStopReason(event);
        yield* completeActiveTurn(context, { stopReason });
        return;
      }

      if (eventType === "turn_end" && !context.turnCompletedEmitted) {
        const state = yield* context.client.getState().pipe(Effect.option);
        if (Option.isNone(state) || state.value.isStreaming === false) {
          yield* completeActiveTurn(context, { stopReason: extractAssistantStopReason(event) });
        }
      }
    });

    const handleExtensionUiRequest = Effect.fn("handleExtensionUiRequest")(function* (
      context: PiSessionContext,
      event: PiRpcStreamEvent,
    ) {
      const method = readString(event.method);
      if (!method) {
        return;
      }
      if (FIRE_AND_FORGET_EXTENSION_UI_METHODS.has(method)) {
        const message = readString(event.message) ?? readString(event.statusText) ?? method;
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId: context.activeTurnId,
            raw: event,
            rawSource: "extension_ui",
          })),
          type: "runtime.warning",
          payload: { message },
        });
        return;
      }

      if (!BLOCKING_EXTENSION_UI_METHODS.has(method)) {
        yield* failActiveTurn(context, `Unsupported blocking Pi extension UI method '${method}'.`);
        const piRequestId = readString(event.id);
        if (piRequestId) {
          yield* context.client
            .sendExtensionUiResponse({ id: piRequestId, cancelled: true })
            .pipe(Effect.ignore);
        }
        return;
      }

      const questions = extensionUiQuestions(event);
      if (!questions) {
        yield* failActiveTurn(context, `Unable to map Pi extension UI request '${method}'.`);
        const piRequestId = readString(event.id);
        if (piRequestId) {
          yield* context.client
            .sendExtensionUiResponse({ id: piRequestId, cancelled: true })
            .pipe(Effect.ignore);
        }
        return;
      }

      const piRequestId = readString(event.id);
      if (!piRequestId) {
        yield* failActiveTurn(context, "Pi extension UI request missing id.");
        return;
      }

      const t3RequestId = ApprovalRequestId.make(piRequestId);
      const timeoutMs =
        typeof event.timeout === "number" && event.timeout > 0
          ? event.timeout
          : EXTENSION_UI_DEFAULT_TIMEOUT_MS;

      const timeoutFiber = yield* Effect.sleep(Duration.millis(timeoutMs)).pipe(
        Effect.andThen(
          Effect.gen(function* () {
            context.pendingExtensionUi.delete(t3RequestId);
            yield* context.client
              .sendExtensionUiResponse({ id: piRequestId, cancelled: true })
              .pipe(Effect.ignore);
            yield* failActiveTurn(
              context,
              "Pi extension UI request timed out before a response was received.",
            );
          }),
        ),
        Effect.forkScoped,
      );

      context.pendingExtensionUi.set(t3RequestId, {
        piRequestId,
        t3RequestId,
        method,
        timeoutFiber,
      });
      context.turnPhase = "waiting_for_user_input";

      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId: context.activeTurnId,
          requestId: t3RequestId,
          raw: event,
          rawSource: "extension_ui",
        })),
        type: "user-input.requested",
        payload: { questions: [...questions] },
      });
    });

    const handleMessageUpdate = Effect.fn("handleMessageUpdate")(function* (
      context: PiSessionContext,
      event: PiRpcStreamEvent,
    ) {
      const assistantMessageEvent = isRecord(event.assistantMessageEvent)
        ? event.assistantMessageEvent
        : undefined;
      if (!assistantMessageEvent) {
        return;
      }
      const updateType = readString(assistantMessageEvent.type);
      if (!updateType) {
        return;
      }

      const contentIndex =
        typeof assistantMessageEvent.contentIndex === "number"
          ? assistantMessageEvent.contentIndex
          : 0;
      const turnId = context.activeTurnId;

      if (updateType === "text_delta") {
        const delta = readContentString(assistantMessageEvent.delta);
        if (delta === undefined || !turnId) {
          return;
        }
        context.hasAssistantText = true;
        context.turnPhase = "streaming";
        const itemKey = `text:${contentIndex}`;
        yield* emitContentDelta(
          context,
          turnId,
          itemKey,
          "assistant_text",
          delta,
          contentIndex,
          event,
        );
        return;
      }

      if (updateType === "thinking_delta") {
        const delta = readContentString(assistantMessageEvent.delta);
        if (delta === undefined || !turnId) {
          return;
        }
        context.hasReasoningText = true;
        context.turnPhase = "streaming";
        const itemKey = `thinking:${contentIndex}`;
        yield* emitContentDelta(
          context,
          turnId,
          itemKey,
          "reasoning_text",
          delta,
          contentIndex,
          event,
        );
        return;
      }

      if (
        updateType === "toolcall_start" ||
        updateType === "toolcall_delta" ||
        updateType === "toolcall_end"
      ) {
        const toolName = readString(assistantMessageEvent.toolName) ?? "tool";
        const toolId = readString(assistantMessageEvent.toolCallId) ?? `toolcall-${contentIndex}`;
        if (context.toolExecutionIds.has(toolId)) {
          return;
        }
        context.toolcallIds.add(toolId);
        context.hasToolActivity = true;
        context.turnPhase = "streaming";
        const itemType = toToolLifecycleItemType(toolName);
        if (updateType === "toolcall_start") {
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              itemId: toolId,
              raw: event,
            })),
            type: "item.started",
            payload: {
              itemType,
              status: "inProgress",
              title: toolName,
            },
          });
        } else if (updateType === "toolcall_end") {
          yield* emit({
            ...(yield* buildEventBase({
              threadId: context.session.threadId,
              turnId,
              itemId: toolId,
              raw: event,
            })),
            type: "item.completed",
            payload: {
              itemType,
              status: "completed",
              title: toolName,
            },
          });
        }
      }
    });

    const emitContentDelta = Effect.fn("emitContentDelta")(function* (
      context: PiSessionContext,
      turnId: TurnId,
      itemKey: string,
      streamKind: "assistant_text" | "reasoning_text",
      delta: string,
      contentIndex: number,
      raw: unknown,
    ) {
      const previous = context.contentEmittedByKey.get(itemKey) ?? "";
      const next = `${previous}${delta}`;
      context.contentEmittedByKey.set(itemKey, next);
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId,
          itemId: itemKey,
          raw,
        })),
        type: "content.delta",
        payload: {
          streamKind,
          delta,
          contentIndex,
        },
      });
    });

    const handleToolExecutionStart = Effect.fn("handleToolExecutionStart")(function* (
      context: PiSessionContext,
      event: PiRpcStreamEvent,
    ) {
      const toolId = readString(event.toolCallId) ?? readString(event.id);
      if (!toolId || context.toolcallIds.has(toolId)) {
        return;
      }
      context.toolExecutionIds.add(toolId);
      context.hasToolActivity = true;
      context.turnPhase = "streaming";
      const toolName = readString(event.toolName) ?? readString(event.name) ?? "tool";
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId: context.activeTurnId,
          itemId: toolId,
          raw: event,
        })),
        type: "item.started",
        payload: {
          itemType: toCanonicalItemType(toolName),
          status: "inProgress",
          title: toolName,
          ...(readString(event.summary) ? { detail: readString(event.summary) } : {}),
          data: event,
        },
      });
    });

    const handleToolExecutionUpdate = Effect.fn("handleToolExecutionUpdate")(function* (
      context: PiSessionContext,
      event: PiRpcStreamEvent,
    ) {
      const toolId = readString(event.toolCallId) ?? readString(event.id);
      if (!toolId) {
        return;
      }
      context.hasToolActivity = true;
      const toolName = readString(event.toolName) ?? readString(event.name) ?? "tool";
      const output = readContentString(event.output) ?? readContentString(event.content);
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId: context.activeTurnId,
          itemId: toolId,
          raw: event,
        })),
        type: "item.updated",
        payload: {
          itemType: toCanonicalItemType(toolName),
          status: "inProgress",
          title: toolName,
          ...(output ? { detail: output } : {}),
          data: event,
        },
      });
      if (output && context.activeTurnId) {
        yield* emit({
          ...(yield* buildEventBase({
            threadId: context.session.threadId,
            turnId: context.activeTurnId,
            itemId: toolId,
            raw: event,
          })),
          type: "content.delta",
          payload: {
            streamKind: "command_output",
            delta: output,
          },
        });
      }
    });

    const handleToolExecutionEnd = Effect.fn("handleToolExecutionEnd")(function* (
      context: PiSessionContext,
      event: PiRpcStreamEvent,
    ) {
      const toolId = readString(event.toolCallId) ?? readString(event.id);
      if (!toolId || context.toolcallIds.has(toolId)) {
        return;
      }
      context.toolExecutionIds.add(toolId);
      context.hasToolActivity = true;
      const toolName = readString(event.toolName) ?? readString(event.name) ?? "tool";
      const isError = event.isError === true || event.success === false;
      yield* emit({
        ...(yield* buildEventBase({
          threadId: context.session.threadId,
          turnId: context.activeTurnId,
          itemId: toolId,
          raw: event,
        })),
        type: "item.completed",
        payload: {
          itemType: toCanonicalItemType(toolName),
          status: isError ? "failed" : "completed",
          title: toolName,
          ...(readString(event.summary) ? { detail: readString(event.summary) } : {}),
          data: event,
        },
      });
    });

    function extractAssistantStopReason(event: PiRpcStreamEvent): string | null {
      const message = isRecord(event.message) ? event.message : undefined;
      const stopReason = message ? readString(message.stopReason) : undefined;
      return stopReason ?? null;
    }

    const handleUnexpectedProcessExit = Effect.fn("handleUnexpectedProcessExit")(function* (
      context: PiSessionContext,
      detail: string,
    ) {
      if (yield* Ref.get(context.stopped)) {
        return;
      }
      yield* Ref.set(context.stopped, true);
      const threadId = context.session.threadId;
      sessions.delete(threadId);
      if (context.activeTurnId && !context.turnCompletedEmitted) {
        yield* failActiveTurn(context, detail);
      } else {
        yield* cancelPendingExtensionUi(context, detail);
      }
      yield* context.client.close().pipe(Effect.ignore);
      if (context.stderrFiber) {
        yield* Fiber.interrupt(context.stderrFiber).pipe(Effect.ignore);
      }
      yield* Fiber.interrupt(context.eventFiber).pipe(Effect.ignore);
      yield* Scope.close(context.scope, Exit.void).pipe(Effect.ignore);
      yield* emit({
        ...(yield* buildEventBase({ threadId })),
        type: "session.exited",
        payload: { reason: detail, exitKind: "error", recoverable: true },
      });
    });

    const stopPiContext = Effect.fn("stopPiContext")(function* (context: PiSessionContext) {
      if (yield* Ref.getAndSet(context.stopped, true)) {
        return;
      }
      const turnId = context.activeTurnId;
      if (turnId && !context.turnCompletedEmitted) {
        yield* failActiveTurn(context, "Pi session stopped.", { aborted: true });
      }
      yield* context.client.close().pipe(Effect.ignore);
      if (context.stderrFiber) {
        yield* Fiber.interrupt(context.stderrFiber).pipe(Effect.ignore);
      }
      yield* Fiber.interrupt(context.eventFiber).pipe(Effect.ignore);
      yield* Scope.close(context.scope, Exit.void).pipe(Effect.ignore);
    });

    const spawnSessionClient = Effect.fn("spawnSessionClient")(function* (input: {
      readonly threadId: ThreadId;
      readonly cwd: string;
    }) {
      const sessionScope = yield* Scope.make();
      const client = yield* makeRpcClient({
        binaryPath: piSettings.binaryPath,
        cwd: input.cwd,
        noSession: true,
        ...(options?.environment ? { spawnEnv: options.environment } : {}),
      }).pipe(
        Effect.provideService(Scope.Scope, sessionScope),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, childProcessSpawner),
        Effect.mapError(
          (error) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: error.message,
              cause: error,
            }),
        ),
      );
      return { sessionScope, client } satisfies {
        readonly sessionScope: Scope.Closeable;
        readonly client: PiRpcClientShape;
      };
    });

    const validateModelSelection = Effect.fn("validateModelSelection")(function* (
      context: PiSessionContext,
      modelSlug: string | undefined,
    ) {
      if (!modelSlug) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Pi turns require a model selection from the current provider snapshot.",
        });
      }
      const identity = yield* decodePiModelSlug(modelSlug).pipe(
        Effect.mapError(
          (error) =>
            new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: error.message,
            }),
        ),
      );
      const available = yield* context.client
        .getAvailableModels()
        .pipe(Effect.mapError((error) => mapPiClientError("get_available_models", error)));
      const matched = available.models.find(
        (model) => model.provider === identity.provider && model.id === identity.modelId,
      );
      if (!matched) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `Selected Pi model '${modelSlug}' is not available in the current runtime.`,
        });
      }
      return { identity, model: matched, slug: modelSlug } as const;
    });

    const applyModelAndThinking = Effect.fn("applyModelAndThinking")(function* (
      context: PiSessionContext,
      model: PiRpcModel,
      slug: string,
      modelSelection: ProviderSendTurnInput["modelSelection"],
    ) {
      if (context.activeModelSlug !== slug) {
        yield* context.client
          .setModel({ provider: model.provider, modelId: model.id })
          .pipe(Effect.mapError((error) => mapPiClientError("set_model", error)));
        context.activeModelSlug = slug;
      }
      const requestedThinking = getModelSelectionStringOptionValue(modelSelection, "thinkingLevel");
      if (requestedThinking) {
        const resolved = resolvePiThinkingLevelForModel(
          model,
          requestedThinking as PiThinkingLevel,
        );
        if (resolved) {
          yield* context.client
            .setThinkingLevel(resolved)
            .pipe(Effect.mapError((error) => mapPiClientError("set_thinking_level", error)));
        }
      }
    });

    const buildPromptMessage = Effect.fn("buildPromptMessage")(function* (
      threadId: ThreadId,
      input: ProviderSendTurnInput,
    ) {
      const text = input.input?.trim() ?? "";
      const attachmentLines: string[] = [];
      for (const attachment of input.attachments ?? []) {
        const attachmentPath = resolveAttachmentPath({
          attachmentsDir: serverConfig.attachmentsDir,
          attachment,
        });
        if (!attachmentPath) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: `Invalid attachment id '${attachment.id}'.`,
          });
        }
        attachmentLines.push(attachmentPath);
      }
      if (text.length === 0 && attachmentLines.length === 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: "Pi turns require text input or at least one attachment.",
        });
      }
      if (attachmentLines.length === 0) {
        return text;
      }
      const attachmentBlock = ["[Attached files]", ...attachmentLines].join("\n");
      return text.length > 0 ? `${text}\n\n${attachmentBlock}` : attachmentBlock;
    });

    const startSession = (input: ProviderSessionStartInput) =>
      Effect.scoped(
        Effect.gen(function* () {
          const existing = sessions.get(input.threadId);
          if (existing) {
            yield* stopPiContext(existing);
            sessions.delete(input.threadId);
          }

          const directory = input.cwd ?? serverConfig.cwd;
          const staleResumeCursor = input.resumeCursor !== undefined && input.resumeCursor !== null;
          if (staleResumeCursor) {
            yield* Effect.logWarning(
              `Ignoring stale Pi resumeCursor for thread ${input.threadId}; starting fresh ephemeral runtime.`,
            );
          }

          const spawned = yield* spawnSessionClient({
            threadId: input.threadId,
            cwd: directory,
          });
          let sessionScopeTransferred = false;
          yield* Effect.addFinalizer(() =>
            sessionScopeTransferred
              ? Effect.void
              : Scope.close(spawned.sessionScope, Exit.void).pipe(Effect.ignore),
          );

          const createdAt = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd: directory,
            ...(input.modelSelection ? { model: input.modelSelection.model } : {}),
            threadId: input.threadId,
            createdAt,
            updatedAt: createdAt,
          };

          const commandLock = yield* Semaphore.make(1);
          const stopped = yield* Ref.make(false);
          const startedAtMs = yield* Clock.currentTimeMillis;
          const context: PiSessionContext = {
            session,
            scope: spawned.sessionScope,
            client: spawned.client,
            commandLock,
            eventFiber: undefined as unknown as Fiber.Fiber<void, never>,
            stderrFiber: undefined,
            stopped,
            activeTurnId: undefined,
            turnPhase: "idle",
            turnStartedEmitted: false,
            turnCompletedEmitted: false,
            hasAssistantText: false,
            hasReasoningText: false,
            hasToolActivity: false,
            activeModelSlug: undefined,
            turns: [],
            turnItems: new Map(),
            contentEmittedByKey: new Map(),
            toolExecutionIds: new Set(),
            toolcallIds: new Set(),
            pendingExtensionUi: new Map(),
            watchdogFiber: undefined,
            lastEventAtMs: startedAtMs,
            compactionWillRetry: false,
          };

          const dispatchStreamEvent = (event: PiRpcStreamEvent) =>
            handleStreamEvent(context, event).pipe(
              Effect.provideService(Scope.Scope, spawned.sessionScope),
              Effect.catchCause((cause) =>
                failActiveTurn(context, Cause.pretty(cause)).pipe(Effect.asVoid),
              ),
              Effect.asVoid,
            );

          type PiRpcClientWithDirectDispatch = PiRpcClientShape & {
            readonly setStreamEventHandler?: (
              handler: (event: PiRpcStreamEvent) => Effect.Effect<void>,
            ) => Effect.Effect<void>;
          };

          const clientWithDispatch = spawned.client as PiRpcClientWithDirectDispatch;
          const supportsDirectDispatch =
            typeof clientWithDispatch.setStreamEventHandler === "function";

          if (supportsDirectDispatch) {
            yield* clientWithDispatch.setStreamEventHandler!(dispatchStreamEvent);
            context.eventFiber = yield* Effect.void.pipe(Effect.forkIn(spawned.sessionScope));
          } else {
            context.eventFiber = yield* Stream.runForEach(
              spawned.client.streamEvents,
              dispatchStreamEvent,
            ).pipe(
              Effect.ensuring(
                handleUnexpectedProcessExit(context, "Pi RPC process exited unexpectedly.").pipe(
                  Effect.ignore,
                ),
              ),
              Effect.forkIn(spawned.sessionScope),
            );
          }

          context.stderrFiber = yield* spawned.client.stderrLines.pipe(
            Stream.runForEach((line) =>
              Effect.gen(function* () {
                if (yield* Ref.get(context.stopped)) {
                  return;
                }
                const trimmed = line.trim();
                if (trimmed.length === 0) {
                  return;
                }
                yield* writeNativeEvent(
                  context.session.threadId,
                  { type: "stderr", line: trimmed },
                  {
                    ...(context.activeTurnId ? { turnId: context.activeTurnId } : {}),
                    category: "stderr",
                  },
                ).pipe(Effect.ignore);
                yield* emit({
                  ...(yield* buildEventBase({
                    threadId: context.session.threadId,
                    turnId: context.activeTurnId,
                    raw: { type: "stderr", line: trimmed },
                    rawSource: "stderr",
                  })),
                  type: "runtime.warning",
                  payload: { message: trimmed },
                });
              }),
            ),
            Effect.forkIn(spawned.sessionScope),
          );

          sessions.set(input.threadId, context);
          sessionScopeTransferred = true;

          yield* emit({
            ...(yield* buildEventBase({ threadId: input.threadId })),
            type: "session.started",
            payload: staleResumeCursor ? { message: PI_FRESH_RUNTIME_WARNING } : {},
          });
          yield* emit({
            ...(yield* buildEventBase({ threadId: input.threadId })),
            type: "thread.started",
            payload: {},
          });
          if (staleResumeCursor) {
            yield* emit({
              ...(yield* buildEventBase({ threadId: input.threadId })),
              type: "runtime.warning",
              payload: { message: PI_FRESH_RUNTIME_WARNING },
            });
          }

          return session;
        }),
      );

    const sendTurn = Effect.fn("sendTurn")(function* (input: ProviderSendTurnInput) {
      const context = yield* requireSession(input.threadId, "sendTurn");
      if (context.activeTurnId && context.turnPhase !== "idle") {
        const streaming = yield* context.client.getState().pipe(
          Effect.map((state) => state.isStreaming === true),
          Effect.catch(() => Effect.succeed(true)),
        );
        if (streaming) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue:
              "Pi already has an active turn for this thread. Interrupt it before sending another message.",
          });
        }
      }

      const turnId = TurnId.make(yield* Random.nextUUIDv4);
      const modelSelection = input.modelSelection ?? {
        model: context.session.model ?? "",
        instanceId: boundInstanceId,
      };
      if (modelSelection.instanceId && modelSelection.instanceId !== boundInstanceId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `Pi model selection is bound to instance '${modelSelection.instanceId}', expected '${boundInstanceId}'.`,
        });
      }

      const validated = yield* validateModelSelection(context, modelSelection.model);
      const promptMessage = yield* buildPromptMessage(input.threadId, input);

      context.activeTurnId = turnId;
      context.turnPhase = "prompt_pending";
      context.turnStartedEmitted = false;
      context.turnCompletedEmitted = false;
      context.hasAssistantText = false;
      context.hasReasoningText = false;
      context.hasToolActivity = false;
      context.contentEmittedByKey.clear();
      context.toolExecutionIds.clear();
      context.toolcallIds.clear();
      context.compactionWillRetry = false;

      yield* updateSession(context, {
        status: "running",
        activeTurnId: turnId,
        model: validated.slug,
        lastError: undefined,
      });

      yield* context.commandLock
        .withPermits(1)(
          Effect.gen(function* () {
            yield* applyModelAndThinking(context, validated.model, validated.slug, modelSelection);
            yield* context.client
              .prompt({ message: promptMessage })
              .pipe(Effect.mapError((error) => mapPiClientError("prompt", error)));
          }),
        )
        .pipe(
          Effect.catch((error: ProviderAdapterError) =>
            Effect.gen(function* () {
              yield* failActiveTurn(context, providerAdapterErrorDetail(error));
              return yield* error;
            }),
          ),
        );

      context.turnPhase = "accepted";
      context.turns.push({ id: turnId, items: [] });
      context.turnItems.set(turnId, []);
      yield* startWatchdog(context);

      return {
        threadId: input.threadId,
        turnId,
      };
    });

    const interruptTurn = Effect.fn("interruptTurn")(function* (
      threadId: ThreadId,
      turnId?: TurnId,
    ) {
      const context = yield* requireSession(threadId, "interruptTurn");
      if (turnId && context.activeTurnId && context.activeTurnId !== turnId) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "interruptTurn",
          issue: `No active turn '${turnId}' for thread '${threadId}'.`,
        });
      }
      yield* context.commandLock.withPermits(1)(
        context.client.abort().pipe(Effect.mapError((error) => mapPiClientError("abort", error))),
      );
      yield* failActiveTurn(context, "Turn interrupted.", { aborted: true });
    });

    const respondToRequest = Effect.fn("respondToRequest")(function* () {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "respondToRequest",
        issue: "Pi does not support T3 approval requests in v1.",
      });
    });

    const respondToUserInput = Effect.fn("respondToUserInput")(function* (
      threadId: ThreadId,
      requestId: ApprovalRequestId,
      answers: ProviderUserInputAnswers,
    ) {
      const context = yield* requireSession(threadId, "respondToUserInput");
      const pending = context.pendingExtensionUi.get(requestId);
      if (!pending) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToUserInput",
          issue: `No pending Pi extension UI request '${requestId}'.`,
        });
      }
      context.pendingExtensionUi.delete(requestId);
      if (pending.timeoutFiber) {
        yield* Fiber.interrupt(pending.timeoutFiber).pipe(Effect.ignore);
      }

      const answer =
        answers[requestId] ?? answers[pending.piRequestId] ?? Object.values(answers)[0];
      let response: Record<string, unknown> = { id: pending.piRequestId };
      switch (pending.method) {
        case "select":
          response = { ...response, value: answer ?? "" };
          break;
        case "confirm":
          response = {
            ...response,
            confirmed: typeof answer === "string" && answer.toLowerCase().startsWith("y"),
          };
          break;
        case "input":
        case "editor":
          response = answer ? { ...response, value: answer } : { ...response, cancelled: true };
          break;
        default:
          response = { ...response, cancelled: true };
          break;
      }

      yield* context.client
        .sendExtensionUiResponse(response)
        .pipe(Effect.mapError((error) => mapPiClientError("extension_ui_response", error)));

      yield* emit({
        ...(yield* buildEventBase({
          threadId,
          turnId: context.activeTurnId,
          requestId,
        })),
        type: "user-input.resolved",
        payload: { answers },
      });

      if (context.turnPhase === "waiting_for_user_input") {
        context.turnPhase = "streaming";
      }
    });

    const stopSession = Effect.fn("stopSession")(function* (threadId: ThreadId) {
      const context = sessions.get(threadId);
      if (!context) {
        return;
      }
      sessions.delete(threadId);
      yield* stopPiContext(context);
      yield* emit({
        ...(yield* buildEventBase({ threadId })),
        type: "session.exited",
        payload: { reason: "Session stopped.", exitKind: "graceful", recoverable: true },
      });
    });

    const readThread = Effect.fn("readThread")(function* (threadId: ThreadId) {
      const context = yield* requireSession(threadId, "readThread");
      const turns: ReadonlyArray<ProviderThreadTurnSnapshot> = context.turns.map((turn) => ({
        id: turn.id,
        items: [...(context.turnItems.get(turn.id) ?? turn.items)],
      }));
      return {
        threadId,
        turns,
      } satisfies ProviderThreadSnapshot;
    });

    const rollbackThread = Effect.fn("rollbackThread")(function* () {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "rollbackThread",
        issue: "Pi thread rollback is not supported in v1.",
      });
    });

    const stopAll = Effect.fn("stopAll")(function* () {
      const contexts = [...sessions.values()];
      sessions.clear();
      yield* Effect.forEach(contexts, (context) => stopPiContext(context), {
        concurrency: "unbounded",
        discard: true,
      });
    });

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions: () => Effect.succeed([...sessions.values()].map((context) => context.session)),
      hasSession: (threadId) => Effect.succeed(sessions.has(threadId)),
      readThread,
      rollbackThread,
      stopAll,
      get streamEvents() {
        return Stream.fromQueue(runtimeEvents);
      },
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
}

// Re-export for tests that need slug encoding in fixtures.
export { encodePiModelSlug };
