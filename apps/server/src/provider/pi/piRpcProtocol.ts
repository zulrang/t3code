import * as Cause from "effect/Cause";
import type * as Scope from "effect/Scope";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Duration from "effect/Duration";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";

import {
  encodeJsonlLine,
  parseJsonlLine,
  splitJsonlChunk,
  type PiJsonlParseError,
} from "./piJsonl.ts";
import {
  isPiRpcResponseEnvelope,
  type PiRpcResponseEnvelope,
  type PiRpcStreamEvent,
} from "./piRpcTypes.ts";

const encoder = new TextEncoder();

export class PiRpcProtocolTransportError extends Schema.TaggedErrorClass<PiRpcProtocolTransportError>()(
  "PiRpcProtocolTransportError",
  {
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export class PiRpcProtocolProcessExitedError extends Schema.TaggedErrorClass<PiRpcProtocolProcessExitedError>()(
  "PiRpcProtocolProcessExitedError",
  {
    detail: Schema.String,
    exitCode: Schema.optional(Schema.Number),
    signal: Schema.optional(Schema.String),
  },
) {}

export class PiRpcProtocolRequestTimeoutError extends Schema.TaggedErrorClass<PiRpcProtocolRequestTimeoutError>()(
  "PiRpcProtocolRequestTimeoutError",
  {
    command: Schema.String,
    requestId: Schema.String,
    timeoutMs: Schema.Number,
  },
) {}

export class PiRpcProtocolCommandFailedError extends Schema.TaggedErrorClass<PiRpcProtocolCommandFailedError>()(
  "PiRpcProtocolCommandFailedError",
  {
    command: Schema.String,
    requestId: Schema.optional(Schema.String),
    error: Schema.String,
  },
) {}

export class PiRpcProtocolStdinError extends Schema.TaggedErrorClass<PiRpcProtocolStdinError>()(
  "PiRpcProtocolStdinError",
  {
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export type PiRpcProtocolError =
  | PiRpcProtocolTransportError
  | PiRpcProtocolProcessExitedError
  | PiRpcProtocolRequestTimeoutError
  | PiRpcProtocolCommandFailedError
  | PiRpcProtocolStdinError
  | PiJsonlParseError;

export interface PiRpcProtocolOptions {
  readonly stdio: Stdio.Stdio;
  readonly stderr?: Stream.Stream<Uint8Array, unknown>;
  readonly requestTimeoutMs?: number;
  readonly onNonJsonStdoutLine?: (line: string) => Effect.Effect<void, never>;
  readonly onTermination?: (error: PiRpcProtocolProcessExitedError) => Effect.Effect<void, never>;
}

export interface PiRpcProtocolShape {
  readonly streamEvents: Stream.Stream<PiRpcStreamEvent, never>;
  readonly stderrLines: Stream.Stream<string, never>;
  readonly sendCommand: <T>(
    command: string,
    payload: Record<string, unknown>,
    decodeData: (data: unknown) => Effect.Effect<T, PiRpcProtocolError>,
  ) => Effect.Effect<T, PiRpcProtocolError>;
  readonly sendRaw: (
    payload: Record<string, unknown>,
  ) => Effect.Effect<void, PiRpcProtocolStdinError>;
  readonly failAllPending: (error: PiRpcProtocolProcessExitedError) => Effect.Effect<void>;
}

export const makePiRpcProtocol = Effect.fn("makePiRpcProtocol")(function* (
  options: PiRpcProtocolOptions,
): Effect.fn.Return<PiRpcProtocolShape, never, Scope.Scope> {
  const requestTimeoutMs = options.requestTimeoutMs ?? 120_000;
  const requestTimeout = Duration.millis(requestTimeoutMs);

  const pending = yield* Ref.make(
    new Map<string, Deferred.Deferred<PiRpcResponseEnvelope, PiRpcProtocolError>>(),
  );
  const nextRequestId = yield* Ref.make(1);
  const remainder = yield* Ref.make("");
  const streamEvents = yield* Queue.unbounded<PiRpcStreamEvent>();
  const stderrLines = yield* Queue.unbounded<string>();
  const terminated = yield* Ref.make(false);

  const failAllPending = (error: PiRpcProtocolProcessExitedError) =>
    Ref.get(pending).pipe(
      Effect.flatMap((current) =>
        Effect.forEach(
          [...current.entries()],
          ([requestId, deferred]) => Deferred.fail(deferred, error).pipe(Effect.as(requestId)),
          { discard: true },
        ),
      ),
      Effect.andThen(Ref.set(pending, new Map())),
      Effect.andThen(Queue.shutdown(streamEvents)),
    );

  const handleTermination = (error: PiRpcProtocolProcessExitedError) =>
    Ref.get(terminated).pipe(
      Effect.flatMap((alreadyTerminated) => {
        if (alreadyTerminated) {
          return Effect.void;
        }
        return Ref.set(terminated, true).pipe(
          Effect.andThen(failAllPending(error)),
          Effect.andThen(options.onTermination ? options.onTermination(error) : Effect.void),
        );
      }),
    );

  const resolveResponse = (response: PiRpcResponseEnvelope) => {
    const requestId = response.id;
    if (requestId === undefined) {
      return Effect.void;
    }
    return Ref.modify(pending, (current) => {
      const deferred = current.get(requestId);
      if (!deferred) {
        return [Effect.void, current] as const;
      }
      const next = new Map(current);
      next.delete(requestId);
      return [Deferred.succeed(deferred, response), next] as const;
    }).pipe(Effect.flatten);
  };

  const handleParsedMessage = (message: unknown): Effect.Effect<void, PiRpcProtocolError> => {
    if (isPiRpcResponseEnvelope(message)) {
      return resolveResponse(message);
    }
    if (typeof message === "object" && message !== null) {
      return Queue.offer(streamEvents, message as PiRpcStreamEvent).pipe(Effect.asVoid);
    }
    return Effect.void;
  };

  const handleLine = (line: string): Effect.Effect<void, PiRpcProtocolError> => {
    if (line.trim().length === 0) {
      return Effect.void;
    }
    return parseJsonlLine(line).pipe(
      Effect.catchTag("PiJsonlParseError", () =>
        options.onNonJsonStdoutLine
          ? options.onNonJsonStdoutLine(line).pipe(Effect.asVoid)
          : Effect.void,
      ),
      Effect.flatMap(handleParsedMessage),
    );
  };

  const flushRemainder = Effect.gen(function* () {
    const line = yield* Ref.get(remainder);
    if (line.trim().length === 0) {
      return;
    }
    yield* handleLine(line);
    yield* Ref.set(remainder, "");
  });

  if (options.stderr) {
    yield* attachStderrReader(options.stderr, stderrLines);
  }

  yield* options.stdio.stdin.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Ref.modify(remainder, (current) => {
        const split = splitJsonlChunk(current, chunk);
        return [split.lines, split.remainder] as const;
      }).pipe(Effect.flatMap((lines) => Effect.forEach(lines, handleLine, { discard: true }))),
    ),
    Effect.matchEffect({
      onFailure: (cause) =>
        handleTermination(
          new PiRpcProtocolProcessExitedError({
            detail: `Pi RPC stdout stream failed: ${String(cause)}`,
          }),
        ),
      onSuccess: () =>
        flushRemainder.pipe(
          Effect.matchEffect({
            onFailure: () =>
              handleTermination(
                new PiRpcProtocolProcessExitedError({
                  detail: "Pi RPC stdout flush failed",
                }),
              ),
            onSuccess: () =>
              handleTermination(
                new PiRpcProtocolProcessExitedError({
                  detail: "Pi RPC stdout stream ended",
                }),
              ),
          }),
        ),
    }),
    Effect.forkScoped,
  );

  const writeToStdin = (payload: string) =>
    Stream.fromIterable([encoder.encode(payload)]).pipe(
      Stream.run(options.stdio.stdout()),
      Effect.mapError(
        (cause) =>
          new PiRpcProtocolStdinError({
            detail: "Failed to write Pi RPC command to stdin",
            cause,
          }),
      ),
    );

  const allocateRequestId = Ref.modify(
    nextRequestId,
    (current) => [`t3-pi-${String(current)}`, current + 1] as const,
  );

  const sendRaw = (payload: Record<string, unknown>) => writeToStdin(encodeJsonlLine(payload));

  const sendCommand = <T>(
    command: string,
    payload: Record<string, unknown>,
    decodeData: (data: unknown) => Effect.Effect<T, PiRpcProtocolError>,
  ): Effect.Effect<T, PiRpcProtocolError> =>
    Effect.gen(function* () {
      const requestId = yield* allocateRequestId;
      const deferred = yield* Deferred.make<PiRpcResponseEnvelope, PiRpcProtocolError>();
      yield* Ref.update(pending, (current) => new Map(current).set(requestId, deferred));
      yield* sendRaw({
        ...payload,
        type: command,
        id: requestId,
      }).pipe(
        Effect.catch((error) =>
          Ref.update(pending, (current) => {
            const next = new Map(current);
            next.delete(requestId);
            return next;
          }).pipe(Effect.andThen(Effect.fail(error))),
        ),
      );

      const timeoutError = new PiRpcProtocolRequestTimeoutError({
        command,
        requestId,
        timeoutMs: requestTimeoutMs,
      });

      const response = yield* Effect.raceFirst(
        Deferred.await(deferred).pipe(
          Effect.onInterrupt(() =>
            Ref.update(pending, (current) => {
              const next = new Map(current);
              next.delete(requestId);
              return next;
            }),
          ),
        ),
        Effect.sleep(requestTimeout).pipe(
          Effect.andThen(
            Ref.update(pending, (current) => {
              const next = new Map(current);
              next.delete(requestId);
              return next;
            }),
          ),
          Effect.andThen(Effect.fail(timeoutError)),
        ),
      );

      if (!response.success) {
        return yield* new PiRpcProtocolCommandFailedError({
          command,
          requestId: response.id,
          error: response.error ?? "Pi RPC command failed",
        });
      }

      return yield* decodeData(response.data);
    });

  return {
    streamEvents: Stream.fromQueue(streamEvents),
    stderrLines: Stream.fromQueue(stderrLines),
    sendCommand,
    sendRaw,
    failAllPending,
  } satisfies PiRpcProtocolShape;
});

export const makeInMemoryPiStdio = Effect.fn("makeInMemoryPiStdio")(function* () {
  const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
  const output = yield* Queue.unbounded<string>();
  const decoder = new TextDecoder();

  const stdio = Stdio.make({
    args: Effect.succeed([]),
    stdin: Stream.fromQueue(input),
    stdout: () =>
      Sink.forEach((chunk: string | Uint8Array) =>
        Queue.offer(
          output,
          typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true }),
        ),
      ),
    stderr: () => Sink.drain,
  });

  return { stdio, input, output } as const;
});

const attachStderrReader = (
  stderr: Stream.Stream<Uint8Array, unknown>,
  stderrLines: Queue.Queue<string>,
): Effect.Effect<void, never, Scope.Scope> =>
  stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) => {
      const lines = chunk.split(/\r?\n/g).filter((line) => line.length > 0);
      return Effect.forEach(lines, (line) => Queue.offer(stderrLines, line), { discard: true });
    }),
    Effect.catch(() => Effect.void),
    Effect.forkScoped,
  );

export const makeChildStdio = (
  handle: import("effect/unstable/process").ChildProcessSpawner.ChildProcessHandle,
) =>
  Stdio.make({
    args: Effect.succeed([]),
    stdin: handle.stdout,
    stdout: () =>
      Sink.mapInput(handle.stdin, (chunk: string | Uint8Array) =>
        typeof chunk === "string" ? encoder.encode(chunk) : chunk,
      ),
    stderr: () => Sink.drain,
  });
