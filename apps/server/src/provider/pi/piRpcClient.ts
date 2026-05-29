import type { ProviderInstanceEnvironment } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { piChildProcessSpawnOptions } from "./piSpawnOptions.ts";
import {
  makeChildStdio,
  makePiRpcProtocol,
  type PiRpcProtocolError,
  PiRpcProtocolCommandFailedError,
  PiRpcProtocolProcessExitedError,
  PiRpcProtocolTransportError,
} from "./piRpcProtocol.ts";
import {
  PiExtensionUiResponse,
  PiRpcAvailableModels,
  PiRpcCommands,
  PiRpcModel,
  PiRpcState,
  PiThinkingLevel,
  type PiRpcStreamEvent,
} from "./piRpcTypes.ts";

export interface PiRpcClientOptions {
  readonly binaryPath: string;
  readonly cwd: string;
  readonly environment?: ProviderInstanceEnvironment;
  /** Pre-merged process env for probes and other callers that already resolved instance env. */
  readonly spawnEnv?: NodeJS.ProcessEnv;
  /** When true (default), spawn with `--no-session` for ephemeral runtime/probe clients. */
  readonly noSession?: boolean;
  readonly requestTimeoutMs?: number;
  readonly forceKillAfter?: Duration.Input;
  readonly piVersion?: string;
}

export type PiRpcClientError = PiRpcProtocolError | PiRpcClientSpawnError;

export class PiRpcClientSpawnError extends Schema.TaggedErrorClass<PiRpcClientSpawnError>()(
  "PiRpcClientSpawnError",
  {
    command: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Failed to spawn Pi RPC process: ${this.command}`;
  }
}

const mapSchemaDecodeError =
  (command: string) =>
  (cause: unknown): PiRpcProtocolTransportError =>
    new PiRpcProtocolTransportError({
      detail: `Failed to decode Pi RPC '${command}' response data`,
      cause,
    });

const decodePiState = (data: unknown): Effect.Effect<PiRpcState, PiRpcProtocolTransportError> =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(PiRpcState)(data),
    catch: mapSchemaDecodeError("get_state"),
  });

const decodePiAvailableModels = (
  data: unknown,
): Effect.Effect<PiRpcAvailableModels, PiRpcProtocolTransportError> =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(PiRpcAvailableModels)(data),
    catch: mapSchemaDecodeError("get_available_models"),
  });

const decodePiCommands = (
  data: unknown,
): Effect.Effect<PiRpcCommands, PiRpcProtocolTransportError> =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(PiRpcCommands)(data),
    catch: mapSchemaDecodeError("get_commands"),
  });

const decodePiModel = (data: unknown): Effect.Effect<PiRpcModel, PiRpcProtocolTransportError> =>
  Effect.try({
    try: () => Schema.decodeUnknownSync(PiRpcModel)(data),
    catch: mapSchemaDecodeError("set_model"),
  });

export interface PiRpcClientShape {
  readonly piVersion: string | undefined;
  readonly streamEvents: Stream.Stream<PiRpcStreamEvent, never>;
  readonly stderrLines: Stream.Stream<string, never>;
  readonly getState: () => Effect.Effect<PiRpcState, PiRpcClientError>;
  readonly getAvailableModels: () => Effect.Effect<PiRpcAvailableModels, PiRpcClientError>;
  readonly getCommands: () => Effect.Effect<PiRpcCommands, PiRpcClientError>;
  readonly setModel: (input: {
    readonly provider: string;
    readonly modelId: string;
  }) => Effect.Effect<PiRpcModel, PiRpcClientError>;
  readonly setThinkingLevel: (level: PiThinkingLevel) => Effect.Effect<void, PiRpcClientError>;
  readonly prompt: (input: { readonly message: string }) => Effect.Effect<void, PiRpcClientError>;
  readonly abort: () => Effect.Effect<void, PiRpcClientError>;
  readonly sendExtensionUiResponse: (
    response: PiExtensionUiResponse,
  ) => Effect.Effect<void, PiRpcClientError>;
  readonly close: () => Effect.Effect<void>;
}

export const makePiRpcClient = Effect.fn("makePiRpcClient")(function* (
  options: PiRpcClientOptions,
): Effect.fn.Return<
  PiRpcClientShape,
  PiRpcClientSpawnError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
> {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runtimeScope = yield* Scope.Scope;
  const closedRef = yield* Ref.make(false);

  const useNoSession = options.noSession !== false;
  const args = ["--mode", "rpc", ...(useNoSession ? ["--no-session"] : [])];
  const commandLabel = `${options.binaryPath} ${args.join(" ")}`;
  const env = options.spawnEnv ?? mergeProviderInstanceEnvironment(options.environment);

  const child = yield* spawner
    .spawn(
      ChildProcess.make(
        options.binaryPath,
        args,
        piChildProcessSpawnOptions({
          cwd: options.cwd,
          env,
          ...(options.forceKillAfter !== undefined
            ? { forceKillAfter: options.forceKillAfter }
            : {}),
        }),
      ),
    )
    .pipe(
      Effect.provideService(Scope.Scope, runtimeScope),
      Effect.mapError(
        (cause) =>
          new PiRpcClientSpawnError({
            command: commandLabel,
            cause,
          }),
      ),
    );

  const protocol = yield* makePiRpcProtocol({
    stdio: makeChildStdio(child),
    stderr: child.stderr,
    ...(options.requestTimeoutMs !== undefined
      ? { requestTimeoutMs: options.requestTimeoutMs }
      : {}),
    onNonJsonStdoutLine: () => Effect.void,
  });

  yield* child.exitCode.pipe(
    Effect.flatMap((code) => {
      const numericCode = Number(code);
      return protocol.failAllPending(
        new PiRpcProtocolProcessExitedError({
          detail: `Pi RPC process exited (code: ${String(numericCode)})`,
          exitCode: Number.isFinite(numericCode) ? numericCode : undefined,
        }),
      );
    }),
    Effect.ignore,
    Effect.forkIn(runtimeScope),
  );

  const forceKillAfter = options.forceKillAfter ?? "2 seconds";
  const terminateChild = child.kill({ killSignal: "SIGTERM" }).pipe(
    Effect.andThen(
      child.kill({
        killSignal: "SIGKILL",
        forceKillAfter,
      }),
    ),
    Effect.ignore,
  );

  const closeClient = Effect.gen(function* () {
    const closed = yield* Ref.get(closedRef);
    if (closed) {
      return;
    }
    yield* Ref.set(closedRef, true);
    yield* protocol.failAllPending(
      new PiRpcProtocolProcessExitedError({
        detail: "Pi RPC client closed",
      }),
    );
    yield* terminateChild;
  });

  yield* Scope.addFinalizer(runtimeScope, closeClient);

  const sendCommand = protocol.sendCommand;

  return {
    piVersion: options.piVersion,
    streamEvents: protocol.streamEvents,
    stderrLines: protocol.stderrLines,
    getState: () => sendCommand("get_state", {}, decodePiState),
    getAvailableModels: () => sendCommand("get_available_models", {}, decodePiAvailableModels),
    getCommands: () => sendCommand("get_commands", {}, decodePiCommands),
    setModel: (input) =>
      sendCommand(
        "set_model",
        {
          provider: input.provider,
          modelId: input.modelId,
        },
        decodePiModel,
      ),
    setThinkingLevel: (level) =>
      sendCommand("set_thinking_level", { level }, () => Effect.void).pipe(Effect.asVoid),
    prompt: (input) => sendCommand("prompt", { message: input.message }, () => Effect.void),
    abort: () => sendCommand("abort", {}, () => Effect.void),
    sendExtensionUiResponse: (response) =>
      protocol.sendRaw({
        type: "extension_ui_response",
        ...response,
      }),
    close: () => closeClient,
  } satisfies PiRpcClientShape;
});

export {
  PiRpcProtocolCommandFailedError,
  PiRpcProtocolProcessExitedError,
  PiRpcProtocolTransportError,
};
