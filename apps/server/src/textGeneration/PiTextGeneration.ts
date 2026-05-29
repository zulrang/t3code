import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { TextGenerationError, type ModelSelection, type PiSettings } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import { decodePiModelSlug } from "../provider/pi/piModelSlug.ts";
import { resolvePiThinkingLevelForModel } from "../provider/pi/piModelMapping.ts";
import {
  makePiRpcClient,
  PiRpcClientSpawnError,
  type PiRpcClientShape,
} from "../provider/pi/piRpcClient.ts";
import {
  PiRpcProtocolCommandFailedError,
  PiRpcProtocolProcessExitedError,
  PiRpcProtocolRequestTimeoutError,
  PiRpcProtocolTransportError,
} from "../provider/pi/piRpcProtocol.ts";
import {
  appendPiAssistantTextFromStreamEvent,
  isPiTurnTerminalStreamEvent,
} from "../provider/pi/piStreamText.ts";
import type { PiRpcStreamEvent, PiThinkingLevel } from "../provider/pi/piRpcTypes.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import { type TextGenerationShape } from "./TextGeneration.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const PI_TEXT_GENERATION_TIMEOUT_MS = 180_000;
const PI_TEXT_GENERATION_REQUEST_TIMEOUT_MS = 30_000;

export type PiRpcClientFactory = (
  options: Parameters<typeof makePiRpcClient>[0],
) => Effect.Effect<
  PiRpcClientShape,
  PiRpcClientSpawnError,
  ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
>;

export interface PiTextGenerationDeps {
  readonly makeClient?: PiRpcClientFactory;
}

const isTextGenerationError = Schema.is(TextGenerationError);
const isPiRpcClientSpawnError = Schema.is(PiRpcClientSpawnError);
const isPiRpcProtocolCommandFailedError = Schema.is(PiRpcProtocolCommandFailedError);
const isPiRpcProtocolRequestTimeoutError = Schema.is(PiRpcProtocolRequestTimeoutError);
const isPiRpcProtocolProcessExitedError = Schema.is(PiRpcProtocolProcessExitedError);
const isPiRpcProtocolTransportError = Schema.is(PiRpcProtocolTransportError);

function piTextGenerationErrorDetail(error: unknown): string {
  if (isTextGenerationError(error)) {
    return error.detail;
  }
  if (isPiRpcClientSpawnError(error)) {
    return error.message;
  }
  if (isPiRpcProtocolCommandFailedError(error)) {
    return error.error.trim().length > 0 ? error.error : error.message;
  }
  if (isPiRpcProtocolRequestTimeoutError(error)) {
    return error.message;
  }
  if (isPiRpcProtocolProcessExitedError(error)) {
    return error.detail;
  }
  if (isPiRpcProtocolTransportError(error)) {
    return error.detail;
  }
  if (error instanceof Error) {
    const trimmed = error.message.trim();
    return trimmed.length > 0 ? trimmed : "Pi RPC text generation failed.";
  }
  return "Pi RPC text generation failed.";
}

function mapPiTextGenerationError(
  operation:
    | "generateCommitMessage"
    | "generatePrContent"
    | "generateBranchName"
    | "generateThreadTitle",
  cause: unknown,
): TextGenerationError {
  if (isTextGenerationError(cause)) {
    return cause;
  }
  return new TextGenerationError({
    operation,
    detail: piTextGenerationErrorDetail(cause),
    ...(cause !== undefined ? { cause } : {}),
  });
}

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
  deps: PiTextGenerationDeps = {},
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const makeClient = deps.makeClient ?? makePiRpcClient;

  const runPiJson = <S extends Schema.Top>(input: {
    readonly operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    readonly cwd: string;
    readonly prompt: string;
    readonly outputSchemaJson: S;
    readonly modelSelection: ModelSelection;
  }) =>
    Effect.gen(function* () {
      const identity = yield* decodePiModelSlug(input.modelSelection.model).pipe(
        Effect.mapError(
          (error) =>
            new TextGenerationError({
              operation: input.operation,
              detail: error.message,
            }),
        ),
      );

      return yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeClient({
            binaryPath: piSettings.binaryPath,
            cwd: input.cwd,
            noSession: true,
            requestTimeoutMs: PI_TEXT_GENERATION_REQUEST_TIMEOUT_MS,
            spawnEnv: environment,
          }).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.mapError((cause) => mapPiTextGenerationError(input.operation, cause)),
          );

          const available = yield* client
            .getAvailableModels()
            .pipe(Effect.mapError((cause) => mapPiTextGenerationError(input.operation, cause)));
          const matched = available.models.find(
            (model) => model.provider === identity.provider && model.id === identity.modelId,
          );
          if (!matched) {
            return yield* new TextGenerationError({
              operation: input.operation,
              detail: `Selected Pi model '${input.modelSelection.model}' is not available in the current runtime.`,
            });
          }

          yield* client
            .setModel({ provider: matched.provider, modelId: matched.id })
            .pipe(Effect.mapError((cause) => mapPiTextGenerationError(input.operation, cause)));

          const requestedThinking = getModelSelectionStringOptionValue(
            input.modelSelection,
            "thinkingLevel",
          );
          if (requestedThinking) {
            const resolved = resolvePiThinkingLevelForModel(
              matched,
              requestedThinking as PiThinkingLevel,
            );
            if (resolved) {
              yield* client
                .setThinkingLevel(resolved)
                .pipe(Effect.mapError((cause) => mapPiTextGenerationError(input.operation, cause)));
            }
          }

          const textBuffer = yield* Ref.make("");
          const turnDone = yield* Deferred.make<void, TextGenerationError>();
          const streamFiber = yield* client.streamEvents.pipe(
            Stream.runForEach((event: PiRpcStreamEvent) =>
              Effect.gen(function* () {
                const current = yield* Ref.get(textBuffer);
                yield* Ref.set(
                  textBuffer,
                  appendPiAssistantTextFromStreamEvent(event, { text: current }).text,
                );
                if (isPiTurnTerminalStreamEvent(event)) {
                  yield* Deferred.succeed(turnDone, undefined);
                }
              }),
            ),
            Effect.forkScoped,
          );

          yield* client
            .prompt({ message: input.prompt })
            .pipe(Effect.mapError((cause) => mapPiTextGenerationError(input.operation, cause)));

          yield* Deferred.await(turnDone).pipe(
            Effect.timeoutOption(PI_TEXT_GENERATION_TIMEOUT_MS),
            Effect.flatMap(
              Option.match({
                onNone: () =>
                  Effect.fail(
                    new TextGenerationError({
                      operation: input.operation,
                      detail: "Pi text generation timed out waiting for the model response.",
                    }),
                  ),
                onSome: (value) => Effect.succeed(value),
              }),
            ),
            Effect.mapError((cause) => mapPiTextGenerationError(input.operation, cause)),
          );

          yield* Fiber.interrupt(streamFiber).pipe(Effect.ignore);

          const rawText = (yield* Ref.get(textBuffer)).trim();
          if (rawText.length === 0) {
            return yield* new TextGenerationError({
              operation: input.operation,
              detail: "Pi returned empty output.",
            });
          }

          const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(input.outputSchemaJson));
          return yield* decodeOutput(extractJsonObject(rawText)).pipe(
            Effect.catchTag("SchemaError", (cause) =>
              Effect.fail(
                new TextGenerationError({
                  operation: input.operation,
                  detail: "Pi returned invalid structured output.",
                  cause,
                }),
              ),
            ),
            Effect.mapError((cause) => mapPiTextGenerationError(input.operation, cause)),
          );
        }),
      );
    });

  const generateCommitMessage: TextGenerationShape["generateCommitMessage"] = Effect.fn(
    "PiTextGeneration.generateCommitMessage",
  )(function* (input) {
    const { prompt, outputSchema } = buildCommitMessagePrompt({
      branch: input.branch,
      stagedSummary: input.stagedSummary,
      stagedPatch: input.stagedPatch,
      includeBranch: input.includeBranch === true,
    });
    const generated = yield* runPiJson({
      operation: "generateCommitMessage",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: outputSchema,
      modelSelection: input.modelSelection,
    });

    return {
      subject: sanitizeCommitSubject(generated.subject),
      body: generated.body.trim(),
      ...("branch" in generated && typeof generated.branch === "string"
        ? { branch: sanitizeFeatureBranchName(generated.branch) }
        : {}),
    };
  });

  const generatePrContent: TextGenerationShape["generatePrContent"] = Effect.fn(
    "PiTextGeneration.generatePrContent",
  )(function* (input) {
    const { prompt, outputSchema } = buildPrContentPrompt({
      baseBranch: input.baseBranch,
      headBranch: input.headBranch,
      commitSummary: input.commitSummary,
      diffSummary: input.diffSummary,
      diffPatch: input.diffPatch,
    });
    const generated = yield* runPiJson({
      operation: "generatePrContent",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: outputSchema,
      modelSelection: input.modelSelection,
    });

    return {
      title: sanitizePrTitle(generated.title),
      body: generated.body.trim(),
    };
  });

  const generateBranchName: TextGenerationShape["generateBranchName"] = Effect.fn(
    "PiTextGeneration.generateBranchName",
  )(function* (input) {
    const { prompt, outputSchema } = buildBranchNamePrompt({
      message: input.message,
      attachments: input.attachments,
    });
    const generated = yield* runPiJson({
      operation: "generateBranchName",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: outputSchema,
      modelSelection: input.modelSelection,
    });

    return {
      branch: sanitizeBranchFragment(generated.branch),
    };
  });

  const generateThreadTitle: TextGenerationShape["generateThreadTitle"] = Effect.fn(
    "PiTextGeneration.generateThreadTitle",
  )(function* (input) {
    const { prompt, outputSchema } = buildThreadTitlePrompt({
      message: input.message,
      attachments: input.attachments,
    });
    const generated = yield* runPiJson({
      operation: "generateThreadTitle",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: outputSchema,
      modelSelection: input.modelSelection,
    });

    return {
      title: sanitizeThreadTitle(generated.title),
    };
  });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGenerationShape;
});
