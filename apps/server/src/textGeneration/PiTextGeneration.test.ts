import * as NodeServices from "@effect/platform-node/NodeServices";
import { PiSettings, ProviderInstanceId, TextGenerationError } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { assert, describe, expect } from "vitest";

import { encodePiModelSlug } from "../provider/pi/piModelSlug.ts";
import type { PiRpcClientShape } from "../provider/pi/piRpcClient.ts";
import type {
  PiRpcAvailableModels,
  PiRpcModel,
  PiRpcStreamEvent,
} from "../provider/pi/piRpcTypes.ts";
import { makePiTextGeneration } from "./PiTextGeneration.ts";

const decodePiSettings = Schema.decodeSync(PiSettings);

const MODEL_SLUG = encodePiModelSlug("openai-codex", "gpt-5.4-mini");
const MATCHED_MODEL: PiRpcModel = {
  id: "gpt-5.4-mini",
  provider: "openai-codex",
  reasoning: true,
  thinkingLevelMap: { minimal: "minimal", low: "low" },
};

class FakePiTextRpcClient implements PiRpcClientShape {
  readonly piVersion = "0.72.1-test";
  readonly streamInput = Effect.runSync(Queue.unbounded<PiRpcStreamEvent, Cause.Done<void>>());
  readonly streamEvents = Stream.fromQueue(this.streamInput);
  readonly stderrLines = Stream.empty;
  readonly calls: {
    setModel: Array<{ provider: string; modelId: string }>;
    setThinkingLevel: Array<string>;
    prompt: Array<{ message: string }>;
  } = { setModel: [], setThinkingLevel: [], prompt: [] };

  readonly availableModels: PiRpcAvailableModels = {
    models: [MATCHED_MODEL],
  };

  getState = () =>
    Effect.succeed({
      isStreaming: false,
      model: MATCHED_MODEL,
    });

  getAvailableModels = () => Effect.succeed(this.availableModels);
  getCommands = () => Effect.succeed({ commands: [] });

  setModel = (input: { provider: string; modelId: string }) =>
    Effect.sync(() => {
      this.calls.setModel.push(input);
      return MATCHED_MODEL;
    });

  setThinkingLevel = (level: string) =>
    Effect.sync(() => {
      this.calls.setThinkingLevel.push(level);
    });

  prompt = (input: { message: string }) => {
    const client = this;
    return Effect.gen(function* () {
      client.calls.prompt.push(input);
      yield* Queue.offer(client.streamInput, { type: "agent_start" });
      yield* Queue.offer(client.streamInput, {
        type: "message_update",
        assistantMessageEvent: {
          type: "text_delta",
          delta: '{"branch":"pi-branch"}',
          contentIndex: 0,
        },
      });
      yield* Queue.offer(client.streamInput, { type: "turn_end" });
    });
  };

  abort = () => Effect.void;
  sendExtensionUiResponse = () => Effect.void;
  close = () => Effect.void;
}

describe("PiTextGeneration", () => {
  it.effect("generates structured output via Pi RPC prompt", () =>
    Effect.gen(function* () {
      const fakeClient = new FakePiTextRpcClient();
      const textGeneration = yield* makePiTextGeneration(
        decodePiSettings({ enabled: false, binaryPath: "pi", customModels: [] }),
        process.env,
        {
          makeClient: () => Effect.succeed(fakeClient),
        },
      );

      const generated = yield* textGeneration.generateBranchName({
        cwd: process.cwd(),
        message: "Add Pi text generation",
        modelSelection: createModelSelection(ProviderInstanceId.make("pi"), MODEL_SLUG, [
          { id: "thinkingLevel", value: "minimal" },
        ]),
      });

      expect(generated.branch).toBe("pi-branch");
      expect(fakeClient.calls.setModel).toEqual([
        { provider: "openai-codex", modelId: "gpt-5.4-mini" },
      ]);
      expect(fakeClient.calls.setThinkingLevel).toEqual(["minimal"]);
      expect(fakeClient.calls.prompt.length).toBe(1);
      expect(fakeClient.calls.prompt[0]?.message).toContain("Return a JSON object");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects models missing from the Pi runtime snapshot", () =>
    Effect.gen(function* () {
      const fakeClient = new FakePiTextRpcClient();
      const textGeneration = yield* makePiTextGeneration(
        decodePiSettings({ enabled: false, binaryPath: "pi", customModels: [] }),
        process.env,
        {
          makeClient: () => Effect.succeed(fakeClient),
        },
      );

      const result = yield* textGeneration
        .generateThreadTitle({
          cwd: process.cwd(),
          message: "hello",
          modelSelection: createModelSelection(
            ProviderInstanceId.make("pi"),
            encodePiModelSlug("anthropic", "missing-model"),
          ),
        })
        .pipe(Effect.result);

      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.failure).toBeInstanceOf(TextGenerationError);
        expect(result.failure.detail).toMatch(/not available/i);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("fails when Pi returns empty assistant text", () =>
    Effect.gen(function* () {
      const fakeClient = new FakePiTextRpcClient();
      fakeClient.prompt = () => Queue.offer(fakeClient.streamInput, { type: "turn_end" });

      const textGeneration = yield* makePiTextGeneration(
        decodePiSettings({ enabled: false, binaryPath: "pi", customModels: [] }),
        process.env,
        {
          makeClient: () => Effect.succeed(fakeClient),
        },
      );

      const result = yield* textGeneration
        .generateThreadTitle({
          cwd: process.cwd(),
          message: "hello",
          modelSelection: createModelSelection(ProviderInstanceId.make("pi"), MODEL_SLUG),
        })
        .pipe(Effect.result);

      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.failure.detail).toMatch(/empty output/i);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
