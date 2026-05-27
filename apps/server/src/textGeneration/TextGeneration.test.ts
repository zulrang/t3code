import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as PubSub from "effect/PubSub";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";
import { describe, expect } from "vitest";

import { ProviderDriverKind, ProviderInstanceId, TextGenerationError } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";

import type { ProviderInstance } from "../provider/ProviderDriver.ts";
import type { ProviderInstanceRegistryShape } from "../provider/Services/ProviderInstanceRegistry.ts";
import type { TextGenerationShape } from "./TextGeneration.ts";

import { makeTextGenerationFromRegistry } from "./TextGeneration.ts";

const makeStubTextGeneration = (overrides: Partial<TextGenerationShape>): TextGenerationShape => ({
  generateCommitMessage: () =>
    Effect.die("generateCommitMessage stub not configured for this test"),
  generatePrContent: () => Effect.die("generatePrContent stub not configured for this test"),
  generateBranchName: () => Effect.die("generateBranchName stub not configured for this test"),
  generateThreadTitle: () => Effect.die("generateThreadTitle stub not configured for this test"),
  ...overrides,
});

const makeStubInstance = (
  instanceId: ProviderInstanceId,
  driverKind: ProviderInstance["driverKind"],
  textGeneration: TextGenerationShape,
): ProviderInstance =>
  ({
    instanceId,
    driverKind,
    continuationIdentity: {
      driverKind,
      continuationKey: `${instanceId}:test`,
    },
    displayName: undefined,
    enabled: true,
    snapshot: {} as ProviderInstance["snapshot"],
    adapter: {} as ProviderInstance["adapter"],
    textGeneration,
  }) satisfies ProviderInstance;

const makeStubRegistry = (
  instances: ReadonlyArray<ProviderInstance>,
): ProviderInstanceRegistryShape => {
  const byId = new Map(instances.map((instance) => [instance.instanceId, instance] as const));
  return {
    getInstance: (id) => Effect.succeed(byId.get(id)),
    listInstances: Effect.succeed(instances),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.empty,
    // Tests never drive changes through this stub; acquire a throwaway
    // subscription on an unused PubSub so the shape is satisfied.
    subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
      PubSub.subscribe(pubsub),
    ),
  };
};

describe("makeTextGenerationFromRegistry", () => {
  it.effect("delegates to the matching instance's textGeneration closure", () =>
    Effect.gen(function* () {
      const personalId = ProviderInstanceId.make("codex_personal");
      const personalCalls: string[] = [];
      const personal = makeStubInstance(
        personalId,
        ProviderDriverKind.make("codex"),
        makeStubTextGeneration({
          generateBranchName: (input) => {
            personalCalls.push(input.message);
            return Effect.succeed({ branch: "personal-branch" });
          },
        }),
      );

      const workId = ProviderInstanceId.make("codex_work");
      const work = makeStubInstance(
        workId,
        ProviderDriverKind.make("codex"),
        makeStubTextGeneration({
          generateBranchName: () => Effect.succeed({ branch: "work-branch" }),
        }),
      );

      const tg = makeTextGenerationFromRegistry(makeStubRegistry([personal, work]));

      const result = yield* tg.generateBranchName({
        cwd: process.cwd(),
        message: "Refactor the routing layer",
        modelSelection: createModelSelection(ProviderInstanceId.make("codex_personal"), "gpt-5"),
      });

      expect(result.branch).toBe("personal-branch");
      expect(personalCalls).toEqual(["Refactor the routing layer"]);
    }),
  );

  it.effect("fails with TextGenerationError when the instance is unknown", () =>
    Effect.gen(function* () {
      const tg = makeTextGenerationFromRegistry(makeStubRegistry([]));

      const result = yield* tg
        .generateBranchName({
          cwd: process.cwd(),
          message: "anything",
          modelSelection: createModelSelection(
            ProviderInstanceId.make("missing_instance"),
            "gpt-5",
          ),
        })
        .pipe(Effect.result);

      expect(Result.isFailure(result)).toBe(true);
      if (Result.isFailure(result)) {
        expect(result.failure._tag).toBe("TextGenerationError");
        expect(result.failure.operation).toBe("generateBranchName");
        expect(result.failure.detail).toContain("missing_instance");
      }
    }),
  );

  it.effect("resolves Pi instances without falling back to another provider", () =>
    Effect.gen(function* () {
      const piId = ProviderInstanceId.make("pi");
      const piCalls: string[] = [];
      const pi = makeStubInstance(
        piId,
        ProviderDriverKind.make("pi"),
        makeStubTextGeneration({
          generateThreadTitle: (input) => {
            piCalls.push(input.message);
            return Effect.fail(
              new TextGenerationError({
                operation: "generateThreadTitle",
                detail: "Pi text generation stub",
              }),
            );
          },
        }),
      );

      const tg = makeTextGenerationFromRegistry(makeStubRegistry([pi]));

      const result = yield* tg
        .generateThreadTitle({
          cwd: process.cwd(),
          message: "Wire Pi into text generation routing",
          modelSelection: createModelSelection(piId, "openai/gpt-4"),
        })
        .pipe(Effect.result);

      expect(Result.isFailure(result)).toBe(true);
      expect(piCalls).toEqual(["Wire Pi into text generation routing"]);
      if (Result.isFailure(result)) {
        expect(result.failure._tag).toBe("TextGenerationError");
        expect(result.failure.detail).toBe("Pi text generation stub");
      }
    }),
  );
});
