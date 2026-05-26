import { assert, describe, it } from "vitest";

import { mapPiRpcModelToServerModel } from "./piModelMapping.ts";
import type { PiRpcModel } from "./piRpcTypes.ts";

describe("piModelMapping", () => {
  it("maps reasoning models with explicit thinkingLevelMap keys only", () => {
    const model: PiRpcModel = {
      id: "gpt-5.4-mini",
      name: "GPT-5.4 Mini",
      provider: "openai-codex",
      reasoning: true,
      thinkingLevelMap: {
        xhigh: "xhigh",
        minimal: "low",
      },
    };

    const mapped = mapPiRpcModelToServerModel(model);
    assert.ok(mapped);
    assert.equal(mapped.slug, "openai-codex/gpt-5.4-mini");
    const thinking = mapped.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "thinkingLevel",
    );
    assert.ok(thinking && thinking.type === "select");
    assert.deepEqual(
      thinking.options.map((option) => option.id),
      ["minimal", "xhigh"],
    );
  });

  it("uses conservative thinking levels when thinkingLevelMap is absent", () => {
    const model: PiRpcModel = {
      id: "gpt-5.1",
      name: "GPT-5.1",
      provider: "openai-codex",
      reasoning: true,
    };

    const mapped = mapPiRpcModelToServerModel(model);
    assert.ok(mapped);
    const thinking = mapped.capabilities?.optionDescriptors?.find(
      (descriptor) => descriptor.id === "thinkingLevel",
    );
    assert.ok(thinking && thinking.type === "select");
    assert.deepEqual(
      thinking.options.map((option) => option.id),
      ["off", "minimal", "low", "medium", "high"],
    );
    assert.equal(
      thinking.options.some((option) => option.id === "xhigh"),
      false,
      "xhigh must not be exposed without explicit support",
    );
  });

  it("omits thinking descriptors for non-reasoning models", () => {
    const model: PiRpcModel = {
      id: "fast-model",
      name: "Fast Model",
      provider: "openai-codex",
      reasoning: false,
    };

    const mapped = mapPiRpcModelToServerModel(model);
    assert.ok(mapped);
    assert.deepEqual(mapped.capabilities?.optionDescriptors ?? [], []);
  });
});
