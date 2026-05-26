import { assert, describe, it } from "vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import { encodePiModelSlug, decodePiModelSlug } from "./piModelSlug.ts";

describe("piModelSlug", () => {
  it("encodes built-in and slash-containing model ids", () => {
    assert.equal(encodePiModelSlug("openai-codex", "gpt-5.4-mini"), "openai-codex/gpt-5.4-mini");
    assert.equal(
      encodePiModelSlug("openrouter", "anthropic/claude-sonnet"),
      "openrouter/anthropic%2Fclaude-sonnet",
    );
    assert.equal(
      encodePiModelSlug("custom/provider", "model/name"),
      "custom%2Fprovider/model%2Fname",
    );
  });

  it("round-trips encoded slugs", () => {
    const cases = [
      { provider: "openai-codex", modelId: "gpt-5.4-mini" },
      { provider: "openrouter", modelId: "anthropic/claude-sonnet" },
      { provider: "custom/provider", modelId: "model/name" },
    ] as const;

    for (const entry of cases) {
      const slug = encodePiModelSlug(entry.provider, entry.modelId);
      const decoded = Effect.runSync(decodePiModelSlug(slug));
      assert.deepEqual(decoded, entry);
    }
  });

  it("rejects malformed slugs", () => {
    const malformed = ["", "missing-slash", "/missing-provider", "provider/", "%ZZ/model"];
    for (const slug of malformed) {
      const exit = Effect.runSyncExit(decodePiModelSlug(slug));
      assert.equal(exit._tag, "Failure");
    }
  });

  it("rejects empty provider or model id on encode", () => {
    assert.throws(() => encodePiModelSlug("", "gpt-5.4-mini"));
    assert.throws(() => encodePiModelSlug("openai-codex", "   "));
  });
});
