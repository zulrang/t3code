import { describe, expect, it } from "vitest";

import {
  appendPiAssistantTextFromStreamEvent,
  isPiTurnTerminalStreamEvent,
} from "./piStreamText.ts";
import type { PiRpcStreamEvent } from "./piRpcTypes.ts";

describe("piStreamText", () => {
  it("accumulates text deltas and prefers text_end content", () => {
    let buffer = { text: "" };
    buffer = appendPiAssistantTextFromStreamEvent(
      {
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", delta: "hello", contentIndex: 0 },
      } satisfies PiRpcStreamEvent,
      buffer,
    );
    buffer = appendPiAssistantTextFromStreamEvent(
      {
        type: "message_update",
        assistantMessageEvent: { type: "text_delta", delta: " world", contentIndex: 0 },
      } satisfies PiRpcStreamEvent,
      buffer,
    );
    expect(buffer.text).toBe("hello world");

    buffer = appendPiAssistantTextFromStreamEvent(
      {
        type: "message_update",
        assistantMessageEvent: {
          type: "text_end",
          content: "final",
          contentIndex: 0,
        },
      } satisfies PiRpcStreamEvent,
      buffer,
    );
    expect(buffer.text).toBe("final");
  });

  it("detects terminal turn events", () => {
    expect(isPiTurnTerminalStreamEvent({ type: "turn_end" })).toBe(true);
    expect(isPiTurnTerminalStreamEvent({ type: "agent_end" })).toBe(true);
    expect(isPiTurnTerminalStreamEvent({ type: "message_update" })).toBe(false);
  });
});
