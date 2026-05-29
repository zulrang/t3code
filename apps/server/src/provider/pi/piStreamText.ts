import type { PiRpcStreamEvent } from "./piRpcTypes.ts";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

export function isPiTurnTerminalStreamEvent(event: PiRpcStreamEvent): boolean {
  return event.type === "turn_end" || event.type === "agent_end";
}

/**
 * Accumulates assistant text from Pi `message_update` stream events.
 * Prefers `text_end` content when present; otherwise concatenates `text_delta`.
 */
export function appendPiAssistantTextFromStreamEvent(
  event: PiRpcStreamEvent,
  buffer: { readonly text: string },
): { readonly text: string } {
  if (event.type !== "message_update" || !isRecord(event.assistantMessageEvent)) {
    return buffer;
  }

  const update = event.assistantMessageEvent;
  const updateType = readString(update.type);
  if (updateType === "text_delta") {
    const delta = readString(update.delta);
    return delta !== undefined ? { text: buffer.text + delta } : buffer;
  }

  if (updateType === "text_end") {
    const content = readString(update.content);
    return content !== undefined ? { text: content } : buffer;
  }

  return buffer;
}
