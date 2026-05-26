import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export interface JsonlChunkSplitResult {
  readonly lines: ReadonlyArray<string>;
  readonly remainder: string;
}

/** Split UTF-8 chunks on LF only; strip optional trailing CR per line. */
export function splitJsonlChunk(remainder: string, chunk: string): JsonlChunkSplitResult {
  const combined = remainder + chunk;
  const parts = combined.split("\n");
  const nextRemainder = parts.pop() ?? "";
  const lines = parts.map((line) => line.replace(/\r$/, ""));
  return { lines, remainder: nextRemainder };
}

export class PiJsonlParseError extends Schema.TaggedErrorClass<PiJsonlParseError>()(
  "PiJsonlParseError",
  {
    line: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {
  override get message(): string {
    return `Failed to parse Pi RPC JSONL line: ${this.line.slice(0, 200)}`;
  }
}

export const parseJsonlLine = (line: string): Effect.Effect<unknown, PiJsonlParseError> =>
  Effect.gen(function* () {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      return yield* new PiJsonlParseError({
        line,
      });
    }
    return yield* Effect.try({
      try: () => JSON.parse(trimmed) as unknown,
      catch: (cause) =>
        new PiJsonlParseError({
          line,
          cause,
        }),
    });
  });

export const encodeJsonlLine = (value: unknown): string => `${JSON.stringify(value)}\n`;
