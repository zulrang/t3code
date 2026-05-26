import * as Data from "effect/Data";
import * as Effect from "effect/Effect";

export interface PiModelIdentity {
  readonly provider: string;
  readonly modelId: string;
}

export class PiModelSlugError extends Data.TaggedError("PiModelSlugError")<{
  readonly message: string;
}> {}

export function encodePiModelSlug(provider: string, modelId: string): string {
  const trimmedProvider = provider.trim();
  const trimmedModelId = modelId.trim();
  if (trimmedProvider.length === 0 || trimmedModelId.length === 0) {
    throw new PiModelSlugError({
      message: "Pi model slug requires non-empty provider and model id.",
    });
  }
  return `${encodeURIComponent(trimmedProvider)}/${encodeURIComponent(trimmedModelId)}`;
}

const decodeUriComponent = (
  segment: string,
  label: string,
): Effect.Effect<string, PiModelSlugError> =>
  Effect.try({
    try: () => decodeURIComponent(segment),
    catch: () =>
      new PiModelSlugError({
        message: `Invalid Pi model slug: malformed ${label} encoding.`,
      }),
  }).pipe(
    Effect.flatMap((decoded) => {
      const trimmed = decoded.trim();
      if (trimmed.length === 0) {
        return Effect.fail(
          new PiModelSlugError({
            message: `Invalid Pi model slug: empty ${label} after decoding.`,
          }),
        );
      }
      return Effect.succeed(trimmed);
    }),
  );

export function decodePiModelSlug(slug: string): Effect.Effect<PiModelIdentity, PiModelSlugError> {
  const trimmed = slug.trim();
  const slashIndex = trimmed.indexOf("/");
  if (slashIndex <= 0 || slashIndex === trimmed.length - 1) {
    return Effect.fail(
      new PiModelSlugError({
        message: "Invalid Pi model slug: expected '{provider}/{modelId}' format.",
      }),
    );
  }

  return Effect.gen(function* () {
    const provider = yield* decodeUriComponent(trimmed.slice(0, slashIndex), "provider");
    const modelId = yield* decodeUriComponent(trimmed.slice(slashIndex + 1), "model id");
    return { provider, modelId } satisfies PiModelIdentity;
  });
}
