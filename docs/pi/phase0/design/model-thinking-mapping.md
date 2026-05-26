# Pi Model And Thinking Mapping

Draft rules for Phase 3/5 implementation.

## Model Discovery Shape

Local `pi 0.72.1` `get_available_models` returned models like:

```json
{
  "id": "gpt-5.4-mini",
  "name": "GPT-5.4 Mini",
  "api": "openai-codex-responses",
  "provider": "openai-codex",
  "reasoning": true,
  "thinkingLevelMap": {
    "xhigh": "xhigh",
    "minimal": "low"
  },
  "input": ["text", "image"],
  "contextWindow": 272000,
  "maxTokens": 128000
}
```

## ServerProviderModel Mapping

| Pi field                        | T3 field                          | Rule                                                                                                                |
| ------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `provider`                      | slug component and metadata       | Preserve exact provider id for `set_model`.                                                                         |
| `id`                            | slug component and metadata       | Preserve exact model id for `set_model`.                                                                            |
| `name`                          | `ServerProviderModel.name`        | Use Pi name when non-empty; fallback to id only if needed.                                                          |
| `provider`                      | `ServerProviderModel.subProvider` | Use humanized provider id or Pi provider display name if available.                                                 |
| `reasoning`, `thinkingLevelMap` | `capabilities.optionDescriptors`  | Add thinking option only when reasoning is true and supported levels can be described honestly.                     |
| `input`                         | future capability metadata        | T3 model capabilities currently do not encode text/image support; adapter should validate attachments at send time. |
| costs/context/max tokens        | optional raw metadata only        | Do not add to contracts unless another T3 surface needs it.                                                         |

## Slug Encoding

Chosen Phase 0 policy:

```text
slug = encodeURIComponent(provider) + "/" + encodeURIComponent(modelId)
```

Examples:

| Provider          | Model id                  | T3 slug                                |
| ----------------- | ------------------------- | -------------------------------------- |
| `openai-codex`    | `gpt-5.4-mini`            | `openai-codex/gpt-5.4-mini`            |
| `openrouter`      | `anthropic/claude-sonnet` | `openrouter/anthropic%2Fclaude-sonnet` |
| `custom/provider` | `model/name`              | `custom%2Fprovider/model%2Fname`       |

Rationale:

- Local provider ids did not contain `/`, but upstream custom-provider docs do not document a slash ban.
- The encoded form stays readable for common built-in ids.
- It remains reversible if a custom provider or model id contains `/`.
- Pi-specific runtime code should still carry raw `{ provider, modelId }` metadata when possible so parsing is centralized and testable.

Malformed slug policy:

- Missing slash: reject with validation error.
- Empty provider/model after decoding: reject.
- Invalid percent encoding: reject.
- Decoded `{ provider, modelId }` not present in current model snapshot: reject before `set_model`.

## Thinking Level Rules

Pi thinking levels:

```text
off, minimal, low, medium, high, xhigh
```

Mapping rules:

- If `reasoning !== true`, do not expose a thinking selector.
- If `thinkingLevelMap` exists:
  - key with `null`: hide/disable that Pi level
  - key with string: expose that Pi level and send the Pi level through `set_thinking_level`
  - omitted key: upstream docs say provider default mapping is supported; expose only if the model/provider default policy is known enough for v1
- If `thinkingLevelMap` is absent on a reasoning model:
  - expose a conservative set only if Phase 3 confirms Pi will clamp/reject predictably
  - otherwise omit descriptor and handle selected/default thinking level internally
- Do not expose `xhigh` unless the model explicitly supports it or Phase 3 verifies omitted means supported for that model/provider.
- On `set_thinking_level` rejection, fail the turn visibly; do not silently continue with a different level.

## Dynamic Defaults

- Do not add `DEFAULT_MODEL_BY_PROVIDER["pi"]`.
- Do not add fake/fallback Pi models.
- Select default Pi chat model from the current `ServerProvider.models` snapshot only.
- If discovery returns no models, Pi remains unavailable/unselectable with actionable message.
- `PiTextGeneration` must use a concrete discovered model selection. It must not fall back to `DEFAULT_GIT_TEXT_GENERATION_MODEL` or any Codex default.
