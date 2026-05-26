# Pi Phase 0 RPC Probe

Probe date: 2026-05-24

Local CLI:

```text
pi --version
0.72.1
```

Host: Linux WSL2.

## Commands Exercised

| Command                         | Result                                                                                                                                                                                                                                            |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `get_state` with `--no-session` | Succeeded. Response omitted `sessionFile`, included `sessionId`, current model, thinking level, streaming flags, queue modes, auto-compaction state, and message counts.                                                                          |
| `get_state` sessionful          | Succeeded. Response included `sessionFile`, proving `--no-session` is the correct v1 flag for ephemeral runtime sessions.                                                                                                                         |
| `get_available_models`          | Succeeded. Returned full model objects including `provider`, `id`, `api`, `baseUrl`, `reasoning`, `input`, costs, context, max tokens, and per-model `thinkingLevelMap` for some models.                                                          |
| `get_commands`                  | Succeeded. Local setup returned prompt/skill commands only, with source paths redacted. No `source: "extension"` commands were installed, so no live extension UI request could be produced from this setup without adding a temporary extension. |
| `set_model`                     | Succeeded for `provider: "openai-codex"`, `modelId: "gpt-5.4-mini"`. Response returned the selected full model object.                                                                                                                            |
| `set_thinking_level`            | Succeeded for `minimal`; Pi emitted a `thinking_level_changed` event before the command response.                                                                                                                                                 |
| `prompt`                        | Succeeded. The command response means accepted; stream events followed (`agent_start`, `turn_start`, `message_start`, `message_update` text deltas, `turn_end`, `agent_end`).                                                                     |
| `abort`                         | Succeeded. A fast abort produced assistant `stopReason: "aborted"` and `errorMessage: "Request was aborted"` followed by `turn_end`, `agent_end`, and finally the `abort` response.                                                               |
| Command failure                 | Invalid `set_model` returned `success: false` with `error: "Model not found: __t3_invalid_provider__/__missing__"`.                                                                                                                               |
| Process exit mid-command        | Killing the process shortly after sending `prompt` produced no stdout lines and exited with `signal: "SIGTERM"`. The future RPC client must reject pending commands on exit.                                                                      |

## Observed `--no-session` vs Sessionful Difference

`--no-session`:

```json
{
  "command": "get_state",
  "success": true,
  "data": {
    "sessionId": "<uuid>",
    "messageCount": 0,
    "pendingMessageCount": 0
  }
}
```

Sessionful:

```json
{
  "command": "get_state",
  "success": true,
  "data": {
    "sessionFile": "<HOME_PATH>",
    "sessionId": "<uuid>",
    "messageCount": 0,
    "pendingMessageCount": 0
  }
}
```

This confirms the Phase 0 v1 policy: runtime and probe processes should pass `--no-session`; no Pi `sessionFile`, `switch_session`, `fork`, or persisted Pi resume is used in v1.

## Model And Thinking Findings

- Observed provider id: `openai-codex`.
- Observed model ids did not contain `/`.
- Upstream docs and custom-provider APIs do not document a slash ban for custom provider ids. Model ids can contain provider-like separators in ecosystems such as OpenRouter or Bedrock-style ids.
- `get_available_models` includes `thinkingLevelMap` on some but not all reasoning models.
- `thinkingLevelMap` keys are Pi thinking levels. Upstream docs define `null` as unsupported/hidden, strings as provider-mapped values, and omitted keys as supported via provider defaults.

Phase 1+ implication: store `{ provider, modelId }` in Pi-specific metadata or use a reversible escaped slug. Do not rely on unescaped first-slash splitting unless Phase 1 adds tests proving provider ids cannot contain `/`.

## Extension UI Probe Gap

The local Pi setup did not expose any extension commands in `get_commands`, so no live `extension_ui_request` could be triggered without adding a temporary extension to the Pi environment. The fixture in `fixtures/extension-ui-request-response.jsonl` is therefore derived from upstream `rpc.md` and `rpc-types.ts`, not live local output.

This does not block Phase 1 contracts/settings work, but Phase 5 extension UI implementation should be re-verified with a controlled Pi extension that calls `ctx.ui.select`, `ctx.ui.confirm`, `ctx.ui.input`, and `ctx.ui.editor`.

## Fixture Index

- `fixtures/probe-metadata.json`
- `fixtures/model-discovery.json`
- `fixtures/normal-prompt-stream.jsonl`
- `fixtures/command-failure.jsonl`
- `fixtures/extension-ui-request-response.jsonl`
- `fixtures/process-exit-mid-command.json`
