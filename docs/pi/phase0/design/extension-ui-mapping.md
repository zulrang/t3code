# Pi Extension UI Mapping

Pi RPC extension UI is a sub-protocol on top of JSONL:

- Pi emits `extension_ui_request` on stdout.
- For dialog methods, Pi blocks until T3 writes `extension_ui_response` with the matching `id` to stdin.
- Fire-and-forget methods emit a request-shaped event but do not expect a response.

Local probe note: this Pi setup returned only prompt/skill commands from `get_commands`, no `source: "extension"` commands, so live extension UI output was not available without adding a temporary extension. Shapes below are from upstream `rpc.md` / `rpc-types.ts`.

## Mapping Table

| Pi method                      | Pi request shape                                                 | Blocking                                                  | T3 mapping                                                                                                                                 | Response policy                                                                                                |
| ------------------------------ | ---------------------------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `select`                       | `{ type, id, method, title, options, timeout? }`                 | Yes                                                       | `user-input.requested` with one single-select question                                                                                     | On answer, write `{ type: "extension_ui_response", id, value }`; on cancel/stale, write `{ cancelled: true }`. |
| `confirm`                      | `{ type, id, method, title, message, timeout? }`                 | Yes                                                       | `user-input.requested` with yes/no options or approval-style prompt                                                                        | Write `{ confirmed: true/false }`; on cancel/stale, write `{ cancelled: true }`.                               |
| `input`                        | `{ type, id, method, title, placeholder?, timeout? }`            | Yes                                                       | T3 currently has option-based `UserInputQuestion`; v1 can use a single freeform-like option only if UI supports it, otherwise fail clearly | Prefer adding/using a text user-input UI before enabling; fallback cancel/fail turn.                           |
| `editor`                       | `{ type, id, method, title, prefill? }`                          | Yes                                                       | Textarea user-input if available                                                                                                           | If no textarea UI exists in v1, fail/cancel with clear message rather than blocking Pi.                        |
| `notify`                       | `{ type, id, method, message, notifyType? }`                     | No                                                        | `runtime.warning` or status toast/log                                                                                                      | No stdin response.                                                                                             |
| `setStatus`                    | `{ type, id, method, statusKey, statusText }`                    | No                                                        | Status/log only                                                                                                                            | No stdin response.                                                                                             |
| `setWidget`                    | `{ type, id, method, widgetKey, widgetLines, widgetPlacement? }` | No                                                        | Ignore or raw-log in v1                                                                                                                    | No stdin response.                                                                                             |
| `setTitle`                     | `{ type, id, method, title }`                                    | No                                                        | Ignore or raw-log in v1                                                                                                                    | No stdin response.                                                                                             |
| `set_editor_text`              | `{ type, id, method, text }`                                     | No                                                        | Ignore or raw-log in v1                                                                                                                    | No stdin response.                                                                                             |
| Unknown blocking method        | Unknown                                                          | Treat as blocking if docs/type indicate response expected | `runtime.error` and fail/cancel active turn                                                                                                | Try `{ cancelled: true }` if `id` is present, then fail turn.                                                  |
| Unknown fire-and-forget method | Unknown                                                          | No                                                        | Raw-log only                                                                                                                               | No stdin response.                                                                                             |

## Stale Request Policy

| Scenario                           | v1 behavior                                                                                                                   |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Browser disconnects while Pi waits | Start/continue timeout. If unanswered, write cancellation if process is alive and fail the active turn.                       |
| Server restarts while Pi waits     | Process scope closes. Reject pending command, emit terminal failure before shutdown if possible.                              |
| User changes provider/session      | Resolve pending request as cancelled if possible; otherwise fail active turn.                                                 |
| Pi exits before T3 responds        | Mark request stale, ignore late UI answer, fail active turn if it was active.                                                 |
| Request timeout field exists       | Pi may auto-resolve. T3 should still clear local pending state when later events or process state indicate no longer waiting. |

## Implementation Notes

- `respondToUserInput` is required for Pi v1 because extension UI is not equivalent to T3 approvals.
- Pending extension requests should be keyed by T3 `requestId` and Pi `id`.
- The adapter must write to stdin through the RPC client, not emit only T3 `user-input.resolved`.
- For `select`, Pi expects the selected option string, not an index.
- For `confirm`, Pi expects `confirmed: boolean`.
- Cancellation is valid for any dialog method.
