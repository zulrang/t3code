# Pi Turn Lifecycle State Machine

Draft Phase 5 state machine. Goal: one T3 turn per accepted T3 user prompt, no duplicate starts/completions, no blank assistant messages, and no indefinite `working...`.

```text
idle
  |
  | sendTurn validates input/model and sends set_model/set_thinking_level/prompt
  v
prompt_pending
  |
  | prompt response success
  v
accepted
  |
  | agent_start or turn_start
  v
running
  |
  | message_update text/thinking/tool events
  v
streaming
  |
  | extension_ui_request(dialog)
  v
waiting_for_user_input
  |
  | extension_ui_response written to stdin
  v
streaming
  |
  | agent_end OR turn_end + get_state.isStreaming=false
  v
completed

Any active state
  | set_model/prompt failure, process exit, stale UI, watchdog timeout
  v
failed

Any active state
  | interruptTurn -> abort -> abort stream/response
  v
aborted
```

## State Rules

| State                    | Entered when                                                         | Exit conditions                                                                                                           |
| ------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `idle`                   | No active Pi turn for thread                                         | `sendTurn` accepted for processing.                                                                                       |
| `prompt_pending`         | T3 allocated a turn id and is preparing Pi command sequence          | `prompt` response success -> `accepted`; command response failure -> fail before/at turn start.                           |
| `accepted`               | Pi returned `response` success for `prompt`                          | First `agent_start`/`turn_start` -> `running`; silence watchdog starts here.                                              |
| `running`                | Pi native agent/turn has started                                     | Content/tool events -> `streaming`; extension dialog -> `waiting_for_user_input`; terminal event -> `completed`/`failed`. |
| `streaming`              | Assistant/tool content has been observed                             | More stream events, extension dialog, terminal event, watchdog timeout, process exit, abort.                              |
| `waiting_for_user_input` | Blocking `extension_ui_request` received                             | `respondToUserInput` writes response -> `streaming`; stale/timeout/disconnect -> `failed`.                                |
| `completed`              | `agent_end` or reconciled idle success                               | Clear active turn id, pending maps, watchdogs.                                                                            |
| `failed`                 | Pre/post-accept failure, process exit, stale UI, or watchdog timeout | Clear active turn id, pending maps, watchdogs; emit terminal failure.                                                     |
| `aborted`                | Abort succeeds or stream indicates `stopReason: "aborted"`           | Clear active turn id, pending maps, watchdogs.                                                                            |

## Terminal Event Policy

- Prefer `agent_end` as the terminal event.
- Treat `turn_end` as a fallback only if no `agent_end` arrives and `get_state.isStreaming` is false.
- If `message.stopReason === "aborted"` or error message is "Request was aborted", emit abort semantics.
- If Pi exits mid-turn, emit `session.exited` and terminal turn failure.
- If post-accept stream errors arrive, emit terminal turn failure; do not wait for another response to the original `prompt`.

## Watchdog Policy

- Start a bounded silence timer after `prompt` success.
- Reset the timer on every Pi RPC event for the active turn.
- On silence, call `get_state`.
- If `get_state.isStreaming === false` and no terminal event was emitted, synthesize `turn.completed` if the last assistant message has a non-error stop reason, otherwise `turn.completed` with failed state.
- If silence exceeds hard timeout or `get_state` fails, emit terminal failure with actionable message.
- Clear timers on completion, failure, abort, process exit, stopSession, and scope close.

## Concurrency Policy

- One active turn per Pi thread process.
- Second user message while streaming is rejected in conservative v1 unless Phase 5 explicitly implements Pi `steer`/`follow_up`.
- `set_model` and `prompt` run on the same per-thread process; do not share a process across threads without command-level locking.
