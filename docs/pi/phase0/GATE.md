# Pi Phase 0 Gate

Phase 0 status: signed off by user on 2026-05-25; Phase 1 may begin.

- [x] All 24 decisions resolved or explicitly deferred with owner.
- [x] Implementation file list confirmed.
- [x] RPC fixtures captured, with live extension UI gap documented.
- [x] `pi --version` recorded: `0.72.1`.
- [x] No Phase 1 blockers remain.
- [x] Ready for user sign-off.

## Deliverables

- `docs/pi/phase0/codebase-inspection.md`
- `docs/pi/phase0/github-issue-archaeology.md`
- `docs/pi/phase0/rpc-probe.md`
- `docs/pi/phase0/rpc-client-comparison.md`
- `docs/pi/phase0/decision-log.md`
- `docs/pi/phase0/design/event-mapping.md`
- `docs/pi/phase0/design/extension-ui-mapping.md`
- `docs/pi/phase0/design/turn-lifecycle.md`
- `docs/pi/phase0/design/model-thinking-mapping.md`
- `docs/pi/phase0/fixtures/`

## Explicit Deferred Re-Verification

Owner: Phase 5 implementer.

The local Pi install had no extension commands installed, so Phase 0 could not capture a live `extension_ui_request` without adding a temporary extension to the user Pi environment. The checked-in fixture is docs-derived and should be replaced or supplemented when Phase 5 adds the extension UI bridge.

Required re-verification:

- controlled extension emits `select`
- controlled extension emits `confirm`
- controlled extension emits `input`
- controlled extension emits `editor`
- T3 writes matching `extension_ui_response` on stdin
- stale request cases fail/cancel deterministically

## Phase 1 Entry Criteria

Phase 1 may start only after user sign-off on this gate. Phase 1 scope should remain contracts/settings/web slug only: no Pi RPC runtime, no driver registration, no adapter, no `PiTextGeneration`, and no live turns.
