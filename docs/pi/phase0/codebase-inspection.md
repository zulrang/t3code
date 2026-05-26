# Pi Phase 0 Codebase Inspection

Phase 0 inspection target: identify the implementation touchpoints for adding a `pi` provider later, without changing production code.

## Confirmed Implementation Files

### Contracts And Settings

| File                                         | Purpose for Pi integration                                                                                                                                                                                                                                                           |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `packages/contracts/src/settings.ts`         | Add `PiSettings` with form annotations, `providers.pi` with decoding default, and `PiSettingsPatch` so settings decode and patch round-trips remain backward compatible.                                                                                                             |
| `packages/contracts/src/model.ts`            | Add Pi display metadata only; do not add static Pi defaults unless a real discovered model selection exists. Existing default helpers fall back to Codex/global defaults when provider-specific entries are missing, so Pi needs explicit dynamic handling in web/server call sites. |
| `packages/contracts/src/providerRuntime.ts`  | Add raw event source template such as `pi.rpc.${string}` so Pi RPC responses/events can be logged without schema churn.                                                                                                                                                              |
| `packages/contracts/src/providerInstance.ts` | `ProviderDriverKind` is already an open branded slug; `defaultInstanceIdForDriver("pi")` will produce default instance id `pi`.                                                                                                                                                      |
| `packages/contracts/src/provider.ts`         | `ProviderSession.resumeCursor` accepts opaque values, but Pi v1 should omit/null it and ignore stale non-null cursors on recovery.                                                                                                                                                   |
| `packages/contracts/src/server.ts`           | `ServerProvider` and `ServerProviderModel` are the dynamic provider/model snapshot contract. Pi model metadata must fit `slug`, `name`, optional `subProvider`, `isCustom`, and `capabilities`.                                                                                      |
| `packages/shared/src/model.ts`               | `resolveModelSlugForProvider` falls back to `DEFAULT_MODEL_BY_PROVIDER[provider] ?? DEFAULT_MODEL`; Pi must avoid this path when no discovered model exists.                                                                                                                         |

Concrete notes:

- `ProviderDriverKind` and `ProviderInstanceId` accept only slug characters (`^[a-zA-Z][a-zA-Z0-9_-]*$`), so driver kind `pi` and instance id `pi` are valid.
- `ServerSettings.providers` is a closed struct today (`codex`, `claudeAgent`, `cursor`, `opencode`), so `providers.pi` is a real schema addition, not just an open-map write.
- `ServerSettingsPatch.providers` is also closed and must add `pi`.
- `ServerProviderModel.slug` is the only stable dispatch string available to the web. `subProvider` is display-only and cannot be relied on for `set_model`.

### Server Provider Stack

| File                                                                   | Purpose for Pi integration                                                                                                                                                                                                                          |
| ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `apps/server/src/provider/ProviderDriver.ts`                           | New `PiDriver` must materialize one `ProviderInstance` containing `snapshot`, `adapter`, `textGeneration`, and a stable `continuationIdentity`.                                                                                                     |
| `apps/server/src/provider/builtInDrivers.ts`                           | Register `PiDriver` and add its environment requirements to `BuiltInDriversEnv`.                                                                                                                                                                    |
| `apps/server/src/provider/Layers/ProviderInstanceRegistryHydration.ts` | Hydrates built-in drivers from legacy `settings.providers.<driverKind>` into `providerInstances`; adding `providers.pi` plus `PiDriver` is enough for default `pi` instance synthesis because the code dynamically indexes by built-in driver kind. |
| `apps/server/src/provider/Services/ProviderAdapter.ts`                 | `PiAdapter` must implement the full SPI, not only prompt/abort: capabilities, start/send/interrupt, approval/user-input responses, session listing, `readThread`, unsupported rollback, stop, and event stream.                                     |
| `apps/server/src/provider/Services/ServerProvider.ts`                  | Pi snapshot implementation must satisfy `getSnapshot`, `refresh`, and `streamChanges`; reuse `makeManagedServerProvider` patterns.                                                                                                                  |
| `apps/server/src/provider/makeManagedServerProvider.ts`                | Existing managed snapshot helper serializes refreshes with a semaphore and does async initial refresh; Pi can reuse it but should add per-instance jitter before/around scheduled checks.                                                           |
| `apps/server/src/provider/providerSnapshot.ts`                         | Reference helpers for building provider snapshots, parsing CLI versions, custom models, and unavailable states.                                                                                                                                     |
| `apps/server/src/provider/ProviderInstanceEnvironment.ts`              | Pi process environment should merge per-instance environment variables through the same helper as other drivers.                                                                                                                                    |
| `apps/server/src/provider/Drivers/OpenCodeDriver.ts`                   | Best driver reference: multi-instance driver, dynamic snapshot, scoped adapter/text generation, env merge, and managed provider refresh.                                                                                                            |
| `apps/server/src/provider/Layers/OpenCodeProvider.ts`                  | Best model-discovery reference: dynamic provider/model flattening, provider/model slug construction, option descriptors, version checks, and auth/error messaging.                                                                                  |
| `apps/server/src/provider/opencodeRuntime.ts`                          | Best subprocess lifecycle reference: scoped child processes, runtime errors, model slug parsing, attachment file conversion, permission/user-input mapping helpers.                                                                                 |
| `apps/server/src/provider/Layers/OpenCodeAdapter.ts`                   | Best adapter reference: per-thread session contexts, native raw logging, `turn.started`/content/tool/request events, pending permission/user-input maps, scoped cleanup, and full SPI methods.                                                      |
| `apps/server/src/provider/Layers/CodexSessionRuntime.ts`               | Best JSON-RPC/stdout subprocess reference: starts `codex app-server`, owns lifecycle, persists `resumeCursor`, maps app-server events, rejects pending work on failure.                                                                             |
| `apps/server/src/provider/Layers/CodexAdapter.ts`                      | Reference for session recovery integration and adapter-level wrapping of a protocol runtime. Pi differs by intentionally not resuming provider-native sessions in v1.                                                                               |
| `apps/server/src/provider/Layers/CursorAdapter.ts`                     | Best extension UI bridge reference: maps Cursor ACP extension calls to `user-input.requested`, waits on `Deferred`, and resolves through `respondToUserInput`.                                                                                      |
| `apps/server/src/provider/acp/CursorAcpSupport.ts`                     | Reference for subprocess ACP startup and model-selection application.                                                                                                                                                                               |
| `apps/server/src/textGeneration/TextGeneration.ts`                     | Registry-based text generation resolves by `modelSelection.instanceId`, but the exported `TextGenerationProvider` union is closed and must be updated or removed when Pi text generation lands.                                                     |
| `apps/server/src/textGeneration/OpenCodeTextGeneration.ts`             | Reference for shared short-lived provider runtime with mutex, idle TTL, structured prompts, attachment path conversion, and cleanup finalizers.                                                                                                     |
| `apps/server/src/textGeneration/CursorTextGeneration.ts`               | Reference for prompt-based text generation over a subprocess runtime with timeouts, structured JSON extraction, and model-selection application.                                                                                                    |

Concrete notes:

- OpenCode model slugs use `${provider.id}/${model.id}` and parse on the first slash. Pi cannot blindly copy this if custom Pi provider ids can contain `/`.
- OpenCode pending permission and question maps are directly relevant to Pi extension UI: Pi needs a pending extension UI map keyed by request id and must always answer blocking requests on stdin.
- `makeManagedServerProvider` currently has a fixed sleep interval and no built-in jitter; Pi should add jitter outside or extend the helper in a later implementation PR.
- `continuationIdentity` defaults to `driverKind:instance:<instanceId>` today. For Pi, the Phase 0 plan wants the normalized binary path included so different Pi binaries do not look continuation-compatible.

### Web

| File                                                                   | Purpose for Pi integration                                                                                                                                          |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `apps/web/src/components/settings/providerDriverMeta.ts`               | Add Pi client definition with `ProviderDriverKind.make("pi")`, label `Pi`, icon, and `PiSettings` schema so generic settings forms render.                          |
| `apps/web/src/components/settings/AddProviderInstanceDialog.tsx`       | Remove coming-soon `piAgent` entry and make Pi an active driver option. Keep `PiAgentIcon` unless a cosmetic rename is intentionally included.                      |
| `apps/web/src/providerModels.ts`                                       | Default model selection currently falls back to static provider/default maps; Pi should prefer discovered `ServerProvider.models` and avoid global fallback.        |
| `apps/web/src/modelSelection.ts`                                       | Text-generation model selection should use discovered Pi models only; current fallback path can return an empty model when no provider-specific git default exists. |
| `apps/web/src/composerDraftStore.ts`                                   | Composer draft fallbacks use `DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL`; Pi integration must guard against invalid Codex/global defaults for Pi.          |
| `apps/web/src/components/chat/CompactComposerControlsMenu.browser.tsx` | Compact composer uses provider default model fallback and should not synthesize a Pi model before discovery.                                                        |

Concrete notes:

- The UI already contains a `piAgent` coming-soon stub. The implementation slug must be `pi`, not `piAgent`, to preserve default instance ids, favorites, settings keys, and routing.
- Settings forms are generic and schema-driven; Pi settings should not require a custom UI flow.
