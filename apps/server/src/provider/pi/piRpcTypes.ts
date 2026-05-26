import * as Schema from "effect/Schema";

export const PiThinkingLevel = Schema.Literals([
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
]);
export type PiThinkingLevel = typeof PiThinkingLevel.Type;

export const PiThinkingLevelMap = Schema.Record(Schema.String, Schema.NullOr(PiThinkingLevel));
export type PiThinkingLevelMap = typeof PiThinkingLevelMap.Type;

export const PiRpcModel = Schema.Struct({
  id: Schema.String,
  name: Schema.optional(Schema.String),
  api: Schema.optional(Schema.String),
  provider: Schema.String,
  baseUrl: Schema.optional(Schema.String),
  reasoning: Schema.optional(Schema.Boolean),
  thinkingLevelMap: Schema.optional(PiThinkingLevelMap),
  input: Schema.optional(Schema.Array(Schema.String)),
  contextWindow: Schema.optional(Schema.Number),
  maxTokens: Schema.optional(Schema.Number),
});
export type PiRpcModel = typeof PiRpcModel.Type;

export const PiRpcState = Schema.Struct({
  sessionId: Schema.optional(Schema.String),
  sessionFile: Schema.optional(Schema.String),
  messageCount: Schema.optional(Schema.Number),
  pendingMessageCount: Schema.optional(Schema.Number),
  isStreaming: Schema.optional(Schema.Boolean),
  model: Schema.optional(PiRpcModel),
  thinkingLevel: Schema.optional(PiThinkingLevel),
});
export type PiRpcState = typeof PiRpcState.Type;

export const PiRpcAvailableModels = Schema.Struct({
  models: Schema.Array(PiRpcModel),
});
export type PiRpcAvailableModels = typeof PiRpcAvailableModels.Type;

export const PiRpcCommandDescriptor = Schema.Struct({
  name: Schema.String,
  description: Schema.optional(Schema.String),
  source: Schema.optional(Schema.String),
});
export type PiRpcCommandDescriptor = typeof PiRpcCommandDescriptor.Type;

export const PiRpcCommands = Schema.Struct({
  commands: Schema.Array(PiRpcCommandDescriptor),
});
export type PiRpcCommands = typeof PiRpcCommands.Type;

export const PiRpcResponseEnvelope = Schema.Struct({
  type: Schema.Literal("response"),
  command: Schema.String,
  success: Schema.Boolean,
  id: Schema.optional(Schema.String),
  data: Schema.optional(Schema.Unknown),
  error: Schema.optional(Schema.String),
});
export type PiRpcResponseEnvelope = typeof PiRpcResponseEnvelope.Type;

export const PiRpcStreamEvent = Schema.Record(Schema.String, Schema.Unknown);
export type PiRpcStreamEvent = typeof PiRpcStreamEvent.Type;

export const PiExtensionUiResponse = Schema.Record(Schema.String, Schema.Unknown);
export type PiExtensionUiResponse = typeof PiExtensionUiResponse.Type;

export const isPiRpcResponseEnvelope = Schema.is(PiRpcResponseEnvelope);
