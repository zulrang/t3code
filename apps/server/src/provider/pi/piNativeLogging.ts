import type {
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeEventRawSource,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";

const SECRET_KEY_PATTERN = /(api[_-]?key|token|password|secret|authorization|bearer|credential)/i;
const PATH_LIKE_PATTERN = /(?:^|[\s"'`])(\/[\w./-]+|~\/[\w./-]+|[A-Za-z]:\\[\w\\.-]+)/;
const HOME_DIR = process.env.HOME ?? process.env.USERPROFILE ?? "";

const REDACTED = "<redacted>";
const REDACTED_PATH = "<redacted-path>";

export function piRpcRawSource(category: string): RuntimeEventRawSource {
  const normalized = category.trim().length > 0 ? category.trim() : "event";
  return `pi.rpc.${normalized}` as RuntimeEventRawSource;
}

function redactStringValue(key: string, value: string): string {
  if (SECRET_KEY_PATTERN.test(key)) {
    return REDACTED;
  }
  if (key === "encryptedContent" || key === "signature" || key === "thinkingSignature") {
    return REDACTED;
  }
  if (HOME_DIR.length > 0 && value.includes(HOME_DIR)) {
    return value.split(HOME_DIR).join(REDACTED_PATH);
  }
  if (PATH_LIKE_PATTERN.test(value) && value.length > 64) {
    return REDACTED_PATH;
  }
  return value;
}

export function redactPiRpcPayload(payload: unknown): unknown {
  if (payload === null || payload === undefined) {
    return payload;
  }
  if (typeof payload === "string") {
    if (HOME_DIR.length > 0 && payload.includes(HOME_DIR)) {
      return payload.split(HOME_DIR).join(REDACTED_PATH);
    }
    return payload.length > 4_096 ? `${payload.slice(0, 4_096)}…` : payload;
  }
  if (typeof payload !== "object") {
    return payload;
  }
  if (Array.isArray(payload)) {
    return payload.map((entry) => redactPiRpcPayload(entry));
  }

  const record = payload as Record<string, unknown>;
  const redacted: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(record)) {
    if (SECRET_KEY_PATTERN.test(key)) {
      redacted[key] = REDACTED;
      continue;
    }
    if (typeof value === "string") {
      redacted[key] = redactStringValue(key, value);
      continue;
    }
    redacted[key] = redactPiRpcPayload(value);
  }
  return redacted;
}

export interface PiNativeLogRecord {
  readonly source: RuntimeEventRawSource;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId: ProviderInstanceId;
  readonly threadId: ThreadId;
  readonly turnId?: TurnId;
  readonly type: string;
  readonly payload: unknown;
}

export function formatPiNativeLogRecord(input: {
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId: ProviderInstanceId;
  readonly threadId: ThreadId;
  readonly turnId?: TurnId;
  readonly category: string;
  readonly type: string;
  readonly payload: unknown;
}): PiNativeLogRecord {
  return {
    source: piRpcRawSource(input.category),
    provider: input.provider,
    providerInstanceId: input.providerInstanceId,
    threadId: input.threadId,
    ...(input.turnId ? { turnId: input.turnId } : {}),
    type: input.type,
    payload: redactPiRpcPayload(input.payload),
  };
}
