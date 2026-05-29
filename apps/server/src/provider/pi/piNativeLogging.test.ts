import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { formatPiNativeLogRecord, piRpcRawSource, redactPiRpcPayload } from "./piNativeLogging.ts";

describe("piNativeLogging", () => {
  it("builds pi.rpc.* raw sources", () => {
    expect(piRpcRawSource("event")).toBe("pi.rpc.event");
    expect(piRpcRawSource("stderr")).toBe("pi.rpc.stderr");
    expect(piRpcRawSource("extension_ui")).toBe("pi.rpc.extension_ui");
  });

  it("redacts secret keys and home directory paths", () => {
    const home = process.env.HOME ?? process.env.USERPROFILE ?? "/home/tester";
    const redacted = redactPiRpcPayload({
      apiKey: "sk-secret",
      message: `read ${home}/projects/secret/file.ts`,
      nested: { authorization: "Bearer abc" },
    }) as Record<string, unknown>;

    expect(redacted.apiKey).toBe("<redacted>");
    expect(redacted.nested).toEqual({ authorization: "<redacted>" });
    expect(String(redacted.message)).not.toContain(home);
    expect(String(redacted.message)).toContain("<redacted-path>");
  });

  it("formats native log records with instance and thread metadata", () => {
    const record = formatPiNativeLogRecord({
      provider: ProviderDriverKind.make("pi"),
      providerInstanceId: ProviderInstanceId.make("pi"),
      threadId: ThreadId.make("thread-1"),
      category: "message_update",
      type: "message_update",
      payload: { token: "secret" },
    });

    expect(record.source).toBe("pi.rpc.message_update");
    expect(record.providerInstanceId).toBe("pi");
    expect(record.threadId).toBe("thread-1");
    expect((record.payload as { token: string }).token).toBe("<redacted>");
  });
});
