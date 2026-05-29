import { describe, expect, it } from "vitest";

import { piChildProcessSpawnOptions } from "./piSpawnOptions.ts";

describe("piChildProcessSpawnOptions", () => {
  it("keeps stdin open across multiple RPC commands", () => {
    const options = piChildProcessSpawnOptions({
      cwd: "/tmp/pi",
      env: process.env,
    });

    expect(options.stdin).toEqual({ stream: "pipe", endOnDone: false });
  });
});
