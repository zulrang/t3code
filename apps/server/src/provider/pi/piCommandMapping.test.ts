import { assert, describe, it } from "vitest";

import { mapPiRpcCommandsToSlashCommands } from "./piCommandMapping.ts";

describe("mapPiRpcCommandsToSlashCommands", () => {
  it("maps Pi command descriptors to server slash commands", () => {
    assert.deepEqual(
      mapPiRpcCommandsToSlashCommands([
        { name: "reload", description: "Reload extensions" },
        { name: "rpc-input", source: "~/.pi/agent/prompts/rpc-input.md" },
      ]),
      [{ name: "reload", description: "Reload extensions" }, { name: "rpc-input" }],
    );
  });

  it("dedupes commands by case-insensitive name and drops empty names", () => {
    assert.deepEqual(
      mapPiRpcCommandsToSlashCommands([
        { name: "Reload", description: "first" },
        { name: "reload", description: "second" },
        { name: "  ", description: "ignored" },
      ]),
      [{ name: "Reload", description: "first" }],
    );
  });
});
