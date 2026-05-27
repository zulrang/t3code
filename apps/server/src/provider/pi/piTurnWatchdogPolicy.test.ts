import { TurnId } from "@t3tools/contracts";
import { describe, expect, it } from "vitest";

import { evaluatePiWatchdogTick } from "./piTurnWatchdogPolicy.ts";

const turnId = TurnId.make("turn-watchdog");

describe("evaluatePiWatchdogTick", () => {
  it("stops when the turn already completed", () => {
    expect(
      evaluatePiWatchdogTick({
        turnCompletedEmitted: true,
        activeTurnId: turnId,
        silenceMs: 10_000,
        turnSilenceHardMs: 500,
        isStreaming: true,
      }),
    ).toBe("stop");
  });

  it("stops when there is no active turn", () => {
    expect(
      evaluatePiWatchdogTick({
        turnCompletedEmitted: false,
        activeTurnId: undefined,
        silenceMs: 10_000,
        turnSilenceHardMs: 500,
        isStreaming: true,
      }),
    ).toBe("stop");
  });

  it("fails after the hard silence threshold", () => {
    expect(
      evaluatePiWatchdogTick({
        turnCompletedEmitted: false,
        activeTurnId: turnId,
        silenceMs: 500,
        turnSilenceHardMs: 500,
        isStreaming: true,
      }),
    ).toBe("fail");
  });

  it("completes when get_state reports the turn is no longer streaming", () => {
    expect(
      evaluatePiWatchdogTick({
        turnCompletedEmitted: false,
        activeTurnId: turnId,
        silenceMs: 100,
        turnSilenceHardMs: 500,
        isStreaming: false,
      }),
    ).toBe("complete");
  });

  it("continues while streaming and under the hard silence threshold", () => {
    expect(
      evaluatePiWatchdogTick({
        turnCompletedEmitted: false,
        activeTurnId: turnId,
        silenceMs: 100,
        turnSilenceHardMs: 500,
        isStreaming: true,
      }),
    ).toBe("continue");
  });
});
