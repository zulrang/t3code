import type { TurnId } from "@t3tools/contracts";

export type PiWatchdogTickAction = "continue" | "fail" | "complete" | "stop";

export interface PiWatchdogTickInput {
  readonly turnCompletedEmitted: boolean;
  readonly activeTurnId: TurnId | undefined;
  readonly silenceMs: number;
  readonly turnSilenceHardMs: number;
  readonly isStreaming: boolean | undefined;
}

export function evaluatePiWatchdogTick(input: PiWatchdogTickInput): PiWatchdogTickAction {
  if (input.turnCompletedEmitted || !input.activeTurnId) {
    return "stop";
  }
  if (input.silenceMs >= input.turnSilenceHardMs) {
    return "fail";
  }
  if (input.isStreaming === false) {
    return "complete";
  }
  return "continue";
}
