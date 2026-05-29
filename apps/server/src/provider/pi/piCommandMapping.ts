import type { ServerProviderSlashCommand } from "@t3tools/contracts";

import type { PiRpcCommandDescriptor } from "./piRpcTypes.ts";

function trimNonEmpty(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

export function mapPiRpcCommandsToSlashCommands(
  commands: ReadonlyArray<PiRpcCommandDescriptor>,
): ReadonlyArray<ServerProviderSlashCommand> {
  const byName = new Map<string, ServerProviderSlashCommand>();

  for (const command of commands) {
    const name = trimNonEmpty(command.name);
    if (!name) {
      continue;
    }

    const key = name.toLowerCase();
    if (byName.has(key)) {
      continue;
    }

    const description = trimNonEmpty(command.description);
    byName.set(key, {
      name,
      ...(description ? { description } : {}),
    });
  }

  return [...byName.values()].toSorted((left, right) => left.name.localeCompare(right.name));
}
