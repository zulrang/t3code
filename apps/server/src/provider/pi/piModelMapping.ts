import type { ModelCapabilities, ServerProviderModel } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";

import { buildSelectOptionDescriptor, nonEmptyTrimmed } from "../providerSnapshot.ts";
import { encodePiModelSlug } from "./piModelSlug.ts";
import { PiThinkingLevel, type PiRpcModel, type PiThinkingLevelMap } from "./piRpcTypes.ts";

const THINKING_LEVEL_LABELS: Record<PiThinkingLevel, string> = {
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
};

const ALL_PI_THINKING_LEVELS = [
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
] as const satisfies ReadonlyArray<PiThinkingLevel>;

const CONSERVATIVE_REASONING_LEVELS: ReadonlyArray<PiThinkingLevel> = [
  "off",
  "minimal",
  "low",
  "medium",
  "high",
];

function titleCaseSlug(value: string): string {
  return value
    .split(/[-_/]+/)
    .filter((segment) => segment.length > 0)
    .map((segment) => segment.charAt(0).toUpperCase() + segment.slice(1))
    .join(" ");
}

function resolveThinkingLevels(model: PiRpcModel): ReadonlyArray<PiThinkingLevel> {
  if (model.reasoning !== true) {
    return [];
  }

  const map = model.thinkingLevelMap;
  if (map) {
    return ALL_PI_THINKING_LEVELS.filter((level) => level in map && map[level] !== null);
  }

  return CONSERVATIVE_REASONING_LEVELS;
}

function thinkingCapabilitiesForModel(model: PiRpcModel): ModelCapabilities {
  const levels = resolveThinkingLevels(model);
  if (levels.length === 0) {
    return createModelCapabilities({ optionDescriptors: [] });
  }

  const defaultLevel = levels.includes("medium")
    ? "medium"
    : levels.includes("low")
      ? "low"
      : levels[0];

  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "thinkingLevel",
        label: "Thinking",
        options: levels.map((level) => ({
          value: level,
          label: THINKING_LEVEL_LABELS[level],
          isDefault: level === defaultLevel,
        })),
      }),
    ],
  });
}

export function mapPiRpcModelToServerModel(model: PiRpcModel): ServerProviderModel | null {
  const provider = nonEmptyTrimmed(model.provider);
  const modelId = nonEmptyTrimmed(model.id);
  if (!provider || !modelId) {
    return null;
  }

  const name = nonEmptyTrimmed(model.name) ?? modelId;
  const subProvider = titleCaseSlug(provider);

  return {
    slug: encodePiModelSlug(provider, modelId),
    name,
    subProvider,
    isCustom: false,
    capabilities: thinkingCapabilitiesForModel(model),
  };
}

export function mapPiRpcModelsToServerModels(
  models: ReadonlyArray<PiRpcModel>,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const mapped: ServerProviderModel[] = [];

  for (const model of models) {
    const entry = mapPiRpcModelToServerModel(model);
    if (!entry || seen.has(entry.slug)) {
      continue;
    }
    seen.add(entry.slug);
    mapped.push(entry);
  }

  return mapped.toSorted((left, right) => left.name.localeCompare(right.name));
}

export function resolvePiThinkingLevelForModel(
  model: PiRpcModel,
  requestedLevel: PiThinkingLevel,
): PiThinkingLevel | null {
  if (model.reasoning !== true) {
    return null;
  }

  const map: PiThinkingLevelMap | undefined = model.thinkingLevelMap;
  if (map) {
    if (!(requestedLevel in map)) {
      return null;
    }
    const mapped = map[requestedLevel];
    return mapped ?? null;
  }

  return CONSERVATIVE_REASONING_LEVELS.includes(requestedLevel) ? requestedLevel : null;
}
