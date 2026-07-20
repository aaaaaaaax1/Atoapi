import type { AgentInjectionConfig, ProviderConfig } from "../lib/api";

/** Shared Agent-scope rule for Graphite presentation and write operations. */
export function providerBelongsToAgent(providerId: string, agentId: string): boolean {
  return providerId.startsWith(`agent-${providerIdPart(agentId)}-`);
}

export function providerCloneMatchesSource(
  providerId: string,
  sourceProviderId: string,
  agentId: string
): boolean {
  const base = `agent-${providerIdPart(agentId)}-${providerIdPart(sourceProviderId)}`;
  if (providerId === base) return true;
  const suffix = providerId.slice(base.length + 1);
  return providerId.startsWith(`${base}-`) && /^\d+$/.test(suffix);
}

export function providersForGraphiteAgent(
  providers: ProviderConfig[],
  agent: AgentInjectionConfig
): ProviderConfig[] {
  const hiddenProviderIds = new Set(agent.hidden_provider_ids ?? []);
  const ownedProviders = providers.filter((provider) => providerBelongsToAgent(provider.id, agent.id));
  const sharedProviders = providers.filter((provider) => !provider.id.startsWith("agent-"));
  const clonedSourceIds = new Set(
    ownedProviders
      .map((privateProvider) =>
        sharedProviders
          .slice()
          .sort((left, right) => right.id.length - left.id.length)
          .find((sharedProvider) =>
            providerCloneMatchesSource(privateProvider.id, sharedProvider.id, agent.id)
          )?.id
      )
      .filter((id): id is string => Boolean(id))
  );

  return providers.filter((provider) => {
    if (provider.id.startsWith("agent-")) return providerBelongsToAgent(provider.id, agent.id);
    return !clonedSourceIds.has(provider.id) && !hiddenProviderIds.has(provider.id);
  });
}

function providerIdPart(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "") || "provider";
}
