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

/**
 * Prefixes are display-friendly names, not durable ownership records. An
 * Agent may manage a provider only after it appears in its binding or saved
 * order; hidden records stay hidden even if an old order still contains them.
 */
export function providerIsRegisteredToAgent(
  providerId: string,
  agent: AgentInjectionConfig,
  providerOrder: readonly string[] = []
): boolean {
  return !(agent.hidden_provider_ids ?? []).includes(providerId)
    && (providerId === agent.provider_id || providerOrder.includes(providerId));
}

export function providerIsTrustedPrivateToAgent(
  providerId: string,
  agent: AgentInjectionConfig,
  providerOrder: readonly string[] = []
): boolean {
  return providerBelongsToAgent(providerId, agent.id)
    && providerIsRegisteredToAgent(providerId, agent, providerOrder);
}

export function providersForGraphiteAgent(
  providers: ProviderConfig[],
  agent: AgentInjectionConfig,
  providerOrder: readonly string[] = []
): ProviderConfig[] {
  // A provider appears only through this Agent's explicit binding or saved
  // order. This keeps manual/legacy `agent-...` IDs from being mistaken for
  // current private records after a refresh.
  const orderIndex = new Map(providerOrder.map((providerId, index) => [providerId, index]));
  const hiddenProviderIds = new Set(agent.hidden_provider_ids ?? []);
  return providers
    .map((provider, sourceIndex) => ({ provider, sourceIndex }))
    .filter(({ provider }) =>
      !hiddenProviderIds.has(provider.id)
        && providerIsRegisteredToAgent(provider.id, agent, providerOrder)
    )
    .sort((left, right) => {
      const leftOrder = orderIndex.get(left.provider.id) ?? Number.MAX_SAFE_INTEGER;
      const rightOrder = orderIndex.get(right.provider.id) ?? Number.MAX_SAFE_INTEGER;
      return leftOrder - rightOrder || left.sourceIndex - right.sourceIndex;
    })
    .map(({ provider }) => provider);
}

function providerIdPart(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "") || "provider";
}
