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
  agent: AgentInjectionConfig,
  providerOrder: readonly string[] = []
): ProviderConfig[] {
  // New providers are private to the Agent page that created them. Retain a
  // legacy shared provider only when this Agent is still explicitly bound to
  // it, so a user can select/clone it without suddenly seeing every other
  // Agent's historical provider list.
  const orderIndex = new Map(providerOrder.map((providerId, index) => [providerId, index]));
  return providers
    .map((provider, sourceIndex) => ({ provider, sourceIndex }))
    .filter(({ provider }) =>
    providerBelongsToAgent(provider.id, agent.id) || provider.id === agent.provider_id
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
