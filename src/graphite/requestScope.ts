export interface AgentRequestScopeRecord {
  agent_id?: string | null;
  provider_id?: string | null;
  provider?: string | null;
}

export interface AgentRequestScope {
  id: string;
  label: string;
  providerId: string | null;
}

export interface AgentProviderTrafficScopeRecord {
  agent_id?: string | null;
  provider_id?: string | null;
  provider?: string | null;
  total_requests?: number | null;
  successful_requests?: number | null;
  error_statuses?: number | null;
  input_tokens?: number | null;
  output_tokens?: number | null;
  cache_read_tokens?: number | null;
  compaction_requests?: number | null;
  cache_shortfall_tokens?: number | null;
  cache_avoidable_gap_tokens?: number | null;
  cache_new_tail_gap_tokens?: number | null;
  cold_start_requests?: number | null;
  cold_start_input_tokens?: number | null;
  cold_start_output_tokens?: number | null;
  cold_start_cache_read_tokens?: number | null;
  cold_start_cache_shortfall_tokens?: number | null;
  cold_start_cache_avoidable_gap_tokens?: number | null;
  cold_start_cache_new_tail_gap_tokens?: number | null;
}

export interface AgentProviderTrafficTotals {
  totalRequests: number;
  successfulRequests: number;
  errors: number;
  inputTokens: number;
  outputTokens: number;
  cachedTokens: number;
  compactionRequests: number;
  cacheShortfallTokens: number;
  cacheAvoidableGapTokens: number;
  cacheNewTailGapTokens: number;
  coldStartRequests: number;
  coldStartInputTokens: number;
  coldStartOutputTokens: number;
  coldStartCachedTokens: number;
  coldStartCacheShortfallTokens: number;
  coldStartCacheAvoidableGapTokens: number;
  coldStartCacheNewTailGapTokens: number;
}

/**
 * The record panel is deliberately bounded for rendering; callers must keep
 * lifetime statistics separate from this display-only limit.
 */
export function limitVisibleRequestRecords<T>(records: readonly T[], limit: number): T[] {
  return records.slice(0, Math.max(0, Math.floor(limit)));
}

/**
 * Historical entries without an Agent id cannot be safely attributed, so they
 * stay out of an Agent-scoped view instead of leaking into another Agent.
 */
export function recordsForAgent<T extends AgentRequestScopeRecord>(
  records: readonly T[],
  agentId: string | null | undefined
): T[] {
  if (!agentId) return [];
  return records.filter((record) => record.agent_id === agentId);
}

/** Build tabs from successful traffic, never from merely configured providers. */
export function scopesForSuccessfulAgentRequests(
  records: readonly AgentRequestScopeRecord[]
): AgentRequestScope[] {
  const scopes: AgentRequestScope[] = [{ id: "all", label: "全部", providerId: null }];
  const providerIds = new Set<string>();

  for (const record of records) {
    const providerId = record.provider_id?.trim();
    if (!providerId || providerIds.has(providerId)) continue;
    providerIds.add(providerId);
    scopes.push({
      id: `provider:${providerId}`,
      label: record.provider?.trim() || providerId,
      providerId
    });
  }

  return scopes;
}

/**
 * Uses lifetime counters when the backend exposes them. `null` deliberately
 * means that the backend did not supply an aggregate for this exact scope.
 * Callers must keep it visibly distinct from a genuine zero-valued row.
 */
export function trafficForAgentScope(
  records: readonly AgentProviderTrafficScopeRecord[] | null | undefined,
  agentId: string | null | undefined,
  providerId: string | null
): AgentProviderTrafficTotals | null {
  if (!records || !agentId) return null;
  const rows = records.filter((record) =>
    record.agent_id === agentId && (!providerId || record.provider_id === providerId)
  );
  if (!rows.length) return null;
  return rows.reduce<AgentProviderTrafficTotals>((total, record) => ({
    totalRequests: total.totalRequests + count(record.total_requests),
    successfulRequests: total.successfulRequests + count(record.successful_requests),
    errors: total.errors + count(record.error_statuses),
    inputTokens: total.inputTokens + count(record.input_tokens),
    outputTokens: total.outputTokens + count(record.output_tokens),
    cachedTokens: total.cachedTokens + count(record.cache_read_tokens),
    compactionRequests: total.compactionRequests + count(record.compaction_requests),
    cacheShortfallTokens: total.cacheShortfallTokens + count(record.cache_shortfall_tokens),
    cacheAvoidableGapTokens: total.cacheAvoidableGapTokens + count(record.cache_avoidable_gap_tokens),
    cacheNewTailGapTokens: total.cacheNewTailGapTokens + count(record.cache_new_tail_gap_tokens),
    coldStartRequests: total.coldStartRequests + count(record.cold_start_requests),
    coldStartInputTokens: total.coldStartInputTokens + count(record.cold_start_input_tokens),
    coldStartOutputTokens: total.coldStartOutputTokens + count(record.cold_start_output_tokens),
    coldStartCachedTokens: total.coldStartCachedTokens + count(record.cold_start_cache_read_tokens),
    coldStartCacheShortfallTokens: total.coldStartCacheShortfallTokens + count(record.cold_start_cache_shortfall_tokens),
    coldStartCacheAvoidableGapTokens: total.coldStartCacheAvoidableGapTokens + count(record.cold_start_cache_avoidable_gap_tokens),
    coldStartCacheNewTailGapTokens: total.coldStartCacheNewTailGapTokens + count(record.cold_start_cache_new_tail_gap_tokens)
  }), {
    totalRequests: 0,
    successfulRequests: 0,
    errors: 0,
    inputTokens: 0,
    outputTokens: 0,
    cachedTokens: 0,
    compactionRequests: 0,
    cacheShortfallTokens: 0,
    cacheAvoidableGapTokens: 0,
    cacheNewTailGapTokens: 0,
    coldStartRequests: 0,
    coldStartInputTokens: 0,
    coldStartOutputTokens: 0,
    coldStartCachedTokens: 0,
    coldStartCacheShortfallTokens: 0,
    coldStartCacheAvoidableGapTokens: 0,
    coldStartCacheNewTailGapTokens: 0
  });
}

function count(value?: number | null): number {
  return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : 0;
}
