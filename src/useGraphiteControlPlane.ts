import { useEffect, useRef, useState } from "react";
import {
  command,
  model,
  type AgentInjectionConfig,
  type AgentInjectionResult,
  type AppConfig,
  type CacheValidationMode,
  type CacheValidationStatus,
  type Channel,
  type FetchModelsInput,
  type MetricsSnapshot,
  type ModelConfig,
    type ProviderConfig,
    type ProviderBalanceProbeResult,
    type ProviderCacheCapabilityProbeResult,
    type ProviderConnectionPathTestResult,
  type ProviderHealthProbeInput,
  type ProviderHealthProbeResult,
  type ProviderInput,
  type ProviderKeyPoolHealthSnapshot,
  type ProviderKeyTestResult,
  type ProviderNetworkPathDiagnosticResult,
  type ProxyStatus
} from "./lib/api";
import type {
  GraphiteBridgeResponse,
  GraphitePrototypeHostProps,
  GraphiteProviderPayload
} from "./graphite/frameProtocol";
import {
  providerIsTrustedPrivateToAgent,
  providersForGraphiteAgent
} from "./graphite/providerScope";

// Keep the UI version badge tied to the package that is actually being built.
// Vite injects this value from package.json, so candidate builds cannot silently
// retain an old champion version when a new candidate is assembled.
const APP_VERSION = __ATOAPI_APP_VERSION__;
type MetricsRefreshPolicy = "visible-1s" | "5s" | "manual";
type RequestLogEntry = MetricsSnapshot["recent_requests"][number];
type MetricsSnapshotFetch = {
  sequence: number;
  snapshot: MetricsSnapshot;
};
const PROVIDER_BALANCE_REFRESH_MS = 15 * 60 * 1000;
const CACHE_VALIDATION_REFRESH_MS = 5_000;
const PROVIDER_BALANCE_BATCH_SIZE = 4;
const PROVIDER_CONNECTION_BATCH_SIZE = 4;

function cacheValidationFingerprint(value: CacheValidationStatus | null): string {
  return value ? JSON.stringify(value) : "";
}

function formatBalanceNotice(value: string | null | undefined): string {
  const text = String(value ?? "").trim();
  if (!text || text === "无限额度") return text;
  const normalized = text.replace(/,/g, "");
  return /^[+-]?\d+(?:\.\d+)?$/.test(normalized)
    ? Number(normalized).toFixed(2)
    : text;
}

/**
 * The balance belongs to a provider/key scope, not to transient health
 * bookkeeping.  Connection tests update key status/last_checked_at in the
 * persisted config; those fields must not invalidate a previously measured
 * balance.  Only routing identity and the public key snapshot participate in
 * this fingerprint, so a replaced URL/key still clears stale quota data.
 */
function providerBalanceScopeFingerprint(provider: ProviderConfig): string {
  const pool = provider.key_pool;
  return JSON.stringify({
    id: provider.id,
    base_url: provider.base_url,
    channel: provider.channel,
    channel_mode: provider.channel_mode,
    is_full_url: provider.is_full_url,
    models_url: provider.models_url ?? null,
    custom_user_agent: provider.custom_user_agent ?? null,
    use_system_proxy: provider.use_system_proxy,
    enabled: provider.enabled,
    has_api_key: provider.has_api_key,
    key_pool: pool
      ? {
          enabled: pool.enabled,
          keys: pool.keys.map((key) => ({
            id: key.id,
            preview: key.preview,
            enabled: key.enabled,
            has_saved_secret: key.has_saved_secret
          }))
        }
      : null
  });
}

function providersForOpenAgents(config: AppConfig): ProviderConfig[] {
  const openProviderIds = new Set<string>();
  const openAgents = (config.agent_injections ?? []).filter((agent) => agent.enabled);
  for (const agent of openAgents) {
    const order = config.agent_provider_orders?.find((entry) => entry.agent_id === agent.id)?.provider_ids ?? [];
    for (const provider of providersForGraphiteAgent(config.providers, agent, order)) {
      openProviderIds.add(provider.id);
    }
    if (agent.provider_id) openProviderIds.add(agent.provider_id);
  }
  if (!openProviderIds.size && config.active_provider_id) {
    openProviderIds.add(config.active_provider_id);
  }
  const candidates = config.providers.filter((provider) =>
    provider.enabled &&
    openProviderIds.has(provider.id) &&
    (provider.has_api_key || (provider.key_pool?.enabled === true && provider.key_pool.available_keys > 0))
  );
  return candidates.length ? candidates : config.providers.filter((provider) =>
    provider.enabled &&
    (provider.has_api_key || (provider.key_pool?.enabled === true && provider.key_pool.available_keys > 0))
  );
}

function providerConnectionTestInput(provider: ProviderConfig) {
  return {
    provider_id: provider.id,
    key_id: null,
    api_key: null,
    base_url: provider.base_url,
    models_url: provider.models_url ?? null,
    is_full_url: provider.is_full_url,
    custom_user_agent: provider.custom_user_agent ?? null,
    channel: provider.channel,
    use_system_proxy: provider.use_system_proxy
  };
}

function providerProbeScopeSignature(config: AppConfig | null): string {
  if (!config) return "";
  return JSON.stringify({
    active_provider_id: config.active_provider_id ?? null,
    providers: config.providers.map(providerBalanceScopeFingerprint),
    agents: (config.agent_injections ?? [])
      .filter((agent) => agent.enabled)
      .map((agent) => ({
        id: agent.id,
        provider_id: agent.provider_id ?? null,
        provider_order: config.agent_provider_orders?.find((entry) => entry.agent_id === agent.id)?.provider_ids ?? []
      }))
  });
}

/**
 * The one control-plane module used by the accepted Graphite shell.
 * It owns local UI state and turns iframe actions into Tauri commands; no
 * legacy JSX surface or legacy editor state is retained by callers.
 */
export function useGraphiteControlPlane(): GraphitePrototypeHostProps {
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [selectedAgentId, setSelectedAgentId] = useState("");
  const [includeColdStarts, setIncludeColdStarts] = useState(true);
  const [includeCompactions, setIncludeCompactions] = useState(true);
  const [showDetailedErrors, setShowDetailedErrors] = useState(false);
  const [providerConnectionStatus, setProviderConnectionStatus] = useState<Record<string, string>>({});
  const [providerBalanceStatus, setProviderBalanceStatus] = useState<Record<string, ProviderBalanceProbeResult>>({});
  const [metricsRefreshPolicy, setMetricsRefreshPolicy] = useState<MetricsRefreshPolicy>("visible-1s");
  const [proxyStatus, setProxyStatus] = useState<ProxyStatus | null>(null);
  const [networkPathDiagnostic, setNetworkPathDiagnostic] =
    useState<ProviderNetworkPathDiagnosticResult | null>(null);
  const [cacheValidation, setCacheValidation] = useState<CacheValidationStatus | null>(null);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const reasoningFallbackSyncing = useRef(false);
  const seenReasoningFallbackFailures = useRef(new Set<string>());
  const balanceProbeGeneration = useRef(0);
  const balanceProbeInFlight = useRef(false);
  const balanceProbeInFlightGeneration = useRef(0);
  const balanceProbeScopes = useRef<Record<string, string>>({});
  const startupConnectionProbeScope = useRef("");
  const metricsRefreshInFlight = useRef<Promise<MetricsSnapshotFetch | null> | null>(null);
  const metricsRefreshSequence = useRef(0);
  const metricsAppliedSequence = useRef(0);
  const providerProbeScope = providerProbeScopeSignature(config);

  async function refreshAll() {
    setError("");
    try {
      const [nextConfig, nextMetrics, nextProxyStatus, nextCacheValidation] = await Promise.all([
        command<AppConfig>("reload_config"),
        loadMetricsSnapshot(),
        command<ProxyStatus>("get_proxy_status"),
        command<CacheValidationStatus>("get_cache_validation_status")
      ]);
      setConfig(nextConfig);
      applyMetricsSnapshot(nextMetrics);
      setProxyStatus(nextProxyStatus);
      setCacheValidation((current) =>
        cacheValidationFingerprint(current) === cacheValidationFingerprint(nextCacheValidation)
          ? current
          : nextCacheValidation
      );
      const agents = visibleAgentInjections(nextConfig.agent_injections);
      setSelectedAgentId((current) => {
        if (current && agents.some((agent) => agent.id === current)) return current;
        return (agents.find((agent) => agent.enabled) ?? agents[0])?.id ?? "";
      });
    } catch (cause) {
      setError(String(cause));
    }
  }

  function loadMetricsSnapshot(forceAfterCurrent = false): Promise<MetricsSnapshotFetch | null> {
    const inFlight = metricsRefreshInFlight.current;
    if (inFlight) {
      // A post-mutation caller (for example clear-cache) must not accept a
      // snapshot that began before the mutation. It waits, then starts one
      // fresh read; normal 1s polling simply shares the in-flight request.
      return forceAfterCurrent
        ? inFlight.then(() => loadMetricsSnapshot(false))
        : inFlight;
    }
    const sequence = ++metricsRefreshSequence.current;
    let task: Promise<MetricsSnapshotFetch | null>;
    task = command<MetricsSnapshot>("get_metrics")
      .then((snapshot) => ({ sequence, snapshot }))
      .catch(() => null)
      .finally(() => {
        if (metricsRefreshInFlight.current === task) metricsRefreshInFlight.current = null;
      });
    metricsRefreshInFlight.current = task;
    return task;
  }

  function applyMetricsSnapshot(result: MetricsSnapshotFetch | null) {
    if (!result || result.sequence < metricsAppliedSequence.current) return;
    metricsAppliedSequence.current = result.sequence;
    setMetrics(result.snapshot);
  }

  async function refreshMetrics(forceAfterCurrent = false): Promise<void> {
    applyMetricsSnapshot(await loadMetricsSnapshot(forceAfterCurrent));
  }

  async function refreshCacheValidation() {
    try {
      const nextCacheValidation = await command<CacheValidationStatus>("get_cache_validation_status");
      setCacheValidation((current) =>
        cacheValidationFingerprint(current) === cacheValidationFingerprint(nextCacheValidation)
          ? current
          : nextCacheValidation
      );
    } catch {
      // Cache-validation progress is advisory UI state; retain the last verified snapshot.
    }
  }

  async function syncPersistedModelReasoningFallback() {
    setConfig(await command<AppConfig>("get_config"));
  }

  useEffect(() => {
    void refreshAll();
  }, []);

  useEffect(() => {
    if (metricsRefreshPolicy === "manual") return;
    const intervalMs = metricsRefreshPolicy === "5s" ? 5_000 : 1_000;
    const refreshIfAllowed = () => {
      if (metricsRefreshPolicy === "visible-1s" && document.visibilityState !== "visible") return;
      void refreshMetrics();
    };
    const timer = window.setInterval(refreshIfAllowed, intervalMs);
    return () => window.clearInterval(timer);
  }, [metricsRefreshPolicy]);

  useEffect(() => {
    if (metricsRefreshPolicy === "manual" || !cacheValidation || cacheValidation.mode === "auto") return;
    void refreshCacheValidation();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void refreshCacheValidation();
    }, CACHE_VALIDATION_REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [cacheValidation?.mode, metricsRefreshPolicy]);

  useEffect(() => {
    const fallbackRequests = [
      ...(metrics?.recent_failed_requests ?? []),
      ...(metrics?.recent_requests ?? [])
    ].filter(isPersistedModelReasoningFallbackRequest);
    const hasUnseenFallback = fallbackRequests.some(
      (request) => !seenReasoningFallbackFailures.current.has(request.id)
    );
    if (!hasUnseenFallback || reasoningFallbackSyncing.current) return;

    reasoningFallbackSyncing.current = true;
    void syncPersistedModelReasoningFallback().then(
      () => fallbackRequests.forEach((request) => seenReasoningFallbackFailures.current.add(request.id)),
      () => undefined
    ).finally(() => {
      reasoningFallbackSyncing.current = false;
    });
  }, [metrics]);

  useEffect(() => {
    const agents = visibleAgentInjections(config?.agent_injections ?? []);
    if (!agents.length) return;
    setSelectedAgentId((current) =>
      current && agents.some((agent) => agent.id === current)
        ? current
        : (agents.find((agent) => agent.enabled) ?? agents[0]).id
    );
  }, [config]);

  useEffect(() => {
    // A balance result belongs to the exact saved Key/config snapshot that
    // produced it. Any config/key change invalidates the old display until a
    // fresh probe completes; this prevents a retired Key's quota from being
    // shown for the newly selected Key.
    const previousScopes = balanceProbeScopes.current;
    const nextScopes = Object.fromEntries(
      (config?.providers ?? []).map((provider) => [provider.id, providerBalanceScopeFingerprint(provider)])
    );
    balanceProbeScopes.current = nextScopes;
    balanceProbeGeneration.current += 1;
    const generation = balanceProbeGeneration.current;
    // Keep balances for unchanged provider/key scopes.  A connectivity/Key
    // health test refreshes config metadata, but it does not replace the Key;
    // clearing every row in that case made the UI briefly (and needlessly)
    // report "余额未探测" for all upstreams.  Changed or removed scopes are
    // pruned and will be re-probed below.
    setProviderBalanceStatus((current) => Object.fromEntries(
      Object.entries(current).filter(([providerId]) =>
        nextScopes[providerId] && previousScopes[providerId] === nextScopes[providerId]
      )
    ));
    if (!config) return;

    let disposed = false;
    const candidates = providersForOpenAgents(config);
    const probeAllProviderBalances = async () => {
      if (disposed || (balanceProbeInFlight.current && balanceProbeInFlightGeneration.current === generation)) return;
      balanceProbeInFlight.current = true;
      balanceProbeInFlightGeneration.current = generation;
      try {
        const next: Record<string, ProviderBalanceProbeResult> = {};
        for (let offset = 0; offset < candidates.length; offset += PROVIDER_BALANCE_BATCH_SIZE) {
          if (disposed || balanceProbeGeneration.current !== generation) return;
          const batch = candidates.slice(offset, offset + PROVIDER_BALANCE_BATCH_SIZE);
          const results = await Promise.all(batch.map(async (provider) => {
            try {
              return await command<ProviderBalanceProbeResult>("probe_provider_balance", {
                providerId: provider.id,
                provider_id: provider.id
              });
            } catch {
              return {
                provider_id: provider.id,
                supported: false,
                ok: false,
                elapsed_ms: 0,
                balance: null,
                message: "balance_probe_unavailable"
              } satisfies ProviderBalanceProbeResult;
            }
          }));
          results.forEach((result) => { next[result.provider_id] = result; });
        }
        if (!disposed && balanceProbeGeneration.current === generation) {
          setProviderBalanceStatus(next);
        }
      } finally {
        if (balanceProbeInFlightGeneration.current === generation) {
          balanceProbeInFlight.current = false;
        }
      }
    };

    const probeConnectionsOnOpen = async () => {
      const connectionScope = `${selectedAgentId}|${candidates.map((provider) => providerBalanceScopeFingerprint(provider)).join(";")}`;
      if (startupConnectionProbeScope.current === connectionScope) return;
      startupConnectionProbeScope.current = connectionScope;
      const next: Record<string, string> = {};
      for (let offset = 0; offset < candidates.length; offset += PROVIDER_CONNECTION_BATCH_SIZE) {
        if (disposed || balanceProbeGeneration.current !== generation) return;
        const batch = candidates.slice(offset, offset + PROVIDER_CONNECTION_BATCH_SIZE);
        const results = await Promise.all(batch.map(async (provider) => {
          const startedAt = performance.now();
          try {
            const result = await command<ProviderConnectionPathTestResult>("test_provider_connection_paths", {
              input: providerConnectionTestInput(provider)
            });
            const selectedPath = result.paths.find((path) =>
              result.recommended_use_system_proxy ? path.path === "system-proxy" : path.path === "direct"
            );
            const pathLabel = result.recommended_use_system_proxy ? "系统代理更快" : "直连更快";
            return [
              provider.id,
              result.ok
                ? `${pathLabel} · ${selectedPath?.elapsed_ms ?? Math.max(0, Math.round(performance.now() - startedAt))}ms`
                : `测试失败 · ${Math.max(0, Math.round(performance.now() - startedAt))}ms`
            ] as const;
          } catch {
            return [provider.id, "测试失败"] as const;
          }
        }));
        results.forEach(([providerId, status]) => { next[providerId] = status; });
        if (!disposed && balanceProbeGeneration.current === generation) {
          setProviderConnectionStatus((current) => ({ ...current, ...next }));
        }
      }
    };

    void probeAllProviderBalances();
    void probeConnectionsOnOpen();
    const timer = window.setInterval(() => { void probeAllProviderBalances(); }, PROVIDER_BALANCE_REFRESH_MS);
    return () => {
      disposed = true;
      window.clearInterval(timer);
    };
  }, [providerProbeScope, selectedAgentId]);

  async function saveProviderFromGraphite(agentId: string, payload: GraphiteProviderPayload) {
    const name = payload.name.trim();
    const baseUrl = payload.base_url.trim();
    if (!name || !baseUrl) throw new Error("上游名称和 Base URL 不能为空");

    setError("");
    setNotice("");
    const editablePayload = payload;
    let existing = config?.providers.find((provider) => provider.id === editablePayload.id) ?? null;
    const editingAgent = config?.agent_injections.find((agent) => agent.id === agentId);
    const editingOrder = config?.agent_provider_orders
      ?.find((order) => order.agent_id === agentId)?.provider_ids ?? [];
    const editingBoundLegacyProvider = Boolean(
      agentId && existing && editingAgent
        && !providerIsTrustedPrivateToAgent(existing.id, editingAgent, editingOrder)
    );

    const existingKeys = new Map((existing?.key_pool?.keys ?? []).map((key) => [key.id, key]));
    const input: ProviderInput = {
      id: editablePayload.id ?? undefined,
      owner_agent_id: agentId || undefined,
      name,
      base_url: baseUrl,
      models_url: cleanOptionalText(editablePayload.models_url) ?? undefined,
      is_full_url: existing?.is_full_url ?? false,
      custom_user_agent: cleanOptionalText(editablePayload.custom_user_agent) ?? undefined,
      channel_mode: editablePayload.channel_mode,
      channel: editablePayload.channel,
      prompt_cache_retention_enabled: editablePayload.prompt_cache_retention_enabled,
      request_body_gzip_enabled: editablePayload.request_body_gzip_enabled,
      use_system_proxy: editablePayload.use_system_proxy,
      non_sse_compact_compat_enabled: editablePayload.non_sse_compact_compat_enabled,
      auto_compact_token_limit: editablePayload.auto_compact_token_limit ?? null,
      auto_compact_token_limit_configured: true,
      key_pool: {
        enabled: editablePayload.key_pool?.enabled ?? existing?.key_pool?.enabled ?? editablePayload.keys.length > 0,
        strategy: editablePayload.key_pool?.strategy ?? existing?.key_pool?.strategy ?? "sequential",
        failure_threshold: editablePayload.key_pool?.failure_threshold ?? existing?.key_pool?.failure_threshold ?? 3,
        recovery_minutes: editablePayload.key_pool?.recovery_minutes ?? existing?.key_pool?.recovery_minutes ?? 10,
        keys: editablePayload.keys.map((key) => {
          const prior = key.id ? existingKeys.get(key.id) : undefined;
          return {
            id: key.id,
            alias: cleanOptionalText(key.alias) ?? prior?.alias ?? null,
            key: cleanOptionalText(key.key) ?? null,
            enabled: key.enabled ?? prior?.enabled ?? true,
            priority: key.priority,
            status: prior?.status ?? "unknown",
            total_requests: prior?.total_requests ?? 0,
            successes: prior?.successes ?? 0,
            failures: prior?.failures ?? 0,
            last_checked_at: prior?.last_checked_at ?? null,
            last_error: prior?.last_error ?? null,
            disabled_until: prior?.disabled_until ?? null
          };
        })
      },
      api_key: cleanOptionalText(editablePayload.api_key) ?? undefined,
      enabled: existing?.enabled ?? true
    };

    try {
      let nextConfig = await command<AppConfig>("add_or_update_provider", { input });
      let savedProvider =
        nextConfig.providers.find((provider) => provider.id === editablePayload.id) ??
        (editingBoundLegacyProvider
          ? nextConfig.providers.find((provider) =>
              provider.id === nextConfig.agent_injections.find((agent) => agent.id === agentId)?.provider_id
            )
          : undefined) ??
        nextConfig.providers.find((provider) => provider.name === name && provider.base_url === baseUrl);
      if (!savedProvider) throw new Error("上游保存后未返回配置记录");

      const previousModelIds = new Set(existing?.models.map((item) => item.id) ?? []);
      const nextModels = normalizeGraphiteModels(editablePayload.models).map((item) => {
        const prior = existing?.models.find((modelItem) => modelItem.id === item.id);
        return {
          ...model(item.id),
          ...prior,
          id: item.id,
          display_name: prior?.display_name ?? item.id,
          request_model_id: cleanOptionalText(item.request_model_id) ?? null,
          context_window: item.context_window ?? null,
          reasoning_effort_override_enabled: Boolean(item.reasoning_effort),
          reasoning_effort: cleanOptionalText(item.reasoning_effort) ?? null,
          enabled: prior?.enabled ?? true
        };
      });
      for (const modelItem of nextModels) {
        nextConfig = await command<AppConfig>("add_or_update_model", {
          input: { provider_id: savedProvider.id, model: modelItem }
        });
      }
      for (const modelId of previousModelIds) {
        if (!nextModels.some((item) => item.id === modelId)) {
          nextConfig = await command<AppConfig>("delete_model", {
            providerId: savedProvider.id,
            provider_id: savedProvider.id,
            modelId,
            model_id: modelId
          });
        }
      }
      setConfig(nextConfig);
      setNotice("上游配置已保存，当前 Agent 的使用上游未改变");
    } catch (cause) {
      const message = String(cause);
      setError(message);
      throw cause;
    }
  }

  async function deleteProviderFromGraphite(agentId: string, providerId: string): Promise<string> {
    if (!providerId) return "";
    setError("");
    try {
      const nextConfig = await command<AppConfig>("delete_provider", {
        providerId,
        provider_id: providerId,
        agentId: agentId || null,
        agent_id: agentId || null
      });
      setConfig(nextConfig);
      const notice = agentId ? "已从当前 Agent 移除" : "上游已删除";
      setNotice(notice);
      return notice;
    } catch (cause) {
      const message = String(cause);
      setError(message);
      throw cause;
    }
  }

  async function toggleAgentInjection(agent: AgentInjectionConfig): Promise<string> {
    if (!agent.enabled && !agent.provider_id) {
      setSelectedAgentId(agent.id);
      throw new Error("请先为当前 Agent 明确选择上游，然后再开启注入。");
    }
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("set_agent_injection_enabled", {
        input: { id: agent.id, enabled: !agent.enabled }
      });
      setConfig(await command<AppConfig>("get_config"));
      return results[0]?.status ?? `${agent.label} 已更新`;
    } catch (cause) {
      setError(String(cause));
      throw cause;
    }
  }

  async function activateAgentProvider(
    agent: AgentInjectionConfig,
    provider: ProviderConfig,
    enableAfterBind = false
  ): Promise<string> {
    setError("");
    setNotice("");
    try {
      await command<AgentInjectionResult[]>("update_agent_injection_route", {
        input: {
          id: agent.id,
          provider_id: provider.id,
          model_id: agent.model_id && provider.models.some((model) =>
            model.enabled && (model.id === agent.model_id || model.request_model_id === agent.model_id)
          ) ? agent.model_id : null
        }
      });
      let latestConfig = await command<AppConfig>("get_config");
      const latestAgent = latestConfig.agent_injections.find((item) => item.id === agent.id);
      if (enableAfterBind && !latestAgent?.enabled) {
        await command<AgentInjectionResult[]>("set_agent_injection_enabled", {
          input: { id: agent.id, enabled: true }
        });
        latestConfig = await command<AppConfig>("get_config");
      }
      setConfig(latestConfig);
      return enableAfterBind
        ? `${agent.label} 已启用并绑定 ${provider.name}`
        : `${agent.label} 已绑定 ${provider.name}，未打开开关时不会注入`;
    } catch (cause) {
      setError(String(cause));
      throw cause;
    }
  }

  async function testSavedProviderKeyHealth(providerId: string): Promise<GraphiteBridgeResponse> {
    const provider = config?.providers.find((item) => item.id === providerId);
    if (!provider) throw new Error("未找到待测试的上游配置");

    // The provider-list button compares direct and system-proxy connectivity
    // and reports the faster path, rather than only saying the current Key is usable.
    const startedAt = performance.now();
    const result = await command<ProviderConnectionPathTestResult>("test_provider_connection_paths", {
      input: providerConnectionTestInput(provider)
    });
    const elapsedMs = Math.max(0, Math.round(performance.now() - startedAt));
    const selectedPath = result.paths.find((path) =>
      result.recommended_use_system_proxy ? path.path === "system-proxy" : path.path === "direct"
    );
    const pathLabel = result.recommended_use_system_proxy ? "系统代理更快" : "直连更快";
    setProviderConnectionStatus((current) => ({
      ...current,
      [provider.id]: result.ok
        ? `${pathLabel} · ${selectedPath?.elapsed_ms ?? elapsedMs}ms`
        : `测试失败 · ${elapsedMs}ms`
    }));
    return result.ok
      ? {
          notice: `${provider.name} 连通正常 · 推荐${pathLabel}${selectedPath ? ` ${selectedPath.elapsed_ms}ms${selectedPath.http_version ? ` · ${selectedPath.http_version}` : ""}` : ""}${result.models_count ? ` · ${result.models_count} 个模型` : ""}；代理开关未被修改，请手动决定。`,
          payload: { connectionTest: result }
        }
      : { error: result.message, payload: { connectionTest: result } };
  }

  async function testDraftProviderConnection(
    draft: GraphiteProviderPayload,
    providerId: string
  ): Promise<GraphiteBridgeResponse> {
    const savedProvider = config?.providers.find((item) => item.id === providerId);
    const input = draftProviderTestInput(draft, null, savedProvider?.is_full_url ?? false);
    if (!input.base_url.trim()) throw new Error("请先填写 Base URL");
    if (!input.api_key?.trim() && !savedProvider) throw new Error("请先填写 API Key");
    const startedAt = performance.now();
    const result = await command<ProviderConnectionPathTestResult>("test_provider_connection_paths", { input });
    const elapsedMs = Math.max(0, Math.round(performance.now() - startedAt));
    const selectedPath = result.paths.find((path) =>
      result.recommended_use_system_proxy ? path.path === "system-proxy" : path.path === "direct"
    );
    const pathLabel = result.recommended_use_system_proxy ? "系统代理" : "直连";
    const statusKey = savedProvider?.id ?? `draft:${draft.name || draft.base_url}`;
    setProviderConnectionStatus((current) => ({
      ...current,
      [statusKey]: result.ok ? `${pathLabel}更快 · ${selectedPath?.elapsed_ms ?? elapsedMs}ms` : `失败 · ${elapsedMs}ms`
    }));
    return result.ok
      ? {
          notice: `${draft.name || savedProvider?.name || "未保存上游"} 连通正常 · 推荐${pathLabel}${selectedPath ? ` ${selectedPath.elapsed_ms}ms${selectedPath.http_version ? ` · ${selectedPath.http_version}` : ""}` : ""}${result.models_count ? ` · ${result.models_count} 个模型` : ""}；当前代理开关未被修改，请手动决定。`,
          payload: { connectionTest: result }
        }
      : { error: result.message };
  }

  async function onBridgeAction(
    action: string,
    payload: Record<string, unknown>
  ): Promise<GraphiteBridgeResponse | void> {
    const text = (key: string) => String(payload[key] ?? "").trim();
    const providerPayload = () => (payload.provider ?? payload) as GraphiteProviderPayload;

    if (action === "refresh") {
      await refreshAll();
      return { notice: "配置和统计已刷新" };
    }
    if (action === "select-agent") {
      const agentId = text("agentId");
      if (agentId) setSelectedAgentId(agentId);
      return;
    }
    if (action === "toggle-agent") {
      const agent = config?.agent_injections.find((item) => item.id === text("agentId"));
      if (!agent) throw new Error("未找到当前 Agent 配置");
      return { notice: await toggleAgentInjection(agent) };
    }
    if (action === "bind-provider") {
      const agent = config?.agent_injections.find((item) => item.id === text("agentId"));
      const provider = config?.providers.find((item) => item.id === text("providerId"));
      if (!agent || !provider) throw new Error("未找到 Agent 或上游配置");
      return { notice: await activateAgentProvider(agent, provider) };
    }
    if (action === "reorder-providers") {
      const agentId = text("agentId");
      const providerIds = Array.isArray(payload.providerIds)
        ? payload.providerIds.map((providerId) => String(providerId).trim()).filter(Boolean)
        : [];
      if (!agentId || !providerIds.length) throw new Error("上游排序数据不完整");
      const nextConfig = await command<AppConfig>("reorder_agent_providers", {
        input: { agent_id: agentId, provider_ids: providerIds }
      });
      setConfig(nextConfig);
      return { notice: "当前 Agent 的上游顺序已保存" };
    }
    if (action === "save-provider") {
      await saveProviderFromGraphite(
        String(payload.agentId ?? ""),
        payload as unknown as GraphiteProviderPayload
      );
      return { notice: "上游已保存，当前使用上游未改变", closeOverlay: "providerOverlay" };
    }
    if (action === "delete-provider") {
      const notice = await deleteProviderFromGraphite(text("agentId"), text("providerId"));
      return { notice, closeOverlay: "confirmOverlay" };
    }
    if (action === "fetch-models") {
      const provider = providerPayload();
      const baseUrl = provider.base_url?.trim();
      if (!baseUrl) throw new Error("请先填写 Base URL");
      const existingProvider = provider.id
        ? config?.providers.find((item) => item.id === provider.id)
        : undefined;
      const models = await command<ModelConfig[]>("fetch_provider_models", {
        input: {
          provider_id: provider.id ?? undefined,
          name: provider.name?.trim() || undefined,
          base_url: baseUrl,
          models_url: cleanOptionalText(provider.models_url) ?? undefined,
          is_full_url: existingProvider?.is_full_url ?? false,
          custom_user_agent: cleanOptionalText(provider.custom_user_agent) ?? undefined,
          channel: provider.channel || "responses",
          api_key: cleanOptionalText(provider.api_key) ?? undefined,
          use_system_proxy: Boolean(provider.use_system_proxy)
        } satisfies FetchModelsInput
      });
      return {
        notice: models.length
          ? `已获取 ${models.length} 个模型；选择并添加后才会保存，也可继续手动输入`
          : "未返回模型；仍可手动填写实际模型 ID",
        payload: { models: models.map((item) => ({ id: item.id })) }
      };
    }
    if (action === "fetch-health-probe-models") {
      const providerId = text("providerId");
      if (!providerId) throw new Error("请先保存上游，再获取测活模型");
      const models = await command<ModelConfig[]>("fetch_provider_health_models", {
        providerId,
        provider_id: providerId
      });
      return {
        notice: models.length ? `已获取 ${models.length} 个模型` : "未返回模型列表",
        payload: { healthProbeModels: models.map((item) => ({ id: item.id })) }
      };
    }
    if (action === "run-health-probe") {
      const providerId = text("providerId");
      const modelId = text("modelId");
      const requestedMode = text("mode") as ProviderHealthProbeInput["mode"];
      const mode = requestedMode === "minimal_cost" ? "responses_streaming" : requestedMode;
      const target = text("target") as ProviderHealthProbeInput["target"];
      const keyIds = Array.isArray(payload.keyIds)
        ? payload.keyIds.map((keyId) => String(keyId).trim()).filter(Boolean)
        : [];
      if (!providerId) throw new Error("请先保存上游，再执行测活");
      if (!modelId) throw new Error("请选择测活模型");
      const supportedModes: ProviderHealthProbeInput["mode"][] = [
        "minimal_cost",
        "responses_streaming",
        "chat_streaming",
        "chat_json",
        "responses_json",
        "anthropic_streaming",
        "anthropic_json"
      ];
      if (!supportedModes.includes(requestedMode)) {
        throw new Error("测活请求形态无效");
      }
      const result = await command<ProviderHealthProbeResult>("probe_provider_health", {
        input: {
          provider_id: providerId,
          key_ids: keyIds,
          target: ["current", "all_enabled", "selected"].includes(target ?? "") ? target : "current",
          model: modelId,
          mode,
          prompt: text("prompt") || "hi"
        } satisfies ProviderHealthProbeInput
      });
      const passed = result.results.filter((item) => item.ok).length;
      return {
        notice: `${passed}/${result.results.length} 个 Key 测活通过`,
        payload: { healthProbe: result }
      };
    }
    if (action === "probe-provider-balance") {
      const providerId = text("providerId");
      if (!providerId) throw new Error("未找到待探测余额的上游");
      const result = await command<ProviderBalanceProbeResult>("probe_provider_balance", {
        providerId,
        provider_id: providerId
      });
      setProviderBalanceStatus((current) => ({ ...current, [providerId]: result }));
      return {
        notice: result.ok
          ? `余额：${formatBalanceNotice(result.balance) || "已获取"}`
          : result.supported
            ? `余额探测未通过：${result.message}`
            : "该上游未识别到通用余额 API",
        payload: { balanceProbe: result }
      };
    }
    if (action === "test-provider") {
      const draft = providerPayload();
      const providerId = text("providerId") || draft.id || "";
      // Provider-list actions contain only providerId. Editor actions include
      // the unsaved provider payload and therefore must retain their dual-path
      // connection recommendation behavior.
      if (providerId && !("provider" in payload)) {
        return testSavedProviderKeyHealth(providerId);
      }
      return testDraftProviderConnection(draft, providerId);
    }
    if (action === "test-provider-key") {
      const draft = providerPayload();
      const keyId = text("keyId");
      const draftKey = draft.keys.find((item) => item.id === keyId);
      const draftSecret = cleanOptionalText(draftKey?.key);
      const provider = draft.id
        ? config?.providers.find((item) => item.id === draft.id)
        : undefined;
      if (!keyId) throw new Error("未找到待测试的上游 Key");
      if (!provider && !draftSecret) throw new Error("请先填写待测试的 Key");

      const result = await command<ProviderKeyTestResult>("test_provider_key", {
        input: draftProviderKeyTestInput(draft, keyId, draftSecret)
      });
      if (!provider) {
        return result.ok ? { notice: result.message } : { error: result.message };
      }
      const nextConfig = await command<AppConfig>("get_config");
      setConfig(nextConfig);
      const keyPoolHealth = graphiteKeyPoolHealth(nextConfig, provider.id);
      return result.ok
        ? { notice: result.message, payload: { keyPoolHealth } }
        : { error: result.message, payload: { keyPoolHealth } };
    }
    if (action === "test-provider-key-pool") {
      const providerId = text("providerId");
      if (!providerId) throw new Error("请先保存上游，再测试 Key 池");
      const results = await command<ProviderKeyTestResult[]>("test_provider_key_pool", {
        providerId,
        provider_id: providerId
      });
      const nextConfig = await command<AppConfig>("get_config");
      setConfig(nextConfig);
      const keyPoolHealth = graphiteKeyPoolHealth(nextConfig, providerId);
      const passed = results.filter((result) => result.ok).length;
      return passed === results.length
        ? { notice: `${passed} 个 Key 测试通过`, payload: { keyPoolHealth } }
        : { error: `${passed}/${results.length} 个 Key 测试通过；请在列表中查看状态`, payload: { keyPoolHealth } };
    }
    if (action === "sync-provider-key-pool-health") {
      const providerId = text("providerId");
      if (!providerId) return;
      const snapshot = await command<ProviderKeyPoolHealthSnapshot>("get_provider_key_pool_health", {
        providerId,
        provider_id: providerId
      });
      return {
        payload: {
          keyPoolHealth: {
            providerId: snapshot.provider_id,
            keys: snapshot.keys
          }
        }
      };
    }
    if (action === "diagnose-network-paths") {
      const providerId = text("providerId");
      if (!providerId) throw new Error("请先保存上游，再比较网络路径");
      const result = await command<ProviderNetworkPathDiagnosticResult>("diagnose_provider_network_paths", {
        providerId,
        provider_id: providerId
      });
      setNetworkPathDiagnostic(result);
      const summary = result.paths.map((path) => `${path.path} ${path.ok ? `${path.elapsed_ms}ms` : "失败"}`).join(" · ");
      return { notice: `路径诊断完成：${summary}`, payload: { networkDiagnostic: result } };
    }
    if (action === "reveal-provider-api-key" || action === "reveal-provider-key") {
      const providerId = text("providerId");
      const targetId = text("targetId");
      if (!providerId || !targetId) throw new Error("未找到待显示的 Key");
      const secret = action === "reveal-provider-key"
        ? await command<string | null>("reveal_provider_key", {
            providerId,
            provider_id: providerId,
            keyId: text("keyId"),
            key_id: text("keyId")
          })
        : await command<string | null>("reveal_provider_api_key", { providerId, provider_id: providerId });
      return secret ? { payload: { secret, targetId } } : { error: "未找到已保存的 Key" };
    }
    if (action === "reveal-local-key") {
      const targetId = text("targetId");
      return config?.local_key
        ? { payload: { secret: config.local_key, targetId } }
        : { error: "本地 Key 尚未设置" };
    }
    if (action === "probe-cache-capabilities") {
      const providerId = text("providerId");
      const modelId = text("modelId");
      if (!providerId) throw new Error("请先保存上游，再验证缓存控制字段");
      if (!modelId) throw new Error("请选择或填写实际上游模型 ID，再验证缓存控制字段");
      const result = await command<ProviderCacheCapabilityProbeResult>(
        "probe_provider_cache_capabilities",
        { input: { provider_id: providerId, model_id: modelId, channel: null } }
      );
      const nextConfig = await command<AppConfig>("get_config");
      setConfig(nextConfig);
      const accepted = result.fields.filter((field) => field.status === "verified").length;
      return {
        notice: `${modelId} 已完成缓存字段探测：${accepted}/${result.fields.length} 可用于手动验证`,
        payload: { compatibility: nextConfig.providers.find((provider) => provider.id === providerId) }
      };
    }
    if (action === "set-cache-validation") {
      const mode = String(payload.mode ?? "auto") as CacheValidationMode;
      if (mode !== "auto" && mode !== "baseline" && mode !== "candidate") {
        throw new Error("缓存验证模式无效");
      }
      const providerId = text("providerId");
      const modelId = text("modelId");
      if (mode !== "auto" && !providerId) throw new Error("请先保存上游，再开始缓存验证");
      if (mode !== "auto" && !modelId) throw new Error("请选择或填写实际上游模型 ID，再开始缓存验证");
      const status = await command<CacheValidationStatus>("set_cache_validation_mode", {
        input: {
          mode,
          provider_id: mode === "auto" ? null : providerId,
          model: mode === "auto" ? null : modelId
        }
      });
      setCacheValidation(status);
      if (mode === "auto") return { notice: "缓存验证已停止；正常转发不受影响" };
      return {
        notice: mode === "baseline" ? "缓存基线已开始，后续正常请求会被被动统计" : "缓存候选已开始，仅测试一个已探测字段",
        payload: { cacheValidation: status }
      };
    }
    if (action === "save-proxy-mode") {
      const host = text("host");
      const port = Number(payload.port);
      if (!host || !Number.isInteger(port) || port <= 0 || port > 65535) {
        throw new Error("Proxy Mode 地址或端口不合法");
      }
      if (config && proxyAddressConflicts(host, port, config.host, config.port)) {
        throw new Error("Proxy Mode 地址不能和主代理监听地址相同");
      }
      setConfig(await command<AppConfig>("save_proxy_mode_config", { input: { host, port } }));
      return { notice: "Proxy Mode 地址已保存" };
    }
    if (action === "save-cache-enabled") {
      if (!config) throw new Error("缓存配置尚未加载完成");
      setConfig(await command<AppConfig>("save_cache_policy", {
        input: { ...config.cache, enabled: payload.enabled === true }
      }));
      return { notice: payload.enabled === true ? "智能缓存已开启" : "智能缓存已关闭" };
    }
    if (action === "set-include-special-requests") {
      const enabled = payload.enabled === true;
      setIncludeColdStarts(enabled);
      setIncludeCompactions(enabled);
      return { notice: enabled ? "统计已计入冷启动和压缩" : "统计已排除冷启动和压缩" };
    }
    if (action === "set-include-cold-starts") {
      const enabled = payload.enabled === true;
      setIncludeColdStarts(enabled);
      return { notice: enabled ? "统计已计入冷启动" : "统计已排除冷启动" };
    }
    if (action === "set-include-compactions") {
      const enabled = payload.enabled === true;
      setIncludeCompactions(enabled);
      return { notice: enabled ? "统计已计入压缩" : "统计已排除压缩" };
    }
    if (action === "set-show-detailed-errors") {
      const enabled = payload.enabled === true;
      setShowDetailedErrors(enabled);
      return { notice: enabled ? "错误记录将显示 HTTP 状态" : "错误记录仅显示 error" };
    }
    if (action === "save-settings") {
      const settings = (payload.settings ?? {}) as Record<string, unknown>;
      if (!config) throw new Error("配置尚未加载完成");
      const host = String(settings.host ?? "").trim();
      const port = Number(settings.port);
      const defaultChannel = String(settings.default_channel ?? config.default_channel) as Channel;
      if (!host || !Number.isInteger(port) || port <= 0 || port > 65535) {
        throw new Error("监听地址或端口不合法");
      }
      if (!isChannel(defaultChannel)) throw new Error("默认通道不合法");
      const localKey = String(settings.local_key ?? "").trim() || config.local_key;
      const upstreamProxyUrl = String(settings.upstream_proxy_url ?? "").trim();
      const refreshPolicy = String(settings.refresh_policy ?? metricsRefreshPolicy) as MetricsRefreshPolicy;
      if (!isMetricsRefreshPolicy(refreshPolicy)) throw new Error("统计刷新策略不合法");
      const nextConfig = await command<AppConfig>("save_config", {
        input: {
          host,
          port,
          proxy_auto_start: settings.proxy_auto_start === true,
          upstream_proxy_url: upstreamProxyUrl,
          local_key: localKey,
          default_channel: defaultChannel,
          workspace_fingerprint: config.workspace_fingerprint,
          cache: config.cache
        }
      });
      setConfig(nextConfig);
      setMetricsRefreshPolicy(refreshPolicy);
      const addressChanged = host !== config.host || port !== config.port;
      return {
        notice: addressChanged ? "设置已保存；监听地址变更将在手动重启代理后生效" : "设置已保存",
        closeOverlay: "settingsOverlay"
      };
    }
    if (action === "restart-main-proxy") {
      await command("stop_proxy");
      await command("start_proxy");
      setProxyStatus(await command<ProxyStatus>("get_proxy_status"));
      return { notice: "本地代理已重启" };
    }
    if (action === "clear-cache") {
      await command("clear_cache");
      await refreshMetrics(true);
      return { notice: "缓存已清理" };
    }
    return { error: `暂未识别的 Graphite 操作：${action}` };
  }

  return {
    config,
    metrics,
    selectedAgentId,
    includeColdStarts,
    includeCompactions,
    showDetailedErrors,
    providerConnectionStatus,
    providerBalanceStatus,
    metricsRefreshPolicy,
    proxyStatus,
    networkPathDiagnostic,
    cacheValidation,
    appVersion: APP_VERSION,
    notice,
    error,
    onBridgeAction
  };
}

function visibleAgentInjections(items: AgentInjectionConfig[]): AgentInjectionConfig[] {
  return items;
}

function proxyAddressConflicts(leftHost: string, leftPort: number, rightHost: string, rightPort: number): boolean {
  if (!Number.isInteger(leftPort) || leftPort !== rightPort) return false;
  const normalize = (host: string) => {
    const value = host.trim().toLowerCase();
    return value === "localhost" ? "127.0.0.1" : value;
  };
  const left = normalize(leftHost);
  const right = normalize(rightHost);
  return left === right || left === "0.0.0.0" || right === "0.0.0.0";
}

function isPersistedModelReasoningFallbackRequest(request: RequestLogEntry): boolean {
  const source = request.reasoning_effort_source?.trim() ?? "";
  return (
    (source === "model_override_fallback" || source === "model_override_opaque_502_fallback") &&
    Boolean(request.configured_reasoning_effort?.trim()) &&
    Boolean(request.effective_reasoning_effort?.trim())
  );
}

function cleanOptionalText(value?: string | null): string | null {
  const text = (value ?? "").trim();
  return text || null;
}

function graphiteKeyPoolHealth(config: AppConfig, providerId: string) {
  const keys = config.providers.find((provider) => provider.id === providerId)?.key_pool?.keys ?? [];
  return {
    providerId,
    keys: keys.map((key) => ({
      id: key.id,
      enabled: key.enabled,
      status: key.status
    }))
  };
}

function normalizeGraphiteModels(models: GraphiteProviderPayload["models"]) {
  const byId = new Map<string, GraphiteProviderPayload["models"][number]>();
  for (const source of models) {
    const id = source.id.trim();
    if (!id) continue;
    const requestModelId = cleanOptionalText(source.request_model_id);
    byId.set(id, {
      ...source,
      id,
      request_model_id: requestModelId === id ? null : requestModelId,
      context_window: source.context_window ?? null,
      reasoning_effort: cleanOptionalText(source.reasoning_effort)
    });
  }
  return [...byId.values()];
}

function isChannel(value: string): value is Channel {
  return value === "chat" || value === "responses" || value === "anthropic";
}

function isMetricsRefreshPolicy(value: string): value is MetricsRefreshPolicy {
  return value === "visible-1s" || value === "5s" || value === "manual";
}

function draftProviderTestInput(
  provider: GraphiteProviderPayload,
  key: string | null | undefined,
  isFullUrl = false
) {
  return {
    provider_id: provider.id ?? null,
    key_id: null,
    api_key: key ?? cleanOptionalText(provider.api_key),
    base_url: provider.base_url ?? "",
    models_url: cleanOptionalText(provider.models_url),
    is_full_url: isFullUrl,
    custom_user_agent: cleanOptionalText(provider.custom_user_agent),
    channel: provider.channel || "responses",
    use_system_proxy: provider.use_system_proxy ?? true
  };
}

function draftProviderKeyTestInput(
  provider: GraphiteProviderPayload,
  keyId: string,
  key: string | null
) {
  return {
    ...draftProviderTestInput(provider, key),
    key_id: provider.id ? keyId : null
  };
}
