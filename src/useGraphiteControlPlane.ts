import { useEffect, useRef, useState } from "react";
import {
  command,
  model,
  type AgentInjectionConfig,
  type AgentInjectionResult,
  type AppConfig,
  type Channel,
  type FetchModelsInput,
  type MetricsSnapshot,
  type ModelConfig,
  type ProviderConfig,
  type ProviderInput,
  type ProviderKeyTestResult,
  type ProviderNetworkPathDiagnosticResult,
  type ProviderResponseSessionReuseProbeResult,
  type ProxyStatus
} from "./lib/api";
import type {
  GraphiteBridgeResponse,
  GraphitePrototypeHostProps,
  GraphiteProviderPayload
} from "./GraphitePrototypeHost";
import { providerBelongsToAgent } from "./graphite/providerScope";

const APP_VERSION = "v1.3.5";
type MetricsRefreshPolicy = "visible-1s" | "5s" | "manual";
type RequestLogEntry = MetricsSnapshot["recent_requests"][number];

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
  const [showDetailedErrors, setShowDetailedErrors] = useState(false);
  const [providerConnectionStatus, setProviderConnectionStatus] = useState<Record<string, string>>({});
  const [metricsRefreshPolicy, setMetricsRefreshPolicy] = useState<MetricsRefreshPolicy>("visible-1s");
  const [proxyStatus, setProxyStatus] = useState<ProxyStatus | null>(null);
  const [networkPathDiagnostic, setNetworkPathDiagnostic] =
    useState<ProviderNetworkPathDiagnosticResult | null>(null);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const reasoningFallbackSyncing = useRef(false);
  const seenReasoningFallbackFailures = useRef(new Set<string>());

  async function refreshAll() {
    setError("");
    try {
      const [nextConfig, nextMetrics, nextProxyStatus] = await Promise.all([
        command<AppConfig>("reload_config"),
        command<MetricsSnapshot>("get_metrics"),
        command<ProxyStatus>("get_proxy_status")
      ]);
      setConfig(nextConfig);
      setMetrics(nextMetrics);
      setProxyStatus(nextProxyStatus);
      const agents = visibleAgentInjections(nextConfig.agent_injections);
      setSelectedAgentId((current) => {
        if (current && agents.some((agent) => agent.id === current)) return current;
        return (agents.find((agent) => agent.enabled) ?? agents[0])?.id ?? "";
      });
    } catch (cause) {
      setError(String(cause));
    }
  }

  async function refreshMetrics() {
    try {
      setMetrics(await command<MetricsSnapshot>("get_metrics"));
    } catch {
      // Keep the last verified snapshot visible when a transient refresh fails.
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

  async function saveProviderFromGraphite(agentId: string, payload: GraphiteProviderPayload) {
    const name = payload.name.trim();
    const baseUrl = payload.base_url.trim();
    if (!name || !baseUrl) throw new Error("上游名称和 Base URL 不能为空");

    setError("");
    setNotice("");
    let editablePayload = payload;
    let existing = config?.providers.find((provider) => provider.id === editablePayload.id) ?? null;
    if (agentId && existing && !providerBelongsToAgent(existing.id, agentId)) {
      if (existing.id.startsWith("agent-")) {
        throw new Error("不能编辑其他 Agent 的独立上游");
      }
      const clonedConfig = await command<AppConfig>("clone_provider_for_agent", {
        input: { agent_id: agentId, provider_id: existing.id, model_id: null }
      });
      const clonedProviderId = clonedConfig.agent_injections.find((agent) => agent.id === agentId)?.provider_id;
      if (!clonedProviderId) throw new Error("无法创建当前 Agent 的独立上游");
      editablePayload = { ...payload, id: clonedProviderId };
      existing = clonedConfig.providers.find((provider) => provider.id === clonedProviderId) ?? null;
    }

    const existingKeys = new Map((existing?.key_pool?.keys ?? []).map((key) => [key.id, key]));
    const input: ProviderInput = {
      id: editablePayload.id ?? undefined,
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
      key_pool: {
        enabled: editablePayload.key_pool?.enabled ?? existing?.key_pool?.enabled ?? editablePayload.keys.length > 0,
        strategy: editablePayload.key_pool?.strategy ?? existing?.key_pool?.strategy ?? "round-robin",
        failure_threshold: editablePayload.key_pool?.failure_threshold ?? existing?.key_pool?.failure_threshold ?? 3,
        recovery_minutes: editablePayload.key_pool?.recovery_minutes ?? existing?.key_pool?.recovery_minutes ?? 10,
        keys: editablePayload.keys.map((key) => {
          const prior = key.id ? existingKeys.get(key.id) : undefined;
          return {
            id: key.id,
            alias: cleanOptionalText(key.alias) ?? prior?.alias ?? null,
            key: cleanOptionalText(key.key) ?? null,
            enabled: prior?.enabled ?? true,
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
      if (agentId) {
        const boundProviderId = nextConfig.agent_injections.find((agent) => agent.id === agentId)?.provider_id;
        if (boundProviderId !== savedProvider.id) {
          await command<AgentInjectionResult[]>("update_agent_injection_route", {
            input: { id: agentId, provider_id: savedProvider.id }
          });
          nextConfig = await command<AppConfig>("get_config");
        }
        const boundProvider = nextConfig.providers.find(
          (provider) => provider.id === nextConfig.agent_injections.find((agent) => agent.id === agentId)?.provider_id
        );
        if (boundProvider) savedProvider = boundProvider;
      }
      setConfig(nextConfig);
      setNotice("上游配置已保存并绑定到当前 Agent");
    } catch (cause) {
      const message = String(cause);
      setError(message);
      throw cause;
    }
  }

  async function deleteProviderFromGraphite(agentId: string, providerId: string) {
    if (!providerId) return;
    setError("");
    try {
      const nextConfig = await command<AppConfig>("delete_provider", {
        providerId,
        provider_id: providerId,
        agentId: agentId || null,
        agent_id: agentId || null
      });
      setConfig(nextConfig);
      setNotice("上游已删除");
    } catch (cause) {
      const message = String(cause);
      setError(message);
      throw cause;
    }
  }

  async function toggleAgentInjection(agent: AgentInjectionConfig): Promise<string> {
    if (!agent.enabled && !agent.provider_id) {
      const defaultProvider =
        config?.providers.find((provider) => provider.id === config.active_provider_id) ??
        config?.providers[0];
      if (!defaultProvider) {
        setSelectedAgentId(agent.id);
        throw new Error("请先添加一个上游，然后再开启这个 Agent。");
      }
      return activateAgentProvider(agent, defaultProvider, true);
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
        input: { id: agent.id, provider_id: provider.id }
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
    if (action === "save-provider") {
      await saveProviderFromGraphite(
        String(payload.agentId ?? ""),
        payload as unknown as GraphiteProviderPayload
      );
      return { notice: "上游已保存并绑定", closeOverlay: "providerOverlay" };
    }
    if (action === "delete-provider") {
      await deleteProviderFromGraphite(text("agentId"), text("providerId"));
      return { notice: "上游已删除", closeOverlay: "confirmOverlay" };
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
        notice: models.length ? `已获取 ${models.length} 个模型，可在实际模型框或映射行选择，也可继续手动输入` : "未返回模型；仍可手动填写实际模型 ID",
        payload: { models: models.map((item) => ({ id: item.id })) }
      };
    }
    if (action === "test-provider") {
      const provider = config?.providers.find((item) => item.id === text("providerId"));
      if (!provider) throw new Error("请先保存上游，再测试连通性");
      const startedAt = performance.now();
      const result = await command<ProviderKeyTestResult>("test_provider_key", {
        input: providerTestInput(provider, null)
      });
      const elapsedMs = Math.max(0, Math.round(performance.now() - startedAt));
      setProviderConnectionStatus((current) => ({
        ...current,
        [provider.id]: result.ok ? `刚刚成功 · ${elapsedMs}ms` : `失败 · ${elapsedMs}ms`
      }));
      await refreshAll();
      return result.ok
        ? { notice: `${provider.name} 连通正常${result.models_count ? ` · ${result.models_count} 个模型` : ""}` }
        : { error: result.message };
    }
    if (action === "test-provider-key") {
      const provider = config?.providers.find((item) => item.id === text("providerId"));
      const keyId = text("keyId");
      if (!provider || !keyId) throw new Error("未找到待测试的上游 Key");
      const result = await command<ProviderKeyTestResult>("test_provider_key", {
        input: providerTestInput(provider, keyId)
      });
      setConfig(await command<AppConfig>("get_config"));
      return result.ok ? { notice: result.message } : { error: result.message };
    }
    if (action === "test-provider-key-pool") {
      const providerId = text("providerId");
      if (!providerId) throw new Error("请先保存上游，再测试 Key 池");
      const results = await command<ProviderKeyTestResult[]>("test_provider_key_pool", {
        providerId,
        provider_id: providerId
      });
      setConfig(await command<AppConfig>("get_config"));
      const passed = results.filter((result) => result.ok).length;
      return passed === results.length
        ? { notice: `${passed} 个 Key 测试通过` }
        : { error: `${passed}/${results.length} 个 Key 测试通过；请在列表中查看状态` };
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
    if (action === "probe-session-reuse") {
      const providerId = text("providerId");
      const modelId = text("modelId");
      if (!providerId) throw new Error("请先保存上游，再验证会话复用");
      if (!modelId) throw new Error("请选择或填写一个实际模型 ID，再验证会话复用");
      const result = await command<ProviderResponseSessionReuseProbeResult>(
        "probe_provider_response_session_reuse",
        { input: { provider_id: providerId, model_id: modelId } }
      );
      const nextConfig = await command<AppConfig>("get_config");
      setConfig(nextConfig);
      const compatibility = nextConfig.providers.find((provider) => provider.id === providerId);
      return result.status === "verified" && result.enabled
        ? { notice: `${modelId} 已验证并启用会话增量复用`, payload: { compatibility } }
        : { error: `${modelId} 未启用会话复用：${result.message}`, payload: { compatibility } };
    }
    if (action === "set-session-reuse") {
      const providerId = text("providerId");
      const modelId = text("modelId");
      if (!providerId) throw new Error("未找到会话复用对应的上游");
      if (!modelId) throw new Error("请选择或填写会话复用对应的实际模型 ID");
      const enabled = payload.enabled === true;
      const nextConfig = await command<AppConfig>("set_provider_response_session_reuse_enabled", {
        providerId,
        provider_id: providerId,
        modelId,
        model_id: modelId,
        enabled
      });
      setConfig(nextConfig);
      return {
        notice: enabled ? `${modelId} 会话复用已开启` : `${modelId} 会话复用已关闭`,
        payload: { compatibility: nextConfig.providers.find((provider) => provider.id === providerId) }
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
    if (action === "set-include-cold-starts") {
      const enabled = payload.enabled === true;
      setIncludeColdStarts(enabled);
      return { notice: enabled ? "统计已计入冷启动" : "统计已排除冷启动" };
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
      await refreshMetrics();
      return { notice: "缓存已清理" };
    }
    return { error: `暂未识别的 Graphite 操作：${action}` };
  }

  return {
    config,
    metrics,
    selectedAgentId,
    includeColdStarts,
    showDetailedErrors,
    providerConnectionStatus,
    metricsRefreshPolicy,
    proxyStatus,
    networkPathDiagnostic,
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

function providerTestInput(provider: ProviderConfig, keyId: string | null) {
  return {
    provider_id: provider.id,
    key_id: keyId,
    api_key: null,
    base_url: provider.base_url,
    models_url: provider.models_url ?? null,
    is_full_url: provider.is_full_url,
    custom_user_agent: provider.custom_user_agent ?? null,
    channel: provider.channel,
    use_system_proxy: provider.use_system_proxy
  };
}
