import {
  Activity,
  AlertTriangle,
  ArrowDown,
  ArrowUp,
  Bot,
  BrainCircuit,
  Check,
  Code2,
  ChevronDown,
  Copy,
  DatabaseZap,
  Eye,
  EyeOff,
  Gauge,
  KeyRound,
  Link2,
  Loader2,
  Network,
  Play,
  Plus,
  RefreshCw,
  Save,
  Settings2,
  ShieldCheck,
  Sparkles,
  TerminalSquare,
  Trash2,
  Workflow,
  X,
  Zap
} from "lucide-react";
import type { Dispatch, ReactNode, SetStateAction } from "react";
import { useEffect, useMemo, useState } from "react";
import {
  AgentInjectionConfig,
  AgentInjectionResult,
  AppConfig,
  Channel,
  command,
  FetchModelsInput,
  KeyLoadBalanceStrategy,
  MetricsSnapshot,
  model,
  ModelConfig,
  ProviderChannelMode,
  ProviderConfig,
  ProviderInput,
  ProviderKeyStatus,
  ProviderKeyTestResult,
  ProxyModeConfigInput
} from "./lib/api";

type ViewId = "agent" | "cache";
type RequestLogEntry = MetricsSnapshot["recent_requests"][number];
type RequestFeedEntry =
  | ({ feed_kind: "request" } & RequestLogEntry)
  | ({ feed_kind: "failed" } & RequestLogEntry);

interface ProviderDraft {
  id?: string;
  name: string;
  base_url: string;
  models_url: string;
  is_full_url: boolean;
  custom_user_agent: string;
  api_key: string;
  channel_mode: ProviderChannelMode;
  channel: Channel;
  prompt_cache_retention_enabled: boolean;
  request_body_gzip_enabled: boolean;
  use_system_proxy: boolean;
  non_sse_compact_compat_enabled: boolean;
  key_pool: ProviderKeyPoolDraft;
  models: ModelConfig[];
  enabled: boolean;
}

interface ProviderKeyPoolDraft {
  enabled: boolean;
  strategy: KeyLoadBalanceStrategy;
  failure_threshold: number;
  recovery_minutes: number;
  keys: ProviderKeyDraft[];
}

interface ProviderKeyDraft {
  id: string;
  alias: string;
  key: string;
  preview: string;
  enabled: boolean;
  priority: number;
  status: ProviderKeyStatus;
  total_requests: number;
  successes: number;
  failures: number;
  last_checked_at?: string | null;
  last_error?: string | null;
  disabled_until?: string | null;
}

interface ProxyModeDraft {
  host: string;
  port: string;
}
const channelOptions: Array<{ value: Channel; label: string; endpoint: string }> = [
  { value: "anthropic", label: "Anthropic", endpoint: "/v1/messages" },
  { value: "chat", label: "OpenAI Chat", endpoint: "/v1/chat/completions" },
  { value: "responses", label: "OpenAI Responses", endpoint: "/v1/responses" }
];

const upstreamModeOptions: Array<{ value: "auto" | Channel; label: string; detail: string }> = [
  { value: "auto", label: "自动识别（推荐）", detail: "按客户端请求和压缩场景自动选择最合适的上游格式" },
  { value: "responses", label: "手动：Responses", detail: "/v1/responses" },
  { value: "chat", label: "手动：Chat", detail: "/v1/chat/completions" },
  { value: "anthropic", label: "手动：Anthropic", detail: "/v1/messages" }
];

const keyStrategyOptions: Array<{ value: KeyLoadBalanceStrategy; label: string; hint: string }> = [
  { value: "round-robin", label: "轮询", hint: "每次请求按顺序切到下一个可用 Key，适合额度接近的多个 Key。" },
  { value: "priority", label: "优先级", hint: "优先使用优先级更高的 Key，失败或不可用时再切换。" },
  { value: "least-used", label: "最少使用", hint: "优先使用历史请求数最少的 Key，尽量摊平消耗。" },
  { value: "random", label: "随机", hint: "在可用 Key 中随机选择，适合临时混合池。" },
  { value: "sequential", label: "顺序消耗", hint: "一直使用列表里第一个可用 Key，异常后顺序切到下一个。" }
];

const reasoningEffortOptions = [
  "none",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
  "ultra"
] as const;

const utilityViews: Array<{ id: ViewId; label: string; icon: ReactNode }> = [
  { id: "cache", label: "缓存统计", icon: <Gauge size={16} /> }
];

const requestPageSize = 20;
const maxRequestPages = 10;
const appVersion = "v0.1.92";
const appVersionNotes = [
  "v0.1.92: Responses 核心重构，使用量观测与会话身份归并为单一可信路径。",
  "v0.1.92: 仅在精确可避免证据下等待，最长 1 秒；流生命周期和全阶段尾巴分类已覆盖。"
];

const emptyDraft: ProviderDraft = {
  name: "",
  base_url: "",
  models_url: "",
  is_full_url: false,
  custom_user_agent: "",
  api_key: "",
  channel_mode: "auto",
  channel: "responses",
  prompt_cache_retention_enabled: true,
  request_body_gzip_enabled: false,
  use_system_proxy: false,
  non_sse_compact_compat_enabled: false,
  key_pool: {
    enabled: false,
    strategy: "round-robin",
    failure_threshold: 3,
    recovery_minutes: 5,
    keys: []
  },
  models: [],
  enabled: true
};

export default function App() {
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [activeView, setActiveView] = useState<ViewId>("agent");
  const [selectedAgentId, setSelectedAgentId] = useState("");
  const [selectedProviderId, setSelectedProviderId] = useState("new");
  const [draft, setDraft] = useState<ProviderDraft>(emptyDraft);
  const [providerEditorOpen, setProviderEditorOpen] = useState(false);
  const [modelCandidates, setModelCandidates] = useState<ModelConfig[]>([]);
  const [selectedFetchedModelId, setSelectedFetchedModelId] = useState("");
  const [apiKeyVisible, setApiKeyVisible] = useState(false);
  const [loadingModels, setLoadingModels] = useState(false);
  const [savingProvider, setSavingProvider] = useState(false);
  const [savingCachePolicy, setSavingCachePolicy] = useState(false);
  const [savingProxyModeConfig, setSavingProxyModeConfig] = useState(false);
  const [proxyModeDraft, setProxyModeDraft] = useState<ProxyModeDraft>({ host: "127.0.0.1", port: "18884" });
  const [injectingId, setInjectingId] = useState("");
  const [cacheProviderFilter, setCacheProviderFilter] = useState("all");
  const [includeColdStarts, setIncludeColdStarts] = useState(true);
  const [versionOpen, setVersionOpen] = useState(false);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [dismissedRetentionWarningKey, setDismissedRetentionWarningKey] = useState("");

  useEffect(() => {
    void refreshAll();
    const timer = window.setInterval(() => {
      void refreshMetrics();
    }, 2500);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!versionOpen) return;
    const closeVersionPopover = (event: MouseEvent) => {
      const target = event.target as HTMLElement | null;
      if (target?.closest(".version-wrap")) return;
      setVersionOpen(false);
    };
    window.addEventListener("mousedown", closeVersionPopover);
    return () => window.removeEventListener("mousedown", closeVersionPopover);
  }, [versionOpen]);

  useEffect(() => {
    if (!config) return;
    if (selectedProviderId === "new") {
      setDraft((current) => (current.id ? emptyDraft : current));
      return;
    }
    const provider = config.providers.find((item) => item.id === selectedProviderId);
    if (!provider) return;
    setDraft((current) => (current.id === provider.id ? current : providerToDraft(provider)));
  }, [config, selectedProviderId]);

  useEffect(() => {
    if (!config) return;
    setProxyModeDraft({
      host: config.proxy_mode_host,
      port: String(config.proxy_mode_port)
    });
  }, [config?.proxy_mode_host, config?.proxy_mode_port]);
  useEffect(() => {
    const items = visibleAgentInjections(config?.agent_injections ?? []);
    if (!items.length) return;
    if (selectedAgentId && !items.some((item) => item.id === selectedAgentId)) {
      setSelectedAgentId("");
    }
  }, [config, selectedAgentId]);

  const baseUrl = useMemo(() => {
    if (!config) return "http://127.0.0.1:18883";
    return `http://${config.host}:${config.port}`;
  }, [config]);

  const activeProvider = useMemo(
    () => {
      const selectedAgent = visibleAgentInjections(config?.agent_injections ?? []).find((item) => item.id === selectedAgentId);
      const agentProvider = selectedAgent?.provider_id
        ? config?.providers.find((item) => item.id === selectedAgent.provider_id) ?? null
        : null;
      if (activeView === "agent") return agentProvider;
      return config?.providers.find((item) => item.id === config.active_provider_id) ?? null;
    },
    [activeView, config, selectedAgentId]
  );

  async function refreshAll() {
    setError("");
    const [nextConfig, nextMetrics] = await Promise.all([
      command<AppConfig>("reload_config"),
      command<MetricsSnapshot>("get_metrics")
    ]);
    setConfig(nextConfig);
    setMetrics(nextMetrics);
    if (selectedProviderId === "new" && nextConfig.providers.length > 0 && !draftHasInput(draft)) {
      setSelectedProviderId(nextConfig.active_provider_id ?? nextConfig.providers[0].id);
    }
    if (!selectedAgentId) {
      const preferredAgent =
        visibleAgentInjections(nextConfig.agent_injections).find((item) => item.enabled) ??
        visibleAgentInjections(nextConfig.agent_injections)[0];
      if (preferredAgent) {
        setSelectedAgentId(preferredAgent.id);
        setActiveView("agent");
      }
    }
  }

  async function refreshMetrics() {
    try {
      setMetrics(await command<MetricsSnapshot>("get_metrics"));
    } catch {
      // Keep the last known state in the UI.
    }
  }

  async function saveProxyModeConfig() {
    if (!config) return;
    const host = proxyModeDraft.host.trim();
    const port = Number(proxyModeDraft.port);
    if (!host || !Number.isInteger(port) || port <= 0 || port > 65535) {
      setError("本地代理模式 IP 或端口不合法");
      return;
    }
    if (proxyAddressConflicts(host, port, config.host, config.port)) {
      setError("本地代理模式地址不能和其他 Agent 注入地址相同");
      return;
    }
    setSavingProxyModeConfig(true);
    setError("");
    setNotice("");
    try {
      const input: ProxyModeConfigInput = { host, port };
      const nextConfig = await command<AppConfig>("save_proxy_mode_config", { input });
      setConfig(nextConfig);
      setNotice("本地代理模式地址已保存");
    } catch (err) {
      setError(String(err));
    } finally {
      setSavingProxyModeConfig(false);
    }
  }
  function resetModelPicker() {
    setModelCandidates([]);
    setSelectedFetchedModelId("");
  }

  function createProvider() {
    setSelectedProviderId("new");
    setDraft(emptyDraft);
    setApiKeyVisible(false);
    resetModelPicker();
    setProviderEditorOpen(true);
    setNotice("");
    setError("");
  }

  function editProvider(provider: ProviderConfig) {
    setSelectedProviderId(provider.id);
    setDraft(providerToDraft(provider));
    setApiKeyVisible(false);
    resetModelPicker();
    setProviderEditorOpen(true);
    setNotice("");
    setError("");
  }

  async function editAgentProvider(item: AgentInjectionConfig, provider: ProviderConfig) {
    if (!config || providerBelongsToAgent(provider.id, item.id)) {
      editProvider(provider);
      return;
    }
    setInjectingId(`${item.id}:route`);
    setError("");
    setNotice("");
    try {
      const nextConfig = await command<AppConfig>("clone_provider_for_agent", {
        input: {
          agent_id: item.id,
          provider_id: provider.id,
          model_id: item.model_id ?? null
        }
      });
      setConfig(nextConfig);
      const updatedAgent = nextConfig.agent_injections.find((candidate) => candidate.id === item.id);
      const clonedProvider = nextConfig.providers.find(
        (candidate) => candidate.id === updatedAgent?.provider_id
      );
      editProvider(clonedProvider ?? provider);
      setNotice(`${item.label} 已切换为独立设置，当前编辑不会影响其他 Agent`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function selectProvider(provider: ProviderConfig) {
    setSelectedProviderId(provider.id);
    setDraft(providerToDraft(provider));
    setApiKeyVisible(false);
    resetModelPicker();
    setError("");
    try {
      const nextConfig = await command<AppConfig>("select_provider", {
        providerId: provider.id,
        provider_id: provider.id
      });
      setConfig(nextConfig);
      setNotice(`已选择 ${provider.name}`);
    } catch (err) {
      setError(String(err));
    }
  }

  async function fetchModels() {
    if (!draft.base_url.trim()) {
      setError("请先填写 Base URL");
      return;
    }
    setLoadingModels(true);
    setError("");
    setNotice("");
    try {
      const input: FetchModelsInput = {
        provider_id: draft.id,
        name: draft.name,
        base_url: draft.base_url.trim(),
        models_url: draft.models_url.trim() || undefined,
        is_full_url: draft.is_full_url,
        custom_user_agent: draft.custom_user_agent.trim() || undefined,
        channel: draft.channel,
        api_key: draft.api_key || undefined,
        use_system_proxy: draft.use_system_proxy
      };
      const models = await command<ModelConfig[]>("fetch_provider_models", { input });
      setModelCandidates(models);
      setSelectedFetchedModelId(models[0]?.id ?? "");
      setNotice(models.length ? `已获取 ${models.length} 个模型，可作为映射项加入` : "没有获取到模型；不添加映射也可以直接代理转发");
    } catch (err) {
      setError(String(err));
    } finally {
      setLoadingModels(false);
    }
  }

  function addFetchedModel() {
    const selected = modelCandidates.find((item) => item.id === selectedFetchedModelId);
    if (!selected) return;
    setDraft((current) => {
      const exists = current.models.some((item) => item.id === selected.id);
      return {
        ...current,
        models: exists
          ? current.models.map((item) => (item.id === selected.id ? selected : item))
          : [...current.models, selected]
      };
    });
    setNotice(`已加入模型 ${selected.id}`);
  }

  function addManualModel() {
    setDraft((current) => ({
      ...current,
      models: [...current.models, model(nextManualModelId(current.models))]
    }));
  }

  function updateModel(index: number, patch: Partial<ModelConfig>) {
    setDraft((current) => ({
      ...current,
      models: current.models.map((item, itemIndex) =>
        itemIndex === index ? { ...item, ...patch } : item
      )
    }));
  }

  function removeModel(index: number) {
    setDraft((current) => ({
      ...current,
      models: current.models.filter((_, itemIndex) => itemIndex !== index)
    }));
  }

  async function saveProvider() {
    if (!draft.name.trim() || !draft.base_url.trim()) {
      setError("名称和 Base URL 不能为空");
      return;
    }
    setSavingProvider(true);
    setError("");
    setNotice("");
    try {
      const input: ProviderInput = {
        id: draft.id,
        name: draft.name.trim(),
        base_url: draft.base_url.trim(),
        models_url: draft.models_url.trim() || undefined,
        is_full_url: draft.is_full_url,
        custom_user_agent: draft.custom_user_agent.trim() || undefined,
        channel_mode: draft.channel_mode,
        channel: draft.channel,
        prompt_cache_retention_enabled: draft.prompt_cache_retention_enabled,
        request_body_gzip_enabled: draft.request_body_gzip_enabled,
        use_system_proxy: draft.use_system_proxy,
        non_sse_compact_compat_enabled: draft.non_sse_compact_compat_enabled,
        key_pool: providerKeyPoolInput(draft.key_pool),
        api_key: draft.api_key || undefined,
        enabled: draft.enabled
      };
      const previousModelIds = new Set(
        config?.providers.find((item) => item.id === draft.id)?.models.map((item) => item.id) ?? []
      );
      const modelsToSave = normalizeModels(draft.models);
      let nextConfig = await command<AppConfig>("add_or_update_provider", { input });
      const provider =
        nextConfig.providers.find((item) => item.id === draft.id) ??
        nextConfig.providers.find((item) => item.name === input.name && item.base_url === input.base_url);

      if (provider) {
        for (const item of modelsToSave) {
          nextConfig = await command<AppConfig>("add_or_update_model", {
            input: { provider_id: provider.id, model: item }
          });
        }
        for (const modelId of previousModelIds) {
          if (!modelsToSave.some((item) => item.id === modelId)) {
            nextConfig = await command<AppConfig>("delete_model", {
              providerId: provider.id,
              provider_id: provider.id,
              modelId,
              model_id: modelId
            });
          }
        }
        setSelectedProviderId(provider.id);
        setDraft({ ...draft, id: provider.id, models: modelsToSave });
        const selectedAgent = nextConfig.agent_injections.find((item) => item.id === selectedAgentId);
        if (activeView === "agent" && selectedAgent) {
          await command<AgentInjectionResult[]>("update_agent_injection_route", {
            input: {
              id: selectedAgent.id,
              provider_id: provider.id
            }
          });
          nextConfig = await command<AppConfig>("get_config");
        }
      }
      setConfig(nextConfig);
      setProviderEditorOpen(false);
      setActiveView("agent");
      setNotice("上游配置已保存并已绑定到当前 Agent");
    } catch (err) {
      setError(String(err));
    } finally {
      setSavingProvider(false);
    }
  }

  async function removeProvider(provider: ProviderConfig, agentId?: string) {
    const scope = agentId ? "当前 Agent 的" : "";
    if (!window.confirm(`删除${scope}上游 ${provider.name}？`)) return;
    setError("");
    try {
      const nextConfig = await command<AppConfig>("delete_provider", {
        providerId: provider.id,
        provider_id: provider.id,
        agentId: agentId ?? null,
        agent_id: agentId ?? null
      });
      setConfig(nextConfig);
      if (selectedProviderId === provider.id) {
        setSelectedProviderId("new");
        setDraft(emptyDraft);
        setProviderEditorOpen(false);
      }
      setNotice(agentId ? "已只删除当前 Agent 的上游，其他 Agent 不受影响" : "上游已删除");
    } catch (err) {
      setError(String(err));
    }
  }


  async function toggleApiKeyVisibility(visible: boolean) {
    if (visible && draft.id && !draft.api_key) {
      try {
        const revealed = await command<string | null>("reveal_provider_api_key", {
          providerId: draft.id,
          provider_id: draft.id
        });
        if (revealed) {
          setDraft((current) =>
            current.id === draft.id ? { ...current, ["api_key"]: revealed } : current
          );
        }
      } catch (err) {
        setError(String(err));
      }
    }
    setApiKeyVisible(visible);
  }

  async function toggleAgentInjection(item: AgentInjectionConfig) {
    if (!item.enabled && !item.provider_id) {
      const defaultProvider =
        providers.find((provider) => provider.id === config?.active_provider_id) ??
        activeProvider ??
        providers[0];
      if (!defaultProvider) {
        setSelectedAgentId(item.id);
        setActiveView("agent");
        setNotice("");
        setError("请先添加一个上游，然后再开启这个 Agent。");
        return;
      }
      await activateAgentProvider(item, defaultProvider, true);
      return;
    }
    setInjectingId(item.id);
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("set_agent_injection_enabled", {
        input: { id: item.id, enabled: !item.enabled }
      });
      setConfig(await command<AppConfig>("get_config"));
      setNotice(results[0]?.status ?? `${item.label} 已更新`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function applyAgentInjection(item: AgentInjectionConfig) {
    if (!item.provider_id) {
      setSelectedAgentId(item.id);
      setActiveView("agent");
      setNotice("");
      setError("请先为这个 Agent 选择一个上游。");
      return;
    }
    setInjectingId(item.id);
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("apply_agent_injection", { id: item.id });
      setConfig(await command<AppConfig>("get_config"));
      setNotice(results[0]?.status ?? `${item.label} 已注入`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function applyEnabledInjections() {
    setInjectingId("all");
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("apply_enabled_agent_injections");
      setConfig(await command<AppConfig>("get_config"));
      setNotice(results.length ? `已刷新 ${results.length} 个 Agent 配置` : "没有已启用的注入配置");
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function updateAgentInjectionRoute(
    item: AgentInjectionConfig,
    providerId: string
  ) {
    setInjectingId(`${item.id}:route`);
    setError("");
    setNotice("");
    try {
      const results = await command<AgentInjectionResult[]>("update_agent_injection_route", {
        input: {
          id: item.id,
          provider_id: providerId || null
        }
      });
      setConfig(await command<AppConfig>("get_config"));
      setNotice(results[0]?.status ?? `${item.label} 路由已更新`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function activateAgentProvider(
    item: AgentInjectionConfig,
    provider: ProviderConfig,
    enableAfterBind = false
  ) {
    setInjectingId(`${item.id}:route`);
    setError("");
    setNotice("");
    try {
      await command<AgentInjectionResult[]>("update_agent_injection_route", {
        input: {
          id: item.id,
          provider_id: provider.id
        }
      });
      let latestConfig = await command<AppConfig>("get_config");
      const latestItem = latestConfig.agent_injections.find((candidate) => candidate.id === item.id);
      if (enableAfterBind && !latestItem?.enabled) {
        await command<AgentInjectionResult[]>("set_agent_injection_enabled", {
          input: { id: item.id, enabled: true }
        });
        latestConfig = await command<AppConfig>("get_config");
      }
      setConfig(latestConfig);
      setNotice(enableAfterBind ? `${item.label} 已启用并绑定 ${provider.name}` : `${item.label} 已绑定 ${provider.name}，未打开开关时不会注入`);
    } catch (err) {
      setError(String(err));
    } finally {
      setInjectingId("");
    }
  }

  async function saveCachePolicy(nextCache = config?.cache) {
    if (!nextCache) return;
    setSavingCachePolicy(true);
    setError("");
    setNotice("");
    try {
      const nextConfig = await command<AppConfig>("save_cache_policy", { input: nextCache });
      setConfig(nextConfig);
      setNotice("缓存策略已保存");
    } catch (err) {
      setError(String(err));
    } finally {
      setSavingCachePolicy(false);
    }
  }

  function updateConfig(patch: Partial<AppConfig>) {
    setConfig((current) => (current ? { ...current, ...patch } : current));
  }

  function updateCache(patch: Partial<AppConfig["cache"]>) {
    setConfig((current) =>
      current ? { ...current, cache: { ...current.cache, ...patch } } : current
    );
  }

  const providers = config?.providers ?? [];
  const injections = visibleAgentInjections(config?.agent_injections ?? []);
  const selectedAgent = injections.find((item) => item.id === selectedAgentId) ?? null;
  const selectedAgentProvider =
    selectedAgent?.provider_id
      ? providers.find((provider) => provider.id === selectedAgent.provider_id) ?? null
      : null;
  const promptCacheRetentionWarning = useMemo(() => {
    const recentError = metrics?.recent_errors?.find((item) =>
      /prompt_cache_retention/i.test(item.message) &&
      /(unsupported|invalid|unknown|not support|not_supported)/i.test(item.message)
    );
    if (!recentError) return null;
    const provider =
      activeProvider?.prompt_cache_retention_enabled
        ? activeProvider
        : selectedAgentProvider?.prompt_cache_retention_enabled
          ? selectedAgentProvider
          : providers.find((item) => item.prompt_cache_retention_enabled) ?? activeProvider ?? selectedAgentProvider;
    return {
      key: `${recentError.at}:${recentError.message}`,
      message: recentError.message,
      provider
    };
  }, [activeProvider, metrics, providers, selectedAgentProvider]);
  const showPromptCacheRetentionWarning =
    Boolean(promptCacheRetentionWarning) &&
    promptCacheRetentionWarning?.key !== dismissedRetentionWarningKey;
  const agentBindingNotice =
    activeView === "agent" && selectedAgent?.enabled && selectedAgentProvider
      ? `${selectedAgent.label} 已启用并绑定 ${selectedAgentProvider.name}`
      : "";
  const feedbackMessage = error || notice || agentBindingNotice;
  const summaryUsage = useMemo(() => {
    if (!metrics) return null;
    if (activeView === "cache") {
      return cacheProviderFilter === "all"
        ? metrics.usage
        : metrics.usage.by_provider.find((item) => item.key === cacheProviderFilter) ?? metrics.usage;
    }
    const activeProviderUsage = activeProvider
      ? metrics.usage.by_provider.find((item) => item.key === activeProvider.name)
      : null;
    if (activeProviderUsage && activeProviderUsage.total_tokens > 0) return activeProviderUsage;
    return metrics.usage;
  }, [activeProvider, activeView, cacheProviderFilter, metrics]);
  const summaryColdAdjusted = coldAdjustedUsage(summaryUsage, includeColdStarts);
  const currentProviderInputTokens = summaryColdAdjusted.inputTokens;
  const currentProviderTotalTokens = summaryColdAdjusted.totalTokens;
  const currentProviderCacheReadTokens = summaryUsage?.cache_read_tokens ?? 0;
  const historyCacheRatio =
    currentProviderInputTokens > 0 ? currentProviderCacheReadTokens / currentProviderInputTokens : 0;

  return (
    <main className="app-shell">
      <aside className="side-rail">
        <div className="brand-lockup">
          <div className="brand-mark">
            <Zap size={21} />
          </div>
          <div>
            <h1>Atoapi</h1>
            <p>本地代理加速器</p>
            <div className="version-wrap">
              <button
                className="version-badge"
                type="button"
                onClick={() => setVersionOpen((open) => !open)}
                aria-expanded={versionOpen}
                aria-label="查看版本更新"
              >
                {appVersion}
              </button>
              {versionOpen ? (
                <div className="version-popover" role="status">
                  <strong>更新内容</strong>
                  {appVersionNotes.map((item) => (
                    <span key={item}>{item}</span>
                  ))}
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="side-section-head">
          <span>Agent 注入</span>
          <button className="tiny-button" onClick={() => void applyEnabledInjections()} disabled={injectingId === "all"}>
            {injectingId === "all" ? <Loader2 className="spin" size={14} /> : <RefreshCw size={14} />}
            刷新
          </button>
        </div>

        <nav className="provider-list agent-side-list">
          {injections.map((item) => (
            <AgentSideTab
              key={item.id}
              item={item}
              provider={providers.find((provider) => provider.id === item.provider_id) ?? null}
              selected={selectedAgent?.id === item.id && activeView === "agent"}
              injectingId={injectingId}
              onSelect={() => {
                setSelectedAgentId(item.id);
                setActiveView("agent");
              }}
              onToggle={() => void toggleAgentInjection(item)}
            />
          ))}
          {!injections.length && <div className="empty-mini">还没有 Agent 注入配置。</div>}
        </nav>

        <div className="side-utility-nav">
          {utilityViews.map((view) => (
            <button
              key={view.id}
              className={activeView === view.id ? "utility-tab active" : "utility-tab"}
              onClick={() => setActiveView(view.id)}
            >
              {view.icon}
              {view.label}
            </button>
          ))}
        </div>
      </aside>

      <section className="main-panel">
        <header className="topbar">
          <div>
            <p className="overline">Control desk</p>
            <h2>{activeView === "agent" ? selectedAgent?.label ?? "Agent 注入" : "缓存统计"}</h2>
          </div>
          <div className="summary-strip">
            <Summary tone="red" label="累计真实 token" value={formatCompactTokens(currentProviderTotalTokens)} />
            <Summary tone="red" label="累计上游命中" value={formatCompactTokens(currentProviderCacheReadTokens)} />
            <Summary tone="red" label="历史前缀命中率" value={percent(historyCacheRatio)} />
          </div>
        </header>

        {feedbackMessage && (
          <div className={error ? "notice error" : "notice success"}>
            {error ? <ShieldCheck size={16} /> : <Check size={16} />}
            <span>{feedbackMessage}</span>
          </div>
        )}

        <section className="workspace">
          {activeView === "agent" && selectedAgent && (
            <AgentWorkspace
              item={selectedAgent}
              config={config}
              baseUrl={baseUrl}
              proxyModeDraft={proxyModeDraft}
              savingProxyModeConfig={savingProxyModeConfig}
              providers={providers}
              injectingId={injectingId}
              onToggle={toggleAgentInjection}
              onProviderSelect={(provider) => void activateAgentProvider(selectedAgent, provider)}
              onCreateProvider={createProvider}
              onProxyModeDraftChange={setProxyModeDraft}
              onSaveProxyModeConfig={() => void saveProxyModeConfig()}
              onEditProvider={(provider) => void editAgentProvider(selectedAgent, provider)}
              onDeleteProvider={(provider) => void removeProvider(provider, selectedAgent.id)}
            />
          )}

          {activeView === "agent" && !selectedAgent && (
            <AgentEmptySelection providers={providers} onCreateProvider={createProvider} />
          )}

          {activeView === "cache" && (
            <CachePanel
              config={config}
              metrics={metrics}
              selectedProvider={cacheProviderFilter}
              savingCachePolicy={savingCachePolicy}
              includeColdStarts={includeColdStarts}
              onSelectedProviderChange={setCacheProviderFilter}
              onIncludeColdStartsChange={setIncludeColdStarts}
              onSmartCacheChange={(nextCache) => void saveCachePolicy(nextCache)}
              onRefresh={() => void refreshMetrics()}
            />
          )}
        </section>
      </section>
      {providerEditorOpen && (
        <ProviderEditorModal
          draft={draft}
          config={config}
          selectedProviderId={selectedProviderId}
          apiKeyVisible={apiKeyVisible}
          loadingModels={loadingModels}
          savingProvider={savingProvider}
          modelCandidates={modelCandidates}
          selectedFetchedModelId={selectedFetchedModelId}
          onDraftChange={setDraft}
          onApiKeyVisibleChange={(visible) => void toggleApiKeyVisibility(visible)}
          onFetchModels={() => void fetchModels()}
          onSelectedFetchedModelChange={setSelectedFetchedModelId}
          onAddFetchedModel={addFetchedModel}
          onAddManualModel={addManualModel}
          onUpdateModel={updateModel}
          onRemoveModel={removeModel}
          onSave={() => void saveProvider()}
          onDelete={() => {
            const provider = config?.providers.find((item) => item.id === draft.id);
            if (provider) {
              void removeProvider(
                provider,
                activeView === "agent" ? selectedAgent?.id : undefined
              );
            }
          }}
          onClose={() => setProviderEditorOpen(false)}
        />
      )}
      {showPromptCacheRetentionWarning && promptCacheRetentionWarning && (
        <div className="modal-backdrop warning-backdrop" role="presentation">
          <section className="warning-modal" role="dialog" aria-modal="true" aria-label="prompt_cache_retention 不兼容提醒">
            <div>
              <h3>当前上游可能不支持 prompt_cache_retention</h3>
              <p>
                上游返回了和 prompt_cache_retention 相关的错误。这个参数用于请求更长时间保留前缀缓存，
                但部分第三方中转不支持，会导致请求失败。
              </p>
              <code>{promptCacheRetentionWarning.message}</code>
            </div>
            <div className="warning-actions">
              <button
                className="soft-button"
                onClick={() => setDismissedRetentionWarningKey(promptCacheRetentionWarning.key)}
              >
                知道了
              </button>
              <button
                className="primary-button"
                onClick={() => {
                  setDismissedRetentionWarningKey(promptCacheRetentionWarning.key);
                  if (promptCacheRetentionWarning.provider) {
                    editProvider(promptCacheRetentionWarning.provider);
                  }
                }}
              >
                去关闭这个开关
              </button>
            </div>
          </section>
        </div>
      )}
    </main>
  );
}

function AgentSideTab({
  item,
  provider,
  selected,
  injectingId,
  onSelect,
  onToggle
}: {
  item: AgentInjectionConfig;
  provider: ProviderConfig | null;
  selected: boolean;
  injectingId: string;
  onSelect: () => void;
  onToggle: () => void;
}) {
  const routeSummary = provider
    ? `${provider.name} · ${providerModelMappingLabel(provider)}`
    : "未选择上游";
  const busy = injectingId === item.id || injectingId === `${item.id}:route`;

  return (
    <div
      className={selected ? "agent-side-tab active" : "agent-side-tab"}
      role="button"
      tabIndex={0}
      onClick={onSelect}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          onSelect();
        }
      }}
    >
      <div className="agent-icon">{agentIcon(item.kind)}</div>
      <div className="agent-side-copy">
        <b>{item.label}</b>
        <small>{routeSummary}</small>
      </div>
      <button
        className={item.enabled ? "mini-toggle on" : "mini-toggle"}
        disabled={busy}
        onClick={(event) => {
          event.stopPropagation();
          onToggle();
        }}
        title={item.enabled ? "关闭这个 Agent 注入" : "开启这个 Agent 注入"}
      >
        <span />
      </button>
    </div>
  );
}

function AgentEmptySelection({
  providers,
  onCreateProvider
}: {
  providers: ProviderConfig[];
  onCreateProvider: () => void;
}) {
  return (
    <section className="agent-workspace">
      <div className="empty-state agent-empty">
        <Workflow size={26} />
        <span>
          请选择左侧某个 Agent 后再配置上游。已启用的 Agent 会继续使用上次保存的上游；模型未命中映射时会直接透传。
        </span>
        {!providers.length && (
          <button className="primary-button" onClick={onCreateProvider}>
            <Plus size={16} />
            新增上游
          </button>
        )}
      </div>
    </section>
  );
}

function AgentWorkspace({
  item,
  config,
  baseUrl,
  proxyModeDraft,
  savingProxyModeConfig,
  providers,
  injectingId,
  onToggle,
  onProviderSelect,
  onCreateProvider,
  onProxyModeDraftChange,
  onSaveProxyModeConfig,
  onEditProvider,
  onDeleteProvider
}: {
  item: AgentInjectionConfig;
  config: AppConfig | null;
  baseUrl: string;
  proxyModeDraft: ProxyModeDraft;
  savingProxyModeConfig: boolean;
  providers: ProviderConfig[];
  injectingId: string;
  onToggle: (item: AgentInjectionConfig) => void;
  onProviderSelect: (provider: ProviderConfig) => void;
  onCreateProvider: () => void;
  onProxyModeDraftChange: Dispatch<SetStateAction<ProxyModeDraft>>;
  onSaveProxyModeConfig: () => void;
  onEditProvider: (provider: ProviderConfig) => void;
  onDeleteProvider: (provider: ProviderConfig) => void;
}) {
  const selectedProvider = providers.find((provider) => provider.id === item.provider_id) ?? null;
  const visibleProviders = providersForAgentEditor(providers, item);
  const routeBusy = injectingId === `${item.id}:route`;

  return (
    <section className="agent-workspace">
      <div className="agent-hero">
        <div className="agent-hero-main">
          <div className="agent-icon large">{agentIcon(item.kind)}</div>
          <div>
            <h3>{item.label}</h3>
            <p>
              {selectedProvider
                ? item.enabled
                  ? `已启用并绑定 ${selectedProvider.name} · ${providerModelMappingLabel(selectedProvider)}`
                  : `已选择 ${selectedProvider.name} · ${providerModelMappingLabel(selectedProvider)}，但这个 Agent 未启用`
                : "为这个 Agent 选择一个中转上游。模型未命中映射时会直接透传 Agent 请求里的 model。"}
            </p>
            {item.target_path && <code>{item.target_path}</code>}
          </div>
        </div>
        <div className="agent-hero-actions compact">
          <button className="primary-button" onClick={onCreateProvider}>
            <Plus size={16} />
            新增上游
          </button>
        </div>
        {item.kind !== "proxy-mode" && (
          <AgentEndpointPanel item={item} config={config} baseUrl={baseUrl} />
        )}
      </div>

      {item.kind === "proxy-mode" ? (
        <ProxyModeSettings
          item={item}
          config={config}
          mainBaseUrl={baseUrl}
          draft={proxyModeDraft}
          saving={savingProxyModeConfig}
          onDraftChange={onProxyModeDraftChange}
          onSave={onSaveProxyModeConfig}
        />
      ) : null}
      {item.last_status && <div className="agent-last-status">{item.last_status}</div>}

      <div className="agent-provider-head">
        <div>
          <h3>选择这个 Agent 使用的上游</h3>
          <p>点中某个上游后，这个 Agent 会绑定并同步配置；模型映射在上游编辑里维护，未命中时直接透传。</p>
        </div>
        {routeBusy && (
          <span className="route-saving">
            <Loader2 className="spin" size={15} />
            保存中
          </span>
        )}
      </div>

      {visibleProviders.length ? (
        <div className="agent-provider-grid">
          {visibleProviders.map((provider) => {
            const isSelected = item.provider_id === provider.id;

            return (
              <div
                className={isSelected ? "agent-provider-card active" : "agent-provider-card"}
                key={provider.id}
                role="button"
                tabIndex={0}
                onClick={() => onProviderSelect(provider)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    onProviderSelect(provider);
                  }
                }}
              >
                <div className="provider-card-top">
                  <span className="provider-glyph">{provider.name.slice(0, 1).toUpperCase()}</span>
                  <div>
                    <h4>{provider.name}</h4>
                    <p>{channelLabel(provider.channel)} / {providerModelMappingLabel(provider)}</p>
                  </div>
                  {isSelected ? (
                    <span className={item.enabled ? "selected-badge" : "selected-badge pending"}>
                      <Check size={14} />
                      {item.enabled ? "已启用并绑定" : "已选择未启用"}
                    </span>
                  ) : (
                    <span className={provider.enabled ? "state-dot" : "state-dot muted"} />
                  )}
                </div>

                <code>{provider.base_url}</code>

                <div className="provider-card-actions" onClick={(event) => event.stopPropagation()}>
                  <button className="soft-button" onClick={() => onEditProvider(provider)}>
                    <Settings2 size={15} />
                    编辑
                  </button>
                  <button className="danger-button" onClick={() => onDeleteProvider(provider)}>
                    <Trash2 size={15} />
                    删除
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <div className="empty-state agent-empty">
          <DatabaseZap size={24} />
          <span>还没有上游。先新增一个中转，然后这个 Agent 会自动绑定到它。</span>
          <button className="primary-button" onClick={onCreateProvider}>
            <Plus size={16} />
            新增上游
          </button>
        </div>
      )}
    </section>
  );
}

function AgentEndpointPanel({
  item,
  config,
  baseUrl
}: {
  item: AgentInjectionConfig;
  config: AppConfig | null;
  baseUrl: string;
}) {
  const key = item.kind === "codex" ? "PROXY_MANAGED" : item.local_key ?? config?.local_key ?? "";
  const rows = agentEndpointRows(item, baseUrl, key);
  if (!rows.length) return null;
  return (
    <section className="agent-endpoint-card">
      <div className="agent-endpoint-head">
        <div>
          <h3>当前 Agent 注入地址</h3>
          <p>这个地址只给当前 Agent 使用。其他 Agent 即使选择同一个上游，也会使用自己的注入配置。</p>
        </div>
        <span className="endpoint-pill">主代理</span>
      </div>
      <div className="endpoint-grid">
        {rows.map((row) => (
          <EndpointRow key={row.label} label={row.label} value={row.value} />
        ))}
      </div>
    </section>
  );
}

function ProxyModeSettings({
  item,
  config,
  mainBaseUrl,
  draft,
  saving,
  onDraftChange,
  onSave
}: {
  item: AgentInjectionConfig;
  config: AppConfig | null;
  mainBaseUrl: string;
  draft: ProxyModeDraft;
  saving: boolean;
  onDraftChange: Dispatch<SetStateAction<ProxyModeDraft>>;
  onSave: () => void;
}) {
  const port = Number(draft.port);
  const previewBaseUrl = `http://${draft.host.trim() || "127.0.0.1"}:${draft.port || "18884"}`;
  const conflictsWithMain = Boolean(config && proxyAddressConflicts(draft.host, port, config.host, config.port));
  return (
    <section className="proxy-mode-settings">
      <div className="proxy-mode-head">
        <div>
          <h3>本地代理设置</h3>
          <p>手动填写代理地址的客户端走这里。它使用独立监听地址和代理模式 key，不和其他 Agent 注入入口混用。</p>
        </div>
        <span className={item.enabled ? "status-pill online" : "status-pill"}>
          {item.enabled ? "已启用" : "未启用"}
        </span>
      </div>
      <div className="proxy-mode-form">
        <Field label="代理 IP">
          <input
            value={draft.host}
            onChange={(event) => onDraftChange((current) => ({ ...current, host: event.target.value }))}
            placeholder="127.0.0.1"
          />
        </Field>
        <Field label="端口">
          <input
            value={draft.port}
            onChange={(event) => onDraftChange((current) => ({ ...current, port: event.target.value }))}
            placeholder="18884"
          />
        </Field>
        <button className="primary-button" onClick={onSave} disabled={saving || conflictsWithMain}>
          {saving ? <Loader2 className="spin" size={16} /> : <Save size={16} />}
          保存地址
        </button>
      </div>
      {conflictsWithMain ? (
        <div className="endpoint-warning">本地代理模式不能和其他 Agent 注入地址相同：{mainBaseUrl}</div>
      ) : null}
      <div className="proxy-mode-grid">
        <EndpointRow label="OpenAI Base URL" value={`${previewBaseUrl}/v1`} />
        <EndpointRow label="Anthropic Base URL" value={previewBaseUrl} />
        <EndpointRow label="代理模式 Key" value={item.local_key ?? config?.local_key ?? ""} wide />
      </div>
    </section>
  );
}

function EndpointRow({ label, value, wide = false }: { label: string; value: string; wide?: boolean }) {
  return (
    <div className={wide ? "endpoint-row wide" : "endpoint-row"}>
      <span>{label}</span>
      <code>{value}</code>
      <button className="icon-button" onClick={() => void navigator.clipboard.writeText(value)} title={`复制 ${label}`}>
        <Copy size={16} />
      </button>
    </div>
  );
}

function agentEndpointRows(item: AgentInjectionConfig, baseUrl: string, key: string) {
  if (item.kind === "codex") {
    return [
      { label: "Codex Responses Base URL", value: `${baseUrl}/codex/v1` },
      { label: "Codex Token", value: key }
    ];
  }
  if (item.kind === "claude-code" || item.kind === "claude-desktop") {
    return [
      { label: "Anthropic Base URL", value: baseUrl },
      { label: "Agent Key", value: key }
    ];
  }
  if (item.kind === "open-code" || item.kind === "open-claw" || item.kind === "hermes") {
    return [
      { label: "OpenAI Base URL", value: `${baseUrl}/v1` },
      { label: "Agent Key", value: key }
    ];
  }
  if (item.kind === "gemini") {
    return [{ label: "状态", value: "暂未启用原生 Gemini generateContent 代理入口" }];
  }
  return [];
}
function ProviderEditorModal({
  draft,
  config,
  selectedProviderId,
  apiKeyVisible,
  loadingModels,
  savingProvider,
  modelCandidates,
  selectedFetchedModelId,
  onDraftChange,
  onApiKeyVisibleChange,
  onFetchModels,
  onSelectedFetchedModelChange,
  onAddFetchedModel,
  onAddManualModel,
  onUpdateModel,
  onRemoveModel,
  onSave,
  onDelete,
  onClose
}: {
  draft: ProviderDraft;
  config: AppConfig | null;
  selectedProviderId: string;
  apiKeyVisible: boolean;
  loadingModels: boolean;
  savingProvider: boolean;
  modelCandidates: ModelConfig[];
  selectedFetchedModelId: string;
  onDraftChange: Dispatch<SetStateAction<ProviderDraft>>;
  onApiKeyVisibleChange: (visible: boolean) => void;
  onFetchModels: () => void;
  onSelectedFetchedModelChange: (id: string) => void;
  onAddFetchedModel: () => void;
  onAddManualModel: () => void;
  onUpdateModel: (index: number, patch: Partial<ModelConfig>) => void;
  onRemoveModel: (index: number) => void;
  onSave: () => void;
  onDelete: () => void;
  onClose: () => void;
}) {
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section
        className="provider-modal"
        role="dialog"
        aria-modal="true"
        aria-label="上游配置"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="modal-head">
          <div>
            <h3>{selectedProviderId === "new" ? "新增中转上游" : "编辑中转上游"}</h3>
            <p>保存后会回到当前 Agent 页面，并把它绑定到这个上游。</p>
          </div>
          <button className="icon-button" onClick={onClose} title="关闭">
            <X size={17} />
          </button>
        </div>
        <div className="modal-body">
          <ProviderPanel
            draft={draft}
            config={config}
            selectedProviderId={selectedProviderId}
            apiKeyVisible={apiKeyVisible}
            loadingModels={loadingModels}
            savingProvider={savingProvider}
            modelCandidates={modelCandidates}
            selectedFetchedModelId={selectedFetchedModelId}
            onDraftChange={onDraftChange}
            onApiKeyVisibleChange={onApiKeyVisibleChange}
            onFetchModels={onFetchModels}
            onSelectedFetchedModelChange={onSelectedFetchedModelChange}
            onAddFetchedModel={onAddFetchedModel}
            onAddManualModel={onAddManualModel}
            onUpdateModel={onUpdateModel}
            onRemoveModel={onRemoveModel}
            onSave={onSave}
            onDelete={onDelete}
          />
        </div>
      </section>
    </div>
  );
}

function ProviderPanel({
  draft,
  config,
  selectedProviderId,
  apiKeyVisible,
  loadingModels,
  savingProvider,
  modelCandidates,
  selectedFetchedModelId,
  onDraftChange,
  onApiKeyVisibleChange,
  onFetchModels,
  onSelectedFetchedModelChange,
  onAddFetchedModel,
  onAddManualModel,
  onUpdateModel,
  onRemoveModel,
  onSave,
  onDelete
}: {
  draft: ProviderDraft;
  config: AppConfig | null;
  selectedProviderId: string;
  apiKeyVisible: boolean;
  loadingModels: boolean;
  savingProvider: boolean;
  modelCandidates: ModelConfig[];
  selectedFetchedModelId: string;
  onDraftChange: Dispatch<SetStateAction<ProviderDraft>>;
  onApiKeyVisibleChange: (visible: boolean) => void;
  onFetchModels: () => void;
  onSelectedFetchedModelChange: (id: string) => void;
  onAddFetchedModel: () => void;
  onAddManualModel: () => void;
  onUpdateModel: (index: number, patch: Partial<ModelConfig>) => void;
  onRemoveModel: (index: number) => void;
  onSave: () => void;
  onDelete: () => void;
}) {
  const isActive = Boolean(draft.id && config?.active_provider_id === draft.id);
  const modeValue: "auto" | Channel = draft.channel_mode === "auto" ? "auto" : draft.channel;
  const supportsPromptCacheRetention = draft.channel !== "anthropic";
  const [contextDrafts, setContextDrafts] = useState<Record<number, string>>({});

  function updateMode(value: "auto" | Channel) {
    if (value === "auto") {
      onDraftChange({ ...draft, channel_mode: "auto", channel: draft.channel || "responses" });
      return;
    }
    onDraftChange({ ...draft, channel_mode: "manual", channel: value });
  }

  useEffect(() => {
    setContextDrafts({});
  }, [draft.id]);

  function beginContextEdit(index: number, value?: number | null) {
    setContextDrafts((current) => ({
      ...current,
      [index]: formatRawContextInput(value)
    }));
  }

  function updateContextEdit(index: number, value: string) {
    setContextDrafts((current) => ({
      ...current,
      [index]: value
    }));
    onUpdateModel(index, { context_window: parseContext(value) });
  }

  function endContextEdit(index: number) {
    setContextDrafts((current) => {
      const next = { ...current };
      delete next[index];
      return next;
    });
  }

  return (
    <div className="panel-grid provider-grid">
      <section className="surface">
        <div className="panel-head">
          <div>
            <h3>{selectedProviderId === "new" ? "新增上游" : "上游配置"}</h3>
            <p>填写真实上游地址。默认自动识别通道，特殊上游再切到手动。</p>
          </div>
          <div className="chip-row">
            {isActive && <span className="active-chip"><Check size={14} /> 当前上游</span>}
          </div>
        </div>

        <div className="form-grid">
          <Field label="名称">
            <input
              value={draft.name}
              onChange={(event) => onDraftChange({ ...draft, name: event.target.value })}
              placeholder="上游名称"
            />
          </Field>
          <Field label="通道">
            <SelectShell>
              <select
                value={modeValue}
                onChange={(event) => updateMode(event.target.value as "auto" | Channel)}
                title="默认自动识别。需要排查或上游只支持某种格式时，再手动指定通道。"
              >
                {upstreamModeOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label} · {option.detail}
                  </option>
                ))}
              </select>
            </SelectShell>
          </Field>
          <Field label="Base URL" wide>
            <div className="input-with-icon">
              <Link2 size={17} />
              <input
                value={draft.base_url}
                onChange={(event) => onDraftChange({ ...draft, base_url: event.target.value })}
                placeholder="https://example.com/v1"
              />
            </div>
          </Field>
          <Field label="API Key" wide>
            <div className="input-with-icon">
              <KeyRound size={17} />
              <input
                type={apiKeyVisible ? "text" : "password"}
                value={draft.api_key}
                onChange={(event) => onDraftChange({ ...draft, api_key: event.target.value })}
                placeholder={draft.id ? "留空则保留已保存密钥" : "输入上游 API Key"}
              />
              <button className="inline-icon" onClick={() => onApiKeyVisibleChange(!apiKeyVisible)} type="button" title={apiKeyVisible ? "隐藏 Key" : "显示 Key"}>
                {apiKeyVisible ? <EyeOff size={16} /> : <Eye size={16} />}
              </button>
            </div>
          </Field>
        </div>

        <div className="provider-option-stack">
          <div className={draft.use_system_proxy ? "provider-option-control active" : "provider-option-control"}>
            <h4
              className="provider-option-title"
              data-help="默认关闭并直连上游。只有该上游必须通过 Windows 系统代理才能访问时才开启；模型获取、Key 测活、普通转发、压缩和同步都会使用同一网络路径。"
              tabIndex={0}
            >
              使用系统代理
            </h4>
            <button
              className={draft.use_system_proxy ? "smart-cache-toggle on" : "smart-cache-toggle"}
              type="button"
              onClick={() =>
                onDraftChange({
                  ...draft,
                  use_system_proxy: !draft.use_system_proxy
                })
              }
            >
              <span />
              <b>{draft.use_system_proxy ? "开" : "关"}</b>
            </button>
          </div>

          {supportsPromptCacheRetention && (
            <div className={draft.prompt_cache_retention_enabled ? "provider-option-control active" : "provider-option-control"}>
              <h4
                className="provider-option-title"
                data-help="默认开启。仅对 OpenAI Chat / Responses 上游发送 prompt_cache_retention=24h，用来请求更久的前缀缓存保留；上游不支持并报错时关闭。"
                tabIndex={0}
              >
                prompt_cache_retention
              </h4>
              <button
                className={draft.prompt_cache_retention_enabled ? "smart-cache-toggle on" : "smart-cache-toggle"}
                type="button"
                onClick={() =>
                  onDraftChange({
                    ...draft,
                    prompt_cache_retention_enabled: !draft.prompt_cache_retention_enabled
                  })
                }
              >
                <span />
                <b>{draft.prompt_cache_retention_enabled ? "开" : "关"}</b>
              </button>
            </div>
          )}

          <div className={draft.request_body_gzip_enabled ? "provider-option-control active" : "provider-option-control"}>
            <h4
              className="provider-option-title"
              data-help="默认关闭。只对超过 600KB 的上游请求体尝试 gzip 发送；上游不支持时会回退普通 JSON，不改变语义。"
              tabIndex={0}
            >
              大请求体 gzip
            </h4>
            <button
              className={draft.request_body_gzip_enabled ? "smart-cache-toggle on" : "smart-cache-toggle"}
              type="button"
              onClick={() =>
                onDraftChange({
                  ...draft,
                  request_body_gzip_enabled: !draft.request_body_gzip_enabled
                })
              }
            >
              <span />
              <b>{draft.request_body_gzip_enabled ? "开" : "关"}</b>
            </button>
          </div>

          <div className={draft.non_sse_compact_compat_enabled ? "provider-option-control active warning" : "provider-option-control"}>
            <h4
              className="provider-option-title"
              data-help="默认关闭即快速压缩模式，按 cc-switch 思路优先走更快的协议转换。只有 ZCode 或类似 Agent 对非 SSE Responses 压缩 JSON 字段校验很严格时才开启。"
              tabIndex={0}
            >
              非 SSE 压缩校验兼容
            </h4>
            <button
              className={draft.non_sse_compact_compat_enabled ? "smart-cache-toggle on" : "smart-cache-toggle"}
              type="button"
              onClick={() =>
                onDraftChange({
                  ...draft,
                  non_sse_compact_compat_enabled: !draft.non_sse_compact_compat_enabled
                })
              }
            >
              <span />
              <b>{draft.non_sse_compact_compat_enabled ? "开" : "关"}</b>
            </button>
          </div>
        </div>

        <MultiKeyManager draft={draft} onDraftChange={onDraftChange} />

        <div className="action-row">
          <button className="primary-button" onClick={onSave} disabled={savingProvider}>
            {savingProvider ? <Loader2 className="spin" size={16} /> : <Save size={16} />}
            保存并绑定当前 Agent
          </button>
          {draft.id && (
            <button className="danger-button" onClick={onDelete}>
              <Trash2 size={16} />
              删除上游
            </button>
          )}
        </div>
      </section>

      <section className="surface model-surface">
        <div className="panel-head compact">
          <div>
            <h3>模型映射</h3>
            <p>{draft.models.length ? draft.models.length + " 个映射已在列表中" : "不配置映射也可以直接代理转发；获取模型只是为了快速加入映射。"}</p>
          </div>
          <button className="soft-button" onClick={onAddManualModel}>
            <Plus size={16} />
            手动添加
          </button>
        </div>

        <div className="model-picker">
          <button className="accent-button" onClick={onFetchModels} disabled={loadingModels || !draft.base_url}>
            {loadingModels ? <Loader2 className="spin" size={16} /> : <DatabaseZap size={16} />}
            获取模型
          </button>
          <SelectShell disabled={!modelCandidates.length}>
            <select
              value={selectedFetchedModelId}
              disabled={!modelCandidates.length}
              onChange={(event) => onSelectedFetchedModelChange(event.target.value)}
            >
              {!modelCandidates.length && <option value="">等待获取模型列表</option>}
              {modelCandidates.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.id}
                </option>
              ))}
            </select>
          </SelectShell>
          <button className="soft-button" onClick={onAddFetchedModel} disabled={!selectedFetchedModelId}>
            <Plus size={16} />
            加入
          </button>
        </div>

        <div className="model-table">
          {draft.models.length === 0 ? (
            <div className="empty-state">
              <DatabaseZap size={22} />
              <span>映射列表为空。Agent 发来的模型会原样发给上游。</span>
            </div>
          ) : (
            draft.models.map((item, index) => (
              <div className="model-row" key={"model-row-" + index}>
                <label className="model-map-field request-model-field">
                  <span>请求模型</span>
                  <input
                    value={item.request_model_id ?? ""}
                    onChange={(event) =>
                      onUpdateModel(index, {
                        request_model_id: event.target.value
                      })
                    }
                    placeholder={item.id || "agent 看到的模型名"}
                  />
                </label>
                <span className="model-map-arrow">→</span>
                <label className="model-map-field upstream-model-field">
                  <span>实际模型</span>
                  <input
                    value={item.id}
                    onChange={(event) =>
                      onUpdateModel(index, {
                        id: event.target.value,
                        display_name: event.target.value
                      })
                    }
                    placeholder="发送到 API 的模型"
                  />
                </label>
                <input
                  className="context-field"
                  inputMode="numeric"
                  value={contextDrafts[index] ?? formatContextInput(item.context_window)}
                  onFocus={() => beginContextEdit(index, item.context_window)}
                  onBlur={() => endContextEdit(index)}
                  onChange={(event) => updateContextEdit(index, event.target.value)}
                  placeholder="上下文"
                  title={item.context_window ? formatNumber(item.context_window) : "上下文"}
                />
                <div className="model-reasoning-row">
                  <div
                    className="model-reasoning-label"
                    title={
                      item.supported_reasoning_efforts?.length
                        ? `上游声明支持：${item.supported_reasoning_efforts.join(" / ")}`
                        : "默认跟随 Agent，不额外探测上游能力。开启后强制使用右侧强度。"
                    }
                  >
                    <BrainCircuit size={15} />
                    <span>推理强度</span>
                  </div>
                  <button
                    className={item.reasoning_effort_override_enabled ? "mini-toggle on" : "mini-toggle"}
                    type="button"
                    onClick={() =>
                      onUpdateModel(index, {
                        reasoning_effort_override_enabled: !item.reasoning_effort_override_enabled,
                        reasoning_effort: !item.reasoning_effort_override_enabled
                          ? item.reasoning_effort ?? "medium"
                          : item.reasoning_effort
                      })
                    }
                    title={item.reasoning_effort_override_enabled ? "关闭后跟随 Agent" : "开启模型级强制覆盖"}
                  >
                    <span />
                  </button>
                  <SelectShell disabled={!item.reasoning_effort_override_enabled}>
                    <select
                      value={
                        item.reasoning_effort_override_enabled
                          ? item.reasoning_effort ?? "medium"
                          : ""
                      }
                      disabled={!item.reasoning_effort_override_enabled}
                      onChange={(event) =>
                        onUpdateModel(index, {
                          reasoning_effort: event.target.value
                        })
                      }
                    >
                      {!item.reasoning_effort_override_enabled ? (
                        <option value="">跟随 Agent</option>
                      ) : null}
                      {reasoningEffortOptions.map((effort) => (
                        <option key={effort} value={effort}>
                          {effort}
                        </option>
                      ))}
                    </select>
                  </SelectShell>
                </div>
                <button
                  className={item.enabled ? "mini-toggle on" : "mini-toggle"}
                  onClick={() => onUpdateModel(index, { enabled: !item.enabled })}
                  title={item.enabled ? "停用模型" : "启用模型"}
                >
                  <span />
                </button>
                <button className="icon-button danger" onClick={() => onRemoveModel(index)} title="删除模型">
                  <Trash2 size={15} />
                </button>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

function MultiKeyManager({
  draft,
  onDraftChange
}: {
  draft: ProviderDraft;
  onDraftChange: Dispatch<SetStateAction<ProviderDraft>>;
}) {
  const [batchKeys, setBatchKeys] = useState("");
  const [testingKeyId, setTestingKeyId] = useState("");
  const [testingAll, setTestingAll] = useState(false);
  const [revealingKeyId, setRevealingKeyId] = useState("");
  const pool = draft.key_pool;
  const availableKeys = pool.keys.filter((key) => key.enabled && key.status !== "unhealthy").length;
  const strategy = keyStrategyOptions.find((item) => item.value === pool.strategy);

  function updatePoolFrom(updater: (pool: ProviderKeyPoolDraft) => ProviderKeyPoolDraft) {
    onDraftChange((current) => ({
      ...current,
      key_pool: updater(current.key_pool)
    }));
  }

  function updatePool(patch: Partial<ProviderKeyPoolDraft>) {
    updatePoolFrom((currentPool) => ({ ...currentPool, ...patch }));
  }

  function updateKey(id: string, patch: Partial<ProviderKeyDraft>) {
    updatePoolFrom((currentPool) => ({
      ...currentPool,
      keys: currentPool.keys.map((key) => (key.id === id ? { ...key, ...patch } : key))
    }));
  }

  async function revealKeySecret(key: ProviderKeyDraft) {
    const currentSecret = pool.keys.find((item) => item.id === key.id)?.key.trim() || key.key.trim();
    if (currentSecret) return currentSecret;
    if (!draft.id) return "";
    setRevealingKeyId(key.id);
    try {
      const revealed = await command<string | null>("reveal_provider_key", {
        providerId: draft.id,
        provider_id: draft.id,
        keyId: key.id,
        key_id: key.id
      });
      if (revealed) {
        updateKey(key.id, { key: revealed });
        return revealed;
      }
    } catch (err) {
      updateKey(key.id, { last_error: String(err) });
    } finally {
      setRevealingKeyId((current) => (current === key.id ? "" : current));
    }
    return "";
  }

  async function copyKeySecret(key: ProviderKeyDraft) {
    const secret = await revealKeySecret(key);
    if (secret) {
      await navigator.clipboard.writeText(secret);
    }
  }

  function addKeys() {
    const parsed = parseBatchKeys(batchKeys);
    if (!parsed.length) return;
    const existing = new Set(pool.keys.map((key) => (key.key || key.preview).trim()).filter(Boolean));
    const nextKeys = parsed
      .filter((key) => !existing.has(key))
      .map((key) => newProviderKeyDraft(key));
    if (!nextKeys.length) return;
    updatePoolFrom((currentPool) => ({ ...currentPool, keys: [...currentPool.keys, ...nextKeys], enabled: true }));
    setBatchKeys("");
  }

  async function testKey(key: ProviderKeyDraft) {
    setTestingKeyId(key.id);
    try {
      const result = await command<ProviderKeyTestResult>("test_provider_key", {
        input: {
          provider_id: draft.id ?? null,
          key_id: key.id,
          api_key: key.key.trim() || undefined,
          base_url: draft.base_url,
          models_url: draft.models_url || undefined,
          is_full_url: draft.is_full_url,
          custom_user_agent: draft.custom_user_agent || undefined,
          channel: draft.channel,
          use_system_proxy: draft.use_system_proxy
        }
      });
      updateKey(key.id, {
        enabled: result.ok,
        status: result.ok ? "healthy" : "unhealthy",
        last_checked_at: new Date().toISOString(),
        last_error: result.ok ? null : result.message,
        successes: result.ok ? key.successes + 1 : key.successes,
        failures: result.ok ? key.failures : key.failures + 1
      });
    } catch (err) {
      updateKey(key.id, {
        enabled: false,
        status: "unhealthy",
        last_checked_at: new Date().toISOString(),
        last_error: String(err),
        failures: key.failures + 1
      });
    } finally {
      setTestingKeyId("");
    }
  }

  async function testAllKeys() {
    setTestingAll(true);
    try {
      for (const key of pool.keys) {
        await testKey(key);
      }
    } finally {
      setTestingAll(false);
    }
  }

  function moveKey(id: string, direction: -1 | 1) {
    updatePoolFrom((currentPool) => {
      const index = currentPool.keys.findIndex((key) => key.id === id);
      const target = index + direction;
      if (index < 0 || target < 0 || target >= currentPool.keys.length) return currentPool;
      const keys = [...currentPool.keys];
      [keys[index], keys[target]] = [keys[target], keys[index]];
      return { ...currentPool, keys };
    });
  }

  return (
    <section className={pool.enabled ? "multi-key-card active" : "multi-key-card"}>
      <button
        className="multi-key-summary"
        type="button"
        onClick={() => updatePool({ enabled: !pool.enabled })}
        title="开启后，请求会按负载均衡策略自动选择可用 Key；余额不足、限额、鉴权失败等错误会自动停用异常 Key。"
      >
        <span className="multi-key-summary-icon"><KeyRound size={16} /></span>
        <span>
          <b>多 Key 管理</b>
          <small>{pool.enabled ? availableKeys + "/" + pool.keys.length + " 可用 · " + (strategy?.label ?? "轮询") : "关闭 · 点击开启"}</small>
        </span>
        <span className={pool.enabled ? "mini-toggle on" : "mini-toggle"} aria-hidden="true"><span /></span>
      </button>

      {pool.enabled ? (
        <div className="multi-key-body">
          <div className="multi-key-grid compact">
            <Field label="负载均衡">
              <SelectShell>
                <select
                  value={pool.strategy}
                  onChange={(event) =>
                    updatePool({ strategy: event.target.value as KeyLoadBalanceStrategy })
                  }
                >
                  {keyStrategyOptions.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </SelectShell>
            </Field>
            <Field label="失败阈值">
              <input
                value={pool.failure_threshold}
                onChange={(event) => updatePool({ failure_threshold: Number(event.target.value) || 1 })}
              />
            </Field>
            <Field label="恢复分钟">
              <input
                value={pool.recovery_minutes}
                onChange={(event) => updatePool({ recovery_minutes: Number(event.target.value) || 1 })}
              />
            </Field>
          </div>
          {strategy ? <div className="multi-key-hint">{strategy.hint}</div> : null}

          <div className="multi-key-batch">
            <label>
              <span>批量添加 Key</span>
              <textarea
                value={batchKeys}
                onChange={(event) => setBatchKeys(event.target.value)}
                placeholder="每行一个 Key，也支持用空格、逗号或分号隔开"
              />
            </label>
            <div className="multi-key-actions">
              <button className="primary-button" type="button" onClick={addKeys} disabled={!batchKeys.trim()}>
                <Plus size={16} />
                添加
              </button>
              <button className="soft-button" type="button" onClick={() => void testAllKeys()} disabled={!pool.keys.length || testingAll}>
                {testingAll ? <Loader2 className="spin" size={16} /> : <Activity size={16} />}
                全部检测
              </button>
              <button
                className="danger-button"
                type="button"
                onClick={() =>
                  updatePoolFrom((currentPool) => ({
                    ...currentPool,
                    keys: currentPool.keys.filter((key) => key.enabled && key.status !== "unhealthy")
                  }))
                }
                disabled={!pool.keys.some((key) => !key.enabled || key.status === "unhealthy")}
              >
                <Trash2 size={16} />
                清不可用
              </button>
            </div>
          </div>

          <div className="multi-key-list">
            {pool.keys.length ? (
              pool.keys.map((key, index) => (
                <div className="multi-key-row" key={key.id}>
                  <div className="multi-key-row-head">
                    <span className="multi-key-index">#{index + 1}</span>
                    <span className={"multi-key-status " + key.status}>{keyStatusLabel(key.status)}</span>
                    <div className="multi-key-stats">
                      <span>总 {formatNumber(key.total_requests)}</span>
                      <span>成功 {formatNumber(key.successes)}</span>
                      <span>失败 {formatNumber(key.failures)}</span>
                    </div>
                  </div>

                  <div className="multi-key-fields">
                    <label className="multi-key-field alias-field">
                      <span>别名</span>
                      <input
                        value={key.alias}
                        onChange={(event) => updateKey(key.id, { alias: event.target.value })}
                        placeholder="可选"
                      />
                    </label>
                                        <div className="multi-key-field key-field">
                      <span>Key</span>
                      <div className="multi-key-secret-wrap">
                        <input
                          className="multi-key-secret"
                          value={key.key}
                          autoComplete="off"
                          spellCheck={false}
                          onFocus={() => void revealKeySecret(key)}
                          onChange={(event) =>
                            updateKey(key.id, {
                              key: event.target.value,
                              status: "unknown",
                              last_error: null,
                              disabled_until: null
                            })
                          }
                          placeholder={key.preview || "输入 Key"}
                          title="点击加载已保存的 Key 原文，可直接编辑或复制"
                        />
                        <button
                          className="icon-button multi-key-copy"
                          type="button"
                          onClick={() => void copyKeySecret(key)}
                          disabled={revealingKeyId === key.id || (!key.key.trim() && !draft.id)}
                          title="复制 Key"
                        >
                          {revealingKeyId === key.id ? <Loader2 className="spin" size={14} /> : <Copy size={14} />}
                        </button>
                      </div>
                    </div>
                    <label className="multi-key-field priority-field">
                      <span>优先级</span>
                      <input
                        className="multi-key-priority"
                        value={key.priority}
                        onChange={(event) => updateKey(key.id, { priority: Number(event.target.value) || 0 })}
                        title="优先级"
                      />
                    </label>
                  </div>

                  <div className="multi-key-row-actions">
                    <button
                      className={key.enabled ? "mini-toggle on" : "mini-toggle"}
                      type="button"
                      onClick={() => updateKey(key.id, { enabled: !key.enabled })}
                      title={key.enabled ? "关闭这个 Key" : "启用这个 Key"}
                    >
                      <span />
                    </button>
                    <button
                      className="icon-button"
                      type="button"
                      onClick={() => void testKey(key)}
                      title="检测这个 Key"
                      disabled={testingKeyId === key.id || (!key.key.trim() && !draft.id)}
                    >
                      {testingKeyId === key.id ? <Loader2 className="spin" size={15} /> : <Play size={15} />}
                    </button>
                    <button className="icon-button" type="button" onClick={() => moveKey(key.id, -1)} disabled={index === 0} title="上移">
                      <ArrowUp size={15} />
                    </button>
                    <button className="icon-button" type="button" onClick={() => moveKey(key.id, 1)} disabled={index === pool.keys.length - 1} title="下移">
                      <ArrowDown size={15} />
                    </button>
                    <button
                      className="icon-button danger"
                      type="button"
                      onClick={() =>
                        updatePoolFrom((currentPool) => ({
                          ...currentPool,
                          keys: currentPool.keys.filter((item) => item.id !== key.id)
                        }))
                      }
                      title="删除 Key"
                    >
                      <Trash2 size={15} />
                    </button>
                  </div>
                </div>
              ))
            ) : (
              <div className="multi-key-empty">暂无 Key。添加后可以检测可用性，并按策略热重载生效。</div>
            )}
          </div>
        </div>
      ) : null}
    </section>
  );
}

function CachePanel({
  config,
  metrics,
  selectedProvider,
  savingCachePolicy,
  includeColdStarts,
  onSelectedProviderChange,
  onIncludeColdStartsChange,
  onSmartCacheChange,
  onRefresh
}: {
  config: AppConfig | null;
  metrics: MetricsSnapshot | null;
  selectedProvider: string;
  savingCachePolicy: boolean;
  includeColdStarts: boolean;
  onSelectedProviderChange: (provider: string) => void;
  onIncludeColdStartsChange: (include: boolean) => void;
  onSmartCacheChange: (nextCache: AppConfig["cache"]) => void;
  onRefresh: () => void;
}) {
  const [requestPage, setRequestPage] = useState(1);
  const usage = selectedProvider === "all"
    ? metrics?.usage
    : metrics?.usage.by_provider.find((item) => item.key === selectedProvider);
  const traffic = selectedProvider === "all"
    ? null
    : metrics?.provider_stats.find((item) => item.provider === selectedProvider) ?? null;
  const providerOptions = Array.from(
    new Set([
      ...(metrics?.usage.by_provider.map((item) => item.key) ?? []),
      ...(metrics?.provider_stats.map((item) => item.provider) ?? [])
    ])
  ).filter(Boolean);
  const adjustedUsage = coldAdjustedUsage(usage, includeColdStarts);
  const inputTokens = adjustedUsage.inputTokens;
  const outputTokens = adjustedUsage.outputTokens;
  const cacheReadTokens = usage?.cache_read_tokens ?? 0;
  const cacheCreationTokens = usage?.cache_creation_tokens ?? 0;
  const totalTokens = adjustedUsage.totalTokens;
  const recentUsage = selectedProvider === "all" ? metrics?.recent_usage : traffic?.recent_usage;
  const adjustedRecentUsage = coldAdjustedUsage(recentUsage, includeColdStarts);
  const recentInputTokens = adjustedRecentUsage.inputTokens;
  const recentCacheReadTokens = recentUsage?.cache_read_tokens ?? 0;
  const recentTotalTokens = adjustedRecentUsage.totalTokens;
  const recentCacheRatio = recentInputTokens > 0 ? recentCacheReadTokens / recentInputTokens : 0;
  const recentRequests = recentUsage?.requests ?? 0;
  const recentWindowLabel = formatRecentWindowLabel(recentUsage?.window_seconds ?? 1800);
  const coldStartRequests = selectedProvider === "all"
    ? metrics?.usage.cold_start_requests ?? 0
    : usage?.cold_start_requests ?? traffic?.cold_start_requests ?? 0;
  const coldStartScopeLabel = selectedProvider === "all" ? "全部上游冷启动" : "当前上游冷启动";
  const recentColdStartRequests = recentUsage?.cold_start_requests ?? 0;
  const totalRequests = selectedProvider === "all"
    ? metrics?.total_requests ?? 0
    : traffic?.total_requests ?? selectedUsageRequests(usage) ?? 0;
  const cacheRatio = inputTokens > 0 ? cacheReadTokens / inputTokens : 0;
  const activeCacheRatio = recentInputTokens > 0 ? recentCacheRatio : cacheRatio;
  const successfulRequestFeed = metrics?.recent_upstream_calls ?? metrics?.recent_requests ?? [];
  const requestFeed: RequestFeedEntry[] = [
    ...successfulRequestFeed.map((request) => ({ ...request, feed_kind: "request" as const })),
    ...(metrics?.recent_failed_requests ?? []).map((request) => ({ ...request, feed_kind: "failed" as const }))
  ].sort((left, right) => Date.parse(right.at) - Date.parse(left.at));
  const shownRequests = requestFeed.filter((request) =>
    selectedProvider === "all" || request.provider === selectedProvider
  );
  const pageableRequests = shownRequests.slice(0, requestPageSize * maxRequestPages);
  const requestPageCount = Math.max(1, Math.ceil(pageableRequests.length / requestPageSize));
  const safeRequestPage = Math.min(requestPage, requestPageCount);
  const requestStart = pageableRequests.length ? (safeRequestPage - 1) * requestPageSize : 0;
  const requestEnd = Math.min(requestStart + requestPageSize, pageableRequests.length);
  const pageRequests = pageableRequests.slice(requestStart, requestEnd);
  const estimatedCost = estimateCost(totalTokens, cacheReadTokens);
  const smartCacheEnabled = Boolean(
    config?.cache.enabled &&
      config.cache.exact_enabled &&
      config.cache.semantic_enabled &&
      config.cache.prewarm_enabled &&
      config.cache.mode === "prefix-prewarm"
  );

  function toggleSmartCache() {
    if (!config) return;
    onSmartCacheChange({
      ...config.cache,
      mode: "prefix-prewarm",
      enabled: !smartCacheEnabled,
      exact_enabled: !smartCacheEnabled,
      semantic_enabled: !smartCacheEnabled,
      semantic_threshold: 0.985,
      prewarm_enabled: !smartCacheEnabled
    });
  }

  useEffect(() => {
    setRequestPage(1);
  }, [selectedProvider]);

  useEffect(() => {
    if (requestPage > requestPageCount) {
      setRequestPage(requestPageCount);
    }
  }, [requestPage, requestPageCount]);

  return (
    <div className="panel-grid cache-grid">
      <section className="surface">
        <div className="panel-head">
          <div>
            <h3>缓存策略</h3>
            <p>简单模式：保留主请求内的安全缓存优化，不再额外补发同步热补请求。</p>
          </div>
        </div>

        <div className={smartCacheEnabled ? "smart-cache-card active" : "smart-cache-card"}>
          <div className="smart-cache-icon">
            <Zap size={20} />
          </div>
          <div>
            <h4>智能最大命中</h4>
            <p>启用上游前缀缓存、稳定 cache key、请求体规范化和安全会话续接；不主动多发热补请求。</p>
          </div>
          <button
            className={smartCacheEnabled ? "smart-cache-toggle on" : "smart-cache-toggle"}
            onClick={toggleSmartCache}
            disabled={savingCachePolicy || !config}
            aria-pressed={smartCacheEnabled}
          >
            <span />
            <b>{savingCachePolicy ? "保存中" : smartCacheEnabled ? "已开启" : "已关闭"}</b>
          </button>
        </div>


        <div className="cold-start-control">
          <div>
            <h4>冷启动统计口径</h4>
            <p>默认计入冷启动，按原始上游累计计算；临时关闭只用于排查 warm 命中。</p>
          </div>
          <div className="cold-start-meta">
            <span>{coldStartScopeLabel}</span>
            <b>{formatNumber(coldStartRequests)} 次</b>
            <small>{recentWindowLabel} {formatNumber(recentColdStartRequests)} 次</small>
          </div>
          <button
            className={includeColdStarts ? "smart-cache-toggle on" : "smart-cache-toggle"}
            onClick={() => onIncludeColdStartsChange(!includeColdStarts)}
            aria-pressed={includeColdStarts}
          >
            <span />
            <b>{includeColdStarts ? "计入" : "排除"}</b>
          </button>
        </div>
      </section>

      <section className="surface usage-surface">
        <div className="panel-head compact">
          <div>
            <h3>使用统计</h3>
            <p>查看全部或单个中转的 tokens、成本和缓存命中。</p>
          </div>
        </div>

        <div className="usage-filter">
          <button
            className={selectedProvider === "all" ? "filter-tab active" : "filter-tab"}
            onClick={() => onSelectedProviderChange("all")}
          >
            全部
          </button>
          {providerOptions.map((provider) => (
            <button
              className={selectedProvider === provider ? "filter-tab active" : "filter-tab"}
              key={provider}
              onClick={() => onSelectedProviderChange(provider)}
            >
              {provider}
            </button>
          ))}
          <span className="refresh-hint">
            <RefreshCw size={15} />
            自动刷新
          </span>
          <button className="soft-button refresh-button" onClick={onRefresh}>
            <RefreshCw size={15} />
            刷新
          </button>
        </div>
        <div className="usage-ledger">
          <div className="usage-ledger-row">
            <UsageStatCard
              title="历史 Tokens"
              value={formatCompactTokens(totalTokens)}
              rows={[
                { label: "输入", value: formatCompactTokens(inputTokens), detail: percent(totalTokens > 0 ? inputTokens / totalTokens : 0) },
                { label: "输出", value: formatCompactTokens(outputTokens), detail: percent(totalTokens > 0 ? outputTokens / totalTokens : 0) },
                { label: "缓存", value: `读 ${formatCompactTokens(cacheReadTokens)} · 写 ${formatCompactTokens(cacheCreationTokens)}`, detail: `占输入 ${percent(cacheRatio)}` }
              ]}
            />
            <UsageStatCard
              title="历史请求数"
              value={formatNumber(totalRequests)}
              rows={[
                { label: "单次平均", value: formatCompactTokens(totalRequests ? Math.round(totalTokens / totalRequests) : 0), detail: "tokens" },
                { label: "平均输入", value: formatCompactTokens(totalRequests ? Math.round(inputTokens / totalRequests) : 0), detail: percent(inputTokens > 0 ? cacheRatio : 0) },
                { label: "平均输出", value: formatCompactTokens(totalRequests ? Math.round(outputTokens / totalRequests) : 0), detail: percent(totalTokens > 0 ? outputTokens / totalTokens : 0) },
                { label: "平均缓存", value: formatCompactTokens(totalRequests ? Math.round(cacheReadTokens / totalRequests) : 0), detail: `占输入 ${percent(cacheRatio)}` }
              ]}
            />
          </div>
          <div className="usage-ledger-row">
            <UsageStatCard
              title={`${recentWindowLabel} Tokens`}
              value={formatCompactTokens(recentTotalTokens)}
              rows={[
                { label: "输入", value: formatCompactTokens(recentInputTokens), detail: percent(recentTotalTokens > 0 ? recentInputTokens / recentTotalTokens : 0) },
                { label: "输出", value: formatCompactTokens(adjustedRecentUsage.outputTokens), detail: percent(recentTotalTokens > 0 ? adjustedRecentUsage.outputTokens / recentTotalTokens : 0) },
                { label: "缓存", value: `读 ${formatCompactTokens(recentCacheReadTokens)} · 写 ${formatCompactTokens(recentUsage?.cache_creation_tokens ?? 0)}`, detail: `占输入 ${percent(recentInputTokens > 0 ? recentCacheRatio : 0)}` }
              ]}
            />
            <UsageStatCard
              title={`${recentWindowLabel} 请求数`}
              value={formatNumber(recentRequests)}
              rows={[
                { label: "单次平均", value: formatCompactTokens(recentRequests ? Math.round(recentTotalTokens / recentRequests) : 0), detail: "tokens" },
                { label: "平均输入", value: formatCompactTokens(recentRequests ? Math.round(recentInputTokens / recentRequests) : 0), detail: percent(recentInputTokens > 0 ? recentCacheRatio : 0) },
                { label: "平均输出", value: formatCompactTokens(recentRequests ? Math.round(adjustedRecentUsage.outputTokens / recentRequests) : 0), detail: percent(recentTotalTokens > 0 ? adjustedRecentUsage.outputTokens / recentTotalTokens : 0) },
                { label: "平均缓存", value: formatCompactTokens(recentRequests ? Math.round(recentCacheReadTokens / recentRequests) : 0), detail: `占输入 ${percent(recentInputTokens > 0 ? recentCacheRatio : 0)}` }
              ]}
            />
          </div>
        </div>

        <div className="hit-meter">
          <div>
            <span>上游前缀缓存命中率（{recentWindowLabel}）</span>
            <b>{percent(activeCacheRatio)}</b>
          </div>
          <div className="meter-track">
            <span style={{ width: `${Math.min(100, Math.max(0, activeCacheRatio * 100))}%` }} />
          </div>
        </div>

        <div className="prefix-cache-strip">
          <Summary label="累计前缀命中率" value={percent(cacheRatio)} />
          <Summary label="创建缓存 token" value={cacheCreationTokens ? formatCompactTokens(cacheCreationTokens) : "0"} />
          <Summary label="累计上游命中" value={formatCompactTokens(cacheReadTokens)} />
        </div>

        <div className="request-log-panel">
          <div className="request-log-head">
            <h4>请求记录</h4>
            <span>
              {pageableRequests.length
                ? `显示第 ${requestStart + 1} 条 - 第 ${requestEnd} 条，共 ${pageableRequests.length} 条`
                : "暂无记录"}
            </span>
          </div>
          <div className="request-feed">
            {pageRequests.map((request) => {
              const cacheDisplay = providerBucketDisplay(
                request.input_tokens ?? 0,
                request.cache_read_tokens ?? 0,
                request.provider_cache_token_ratio ?? 0,
                request.cache_shortfall_tokens ?? 0,
                request.cache_new_tail_gap_tokens ?? 0,
                request.cache_avoidable_gap_tokens ?? 0,
                request.cache_provider_unstable_gap_tokens ?? 0
              );
              const primaryStatus = requestPrimaryStatus(
                request,
                request.input_tokens ?? 0,
                request.cache_read_tokens ?? 0
              );
              const cacheStatusLabel = ["miss", "compact", "error"].includes(request.cache_status)
                ? ""
                : request.cache_status;
              const cacheResultLabel = [primaryStatus, cacheStatusLabel, cacheDisplay.primary]
                .filter(Boolean)
                .join(" · ");
              const inputTokens = request.input_tokens ?? 0;
              const outputTokens = request.output_tokens ?? 0;
              const cacheReadTokens = request.cache_read_tokens ?? 0;
              const isFailedRequest =
                request.feed_kind === "failed" || request.status >= 400 || request.cache_status === "error";
              const requestedModel = request.requested_model?.trim();
              const hasModelMapping = Boolean(requestedModel && requestedModel !== request.model);
              const displayModel = hasModelMapping ? requestedModel : request.model;
              const metricTitle = [
                `首字 ${formatDurationMs(request.ttft_ms)}`,
                `用时 ${formatDurationMs(request.total_ms)}`,
                `输入 ${formatNumber(inputTokens)}`,
                `输出 ${formatNumber(outputTokens)}`,
                `缓存命中 ${formatNumber(cacheReadTokens)}`,
                request.inbound_request_id ? `入站 ${shortTraceId(request.inbound_request_id)}` : "",
                request.upstream_request_id ? `上游 ${shortTraceId(request.upstream_request_id)}` : "",
                request.upstream_attempt_total ? `尝试 ${request.upstream_attempt_index ?? 1}/${request.upstream_attempt_total}` : ""
              ].filter(Boolean).join(" · ");
              return (
                <div className={isFailedRequest ? "request-row failed" : "request-row"} key={`${request.feed_kind}-${request.id}`}>
                  <div className="request-identity">
                    {isFailedRequest ? <AlertTriangle size={14} /> : <Activity size={14} />}
                    <div>
                      <time>{formatRequestTime(request.at)}</time>
                      <span className="request-provider-text">{request.provider} · {displayModel}</span>
                      {hasModelMapping ? (
                        <span className="request-model-map" title={`实际发送到上游：${request.model}`}>
                          ↳ {request.model}
                        </span>
                      ) : null}
                    </div>
                  </div>
                  <div className="request-badges">
                    <span className={`request-call-badge ${requestCallKindClass(request.upstream_call_kind)}`}>
                      {requestCallKindLabel(request.upstream_call_kind, request.upstream_call_source)}
                    </span>
                    <span className="request-channel-badge">
                      {requestChannelLabel(request.client_channel, request.upstream_channel)}
                    </span>
                    {request.agent_id ? (
                      <span className="request-agent-badge" title={request.agent_label ?? request.agent_id}>
                        {request.agent_id}
                      </span>
                    ) : null}
                  </div>
                  <div className="request-metrics" title={metricTitle}>
                    <strong>首字 {formatDurationMs(request.ttft_ms)} · 用时 {formatDurationMs(request.total_ms)}</strong>
                    <em>输入 {formatCompactTokens(inputTokens)} · 输出 {formatCompactTokens(outputTokens)} · 命中 {formatCompactTokens(cacheReadTokens)}</em>
                  </div>
                  <div className="request-cache-result">
                    {cacheResultLabel ? <b>{cacheResultLabel}</b> : null}
                    {cacheDisplay.secondary ? (
                      <small title={cacheDisplay.secondaryTitle ?? cacheDisplay.secondary}>
                        {cacheDisplay.secondary}
                      </small>
                    ) : null}
                  </div>
                </div>
              );
            })}
            {!pageableRequests.length && <div className="empty-mini">等待第一条代理请求。</div>}
          </div>
          {pageableRequests.length > requestPageSize && (
            <div className="request-pager">
              <span>总页数：{requestPageCount}</span>
              <button
                className="icon-button"
                onClick={() => setRequestPage((page) => Math.max(1, page - 1))}
                disabled={safeRequestPage === 1}
                title="上一页"
              >
                <ChevronDown className="pager-prev-icon" size={16} />
              </button>
              {visiblePages(safeRequestPage, requestPageCount).map((page, index) =>
                page === "ellipsis" ? (
                  <span className="pager-ellipsis" key={`ellipsis-${index}`}>...</span>
                ) : (
                  <button
                    className={safeRequestPage === page ? "pager-button active" : "pager-button"}
                    key={page}
                    onClick={() => setRequestPage(page)}
                  >
                    {page}
                  </button>
                )
              )}
              <button
                className="icon-button"
                onClick={() => setRequestPage((page) => Math.min(requestPageCount, page + 1))}
                disabled={safeRequestPage === requestPageCount}
                title="下一页"
              >
                <ChevronDown className="pager-next-icon" size={16} />
              </button>
              <span>每页 20 条</span>
            </div>
          )}
        </div>

        <div className="provider-stats">
          <h4>上游流量统计</h4>
          {(metrics?.provider_stats ?? []).map((item) => (
            <div className="provider-stat-row" key={item.provider}>
              <div>
                <b>{item.provider}</b>
                <span>
                  本地请求 {item.total_requests} · 转发上游 {item.upstream_requests} · 本地未命中 {item.cache_misses} · 绕过 {item.bypassed}
                </span>
              </div>
              <div>
                <b>{percent(item.recent_usage.cache_token_ratio)}</b>
                <span>上游前缀命中 · 完整复用 {percent(item.cache_hit_rate)} · TTFT {item.ttft_p95_ms}ms</span>
              </div>
            </div>
          ))}
          {!metrics?.provider_stats?.length && <div className="empty-mini">还没有上游流量统计。</div>}
        </div>
      </section>
    </div>
  );
}

function ProviderTab({ provider, selected, onSelect }: { provider: ProviderConfig; selected: boolean; onSelect: () => void }) {
  return (
    <button className={selected ? "provider-tab active" : "provider-tab"} onClick={onSelect}>
      <span className="provider-glyph">{provider.name.slice(0, 1).toUpperCase()}</span>
      <span>
        <b>{provider.name}</b>
        <small>{channelLabel(provider.channel)} · {provider.models.length} 模型</small>
      </span>
      {selected ? <Check size={16} /> : <span className={provider.enabled ? "state-dot" : "state-dot muted"} />}
    </button>
  );
}

function Field({ label, wide, children }: { label: string; wide?: boolean; children: ReactNode }) {
  return (
    <label className={wide ? "field wide" : "field"}>
      <span>{label}</span>
      {children}
    </label>
  );
}

function SelectShell({ children, disabled }: { children: ReactNode; disabled?: boolean }) {
  return (
    <div className={disabled ? "select-shell disabled" : "select-shell"}>
      {children}
      <ChevronDown size={16} />
    </div>
  );
}

function Summary({ label, value, tone }: { label: string; value: string; tone?: "red" }) {
  return (
    <div className={tone === "red" ? "summary-card red" : "summary-card"}>
      <span>{label}</span>
      <b>{value}</b>
    </div>
  );
}

function UsageStatCard({
  title,
  value,
  rows
}: {
  title: string;
  value: string;
  rows: Array<{ label: string; value: string; detail?: string }>;
}) {
  return (
    <div className="usage-stat-card">
      <div className="usage-stat-head">
        <span>{title}</span>
      </div>
      <strong>{value}</strong>
      <div className="usage-stat-rule" />
      <div className="usage-stat-rows">
        {rows.map((row) => (
          <div className="usage-stat-row" key={row.label}>
            <span>{row.label}</span>
            <p>
              <b>{row.value}</b>
              {row.detail ? <em>{row.detail}</em> : null}
            </p>
          </div>
        ))}
      </div>
    </div>
  );
}

function MetricTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="metric-tile">
      <span>{label}</span>
      <b>{value}</b>
    </div>
  );
}

function agentIcon(kind: AgentInjectionConfig["kind"]) {
  if (kind === "claude-code") return <TerminalSquare size={18} />;
  if (kind === "claude-desktop") return <Bot size={18} />;
  if (kind === "codex") return <BrainCircuit size={18} />;
  if (kind === "gemini") return <Sparkles size={18} />;
  if (kind === "open-code") return <Code2 size={18} />;
  if (kind === "open-claw") return <Workflow size={18} />;
  if (kind === "hermes") return <Zap size={18} />;
  if (kind === "proxy-mode") return <Network size={18} />;
  return <Workflow size={18} />;
}

function visibleAgentInjections(items: AgentInjectionConfig[]): AgentInjectionConfig[] {
  return items;
}

function proxyAddressConflicts(leftHost: string, leftPort: number, rightHost: string, rightPort: number): boolean {
  if (!Number.isInteger(leftPort) || leftPort !== rightPort) return false;
  const left = normalizeProxyHost(leftHost);
  const right = normalizeProxyHost(rightHost);
  return left === right || left === "0.0.0.0" || right === "0.0.0.0";
}

function normalizeProxyHost(host: string): string {
  const normalized = host.trim().toLowerCase();
  return normalized === "localhost" ? "127.0.0.1" : normalized;
}

function shortTraceId(id: string): string {
  return id.length > 8 ? id.slice(0, 8) : id;
}

function providersForAgentEditor(
  providers: ProviderConfig[],
  agent: AgentInjectionConfig
): ProviderConfig[] {
  const hiddenProviderIds = new Set(agent.hidden_provider_ids ?? []);
  const ownedProviders = providers.filter((provider) =>
    providerBelongsToAgent(provider.id, agent.id)
  );
  const sharedProviders = providers.filter((provider) => !provider.id.startsWith("agent-"));
  const clonedSourceIds = new Set(
    ownedProviders
      .map((privateProvider) =>
        sharedProviders
          .sort((left, right) => right.id.length - left.id.length)
          .find((sharedProvider) =>
            providerCloneMatchesSource(privateProvider.id, sharedProvider.id, agent.id)
          )?.id
      )
      .filter((id): id is string => Boolean(id))
  );

  return providers.filter((provider) => {
    if (provider.id.startsWith("agent-")) {
      return providerBelongsToAgent(provider.id, agent.id);
    }
    return !clonedSourceIds.has(provider.id) && !hiddenProviderIds.has(provider.id);
  });
}

function providerBelongsToAgent(providerId: string, agentId: string): boolean {
  return providerId.startsWith(`agent-${providerIdPart(agentId)}-`);
}

function providerCloneMatchesSource(
  providerId: string,
  sourceProviderId: string,
  agentId: string
): boolean {
  const base = `agent-${providerIdPart(agentId)}-${providerIdPart(sourceProviderId)}`;
  if (providerId === base) return true;
  const suffix = providerId.slice(base.length + 1);
  return providerId.startsWith(`${base}-`) && /^\d+$/.test(suffix);
}

function providerIdPart(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "") || "provider";
}

function providerToDraft(provider: ProviderConfig): ProviderDraft {
  return {
    id: provider.id,
    name: provider.name,
    base_url: provider.base_url,
    models_url: provider.models_url ?? "",
    is_full_url: provider.is_full_url,
    custom_user_agent: provider.custom_user_agent ?? "",
    api_key: "",
    channel_mode: provider.channel_mode ?? "auto",
    channel: provider.channel,
    prompt_cache_retention_enabled: provider.prompt_cache_retention_enabled,
    request_body_gzip_enabled: provider.request_body_gzip_enabled,
    use_system_proxy: provider.use_system_proxy ?? false,
    non_sse_compact_compat_enabled: provider.non_sse_compact_compat_enabled ?? false,
    key_pool: providerKeyPoolToDraft(provider.key_pool),
    models: provider.models,
    enabled: provider.enabled
  };
}

function providerKeyPoolToDraft(pool?: ProviderConfig["key_pool"] | null): ProviderKeyPoolDraft {
  return {
    enabled: pool?.enabled ?? false,
    strategy: pool?.strategy ?? "round-robin",
    failure_threshold: pool?.failure_threshold ?? 3,
    recovery_minutes: pool?.recovery_minutes ?? 5,
    keys: (pool?.keys ?? []).map((key) => ({
      id: key.id,
      alias: key.alias ?? "",
      key: "",
      preview: key.preview,
      enabled: key.enabled,
      priority: key.priority,
      status: key.status,
      total_requests: key.total_requests,
      successes: key.successes,
      failures: key.failures,
      last_checked_at: key.last_checked_at,
      last_error: key.last_error,
      disabled_until: key.disabled_until
    }))
  };
}

function providerKeyPoolInput(pool: ProviderKeyPoolDraft) {
  return {
    enabled: pool.enabled,
    strategy: pool.strategy,
    failure_threshold: Math.max(1, Number(pool.failure_threshold) || 3),
    recovery_minutes: Math.max(1, Number(pool.recovery_minutes) || 5),
    keys: pool.keys.map((key) => ({
      id: key.id,
      alias: key.alias.trim() || null,
      key: key.key.trim() || null,
      enabled: key.enabled,
      priority: Math.max(0, Number(key.priority) || 0),
      status: key.status,
      total_requests: key.total_requests,
      successes: key.successes,
      failures: key.failures,
      last_checked_at: key.last_checked_at ?? null,
      last_error: key.last_error ?? null,
      disabled_until: key.disabled_until ?? null
    }))
  };
}

function newProviderKeyDraft(key: string): ProviderKeyDraft {
  const value = key.trim();
  return {
    id: "key_" + Date.now() + "_" + Math.random().toString(16).slice(2),
    alias: "",
    key: value,
    preview: maskKey(value),
    enabled: true,
    priority: 5,
    status: "unknown",
    total_requests: 0,
    successes: 0,
    failures: 0,
    last_checked_at: null,
    last_error: null,
    disabled_until: null
  };
}

function parseBatchKeys(value: string) {
  return value
    .split(/[\s,;，；]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function maskKey(key: string) {
  if (key.length <= 12) return key ? key.slice(0, 4) + "..." : "";
  return key.slice(0, 6) + "..." + key.slice(-4);
}

function keyStatusLabel(status: ProviderKeyStatus) {
  if (status === "healthy") return "可用";
  if (status === "unhealthy") return "不可用";
  return "未知";
}


function normalizeModels(models: ModelConfig[]) {
  const byId = new Map<string, ModelConfig>();
  for (const item of models) {
    const id = item.id.trim();
    if (!id) continue;
    const requestModelId = cleanOptionalText(item.request_model_id);
    byId.set(id, {
      ...item,
      id,
      request_model_id: requestModelId && requestModelId !== id ? requestModelId : null,
      display_name: (item.display_name || id).trim() || id,
      reasoning_effort_override_enabled: Boolean(item.reasoning_effort_override_enabled),
      reasoning_effort: cleanOptionalText(item.reasoning_effort),
      supported_reasoning_efforts: item.supported_reasoning_efforts ?? []
    });
  }
  return [...byId.values()];
}

function cleanOptionalText(value?: string | null) {
  const text = (value ?? "").trim();
  return text || null;
}

function nextManualModelId(models: ModelConfig[]) {
  let index = models.length + 1;
  let id = "new-model";
  while (models.some((item) => item.id === id)) {
    id = `new-model-${index}`;
    index += 1;
  }
  return id;
}

function draftHasInput(draft: ProviderDraft) {
  return Boolean(
    draft.id ||
      draft.name.trim() ||
      draft.base_url.trim() ||
      draft.models_url.trim() ||
      draft.custom_user_agent.trim() ||
      draft.api_key.trim() ||
      draft.models.length
  );
}

function percent(value?: number) {
  return `${((value ?? 0) * 100).toFixed(1)}%`;
}

function requestPrimaryStatus(
  request: MetricsSnapshot["recent_requests"][number],
  inputTokens: number,
  cacheReadTokens: number
) {
  if (request.status >= 400 || request.cache_status === "error") {
    return request.status ? `上游失败 ${request.status}` : "上游异常";
  }
  if (request.downstream_disconnected) {
    return "下游已断开";
  }
  if (request.cache_status === "compact") {
    return "实际压缩";
  }
  if (
    request.response_session_reused === false &&
    request.response_session_skip_reason &&
    request.response_session_skip_reason !== "compact_non_streaming"
  ) {
    return `会话复用失败：${request.response_session_skip_reason}`;
  }
  if (inputTokens >= 1024 && cacheReadTokens === 0) {
    return "冷启动";
  }
  return "";
}

function providerBucketDisplay(
  inputTokens: number,
  cachedTokens: number,
  rawRatio: number,
  _shortfallTokens: number,
  newTailGapTokens = 0,
  avoidableGapTokens = 0,
  providerUnstableGapTokens = 0
) {
  if (!inputTokens) return { primary: "", secondary: "" };
  const tokenSummary = `${formatCompactTokens(cachedTokens)} / ${formatCompactTokens(inputTokens)}`;
  const bucketMax = Math.floor(inputTokens / 128) * 128;
  const bucketGap = Math.max(bucketMax - cachedTokens, 0);
  if (!cachedTokens) {
    const gapText = bucketGap ? ` · 缺口 ${formatCompactTokens(bucketGap)}` : "";
    return {
      primary: "冷启动",
      secondary: `${tokenSummary}${gapText}`,
      secondaryTitle: `${tokenSummary}${gapText}`
    };
  }
  const realRatio = inputTokens > 0 ? cachedTokens / inputTokens : rawRatio;
  const primary = percent(realRatio);
  if (bucketMax > 0 && bucketGap === 0) {
    return {
      primary,
      secondary: `${tokenSummary} · 满桶`,
      secondaryTitle: `${tokenSummary} · 满桶`
    };
  }
  const gapDisplay = providerGapDisplay(
    tokenSummary,
    bucketGap,
    newTailGapTokens,
    avoidableGapTokens,
    providerUnstableGapTokens
  );
  return {
    primary,
    secondary: gapDisplay.compact,
    secondaryTitle: gapDisplay.full
  };
}

function providerGapDisplay(
  tokenSummary: string,
  totalGapTokens: number,
  newTailGapTokens: number,
  avoidableGapTokens: number,
  providerUnstableGapTokens: number
) {
  const total = formatCompactTokens(totalGapTokens);
  const avoidable = formatCompactTokens(avoidableGapTokens);
  const newTail = formatCompactTokens(newTailGapTokens);
  const unstable = formatCompactTokens(providerUnstableGapTokens);
  if (providerUnstableGapTokens > 0) {
    const details = [
      avoidableGapTokens > 0 ? `可避免 ${avoidable}` : "",
      providerUnstableGapTokens > 0 ? `上游缓存水线下降 ${unstable}` : "",
      newTailGapTokens > 0 ? `新尾巴 ${newTail}` : ""
    ].filter(Boolean);
    return {
      compact: `${tokenSummary} · 缺 ${total} · 水线下降 ${unstable}${newTailGapTokens > 0 ? ` · 新 ${newTail}` : ""}`,
      full: `${tokenSummary} · 总缺口 ${total}（${details.join(" / ")}）`
    };
  }
  if (avoidableGapTokens > 0 && newTailGapTokens > 0) {
    return {
      compact: `${tokenSummary} · 缺 ${total} · 可 ${avoidable} · 新 ${newTail}`,
      full: `${tokenSummary} · 总缺口 ${total}（可避免 ${avoidable} / 新尾巴 ${newTail}）`
    };
  }
  if (avoidableGapTokens > 0) {
    return {
      compact: `${tokenSummary} · 可 ${avoidable}`,
      full: `${tokenSummary} · 可避免缺口 ${avoidable}`
    };
  }
  if (newTailGapTokens > 0) {
    return {
      compact: `${tokenSummary} · 新 ${newTail}`,
      full: `${tokenSummary} · 新尾巴 ${newTail}`
    };
  }
  return {
    compact: `${tokenSummary} · 缺 ${total}`,
    full: `${tokenSummary} · 缺口 ${total}`
  };
}

function channelLabel(channel: Channel) {
  return channelOptions.find((option) => option.value === channel)?.label ?? channel;
}

function providerModelMappingLabel(provider: ProviderConfig) {
  return provider.models.length ? `${provider.models.length} 个映射` : "直接透传";
}

function requestChannelLabel(clientChannel?: string | null, upstreamChannel?: string | null) {
  const client = compactChannelLabel(clientChannel);
  const upstream = compactChannelLabel(upstreamChannel);
  if (client && upstream && client !== upstream) {
    return `${client} -> ${upstream}`;
  }
  return upstream || client || "Unknown";
}

function requestCallKindLabel(kind?: string | null, source?: string | null) {
  if (kind === "stream") return "流式";
  if (kind === "sync") return "同步";
  if (kind === "prewarm-sync") {
    if (source === "foreground_prewarm") return "前台补热";
    if (source === "background_bucket_prewarm") return "桶补热";
    return "补热同步";
  }
  if (kind === "cache") return "本地";
  return "同步";
}

function requestCallKindClass(kind?: string | null) {
  if (kind === "stream") return "stream";
  if (kind === "prewarm-sync") return "prewarm";
  if (kind === "cache") return "cache";
  return "sync";
}

function compactChannelLabel(channel?: string | null) {
  if (channel === "responses") return "Responses";
  if (channel === "chat") return "Chat";
  if (channel === "anthropic") return "Anthropic";
  return channel || "";
}

function formatRecentWindowLabel(seconds: number) {
  const minutes = Math.max(1, Math.round(seconds / 60));
  return `近 ${minutes} 分钟`;
}

function formatContextInput(value?: number | null) {
  if (!value) return "";
  if (value >= 10000) return `${Math.round(value / 10000)}万`;
  return String(value);
}

function formatRawContextInput(value?: number | null) {
  if (!value) return "";
  return String(Math.round(value));
}

function parseContext(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (trimmed.endsWith("万")) {
    const number = Number(trimmed.slice(0, -1));
    return Number.isFinite(number) ? Math.round(number * 10000) : null;
  }
  const number = Number(trimmed.replace(/,/g, ""));
  return Number.isFinite(number) ? number : null;
}

function formatNumber(value: number) {
  return Math.round(value).toLocaleString("zh-CN");
}

function formatCompactTokens(value: number) {
  if (!value) return "0";
  if (value >= 100_000_000) return `${trimNumber(value / 100_000_000)} 亿`;
  if (value >= 10_000) return `${trimNumber(value / 10_000)} 万`;
  return formatNumber(value);
}

function formatRequestTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--:--:--";
  return date.toLocaleTimeString("zh-CN", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  });
}

function formatDurationMs(value?: number | null) {
  const ms = Math.max(0, Math.round(value ?? 0));
  if (ms >= 60_000) return `${trimNumber(ms / 60_000)}m`;
  if (ms >= 10_000) return `${Math.round(ms / 1000)}s`;
  if (ms >= 1000) return `${trimNumber(ms / 1000)}s`;
  return `${ms}ms`;
}

function visiblePages(current: number, total: number): Array<number | "ellipsis"> {
  if (total <= 7) {
    return Array.from({ length: total }, (_, index) => index + 1);
  }
  const pages = new Set<number>([1, 2, total - 1, total, current - 1, current, current + 1]);
  const normalized = [...pages]
    .filter((page) => page >= 1 && page <= total)
    .sort((left, right) => left - right);
  const result: Array<number | "ellipsis"> = [];
  for (const page of normalized) {
    const previous = result[result.length - 1];
    if (typeof previous === "number" && page - previous > 1) {
      result.push("ellipsis");
    }
    result.push(page);
  }
  return result;
}

function trimNumber(value: number) {
  return value.toFixed(value >= 100 ? 0 : value >= 10 ? 1 : 2).replace(/\.0+$/, "");
}

function estimateCost(totalTokens: number, cacheReadTokens: number) {
  const billableTokens = Math.max(totalTokens - cacheReadTokens * 0.75, 0);
  return `$${(billableTokens / 1_000_000 * 1.2).toFixed(4)}`;
}

function selectedUsageRequests(usage?: MetricsSnapshot["usage"] | MetricsSnapshot["usage"]["by_provider"][number]) {
  return usage && "requests" in usage ? usage.requests : 0;
}

type ColdAdjustableUsage = {
  input_tokens?: number;
  output_tokens?: number;
  total_tokens?: number;
  cold_start_input_tokens?: number;
  cold_start_output_tokens?: number;
} | null | undefined;

function coldAdjustedUsage(usage: ColdAdjustableUsage, includeColdStarts: boolean) {
  const input = usage?.input_tokens ?? 0;
  const output = usage?.output_tokens ?? 0;
  if (includeColdStarts) {
    return {
      inputTokens: input,
      outputTokens: output,
      totalTokens: usage?.total_tokens ?? input + output
    };
  }
  const coldInput = usage?.cold_start_input_tokens ?? 0;
  const coldOutput = usage?.cold_start_output_tokens ?? 0;
  const inputTokens = Math.max(0, input - coldInput);
  const outputTokens = Math.max(0, output - coldOutput);
  return {
    inputTokens,
    outputTokens,
    totalTokens: inputTokens + outputTokens
  };
}
