import { invoke } from "@tauri-apps/api/core";

export type Channel = "chat" | "responses" | "anthropic";
export type CacheMode = "passive-warm" | "session-prewarm" | "prefix-prewarm";
export type AgentInjectionKind = "claude-code" | "codex" | "claude-desktop" | "proxy-mode";

export interface ModelConfig {
  id: string;
  display_name: string;
  context_window?: number | null;
  output_window?: number | null;
  supports_tools: boolean;
  supports_streaming: boolean;
  enabled: boolean;
}

export interface ProviderConfig {
  id: string;
  name: string;
  base_url: string;
  models_url?: string | null;
  is_full_url: boolean;
  custom_user_agent?: string | null;
  channel: Channel;
  prompt_cache_retention_enabled: boolean;
  request_body_gzip_enabled: boolean;
  has_api_key: boolean;
  models: ModelConfig[];
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface AppConfig {
  host: string;
  port: number;
  proxy_auto_start: boolean;
  local_key: string;
  default_channel: Channel;
  active_provider_id?: string | null;
  workspace_fingerprint: string;
  providers: ProviderConfig[];
  route_profiles?: Array<{
    name: string;
    client_channel: Channel;
    upstream_channel: Channel;
    provider_id?: string | null;
    model_alias?: string | null;
    long_context_threshold: number;
  }>;
  cache: {
    mode: CacheMode;
    enabled: boolean;
    exact_enabled: boolean;
    semantic_enabled: boolean;
    semantic_threshold: number;
    max_age_seconds: number;
    max_entries: number;
    persist_encrypted: boolean;
    prewarm_enabled: boolean;
    background_prewarm_enabled: boolean;
  };
  agent_injections: AgentInjectionConfig[];
  updated_at: string;
  config_path: string;
}

export interface AgentInjectionConfig {
  id: string;
  label: string;
  kind: AgentInjectionKind;
  enabled: boolean;
  provider_id?: string | null;
  model_id?: string | null;
  target_path?: string | null;
  last_injected_at?: string | null;
  last_status?: string | null;
}

export interface AgentInjectionResult {
  id: string;
  label: string;
  enabled: boolean;
  target_path?: string | null;
  backup_path?: string | null;
  status: string;
  injected_at: string;
}

export interface ProxyStatus {
  running: boolean;
  address?: string | null;
}

export interface MetricsSnapshot {
  started_at?: string;
  total_requests: number;
  upstream_requests: number;
  response_cache_hits: number;
  semantic_cache_hits: number;
  cache_misses: number;
  errors: number;
  retries: number;
  ttft_p95_ms: number;
  total_p95_ms: number;
  provider_cached_tokens?: number;
  provider_input_tokens?: number;
  provider_cache_hit_requests?: number;
  provider_cache_token_ratio: number;
  provider_cache_request_hit_rate?: number;
  combined_cache_hit_rate?: number;
  recent_usage: RecentUsageStats;
  eligible_cache_lookups: number;
  eligible_cache_hits: number;
  first_seen_eligible_misses: number;
  repeatable_eligible_lookups: number;
  repeatable_eligible_hits: number;
  overall_eligible_cache_hit_rate: number;
  repeatable_eligible_cache_hit_rate: number;
  eligible_cache_hit_rate: number;
  usage: UsageSnapshot;
  local_proxy: LocalProxyStats;
  background_prewarm?: BackgroundPrewarmStats[];
  gap_buckets?: GapBucketStats[];
  request_body_buckets?: RequestBodyBucketStats[];
  provider_stats: ProviderTrafficStats[];
  recent_requests: Array<{
    id: string;
    at: string;
    client_channel: string;
    upstream_channel: string;
    provider: string;
    model: string;
    cache_status: string;
    upstream_call_kind?: "stream" | "sync" | "prewarm-sync" | "cache" | string | null;
    upstream_call_source?: string | null;
    status: number;
    ttft_ms: number;
    upstream_ttft_ms?: number | null;
    local_prepare_ms?: number | null;
    upstream_headers_ms?: number | null;
    upstream_first_chunk_ms?: number | null;
    upstream_retry_wait_ms?: number | null;
    upstream_attempts?: number | null;
    request_body_bytes?: number | null;
    sent_body_bytes?: number | null;
    gzip_attempted?: boolean | null;
    gzip_fallback_used?: boolean | null;
    upstream_header_wait_class?: string | null;
    prefix_cache_instability_score?: number | null;
    prefix_seen_bucket_tokens?: number | null;
    prefix_state_cache_read_tokens?: number | null;
    total_ms: number;
    input_tokens?: number | null;
    output_tokens?: number | null;
    cache_read_tokens?: number | null;
    cache_shortfall_tokens?: number | null;
    cache_new_tail_gap_tokens?: number | null;
    cache_avoidable_gap_tokens?: number | null;
    provider_cache_token_ratio?: number | null;
    response_session_reused?: boolean | null;
    session_anchor_hash?: string | null;
    session_anchor_source?: string | null;
    session_anchor_changed?: boolean | null;
    session_anchor_peer_count?: number | null;
  }>;
  recent_errors?: Array<{
    at: string;
    scope: string;
    message: string;
  }>;
}

export interface ProviderTrafficStats {
  provider: string;
  total_requests: number;
  upstream_requests: number;
  cache_hits: number;
  exact_hits: number;
  semantic_hits: number;
  cache_misses: number;
  bypassed: number;
  error_statuses: number;
  cold_start_requests?: number;
  cold_start_input_tokens?: number;
  cold_start_output_tokens?: number;
  cold_start_total_tokens?: number;
  ttft_p95_ms: number;
  total_p95_ms: number;
  cache_hit_rate: number;
  recent_usage: RecentUsageStats;
  gap_buckets?: GapBucketStats[];
  request_body_buckets?: RequestBodyBucketStats[];
}

export interface GapBucketStats {
  bucket: string;
  requests: number;
  total_gap_tokens: number;
  new_tail_gap_tokens: number;
  avoidable_gap_tokens: number;
}

export interface BackgroundPrewarmStats {
  channel: string;
  attempts: number;
  successes: number;
  trigger_new_tail_tokens: number;
  trigger_avoidable_tokens: number;
  input_tokens: number;
  cache_read_tokens: number;
}

export interface UsageSnapshot {
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_creation_tokens: number;
  total_tokens: number;
  cold_start_requests?: number;
  cold_start_input_tokens?: number;
  cold_start_output_tokens?: number;
  cold_start_total_tokens?: number;
  by_provider: UsageGroup[];
  by_model: UsageGroup[];
}

export interface UsageGroup {
  key: string;
  requests: number;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_creation_tokens: number;
  total_tokens: number;
  cold_start_requests?: number;
  cold_start_input_tokens?: number;
  cold_start_output_tokens?: number;
  cold_start_total_tokens?: number;
}

export interface RecentUsageStats {
  window_seconds: number;
  requests: number;
  cache_hit_requests?: number;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_creation_tokens: number;
  cold_start_requests?: number;
  cold_start_input_tokens?: number;
  cold_start_output_tokens?: number;
  cold_start_total_tokens?: number;
  cache_token_ratio: number;
  cache_request_hit_rate?: number;
}

export interface LocalProxyStats {
  local_cache_hits: number;
  upstream_requests_saved: number;
  estimated_tokens_saved: number;
  exact_hits: number;
  semantic_hits: number;
  eligible_lookups: number;
  eligible_hits: number;
  first_seen_eligible_misses: number;
  repeatable_eligible_lookups: number;
  repeatable_eligible_hits: number;
  overall_hit_rate: number;
  repeatable_hit_rate: number;
}

export interface RequestBodyBucketStats {
  bucket: string;
  risk: string;
  requests: number;
  total_bytes: number;
  max_bytes: number;
}

export interface ProviderInput {
  id?: string;
  name: string;
  base_url: string;
  models_url?: string;
  is_full_url: boolean;
  custom_user_agent?: string;
  channel: Channel;
  prompt_cache_retention_enabled: boolean;
  request_body_gzip_enabled: boolean;
  api_key?: string;
  enabled: boolean;
}

export interface FetchModelsInput {
  provider_id?: string;
  name?: string;
  base_url: string;
  models_url?: string;
  is_full_url: boolean;
  custom_user_agent?: string;
  channel: Channel;
  api_key?: string;
}

export interface GeneralConfigInput {
  host: string;
  port: number;
  local_key: string;
  default_channel: Channel;
  workspace_fingerprint: string;
  cache: AppConfig["cache"];
}

let fallbackConfig: AppConfig = {
  host: "127.0.0.1",
  port: 18883,
  proxy_auto_start: true,
  local_key: "ato-local-preview",
  default_channel: "anthropic",
  active_provider_id: null,
  workspace_fingerprint: "default-workspace",
  providers: [],
  route_profiles: [
    routeProfile("anthropic", "anthropic", null),
    routeProfile("chat", "chat", null),
    routeProfile("responses", "responses", null)
  ],
  cache: {
    mode: "prefix-prewarm",
    enabled: true,
    exact_enabled: true,
    semantic_enabled: true,
    semantic_threshold: 0.985,
    max_age_seconds: 86400,
    max_entries: 300000,
    persist_encrypted: true,
    prewarm_enabled: true,
    background_prewarm_enabled: false
  },
  agent_injections: [
    injection("claude-code", "Claude Code", "claude-code"),
    injection("codex", "Codex", "codex"),
    injection("claude-desktop", "Claude Desktop", "claude-desktop"),
    injection("proxy-mode", "代理模式", "proxy-mode")
  ],
  updated_at: new Date().toISOString(),
  config_path: "%APPDATA%/Atoapi/config.toml"
};

const emptyRecentUsage: RecentUsageStats = {
  window_seconds: 300,
  requests: 0,
  cache_hit_requests: 0,
  input_tokens: 0,
  output_tokens: 0,
  cache_read_tokens: 0,
  cache_creation_tokens: 0,
  cold_start_requests: 0,
  cold_start_input_tokens: 0,
  cold_start_output_tokens: 0,
  cold_start_total_tokens: 0,
  cache_token_ratio: 0,
  cache_request_hit_rate: 0
};

const fallbackMetrics: MetricsSnapshot = {
  total_requests: 0,
  upstream_requests: 0,
  response_cache_hits: 0,
  semantic_cache_hits: 0,
  cache_misses: 0,
  errors: 0,
  retries: 0,
  ttft_p95_ms: 0,
  total_p95_ms: 0,
  provider_cached_tokens: 0,
  provider_input_tokens: 0,
  provider_cache_hit_requests: 0,
  provider_cache_token_ratio: 0,
  provider_cache_request_hit_rate: 0,
  combined_cache_hit_rate: 0,
  recent_usage: emptyRecentUsage,
  eligible_cache_lookups: 0,
  eligible_cache_hits: 0,
  first_seen_eligible_misses: 0,
  repeatable_eligible_lookups: 0,
  repeatable_eligible_hits: 0,
  overall_eligible_cache_hit_rate: 0,
  repeatable_eligible_cache_hit_rate: 0,
  eligible_cache_hit_rate: 0,
  usage: {
    input_tokens: 0,
    output_tokens: 0,
    cache_read_tokens: 0,
    cache_creation_tokens: 0,
    total_tokens: 0,
    cold_start_requests: 0,
    cold_start_input_tokens: 0,
    cold_start_output_tokens: 0,
    cold_start_total_tokens: 0,
    by_provider: [],
    by_model: []
  },
  local_proxy: {
    local_cache_hits: 0,
    upstream_requests_saved: 0,
    estimated_tokens_saved: 0,
    exact_hits: 0,
    semantic_hits: 0,
    eligible_lookups: 0,
    eligible_hits: 0,
    first_seen_eligible_misses: 0,
    repeatable_eligible_lookups: 0,
    repeatable_eligible_hits: 0,
    overall_hit_rate: 0,
    repeatable_hit_rate: 0
  },
  gap_buckets: [],
  request_body_buckets: [],
  provider_stats: [],
  recent_requests: []
};

const fallbackProviderSecrets = new Map<string, string>();

export async function command<T>(name: string, args?: Record<string, unknown>): Promise<T> {
  if (!hasTauriRuntime()) {
    return fallback(name, args) as T;
  }
  try {
    return await invoke<T>(name, args);
  } catch (error) {
    if (import.meta.env.DEV) {
      return fallback(name, args) as T;
    }
    throw error;
  }
}

function hasTauriRuntime() {
  if (typeof window === "undefined") return false;
  const runtime = window as Window & {
    __TAURI_INTERNALS__?: {
      invoke?: unknown;
    };
  };
  return typeof runtime.__TAURI_INTERNALS__?.invoke === "function";
}

function fallback(name: string, args?: Record<string, unknown>) {
  if (name === "get_config") return fallbackConfig;
  if (name === "reload_config") return fallbackConfig;
  if (name === "reveal_provider_api_key") {
    const providerId = String(args?.providerId ?? args?.provider_id ?? "");
    return fallbackProviderSecrets.get(providerId) ?? null;
  }
  if (name === "get_agent_injections") return fallbackConfig.agent_injections;
  if (name === "set_agent_injection_enabled") {
    const input = args?.input as { id: string; enabled: boolean } | undefined;
    if (!input) return [];
    fallbackConfig = {
      ...fallbackConfig,
      agent_injections: fallbackConfig.agent_injections.map((item) =>
        item.id === input.id
          ? {
              ...item,
              enabled: input.enabled,
              last_injected_at: new Date().toISOString(),
              last_status: input.enabled ? "预览模式：已注入" : "预览模式：已关闭"
            }
          : item
      )
    };
    return input.enabled ? [injectionResult(input.id)] : [];
  }
  if (name === "update_agent_injection_route") {
    const input = args?.input as { id: string; provider_id?: string | null; model_id?: string | null } | undefined;
    if (!input) return [];
    fallbackConfig = {
      ...fallbackConfig,
      agent_injections: fallbackConfig.agent_injections.map((item) =>
        item.id === input.id
          ? {
              ...item,
              provider_id: input.provider_id ?? null,
              model_id: input.model_id ?? null,
              last_injected_at: item.enabled ? new Date().toISOString() : item.last_injected_at,
              last_status: item.enabled ? "预览模式：已更新模型路由" : item.last_status
            }
          : item
      )
    };
    return input.id ? [injectionResult(input.id)] : [];
  }
  if (name === "apply_agent_injection") {
    const id = String(args?.id ?? "");
    fallbackConfig = {
      ...fallbackConfig,
      agent_injections: fallbackConfig.agent_injections.map((item) =>
        item.id === id
          ? { ...item, enabled: true, last_injected_at: new Date().toISOString(), last_status: "预览模式：已注入" }
          : item
      )
    };
    return [injectionResult(id)];
  }
  if (name === "apply_enabled_agent_injections") {
    return fallbackConfig.agent_injections.filter((item) => item.enabled).map((item) => injectionResult(item.id));
  }
  if (name === "get_proxy_status") {
    return {
      running: fallbackConfig.proxy_auto_start,
      address: fallbackConfig.proxy_auto_start ? "127.0.0.1:18883" : null
    };
  }
  if (name === "get_metrics") return fallbackMetrics;
  if (name === "fetch_provider_models") {
    const input = args?.input as FetchModelsInput | undefined;
    const models = inferPreviewModels(input?.base_url ?? "", input?.channel ?? "anthropic");
    if (input?.provider_id) {
      fallbackConfig = withProvider(input.provider_id, (provider) => ({
        ...provider,
        models,
        updated_at: new Date().toISOString()
      }));
    }
    return models;
  }
  if (name === "select_provider" && (args?.providerId || args?.provider_id)) {
    const providerId = String(args.providerId ?? args.provider_id);
    const provider = fallbackConfig.providers.find((item) => item.id === providerId);
    if (!provider) return fallbackConfig;
    fallbackConfig = {
      ...fallbackConfig,
      active_provider_id: providerId,
      default_channel: provider.channel,
      route_profiles: fallbackConfig.route_profiles?.map((profile) => ({
        ...profile,
        upstream_channel: provider.channel,
        provider_id: providerId
      })),
      updated_at: new Date().toISOString()
    };
    return fallbackConfig;
  }
  if (name === "add_or_update_provider") {
    const input = args?.input as ProviderInput | undefined;
    if (!input) return fallbackConfig;
    const id = input.id || slugify(input.name);
    const existing = fallbackConfig.providers.find((item) => item.id === id);
    const now = new Date().toISOString();
    if (input.api_key) {
      fallbackProviderSecrets.set(id, input.api_key);
    }
    const provider: ProviderConfig = {
      id,
      name: input.name,
      base_url: input.base_url,
      models_url: input.models_url ?? existing?.models_url ?? null,
      is_full_url: input.is_full_url,
      custom_user_agent: input.custom_user_agent ?? existing?.custom_user_agent ?? null,
      channel: input.channel,
      prompt_cache_retention_enabled: input.prompt_cache_retention_enabled ?? true,
      request_body_gzip_enabled: input.request_body_gzip_enabled ?? existing?.request_body_gzip_enabled ?? false,
      has_api_key: Boolean(input.api_key) || existing?.has_api_key || false,
      models: existing?.models ?? [],
      enabled: input.enabled,
      created_at: existing?.created_at ?? now,
      updated_at: now
    };
    fallbackConfig = {
      ...fallbackConfig,
      active_provider_id: fallbackConfig.active_provider_id ?? id,
      providers: existing
        ? fallbackConfig.providers.map((item) => (item.id === id ? provider : item))
        : [...fallbackConfig.providers, provider],
      updated_at: now
    };
    return fallbackConfig;
  }
  if (name === "add_or_update_model") {
    const input = args?.input as { provider_id: string; model: ModelConfig } | undefined;
    if (!input) return fallbackConfig;
    fallbackConfig = withProvider(input.provider_id, (provider) => {
      const exists = provider.models.some((item) => item.id === input.model.id);
      return {
        ...provider,
        models: exists
          ? provider.models.map((item) => (item.id === input.model.id ? input.model : item))
          : [...provider.models, input.model],
        updated_at: new Date().toISOString()
      };
    });
    return fallbackConfig;
  }
  if (name === "delete_model") {
    const providerId = String(args?.providerId ?? args?.provider_id ?? "");
    const modelId = String(args?.modelId ?? args?.model_id ?? "");
    fallbackConfig = withProvider(providerId, (provider) => ({
      ...provider,
      models: provider.models.filter((item) => item.id !== modelId),
      updated_at: new Date().toISOString()
    }));
    return fallbackConfig;
  }
  if (name === "delete_provider" && (args?.providerId || args?.provider_id)) {
    const providerId = String(args.providerId ?? args.provider_id);
    fallbackProviderSecrets.delete(providerId);
    const providers = fallbackConfig.providers.filter((item) => item.id !== providerId);
    const activeProviderId =
      fallbackConfig.active_provider_id === providerId
        ? providers[0]?.id ?? null
        : fallbackConfig.active_provider_id;
    fallbackConfig = {
      ...fallbackConfig,
      providers,
      active_provider_id: activeProviderId,
      agent_injections: fallbackConfig.agent_injections.map((item) =>
        item.provider_id === providerId
          ? { ...item, provider_id: null, model_id: null }
          : item
      ),
      updated_at: new Date().toISOString()
    };
    return fallbackConfig;
  }
  if (name === "save_config") {
    const input = args?.input as GeneralConfigInput | undefined;
    if (!input) return fallbackConfig;
    fallbackConfig = {
      ...fallbackConfig,
      host: input.host,
      port: input.port,
      local_key: input.local_key,
      default_channel: input.default_channel,
      workspace_fingerprint: input.workspace_fingerprint,
      cache: input.cache,
      updated_at: new Date().toISOString()
    };
    return fallbackConfig;
  }
  if (name === "save_cache_policy") {
    const input = args?.input as AppConfig["cache"] | undefined;
    if (!input) return fallbackConfig;
    fallbackConfig = {
      ...fallbackConfig,
      cache: input,
      updated_at: new Date().toISOString()
    };
    return fallbackConfig;
  }
  if (name === "start_proxy") {
    fallbackConfig = { ...fallbackConfig, proxy_auto_start: true };
    return { running: true, address: "127.0.0.1:18883" };
  }
  if (name === "stop_proxy") {
    fallbackConfig = { ...fallbackConfig, proxy_auto_start: false };
    return { running: false, address: null };
  }
  if (name === "clear_cache") return undefined;
  console.warn("No browser fallback for command", name, args);
  return undefined;
}

export function model(id: string, contextWindow?: number): ModelConfig {
  return {
    id,
    display_name: id,
    context_window: contextWindow ?? null,
    output_window: null,
    supports_tools: true,
    supports_streaming: true,
    enabled: true
  };
}

function routeProfile(client: Channel, upstream: Channel, providerId: string | null) {
  return {
    name: client,
    client_channel: client,
    upstream_channel: upstream,
    provider_id: providerId,
    model_alias: null,
    long_context_threshold: 60000
  };
}

function injection(id: string, label: string, kind: AgentInjectionKind): AgentInjectionConfig {
  return {
    id,
    label,
    kind,
    enabled: false,
    provider_id: null,
    model_id: null,
    target_path: null,
    last_injected_at: null,
    last_status: null
  };
}

function injectionResult(id: string): AgentInjectionResult {
  const item = fallbackConfig.agent_injections.find((injection) => injection.id === id);
  return {
    id,
    label: item?.label ?? id,
    enabled: true,
    target_path: item?.target_path ?? "%APPDATA%/Atoapi/preview.json",
    backup_path: null,
    status: "预览模式：已注入当前本地代理",
    injected_at: new Date().toISOString()
  };
}

function withProvider(
  providerId: string,
  updater: (provider: ProviderConfig) => ProviderConfig
): AppConfig {
  return {
    ...fallbackConfig,
    providers: fallbackConfig.providers.map((provider) =>
      provider.id === providerId ? updater(provider) : provider
    ),
    updated_at: new Date().toISOString()
  };
}

function inferPreviewModels(baseUrl: string, channel: Channel): ModelConfig[] {
  const lower = baseUrl.toLowerCase();
  if (lower.includes("z.ai") || lower.includes("zhipu") || lower.includes("glm")) {
    return [model("GLM-5.2", 1_000_000), model("GLM-5-Turbo", 200_000), model("glm-4.5", 128_000)];
  }
  if (channel === "anthropic") {
    return [model("claude-sonnet-4-5", 200_000), model("claude-haiku-4-5", 200_000)];
  }
  if (channel === "responses") {
    return [model("gpt-5.2", 400_000), model("gpt-5.2-mini", 400_000)];
  }
  return [model("gpt-5.2", 400_000), model("gpt-5.2-mini", 400_000), model("o4-mini", 200_000)];
}

function slugify(value: string) {
  const slug = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug || `provider-${Date.now()}`;
}
