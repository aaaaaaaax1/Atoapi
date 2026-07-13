import { invoke } from "@tauri-apps/api/core";

export type Channel = "chat" | "responses" | "anthropic";
export type ProviderChannelMode = "auto" | "manual";
export type CacheMode = "passive-warm" | "session-prewarm" | "prefix-prewarm";
export type AgentInjectionKind =
  | "claude-code"
  | "claude-desktop"
  | "codex"
  | "gemini"
  | "open-code"
  | "open-claw"
  | "hermes"
  | "proxy-mode";
export type KeyLoadBalanceStrategy = "round-robin" | "priority" | "least-used" | "random" | "sequential";
export type ProviderKeyStatus = "unknown" | "healthy" | "unhealthy";
export type ProviderResponseSessionReuseStatus = "unverified" | "verified" | "unsupported" | "error";

export interface ModelConfig {
  id: string;
  request_model_id?: string | null;
  display_name: string;
  context_window?: number | null;
  output_window?: number | null;
  reasoning_effort_override_enabled: boolean;
  reasoning_effort?: string | null;
  supported_reasoning_efforts?: string[];
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
  channel_mode: ProviderChannelMode;
  channel: Channel;
  prompt_cache_retention_enabled: boolean;
  request_body_gzip_enabled: boolean;
  use_system_proxy: boolean;
  non_sse_compact_compat_enabled: boolean;
  response_session_reuse_models?: ProviderResponseSessionReuseConfig[];
  has_api_key: boolean;
  key_pool?: PublicProviderKeyPool | null;
  models: ModelConfig[];
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface PublicProviderKeyPool {
  enabled: boolean;
  strategy: KeyLoadBalanceStrategy;
  failure_threshold: number;
  recovery_minutes: number;
  available_keys: number;
  keys: PublicProviderKey[];
}

export interface PublicProviderKey {
  id: string;
  alias?: string | null;
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

export interface ProviderKeyPoolInput {
  enabled: boolean;
  strategy: KeyLoadBalanceStrategy;
  failure_threshold: number;
  recovery_minutes: number;
  keys: ProviderKeyInput[];
}

export interface ProviderKeyInput {
  id?: string;
  alias?: string | null;
  key?: string | null;
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

export interface ProviderKeyTestResult {
  provider_id?: string | null;
  key_id?: string | null;
  ok: boolean;
  message: string;
  models_count: number;
}

export interface ProviderNetworkPathResult {
  path: "direct" | "system-proxy" | string;
  ok: boolean;
  status?: number | null;
  elapsed_ms: number;
  http_version?: string | null;
  remote_addr?: string | null;
  error?: string | null;
}

export interface ProviderNetworkPathDiagnosticResult {
  provider_id: string;
  target_url: string;
  paths: ProviderNetworkPathResult[];
}

export interface AppConfig {
  host: string;
  port: number;
  proxy_auto_start: boolean;
  proxy_mode_host: string;
  proxy_mode_port: number;
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
  provider_key_pools?: Array<{ provider_id: string; pool: PublicProviderKeyPool }>;
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
  local_key?: string | null;
  hidden_provider_ids?: string[];
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
  successful_requests: number;
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
  recent_upstream_calls?: Array<{
    id: string;
    at: string;
    inbound_request_id?: string | null;
    upstream_request_id?: string | null;
    upstream_attempt_index?: number | null;
    upstream_attempt_total?: number | null;
    client_channel: string;
    upstream_channel: string;
    provider: string;
    model: string;
    requested_model?: string | null;
    agent_reasoning_effort?: string | null;
    configured_reasoning_effort?: string | null;
    effective_reasoning_effort?: string | null;
    reasoning_effort_source?: string | null;
    cache_status: string;
    agent_id?: string | null;
    agent_label?: string | null;
    upstream_call_kind?: "stream" | "sync" | "prewarm-sync" | "cache" | string | null;
    upstream_call_source?: string | null;
    status: number;
    ttft_ms: number;
    upstream_ttft_ms?: number | null;
    local_prepare_ms?: number | null;
    upstream_headers_ms?: number | null;
    upstream_last_attempt_headers_ms?: number | null;
    upstream_http_version?: string | null;
    upstream_network_path?: string | null;
    upstream_remote_addr?: string | null;
    upstream_pool_diagnostic?: string | null;
    upstream_trace_id?: string | null;
    upstream_trace_source?: string | null;
    upstream_server_timing?: string | null;
    upstream_timing_source?: string | null;
    upstream_reported_processing_ms?: number | null;
    upstream_non_processing_ms?: number | null;
    upstream_first_chunk_ms?: number | null;
    stream_upstream_wait_ms?: number | null;
    stream_client_backpressure_ms?: number | null;
    sse_end_reason?: string | null;
    downstream_disconnected?: boolean | null;
    upstream_retry_wait_ms?: number | null;
    upstream_attempts?: number | null;
    request_body_bytes?: number | null;
    sent_body_bytes?: number | null;
    request_body_encode_ms?: number | null;
    gzip_encode_ms?: number | null;
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
    cache_provider_unstable_gap_tokens?: number | null;
    provider_cache_token_ratio?: number | null;
    prefix_guard_skip_reason?: string | null;
    response_session_reused?: boolean | null;
    response_session_candidate_count?: number | null;
    response_session_skip_reason?: string | null;
    response_session_exact_key_hit?: boolean | null;
    response_session_scope_match_count?: number | null;
    response_session_append_delta_match?: boolean | null;
    response_session_delta_items?: number | null;
    response_session_cooldown_active?: boolean | null;
    response_session_rejected_status?: number | null;
    session_anchor_hash?: string | null;
    session_anchor_source?: string | null;
    session_anchor_changed?: boolean | null;
    session_anchor_peer_count?: number | null;
  }>;
  recent_requests: Array<{
    id: string;
    at: string;
    inbound_request_id?: string | null;
    upstream_request_id?: string | null;
    upstream_attempt_index?: number | null;
    upstream_attempt_total?: number | null;
    client_channel: string;
    upstream_channel: string;
    provider: string;
    model: string;
    requested_model?: string | null;
    agent_reasoning_effort?: string | null;
    configured_reasoning_effort?: string | null;
    effective_reasoning_effort?: string | null;
    reasoning_effort_source?: string | null;
    cache_status: string;
    agent_id?: string | null;
    agent_label?: string | null;
    upstream_call_kind?: "stream" | "sync" | "prewarm-sync" | "cache" | string | null;
    upstream_call_source?: string | null;
    status: number;
    ttft_ms: number;
    upstream_ttft_ms?: number | null;
    local_prepare_ms?: number | null;
    upstream_headers_ms?: number | null;
    upstream_last_attempt_headers_ms?: number | null;
    upstream_http_version?: string | null;
    upstream_network_path?: string | null;
    upstream_remote_addr?: string | null;
    upstream_pool_diagnostic?: string | null;
    upstream_trace_id?: string | null;
    upstream_trace_source?: string | null;
    upstream_server_timing?: string | null;
    upstream_timing_source?: string | null;
    upstream_reported_processing_ms?: number | null;
    upstream_non_processing_ms?: number | null;
    upstream_first_chunk_ms?: number | null;
    stream_upstream_wait_ms?: number | null;
    stream_client_backpressure_ms?: number | null;
    sse_end_reason?: string | null;
    downstream_disconnected?: boolean | null;
    upstream_retry_wait_ms?: number | null;
    upstream_attempts?: number | null;
    request_body_bytes?: number | null;
    sent_body_bytes?: number | null;
    request_body_encode_ms?: number | null;
    gzip_encode_ms?: number | null;
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
    cache_provider_unstable_gap_tokens?: number | null;
    provider_cache_token_ratio?: number | null;
    prefix_guard_skip_reason?: string | null;
    response_session_reused?: boolean | null;
    response_session_candidate_count?: number | null;
    response_session_skip_reason?: string | null;
    response_session_exact_key_hit?: boolean | null;
    response_session_scope_match_count?: number | null;
    response_session_append_delta_match?: boolean | null;
    response_session_delta_items?: number | null;
    response_session_cooldown_active?: boolean | null;
    response_session_rejected_status?: number | null;
    session_anchor_hash?: string | null;
    session_anchor_source?: string | null;
    session_anchor_changed?: boolean | null;
    session_anchor_peer_count?: number | null;
  }>;
  recent_failed_requests?: MetricsSnapshot["recent_requests"];
  recent_errors?: Array<{
    at: string;
    scope: string;
    message: string;
  }>;
}

export interface ProviderTrafficStats {
  provider: string;
  total_requests: number;
  successful_requests: number;
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
  provider_unstable_gap_tokens: number;
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
  channel_mode: ProviderChannelMode;
  channel: Channel;
  prompt_cache_retention_enabled: boolean;
  request_body_gzip_enabled: boolean;
  use_system_proxy: boolean;
  non_sse_compact_compat_enabled: boolean;
  key_pool?: ProviderKeyPoolInput | null;
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
  use_system_proxy: boolean;
}

export interface ProxyModeConfigInput {
  host: string;
  port: number;
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
  proxy_mode_host: "127.0.0.1",
  proxy_mode_port: 18884,
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
    injection("claude-desktop", "Claude Desktop", "claude-desktop"),
    injection("codex", "Codex", "codex"),
    injection("gemini", "Gemini", "gemini"),
    injection("opencode", "OpenCode", "open-code"),
    injection("openclaw", "OpenClaw", "open-claw"),
    injection("hermes", "Hermes", "hermes"),
    injection("proxy-mode", "本地代理模式", "proxy-mode")
  ],
  provider_key_pools: [],
  updated_at: new Date().toISOString(),
  config_path: "%APPDATA%/Atoapi/config.toml"
};

const emptyRecentUsage: RecentUsageStats = {
  window_seconds: 1800,
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
  successful_requests: 0,
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
  recent_requests: [],
  recent_failed_requests: []
};

const fallbackProviderSecrets = new Map<string, string>();
const fallbackProviderKeySecrets = new Map<string, string>();

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
  if (name === "reveal_provider_key") {
    const providerId = String(args?.providerId ?? args?.provider_id ?? "");
    const keyId = String(args?.keyId ?? args?.key_id ?? "");
    return fallbackProviderKeySecrets.get(providerKeySecretId(providerId, keyId)) ?? null;
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
  if (name === "diagnose_provider_network_paths") {
    throw new Error("网络路径诊断仅在桌面代理运行时可用");
  }
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
  if (name === "test_provider_key") {
    const input = args?.input as { provider_id?: string; key_id?: string; api_key?: string; base_url?: string; channel?: Channel } | undefined;
    const usable = Boolean(input?.api_key || input?.key_id);
    const models = inferPreviewModels(input?.base_url ?? "", input?.channel ?? "anthropic");
    return {
      provider_id: input?.provider_id ?? null,
      key_id: input?.key_id ?? null,
      ok: usable,
      message: usable ? "预览模式：可用，获取到 " + models.length + " 个模型" : "Key 为空",
      models_count: usable ? models.length : 0
    };
  }
  if (name === "test_provider_key_pool") {
    const providerId = String(args?.providerId ?? args?.provider_id ?? "");
    const provider = fallbackConfig.providers.find((item) => item.id === providerId);
    return (provider?.key_pool?.keys ?? []).map((key) => ({
      provider_id: providerId,
      key_id: key.id,
      ok: key.enabled,
      message: key.enabled ? "预览模式：可用" : "预览模式：已关闭",
      models_count: key.enabled ? 3 : 0
    }));
  }
  if (name === "probe_provider_response_session_reuse") {
    const input = args?.input as ProviderResponseSessionReuseProbeInput | undefined;
    return {
      provider_id: input?.provider_id ?? "",
      model_id: input?.model_id ?? "",
      status: "error",
      enabled: false,
      message: "预览模式不会向外部上游发送会话复用兼容性探测",
      checked_at: new Date().toISOString(),
      first_status: null,
      continuation_status: null
    } satisfies ProviderResponseSessionReuseProbeResult;
  }
  if (name === "set_provider_response_session_reuse_enabled") {
    const providerId = String(args?.providerId ?? args?.provider_id ?? "");
    const modelId = String(args?.modelId ?? args?.model_id ?? "");
    const enabled = Boolean(args?.enabled);
    fallbackConfig = withProvider(providerId, (provider) => ({
      ...provider,
      response_session_reuse_models: (provider.response_session_reuse_models ?? []).map((item) =>
        item.model_id === modelId && item.status === "verified"
          ? { ...item, enabled, updated_at: new Date().toISOString() }
          : item
      )
    }));
    return fallbackConfig;
  }
  if (name === "select_provider" && (args?.providerId || args?.provider_id)) {
    const providerId = String(args.providerId ?? args.provider_id);
    const provider = fallbackConfig.providers.find((item) => item.id === providerId);
    if (!provider) return fallbackConfig;
    fallbackConfig = {
      ...fallbackConfig,
      active_provider_id: providerId,
      default_channel: provider.channel,
      updated_at: new Date().toISOString()
    };
    return fallbackConfig;
  }
  if (name === "clone_provider_for_agent") {
    const input = args?.input as { agent_id?: string; provider_id?: string; model_id?: string | null } | undefined;
    const agentId = input?.agent_id ?? "";
    const providerId = input?.provider_id ?? "";
    const source = fallbackConfig.providers.find((item) => item.id === providerId);
    if (!agentId || !source) return fallbackConfig;
    const now = new Date().toISOString();
    const existingClone = fallbackConfig.providers.find((item) =>
      providerCloneMatchesSourcePreview(item.id, providerId, agentId)
    );
    if (providerBelongsToAgentPreview(providerId, agentId) || existingClone) {
      const target = existingClone ?? source;
      fallbackConfig = {
        ...fallbackConfig,
        agent_injections: fallbackConfig.agent_injections.map((item) =>
          item.id === agentId
            ? {
                ...item,
                provider_id: target.id,
                hidden_provider_ids: (item.hidden_provider_ids ?? []).filter(
                  (hiddenId) => hiddenId !== providerId
                ),
                model_id: input?.model_id ?? null
              }
            : item
        ),
        updated_at: now
      };
      return fallbackConfig;
    }
    const id = uniquePreviewProviderId(`agent-${agentId}-${providerId}`);
    const name = uniquePreviewProviderName(`${source.name} / ${agentId}`);
    const clonedKeyPool = source.key_pool
      ? { ...source.key_pool, keys: source.key_pool.keys.map((key) => ({ ...key })) }
      : null;
    const cloned: ProviderConfig = {
      ...source,
      id,
      name,
      key_pool: clonedKeyPool,
      created_at: now,
      updated_at: now
    };
    const secret = fallbackProviderSecrets.get(providerId);
    if (secret) fallbackProviderSecrets.set(id, secret);
    for (const item of source.key_pool?.keys ?? []) {
      const keySecret = fallbackProviderKeySecrets.get(providerKeySecretId(providerId, item.id));
      if (keySecret) fallbackProviderKeySecrets.set(providerKeySecretId(id, item.id), keySecret);
    }
    fallbackConfig = {
      ...fallbackConfig,
      providers: [...fallbackConfig.providers, cloned],
      provider_key_pools: clonedKeyPool
        ? [...(fallbackConfig.provider_key_pools ?? []), { provider_id: id, pool: clonedKeyPool }]
        : fallbackConfig.provider_key_pools,
      agent_injections: fallbackConfig.agent_injections.map((item) =>
        item.id === agentId
          ? {
              ...item,
              provider_id: id,
              hidden_provider_ids: (item.hidden_provider_ids ?? []).filter(
                (hiddenId) => hiddenId !== providerId
              ),
              model_id: input?.model_id ?? null
            }
          : item
      ),
      updated_at: now
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
    for (const item of input.key_pool?.keys ?? []) {
      if (item.id && item.key) {
        fallbackProviderKeySecrets.set(providerKeySecretId(id, item.id), item.key);
      }
    }
    const provider: ProviderConfig = {
      id,
      name: input.name,
      base_url: input.base_url,
      models_url: input.models_url ?? existing?.models_url ?? null,
      is_full_url: input.is_full_url,
      custom_user_agent: input.custom_user_agent ?? existing?.custom_user_agent ?? null,
      channel_mode: input.channel_mode ?? existing?.channel_mode ?? "auto",
      channel: input.channel,
      prompt_cache_retention_enabled: input.prompt_cache_retention_enabled ?? true,
      request_body_gzip_enabled: input.request_body_gzip_enabled ?? existing?.request_body_gzip_enabled ?? false,
      use_system_proxy: input.use_system_proxy ?? existing?.use_system_proxy ?? false,
      non_sse_compact_compat_enabled: input.non_sse_compact_compat_enabled ?? existing?.non_sse_compact_compat_enabled ?? false,
      response_session_reuse_models: existing?.response_session_reuse_models ?? [],
      has_api_key: Boolean(input.api_key) || existing?.has_api_key || false,
      key_pool: input.key_pool
        ? previewKeyPool(input.key_pool, existing?.key_pool ?? null)
        : existing?.key_pool ?? null,
      models: existing?.models ?? [],
      enabled: input.enabled,
      created_at: existing?.created_at ?? now,
      updated_at: now
    };
    fallbackConfig = {
      ...fallbackConfig,
      active_provider_id: fallbackConfig.active_provider_id ?? id,
      provider_key_pools: input.key_pool
        ? [
            ...(fallbackConfig.provider_key_pools ?? []).filter((item) => item.provider_id !== id),
            { provider_id: id, pool: previewKeyPool(input.key_pool, existing?.key_pool ?? null) }
          ]
        : fallbackConfig.provider_key_pools,
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
    const agentId = String(args?.agentId ?? args?.agent_id ?? "");
    if (agentId && !providerBelongsToAgentPreview(providerId, agentId)) {
      fallbackConfig = {
        ...fallbackConfig,
        agent_injections: fallbackConfig.agent_injections.map((item) =>
          item.id === agentId
            ? {
                ...item,
                ...(item.provider_id === providerId
                  ? { enabled: false, provider_id: null, model_id: null }
                  : {}),
                hidden_provider_ids: Array.from(
                  new Set([...(item.hidden_provider_ids ?? []), providerId])
                )
              }
            : item
        ),
        updated_at: new Date().toISOString()
      };
      return fallbackConfig;
    }
    const sourceProviderId = agentId
      ? fallbackConfig.providers.find(
          (item) =>
            !item.id.startsWith("agent-") &&
            providerCloneMatchesSourcePreview(providerId, item.id, agentId)
        )?.id
      : undefined;
    fallbackProviderSecrets.delete(providerId);
    for (const secretId of Array.from(fallbackProviderKeySecrets.keys())) {
      if (secretId.startsWith(providerId + ":")) fallbackProviderKeySecrets.delete(secretId);
    }
    const providers = fallbackConfig.providers.filter((item) => item.id !== providerId);
    const activeProviderId =
      fallbackConfig.active_provider_id === providerId
        ? providers[0]?.id ?? null
        : fallbackConfig.active_provider_id;
    fallbackConfig = {
      ...fallbackConfig,
      providers,
      provider_key_pools: (fallbackConfig.provider_key_pools ?? []).filter((item) => item.provider_id !== providerId),
      active_provider_id: activeProviderId,
      agent_injections: fallbackConfig.agent_injections.map((item) =>
        item.provider_id === providerId && (!agentId || item.id === agentId)
          ? {
              ...item,
              provider_id: null,
              model_id: null,
              hidden_provider_ids: sourceProviderId
                ? Array.from(new Set([...(item.hidden_provider_ids ?? []), sourceProviderId]))
                : item.hidden_provider_ids
            }
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
  if (name === "save_proxy_mode_config") {
    const input = args?.input as ProxyModeConfigInput | undefined;
    if (!input) return fallbackConfig;
    fallbackConfig = {
      ...fallbackConfig,
      proxy_mode_host: input.host,
      proxy_mode_port: input.port,
      updated_at: new Date().toISOString()
    };
    return fallbackConfig;
  }  if (name === "save_cache_policy") {
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
    request_model_id: null,
    display_name: id,
    context_window: contextWindow ?? null,
    output_window: null,
    reasoning_effort_override_enabled: false,
    reasoning_effort: null,
    supported_reasoning_efforts: [],
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
    last_status: null,
    hidden_provider_ids: []
  };
}

export interface ProviderResponseSessionReuseConfig {
  provider_id: string;
  model_id: string;
  enabled: boolean;
  status: ProviderResponseSessionReuseStatus;
  checked_at?: string | null;
  last_error?: string | null;
  updated_at: string;
}

export interface ProviderResponseSessionReuseProbeResult {
  provider_id: string;
  model_id: string;
  status: ProviderResponseSessionReuseStatus;
  enabled: boolean;
  message: string;
  checked_at?: string | null;
  first_status?: number | null;
  continuation_status?: number | null;
}

export interface ProviderResponseSessionReuseProbeInput {
  provider_id: string;
  model_id: string;
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


function providerKeySecretId(providerId: string, keyId: string) {
  return providerId + ":" + keyId;
}

function previewKeyPool(input: ProviderKeyPoolInput, existing: PublicProviderKeyPool | null): PublicProviderKeyPool {
  const keys = input.keys.map((key) => {
    const id = key.id || "key-" + Math.random().toString(36).slice(2);
    const previous = existing?.keys.find((item) => item.id === id);
    return {
      id,
      alias: key.alias ?? previous?.alias ?? null,
      preview: key.key ? maskKeyPreview(key.key) : previous?.preview ?? "未保存",
      enabled: key.enabled,
      priority: key.priority,
      status: key.status,
      total_requests: key.total_requests,
      successes: key.successes,
      failures: key.failures,
      last_checked_at: key.last_checked_at ?? previous?.last_checked_at ?? null,
      last_error: key.last_error ?? null,
      disabled_until: key.disabled_until ?? null
    };
  });
  return {
    enabled: input.enabled,
    strategy: input.strategy,
    failure_threshold: input.failure_threshold,
    recovery_minutes: input.recovery_minutes,
    available_keys: keys.filter((key) => key.enabled).length,
    keys
  };
}

function maskKeyPreview(value: string) {
  const key = value.trim();
  if (!key) return "未保存";
  if (key.length <= 10) return "*".repeat(Math.max(4, key.length));
  return key.slice(0, 6) + "..." + key.slice(-4);
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

function providerBelongsToAgentPreview(providerId: string, agentId: string) {
  return providerId.startsWith(`agent-${slugify(agentId)}-`);
}

function providerCloneMatchesSourcePreview(providerId: string, sourceId: string, agentId: string) {
  const base = `agent-${slugify(agentId)}-${slugify(sourceId)}`;
  if (providerId === base) return true;
  const suffix = providerId.slice(base.length + 1);
  return providerId.startsWith(`${base}-`) && /^\d+$/.test(suffix);
}

function uniquePreviewProviderId(base: string) {
  const cleanBase = slugify(base);
  let candidate = cleanBase;
  let index = 2;
  while (fallbackConfig.providers.some((provider) => provider.id === candidate)) {
    candidate = `${cleanBase}-${index}`;
    index += 1;
  }
  return candidate;
}

function uniquePreviewProviderName(base: string) {
  const cleanBase = base.trim() || "Agent provider";
  let candidate = cleanBase;
  let index = 2;
  while (fallbackConfig.providers.some((provider) => provider.name === candidate)) {
    candidate = `${cleanBase} (${index})`;
    index += 1;
  }
  return candidate;
}

function slugify(value: string) {
  const slug = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return slug || `provider-${Date.now()}`;
}
