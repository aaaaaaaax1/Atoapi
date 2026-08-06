import type {
    AppConfig,
    CacheValidationStatus,
    Channel,
    MetricsSnapshot,
    ProviderBalanceProbeResult,
    ProxyStatus
} from "../lib/api";

/** Stable, versioned contract between the React control plane and Graphite's iframe. */
export const GRAPHITE_BRIDGE_CHANNEL = "atoapi.graphite.bridge.v1";

export interface GraphiteProviderPayload {
  id?: string | null;
  name: string;
  base_url: string;
  models_url?: string;
  api_key?: string;
  custom_user_agent?: string;
  channel: Channel;
  channel_mode: "auto" | "manual";
  use_system_proxy: boolean;
  prompt_cache_retention_enabled: boolean;
  request_body_gzip_enabled: boolean;
  non_sse_compact_compat_enabled: boolean;
  auto_compact_token_limit?: number | null;
  models: Array<{
    id: string;
    request_model_id?: string | null;
    context_window?: number | null;
    reasoning_effort?: string | null;
  }>;
  keys: Array<{
    id?: string;
    alias?: string;
    key?: string;
    enabled: boolean;
    priority: number;
  }>;
  key_pool?: {
    enabled?: boolean;
    strategy: "round-robin" | "priority" | "least-used" | "random" | "sequential";
    failure_threshold: number;
    recovery_minutes: number;
  };
}

export interface GraphiteBridgeResponse {
  notice?: string;
  error?: string;
  closeOverlay?: string;
  payload?: Record<string, unknown>;
}

export interface GraphitePrototypeHostProps {
  config: AppConfig | null;
  metrics: MetricsSnapshot | null;
  selectedAgentId: string;
  includeColdStarts: boolean;
  includeCompactions: boolean;
  showDetailedErrors: boolean;
  providerConnectionStatus: Record<string, string>;
  providerBalanceStatus: Record<string, ProviderBalanceProbeResult>;
  metricsRefreshPolicy: "visible-1s" | "5s" | "manual";
  proxyStatus: ProxyStatus | null;
  networkPathDiagnostic: {
    provider_id: string;
    paths: Array<{
      path: string;
      ok: boolean;
      elapsed_ms: number;
      status?: number | null;
      http_version?: string | null;
      error?: string | null;
    }>;
  } | null;
  cacheValidation: CacheValidationStatus | null;
  appVersion: string;
  notice?: string;
  error?: string;
  onBridgeAction: (
    action: string,
    payload: Record<string, unknown>
  ) => Promise<GraphiteBridgeResponse | void> | GraphiteBridgeResponse | void;
}

export type GraphiteMessage = {
  channel?: string;
  kind?: string;
  action?: string;
  requestId?: string;
  payload?: Record<string, unknown>;
};
