use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::RwLock;

const RECENT_USAGE_WINDOW_MINUTES: i64 = 30;
const RECENT_USAGE_WINDOW_SECONDS: u64 = 30 * 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub started_at: DateTime<Utc>,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub upstream_requests: u64,
    pub response_cache_hits: u64,
    pub semantic_cache_hits: u64,
    pub cache_misses: u64,
    pub errors: u64,
    pub retries: u64,
    pub ttft_p95_ms: u64,
    pub total_p95_ms: u64,
    pub provider_cached_tokens: u64,
    pub provider_input_tokens: u64,
    pub provider_cache_hit_requests: u64,
    pub provider_cache_token_ratio: f64,
    pub provider_cache_request_hit_rate: f64,
    pub combined_cache_hit_rate: f64,
    pub recent_usage: RecentUsageStats,
    pub eligible_cache_lookups: u64,
    pub eligible_cache_hits: u64,
    pub first_seen_eligible_misses: u64,
    pub repeatable_eligible_lookups: u64,
    pub repeatable_eligible_hits: u64,
    pub overall_eligible_cache_hit_rate: f64,
    pub repeatable_eligible_cache_hit_rate: f64,
    pub eligible_cache_hit_rate: f64,
    pub usage: UsageSnapshot,
    pub local_proxy: LocalProxyStats,
    pub background_prewarm: Vec<BackgroundPrewarmStats>,
    pub gap_buckets: Vec<GapBucketStats>,
    pub request_body_buckets: Vec<RequestBodyBucketStats>,
    pub provider_stats: Vec<ProviderTrafficStats>,
    pub recent_upstream_calls: Vec<RequestLog>,
    pub recent_requests: Vec<RequestLog>,
    pub recent_failed_requests: Vec<RequestLog>,
    pub recent_errors: Vec<ErrorLog>,
    #[serde(default)]
    pub agent_generation: AgentGenerationStats,
    #[serde(default)]
    pub shadow_affinity: ShadowAffinityMetrics,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_agent_inbound_outcomes: Vec<AgentInboundOutcomeLog>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_agent_upstream_attempts: Vec<AgentUpstreamAttemptLog>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentGenerationStats {
    pub inbound_requests: u64,
    pub successful_inbounds: u64,
    pub failed_inbounds: u64,
    pub generation_attempts: u64,
    pub multi_attempt_inbounds: u64,
    pub max_attempts_per_inbound: u64,
    pub active_inbounds: u64,
    pub active_attempts: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowAffinityMetrics {
    pub decisions: u64,
    pub assigned_decisions: u64,
    pub transparent_decisions: u64,
    pub applied_decisions: u64,
    pub candidate_decisions: u64,
    pub observations: u64,
    pub successful_observations: u64,
    pub usage_observations: u64,
    pub inconclusive_observations: u64,
    pub policy_compute_ms_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentInboundOutcome {
    Success,
    HttpError,
    TransportError,
    StreamError,
    Incomplete,
    RelayAborted,
}

impl AgentInboundOutcome {
    fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentAttemptOutcome {
    HttpSuccess,
    HttpError,
    TransportError,
    StreamError,
    RelayAborted,
}

#[derive(Debug, Clone)]
pub struct AgentInboundStart {
    pub inbound_request_id: String,
    pub at: DateTime<Utc>,
    pub attempt_policy: String,
    pub attempt_budget: u64,
}

#[derive(Debug, Clone)]
pub struct AgentAttemptStart {
    pub inbound_request_id: String,
    pub attempt_id: String,
    pub at: DateTime<Utc>,
    pub attempt_reason: String,
    pub provider: String,
    pub model: String,
    pub upstream_channel: String,
}

#[derive(Debug, Clone)]
pub struct AgentAttemptFinish {
    pub finished_at: DateTime<Utc>,
    pub outcome: AgentAttemptOutcome,
    pub status: Option<u16>,
    pub error_scope: Option<String>,
    pub terminal_state: Option<String>,
    pub total_ms: u64,
    pub upstream_headers_ms: Option<u64>,
    pub upstream_network_path: Option<String>,
    pub request_body_bytes: Option<u64>,
    pub sent_body_bytes: Option<u64>,
    pub gzip_attempted: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentInboundOutcomeLog {
    #[serde(flatten)]
    pub request: RequestLog,
    pub started_at: DateTime<Utc>,
    pub attempt_policy: String,
    pub attempt_count: u64,
    pub attempt_budget: u64,
    pub final_attempt_id: Option<String>,
    pub outcome: AgentInboundOutcome,
    pub terminal_state: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentUpstreamAttemptLog {
    pub inbound_request_id: String,
    pub attempt_id: String,
    pub attempt_index: u64,
    pub attempt_budget: u64,
    pub attempt_policy: String,
    pub attempt_reason: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub provider: String,
    pub model: String,
    pub upstream_channel: String,
    pub outcome: AgentAttemptOutcome,
    pub status: Option<u16>,
    pub error_scope: Option<String>,
    pub terminal_state: Option<String>,
    pub total_ms: u64,
    pub upstream_headers_ms: Option<u64>,
    pub upstream_network_path: Option<String>,
    pub request_body_bytes: Option<u64>,
    pub sent_body_bytes: Option<u64>,
    pub gzip_attempted: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageSnapshot {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_tokens: u64,
    pub cold_start_requests: u64,
    pub cold_start_input_tokens: u64,
    pub cold_start_output_tokens: u64,
    pub cold_start_total_tokens: u64,
    pub by_provider: Vec<UsageGroup>,
    pub by_model: Vec<UsageGroup>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageGroup {
    pub key: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub total_tokens: u64,
    pub cold_start_requests: u64,
    pub cold_start_input_tokens: u64,
    pub cold_start_output_tokens: u64,
    pub cold_start_total_tokens: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecentUsageStats {
    pub window_seconds: u64,
    pub requests: u64,
    pub cache_hit_requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cold_start_requests: u64,
    pub cold_start_input_tokens: u64,
    pub cold_start_output_tokens: u64,
    pub cold_start_total_tokens: u64,
    pub cache_token_ratio: f64,
    pub cache_request_hit_rate: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageRecord {
    pub provider: String,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_creation_tokens: u64,
}

impl UsageRecord {
    pub fn has_usage(&self) -> bool {
        self.input_tokens > 0
            || self.output_tokens > 0
            || self.cache_read_tokens > 0
            || self.cache_creation_tokens > 0
    }

    pub fn merge(&mut self, other: UsageRecord) {
        if self.model == "" || self.model == "unknown" {
            self.model = other.model;
        }
        self.input_tokens += other.input_tokens;
        self.output_tokens += other.output_tokens;
        self.cache_read_tokens += other.cache_read_tokens;
        self.cache_creation_tokens += other.cache_creation_tokens;
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LocalProxyStats {
    pub local_cache_hits: u64,
    pub upstream_requests_saved: u64,
    pub estimated_tokens_saved: u64,
    pub exact_hits: u64,
    pub semantic_hits: u64,
    pub eligible_lookups: u64,
    pub eligible_hits: u64,
    pub first_seen_eligible_misses: u64,
    pub repeatable_eligible_lookups: u64,
    pub repeatable_eligible_hits: u64,
    pub overall_hit_rate: f64,
    pub repeatable_hit_rate: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderTrafficStats {
    pub provider: String,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub upstream_requests: u64,
    pub cache_hits: u64,
    pub exact_hits: u64,
    pub semantic_hits: u64,
    pub cache_misses: u64,
    pub bypassed: u64,
    pub error_statuses: u64,
    pub cold_start_requests: u64,
    pub cold_start_input_tokens: u64,
    pub cold_start_output_tokens: u64,
    pub cold_start_total_tokens: u64,
    pub ttft_p95_ms: u64,
    pub total_p95_ms: u64,
    pub cache_hit_rate: f64,
    pub recent_usage: RecentUsageStats,
    pub gap_buckets: Vec<GapBucketStats>,
    pub request_body_buckets: Vec<RequestBodyBucketStats>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GapBucketStats {
    pub bucket: String,
    pub requests: u64,
    pub total_gap_tokens: u64,
    pub new_tail_gap_tokens: u64,
    pub avoidable_gap_tokens: u64,
    pub provider_unstable_gap_tokens: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RequestBodyBucketStats {
    pub bucket: String,
    pub risk: String,
    pub requests: u64,
    pub total_bytes: u64,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackgroundPrewarmStats {
    pub channel: String,
    pub attempts: u64,
    pub successes: u64,
    pub trigger_new_tail_tokens: u64,
    pub trigger_avoidable_tokens: u64,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponsesWirePrefixFingerprints {
    pub version: u8,
    pub cache_metadata: String,
    pub instructions: String,
    pub tools_schema: String,
    pub input_history: String,
    pub input_full: String,
    pub input_item_count: u64,
    pub input_prefixes: Vec<String>,
    pub pre_input_wire: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestLog {
    pub id: String,
    pub at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inbound_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_attempt_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_attempt_total: Option<u64>,
    pub client_channel: String,
    pub upstream_channel: String,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub configured_reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_reasoning_effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort_source: Option<String>,
    pub cache_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_call_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_call_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_prefix_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_prefix_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbound_prefix_fingerprints: Option<ResponsesWirePrefixFingerprints>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_cache_diagnostic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_arm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_realm_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_cohort_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_lane: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_shard: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_policy_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_anchor_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_trusted_identity: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_decision: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadow_affinity_policy_compute_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_wait_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_wait_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_state_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_wait_effect: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_lag_classification: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_lag_input_delta_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_lag_cache_delta_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_lag_previous_gap_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_cache_instability_score: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_seen_bucket_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_state_cache_read_tokens: Option<u64>,
    pub status: u16,
    pub ttft_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_ttft_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_prepare_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_headers_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_last_attempt_headers_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_http_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_network_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_remote_addr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_pool_diagnostic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_trace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_trace_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_server_timing: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_timing_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_reported_processing_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_non_processing_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_first_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_upstream_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_client_backpressure_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate_done_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_retry_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_attempts: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_encode_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gzip_encode_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gzip_attempted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gzip_fallback_used: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_header_wait_class: Option<String>,
    pub total_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_shortfall_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_new_tail_gap_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_avoidable_gap_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_provider_unstable_gap_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_cache_token_ratio: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_input_items: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_message_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_call_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_largest_tool_output_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_lines: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_repeated_line_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_timestamp_like_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_path_like_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_url_like_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_hash_like_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_json_like_chars: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_tool_output_noise_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_reused: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_candidate_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_skip_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_exact_key_hit: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_scope_match_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_append_delta_match: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_delta_items: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_cooldown_active: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_rejected_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_anchor_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_anchor_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_anchor_changed: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_anchor_peer_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_body_is_delta: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_too_large_rescue_attempted: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_too_large_rescue_used: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_end_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub downstream_disconnected: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub downstream_disconnect_stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_completed_event_seen: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_done_marker_seen: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sse_chunks: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorLog {
    pub at: DateTime<Utc>,
    pub scope: String,
    pub message: String,
}

#[derive(Debug)]
struct MetricsInner {
    started_at: DateTime<Utc>,
    total_requests: u64,
    successful_requests: u64,
    upstream_requests: u64,
    response_cache_hits: u64,
    semantic_cache_hits: u64,
    cache_misses: u64,
    errors: u64,
    retries: u64,
    ttft_samples: VecDeque<u64>,
    total_samples: VecDeque<u64>,
    provider_cached_tokens: u64,
    provider_input_tokens: u64,
    provider_usage_requests: u64,
    provider_cache_hit_requests: u64,
    eligible_cache_lookups: u64,
    eligible_cache_hits: u64,
    first_seen_eligible_misses: u64,
    repeatable_eligible_lookups: u64,
    repeatable_eligible_hits: u64,
    seen_eligible_cache_keys: HashSet<String>,
    seen_eligible_cache_key_order: VecDeque<String>,
    usage: UsageAccumulator,
    recent_usage: VecDeque<TimedUsageRecord>,
    cold_start_keys: HashSet<String>,
    request_cold_start_keys: HashSet<String>,
    provider_stats: Vec<ProviderTrafficAccumulator>,
    gap_buckets: Vec<GapBucketAccumulator>,
    request_body_buckets: Vec<RequestBodyBucketAccumulator>,
    background_prewarm: Vec<BackgroundPrewarmAccumulator>,
    local_proxy_estimated_tokens_saved: u64,
    recent_upstream_calls: VecDeque<RequestLog>,
    recent_requests: VecDeque<RequestLog>,
    recent_failed_requests: VecDeque<RequestLog>,
    recent_errors: VecDeque<ErrorLog>,
    agent_generation: AgentGenerationStats,
    shadow_affinity: ShadowAffinityMetrics,
    active_agent_inbounds: HashMap<String, ActiveAgentInbound>,
    active_agent_attempts: HashMap<String, ActiveAgentAttempt>,
    completed_agent_inbound_ids: HashSet<String>,
    completed_agent_inbound_order: VecDeque<String>,
    completed_agent_attempt_ids: HashSet<String>,
    completed_agent_attempt_order: VecDeque<String>,
    recent_agent_inbound_outcomes: VecDeque<AgentInboundOutcomeLog>,
    recent_agent_upstream_attempts: VecDeque<AgentUpstreamAttemptLog>,
}

#[derive(Debug, Clone)]
struct ActiveAgentInbound {
    at: DateTime<Utc>,
    attempt_policy: String,
    attempt_budget: u64,
    attempt_count: u64,
    last_attempt_id: Option<String>,
}

#[derive(Debug, Clone)]
struct ActiveAgentAttempt {
    inbound_request_id: String,
    attempt_id: String,
    attempt_index: u64,
    attempt_budget: u64,
    attempt_policy: String,
    attempt_reason: String,
    started_at: DateTime<Utc>,
    provider: String,
    model: String,
    upstream_channel: String,
}

#[derive(Debug, Clone, Default)]
struct UsageAccumulator {
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_creation_tokens: u64,
    cold_start_requests: u64,
    cold_start_input_tokens: u64,
    cold_start_output_tokens: u64,
    by_provider: Vec<UsageGroup>,
    by_model: Vec<UsageGroup>,
}

#[derive(Debug, Clone)]
struct TimedUsageRecord {
    at: DateTime<Utc>,
    record: UsageRecord,
    cold_start_counted: bool,
}

#[derive(Debug, Clone, Default)]
struct ProviderTrafficAccumulator {
    provider: String,
    total_requests: u64,
    successful_requests: u64,
    upstream_requests: u64,
    cache_hits: u64,
    exact_hits: u64,
    semantic_hits: u64,
    cache_misses: u64,
    bypassed: u64,
    error_statuses: u64,
    cold_start_requests: u64,
    cold_start_input_tokens: u64,
    cold_start_output_tokens: u64,
    ttft_samples: VecDeque<u64>,
    total_samples: VecDeque<u64>,
    gap_buckets: Vec<GapBucketAccumulator>,
    request_body_buckets: Vec<RequestBodyBucketAccumulator>,
}

#[derive(Debug, Clone, Default)]
struct GapBucketAccumulator {
    bucket: String,
    requests: u64,
    total_gap_tokens: u64,
    new_tail_gap_tokens: u64,
    avoidable_gap_tokens: u64,
    provider_unstable_gap_tokens: u64,
}

#[derive(Debug, Clone, Default)]
struct RequestBodyBucketAccumulator {
    bucket: String,
    risk: String,
    requests: u64,
    total_bytes: u64,
    max_bytes: u64,
}

#[derive(Debug, Clone, Default)]
struct BackgroundPrewarmAccumulator {
    channel: String,
    attempts: u64,
    successes: u64,
    trigger_new_tail_tokens: u64,
    trigger_avoidable_tokens: u64,
    input_tokens: u64,
    cache_read_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct MetricsStore {
    inner: Arc<RwLock<MetricsInner>>,
}

impl MetricsStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MetricsInner {
                started_at: Utc::now(),
                total_requests: 0,
                successful_requests: 0,
                upstream_requests: 0,
                response_cache_hits: 0,
                semantic_cache_hits: 0,
                cache_misses: 0,
                errors: 0,
                retries: 0,
                ttft_samples: VecDeque::new(),
                total_samples: VecDeque::new(),
                provider_cached_tokens: 0,
                provider_input_tokens: 0,
                provider_usage_requests: 0,
                provider_cache_hit_requests: 0,
                eligible_cache_lookups: 0,
                eligible_cache_hits: 0,
                first_seen_eligible_misses: 0,
                repeatable_eligible_lookups: 0,
                repeatable_eligible_hits: 0,
                seen_eligible_cache_keys: HashSet::new(),
                seen_eligible_cache_key_order: VecDeque::new(),
                usage: UsageAccumulator::default(),
                recent_usage: VecDeque::new(),
                cold_start_keys: HashSet::new(),
                request_cold_start_keys: HashSet::new(),
                provider_stats: Vec::new(),
                gap_buckets: Vec::new(),
                request_body_buckets: Vec::new(),
                background_prewarm: Vec::new(),
                local_proxy_estimated_tokens_saved: 0,
                recent_upstream_calls: VecDeque::new(),
                recent_requests: VecDeque::new(),
                recent_failed_requests: VecDeque::new(),
                recent_errors: VecDeque::new(),
                agent_generation: AgentGenerationStats::default(),
                shadow_affinity: ShadowAffinityMetrics::default(),
                active_agent_inbounds: HashMap::new(),
                active_agent_attempts: HashMap::new(),
                completed_agent_inbound_ids: HashSet::new(),
                completed_agent_inbound_order: VecDeque::new(),
                completed_agent_attempt_ids: HashSet::new(),
                completed_agent_attempt_order: VecDeque::new(),
                recent_agent_inbound_outcomes: VecDeque::new(),
                recent_agent_upstream_attempts: VecDeque::new(),
            })),
        }
    }

    pub async fn begin_agent_inbound(&self, mut start: AgentInboundStart) -> bool {
        start.inbound_request_id = start.inbound_request_id.trim().to_string();
        if start.inbound_request_id.is_empty() {
            return false;
        }
        if start.attempt_policy.trim().is_empty() {
            start.attempt_policy = "single".to_string();
        }
        start.attempt_budget = start.attempt_budget.max(1);

        let mut inner = self.inner.write().await;
        if inner
            .active_agent_inbounds
            .contains_key(&start.inbound_request_id)
            || inner
                .completed_agent_inbound_ids
                .contains(&start.inbound_request_id)
        {
            return false;
        }
        inner.active_agent_inbounds.insert(
            start.inbound_request_id,
            ActiveAgentInbound {
                at: start.at,
                attempt_policy: start.attempt_policy,
                attempt_budget: start.attempt_budget,
                attempt_count: 0,
                last_attempt_id: None,
            },
        );
        inner.agent_generation.inbound_requests += 1;
        inner.agent_generation.active_inbounds += 1;
        true
    }

    pub async fn record_shadow_decision(&self, assigned: bool, policy_compute_ms: u64) {
        let mut inner = self.inner.write().await;
        inner.shadow_affinity.decisions += 1;
        inner.shadow_affinity.policy_compute_ms_total += policy_compute_ms;
        if assigned {
            inner.shadow_affinity.assigned_decisions += 1;
        } else {
            inner.shadow_affinity.transparent_decisions += 1;
        }
    }

    pub async fn record_shadow_application(&self, candidate: bool) {
        let mut inner = self.inner.write().await;
        inner.shadow_affinity.applied_decisions += 1;
        if candidate {
            inner.shadow_affinity.candidate_decisions += 1;
        }
    }

    pub async fn record_shadow_observation(
        &self,
        success: bool,
        has_usage: bool,
        inconclusive: bool,
    ) {
        let mut inner = self.inner.write().await;
        inner.shadow_affinity.observations += 1;
        if success {
            inner.shadow_affinity.successful_observations += 1;
        }
        if has_usage {
            inner.shadow_affinity.usage_observations += 1;
        }
        if inconclusive {
            inner.shadow_affinity.inconclusive_observations += 1;
        }
    }

    /// Registers one real upstream POST immediately before transport I/O.
    /// Returns the assigned one-based attempt index, or `None` for a duplicate,
    /// unknown inbound, or exhausted attempt budget.
    pub async fn begin_agent_attempt(&self, mut start: AgentAttemptStart) -> Option<u64> {
        start.inbound_request_id = start.inbound_request_id.trim().to_string();
        start.attempt_id = start.attempt_id.trim().to_string();
        if start.inbound_request_id.is_empty() || start.attempt_id.is_empty() {
            return None;
        }

        let mut inner = self.inner.write().await;
        if inner.active_agent_attempts.contains_key(&start.attempt_id)
            || inner
                .completed_agent_attempt_ids
                .contains(&start.attempt_id)
        {
            return None;
        }
        let (attempt_index, attempt_budget, attempt_policy) = {
            let inbound = inner
                .active_agent_inbounds
                .get_mut(&start.inbound_request_id)?;
            if inbound.attempt_count >= inbound.attempt_budget {
                return None;
            }
            inbound.attempt_count += 1;
            inbound.last_attempt_id = Some(start.attempt_id.clone());
            (
                inbound.attempt_count,
                inbound.attempt_budget,
                inbound.attempt_policy.clone(),
            )
        };
        let provider = start.provider.trim().to_string();
        inner.active_agent_attempts.insert(
            start.attempt_id.clone(),
            ActiveAgentAttempt {
                inbound_request_id: start.inbound_request_id,
                attempt_id: start.attempt_id,
                attempt_index,
                attempt_budget,
                attempt_policy,
                attempt_reason: if start.attempt_reason.trim().is_empty() {
                    "primary".to_string()
                } else {
                    start.attempt_reason
                },
                started_at: start.at,
                provider: provider.clone(),
                model: start.model,
                upstream_channel: start.upstream_channel,
            },
        );
        inner.agent_generation.generation_attempts += 1;
        inner.agent_generation.active_attempts += 1;
        inner.upstream_requests += 1;
        increment_provider_upstream_attempt(&mut inner.provider_stats, &provider);
        Some(attempt_index)
    }

    pub async fn finish_agent_attempt(&self, attempt_id: &str, finish: AgentAttemptFinish) -> bool {
        let attempt_id = attempt_id.trim();
        if attempt_id.is_empty() {
            return false;
        }

        let mut inner = self.inner.write().await;
        if inner.completed_agent_attempt_ids.contains(attempt_id) {
            return false;
        }
        let Some(active) = inner.active_agent_attempts.remove(attempt_id) else {
            return false;
        };
        {
            let MetricsInner {
                completed_agent_attempt_ids,
                completed_agent_attempt_order,
                ..
            } = &mut *inner;
            remember_completed_id(
                completed_agent_attempt_ids,
                completed_agent_attempt_order,
                attempt_id,
            );
        }
        inner.agent_generation.active_attempts =
            inner.agent_generation.active_attempts.saturating_sub(1);
        push_limited(
            &mut inner.recent_agent_upstream_attempts,
            AgentUpstreamAttemptLog {
                inbound_request_id: active.inbound_request_id,
                attempt_id: active.attempt_id,
                attempt_index: active.attempt_index,
                attempt_budget: active.attempt_budget,
                attempt_policy: active.attempt_policy,
                attempt_reason: active.attempt_reason,
                started_at: active.started_at,
                finished_at: finish.finished_at,
                provider: active.provider,
                model: active.model,
                upstream_channel: active.upstream_channel,
                outcome: finish.outcome,
                status: finish.status,
                error_scope: finish.error_scope,
                terminal_state: finish.terminal_state,
                total_ms: finish.total_ms,
                upstream_headers_ms: finish.upstream_headers_ms,
                upstream_network_path: finish.upstream_network_path,
                request_body_bytes: finish.request_body_bytes,
                sent_body_bytes: finish.sent_body_bytes,
                gzip_attempted: finish.gzip_attempted,
            },
            400,
        );
        true
    }

    pub async fn finish_agent_inbound(
        &self,
        inbound_request_id: &str,
        mut request: RequestLog,
        mut outcome: AgentInboundOutcome,
        terminal_state: Option<String>,
    ) -> bool {
        let inbound_request_id = inbound_request_id.trim();
        if inbound_request_id.is_empty() {
            return false;
        }

        let mut inner = self.inner.write().await;
        if inner
            .completed_agent_inbound_ids
            .contains(inbound_request_id)
            || inner
                .active_agent_attempts
                .values()
                .any(|attempt| attempt.inbound_request_id == inbound_request_id)
        {
            return false;
        }
        let Some(active) = inner.active_agent_inbounds.remove(inbound_request_id) else {
            return false;
        };
        {
            let MetricsInner {
                completed_agent_inbound_ids,
                completed_agent_inbound_order,
                ..
            } = &mut *inner;
            remember_completed_id(
                completed_agent_inbound_ids,
                completed_agent_inbound_order,
                inbound_request_id,
            );
        }
        inner.agent_generation.active_inbounds =
            inner.agent_generation.active_inbounds.saturating_sub(1);
        inner.agent_generation.max_attempts_per_inbound = inner
            .agent_generation
            .max_attempts_per_inbound
            .max(active.attempt_count);
        if active.attempt_count > 1 {
            inner.agent_generation.multi_attempt_inbounds += 1;
        }

        request.id = inbound_request_id.to_string();
        request.inbound_request_id = Some(inbound_request_id.to_string());
        request.upstream_request_id = active.last_attempt_id.clone();
        request.upstream_attempt_index = (active.attempt_count > 0).then_some(active.attempt_count);
        request.upstream_attempt_total = Some(active.attempt_count);
        let successful = outcome.is_success() && request_log_is_successful_history(&request);
        if successful {
            inner.agent_generation.successful_inbounds += 1;
        } else {
            if outcome.is_success() {
                outcome = AgentInboundOutcome::HttpError;
            }
            request.cache_status = "error".to_string();
            inner.agent_generation.failed_inbounds += 1;
        }

        let projection = request.clone();
        record_request_inner(&mut inner, projection.clone(), false);
        if successful {
            push_limited(&mut inner.recent_upstream_calls, projection, 400);
        }
        push_limited(
            &mut inner.recent_agent_inbound_outcomes,
            AgentInboundOutcomeLog {
                request,
                started_at: active.at,
                attempt_policy: active.attempt_policy,
                attempt_count: active.attempt_count,
                attempt_budget: active.attempt_budget,
                final_attempt_id: active.last_attempt_id,
                outcome,
                terminal_state,
            },
            200,
        );
        true
    }

    pub async fn record_upstream_call(&self, log: RequestLog) {
        if !request_log_is_successful_history(&log) {
            return;
        }
        let mut inner = self.inner.write().await;
        push_limited(&mut inner.recent_upstream_calls, log, 400);
    }

    pub async fn record_request(&self, log: RequestLog, upstream: bool) {
        let mut inner = self.inner.write().await;
        record_request_inner(&mut inner, log, upstream);
    }

    pub async fn record_upstream_observation(&self, log: RequestLog) {
        let mut inner = self.inner.write().await;
        inner.total_requests += 1;
        if request_log_is_successful_history(&log) {
            inner.successful_requests += 1;
        }
        inner.upstream_requests += 1;
        push_limited(&mut inner.ttft_samples, log.ttft_ms, 512);
        push_limited(&mut inner.total_samples, log.total_ms, 512);
        upsert_gap_bucket(&mut inner.gap_buckets, &log);
        upsert_request_body_bucket(&mut inner.request_body_buckets, &log);
        let count_cold_start = request_log_is_provider_cold_start(&log)
            && request_log_cold_start_key(&log)
                .map(|key| remember_bounded_cold_start_key(&mut inner.request_cold_start_keys, key))
                .unwrap_or(true);
        upsert_provider_traffic(&mut inner.provider_stats, &log, true, count_cold_start);
        if request_log_is_successful_history(&log) {
            push_limited(&mut inner.recent_upstream_calls, log.clone(), 400);
            push_limited(&mut inner.recent_requests, log, 200);
        } else {
            push_limited(&mut inner.recent_failed_requests, log, 200);
        }
    }

    pub async fn record_local_proxy_hit(&self, estimated_tokens_saved: u64) {
        let mut inner = self.inner.write().await;
        inner.local_proxy_estimated_tokens_saved += estimated_tokens_saved;
    }

    pub async fn record_usage(&self, record: UsageRecord) {
        self.record_usage_with_cold_start_key(record, None).await;
    }

    pub async fn record_usage_with_cold_start_key(
        &self,
        record: UsageRecord,
        cold_start_key: Option<&str>,
    ) {
        let mut inner = self.inner.write().await;
        inner.provider_input_tokens += record.input_tokens;
        inner.provider_cached_tokens += record.cache_read_tokens;
        if record.input_tokens > 0 {
            inner.provider_usage_requests += 1;
        }
        if record.cache_read_tokens > 0 {
            inner.provider_cache_hit_requests += 1;
        }
        inner.usage.input_tokens += record.input_tokens;
        inner.usage.output_tokens += record.output_tokens;
        inner.usage.cache_read_tokens += record.cache_read_tokens;
        inner.usage.cache_creation_tokens += record.cache_creation_tokens;
        let count_cold_start = provider_usage_is_cold_start(&record)
            && cold_start_key
                .map(|key| remember_bounded_cold_start_key(&mut inner.cold_start_keys, key))
                .unwrap_or(true);
        if count_cold_start {
            inner.usage.cold_start_requests += 1;
            inner.usage.cold_start_input_tokens += record.input_tokens;
            inner.usage.cold_start_output_tokens += record.output_tokens;
        }
        upsert_usage_group(
            &mut inner.usage.by_provider,
            &record.provider,
            &record,
            count_cold_start,
        );
        upsert_usage_group(
            &mut inner.usage.by_model,
            &record.model,
            &record,
            count_cold_start,
        );
        push_recent_usage(
            &mut inner.recent_usage,
            TimedUsageRecord {
                at: Utc::now(),
                record,
                cold_start_counted: count_cold_start,
            },
        );
    }

    pub async fn record_retry(&self) {
        self.inner.write().await.retries += 1;
    }

    pub async fn record_error(&self, scope: &str, message: &str) {
        let mut inner = self.inner.write().await;
        inner.errors += 1;
        push_limited(
            &mut inner.recent_errors,
            ErrorLog {
                at: Utc::now(),
                scope: scope.to_string(),
                message: message.to_string(),
            },
            40,
        );
    }

    pub async fn snapshot(&self) -> MetricsSnapshot {
        let inner = self.inner.read().await;
        let hits = inner.response_cache_hits + inner.semantic_cache_hits;
        let eligible = hits + inner.cache_misses;
        let overall_hit_rate = ratio(inner.eligible_cache_hits, inner.eligible_cache_lookups);
        let repeatable_hit_rate = ratio(
            inner.repeatable_eligible_hits,
            inner.repeatable_eligible_lookups,
        );
        let recent_usage = recent_usage_stats(&inner.recent_usage, None);
        let provider_cache_request_hit_rate = ratio(
            inner.provider_cache_hit_requests,
            inner.provider_usage_requests,
        );
        let combined_cache_hits = inner.eligible_cache_hits + inner.provider_cache_hit_requests;
        let combined_cache_lookups = inner.eligible_cache_lookups + inner.provider_usage_requests;
        MetricsSnapshot {
            started_at: inner.started_at,
            total_requests: inner.total_requests,
            successful_requests: inner.successful_requests,
            upstream_requests: inner.upstream_requests,
            response_cache_hits: inner.response_cache_hits,
            semantic_cache_hits: inner.semantic_cache_hits,
            cache_misses: inner.cache_misses,
            errors: inner.errors,
            retries: inner.retries,
            ttft_p95_ms: percentile(&inner.ttft_samples, 0.95),
            total_p95_ms: percentile(&inner.total_samples, 0.95),
            provider_cached_tokens: inner.provider_cached_tokens,
            provider_input_tokens: inner.provider_input_tokens,
            provider_cache_hit_requests: inner.provider_cache_hit_requests,
            provider_cache_token_ratio: ratio(
                inner.provider_cached_tokens,
                inner.provider_input_tokens,
            ),
            provider_cache_request_hit_rate,
            combined_cache_hit_rate: ratio(combined_cache_hits, combined_cache_lookups),
            recent_usage,
            eligible_cache_lookups: inner.eligible_cache_lookups,
            eligible_cache_hits: inner.eligible_cache_hits,
            first_seen_eligible_misses: inner.first_seen_eligible_misses,
            repeatable_eligible_lookups: inner.repeatable_eligible_lookups,
            repeatable_eligible_hits: inner.repeatable_eligible_hits,
            overall_eligible_cache_hit_rate: overall_hit_rate,
            repeatable_eligible_cache_hit_rate: repeatable_hit_rate,
            eligible_cache_hit_rate: ratio(hits, eligible),
            usage: inner.usage.snapshot(),
            local_proxy: LocalProxyStats {
                local_cache_hits: hits,
                upstream_requests_saved: hits,
                estimated_tokens_saved: inner.local_proxy_estimated_tokens_saved,
                exact_hits: inner.response_cache_hits,
                semantic_hits: inner.semantic_cache_hits,
                eligible_lookups: inner.eligible_cache_lookups,
                eligible_hits: inner.eligible_cache_hits,
                first_seen_eligible_misses: inner.first_seen_eligible_misses,
                repeatable_eligible_lookups: inner.repeatable_eligible_lookups,
                repeatable_eligible_hits: inner.repeatable_eligible_hits,
                overall_hit_rate,
                repeatable_hit_rate,
            },
            background_prewarm: sorted_background_prewarm(&inner.background_prewarm),
            gap_buckets: sorted_gap_buckets(&inner.gap_buckets),
            request_body_buckets: sorted_request_body_buckets(&inner.request_body_buckets),
            provider_stats: sorted_provider_stats(&inner.provider_stats, &inner.recent_usage),
            recent_upstream_calls: inner.recent_upstream_calls.iter().cloned().collect(),
            recent_requests: inner.recent_requests.iter().cloned().collect(),
            recent_failed_requests: inner.recent_failed_requests.iter().cloned().collect(),
            recent_errors: inner.recent_errors.iter().cloned().collect(),
            agent_generation: inner.agent_generation.clone(),
            shadow_affinity: inner.shadow_affinity.clone(),
            recent_agent_inbound_outcomes: inner
                .recent_agent_inbound_outcomes
                .iter()
                .cloned()
                .collect(),
            recent_agent_upstream_attempts: inner
                .recent_agent_upstream_attempts
                .iter()
                .cloned()
                .collect(),
        }
    }
}

fn record_request_inner(inner: &mut MetricsInner, log: RequestLog, upstream: bool) {
    inner.total_requests += 1;
    if request_log_is_successful_history(&log) {
        inner.successful_requests += 1;
    }
    if upstream {
        inner.upstream_requests += 1;
    }
    match log.cache_status.as_str() {
        "exact" => inner.response_cache_hits += 1,
        "semantic" => inner.semantic_cache_hits += 1,
        "miss" => inner.cache_misses += 1,
        _ => {}
    }

    let cache_hit = matches!(log.cache_status.as_str(), "exact" | "semantic");
    let cache_miss = log.cache_status == "miss";
    if cache_hit || cache_miss {
        inner.eligible_cache_lookups += 1;
        if cache_hit {
            inner.eligible_cache_hits += 1;
        }

        let was_seen = log
            .cache_key
            .as_deref()
            .map(|key| remember_seen_cache_key(inner, key))
            .unwrap_or(false);
        let repeatable = cache_hit || was_seen;
        if repeatable {
            inner.repeatable_eligible_lookups += 1;
            if cache_hit {
                inner.repeatable_eligible_hits += 1;
            }
        } else if cache_miss {
            inner.first_seen_eligible_misses += 1;
        }
    }

    push_limited(&mut inner.ttft_samples, log.ttft_ms, 512);
    push_limited(&mut inner.total_samples, log.total_ms, 512);
    upsert_gap_bucket(&mut inner.gap_buckets, &log);
    upsert_request_body_bucket(&mut inner.request_body_buckets, &log);
    let count_cold_start = request_log_is_provider_cold_start(&log)
        && request_log_cold_start_key(&log)
            .map(|key| remember_bounded_cold_start_key(&mut inner.request_cold_start_keys, key))
            .unwrap_or(true);
    upsert_provider_traffic(&mut inner.provider_stats, &log, upstream, count_cold_start);
    if request_log_is_successful_history(&log) {
        push_limited(&mut inner.recent_requests, log, 200);
    } else {
        push_limited(&mut inner.recent_failed_requests, log, 200);
    }
}

impl ProviderTrafficAccumulator {
    fn snapshot(&self, recent_usage: &VecDeque<TimedUsageRecord>) -> ProviderTrafficStats {
        let eligible = self.cache_hits + self.cache_misses;
        ProviderTrafficStats {
            provider: self.provider.clone(),
            total_requests: self.total_requests,
            successful_requests: self.successful_requests,
            upstream_requests: self.upstream_requests,
            cache_hits: self.cache_hits,
            exact_hits: self.exact_hits,
            semantic_hits: self.semantic_hits,
            cache_misses: self.cache_misses,
            bypassed: self.bypassed,
            error_statuses: self.error_statuses,
            cold_start_requests: self.cold_start_requests,
            cold_start_input_tokens: self.cold_start_input_tokens,
            cold_start_output_tokens: self.cold_start_output_tokens,
            cold_start_total_tokens: self.cold_start_input_tokens + self.cold_start_output_tokens,
            ttft_p95_ms: percentile(&self.ttft_samples, 0.95),
            total_p95_ms: percentile(&self.total_samples, 0.95),
            cache_hit_rate: ratio(self.cache_hits, eligible),
            recent_usage: recent_usage_stats(recent_usage, Some(&self.provider)),
            gap_buckets: sorted_gap_buckets(&self.gap_buckets),
            request_body_buckets: sorted_request_body_buckets(&self.request_body_buckets),
        }
    }
}

impl UsageAccumulator {
    fn snapshot(&self) -> UsageSnapshot {
        UsageSnapshot {
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            cache_creation_tokens: self.cache_creation_tokens,
            total_tokens: self.input_tokens + self.output_tokens,
            cold_start_requests: self.cold_start_requests,
            cold_start_input_tokens: self.cold_start_input_tokens,
            cold_start_output_tokens: self.cold_start_output_tokens,
            cold_start_total_tokens: self.cold_start_input_tokens + self.cold_start_output_tokens,
            by_provider: sorted_usage_groups(&self.by_provider),
            by_model: sorted_usage_groups(&self.by_model),
        }
    }
}

fn upsert_usage_group(
    groups: &mut Vec<UsageGroup>,
    key: &str,
    record: &UsageRecord,
    count_cold_start: bool,
) {
    let key = if key.trim().is_empty() {
        "unknown"
    } else {
        key
    };
    let cold_start_requests = u64::from(count_cold_start);
    let cold_start_input_tokens = if count_cold_start {
        record.input_tokens
    } else {
        0
    };
    let cold_start_output_tokens = if count_cold_start {
        record.output_tokens
    } else {
        0
    };
    let Some(group) = groups.iter_mut().find(|group| group.key == key) else {
        groups.push(UsageGroup {
            key: key.to_string(),
            requests: 1,
            input_tokens: record.input_tokens,
            output_tokens: record.output_tokens,
            cache_read_tokens: record.cache_read_tokens,
            cache_creation_tokens: record.cache_creation_tokens,
            total_tokens: record.input_tokens + record.output_tokens,
            cold_start_requests,
            cold_start_input_tokens,
            cold_start_output_tokens,
            cold_start_total_tokens: cold_start_input_tokens + cold_start_output_tokens,
        });
        return;
    };
    group.requests += 1;
    group.input_tokens += record.input_tokens;
    group.output_tokens += record.output_tokens;
    group.cache_read_tokens += record.cache_read_tokens;
    group.cache_creation_tokens += record.cache_creation_tokens;
    group.total_tokens += record.input_tokens + record.output_tokens;
    group.cold_start_requests += cold_start_requests;
    group.cold_start_input_tokens += cold_start_input_tokens;
    group.cold_start_output_tokens += cold_start_output_tokens;
    group.cold_start_total_tokens += cold_start_input_tokens + cold_start_output_tokens;
}

fn sorted_usage_groups(groups: &[UsageGroup]) -> Vec<UsageGroup> {
    let mut groups = groups.to_vec();
    groups.sort_by(|left, right| {
        right
            .total_tokens
            .cmp(&left.total_tokens)
            .then_with(|| right.requests.cmp(&left.requests))
    });
    groups.truncate(12);
    groups
}

fn upsert_provider_traffic(
    groups: &mut Vec<ProviderTrafficAccumulator>,
    log: &RequestLog,
    upstream: bool,
    count_cold_start: bool,
) {
    let provider = if log.provider.trim().is_empty() {
        "unknown"
    } else {
        log.provider.trim()
    };
    let index = groups
        .iter()
        .position(|group| group.provider == provider)
        .unwrap_or_else(|| {
            groups.push(ProviderTrafficAccumulator {
                provider: provider.to_string(),
                ..ProviderTrafficAccumulator::default()
            });
            groups.len() - 1
        });
    let group = &mut groups[index];
    group.total_requests += 1;
    if request_log_is_successful_history(log) {
        group.successful_requests += 1;
    }
    if upstream {
        group.upstream_requests += 1;
    }
    match log.cache_status.as_str() {
        "exact" => {
            group.cache_hits += 1;
            group.exact_hits += 1;
        }
        "semantic" => {
            group.cache_hits += 1;
            group.semantic_hits += 1;
        }
        "miss" => group.cache_misses += 1,
        "bypass" => group.bypassed += 1,
        _ => {}
    }
    if log.status >= 400 {
        group.error_statuses += 1;
    }
    if count_cold_start {
        group.cold_start_requests += 1;
        group.cold_start_input_tokens += log.input_tokens.unwrap_or_default();
        group.cold_start_output_tokens += 0;
    }
    upsert_gap_bucket(&mut group.gap_buckets, log);
    upsert_request_body_bucket(&mut group.request_body_buckets, log);
    push_limited(&mut group.ttft_samples, log.ttft_ms, 512);
    push_limited(&mut group.total_samples, log.total_ms, 512);
}

fn increment_provider_upstream_attempt(
    groups: &mut Vec<ProviderTrafficAccumulator>,
    provider: &str,
) {
    let provider = if provider.trim().is_empty() {
        "unknown"
    } else {
        provider.trim()
    };
    let index = groups
        .iter()
        .position(|group| group.provider == provider)
        .unwrap_or_else(|| {
            groups.push(ProviderTrafficAccumulator {
                provider: provider.to_string(),
                ..ProviderTrafficAccumulator::default()
            });
            groups.len() - 1
        });
    groups[index].upstream_requests += 1;
}

fn sorted_provider_stats(
    groups: &[ProviderTrafficAccumulator],
    recent_usage: &VecDeque<TimedUsageRecord>,
) -> Vec<ProviderTrafficStats> {
    let mut stats = groups
        .iter()
        .map(|group| group.snapshot(recent_usage))
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| {
        right
            .total_requests
            .cmp(&left.total_requests)
            .then_with(|| left.provider.cmp(&right.provider))
    });
    stats
}

fn push_limited<T>(items: &mut VecDeque<T>, item: T, limit: usize) {
    items.push_front(item);
    while items.len() > limit {
        items.pop_back();
    }
}

fn upsert_gap_bucket(buckets: &mut Vec<GapBucketAccumulator>, log: &RequestLog) {
    if log.status >= 400 || log.input_tokens.unwrap_or_default() == 0 {
        return;
    }
    let total_gap = log.cache_shortfall_tokens.unwrap_or_default();
    let bucket = gap_bucket_label(total_gap);
    let index = buckets
        .iter()
        .position(|item| item.bucket == bucket)
        .unwrap_or_else(|| {
            buckets.push(GapBucketAccumulator {
                bucket: bucket.to_string(),
                ..GapBucketAccumulator::default()
            });
            buckets.len() - 1
        });
    let item = &mut buckets[index];
    item.requests += 1;
    item.total_gap_tokens += total_gap;
    item.new_tail_gap_tokens += log.cache_new_tail_gap_tokens.unwrap_or_default();
    item.avoidable_gap_tokens += log.cache_avoidable_gap_tokens.unwrap_or_default();
    item.provider_unstable_gap_tokens += log.cache_provider_unstable_gap_tokens.unwrap_or_default();
}

fn gap_bucket_label(total_gap: u64) -> &'static str {
    match total_gap {
        0 => "full",
        1..=128 => "1-128",
        129..=512 => "129-512",
        513..=1024 => "513-1024",
        1025..=2048 => "1025-2048",
        2049..=4096 => "2049-4096",
        4097..=8192 => "4097-8192",
        8193..=16_384 => "8193-16384",
        16_385..=32_768 => "16385-32768",
        32_769..=65_536 => "32769-65536",
        65_537..=131_072 => "65537-131072",
        _ => "131073+",
    }
}

fn sorted_gap_buckets(buckets: &[GapBucketAccumulator]) -> Vec<GapBucketStats> {
    let order = [
        "full",
        "1-128",
        "129-512",
        "513-1024",
        "1025-2048",
        "2049-4096",
        "4097-8192",
        "8193-16384",
        "16385-32768",
        "32769-65536",
        "65537-131072",
        "131073+",
    ];
    let mut stats = buckets
        .iter()
        .map(|item| GapBucketStats {
            bucket: item.bucket.clone(),
            requests: item.requests,
            total_gap_tokens: item.total_gap_tokens,
            new_tail_gap_tokens: item.new_tail_gap_tokens,
            avoidable_gap_tokens: item.avoidable_gap_tokens,
            provider_unstable_gap_tokens: item.provider_unstable_gap_tokens,
        })
        .collect::<Vec<_>>();
    stats.sort_by_key(|item| {
        order
            .iter()
            .position(|bucket| *bucket == item.bucket)
            .unwrap_or(order.len())
    });
    stats
}

fn upsert_request_body_bucket(buckets: &mut Vec<RequestBodyBucketAccumulator>, log: &RequestLog) {
    let Some(bytes) = log.request_body_bytes else {
        return;
    };
    if bytes == 0 {
        return;
    }
    let (bucket, risk) = request_body_bucket(bytes);
    let index = buckets
        .iter()
        .position(|item| item.bucket == bucket)
        .unwrap_or_else(|| {
            buckets.push(RequestBodyBucketAccumulator {
                bucket: bucket.to_string(),
                risk: risk.to_string(),
                ..RequestBodyBucketAccumulator::default()
            });
            buckets.len() - 1
        });
    let item = &mut buckets[index];
    item.requests += 1;
    item.total_bytes += bytes;
    item.max_bytes = item.max_bytes.max(bytes);
}

fn request_body_bucket(bytes: u64) -> (&'static str, &'static str) {
    let bucket = match bytes {
        0..=262_144 => "<=256KB",
        262_145..=614_400 => "256KB-600KB",
        614_401..=1_048_576 => "600KB-1MB",
        _ => ">1MB",
    };
    let risk = match bucket {
        "600KB-1MB" => "high",
        ">1MB" => "critical",
        _ => "normal",
    };
    (bucket, risk)
}

fn sorted_request_body_buckets(
    buckets: &[RequestBodyBucketAccumulator],
) -> Vec<RequestBodyBucketStats> {
    let order = ["<=256KB", "256KB-600KB", "600KB-1MB", ">1MB"];
    let mut stats = buckets
        .iter()
        .map(|item| RequestBodyBucketStats {
            bucket: item.bucket.clone(),
            risk: item.risk.clone(),
            requests: item.requests,
            total_bytes: item.total_bytes,
            max_bytes: item.max_bytes,
        })
        .collect::<Vec<_>>();
    stats.sort_by_key(|item| {
        order
            .iter()
            .position(|bucket| *bucket == item.bucket)
            .unwrap_or(order.len())
    });
    stats
}

fn sorted_background_prewarm(
    items: &[BackgroundPrewarmAccumulator],
) -> Vec<BackgroundPrewarmStats> {
    let mut stats = items
        .iter()
        .map(|item| BackgroundPrewarmStats {
            channel: item.channel.clone(),
            attempts: item.attempts,
            successes: item.successes,
            trigger_new_tail_tokens: item.trigger_new_tail_tokens,
            trigger_avoidable_tokens: item.trigger_avoidable_tokens,
            input_tokens: item.input_tokens,
            cache_read_tokens: item.cache_read_tokens,
        })
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| {
        right
            .attempts
            .cmp(&left.attempts)
            .then_with(|| left.channel.cmp(&right.channel))
    });
    stats
}

fn push_recent_usage(items: &mut VecDeque<TimedUsageRecord>, item: TimedUsageRecord) {
    items.push_front(item);
    prune_recent_usage(items);
}

fn prune_recent_usage(items: &mut VecDeque<TimedUsageRecord>) {
    let cutoff = Utc::now() - Duration::minutes(RECENT_USAGE_WINDOW_MINUTES);
    while items.back().is_some_and(|item| item.at < cutoff) {
        items.pop_back();
    }
}

fn recent_usage_stats(
    items: &VecDeque<TimedUsageRecord>,
    provider: Option<&str>,
) -> RecentUsageStats {
    let cutoff = Utc::now() - Duration::minutes(RECENT_USAGE_WINDOW_MINUTES);
    let mut stats = RecentUsageStats {
        window_seconds: RECENT_USAGE_WINDOW_SECONDS,
        ..RecentUsageStats::default()
    };

    for item in items.iter().filter(|item| item.at >= cutoff) {
        if provider.is_some_and(|provider| provider != item.record.provider) {
            continue;
        }
        stats.requests += 1;
        if item.record.cache_read_tokens > 0 {
            stats.cache_hit_requests += 1;
        }
        stats.input_tokens += item.record.input_tokens;
        stats.output_tokens += item.record.output_tokens;
        stats.cache_read_tokens += item.record.cache_read_tokens;
        stats.cache_creation_tokens += item.record.cache_creation_tokens;
        if item.cold_start_counted {
            stats.cold_start_requests += 1;
            stats.cold_start_input_tokens += item.record.input_tokens;
            stats.cold_start_output_tokens += item.record.output_tokens;
            stats.cold_start_total_tokens += item.record.input_tokens + item.record.output_tokens;
        }
    }
    stats.cache_token_ratio = ratio(stats.cache_read_tokens, stats.input_tokens);
    stats.cache_request_hit_rate = ratio(stats.cache_hit_requests, stats.requests);
    stats
}

fn provider_usage_is_cold_start(record: &UsageRecord) -> bool {
    record.input_tokens >= 1024 && record.cache_read_tokens == 0
}

fn request_log_is_provider_cold_start(log: &RequestLog) -> bool {
    log.input_tokens.unwrap_or_default() >= 1024 && log.cache_read_tokens.unwrap_or_default() == 0
}

fn request_log_cold_start_key(log: &RequestLog) -> Option<&str> {
    log.session_anchor_hash
        .as_deref()
        .or(log.provider_prefix_key.as_deref())
}

fn remember_bounded_cold_start_key(keys: &mut HashSet<String>, key: &str) -> bool {
    const COLD_START_KEY_LIMIT: usize = 100_000;
    if keys.len() >= COLD_START_KEY_LIMIT {
        keys.clear();
    }
    if !keys.insert(key.to_string()) {
        return false;
    }
    true
}

fn request_log_is_successful_history(log: &RequestLog) -> bool {
    (200..300).contains(&log.status) && log.cache_status != "error"
}

fn remember_completed_id(ids: &mut HashSet<String>, order: &mut VecDeque<String>, id: &str) {
    const COMPLETED_LIFECYCLE_ID_LIMIT: usize = 4096;

    let id = id.to_string();
    if !ids.insert(id.clone()) {
        return;
    }
    order.push_front(id);
    while order.len() > COMPLETED_LIFECYCLE_ID_LIMIT {
        if let Some(oldest) = order.pop_back() {
            ids.remove(&oldest);
        }
    }
}

fn remember_seen_cache_key(inner: &mut MetricsInner, key: &str) -> bool {
    const SEEN_CACHE_KEY_LIMIT: usize = 300_000;

    let was_seen = inner.seen_eligible_cache_keys.contains(key);
    if !was_seen {
        let owned = key.to_string();
        inner.seen_eligible_cache_keys.insert(owned.clone());
        inner.seen_eligible_cache_key_order.push_front(owned);
        while inner.seen_eligible_cache_key_order.len() > SEEN_CACHE_KEY_LIMIT {
            if let Some(oldest) = inner.seen_eligible_cache_key_order.pop_back() {
                inner.seen_eligible_cache_keys.remove(&oldest);
            }
        }
    }
    was_seen
}

fn percentile(samples: &VecDeque<u64>, pct: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut values = samples.iter().copied().collect::<Vec<_>>();
    values.sort_unstable();
    let index = ((values.len() as f64 - 1.0) * pct).round() as usize;
    values[index]
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_log(cache_status: &str, cache_key: Option<&str>) -> RequestLog {
        RequestLog {
            id: format!("request-{cache_status}"),
            at: Utc::now(),
            inbound_request_id: None,
            upstream_request_id: None,
            upstream_attempt_index: None,
            upstream_attempt_total: None,
            client_channel: "chat".to_string(),
            upstream_channel: "chat".to_string(),
            provider: "provider".to_string(),
            model: "model".to_string(),
            requested_model: None,
            agent_reasoning_effort: None,
            configured_reasoning_effort: None,
            effective_reasoning_effort: None,
            reasoning_effort_source: None,
            cache_status: cache_status.to_string(),
            agent_id: None,
            agent_label: None,
            upstream_call_kind: None,
            upstream_call_source: None,
            cache_key: cache_key.map(ToOwned::to_owned),
            provider_prefix_key: None,
            provider_prefix_fingerprint: None,
            outbound_prefix_fingerprints: None,
            provider_cache_diagnostic: None,
            shadow_affinity_mode: None,
            shadow_affinity_arm: None,
            shadow_affinity_realm_id: None,
            shadow_affinity_cohort_id: None,
            shadow_affinity_lane: None,
            shadow_affinity_shard: None,
            shadow_affinity_policy_epoch: None,
            shadow_affinity_anchor_epoch: None,
            shadow_affinity_trusted_identity: None,
            shadow_affinity_decision: None,
            shadow_affinity_skip_reason: None,
            shadow_affinity_policy_compute_ms: None,
            prefix_guard_wait_ms: None,
            prefix_guard_wait_reason: None,
            prefix_guard_wait_source: None,
            prefix_guard_state_age_ms: None,
            prefix_guard_skip_reason: None,
            prefix_guard_wait_effect: None,
            prefix_lag_classification: None,
            prefix_lag_input_delta_tokens: None,
            prefix_lag_cache_delta_tokens: None,
            prefix_lag_previous_gap_tokens: None,
            prefix_cache_instability_score: None,
            prefix_seen_bucket_tokens: None,
            prefix_state_cache_read_tokens: None,
            status: 200,
            ttft_ms: 1,
            upstream_ttft_ms: None,
            local_prepare_ms: None,
            upstream_headers_ms: None,
            upstream_last_attempt_headers_ms: None,
            upstream_http_version: None,
            upstream_network_path: None,
            upstream_remote_addr: None,
            upstream_pool_diagnostic: None,
            upstream_trace_id: None,
            upstream_trace_source: None,
            upstream_server_timing: None,
            upstream_timing_source: None,
            upstream_reported_processing_ms: None,
            upstream_non_processing_ms: None,
            upstream_first_chunk_ms: None,
            stream_upstream_wait_ms: None,
            stream_client_backpressure_ms: None,
            aggregate_done_ms: None,
            upstream_retry_wait_ms: None,
            upstream_attempts: None,
            request_body_bytes: None,
            sent_body_bytes: None,
            request_body_encode_ms: None,
            gzip_encode_ms: None,
            gzip_attempted: None,
            gzip_fallback_used: None,
            upstream_header_wait_class: None,
            total_ms: 2,
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_shortfall_tokens: None,
            cache_new_tail_gap_tokens: None,
            cache_avoidable_gap_tokens: None,
            cache_provider_unstable_gap_tokens: None,
            provider_cache_token_ratio: None,
            tail_input_items: None,
            tail_message_chars: None,
            tail_tool_call_chars: None,
            tail_tool_output_chars: None,
            tail_largest_tool_output_chars: None,
            tail_tool_output_lines: None,
            tail_tool_output_repeated_line_chars: None,
            tail_tool_output_timestamp_like_count: None,
            tail_tool_output_path_like_count: None,
            tail_tool_output_url_like_count: None,
            tail_tool_output_hash_like_count: None,
            tail_tool_output_json_like_chars: None,
            tail_tool_output_noise_hint: None,
            tail_source: None,
            response_session_reused: None,
            response_session_candidate_count: None,
            response_session_skip_reason: None,
            response_session_exact_key_hit: None,
            response_session_scope_match_count: None,
            response_session_append_delta_match: None,
            response_session_delta_items: None,
            response_session_cooldown_active: None,
            response_session_rejected_status: None,
            session_anchor_hash: None,
            session_anchor_source: None,
            session_anchor_changed: None,
            session_anchor_peer_count: None,
            original_body_bytes: None,
            send_body_bytes: None,
            send_body_is_delta: None,
            payload_too_large_rescue_attempted: None,
            payload_too_large_rescue_used: None,
            sse_end_reason: None,
            downstream_disconnected: None,
            downstream_disconnect_stage: None,
            sse_completed_event_seen: None,
            sse_done_marker_seen: None,
            sse_chunks: None,
        }
    }

    #[test]
    fn request_log_shadow_fields_default_for_legacy_metrics() {
        let log = request_log("miss", Some("cache-key"));
        let mut value = serde_json::to_value(log).unwrap();
        let object = value.as_object_mut().unwrap();
        for key in [
            "shadow_affinity_mode",
            "shadow_affinity_arm",
            "shadow_affinity_realm_id",
            "shadow_affinity_cohort_id",
            "shadow_affinity_lane",
            "shadow_affinity_shard",
            "shadow_affinity_policy_epoch",
            "shadow_affinity_anchor_epoch",
            "shadow_affinity_trusted_identity",
            "shadow_affinity_decision",
            "shadow_affinity_skip_reason",
            "shadow_affinity_policy_compute_ms",
        ] {
            object.remove(key);
        }
        let restored: RequestLog = serde_json::from_value(value).unwrap();
        assert!(restored.shadow_affinity_mode.is_none());
        assert!(restored.shadow_affinity_policy_compute_ms.is_none());
    }

    fn agent_inbound_start(id: &str, policy: &str, budget: u64) -> AgentInboundStart {
        AgentInboundStart {
            inbound_request_id: id.to_string(),
            at: Utc::now(),
            attempt_policy: policy.to_string(),
            attempt_budget: budget,
        }
    }

    fn agent_attempt_start(inbound_id: &str, attempt_id: &str, reason: &str) -> AgentAttemptStart {
        AgentAttemptStart {
            inbound_request_id: inbound_id.to_string(),
            attempt_id: attempt_id.to_string(),
            at: Utc::now(),
            attempt_reason: reason.to_string(),
            provider: "provider".to_string(),
            model: "model".to_string(),
            upstream_channel: "responses".to_string(),
        }
    }

    fn agent_attempt_finish(
        outcome: AgentAttemptOutcome,
        status: Option<u16>,
    ) -> AgentAttemptFinish {
        AgentAttemptFinish {
            finished_at: Utc::now(),
            outcome,
            status,
            error_scope: None,
            terminal_state: None,
            total_ms: 10,
            upstream_headers_ms: Some(5),
            upstream_network_path: Some("direct".to_string()),
            request_body_bytes: Some(128),
            sent_body_bytes: Some(128),
            gzip_attempted: Some(false),
        }
    }

    #[tokio::test]
    async fn metrics_separate_overall_and_repeatable_cache_hit_rates() {
        let metrics = MetricsStore::new();

        metrics
            .record_request(request_log("miss", Some("cache-key-a")), true)
            .await;
        let first = metrics.snapshot().await;
        assert_eq!(first.eligible_cache_lookups, 1);
        assert_eq!(first.first_seen_eligible_misses, 1);
        assert_eq!(first.repeatable_eligible_lookups, 0);
        assert_eq!(first.overall_eligible_cache_hit_rate, 0.0);

        metrics
            .record_request(request_log("exact", Some("cache-key-a")), false)
            .await;
        let second = metrics.snapshot().await;
        assert_eq!(second.eligible_cache_lookups, 2);
        assert_eq!(second.eligible_cache_hits, 1);
        assert_eq!(second.first_seen_eligible_misses, 1);
        assert_eq!(second.repeatable_eligible_lookups, 1);
        assert_eq!(second.repeatable_eligible_hits, 1);
        assert_eq!(second.overall_eligible_cache_hit_rate, 0.5);
        assert_eq!(second.repeatable_eligible_cache_hit_rate, 1.0);
    }

    #[tokio::test]
    async fn provider_cache_request_hit_rate_ignores_requests_without_usage() {
        let metrics = MetricsStore::new();
        let mut error_log = request_log("error", None);
        error_log.status = 429;

        metrics.record_request(error_log, true).await;
        metrics
            .record_usage(UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 30_000,
                output_tokens: 100,
                cache_read_tokens: 29_744,
                cache_creation_tokens: 256,
            })
            .await;

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.upstream_requests, 1);
        assert_eq!(snapshot.provider_cache_hit_requests, 1);
        assert_eq!(snapshot.provider_cache_request_hit_rate, 1.0);
    }

    #[tokio::test]
    async fn cold_start_is_counted_once_per_provider_prefix() {
        let metrics = MetricsStore::new();
        for key in [Some("prefix-a"), Some("prefix-a"), Some("prefix-b")] {
            metrics
                .record_usage_with_cold_start_key(
                    UsageRecord {
                        provider: "provider".to_string(),
                        model: "model".to_string(),
                        input_tokens: 30_000,
                        output_tokens: 100,
                        cache_read_tokens: 0,
                        cache_creation_tokens: 0,
                    },
                    key,
                )
                .await;
        }

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.usage.cold_start_requests, 2);
        assert_eq!(snapshot.recent_usage.cold_start_requests, 2);
        assert_eq!(snapshot.usage.by_provider[0].cold_start_requests, 2);
        assert_eq!(snapshot.usage.by_model[0].cold_start_requests, 2);
    }

    #[tokio::test]
    async fn failed_upstream_attempts_are_not_added_to_success_history() {
        let metrics = MetricsStore::new();
        let mut failed = request_log("error", None);
        failed.status = 503;
        metrics.record_upstream_call(failed.clone()).await;
        metrics.record_request(failed, true).await;

        let successful = request_log("miss", Some("success-history"));
        metrics.record_upstream_call(successful.clone()).await;
        metrics.record_request(successful, true).await;

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 2);
        assert_eq!(snapshot.successful_requests, 1);
        assert_eq!(snapshot.upstream_requests, 2);
        assert_eq!(snapshot.recent_upstream_calls.len(), 1);
        assert_eq!(snapshot.recent_requests.len(), 1);
        assert_eq!(snapshot.recent_failed_requests.len(), 1);
        assert_eq!(snapshot.recent_upstream_calls[0].status, 200);
        assert_eq!(snapshot.recent_requests[0].status, 200);
        assert_eq!(snapshot.recent_failed_requests[0].status, 503);
        assert_eq!(snapshot.provider_stats[0].successful_requests, 1);
        assert_eq!(snapshot.provider_stats[0].error_statuses, 1);
    }

    #[tokio::test]
    async fn sync_compact_with_usage_is_counted_in_true_upstream_metrics() {
        let metrics = MetricsStore::new();
        let mut compact_log = request_log("miss", Some("compact-sync"));
        compact_log.upstream_call_kind = Some("sync".to_string());
        compact_log.upstream_call_source = Some("responses-sync-main".to_string());
        compact_log.upstream_attempt_total = Some(2);
        compact_log.ttft_ms = 144_000;
        compact_log.total_ms = 144_000;
        compact_log.input_tokens = Some(9_519);
        compact_log.cache_read_tokens = Some(0);
        compact_log.cache_shortfall_tokens = Some(9_216);
        compact_log.cache_new_tail_gap_tokens = Some(9_216);

        metrics.record_request(compact_log, true).await;

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.upstream_requests, 1);
        assert_eq!(snapshot.provider_stats[0].total_requests, 1);
        assert_eq!(snapshot.provider_stats[0].upstream_requests, 1);
        assert_eq!(snapshot.ttft_p95_ms, 144_000);
        assert_eq!(snapshot.gap_buckets[0].bucket, "8193-16384");
        assert_eq!(snapshot.provider_stats[0].ttft_p95_ms, 144_000);
        assert_eq!(
            snapshot.provider_stats[0].gap_buckets[0].bucket,
            "8193-16384"
        );
        assert_eq!(snapshot.provider_stats[0].cold_start_requests, 1);
        assert_eq!(
            snapshot.recent_requests[0].upstream_call_source.as_deref(),
            Some("responses-sync-main")
        );
    }

    #[tokio::test]
    async fn metrics_groups_provider_gap_buckets() {
        let metrics = MetricsStore::new();
        let mut small_gap = request_log("miss", Some("small-gap"));
        small_gap.input_tokens = Some(105_524);
        small_gap.cache_read_tokens = Some(104_960);
        small_gap.cache_shortfall_tokens = Some(512);
        small_gap.cache_new_tail_gap_tokens = Some(512);
        small_gap.cache_avoidable_gap_tokens = Some(0);
        metrics.record_request(small_gap, true).await;

        let mut avoidable_gap = request_log("miss", Some("avoidable-gap"));
        avoidable_gap.input_tokens = Some(112_763);
        avoidable_gap.cache_read_tokens = Some(111_104);
        avoidable_gap.cache_shortfall_tokens = Some(1536);
        avoidable_gap.cache_new_tail_gap_tokens = Some(1024);
        avoidable_gap.cache_avoidable_gap_tokens = Some(512);
        metrics.record_request(avoidable_gap, true).await;

        let mut provider_rollback = request_log("miss", Some("provider-rollback"));
        provider_rollback.input_tokens = Some(134_549);
        provider_rollback.cache_read_tokens = Some(130_432);
        provider_rollback.cache_shortfall_tokens = Some(3712);
        provider_rollback.cache_new_tail_gap_tokens = Some(640);
        provider_rollback.cache_avoidable_gap_tokens = Some(0);
        provider_rollback.cache_provider_unstable_gap_tokens = Some(3072);
        metrics.record_request(provider_rollback, true).await;

        let snapshot = metrics.snapshot().await;
        let small = snapshot
            .gap_buckets
            .iter()
            .find(|bucket| bucket.bucket == "129-512")
            .expect("small gap bucket should exist");
        assert_eq!(small.requests, 1);
        assert_eq!(small.new_tail_gap_tokens, 512);

        let medium = snapshot
            .gap_buckets
            .iter()
            .find(|bucket| bucket.bucket == "1025-2048")
            .expect("medium gap bucket should exist");
        assert_eq!(medium.requests, 1);
        assert_eq!(medium.avoidable_gap_tokens, 512);

        let rollback = snapshot
            .gap_buckets
            .iter()
            .find(|bucket| bucket.bucket == "2049-4096")
            .expect("provider rollback bucket should exist");
        assert_eq!(rollback.new_tail_gap_tokens, 640);
        assert_eq!(rollback.avoidable_gap_tokens, 0);
        assert_eq!(rollback.provider_unstable_gap_tokens, 3072);
    }

    #[tokio::test]
    async fn agent_generation_records_one_inbound_and_one_attempt_idempotently() {
        let metrics = MetricsStore::new();
        assert!(
            metrics
                .begin_agent_inbound(agent_inbound_start("inbound-1", "single", 1))
                .await
        );
        assert!(
            !metrics
                .begin_agent_inbound(agent_inbound_start("inbound-1", "single", 1))
                .await
        );
        assert_eq!(
            metrics
                .begin_agent_attempt(agent_attempt_start("inbound-1", "attempt-1", "primary"))
                .await,
            Some(1)
        );
        assert_eq!(
            metrics
                .begin_agent_attempt(agent_attempt_start(
                    "inbound-1",
                    "attempt-over-budget",
                    "primary"
                ))
                .await,
            None
        );
        assert!(
            metrics
                .finish_agent_attempt(
                    "attempt-1",
                    agent_attempt_finish(AgentAttemptOutcome::HttpSuccess, Some(200))
                )
                .await
        );
        assert!(
            !metrics
                .finish_agent_attempt(
                    "attempt-1",
                    agent_attempt_finish(AgentAttemptOutcome::HttpSuccess, Some(200))
                )
                .await
        );

        let log = request_log("miss", Some("agent-success"));
        assert!(
            metrics
                .finish_agent_inbound(
                    "inbound-1",
                    log.clone(),
                    AgentInboundOutcome::Success,
                    Some("response_completed".to_string())
                )
                .await
        );
        assert!(
            !metrics
                .finish_agent_inbound(
                    "inbound-1",
                    log,
                    AgentInboundOutcome::Success,
                    Some("response_completed".to_string())
                )
                .await
        );

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 1);
        assert_eq!(snapshot.upstream_requests, 1);
        assert_eq!(snapshot.agent_generation.inbound_requests, 1);
        assert_eq!(snapshot.agent_generation.successful_inbounds, 1);
        assert_eq!(snapshot.agent_generation.failed_inbounds, 0);
        assert_eq!(snapshot.agent_generation.generation_attempts, 1);
        assert_eq!(snapshot.agent_generation.active_inbounds, 0);
        assert_eq!(snapshot.agent_generation.active_attempts, 0);
        assert_eq!(snapshot.recent_requests.len(), 1);
        assert_eq!(snapshot.recent_upstream_calls.len(), 1);
        assert_eq!(snapshot.recent_failed_requests.len(), 0);
        assert_eq!(snapshot.recent_agent_inbound_outcomes.len(), 1);
        assert_eq!(snapshot.recent_agent_upstream_attempts.len(), 1);
        assert_eq!(snapshot.recent_requests[0].id, "inbound-1");
        assert_eq!(snapshot.recent_upstream_calls[0].id, "inbound-1");
        assert_eq!(
            snapshot.recent_requests[0].upstream_request_id.as_deref(),
            Some("attempt-1")
        );
        assert_eq!(snapshot.recent_requests[0].upstream_attempt_index, Some(1));
        assert_eq!(snapshot.recent_requests[0].upstream_attempt_total, Some(1));
    }

    #[tokio::test]
    async fn reasoning_compatibility_counts_two_attempts_as_one_successful_inbound() {
        let metrics = MetricsStore::new();
        assert!(
            metrics
                .begin_agent_inbound(agent_inbound_start(
                    "reasoning-inbound",
                    "reasoning_compatibility",
                    2
                ))
                .await
        );
        assert_eq!(
            metrics
                .begin_agent_attempt(agent_attempt_start(
                    "reasoning-inbound",
                    "reasoning-attempt-1",
                    "primary"
                ))
                .await,
            Some(1)
        );
        assert!(
            metrics
                .finish_agent_attempt(
                    "reasoning-attempt-1",
                    agent_attempt_finish(AgentAttemptOutcome::HttpError, Some(502))
                )
                .await
        );
        assert_eq!(
            metrics
                .begin_agent_attempt(agent_attempt_start(
                    "reasoning-inbound",
                    "reasoning-attempt-2",
                    "reasoning_explicit"
                ))
                .await,
            Some(2)
        );
        assert!(
            metrics
                .finish_agent_attempt(
                    "reasoning-attempt-2",
                    agent_attempt_finish(AgentAttemptOutcome::HttpSuccess, Some(200))
                )
                .await
        );

        assert!(
            metrics
                .finish_agent_inbound(
                    "reasoning-inbound",
                    request_log("miss", Some("reasoning-success")),
                    AgentInboundOutcome::Success,
                    Some("response_completed".to_string())
                )
                .await
        );

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 1);
        assert_eq!(snapshot.upstream_requests, 2);
        assert_eq!(snapshot.agent_generation.inbound_requests, 1);
        assert_eq!(snapshot.agent_generation.generation_attempts, 2);
        assert_eq!(snapshot.agent_generation.multi_attempt_inbounds, 1);
        assert_eq!(snapshot.agent_generation.max_attempts_per_inbound, 2);
        assert_eq!(snapshot.recent_requests.len(), 1);
        assert_eq!(snapshot.recent_failed_requests.len(), 0);
        assert_eq!(snapshot.recent_agent_upstream_attempts.len(), 2);
        assert_eq!(snapshot.provider_stats[0].total_requests, 1);
        assert_eq!(snapshot.provider_stats[0].successful_requests, 1);
        assert_eq!(snapshot.provider_stats[0].upstream_requests, 2);
        assert_eq!(snapshot.recent_requests[0].upstream_attempt_index, Some(2));
        assert_eq!(snapshot.recent_requests[0].upstream_attempt_total, Some(2));
        assert_eq!(
            snapshot.recent_requests[0].upstream_request_id.as_deref(),
            Some("reasoning-attempt-2")
        );
    }

    #[tokio::test]
    async fn agent_transport_failure_finishes_once_without_success_history() {
        let metrics = MetricsStore::new();
        assert!(
            metrics
                .begin_agent_inbound(agent_inbound_start("failed-inbound", "single", 1))
                .await
        );
        assert_eq!(
            metrics
                .begin_agent_attempt(agent_attempt_start(
                    "failed-inbound",
                    "failed-attempt",
                    "primary"
                ))
                .await,
            Some(1)
        );
        assert!(
            !metrics
                .finish_agent_inbound(
                    "failed-inbound",
                    request_log("error", None),
                    AgentInboundOutcome::TransportError,
                    Some("transport_error".to_string())
                )
                .await
        );
        assert!(
            metrics
                .finish_agent_attempt(
                    "failed-attempt",
                    agent_attempt_finish(AgentAttemptOutcome::TransportError, None)
                )
                .await
        );
        let mut failed_log = request_log("error", None);
        failed_log.status = 0;
        assert!(
            metrics
                .finish_agent_inbound(
                    "failed-inbound",
                    failed_log,
                    AgentInboundOutcome::TransportError,
                    Some("transport_error".to_string())
                )
                .await
        );

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 0);
        assert_eq!(snapshot.upstream_requests, 1);
        assert_eq!(snapshot.agent_generation.failed_inbounds, 1);
        assert_eq!(snapshot.recent_requests.len(), 0);
        assert_eq!(snapshot.recent_upstream_calls.len(), 0);
        assert_eq!(snapshot.recent_failed_requests.len(), 1);
        assert_eq!(snapshot.recent_agent_inbound_outcomes.len(), 1);
        assert_eq!(snapshot.recent_agent_upstream_attempts.len(), 1);
        assert_eq!(
            snapshot.recent_agent_upstream_attempts[0].outcome,
            AgentAttemptOutcome::TransportError
        );
    }

    #[tokio::test]
    async fn shadow_affinity_metrics_keep_decisions_and_observation_coverage() {
        let metrics = MetricsStore::new();
        metrics.record_shadow_decision(true, 3).await;
        metrics.record_shadow_decision(false, 1).await;
        metrics.record_shadow_observation(true, true, false).await;
        metrics.record_shadow_observation(false, false, true).await;

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.shadow_affinity.decisions, 2);
        assert_eq!(snapshot.shadow_affinity.assigned_decisions, 1);
        assert_eq!(snapshot.shadow_affinity.transparent_decisions, 1);
        assert_eq!(snapshot.shadow_affinity.applied_decisions, 0);
        assert_eq!(snapshot.shadow_affinity.candidate_decisions, 0);
        assert_eq!(snapshot.shadow_affinity.observations, 2);
        assert_eq!(snapshot.shadow_affinity.successful_observations, 1);
        assert_eq!(snapshot.shadow_affinity.usage_observations, 1);
        assert_eq!(snapshot.shadow_affinity.inconclusive_observations, 1);
        assert_eq!(snapshot.shadow_affinity.policy_compute_ms_total, 4);
    }
}
