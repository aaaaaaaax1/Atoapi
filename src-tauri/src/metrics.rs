use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::RwLock;

const RECENT_USAGE_WINDOW_MINUTES: i64 = 30;
const RECENT_USAGE_WINDOW_SECONDS: u64 = 30 * 60;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub started_at: DateTime<Utc>,
    pub total_requests: u64,
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
    pub recent_errors: Vec<ErrorLog>,
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
    pub provider_cache_diagnostic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_wait_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_guard_wait_source: Option<String>,
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
    provider_stats: Vec<ProviderTrafficAccumulator>,
    gap_buckets: Vec<GapBucketAccumulator>,
    request_body_buckets: Vec<RequestBodyBucketAccumulator>,
    background_prewarm: Vec<BackgroundPrewarmAccumulator>,
    local_proxy_estimated_tokens_saved: u64,
    recent_upstream_calls: VecDeque<RequestLog>,
    recent_requests: VecDeque<RequestLog>,
    recent_errors: VecDeque<ErrorLog>,
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
}

#[derive(Debug, Clone, Default)]
struct ProviderTrafficAccumulator {
    provider: String,
    total_requests: u64,
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
                provider_stats: Vec::new(),
                gap_buckets: Vec::new(),
                request_body_buckets: Vec::new(),
                background_prewarm: Vec::new(),
                local_proxy_estimated_tokens_saved: 0,
                recent_upstream_calls: VecDeque::new(),
                recent_requests: VecDeque::new(),
                recent_errors: VecDeque::new(),
            })),
        }
    }

    pub async fn record_upstream_call(&self, log: RequestLog) {
        let mut inner = self.inner.write().await;
        push_limited(&mut inner.recent_upstream_calls, log, 400);
    }

    pub async fn record_request(&self, log: RequestLog, upstream: bool) {
        let mut inner = self.inner.write().await;
        let upstream_attempts = if upstream {
            request_log_upstream_attempts(&log)
        } else {
            1
        };
        inner.total_requests += upstream_attempts;
        if upstream {
            inner.upstream_requests += upstream_attempts;
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
                .map(|key| remember_seen_cache_key(&mut inner, key))
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
        upsert_provider_traffic(&mut inner.provider_stats, &log, upstream);
        push_limited(&mut inner.recent_requests, log, 200);
    }

    pub async fn record_upstream_observation(&self, log: RequestLog) {
        let mut inner = self.inner.write().await;
        let upstream_attempts = request_log_upstream_attempts(&log);
        inner.total_requests += upstream_attempts;
        inner.upstream_requests += upstream_attempts;
        push_limited(&mut inner.ttft_samples, log.ttft_ms, 512);
        push_limited(&mut inner.total_samples, log.total_ms, 512);
        upsert_gap_bucket(&mut inner.gap_buckets, &log);
        upsert_request_body_bucket(&mut inner.request_body_buckets, &log);
        upsert_provider_traffic(&mut inner.provider_stats, &log, true);
        push_limited(&mut inner.recent_upstream_calls, log.clone(), 400);
        push_limited(&mut inner.recent_requests, log, 200);
    }

    pub async fn record_local_proxy_hit(&self, estimated_tokens_saved: u64) {
        let mut inner = self.inner.write().await;
        inner.local_proxy_estimated_tokens_saved += estimated_tokens_saved;
    }

    pub async fn record_usage(&self, record: UsageRecord) {
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
        if provider_usage_is_cold_start(&record) {
            inner.usage.cold_start_requests += 1;
            inner.usage.cold_start_input_tokens += record.input_tokens;
            inner.usage.cold_start_output_tokens += record.output_tokens;
        }
        upsert_usage_group(&mut inner.usage.by_provider, &record.provider, &record);
        upsert_usage_group(&mut inner.usage.by_model, &record.model, &record);
        push_recent_usage(
            &mut inner.recent_usage,
            TimedUsageRecord {
                at: Utc::now(),
                record,
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
            recent_errors: inner.recent_errors.iter().cloned().collect(),
        }
    }
}

impl ProviderTrafficAccumulator {
    fn snapshot(&self, recent_usage: &VecDeque<TimedUsageRecord>) -> ProviderTrafficStats {
        let eligible = self.cache_hits + self.cache_misses;
        ProviderTrafficStats {
            provider: self.provider.clone(),
            total_requests: self.total_requests,
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

fn upsert_usage_group(groups: &mut Vec<UsageGroup>, key: &str, record: &UsageRecord) {
    let key = if key.trim().is_empty() {
        "unknown"
    } else {
        key
    };
    let is_cold_start = provider_usage_is_cold_start(record);
    let cold_start_requests = u64::from(is_cold_start);
    let cold_start_input_tokens = if is_cold_start {
        record.input_tokens
    } else {
        0
    };
    let cold_start_output_tokens = if is_cold_start {
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
    let upstream_attempts = if upstream {
        request_log_upstream_attempts(log)
    } else {
        1
    };
    group.total_requests += upstream_attempts;
    if upstream {
        group.upstream_requests += upstream_attempts;
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
    if request_log_is_provider_cold_start(log) {
        group.cold_start_requests += 1;
        group.cold_start_input_tokens += log.input_tokens.unwrap_or_default();
        group.cold_start_output_tokens += 0;
    }
    upsert_gap_bucket(&mut group.gap_buckets, log);
    upsert_request_body_bucket(&mut group.request_body_buckets, log);
    push_limited(&mut group.ttft_samples, log.ttft_ms, 512);
    push_limited(&mut group.total_samples, log.total_ms, 512);
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
        if provider_usage_is_cold_start(&item.record) {
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

fn request_log_upstream_attempts(log: &RequestLog) -> u64 {
    log.upstream_attempt_total
        .or(log.upstream_attempts)
        .unwrap_or(1)
        .max(1)
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
            provider_cache_diagnostic: None,
            prefix_guard_wait_ms: None,
            prefix_guard_wait_reason: None,
            prefix_guard_wait_source: None,
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
            sse_completed_event_seen: None,
            sse_done_marker_seen: None,
            sse_chunks: None,
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
        assert_eq!(snapshot.total_requests, 2);
        assert_eq!(snapshot.upstream_requests, 2);
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
}
