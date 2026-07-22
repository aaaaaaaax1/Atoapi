use crate::metrics_history::{
    MetricsHistory, MetricsHistoryObservation, MetricsTrendQueryInput, MetricsTrendSnapshot,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex},
};
use tokio::sync::{Notify, RwLock};

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
    #[serde(default)]
    pub agent_provider_stats: Vec<AgentProviderTrafficStats>,
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

#[derive(Debug, Clone)]
enum MetricsTerminal {
    Upstream {
        request: RequestLog,
        upstream_attempts: u64,
    },
    LocalCache {
        request: RequestLog,
        estimated_tokens_saved: u64,
    },
    LocalRejection {
        request: RequestLog,
    },
    Agent {
        inbound_request_id: String,
        attempt_id: String,
        attempt_finish: AgentAttemptFinish,
        request: RequestLog,
        inbound_outcome: AgentInboundOutcome,
        terminal_state: Option<String>,
    },
    AgentOwnerFailure {
        inbound_request_id: String,
        request: RequestLog,
        terminal_state: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct AgentTerminalSettlement {
    pub inbound_request_id: String,
    pub attempt_id: String,
    pub attempt_finish: AgentAttemptFinish,
    pub request: RequestLog,
    pub inbound_outcome: AgentInboundOutcome,
    pub terminal_state: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AgentOwnerFailureSettlement {
    pub inbound_request_id: String,
    pub request: RequestLog,
    pub terminal_state: Option<String>,
}

#[derive(Debug, Clone)]
struct PendingUsage {
    observed_at: DateTime<Utc>,
    record: UsageRecord,
    cold_start_key: Option<String>,
}

#[derive(Debug, Clone)]
struct PendingError {
    at: DateTime<Utc>,
    scope: String,
    message: String,
}

#[derive(Debug, Clone)]
pub struct MetricsTransaction {
    commit_key: String,
    terminal: MetricsTerminal,
    usage: Option<PendingUsage>,
    errors: Vec<PendingError>,
}

impl MetricsTransaction {
    pub fn upstream(request: RequestLog) -> Self {
        let upstream_attempts = request
            .upstream_attempts
            .or(request.upstream_attempt_total)
            .unwrap_or(1)
            .max(1);
        Self {
            commit_key: metrics_transaction_key(&request.id),
            terminal: MetricsTerminal::Upstream {
                request,
                upstream_attempts,
            },
            usage: None,
            errors: Vec::new(),
        }
    }

    pub fn local_cache(request: RequestLog, estimated_tokens_saved: u64) -> Self {
        Self {
            commit_key: metrics_transaction_key(&request.id),
            terminal: MetricsTerminal::LocalCache {
                request,
                estimated_tokens_saved,
            },
            usage: None,
            errors: Vec::new(),
        }
    }

    pub fn local_rejection(request: RequestLog) -> Self {
        Self {
            commit_key: metrics_transaction_key(&request.id),
            terminal: MetricsTerminal::LocalRejection { request },
            usage: None,
            errors: Vec::new(),
        }
    }

    pub fn agent_terminal(mut settlement: AgentTerminalSettlement) -> Self {
        let inbound_request_id = settlement.inbound_request_id.trim().to_string();
        settlement.request.id = inbound_request_id.clone();
        settlement.request.inbound_request_id = Some(inbound_request_id.clone());
        Self {
            commit_key: metrics_transaction_key(&inbound_request_id),
            terminal: MetricsTerminal::Agent {
                inbound_request_id,
                attempt_id: settlement.attempt_id.trim().to_string(),
                attempt_finish: settlement.attempt_finish,
                request: settlement.request,
                inbound_outcome: settlement.inbound_outcome,
                terminal_state: settlement.terminal_state,
            },
            usage: None,
            errors: Vec::new(),
        }
    }

    pub fn agent_owner_failure(mut settlement: AgentOwnerFailureSettlement) -> Self {
        let inbound_request_id = settlement.inbound_request_id.trim().to_string();
        settlement.request.id = inbound_request_id.clone();
        settlement.request.inbound_request_id = Some(inbound_request_id.clone());
        Self {
            commit_key: metrics_transaction_key(&inbound_request_id),
            terminal: MetricsTerminal::AgentOwnerFailure {
                inbound_request_id,
                request: settlement.request,
                terminal_state: settlement.terminal_state,
            },
            usage: None,
            errors: Vec::new(),
        }
    }

    pub fn observe_usage(&mut self, record: UsageRecord, cold_start_key: Option<&str>) {
        if let Some(pending) = self.usage.as_mut() {
            pending.record.merge(record);
            if pending.cold_start_key.is_none() {
                pending.cold_start_key = cold_start_key.map(str::to_string);
            }
        } else {
            self.usage = Some(PendingUsage {
                observed_at: Utc::now(),
                record,
                cold_start_key: cold_start_key.map(str::to_string),
            });
        }
    }

    pub fn observe_error(&mut self, scope: impl Into<String>, message: impl Into<String>) {
        self.errors.push(PendingError {
            at: Utc::now(),
            scope: scope.into(),
            message: message.into(),
        });
    }
}

fn metrics_transaction_key(id: &str) -> String {
    let id = id.trim();
    if id.is_empty() {
        String::new()
    } else {
        id.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsCommitResult {
    Applied,
    Duplicate,
    Rejected,
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

    /// Merge cumulative provider snapshots from one upstream response.
    /// Responses/Chat/Anthropic streaming usage is normally repeated or
    /// monotonically completed, not emitted as independent billable deltas.
    pub fn merge_provider_snapshot(&mut self, other: UsageRecord) {
        if self.provider.is_empty() {
            self.provider = other.provider;
        }
        if self.model.is_empty() || self.model == "unknown" {
            self.model = other.model;
        }
        self.input_tokens = self.input_tokens.max(other.input_tokens);
        self.output_tokens = self.output_tokens.max(other.output_tokens);
        self.cache_read_tokens = self.cache_read_tokens.max(other.cache_read_tokens);
        self.cache_creation_tokens = self.cache_creation_tokens.max(other.cache_creation_tokens);
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

/// Lifetime traffic totals scoped to one Agent and one stable provider ID.
/// This intentionally does not reuse the bounded request-history buffers.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AgentProviderTrafficStats {
    pub agent_id: String,
    pub agent_label: String,
    pub provider_id: String,
    pub provider: String,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub error_statuses: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub compaction_requests: u64,
    pub cache_shortfall_tokens: u64,
    pub cache_avoidable_gap_tokens: u64,
    pub cache_new_tail_gap_tokens: u64,
    pub cold_start_requests: u64,
    pub cold_start_input_tokens: u64,
    pub cold_start_output_tokens: u64,
    pub cold_start_cache_read_tokens: u64,
    pub cold_start_cache_shortfall_tokens: u64,
    pub cold_start_cache_avoidable_gap_tokens: u64,
    pub cold_start_cache_new_tail_gap_tokens: u64,
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

/// Shadow-only final-scope cache evidence. These values never contribute to
/// true cache-hit metrics, request-cache metrics, or cache-gap totals.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinalScopeWaterlineLog {
    #[serde(default)]
    pub version: u8,
    #[serde(default)]
    pub outcome: String,
    /// Opaque final-scope identity used only to correlate shadow observations.
    /// It contains no raw conversation, provider Key, or request content.
    pub scope_digest: String,
    pub entry_generation: u64,
    #[serde(default)]
    pub sent_prediction_eligible: bool,
    #[serde(default)]
    pub predecessor_proof: String,
    #[serde(default)]
    pub predecessor_exact: bool,
    #[serde(default)]
    pub predecessor_bound: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_epoch: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor_generation: Option<u64>,
    #[serde(default)]
    pub predecessor_input_items: u64,
    #[serde(default)]
    pub current_input_items: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_settlement_age_ms: Option<u64>,
    #[serde(default)]
    pub prior_observed_cache_read_tokens: u64,
    #[serde(default)]
    pub prior_sent_prefix_bucket_tokens_128: u64,
    #[serde(default)]
    pub prior_settled_prefix_bucket_tokens_128: u64,
    #[serde(default)]
    pub prior_latest_sent_prefix_bucket_tokens_128: u64,
    #[serde(default)]
    pub prior_latest_settled_prefix_bucket_tokens_128: u64,
    #[serde(default)]
    pub raw_input_tokens: u64,
    #[serde(default)]
    pub raw_cache_read_tokens: u64,
    pub observed_cache_read_tokens: u64,
    pub sent_prefix_bucket_tokens_128: u64,
    pub settled_prefix_bucket_tokens_128: u64,
    #[serde(default)]
    pub latest_sent_prefix_bucket_tokens_128: u64,
    #[serde(default)]
    pub latest_settled_prefix_bucket_tokens_128: u64,
    #[serde(default)]
    pub cache_regression_streak: u64,
    #[serde(default)]
    pub stable_settlement_streak: u64,
    #[serde(default)]
    pub continuity_generation: u64,
    #[serde(default)]
    pub continuity_reset: bool,
    #[serde(default)]
    pub candidate_avoidable_tokens_128: u64,
    #[serde(default)]
    pub rollback_tokens_128: u64,
    pub dispatch_seq: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
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
    /// Whether this terminal record consumed the canonical, de-duplicated
    /// usage cold-start allowance. `None` means no usage observation was
    /// attached to the terminal (including historical records).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_start: Option<bool>,
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
    pub final_scope_waterline: Option<FinalScopeWaterlineLog>,
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
    /// First raw upstream SSE bytes observed by the relay, measured from
    /// request ingress. This intentionally differs from `ttft_ms`, which is
    /// kept as the first model-output timing used by latency diagnostics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_byte_ms: Option<u64>,
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
    pub response_context_plan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_semantic_reuse_items: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_wire_saved_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_session_wire_saved_ratio: Option<f64>,
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
    pub inbound_body_bytes: Option<u64>,
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
    agent_provider_stats: Vec<AgentProviderTrafficAccumulator>,
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
    completed_transaction_ids: HashSet<String>,
    completed_transaction_order: VecDeque<String>,
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
struct AgentProviderTrafficAccumulator {
    agent_id: String,
    agent_label: String,
    provider_id: String,
    provider: String,
    total_requests: u64,
    successful_requests: u64,
    error_statuses: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    compaction_requests: u64,
    cache_shortfall_tokens: u64,
    cache_avoidable_gap_tokens: u64,
    cache_new_tail_gap_tokens: u64,
    cold_start_requests: u64,
    cold_start_input_tokens: u64,
    cold_start_output_tokens: u64,
    cold_start_cache_read_tokens: u64,
    cold_start_cache_shortfall_tokens: u64,
    cold_start_cache_avoidable_gap_tokens: u64,
    cold_start_cache_new_tail_gap_tokens: u64,
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
    commit_tracker: Arc<MetricsCommitTracker>,
    history: MetricsHistory,
}

#[derive(Debug)]
struct MetricsCommitTracker {
    state: StdMutex<MetricsCommitTrackerState>,
    notify: Notify,
}

#[derive(Debug)]
struct MetricsCommitTrackerState {
    accepting: bool,
    active: usize,
}

struct MetricsCommitGuard {
    tracker: Arc<MetricsCommitTracker>,
}

impl Drop for MetricsCommitGuard {
    fn drop(&mut self) {
        let idle = {
            let mut state = self
                .tracker
                .state
                .lock()
                .expect("metrics commit tracker lock must not be poisoned");
            state.active = state.active.saturating_sub(1);
            state.active == 0
        };
        if idle {
            self.tracker.notify.notify_waiters();
        }
    }
}

impl MetricsCommitTracker {
    fn enter(self: &Arc<Self>) -> Option<MetricsCommitGuard> {
        let mut state = self
            .state
            .lock()
            .expect("metrics commit tracker lock must not be poisoned");
        if !state.accepting {
            return None;
        }
        state.active += 1;
        Some(MetricsCommitGuard {
            tracker: self.clone(),
        })
    }

    fn is_idle(&self) -> bool {
        self.state
            .lock()
            .expect("metrics commit tracker lock must not be poisoned")
            .active
            == 0
    }

    async fn close_and_wait(&self) {
        {
            let mut state = self
                .state
                .lock()
                .expect("metrics commit tracker lock must not be poisoned");
            state.accepting = false;
        }
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_idle() {
                return;
            }
            notified.await;
        }
    }
}

impl MetricsStore {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::with_history(MetricsHistory::in_memory())
    }

    pub fn with_history_path(path: PathBuf) -> Self {
        Self::with_history(MetricsHistory::load(path))
    }

    fn with_history(history: MetricsHistory) -> Self {
        Self {
            commit_tracker: Arc::new(MetricsCommitTracker {
                state: StdMutex::new(MetricsCommitTrackerState {
                    accepting: true,
                    active: 0,
                }),
                notify: Notify::new(),
            }),
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
                agent_provider_stats: Vec::new(),
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
                completed_transaction_ids: HashSet::new(),
                completed_transaction_order: VecDeque::new(),
                recent_agent_inbound_outcomes: VecDeque::new(),
                recent_agent_upstream_attempts: VecDeque::new(),
            })),
            history,
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
        if attempt_index > 1 {
            inner.retries += 1;
        }
        increment_provider_upstream_attempt(&mut inner.provider_stats, &provider);
        Some(attempt_index)
    }

    #[cfg(test)]
    pub async fn finish_agent_attempt(&self, attempt_id: &str, finish: AgentAttemptFinish) -> bool {
        let attempt_id = attempt_id.trim();
        if attempt_id.is_empty() {
            return false;
        }

        let mut inner = self.inner.write().await;
        finish_agent_attempt_inner(&mut inner, attempt_id, finish)
    }

    #[cfg(test)]
    async fn finish_agent_inbound(
        &self,
        inbound_request_id: &str,
        request: RequestLog,
        outcome: AgentInboundOutcome,
        terminal_state: Option<String>,
    ) -> bool {
        let inbound_request_id = inbound_request_id.trim();
        if inbound_request_id.is_empty() {
            return false;
        }
        let mut inner = self.inner.write().await;
        let (finished, observation) = finish_agent_inbound_inner(
            &mut inner,
            inbound_request_id,
            request,
            outcome,
            terminal_state,
            0,
        );
        drop(inner);
        if let Some(observation) = observation {
            self.history.observe(observation);
        }
        finished
    }

    #[cfg(test)]
    async fn record_upstream_call(&self, log: RequestLog) {
        if request_log_is_successful_history(&log) {
            let mut inner = self.inner.write().await;
            push_limited(&mut inner.recent_upstream_calls, log, 400);
        }
    }

    #[cfg(test)]
    async fn record_request(&self, log: RequestLog, upstream: bool) {
        let mut inner = self.inner.write().await;
        let observation = record_request_inner(&mut inner, log, upstream, 0);
        drop(inner);
        if let Some(observation) = observation {
            self.history.observe(observation);
        }
    }

    #[cfg(test)]
    async fn record_usage(&self, record: UsageRecord) {
        self.record_usage_with_cold_start_key(record, None).await;
    }

    #[cfg(test)]
    async fn record_usage_with_cold_start_key(
        &self,
        record: UsageRecord,
        cold_start_key: Option<&str>,
    ) {
        let mut inner = self.inner.write().await;
        record_usage_inner(&mut inner, record, cold_start_key, Utc::now());
    }

    pub async fn record_error(&self, scope: &str, message: &str) {
        let mut inner = self.inner.write().await;
        record_error_inner(&mut inner, scope, message, Utc::now());
    }

    pub async fn commit(&self, transaction: MetricsTransaction) -> MetricsCommitResult {
        let Some(commit_guard) = self.commit_tracker.enter() else {
            return MetricsCommitResult::Rejected;
        };
        if let Ok(mut inner) = self.inner.try_write() {
            let outcome = commit_metrics_transaction(&mut inner, transaction);
            drop(inner);
            if let Some(observation) = outcome.history_observation {
                self.history.observe(observation);
            }
            return outcome.result;
        }

        let inner = self.inner.clone();
        let history = self.history.clone();
        tokio::spawn(async move {
            let _commit_guard = commit_guard;
            let mut inner = inner.write().await;
            let outcome = commit_metrics_transaction(&mut inner, transaction);
            drop(inner);
            if let Some(observation) = outcome.history_observation {
                history.observe(observation);
            }
            outcome.result
        })
        .await
        .unwrap_or(MetricsCommitResult::Rejected)
    }

    pub(crate) fn commit_detached(&self, transaction: MetricsTransaction) -> bool {
        let Some(commit_guard) = self.commit_tracker.enter() else {
            return false;
        };
        if let Ok(mut inner) = self.inner.try_write() {
            let outcome = commit_metrics_transaction(&mut inner, transaction);
            drop(inner);
            if let Some(observation) = outcome.history_observation {
                self.history.observe(observation);
            }
            return true;
        }

        let inner = self.inner.clone();
        let history = self.history.clone();
        tokio::spawn(async move {
            let _commit_guard = commit_guard;
            let mut inner = inner.write().await;
            let outcome = commit_metrics_transaction(&mut inner, transaction);
            drop(inner);
            if let Some(observation) = outcome.history_observation {
                history.observe(observation);
            }
            outcome.result
        });
        true
    }

    pub(crate) async fn close_and_wait_for_commits(&self) {
        self.commit_tracker.close_and_wait().await;
    }

    pub fn trend(&self, input: MetricsTrendQueryInput) -> anyhow::Result<MetricsTrendSnapshot> {
        self.history.query(input)
    }

    pub(crate) async fn flush_history(&self) -> anyhow::Result<()> {
        self.history.flush().await
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
            agent_provider_stats: sorted_agent_provider_stats(&inner.agent_provider_stats),
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

struct MetricsCommitOutcome {
    result: MetricsCommitResult,
    history_observation: Option<MetricsHistoryObservation>,
}

impl MetricsCommitOutcome {
    fn without_history(result: MetricsCommitResult) -> Self {
        Self {
            result,
            history_observation: None,
        }
    }
}

fn commit_metrics_transaction(
    inner: &mut MetricsInner,
    transaction: MetricsTransaction,
) -> MetricsCommitOutcome {
    let MetricsTransaction {
        commit_key,
        terminal,
        usage,
        errors,
    } = transaction;
    if commit_key.is_empty() {
        return MetricsCommitOutcome::without_history(MetricsCommitResult::Rejected);
    }
    if inner.completed_transaction_ids.contains(&commit_key) {
        return MetricsCommitOutcome::without_history(MetricsCommitResult::Duplicate);
    }
    if let MetricsTerminal::Agent {
        inbound_request_id,
        attempt_id,
        ..
    } = &terminal
    {
        if !agent_terminal_is_valid(inner, inbound_request_id, attempt_id) {
            record_error_inner(
                inner,
                "agent_lifecycle",
                &format!(
                    "Agent terminal transaction rejected inconsistent lifecycle state for inbound {inbound_request_id}"
                ),
                Utc::now(),
            );
            return MetricsCommitOutcome::without_history(MetricsCommitResult::Rejected);
        }
    }
    if let MetricsTerminal::AgentOwnerFailure {
        inbound_request_id, ..
    } = &terminal
    {
        if inner
            .completed_agent_inbound_ids
            .contains(inbound_request_id)
            && !inner.active_agent_inbounds.contains_key(inbound_request_id)
        {
            return MetricsCommitOutcome::without_history(MetricsCommitResult::Duplicate);
        }
    }

    let cache_creation_tokens = usage
        .as_ref()
        .map(|usage| usage.record.cache_creation_tokens)
        .unwrap_or_default();
    let cold_start = usage.map(|usage| {
        record_usage_inner(
            inner,
            usage.record,
            usage.cold_start_key.as_deref(),
            usage.observed_at,
        )
    });
    for error in errors {
        record_error_inner(inner, &error.scope, &error.message, error.at);
    }

    let history_observation = match terminal {
        MetricsTerminal::Upstream {
            mut request,
            upstream_attempts,
        } => {
            request.cold_start = cold_start;
            let successful = request_log_is_successful_history(&request);
            let projection = request.clone();
            let observation = record_request_inner(inner, request, false, cache_creation_tokens);
            record_upstream_attempts_inner(inner, &projection.provider, upstream_attempts);
            if successful {
                push_limited(&mut inner.recent_upstream_calls, projection, 400);
            }
            observation
        }
        MetricsTerminal::LocalCache {
            mut request,
            estimated_tokens_saved,
        } => {
            request.cold_start = cold_start;
            inner.local_proxy_estimated_tokens_saved = inner
                .local_proxy_estimated_tokens_saved
                .saturating_add(estimated_tokens_saved);
            let _ = record_request_inner(inner, request, false, cache_creation_tokens);
            None
        }
        MetricsTerminal::LocalRejection { mut request } => {
            request.cold_start = cold_start;
            let _ = record_request_inner(inner, request, false, cache_creation_tokens);
            None
        }
        MetricsTerminal::Agent {
            inbound_request_id,
            attempt_id,
            attempt_finish,
            mut request,
            inbound_outcome,
            terminal_state,
        } => {
            request.cold_start = cold_start;
            apply_agent_terminal_inner(
                inner,
                &inbound_request_id,
                &attempt_id,
                attempt_finish,
                request,
                inbound_outcome,
                terminal_state,
                cache_creation_tokens,
            )
        }
        MetricsTerminal::AgentOwnerFailure {
            inbound_request_id,
            mut request,
            terminal_state,
        } => {
            request.cold_start = cold_start;
            apply_agent_owner_failure_inner(
                inner,
                &inbound_request_id,
                request,
                terminal_state,
                cache_creation_tokens,
            )
        }
    };

    remember_completed_id(
        &mut inner.completed_transaction_ids,
        &mut inner.completed_transaction_order,
        &commit_key,
    );
    MetricsCommitOutcome {
        result: MetricsCommitResult::Applied,
        history_observation,
    }
}

fn finish_agent_attempt_inner(
    inner: &mut MetricsInner,
    attempt_id: &str,
    finish: AgentAttemptFinish,
) -> bool {
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
        } = inner;
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

fn finish_agent_inbound_inner(
    inner: &mut MetricsInner,
    inbound_request_id: &str,
    mut request: RequestLog,
    mut outcome: AgentInboundOutcome,
    terminal_state: Option<String>,
    cache_creation_tokens: u64,
) -> (bool, Option<MetricsHistoryObservation>) {
    if inner
        .completed_agent_inbound_ids
        .contains(inbound_request_id)
        || inner
            .active_agent_attempts
            .values()
            .any(|attempt| attempt.inbound_request_id == inbound_request_id)
    {
        return (false, None);
    }
    let Some(active) = inner.active_agent_inbounds.remove(inbound_request_id) else {
        return (false, None);
    };
    {
        let MetricsInner {
            completed_agent_inbound_ids,
            completed_agent_inbound_order,
            ..
        } = inner;
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
    let history_observation =
        record_request_inner(inner, projection.clone(), false, cache_creation_tokens);
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
    (true, history_observation)
}

fn agent_terminal_is_valid(
    inner: &MetricsInner,
    inbound_request_id: &str,
    attempt_id: &str,
) -> bool {
    if inbound_request_id.is_empty() || attempt_id.is_empty() {
        return false;
    }
    let Some(inbound) = inner.active_agent_inbounds.get(inbound_request_id) else {
        return false;
    };
    inbound.last_attempt_id.as_deref() == Some(attempt_id)
        && !inner
            .completed_agent_inbound_ids
            .contains(inbound_request_id)
        && !inner.completed_agent_attempt_ids.contains(attempt_id)
        && inner
            .active_agent_attempts
            .get(attempt_id)
            .is_some_and(|attempt| attempt.inbound_request_id == inbound_request_id)
        && inner
            .active_agent_attempts
            .values()
            .filter(|attempt| attempt.inbound_request_id == inbound_request_id)
            .count()
            == 1
}

fn apply_agent_terminal_inner(
    inner: &mut MetricsInner,
    inbound_request_id: &str,
    attempt_id: &str,
    attempt_finish: AgentAttemptFinish,
    request: RequestLog,
    inbound_outcome: AgentInboundOutcome,
    terminal_state: Option<String>,
    cache_creation_tokens: u64,
) -> Option<MetricsHistoryObservation> {
    assert!(finish_agent_attempt_inner(
        inner,
        attempt_id,
        attempt_finish
    ));
    let (finished, history_observation) = finish_agent_inbound_inner(
        inner,
        inbound_request_id,
        request,
        inbound_outcome,
        terminal_state,
        cache_creation_tokens,
    );
    assert!(finished);
    history_observation
}

fn apply_agent_owner_failure_inner(
    inner: &mut MetricsInner,
    inbound_request_id: &str,
    request: RequestLog,
    terminal_state: Option<String>,
    cache_creation_tokens: u64,
) -> Option<MetricsHistoryObservation> {
    let attempt_ids = inner
        .active_agent_attempts
        .values()
        .filter(|attempt| attempt.inbound_request_id == inbound_request_id)
        .map(|attempt| attempt.attempt_id.clone())
        .collect::<Vec<_>>();
    for attempt_id in attempt_ids {
        let _ = finish_agent_attempt_inner(
            inner,
            &attempt_id,
            AgentAttemptFinish {
                finished_at: Utc::now(),
                outcome: AgentAttemptOutcome::RelayAborted,
                status: Some(request.status),
                error_scope: Some("agent_generation_owner".to_string()),
                terminal_state: terminal_state.clone(),
                total_ms: request.total_ms,
                upstream_headers_ms: request.upstream_headers_ms,
                upstream_network_path: request.upstream_network_path.clone(),
                request_body_bytes: request.request_body_bytes,
                sent_body_bytes: request.sent_body_bytes,
                gzip_attempted: request.gzip_attempted,
            },
        );
    }

    if !inner.active_agent_inbounds.contains_key(inbound_request_id) {
        inner.active_agent_inbounds.insert(
            inbound_request_id.to_string(),
            ActiveAgentInbound {
                at: request.at,
                attempt_policy: "owner-failure".to_string(),
                attempt_budget: 1,
                attempt_count: 0,
                last_attempt_id: None,
            },
        );
        inner.agent_generation.inbound_requests += 1;
        inner.agent_generation.active_inbounds += 1;
    }

    let (finished, history_observation) = finish_agent_inbound_inner(
        inner,
        inbound_request_id,
        request,
        AgentInboundOutcome::RelayAborted,
        terminal_state,
        cache_creation_tokens,
    );
    assert!(finished);
    history_observation
}

fn record_usage_inner(
    inner: &mut MetricsInner,
    record: UsageRecord,
    cold_start_key: Option<&str>,
    observed_at: DateTime<Utc>,
) -> bool {
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
            at: observed_at,
            record,
            cold_start_counted: count_cold_start,
        },
    );
    count_cold_start
}

fn record_error_inner(inner: &mut MetricsInner, scope: &str, message: &str, at: DateTime<Utc>) {
    inner.errors += 1;
    push_limited(
        &mut inner.recent_errors,
        ErrorLog {
            at,
            scope: scope.to_string(),
            message: message.to_string(),
        },
        40,
    );
}

fn record_request_inner(
    inner: &mut MetricsInner,
    log: RequestLog,
    upstream: bool,
    cache_creation_tokens: u64,
) -> Option<MetricsHistoryObservation> {
    let history_observation = metrics_history_observation(&log, cache_creation_tokens);
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
    upsert_agent_provider_traffic(&mut inner.agent_provider_stats, &log);
    if request_log_is_successful_history(&log) {
        push_limited(&mut inner.recent_requests, log, 200);
    } else {
        push_limited(&mut inner.recent_failed_requests, log, 200);
    }
    history_observation
}

fn metrics_history_observation(
    log: &RequestLog,
    cache_creation_tokens: u64,
) -> Option<MetricsHistoryObservation> {
    if !request_log_is_successful_history(log) {
        return None;
    }
    let agent_id = log
        .agent_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let provider_id = log
        .provider_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(MetricsHistoryObservation {
        at: log.at,
        agent_id: agent_id.to_string(),
        provider_id: provider_id.to_string(),
        input_tokens: log.input_tokens.unwrap_or_default(),
        output_tokens: log.output_tokens.unwrap_or_default(),
        cache_read_tokens: log.cache_read_tokens.unwrap_or_default(),
        cache_creation_tokens,
        cache_shortfall_tokens: log.cache_shortfall_tokens.unwrap_or_default(),
        cache_avoidable_gap_tokens: log.cache_avoidable_gap_tokens.unwrap_or_default(),
        cache_new_tail_gap_tokens: log.cache_new_tail_gap_tokens.unwrap_or_default(),
        compaction: request_log_is_confirmed_compaction(log),
        cold_start: log.cold_start == Some(true),
    })
}

fn request_log_is_confirmed_compaction(log: &RequestLog) -> bool {
    log.cache_status == "compact"
        && matches!(
            log.upstream_call_source.as_deref(),
            Some(
                "responses-compaction-v2"
                    | "compact"
                    | "compact-fallback"
                    | "compact-chat-compat"
                    | "compact-fallback-chat-compat"
            )
        )
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

impl AgentProviderTrafficAccumulator {
    fn snapshot(&self) -> AgentProviderTrafficStats {
        AgentProviderTrafficStats {
            agent_id: self.agent_id.clone(),
            agent_label: self.agent_label.clone(),
            provider_id: self.provider_id.clone(),
            provider: self.provider.clone(),
            total_requests: self.total_requests,
            successful_requests: self.successful_requests,
            error_statuses: self.error_statuses,
            input_tokens: self.input_tokens,
            output_tokens: self.output_tokens,
            cache_read_tokens: self.cache_read_tokens,
            compaction_requests: self.compaction_requests,
            cache_shortfall_tokens: self.cache_shortfall_tokens,
            cache_avoidable_gap_tokens: self.cache_avoidable_gap_tokens,
            cache_new_tail_gap_tokens: self.cache_new_tail_gap_tokens,
            cold_start_requests: self.cold_start_requests,
            cold_start_input_tokens: self.cold_start_input_tokens,
            cold_start_output_tokens: self.cold_start_output_tokens,
            cold_start_cache_read_tokens: self.cold_start_cache_read_tokens,
            cold_start_cache_shortfall_tokens: self.cold_start_cache_shortfall_tokens,
            cold_start_cache_avoidable_gap_tokens: self.cold_start_cache_avoidable_gap_tokens,
            cold_start_cache_new_tail_gap_tokens: self.cold_start_cache_new_tail_gap_tokens,
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

fn upsert_agent_provider_traffic(
    groups: &mut Vec<AgentProviderTrafficAccumulator>,
    log: &RequestLog,
) {
    let Some(agent_id) = log
        .agent_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    let provider = if log.provider.trim().is_empty() {
        "unknown"
    } else {
        log.provider.trim()
    };
    let provider_id = log
        .provider_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(provider);
    let agent_label = log
        .agent_label
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(agent_id);
    let index = groups
        .iter()
        .position(|group| group.agent_id == agent_id && group.provider_id == provider_id)
        .unwrap_or_else(|| {
            groups.push(AgentProviderTrafficAccumulator {
                agent_id: agent_id.to_string(),
                agent_label: agent_label.to_string(),
                provider_id: provider_id.to_string(),
                provider: provider.to_string(),
                ..AgentProviderTrafficAccumulator::default()
            });
            groups.len() - 1
        });
    let group = &mut groups[index];
    group.total_requests += 1;
    let successful = request_log_is_successful_history(log);
    if successful {
        group.successful_requests += 1;
        // These are the same successful-request usage totals shown by the
        // request dashboard. Failed attempts remain visible through
        // `error_statuses`, but must not inflate cache-hit/token statistics.
        group.input_tokens += log.input_tokens.unwrap_or_default();
        group.output_tokens += log.output_tokens.unwrap_or_default();
        group.cache_read_tokens += log.cache_read_tokens.unwrap_or_default();
        if log.cache_status == "compact" {
            group.compaction_requests += 1;
        }
        group.cache_shortfall_tokens += log.cache_shortfall_tokens.unwrap_or_default();
        group.cache_avoidable_gap_tokens += log.cache_avoidable_gap_tokens.unwrap_or_default();
        group.cache_new_tail_gap_tokens += log.cache_new_tail_gap_tokens.unwrap_or_default();
        if log.cold_start == Some(true) {
            group.cold_start_requests += 1;
            group.cold_start_input_tokens += log.input_tokens.unwrap_or_default();
            group.cold_start_output_tokens += log.output_tokens.unwrap_or_default();
            group.cold_start_cache_read_tokens += log.cache_read_tokens.unwrap_or_default();
            group.cold_start_cache_shortfall_tokens +=
                log.cache_shortfall_tokens.unwrap_or_default();
            group.cold_start_cache_avoidable_gap_tokens +=
                log.cache_avoidable_gap_tokens.unwrap_or_default();
            group.cold_start_cache_new_tail_gap_tokens +=
                log.cache_new_tail_gap_tokens.unwrap_or_default();
        }
    }
    if log.status >= 400 || log.cache_status == "error" {
        group.error_statuses += 1;
    }
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

fn record_upstream_attempts_inner(inner: &mut MetricsInner, provider: &str, attempts: u64) {
    inner.upstream_requests = inner.upstream_requests.saturating_add(attempts);
    inner.retries = inner.retries.saturating_add(attempts.saturating_sub(1));
    for _ in 0..attempts {
        increment_provider_upstream_attempt(&mut inner.provider_stats, provider);
    }
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

fn sorted_agent_provider_stats(
    groups: &[AgentProviderTrafficAccumulator],
) -> Vec<AgentProviderTrafficStats> {
    let mut stats = groups
        .iter()
        .map(AgentProviderTrafficAccumulator::snapshot)
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| {
        left.agent_id
            .cmp(&right.agent_id)
            .then_with(|| left.provider_id.cmp(&right.provider_id))
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
            provider_id: Some("provider-id".to_string()),
            model: "model".to_string(),
            requested_model: None,
            agent_reasoning_effort: None,
            configured_reasoning_effort: None,
            effective_reasoning_effort: None,
            reasoning_effort_source: None,
            cache_status: cache_status.to_string(),
            cold_start: None,
            agent_id: None,
            agent_label: None,
            upstream_call_kind: None,
            upstream_call_source: None,
            cache_key: cache_key.map(ToOwned::to_owned),
            provider_prefix_key: None,
            provider_prefix_fingerprint: None,
            outbound_prefix_fingerprints: None,
            provider_cache_diagnostic: None,
            final_scope_waterline: None,
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
            first_byte_ms: None,
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
            response_context_plan: None,
            response_session_semantic_reuse_items: None,
            response_session_wire_saved_bytes: None,
            response_session_wire_saved_ratio: None,
            response_session_cooldown_active: None,
            response_session_rejected_status: None,
            session_anchor_hash: None,
            session_anchor_source: None,
            session_anchor_changed: None,
            session_anchor_peer_count: None,
            inbound_body_bytes: None,
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
    fn request_log_optional_fields_default_for_legacy_metrics() {
        let mut log = request_log("miss", Some("cache-key"));
        log.cold_start = Some(true);
        let mut value = serde_json::to_value(log).unwrap();
        let object = value.as_object_mut().unwrap();
        for key in [
            "provider_id",
            "cold_start",
            "first_byte_ms",
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
        assert!(restored.provider_id.is_none());
        assert!(restored.cold_start.is_none());
        assert!(restored.first_byte_ms.is_none());
        assert!(restored.shadow_affinity_mode.is_none());
        assert!(restored.shadow_affinity_policy_compute_ms.is_none());
    }

    #[test]
    fn request_log_serializes_stable_provider_id() {
        let value = serde_json::to_value(request_log("miss", Some("cache-key"))).unwrap();
        assert_eq!(value["provider_id"], "provider-id");
        assert!(value.get("cold_start").is_none());
    }

    #[test]
    fn request_log_serializes_known_cold_start() {
        let mut log = request_log("miss", Some("cache-key"));
        log.cold_start = Some(true);

        let value = serde_json::to_value(log).unwrap();
        assert_eq!(value["cold_start"], true);
    }

    #[test]
    fn request_log_keeps_provider_cache_delta_and_waterline_metrics_separate() {
        let mut log = request_log("hit", Some("cache-key"));
        log.input_tokens = Some(100_000);
        log.cache_read_tokens = Some(81_920);
        log.provider_cache_token_ratio = Some(0.8192);
        log.response_context_plan = Some("verified_native_delta".to_string());
        log.response_session_semantic_reuse_items = Some(42);
        log.response_session_wire_saved_bytes = Some(65_536);
        log.response_session_wire_saved_ratio = Some(0.75);
        log.final_scope_waterline = Some(FinalScopeWaterlineLog {
            scope_digest: "scope-a".to_string(),
            entry_generation: 3,
            observed_cache_read_tokens: 80_000,
            sent_prefix_bucket_tokens_128: 99_968,
            settled_prefix_bucket_tokens_128: 79_872,
            dispatch_seq: 7,
            ..FinalScopeWaterlineLog::default()
        });

        let value = serde_json::to_value(log).unwrap();
        assert_eq!(value["cache_read_tokens"], 81_920);
        assert_eq!(value["provider_cache_token_ratio"], 0.8192);
        assert_eq!(value["response_context_plan"], "verified_native_delta");
        assert_eq!(value["response_session_semantic_reuse_items"], 42);
        assert_eq!(value["response_session_wire_saved_bytes"], 65_536);
        assert_eq!(value["response_session_wire_saved_ratio"], 0.75);
        assert_eq!(value["final_scope_waterline"]["scope_digest"], "scope-a");
        assert_eq!(value["final_scope_waterline"]["entry_generation"], 3);
        assert_eq!(
            value["final_scope_waterline"]["observed_cache_read_tokens"],
            80_000
        );
        assert_eq!(
            value["final_scope_waterline"]["sent_prefix_bucket_tokens_128"],
            99_968
        );
        assert_eq!(
            value["cache_read_tokens"], 81_920,
            "shadow waterlines must not overwrite true upstream cache telemetry"
        );
    }

    #[tokio::test]
    async fn final_scope_waterline_is_shadow_only_for_live_and_trend_cache_metrics() {
        let metrics = MetricsStore::new();
        let hour_timestamp = Utc::now().timestamp().div_euclid(60 * 60) * 60 * 60;
        let hour = DateTime::<Utc>::from_timestamp(hour_timestamp, 0)
            .expect("current UTC hour must be representable");

        let mut request = request_log("bypass", Some("waterline-shadow-only"));
        request.id = "waterline-shadow-only".to_string();
        request.at = hour + Duration::seconds(1);
        request.agent_id = Some("codex".to_string());
        request.agent_label = Some("Codex".to_string());
        request.provider_id = Some("provider-a".to_string());
        request.input_tokens = Some(100_000);
        request.output_tokens = Some(320);
        request.cache_read_tokens = Some(81_920);
        request.cache_shortfall_tokens = Some(18_080);
        request.cache_avoidable_gap_tokens = Some(8_000);
        request.cache_new_tail_gap_tokens = Some(10_080);
        request.provider_cache_token_ratio = Some(0.8192);
        request.final_scope_waterline = Some(FinalScopeWaterlineLog {
            scope_digest: "scope-shadow-only".to_string(),
            entry_generation: 9,
            // Deliberately impossible values: if any shadow field leaks into a
            // real aggregate, every assertion below fails by a wide margin.
            observed_cache_read_tokens: 9_000_000,
            sent_prefix_bucket_tokens_128: 8_000_000,
            settled_prefix_bucket_tokens_128: 7_000_000,
            dispatch_seq: 99,
            ..FinalScopeWaterlineLog::default()
        });

        let mut transaction = MetricsTransaction::upstream(request);
        transaction.observe_usage(
            UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 100_000,
                output_tokens: 320,
                cache_read_tokens: 81_920,
                cache_creation_tokens: 512,
            },
            None,
        );
        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.provider_input_tokens, 100_000);
        assert_eq!(snapshot.provider_cached_tokens, 81_920);
        assert_eq!(snapshot.provider_cache_token_ratio, 0.8192);
        assert_eq!(snapshot.usage.input_tokens, 100_000);
        assert_eq!(snapshot.usage.cache_read_tokens, 81_920);
        assert_eq!(snapshot.recent_usage.cache_read_tokens, 81_920);
        assert_eq!(snapshot.recent_usage.cache_token_ratio, 0.8192);
        assert_eq!(snapshot.recent_requests[0].cache_read_tokens, Some(81_920));
        assert_eq!(
            snapshot.recent_requests[0]
                .final_scope_waterline
                .as_ref()
                .map(|waterline| waterline.observed_cache_read_tokens),
            Some(9_000_000),
            "shadow evidence remains observable without becoming cache telemetry"
        );
        let agent_provider = snapshot
            .agent_provider_stats
            .iter()
            .find(|item| item.agent_id == "codex" && item.provider_id == "provider-a")
            .expect("agent/provider aggregate should be present");
        assert_eq!(agent_provider.input_tokens, 100_000);
        assert_eq!(agent_provider.cache_read_tokens, 81_920);
        assert_eq!(agent_provider.cache_shortfall_tokens, 18_080);
        assert_eq!(agent_provider.cache_avoidable_gap_tokens, 8_000);
        assert_eq!(agent_provider.cache_new_tail_gap_tokens, 10_080);

        let trend = metrics
            .trend(MetricsTrendQueryInput {
                start_utc: hour.to_rfc3339(),
                end_utc: (hour + Duration::hours(1)).to_rfc3339(),
                agent_id: "codex".to_string(),
                provider_id: Some("provider-a".to_string()),
                include_cold_starts: true,
            })
            .expect("trend query should succeed");
        assert_eq!(trend.summary.successful_requests, 1);
        assert_eq!(trend.summary.input_tokens, 100_000);
        assert_eq!(trend.summary.cache_read_tokens, 81_920);
        assert_eq!(trend.summary.cache_miss_tokens, 18_080);
        assert_eq!(trend.summary.cache_creation_tokens, 512);
        assert_eq!(trend.summary.cache_shortfall_tokens, 18_080);
        assert_eq!(trend.summary.cache_avoidable_gap_tokens, 8_000);
        assert_eq!(trend.summary.cache_new_tail_gap_tokens, 10_080);
        assert_eq!(trend.summary.cache_hit_rate, 0.8192);
        assert_eq!(trend.points.len(), 1);
        assert_eq!(trend.points[0].values, trend.summary);
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
    async fn metrics_transaction_commits_usage_request_and_histories_once() {
        let metrics = MetricsStore::new();
        let mut request = request_log("miss", Some("transaction-success"));
        request.id = "transaction-success".to_string();
        let mut transaction = MetricsTransaction::upstream(request);
        transaction.observe_usage(
            UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 2_048,
                output_tokens: 32,
                cache_read_tokens: 1_920,
                cache_creation_tokens: 0,
            },
            Some("prefix-success"),
        );
        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 1);
        assert_eq!(snapshot.upstream_requests, 1);
        assert_eq!(snapshot.retries, 0);
        assert_eq!(snapshot.usage.input_tokens, 2_048);
        assert_eq!(snapshot.usage.cache_read_tokens, 1_920);
        assert_eq!(snapshot.recent_requests.len(), 1);
        assert_eq!(snapshot.recent_upstream_calls.len(), 1);
        assert_eq!(snapshot.recent_requests[0].cold_start, Some(false));
        assert_eq!(snapshot.recent_upstream_calls[0].cold_start, Some(false));
        assert!(snapshot.recent_failed_requests.is_empty());
    }

    #[tokio::test]
    async fn transaction_request_logs_project_deduplicated_usage_cold_start() {
        let metrics = MetricsStore::new();
        for id in ["cold-start-first", "cold-start-repeat"] {
            let mut request = request_log("miss", Some("cold-start-history"));
            request.id = id.to_string();
            request.input_tokens = Some(30_000);
            request.cache_read_tokens = Some(0);
            let mut transaction = MetricsTransaction::upstream(request);
            transaction.observe_usage(
                UsageRecord {
                    provider: "provider".to_string(),
                    model: "model".to_string(),
                    input_tokens: 30_000,
                    output_tokens: 100,
                    cache_read_tokens: 0,
                    cache_creation_tokens: 0,
                },
                Some("shared-cold-start-key"),
            );
            assert_eq!(
                metrics.commit(transaction).await,
                MetricsCommitResult::Applied
            );
        }

        let snapshot = metrics.snapshot().await;
        let request_cold_start = |id: &str| {
            snapshot
                .recent_requests
                .iter()
                .find(|request| request.id == id)
                .and_then(|request| request.cold_start)
        };
        let upstream_cold_start = |id: &str| {
            snapshot
                .recent_upstream_calls
                .iter()
                .find(|request| request.id == id)
                .and_then(|request| request.cold_start)
        };

        assert_eq!(request_cold_start("cold-start-first"), Some(true));
        assert_eq!(request_cold_start("cold-start-repeat"), Some(false));
        assert_eq!(upstream_cold_start("cold-start-first"), Some(true));
        assert_eq!(upstream_cold_start("cold-start-repeat"), Some(false));
        assert_eq!(snapshot.usage.cold_start_requests, 1);
    }

    #[tokio::test]
    async fn metrics_transaction_counts_real_upstream_attempts_without_duplicating_inbound() {
        let metrics = MetricsStore::new();
        let mut request = request_log("miss", Some("transaction-retry"));
        request.id = "transaction-retry".to_string();
        request.upstream_attempt_total = Some(3);
        request.upstream_attempts = Some(3);

        assert_eq!(
            metrics.commit(MetricsTransaction::upstream(request)).await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 1);
        assert_eq!(snapshot.upstream_requests, 3);
        assert_eq!(snapshot.retries, 2);
        assert_eq!(snapshot.recent_requests.len(), 1);
        assert_eq!(snapshot.recent_upstream_calls.len(), 1);
        assert!(snapshot.recent_failed_requests.is_empty());
        assert_eq!(snapshot.provider_stats[0].total_requests, 1);
        assert_eq!(snapshot.provider_stats[0].upstream_requests, 3);
    }

    #[tokio::test]
    async fn metrics_transaction_duplicate_key_is_a_complete_noop() {
        let metrics = MetricsStore::new();
        let mut request = request_log("miss", Some("transaction-duplicate"));
        request.id = "transaction-duplicate".to_string();
        let mut transaction = MetricsTransaction::upstream(request);
        transaction.observe_error("upstream", "one error event");
        transaction.observe_usage(
            UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 100,
                output_tokens: 10,
                cache_read_tokens: 50,
                cache_creation_tokens: 0,
            },
            None,
        );
        assert_eq!(
            metrics.commit(transaction.clone()).await,
            MetricsCommitResult::Applied
        );
        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Duplicate
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.usage.input_tokens, 100);
        assert_eq!(snapshot.recent_requests.len(), 1);
        assert_eq!(snapshot.recent_upstream_calls.len(), 1);
    }

    #[tokio::test]
    async fn metrics_transaction_deduplicates_conflicting_terminal_kinds_by_inbound_id() {
        let metrics = MetricsStore::new();
        let mut cached = request_log("exact", Some("shared-raw-id"));
        cached.id = "shared-raw-id".to_string();
        let mut upstream = request_log("miss", Some("shared-raw-id"));
        upstream.id = "shared-raw-id".to_string();

        assert_eq!(
            metrics
                .commit(MetricsTransaction::local_cache(cached, 64))
                .await,
            MetricsCommitResult::Applied
        );
        assert_eq!(
            metrics.commit(MetricsTransaction::upstream(upstream)).await,
            MetricsCommitResult::Duplicate
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.upstream_requests, 0);
        assert_eq!(snapshot.local_proxy.local_cache_hits, 1);
    }

    #[tokio::test]
    async fn metrics_transaction_merges_multiple_usage_observations() {
        let metrics = MetricsStore::new();
        let mut request = request_log("miss", Some("merged-usage"));
        request.id = "merged-usage".to_string();
        let mut transaction = MetricsTransaction::upstream(request);
        transaction.observe_usage(
            UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 100,
                output_tokens: 10,
                cache_read_tokens: 50,
                cache_creation_tokens: 2,
            },
            None,
        );
        transaction.observe_usage(
            UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 20,
                output_tokens: 3,
                cache_read_tokens: 10,
                cache_creation_tokens: 1,
            },
            Some("merged-usage-prefix"),
        );

        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.usage.input_tokens, 120);
        assert_eq!(snapshot.usage.output_tokens, 13);
        assert_eq!(snapshot.usage.cache_read_tokens, 60);
        assert_eq!(snapshot.usage.cache_creation_tokens, 3);
    }

    #[tokio::test]
    async fn contended_metrics_commit_survives_caller_cancellation() {
        let metrics = MetricsStore::new();
        let guard = metrics.inner.write().await;
        let mut request = request_log("miss", Some("cancel-safe-commit"));
        request.id = "cancel-safe-commit".to_string();
        let transaction = MetricsTransaction::upstream(request);
        let metrics_for_commit = metrics.clone();
        let caller = tokio::spawn(async move { metrics_for_commit.commit(transaction).await });

        while Arc::strong_count(&metrics.inner) < 3 {
            tokio::task::yield_now().await;
        }
        caller.abort();
        let _ = caller.await;
        drop(guard);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if metrics.snapshot().await.total_requests == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the detached commit must finish after its caller is cancelled");
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.upstream_requests, 1);
    }

    #[tokio::test]
    async fn metrics_close_waits_for_detached_commit_and_rejects_late_commits() {
        let metrics = MetricsStore::new();
        let guard = metrics.inner.write().await;
        let mut request = request_log("miss", Some("shutdown-commit"));
        request.id = "shutdown-commit".to_string();
        assert!(metrics.commit_detached(MetricsTransaction::upstream(request)));

        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
        let metrics_for_close = metrics.clone();
        tokio::spawn(async move {
            metrics_for_close.close_and_wait_for_commits().await;
            let _ = closed_tx.send(());
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), closed_rx)
                .await
                .is_err()
        );
        drop(guard);
        metrics.close_and_wait_for_commits().await;

        let mut late = request_log("miss", Some("late-commit"));
        late.id = "late-commit".to_string();
        assert!(!metrics.commit_detached(MetricsTransaction::upstream(late)));
        assert_eq!(metrics.snapshot().await.total_requests, 1);
    }

    #[tokio::test]
    async fn metrics_transaction_failure_keeps_errors_out_of_success_history() {
        let metrics = MetricsStore::new();
        let mut failed = request_log("error", None);
        failed.id = "transaction-failure".to_string();
        failed.status = 503;
        let mut transaction = MetricsTransaction::upstream(failed);
        transaction.observe_error("upstream_http", "service unavailable");

        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 0);
        assert_eq!(snapshot.upstream_requests, 1);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.recent_failed_requests.len(), 1);
        assert!(snapshot.recent_requests.is_empty());
        assert!(snapshot.recent_upstream_calls.is_empty());
    }

    #[tokio::test]
    async fn metrics_transaction_local_cache_updates_request_and_saved_tokens_together() {
        let metrics = MetricsStore::new();
        let mut request = request_log("exact", Some("local-cache-transaction"));
        request.id = "local-cache-transaction".to_string();

        assert_eq!(
            metrics
                .commit(MetricsTransaction::local_cache(request, 4_096))
                .await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.upstream_requests, 0);
        assert_eq!(snapshot.local_proxy.estimated_tokens_saved, 4_096);
        assert_eq!(snapshot.local_proxy.local_cache_hits, 1);
        assert_eq!(snapshot.recent_requests.len(), 1);
    }

    #[tokio::test]
    async fn metrics_transaction_local_rejection_counts_failure_without_upstream() {
        let metrics = MetricsStore::new();
        let mut request = request_log("error", None);
        request.id = "local-rejection-transaction".to_string();
        request.status = 503;
        request.upstream_request_id = None;
        request.upstream_attempt_index = None;
        request.upstream_attempt_total = None;
        request.upstream_attempts = None;
        let mut transaction = MetricsTransaction::local_rejection(request);
        transaction.observe_error("local_rejection", "cooldown active");

        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 0);
        assert_eq!(snapshot.upstream_requests, 0);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.recent_failed_requests.len(), 1);
        assert!(snapshot.recent_requests.is_empty());
        assert!(snapshot.recent_upstream_calls.is_empty());
    }

    #[tokio::test]
    async fn metrics_transaction_finishes_agent_attempt_and_inbound_atomically() {
        let metrics = MetricsStore::new();
        assert!(
            metrics
                .begin_agent_inbound(agent_inbound_start("agent-transaction", "single", 1))
                .await
        );
        assert_eq!(
            metrics
                .begin_agent_attempt(agent_attempt_start(
                    "agent-transaction",
                    "agent-attempt",
                    "primary"
                ))
                .await,
            Some(1)
        );
        let transaction = MetricsTransaction::agent_terminal(AgentTerminalSettlement {
            inbound_request_id: "agent-transaction".to_string(),
            attempt_id: "agent-attempt".to_string(),
            attempt_finish: agent_attempt_finish(AgentAttemptOutcome::HttpSuccess, Some(200)),
            request: request_log("miss", Some("agent-transaction")),
            inbound_outcome: AgentInboundOutcome::Success,
            terminal_state: Some("response_completed".to_string()),
        });

        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.agent_generation.active_attempts, 0);
        assert_eq!(snapshot.agent_generation.active_inbounds, 0);
        assert_eq!(snapshot.agent_generation.successful_inbounds, 1);
        assert_eq!(snapshot.recent_agent_upstream_attempts.len(), 1);
        assert_eq!(snapshot.recent_agent_inbound_outcomes.len(), 1);
        assert_eq!(snapshot.recent_requests.len(), 1);
        assert_eq!(snapshot.recent_upstream_calls.len(), 1);
    }

    #[tokio::test]
    async fn agent_owner_failure_synthesizes_one_failed_inbound_before_lifecycle_start() {
        let metrics = MetricsStore::new();
        let mut request = request_log("error", None);
        request.id = "owner-failure-before-start".to_string();
        request.status = 502;
        let mut transaction =
            MetricsTransaction::agent_owner_failure(AgentOwnerFailureSettlement {
                inbound_request_id: "owner-failure-before-start".to_string(),
                request,
                terminal_state: Some("owner_stopped".to_string()),
            });
        transaction.observe_error("agent_generation_owner", "owner stopped");

        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.upstream_requests, 0);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.agent_generation.inbound_requests, 1);
        assert_eq!(snapshot.agent_generation.failed_inbounds, 1);
        assert_eq!(snapshot.agent_generation.active_inbounds, 0);
        assert_eq!(snapshot.agent_generation.active_attempts, 0);
        assert_eq!(snapshot.recent_failed_requests.len(), 1);
        assert_eq!(snapshot.recent_agent_inbound_outcomes.len(), 1);
    }

    #[tokio::test]
    async fn agent_owner_failure_closes_an_active_attempt_and_inbound() {
        let metrics = MetricsStore::new();
        assert!(
            metrics
                .begin_agent_inbound(agent_inbound_start("owner-failure-active", "single", 1))
                .await
        );
        assert_eq!(
            metrics
                .begin_agent_attempt(agent_attempt_start(
                    "owner-failure-active",
                    "owner-failure-attempt",
                    "primary",
                ))
                .await,
            Some(1)
        );
        let mut request = request_log("error", None);
        request.id = "owner-failure-active".to_string();
        request.status = 502;
        let transaction = MetricsTransaction::agent_owner_failure(AgentOwnerFailureSettlement {
            inbound_request_id: "owner-failure-active".to_string(),
            request,
            terminal_state: Some("owner_stopped".to_string()),
        });

        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.upstream_requests, 1);
        assert_eq!(snapshot.agent_generation.failed_inbounds, 1);
        assert_eq!(snapshot.agent_generation.active_inbounds, 0);
        assert_eq!(snapshot.agent_generation.active_attempts, 0);
        assert_eq!(snapshot.recent_agent_upstream_attempts.len(), 1);
        assert_eq!(
            snapshot.recent_agent_upstream_attempts[0].outcome,
            AgentAttemptOutcome::RelayAborted
        );
    }

    #[tokio::test]
    async fn metrics_transaction_rejects_missing_agent_lifecycle_without_partial_commit() {
        let metrics = MetricsStore::new();
        let mut failed = request_log("error", None);
        failed.status = 502;
        failed.upstream_request_id = Some("missing-attempt".to_string());
        failed.upstream_attempt_index = Some(1);
        failed.upstream_attempt_total = Some(1);
        let mut transaction = MetricsTransaction::agent_terminal(AgentTerminalSettlement {
            inbound_request_id: "missing-agent-lifecycle".to_string(),
            attempt_id: "missing-attempt".to_string(),
            attempt_finish: agent_attempt_finish(AgentAttemptOutcome::HttpError, Some(502)),
            request: failed,
            inbound_outcome: AgentInboundOutcome::HttpError,
            terminal_state: Some("http_error".to_string()),
        });
        transaction.observe_error("pending_error", "must not be partially committed");
        transaction.observe_usage(
            UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 999,
                output_tokens: 1,
                cache_read_tokens: 0,
                cache_creation_tokens: 0,
            },
            None,
        );

        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Rejected
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 0);
        assert_eq!(snapshot.successful_requests, 0);
        assert_eq!(snapshot.upstream_requests, 0);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.usage.input_tokens, 0);
        assert_eq!(snapshot.agent_generation.inbound_requests, 0);
        assert_eq!(snapshot.agent_generation.generation_attempts, 0);
        assert_eq!(snapshot.agent_generation.failed_inbounds, 0);
        assert!(snapshot.recent_failed_requests.is_empty());
        assert!(snapshot.recent_agent_upstream_attempts.is_empty());
        assert!(snapshot.recent_agent_inbound_outcomes.is_empty());
        assert!(snapshot.recent_requests.is_empty());
        assert!(snapshot.recent_upstream_calls.is_empty());
    }

    #[tokio::test]
    async fn rejected_metrics_transaction_does_not_consume_the_commit_key() {
        let metrics = MetricsStore::new();
        let rejected = MetricsTransaction::agent_terminal(AgentTerminalSettlement {
            inbound_request_id: "reusable-agent-key".to_string(),
            attempt_id: "reusable-attempt".to_string(),
            attempt_finish: agent_attempt_finish(AgentAttemptOutcome::HttpError, Some(502)),
            request: request_log("error", None),
            inbound_outcome: AgentInboundOutcome::HttpError,
            terminal_state: Some("http_error".to_string()),
        });
        assert_eq!(
            metrics.commit(rejected).await,
            MetricsCommitResult::Rejected
        );

        let mut valid_request = request_log("miss", Some("reusable-agent-key"));
        valid_request.id = "reusable-agent-key".to_string();
        let valid = MetricsTransaction::upstream(valid_request);
        assert_eq!(metrics.commit(valid).await, MetricsCommitResult::Applied);
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.recent_requests.len(), 1);
    }

    #[tokio::test]
    async fn metrics_transaction_rejects_attempt_from_another_inbound() {
        let metrics = MetricsStore::new();
        for inbound_id in ["inbound-a", "inbound-b"] {
            assert!(
                metrics
                    .begin_agent_inbound(agent_inbound_start(inbound_id, "single", 1))
                    .await
            );
            assert_eq!(
                metrics
                    .begin_agent_attempt(agent_attempt_start(
                        inbound_id,
                        &format!("attempt-{}", &inbound_id[inbound_id.len() - 1..]),
                        "primary"
                    ))
                    .await,
                Some(1)
            );
        }
        let transaction = MetricsTransaction::agent_terminal(AgentTerminalSettlement {
            inbound_request_id: "inbound-a".to_string(),
            attempt_id: "attempt-b".to_string(),
            attempt_finish: agent_attempt_finish(AgentAttemptOutcome::HttpSuccess, Some(200)),
            request: request_log("miss", Some("wrong-attempt")),
            inbound_outcome: AgentInboundOutcome::Success,
            terminal_state: Some("response_completed".to_string()),
        });

        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Rejected
        );
        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 0);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.agent_generation.active_inbounds, 2);
        assert_eq!(snapshot.agent_generation.active_attempts, 2);
        assert!(snapshot.recent_agent_upstream_attempts.is_empty());
        assert!(snapshot.recent_agent_inbound_outcomes.is_empty());
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
    async fn persisted_trend_accepts_only_one_successful_applied_transaction() {
        let metrics = MetricsStore::new();
        let hour_timestamp = Utc::now().timestamp().div_euclid(60 * 60) * 60 * 60;
        let hour = DateTime::<Utc>::from_timestamp(hour_timestamp, 0)
            .expect("current UTC hour must be representable");

        let mut success = request_log("miss", Some("trend-success"));
        success.id = "trend-success".to_string();
        success.at = hour + Duration::minutes(1);
        success.agent_id = Some("codex".to_string());
        success.agent_label = Some("Codex".to_string());
        success.provider_id = Some("provider-a".to_string());
        success.input_tokens = Some(1_000);
        success.output_tokens = Some(25);
        success.cache_read_tokens = Some(800);
        success.cache_shortfall_tokens = Some(200);
        success.cache_avoidable_gap_tokens = Some(80);
        success.cache_new_tail_gap_tokens = Some(120);
        let mut transaction = MetricsTransaction::upstream(success);
        transaction.observe_usage(
            UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 1_000,
                output_tokens: 25,
                cache_read_tokens: 800,
                cache_creation_tokens: 64,
            },
            None,
        );
        assert_eq!(
            metrics.commit(transaction.clone()).await,
            MetricsCommitResult::Applied
        );
        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Duplicate
        );

        let mut failed = request_log("error", Some("trend-failed"));
        failed.id = "trend-failed".to_string();
        failed.at = hour + Duration::minutes(2);
        failed.status = 502;
        failed.agent_id = Some("codex".to_string());
        failed.provider_id = Some("provider-a".to_string());
        failed.input_tokens = Some(9_999);
        failed.cache_read_tokens = Some(9_999);
        assert_eq!(
            metrics.commit(MetricsTransaction::upstream(failed)).await,
            MetricsCommitResult::Applied
        );

        let trend = metrics
            .trend(MetricsTrendQueryInput {
                start_utc: hour.to_rfc3339(),
                end_utc: (hour + Duration::hours(1)).to_rfc3339(),
                agent_id: "codex".to_string(),
                provider_id: Some("provider-a".to_string()),
                include_cold_starts: true,
            })
            .expect("trend query should succeed");
        assert_eq!(trend.summary.successful_requests, 1);
        assert_eq!(trend.summary.input_tokens, 1_000);
        assert_eq!(trend.summary.cache_read_tokens, 800);
        assert_eq!(trend.summary.cache_miss_tokens, 200);
        assert_eq!(trend.summary.cache_creation_tokens, 64);
        assert_eq!(trend.summary.cache_shortfall_tokens, 200);
        assert_eq!(trend.summary.cache_avoidable_gap_tokens, 80);
        assert_eq!(trend.summary.cache_new_tail_gap_tokens, 120);
    }

    #[tokio::test]
    async fn local_cache_replay_does_not_duplicate_upstream_usage_in_trend() {
        let metrics = MetricsStore::new();
        let hour_timestamp = Utc::now().timestamp().div_euclid(60 * 60) * 60 * 60;
        let hour = DateTime::<Utc>::from_timestamp(hour_timestamp, 0)
            .expect("current UTC hour must be representable");

        let mut upstream = request_log("miss", Some("trend-local-cache-replay"));
        upstream.id = "trend-upstream".to_string();
        upstream.at = hour + Duration::minutes(1);
        upstream.agent_id = Some("codex".to_string());
        upstream.agent_label = Some("Codex".to_string());
        upstream.provider_id = Some("provider-a".to_string());
        upstream.input_tokens = Some(1_000);
        upstream.output_tokens = Some(25);
        upstream.cache_read_tokens = Some(800);
        let mut transaction = MetricsTransaction::upstream(upstream.clone());
        transaction.observe_usage(
            UsageRecord {
                provider: "provider".to_string(),
                model: "model".to_string(),
                input_tokens: 1_000,
                output_tokens: 25,
                cache_read_tokens: 800,
                cache_creation_tokens: 64,
            },
            None,
        );
        assert_eq!(
            metrics.commit(transaction).await,
            MetricsCommitResult::Applied
        );

        let mut cached = upstream;
        cached.id = "trend-local-cache-replay".to_string();
        cached.at = hour + Duration::minutes(2);
        cached.cache_status = "exact".to_string();
        assert_eq!(
            metrics
                .commit(MetricsTransaction::local_cache(cached, 1_000))
                .await,
            MetricsCommitResult::Applied
        );

        let trend = metrics
            .trend(MetricsTrendQueryInput {
                start_utc: hour.to_rfc3339(),
                end_utc: (hour + Duration::hours(1)).to_rfc3339(),
                agent_id: "codex".to_string(),
                provider_id: Some("provider-a".to_string()),
                include_cold_starts: true,
            })
            .expect("trend query should succeed");
        assert_eq!(trend.summary.successful_requests, 1);
        assert_eq!(trend.summary.input_tokens, 1_000);
        assert_eq!(trend.summary.cache_read_tokens, 800);

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 2);
        assert_eq!(snapshot.successful_requests, 2);
        assert_eq!(snapshot.local_proxy.local_cache_hits, 1);
        assert_eq!(snapshot.local_proxy.estimated_tokens_saved, 1_000);
        assert_eq!(snapshot.recent_requests.len(), 2);
        assert_eq!(snapshot.usage.input_tokens, 1_000);
        assert_eq!(snapshot.usage.cache_read_tokens, 800);
    }

    #[tokio::test]
    async fn agent_provider_success_count_outlives_recent_history_limit() {
        let metrics = MetricsStore::new();
        for index in 0..201 {
            let mut log = request_log("miss", Some("agent-success-history"));
            log.id = format!("codex-success-{index}");
            log.agent_id = Some("codex".to_string());
            log.agent_label = Some("Codex".to_string());
            log.provider = "shared-provider".to_string();
            log.provider_id = Some("shared-provider-id".to_string());
            log.input_tokens = Some(10);
            log.output_tokens = Some(2);
            log.cache_read_tokens = Some(9);
            log.cache_shortfall_tokens = Some(10);
            log.cache_avoidable_gap_tokens = Some(3);
            log.cache_new_tail_gap_tokens = Some(7);
            log.cold_start = (index == 0).then_some(true);
            if index == 0 {
                log.cache_status = "compact".to_string();
                log.upstream_call_source = Some("responses-compaction-v2".to_string());
            }
            metrics.record_request(log, true).await;
        }
        let mut failed = request_log("error", Some("agent-failed-history"));
        failed.id = "codex-failed".to_string();
        failed.status = 502;
        failed.agent_id = Some("codex".to_string());
        failed.agent_label = Some("Codex".to_string());
        failed.provider = "shared-provider".to_string();
        failed.provider_id = Some("shared-provider-id".to_string());
        failed.input_tokens = Some(9_999);
        failed.output_tokens = Some(9_999);
        failed.cache_read_tokens = Some(9_999);
        metrics.record_request(failed, true).await;
        for index in 0..3 {
            let mut log = request_log("miss", Some("other-agent-success-history"));
            log.id = format!("claude-success-{index}");
            log.agent_id = Some("claude-code".to_string());
            log.agent_label = Some("Claude Code".to_string());
            log.provider = "shared-provider".to_string();
            log.provider_id = Some("shared-provider-id".to_string());
            metrics.record_request(log, true).await;
        }

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.recent_requests.len(), 200);
        let codex = snapshot
            .agent_provider_stats
            .iter()
            .find(|item| item.agent_id == "codex" && item.provider_id == "shared-provider-id")
            .expect("Codex aggregate should be present");
        assert_eq!(codex.successful_requests, 201);
        assert_eq!(codex.total_requests, 202);
        assert_eq!(codex.error_statuses, 1);
        assert_eq!(codex.input_tokens, 2_010);
        assert_eq!(codex.output_tokens, 402);
        assert_eq!(codex.cache_read_tokens, 1_809);
        assert_eq!(codex.compaction_requests, 1);
        assert_eq!(codex.cache_shortfall_tokens, 2_010);
        assert_eq!(codex.cache_avoidable_gap_tokens, 603);
        assert_eq!(codex.cache_new_tail_gap_tokens, 1_407);
        assert_eq!(codex.cold_start_requests, 1);
        assert_eq!(codex.cold_start_cache_shortfall_tokens, 10);
        assert_eq!(codex.cold_start_cache_avoidable_gap_tokens, 3);
        assert_eq!(codex.cold_start_cache_new_tail_gap_tokens, 7);
        let claude = snapshot
            .agent_provider_stats
            .iter()
            .find(|item| item.agent_id == "claude-code")
            .expect("Claude aggregate should be present");
        assert_eq!(claude.successful_requests, 3);
    }

    #[tokio::test]
    async fn compact_history_without_usage_keeps_cold_start_unknown() {
        let metrics = MetricsStore::new();
        let mut compact = request_log("compact", Some("compact-history"));
        compact.id = "compact-history".to_string();
        compact.upstream_call_source = Some("responses-compaction-v2".to_string());
        compact.input_tokens = Some(9_216);
        compact.cache_read_tokens = Some(0);

        assert_eq!(
            metrics.commit(MetricsTransaction::upstream(compact)).await,
            MetricsCommitResult::Applied
        );

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.recent_requests[0].cache_status, "compact");
        assert_eq!(snapshot.recent_requests[0].cold_start, None);
        assert_eq!(snapshot.recent_upstream_calls[0].cold_start, None);
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
    async fn reasoning_rejection_counts_one_failed_single_attempt_inbound() {
        let metrics = MetricsStore::new();
        assert!(
            metrics
                .begin_agent_inbound(agent_inbound_start("reasoning-inbound", "single", 1))
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
        let mut failed_log = request_log("error", None);
        failed_log.status = 502;
        assert!(
            metrics
                .finish_agent_inbound(
                    "reasoning-inbound",
                    failed_log,
                    AgentInboundOutcome::HttpError,
                    Some("reasoning_effort_rejected".to_string())
                )
                .await
        );

        let snapshot = metrics.snapshot().await;
        assert_eq!(snapshot.total_requests, 1);
        assert_eq!(snapshot.successful_requests, 0);
        assert_eq!(snapshot.upstream_requests, 1);
        assert_eq!(snapshot.agent_generation.inbound_requests, 1);
        assert_eq!(snapshot.agent_generation.successful_inbounds, 0);
        assert_eq!(snapshot.agent_generation.failed_inbounds, 1);
        assert_eq!(snapshot.agent_generation.generation_attempts, 1);
        assert_eq!(snapshot.agent_generation.multi_attempt_inbounds, 0);
        assert_eq!(snapshot.agent_generation.max_attempts_per_inbound, 1);
        assert!(snapshot.recent_requests.is_empty());
        assert!(snapshot.recent_upstream_calls.is_empty());
        assert_eq!(snapshot.recent_failed_requests.len(), 1);
        assert_eq!(snapshot.recent_agent_upstream_attempts.len(), 1);
        assert_eq!(snapshot.provider_stats[0].total_requests, 1);
        assert_eq!(snapshot.provider_stats[0].successful_requests, 0);
        assert_eq!(snapshot.provider_stats[0].upstream_requests, 1);
        assert_eq!(
            snapshot.recent_agent_inbound_outcomes[0].outcome,
            AgentInboundOutcome::HttpError
        );
        assert_eq!(snapshot.recent_agent_upstream_attempts[0].status, Some(502));
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
