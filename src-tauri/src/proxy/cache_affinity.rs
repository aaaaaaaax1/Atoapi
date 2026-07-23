use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::Instant;

use super::affinity_identity::AffinityIdentity;

pub(crate) const SHADOW_POLICY_EPOCH: u64 = 2;
pub(super) const SHADOW_ASSIGNMENT_LIMIT: usize = 4096;
pub(super) const SHADOW_ASSIGNMENT_TTL_HOURS: i64 = 24;
pub(super) const STATIC_COHORT_CANARY_PERCENT: u8 = 5;
const GIANT_TAIL_CHARS: u64 = 80_000;
const POST_BURST_FOLLOWUP_REQUESTS: u8 = 3;
const POST_COMPACTION_FOLLOWUP_REQUESTS: u8 = 4;
const POST_BURST_WINDOW_TTL_HOURS: i64 = 4;
const POST_BURST_EVIDENCE_TTL_HOURS: i64 = 24;
const POST_BURST_WINDOW_LIMIT: usize = 512;
const POST_BURST_EVIDENCE_LIMIT: usize = 1536;
const POST_BURST_READINESS_MAX_AGE_HOURS: i64 = POST_BURST_EVIDENCE_TTL_HOURS;
const SHADOW_SETTLEMENT_MAINTENANCE_BUDGET: usize = 32;
const POST_BURST_MIN_ARM_OBSERVATIONS: u64 = 9;
const POST_BURST_MIN_CANARY_OBSERVATIONS: u64 = 3;
const POST_BURST_MIN_EFFICACY_OBSERVATIONS: u64 = 9;
const POST_BURST_MIN_PROMOTION_OBSERVATIONS: u64 = 18;
const POST_BURST_MIN_USAGE_COVERAGE_BPS: u64 = 8_000;
const POST_BURST_MAX_PROVIDER_UNSTABLE_BPS: u64 = 2_500;
const POST_BURST_MAX_INPUT_IMBALANCE_BPS: u64 = 5_000;
const POST_BURST_ROLLBACK_SUCCESS_DELTA_BPS: u64 = 1_000;
const POST_BURST_ROLLBACK_CACHE_DELTA_BPS: u64 = 500;
const POST_BURST_ROLLBACK_AVOIDABLE_DELTA: u64 = 2_048;
const POST_BURST_ROLLBACK_TTFT_DELTA_MS: u64 = 1_000;
const POST_BURST_PROMOTION_MAX_ERROR_DELTA_BPS: u64 = 50;
const POST_BURST_PROMOTION_TTFT_P50_DELTA_MS: u64 = 200;
const POST_BURST_PROMOTION_TTFT_P95_DELTA_MS: u64 = 300;
const POST_BURST_RESIDUAL_CACHE_GAP_BPS: u64 = 9_995;
// The live route is deliberately tiny: one current scope per exact upstream
// realm.  It is a placement-only experiment, so it must earn the right to
// remain enabled from the upstream's real usage before it can stay sticky.
#[cfg(test)]
const ACTIVE_CACHE_ROUTE_MIN_BASELINE_SUCCESSFUL_OBSERVATIONS: u64 = 4;
#[cfg(test)]
const ACTIVE_CACHE_ROUTE_MIN_BASELINE_USAGE_OBSERVATIONS: u64 = 4;
#[cfg(test)]
const ACTIVE_CACHE_ROUTE_MIN_BASELINE_INPUT_TOKENS: u64 = 32 * 1024;
const ACTIVE_CACHE_ROUTE_MIN_CANDIDATE_SUCCESSFUL_OBSERVATIONS: u64 = 18;
const ACTIVE_CACHE_ROUTE_MAX_INCONCLUSIVE_OBSERVATIONS: u64 = 3;
const ACTIVE_CACHE_ROUTE_TTFT_REGRESSION_MS: u64 = 500;
const ACTIVE_CACHE_ROUTE_TTFT_REGRESSION_BPS: u64 = 12_500;
const ACTIVE_CACHE_ROUTE_TTFT_SAMPLE_LIMIT: usize = 24;
const ACTIVE_CACHE_ROUTE_LEASE_HOURS: i64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShadowCacheLane {
    Steady,
    ToolBurstQuarantine,
    CompactedAnchor,
    Transparent,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShadowCacheCandidateVariant {
    #[default]
    CohortKey,
    CohortTwoShard,
    ProviderNative,
}

impl Default for ShadowCacheLane {
    fn default() -> Self {
        Self::Transparent
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShadowAffinityArm {
    #[default]
    Baseline,
    Candidate,
}

/// Per-conversation lifecycle for the only production-affecting cache
/// affinity path.  The route never changes model input; it only selects a
/// stable root `prompt_cache_key` after baseline traffic proves there is room
/// for a real cache improvement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ActiveCacheRouteState {
    #[default]
    Baseline,
    Candidate,
    Promoted,
    RolledBack,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct ActiveCacheRouteEvidence {
    pub(crate) observations: u64,
    pub(crate) successful_observations: u64,
    pub(crate) usage_observations: u64,
    pub(crate) inconclusive_observations: u64,
    pub(crate) input_tokens: u64,
    pub(crate) cache_read_tokens: u64,
    pub(crate) avoidable_gap_tokens: u64,
    pub(crate) provider_unstable_gap_tokens: u64,
    pub(crate) multi_attempt_observations: u64,
    pub(crate) ttft_samples: VecDeque<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ShadowAffinityAssignment {
    pub(crate) conversation_id: String,
    pub(crate) cohort_id: String,
    pub(crate) realm_id: String,
    pub(crate) policy_epoch: u64,
    pub(crate) lane: ShadowCacheLane,
    #[serde(default)]
    pub(crate) arm: ShadowAffinityArm,
    pub(crate) shard: u32,
    pub(crate) anchor_epoch: u64,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) last_seen_at: DateTime<Utc>,
    #[serde(default)]
    pub(crate) observations: u64,
    #[serde(default)]
    pub(crate) successful_observations: u64,
    #[serde(default)]
    pub(crate) usage_observations: u64,
    #[serde(default)]
    pub(crate) inconclusive_observations: u64,
    #[serde(default)]
    pub(crate) input_tokens: u64,
    #[serde(default)]
    pub(crate) cache_read_tokens: u64,
    #[serde(default)]
    pub(crate) active_cache_route_state: ActiveCacheRouteState,
    #[serde(default)]
    pub(crate) active_cache_route_baseline: ActiveCacheRouteEvidence,
    #[serde(default)]
    pub(crate) active_cache_route_candidate: ActiveCacheRouteEvidence,
    #[serde(default)]
    pub(crate) active_cache_route_reason: Option<String>,
    /// Legacy aggregate observations can seed the first post-upgrade scope
    /// once.  Any scope boundary consumes this forever so pre-boundary
    /// metrics can never be replayed into a fresh cohort or compaction epoch.
    #[serde(default)]
    pub(crate) active_cache_route_legacy_seed_consumed: bool,
    #[serde(default)]
    pub(crate) active_cache_route_valid_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PostBurstWindow {
    pub(crate) window_id: u64,
    pub(crate) conversation_id: String,
    pub(crate) opened_at: DateTime<Utc>,
    pub(crate) expires_at: DateTime<Utc>,
    pub(crate) remaining_requests: u8,
    pub(crate) captured_requests: u8,
    #[serde(default = "default_post_burst_window_lane")]
    pub(crate) lane: ShadowCacheLane,
    #[serde(default)]
    pub(crate) candidate_variant: ShadowCacheCandidateVariant,
    #[serde(default)]
    pub(crate) realm_id: String,
    #[serde(default)]
    pub(crate) policy_epoch: u64,
    #[serde(default)]
    pub(crate) anchor_epoch: u64,
}

fn default_post_burst_window_lane() -> ShadowCacheLane {
    ShadowCacheLane::ToolBurstQuarantine
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PostBurstEvidence {
    pub(crate) window_id: u64,
    pub(crate) conversation_id: String,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) followup_index: u8,
    pub(crate) realm_id: String,
    pub(crate) lane: ShadowCacheLane,
    #[serde(default)]
    pub(crate) candidate_variant: ShadowCacheCandidateVariant,
    pub(crate) arm: ShadowAffinityArm,
    pub(crate) policy_epoch: u64,
    pub(crate) anchor_epoch: u64,
    pub(crate) success: bool,
    pub(crate) status: u16,
    pub(crate) has_usage: bool,
    pub(crate) input_tokens: u64,
    pub(crate) cache_read_tokens: u64,
    pub(crate) cache_ratio_bps: u64,
    pub(crate) avoidable_gap_tokens: u64,
    pub(crate) provider_unstable_gap_tokens: u64,
    pub(crate) ttft_ms: u64,
    pub(crate) attempt_count: u64,
    pub(crate) candidate_applied: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(crate) struct PostBurstPhaseSummary {
    pub(crate) observations: u64,
    pub(crate) successful_observations: u64,
    pub(crate) usage_observations: u64,
    pub(crate) applied_observations: u64,
    pub(crate) multi_attempt_observations: u64,
    pub(crate) provider_unstable_observations: u64,
    pub(crate) success_rate_bps: u64,
    pub(crate) usage_coverage_bps: u64,
    pub(crate) cache_ratio_bps: u64,
    pub(crate) average_input_tokens: u64,
    pub(crate) average_avoidable_gap_tokens: u64,
    pub(crate) average_provider_unstable_gap_tokens: u64,
    pub(crate) provider_unstable_ratio_bps: u64,
    pub(crate) average_ttft_ms: u64,
    pub(crate) ttft_p50_ms: u64,
    pub(crate) ttft_p95_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(crate) struct PostBurstArmSummary {
    pub(crate) observations: u64,
    pub(crate) successful_observations: u64,
    pub(crate) usage_observations: u64,
    pub(crate) applied_observations: u64,
    pub(crate) multi_attempt_observations: u64,
    pub(crate) provider_unstable_observations: u64,
    pub(crate) success_rate_bps: u64,
    pub(crate) usage_coverage_bps: u64,
    pub(crate) cache_ratio_bps: u64,
    pub(crate) average_input_tokens: u64,
    pub(crate) average_avoidable_gap_tokens: u64,
    pub(crate) average_provider_unstable_gap_tokens: u64,
    pub(crate) provider_unstable_ratio_bps: u64,
    pub(crate) average_ttft_ms: u64,
    pub(crate) ttft_p50_ms: u64,
    pub(crate) ttft_p95_ms: u64,
    pub(crate) first_followup: PostBurstPhaseSummary,
    pub(crate) stable_followups: PostBurstPhaseSummary,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PostBurstReadinessStatus {
    #[default]
    InsufficientEvidence,
    ReadyForCanary,
    CanaryCollecting,
    CanaryHealthy,
    ReadyForPromotion,
    RollbackRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PostBurstReadiness {
    pub(crate) comparison_key: String,
    pub(crate) realm_id: String,
    pub(crate) lane: ShadowCacheLane,
    #[serde(default)]
    pub(crate) candidate_variant: ShadowCacheCandidateVariant,
    pub(crate) status: PostBurstReadinessStatus,
    pub(crate) reason: String,
    pub(crate) baseline: PostBurstArmSummary,
    pub(crate) candidate_shadow: PostBurstArmSummary,
    #[serde(default)]
    pub(crate) canary_baseline: PostBurstArmSummary,
    pub(crate) candidate_applied: PostBurstArmSummary,
    pub(crate) updated_at: DateTime<Utc>,
    #[serde(default)]
    pub(crate) evidence_generation: u64,
    #[serde(default)]
    pub(crate) valid_until: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct PostBurstEvidenceLedger {
    #[serde(default)]
    pub(crate) next_window_id: u64,
    #[serde(default)]
    pub(crate) windows: HashMap<String, PostBurstWindow>,
    #[serde(default)]
    pub(crate) evidence: VecDeque<PostBurstEvidence>,
    #[serde(default)]
    pub(crate) readiness: HashMap<String, PostBurstReadiness>,
    #[serde(default)]
    pub(crate) rollbacks: HashMap<String, String>,
    #[serde(default)]
    pub(crate) evidence_generations: HashMap<String, u64>,
    #[serde(skip)]
    window_age_index: BTreeSet<(DateTime<Utc>, String)>,
    #[serde(skip)]
    evidence_scope_latest_expiry: HashMap<String, DateTime<Utc>>,
    #[serde(skip)]
    evidence_scope_expiries: HashMap<String, BTreeMap<DateTime<Utc>, u32>>,
    #[serde(skip)]
    evidence_scope_indexed_len: usize,
    #[serde(skip)]
    evidence_scope_records: HashMap<String, VecDeque<PostBurstEvidence>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PostBurstEvidenceStatus {
    pub(crate) open_windows: usize,
    pub(crate) evidence_records: usize,
    pub(crate) readiness: Vec<PostBurstReadiness>,
    pub(crate) rollback_keys: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ShadowAffinityStore {
    #[serde(default)]
    pub assignments: HashMap<String, ShadowAffinityAssignment>,
    #[serde(default)]
    pub(crate) post_burst: PostBurstEvidenceLedger,
    /// One owner per exact upstream/key realm keeps the live canary bounded
    /// without scanning every conversation on the send path.
    #[serde(default)]
    pub(crate) active_cache_route_owners: HashMap<String, String>,
    #[serde(skip)]
    assignment_age_index: BTreeSet<(DateTime<Utc>, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ShadowAffinityDecision {
    pub mode: String,
    pub assignment_key: Option<String>,
    pub realm_id: String,
    pub cohort_id: String,
    pub lane: ShadowCacheLane,
    pub candidate_variant: ShadowCacheCandidateVariant,
    pub arm: ShadowAffinityArm,
    pub shard: u32,
    pub policy_epoch: u64,
    pub anchor_epoch: u64,
    pub trusted_identity: bool,
    pub decision: String,
    pub skip_reason: Option<String>,
    pub policy_compute_ms: u64,
    #[serde(default)]
    pub validation_run_id: Option<String>,
    #[serde(default)]
    pub automatic_canary_status: Option<PostBurstReadinessStatus>,
    #[serde(default)]
    pub automatic_canary_reason: Option<String>,
}

/// A lock-free fallback for the request hot path.  Affinity is an optimization
/// and its persisted state is never conversation truth, so a background
/// runtime snapshot must not hold an inbound request before the first upstream
/// byte.  The caller records this decision as shadow-only and skips controlled
/// candidate routing for this one request.
pub(super) fn shadow_affinity_snapshot_lock_busy(
    identity: &AffinityIdentity,
    policy_compute_ms: u64,
) -> ShadowAffinityDecision {
    ShadowAffinityDecision {
        mode: "shadow".to_string(),
        assignment_key: None,
        realm_id: identity.realm_id.clone(),
        cohort_id: identity.cohort_id.clone(),
        lane: ShadowCacheLane::Transparent,
        candidate_variant: ShadowCacheCandidateVariant::CohortKey,
        arm: ShadowAffinityArm::Baseline,
        shard: 0,
        policy_epoch: SHADOW_POLICY_EPOCH,
        anchor_epoch: 0,
        trusted_identity: identity.trusted_conversation_id.is_some(),
        decision: "snapshot_lock_busy".to_string(),
        skip_reason: Some("runtime_snapshot_lock_busy".to_string()),
        policy_compute_ms,
        validation_run_id: None,
        automatic_canary_status: None,
        automatic_canary_reason: None,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ShadowObservationInput {
    pub success: bool,
    pub has_usage: bool,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
    pub giant_tail: bool,
    pub compaction_boundary: bool,
    pub status: u16,
    pub ttft_ms: u64,
    pub avoidable_gap_tokens: u64,
    pub provider_unstable_gap_tokens: u64,
    pub attempt_count: u64,
}

fn ratio_bps(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    ((numerator as u128 * 10_000) / denominator as u128).min(10_000) as u64
}

/// Compare provider-reported cache ratios without rounding.  One token of
/// genuine improvement/regression matters to the route lifecycle; bps remain
/// suitable for dashboards but are too coarse for promotion decisions.
fn active_cache_route_ratio_is_strictly_lower(
    left_cache_read_tokens: u64,
    left_input_tokens: u64,
    right_cache_read_tokens: u64,
    right_input_tokens: u64,
) -> bool {
    left_input_tokens > 0
        && right_input_tokens > 0
        && (left_cache_read_tokens as u128 * right_input_tokens as u128)
            < (right_cache_read_tokens as u128 * left_input_tokens as u128)
}

fn active_cache_route_ratio_is_strictly_higher(
    left_cache_read_tokens: u64,
    left_input_tokens: u64,
    right_cache_read_tokens: u64,
    right_input_tokens: u64,
) -> bool {
    active_cache_route_ratio_is_strictly_lower(
        right_cache_read_tokens,
        right_input_tokens,
        left_cache_read_tokens,
        left_input_tokens,
    )
}

#[cfg(test)]
fn active_cache_route_ratio_is_below_target(
    evidence: &ActiveCacheRouteEvidence,
    target_bps: u64,
) -> bool {
    evidence.input_tokens > 0
        && (evidence.cache_read_tokens as u128 * 10_000)
            < (evidence.input_tokens as u128 * target_bps as u128)
}

fn active_cache_route_ttft_p95_ms(evidence: &ActiveCacheRouteEvidence) -> u64 {
    let mut samples = evidence.ttft_samples.iter().copied().collect::<Vec<_>>();
    percentile_ms(&mut samples, 95)
}

fn record_active_cache_route_evidence(
    evidence: &mut ActiveCacheRouteEvidence,
    input: ShadowObservationInput,
) {
    evidence.observations = evidence.observations.saturating_add(1);
    evidence.successful_observations = evidence
        .successful_observations
        .saturating_add(u64::from(input.success));
    evidence.multi_attempt_observations = evidence
        .multi_attempt_observations
        .saturating_add(u64::from(input.attempt_count.max(1) != 1));
    if input.success && input.ttft_ms > 0 {
        evidence.ttft_samples.push_back(input.ttft_ms);
        while evidence.ttft_samples.len() > ACTIVE_CACHE_ROUTE_TTFT_SAMPLE_LIMIT {
            evidence.ttft_samples.pop_front();
        }
    }
    if input.has_usage && input.input_tokens > 0 {
        evidence.usage_observations = evidence.usage_observations.saturating_add(1);
        evidence.input_tokens = evidence.input_tokens.saturating_add(input.input_tokens);
        evidence.cache_read_tokens = evidence
            .cache_read_tokens
            .saturating_add(input.cache_read_tokens);
        evidence.avoidable_gap_tokens = evidence
            .avoidable_gap_tokens
            .saturating_add(input.avoidable_gap_tokens);
        evidence.provider_unstable_gap_tokens = evidence
            .provider_unstable_gap_tokens
            .saturating_add(input.provider_unstable_gap_tokens);
    } else {
        evidence.inconclusive_observations = evidence.inconclusive_observations.saturating_add(1);
    }
}

/// Existing releases persisted aggregate shadow observations but not the new
/// route-specific evidence.  Seed the first baseline from those exact same
/// upstream usage records so a busy live conversation can be admitted without
/// making the user create another twenty sessions.  No synthetic usage is
/// introduced: missing historical TTFT samples simply leave the TTFT guard
/// inactive until fresh evidence arrives.
#[cfg(test)]
fn seed_active_cache_route_baseline(assignment: &mut ShadowAffinityAssignment) {
    if assignment.active_cache_route_baseline.observations > 0
        || assignment.active_cache_route_legacy_seed_consumed
        || assignment.observations == 0
    {
        return;
    }
    assignment.active_cache_route_baseline = ActiveCacheRouteEvidence {
        observations: assignment.observations,
        successful_observations: assignment.successful_observations,
        usage_observations: assignment.usage_observations,
        inconclusive_observations: assignment.inconclusive_observations,
        input_tokens: assignment.input_tokens,
        cache_read_tokens: assignment.cache_read_tokens,
        ..ActiveCacheRouteEvidence::default()
    };
    assignment.active_cache_route_legacy_seed_consumed = true;
}

#[cfg(test)]
fn active_cache_route_baseline_is_eligible(evidence: &ActiveCacheRouteEvidence) -> bool {
    evidence.successful_observations >= ACTIVE_CACHE_ROUTE_MIN_BASELINE_SUCCESSFUL_OBSERVATIONS
        && evidence.usage_observations >= ACTIVE_CACHE_ROUTE_MIN_BASELINE_USAGE_OBSERVATIONS
        && evidence.input_tokens >= ACTIVE_CACHE_ROUTE_MIN_BASELINE_INPUT_TOKENS
        && evidence.multi_attempt_observations == 0
        && !evidence.ttft_samples.is_empty()
        && active_cache_route_ratio_is_below_target(evidence, POST_BURST_RESIDUAL_CACHE_GAP_BPS)
}

fn active_cache_route_is_applied(decision: &ShadowAffinityDecision) -> bool {
    matches!(
        decision.decision.as_str(),
        "active_cache_candidate_applied" | "active_cache_route_promoted"
    )
}

fn active_cache_route_rollback_reason(
    baseline: &ActiveCacheRouteEvidence,
    candidate: &ActiveCacheRouteEvidence,
    input: ShadowObservationInput,
) -> Option<&'static str> {
    if !input.success {
        return Some("active_cache_route_upstream_error");
    }
    if input.attempt_count.max(1) != 1 {
        return Some("active_cache_route_attempt_regression");
    }
    if !input.has_usage || input.input_tokens == 0 {
        return (candidate.inconclusive_observations
            >= ACTIVE_CACHE_ROUTE_MAX_INCONCLUSIVE_OBSERVATIONS)
            .then_some("active_cache_route_usage_inconclusive");
    }

    // The route has no semantic effect and must not retain even one genuine
    // raw cache-ratio loss.  This compares only provider-returned
    // cached_tokens/input_tokens, never a local "equivalent" estimate.
    if baseline.input_tokens > 0
        && active_cache_route_ratio_is_strictly_lower(
            input.cache_read_tokens,
            input.input_tokens,
            baseline.cache_read_tokens,
            baseline.input_tokens,
        )
    {
        return Some("active_cache_route_raw_cache_regression");
    }

    let baseline_ttft_p95 = active_cache_route_ttft_p95_ms(baseline);
    if baseline_ttft_p95 > 0
        && input.ttft_ms > baseline_ttft_p95.saturating_add(ACTIVE_CACHE_ROUTE_TTFT_REGRESSION_MS)
        && input.ttft_ms.saturating_mul(10_000)
            > baseline_ttft_p95.saturating_mul(ACTIVE_CACHE_ROUTE_TTFT_REGRESSION_BPS)
    {
        return Some("active_cache_route_ttft_regression");
    }
    None
}

fn reset_active_cache_route(assignment: &mut ShadowAffinityAssignment) {
    assignment.active_cache_route_state = ActiveCacheRouteState::Baseline;
    assignment.active_cache_route_baseline = ActiveCacheRouteEvidence::default();
    assignment.active_cache_route_candidate = ActiveCacheRouteEvidence::default();
    assignment.active_cache_route_reason = None;
    assignment.active_cache_route_legacy_seed_consumed = true;
    assignment.active_cache_route_valid_until = None;
}

fn clear_active_cache_route_owner(
    store: &mut ShadowAffinityStore,
    realm_id: &str,
    conversation_id: &str,
) {
    if store
        .active_cache_route_owners
        .get(realm_id)
        .is_some_and(|owner| owner == conversation_id)
    {
        store.active_cache_route_owners.remove(realm_id);
    }
}

fn cacheable_input_tokens_128(input_tokens: u64) -> u64 {
    if input_tokens < 1024 {
        0
    } else {
        1024 + ((input_tokens - 1024) / 128) * 128
    }
}

fn percentile_ms(samples: &mut [u64], percentile: usize) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let index = ((samples.len() * percentile).div_ceil(100))
        .saturating_sub(1)
        .min(samples.len() - 1);
    samples[index]
}

fn shadow_cache_lane_key(lane: ShadowCacheLane) -> &'static str {
    match lane {
        ShadowCacheLane::Steady => "steady",
        ShadowCacheLane::ToolBurstQuarantine => "tool_burst_quarantine",
        ShadowCacheLane::CompactedAnchor => "compacted_anchor",
        ShadowCacheLane::Transparent => "transparent",
    }
}

fn shadow_cache_candidate_key(candidate: ShadowCacheCandidateVariant) -> &'static str {
    match candidate {
        ShadowCacheCandidateVariant::CohortKey => "cohort_key",
        ShadowCacheCandidateVariant::CohortTwoShard => "cohort_two_shard",
        ShadowCacheCandidateVariant::ProviderNative => "provider_native",
    }
}

#[cfg(test)]
fn post_burst_comparison_key(realm_id: &str, lane: ShadowCacheLane) -> String {
    post_burst_comparison_key_for_candidate(realm_id, lane, ShadowCacheCandidateVariant::CohortKey)
}

fn post_burst_comparison_key_for_candidate(
    realm_id: &str,
    lane: ShadowCacheLane,
    candidate: ShadowCacheCandidateVariant,
) -> String {
    format!(
        "{realm_id}:{}:{}:epoch-{SHADOW_POLICY_EPOCH}",
        shadow_cache_lane_key(lane),
        shadow_cache_candidate_key(candidate)
    )
}

fn post_burst_evidence_scope_key(observation: &PostBurstEvidence) -> String {
    post_burst_comparison_key_for_candidate(
        &observation.realm_id,
        observation.lane,
        observation.candidate_variant,
    )
}

fn evidence_generation(ledger: &PostBurstEvidenceLedger, comparison_key: &str) -> u64 {
    ledger
        .evidence_generations
        .get(comparison_key)
        .copied()
        .unwrap_or_default()
}

fn bump_evidence_generation(ledger: &mut PostBurstEvidenceLedger, comparison_key: &str) {
    let generation = ledger
        .evidence_generations
        .entry(comparison_key.to_string())
        .or_default();
    *generation = generation.saturating_add(1);
}

fn rebuild_evidence_scope_index(ledger: &mut PostBurstEvidenceLedger) {
    ledger.evidence_scope_latest_expiry.clear();
    ledger.evidence_scope_expiries.clear();
    ledger.evidence_scope_records.clear();
    for observation in &ledger.evidence {
        let comparison_key = post_burst_evidence_scope_key(observation);
        let expires_at = observation.observed_at + Duration::hours(POST_BURST_EVIDENCE_TTL_HOURS);
        ledger
            .evidence_scope_latest_expiry
            .entry(comparison_key.clone())
            .and_modify(|current| *current = (*current).max(expires_at))
            .or_insert(expires_at);
        ledger
            .evidence_scope_expiries
            .entry(comparison_key.clone())
            .or_default()
            .entry(expires_at)
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
        ledger
            .evidence_scope_records
            .entry(comparison_key)
            .or_default()
            .push_back(observation.clone());
    }
    ledger.evidence_scope_indexed_len = ledger.evidence.len();
}

fn ensure_evidence_scope_index(ledger: &mut PostBurstEvidenceLedger) {
    if ledger.evidence_scope_indexed_len != ledger.evidence.len() {
        rebuild_evidence_scope_index(ledger);
    }
}

fn record_evidence_scope(ledger: &mut PostBurstEvidenceLedger, observation: &PostBurstEvidence) {
    ensure_evidence_scope_index(ledger);
    let comparison_key = post_burst_evidence_scope_key(observation);
    let expires_at = observation.observed_at + Duration::hours(POST_BURST_EVIDENCE_TTL_HOURS);
    ledger
        .evidence_scope_latest_expiry
        .entry(comparison_key.clone())
        .and_modify(|current| *current = (*current).max(expires_at))
        .or_insert(expires_at);
    ledger
        .evidence_scope_expiries
        .entry(comparison_key.clone())
        .or_default()
        .entry(expires_at)
        .and_modify(|count| *count = count.saturating_add(1))
        .or_insert(1);
    ledger
        .evidence_scope_records
        .entry(comparison_key)
        .or_default()
        .push_back(observation.clone());
    ledger.evidence_scope_indexed_len = ledger.evidence_scope_indexed_len.saturating_add(1);
}

/// Remove one globally evicted evidence item from its scope index in O(log n).
/// The caller removes from `ledger.evidence` immediately before this call, so
/// the index length is intentionally one ahead until this function settles it.
/// Any unexpected ordering mismatch is safe: rebuild the small bounded index
/// instead of retaining stale readiness evidence.
fn remove_evidence_scope(ledger: &mut PostBurstEvidenceLedger, observation: &PostBurstEvidence) {
    let comparison_key = post_burst_evidence_scope_key(observation);
    let front_matches = ledger
        .evidence_scope_records
        .get(&comparison_key)
        .and_then(VecDeque::front)
        .is_some_and(|front| front == observation);
    if !front_matches {
        rebuild_evidence_scope_index(ledger);
        return;
    }

    let Some(records) = ledger.evidence_scope_records.get_mut(&comparison_key) else {
        rebuild_evidence_scope_index(ledger);
        return;
    };
    let remove_scope = {
        records.pop_front();
        records.is_empty()
    };
    if remove_scope {
        ledger.evidence_scope_records.remove(&comparison_key);
    }

    let expires_at = observation.observed_at + Duration::hours(POST_BURST_EVIDENCE_TTL_HOURS);
    let expiry_update = ledger
        .evidence_scope_expiries
        .get_mut(&comparison_key)
        .and_then(|expiries| {
            let count = expiries.get_mut(&expires_at)?;
            let remove_expiry = *count <= 1;
            if remove_expiry {
                expiries.remove(&expires_at);
            } else {
                *count -= 1;
            }
            Some(expiries.last_key_value().map(|(expiry, _)| *expiry))
        });
    let Some(latest_expiry) = expiry_update else {
        rebuild_evidence_scope_index(ledger);
        return;
    };
    if let Some(latest_expiry) = latest_expiry {
        ledger
            .evidence_scope_latest_expiry
            .insert(comparison_key.clone(), latest_expiry);
    } else {
        ledger.evidence_scope_expiries.remove(&comparison_key);
        ledger.evidence_scope_latest_expiry.remove(&comparison_key);
    }
    ledger.evidence_scope_indexed_len = ledger.evidence_scope_indexed_len.saturating_sub(1);
}

fn has_fresh_evidence_for_scope(
    ledger: &mut PostBurstEvidenceLedger,
    comparison_key: &str,
    now: DateTime<Utc>,
) -> bool {
    ensure_evidence_scope_index(ledger);
    ledger
        .evidence_scope_latest_expiry
        .get(comparison_key)
        .is_some_and(|expires_at| *expires_at >= now)
}

fn prune_evidence_scope_state(ledger: &mut PostBurstEvidenceLedger) {
    ensure_evidence_scope_index(ledger);
    ledger.readiness.retain(|key, _| {
        ledger.evidence_scope_latest_expiry.contains_key(key) || ledger.rollbacks.contains_key(key)
    });
    ledger.evidence_generations.retain(|key, _| {
        ledger.readiness.contains_key(key)
            || ledger.rollbacks.contains_key(key)
            || ledger.evidence_scope_latest_expiry.contains_key(key)
    });
    ledger
        .evidence_scope_records
        .retain(|key, _| ledger.evidence_scope_latest_expiry.contains_key(key));
    ledger
        .evidence_scope_expiries
        .retain(|key, _| ledger.evidence_scope_latest_expiry.contains_key(key));
}

fn rebuild_assignment_age_index(store: &mut ShadowAffinityStore) {
    store.assignment_age_index.clear();
    store.assignment_age_index.extend(
        store
            .assignments
            .iter()
            .map(|(key, assignment)| (assignment.last_seen_at, key.clone())),
    );
}

/// Reconstruct the bounded live-route ownership index only during state
/// preparation.  Normal request processing performs O(1) lookups and never
/// scans all assignments to decide whether it may send upstream.
fn rebuild_active_cache_route_owners(store: &mut ShadowAffinityStore, now: DateTime<Utc>) {
    store.active_cache_route_owners.clear();
    let mut expired_or_unleased = Vec::new();
    let mut candidates = store
        .assignments
        .iter()
        .filter(|(_, assignment)| {
            matches!(
                assignment.active_cache_route_state,
                ActiveCacheRouteState::Candidate | ActiveCacheRouteState::Promoted
            ) && assignment
                .active_cache_route_valid_until
                .is_some_and(|valid_until| valid_until > now)
        })
        .map(|(conversation_id, assignment)| {
            (
                assignment.realm_id.clone(),
                assignment.last_seen_at,
                conversation_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    expired_or_unleased.extend(
        store
            .assignments
            .iter()
            .filter(|(_, assignment)| {
                matches!(
                    assignment.active_cache_route_state,
                    ActiveCacheRouteState::Candidate | ActiveCacheRouteState::Promoted
                ) && assignment
                    .active_cache_route_valid_until
                    .is_none_or(|valid_until| valid_until <= now)
            })
            .map(|(conversation_id, _)| conversation_id.clone()),
    );
    // Prefer the most recently active owner if a stale/corrupt persisted file
    // contains two candidates for one realm.  The loser fails closed instead
    // of silently producing two live placement routes after a restart.
    candidates.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let mut conflicting_candidates = Vec::new();
    for (realm_id, _, conversation_id) in candidates {
        if store.active_cache_route_owners.contains_key(&realm_id) {
            conflicting_candidates.push(conversation_id);
        } else {
            store
                .active_cache_route_owners
                .insert(realm_id, conversation_id);
        }
    }
    expired_or_unleased.extend(conflicting_candidates);
    for conversation_id in expired_or_unleased {
        if let Some(assignment) = store.assignments.get_mut(&conversation_id) {
            let owner_conflict = assignment
                .active_cache_route_valid_until
                .is_some_and(|valid_until| valid_until > now);
            reset_active_cache_route(assignment);
            assignment.active_cache_route_reason = Some(
                if owner_conflict {
                    "active_cache_route_owner_conflict_after_restore"
                } else {
                    "active_cache_route_lease_expired_after_restore"
                }
                .to_string(),
            );
        }
    }
}

fn ensure_assignment_age_index(store: &mut ShadowAffinityStore) {
    if store.assignment_age_index.len() != store.assignments.len() {
        rebuild_assignment_age_index(store);
    }
}

fn rebuild_post_burst_window_age_index(ledger: &mut PostBurstEvidenceLedger) {
    ledger.window_age_index.clear();
    ledger.window_age_index.extend(
        ledger
            .windows
            .iter()
            .map(|(key, window)| (window.opened_at, key.clone())),
    );
}

fn ensure_post_burst_window_age_index(ledger: &mut PostBurstEvidenceLedger) {
    if ledger.window_age_index.len() != ledger.windows.len() {
        rebuild_post_burst_window_age_index(ledger);
    }
}

pub(crate) fn prepare_shadow_affinity_store(store: &mut ShadowAffinityStore) {
    let now = Utc::now();
    rebuild_assignment_age_index(store);
    rebuild_active_cache_route_owners(store, now);
    rebuild_post_burst_window_age_index(&mut store.post_burst);
    rebuild_evidence_scope_index(&mut store.post_burst);
    evict_assignments(store, now);
}

fn remove_post_burst_window(
    ledger: &mut PostBurstEvidenceLedger,
    conversation_id: &str,
) -> Option<PostBurstWindow> {
    ensure_post_burst_window_age_index(ledger);
    let window = ledger.windows.remove(conversation_id)?;
    ledger
        .window_age_index
        .remove(&(window.opened_at, conversation_id.to_string()));
    Some(window)
}

fn evict_oldest_post_burst_window(ledger: &mut PostBurstEvidenceLedger) -> bool {
    ensure_post_burst_window_age_index(ledger);
    let Some((_, oldest)) = ledger.window_age_index.first().cloned() else {
        return false;
    };
    remove_post_burst_window(ledger, &oldest).is_some()
}

/// Make room before a new window is inserted.  The incoming conversation is
/// deliberately absent from the index at this point, so eviction never needs
/// a protected-key scan on the request/settlement path.
fn make_room_for_new_post_burst_window(ledger: &mut PostBurstEvidenceLedger) {
    while ledger.windows.len() >= POST_BURST_WINDOW_LIMIT {
        if !evict_oldest_post_burst_window(ledger) {
            break;
        }
    }
}

/// Cold-path recovery for restored or otherwise over-limit state.  Normal
/// insertions make room before mutation and never call this helper.
fn enforce_post_burst_window_capacity(ledger: &mut PostBurstEvidenceLedger) {
    while ledger.windows.len() > POST_BURST_WINDOW_LIMIT {
        if !evict_oldest_post_burst_window(ledger) {
            break;
        }
    }
}

fn insert_post_burst_window(
    ledger: &mut PostBurstEvidenceLedger,
    conversation_id: String,
    window: PostBurstWindow,
) {
    ensure_post_burst_window_age_index(ledger);
    if !ledger.windows.contains_key(&conversation_id) {
        make_room_for_new_post_burst_window(ledger);
    }
    if let Some(replaced) = ledger
        .windows
        .insert(conversation_id.clone(), window.clone())
    {
        ledger
            .window_age_index
            .remove(&(replaced.opened_at, conversation_id.clone()));
    }
    ledger
        .window_age_index
        .insert((window.opened_at, conversation_id.clone()));
}

fn evict_post_burst_evidence(ledger: &mut PostBurstEvidenceLedger, now: DateTime<Utc>) {
    let expired_windows = ledger
        .windows
        .iter()
        .filter(|(_, window)| {
            window.expires_at < now
                || window.remaining_requests == 0
                || window.realm_id.is_empty()
                || window.policy_epoch != SHADOW_POLICY_EPOCH
        })
        .map(|(key, _)| key.clone())
        .collect::<Vec<_>>();
    for conversation_id in expired_windows {
        remove_post_burst_window(ledger, &conversation_id);
    }
    enforce_post_burst_window_capacity(ledger);

    let cutoff = now - Duration::hours(POST_BURST_EVIDENCE_TTL_HOURS);
    let mut invalidated_scopes = BTreeSet::new();
    let mut retained = VecDeque::with_capacity(ledger.evidence.len());
    for observation in ledger.evidence.drain(..) {
        if observation.observed_at >= cutoff && observation.policy_epoch == SHADOW_POLICY_EPOCH {
            retained.push_back(observation);
        } else {
            invalidated_scopes.insert(post_burst_evidence_scope_key(&observation));
        }
    }
    ledger.evidence = retained;
    while ledger.evidence.len() > POST_BURST_EVIDENCE_LIMIT {
        if let Some(observation) = ledger.evidence.pop_front() {
            invalidated_scopes.insert(post_burst_evidence_scope_key(&observation));
        }
    }
    rebuild_evidence_scope_index(ledger);
    for comparison_key in invalidated_scopes {
        bump_evidence_generation(ledger, &comparison_key);
        ledger.readiness.remove(&comparison_key);
    }
    prune_evidence_scope_state(ledger);
}

fn assignment_is_current(assignment: &ShadowAffinityAssignment, now: DateTime<Utc>) -> bool {
    assignment.policy_epoch == SHADOW_POLICY_EPOCH
        && assignment.last_seen_at >= now - Duration::hours(SHADOW_ASSIGNMENT_TTL_HOURS)
}

fn discard_inactive_assignment_for_scope(
    store: &mut ShadowAffinityStore,
    conversation_id: &str,
    realm_id: &str,
    now: DateTime<Utc>,
) {
    let inactive = store
        .assignments
        .get(conversation_id)
        .is_some_and(|assignment| {
            !assignment_is_current(assignment, now) || assignment.realm_id != realm_id
        });
    if inactive {
        remove_assignment(store, conversation_id);
    }
}

fn remove_assignment(
    store: &mut ShadowAffinityStore,
    conversation_id: &str,
) -> Option<ShadowAffinityAssignment> {
    ensure_assignment_age_index(store);
    let assignment = store.assignments.remove(conversation_id)?;
    store
        .assignment_age_index
        .remove(&(assignment.last_seen_at, conversation_id.to_string()));
    clear_active_cache_route_owner(store, &assignment.realm_id, conversation_id);
    remove_post_burst_window(&mut store.post_burst, conversation_id);
    Some(assignment)
}

fn active_post_burst_window_for_scope(
    ledger: &mut PostBurstEvidenceLedger,
    conversation_id: &str,
    realm_id: &str,
    policy_epoch: u64,
    anchor_epoch: u64,
    now: DateTime<Utc>,
) -> Option<(ShadowCacheLane, ShadowCacheCandidateVariant)> {
    let invalid = ledger.windows.get(conversation_id).is_some_and(|window| {
        window.conversation_id != conversation_id
            || window.expires_at < now
            || window.remaining_requests == 0
            || window.realm_id != realm_id
            || window.policy_epoch != policy_epoch
            || window.anchor_epoch != anchor_epoch
    });
    if invalid {
        remove_post_burst_window(ledger, conversation_id);
        return None;
    }
    ledger
        .windows
        .get(conversation_id)
        .map(|window| (window.lane, window.candidate_variant))
}

fn evict_oldest_assignment(store: &mut ShadowAffinityStore) -> bool {
    ensure_assignment_age_index(store);
    let Some((_, oldest)) = store.assignment_age_index.first().cloned() else {
        return false;
    };
    remove_assignment(store, &oldest).is_some()
}

/// Make room before a new assignment is inserted.  This keeps the hot path at
/// O(log n): no protected-key search and no capacity sweep after insertion.
fn make_room_for_new_assignment(store: &mut ShadowAffinityStore) {
    while store.assignments.len() >= SHADOW_ASSIGNMENT_LIMIT {
        if !evict_oldest_assignment(store) {
            break;
        }
    }
}

/// Cold-path recovery for a restored state that is already over capacity.
fn enforce_assignment_capacity(store: &mut ShadowAffinityStore) {
    while store.assignments.len() > SHADOW_ASSIGNMENT_LIMIT {
        if !evict_oldest_assignment(store) {
            break;
        }
    }
}

fn maintain_shadow_affinity_after_settlement(store: &mut ShadowAffinityStore, now: DateTime<Utc>) {
    ensure_assignment_age_index(store);
    let assignment_cutoff = now - Duration::hours(SHADOW_ASSIGNMENT_TTL_HOURS);
    for _ in 0..SHADOW_SETTLEMENT_MAINTENANCE_BUDGET {
        let Some((last_seen_at, conversation_id)) = store.assignment_age_index.first().cloned()
        else {
            break;
        };
        if last_seen_at >= assignment_cutoff {
            break;
        }
        remove_assignment(store, &conversation_id);
    }

    ensure_post_burst_window_age_index(&mut store.post_burst);
    for _ in 0..SHADOW_SETTLEMENT_MAINTENANCE_BUDGET {
        let Some((_, conversation_id)) = store.post_burst.window_age_index.first().cloned() else {
            break;
        };
        let removable = store
            .post_burst
            .windows
            .get(&conversation_id)
            .is_some_and(|window| {
                window.expires_at < now
                    || window.realm_id.is_empty()
                    || window.policy_epoch != SHADOW_POLICY_EPOCH
            });
        if !removable {
            break;
        }
        remove_post_burst_window(&mut store.post_burst, &conversation_id);
    }
}

fn summarize_post_burst_arm<'a>(
    observations: impl Iterator<Item = &'a PostBurstEvidence>,
) -> PostBurstArmSummary {
    let observations = observations.collect::<Vec<_>>();
    let aggregate = summarize_post_burst_phase(observations.iter().copied());
    let first_followup = summarize_post_burst_phase(
        observations
            .iter()
            .copied()
            .filter(|observation| observation.followup_index == 1),
    );
    let stable_followups = summarize_post_burst_phase(
        observations
            .iter()
            .copied()
            .filter(|observation| observation.followup_index > 1),
    );
    PostBurstArmSummary {
        observations: aggregate.observations,
        successful_observations: aggregate.successful_observations,
        usage_observations: aggregate.usage_observations,
        applied_observations: aggregate.applied_observations,
        multi_attempt_observations: aggregate.multi_attempt_observations,
        provider_unstable_observations: aggregate.provider_unstable_observations,
        success_rate_bps: aggregate.success_rate_bps,
        usage_coverage_bps: aggregate.usage_coverage_bps,
        cache_ratio_bps: aggregate.cache_ratio_bps,
        average_input_tokens: aggregate.average_input_tokens,
        average_avoidable_gap_tokens: aggregate.average_avoidable_gap_tokens,
        average_provider_unstable_gap_tokens: aggregate.average_provider_unstable_gap_tokens,
        provider_unstable_ratio_bps: aggregate.provider_unstable_ratio_bps,
        average_ttft_ms: aggregate.average_ttft_ms,
        ttft_p50_ms: aggregate.ttft_p50_ms,
        ttft_p95_ms: aggregate.ttft_p95_ms,
        first_followup,
        stable_followups,
    }
}

fn summarize_post_burst_phase<'a>(
    observations: impl Iterator<Item = &'a PostBurstEvidence>,
) -> PostBurstPhaseSummary {
    let mut summary = PostBurstPhaseSummary::default();
    let mut input_tokens = 0_u64;
    let mut cacheable_input_tokens = 0_u64;
    let mut cache_read_tokens = 0_u64;
    let mut avoidable_gap_tokens = 0_u64;
    let mut provider_unstable_gap_tokens = 0_u64;
    let mut ttft_ms = 0_u64;
    let mut ttft_samples = Vec::new();
    for observation in observations {
        summary.observations = summary.observations.saturating_add(1);
        summary.successful_observations = summary
            .successful_observations
            .saturating_add(u64::from(observation.success));
        summary.usage_observations = summary
            .usage_observations
            .saturating_add(u64::from(observation.has_usage));
        summary.applied_observations = summary
            .applied_observations
            .saturating_add(u64::from(observation.candidate_applied));
        summary.multi_attempt_observations = summary
            .multi_attempt_observations
            .saturating_add(u64::from(observation.attempt_count > 1));
        summary.provider_unstable_observations = summary
            .provider_unstable_observations
            .saturating_add(u64::from(observation.provider_unstable_gap_tokens > 0));
        input_tokens = input_tokens.saturating_add(observation.input_tokens);
        cacheable_input_tokens = cacheable_input_tokens
            .saturating_add(cacheable_input_tokens_128(observation.input_tokens));
        cache_read_tokens = cache_read_tokens.saturating_add(observation.cache_read_tokens);
        avoidable_gap_tokens =
            avoidable_gap_tokens.saturating_add(observation.avoidable_gap_tokens);
        provider_unstable_gap_tokens =
            provider_unstable_gap_tokens.saturating_add(observation.provider_unstable_gap_tokens);
        ttft_ms = ttft_ms.saturating_add(observation.ttft_ms);
        if observation.success && observation.ttft_ms > 0 {
            ttft_samples.push(observation.ttft_ms);
        }
    }
    if summary.observations == 0 {
        return summary;
    }
    summary.success_rate_bps = ratio_bps(summary.successful_observations, summary.observations);
    summary.usage_coverage_bps = ratio_bps(summary.usage_observations, summary.observations);
    summary.cache_ratio_bps = ratio_bps(cache_read_tokens, cacheable_input_tokens);
    summary.average_input_tokens = input_tokens / summary.observations;
    summary.average_avoidable_gap_tokens = avoidable_gap_tokens / summary.observations;
    summary.average_provider_unstable_gap_tokens =
        provider_unstable_gap_tokens / summary.observations;
    summary.provider_unstable_ratio_bps = ratio_bps(provider_unstable_gap_tokens, input_tokens);
    summary.average_ttft_ms = ttft_ms / summary.observations;
    let mut p50_samples = ttft_samples.clone();
    summary.ttft_p50_ms = percentile_ms(&mut p50_samples, 50);
    summary.ttft_p95_ms = percentile_ms(&mut ttft_samples, 95);
    summary
}

fn post_burst_promotion_blocker(
    baseline: &PostBurstArmSummary,
    candidate: &PostBurstArmSummary,
) -> Option<&'static str> {
    if baseline.success_rate_bps
        > candidate
            .success_rate_bps
            .saturating_add(POST_BURST_PROMOTION_MAX_ERROR_DELTA_BPS)
    {
        return Some("candidate_error_not_non_inferior");
    }
    if candidate.provider_unstable_ratio_bps > baseline.provider_unstable_ratio_bps {
        return Some("candidate_provider_unstable_not_non_inferior");
    }
    if baseline.ttft_p50_ms > 0
        && candidate.ttft_p50_ms
            > baseline
                .ttft_p50_ms
                .saturating_add(POST_BURST_PROMOTION_TTFT_P50_DELTA_MS)
        && candidate.ttft_p50_ms > baseline.ttft_p50_ms.saturating_mul(105) / 100
    {
        return Some("candidate_ttft_p50_not_non_inferior");
    }
    if baseline.ttft_p95_ms > 0
        && candidate.ttft_p95_ms
            > baseline
                .ttft_p95_ms
                .saturating_add(POST_BURST_PROMOTION_TTFT_P95_DELTA_MS)
        && candidate.ttft_p95_ms > baseline.ttft_p95_ms.saturating_mul(105) / 100
    {
        return Some("candidate_ttft_p95_not_non_inferior");
    }

    let cache_improved = candidate.cache_ratio_bps > baseline.cache_ratio_bps;
    let ttft_p50_improved = baseline.ttft_p50_ms > 0
        && candidate
            .ttft_p50_ms
            .saturating_add(POST_BURST_PROMOTION_TTFT_P50_DELTA_MS)
            <= baseline.ttft_p50_ms
        && candidate.ttft_p50_ms <= baseline.ttft_p50_ms.saturating_mul(95) / 100;
    let ttft_p95_improved = baseline.ttft_p95_ms > 0
        && candidate
            .ttft_p95_ms
            .saturating_add(POST_BURST_PROMOTION_TTFT_P95_DELTA_MS)
            <= baseline.ttft_p95_ms
        && candidate.ttft_p95_ms <= baseline.ttft_p95_ms.saturating_mul(95) / 100;
    if !cache_improved
        && !(candidate.cache_ratio_bps == baseline.cache_ratio_bps
            && (ttft_p50_improved || ttft_p95_improved))
    {
        return Some("candidate_has_no_net_benefit");
    }

    None
}

fn post_burst_clear_ttft_regression(
    baseline: &PostBurstArmSummary,
    candidate: &PostBurstArmSummary,
) -> bool {
    baseline.ttft_p50_ms > 0
        && baseline.ttft_p95_ms > 0
        && candidate.ttft_p50_ms
            > baseline
                .ttft_p50_ms
                .saturating_add(POST_BURST_ROLLBACK_TTFT_DELTA_MS)
        && candidate.ttft_p50_ms > baseline.ttft_p50_ms.saturating_mul(3) / 2
        && candidate.ttft_p95_ms
            > baseline
                .ttft_p95_ms
                .saturating_add(POST_BURST_ROLLBACK_TTFT_DELTA_MS)
        && candidate.ttft_p95_ms > baseline.ttft_p95_ms.saturating_mul(3) / 2
}

fn post_burst_has_addressable_gap(
    baseline: &PostBurstArmSummary,
    candidate: &PostBurstArmSummary,
) -> bool {
    baseline.average_avoidable_gap_tokens > 0
        || candidate.average_avoidable_gap_tokens > 0
        || baseline.provider_unstable_ratio_bps > 0
        || candidate.provider_unstable_ratio_bps > 0
        || baseline.cache_ratio_bps < POST_BURST_RESIDUAL_CACHE_GAP_BPS
        || candidate.cache_ratio_bps < POST_BURST_RESIDUAL_CACHE_GAP_BPS
}

fn post_burst_baseline_has_addressable_gap(baseline: &PostBurstArmSummary) -> bool {
    baseline.average_avoidable_gap_tokens > 0
        || baseline.provider_unstable_ratio_bps > 0
        || baseline.cache_ratio_bps < POST_BURST_RESIDUAL_CACHE_GAP_BPS
}

fn post_burst_arms_are_comparable(
    baseline: &PostBurstArmSummary,
    candidate: &PostBurstArmSummary,
) -> Result<(), &'static str> {
    post_burst_arms_are_comparable_with_minimum(
        baseline,
        candidate,
        POST_BURST_MIN_ARM_OBSERVATIONS,
    )
}

fn post_burst_arms_are_comparable_with_minimum(
    baseline: &PostBurstArmSummary,
    candidate: &PostBurstArmSummary,
    minimum_observations: u64,
) -> Result<(), &'static str> {
    if baseline.observations < minimum_observations || candidate.observations < minimum_observations
    {
        return Err("insufficient_arm_observations");
    }
    if baseline.usage_coverage_bps < POST_BURST_MIN_USAGE_COVERAGE_BPS
        || candidate.usage_coverage_bps < POST_BURST_MIN_USAGE_COVERAGE_BPS
    {
        return Err("insufficient_usage_coverage");
    }
    if ratio_bps(
        baseline.provider_unstable_observations,
        baseline.observations,
    ) > POST_BURST_MAX_PROVIDER_UNSTABLE_BPS
        || ratio_bps(
            candidate.provider_unstable_observations,
            candidate.observations,
        ) > POST_BURST_MAX_PROVIDER_UNSTABLE_BPS
    {
        return Err("provider_unstable_evidence");
    }
    let largest_input = baseline
        .average_input_tokens
        .max(candidate.average_input_tokens);
    if largest_input == 0
        || ratio_bps(
            baseline
                .average_input_tokens
                .abs_diff(candidate.average_input_tokens),
            largest_input,
        ) > POST_BURST_MAX_INPUT_IMBALANCE_BPS
    {
        return Err("input_size_imbalance");
    }
    Ok(())
}

fn evaluate_post_burst_readiness(
    ledger: &PostBurstEvidenceLedger,
    realm_id: &str,
    lane: ShadowCacheLane,
    candidate_variant: ShadowCacheCandidateVariant,
    now: DateTime<Utc>,
) -> PostBurstReadiness {
    let comparison_key = post_burst_comparison_key_for_candidate(realm_id, lane, candidate_variant);
    let current_evidence_generation = evidence_generation(ledger, &comparison_key);
    let cutoff = now - Duration::hours(POST_BURST_EVIDENCE_TTL_HOURS);
    let scoped_evidence = ledger.evidence_scope_records.get(&comparison_key);
    let matching = |observation: &&PostBurstEvidence| {
        observation.realm_id == realm_id
            && observation.lane == lane
            && observation.candidate_variant == candidate_variant
            && observation.policy_epoch == SHADOW_POLICY_EPOCH
            && observation.observed_at >= cutoff
    };
    let baseline = summarize_post_burst_arm(
        scoped_evidence
            .iter()
            .flat_map(|records| records.iter())
            .filter(matching)
            .filter(|observation| observation.arm == ShadowAffinityArm::Baseline),
    );
    let candidate_shadow = summarize_post_burst_arm(
        scoped_evidence
            .iter()
            .flat_map(|records| records.iter())
            .filter(matching)
            .filter(|observation| {
                observation.arm == ShadowAffinityArm::Candidate && !observation.candidate_applied
            }),
    );
    let candidate_applied = summarize_post_burst_arm(
        scoped_evidence
            .iter()
            .flat_map(|records| records.iter())
            .filter(matching)
            .filter(|observation| {
                observation.arm == ShadowAffinityArm::Candidate && observation.candidate_applied
            }),
    );
    let canary_baseline = scoped_evidence
        .iter()
        .flat_map(|records| records.iter())
        .filter(matching)
        .filter(|observation| {
            observation.arm == ShadowAffinityArm::Candidate && observation.candidate_applied
        })
        .map(|observation| observation.observed_at)
        .min()
        .map(|canary_started_at| {
            let contemporaneous = scoped_evidence
                .iter()
                .flat_map(|records| records.iter())
                .filter(matching)
                .filter(|observation| {
                    observation.arm == ShadowAffinityArm::Baseline
                        && observation.observed_at >= canary_started_at
                })
                .collect::<Vec<_>>();
            let keep = candidate_applied.observations as usize;
            let skip = contemporaneous.len().saturating_sub(keep);
            summarize_post_burst_arm(contemporaneous.into_iter().skip(skip))
        })
        .unwrap_or_default();

    let (status, reason) = if let Some(reason) = ledger.rollbacks.get(&comparison_key) {
        (PostBurstReadinessStatus::RollbackRequired, reason.clone())
    } else if let Err(reason) = post_burst_arms_are_comparable(&baseline, &candidate_shadow) {
        (
            PostBurstReadinessStatus::InsufficientEvidence,
            reason.to_string(),
        )
    } else if candidate_applied.observations == 0
        && !post_burst_has_addressable_gap(&baseline, &candidate_shadow)
    {
        (
            PostBurstReadinessStatus::CanaryHealthy,
            "no_addressable_post_burst_gap".to_string(),
        )
    } else if candidate_applied.observations == 0 {
        (
            PostBurstReadinessStatus::ReadyForCanary,
            "comparable_shadow_evidence_ready".to_string(),
        )
    } else if candidate_applied.multi_attempt_observations > 0 {
        (
            PostBurstReadinessStatus::RollbackRequired,
            "candidate_multi_attempt_observed".to_string(),
        )
    } else if canary_baseline.observations < candidate_applied.observations {
        (
            PostBurstReadinessStatus::CanaryCollecting,
            "insufficient_paired_canary_evidence".to_string(),
        )
    } else if post_burst_arms_are_comparable_with_minimum(
        &canary_baseline,
        &candidate_applied,
        POST_BURST_MIN_CANARY_OBSERVATIONS,
    )
    .is_err()
    {
        (
            PostBurstReadinessStatus::CanaryCollecting,
            "insufficient_clean_canary_evidence".to_string(),
        )
    } else if canary_baseline.success_rate_bps
        > candidate_applied
            .success_rate_bps
            .saturating_add(POST_BURST_ROLLBACK_SUCCESS_DELTA_BPS)
    {
        (
            PostBurstReadinessStatus::RollbackRequired,
            "candidate_success_regression".to_string(),
        )
    } else if candidate_applied.observations >= POST_BURST_MIN_EFFICACY_OBSERVATIONS
        && canary_baseline.cache_ratio_bps
            > candidate_applied
                .cache_ratio_bps
                .saturating_add(POST_BURST_ROLLBACK_CACHE_DELTA_BPS)
        && candidate_applied.average_avoidable_gap_tokens
            > canary_baseline
                .average_avoidable_gap_tokens
                .saturating_add(POST_BURST_ROLLBACK_AVOIDABLE_DELTA)
    {
        (
            PostBurstReadinessStatus::RollbackRequired,
            "candidate_cache_regression".to_string(),
        )
    } else if candidate_applied.observations >= POST_BURST_MIN_EFFICACY_OBSERVATIONS
        && post_burst_clear_ttft_regression(&canary_baseline, &candidate_applied)
    {
        (
            PostBurstReadinessStatus::RollbackRequired,
            "candidate_ttft_regression".to_string(),
        )
    } else if candidate_applied.observations < POST_BURST_MIN_PROMOTION_OBSERVATIONS {
        (
            PostBurstReadinessStatus::CanaryCollecting,
            "collecting_promotion_evidence".to_string(),
        )
    } else if let Some(reason) = post_burst_promotion_blocker(&canary_baseline, &candidate_applied)
    {
        (PostBurstReadinessStatus::CanaryHealthy, reason.to_string())
    } else {
        (
            PostBurstReadinessStatus::ReadyForPromotion,
            "canary_efficacy_gate_passed".to_string(),
        )
    };

    PostBurstReadiness {
        comparison_key,
        realm_id: realm_id.to_string(),
        lane,
        candidate_variant,
        status,
        reason,
        baseline,
        candidate_shadow,
        canary_baseline,
        candidate_applied,
        updated_at: now,
        evidence_generation: current_evidence_generation,
        valid_until: Some(
            scoped_evidence
                .iter()
                .flat_map(|records| records.iter())
                .filter(matching)
                .map(|observation| {
                    observation.observed_at + Duration::hours(POST_BURST_EVIDENCE_TTL_HOURS)
                })
                .min()
                .unwrap_or_else(|| now + Duration::hours(POST_BURST_READINESS_MAX_AGE_HOURS)),
        ),
    }
}

fn refresh_post_burst_readiness(
    ledger: &mut PostBurstEvidenceLedger,
    realm_id: &str,
    lane: ShadowCacheLane,
    candidate_variant: ShadowCacheCandidateVariant,
    now: DateTime<Utc>,
) -> PostBurstReadiness {
    let comparison_key = post_burst_comparison_key_for_candidate(realm_id, lane, candidate_variant);
    if !has_fresh_evidence_for_scope(ledger, &comparison_key, now)
        && !ledger.rollbacks.contains_key(&comparison_key)
    {
        return empty_post_burst_readiness(ledger, realm_id, lane, candidate_variant, now);
    }
    let readiness = evaluate_post_burst_readiness(ledger, realm_id, lane, candidate_variant, now);
    if readiness.status == PostBurstReadinessStatus::RollbackRequired {
        ledger
            .rollbacks
            .entry(readiness.comparison_key.clone())
            .or_insert_with(|| readiness.reason.clone());
    }
    ledger
        .readiness
        .insert(readiness.comparison_key.clone(), readiness.clone());
    readiness
}

fn empty_post_burst_readiness(
    ledger: &PostBurstEvidenceLedger,
    realm_id: &str,
    lane: ShadowCacheLane,
    candidate_variant: ShadowCacheCandidateVariant,
    now: DateTime<Utc>,
) -> PostBurstReadiness {
    PostBurstReadiness {
        comparison_key: post_burst_comparison_key_for_candidate(realm_id, lane, candidate_variant),
        realm_id: realm_id.to_string(),
        lane,
        candidate_variant,
        status: PostBurstReadinessStatus::InsufficientEvidence,
        reason: "insufficient_arm_observations".to_string(),
        baseline: PostBurstArmSummary::default(),
        candidate_shadow: PostBurstArmSummary::default(),
        canary_baseline: PostBurstArmSummary::default(),
        candidate_applied: PostBurstArmSummary::default(),
        updated_at: now,
        evidence_generation: evidence_generation(
            ledger,
            &post_burst_comparison_key_for_candidate(realm_id, lane, candidate_variant),
        ),
        valid_until: None,
    }
}

fn current_post_burst_readiness(
    ledger: &mut PostBurstEvidenceLedger,
    realm_id: &str,
    lane: ShadowCacheLane,
    candidate_variant: ShadowCacheCandidateVariant,
    now: DateTime<Utc>,
) -> PostBurstReadiness {
    let comparison_key = post_burst_comparison_key_for_candidate(realm_id, lane, candidate_variant);
    let has_evidence = has_fresh_evidence_for_scope(ledger, &comparison_key, now);
    let has_rollback = ledger.rollbacks.contains_key(&comparison_key);
    if !has_evidence && !has_rollback {
        return empty_post_burst_readiness(ledger, realm_id, lane, candidate_variant, now);
    }
    if let Some(readiness) = ledger.readiness.get(&comparison_key).filter(|readiness| {
        let rollback_matches = ledger.rollbacks.contains_key(&comparison_key)
            == (readiness.status == PostBurstReadinessStatus::RollbackRequired);
        readiness.evidence_generation == evidence_generation(ledger, &comparison_key)
            && readiness
                .valid_until
                .is_some_and(|valid_until| valid_until > now)
            && rollback_matches
    }) {
        return readiness.clone();
    }
    refresh_post_burst_readiness(ledger, realm_id, lane, candidate_variant, now)
}

fn observe_post_burst_evidence(
    ledger: &mut PostBurstEvidenceLedger,
    decision: &ShadowAffinityDecision,
    input: ShadowObservationInput,
    now: DateTime<Utc>,
) {
    let Some(conversation_id) = decision.assignment_key.as_deref() else {
        return;
    };
    active_post_burst_window_for_scope(
        ledger,
        conversation_id,
        &decision.realm_id,
        decision.policy_epoch,
        decision.anchor_epoch,
        now,
    );

    let captured = (!input.compaction_boundary)
        .then(|| {
            ledger.windows.get_mut(conversation_id).map(|window| {
                window.captured_requests = window.captured_requests.saturating_add(1);
                window.remaining_requests = window.remaining_requests.saturating_sub(1);
                (
                    window.window_id,
                    window.captured_requests,
                    window.remaining_requests == 0,
                    window.lane,
                    window.candidate_variant,
                )
            })
        })
        .flatten();
    if let Some((window_id, followup_index, finished, evidence_lane, candidate_variant)) = captured
    {
        let observation = PostBurstEvidence {
            window_id,
            conversation_id: conversation_id.to_string(),
            observed_at: now,
            followup_index,
            realm_id: decision.realm_id.clone(),
            lane: evidence_lane,
            candidate_variant,
            arm: decision.arm,
            policy_epoch: decision.policy_epoch,
            anchor_epoch: decision.anchor_epoch,
            success: input.success,
            status: input.status,
            has_usage: input.has_usage,
            input_tokens: input.input_tokens,
            cache_read_tokens: input.cache_read_tokens,
            cache_ratio_bps: ratio_bps(input.cache_read_tokens, input.input_tokens),
            avoidable_gap_tokens: input.avoidable_gap_tokens,
            provider_unstable_gap_tokens: input.provider_unstable_gap_tokens,
            ttft_ms: input.ttft_ms,
            attempt_count: input.attempt_count.max(1),
            candidate_applied: decision.arm == ShadowAffinityArm::Candidate
                && matches!(decision.mode.as_str(), "applied" | "validation_applied"),
        };
        let comparison_key = post_burst_evidence_scope_key(&observation);
        record_evidence_scope(ledger, &observation);
        ledger.evidence.push_back(observation);
        if finished {
            remove_post_burst_window(ledger, conversation_id);
        }
        let mut invalidated_scopes = BTreeSet::from([comparison_key]);
        while ledger.evidence.len() > POST_BURST_EVIDENCE_LIMIT {
            if let Some(removed) = ledger.evidence.pop_front() {
                let removed_comparison_key = post_burst_evidence_scope_key(&removed);
                remove_evidence_scope(ledger, &removed);
                invalidated_scopes.insert(removed_comparison_key);
            }
        }
        for comparison_key in invalidated_scopes {
            bump_evidence_generation(ledger, &comparison_key);
        }
        prune_evidence_scope_state(ledger);
        refresh_post_burst_readiness(
            ledger,
            &decision.realm_id,
            evidence_lane,
            candidate_variant,
            now,
        );
    }

    if input.giant_tail {
        ledger.next_window_id = ledger.next_window_id.saturating_add(1).max(1);
        insert_post_burst_window(
            ledger,
            conversation_id.to_string(),
            PostBurstWindow {
                window_id: ledger.next_window_id,
                conversation_id: conversation_id.to_string(),
                opened_at: now,
                expires_at: now + Duration::hours(POST_BURST_WINDOW_TTL_HOURS),
                remaining_requests: POST_BURST_FOLLOWUP_REQUESTS,
                captured_requests: 0,
                lane: ShadowCacheLane::ToolBurstQuarantine,
                candidate_variant: decision.candidate_variant,
                realm_id: decision.realm_id.clone(),
                policy_epoch: decision.policy_epoch,
                anchor_epoch: decision.anchor_epoch,
            },
        );
    }
}

pub(crate) fn post_burst_evidence_status(
    store: &mut ShadowAffinityStore,
    now: DateTime<Utc>,
) -> PostBurstEvidenceStatus {
    evict_post_burst_evidence(&mut store.post_burst, now);
    let comparison_scopes = store
        .post_burst
        .evidence
        .iter()
        .map(|observation| {
            (
                observation.realm_id.clone(),
                observation.lane,
                observation.candidate_variant,
            )
        })
        .collect::<Vec<_>>();
    for (realm_id, lane, candidate_variant) in comparison_scopes {
        refresh_post_burst_readiness(
            &mut store.post_burst,
            &realm_id,
            lane,
            candidate_variant,
            now,
        );
    }
    let mut readiness = store
        .post_burst
        .readiness
        .values()
        .cloned()
        .collect::<Vec<_>>();
    readiness.sort_by(|left, right| left.comparison_key.cmp(&right.comparison_key));
    let mut rollback_keys = store
        .post_burst
        .rollbacks
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    rollback_keys.sort();
    PostBurstEvidenceStatus {
        open_windows: store.post_burst.windows.len(),
        evidence_records: store.post_burst.evidence.len(),
        readiness,
        rollback_keys,
    }
}

fn active_candidate_for_realm(
    ledger: &mut PostBurstEvidenceLedger,
    realm_id: &str,
    lane: ShadowCacheLane,
    now: DateTime<Utc>,
) -> (ShadowCacheCandidateVariant, PostBurstReadiness) {
    let cohort = current_post_burst_readiness(
        ledger,
        realm_id,
        lane,
        ShadowCacheCandidateVariant::CohortKey,
        now,
    );
    if !candidate_is_exhausted(&cohort)
        || !post_burst_baseline_has_addressable_gap(&cohort.baseline)
    {
        return (ShadowCacheCandidateVariant::CohortKey, cohort);
    }
    let cohort_two_shard = current_post_burst_readiness(
        ledger,
        realm_id,
        lane,
        ShadowCacheCandidateVariant::CohortTwoShard,
        now,
    );
    if !candidate_is_exhausted(&cohort_two_shard)
        || !post_burst_baseline_has_addressable_gap(&cohort_two_shard.baseline)
    {
        return (
            ShadowCacheCandidateVariant::CohortTwoShard,
            cohort_two_shard,
        );
    }
    let provider_native = current_post_burst_readiness(
        ledger,
        realm_id,
        lane,
        ShadowCacheCandidateVariant::ProviderNative,
        now,
    );
    (ShadowCacheCandidateVariant::ProviderNative, provider_native)
}

fn candidate_is_exhausted(readiness: &PostBurstReadiness) -> bool {
    readiness.status == PostBurstReadinessStatus::RollbackRequired
        || (readiness.status == PostBurstReadinessStatus::CanaryHealthy
            && readiness.reason != "no_addressable_post_burst_gap")
}

fn forced_isolated_candidate_variant() -> Option<ShadowCacheCandidateVariant> {
    let isolated = std::env::var("ATOAPI_ISOLATED_TEST_INSTANCE")
        .ok()
        .is_some_and(|value| matches!(value.trim(), "1" | "true" | "on" | "enabled"));
    if !isolated {
        return None;
    }
    match std::env::var("ATOAPI_FORCE_CACHE_CANDIDATE").ok()?.trim() {
        "cohort_key" => Some(ShadowCacheCandidateVariant::CohortKey),
        "cohort_two_shard" => Some(ShadowCacheCandidateVariant::CohortTwoShard),
        "provider_native" => Some(ShadowCacheCandidateVariant::ProviderNative),
        _ => None,
    }
}

/// Admit (or resume) a production cache-placement candidate for the current
/// exact scope. It is an explicitly single-scope, time-bounded canary: one
/// owner per realm and no automatic cohort-wide rollout. Callers must already
/// have established that the client did not supply a cache key. This function
/// is synchronous and intended to run only while a caller holds a `try_lock`;
/// it never waits for disk, another request, or upstream I/O.
#[cfg(test)]
pub(super) fn admit_active_cache_route(
    store: &mut ShadowAffinityStore,
    decision: &mut ShadowAffinityDecision,
    provider_route_eligible: bool,
    provider_prompt_cache_key_verified: bool,
    now: DateTime<Utc>,
) -> bool {
    if !provider_route_eligible {
        return false;
    }
    if !decision.trusted_identity || decision.lane != ShadowCacheLane::Steady {
        return false;
    }
    let Some(conversation_id) = decision.assignment_key.as_deref() else {
        return false;
    };

    let expired_route = store
        .assignments
        .get(conversation_id)
        .is_some_and(|assignment| {
            matches!(
                assignment.active_cache_route_state,
                ActiveCacheRouteState::Candidate | ActiveCacheRouteState::Promoted
            ) && assignment
                .active_cache_route_valid_until
                .is_some_and(|valid_until| valid_until <= now)
        });
    if expired_route {
        let realm_id = store
            .assignments
            .get(conversation_id)
            .map(|assignment| assignment.realm_id.clone());
        if let Some(assignment) = store.assignments.get_mut(conversation_id) {
            reset_active_cache_route(assignment);
            assignment.active_cache_route_reason =
                Some("active_cache_route_lease_expired".to_string());
        }
        if let Some(realm_id) = realm_id {
            clear_active_cache_route_owner(store, &realm_id, conversation_id);
        }
        decision.arm = ShadowAffinityArm::Baseline;
        decision.skip_reason = Some("active_cache_route_lease_expired".to_string());
        return false;
    }

    let owner = store
        .active_cache_route_owners
        .get(&decision.realm_id)
        .cloned();
    let (state, claim_owner) = {
        let Some(assignment) = store.assignments.get_mut(conversation_id) else {
            decision.skip_reason = Some("active_cache_route_assignment_missing".to_string());
            return false;
        };
        if assignment.realm_id != decision.realm_id
            || assignment.cohort_id != decision.cohort_id
            || assignment.anchor_epoch != decision.anchor_epoch
        {
            decision.skip_reason = Some("active_cache_route_scope_changed".to_string());
            return false;
        }
        seed_active_cache_route_baseline(assignment);
        if !provider_prompt_cache_key_verified
            && assignment.active_cache_route_baseline.cache_read_tokens == 0
        {
            decision.skip_reason =
                Some("active_cache_route_prompt_cache_key_unverified".to_string());
            return false;
        }
        match assignment.active_cache_route_state {
            ActiveCacheRouteState::Baseline => {
                if !active_cache_route_baseline_is_eligible(&assignment.active_cache_route_baseline)
                {
                    decision.skip_reason =
                        Some("active_cache_route_baseline_insufficient".to_string());
                    return false;
                }
                if owner
                    .as_deref()
                    .is_some_and(|current_owner| current_owner != conversation_id)
                {
                    decision.skip_reason = Some("active_cache_route_realm_busy".to_string());
                    return false;
                }
                assignment.active_cache_route_state = ActiveCacheRouteState::Candidate;
                assignment.active_cache_route_candidate = ActiveCacheRouteEvidence::default();
                assignment.active_cache_route_reason =
                    Some("active_cache_route_collecting".to_string());
                assignment.active_cache_route_valid_until =
                    Some(now + Duration::hours(ACTIVE_CACHE_ROUTE_LEASE_HOURS));
                (ActiveCacheRouteState::Candidate, true)
            }
            ActiveCacheRouteState::Candidate | ActiveCacheRouteState::Promoted => {
                if owner
                    .as_deref()
                    .is_some_and(|current_owner| current_owner != conversation_id)
                {
                    decision.skip_reason = Some("active_cache_route_realm_busy".to_string());
                    return false;
                }
                if assignment.active_cache_route_valid_until.is_none() {
                    assignment.active_cache_route_valid_until =
                        Some(now + Duration::hours(ACTIVE_CACHE_ROUTE_LEASE_HOURS));
                }
                (assignment.active_cache_route_state, owner.is_none())
            }
            ActiveCacheRouteState::RolledBack => {
                decision.arm = ShadowAffinityArm::Baseline;
                decision.skip_reason = Some("active_cache_route_rolled_back".to_string());
                return false;
            }
        }
    };
    if claim_owner {
        store
            .active_cache_route_owners
            .insert(decision.realm_id.clone(), conversation_id.to_string());
    }

    decision.candidate_variant = ShadowCacheCandidateVariant::CohortKey;
    decision.arm = ShadowAffinityArm::Candidate;
    decision.mode = "applied".to_string();
    decision.decision = match state {
        ActiveCacheRouteState::Candidate => "active_cache_candidate_applied",
        ActiveCacheRouteState::Promoted => "active_cache_route_promoted",
        ActiveCacheRouteState::Baseline | ActiveCacheRouteState::RolledBack => unreachable!(),
    }
    .to_string();
    decision.skip_reason = None;
    true
}

fn observe_active_cache_route(
    store: &mut ShadowAffinityStore,
    decision: &ShadowAffinityDecision,
    input: ShadowObservationInput,
    now: DateTime<Utc>,
) {
    let Some(conversation_id) = decision.assignment_key.as_deref() else {
        return;
    };
    let mut release_owner = None;
    {
        let Some(assignment) = store.assignments.get_mut(conversation_id) else {
            return;
        };
        if assignment.realm_id != decision.realm_id {
            return;
        }
        if active_cache_route_is_applied(decision) {
            record_active_cache_route_evidence(&mut assignment.active_cache_route_candidate, input);
            let rollback = active_cache_route_rollback_reason(
                &assignment.active_cache_route_baseline,
                &assignment.active_cache_route_candidate,
                input,
            );
            if let Some(reason) = rollback {
                assignment.active_cache_route_state = ActiveCacheRouteState::RolledBack;
                assignment.active_cache_route_reason = Some(reason.to_string());
                release_owner = Some(assignment.realm_id.clone());
            } else if assignment.active_cache_route_state == ActiveCacheRouteState::Candidate
                && assignment
                    .active_cache_route_candidate
                    .successful_observations
                    >= ACTIVE_CACHE_ROUTE_MIN_CANDIDATE_SUCCESSFUL_OBSERVATIONS
            {
                if active_cache_route_ratio_is_strictly_higher(
                    assignment.active_cache_route_candidate.cache_read_tokens,
                    assignment.active_cache_route_candidate.input_tokens,
                    assignment.active_cache_route_baseline.cache_read_tokens,
                    assignment.active_cache_route_baseline.input_tokens,
                ) {
                    assignment.active_cache_route_state = ActiveCacheRouteState::Promoted;
                    assignment.active_cache_route_reason =
                        Some("active_cache_route_positive_raw_cache_gain".to_string());
                    assignment.active_cache_route_valid_until =
                        Some(now + Duration::hours(ACTIVE_CACHE_ROUTE_LEASE_HOURS));
                } else {
                    assignment.active_cache_route_state = ActiveCacheRouteState::RolledBack;
                    assignment.active_cache_route_reason =
                        Some("active_cache_route_no_positive_raw_cache_gain".to_string());
                    release_owner = Some(assignment.realm_id.clone());
                }
            }
        } else if assignment.active_cache_route_state == ActiveCacheRouteState::Baseline {
            record_active_cache_route_evidence(&mut assignment.active_cache_route_baseline, input);
            assignment.active_cache_route_legacy_seed_consumed = true;
        }
    }
    if let Some(realm_id) = release_owner {
        clear_active_cache_route_owner(store, &realm_id, conversation_id);
    }
}

pub(super) fn compute_shadow_affinity(
    store: &mut ShadowAffinityStore,
    identity: &AffinityIdentity,
    stateless_assignment_key: Option<&str>,
    now: DateTime<Utc>,
    tool_output_chars: u64,
    largest_tool_output_chars: u64,
) -> ShadowAffinityDecision {
    let started = Instant::now();
    let giant_tail =
        tool_output_chars >= GIANT_TAIL_CHARS || largest_tool_output_chars >= GIANT_TAIL_CHARS;
    let Some(conversation_id) = identity.trusted_conversation_id.clone() else {
        if let Some(stateless_assignment_key) = stateless_assignment_key
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            return ShadowAffinityDecision {
                mode: "shadow".to_string(),
                assignment_key: None,
                realm_id: identity.realm_id.clone(),
                cohort_id: identity.cohort_id.clone(),
                lane: if giant_tail {
                    ShadowCacheLane::ToolBurstQuarantine
                } else {
                    ShadowCacheLane::Steady
                },
                candidate_variant: ShadowCacheCandidateVariant::CohortKey,
                arm: canary_arm_for_conversation(
                    stateless_assignment_key,
                    STATIC_COHORT_CANARY_PERCENT,
                ),
                shard: 0,
                policy_epoch: SHADOW_POLICY_EPOCH,
                anchor_epoch: 0,
                trusted_identity: false,
                decision: "stateless_assigned".to_string(),
                skip_reason: None,
                policy_compute_ms: started.elapsed().as_millis() as u64,
                validation_run_id: None,
                automatic_canary_status: None,
                automatic_canary_reason: None,
            };
        }
        return ShadowAffinityDecision {
            mode: "shadow".to_string(),
            assignment_key: None,
            realm_id: identity.realm_id.clone(),
            cohort_id: identity.cohort_id.clone(),
            lane: ShadowCacheLane::Transparent,
            candidate_variant: ShadowCacheCandidateVariant::CohortKey,
            arm: ShadowAffinityArm::Baseline,
            shard: 0,
            policy_epoch: SHADOW_POLICY_EPOCH,
            anchor_epoch: 0,
            trusted_identity: false,
            decision: "transparent".to_string(),
            skip_reason: Some("missing_trusted_conversation_identity".to_string()),
            policy_compute_ms: started.elapsed().as_millis() as u64,
            validation_run_id: None,
            automatic_canary_status: None,
            automatic_canary_reason: None,
        };
    };

    discard_inactive_assignment_for_scope(store, &conversation_id, &identity.realm_id, now);
    ensure_assignment_age_index(store);
    if !store.assignments.contains_key(&conversation_id) {
        make_room_for_new_assignment(store);
    }
    if let Some(previous_last_seen_at) = store
        .assignments
        .get(&conversation_id)
        .map(|assignment| assignment.last_seen_at)
    {
        store
            .assignment_age_index
            .remove(&(previous_last_seen_at, conversation_id.clone()));
    }
    let (
        realm_id,
        cohort_id,
        assignment_lane,
        arm,
        shard,
        policy_epoch,
        anchor_epoch,
        active_cache_route_scope_reset,
    ) = {
        let assignment = store
            .assignments
            .entry(conversation_id.clone())
            .or_insert_with(|| ShadowAffinityAssignment {
                conversation_id: conversation_id.clone(),
                cohort_id: identity.cohort_id.clone(),
                realm_id: identity.realm_id.clone(),
                policy_epoch: SHADOW_POLICY_EPOCH,
                lane: if giant_tail {
                    ShadowCacheLane::ToolBurstQuarantine
                } else {
                    ShadowCacheLane::Steady
                },
                arm: canary_arm_for_conversation(&conversation_id, STATIC_COHORT_CANARY_PERCENT),
                shard: 0,
                anchor_epoch: 0,
                created_at: now,
                last_seen_at: now,
                observations: 0,
                successful_observations: 0,
                usage_observations: 0,
                inconclusive_observations: 0,
                input_tokens: 0,
                cache_read_tokens: 0,
                active_cache_route_state: ActiveCacheRouteState::Baseline,
                active_cache_route_baseline: ActiveCacheRouteEvidence::default(),
                active_cache_route_candidate: ActiveCacheRouteEvidence::default(),
                active_cache_route_reason: None,
                active_cache_route_legacy_seed_consumed: false,
                active_cache_route_valid_until: None,
            });
        assignment.last_seen_at = now;
        let cohort_changed = assignment.cohort_id != identity.cohort_id;
        if cohort_changed {
            reset_active_cache_route(assignment);
        }
        assignment.cohort_id = identity.cohort_id.clone();
        assignment.realm_id = identity.realm_id.clone();
        let giant_tail_scope_reset = giant_tail && assignment.lane == ShadowCacheLane::Steady;
        if giant_tail_scope_reset {
            reset_active_cache_route(assignment);
            assignment.lane = ShadowCacheLane::ToolBurstQuarantine;
        }
        (
            assignment.realm_id.clone(),
            assignment.cohort_id.clone(),
            assignment.lane,
            assignment.arm,
            assignment.shard,
            assignment.policy_epoch,
            assignment.anchor_epoch,
            cohort_changed || giant_tail_scope_reset,
        )
    };
    store
        .assignment_age_index
        .insert((now, conversation_id.clone()));
    if active_cache_route_scope_reset {
        clear_active_cache_route_owner(store, &realm_id, &conversation_id);
        remove_post_burst_window(&mut store.post_burst, &conversation_id);
    }
    let active_window = active_post_burst_window_for_scope(
        &mut store.post_burst,
        &conversation_id,
        &realm_id,
        policy_epoch,
        anchor_epoch,
        now,
    );
    let lane = active_window
        .map(|(lane, _)| lane)
        .unwrap_or(assignment_lane);
    let window_candidate_variant = active_window.map(|(_, candidate_variant)| candidate_variant);
    let forced_candidate_variant = forced_isolated_candidate_variant();
    let (candidate_variant, readiness) = if let Some(candidate_variant) = window_candidate_variant {
        let readiness = current_post_burst_readiness(
            &mut store.post_burst,
            &realm_id,
            lane,
            candidate_variant,
            now,
        );
        (candidate_variant, readiness)
    } else if let Some(candidate_variant) = forced_candidate_variant {
        let readiness = current_post_burst_readiness(
            &mut store.post_burst,
            &realm_id,
            lane,
            candidate_variant,
            now,
        );
        (candidate_variant, readiness)
    } else {
        active_candidate_for_realm(&mut store.post_burst, &realm_id, lane, now)
    };
    let shard = if candidate_variant == ShadowCacheCandidateVariant::CohortTwoShard {
        cohort_two_shard_index(&conversation_id) as u32
    } else {
        shard
    };
    ShadowAffinityDecision {
        mode: "shadow".to_string(),
        assignment_key: Some(conversation_id),
        realm_id,
        cohort_id,
        lane,
        candidate_variant,
        arm,
        shard,
        policy_epoch,
        anchor_epoch,
        trusted_identity: true,
        decision: "assigned".to_string(),
        skip_reason: None,
        policy_compute_ms: started.elapsed().as_millis() as u64,
        validation_run_id: None,
        automatic_canary_status: Some(readiness.status),
        automatic_canary_reason: Some(readiness.reason),
    }
}

#[cfg(test)]
pub(super) fn apply_static_cohort_canary(
    decision: &mut ShadowAffinityDecision,
    smart_hit_enabled: bool,
) -> bool {
    let assigned = matches!(
        decision.decision.as_str(),
        "assigned" | "stateless_assigned"
    );
    if !smart_hit_enabled
        || !assigned
        || decision.arm != ShadowAffinityArm::Candidate
        || decision.lane != ShadowCacheLane::Steady
    {
        return false;
    }
    decision.mode = "applied".to_string();
    decision.decision = if decision.trusted_identity {
        "candidate_applied"
    } else {
        "stateless_candidate_applied"
    }
    .to_string();
    true
}

#[cfg(test)]
pub(super) fn apply_automatic_static_cohort_canary(
    decision: &mut ShadowAffinityDecision,
    smart_hit_enabled: bool,
) -> bool {
    apply_automatic_static_cohort_canary_with_switch(
        decision,
        smart_hit_enabled,
        automatic_static_cohort_admission_enabled(),
    )
}

#[cfg(test)]
fn automatic_static_cohort_admission_enabled() -> bool {
    false
}

#[cfg(test)]
fn automatic_static_cohort_force_no_gap_enabled() -> bool {
    false
}

#[cfg(test)]
fn provider_native_candidate_isolation_override_enabled() -> bool {
    false
}

#[cfg(test)]
fn apply_automatic_static_cohort_canary_with_switch(
    decision: &mut ShadowAffinityDecision,
    smart_hit_enabled: bool,
    kill_switch_enabled: bool,
) -> bool {
    let assigned = matches!(
        decision.decision.as_str(),
        "assigned" | "stateless_assigned"
    );
    let lane_eligible = matches!(
        decision.lane,
        ShadowCacheLane::Steady | ShadowCacheLane::ToolBurstQuarantine
    );
    if decision.lane == ShadowCacheLane::CompactedAnchor {
        decision.decision = "candidate_shadow_only".to_string();
        decision.skip_reason = Some("compaction_candidate_not_beneficial".to_string());
        return false;
    }
    if !smart_hit_enabled || !assigned || !lane_eligible {
        return false;
    }

    if decision.candidate_variant == ShadowCacheCandidateVariant::ProviderNative
        && !provider_native_candidate_isolation_override_enabled()
    {
        decision.decision = "candidate_shadow_only".to_string();
        decision.skip_reason = Some("provider_native_candidate_disabled".to_string());
        return false;
    }

    let readiness = decision
        .automatic_canary_status
        .unwrap_or(PostBurstReadinessStatus::InsufficientEvidence);
    let promoted = readiness == PostBurstReadinessStatus::ReadyForPromotion;
    if !promoted && decision.arm != ShadowAffinityArm::Candidate {
        return false;
    }
    decision.decision = "candidate_shadow_only".to_string();
    let force_no_gap_canary = automatic_static_cohort_force_no_gap_enabled()
        && readiness == PostBurstReadinessStatus::CanaryHealthy
        && decision.automatic_canary_reason.as_deref() == Some("no_addressable_post_burst_gap");
    if readiness == PostBurstReadinessStatus::RollbackRequired {
        decision.skip_reason = Some("automatic_canary_rolled_back".to_string());
        return false;
    }
    if readiness == PostBurstReadinessStatus::CanaryHealthy && !force_no_gap_canary {
        decision.skip_reason = Some("canary_not_promotable".to_string());
        return false;
    }
    let ready = matches!(
        readiness,
        PostBurstReadinessStatus::ReadyForCanary
            | PostBurstReadinessStatus::CanaryCollecting
            | PostBurstReadinessStatus::ReadyForPromotion
    ) || force_no_gap_canary;
    if !ready {
        decision.skip_reason = Some("awaiting_efficacy_evidence".to_string());
        return false;
    }
    if !kill_switch_enabled {
        decision.skip_reason = Some("automatic_canary_kill_switch_off".to_string());
        return false;
    }

    decision.mode = "applied".to_string();
    decision.decision = if promoted {
        "automatic_promoted_applied"
    } else {
        "automatic_candidate_applied"
    }
    .to_string();
    decision.skip_reason = None;
    true
}

pub(super) fn static_cohort_prompt_cache_key(decision: &ShadowAffinityDecision) -> Option<String> {
    if !matches!(decision.mode.as_str(), "applied" | "validation_applied") {
        return None;
    }
    let key_material = match decision.candidate_variant {
        ShadowCacheCandidateVariant::CohortKey => {
            format!("cache-cohort-key-v3\0{}", decision.cohort_id)
        }
        ShadowCacheCandidateVariant::CohortTwoShard => {
            let assignment_key = decision.assignment_key.as_deref()?;
            let shard = cohort_two_shard_index(assignment_key);
            format!(
                "cache-cohort-two-shard-v1\0{}\0{}",
                decision.cohort_id, shard
            )
        }
        ShadowCacheCandidateVariant::ProviderNative => return None,
    };
    Some(format!("{:x}", Sha256::digest(key_material.as_bytes())))
}

fn cohort_two_shard_index(assignment_key: &str) -> u8 {
    Sha256::digest(assignment_key.as_bytes())[0] % 2
}

pub(super) fn provider_native_candidate_applied(decision: &ShadowAffinityDecision) -> bool {
    matches!(decision.mode.as_str(), "applied" | "validation_applied")
        && decision.candidate_variant == ShadowCacheCandidateVariant::ProviderNative
}

fn canary_arm_for_conversation(conversation_id: &str, percent: u8) -> ShadowAffinityArm {
    if percent == 0 {
        return ShadowAffinityArm::Baseline;
    }
    let digest = Sha256::digest(conversation_id.as_bytes());
    let bucket = u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]]);
    if bucket % 100 < percent.min(100) as u32 {
        ShadowAffinityArm::Candidate
    } else {
        ShadowAffinityArm::Baseline
    }
}

pub(super) fn observe_shadow_affinity(
    store: &mut ShadowAffinityStore,
    decision: &ShadowAffinityDecision,
    input: ShadowObservationInput,
    now: DateTime<Utc>,
) {
    let Some(key) = decision.assignment_key.as_deref() else {
        return;
    };
    ensure_assignment_age_index(store);
    let Some(previous_last_seen_at) = store
        .assignments
        .get(key)
        .map(|assignment| assignment.last_seen_at)
    else {
        return;
    };
    store
        .assignment_age_index
        .remove(&(previous_last_seen_at, key.to_string()));
    {
        let assignment = store.assignments.get_mut(key).expect("checked above");
        assignment.last_seen_at = now;
        assignment.observations += 1;
        if input.success {
            assignment.successful_observations += 1;
        }
        if input.has_usage && input.input_tokens > 0 {
            assignment.usage_observations += 1;
            assignment.input_tokens += input.input_tokens;
            assignment.cache_read_tokens += input.cache_read_tokens;
        } else {
            assignment.inconclusive_observations += 1;
        }
        if input.giant_tail {
            assignment.inconclusive_observations += 1;
        }
        if decision.lane == ShadowCacheLane::CompactedAnchor
            && input.success
            && input.has_usage
            && !input.compaction_boundary
        {
            assignment.lane = if input.giant_tail {
                ShadowCacheLane::ToolBurstQuarantine
            } else {
                ShadowCacheLane::Steady
            };
        }
    }
    store.assignment_age_index.insert((now, key.to_string()));
    observe_active_cache_route(store, decision, input, now);
    observe_post_burst_evidence(&mut store.post_burst, decision, input, now);
    maintain_shadow_affinity_after_settlement(store, now);
}

pub(super) fn reset_anchor(
    store: &mut ShadowAffinityStore,
    conversation_id: &str,
    now: DateTime<Utc>,
) {
    ensure_assignment_age_index(store);
    let opened = if let Some(previous_last_seen_at) = store
        .assignments
        .get(conversation_id)
        .map(|assignment| assignment.last_seen_at)
    {
        store
            .assignment_age_index
            .remove(&(previous_last_seen_at, conversation_id.to_string()));
        let assignment = store
            .assignments
            .get_mut(conversation_id)
            .expect("checked above");
        reset_active_cache_route(assignment);
        assignment.anchor_epoch = assignment.anchor_epoch.saturating_add(1);
        assignment.lane = ShadowCacheLane::CompactedAnchor;
        assignment.last_seen_at = now;
        Some((
            assignment.realm_id.clone(),
            assignment.policy_epoch,
            assignment.anchor_epoch,
        ))
    } else {
        None
    };
    if let Some((realm_id, policy_epoch, anchor_epoch)) = opened {
        clear_active_cache_route_owner(store, &realm_id, conversation_id);
        store
            .assignment_age_index
            .insert((now, conversation_id.to_string()));
        store.post_burst.next_window_id = store.post_burst.next_window_id.saturating_add(1).max(1);
        let window_id = store.post_burst.next_window_id;
        insert_post_burst_window(
            &mut store.post_burst,
            conversation_id.to_string(),
            PostBurstWindow {
                window_id,
                conversation_id: conversation_id.to_string(),
                opened_at: now,
                expires_at: now + Duration::hours(POST_BURST_WINDOW_TTL_HOURS),
                remaining_requests: POST_COMPACTION_FOLLOWUP_REQUESTS,
                captured_requests: 0,
                lane: ShadowCacheLane::CompactedAnchor,
                candidate_variant: ShadowCacheCandidateVariant::CohortKey,
                realm_id,
                policy_epoch,
                anchor_epoch,
            },
        );
    }
}

pub(crate) fn evict_assignments(store: &mut ShadowAffinityStore, now: DateTime<Utc>) {
    let expired = store
        .assignments
        .iter()
        .filter(|(_, assignment)| !assignment_is_current(assignment, now))
        .map(|(conversation_id, _)| conversation_id.clone())
        .collect::<Vec<_>>();
    for conversation_id in expired {
        remove_assignment(store, &conversation_id);
    }
    enforce_assignment_capacity(store);
    rebuild_assignment_age_index(store);
    evict_post_burst_evidence(&mut store.post_burst, now);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, Channel, ProviderConfig, SelectedProviderKey};
    use crate::proxy::affinity_identity;
    use chrono::Utc;
    use serde_json::json;

    fn identity(thread: &str) -> AffinityIdentity {
        let config = AppConfig::default();
        let decision = super::super::RouteDecision {
            provider: ProviderConfig {
                id: "provider".to_string(),
                name: "provider".to_string(),
                base_url: "https://example.test/v1".to_string(),
                models_url: None,
                is_full_url: false,
                custom_user_agent: None,
                api_key_encrypted: None,
                channel: Channel::Responses,
                prompt_cache_retention_enabled: false,
                request_body_gzip_enabled: false,
                use_system_proxy: false,
                models: Vec::new(),
                enabled: true,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            upstream_channel: Channel::Responses,
            model: "model".to_string(),
        };
        let request = json!({"thread_id": thread, "instructions": "stable"});
        affinity_identity::derive(
            &config,
            &decision,
            &request,
            &request,
            Some("codex"),
            &SelectedProviderKey {
                secret: "secret".to_string(),
                key_id: Some("key".to_string()),
            },
        )
    }

    fn append_scope_evidence(
        ledger: &mut PostBurstEvidenceLedger,
        realm_id: &str,
        now: DateTime<Utc>,
    ) -> String {
        let observation = PostBurstEvidence {
            window_id: 1,
            conversation_id: format!("{realm_id}-conversation"),
            observed_at: now,
            followup_index: 1,
            realm_id: realm_id.to_string(),
            lane: ShadowCacheLane::Steady,
            candidate_variant: ShadowCacheCandidateVariant::CohortKey,
            arm: ShadowAffinityArm::Baseline,
            policy_epoch: SHADOW_POLICY_EPOCH,
            anchor_epoch: 0,
            success: true,
            status: 200,
            has_usage: true,
            input_tokens: 16_384,
            cache_read_tokens: 15_360,
            cache_ratio_bps: 9_375,
            avoidable_gap_tokens: 0,
            provider_unstable_gap_tokens: 0,
            ttft_ms: 100,
            attempt_count: 1,
            candidate_applied: false,
        };
        let comparison_key = post_burst_evidence_scope_key(&observation);
        record_evidence_scope(ledger, &observation);
        ledger.evidence.push_back(observation);
        comparison_key
    }

    #[allow(clippy::too_many_arguments)]
    fn record_post_burst_window(
        store: &mut ShadowAffinityStore,
        thread: &str,
        arm: ShadowAffinityArm,
        now: DateTime<Utc>,
        candidate_applied: bool,
        success: bool,
        cache_read_tokens: u64,
        avoidable_gap_tokens: u64,
        provider_unstable_gap_tokens: u64,
        ttft_ms: u64,
        attempt_count: u64,
    ) -> String {
        let identity = identity(thread);
        let mut burst = compute_shadow_affinity(
            store,
            &identity,
            None,
            now,
            GIANT_TAIL_CHARS,
            GIANT_TAIL_CHARS,
        );
        burst.arm = arm;
        let realm_id = burst.realm_id.clone();
        observe_shadow_affinity(
            store,
            &burst,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 90_000,
                giant_tail: true,
                status: 200,
                ttft_ms,
                attempt_count: 1,
                ..ShadowObservationInput::default()
            },
            now,
        );

        for followup in 1..=POST_BURST_FOLLOWUP_REQUESTS {
            let observed_at = now + Duration::seconds(i64::from(followup));
            let mut decision = compute_shadow_affinity(store, &identity, None, observed_at, 0, 0);
            decision.arm = arm;
            if candidate_applied {
                decision.mode = "applied".to_string();
                decision.decision = "automatic_candidate_applied".to_string();
            }
            observe_shadow_affinity(
                store,
                &decision,
                ShadowObservationInput {
                    success,
                    has_usage: true,
                    input_tokens: 100_000,
                    cache_read_tokens,
                    status: if success { 200 } else { 502 },
                    ttft_ms,
                    avoidable_gap_tokens,
                    provider_unstable_gap_tokens,
                    attempt_count,
                    ..ShadowObservationInput::default()
                },
                observed_at,
            );
        }
        realm_id
    }

    fn phase_evidence(
        followup_index: u8,
        cache_read_tokens: u64,
        ttft_ms: u64,
    ) -> PostBurstEvidence {
        PostBurstEvidence {
            window_id: 1,
            conversation_id: "phase-test".to_string(),
            observed_at: Utc::now(),
            followup_index,
            realm_id: "realm".to_string(),
            lane: ShadowCacheLane::CompactedAnchor,
            candidate_variant: ShadowCacheCandidateVariant::CohortKey,
            arm: ShadowAffinityArm::Baseline,
            policy_epoch: 1,
            anchor_epoch: 1,
            success: true,
            status: 200,
            has_usage: true,
            input_tokens: 100_000,
            cache_read_tokens,
            cache_ratio_bps: ratio_bps(cache_read_tokens, 100_000),
            avoidable_gap_tokens: 0,
            provider_unstable_gap_tokens: 0,
            ttft_ms,
            attempt_count: 1,
            candidate_applied: false,
        }
    }

    #[test]
    fn post_burst_summary_separates_first_cold_followup_from_stable_recovery() {
        let evidence = [
            phase_evidence(1, 20_000, 9_000),
            phase_evidence(2, 90_000, 3_000),
            phase_evidence(3, 95_000, 2_000),
        ];

        let summary = summarize_post_burst_arm(evidence.iter());

        assert_eq!(summary.observations, 3);
        assert_eq!(summary.cache_ratio_bps, 6_835);
        assert_eq!(summary.first_followup.observations, 1);
        assert_eq!(summary.first_followup.cache_ratio_bps, 2_000);
        assert_eq!(summary.first_followup.average_ttft_ms, 9_000);
        assert_eq!(summary.stable_followups.observations, 2);
        assert_eq!(summary.stable_followups.cache_ratio_bps, 9_252);
        assert_eq!(summary.stable_followups.average_ttft_ms, 2_500);
    }

    #[test]
    fn trusted_assignments_enter_sticky_tool_burst_quarantine() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let first = identity("thread-a");
        let first_decision = compute_shadow_affinity(&mut store, &first, None, now, 0, 0);
        let second_decision = compute_shadow_affinity(
            &mut store,
            &first,
            None,
            now + Duration::minutes(1),
            GIANT_TAIL_CHARS,
            0,
        );
        assert_eq!(
            first_decision.assignment_key,
            second_decision.assignment_key
        );
        assert_eq!(first_decision.lane, ShadowCacheLane::Steady);
        assert_eq!(second_decision.lane, ShadowCacheLane::ToolBurstQuarantine);
        let assignment_key = second_decision.assignment_key.clone().unwrap();
        assert_eq!(
            store.assignments[&assignment_key].lane,
            ShadowCacheLane::ToolBurstQuarantine
        );

        let quiet_followup =
            compute_shadow_affinity(&mut store, &first, None, now + Duration::minutes(2), 0, 0);
        assert_eq!(quiet_followup.lane, ShadowCacheLane::ToolBurstQuarantine);
        observe_shadow_affinity(
            &mut store,
            &quiet_followup,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 99_000,
                ..ShadowObservationInput::default()
            },
            now + Duration::minutes(2),
        );
        assert_eq!(
            store.assignments[&assignment_key].lane,
            ShadowCacheLane::ToolBurstQuarantine
        );

        reset_anchor(&mut store, &assignment_key, now + Duration::minutes(3));
        let reset =
            compute_shadow_affinity(&mut store, &first, None, now + Duration::minutes(3), 0, 0);
        assert_eq!(reset.lane, ShadowCacheLane::CompactedAnchor);
        assert_eq!(store.assignments.len(), 1);

        let mut no_identity = first.clone();
        no_identity.trusted_conversation_id = None;
        let transparent = compute_shadow_affinity(&mut store, &no_identity, None, now, 0, 0);
        assert!(!transparent.trusted_identity);
        assert!(transparent.assignment_key.is_none());
        assert_eq!(transparent.lane, ShadowCacheLane::Transparent);
    }

    #[test]
    fn missing_trusted_identity_uses_stateless_anchor_without_persisting_assignment() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut untrusted = identity("thread-a");
        untrusted.trusted_conversation_id = None;

        let decision =
            compute_shadow_affinity(&mut store, &untrusted, Some("content-anchor-a"), now, 0, 0);

        assert!(!decision.trusted_identity);
        assert!(decision.assignment_key.is_none());
        assert_eq!(decision.lane, ShadowCacheLane::Steady);
        assert_eq!(decision.decision, "stateless_assigned");
        assert!(store.assignments.is_empty());

        let burst = compute_shadow_affinity(
            &mut store,
            &untrusted,
            Some("content-anchor-a"),
            now + Duration::seconds(1),
            GIANT_TAIL_CHARS,
            GIANT_TAIL_CHARS,
        );
        assert_eq!(burst.lane, ShadowCacheLane::ToolBurstQuarantine);
        assert!(store.assignments.is_empty());
    }

    #[test]
    fn stateless_candidate_is_stable_applied_and_never_persisted() {
        let mut candidate_anchor = None;
        for index in 0..200 {
            let anchor = format!("content-anchor-{index}");
            if canary_arm_for_conversation(&anchor, STATIC_COHORT_CANARY_PERCENT)
                == ShadowAffinityArm::Candidate
            {
                candidate_anchor = Some(anchor);
                break;
            }
        }
        let candidate_anchor =
            candidate_anchor.expect("the bounded sample should find a stateless candidate");
        let mut store = ShadowAffinityStore::default();
        let mut untrusted = identity("thread-a");
        untrusted.trusted_conversation_id = None;
        let now = Utc::now();

        let mut first =
            compute_shadow_affinity(&mut store, &untrusted, Some(&candidate_anchor), now, 0, 0);
        let second = compute_shadow_affinity(
            &mut store,
            &untrusted,
            Some(&candidate_anchor),
            now + Duration::minutes(1),
            0,
            0,
        );

        assert_eq!(first.arm, ShadowAffinityArm::Candidate);
        assert_eq!(second.arm, ShadowAffinityArm::Candidate);
        assert!(apply_static_cohort_canary(&mut first, true));
        assert_eq!(first.decision, "stateless_candidate_applied");
        assert!(store.assignments.is_empty());
    }

    #[test]
    fn automatic_admission_stays_shadow_only_until_manual_validation_wins() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut saw_candidate = false;

        for index in 0..200 {
            let mut decision = compute_shadow_affinity(
                &mut store,
                &identity(&format!("auto-thread-{index}")),
                None,
                now,
                0,
                0,
            );
            saw_candidate |= decision.arm == ShadowAffinityArm::Candidate;
            assert!(!apply_automatic_static_cohort_canary(&mut decision, true));
            assert_ne!(decision.mode, "applied");
            if decision.arm == ShadowAffinityArm::Candidate {
                assert_eq!(decision.decision, "candidate_shadow_only");
                assert_eq!(
                    decision.skip_reason.as_deref(),
                    Some("awaiting_efficacy_evidence")
                );
            }
        }
        assert!(saw_candidate);

        let mut smart_cache_disabled = ShadowAffinityDecision {
            arm: ShadowAffinityArm::Candidate,
            ..compute_shadow_affinity(
                &mut store,
                &identity("disabled-smart-cache"),
                None,
                now,
                0,
                0,
            )
        };
        smart_cache_disabled.decision = "assigned".to_string();
        assert!(!apply_automatic_static_cohort_canary(
            &mut smart_cache_disabled,
            false
        ));
        assert_eq!(smart_cache_disabled.decision, "assigned");

        let mut compacted = smart_cache_disabled.clone();
        compacted.lane = ShadowCacheLane::CompactedAnchor;
        assert!(!apply_automatic_static_cohort_canary(&mut compacted, true));
        assert_eq!(compacted.decision, "candidate_shadow_only");
        assert_eq!(
            compacted.skip_reason.as_deref(),
            Some("compaction_candidate_not_beneficial")
        );

        let mut transparent = smart_cache_disabled;
        transparent.lane = ShadowCacheLane::Transparent;
        assert!(!apply_automatic_static_cohort_canary(
            &mut transparent,
            true
        ));
        assert_eq!(transparent.decision, "assigned");
    }

    #[test]
    fn provider_native_candidate_stays_disabled_outside_isolated_override() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut decision = compute_shadow_affinity(
            &mut store,
            &identity("provider-native-disabled"),
            None,
            now,
            0,
            0,
        );
        decision.arm = ShadowAffinityArm::Candidate;
        decision.candidate_variant = ShadowCacheCandidateVariant::ProviderNative;
        decision.decision = "assigned".to_string();
        assert!(!apply_automatic_static_cohort_canary(&mut decision, true));
        assert_eq!(decision.mode, "shadow");
        assert_eq!(decision.decision, "candidate_shadow_only");
        assert_eq!(
            decision.skip_reason.as_deref(),
            Some("provider_native_candidate_disabled")
        );
    }

    #[test]
    fn failed_missing_usage_and_giant_tail_never_create_positive_learning() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let decision = compute_shadow_affinity(&mut store, &identity("thread-a"), None, now, 0, 0);
        observe_shadow_affinity(
            &mut store,
            &decision,
            ShadowObservationInput {
                success: false,
                has_usage: false,
                giant_tail: true,
                ..ShadowObservationInput::default()
            },
            now,
        );
        let assignment = store.assignments.values().next().unwrap();
        assert_eq!(assignment.successful_observations, 0);
        assert_eq!(assignment.usage_observations, 0);
        assert_eq!(assignment.inconclusive_observations, 2);
    }

    #[test]
    fn compacted_anchor_returns_to_steady_after_first_successful_followup() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let identity = identity("thread-after-compaction");
        let initial = compute_shadow_affinity(&mut store, &identity, None, now, 0, 0);
        let assignment_key = initial.assignment_key.clone().unwrap();
        reset_anchor(&mut store, &assignment_key, now + Duration::seconds(1));

        let boundary = compute_shadow_affinity(
            &mut store,
            &identity,
            None,
            now + Duration::seconds(2),
            0,
            0,
        );
        assert_eq!(boundary.lane, ShadowCacheLane::CompactedAnchor);
        observe_shadow_affinity(
            &mut store,
            &boundary,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 30_000,
                cache_read_tokens: 11_000,
                giant_tail: false,
                compaction_boundary: true,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(3),
        );
        assert_eq!(
            store.assignments[&assignment_key].lane,
            ShadowCacheLane::CompactedAnchor
        );

        let followup = compute_shadow_affinity(
            &mut store,
            &identity,
            None,
            now + Duration::seconds(4),
            0,
            0,
        );
        observe_shadow_affinity(
            &mut store,
            &followup,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 30_500,
                cache_read_tokens: 29_500,
                giant_tail: false,
                compaction_boundary: false,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(5),
        );

        assert_eq!(
            store.assignments[&assignment_key].lane,
            ShadowCacheLane::Steady
        );
    }

    #[test]
    fn compacted_anchor_waits_for_usage_and_quarantines_a_giant_followup() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let identity = identity("thread-after-compaction-with-tool-burst");
        let initial = compute_shadow_affinity(&mut store, &identity, None, now, 0, 0);
        let assignment_key = initial.assignment_key.clone().unwrap();
        reset_anchor(&mut store, &assignment_key, now + Duration::seconds(1));

        for (seconds, success, has_usage) in [(2, false, true), (3, true, false)] {
            let decision = compute_shadow_affinity(
                &mut store,
                &identity,
                None,
                now + Duration::seconds(seconds),
                0,
                0,
            );
            observe_shadow_affinity(
                &mut store,
                &decision,
                ShadowObservationInput {
                    success,
                    has_usage,
                    input_tokens: has_usage.then_some(30_000).unwrap_or_default(),
                    cache_read_tokens: has_usage.then_some(11_000).unwrap_or_default(),
                    giant_tail: false,
                    compaction_boundary: false,
                    ..ShadowObservationInput::default()
                },
                now + Duration::seconds(seconds),
            );
            assert_eq!(
                store.assignments[&assignment_key].lane,
                ShadowCacheLane::CompactedAnchor
            );
        }

        let giant_followup = compute_shadow_affinity(
            &mut store,
            &identity,
            None,
            now + Duration::seconds(4),
            GIANT_TAIL_CHARS,
            GIANT_TAIL_CHARS,
        );
        observe_shadow_affinity(
            &mut store,
            &giant_followup,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 120_000,
                cache_read_tokens: 11_000,
                giant_tail: true,
                compaction_boundary: false,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(4),
        );

        assert_eq!(
            store.assignments[&assignment_key].lane,
            ShadowCacheLane::ToolBurstQuarantine
        );
    }

    #[test]
    fn compaction_boundary_opens_a_four_request_recovery_window() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let identity = identity("thread-compaction-window");
        let initial = compute_shadow_affinity(&mut store, &identity, None, now, 0, 0);
        let assignment_key = initial.assignment_key.clone().unwrap();

        reset_anchor(&mut store, &assignment_key, now + Duration::seconds(1));
        assert_eq!(store.post_burst.windows.len(), 1);

        let boundary = compute_shadow_affinity(
            &mut store,
            &identity,
            None,
            now + Duration::seconds(2),
            0,
            0,
        );
        observe_shadow_affinity(
            &mut store,
            &boundary,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 30_000,
                cache_read_tokens: 11_000,
                compaction_boundary: true,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(2),
        );
        assert!(store.post_burst.evidence.is_empty());

        for index in 1..=4 {
            let decision = compute_shadow_affinity(
                &mut store,
                &identity,
                None,
                now + Duration::seconds(index + 2),
                0,
                0,
            );
            assert_eq!(decision.lane, ShadowCacheLane::CompactedAnchor);
            observe_shadow_affinity(
                &mut store,
                &decision,
                ShadowObservationInput {
                    success: true,
                    has_usage: true,
                    input_tokens: 30_000 + index as u64 * 100,
                    cache_read_tokens: 20_000 + index as u64 * 1_000,
                    ..ShadowObservationInput::default()
                },
                now + Duration::seconds(index + 2),
            );
        }

        assert!(!store.post_burst.windows.contains_key(&assignment_key));
        let evidence = store.post_burst.evidence.iter().collect::<Vec<_>>();
        assert_eq!(evidence.len(), 4);
        assert!(evidence
            .iter()
            .all(|item| item.lane == ShadowCacheLane::CompactedAnchor));
        assert_eq!(evidence[0].followup_index, 1);
        assert_eq!(evidence[3].followup_index, 4);
    }

    #[test]
    fn automatic_candidate_canary_stays_disabled_for_compaction_recovery() {
        let mut decision = ShadowAffinityDecision {
            mode: "shadow".to_string(),
            assignment_key: Some("conversation".to_string()),
            realm_id: "realm".to_string(),
            cohort_id: "cohort".to_string(),
            lane: ShadowCacheLane::CompactedAnchor,
            candidate_variant: ShadowCacheCandidateVariant::CohortKey,
            arm: ShadowAffinityArm::Candidate,
            shard: 0,
            policy_epoch: SHADOW_POLICY_EPOCH,
            anchor_epoch: 1,
            trusted_identity: true,
            decision: "assigned".to_string(),
            skip_reason: None,
            policy_compute_ms: 0,
            validation_run_id: None,
            automatic_canary_status: Some(PostBurstReadinessStatus::ReadyForCanary),
            automatic_canary_reason: Some("comparable_shadow_evidence_ready".to_string()),
        };

        assert!(!apply_automatic_static_cohort_canary_with_switch(
            &mut decision,
            true,
            true
        ));
        assert_eq!(decision.decision, "candidate_shadow_only");
        assert_eq!(
            decision.skip_reason.as_deref(),
            Some("compaction_candidate_not_beneficial")
        );
    }

    #[test]
    fn static_cohort_canary_is_sticky_and_only_applies_to_steady_candidates() {
        let mut candidate_id = None;
        for index in 0..200 {
            let id = format!("thread-{index}");
            let identity = identity(&id);
            if identity
                .trusted_conversation_id
                .as_deref()
                .is_some_and(|conversation_id| {
                    canary_arm_for_conversation(conversation_id, STATIC_COHORT_CANARY_PERCENT)
                        == ShadowAffinityArm::Candidate
                })
            {
                candidate_id = Some(id);
                break;
            }
        }
        let candidate_id = candidate_id.expect("the bounded canary sample should find a candidate");
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let identity = identity(&candidate_id);
        let mut decision = compute_shadow_affinity(&mut store, &identity, None, now, 0, 0);
        assert_eq!(decision.arm, ShadowAffinityArm::Candidate);
        assert!(apply_static_cohort_canary(&mut decision, true));
        assert_eq!(decision.mode, "applied");
        assert_eq!(decision.decision, "candidate_applied");
        let candidate_key = static_cohort_prompt_cache_key(&decision)
            .expect("applied candidate must have a stable cohort cache key");
        assert_eq!(candidate_key.len(), 64);
        assert_ne!(candidate_key, decision.cohort_id);
        let mut same_decision = decision.clone();
        same_decision.mode = "validation_applied".to_string();
        assert_eq!(
            static_cohort_prompt_cache_key(&same_decision),
            Some(candidate_key)
        );

        let mut another_conversation = decision.clone();
        another_conversation.assignment_key = Some("another-conversation".to_string());
        assert_eq!(
            static_cohort_prompt_cache_key(&another_conversation),
            static_cohort_prompt_cache_key(&decision)
        );

        let mut two_shard = decision.clone();
        two_shard.candidate_variant = ShadowCacheCandidateVariant::CohortTwoShard;
        let stable_two_shard_key = static_cohort_prompt_cache_key(&two_shard)
            .expect("two-shard candidate must have a cache key");
        assert_eq!(
            static_cohort_prompt_cache_key(&two_shard),
            Some(stable_two_shard_key.clone())
        );
        let mut observed_keys = Vec::new();
        for index in 0..128 {
            two_shard.assignment_key = Some(format!("two-shard-conversation-{index}"));
            let key = static_cohort_prompt_cache_key(&two_shard).unwrap();
            if !observed_keys.contains(&key) {
                observed_keys.push(key);
            }
        }
        assert_eq!(observed_keys.len(), 2);

        two_shard.candidate_variant = ShadowCacheCandidateVariant::ProviderNative;
        assert_eq!(static_cohort_prompt_cache_key(&two_shard), None);

        let mut again = compute_shadow_affinity(&mut store, &identity, None, now, 0, 0);
        assert_eq!(again.arm, ShadowAffinityArm::Candidate);
        assert!(!apply_static_cohort_canary(&mut again, false));

        let mut quarantined = decision.clone();
        quarantined.lane = ShadowCacheLane::ToolBurstQuarantine;
        quarantined.mode = "shadow".to_string();
        assert!(!apply_static_cohort_canary(&mut quarantined, true));
    }

    #[test]
    fn giant_tail_captures_exactly_the_next_three_requests() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let realm_id = record_post_burst_window(
            &mut store,
            "post-burst-exact",
            ShadowAffinityArm::Baseline,
            now,
            false,
            true,
            87_000,
            2_048,
            512,
            1_250,
            1,
        );

        assert!(store.post_burst.windows.is_empty());
        assert_eq!(store.post_burst.evidence.len(), 3);
        let followups = store
            .post_burst
            .evidence
            .iter()
            .map(|observation| observation.followup_index)
            .collect::<Vec<_>>();
        assert_eq!(followups, vec![1, 2, 3]);
        assert!(store
            .post_burst
            .evidence
            .iter()
            .all(|observation| observation.realm_id == realm_id
                && observation.cache_ratio_bps == 8_700
                && observation.avoidable_gap_tokens == 2_048
                && observation.provider_unstable_gap_tokens == 512
                && observation.ttft_ms == 1_250
                && observation.attempt_count == 1));

        let identity = identity("post-burst-exact");
        let decision = compute_shadow_affinity(
            &mut store,
            &identity,
            None,
            now + Duration::seconds(10),
            0,
            0,
        );
        observe_shadow_affinity(
            &mut store,
            &decision,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 99_000,
                status: 200,
                attempt_count: 1,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(10),
        );
        assert_eq!(store.post_burst.evidence.len(), 3);
    }

    #[test]
    fn comparable_post_burst_arms_enable_only_the_controlled_canary_path() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let realm_id = record_post_burst_window(
            &mut store,
            "baseline-a",
            ShadowAffinityArm::Baseline,
            now,
            false,
            true,
            90_000,
            1_024,
            0,
            1_000,
            1,
        );
        record_post_burst_window(
            &mut store,
            "baseline-b",
            ShadowAffinityArm::Baseline,
            now + Duration::seconds(10),
            false,
            true,
            90_000,
            1_024,
            0,
            1_000,
            1,
        );
        record_post_burst_window(
            &mut store,
            "baseline-c",
            ShadowAffinityArm::Baseline,
            now + Duration::seconds(15),
            false,
            true,
            90_000,
            1_024,
            0,
            1_000,
            1,
        );
        record_post_burst_window(
            &mut store,
            "candidate-shadow-a",
            ShadowAffinityArm::Candidate,
            now + Duration::seconds(20),
            false,
            true,
            90_000,
            1_024,
            0,
            1_000,
            1,
        );
        record_post_burst_window(
            &mut store,
            "candidate-shadow-b",
            ShadowAffinityArm::Candidate,
            now + Duration::seconds(30),
            false,
            true,
            90_000,
            1_024,
            0,
            1_000,
            1,
        );
        record_post_burst_window(
            &mut store,
            "candidate-shadow-c",
            ShadowAffinityArm::Candidate,
            now + Duration::seconds(35),
            false,
            true,
            90_000,
            1_024,
            0,
            1_000,
            1,
        );

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::ReadyForCanary);
        assert_eq!(readiness.baseline.observations, 9);
        assert_eq!(readiness.candidate_shadow.observations, 9);

        let candidate_identity = identity("candidate-shadow-a");
        let mut kill_switch_off = compute_shadow_affinity(
            &mut store,
            &candidate_identity,
            None,
            now + Duration::seconds(40),
            0,
            0,
        );
        kill_switch_off.arm = ShadowAffinityArm::Candidate;
        assert_eq!(
            kill_switch_off.automatic_canary_status,
            Some(PostBurstReadinessStatus::ReadyForCanary)
        );
        assert!(!apply_automatic_static_cohort_canary_with_switch(
            &mut kill_switch_off,
            true,
            false,
        ));
        assert_eq!(kill_switch_off.decision, "candidate_shadow_only");
        assert_eq!(
            kill_switch_off.skip_reason.as_deref(),
            Some("automatic_canary_kill_switch_off")
        );

        let mut controlled = compute_shadow_affinity(
            &mut store,
            &candidate_identity,
            None,
            now + Duration::seconds(41),
            0,
            0,
        );
        controlled.arm = ShadowAffinityArm::Candidate;
        assert!(apply_automatic_static_cohort_canary_with_switch(
            &mut controlled,
            true,
            true,
        ));
        assert_eq!(controlled.mode, "applied");
        assert_eq!(controlled.decision, "automatic_candidate_applied");

        let mut burst = compute_shadow_affinity(
            &mut store,
            &candidate_identity,
            None,
            now + Duration::seconds(42),
            GIANT_TAIL_CHARS,
            GIANT_TAIL_CHARS,
        );
        burst.arm = ShadowAffinityArm::Candidate;
        assert!(apply_automatic_static_cohort_canary_with_switch(
            &mut burst, true, true,
        ));
        observe_shadow_affinity(
            &mut store,
            &burst,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 95_000,
                giant_tail: true,
                status: 200,
                ttft_ms: 1_000,
                attempt_count: 1,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(42),
        );
        let mut first_applied = compute_shadow_affinity(
            &mut store,
            &candidate_identity,
            None,
            now + Duration::seconds(43),
            0,
            0,
        );
        first_applied.arm = ShadowAffinityArm::Candidate;
        assert!(apply_automatic_static_cohort_canary_with_switch(
            &mut first_applied,
            true,
            true,
        ));
        observe_shadow_affinity(
            &mut store,
            &first_applied,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 95_000,
                status: 200,
                ttft_ms: 1_000,
                attempt_count: 1,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(43),
        );
        let mut collecting = compute_shadow_affinity(
            &mut store,
            &candidate_identity,
            None,
            now + Duration::seconds(44),
            0,
            0,
        );
        collecting.arm = ShadowAffinityArm::Candidate;
        assert_eq!(
            collecting.automatic_canary_status,
            Some(PostBurstReadinessStatus::CanaryCollecting)
        );
        assert!(apply_automatic_static_cohort_canary_with_switch(
            &mut collecting,
            true,
            true,
        ));
    }

    #[test]
    fn fully_warm_post_burst_evidence_skips_a_canary_with_no_addressable_gap() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut realm_id = String::new();

        for index in 0..3 {
            realm_id = record_post_burst_window(
                &mut store,
                &format!("no-gap-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(index * 10),
                false,
                true,
                99_968,
                0,
                0,
                1_000,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("no-gap-shadow-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(40 + index * 10),
                false,
                true,
                99_968,
                0,
                0,
                1_000,
                1,
            );
        }

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::CanaryHealthy);
        assert_eq!(readiness.reason, "no_addressable_post_burst_gap");

        let mut decision = compute_shadow_affinity(
            &mut store,
            &identity("no-gap-shadow-0"),
            None,
            now + Duration::seconds(80),
            0,
            0,
        );
        decision.arm = ShadowAffinityArm::Candidate;
        assert!(!apply_automatic_static_cohort_canary_with_switch(
            &mut decision,
            true,
            true,
        ));
        assert_eq!(
            decision.skip_reason.as_deref(),
            Some("canary_not_promotable")
        );
        assert_eq!(
            decision.candidate_variant,
            ShadowCacheCandidateVariant::CohortKey
        );
        let two_shard_key = post_burst_comparison_key_for_candidate(
            &realm_id,
            ShadowCacheLane::ToolBurstQuarantine,
            ShadowCacheCandidateVariant::CohortTwoShard,
        );
        assert!(!store.post_burst.readiness.contains_key(&two_shard_key));
    }

    #[test]
    fn regressed_candidates_advance_in_order_with_isolated_evidence() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let realm_id = record_post_burst_window(
            &mut store,
            "rollback-baseline-a",
            ShadowAffinityArm::Baseline,
            now,
            false,
            true,
            92_000,
            512,
            0,
            900,
            1,
        );
        record_post_burst_window(
            &mut store,
            "rollback-baseline-b",
            ShadowAffinityArm::Baseline,
            now + Duration::seconds(10),
            false,
            true,
            92_000,
            512,
            0,
            900,
            1,
        );
        record_post_burst_window(
            &mut store,
            "rollback-baseline-c",
            ShadowAffinityArm::Baseline,
            now + Duration::seconds(15),
            false,
            true,
            92_000,
            512,
            0,
            900,
            1,
        );
        record_post_burst_window(
            &mut store,
            "rollback-shadow-a",
            ShadowAffinityArm::Candidate,
            now + Duration::seconds(20),
            false,
            true,
            92_000,
            512,
            0,
            900,
            1,
        );
        record_post_burst_window(
            &mut store,
            "rollback-shadow-b",
            ShadowAffinityArm::Candidate,
            now + Duration::seconds(30),
            false,
            true,
            92_000,
            512,
            0,
            900,
            1,
        );
        record_post_burst_window(
            &mut store,
            "rollback-shadow-c",
            ShadowAffinityArm::Candidate,
            now + Duration::seconds(35),
            false,
            true,
            92_000,
            512,
            0,
            900,
            1,
        );
        record_post_burst_window(
            &mut store,
            "rollback-applied",
            ShadowAffinityArm::Candidate,
            now + Duration::seconds(40),
            true,
            true,
            50_000,
            12_000,
            0,
            900,
            1,
        );
        record_post_burst_window(
            &mut store,
            "rollback-canary-baseline",
            ShadowAffinityArm::Baseline,
            now + Duration::seconds(45),
            false,
            true,
            92_000,
            512,
            0,
            900,
            1,
        );

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::CanaryCollecting);
        assert_eq!(readiness.reason, "collecting_promotion_evidence");
        assert_eq!(readiness.candidate_applied.observations, 3);
        assert!(!store.post_burst.rollbacks.contains_key(&comparison_key));

        for index in 1..3 {
            record_post_burst_window(
                &mut store,
                &format!("rollback-applied-more-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(50 + index * 20),
                true,
                true,
                50_000,
                12_000,
                0,
                900,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("rollback-canary-baseline-more-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(55 + index * 20),
                false,
                true,
                92_000,
                512,
                0,
                900,
                1,
            );
        }

        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::RollbackRequired);
        assert_eq!(readiness.reason, "candidate_cache_regression");
        assert_eq!(readiness.candidate_applied.observations, 9);
        assert_eq!(
            store
                .post_burst
                .rollbacks
                .get(&comparison_key)
                .map(String::as_str),
            Some("candidate_cache_regression")
        );

        let mut decision = compute_shadow_affinity(
            &mut store,
            &identity("rollback-applied"),
            None,
            now + Duration::seconds(100),
            0,
            0,
        );
        decision.arm = ShadowAffinityArm::Candidate;
        assert!(!apply_automatic_static_cohort_canary_with_switch(
            &mut decision,
            true,
            true,
        ));
        assert_eq!(
            decision.candidate_variant,
            ShadowCacheCandidateVariant::CohortTwoShard
        );
        assert_eq!(
            decision.skip_reason.as_deref(),
            Some("awaiting_efficacy_evidence")
        );

        record_post_burst_window(
            &mut store,
            "provider-native-shadow-baseline",
            ShadowAffinityArm::Baseline,
            now + Duration::seconds(130),
            false,
            true,
            98_000,
            2_000,
            0,
            1_000,
            1,
        );
        let two_shard_key = post_burst_comparison_key_for_candidate(
            &realm_id,
            ShadowCacheLane::ToolBurstQuarantine,
            ShadowCacheCandidateVariant::CohortTwoShard,
        );
        let two_shard = store.post_burst.readiness.get(&two_shard_key).unwrap();
        assert_eq!(two_shard.baseline.observations, 3);
        assert_eq!(
            store
                .post_burst
                .readiness
                .get(&comparison_key)
                .unwrap()
                .candidate_applied
                .observations,
            9
        );

        store.post_burst.rollbacks.insert(
            two_shard_key.clone(),
            "candidate_cache_regression".to_string(),
        );
        let mut provider_decision = compute_shadow_affinity(
            &mut store,
            &identity("provider-native-baseline"),
            None,
            now + Duration::seconds(160),
            GIANT_TAIL_CHARS,
            GIANT_TAIL_CHARS,
        );
        provider_decision.arm = ShadowAffinityArm::Baseline;
        assert_eq!(
            provider_decision.candidate_variant,
            ShadowCacheCandidateVariant::ProviderNative
        );
        observe_shadow_affinity(
            &mut store,
            &provider_decision,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 98_000,
                giant_tail: true,
                status: 200,
                ttft_ms: 1_000,
                attempt_count: 1,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(160),
        );
        let provider_native_key = post_burst_comparison_key_for_candidate(
            &realm_id,
            ShadowCacheLane::ToolBurstQuarantine,
            ShadowCacheCandidateVariant::ProviderNative,
        );
        assert_ne!(comparison_key, two_shard_key);
        assert_ne!(two_shard_key, provider_native_key);
        assert_eq!(
            store
                .post_burst
                .readiness
                .get(&comparison_key)
                .unwrap()
                .candidate_applied
                .observations,
            9
        );
        assert_eq!(
            store
                .post_burst
                .readiness
                .get(&two_shard_key)
                .unwrap()
                .baseline
                .observations,
            3
        );
    }

    #[test]
    fn safe_but_ineffective_cohort_canary_advances_to_two_shard() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let baseline_ttft = [1_000, 1_100, 1_200];
        let candidate_ttft = [1_050, 1_150, 1_250, 1_050, 1_150, 1_250];
        let mut realm_id = String::new();

        for (index, ttft_ms) in baseline_ttft.into_iter().enumerate() {
            realm_id = record_post_burst_window(
                &mut store,
                &format!("no-benefit-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(index as i64 * 10),
                false,
                true,
                97_500,
                2_500,
                0,
                ttft_ms,
                1,
            );
        }
        for (index, ttft_ms) in candidate_ttft.into_iter().enumerate() {
            record_post_burst_window(
                &mut store,
                &format!("no-benefit-shadow-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(40 + index as i64 * 10),
                false,
                true,
                97_500,
                2_500,
                0,
                ttft_ms,
                1,
            );
        }
        for (index, ttft_ms) in candidate_ttft.into_iter().enumerate() {
            record_post_burst_window(
                &mut store,
                &format!("no-benefit-applied-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(80 + index as i64 * 20),
                true,
                true,
                97_500,
                2_500,
                0,
                ttft_ms,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("no-benefit-canary-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(90 + index as i64 * 20),
                false,
                true,
                97_500,
                2_500,
                0,
                ttft_ms,
                1,
            );
        }

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::CanaryHealthy);
        assert_eq!(readiness.reason, "candidate_has_no_net_benefit");
        assert_eq!(readiness.baseline.observations, 27);
        assert_eq!(readiness.candidate_applied.observations, 18);
        assert_eq!(readiness.candidate_applied.ttft_p50_ms, 1_150);
        assert_eq!(readiness.candidate_applied.ttft_p95_ms, 1_250);
        assert_eq!(readiness.canary_baseline.observations, 18);
        assert_eq!(readiness.canary_baseline.cache_ratio_bps, 9_753);
        assert_eq!(readiness.canary_baseline.ttft_p50_ms, 1_150);
        assert_eq!(readiness.canary_baseline.ttft_p95_ms, 1_250);

        let mut decision = compute_shadow_affinity(
            &mut store,
            &identity("no-benefit-applied-0"),
            None,
            now + Duration::seconds(120),
            0,
            0,
        );
        decision.arm = ShadowAffinityArm::Candidate;
        assert!(!apply_automatic_static_cohort_canary_with_switch(
            &mut decision,
            true,
            true,
        ));
        assert_eq!(
            decision.candidate_variant,
            ShadowCacheCandidateVariant::CohortTwoShard
        );
        assert_eq!(
            decision.skip_reason.as_deref(),
            Some("awaiting_efficacy_evidence")
        );
    }

    #[test]
    fn effective_canary_is_ready_for_promotion() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut realm_id = String::new();

        for index in 0..6 {
            realm_id = record_post_burst_window(
                &mut store,
                &format!("promotion-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(index * 10),
                false,
                true,
                99_700,
                300,
                0,
                1_000,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("promotion-shadow-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(40 + index * 10),
                false,
                true,
                99_700,
                300,
                0,
                1_000,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("promotion-applied-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(80 + index * 20),
                true,
                true,
                99_820,
                180,
                0,
                1_050,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("promotion-canary-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(90 + index * 20),
                false,
                true,
                99_700,
                300,
                0,
                1_000,
                1,
            );
        }

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(
            readiness.status,
            PostBurstReadinessStatus::ReadyForPromotion
        );
        assert_eq!(readiness.reason, "canary_efficacy_gate_passed");
        assert_eq!(readiness.canary_baseline.observations, 18);
        assert_eq!(readiness.canary_baseline.cache_ratio_bps, 9_973);
        assert_eq!(readiness.candidate_applied.observations, 18);
        assert_eq!(readiness.candidate_applied.cache_ratio_bps, 9_985);
        assert_eq!(readiness.canary_baseline.provider_unstable_ratio_bps, 0);
        assert_eq!(readiness.candidate_applied.provider_unstable_ratio_bps, 0);

        let mut decision = compute_shadow_affinity(
            &mut store,
            &identity("promotion-applied-0"),
            None,
            now + Duration::seconds(120),
            0,
            0,
        );
        decision.arm = ShadowAffinityArm::Candidate;
        assert!(apply_automatic_static_cohort_canary_with_switch(
            &mut decision,
            true,
            true,
        ));
        assert_eq!(decision.decision, "automatic_promoted_applied");

        let mut promoted_baseline = compute_shadow_affinity(
            &mut store,
            &identity("promotion-baseline-0"),
            None,
            now + Duration::seconds(121),
            0,
            0,
        );
        promoted_baseline.arm = ShadowAffinityArm::Baseline;
        assert!(apply_automatic_static_cohort_canary_with_switch(
            &mut promoted_baseline,
            true,
            true,
        ));
        assert_eq!(promoted_baseline.decision, "automatic_promoted_applied");
    }

    #[test]
    fn positive_high_hit_canary_keeps_collecting_before_eighteen_observations() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut realm_id = String::new();

        for index in 0..3 {
            realm_id = record_post_burst_window(
                &mut store,
                &format!("collecting-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(index * 10),
                false,
                true,
                99_700,
                300,
                0,
                2_811,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("collecting-shadow-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(40 + index * 10),
                false,
                true,
                99_700,
                300,
                0,
                2_811,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("collecting-applied-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(80 + index * 20),
                true,
                true,
                99_820,
                180,
                0,
                2_233,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("collecting-canary-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(90 + index * 20),
                false,
                true,
                99_700,
                300,
                0,
                2_811,
                1,
            );
        }

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::CanaryCollecting);
        assert_eq!(readiness.reason, "collecting_promotion_evidence");
        assert_eq!(readiness.candidate_applied.observations, 9);
    }

    #[test]
    fn promotion_requires_cache_or_ttft_net_benefit() {
        assert!(!post_burst_baseline_has_addressable_gap(
            &PostBurstArmSummary {
                cache_ratio_bps: POST_BURST_RESIDUAL_CACHE_GAP_BPS,
                ..PostBurstArmSummary::default()
            }
        ));
        assert!(post_burst_baseline_has_addressable_gap(
            &PostBurstArmSummary {
                cache_ratio_bps: POST_BURST_RESIDUAL_CACHE_GAP_BPS - 1,
                ..PostBurstArmSummary::default()
            }
        ));
        let baseline = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_949,
            ..PostBurstArmSummary::default()
        };
        let below_target = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_949,
            ..PostBurstArmSummary::default()
        };
        assert_eq!(
            post_burst_promotion_blocker(&baseline, &below_target),
            Some("candidate_has_no_net_benefit")
        );

        let baseline = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_950,
            ..PostBurstArmSummary::default()
        };
        let equal = baseline.clone();
        assert_eq!(
            post_burst_promotion_blocker(&baseline, &equal),
            Some("candidate_has_no_net_benefit")
        );

        let lower = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_949,
            ..PostBurstArmSummary::default()
        };
        assert_eq!(
            post_burst_promotion_blocker(&baseline, &lower),
            Some("candidate_has_no_net_benefit")
        );

        let below_target_baseline = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_940,
            ..PostBurstArmSummary::default()
        };
        let positive_but_below_target = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_949,
            ..PostBurstArmSummary::default()
        };
        assert_eq!(
            post_burst_promotion_blocker(&below_target_baseline, &positive_but_below_target),
            None
        );

        let equal_cache_faster_ttft = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: baseline.cache_ratio_bps,
            average_ttft_ms: 800,
            ttft_p50_ms: 700,
            ttft_p95_ms: 1_400,
            ..PostBurstArmSummary::default()
        };
        let baseline_with_slower_ttft = PostBurstArmSummary {
            average_ttft_ms: 1_200,
            ttft_p50_ms: 1_000,
            ttft_p95_ms: 2_000,
            ..baseline.clone()
        };
        assert_eq!(
            post_burst_promotion_blocker(&baseline_with_slower_ttft, &equal_cache_faster_ttft),
            None
        );

        let equal_cache_faster_average_and_p95 = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: baseline.cache_ratio_bps,
            average_ttft_ms: 2_497,
            ttft_p50_ms: 2_256,
            ttft_p95_ms: 4_331,
            ..PostBurstArmSummary::default()
        };
        let long_tail_baseline = PostBurstArmSummary {
            average_ttft_ms: 2_889,
            ttft_p50_ms: 2_336,
            ttft_p95_ms: 8_689,
            ..baseline.clone()
        };
        assert_eq!(
            post_burst_promotion_blocker(&long_tail_baseline, &equal_cache_faster_average_and_p95),
            None
        );
        assert!(!post_burst_clear_ttft_regression(
            &long_tail_baseline,
            &equal_cache_faster_average_and_p95
        ));
        let systemic_ttft_regression = PostBurstArmSummary {
            ttft_p50_ms: 4_000,
            ttft_p95_ms: 15_000,
            ..equal_cache_faster_average_and_p95.clone()
        };
        assert!(post_burst_clear_ttft_regression(
            &long_tail_baseline,
            &systemic_ttft_regression
        ));

        let baseline = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_950,
            ..PostBurstArmSummary::default()
        };
        let one_basis_point_better = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_951,
            ..PostBurstArmSummary::default()
        };
        assert_eq!(
            post_burst_promotion_blocker(&baseline, &one_basis_point_better),
            None
        );

        let provider_gap_only = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_950,
            provider_unstable_ratio_bps: 0,
            ..PostBurstArmSummary::default()
        };
        let baseline_with_provider_gap = PostBurstArmSummary {
            success_rate_bps: 10_000,
            cache_ratio_bps: 9_950,
            provider_unstable_ratio_bps: 1_000,
            ..PostBurstArmSummary::default()
        };
        assert_eq!(
            post_burst_promotion_blocker(&baseline_with_provider_gap, &provider_gap_only),
            Some("candidate_has_no_net_benefit")
        );
    }

    #[test]
    fn ttft_long_tail_blocks_promotion_without_triggering_rollback() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut realm_id = String::new();

        for index in 0..3 {
            realm_id = record_post_burst_window(
                &mut store,
                &format!("ttft-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(index * 10),
                false,
                true,
                99_500,
                500,
                0,
                1_000,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("ttft-shadow-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(40 + index * 10),
                false,
                true,
                99_500,
                500,
                0,
                1_000,
                1,
            );
        }
        for (index, ttft_ms) in [1_000, 1_000, 1_600, 1_000, 1_000, 1_600]
            .into_iter()
            .enumerate()
        {
            record_post_burst_window(
                &mut store,
                &format!("ttft-applied-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(80 + index as i64 * 20),
                true,
                true,
                99_600,
                400,
                0,
                ttft_ms,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("ttft-canary-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(90 + index as i64 * 20),
                false,
                true,
                99_500,
                500,
                0,
                1_000,
                1,
            );
        }

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::CanaryHealthy);
        assert_eq!(readiness.reason, "candidate_ttft_p95_not_non_inferior");
        assert_eq!(readiness.candidate_applied.average_ttft_ms, 1_200);
        assert_eq!(readiness.candidate_applied.ttft_p50_ms, 1_000);
        assert_eq!(readiness.candidate_applied.ttft_p95_ms, 1_600);
        assert!(!store.post_burst.rollbacks.contains_key(&comparison_key));
    }

    #[test]
    fn canary_uses_contemporaneous_baseline_instead_of_historical_phase() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut realm_id = String::new();

        for index in 0..3 {
            realm_id = record_post_burst_window(
                &mut store,
                &format!("phase-old-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(index * 10),
                false,
                true,
                86_680,
                13_320,
                0,
                1_480,
                1,
            );
            record_post_burst_window(
                &mut store,
                &format!("phase-shadow-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(40 + index * 10),
                false,
                true,
                86_680,
                13_320,
                0,
                1_900,
                1,
            );
        }

        for (index, candidate_ttft_ms) in [1_446, 1_446, 5_900, 1_446, 1_446, 5_900]
            .into_iter()
            .enumerate()
        {
            record_post_burst_window(
                &mut store,
                &format!("phase-applied-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(80 + index as i64 * 20),
                true,
                true,
                99_520,
                480,
                0,
                candidate_ttft_ms,
                1,
            );
            let comparison_key =
                post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
            let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
            assert_eq!(readiness.status, PostBurstReadinessStatus::CanaryCollecting);
            assert_eq!(readiness.reason, "insufficient_paired_canary_evidence");

            record_post_burst_window(
                &mut store,
                &format!("phase-canary-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(90 + index as i64 * 20),
                false,
                true,
                99_510,
                490,
                0,
                [1_432, 1_432, 3_643, 1_432, 1_432, 3_643][index],
                1,
            );
        }

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::CanaryHealthy);
        assert_eq!(readiness.reason, "candidate_ttft_p95_not_non_inferior");
        assert_eq!(readiness.baseline.cache_ratio_bps, 9_526);
        assert_eq!(readiness.canary_baseline.cache_ratio_bps, 9_954);
        assert_eq!(readiness.candidate_applied.cache_ratio_bps, 9_955);
        assert!(!store.post_burst.rollbacks.contains_key(&comparison_key));
    }

    #[test]
    fn post_burst_ledger_is_backward_compatible_and_bounded() {
        let restored_summary: PostBurstArmSummary = serde_json::from_value(json!({})).unwrap();
        assert_eq!(restored_summary.ttft_p50_ms, 0);
        assert_eq!(restored_summary.ttft_p95_ms, 0);

        let restored: ShadowAffinityStore =
            serde_json::from_value(json!({"assignments": {}})).unwrap();
        assert!(restored.post_burst.evidence.is_empty());
        let restored_window: PostBurstWindow = serde_json::from_value(json!({
            "window_id": 1,
            "conversation_id": "legacy-window",
            "opened_at": Utc::now(),
            "expires_at": Utc::now() + Duration::hours(1),
            "remaining_requests": 2,
            "captured_requests": 1
        }))
        .unwrap();
        assert_eq!(restored_window.lane, ShadowCacheLane::ToolBurstQuarantine);

        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        record_post_burst_window(
            &mut store,
            "bounded-ledger",
            ShadowAffinityArm::Baseline,
            now,
            false,
            true,
            90_000,
            0,
            0,
            1_000,
            1,
        );
        let template = store.post_burst.evidence.front().unwrap().clone();
        store.post_burst.evidence.clear();
        for index in 0..(POST_BURST_EVIDENCE_LIMIT + 5) {
            let mut observation = template.clone();
            observation.observed_at = now + Duration::milliseconds(index as i64);
            store.post_burst.evidence.push_back(observation);
        }
        evict_assignments(&mut store, now + Duration::seconds(2));
        assert_eq!(store.post_burst.evidence.len(), POST_BURST_EVIDENCE_LIMIT);
    }

    #[test]
    fn expired_and_over_limit_assignments_are_evicted() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let stale = identity("stale");
        let stale_decision = compute_shadow_affinity(
            &mut store,
            &stale,
            None,
            now - Duration::hours(SHADOW_ASSIGNMENT_TTL_HOURS + 1),
            0,
            0,
        );
        assert!(stale_decision.assignment_key.is_some());
        evict_assignments(&mut store, now);
        assert!(store.assignments.is_empty());
        for index in 0..(SHADOW_ASSIGNMENT_LIMIT + 3) {
            store.assignments.insert(
                format!("{index}"),
                ShadowAffinityAssignment {
                    conversation_id: format!("{index}"),
                    cohort_id: "cohort".to_string(),
                    realm_id: "realm".to_string(),
                    policy_epoch: SHADOW_POLICY_EPOCH,
                    lane: ShadowCacheLane::Steady,
                    arm: ShadowAffinityArm::Baseline,
                    shard: 0,
                    anchor_epoch: 0,
                    created_at: now,
                    last_seen_at: now + Duration::seconds(index as i64),
                    observations: 0,
                    successful_observations: 0,
                    usage_observations: 0,
                    inconclusive_observations: 0,
                    input_tokens: 0,
                    cache_read_tokens: 0,
                    active_cache_route_state: ActiveCacheRouteState::Baseline,
                    active_cache_route_baseline: ActiveCacheRouteEvidence::default(),
                    active_cache_route_candidate: ActiveCacheRouteEvidence::default(),
                    active_cache_route_reason: None,
                    active_cache_route_legacy_seed_consumed: false,
                    active_cache_route_valid_until: None,
                },
            );
        }
        evict_assignments(&mut store, now + Duration::seconds(10_000));
        assert!(store.assignments.len() <= SHADOW_ASSIGNMENT_LIMIT);
    }

    #[test]
    fn current_scope_expiration_does_not_sweep_unrelated_assignments_on_the_send_path() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let current = identity("current-expired-scope");
        let unrelated = identity("unrelated-expired-scope");
        let current_decision = compute_shadow_affinity(
            &mut store,
            &current,
            None,
            now - Duration::hours(SHADOW_ASSIGNMENT_TTL_HOURS + 1),
            0,
            0,
        );
        let unrelated_decision = compute_shadow_affinity(
            &mut store,
            &unrelated,
            None,
            now - Duration::hours(SHADOW_ASSIGNMENT_TTL_HOURS + 1),
            0,
            0,
        );
        let current_key = current_decision.assignment_key.unwrap();
        let unrelated_key = unrelated_decision.assignment_key.unwrap();
        let stale_current = store.assignments.get_mut(&current_key).unwrap();
        stale_current.lane = ShadowCacheLane::CompactedAnchor;
        stale_current.anchor_epoch = 77;

        let recovered = compute_shadow_affinity(&mut store, &current, None, now, 0, 0);

        assert_eq!(recovered.lane, ShadowCacheLane::Steady);
        assert_eq!(recovered.anchor_epoch, 0);
        assert_eq!(store.assignments[&current_key].anchor_epoch, 0);
        assert!(
            store.assignments.contains_key(&unrelated_key),
            "the request hot path must not scan and evict unrelated scopes"
        );
    }

    #[test]
    fn expired_current_post_burst_window_is_ignored_without_sweeping_other_windows() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let current = identity("current-expired-window");
        let initial = compute_shadow_affinity(&mut store, &current, None, now, 0, 0);
        let current_key = initial.assignment_key.unwrap();
        let expired_at = now - Duration::seconds(1);
        store.post_burst.windows.insert(
            current_key.clone(),
            PostBurstWindow {
                window_id: 1,
                conversation_id: current_key.clone(),
                opened_at: expired_at - Duration::minutes(1),
                expires_at: expired_at,
                remaining_requests: 1,
                captured_requests: 0,
                lane: ShadowCacheLane::CompactedAnchor,
                candidate_variant: ShadowCacheCandidateVariant::CohortKey,
                realm_id: String::new(),
                policy_epoch: 0,
                anchor_epoch: 0,
            },
        );
        store.post_burst.windows.insert(
            "unrelated-expired-window".to_string(),
            PostBurstWindow {
                window_id: 2,
                conversation_id: "unrelated-expired-window".to_string(),
                opened_at: expired_at - Duration::minutes(1),
                expires_at: expired_at,
                remaining_requests: 1,
                captured_requests: 0,
                lane: ShadowCacheLane::ToolBurstQuarantine,
                candidate_variant: ShadowCacheCandidateVariant::CohortKey,
                realm_id: String::new(),
                policy_epoch: 0,
                anchor_epoch: 0,
            },
        );

        let recovered = compute_shadow_affinity(&mut store, &current, None, now, 0, 0);

        assert_eq!(recovered.lane, ShadowCacheLane::Steady);
        assert!(!store.post_burst.windows.contains_key(&current_key));
        assert!(
            store
                .post_burst
                .windows
                .contains_key("unrelated-expired-window"),
            "the request hot path must only clean the current conversation window"
        );
    }

    #[test]
    fn key_realm_change_resets_assignment_and_its_open_post_burst_window() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let initial_identity = identity("realm-change-thread");
        let initial = compute_shadow_affinity(&mut store, &initial_identity, None, now, 0, 0);
        let assignment_key = initial.assignment_key.unwrap();
        let assignment = store.assignments.get_mut(&assignment_key).unwrap();
        assignment.lane = ShadowCacheLane::CompactedAnchor;
        assignment.anchor_epoch = 9;
        store.post_burst.windows.insert(
            assignment_key.clone(),
            PostBurstWindow {
                window_id: 1,
                conversation_id: assignment_key.clone(),
                opened_at: now,
                expires_at: now + Duration::hours(POST_BURST_WINDOW_TTL_HOURS),
                remaining_requests: 1,
                captured_requests: 0,
                lane: ShadowCacheLane::CompactedAnchor,
                candidate_variant: ShadowCacheCandidateVariant::CohortKey,
                realm_id: initial_identity.realm_id.clone(),
                policy_epoch: SHADOW_POLICY_EPOCH,
                anchor_epoch: 9,
            },
        );
        let mut different_key_realm = initial_identity.clone();
        different_key_realm.realm_id = "different-selected-key-realm".to_string();
        different_key_realm.cohort_id = "different-selected-key-cohort".to_string();

        let recovered = compute_shadow_affinity(
            &mut store,
            &different_key_realm,
            None,
            now + Duration::seconds(1),
            0,
            0,
        );

        assert_eq!(recovered.lane, ShadowCacheLane::Steady);
        assert_eq!(recovered.anchor_epoch, 0);
        assert_eq!(
            store.assignments[&assignment_key].realm_id,
            "different-selected-key-realm"
        );
        assert!(!store.post_burst.windows.contains_key(&assignment_key));
    }

    #[test]
    fn capacity_eviction_removes_the_displaced_window_and_keeps_the_current_scope() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let oldest_key = format!("capacity-{}", SHADOW_ASSIGNMENT_LIMIT - 1);
        for index in 0..SHADOW_ASSIGNMENT_LIMIT {
            let conversation_id = format!("capacity-{index}");
            store.assignments.insert(
                conversation_id.clone(),
                ShadowAffinityAssignment {
                    conversation_id,
                    cohort_id: "cohort".to_string(),
                    realm_id: "realm".to_string(),
                    policy_epoch: SHADOW_POLICY_EPOCH,
                    lane: ShadowCacheLane::Steady,
                    arm: ShadowAffinityArm::Baseline,
                    shard: 0,
                    anchor_epoch: 0,
                    created_at: now,
                    last_seen_at: now - Duration::seconds(index as i64 + 1),
                    observations: 0,
                    successful_observations: 0,
                    usage_observations: 0,
                    inconclusive_observations: 0,
                    input_tokens: 0,
                    cache_read_tokens: 0,
                    active_cache_route_state: ActiveCacheRouteState::Baseline,
                    active_cache_route_baseline: ActiveCacheRouteEvidence::default(),
                    active_cache_route_candidate: ActiveCacheRouteEvidence::default(),
                    active_cache_route_reason: None,
                    active_cache_route_legacy_seed_consumed: false,
                    active_cache_route_valid_until: None,
                },
            );
        }
        store.post_burst.windows.insert(
            oldest_key.clone(),
            PostBurstWindow {
                window_id: 1,
                conversation_id: oldest_key.clone(),
                opened_at: now - Duration::minutes(1),
                expires_at: now + Duration::hours(POST_BURST_WINDOW_TTL_HOURS),
                remaining_requests: 1,
                captured_requests: 0,
                lane: ShadowCacheLane::ToolBurstQuarantine,
                candidate_variant: ShadowCacheCandidateVariant::CohortKey,
                realm_id: "realm".to_string(),
                policy_epoch: SHADOW_POLICY_EPOCH,
                anchor_epoch: 0,
            },
        );
        prepare_shadow_affinity_store(&mut store);

        let current =
            compute_shadow_affinity(&mut store, &identity("capacity-current"), None, now, 0, 0);
        let current_key = current.assignment_key.unwrap();

        assert!(store.assignments.contains_key(&current_key));
        assert!(store.assignments.len() <= SHADOW_ASSIGNMENT_LIMIT);
        assert!(!store.assignments.contains_key(&oldest_key));
        assert!(!store.post_burst.windows.contains_key(&oldest_key));
    }

    #[test]
    fn settlement_maintenance_prunes_expired_assignments_and_windows_with_a_bounded_index() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let conversation_id = "expired-maintenance-scope".to_string();
        store.assignments.insert(
            conversation_id.clone(),
            ShadowAffinityAssignment {
                conversation_id: conversation_id.clone(),
                cohort_id: "cohort".to_string(),
                realm_id: "realm".to_string(),
                policy_epoch: SHADOW_POLICY_EPOCH,
                lane: ShadowCacheLane::Steady,
                arm: ShadowAffinityArm::Baseline,
                shard: 0,
                anchor_epoch: 0,
                created_at: now - Duration::hours(SHADOW_ASSIGNMENT_TTL_HOURS + 1),
                last_seen_at: now - Duration::hours(SHADOW_ASSIGNMENT_TTL_HOURS + 1),
                observations: 0,
                successful_observations: 0,
                usage_observations: 0,
                inconclusive_observations: 0,
                input_tokens: 0,
                cache_read_tokens: 0,
                active_cache_route_state: ActiveCacheRouteState::Baseline,
                active_cache_route_baseline: ActiveCacheRouteEvidence::default(),
                active_cache_route_candidate: ActiveCacheRouteEvidence::default(),
                active_cache_route_reason: None,
                active_cache_route_legacy_seed_consumed: false,
                active_cache_route_valid_until: None,
            },
        );
        store.post_burst.windows.insert(
            conversation_id.clone(),
            PostBurstWindow {
                window_id: 1,
                conversation_id: conversation_id.clone(),
                opened_at: now - Duration::hours(POST_BURST_WINDOW_TTL_HOURS + 1),
                expires_at: now - Duration::seconds(1),
                remaining_requests: 1,
                captured_requests: 0,
                lane: ShadowCacheLane::Steady,
                candidate_variant: ShadowCacheCandidateVariant::CohortKey,
                realm_id: "realm".to_string(),
                policy_epoch: SHADOW_POLICY_EPOCH,
                anchor_epoch: 0,
            },
        );
        rebuild_assignment_age_index(&mut store);
        rebuild_post_burst_window_age_index(&mut store.post_burst);

        maintain_shadow_affinity_after_settlement(&mut store, now);

        assert!(store.assignments.is_empty());
        assert!(store.post_burst.windows.is_empty());
    }

    #[test]
    fn evidence_scope_index_tracks_push_and_capacity_pop_without_double_counting() {
        let mut ledger = PostBurstEvidenceLedger::default();
        let now = Utc::now();
        let first_key = append_scope_evidence(&mut ledger, "first-realm", now);
        let second_key = append_scope_evidence(&mut ledger, "second-realm", now);
        assert_eq!(ledger.evidence_scope_indexed_len, ledger.evidence.len());
        assert!(ledger.evidence_scope_latest_expiry.contains_key(&first_key));
        assert!(ledger
            .evidence_scope_latest_expiry
            .contains_key(&second_key));

        let removed = ledger.evidence.pop_front().unwrap();
        remove_evidence_scope(&mut ledger, &removed);

        assert_eq!(ledger.evidence_scope_indexed_len, 1);
        assert!(!ledger.evidence_scope_latest_expiry.contains_key(&first_key));
        assert!(ledger
            .evidence_scope_latest_expiry
            .contains_key(&second_key));
    }

    #[test]
    fn evidence_scope_incremental_eviction_preserves_the_newest_expiry() {
        let mut ledger = PostBurstEvidenceLedger::default();
        let now = Utc::now();
        let comparison_key = append_scope_evidence(&mut ledger, "same-realm", now);
        append_scope_evidence(&mut ledger, "same-realm", now + Duration::seconds(1));

        let removed = ledger.evidence.pop_front().unwrap();
        remove_evidence_scope(&mut ledger, &removed);

        assert_eq!(ledger.evidence_scope_indexed_len, ledger.evidence.len());
        assert_eq!(
            ledger.evidence_scope_latest_expiry[&comparison_key],
            now + Duration::seconds(1) + Duration::hours(POST_BURST_EVIDENCE_TTL_HOURS)
        );
        assert_eq!(ledger.evidence_scope_records[&comparison_key].len(), 1);
        assert_eq!(
            ledger.evidence_scope_records[&comparison_key]
                .front()
                .unwrap()
                .observed_at,
            now + Duration::seconds(1)
        );
    }

    #[test]
    fn legacy_store_window_and_readiness_fields_fail_closed_after_restore() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let identity = identity("legacy-store-thread");
        let initial = compute_shadow_affinity(&mut store, &identity, None, now, 0, 0);
        let assignment_key = initial.assignment_key.unwrap();
        store.post_burst.windows.insert(
            assignment_key.clone(),
            PostBurstWindow {
                window_id: 1,
                conversation_id: assignment_key.clone(),
                opened_at: now,
                expires_at: now + Duration::hours(POST_BURST_WINDOW_TTL_HOURS),
                remaining_requests: 1,
                captured_requests: 0,
                lane: ShadowCacheLane::ToolBurstQuarantine,
                candidate_variant: ShadowCacheCandidateVariant::CohortTwoShard,
                realm_id: identity.realm_id.clone(),
                policy_epoch: SHADOW_POLICY_EPOCH,
                anchor_epoch: 0,
            },
        );
        append_scope_evidence(&mut store.post_burst, &identity.realm_id, now);
        current_post_burst_readiness(
            &mut store.post_burst,
            &identity.realm_id,
            ShadowCacheLane::Steady,
            ShadowCacheCandidateVariant::CohortKey,
            now,
        );
        let mut legacy = serde_json::to_value(&store).unwrap();
        let post_burst = legacy
            .pointer_mut("/post_burst")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap();
        post_burst.remove("evidence_generations");
        for window in post_burst
            .get_mut("windows")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .values_mut()
        {
            let window = window.as_object_mut().unwrap();
            window.remove("realm_id");
            window.remove("policy_epoch");
            window.remove("anchor_epoch");
        }
        for readiness in post_burst
            .get_mut("readiness")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap()
            .values_mut()
        {
            let readiness = readiness.as_object_mut().unwrap();
            readiness.remove("evidence_generation");
            readiness.remove("valid_until");
        }
        let mut restored: ShadowAffinityStore = serde_json::from_value(legacy).unwrap();
        prepare_shadow_affinity_store(&mut restored);

        let recovered = compute_shadow_affinity(
            &mut restored,
            &identity,
            None,
            now + Duration::seconds(1),
            0,
            0,
        );

        assert_eq!(recovered.lane, ShadowCacheLane::Steady);
        assert_eq!(
            recovered.candidate_variant,
            ShadowCacheCandidateVariant::CohortKey
        );
        assert!(!restored.post_burst.windows.contains_key(&assignment_key));
    }

    #[test]
    fn no_evidence_readiness_is_transient_and_does_not_grow_the_ledger() {
        let mut ledger = PostBurstEvidenceLedger::default();
        let now = Utc::now();
        for index in 0..512 {
            let readiness = current_post_burst_readiness(
                &mut ledger,
                &format!("empty-realm-{index}"),
                ShadowCacheLane::Steady,
                ShadowCacheCandidateVariant::CohortKey,
                now,
            );
            assert_eq!(
                readiness.status,
                PostBurstReadinessStatus::InsufficientEvidence
            );
        }
        assert!(ledger.readiness.is_empty());
        assert!(ledger.evidence_generations.is_empty());
    }

    #[test]
    fn readiness_cache_is_reused_only_until_the_evidence_changes_or_expires() {
        let mut ledger = PostBurstEvidenceLedger::default();
        let now = Utc::now();
        let comparison_key = append_scope_evidence(&mut ledger, "realm", now);
        let first = current_post_burst_readiness(
            &mut ledger,
            "realm",
            ShadowCacheLane::Steady,
            ShadowCacheCandidateVariant::CohortKey,
            now,
        );
        let reused = current_post_burst_readiness(
            &mut ledger,
            "realm",
            ShadowCacheLane::Steady,
            ShadowCacheCandidateVariant::CohortKey,
            now + Duration::seconds(1),
        );
        assert_eq!(reused.updated_at, first.updated_at);

        let unrelated_comparison_key =
            append_scope_evidence(&mut ledger, "unrelated-realm", now + Duration::seconds(2));
        bump_evidence_generation(&mut ledger, &unrelated_comparison_key);
        let still_reused = current_post_burst_readiness(
            &mut ledger,
            "realm",
            ShadowCacheLane::Steady,
            ShadowCacheCandidateVariant::CohortKey,
            now + Duration::seconds(2),
        );
        assert_eq!(still_reused.updated_at, first.updated_at);

        bump_evidence_generation(&mut ledger, &comparison_key);
        let after_evidence_change = current_post_burst_readiness(
            &mut ledger,
            "realm",
            ShadowCacheLane::Steady,
            ShadowCacheCandidateVariant::CohortKey,
            now + Duration::seconds(3),
        );
        assert_eq!(after_evidence_change.updated_at, now + Duration::seconds(3));
        assert_eq!(
            after_evidence_change.evidence_generation,
            evidence_generation(&ledger, &comparison_key)
        );

        let after_expiry = current_post_burst_readiness(
            &mut ledger,
            "realm",
            ShadowCacheLane::Steady,
            ShadowCacheCandidateVariant::CohortKey,
            now + Duration::hours(POST_BURST_READINESS_MAX_AGE_HOURS) + Duration::seconds(4),
        );
        assert_eq!(
            after_expiry.updated_at,
            now + Duration::hours(POST_BURST_READINESS_MAX_AGE_HOURS) + Duration::seconds(4)
        );
    }

    #[test]
    #[ignore = "manual FastRelayCore full-capacity affinity hot-path baseline"]
    fn fastrelay_full_capacity_affinity_hot_path_baseline() {
        use std::hint::black_box;

        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        for index in 0..SHADOW_ASSIGNMENT_LIMIT {
            let seed = identity(&format!("capacity-seed-{index}"));
            black_box(compute_shadow_affinity(
                &mut store,
                &seed,
                None,
                now + Duration::microseconds(index as i64),
                0,
                0,
            ));
        }
        assert_eq!(store.assignments.len(), SHADOW_ASSIGNMENT_LIMIT);

        let mut samples = Vec::new();
        for index in 0..21 {
            let incoming = identity(&format!("capacity-incoming-{index}"));
            let started = Instant::now();
            black_box(compute_shadow_affinity(
                &mut store,
                &incoming,
                None,
                now + Duration::seconds(index as i64 + 1),
                0,
                0,
            ));
            samples.push(started.elapsed().as_micros());
            assert_eq!(store.assignments.len(), SHADOW_ASSIGNMENT_LIMIT);
        }
        samples.sort_unstable();
        let p95_index = ((samples.len() - 1) * 95).div_ceil(100);
        let p95_us = samples[p95_index];
        println!(
            "fastrelay_affinity_capacity assignments={SHADOW_ASSIGNMENT_LIMIT} p95_us={p95_us} samples_us={samples:?}"
        );
        assert!(
            p95_us <= 5_000,
            "full-capacity affinity p95 ({p95_us}us) exceeded the 5ms hot-path budget"
        );
    }

    #[test]
    #[ignore = "manual FastRelayCore full-capacity readiness refresh baseline"]
    fn fastrelay_full_capacity_readiness_refresh_baseline() {
        use std::hint::black_box;

        let mut ledger = PostBurstEvidenceLedger::default();
        let now = Utc::now();
        let mut comparison_key = String::new();
        for index in 0..POST_BURST_EVIDENCE_LIMIT {
            comparison_key = append_scope_evidence(
                &mut ledger,
                "full-capacity-realm",
                now + Duration::microseconds(index as i64),
            );
        }
        assert_eq!(ledger.evidence.len(), POST_BURST_EVIDENCE_LIMIT);
        assert_eq!(ledger.evidence_scope_indexed_len, POST_BURST_EVIDENCE_LIMIT);

        let mut samples = Vec::new();
        for index in 0..21 {
            bump_evidence_generation(&mut ledger, &comparison_key);
            let started = Instant::now();
            black_box(current_post_burst_readiness(
                &mut ledger,
                "full-capacity-realm",
                ShadowCacheLane::Steady,
                ShadowCacheCandidateVariant::CohortKey,
                now + Duration::seconds(index as i64 + 1),
            ));
            samples.push(started.elapsed().as_micros());
        }
        samples.sort_unstable();
        let p95_index = ((samples.len() - 1) * 95).div_ceil(100);
        let p95_us = samples[p95_index];
        println!(
            "fastrelay_readiness_refresh evidence={POST_BURST_EVIDENCE_LIMIT} p95_us={p95_us} samples_us={samples:?}"
        );
        assert!(
            p95_us <= 5_000,
            "full-capacity readiness refresh p95 ({p95_us}us) exceeded the 5ms bounded-settlement budget"
        );
    }

    #[test]
    fn persisted_readiness_without_hot_path_fields_remains_backward_compatible() {
        let mut ledger = PostBurstEvidenceLedger::default();
        let now = Utc::now();
        append_scope_evidence(&mut ledger, "realm", now);
        let readiness = current_post_burst_readiness(
            &mut ledger,
            "realm",
            ShadowCacheLane::Steady,
            ShadowCacheCandidateVariant::CohortKey,
            now,
        );
        let mut legacy = serde_json::to_value(readiness).unwrap();
        let legacy = legacy.as_object_mut().unwrap();
        legacy.remove("evidence_generation");
        legacy.remove("valid_until");

        let restored: PostBurstReadiness =
            serde_json::from_value(serde_json::Value::Object(legacy.clone())).unwrap();

        assert_eq!(restored.evidence_generation, 0);
        assert_eq!(restored.valid_until, None);
    }

    #[test]
    fn active_cache_route_fields_default_for_pre_route_runtime_state() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let decision =
            compute_shadow_affinity(&mut store, &identity("legacy-route"), None, now, 0, 0);
        let assignment = store.assignments[decision.assignment_key.as_deref().unwrap()].clone();
        let mut legacy = serde_json::to_value(assignment).unwrap();
        let legacy = legacy.as_object_mut().unwrap();
        legacy.remove("active_cache_route_state");
        legacy.remove("active_cache_route_baseline");
        legacy.remove("active_cache_route_candidate");
        legacy.remove("active_cache_route_reason");
        legacy.remove("active_cache_route_legacy_seed_consumed");
        legacy.remove("active_cache_route_valid_until");

        let restored: ShadowAffinityAssignment =
            serde_json::from_value(serde_json::Value::Object(legacy.clone())).unwrap();
        assert_eq!(
            restored.active_cache_route_state,
            ActiveCacheRouteState::Baseline
        );
        assert_eq!(restored.active_cache_route_baseline.observations, 0);
        assert_eq!(restored.active_cache_route_candidate.observations, 0);
        assert_eq!(restored.active_cache_route_reason, None);
        assert!(!restored.active_cache_route_legacy_seed_consumed);
        assert_eq!(restored.active_cache_route_valid_until, None);
    }

    #[test]
    fn active_cache_route_restore_keeps_one_owner_per_realm() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let first =
            compute_shadow_affinity(&mut store, &identity("restore-first"), None, now, 0, 0);
        let second = compute_shadow_affinity(
            &mut store,
            &identity("restore-second"),
            None,
            now + Duration::seconds(1),
            0,
            0,
        );
        let first_key = first.assignment_key.unwrap();
        let second_key = second.assignment_key.unwrap();
        store
            .assignments
            .get_mut(&first_key)
            .unwrap()
            .active_cache_route_state = ActiveCacheRouteState::Candidate;
        store
            .assignments
            .get_mut(&first_key)
            .unwrap()
            .active_cache_route_valid_until = Some(now + Duration::hours(1));
        store
            .assignments
            .get_mut(&second_key)
            .unwrap()
            .active_cache_route_state = ActiveCacheRouteState::Promoted;
        store
            .assignments
            .get_mut(&second_key)
            .unwrap()
            .active_cache_route_valid_until = Some(now + Duration::hours(1));
        store.active_cache_route_owners.clear();

        prepare_shadow_affinity_store(&mut store);

        assert_eq!(store.active_cache_route_owners.len(), 1);
        assert_eq!(
            store.active_cache_route_owners.values().next(),
            Some(&second_key)
        );
        assert_eq!(
            store.assignments[&first_key].active_cache_route_state,
            ActiveCacheRouteState::Baseline
        );
        assert_eq!(
            store.assignments[&first_key]
                .active_cache_route_reason
                .as_deref(),
            Some("active_cache_route_owner_conflict_after_restore")
        );
        assert_eq!(
            store.assignments[&second_key].active_cache_route_state,
            ActiveCacheRouteState::Promoted
        );
    }

    #[test]
    fn active_cache_route_resets_on_stable_prefix_scope_change() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut decision = active_cache_route_baseline(&mut store, "cohort-change", now);
        assert!(admit_active_cache_route(
            &mut store,
            &mut decision,
            true,
            true,
            now,
        ));
        observe_shadow_affinity(
            &mut store,
            &decision,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 96_000,
                status: 200,
                ttft_ms: 450,
                attempt_count: 1,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(1),
        );

        let mut changed_identity = identity("cohort-change");
        changed_identity.cohort_id = "different-stable-prefix".to_string();
        let reset = compute_shadow_affinity(
            &mut store,
            &changed_identity,
            None,
            now + Duration::seconds(2),
            0,
            0,
        );
        let assignment_key = reset.assignment_key.as_deref().unwrap();
        let assignment = &store.assignments[assignment_key];
        assert_eq!(assignment.cohort_id, changed_identity.cohort_id);
        assert_eq!(
            assignment.active_cache_route_state,
            ActiveCacheRouteState::Baseline
        );
        assert_eq!(assignment.active_cache_route_baseline.observations, 0);
        assert_eq!(assignment.active_cache_route_candidate.observations, 0);
        assert!(assignment.active_cache_route_legacy_seed_consumed);
        assert!(store.active_cache_route_owners.is_empty());

        let mut no_reuse = reset.clone();
        assert!(!admit_active_cache_route(
            &mut store,
            &mut no_reuse,
            true,
            true,
            now + Duration::seconds(2),
        ));
        assert_eq!(
            no_reuse.skip_reason.as_deref(),
            Some("active_cache_route_baseline_insufficient")
        );
    }

    #[test]
    fn active_cache_route_compaction_never_reseeds_old_epoch_usage() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let decision = active_cache_route_baseline(&mut store, "compaction-seed", now);
        let assignment_key = decision.assignment_key.clone().unwrap();
        let old_observations = store.assignments[&assignment_key].observations;

        reset_anchor(&mut store, &assignment_key, now + Duration::seconds(1));
        remove_post_burst_window(&mut store.post_burst, &assignment_key);
        store.assignments.get_mut(&assignment_key).unwrap().lane = ShadowCacheLane::Steady;
        let mut post_compaction = compute_shadow_affinity(
            &mut store,
            &identity("compaction-seed"),
            None,
            now + Duration::seconds(2),
            0,
            0,
        );
        assert_eq!(
            store.assignments[&assignment_key].observations,
            old_observations
        );
        assert_eq!(
            store.assignments[&assignment_key]
                .active_cache_route_baseline
                .observations,
            0
        );
        assert!(store.assignments[&assignment_key].active_cache_route_legacy_seed_consumed);
        assert!(!admit_active_cache_route(
            &mut store,
            &mut post_compaction,
            true,
            true,
            now + Duration::seconds(2),
        ));
        assert_eq!(
            post_compaction.skip_reason.as_deref(),
            Some("active_cache_route_baseline_insufficient")
        );
    }

    #[test]
    fn active_cache_route_compares_raw_cache_ratios_without_bps_rounding() {
        assert!(active_cache_route_ratio_is_strictly_higher(
            950_001, 1_000_000, 950_000, 1_000_000
        ));
        assert!(active_cache_route_ratio_is_strictly_lower(
            950_000, 1_000_000, 950_001, 1_000_000
        ));
        assert!(!active_cache_route_ratio_is_strictly_higher(
            950_000, 1_000_000, 950_000, 1_000_000
        ));
    }

    #[test]
    fn active_cache_route_lease_expires_instead_of_becoming_a_permanent_rollout() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut decision = active_cache_route_baseline(&mut store, "lease-expiry", now);
        assert!(admit_active_cache_route(
            &mut store,
            &mut decision,
            true,
            true,
            now,
        ));
        let mut expired = decision.clone();
        assert!(!admit_active_cache_route(
            &mut store,
            &mut expired,
            true,
            true,
            now + Duration::hours(ACTIVE_CACHE_ROUTE_LEASE_HOURS) + Duration::seconds(1),
        ));
        let assignment = &store.assignments[decision.assignment_key.as_deref().unwrap()];
        assert_eq!(
            assignment.active_cache_route_state,
            ActiveCacheRouteState::Baseline
        );
        assert_eq!(
            assignment.active_cache_route_reason.as_deref(),
            Some("active_cache_route_lease_expired")
        );
        assert!(store.active_cache_route_owners.is_empty());
    }

    #[test]
    fn unverified_prompt_cache_key_requires_real_cache_usage_before_live_routing() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut decision = active_cache_route_baseline(&mut store, "unverified-key", now);
        let assignment_key = decision.assignment_key.as_deref().unwrap();
        store
            .assignments
            .get_mut(assignment_key)
            .unwrap()
            .active_cache_route_baseline
            .cache_read_tokens = 0;
        assert!(!admit_active_cache_route(
            &mut store,
            &mut decision,
            true,
            false,
            now,
        ));
        assert_eq!(
            decision.skip_reason.as_deref(),
            Some("active_cache_route_prompt_cache_key_unverified")
        );
    }

    fn active_cache_route_baseline(
        store: &mut ShadowAffinityStore,
        thread: &str,
        now: DateTime<Utc>,
    ) -> ShadowAffinityDecision {
        let identity = identity(thread);
        for index in 0..4 {
            let decision = compute_shadow_affinity(
                store,
                &identity,
                None,
                now + Duration::seconds(index),
                0,
                0,
            );
            observe_shadow_affinity(
                store,
                &decision,
                ShadowObservationInput {
                    success: true,
                    has_usage: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 95_000,
                    status: 200,
                    ttft_ms: 500,
                    attempt_count: 1,
                    ..ShadowObservationInput::default()
                },
                now + Duration::seconds(index),
            );
        }
        compute_shadow_affinity(store, &identity, None, now + Duration::seconds(5), 0, 0)
    }

    #[test]
    fn active_cache_route_admits_an_existing_live_scope_without_new_conversations() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut decision = active_cache_route_baseline(&mut store, "active-route", now);

        assert!(admit_active_cache_route(
            &mut store,
            &mut decision,
            true,
            true,
            now,
        ));
        assert_eq!(decision.mode, "applied");
        assert_eq!(decision.decision, "active_cache_candidate_applied");
        assert_eq!(decision.arm, ShadowAffinityArm::Candidate);
        assert!(static_cohort_prompt_cache_key(&decision).is_some());
    }

    #[test]
    fn active_cache_route_limits_live_exposure_to_one_scope_per_realm() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut first = active_cache_route_baseline(&mut store, "single-scope-first", now);
        let mut second = active_cache_route_baseline(
            &mut store,
            "single-scope-second",
            now + Duration::minutes(1),
        );

        assert!(admit_active_cache_route(
            &mut store,
            &mut first,
            true,
            true,
            now + Duration::minutes(1),
        ));
        assert!(!admit_active_cache_route(
            &mut store,
            &mut second,
            true,
            true,
            now + Duration::minutes(1),
        ));
        assert_eq!(
            second.skip_reason.as_deref(),
            Some("active_cache_route_realm_busy")
        );
        assert_eq!(store.active_cache_route_owners.len(), 1);
    }

    #[test]
    fn active_cache_route_immediately_rolls_back_a_raw_cache_regression() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut decision = active_cache_route_baseline(&mut store, "active-route-rollback", now);
        assert!(admit_active_cache_route(
            &mut store,
            &mut decision,
            true,
            true,
            now,
        ));

        observe_shadow_affinity(
            &mut store,
            &decision,
            ShadowObservationInput {
                success: true,
                has_usage: true,
                input_tokens: 100_000,
                cache_read_tokens: 94_000,
                status: 200,
                ttft_ms: 500,
                attempt_count: 1,
                ..ShadowObservationInput::default()
            },
            now + Duration::seconds(6),
        );

        let assignment_key = decision.assignment_key.as_deref().unwrap();
        assert_eq!(
            store.assignments[assignment_key].active_cache_route_state,
            ActiveCacheRouteState::RolledBack
        );
        let mut next = compute_shadow_affinity(
            &mut store,
            &identity("active-route-rollback"),
            None,
            now + Duration::seconds(7),
            0,
            0,
        );
        assert!(!admit_active_cache_route(
            &mut store,
            &mut next,
            true,
            true,
            now + Duration::seconds(7),
        ));
        assert_eq!(
            next.skip_reason.as_deref(),
            Some("active_cache_route_rolled_back")
        );
    }

    #[test]
    fn active_cache_route_immediately_rolls_back_errors_ttft_and_attempt_regressions() {
        let now = Utc::now();
        for (suffix, input, expected_reason) in [
            (
                "error",
                ShadowObservationInput {
                    success: false,
                    has_usage: false,
                    status: 502,
                    ttft_ms: 500,
                    attempt_count: 1,
                    ..ShadowObservationInput::default()
                },
                "active_cache_route_upstream_error",
            ),
            (
                "ttft",
                ShadowObservationInput {
                    success: true,
                    has_usage: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 96_000,
                    status: 200,
                    ttft_ms: 1_100,
                    attempt_count: 1,
                    ..ShadowObservationInput::default()
                },
                "active_cache_route_ttft_regression",
            ),
            (
                "attempt",
                ShadowObservationInput {
                    success: true,
                    has_usage: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 96_000,
                    status: 200,
                    ttft_ms: 500,
                    attempt_count: 2,
                    ..ShadowObservationInput::default()
                },
                "active_cache_route_attempt_regression",
            ),
        ] {
            let mut store = ShadowAffinityStore::default();
            let thread = format!("active-route-{suffix}");
            let mut decision = active_cache_route_baseline(&mut store, &thread, now);
            assert!(admit_active_cache_route(
                &mut store,
                &mut decision,
                true,
                true,
                now,
            ));
            observe_shadow_affinity(&mut store, &decision, input, now + Duration::seconds(1));
            let assignment = store.assignments[decision.assignment_key.as_deref().unwrap()].clone();
            assert_eq!(
                assignment.active_cache_route_state,
                ActiveCacheRouteState::RolledBack
            );
            assert_eq!(
                assignment.active_cache_route_reason.as_deref(),
                Some(expected_reason)
            );
        }
    }

    #[test]
    fn active_cache_route_promotes_only_after_eighteen_strictly_positive_samples() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut decision = active_cache_route_baseline(&mut store, "active-route-promote", now);
        assert!(admit_active_cache_route(
            &mut store,
            &mut decision,
            true,
            true,
            now,
        ));

        for index in 0..18 {
            let at = now + Duration::seconds(6 + index);
            let mut current = if index == 0 {
                decision.clone()
            } else {
                compute_shadow_affinity(
                    &mut store,
                    &identity("active-route-promote"),
                    None,
                    at,
                    0,
                    0,
                )
            };
            if index > 0 {
                assert!(admit_active_cache_route(
                    &mut store,
                    &mut current,
                    true,
                    true,
                    at,
                ));
            }
            observe_shadow_affinity(
                &mut store,
                &current,
                ShadowObservationInput {
                    success: true,
                    has_usage: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 96_000,
                    status: 200,
                    ttft_ms: 450,
                    attempt_count: 1,
                    ..ShadowObservationInput::default()
                },
                at,
            );
        }

        let assignment_key = decision.assignment_key.as_deref().unwrap();
        assert_eq!(
            store.assignments[assignment_key].active_cache_route_state,
            ActiveCacheRouteState::Promoted
        );
    }
}
