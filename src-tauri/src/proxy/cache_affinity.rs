use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use super::affinity_identity::AffinityIdentity;

pub(crate) const SHADOW_POLICY_EPOCH: u64 = 1;
pub(super) const SHADOW_ASSIGNMENT_LIMIT: usize = 4096;
pub(super) const SHADOW_ASSIGNMENT_TTL_HOURS: i64 = 24;
pub(super) const STATIC_COHORT_CANARY_PERCENT: u8 = 5;
#[cfg(not(test))]
const AUTOMATIC_STATIC_COHORT_ADMISSION_ENV: &str = "ATOAPI_AUTOMATIC_CACHE_CANARY";
const GIANT_TAIL_CHARS: u64 = 80_000;
const POST_BURST_FOLLOWUP_REQUESTS: u8 = 3;
const POST_COMPACTION_FOLLOWUP_REQUESTS: u8 = 4;
const POST_BURST_WINDOW_TTL_HOURS: i64 = 4;
const POST_BURST_EVIDENCE_TTL_HOURS: i64 = 24;
const POST_BURST_WINDOW_LIMIT: usize = 512;
const POST_BURST_EVIDENCE_LIMIT: usize = 1536;
const POST_BURST_MIN_ARM_OBSERVATIONS: u64 = 9;
const POST_BURST_MIN_CANARY_OBSERVATIONS: u64 = 3;
const POST_BURST_MIN_PROMOTION_OBSERVATIONS: u64 = 9;
const POST_BURST_MIN_USAGE_COVERAGE_BPS: u64 = 8_000;
const POST_BURST_MAX_PROVIDER_UNSTABLE_BPS: u64 = 2_500;
const POST_BURST_MAX_INPUT_IMBALANCE_BPS: u64 = 5_000;
const POST_BURST_ROLLBACK_SUCCESS_DELTA_BPS: u64 = 1_000;
const POST_BURST_ROLLBACK_CACHE_DELTA_BPS: u64 = 500;
const POST_BURST_ROLLBACK_AVOIDABLE_DELTA: u64 = 2_048;
const POST_BURST_ROLLBACK_TTFT_DELTA_MS: u64 = 1_000;
const POST_BURST_PROMOTION_CACHE_GAIN_BPS: u64 = 50;
const POST_BURST_PROMOTION_MAX_ERROR_DELTA_BPS: u64 = 50;
const POST_BURST_PROMOTION_TTFT_P50_DELTA_MS: u64 = 200;
const POST_BURST_PROMOTION_TTFT_P95_DELTA_MS: u64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ShadowCacheLane {
    Steady,
    ToolBurstQuarantine,
    CompactedAnchor,
    Transparent,
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
}

fn default_post_burst_window_lane() -> ShadowCacheLane {
    ShadowCacheLane::ToolBurstQuarantine
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PostBurstEvidence {
    pub(crate) window_id: u64,
    pub(crate) conversation_id: String,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) followup_index: u8,
    pub(crate) realm_id: String,
    pub(crate) lane: ShadowCacheLane,
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
    pub(crate) status: PostBurstReadinessStatus,
    pub(crate) reason: String,
    pub(crate) baseline: PostBurstArmSummary,
    pub(crate) candidate_shadow: PostBurstArmSummary,
    #[serde(default)]
    pub(crate) canary_baseline: PostBurstArmSummary,
    pub(crate) candidate_applied: PostBurstArmSummary,
    pub(crate) updated_at: DateTime<Utc>,
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ShadowAffinityDecision {
    pub mode: String,
    pub assignment_key: Option<String>,
    pub realm_id: String,
    pub cohort_id: String,
    pub lane: ShadowCacheLane,
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

fn post_burst_comparison_key(realm_id: &str, lane: ShadowCacheLane) -> String {
    format!("{realm_id}:{}", shadow_cache_lane_key(lane))
}

fn evict_post_burst_evidence(ledger: &mut PostBurstEvidenceLedger, now: DateTime<Utc>) {
    ledger
        .windows
        .retain(|_, window| window.expires_at >= now && window.remaining_requests > 0);
    while ledger.windows.len() > POST_BURST_WINDOW_LIMIT {
        let oldest = ledger
            .windows
            .iter()
            .min_by_key(|(_, window)| window.opened_at)
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            ledger.windows.remove(&oldest);
        } else {
            break;
        }
    }

    let cutoff = now - Duration::hours(POST_BURST_EVIDENCE_TTL_HOURS);
    ledger
        .evidence
        .retain(|observation| observation.observed_at >= cutoff);
    while ledger.evidence.len() > POST_BURST_EVIDENCE_LIMIT {
        ledger.evidence.pop_front();
    }
    ledger.readiness.retain(|key, _| {
        ledger.evidence.iter().any(|observation| {
            post_burst_comparison_key(&observation.realm_id, observation.lane) == *key
        })
    });
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
    summary.cache_ratio_bps = ratio_bps(cache_read_tokens, input_tokens);
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
    if candidate.cache_ratio_bps < baseline.cache_ratio_bps {
        return Some("candidate_cache_not_non_inferior");
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

    let cache_gain = candidate.cache_ratio_bps
        >= baseline
            .cache_ratio_bps
            .saturating_add(POST_BURST_PROMOTION_CACHE_GAIN_BPS);
    let provider_unstable_gain = baseline.provider_unstable_ratio_bps > 0
        && candidate.provider_unstable_ratio_bps.saturating_mul(5)
            <= baseline.provider_unstable_ratio_bps.saturating_mul(4);
    if !cache_gain && !provider_unstable_gain {
        return Some("canary_no_efficacy_gain");
    }
    None
}

fn post_burst_has_addressable_gap(
    baseline: &PostBurstArmSummary,
    candidate: &PostBurstArmSummary,
) -> bool {
    baseline.average_avoidable_gap_tokens > 0
        || candidate.average_avoidable_gap_tokens > 0
        || baseline.provider_unstable_ratio_bps > 0
        || candidate.provider_unstable_ratio_bps > 0
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
    now: DateTime<Utc>,
) -> PostBurstReadiness {
    let comparison_key = post_burst_comparison_key(realm_id, lane);
    let matching = |observation: &&PostBurstEvidence| {
        observation.realm_id == realm_id && observation.lane == lane
    };
    let baseline = summarize_post_burst_arm(
        ledger
            .evidence
            .iter()
            .filter(matching)
            .filter(|observation| observation.arm == ShadowAffinityArm::Baseline),
    );
    let candidate_shadow = summarize_post_burst_arm(
        ledger
            .evidence
            .iter()
            .filter(matching)
            .filter(|observation| {
                observation.arm == ShadowAffinityArm::Candidate && !observation.candidate_applied
            }),
    );
    let candidate_applied = summarize_post_burst_arm(
        ledger
            .evidence
            .iter()
            .filter(matching)
            .filter(|observation| {
                observation.arm == ShadowAffinityArm::Candidate && observation.candidate_applied
            }),
    );
    let canary_baseline = ledger
        .evidence
        .iter()
        .filter(matching)
        .filter(|observation| {
            observation.arm == ShadowAffinityArm::Candidate && observation.candidate_applied
        })
        .map(|observation| observation.observed_at)
        .min()
        .map(|canary_started_at| {
            let contemporaneous = ledger
                .evidence
                .iter()
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
    } else if canary_baseline.cache_ratio_bps
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
    } else if candidate_applied.average_ttft_ms
        > canary_baseline
            .average_ttft_ms
            .saturating_add(POST_BURST_ROLLBACK_TTFT_DELTA_MS)
        && candidate_applied.average_ttft_ms > canary_baseline.average_ttft_ms.saturating_mul(3) / 2
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
        status,
        reason,
        baseline,
        candidate_shadow,
        canary_baseline,
        candidate_applied,
        updated_at: now,
    }
}

fn refresh_post_burst_readiness(
    ledger: &mut PostBurstEvidenceLedger,
    realm_id: &str,
    lane: ShadowCacheLane,
    now: DateTime<Utc>,
) -> PostBurstReadiness {
    let readiness = evaluate_post_burst_readiness(ledger, realm_id, lane, now);
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

fn observe_post_burst_evidence(
    ledger: &mut PostBurstEvidenceLedger,
    decision: &ShadowAffinityDecision,
    input: ShadowObservationInput,
    now: DateTime<Utc>,
) {
    evict_post_burst_evidence(ledger, now);
    let Some(conversation_id) = decision.assignment_key.as_deref() else {
        return;
    };

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
                )
            })
        })
        .flatten();
    if let Some((window_id, followup_index, finished, evidence_lane)) = captured {
        ledger.evidence.push_back(PostBurstEvidence {
            window_id,
            conversation_id: conversation_id.to_string(),
            observed_at: now,
            followup_index,
            realm_id: decision.realm_id.clone(),
            lane: evidence_lane,
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
        });
        if finished {
            ledger.windows.remove(conversation_id);
        }
        while ledger.evidence.len() > POST_BURST_EVIDENCE_LIMIT {
            ledger.evidence.pop_front();
        }
        refresh_post_burst_readiness(ledger, &decision.realm_id, evidence_lane, now);
    }

    if input.giant_tail {
        ledger.next_window_id = ledger.next_window_id.saturating_add(1).max(1);
        ledger.windows.insert(
            conversation_id.to_string(),
            PostBurstWindow {
                window_id: ledger.next_window_id,
                conversation_id: conversation_id.to_string(),
                opened_at: now,
                expires_at: now + Duration::hours(POST_BURST_WINDOW_TTL_HOURS),
                remaining_requests: POST_BURST_FOLLOWUP_REQUESTS,
                captured_requests: 0,
                lane: ShadowCacheLane::ToolBurstQuarantine,
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
        .map(|observation| (observation.realm_id.clone(), observation.lane))
        .collect::<Vec<_>>();
    for (realm_id, lane) in comparison_scopes {
        refresh_post_burst_readiness(&mut store.post_burst, &realm_id, lane, now);
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

    let (realm_id, cohort_id, assignment_lane, arm, shard, policy_epoch, anchor_epoch) = {
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
            });
        assignment.last_seen_at = now;
        assignment.cohort_id = identity.cohort_id.clone();
        assignment.realm_id = identity.realm_id.clone();
        if giant_tail && assignment.lane == ShadowCacheLane::Steady {
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
        )
    };
    let lane = store
        .post_burst
        .windows
        .get(&conversation_id)
        .map(|window| window.lane)
        .unwrap_or(assignment_lane);
    evict_assignments(store, now);
    evict_post_burst_evidence(&mut store.post_burst, now);
    let readiness = refresh_post_burst_readiness(&mut store.post_burst, &realm_id, lane, now);
    ShadowAffinityDecision {
        mode: "shadow".to_string(),
        assignment_key: Some(conversation_id),
        realm_id,
        cohort_id,
        lane,
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

fn automatic_static_cohort_admission_enabled() -> bool {
    #[cfg(test)]
    {
        false
    }
    #[cfg(not(test))]
    {
        std::env::var(AUTOMATIC_STATIC_COHORT_ADMISSION_ENV)
            .ok()
            .is_some_and(|value| matches!(value.trim(), "1" | "true" | "on" | "enabled"))
    }
}

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
        ShadowCacheLane::Steady
            | ShadowCacheLane::ToolBurstQuarantine
            | ShadowCacheLane::CompactedAnchor
    );
    if !smart_hit_enabled
        || !assigned
        || !lane_eligible
        || decision.arm != ShadowAffinityArm::Candidate
    {
        return false;
    }

    decision.decision = "candidate_shadow_only".to_string();
    let readiness = decision
        .automatic_canary_status
        .unwrap_or(PostBurstReadinessStatus::InsufficientEvidence);
    if readiness == PostBurstReadinessStatus::RollbackRequired {
        decision.skip_reason = Some("automatic_canary_rolled_back".to_string());
        return false;
    }
    if readiness == PostBurstReadinessStatus::CanaryHealthy {
        decision.skip_reason = Some("canary_not_promotable".to_string());
        return false;
    }
    let ready = matches!(
        readiness,
        PostBurstReadinessStatus::ReadyForCanary
            | PostBurstReadinessStatus::CanaryCollecting
            | PostBurstReadinessStatus::ReadyForPromotion
    );
    if !ready {
        decision.skip_reason = Some("awaiting_efficacy_evidence".to_string());
        return false;
    }
    if !kill_switch_enabled {
        decision.skip_reason = Some("automatic_canary_kill_switch_off".to_string());
        return false;
    }

    decision.mode = "applied".to_string();
    decision.decision = "automatic_candidate_applied".to_string();
    decision.skip_reason = None;
    true
}

pub(super) fn static_cohort_prompt_cache_key(decision: &ShadowAffinityDecision) -> Option<&str> {
    matches!(decision.mode.as_str(), "applied" | "validation_applied")
        .then_some(decision.cohort_id.as_str())
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
    {
        let Some(assignment) = store.assignments.get_mut(key) else {
            return;
        };
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
    observe_post_burst_evidence(&mut store.post_burst, decision, input, now);
}

pub(super) fn reset_anchor(
    store: &mut ShadowAffinityStore,
    conversation_id: &str,
    now: DateTime<Utc>,
) {
    let opened = if let Some(assignment) = store.assignments.get_mut(conversation_id) {
        assignment.anchor_epoch = assignment.anchor_epoch.saturating_add(1);
        assignment.lane = ShadowCacheLane::CompactedAnchor;
        assignment.last_seen_at = now;
        true
    } else {
        false
    };
    if opened {
        store.post_burst.next_window_id = store.post_burst.next_window_id.saturating_add(1).max(1);
        store.post_burst.windows.insert(
            conversation_id.to_string(),
            PostBurstWindow {
                window_id: store.post_burst.next_window_id,
                conversation_id: conversation_id.to_string(),
                opened_at: now,
                expires_at: now + Duration::hours(POST_BURST_WINDOW_TTL_HOURS),
                remaining_requests: POST_COMPACTION_FOLLOWUP_REQUESTS,
                captured_requests: 0,
                lane: ShadowCacheLane::CompactedAnchor,
            },
        );
    }
}

pub(crate) fn evict_assignments(store: &mut ShadowAffinityStore, now: DateTime<Utc>) {
    let cutoff = now - Duration::hours(SHADOW_ASSIGNMENT_TTL_HOURS);
    store
        .assignments
        .retain(|_, assignment| assignment.last_seen_at >= cutoff);
    while store.assignments.len() > SHADOW_ASSIGNMENT_LIMIT {
        let oldest = store
            .assignments
            .iter()
            .min_by_key(|(_, assignment)| assignment.last_seen_at)
            .map(|(key, _)| key.clone());
        if let Some(oldest) = oldest {
            store.assignments.remove(&oldest);
        } else {
            break;
        }
    }
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
        assert_eq!(summary.cache_ratio_bps, 6_833);
        assert_eq!(summary.first_followup.observations, 1);
        assert_eq!(summary.first_followup.cache_ratio_bps, 2_000);
        assert_eq!(summary.first_followup.average_ttft_ms, 9_000);
        assert_eq!(summary.stable_followups.observations, 2);
        assert_eq!(summary.stable_followups.cache_ratio_bps, 9_250);
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
            Some("awaiting_efficacy_evidence")
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
    fn automatic_candidate_canary_can_apply_to_compaction_recovery() {
        let mut decision = ShadowAffinityDecision {
            mode: "shadow".to_string(),
            assignment_key: Some("conversation".to_string()),
            realm_id: "realm".to_string(),
            cohort_id: "cohort".to_string(),
            lane: ShadowCacheLane::CompactedAnchor,
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

        assert!(apply_automatic_static_cohort_canary_with_switch(
            &mut decision,
            true,
            true
        ));
        assert_eq!(decision.decision, "automatic_candidate_applied");
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
        assert_eq!(
            static_cohort_prompt_cache_key(&decision),
            Some(decision.cohort_id.as_str())
        );

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
                97_500,
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
                97_500,
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
    }

    #[test]
    fn regressed_canary_is_rolled_back_and_stays_blocked() {
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
        assert_eq!(readiness.status, PostBurstReadinessStatus::RollbackRequired);
        assert_eq!(readiness.reason, "candidate_cache_regression");
        assert_eq!(readiness.candidate_applied.observations, 3);
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
            now + Duration::seconds(50),
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
            Some("automatic_canary_rolled_back")
        );
    }

    #[test]
    fn safe_but_ineffective_canary_is_not_promotable_and_stops_applying() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let baseline_ttft = [1_000, 1_100, 1_200];
        let candidate_ttft = [1_050, 1_150, 1_250];
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
        assert_eq!(readiness.reason, "canary_no_efficacy_gain");
        assert_eq!(readiness.baseline.observations, 18);
        assert_eq!(readiness.candidate_applied.observations, 9);
        assert_eq!(readiness.candidate_applied.ttft_p50_ms, 1_150);
        assert_eq!(readiness.candidate_applied.ttft_p95_ms, 1_250);
        assert_eq!(readiness.canary_baseline.observations, 9);
        assert_eq!(readiness.canary_baseline.cache_ratio_bps, 9_750);
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
            decision.skip_reason.as_deref(),
            Some("canary_not_promotable")
        );
    }

    #[test]
    fn effective_canary_is_ready_for_promotion() {
        let mut store = ShadowAffinityStore::default();
        let now = Utc::now();
        let mut realm_id = String::new();

        for index in 0..3 {
            realm_id = record_post_burst_window(
                &mut store,
                &format!("promotion-baseline-{index}"),
                ShadowAffinityArm::Baseline,
                now + Duration::seconds(index * 10),
                false,
                true,
                97_000,
                3_000,
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
                97_000,
                3_000,
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
                97_600,
                2_400,
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
                97_000,
                3_000,
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
                97_000,
                3_000,
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
                97_000,
                3_000,
                0,
                1_000,
                1,
            );
        }
        for (index, ttft_ms) in [1_000, 1_000, 1_600].into_iter().enumerate() {
            record_post_burst_window(
                &mut store,
                &format!("ttft-applied-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(80 + index as i64 * 20),
                true,
                true,
                97_600,
                2_400,
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
                97_000,
                3_000,
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

        for (index, candidate_ttft_ms) in [1_446, 1_446, 5_900].into_iter().enumerate() {
            record_post_burst_window(
                &mut store,
                &format!("phase-applied-{index}"),
                ShadowAffinityArm::Candidate,
                now + Duration::seconds(80 + index as i64 * 20),
                true,
                true,
                97_510,
                2_490,
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
                97_510,
                2_490,
                0,
                [1_432, 1_432, 3_643][index],
                1,
            );
        }

        let comparison_key =
            post_burst_comparison_key(&realm_id, ShadowCacheLane::ToolBurstQuarantine);
        let readiness = store.post_burst.readiness.get(&comparison_key).unwrap();
        assert_eq!(readiness.status, PostBurstReadinessStatus::CanaryHealthy);
        assert_eq!(readiness.reason, "candidate_ttft_p95_not_non_inferior");
        assert_eq!(readiness.baseline.cache_ratio_bps, 9_209);
        assert_eq!(readiness.canary_baseline.cache_ratio_bps, 9_751);
        assert_eq!(readiness.candidate_applied.cache_ratio_bps, 9_751);
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
                },
            );
        }
        evict_assignments(&mut store, now + Duration::seconds(10_000));
        assert!(store.assignments.len() <= SHADOW_ASSIGNMENT_LIMIT);
    }
}
