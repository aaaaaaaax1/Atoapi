use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::{Channel, ProviderCacheCapabilityField, ProviderCacheEffectStatus};

use super::cache_affinity::{ShadowAffinityArm, ShadowAffinityDecision, ShadowCacheLane};

pub(crate) const VALIDATION_TARGET_INPUT_TOKENS: u64 = 5_000_000;
pub(crate) const VALIDATION_TARGET_SUCCESSFUL_REQUESTS: u64 = 50;
const VALIDATION_MAX_HOURS: i64 = 4;
const VALIDATION_BASELINE_MAX_AGE_HOURS: i64 = 4;
const VALIDATION_MIN_COMPARISON_REQUESTS: u64 = 20;
const VALIDATION_MAX_CONSECUTIVE_FAILURES: u64 = 3;
const VALIDATION_MAX_SKIPPED_REQUESTS: u64 = 3;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheValidationMode {
    #[default]
    Auto,
    Baseline,
    Candidate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheValidationControlInput {
    pub mode: CacheValidationMode,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheValidationRunSummary {
    pub run_id: String,
    pub mode: CacheValidationMode,
    pub provider_id: String,
    pub provider_name: String,
    pub model: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub completion_reason: String,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_ratio: f64,
    pub error_rate: f64,
    pub ttft_p95_ms: u64,
    pub usage_observations: u64,
    pub inconclusive_observations: u64,
    pub candidate_applied_requests: u64,
    pub candidate_skipped_requests: u64,
    #[serde(skip)]
    pub(super) scope: Option<CacheValidationScope>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheValidationStatus {
    pub mode: CacheValidationMode,
    pub run_id: Option<String>,
    pub provider_id: Option<String>,
    pub provider_name: Option<String>,
    pub model: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_ratio: f64,
    pub error_rate: f64,
    pub ttft_p95_ms: u64,
    pub usage_observations: u64,
    pub inconclusive_observations: u64,
    pub candidate_applied_requests: u64,
    pub candidate_skipped_requests: u64,
    pub target_input_tokens: u64,
    pub target_successful_requests: u64,
    pub last_run: Option<CacheValidationRunSummary>,
    pub baseline_reference: Option<CacheValidationRunSummary>,
}

#[derive(Debug, Clone)]
pub(super) struct CacheValidationSelection {
    pub run_id: String,
    pub mode: CacheValidationMode,
}

/// Exact upstream scope for a manually controlled validation run. The realm
/// digest binds the deployment, channel, resolved model, and selected key
/// without persisting the secret itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CacheValidationScope {
    pub(super) channel: Channel,
    pub(super) key_id: Option<String>,
    pub(super) realm_id: String,
    pub(super) stream: Option<bool>,
    pub(super) store: Option<bool>,
    /// Present only for generated prompt-cache-key validation. This is a
    /// one-way digest of the exact trusted session placement identity, so a
    /// baseline from one conversation cannot promote another conversation.
    pub(super) generated_key_session_scope_id: Option<String>,
}

impl CacheValidationScope {
    pub(super) fn effect_scope_id(&self, breakpoint_placement_digest: Option<&str>) -> String {
        let stream = match self.stream {
            Some(true) => "stream",
            Some(false) => "sync",
            None => "stream-absent",
        };
        let store = match self.store {
            Some(true) => "store",
            Some(false) => "no-store",
            None => "store-absent",
        };
        let breakpoint = breakpoint_placement_digest.unwrap_or("none");
        format!(
            "cache-effect-v2:{}:{}:{}:bp={breakpoint}",
            self.realm_id, stream, store
        )
    }

    /// Prompt-cache keys use a distinct certificate because the field is
    /// generated from the selected Key realm and session identity, rather
    /// than forwarded from the caller or shared with another control.
    pub(super) fn generated_prompt_cache_key_effect_scope_id(&self) -> Option<String> {
        let session_scope = self.generated_key_session_scope_id.as_deref()?;
        let stream = match self.stream {
            Some(true) => "stream",
            Some(false) => "sync",
            None => "stream-absent",
        };
        let store = match self.store {
            Some(true) => "store",
            Some(false) => "no-store",
            None => "store-absent",
        };
        Some(format!(
            "cache-effect-v4:{}:{}:{}:pk=realm-session-v1:sid={session_scope}",
            self.realm_id, stream, store
        ))
    }
}

#[derive(Debug, Clone)]
pub(super) struct CacheValidationCompletion {
    pub(super) summary: CacheValidationRunSummary,
    pub(super) baseline: Option<CacheValidationRunSummary>,
    pub(super) scope: Option<CacheValidationScope>,
    pub(super) candidate_fields: Vec<ProviderCacheCapabilityField>,
    pub(super) candidate_breakpoint_placement_digest: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct CacheValidationEffectEvidence {
    pub(super) provider_id: String,
    pub(super) model: String,
    pub(super) channel: Channel,
    pub(super) key_id: Option<String>,
    pub(super) effect_scope_id: String,
    pub(super) fields: Vec<ProviderCacheCapabilityField>,
    pub(super) status: ProviderCacheEffectStatus,
    pub(super) message: String,
    pub(super) baseline_cache_read_tokens: u64,
    pub(super) candidate_cache_read_tokens: u64,
    pub(super) baseline_ttft_ms: u64,
    pub(super) candidate_ttft_ms: u64,
}

impl CacheValidationCompletion {
    /// Effect evidence is intentionally emitted only after both arms reached
    /// their normal traffic targets in one exact upstream realm. A positive
    /// generated-key result receives its own strategy-bound certificate.
    pub(super) fn effect_evidence(&self) -> Option<CacheValidationEffectEvidence> {
        let candidate = &self.summary;
        let baseline = self.baseline.as_ref()?;
        let scope = self.scope.as_ref()?;
        if candidate.mode != CacheValidationMode::Candidate
            || !validation_target_reached(candidate)
            || !validation_target_reached(baseline)
            || self.candidate_fields.is_empty()
        {
            return None;
        }
        if self
            .candidate_fields
            .contains(&ProviderCacheCapabilityField::PromptCacheBreakpoint)
            && self.candidate_breakpoint_placement_digest.is_none()
        {
            return None;
        }

        // Compare cache reads at the candidate's observed input volume, then
        // require one full 128-token bucket of real gain. This avoids calling
        // a tiny rounding difference a provider rule improvement.
        let expected_baseline_reads = (baseline.cache_read_tokens as u128)
            .saturating_mul(candidate.input_tokens as u128)
            / baseline.input_tokens.max(1) as u128;
        let candidate_reads = candidate.cache_read_tokens as u128;
        let gained_tokens = candidate_reads.saturating_sub(expected_baseline_reads);
        let positive = candidate_reads > expected_baseline_reads && gained_tokens >= 128;
        let non_regressing = candidate.error_rate <= baseline.error_rate
            && candidate.ttft_p95_ms <= baseline.ttft_p95_ms;
        let status = if positive && non_regressing {
            ProviderCacheEffectStatus::Promoted
        } else {
            ProviderCacheEffectStatus::NoBenefit
        };
        let message = if positive && non_regressing {
            format!(
                "controlled validation improved real cached tokens by {gained_tokens} at the candidate input volume"
            )
        } else if !non_regressing {
            format!(
                "controlled validation gained cached tokens but regressed error rate ({:.4} vs {:.4}) or p95 TTFT ({}ms vs {}ms)",
                candidate.error_rate,
                baseline.error_rate,
                candidate.ttft_p95_ms,
                baseline.ttft_p95_ms,
            )
        } else {
            "controlled validation reached target without a 128-token real cache gain".to_string()
        };
        let effect_scope_id = if self
            .candidate_fields
            .contains(&ProviderCacheCapabilityField::PromptCacheKey)
        {
            scope.generated_prompt_cache_key_effect_scope_id()?
        } else {
            scope.effect_scope_id(self.candidate_breakpoint_placement_digest.as_deref())
        };
        Some(CacheValidationEffectEvidence {
            provider_id: candidate.provider_id.clone(),
            model: candidate.model.clone(),
            channel: scope.channel.clone(),
            key_id: scope.key_id.clone(),
            effect_scope_id,
            fields: self.candidate_fields.clone(),
            status,
            message,
            baseline_cache_read_tokens: baseline.cache_read_tokens,
            candidate_cache_read_tokens: candidate.cache_read_tokens,
            baseline_ttft_ms: baseline.ttft_p95_ms,
            candidate_ttft_ms: candidate.ttft_p95_ms,
        })
    }
}

pub(super) fn apply_controlled_selection(
    decision: &mut ShadowAffinityDecision,
    selection: &CacheValidationSelection,
    smart_hit_enabled: bool,
    channel_eligible: bool,
) -> bool {
    decision.validation_run_id = Some(selection.run_id.clone());
    decision.mode = "validation".to_string();
    decision.skip_reason = None;
    match selection.mode {
        CacheValidationMode::Auto => false,
        CacheValidationMode::Baseline => {
            decision.arm = ShadowAffinityArm::Baseline;
            decision.decision = "validation_baseline".to_string();
            false
        }
        CacheValidationMode::Candidate => {
            decision.arm = ShadowAffinityArm::Candidate;
            if !smart_hit_enabled {
                decision.decision = "validation_candidate_skipped".to_string();
                decision.skip_reason = Some("smart_cache_disabled".to_string());
                return false;
            }
            if !channel_eligible {
                decision.decision = "validation_candidate_skipped".to_string();
                decision.skip_reason = Some("unsupported_channel".to_string());
                return false;
            }
            if decision.lane != ShadowCacheLane::Steady {
                decision.decision = "validation_candidate_skipped".to_string();
                decision.skip_reason = Some("non_steady_lane".to_string());
                return false;
            }
            decision.mode = "validation_applied".to_string();
            decision.decision = "validation_candidate_applied".to_string();
            true
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct CacheValidationObservation {
    pub success: bool,
    pub usage_observed: bool,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
    pub ttft_ms: u64,
    pub candidate_applied: bool,
    pub candidate_fields: Vec<ProviderCacheCapabilityField>,
    /// The exact final-wire placement for a breakpoint candidate. `None` is
    /// meaningful for non-breakpoint candidates and is bound on first use.
    pub candidate_breakpoint_placement_digest: Option<String>,
}

#[derive(Debug, Clone)]
struct ActiveValidationRun {
    run_id: String,
    mode: CacheValidationMode,
    provider_id: String,
    provider_name: String,
    model: String,
    started_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    successful_requests: u64,
    failed_requests: u64,
    consecutive_failures: u64,
    input_tokens: u64,
    cache_read_tokens: u64,
    ttft_samples: Vec<u64>,
    candidate_applied_requests: u64,
    candidate_skipped_requests: u64,
    usage_observations: u64,
    inconclusive_observations: u64,
    scope: Option<CacheValidationScope>,
    candidate_fields: Option<Vec<ProviderCacheCapabilityField>>,
    candidate_breakpoint_placement_digest: Option<Option<String>>,
}

#[derive(Debug, Default)]
pub(crate) struct CacheValidationController {
    active: Option<ActiveValidationRun>,
    last_run: Option<CacheValidationRunSummary>,
    baseline_reference: Option<CacheValidationRunSummary>,
}

impl CacheValidationController {
    pub(crate) fn configure(
        &mut self,
        input: CacheValidationControlInput,
        provider_name: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<CacheValidationStatus, String> {
        if input.mode == CacheValidationMode::Auto {
            self.finish_active("manual_stop", now);
            return Ok(self.status(now));
        }

        let provider_id = required_value(input.provider_id, "provider")?;
        let provider_name = required_value(provider_name, "provider name")?;
        let model = required_value(input.model, "model")?;
        self.finish_active("replaced", now);
        self.active = Some(ActiveValidationRun {
            run_id: Uuid::new_v4().to_string(),
            mode: input.mode,
            provider_id,
            provider_name,
            model,
            started_at: now,
            expires_at: now + Duration::hours(VALIDATION_MAX_HOURS),
            successful_requests: 0,
            failed_requests: 0,
            consecutive_failures: 0,
            input_tokens: 0,
            cache_read_tokens: 0,
            ttft_samples: Vec::new(),
            candidate_applied_requests: 0,
            candidate_skipped_requests: 0,
            usage_observations: 0,
            inconclusive_observations: 0,
            scope: None,
            candidate_fields: None,
            candidate_breakpoint_placement_digest: None,
        });
        Ok(self.status(now))
    }

    pub(super) fn selection(
        &mut self,
        provider_id: &str,
        model: &str,
        scope: CacheValidationScope,
        now: DateTime<Utc>,
    ) -> Option<CacheValidationSelection> {
        self.expire_if_needed(now);
        let active = self.active.as_ref()?;
        if active.provider_id != provider_id || active.model != model {
            return None;
        }
        let incompatible_scope = active.scope.as_ref().is_some_and(|bound| bound != &scope);
        let missing_or_incompatible_baseline = active.mode == CacheValidationMode::Candidate
            && active.scope.is_none()
            && self.baseline_reference.as_ref().is_none_or(|baseline| {
                !baseline_is_ready_for_scope(baseline, provider_id, model, &scope, now)
            });
        if incompatible_scope {
            self.finish_active("scope_mismatch", now);
            return None;
        }
        if missing_or_incompatible_baseline {
            self.finish_active("baseline_scope_mismatch", now);
            return None;
        }

        let active = self
            .active
            .as_mut()
            .expect("active validation was checked before scope handling");
        if active.scope.is_none() {
            active.scope = Some(scope);
        } else {
            debug_assert_eq!(active.scope.as_ref(), Some(&scope));
        }
        Some(CacheValidationSelection {
            run_id: active.run_id.clone(),
            mode: active.mode,
        })
    }

    pub(super) fn observe(
        &mut self,
        run_id: &str,
        observation: CacheValidationObservation,
        now: DateTime<Utc>,
    ) -> Option<CacheValidationCompletion> {
        self.expire_if_needed(now);
        let Some(active) = self.active.as_mut().filter(|run| run.run_id == run_id) else {
            return None;
        };

        if active.mode == CacheValidationMode::Candidate && !observation.candidate_applied {
            active.candidate_skipped_requests += 1;
            if active.candidate_skipped_requests >= VALIDATION_MAX_SKIPPED_REQUESTS
                && active.candidate_applied_requests == 0
            {
                return self.finish_active("candidate_not_applicable", now);
            }
            return None;
        }

        if active.mode == CacheValidationMode::Candidate {
            let mut fields = observation.candidate_fields;
            fields.sort_by_key(|field| field.json_name());
            fields.dedup();
            if fields.is_empty() {
                active.candidate_skipped_requests += 1;
                if active.candidate_skipped_requests >= VALIDATION_MAX_SKIPPED_REQUESTS
                    && active.candidate_applied_requests == 0
                {
                    return self.finish_active("candidate_missing_controls", now);
                }
                return None;
            }
            if fields.len() != 1 {
                return self.finish_active("candidate_multiple_controls", now);
            }
            if active
                .candidate_fields
                .as_ref()
                .is_some_and(|previous| previous != &fields)
            {
                return self.finish_active("candidate_wire_changed", now);
            }
            let breakpoint_candidate =
                fields.contains(&ProviderCacheCapabilityField::PromptCacheBreakpoint);
            if breakpoint_candidate && observation.candidate_breakpoint_placement_digest.is_none() {
                return self.finish_active("candidate_breakpoint_missing_placement", now);
            }
            if active
                .candidate_breakpoint_placement_digest
                .as_ref()
                .is_some_and(|previous| {
                    previous != &observation.candidate_breakpoint_placement_digest
                })
            {
                return self.finish_active("candidate_breakpoint_placement_changed", now);
            }
            if active.candidate_breakpoint_placement_digest.is_none() {
                active.candidate_breakpoint_placement_digest =
                    Some(observation.candidate_breakpoint_placement_digest);
            }
            active.candidate_fields = Some(fields);
            active.candidate_applied_requests += 1;
        }
        if observation.success {
            active.successful_requests += 1;
            active.consecutive_failures = 0;
            active.ttft_samples.push(observation.ttft_ms);
            if observation.usage_observed {
                active.usage_observations += 1;
                active.input_tokens = active.input_tokens.saturating_add(observation.input_tokens);
                active.cache_read_tokens = active
                    .cache_read_tokens
                    .saturating_add(observation.cache_read_tokens);
            } else {
                active.inconclusive_observations += 1;
            }
        } else {
            active.failed_requests += 1;
            active.consecutive_failures += 1;
        }

        let should_complete = active.successful_requests >= VALIDATION_TARGET_SUCCESSFUL_REQUESTS
            && active.usage_observations >= VALIDATION_TARGET_SUCCESSFUL_REQUESTS
            && active.input_tokens >= VALIDATION_TARGET_INPUT_TOKENS;
        let rollback_reason = self.candidate_rollback_reason();
        if let Some(reason) = rollback_reason {
            self.finish_active(reason, now)
        } else if should_complete {
            self.finish_active("target_reached", now)
        } else {
            None
        }
    }

    pub(crate) fn status(&mut self, now: DateTime<Utc>) -> CacheValidationStatus {
        self.expire_if_needed(now);
        let active = self.active.as_ref();
        let (cache_ratio, error_rate, ttft_p95_ms) = active
            .map(|run| {
                (
                    ratio(run.cache_read_tokens, run.input_tokens),
                    ratio(
                        run.failed_requests,
                        run.successful_requests + run.failed_requests,
                    ),
                    percentile_95(&run.ttft_samples),
                )
            })
            .unwrap_or_default();
        CacheValidationStatus {
            mode: active.map(|run| run.mode).unwrap_or_default(),
            run_id: active.map(|run| run.run_id.clone()),
            provider_id: active.map(|run| run.provider_id.clone()),
            provider_name: active.map(|run| run.provider_name.clone()),
            model: active.map(|run| run.model.clone()),
            started_at: active.map(|run| run.started_at),
            expires_at: active.map(|run| run.expires_at),
            successful_requests: active
                .map(|run| run.successful_requests)
                .unwrap_or_default(),
            failed_requests: active.map(|run| run.failed_requests).unwrap_or_default(),
            input_tokens: active.map(|run| run.input_tokens).unwrap_or_default(),
            cache_read_tokens: active.map(|run| run.cache_read_tokens).unwrap_or_default(),
            cache_ratio,
            error_rate,
            ttft_p95_ms,
            usage_observations: active.map(|run| run.usage_observations).unwrap_or_default(),
            inconclusive_observations: active
                .map(|run| run.inconclusive_observations)
                .unwrap_or_default(),
            candidate_applied_requests: active
                .map(|run| run.candidate_applied_requests)
                .unwrap_or_default(),
            candidate_skipped_requests: active
                .map(|run| run.candidate_skipped_requests)
                .unwrap_or_default(),
            target_input_tokens: VALIDATION_TARGET_INPUT_TOKENS,
            target_successful_requests: VALIDATION_TARGET_SUCCESSFUL_REQUESTS,
            last_run: self.last_run.clone(),
            baseline_reference: self.baseline_reference.clone(),
        }
    }

    fn expire_if_needed(&mut self, now: DateTime<Utc>) {
        if self
            .active
            .as_ref()
            .is_some_and(|run| now >= run.expires_at)
        {
            self.finish_active("expired", now);
        }
    }

    fn candidate_rollback_reason(&self) -> Option<&'static str> {
        let active = self
            .active
            .as_ref()
            .filter(|run| run.mode == CacheValidationMode::Candidate)?;
        if active.consecutive_failures >= VALIDATION_MAX_CONSECUTIVE_FAILURES {
            return Some("candidate_consecutive_failures");
        }
        let attempts = active.successful_requests + active.failed_requests;
        if attempts >= VALIDATION_MIN_COMPARISON_REQUESTS
            && ratio(active.failed_requests, attempts) > 0.10
        {
            return Some("candidate_error_rate");
        }
        let baseline = self.baseline_reference.as_ref().filter(|baseline| {
            baseline.successful_requests >= VALIDATION_MIN_COMPARISON_REQUESTS
        })?;
        if active.successful_requests < VALIDATION_MIN_COMPARISON_REQUESTS {
            return None;
        }
        if ratio(active.failed_requests, attempts) > baseline.error_rate + 0.02
            && ratio(active.failed_requests, attempts) > 0.05
        {
            return Some("candidate_error_regression");
        }
        let candidate_p95 = percentile_95(&active.ttft_samples);
        if baseline.ttft_p95_ms > 0
            && candidate_p95 > baseline.ttft_p95_ms.saturating_mul(5) / 4
            && candidate_p95 > baseline.ttft_p95_ms.saturating_add(1_000)
        {
            return Some("candidate_ttft_regression");
        }
        None
    }

    fn finish_active(
        &mut self,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Option<CacheValidationCompletion> {
        let Some(active) = self.active.take() else {
            return None;
        };
        let attempts = active.successful_requests + active.failed_requests;
        let summary = CacheValidationRunSummary {
            run_id: active.run_id,
            mode: active.mode,
            provider_id: active.provider_id,
            provider_name: active.provider_name,
            model: active.model,
            started_at: active.started_at,
            finished_at: now,
            completion_reason: reason.to_string(),
            successful_requests: active.successful_requests,
            failed_requests: active.failed_requests,
            input_tokens: active.input_tokens,
            cache_read_tokens: active.cache_read_tokens,
            cache_ratio: ratio(active.cache_read_tokens, active.input_tokens),
            error_rate: ratio(active.failed_requests, attempts),
            ttft_p95_ms: percentile_95(&active.ttft_samples),
            usage_observations: active.usage_observations,
            inconclusive_observations: active.inconclusive_observations,
            candidate_applied_requests: active.candidate_applied_requests,
            candidate_skipped_requests: active.candidate_skipped_requests,
            scope: active.scope.clone(),
        };
        if summary.mode == CacheValidationMode::Baseline && validation_target_reached(&summary) {
            self.baseline_reference = Some(summary.clone());
        }
        self.last_run = Some(summary);
        let summary = self
            .last_run
            .clone()
            .expect("finished run must be retained");
        (summary.mode == CacheValidationMode::Candidate).then(|| CacheValidationCompletion {
            baseline: self.baseline_reference.clone(),
            scope: active.scope,
            candidate_fields: active.candidate_fields.unwrap_or_default(),
            candidate_breakpoint_placement_digest: active
                .candidate_breakpoint_placement_digest
                .unwrap_or(None),
            summary,
        })
    }
}

fn validation_target_reached(summary: &CacheValidationRunSummary) -> bool {
    summary.completion_reason == "target_reached"
        && summary.successful_requests >= VALIDATION_TARGET_SUCCESSFUL_REQUESTS
        && summary.usage_observations >= VALIDATION_TARGET_SUCCESSFUL_REQUESTS
        && summary.input_tokens >= VALIDATION_TARGET_INPUT_TOKENS
}

fn baseline_is_ready_for_scope(
    baseline: &CacheValidationRunSummary,
    provider_id: &str,
    model: &str,
    scope: &CacheValidationScope,
    now: DateTime<Utc>,
) -> bool {
    baseline.mode == CacheValidationMode::Baseline
        && baseline.provider_id == provider_id
        && baseline.model == model
        && baseline.scope.as_ref() == Some(scope)
        && validation_target_reached(baseline)
        && now <= baseline.finished_at + Duration::hours(VALIDATION_BASELINE_MAX_AGE_HOURS)
}

fn required_value(value: Option<String>, field: &str) -> Result<String, String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{field} is required for cache validation"))
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn percentile_95(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(mode: CacheValidationMode) -> CacheValidationControlInput {
        CacheValidationControlInput {
            mode,
            provider_id: Some("provider-a".to_string()),
            model: Some("gpt-main".to_string()),
        }
    }

    fn scope(realm_id: &str) -> CacheValidationScope {
        CacheValidationScope {
            channel: Channel::Responses,
            key_id: Some("key-a".to_string()),
            realm_id: realm_id.to_string(),
            stream: Some(true),
            store: Some(false),
            generated_key_session_scope_id: Some("a".repeat(64)),
        }
    }

    fn decision() -> ShadowAffinityDecision {
        ShadowAffinityDecision {
            mode: "shadow".to_string(),
            assignment_key: None,
            realm_id: "realm".to_string(),
            cohort_id: "cohort".to_string(),
            lane: ShadowCacheLane::Steady,
            candidate_variant: crate::proxy::cache_affinity::ShadowCacheCandidateVariant::CohortKey,
            arm: ShadowAffinityArm::Baseline,
            shard: 0,
            policy_epoch: 1,
            anchor_epoch: 0,
            trusted_identity: false,
            decision: "stateless_assigned".to_string(),
            skip_reason: None,
            policy_compute_ms: 0,
            validation_run_id: None,
            automatic_canary_status: None,
            automatic_canary_reason: None,
        }
    }

    #[test]
    fn controlled_candidate_applies_without_consulting_client_cache_key() {
        let mut decision = decision();
        let selection = CacheValidationSelection {
            run_id: "run-a".to_string(),
            mode: CacheValidationMode::Candidate,
        };

        assert!(apply_controlled_selection(
            &mut decision,
            &selection,
            true,
            true
        ));
        assert_eq!(decision.mode, "validation_applied");
        assert_eq!(decision.decision, "validation_candidate_applied");
        assert_eq!(decision.validation_run_id.as_deref(), Some("run-a"));
    }

    #[test]
    fn controlled_baseline_never_rewrites_the_cache_key() {
        let mut decision = decision();
        let selection = CacheValidationSelection {
            run_id: "run-b".to_string(),
            mode: CacheValidationMode::Baseline,
        };

        assert!(!apply_controlled_selection(
            &mut decision,
            &selection,
            true,
            true
        ));
        assert_eq!(decision.mode, "validation");
        assert_eq!(decision.decision, "validation_baseline");
        assert_eq!(decision.arm, ShadowAffinityArm::Baseline);
    }

    #[test]
    fn validation_scope_is_exact_and_never_persists_as_default() {
        let now = Utc::now();
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", scope("realm-a"), now)
            .is_some());
        controller.observe(
            &baseline.run_id.unwrap(),
            CacheValidationObservation {
                success: true,
                usage_observed: true,
                input_tokens: 1,
                ..CacheValidationObservation::default()
            },
            now,
        );
        controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now + Duration::seconds(1),
            )
            .unwrap();
        assert!(controller
            .selection("provider-b", "gpt-main", scope("realm-a"), now)
            .is_none());
        assert!(controller
            .selection("provider-a", "gpt-side", scope("realm-a"), now)
            .is_none());
        assert!(controller
            .selection("provider-a", "gpt-main", scope("realm-b"), now)
            .is_none());
        assert_eq!(
            controller
                .status(now + Duration::hours(VALIDATION_MAX_HOURS + 1))
                .mode,
            CacheValidationMode::Auto
        );
    }

    #[test]
    fn generated_key_validation_never_crosses_session_scope() {
        let now = Utc::now();
        let mut controller = CacheValidationController::default();
        controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();

        let trusted_session_a = scope("realm-a");
        let trusted_session_b = CacheValidationScope {
            generated_key_session_scope_id: Some("b".repeat(64)),
            ..trusted_session_a.clone()
        };

        assert!(controller
            .selection("provider-a", "gpt-main", trusted_session_a, now)
            .is_some());
        assert!(controller
            .selection("provider-a", "gpt-main", trusted_session_b, now)
            .is_none());
    }

    #[test]
    fn candidate_without_any_applied_request_returns_to_auto() {
        let now = Utc::now();
        let mut controller = CacheValidationController::default();
        let status = controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let run_id = status.run_id.unwrap();
        for index in 0..VALIDATION_MAX_SKIPPED_REQUESTS {
            controller.observe(
                &run_id,
                CacheValidationObservation::default(),
                now + Duration::seconds(index as i64),
            );
        }

        let status = controller.status(now + Duration::minutes(1));
        assert_eq!(status.mode, CacheValidationMode::Auto);
        assert_eq!(
            status.last_run.unwrap().completion_reason,
            "candidate_not_applicable"
        );
    }

    #[test]
    fn three_applied_candidate_failures_trigger_rollback() {
        let now = Utc::now();
        let mut controller = CacheValidationController::default();
        let status = controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let run_id = status.run_id.unwrap();
        for index in 0..VALIDATION_MAX_CONSECUTIVE_FAILURES {
            controller.observe(
                &run_id,
                CacheValidationObservation {
                    candidate_applied: true,
                    candidate_fields: vec![ProviderCacheCapabilityField::PromptCacheKey],
                    ..CacheValidationObservation::default()
                },
                now + Duration::seconds(index as i64),
            );
        }

        let status = controller.status(now + Duration::minutes(1));
        assert_eq!(status.mode, CacheValidationMode::Auto);
        assert_eq!(
            status.last_run.unwrap().completion_reason,
            "candidate_consecutive_failures"
        );
    }

    #[test]
    fn incomplete_baseline_is_not_kept_for_candidate_comparison() {
        let now = Utc::now();
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let run_id = baseline.run_id.unwrap();
        controller.observe(
            &run_id,
            CacheValidationObservation {
                success: true,
                input_tokens: 1_000,
                cache_read_tokens: 800,
                ttft_ms: 500,
                candidate_applied: false,
                usage_observed: true,
                ..CacheValidationObservation::default()
            },
            now,
        );
        controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now + Duration::minutes(1),
            )
            .unwrap();

        let status = controller.status(now + Duration::minutes(1));
        assert!(status.baseline_reference.is_none());
    }

    #[test]
    fn missing_usage_cannot_complete_a_baseline() {
        let now = Utc::now();
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let run_id = baseline.run_id.unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", scope("realm-a"), now)
            .is_some());

        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            controller.observe(
                &run_id,
                CacheValidationObservation {
                    success: true,
                    input_tokens: 100_000,
                    ..CacheValidationObservation::default()
                },
                now + Duration::seconds(index as i64),
            );
        }

        let status = controller.status(now + Duration::minutes(1));
        assert_eq!(status.mode, CacheValidationMode::Baseline);
        assert_eq!(
            status.successful_requests,
            VALIDATION_TARGET_SUCCESSFUL_REQUESTS
        );
        assert_eq!(status.usage_observations, 0);
        assert!(status.baseline_reference.is_none());
    }

    #[test]
    fn candidate_rejects_an_expired_complete_baseline() {
        let now = Utc::now();
        let validation_scope = scope("realm-a");
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let baseline_run_id = baseline.run_id.unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", validation_scope.clone(), now)
            .is_some());
        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            controller.observe(
                &baseline_run_id,
                CacheValidationObservation {
                    success: true,
                    usage_observed: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 50_000,
                    ttft_ms: 100,
                    ..CacheValidationObservation::default()
                },
                now + Duration::seconds(index as i64),
            );
        }

        let candidate_at = now + Duration::hours(VALIDATION_BASELINE_MAX_AGE_HOURS + 1);
        controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                candidate_at,
            )
            .unwrap();

        assert!(controller
            .selection("provider-a", "gpt-main", validation_scope, candidate_at,)
            .is_none());
        let status = controller.status(candidate_at);
        assert_eq!(status.mode, CacheValidationMode::Auto);
        assert_eq!(
            status.last_run.unwrap().completion_reason,
            "baseline_scope_mismatch"
        );
    }

    #[test]
    fn effect_scope_id_distinguishes_stream_and_store_shape() {
        let stream_no_store = scope("realm-a");
        let sync_no_store = CacheValidationScope {
            stream: Some(false),
            ..stream_no_store.clone()
        };
        let stream_store = CacheValidationScope {
            store: Some(true),
            ..stream_no_store.clone()
        };

        assert_ne!(
            stream_no_store.effect_scope_id(None),
            sync_no_store.effect_scope_id(None)
        );
        assert_ne!(
            stream_no_store.effect_scope_id(None),
            stream_store.effect_scope_id(None)
        );
        assert_eq!(
            stream_no_store.generated_prompt_cache_key_effect_scope_id(),
            Some(
                "cache-effect-v4:realm-a:stream:no-store:pk=realm-session-v1:sid=".to_string()
                    + &"a".repeat(64)
            )
        );
        assert_ne!(
            stream_no_store.generated_prompt_cache_key_effect_scope_id(),
            Some(stream_no_store.effect_scope_id(None))
        );
        assert_eq!(
            CacheValidationScope {
                generated_key_session_scope_id: None,
                ..stream_no_store.clone()
            }
            .generated_prompt_cache_key_effect_scope_id(),
            None
        );
    }

    #[test]
    fn candidate_without_usage_cannot_complete_or_emit_effect_evidence() {
        let now = Utc::now();
        let validation_scope = scope("realm-a");
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let baseline_run_id = baseline.run_id.unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", validation_scope.clone(), now)
            .is_some());
        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            controller.observe(
                &baseline_run_id,
                CacheValidationObservation {
                    success: true,
                    usage_observed: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 50_000,
                    ttft_ms: 100,
                    ..CacheValidationObservation::default()
                },
                now + Duration::seconds(index as i64),
            );
        }

        let candidate = controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now + Duration::minutes(2),
            )
            .unwrap();
        let candidate_run_id = candidate.run_id.unwrap();
        assert!(controller
            .selection(
                "provider-a",
                "gpt-main",
                validation_scope,
                now + Duration::minutes(2),
            )
            .is_some());

        let mut completion = None;
        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            completion = controller.observe(
                &candidate_run_id,
                CacheValidationObservation {
                    success: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 60_000,
                    ttft_ms: 100,
                    candidate_applied: true,
                    candidate_fields: vec![ProviderCacheCapabilityField::PromptCacheRetention],
                    candidate_breakpoint_placement_digest: None,
                    // A successful response that omits usage cannot establish
                    // a real cache gain and must remain inconclusive.
                    usage_observed: false,
                },
                now + Duration::minutes(2) + Duration::seconds(index as i64),
            );
        }

        assert!(completion.is_none());
        let status = controller.status(now + Duration::minutes(4));
        assert_eq!(status.mode, CacheValidationMode::Candidate);
        assert_eq!(
            status.successful_requests,
            VALIDATION_TARGET_SUCCESSFUL_REQUESTS
        );
        assert_eq!(status.usage_observations, 0);
        assert_eq!(
            status.candidate_applied_requests,
            VALIDATION_TARGET_SUCCESSFUL_REQUESTS
        );
        assert_eq!(
            status.last_run.as_ref().map(|run| run.mode),
            Some(CacheValidationMode::Baseline),
            "the active candidate has not finished; the retained completed baseline remains historical evidence"
        );
    }

    #[test]
    fn candidate_rejects_more_than_one_control_field() {
        let now = Utc::now();
        let mut controller = CacheValidationController::default();
        let candidate = controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now,
            )
            .unwrap();

        controller.observe(
            &candidate.run_id.unwrap(),
            CacheValidationObservation {
                candidate_applied: true,
                candidate_fields: vec![
                    ProviderCacheCapabilityField::PromptCacheOptions,
                    ProviderCacheCapabilityField::PromptCacheBreakpoint,
                ],
                ..CacheValidationObservation::default()
            },
            now,
        );

        let status = controller.status(now + Duration::seconds(1));
        assert_eq!(status.mode, CacheValidationMode::Auto);
        assert_eq!(
            status.last_run.unwrap().completion_reason,
            "candidate_multiple_controls"
        );
    }

    #[test]
    fn breakpoint_candidate_cannot_mix_final_wire_placements() {
        let now = Utc::now();
        let validation_scope = scope("realm-a");
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let baseline_run_id = baseline.run_id.unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", validation_scope.clone(), now)
            .is_some());
        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            controller.observe(
                &baseline_run_id,
                CacheValidationObservation {
                    success: true,
                    usage_observed: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 50_000,
                    ttft_ms: 100,
                    ..CacheValidationObservation::default()
                },
                now + Duration::seconds(index as i64),
            );
        }

        let candidate = controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now + Duration::minutes(2),
            )
            .unwrap();
        let candidate_run_id = candidate.run_id.unwrap();
        assert!(controller
            .selection(
                "provider-a",
                "gpt-main",
                validation_scope,
                now + Duration::minutes(2),
            )
            .is_some());
        let first = CacheValidationObservation {
            success: true,
            usage_observed: true,
            input_tokens: 100_000,
            cache_read_tokens: 60_000,
            ttft_ms: 100,
            candidate_applied: true,
            candidate_fields: vec![ProviderCacheCapabilityField::PromptCacheBreakpoint],
            candidate_breakpoint_placement_digest: Some("v2:placement-a".to_string()),
        };
        assert!(controller
            .observe(&candidate_run_id, first, now + Duration::minutes(2))
            .is_none());
        let changed = CacheValidationObservation {
            candidate_breakpoint_placement_digest: Some("v2:placement-b".to_string()),
            ..CacheValidationObservation {
                success: true,
                usage_observed: true,
                input_tokens: 100_000,
                cache_read_tokens: 60_000,
                ttft_ms: 100,
                candidate_applied: true,
                candidate_fields: vec![ProviderCacheCapabilityField::PromptCacheBreakpoint],
                candidate_breakpoint_placement_digest: None,
            }
        };
        let completed = controller
            .observe(
                &candidate_run_id,
                changed,
                now + Duration::minutes(2) + Duration::seconds(1),
            )
            .expect("a moved breakpoint must end the candidate before promotion");
        assert_eq!(
            completed.summary.completion_reason,
            "candidate_breakpoint_placement_changed"
        );
    }

    #[test]
    fn matching_scope_with_real_gain_emits_promotable_effect_evidence() {
        let now = Utc::now();
        let scope = scope("realm-a");
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let baseline_run_id = baseline.run_id.unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", scope.clone(), now)
            .is_some());
        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            controller.observe(
                &baseline_run_id,
                CacheValidationObservation {
                    success: true,
                    usage_observed: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 50_000,
                    ttft_ms: 100,
                    ..CacheValidationObservation::default()
                },
                now + Duration::seconds(index as i64),
            );
        }

        let candidate = controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now + Duration::minutes(2),
            )
            .unwrap();
        let candidate_run_id = candidate.run_id.unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", scope.clone(), now)
            .is_some());
        let mut completion = None;
        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            completion = controller.observe(
                &candidate_run_id,
                CacheValidationObservation {
                    success: true,
                    usage_observed: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 60_000,
                    ttft_ms: 100,
                    candidate_applied: true,
                    candidate_fields: vec![ProviderCacheCapabilityField::PromptCacheOptions],
                    candidate_breakpoint_placement_digest: None,
                },
                now + Duration::minutes(2) + Duration::seconds(index as i64),
            );
        }
        let evidence = completion
            .and_then(|completion| completion.effect_evidence())
            .expect("matching baseline/candidate targets must yield effect evidence");
        assert_eq!(evidence.status, ProviderCacheEffectStatus::Promoted);
        assert_eq!(
            evidence.fields,
            vec![ProviderCacheCapabilityField::PromptCacheOptions]
        );
        assert_eq!(evidence.channel, Channel::Responses);
    }

    #[test]
    fn cache_gain_with_ttft_regression_is_not_promoted() {
        let now = Utc::now();
        let scope = scope("realm-a");
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let baseline_run_id = baseline.run_id.unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", scope.clone(), now)
            .is_some());
        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            controller.observe(
                &baseline_run_id,
                CacheValidationObservation {
                    success: true,
                    usage_observed: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 50_000,
                    ttft_ms: 100,
                    ..CacheValidationObservation::default()
                },
                now + Duration::seconds(index as i64),
            );
        }

        let candidate = controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now + Duration::minutes(2),
            )
            .unwrap();
        let candidate_run_id = candidate.run_id.unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", scope, now + Duration::minutes(2))
            .is_some());
        let mut completion = None;
        for index in 0..VALIDATION_TARGET_SUCCESSFUL_REQUESTS {
            completion = controller.observe(
                &candidate_run_id,
                CacheValidationObservation {
                    success: true,
                    usage_observed: true,
                    input_tokens: 100_000,
                    cache_read_tokens: 60_000,
                    ttft_ms: 101,
                    candidate_applied: true,
                    candidate_fields: vec![ProviderCacheCapabilityField::PromptCacheOptions],
                    candidate_breakpoint_placement_digest: None,
                },
                now + Duration::minutes(2) + Duration::seconds(index as i64),
            );
        }

        let evidence = completion
            .and_then(|completion| completion.effect_evidence())
            .expect("completed candidate must produce a non-promotion result");
        assert_eq!(evidence.status, ProviderCacheEffectStatus::NoBenefit);
    }

    #[test]
    fn candidate_scope_mismatch_cannot_consume_a_baseline() {
        let now = Utc::now();
        let mut controller = CacheValidationController::default();
        let baseline = controller
            .configure(
                input(CacheValidationMode::Baseline),
                Some("A".to_string()),
                now,
            )
            .unwrap();
        let baseline_run_id = baseline.run_id.unwrap();
        let baseline_scope = scope("realm-a");
        assert!(controller
            .selection("provider-a", "gpt-main", baseline_scope, now)
            .is_some());
        controller.observe(
            &baseline_run_id,
            CacheValidationObservation {
                success: true,
                usage_observed: true,
                input_tokens: 1,
                ..CacheValidationObservation::default()
            },
            now,
        );
        controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now + Duration::seconds(1),
            )
            .unwrap();
        assert!(controller
            .selection("provider-a", "gpt-main", scope("realm-b"), now)
            .is_none());
    }
}
