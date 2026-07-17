use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::cache_affinity::{ShadowAffinityArm, ShadowAffinityDecision, ShadowCacheLane};

pub(crate) const VALIDATION_TARGET_INPUT_TOKENS: u64 = 5_000_000;
pub(crate) const VALIDATION_TARGET_SUCCESSFUL_REQUESTS: u64 = 50;
const VALIDATION_MAX_HOURS: i64 = 4;
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
    pub candidate_applied_requests: u64,
    pub candidate_skipped_requests: u64,
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

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CacheValidationObservation {
    pub success: bool,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
    pub ttft_ms: u64,
    pub candidate_applied: bool,
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
        });
        Ok(self.status(now))
    }

    pub(super) fn selection(
        &mut self,
        provider_id: &str,
        model: &str,
        now: DateTime<Utc>,
    ) -> Option<CacheValidationSelection> {
        self.expire_if_needed(now);
        let active = self.active.as_ref()?;
        (active.provider_id == provider_id && active.model.eq_ignore_ascii_case(model)).then(|| {
            CacheValidationSelection {
                run_id: active.run_id.clone(),
                mode: active.mode,
            }
        })
    }

    pub(super) fn observe(
        &mut self,
        run_id: &str,
        observation: CacheValidationObservation,
        now: DateTime<Utc>,
    ) {
        self.expire_if_needed(now);
        let Some(active) = self.active.as_mut().filter(|run| run.run_id == run_id) else {
            return;
        };

        if active.mode == CacheValidationMode::Candidate && !observation.candidate_applied {
            active.candidate_skipped_requests += 1;
            if active.candidate_skipped_requests >= VALIDATION_MAX_SKIPPED_REQUESTS
                && active.candidate_applied_requests == 0
            {
                self.finish_active("candidate_not_applicable", now);
            }
            return;
        }

        if active.mode == CacheValidationMode::Candidate {
            active.candidate_applied_requests += 1;
        }
        if observation.success {
            active.successful_requests += 1;
            active.consecutive_failures = 0;
            active.ttft_samples.push(observation.ttft_ms);
        } else {
            active.failed_requests += 1;
            active.consecutive_failures += 1;
        }
        active.input_tokens = active.input_tokens.saturating_add(observation.input_tokens);
        active.cache_read_tokens = active
            .cache_read_tokens
            .saturating_add(observation.cache_read_tokens);

        let should_complete = active.successful_requests >= VALIDATION_TARGET_SUCCESSFUL_REQUESTS
            && active.input_tokens >= VALIDATION_TARGET_INPUT_TOKENS;
        let rollback_reason = self.candidate_rollback_reason();
        if let Some(reason) = rollback_reason {
            self.finish_active(reason, now);
        } else if should_complete {
            self.finish_active("target_reached", now);
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

    fn finish_active(&mut self, reason: &str, now: DateTime<Utc>) {
        let Some(active) = self.active.take() else {
            return;
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
            candidate_applied_requests: active.candidate_applied_requests,
            candidate_skipped_requests: active.candidate_skipped_requests,
        };
        if summary.mode == CacheValidationMode::Baseline && summary.successful_requests > 0 {
            self.baseline_reference = Some(summary.clone());
        }
        self.last_run = Some(summary);
    }
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
        controller
            .configure(
                input(CacheValidationMode::Candidate),
                Some("A".to_string()),
                now,
            )
            .unwrap();

        assert!(controller
            .selection("provider-a", "gpt-main", now)
            .is_some());
        assert!(controller
            .selection("provider-b", "gpt-main", now)
            .is_none());
        assert!(controller
            .selection("provider-a", "gpt-side", now)
            .is_none());
        assert_eq!(
            controller
                .status(now + Duration::hours(VALIDATION_MAX_HOURS + 1))
                .mode,
            CacheValidationMode::Auto
        );
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
    fn baseline_reference_is_kept_for_candidate_comparison() {
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
        assert_eq!(status.baseline_reference.unwrap().successful_requests, 1);
    }
}
