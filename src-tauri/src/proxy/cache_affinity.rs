use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Instant;

use super::affinity_identity::AffinityIdentity;

pub(crate) const SHADOW_POLICY_EPOCH: u64 = 1;
pub(super) const SHADOW_ASSIGNMENT_LIMIT: usize = 4096;
pub(super) const SHADOW_ASSIGNMENT_TTL_HOURS: i64 = 24;
pub(super) const STATIC_COHORT_CANARY_PERCENT: u8 = 5;
const AUTOMATIC_STATIC_COHORT_ADMISSION_ENABLED: bool = false;
const GIANT_TAIL_CHARS: u64 = 80_000;

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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ShadowAffinityStore {
    #[serde(default)]
    pub assignments: HashMap<String, ShadowAffinityAssignment>,
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
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ShadowObservationInput {
    pub success: bool,
    pub has_usage: bool,
    pub input_tokens: u64,
    pub cache_read_tokens: u64,
    pub giant_tail: bool,
    pub compaction_boundary: bool,
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
        };
    };

    let (realm_id, cohort_id, lane, arm, shard, policy_epoch, anchor_epoch) = {
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
    evict_assignments(store, now);
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
    if !AUTOMATIC_STATIC_COHORT_ADMISSION_ENABLED {
        if smart_hit_enabled
            && decision.lane == ShadowCacheLane::Steady
            && matches!(
                decision.decision.as_str(),
                "assigned" | "stateless_assigned"
            )
            && decision.arm == ShadowAffinityArm::Candidate
        {
            decision.decision = "candidate_shadow_only".to_string();
            decision.skip_reason = Some("awaiting_efficacy_evidence".to_string());
        }
        return false;
    }
    apply_static_cohort_canary(decision, smart_hit_enabled)
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

pub(super) fn reset_anchor(
    store: &mut ShadowAffinityStore,
    conversation_id: &str,
    now: DateTime<Utc>,
) {
    if let Some(assignment) = store.assignments.get_mut(conversation_id) {
        assignment.anchor_epoch = assignment.anchor_epoch.saturating_add(1);
        assignment.lane = ShadowCacheLane::CompactedAnchor;
        assignment.last_seen_at = now;
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

    #[test]
    fn assignments_are_sticky_and_missing_identity_is_transparent() {
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
        assert_eq!(first_decision.lane, second_decision.lane);
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

        let mut nonsteady = smart_cache_disabled.clone();
        nonsteady.lane = ShadowCacheLane::CompactedAnchor;
        assert!(!apply_automatic_static_cohort_canary(&mut nonsteady, true));
        assert_eq!(nonsteady.decision, "assigned");
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
            },
            now + Duration::seconds(4),
        );

        assert_eq!(
            store.assignments[&assignment_key].lane,
            ShadowCacheLane::ToolBurstQuarantine
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
