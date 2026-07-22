use sha2::{Digest, Sha256};

use crate::config::Channel;

use super::cache_control_core::FinalWireReceipt;

const FINAL_SCOPE_SHADOW_VERSION: u8 = 4;

/// Derives request-local evidence for a future final-scope cache controller.
/// This module is deliberately observe-only: it owns no mutable state and
/// cannot influence routing, waiting, retries, metrics, or persistence.
pub(super) struct FinalScopeShadow;

pub(super) struct FinalScopeShadowInput<'a> {
    pub(super) native_responses: bool,
    pub(super) active_channel: &'a Channel,
    pub(super) final_wire: &'a FinalWireReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FinalScopeShadowReceipt {
    pub(super) version: u8,
    pub(super) digest: String,
    /// This receipt may feed observe-only waterlines. It is deliberately not
    /// a promotion certificate: a future controller must also prove exact
    /// predecessor/input-prefix continuity for the current request.
    pub(super) eligible_for_shadow_observation: bool,
    pub(super) missing_attested_scope: bool,
    pub(super) missing_final_wire_static_projection: bool,
    /// A headless lineage lease still has an epoch and may establish the first
    /// baseline. `None` means this request cannot be separated from a later
    /// compaction or recreated session and therefore must remain ineligible.
    pub(super) missing_lineage_epoch: bool,
    pub(super) unsupported_evidence_version: bool,
    pub(super) ambiguous_breakpoint_placement: bool,
}

impl FinalScopeShadow {
    /// Returns no receipt for paths that are not native Responses. A receipt
    /// never becomes a decision in this phase.
    pub(super) fn derive(input: FinalScopeShadowInput<'_>) -> Option<FinalScopeShadowReceipt> {
        if !input.native_responses || !matches!(input.active_channel, Channel::Responses) {
            return None;
        }

        let scope = &input.final_wire.scope;
        let attested = scope.trusted_identity_source.as_deref() == Some("adapter-header");
        let scope_digest = scope.scope_digest.as_deref();
        let key_realm = scope.key_realm_digest.as_deref();
        let missing_attested_scope = !attested || scope_digest.is_none() || key_realm.is_none();
        let final_wire_static_projection = input
            .final_wire
            .wire
            .responses_static_projection_digest
            .as_deref();
        let missing_final_wire_static_projection = final_wire_static_projection.is_none();
        let missing_lineage_epoch = input.final_wire.semantic.lineage_epoch.is_none();
        let unsupported_evidence_version = scope.version != 1
            || input.final_wire.semantic.version != 2
            || input.final_wire.wire.version != 2;
        let ambiguous_breakpoint_placement = input.final_wire.cache_controls.breakpoint_present;
        let scope_version = scope.version.to_string();
        let wire_version = input.final_wire.wire.version.to_string();
        let semantic_version = input.final_wire.semantic.version.to_string();
        let cache_control_mask = input
            .final_wire
            .cache_controls
            .present_field_mask
            .to_string();
        let lineage_epoch = input
            .final_wire
            .semantic
            .lineage_epoch
            .map(|epoch| epoch.to_string())
            .unwrap_or_else(|| "none".to_string());
        let digest = hash_parts(&[
            "final-prefix-scope-shadow-v4",
            &scope_version,
            scope_digest.unwrap_or("unscoped"),
            key_realm.unwrap_or("unscoped-key"),
            &wire_version,
            final_wire_static_projection.unwrap_or("missing-final-wire-static-projection"),
            &cache_control_mask,
            &semantic_version,
            &input.final_wire.semantic.context_mode,
            &lineage_epoch,
        ]);

        Some(FinalScopeShadowReceipt {
            version: FINAL_SCOPE_SHADOW_VERSION,
            digest,
            eligible_for_shadow_observation: !missing_attested_scope
                && !missing_final_wire_static_projection
                && !missing_lineage_epoch
                && !unsupported_evidence_version
                && !ambiguous_breakpoint_placement,
            missing_attested_scope,
            missing_final_wire_static_projection,
            missing_lineage_epoch,
            unsupported_evidence_version,
            ambiguous_breakpoint_placement,
        })
    }
}

fn hash_parts(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part.as_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        metrics::ResponsesWirePrefixFingerprints,
        proxy::{
            action_scope::{ActionScopeInput, CompositeActionScope},
            cache_control_core::{
                ActionScopeReceipt, FinalCacheControls, PreparedWireDigest, SemanticLineageDigest,
            },
        },
    };

    fn receipt() -> FinalWireReceipt {
        FinalWireReceipt {
            scope: ActionScopeReceipt {
                version: 1,
                scope_digest: Some("scope-a".to_string()),
                channel: "responses".to_string(),
                key_realm_digest: Some("realm-a".to_string()),
                trusted_identity_source: Some("adapter-header".to_string()),
            },
            semantic: SemanticLineageDigest {
                version: 2,
                context_mode: "full_replay".to_string(),
                lineage_epoch: Some(3),
            },
            wire: PreparedWireDigest {
                version: 2,
                wire_bytes: 1,
                canonical_member_count: 1,
                responses_static_projection_digest: Some("static-projection-a".to_string()),
                outbound_prefix_fingerprints: None::<ResponsesWirePrefixFingerprints>,
            },
            cache_controls: FinalCacheControls {
                present_field_mask: 1,
                breakpoint_present: false,
            },
        }
    }

    fn input<'a>(receipt: &'a FinalWireReceipt) -> FinalScopeShadowInput<'a> {
        FinalScopeShadowInput {
            native_responses: true,
            active_channel: &Channel::Responses,
            final_wire: receipt,
        }
    }

    fn composite_scope(endpoint: &str) -> CompositeActionScope {
        CompositeActionScope::derive(ActionScopeInput {
            workspace_fingerprint: "workspace-a",
            agent_id: Some("codex"),
            provider_id: "provider-a",
            endpoint,
            resolved_model: "gpt-5.6-sol",
            channel: &Channel::Responses,
            key_realm_id: "realm-a",
            thread_id: Some("thread-a"),
            conversation_id: None,
            session_id: Some("session-a"),
            adapter_attested: true,
            identity_source: "adapter-header",
        })
        .expect("complete attested scope should derive")
    }

    #[test]
    fn final_scope_is_stable_for_the_same_final_tuple() {
        let receipt = receipt();
        let first = FinalScopeShadow::derive(input(&receipt)).unwrap();
        let second = FinalScopeShadow::derive(input(&receipt)).unwrap();

        assert_eq!(first, second);
        assert!(first.eligible_for_shadow_observation);
        assert!(!first.missing_attested_scope);
        assert!(!first.missing_final_wire_static_projection);
        assert!(!first.missing_lineage_epoch);
    }

    #[test]
    fn equivalent_endpoint_spellings_share_the_composite_scope_without_a_raw_url_split() {
        let without_slash = composite_scope("https://example.test/v1/responses");
        let with_slash = composite_scope("https://example.test/v1/responses/");
        assert_eq!(without_slash.anchor_key, with_slash.anchor_key);

        let mut first_receipt = receipt();
        first_receipt.scope.scope_digest = Some(without_slash.anchor_key);
        let mut second_receipt = receipt();
        second_receipt.scope.scope_digest = Some(with_slash.anchor_key);

        assert_eq!(
            FinalScopeShadow::derive(input(&first_receipt))
                .unwrap()
                .digest,
            FinalScopeShadow::derive(input(&second_receipt))
                .unwrap()
                .digest
        );

        let different = composite_scope("https://other.test/v1/responses");
        let mut different_receipt = receipt();
        different_receipt.scope.scope_digest = Some(different.anchor_key);
        assert_ne!(
            FinalScopeShadow::derive(input(&first_receipt))
                .unwrap()
                .digest,
            FinalScopeShadow::derive(input(&different_receipt))
                .unwrap()
                .digest
        );
    }

    #[test]
    fn appended_tail_changes_exact_wire_without_splitting_the_stable_scope() {
        let first_receipt = receipt();
        let first = FinalScopeShadow::derive(input(&first_receipt)).unwrap();
        let mut appended_receipt = receipt();
        appended_receipt.wire.wire_bytes = 4_096;
        let appended = FinalScopeShadow::derive(input(&appended_receipt)).unwrap();

        assert_eq!(first.digest, appended.digest);
        assert!(appended.eligible_for_shadow_observation);
    }

    #[test]
    fn final_scope_splits_actual_realm_cache_and_context_dimensions() {
        let baseline_receipt = receipt();
        let baseline = FinalScopeShadow::derive(input(&baseline_receipt)).unwrap();

        let mut changed_realm = receipt();
        changed_realm.scope.key_realm_digest = Some("realm-b".to_string());
        assert_ne!(
            baseline.digest,
            FinalScopeShadow::derive(input(&changed_realm))
                .unwrap()
                .digest
        );

        let mut changed_cache = receipt();
        changed_cache.cache_controls.present_field_mask = 2;
        assert_ne!(
            baseline.digest,
            FinalScopeShadow::derive(input(&changed_cache))
                .unwrap()
                .digest
        );

        let mut changed_context = receipt();
        changed_context.semantic.context_mode = "verified_native_delta".to_string();
        assert_ne!(
            baseline.digest,
            FinalScopeShadow::derive(input(&changed_context))
                .unwrap()
                .digest
        );

        let mut changed_epoch = receipt();
        changed_epoch.semantic.lineage_epoch = Some(4);
        assert_ne!(
            baseline.digest,
            FinalScopeShadow::derive(input(&changed_epoch))
                .unwrap()
                .digest
        );

        let mut changed_static_projection = receipt();
        changed_static_projection
            .wire
            .responses_static_projection_digest = Some("static-projection-b".to_string());
        assert_ne!(
            baseline.digest,
            FinalScopeShadow::derive(input(&changed_static_projection))
                .unwrap()
                .digest
        );
    }

    #[test]
    fn final_scope_refuses_promotion_without_attestation_or_precise_breakpoint_placement() {
        let mut missing_scope = receipt();
        missing_scope.scope.trusted_identity_source = Some("agent-client-metadata".to_string());
        let missing = FinalScopeShadow::derive(input(&missing_scope)).unwrap();
        assert!(missing.missing_attested_scope);
        assert!(!missing.eligible_for_shadow_observation);

        let mut missing_static_projection = receipt();
        missing_static_projection
            .wire
            .responses_static_projection_digest = None;
        let missing = FinalScopeShadow::derive(input(&missing_static_projection)).unwrap();
        assert!(missing.missing_final_wire_static_projection);
        assert!(!missing.eligible_for_shadow_observation);

        let mut breakpoint = receipt();
        breakpoint.cache_controls.breakpoint_present = true;
        let ambiguous = FinalScopeShadow::derive(input(&breakpoint)).unwrap();
        assert!(ambiguous.ambiguous_breakpoint_placement);
        assert!(!ambiguous.eligible_for_shadow_observation);

        let mut future_wire_version = receipt();
        future_wire_version.wire.version = 3;
        let unsupported = FinalScopeShadow::derive(input(&future_wire_version)).unwrap();
        assert!(unsupported.unsupported_evidence_version);
        assert!(!unsupported.eligible_for_shadow_observation);

        let mut missing_epoch = receipt();
        missing_epoch.semantic.lineage_epoch = None;
        let missing = FinalScopeShadow::derive(input(&missing_epoch)).unwrap();
        assert!(missing.missing_lineage_epoch);
        assert!(!missing.eligible_for_shadow_observation);
    }

    #[test]
    fn absent_and_zero_lineage_epochs_are_distinct() {
        let mut absent = receipt();
        absent.semantic.lineage_epoch = None;
        let absent = FinalScopeShadow::derive(input(&absent)).unwrap();
        let mut zero = receipt();
        zero.semantic.lineage_epoch = Some(0);
        let zero = FinalScopeShadow::derive(input(&zero)).unwrap();

        assert_ne!(absent.digest, zero.digest);
        assert!(!absent.eligible_for_shadow_observation);
        assert!(zero.eligible_for_shadow_observation);
    }

    #[test]
    fn non_native_or_non_responses_paths_do_not_create_shadow_scope() {
        let receipt = receipt();
        let mut non_native = input(&receipt);
        non_native.native_responses = false;
        assert!(FinalScopeShadow::derive(non_native).is_none());

        let chat = FinalScopeShadowInput {
            active_channel: &Channel::Chat,
            ..input(&receipt)
        };
        assert!(FinalScopeShadow::derive(chat).is_none());
    }
}
