use std::sync::OnceLock;

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::Channel;

const ACTION_SCOPE_VERSION: &str = "canonical-action-scope-v3";

/// The evidence needed before a request may inherit state that can alter a
/// future upstream request. Placement hints such as `prompt_cache_key` are
/// intentionally absent from this input.
pub(super) struct ActionScopeInput<'a> {
    pub(super) workspace_fingerprint: &'a str,
    pub(super) agent_id: Option<&'a str>,
    pub(super) provider_id: &'a str,
    pub(super) endpoint: &'a str,
    pub(super) resolved_model: &'a str,
    pub(super) channel: &'a Channel,
    pub(super) key_realm_id: &'a str,
    pub(super) thread_id: Option<&'a str>,
    pub(super) conversation_id: Option<&'a str>,
    pub(super) session_id: Option<&'a str>,
    pub(super) adapter_attested: bool,
    pub(super) identity_source: &'static str,
}

/// A strict, process-bounded identity for continuation, cache-control waits,
/// and future Native Responses delta eligibility. The digest contains no raw
/// conversation IDs or API keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompositeActionScope {
    pub(super) anchor_key: String,
    pub(super) key_realm_id: String,
    pub(super) identity_source: &'static str,
}

impl CompositeActionScope {
    pub(super) fn derive(input: ActionScopeInput<'_>) -> Option<Self> {
        if !input.adapter_attested {
            return None;
        }
        let workspace = non_empty(input.workspace_fingerprint)?;
        let agent_id = non_empty(input.agent_id?)?;
        let provider_id = non_empty(input.provider_id)?;
        let endpoint = normalized_endpoint(input.endpoint)?;
        let model = non_empty(input.resolved_model)?;
        let key_realm_id = non_empty(input.key_realm_id)?;
        let thread_id = optional_identity(input.thread_id)?;
        let conversation_id = optional_identity(input.conversation_id)?;
        let session_id = optional_identity(input.session_id)?;
        if thread_id.is_none() && conversation_id.is_none() && session_id.is_none() {
            return None;
        }

        let anchor_key = if matches!(input.channel, Channel::Responses) {
            // Keep only attested native Codex Responses aligned with the
            // cache-placement identity: the first present Codex dimension is
            // canonical, while secondary IDs may rotate between turns.
            let (canonical_identity_kind, canonical_identity_value) = canonical_identity(
                thread_id.as_deref(),
                conversation_id.as_deref(),
                session_id.as_deref(),
            )?;
            hash_parts(&[
                ACTION_SCOPE_VERSION,
                current_boot_epoch(),
                workspace,
                agent_id,
                provider_id,
                endpoint.as_str(),
                model,
                input.channel.label(),
                key_realm_id,
                canonical_identity_kind,
                canonical_identity_value,
                input.identity_source,
            ])
        } else {
            hash_parts(&[
                ACTION_SCOPE_VERSION,
                current_boot_epoch(),
                workspace,
                agent_id,
                provider_id,
                endpoint.as_str(),
                model,
                input.channel.label(),
                key_realm_id,
                thread_id.as_deref().unwrap_or(""),
                conversation_id.as_deref().unwrap_or(""),
                session_id.as_deref().unwrap_or(""),
                input.identity_source,
            ])
        };

        Some(Self {
            anchor_key,
            key_realm_id: key_realm_id.to_string(),
            identity_source: input.identity_source,
        })
    }
}

fn current_boot_epoch() -> &'static str {
    static EPOCH: OnceLock<String> = OnceLock::new();
    EPOCH.get_or_init(|| format!("boot-{}", Uuid::new_v4()))
}

fn normalized_endpoint(value: &str) -> Option<String> {
    let value = non_empty(value)?;
    Some(
        reqwest::Url::parse(value)
            .map(|url| url.to_string().trim_end_matches('/').to_string())
            .unwrap_or_else(|_| value.trim_end_matches('/').to_string()),
    )
}

fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    (!value.is_empty()).then_some(value)
}

fn optional_identity(value: Option<&str>) -> Option<Option<String>> {
    let Some(raw) = value else {
        return Some(None);
    };
    let value = raw.trim();
    (!value.is_empty()
        && value.len() <= 512
        && value.len() == raw.len()
        && !raw.chars().any(char::is_control))
    .then(|| Some(raw.to_string()))
}

fn canonical_identity<'a>(
    thread_id: Option<&'a str>,
    conversation_id: Option<&'a str>,
    session_id: Option<&'a str>,
) -> Option<(&'static str, &'a str)> {
    thread_id
        .map(|value| ("thread-id", value))
        .or_else(|| conversation_id.map(|value| ("conversation-id", value)))
        .or_else(|| session_id.map(|value| ("session-id", value)))
}

fn hash_parts(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input<'a>() -> ActionScopeInput<'a> {
        ActionScopeInput {
            workspace_fingerprint: "workspace-a",
            agent_id: Some("codex"),
            provider_id: "provider-a",
            endpoint: "https://example.test/v1/responses",
            resolved_model: "gpt-5.6-sol",
            channel: &Channel::Responses,
            key_realm_id: "key-realm-a",
            thread_id: Some("thread-a"),
            conversation_id: Some("conversation-a"),
            session_id: Some("session-a"),
            adapter_attested: true,
            identity_source: "adapter-header",
        }
    }

    #[test]
    fn rejects_unattested_or_incomplete_identity() {
        let mut untrusted = input();
        untrusted.adapter_attested = false;
        assert!(CompositeActionScope::derive(untrusted).is_none());

        let mut missing_identity = input();
        missing_identity.thread_id = None;
        missing_identity.conversation_id = None;
        missing_identity.session_id = None;
        assert!(CompositeActionScope::derive(missing_identity).is_none());

        let too_long = "x".repeat(513);
        let mut malformed_dimension = input();
        malformed_dimension.thread_id = Some(&too_long);
        assert!(
            CompositeActionScope::derive(malformed_dimension).is_none(),
            "a supplied invalid dimension must not collapse onto the remaining session identity"
        );

        let nul_identity = "thread\0other";
        let mut control_character = input();
        control_character.thread_id = Some(nul_identity);
        assert!(CompositeActionScope::derive(control_character).is_none());

        let padded_identity = "thread-a\t";
        let mut trailing_control = input();
        trailing_control.thread_id = Some(padded_identity);
        assert!(CompositeActionScope::derive(trailing_control).is_none());
    }

    #[test]
    fn binds_canonical_identity_and_upstream_realm_dimension() {
        let baseline = CompositeActionScope::derive(input()).unwrap();
        let mut changed = input();
        changed.session_id = Some("session-b");
        assert_eq!(
            baseline,
            CompositeActionScope::derive(changed).unwrap(),
            "secondary IDs must not split a stable attested thread scope"
        );

        let mut changed = input();
        changed.conversation_id = Some("conversation-b");
        assert_eq!(
            baseline,
            CompositeActionScope::derive(changed).unwrap(),
            "secondary IDs must not split a stable attested thread scope"
        );

        let mut changed = input();
        changed.agent_id = Some("other-agent");
        assert_ne!(baseline, CompositeActionScope::derive(changed).unwrap());

        let mut changed = input();
        changed.endpoint = "https://other.test/v1/responses";
        assert_ne!(baseline, CompositeActionScope::derive(changed).unwrap());

        let mut changed = input();
        changed.resolved_model = "gpt-5.6-terra";
        assert_ne!(baseline, CompositeActionScope::derive(changed).unwrap());

        let mut changed = input();
        changed.key_realm_id = "key-realm-b";
        assert_ne!(baseline, CompositeActionScope::derive(changed).unwrap());
    }

    #[test]
    fn falls_back_to_conversation_then_session_when_primary_identity_is_absent() {
        let mut conversation = input();
        conversation.thread_id = None;
        conversation.session_id = None;
        let conversation_scope = CompositeActionScope::derive(conversation).unwrap();

        let mut changed_secondary = input();
        changed_secondary.thread_id = None;
        changed_secondary.session_id = Some("session-b");
        assert_eq!(
            conversation_scope,
            CompositeActionScope::derive(changed_secondary).unwrap()
        );

        let mut changed_conversation = input();
        changed_conversation.thread_id = None;
        changed_conversation.session_id = None;
        changed_conversation.conversation_id = Some("conversation-b");
        assert_ne!(
            conversation_scope,
            CompositeActionScope::derive(changed_conversation).unwrap()
        );

        let mut session = input();
        session.thread_id = None;
        session.conversation_id = None;
        let session_scope = CompositeActionScope::derive(session).unwrap();

        let mut changed_session = input();
        changed_session.thread_id = None;
        changed_session.conversation_id = None;
        changed_session.session_id = Some("session-b");
        assert_ne!(
            session_scope,
            CompositeActionScope::derive(changed_session).unwrap()
        );
    }

    #[test]
    fn non_native_channels_keep_strict_composite_identity() {
        let chat = Channel::Chat;
        let mut baseline = input();
        baseline.channel = &chat;
        let baseline = CompositeActionScope::derive(baseline).unwrap();

        let mut changed = input();
        changed.channel = &chat;
        changed.session_id = Some("session-b");
        assert_ne!(baseline, CompositeActionScope::derive(changed).unwrap());
    }

    #[test]
    fn endpoint_normalization_does_not_split_the_same_deployment() {
        let baseline = CompositeActionScope::derive(input()).unwrap();
        let mut normalized = input();
        normalized.endpoint = "https://example.test/v1/responses/";
        assert_eq!(baseline, CompositeActionScope::derive(normalized).unwrap());
    }

    #[test]
    fn scope_hash_uses_unambiguous_length_prefixes() {
        assert_ne!(hash_parts(&["a\0b", ""]), hash_parts(&["a", "b\0"]));
    }
}
