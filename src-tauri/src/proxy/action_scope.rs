use std::sync::OnceLock;

use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::Channel;

const ACTION_SCOPE_VERSION: &str = "composite-action-scope-v2";

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

        let anchor_key = hash_parts(&[
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
        ]);

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
    fn binds_every_conversation_and_upstream_realm_dimension() {
        let baseline = CompositeActionScope::derive(input()).unwrap();
        let mut changed = input();
        changed.session_id = Some("session-b");
        assert_ne!(baseline, CompositeActionScope::derive(changed).unwrap());

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
