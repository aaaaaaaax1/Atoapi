use serde_json::Value;

use crate::config::{Channel, ProviderConfig};

use super::{prepared_wire_request::PreparedWireRequest, upstream_affinity::UpstreamAffinityScope};

#[derive(Debug)]
pub(super) struct RequestPlan {
    url: String,
    channel: Channel,
    use_system_proxy: bool,
    explicit_proxy_url: Option<String>,
    custom_user_agent: Option<String>,
    request_body_gzip_enabled: bool,
    upstream_affinity_scope: Option<UpstreamAffinityScope>,
    wire: PreparedWireRequest,
}

/// A frozen request plan that can cross the strict data-plane transport seam
/// exactly once. It intentionally has no clone or borrow escape hatch.
#[must_use = "dispatch the frozen plan through OneShotTransport or deliberately drop it"]
#[derive(Debug)]
pub(super) struct OneShotRequestPlan {
    plan: RequestPlan,
}

impl RequestPlan {
    pub(super) fn new(
        provider: &ProviderConfig,
        url: impl Into<String>,
        channel: Channel,
        body: &Value,
    ) -> Self {
        let wire = PreparedWireRequest::from_value(&channel, body);
        Self::from_prepared(provider, url, wire)
    }

    pub(super) fn from_prepared(
        provider: &ProviderConfig,
        url: impl Into<String>,
        wire: PreparedWireRequest,
    ) -> Self {
        Self {
            url: url.into(),
            channel: wire.channel().clone(),
            use_system_proxy: provider.use_system_proxy,
            explicit_proxy_url: None,
            custom_user_agent: provider.custom_user_agent.clone(),
            request_body_gzip_enabled: provider.request_body_gzip_enabled,
            upstream_affinity_scope: None,
            wire,
        }
    }

    pub(super) fn with_gzip_enabled(mut self, enabled: bool) -> Self {
        self.request_body_gzip_enabled = enabled;
        self
    }

    pub(super) fn with_explicit_proxy_url(mut self, proxy_url: Option<String>) -> Self {
        self.explicit_proxy_url = self.use_system_proxy.then_some(proxy_url).flatten();
        self
    }

    /// Carries only an opaque, trusted, process-scoped transport affinity
    /// scope. The upstream affinity value itself remains in AppState and never
    /// enters this frozen request, its JSON wire, logs, or persistence.
    pub(super) fn with_upstream_affinity_scope(
        mut self,
        scope: Option<UpstreamAffinityScope>,
    ) -> Self {
        self.upstream_affinity_scope = scope;
        self
    }

    /// Moves this frozen plan into the strict dispatch interface. Once moved,
    /// the caller no longer has a reusable RequestPlan for another POST.
    pub(super) fn into_one_shot(self) -> OneShotRequestPlan {
        OneShotRequestPlan { plan: self }
    }

    pub(super) fn url(&self) -> &str {
        &self.url
    }

    pub(super) fn channel(&self) -> &Channel {
        &self.channel
    }

    pub(super) fn use_system_proxy(&self) -> bool {
        self.use_system_proxy
    }

    pub(super) fn explicit_proxy_url(&self) -> Option<&str> {
        self.explicit_proxy_url.as_deref()
    }

    pub(super) fn custom_user_agent(&self) -> Option<&str> {
        self.custom_user_agent.as_deref()
    }

    pub(super) fn request_body_gzip_enabled(&self) -> bool {
        self.request_body_gzip_enabled
    }

    pub(super) fn upstream_affinity_scope(&self) -> Option<&UpstreamAffinityScope> {
        self.upstream_affinity_scope.as_ref()
    }

    pub(super) fn wire(&self) -> &PreparedWireRequest {
        &self.wire
    }

    pub(super) fn body_len(&self) -> usize {
        self.wire.len()
    }
}

impl OneShotRequestPlan {
    pub(super) fn url(&self) -> &str {
        self.plan.url()
    }

    pub(super) fn channel(&self) -> &Channel {
        self.plan.channel()
    }

    pub(super) fn use_system_proxy(&self) -> bool {
        self.plan.use_system_proxy()
    }

    pub(super) fn explicit_proxy_url(&self) -> Option<&str> {
        self.plan.explicit_proxy_url()
    }

    pub(super) fn custom_user_agent(&self) -> Option<&str> {
        self.plan.custom_user_agent()
    }

    pub(super) fn request_body_gzip_enabled(&self) -> bool {
        self.plan.request_body_gzip_enabled()
    }

    pub(super) fn upstream_affinity_scope(&self) -> Option<&UpstreamAffinityScope> {
        self.plan.upstream_affinity_scope()
    }

    pub(super) fn wire(&self) -> &PreparedWireRequest {
        self.plan.wire()
    }

    #[cfg(test)]
    pub(super) fn body_len(&self) -> usize {
        self.plan.body_len()
    }
}
