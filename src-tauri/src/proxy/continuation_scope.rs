use super::action_scope::CompositeActionScope;

/// Local lineage identity. It is deliberately derived from the action scope
/// only, so it can guard a manual route switch or compaction boundary without
/// becoming an upstream Responses continuation token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContinuationScope {
    pub anchor_key: String,
}

impl ContinuationScope {
    pub(super) fn from_action_scope(scope: &CompositeActionScope) -> Self {
        Self {
            anchor_key: scope.anchor_key.clone(),
        }
    }
}
