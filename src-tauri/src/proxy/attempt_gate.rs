use thiserror::Error;
use uuid::Uuid;

/// Agent requests always receive exactly one upstream-send authorization.
/// Compatibility decisions are persisted for a later independent inbound
/// request; they never expand this request's attempt budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AttemptPolicy {
    Single,
}

impl AttemptPolicy {
    pub(super) const fn budget(self) -> u8 {
        1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AttemptReason {
    Primary,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(super) enum AttemptGateError {
    #[error("inbound request id must not be empty")]
    EmptyInboundRequestId,
    #[error("the primary attempt has already been issued")]
    PrimaryAlreadyIssued,
}

/// A single authorization to perform one upstream HTTP POST.
///
/// This type intentionally does not implement `Clone`. Its fields are private
/// so callers cannot manufacture another authorization from its metadata.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct AttemptToken {
    inbound_request_id: String,
    attempt_id: String,
    reason: AttemptReason,
}

impl AttemptToken {
    pub(super) fn inbound_request_id(&self) -> &str {
        &self.inbound_request_id
    }

    pub(super) fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    pub(super) const fn index(&self) -> u8 {
        1
    }

    pub(super) const fn reason(&self) -> AttemptReason {
        self.reason
    }
}

/// Issues the single non-cloneable attempt token for one inbound request.
/// Issuing a token is final even if the caller later drops it; a transport
/// failure cannot silently reopen the upstream-send authorization.
#[derive(Debug)]
pub(super) struct AttemptGate {
    inbound_request_id: String,
    primary_issued: bool,
}

impl AttemptGate {
    pub(super) fn new(
        inbound_request_id: impl Into<String>,
        _policy: AttemptPolicy,
    ) -> Result<Self, AttemptGateError> {
        let inbound_request_id = inbound_request_id.into();
        if inbound_request_id.trim().is_empty() {
            return Err(AttemptGateError::EmptyInboundRequestId);
        }

        Ok(Self {
            inbound_request_id,
            primary_issued: false,
        })
    }

    pub(super) fn primary(&mut self) -> Result<AttemptToken, AttemptGateError> {
        if self.primary_issued {
            return Err(AttemptGateError::PrimaryAlreadyIssued);
        }

        self.primary_issued = true;
        Ok(AttemptToken {
            inbound_request_id: self.inbound_request_id.clone(),
            attempt_id: Uuid::new_v4().to_string(),
            reason: AttemptReason::Primary,
        })
    }

    pub(super) const fn policy(&self) -> AttemptPolicy {
        AttemptPolicy::Single
    }

    pub(super) const fn budget(&self) -> u8 {
        AttemptPolicy::Single.budget()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_inbound_request_ids() {
        for inbound_request_id in ["", "   "] {
            assert_eq!(
                AttemptGate::new(inbound_request_id, AttemptPolicy::Single).unwrap_err(),
                AttemptGateError::EmptyInboundRequestId
            );
        }
    }

    #[test]
    fn single_policy_issues_exactly_one_primary_token() {
        let mut gate = AttemptGate::new("inbound-1", AttemptPolicy::Single).unwrap();

        assert_eq!(gate.policy(), AttemptPolicy::Single);
        assert_eq!(gate.budget(), 1);
        let token = gate.primary().unwrap();
        assert_eq!(token.inbound_request_id(), "inbound-1");
        assert!(!token.attempt_id().is_empty());
        assert_eq!(token.index(), 1);
        assert_eq!(token.reason(), AttemptReason::Primary);
        assert_eq!(
            gate.primary().unwrap_err(),
            AttemptGateError::PrimaryAlreadyIssued
        );
    }

    #[test]
    fn tokens_are_unique_even_for_the_same_inbound_id() {
        let mut first = AttemptGate::new("same-inbound", AttemptPolicy::Single).unwrap();
        let mut second = AttemptGate::new("same-inbound", AttemptPolicy::Single).unwrap();

        let first_primary = first.primary().unwrap();
        let second_primary = second.primary().unwrap();
        assert_ne!(first_primary.attempt_id(), second_primary.attempt_id());
        assert_eq!(first_primary.inbound_request_id(), "same-inbound");
        assert_eq!(second_primary.inbound_request_id(), "same-inbound");
    }

    #[test]
    fn dropping_an_issued_token_does_not_restore_authorization() {
        let mut gate = AttemptGate::new("inbound-2", AttemptPolicy::Single).unwrap();
        drop(gate.primary().unwrap());

        assert_eq!(
            gate.primary().unwrap_err(),
            AttemptGateError::PrimaryAlreadyIssued
        );
    }
}
