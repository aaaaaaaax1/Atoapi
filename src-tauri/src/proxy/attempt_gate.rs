use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AttemptPolicy {
    Single,
    ReasoningCompatibility,
}

impl AttemptPolicy {
    pub(super) const fn budget(self) -> u8 {
        match self {
            Self::Single => 1,
            Self::ReasoningCompatibility => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AttemptReason {
    Primary,
    ReasoningExplicit,
    ReasoningOpaque502,
}

/// Evidence that the existing reasoning-compatibility classifier has already
/// accepted. The gate deliberately does not classify provider responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReasoningEvidence {
    ExplicitRejection,
    Opaque502Probe,
}

impl ReasoningEvidence {
    const fn reason(self) -> AttemptReason {
        match self {
            Self::ExplicitRejection => AttemptReason::ReasoningExplicit,
            Self::Opaque502Probe => AttemptReason::ReasoningOpaque502,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub(super) enum AttemptGateError {
    #[error("inbound request id must not be empty")]
    EmptyInboundRequestId,
    #[error("the primary attempt has already been issued")]
    PrimaryAlreadyIssued,
    #[error("the primary attempt must be issued before a reasoning compatibility attempt")]
    PrimaryRequired,
    #[error("attempt policy does not permit a reasoning compatibility attempt")]
    ReasoningCompatibilityDenied,
    #[error("attempt budget of {budget} has been exhausted")]
    BudgetExhausted { budget: u8 },
}

/// A single authorization to perform one upstream HTTP POST.
///
/// This type intentionally does not implement `Clone`. Its fields are private
/// so callers cannot manufacture another authorization from its metadata.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct AttemptToken {
    inbound_request_id: String,
    attempt_id: String,
    index: u8,
    budget: u8,
    policy: AttemptPolicy,
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
        self.index
    }

    pub(super) const fn budget(&self) -> u8 {
        self.budget
    }

    pub(super) const fn policy(&self) -> AttemptPolicy {
        self.policy
    }

    pub(super) const fn reason(&self) -> AttemptReason {
        self.reason
    }
}

/// Issues a bounded sequence of non-cloneable attempt tokens for one inbound
/// request. Issuing a token consumes budget even if the caller later drops it;
/// transport failures therefore cannot silently reopen the budget.
#[derive(Debug)]
pub(super) struct AttemptGate {
    inbound_request_id: String,
    policy: AttemptPolicy,
    issued: u8,
    primary_issued: bool,
}

impl AttemptGate {
    pub(super) fn new(
        inbound_request_id: impl Into<String>,
        policy: AttemptPolicy,
    ) -> Result<Self, AttemptGateError> {
        let inbound_request_id = inbound_request_id.into();
        if inbound_request_id.trim().is_empty() {
            return Err(AttemptGateError::EmptyInboundRequestId);
        }

        Ok(Self {
            inbound_request_id,
            policy,
            issued: 0,
            primary_issued: false,
        })
    }

    pub(super) fn primary(&mut self) -> Result<AttemptToken, AttemptGateError> {
        if self.issued >= self.policy.budget() {
            return Err(self.budget_exhausted());
        }
        if self.primary_issued {
            return Err(AttemptGateError::PrimaryAlreadyIssued);
        }

        self.primary_issued = true;
        Ok(self.issue(AttemptReason::Primary))
    }

    pub(super) fn reasoning_compatibility(
        &mut self,
        evidence: ReasoningEvidence,
    ) -> Result<AttemptToken, AttemptGateError> {
        if self.policy != AttemptPolicy::ReasoningCompatibility {
            return Err(AttemptGateError::ReasoningCompatibilityDenied);
        }
        if self.issued >= self.policy.budget() {
            return Err(self.budget_exhausted());
        }
        if !self.primary_issued {
            return Err(AttemptGateError::PrimaryRequired);
        }

        Ok(self.issue(evidence.reason()))
    }

    pub(super) const fn policy(&self) -> AttemptPolicy {
        self.policy
    }

    pub(super) const fn budget(&self) -> u8 {
        self.policy.budget()
    }

    pub(super) const fn issued_attempts(&self) -> u8 {
        self.issued
    }

    pub(super) const fn remaining_attempts(&self) -> u8 {
        self.policy.budget().saturating_sub(self.issued)
    }

    fn issue(&mut self, reason: AttemptReason) -> AttemptToken {
        self.issued += 1;
        AttemptToken {
            inbound_request_id: self.inbound_request_id.clone(),
            attempt_id: Uuid::new_v4().to_string(),
            index: self.issued,
            budget: self.policy.budget(),
            policy: self.policy,
            reason,
        }
    }

    const fn budget_exhausted(&self) -> AttemptGateError {
        AttemptGateError::BudgetExhausted {
            budget: self.policy.budget(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_inbound_request_ids() {
        assert_eq!(
            AttemptGate::new("", AttemptPolicy::Single).unwrap_err(),
            AttemptGateError::EmptyInboundRequestId
        );
        assert_eq!(
            AttemptGate::new("   ", AttemptPolicy::ReasoningCompatibility).unwrap_err(),
            AttemptGateError::EmptyInboundRequestId
        );
    }

    #[test]
    fn single_policy_issues_exactly_one_primary_token() {
        let mut gate = AttemptGate::new("inbound-1", AttemptPolicy::Single).unwrap();

        assert_eq!(gate.policy(), AttemptPolicy::Single);
        assert_eq!(gate.budget(), 1);
        assert_eq!(gate.issued_attempts(), 0);
        assert_eq!(gate.remaining_attempts(), 1);

        let token = gate.primary().unwrap();
        assert_eq!(token.inbound_request_id(), "inbound-1");
        assert!(!token.attempt_id().is_empty());
        assert_eq!(token.index(), 1);
        assert_eq!(token.budget(), 1);
        assert_eq!(token.policy(), AttemptPolicy::Single);
        assert_eq!(token.reason(), AttemptReason::Primary);
        assert_eq!(gate.issued_attempts(), 1);
        assert_eq!(gate.remaining_attempts(), 0);

        assert_eq!(
            gate.primary().unwrap_err(),
            AttemptGateError::BudgetExhausted { budget: 1 }
        );
        assert_eq!(
            gate.reasoning_compatibility(ReasoningEvidence::ExplicitRejection)
                .unwrap_err(),
            AttemptGateError::ReasoningCompatibilityDenied
        );
        assert_eq!(gate.issued_attempts(), 1);
    }

    #[test]
    fn reasoning_policy_requires_primary_before_explicit_evidence() {
        let mut gate =
            AttemptGate::new("inbound-2", AttemptPolicy::ReasoningCompatibility).unwrap();

        assert_eq!(
            gate.reasoning_compatibility(ReasoningEvidence::ExplicitRejection)
                .unwrap_err(),
            AttemptGateError::PrimaryRequired
        );
        assert_eq!(gate.issued_attempts(), 0);

        let primary = gate.primary().unwrap();
        let fallback = gate
            .reasoning_compatibility(ReasoningEvidence::ExplicitRejection)
            .unwrap();

        assert_eq!(primary.inbound_request_id(), fallback.inbound_request_id());
        assert_ne!(primary.attempt_id(), fallback.attempt_id());
        assert_eq!(primary.index(), 1);
        assert_eq!(fallback.index(), 2);
        assert_eq!(primary.budget(), 2);
        assert_eq!(fallback.budget(), 2);
        assert_eq!(primary.reason(), AttemptReason::Primary);
        assert_eq!(fallback.reason(), AttemptReason::ReasoningExplicit);
        assert_eq!(gate.remaining_attempts(), 0);
    }

    #[test]
    fn reasoning_policy_requires_primary_before_opaque_502_evidence() {
        let mut gate =
            AttemptGate::new("inbound-3", AttemptPolicy::ReasoningCompatibility).unwrap();

        assert_eq!(
            gate.reasoning_compatibility(ReasoningEvidence::Opaque502Probe)
                .unwrap_err(),
            AttemptGateError::PrimaryRequired
        );

        let primary = gate.primary().unwrap();
        let fallback = gate
            .reasoning_compatibility(ReasoningEvidence::Opaque502Probe)
            .unwrap();

        assert_eq!(primary.inbound_request_id(), "inbound-3");
        assert_eq!(fallback.inbound_request_id(), "inbound-3");
        assert_ne!(primary.attempt_id(), fallback.attempt_id());
        assert_eq!(fallback.index(), 2);
        assert_eq!(fallback.reason(), AttemptReason::ReasoningOpaque502);
    }

    #[test]
    fn duplicate_primary_is_rejected_without_consuming_reasoning_budget() {
        let mut gate =
            AttemptGate::new("inbound-4", AttemptPolicy::ReasoningCompatibility).unwrap();

        let _primary = gate.primary().unwrap();
        assert_eq!(
            gate.primary().unwrap_err(),
            AttemptGateError::PrimaryAlreadyIssued
        );
        assert_eq!(gate.issued_attempts(), 1);
        assert_eq!(gate.remaining_attempts(), 1);

        let fallback = gate
            .reasoning_compatibility(ReasoningEvidence::ExplicitRejection)
            .unwrap();
        assert_eq!(fallback.index(), 2);
    }

    #[test]
    fn every_third_token_is_rejected_after_explicit_fallback() {
        let mut gate =
            AttemptGate::new("inbound-5", AttemptPolicy::ReasoningCompatibility).unwrap();
        let _primary = gate.primary().unwrap();
        let _fallback = gate
            .reasoning_compatibility(ReasoningEvidence::ExplicitRejection)
            .unwrap();

        for error in [
            gate.primary().unwrap_err(),
            gate.reasoning_compatibility(ReasoningEvidence::ExplicitRejection)
                .unwrap_err(),
            gate.reasoning_compatibility(ReasoningEvidence::Opaque502Probe)
                .unwrap_err(),
        ] {
            assert_eq!(error, AttemptGateError::BudgetExhausted { budget: 2 });
        }
        assert_eq!(gate.issued_attempts(), 2);
        assert_eq!(gate.remaining_attempts(), 0);
    }

    #[test]
    fn every_third_token_is_rejected_after_opaque_502_fallback() {
        let mut gate =
            AttemptGate::new("inbound-6", AttemptPolicy::ReasoningCompatibility).unwrap();
        let _primary = gate.primary().unwrap();
        let _fallback = gate
            .reasoning_compatibility(ReasoningEvidence::Opaque502Probe)
            .unwrap();

        assert_eq!(
            gate.reasoning_compatibility(ReasoningEvidence::ExplicitRejection)
                .unwrap_err(),
            AttemptGateError::BudgetExhausted { budget: 2 }
        );
        assert_eq!(gate.issued_attempts(), 2);
    }

    #[test]
    fn tokens_have_unique_attempt_ids_even_for_the_same_inbound_id() {
        let mut first =
            AttemptGate::new("same-inbound", AttemptPolicy::ReasoningCompatibility).unwrap();
        let mut second = AttemptGate::new("same-inbound", AttemptPolicy::Single).unwrap();

        let first_primary = first.primary().unwrap();
        let first_fallback = first
            .reasoning_compatibility(ReasoningEvidence::ExplicitRejection)
            .unwrap();
        let second_primary = second.primary().unwrap();

        assert_ne!(first_primary.attempt_id(), first_fallback.attempt_id());
        assert_ne!(first_primary.attempt_id(), second_primary.attempt_id());
        assert_ne!(first_fallback.attempt_id(), second_primary.attempt_id());
        assert_eq!(first_primary.inbound_request_id(), "same-inbound");
        assert_eq!(first_fallback.inbound_request_id(), "same-inbound");
        assert_eq!(second_primary.inbound_request_id(), "same-inbound");
    }

    #[test]
    fn dropping_an_issued_token_does_not_restore_budget() {
        let mut gate = AttemptGate::new("inbound-7", AttemptPolicy::Single).unwrap();
        drop(gate.primary().unwrap());

        assert_eq!(gate.issued_attempts(), 1);
        assert_eq!(gate.remaining_attempts(), 0);
        assert_eq!(
            gate.primary().unwrap_err(),
            AttemptGateError::BudgetExhausted { budget: 1 }
        );
    }
}
