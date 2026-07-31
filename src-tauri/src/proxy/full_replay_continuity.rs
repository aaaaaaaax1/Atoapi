//! Pure local-continuity policy for third-party Responses FullReplay routes.
//!
//! The route may use a local `previous_response_id` as an anchor, but it must
//! never forward that id upstream. This module decides only whether the input
//! is an already-complete replay, a proven append-only delta, or ambiguous.
//! It owns no routing, locks, metrics, or transport client, so every branch is
//! testable without sending an upstream request.

use std::collections::HashSet;

use serde_json::Value;

use crate::continuation_lineage::ResponseSessionState;

use super::{cache_capability, strip_responses_provider_noise};

#[derive(Debug, Clone, PartialEq)]
pub(super) struct FullReplayInputRecovery {
    pub(super) input: Value,
    pub(super) rebuilt_incremental: bool,
    pub(super) ambiguous_local_lineage: bool,
}

/// Returns a semantic FullReplay input only when `current` is a Responses
/// item array. A missing or scalar input is intentionally unrecoverable: the
/// caller must keep its original body but discard the local-only response id.
pub(super) fn recover_full_replay_input(
    session: &ResponseSessionState,
    current: Option<&Value>,
) -> Option<FullReplayInputRecovery> {
    let (Value::Array(previous_items), Value::Array(current_items)) = (&session.input, current?)
    else {
        return None;
    };
    if input_starts_with_known_prefix(current_items, previous_items) {
        return Some(FullReplayInputRecovery {
            input: Value::Array(current_items.clone()),
            rebuilt_incremental: false,
            ambiguous_local_lineage: false,
        });
    }
    if !incremental_input_is_proven_safe(session, current_items) {
        // Preserve a caller-owned, ambiguous item array rather than grafting
        // stale local history onto it. Its local lineage must not settle
        // waterline state, but the inbound still has exactly one FullReplay.
        return Some(FullReplayInputRecovery {
            input: Value::Array(current_items.clone()),
            rebuilt_incremental: false,
            ambiguous_local_lineage: true,
        });
    }

    let mut replay = Vec::with_capacity(
        previous_items
            .len()
            .saturating_add(session.output_items.len())
            .saturating_add(current_items.len()),
    );
    replay.extend(previous_items.iter().cloned());
    replay.extend(session.output_items.iter().cloned());
    replay.extend(current_items.iter().cloned());
    Some(FullReplayInputRecovery {
        input: Value::Array(replay),
        rebuilt_incremental: true,
        ambiguous_local_lineage: false,
    })
}

/// Ignore only protocol breakpoint placement and known provider-generated
/// noise while comparing an already complete semantic replay. This is never a
/// license to reorder items or change caller text/tool output.
fn input_starts_with_known_prefix(current: &[Value], prefix: &[Value]) -> bool {
    current.len() >= prefix.len()
        && prefix
            .iter()
            .zip(current.iter())
            .all(|(expected, current)| item_equal_ignoring_safe_noise(expected, current))
}

fn item_equal_ignoring_safe_noise(left: &Value, right: &Value) -> bool {
    if cache_capability::responses_input_item_equal_ignoring_protocol_breakpoint(left, right) {
        return true;
    }
    let mut left = left.clone();
    let mut right = right.clone();
    strip_responses_provider_noise(&mut left);
    strip_responses_provider_noise(&mut right);
    cache_capability::responses_input_item_equal_ignoring_protocol_breakpoint(&left, &right)
}

/// A local semantic head can be grafted only onto an unambiguous append-only
/// continuation. Safe message sequences are any number of system/developer
/// items followed by one user message. Safe tool sequences must begin with
/// the exact expected output(s), followed by that same message shape.
fn incremental_input_is_proven_safe(
    session: &ResponseSessionState,
    current_items: &[Value],
) -> bool {
    let Some(previous_items) = session.input.as_array() else {
        return false;
    };
    if current_items.is_empty() {
        return false;
    }
    let Some(expected) = expected_response_call_outputs(&session.output_items) else {
        return false;
    };
    if !delta_input_is_safe(current_items, &expected) {
        return false;
    }
    if expected.is_empty() {
        return current_items.len() == 1 || instruction_and_user_delta_is_safe(current_items);
    }
    current_items.len() <= expected.len().saturating_add(2)
        && (current_items.len() < previous_items.len()
            || current_items.len() <= expected.len().saturating_add(1))
}

fn instruction_and_user_delta_is_safe(items: &[Value]) -> bool {
    let Some((last, leading)) = items.split_last() else {
        return false;
    };
    if leading.is_empty()
        || last.get("type").and_then(Value::as_str) != Some("message")
        || last.get("role").and_then(Value::as_str) != Some("user")
    {
        return false;
    }
    leading.iter().all(|item| {
        item.get("type").and_then(Value::as_str) == Some("message")
            && matches!(
                item.get("role").and_then(Value::as_str),
                Some("system" | "developer")
            )
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedResponseCallOutput {
    call_id: String,
    item_type: String,
}

fn expected_response_call_outputs(
    output_items: &[Value],
) -> Option<Vec<ExpectedResponseCallOutput>> {
    let mut expected = Vec::new();
    let mut seen_call_ids = HashSet::new();
    for item in output_items {
        let Some(object) = item.as_object() else {
            continue;
        };
        let Some(item_type) = object.get("type").and_then(Value::as_str) else {
            continue;
        };
        if !(item_type == "function_call" || item_type.ends_with("_call")) {
            continue;
        }
        let call_id = object
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())?;
        if !seen_call_ids.insert(call_id.to_string()) {
            return None;
        }
        expected.push(ExpectedResponseCallOutput {
            call_id: call_id.to_string(),
            item_type: format!("{item_type}_output"),
        });
    }
    Some(expected)
}

fn delta_input_is_safe(
    delta: &[Value],
    expected_call_outputs: &[ExpectedResponseCallOutput],
) -> bool {
    let mut next_call_output = 0usize;
    for item in delta {
        let Some(object) = item.as_object() else {
            return false;
        };
        let item_type = object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message");
        if next_call_output < expected_call_outputs.len() {
            let expected = &expected_call_outputs[next_call_output];
            if item_type != expected.item_type
                || object.get("call_id").and_then(Value::as_str) != Some(expected.call_id.as_str())
            {
                return false;
            }
            next_call_output += 1;
            continue;
        }
        if item_type.ends_with("_call_output") {
            return false;
        }
        if item_type != "message"
            || !object
                .get("role")
                .and_then(Value::as_str)
                .is_some_and(|role| matches!(role, "user" | "system" | "developer"))
        {
            return false;
        }
    }
    next_call_output == expected_call_outputs.len()
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use serde_json::json;

    use super::*;

    fn session() -> ResponseSessionState {
        ResponseSessionState {
            generation: 1,
            parent_generation: None,
            response_id: "resp_seed".to_string(),
            input: json!([{"type":"message","role":"user","content":"before"}]),
            output_items: vec![json!({
                "type":"message",
                "role":"assistant",
                "content":[{"type":"output_text","text":"seed answer"}]
            })],
            finished_at: Instant::now(),
        }
    }

    #[test]
    fn reconstructs_multiple_safe_instruction_items_and_one_user_message() {
        let recovery = recover_full_replay_input(
            &session(),
            Some(&json!([
                {"type":"message","role":"system","content":"s"},
                {"type":"message","role":"developer","content":"d"},
                {"type":"message","role":"user","content":"after"}
            ])),
        )
        .expect("item array");

        assert!(recovery.rebuilt_incremental);
        assert!(!recovery.ambiguous_local_lineage);
        assert_eq!(recovery.input.as_array().map(Vec::len), Some(5));
    }

    #[test]
    fn leaves_missing_or_scalar_input_unrecoverable() {
        assert!(recover_full_replay_input(&session(), None).is_none());
        assert!(recover_full_replay_input(&session(), Some(&json!("not-an-array"))).is_none());
    }
}
