# Atoapi v1.5.2

## Responses tool ID namespace repair

- Fixed Codex Responses requests where custom_tool_call or
  custom_tool_call_output carried fc_/fco_ item IDs.
- Applied the narrow prefix repair at the final native Responses and compact
  wire boundaries, including requests without turn metadata.
- Added a regression test covering an un-attested Codex request.

Validation: 1,064 Rust tests passed; frontend build, cargo check --locked,
and the release build passed.
