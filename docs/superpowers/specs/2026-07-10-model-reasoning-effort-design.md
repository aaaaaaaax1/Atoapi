# Model Reasoning Effort Design

## Scope

- Add a per-model reasoning effort override.
- Keep the override disabled by default so the proxy follows the Agent request.
- Support `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, `max`, and `ultra`.
- Map `ultra` to `max` only when the selected model is known to support `max` but not `ultra`.
- Do not add upstream requests, retries, or probes.
- Keep compact, sync, smart-hit waiting, and tail learning behavior unchanged.

## Data flow

1. Model discovery records upstream-declared reasoning effort capabilities when available.
2. The proxy reads the Agent-requested effort from the inbound request.
3. If the model override is disabled, the transformed request keeps the Agent effort.
4. If enabled, the configured model effort replaces the Agent effort.
5. Request logs record Agent, configured, effective, and source values.

## Context window

- Keep returning `context_window` from the local model-list endpoints.
- When Codex injection is applied, also write the selected model context to
  `model_context_window`.

## Compatibility

- Existing configurations deserialize with the override disabled.
- Unknown model capability lists remain empty and do not trigger probing.
- `ultra` is sent unchanged unless the model capability list proves that only
  `max` is available.
