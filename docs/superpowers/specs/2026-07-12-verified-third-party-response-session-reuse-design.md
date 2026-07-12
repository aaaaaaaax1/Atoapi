# Verified Third-Party Responses Session Reuse

## Objective

Reduce large Responses request bodies only after a third-party provider has
proved that it preserves `previous_response_id` conversation state. A normal
inbound Agent request must always make exactly one upstream generation call.

## Capability lifecycle

Each provider has a persisted capability record, scoped to one actual upstream
model:

- `unverified`: normal full-context forwarding only;
- `verified`: a user-triggered probe proved semantic carry-over for the stored
  model; the provider may use a response-session delta when the local session
  has an append-only match;
- `unsupported` or `error`: normal full-context forwarding only, with the
  concise failure reason retained for the settings UI.

The setting can be turned off after verification without discarding the proof.
Changing the endpoint, channel, supplied API key, or effective key-pool routing
invalidates the proof and turns the setting off. A probe snapshots both the
connection target and the current setting state; it discards its result rather
than overwriting a concurrent save or user disable action.

## Manual compatibility probe

The provider settings UI accepts the actual upstream model id and exposes
`Verify and enable`. The action is never automatic and is not an Agent request.
It sends exactly two tiny non-stream Responses calls:

1. seed a unique random verification token and ask for an acknowledgement;
2. send only a continuation with the first response id and require the token.

Verification succeeds only when the second successful response returns the
unique token. HTTP success alone is not proof, because a gateway could silently
ignore `previous_response_id`.

## Runtime behavior

For an enabled, verified model, the existing append-only session logic builds a
delta request with `previous_response_id`. If the upstream rejects that delta,
the proxy does not issue an internal full-context retry. It clears the local
stale reference; explicit unsupported-parameter evidence also invalidates the
provider capability. The original upstream error is returned to the client.

All normal Agent generation requests are single-attempt at the proxy boundary:
they do not use key failover, protocol fallback, 413 session rescue, or
`previous_response_id` compatibility retries. Non-Agent API clients retain the
existing recovery behavior. The manual compatibility probe remains the only
multi-request operation and never includes Agent conversation content.

Official OpenAI behavior remains unchanged. The verified third-party path is
the only new path and remains model-scoped.

## Verification

- unit-test capability state transitions and invalidation;
- integration-test the two-call probe against a local mock Responses server;
- regression-test that an invalid verified delta results in one upstream call,
  never a full retry;
- run Rust tests, frontend build, formatting, and diff checks.
