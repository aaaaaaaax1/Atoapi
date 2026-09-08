# Atoapi v1.5.6

## Reliability

- Provider details and their complete model mapping are now saved as one
  atomic backend transaction. A failed save no longer leaves a provider saved
  with only part of its intended model catalog.
- Native Codex Responses requests now reject invalid legacy tool-call IDs only
  when they cannot be repaired safely; compatible `fc_` and `fco_` IDs are
  normalized to the required `ctc_` and `ctco_` wire form.
- Oversized upstream JSON and SSE responses are bounded before they can retain
  unbounded memory, and upstream error terminal events are not cached or used
  as continuation state.
- Request-body gzip is attempted only after the active upstream route has
  proven compatible. A rejection affects future requests only and never
  duplicates the current request.

## Compatibility And Security

- Codex model discovery reads the selected provider dynamically and merges it
  with local mappings. Atoapi no longer maintains a fixed official model list.
- The Graphite iframe bridge checks both the exact iframe sender and an
  accepted shell/opaque origin, retaining compatibility with Tauri WebViews.
  The existing desktop CSP behavior is preserved for srcDoc compatibility.
- Fixed a UI startup regression where a Vite template parse error or an
  opaque iframe origin could leave the desktop window blank and prevent the
  initial state handshake.
- Local Key values are not included in routine iframe state; they are returned
  only for an explicit reveal or copy operation.
- Agent injection rejects unsafe paths, directory escapes, and symlink-based
  writes.

## Configuration

- Existing provider settings, including user-configured auto-compaction token
  limits and compression-related preferences, are preserved unchanged.
