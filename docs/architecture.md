# Architecture

## Layers

- UI: React management console in `src/`.
- Tauri commands: `src-tauri/src/admin`.
- Local proxy: `src-tauri/src/proxy`.
- Config: `%APPDATA%/Atoapi/config.toml` through `src-tauri/src/config`.
- Secret storage: Windows DPAPI in `src-tauri/src/crypto.rs`.
- Response cache: encrypted local cache in `src-tauri/src/cache.rs`.
- Metrics: in-memory rolling counters in `src-tauri/src/metrics.rs`.

## Request Flow

1. Client calls one of `/v1/chat/completions`, `/v1/responses`, or `/v1/messages`.
2. Proxy validates the local virtual key.
3. Route decision picks active provider first, then matching route profile, then model match.
4. Request is normalized by setting the resolved model.
5. Prefix-cache hints are injected.
6. Eligible requests check exact and semantic cache.
7. Cache miss forwards to upstream.
8. Response is streamed or collected.
9. Successful eligible response is encrypted and cached.
10. Metrics are updated.

## Upstream Transport

- Providers with `use_system_proxy = false` use a pooled HTTP/1.1 client. This follows the stable CCSwitch direct path and avoids large HTTP/2 request-body stalls observed on some Cloudflare routes.
- Providers with `use_system_proxy = true` use the Windows system proxy and retain normal HTTP protocol negotiation.
- Both clients are reused across requests. A provider setting change is applied through config hot reload and does not create an extra upstream request.
- Request-body gzip is opt-in per provider and uses stronger compression for larger payloads to reduce upload time.
- Streaming metrics distinguish the first SSE chunk from the first real model output. The upstream SSE body remains in one foreground task through EOF so late `response.completed` events are preserved and detached drains cannot overlap later requests.

## Active Upstream

The selected provider in the left UI column writes `active_provider_id`.

Route selection prefers:

1. `active_provider_id`
2. route profile provider
3. provider that owns requested model
4. first enabled provider matching the requested channel
5. first enabled provider

## Cache Safety Defaults

Semantic cache is only used when:

- cache is enabled,
- request temperature is `<= 0.3`,
- request has no tools/tool choice,
- request does not mark metadata cache as `no-store`.

This intentionally favors correctness over aggressive reuse.
