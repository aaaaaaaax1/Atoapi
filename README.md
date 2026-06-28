# Atoapi

Windows desktop local AI-agent proxy for OpenAI-compatible, Responses, and Anthropic-compatible clients.

The app exposes:

- `POST /v1/chat/completions`
- `POST /v1/responses`
- `POST /v1/messages`
- `GET /v1/models`
- `GET /health`
- `GET /admin/metrics`

Default local address:

```text
http://127.0.0.1:18883
```

## What Is Implemented

- Tauri 2 + React desktop shell.
- Rust `axum` local proxy server.
- Provider manager UI with:
  - provider list on the left,
  - selected provider highlight and check mark,
  - provider name, Base URL, API Key, API format,
  - automatic model-list fetch,
  - manual model add/edit/delete,
  - enabled/disabled state.
- Active upstream selection persisted in `%APPDATA%/Atoapi/config.toml`.
- OpenAI Chat, OpenAI Responses, and Anthropic Messages forwarding.
- SSE passthrough for streaming calls.
- Local virtual key authentication.
- Exact response cache and conservative semantic cache.
- Safe multi-layer local cache:
  - exact request key,
  - near-exact normalized key for case, whitespace, and punctuation differences,
  - guarded local n-gram embedding similarity for multilingual near-duplicates.
- Provider prefix-cache optimization:
  - OpenAI-compatible `prompt_cache_key`,
  - Anthropic-compatible `cache_control`.
- Metrics for overall eligible cache hit rate, repeatable eligible cache hit rate, provider cached-token ratio, TTFT, errors, retries, and recent requests.
- DPAPI-protected provider keys and AES-GCM encrypted response cache on Windows.

## Client Setup

Claude Code / Anthropic SDK:

```powershell
$env:ANTHROPIC_BASE_URL="http://127.0.0.1:18883"
$env:ANTHROPIC_AUTH_TOKEN="<local-key-from-ui>"
```

OpenAI-compatible clients:

```powershell
$env:OPENAI_BASE_URL="http://127.0.0.1:18883/v1"
$env:OPENAI_API_KEY="<local-key-from-ui>"
```

## Development

Install Rust first if it is not available:

```powershell
winget install Rustlang.Rustup
```

Then:

```powershell
npm.cmd install
npm.cmd run build
npm.cmd run tauri:dev
```

Build Windows installer:

```powershell
npm.cmd run tauri:build
```

## Model List Fetching

The UI button "获取模型列表" calls the Rust backend, which probes common model endpoints from the configured Base URL:

- `<base>/models`
- `<base>/v1/models`
- roots inferred from `/chat/completions`, `/responses`, and `/messages`
- Z.ai-style candidates when the path contains `/api/anthropic`

Supported response shapes:

- `{ "data": [{ "id": "..." }] }`
- `{ "models": [{ "id": "..." }] }`
- `{ "data": { "models": [...] } }`
- raw array of model records

## Cache Acceptance Metric

The hard 99% target is scoped to warmed or repeatable eligible local response-cache traffic:

```text
(exact cache hits + safe near-exact/session hits + semantic cache hits) / repeatable eligible cache lookups >= 99%
```

First-seen novel prompts, high-temperature requests, tool requests, and `Cache-Control: no-store` bypasses are reported separately. Provider prefix cache is tracked separately because upstream providers only hit when long prompt prefixes are stable and provider-specific cache rules are satisfied.

The UI shows both local cache rates:

- `overall eligible`: all eligible lookups, including first-seen prompts that cannot be cache hits yet.
- `repeatable eligible`: warmed or previously seen eligible lookups. This is the 99% acceptance metric.

### Best Cache Strategy

Do not inject artificial dictionaries, alphabet lists, or repeated filler text into model prompts. That can change model behavior, increase prompt tokens, and slow first-token latency. Dictionaries and normalization belong inside the local cache matcher only.

The safe strategy is:

- Keep user prompts and tool payloads unchanged.
- Use exact cache for identical normalized requests.
- Use near-exact cache only for formatting differences that do not change meaning.
- Use semantic cache only for eligible low-temperature, no-tool requests within the same workspace, provider, model, and request structure; negated, strict, time-sensitive, or code-like prompts are blocked from fuzzy matching.
- Exclude dynamic or code-context requests such as "latest", "today", patches, stack traces, and tool-bearing calls from local response-cache eligibility.
- Use provider prefix cache only for real stable prefixes such as system prompts, tool schemas, and fixed project context.
- Verify locally with:

```powershell
npm.cmd run verify:cache
```

The verification script runs 300k-request workloads and requires:

- 300k exact warm replay for passive mode
- 300k mixed workload for session-prewarm mode
- 300k mixed workload for prefix-prewarm mode
- 300k recorded-like agent trace replay for prefix-prewarm mode
- repeatable eligible local response-cache hit rate `>= 99%`
- cache-hit p95 lookup time `< 50ms`

The agent trace replay mixes stable agent summaries/config tasks, volatile request/session IDs, formatting drift, first-seen novel prompts, and bypassed tool/dynamic/code contexts. It is still synthetic; real captured agent traces should be replayed before claiming production hit rates for a specific workflow.
