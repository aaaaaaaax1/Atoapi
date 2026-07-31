import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

/**
 * Writes a complete, secret-free Responses fixture for isolated relay tests.
 * These tests must never clone the user's provider profile or DPAPI material:
 * their purpose is request/relay behavior, not configuration migration.
 */
export async function writeIsolatedResponsesConfig(configDir, upstreamPort, options = {}) {
  const localKey = String(options.localKey ?? "isolated-responses-local-key");
  const model = String(options.model ?? "gpt-5.6-terra");
  const providerId = String(options.providerId ?? "isolated-responses-provider");
  const workspaceFingerprint = String(options.workspaceFingerprint ?? "isolated-responses-workspace");
  const configPath = join(configDir, "config.toml");
  await mkdir(configDir, { recursive: true });
  await writeFile(
    configPath,
    `host = "127.0.0.1"
port = 18883
proxy_auto_start = false
proxy_mode_host = "127.0.0.1"
proxy_mode_port = 18884
local_key = "${localKey}"
default_channel = "responses"
active_provider_id = "${providerId}"
workspace_fingerprint = "${workspaceFingerprint}"
updated_at = "2026-07-30T00:00:00Z"

[cache]
mode = "prefix-prewarm"
enabled = true
exact_enabled = true
semantic_enabled = true
semantic_threshold = 0.985
max_age_seconds = 86400
max_entries = 16
persist_encrypted = false
prewarm_enabled = false
background_prewarm_enabled = false

[[route_profiles]]
name = "responses"
client_channel = "responses"
upstream_channel = "responses"
long_context_threshold = 60000

[[providers]]
id = "${providerId}"
name = "Isolated Responses Mock"
base_url = "http://127.0.0.1:${upstreamPort}/v1"
channel = "responses"
prompt_cache_retention_enabled = true
request_body_gzip_enabled = false
use_system_proxy = false
api_key_encrypted = "isolated-upstream-placeholder"
enabled = true
created_at = "2026-07-30T00:00:00Z"
updated_at = "2026-07-30T00:00:00Z"
models = [{ id = "${model}", display_name = "${model}", context_window = 353400, output_window = 32768, reasoning_effort_override_enabled = false, supports_tools = true, supports_streaming = true, enabled = true }]

[[agent_injections]]
id = "codex"
label = "Codex"
kind = "codex"
enabled = true
provider_id = "${providerId}"
model_id = "${model}"
hidden_provider_ids = []
`,
    "utf8"
  );
  return { configPath, localKey };
}
