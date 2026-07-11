import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const analyzer = path.join(scriptDir, "analyze-metrics.mjs");
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "atoapi-metrics-test-"));

try {
  const crossSession = [
    request("anchor-a", "key-a", 2048, 1792, 10),
    request("anchor-b", "key-b", 2048, 1792, 20)
  ];
  const clean = analyze(crossSession);
  assert.equal(clean.diagnostics.prefixKeySplits.length, 0);
  assert.equal(clean.summary.warmAvoidableShortfallTokens, 256);
  assert.equal(clean.summary.warmUnclassifiedShortfallTokens, 256);
  assert.equal(clean.summary.network.localExtraTtftMs.p95, 20);

  const sameSessionSplit = [
    ...crossSession,
    request("anchor-a", "key-a-volatile", 2304, 2048, 30)
  ];
  const split = analyze(sameSessionSplit);
  assert.equal(split.diagnostics.prefixKeySplits.length, 1);
  assert.equal(split.diagnostics.prefixKeySplits[0].sessionAnchorHash, "anchor-a");
  assert.equal(split.diagnostics.prefixKeySplits[0].keyCount, 2);

  console.log("analyze-metrics regression tests passed");
} finally {
  fs.rmSync(tempDir, { recursive: true, force: true });
}

function request(anchor, key, input, cached, localExtraMs) {
  return {
    at: "2026-07-11T00:00:00Z",
    provider: "provider",
    model: "model",
    cache_status: "miss",
    provider_prefix_key: key,
    provider_prefix_fingerprint: "stable-fingerprint",
    session_anchor_hash: anchor,
    session_anchor_source: "exact",
    input_tokens: input,
    cache_read_tokens: cached,
    cache_new_tail_gap_tokens: 0,
    cache_avoidable_gap_tokens: 128,
    cache_provider_unstable_gap_tokens: 0,
    upstream_call_kind: "stream",
    status: 200,
    ttft_ms: 1000 + localExtraMs,
    upstream_ttft_ms: 1000,
    upstream_headers_ms: 900,
    total_ms: 1500,
    local_prepare_ms: 5,
    prefix_guard_wait_ms: localExtraMs,
    request_body_bytes: 1000,
    sent_body_bytes: 500,
    gzip_attempted: true,
    gzip_fallback_used: false
  };
}

function analyze(recentRequests) {
  const fixture = path.join(tempDir, `${Math.random().toString(16).slice(2)}.json`);
  fs.writeFileSync(fixture, JSON.stringify({ recent_requests: recentRequests }));
  return JSON.parse(execFileSync(process.execPath, [analyzer, fixture], { encoding: "utf8" }));
}
