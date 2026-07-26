import assert from "node:assert/strict";
import { build } from "esbuild";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const sourcePath = fileURLToPath(new URL("../src/graphite/providerDisplay.ts", import.meta.url));
const result = await build({
  entryPoints: [sourcePath],
  bundle: true,
  format: "esm",
  platform: "node",
  write: false
});
const moduleUrl = `data:text/javascript;base64,${Buffer.from(result.outputFiles[0].text).toString("base64")}`;
const { providerDisplayName, requestAgentBadge } = await import(moduleUrl);

const codex = { id: "codex", label: "Codex", kind: "codex" };
const claude = { id: "claude-code", label: "Claude Code", kind: "claude-code" };

assert.equal(
  providerDisplayName({ id: "agent-codex-bizd", name: "bizd / Codex" }, codex),
  "bizd",
  "an owned Codex clone should hide the generated suffix"
);
assert.equal(
  providerDisplayName({ id: "agent-codex-bizd-2", name: "bizd / Codex (2)" }, codex),
  "bizd (2)",
  "a generated duplicate suffix should retain only its disambiguating number"
);
assert.equal(
  providerDisplayName({ id: "shared-bizd", name: "bizd / Codex" }, codex),
  "bizd / Codex",
  "a user-owned shared provider name must never be rewritten"
);
assert.equal(
  providerDisplayName({ id: "agent-claude-code-bizd", name: "bizd / Codex" }, claude),
  "bizd / Codex",
  "a suffix for another Agent must remain intact"
);
assert.deepEqual(
  requestAgentBadge("codex", "stale label", [codex, claude]),
  { label: "Codex", tone: "codex" },
  "configured Agent metadata wins over stale request labels"
);
assert.deepEqual(
  requestAgentBadge("external-agent", "External", [codex]),
  { label: "External", tone: "generic" }
);
assert.deepEqual(
  requestAgentBadge("codex", "Codex", []),
  { label: "Codex", tone: "codex" },
  "an unconfigured but known Codex request must retain its own badge tone"
);
assert.deepEqual(
  requestAgentBadge("claude-code", "Claude Code", []),
  { label: "Claude Code", tone: "claude" },
  "an unconfigured Claude request must not fall through to OpenClaw"
);
assert.deepEqual(
  requestAgentBadge("gemini", "Gemini", []),
  { label: "Gemini", tone: "gemini" }
);
assert.deepEqual(
  requestAgentBadge("open-code", "OpenCode", []),
  { label: "OpenCode", tone: "opencode" }
);

const controlPlaneSource = await readFile(
  fileURLToPath(new URL("../src/useGraphiteControlPlane.ts", import.meta.url)),
  "utf8"
);
const toggleStart = controlPlaneSource.indexOf("async function toggleAgentInjection");
const toggleEnd = controlPlaneSource.indexOf("async function activateAgentProvider", toggleStart);
const toggleSource = controlPlaneSource.slice(toggleStart, toggleEnd);
assert.doesNotMatch(
  toggleSource,
  /active_provider_id|providers\[0\]/,
  "an unbound Agent must not silently inherit the global upstream"
);
assert.match(
  controlPlaneSource,
  /provider_id:\s*provider\.id,[\s\S]*?model_id:/,
  "an Agent route switch must send an explicit model selection or null"
);
assert.doesNotMatch(
  controlPlaneSource,
  /persist:\s*Boolean\(existingProvider\)/,
  "fetching model candidates must not persist them before the user adds a mapping"
);

console.log("graphite provider display regression tests passed");
