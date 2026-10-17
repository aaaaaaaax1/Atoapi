import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { transform } from "esbuild";

const moduleSource = await readFile(new URL("../src/graphite/codexContextSettings.ts", import.meta.url), "utf8");
const compiled = await transform(moduleSource, { loader: "ts", format: "esm" });
const { parseCodexContextSettings: parse } = await import(`data:text/javascript;base64,${Buffer.from(compiled.code).toString("base64")}`);
assert.deepEqual(parse("", "  "), { model_context_window: null, model_auto_compact_token_limit: null });
assert.deepEqual(parse("872000", "740000"), { model_context_window: 872000, model_auto_compact_token_limit: 740000 });
assert.equal(parse(" 123 ", "").model_context_window, 123);
assert.equal(parse(null, "740000").model_context_window, null);
assert.equal(parse("4294967295", "4294967295").model_auto_compact_token_limit, 4294967295);
for (const invalid of ["0", "-1", "1.5", "1e5", "NaN", "Infinity", "872K", "4294967296", "1 000"]) {
  assert.throws(() => parse(invalid, ""), invalid);
  assert.throws(() => parse("", invalid), invalid);
}
assert.throws(() => parse("100", "101"), /不能大于/);
const host = await readFile(new URL("../src/GraphitePrototypeHost.tsx", import.meta.url), "utf8");
const start = host.indexOf("const bridgeSource = String.raw`");
const end = host.indexOf("`;\n\n// The embedded prototype", start);
assert.ok(start >= 0 && end >= 0);
const bridge = Function(`${host.slice(start, end + 1)}; return bridgeSource;`)();
new Function(bridge);
assert.match(bridge, /save-codex-context/);
assert.ok(bridge.includes('hidden = agent?.sourceId !== "codex"'));
console.log("Codex context settings validation and bridge regression tests passed");
