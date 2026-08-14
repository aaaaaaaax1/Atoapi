import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const source = await readFile(
  resolve(process.cwd(), "src/GraphitePrototypeHost.tsx"),
  "utf8"
);

assert.match(
  source,
  /const modelFirstTokenMs = \(request: RequestRecord\): number => \{[\s\S]{0,240}request\.ttft_ms/,
  "the primary first-token value must use relay model TTFT"
);
assert.match(
  source,
  /ttft: formatDuration\(displayModelFirstTokenMs\)/,
  "the request row must render model TTFT"
);
assert.match(
  source,
  /<span>首字<\/span><b class="value">' \+ escape\(requestDuration\(ttftMs\)\)/,
  "the request row must label model TTFT as 首字"
);
assert.doesNotMatch(
  source,
  /<span>模型首字<\/span>/,
  "the request row must not include the 模型 prefix in the first-token label"
);
assert.match(
  source,
  /const visibleTextFirstTokenMs = \(request: RequestRecord\): number => \{[\s\S]{0,220}request\.visible_text_ttft_ms/,
  "the UI must consume optional visible-text TTFT without falling back to model TTFT"
);
assert.match(
  source,
  /visibleTextTtftRow/,
  "request details must expose visible-text TTFT separately"
);
assert.match(
  source,
  /const upstreamFirstEventMs = \(request: RequestRecord\): number => \{[\s\S]{0,260}request\.first_byte_ms/,
  "raw upstream-event timing must remain a separate diagnostic"
);
assert.match(
  source,
  /first_response_ms/,
  "health probes must retain their first-response diagnostic field"
);

console.log("graphite latency label regression tests passed");
