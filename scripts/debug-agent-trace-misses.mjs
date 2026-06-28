import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import fs from "node:fs";
import vm from "node:vm";

const source = fs
  .readFileSync(new URL("./verify-cache.mjs", import.meta.url), "utf8")
  .replace('import { createHash } from "node:crypto";\n', "")
  .replace('import { performance } from "node:perf_hooks";\n', "");

const firstFunction = source.indexOf("function runWarmReplay");
const functionsOnly = source.slice(firstFunction);
const context = {
  createHash,
  performance,
  console: { log() {} },
  PROVIDER_ID: "provider-a",
  MODEL: "model-a",
  WORKSPACE: "workspace-a",
  WORKLOAD_LABEL: "debug",
  MIN_HIT_RATE: 0.99,
  MAX_P95_MS: 50,
};
vm.createContext(context);
vm.runInContext(functionsOnly, context);

const total = Number(process.argv[2] ?? 300000);
const warmedCorpus = 64000;
const cache = new Map();
for (let index = 0; index < warmedCorpus; index += 1) {
  const request = context.agentTraceRequest(index, warmedCorpus);
  for (const key of context.cacheKeysForMode(request, "prefix-prewarm")) {
    cache.set(key.value, { body: "{}" });
  }
}

const missByWorkload = new Map();
const hitByWorkload = new Map();
const examples = [];

for (let index = 0; index < total; index += 1) {
  const request = context.agentTraceReplayRequest(index, warmedCorpus);
  if (!context.isCacheEligible(request)) {
    continue;
  }

  const firstSeenNovel =
    request.metadata?.workload === "first-seen-novel" ||
    request.metadata?.no_store_after_miss === true;
  const workload = request.metadata?.workload ?? "base-or-variant";
  const hit = context.lookup(cache, request, "prefix-prewarm");

  if (hit && !firstSeenNovel) {
    hitByWorkload.set(workload, (hitByWorkload.get(workload) ?? 0) + 1);
  }
  if (!hit && !firstSeenNovel) {
    missByWorkload.set(workload, (missByWorkload.get(workload) ?? 0) + 1);
    if (examples.length < 20) {
      examples.push({
        index,
        workload,
        metadata: request.metadata ?? null,
        text: request.messages?.[1]?.content ?? null,
        hasTools: Boolean(request.tools),
      });
    }
  }
  if (!hit && !request.metadata?.no_store_after_miss) {
    for (const key of context.cacheKeysForMode(request, "prefix-prewarm")) {
      cache.set(key.value, { body: "{}" });
    }
  }
}

console.log(
  JSON.stringify(
    {
      total,
      missByWorkload: Object.fromEntries(missByWorkload),
      hitByWorkload: Object.fromEntries(hitByWorkload),
      examples,
    },
    null,
    2,
  ),
);
