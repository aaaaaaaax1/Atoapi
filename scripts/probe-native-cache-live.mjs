const args = parseArgs(process.argv.slice(2));
const baseUrl = String(args.url ?? process.env.ATOAPI_BASE_URL ?? "http://127.0.0.1:18883")
  .replace(/\/+$/u, "");
const localKey = String(args.key ?? process.env.ATOAPI_LOCAL_KEY ?? "").trim();
const providerId = String(args.provider ?? process.env.ATOAPI_TEST_PROVIDER ?? "").trim();
const modelId = String(args.model ?? process.env.ATOAPI_TEST_MODEL ?? "gpt-5.6-luna").trim();
const channel = String(args.channel ?? process.env.ATOAPI_TEST_CHANNEL ?? "responses").trim();

if (!localKey) {
  console.error("Set ATOAPI_LOCAL_KEY or pass --key. The key is never written to output.");
  process.exit(2);
}
if (!providerId) {
  console.error("Set ATOAPI_TEST_PROVIDER or pass --provider.");
  process.exit(2);
}
if (!modelId) {
  console.error("Set ATOAPI_TEST_MODEL or pass --model.");
  process.exit(2);
}
if (!new Set(["responses", "chat"]).has(channel)) {
  console.error("--channel must be responses or chat.");
  process.exit(2);
}

const response = await fetch(`${baseUrl}/admin/cache-capabilities/probe`, {
  method: "POST",
  headers: {
    authorization: `Bearer ${localKey}`,
    "content-type": "application/json"
  },
  body: JSON.stringify({ provider_id: providerId, model_id: modelId, channel })
});
const text = await response.text();
let result;
try {
  result = JSON.parse(text);
} catch {
  throw new Error(`probe returned HTTP ${response.status}: ${text.slice(0, 400)}`);
}
if (!response.ok) {
  throw new Error(`probe returned HTTP ${response.status}: ${result.error ?? text.slice(0, 400)}`);
}

const fields = (result.fields ?? []).map((item) => ({
  field: item.field,
  status: item.status,
  enabled: Boolean(item.enabled),
  effectStatus: item.effect_status ?? "unverified",
  httpStatus: item.http_status ?? null,
  message: item.message
}));
const verified = fields.filter((item) => item.status === "verified").length;
const unsupported = fields.filter((item) => item.status === "unsupported").length;
const errors = fields.filter((item) => item.status === "error").length;

console.log(JSON.stringify({
  ok: errors === 0,
  providerId: result.provider_id,
  modelId: result.model_id,
  channel: result.channel,
  keyId: result.key_id ?? null,
  baselineStatus: result.baseline_status ?? null,
  managementRequests: 1 + fields.length,
  verified,
  unsupported,
  errors,
  fields,
  checkedAt: result.checked_at
}, null, 2));

if (errors > 0) process.exit(1);

function parseArgs(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!value.startsWith("--")) continue;
    const [rawKey, inlineValue] = value.slice(2).split("=", 2);
    if (inlineValue !== undefined) {
      parsed[rawKey] = inlineValue;
      continue;
    }
    const next = values[index + 1];
    if (next !== undefined && !next.startsWith("--")) {
      parsed[rawKey] = next;
      index += 1;
    } else {
      parsed[rawKey] = true;
    }
  }
  return parsed;
}
