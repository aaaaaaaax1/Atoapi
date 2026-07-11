import { createPackage, extractAll } from "@electron/asar";
import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

function argument(name) {
  const index = process.argv.indexOf(name);
  if (index < 0 || index + 1 >= process.argv.length) {
    throw new Error(`Missing ${name}`);
  }
  return process.argv[index + 1];
}

async function findAsset(assetsPath, prefix, feature) {
  const entries = await readdir(assetsPath, { withFileTypes: true });
  const candidates = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.startsWith(`${prefix}-`) || !entry.name.endsWith(".js")) {
      continue;
    }
    const path = join(assetsPath, entry.name);
    const content = await readFile(path, "utf8");
    if (content.includes(feature)) {
      candidates.push({ path, content });
    }
  }
  if (candidates.length !== 1) {
    throw new Error(
      `Expected one ${prefix} asset containing ${JSON.stringify(feature)}, found ${candidates.length}`
    );
  }
  return candidates[0];
}

async function replaceFeature(asset, before, after, label) {
  const matches = asset.content.split(before).length - 1;
  if (matches === 0 && asset.content.includes(after)) {
    return;
  }
  if (matches !== 1) {
    throw new Error(
      `${label}: expected one source match in ${basename(asset.path)}, found ${matches}`
    );
  }
  asset.content = asset.content.replace(before, after);
  await writeFile(asset.path, asset.content, "utf8");
}

const input = argument("--input");
const output = argument("--output");
const workPath = await mkdtemp(join(tmpdir(), "atoapi-codex-ui-"));

try {
  extractAll(input, workPath);
  const assetsPath = join(workPath, "webview", "assets");
  const modelFilter = await findAsset(assetsPath, "model-list-filter", "useHiddenModels");
  const modelQueries = await findAsset(assetsPath, "model-queries", "1186680773");
  const serviceTierUi = await findAsset(
    assetsPath,
    "use-service-tier-settings",
    "isServiceTierAllowed"
  );
  const serviceTierRequest = await findAsset(
    assetsPath,
    "read-service-tier-for-request",
    "Failed to read service tier for request"
  );

  await replaceFeature(
    modelFilter,
    "u?n.has(r.model):!r.hidden",
    "u?n.has(r.model):!r.hidden||/^gpt-5\\.6-(?:sol|terra|luna)$/u.test(r.model)",
    "GPT-5.6 visibility"
  );
  await replaceFeature(
    modelQueries,
    "R=[`low`,`medium`,`high`,`xhigh`]",
    "R=[`low`,`medium`,`high`,`xhigh`,`max`,`ultra`]",
    "Max and Ultra reasoning"
  );
  await replaceFeature(
    modelQueries,
    "l=i&&s(D,`1186680773`)",
    "l=i",
    "Ultra display gate"
  );
  await replaceFeature(
    serviceTierUi,
    "p=o&&!f&&u!=null&&u?.requirements?.featureRequirements?.fast_mode!==!1",
    "p=a?.authMethod===`apikey`||o&&!f&&u!=null&&u?.requirements?.featureRequirements?.fast_mode!==!1",
    "API Key Fast UI"
  );
  await replaceFeature(
    serviceTierRequest,
    "if(n!==`chatgpt`)return!1;",
    "if(n===`apikey`)return!0;if(n!==`chatgpt`)return!1;",
    "API Key Fast request"
  );

  await createPackage(workPath, output);
} finally {
  await rm(workPath, { recursive: true, force: true });
}
