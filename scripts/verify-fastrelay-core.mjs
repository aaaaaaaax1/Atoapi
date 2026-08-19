import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const options = new Set(process.argv.slice(2));
const dryRun = options.has("--dry-run");
const includeWireCompat = options.has("--wire-compat");
const includeCacheReplay = options.has("--cache-replay");

for (const option of options) {
  if (!["--dry-run", "--wire-compat", "--cache-replay"].includes(option)) {
    throw new Error(`Unknown option: ${option}`);
  }
}

prepareCargoPath();
const expectedAppVersion = assertVersionParity();

const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const cargo = process.platform === "win32" ? "cargo.exe" : "cargo";
const checks = [
  ["Rust format", cargo, ["fmt", "--manifest-path", "src-tauri/Cargo.toml", "--", "--check"]],
  ["Rust release tests", cargo, ["test", "--manifest-path", "src-tauri/Cargo.toml", "--release"]],
  [
    "FastRelay capacity baselines",
    cargo,
    [
      "test",
      "--manifest-path",
      "src-tauri/Cargo.toml",
      "--release",
      "fastrelay_full_capacity_",
      "--",
      "--ignored",
      "--nocapture"
    ]
  ],
  ["frontend build", npm, ["run", "build"]],
  ["metrics regression", npm, ["run", "test:metrics"]],
  ["request state regression", npm, ["run", "test:request-state"]],
  ["provider display regression", npm, ["run", "test:provider-display"]],
  ["metrics trend UI regression", npm, ["run", "test:metrics-trend-ui"]],
  ["secret-field UI regression", npm, ["run", "test:secret-field-ui"]],
  ["key-pool UI regression", npm, ["run", "test:key-pool-ui"]],
  ["provider connection-path UI regression", npm, ["run", "test:provider-connection-path-ui"]],
  ["owned-dispatch acceptance", npm, ["run", "test:acceptance"]],
  ["release champion verifier self-test", npm, ["run", "test:release-champion"]],
  ["diff whitespace", "git", ["diff", "--check"]]
];

if (includeWireCompat) {
  checks.splice(
    -1,
    0,
    [
      "fresh release executable for isolated wire compatibility",
      cargo,
      ["build", "--manifest-path", "src-tauri/Cargo.toml", "--release"]
    ],
    ["isolated retained-baseline wire compatibility", npm, ["run", "test:isolated-wire-compat"]],
    ["isolated same-prefix dispatch stress", npm, ["run", "test:isolated-wire-stress"]],
    ["isolated same-prefix header-gate concurrency", npm, ["run", "test:isolated-wire-header-concurrency"]],
    ["isolated terminal publication handoff", npm, ["run", "test:terminal-handoff"]]
  );
}
if (includeCacheReplay) {
  checks.splice(-1, 0, ["100k synthetic cache replay", npm, ["run", "verify:cache"]]);
}

for (const [label, command, args] of checks) {
  run(label, command, args);
  if (!dryRun && label === "frontend build") {
    assertBuiltFrontendVersionParity(expectedAppVersion);
  }
}

console.log(JSON.stringify({
  pass: true,
  dryRun,
  wireCompat: includeWireCompat,
  cacheReplay: includeCacheReplay,
  checks: checks.map(([label]) => label)
}, null, 2));

function prepareCargoPath() {
  const home = process.env.USERPROFILE || process.env.HOME || "";
  const candidates = [
    process.env.CARGO_HOME ? join(process.env.CARGO_HOME, "bin") : "",
    home ? join(home, ".cargo", "bin") : "",
    home ? join(home, ".rustup", "toolchains", "stable-x86_64-pc-windows-msvc", "bin") : ""
  ].filter(Boolean);
  const executable = process.platform === "win32" ? "cargo.exe" : "cargo";
  const usable = candidates.filter((directory) => existsSync(join(directory, executable)));
  process.env.PATH = [...usable, process.env.PATH || ""].join(delimiter);
}

function assertVersionParity() {
  const packageVersion = JSON.parse(readFileSync(join(repoRoot, "package.json"), "utf8")).version;
  const cargo = readFileSync(join(repoRoot, "src-tauri", "Cargo.toml"), "utf8");
  const cargoVersion = cargo.match(/^version\s*=\s*"([^"]+)"/mu)?.[1];
  const tauriVersion = JSON.parse(readFileSync(join(repoRoot, "src-tauri", "tauri.conf.json"), "utf8")).version;
  const controlPlane = readFileSync(join(repoRoot, "src", "useGraphiteControlPlane.ts"), "utf8");
  const literalBubbleVersion = controlPlane.match(/const APP_VERSION\s*=\s*"v([^"]+)"/u)?.[1];
  const viteConfig = readFileSync(join(repoRoot, "vite.config.ts"), "utf8");
  const buildInjectedBubbleVersion =
    /const APP_VERSION\s*=\s*__ATOAPI_APP_VERSION__/u.test(controlPlane) &&
    /__ATOAPI_APP_VERSION__\s*:\s*JSON\.stringify\(appVersion\)/u.test(viteConfig) &&
    /const appVersion\s*=\s*`v\$\{packageJson\.version\}`/u.test(viteConfig) &&
    /import packageJson from "\.\/package\.json"/u.test(viteConfig)
      ? packageVersion
      : undefined;
  const bubbleVersion = literalBubbleVersion ?? buildInjectedBubbleVersion;
  const versions = [packageVersion, cargoVersion, tauriVersion, bubbleVersion];
  if (versions.some((version) => !version) || new Set(versions).size !== 1) {
    throw new Error(`Version mismatch: ${JSON.stringify({ packageVersion, cargoVersion, tauriVersion, bubbleVersion })}`);
  }
  return packageVersion;
}

function assertBuiltFrontendVersionParity(expectedVersion) {
  const assetsDirectory = join(repoRoot, "dist", "assets");
  if (!existsSync(assetsDirectory)) {
    throw new Error("Built frontend assets are missing; run npm run build before packaging.");
  }
  const marker = `v${expectedVersion}`;
  const bundles = readdirSync(assetsDirectory)
    .filter((name) => name.endsWith(".js"))
    .map((name) => join(assetsDirectory, name));
  if (bundles.length === 0 || !bundles.some((path) => readFileSync(path, "utf8").includes(marker))) {
    throw new Error(`Built frontend version bubble is stale or missing ${marker}. Rebuild the frontend before packaging.`);
  }
}

function run(label, command, args) {
  console.log(`\n[FastRelayCore preflight] ${label}`);
  if (dryRun) {
    console.log(`${command} ${args.join(" ")}`);
    return;
  }
  const environment = { ...process.env };
  if (label === "100k synthetic cache replay") {
    environment.CCS_VERIFY_TOTAL = "100000";
  }
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    env: environment,
    stdio: "inherit",
    shell: process.platform === "win32" && command.toLowerCase().endsWith(".cmd")
  });
  if (result.status !== 0) {
    process.exit(result.status || 1);
  }
}
