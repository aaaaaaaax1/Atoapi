import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
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
assertVersionParity();

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
  ["provider connection-path UI regression", npm, ["run", "test:provider-connection-path-ui"]],
  ["session reuse UI regression", npm, ["run", "test:session-reuse-ui"]],
  ["owned-dispatch acceptance", npm, ["run", "test:acceptance"]],
  ["diff whitespace", "git", ["diff", "--check"]]
];

if (includeWireCompat) {
  checks.splice(
    -1,
    0,
    ["isolated v1.3.4 wire compatibility", npm, ["run", "test:isolated-wire-compat"]],
    ["isolated same-prefix dispatch stress", npm, ["run", "test:isolated-wire-stress"]],
    ["isolated same-prefix header-gate concurrency", npm, ["run", "test:isolated-wire-header-concurrency"]]
  );
}
if (includeCacheReplay) {
  checks.splice(-1, 0, ["100k synthetic cache replay", npm, ["run", "verify:cache"]]);
}

for (const [label, command, args] of checks) {
  run(label, command, args);
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
  const bubbleVersion = controlPlane.match(/const APP_VERSION\s*=\s*"v([^"]+)"/u)?.[1];
  const versions = [packageVersion, cargoVersion, tauriVersion, bubbleVersion];
  if (versions.some((version) => !version) || new Set(versions).size !== 1) {
    throw new Error(`Version mismatch: ${JSON.stringify({ packageVersion, cargoVersion, tauriVersion, bubbleVersion })}`);
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
