import { existsSync, readFileSync, statSync, unlinkSync } from "node:fs";
import { join, delimiter } from "node:path";
import { spawnSync } from "node:child_process";

const patcherBuild = spawnSync(process.execPath, [join("scripts", "build-codex-ui-patcher.mjs")], {
  stdio: "inherit",
  shell: false
});
if (patcherBuild.status !== 0) {
  process.exit(patcherBuild.status || 1);
}

// A bundle is only allowed after the deterministic FastRelayCore gate.  It
// never starts the desktop instance or talks to the configured upstream; the
// separate release workflow may additionally request the isolated wire/cache
// evidence when an old baseline executable is available.
const preflight = spawnSync(process.execPath, [join("scripts", "verify-fastrelay-core.mjs")], {
  stdio: "inherit",
  shell: false
});
if (preflight.status !== 0) {
  process.exit(preflight.status || 1);
}

const scriptArgs = process.argv.slice(2);
if (scriptArgs.includes("--preflight-only")) {
  process.exit(0);
}

const home = process.env.USERPROFILE || process.env.HOME || "";
const candidateDirs = [
  process.env.CARGO_HOME ? join(process.env.CARGO_HOME, "bin") : "",
  home ? join(home, ".cargo", "bin") : "",
  home ? join(home, ".rustup", "toolchains", "stable-x86_64-pc-windows-msvc", "bin") : ""
].filter(Boolean);

const exe = process.platform === "win32" ? ".exe" : "";
const currentPath = process.env.PATH || "";
// A stale zero-byte rustup shim must not shadow a valid toolchain binary.
// This desktop environment can retain an empty %USERPROFILE%\\.cargo stub
// after toolchain repair, while the real cargo.exe lives under rustup.
const extraDirs = candidateDirs.filter((dir) => {
  const cargoPath = join(dir, `cargo${exe}`);
  return existsSync(cargoPath) && statSync(cargoPath).size > 0;
});
process.env.PATH = [...extraDirs, currentPath].join(delimiter);

const check = spawnSync(`cargo${exe}`, ["--version"], {
  stdio: "ignore",
  shell: false
});

if (check.status !== 0) {
  console.error("cargo not found. Install Rust or ensure cargo.exe exists in %USERPROFILE%\\.cargo\\bin.");
  process.exit(check.status || 1);
}

const config = JSON.parse(readFileSync(join("src-tauri", "tauri.conf.json"), "utf8"));
const productName = config.productName || "Atoapi";
const version = config.version;
const staleBundleFiles = [
  join("src-tauri", "target", "release", "bundle", "msi", `${productName}_${version}_x64_en-US.msi`),
  join("src-tauri", "target", "release", "bundle", "nsis", `${productName}_${version}_x64-setup.exe`)
];

for (const file of staleBundleFiles) {
  if (!existsSync(file)) {
    continue;
  }
  try {
    unlinkSync(file);
  } catch (error) {
    console.error(`failed to remove stale bundle: ${file}`);
    console.error(error instanceof Error ? error.message : String(error));
    console.error("Close any installer window or msiexec process that is using this file, then rerun npm.cmd run tauri:build.");
    process.exit(1);
  }
}

const tauriBin = process.platform === "win32"
  ? join("node_modules", ".bin", "tauri.cmd")
  : join("node_modules", ".bin", "tauri");

const tauriArgs = scriptArgs.includes("--all")
  ? ["build"]
  : ["build", ...(scriptArgs.length > 0 ? scriptArgs : ["--bundles", "nsis"])];

const result = spawnSync(tauriBin, tauriArgs, {
  stdio: "inherit",
  shell: process.platform === "win32",
  env: process.env
});

process.exit(result.status || 0);
