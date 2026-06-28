import { existsSync, readFileSync, unlinkSync } from "node:fs";
import { join, delimiter } from "node:path";
import { spawnSync } from "node:child_process";

const home = process.env.USERPROFILE || process.env.HOME || "";
const candidateDirs = [
  process.env.CARGO_HOME ? join(process.env.CARGO_HOME, "bin") : "",
  home ? join(home, ".cargo", "bin") : "",
  home ? join(home, ".rustup", "toolchains", "stable-x86_64-pc-windows-msvc", "bin") : ""
].filter(Boolean);

const exe = process.platform === "win32" ? ".exe" : "";
const currentPath = process.env.PATH || "";
const extraDirs = candidateDirs.filter((dir) => existsSync(join(dir, `cargo${exe}`)));
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

const scriptArgs = process.argv.slice(2);
const tauriArgs = scriptArgs.includes("--all")
  ? ["build"]
  : ["build", ...(scriptArgs.length > 0 ? scriptArgs : ["--bundles", "nsis"])];

const result = spawnSync(tauriBin, tauriArgs, {
  stdio: "inherit",
  shell: process.platform === "win32",
  env: process.env
});

process.exit(result.status || 0);
