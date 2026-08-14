import { existsSync, lstatSync, rmSync, statSync } from "node:fs";
import { join, delimiter, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";

const scriptArgs = process.argv.slice(2);
const preflightOnly = scriptArgs.includes("--preflight-only");
const repositoryRoot = resolve(".");

// Keep release builds reproducible without touching user data, release archives,
// or the full Rust target cache.  Every target is deliberately whitelisted and
// must resolve inside this repository before it can be removed.
const reproducibleBuildCaches = [
  ["pnpm store", ".pnpm-store"],
  ["Vite output", "dist"],
  ["Vite cache", ".vite"],
  ["Tauri bundle output", join("src-tauri", "target", "release", "bundle")]
];

function resolveRepositoryCache(relativePath) {
  const absolutePath = resolve(repositoryRoot, relativePath);
  const pathFromRepository = relative(repositoryRoot, absolutePath);
  if (!pathFromRepository || pathFromRepository.startsWith("..") || pathFromRepository.includes(":")) {
    throw new Error(`refusing to clear a cache outside the repository: ${relativePath}`);
  }
  return absolutePath;
}

function clearReproducibleBuildCaches() {
  for (const [label, relativePath] of reproducibleBuildCaches) {
    const absolutePath = resolveRepositoryCache(relativePath);
    if (!existsSync(absolutePath)) {
      continue;
    }

    const entry = lstatSync(absolutePath);
    if (!entry.isDirectory() || entry.isSymbolicLink()) {
      throw new Error(`refusing to clear unexpected cache target: ${relativePath}`);
    }

    rmSync(absolutePath, { recursive: true, force: true, maxRetries: 2, retryDelay: 250 });
    console.log(`[tauri-build] cleared ${label}: ${relativePath}`);
  }
}

if (!preflightOnly) {
  clearReproducibleBuildCaches();
}

const patcherBuild = spawnSync(process.execPath, [join("scripts", "build-codex-ui-patcher.mjs")], {
  stdio: "inherit",
  shell: false
});
if (patcherBuild.status !== 0) {
  process.exit(patcherBuild.status || 1);
}

// Tauri's generate_context! macro validates frontendDist during the Rust
// preflight.  A clean package has no dist/ yet, so materialize the fresh
// frontend once before that gate; Tauri will run its normal beforeBuildCommand
// again as part of the final bundle.
const frontendDistEntry = join("dist", "index.html");
if (!existsSync(frontendDistEntry)) {
  const frontendBuild = spawnSync(
    process.platform === "win32" ? "npm.cmd" : "npm",
    ["run", "build"],
    {
      stdio: "inherit",
      shell: process.platform === "win32"
    }
  );
  if (frontendBuild.status !== 0) {
    process.exit(frontendBuild.status || 1);
  }
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

if (preflightOnly) {
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
