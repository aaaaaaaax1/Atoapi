import { build } from "esbuild";
import { join } from "node:path";

await build({
  entryPoints: [join("scripts", "codex-ui-patcher.mjs")],
  outfile: join("src-tauri", "resources", "codex-ui-patcher.mjs"),
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node22",
  logLevel: "info"
});
