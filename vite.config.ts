import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = fileURLToPath(new URL(".", import.meta.url));

// Vite's development watcher must never recursively retain Rust build output
// or release/QA artifacts. Those trees change during every package build and
// are not browser source; watching them on Windows creates thousands of native
// handles and can keep an orphaned dev server growing after its client exits.
const generatedWatchIgnores = [
  "**/.git/**",
  "**/node_modules/**",
  "**/src-tauri/target/**",
  "**/dist/**",
  "**/releases/**",
  "**/output/**"
];

export default defineConfig({
  root: projectRoot,
  plugins: [react()],
  clearScreen: false,
  server: {
    port: 1420,
    strictPort: true,
    watch: {
      ignored: generatedWatchIgnores
    }
  },
  envPrefix: ["VITE_", "TAURI_"],
  build: {
    target: "es2020",
    minify: !process.env.TAURI_DEBUG ? "esbuild" : false,
    sourcemap: Boolean(process.env.TAURI_DEBUG),
    rollupOptions: {
      input: {
        index: resolve(projectRoot, "index.html")
      }
    }
  }
});
