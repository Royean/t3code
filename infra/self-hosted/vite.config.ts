import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: ["src/bin.ts"],
    platform: "node",
    target: "node24",
    format: "esm",
    outDir: "dist",
    clean: true,
    sourcemap: false,
    outputOptions: { codeSplitting: false },
    deps: {
      alwaysBundle: (id) => !id.startsWith("node:"),
      neverBundle: (id) => id.startsWith("node:"),
      onlyBundle: false,
    },
  },
});
