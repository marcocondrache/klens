import { readFileSync } from "node:fs";
import path from "path";
import babel from "@rolldown/plugin-babel";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import { defineConfig, lazyPlugins } from "vite-plus";

function appVersion() {
  try {
    const cargo = readFileSync(path.resolve(import.meta.dirname, "../Cargo.toml"), "utf8");
    return cargo.match(/^version = "(.+)"$/m)?.[1] ?? "0.0.0";
  } catch {
    return "0.0.0";
  }
}

const readonlyPatterns = ["src/api/types.gen.ts", "src/routeTree.gen.ts", "src/components/ui"];

export default defineConfig({
  fmt: {
    ignorePatterns: readonlyPatterns,
  },
  lint: {
    ignorePatterns: readonlyPatterns,
    plugins: ["react", "typescript", "oxc"],
    jsPlugins: ["@shadcn/lint"],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  plugins: lazyPlugins(() => [
    tanstackRouter({
      target: "react",
      autoCodeSplitting: true,
      quoteStyle: "double",
    }),
    react(),
    babel({ presets: [reactCompilerPreset()] }),
    tailwindcss(),
  ]),
  define: {
    __APP_VERSION__: JSON.stringify(appVersion()),
  },
  server: {
    proxy: {
      "/api": "http://localhost:8080",
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(import.meta.dirname, "./src"),
    },
  },
  build: {
    outDir: "../static",
    emptyOutDir: true,
  },
});
