import { readFileSync } from "node:fs";
import path from "path";
import babel from "@rolldown/plugin-babel";
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

export default defineConfig({
  fmt: {},
  lint: {
    ignorePatterns: ["src/graphql/gql.ts", "src/graphql/graphql.ts", "src/graphql/index.ts"],
    plugins: ["react", "typescript", "oxc"],
    rules: {
      "react/rules-of-hooks": "error",
      "react/only-export-components": [
        "warn",
        {
          allowConstantExport: true,
        },
      ],
      "vite-plus/prefer-vite-plus-imports": "error",
    },
    options: {
      typeAware: true,
      typeCheck: true,
    },
    jsPlugins: [
      {
        name: "vite-plus",
        specifier: "vite-plus/oxlint-plugin",
      },
    ],
  },
  plugins: lazyPlugins(() => [react(), babel({ presets: [reactCompilerPreset()] }), tailwindcss()]),
  define: {
    __APP_VERSION__: JSON.stringify(appVersion()),
  },
  server: {
    proxy: {
      "/health": "http://localhost:8080",
      "/api": "http://localhost:8080",
      "/auth": "http://localhost:8080",
      "/graphql": {
        target: "http://localhost:8080",
        ws: true,
      },
      "/graphiql": "http://localhost:8080",
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
