import type { CodegenConfig } from "@graphql-codegen/cli"

const config: CodegenConfig = {
  schema: "../schema.graphql",
  documents: ["src/**/*.{ts,tsx}", "!src/graphql/**/*"],
  ignoreNoDocuments: true,
  generates: {
    "./src/graphql/": {
      preset: "client",
      presetConfig: {
        fragmentMasking: false,
      },
      config: {
        enumsAsTypes: true,
        avoidOptionals: true,
        skipTypename: true,
        useTypeImports: true,
        documentMode: "string",
      },
    },
  },
}

export default config
