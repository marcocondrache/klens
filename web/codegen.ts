import type { CodegenConfig } from "@graphql-codegen/cli"

const config: CodegenConfig = {
  schema: "../schema.graphql",
  ignoreNoDocuments: true,
  generates: {
    "src/lib/api/generated/graphql.ts": {
      plugins: ["typescript"],
      config: {
        enumsAsTypes: true,
        avoidOptionals: true,
        skipTypename: true,
        useTypeImports: true,
      },
    },
  },
}

export default config
