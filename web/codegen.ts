import type { CodegenConfig } from "@graphql-codegen/cli";

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
        scalars: {
          DateTime: "string",
          // Int64 crosses the wire as a string so offsets and lag past 2^53
          // survive. Keeping it a string here means nothing silently rounds.
          Int64: "string",
        },
      },
    },
  },
};

export default config;
