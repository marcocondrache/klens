import type { CodegenConfig } from "@graphql-codegen/cli";
import { Kind, type DocumentNode } from "graphql";

const config: CodegenConfig = {
  schema: "../schema.graphql",
  documents: ["src/**/*.{ts,tsx}", "!src/graphql/**/*"],
  ignoreNoDocuments: true,
  generates: {
    "./src/graphql/": {
      preset: "client",
      presetConfig: {
        fragmentMasking: false,
        onExecutableDocumentNode(document: DocumentNode) {
          const operation = document.definitions.find(
            (definition) => definition.kind === Kind.OPERATION_DEFINITION,
          );
          if (operation?.kind !== Kind.OPERATION_DEFINITION) {
            return;
          }
          const name = operation.name?.value;
          return name ? { operationName: name } : undefined;
        },
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
