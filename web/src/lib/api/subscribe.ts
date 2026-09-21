import { createClient } from "graphql-sse";

import type { TypedDocumentString } from "@/graphql/graphql";

const client = createClient({
  url: "/graphql",
  credentials: "same-origin",
  retryAttempts: 8,
});

export function subscribe<TResult, TVariables extends Record<string, unknown>>(
  document: TypedDocumentString<TResult, TVariables>,
  variables: TVariables,
  onNext: (data: TResult) => void,
) {
  return client.subscribe(
    {
      query: String(document),
      variables,
      operationName: document.__meta__?.operationName,
    },
    {
      next(result) {
        if (result.data) {
          onNext(result.data as TResult);
        }
      },
      error(error) {
        console.error(error);
      },
      complete() {},
    },
  );
}
