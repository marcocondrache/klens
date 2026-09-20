import { createClient } from "graphql-ws";

import type { TypedDocumentString } from "@/graphql/graphql";
import { operationName } from "@/graphql/operation-name";

function websocketUrl() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  return `${protocol}://${window.location.host}/graphql`;
}

const client = createClient({
  url: websocketUrl,
  lazy: true,
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
      operationName: operationName(document),
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
