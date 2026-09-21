import { createClient, type ClientOptions } from "graphql-sse";

import type { TypedDocumentString } from "@/graphql/graphql";

// graphql-sse posts the operation. Subscriptions are GET, same as the old
// socket upgrade, so the JSON body is moved onto the query string.
const fetchSubscription: ClientOptions["fetchFn"] = async (
  url: Parameters<typeof fetch>[0],
  init?: Parameters<typeof fetch>[1],
) => {
  const body = init?.body;
  if (typeof body !== "string") {
    return fetch(url, init);
  }

  const request = JSON.parse(body) as {
    query: string;
    operationName?: string;
    variables?: Record<string, unknown>;
  };
  const params = new URLSearchParams({ query: request.query });
  if (request.operationName) {
    params.set("operationName", request.operationName);
  }
  if (request.variables) {
    params.set("variables", JSON.stringify(request.variables));
  }

  if (typeof url !== "string") {
    return fetch(url, init);
  }
  const target = new URL(url, window.location.origin);
  target.search = params.toString();
  const headers = new Headers(init?.headers);
  headers.delete("content-type");
  return fetch(target, { ...init, method: "GET", body: undefined, headers });
};

const client = createClient({
  url: "/graphql",
  credentials: "same-origin",
  retryAttempts: 8,
  fetchFn: fetchSubscription,
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
