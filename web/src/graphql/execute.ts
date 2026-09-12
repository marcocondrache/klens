import { GraphQLError } from "@/lib/graphql-error";

import type { TypedDocumentString } from "./graphql";

export async function execute<TResult, TVariables>(
  query: TypedDocumentString<TResult, TVariables>,
  ...[variables]: TVariables extends Record<string, never> ? [] : [TVariables]
) {
  const response = await fetch("/graphql", {
    method: "POST",
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/graphql-response+json",
    },
    body: JSON.stringify({
      query,
      variables,
    }),
  });

  if (response.status === 401) {
    if (window.location.pathname !== "/login") {
      window.location.assign("/login");
    }
    throw new Error("Unauthorized");
  }

  const payload = (await response.json()) as {
    data?: TResult;
    errors?: Array<{ message: string; extensions?: { code?: string } }>;
  };

  if (!response.ok) {
    const first = payload.errors?.[0];
    throw new GraphQLError(
      first?.message ?? "Network response was not ok",
      first?.extensions?.code,
    );
  }

  if (payload.errors?.length) {
    const first = payload.errors[0];
    throw new GraphQLError(first?.message ?? "GraphQL request failed", first?.extensions?.code);
  }

  if (payload.data === undefined) {
    throw new Error("GraphQL response was empty");
  }

  return payload.data;
}
