import { queryOptions, useQuery } from "@tanstack/react-query";

import { fetchAuth } from "@/lib/auth";

export const authQuery = queryOptions({
  queryKey: ["auth", "me"],
  queryFn: fetchAuth,
  staleTime: 60_000,
  retry: false,
});

export function useAuth() {
  return useQuery(authQuery);
}
