import { queryOptions, useQuery } from "@tanstack/react-query";

import { get } from "@/lib/api/client";
import type { AuthMe } from "@/lib/auth";

export const authQuery = queryOptions({
  queryKey: ["auth", "me"],
  queryFn: () => get<AuthMe>("/auth/me"),
  staleTime: 60_000,
});

export function useAuth() {
  return useQuery(authQuery);
}
