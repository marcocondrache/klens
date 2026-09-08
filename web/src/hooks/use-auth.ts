import { useQuery } from "@tanstack/react-query";

import { fetchAuth } from "@/lib/auth";

export const authQueryKey = ["auth", "me"] as const;

export function useAuth() {
  return useQuery({
    queryKey: authQueryKey,
    queryFn: fetchAuth,
    staleTime: 60_000,
    retry: false,
  });
}
