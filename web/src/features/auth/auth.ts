import { queryOptions, useQuery } from "@tanstack/react-query";

import { apiPath, get, LOGIN_PATH } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { AuthMe, AuthUser } from "@/lib/api/types";

export async function signOut(): Promise<void> {
  await fetch(apiPath("/auth/logout"), { method: "POST", credentials: "include" });
  window.location.assign(LOGIN_PATH);
}

export function displayName(user: AuthUser): string {
  return user.name?.trim() || user.email?.trim() || user.sub;
}

export function initials(user: AuthUser): string {
  const name = user.name?.trim();
  if (name) {
    const parts = name.split(/\s+/).filter(Boolean);
    if (parts.length >= 2) {
      return `${parts[0][0]}${parts[parts.length - 1][0]}`.toUpperCase();
    }
    return name.slice(0, 2).toUpperCase();
  }

  const email = user.email?.trim();
  if (email) {
    return email.slice(0, 2).toUpperCase();
  }

  return user.sub.slice(0, 2).toUpperCase();
}

export const authQuery = queryOptions({
  queryKey: keys.authMe(),
  queryFn: () => get<AuthMe>("/auth/me"),
  staleTime: 60_000,
});

export function useAuth() {
  return useQuery(authQuery);
}
