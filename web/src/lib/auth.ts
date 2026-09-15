export type AuthRole = "admin" | "viewer";

export type AccessPrivilege = "records" | "configs" | "schemaText" | "acls";

export type AuthUser = {
  sub: string;
  email: string | null;
  name: string | null;
  role: AuthRole | null;
  clusters: string[] | null;
};

export type AuthMe = {
  enabled: boolean;
  user: AuthUser | null;
};

export async function fetchAuth(): Promise<AuthMe> {
  const response = await fetch("/auth/me", { credentials: "include" });
  if (!response.ok) {
    throw new Error("Could not load authentication state");
  }

  return (await response.json()) as AuthMe;
}

export async function signOut(): Promise<void> {
  await fetch("/auth/logout", { method: "POST", credentials: "include" });
  window.location.assign("/login");
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

export function canAccessCluster(me: AuthMe | undefined, cluster: string): boolean {
  if (!me?.enabled) return true;
  const user = me.user;
  if (!user) return false;
  if (user.clusters == null) return true;
  return user.clusters.includes(cluster);
}

export function canAccess(
  me: AuthMe | undefined,
  cluster: string,
  privilege: AccessPrivilege,
): boolean {
  if (!me?.enabled) return true;
  const user = me.user;
  if (!user) return false;
  if (user.role == null) return true;
  if (!canAccessCluster(me, cluster)) return false;
  if (user.role === "admin") return true;
  switch (privilege) {
    case "records":
    case "configs":
    case "schemaText":
    case "acls":
      return false;
  }
}
