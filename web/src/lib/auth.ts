export type AuthUser = {
  sub: string
  email: string | null
  name: string | null
}

export type AuthMe = {
  enabled: boolean
  user: AuthUser | null
}

export async function fetchAuth(): Promise<AuthMe> {
  const response = await fetch("/auth/me", { credentials: "include" })
  if (!response.ok) {
    throw new Error("Could not load authentication state")
  }

  return (await response.json()) as AuthMe
}

export async function signOut(): Promise<void> {
  await fetch("/auth/logout", { method: "POST", credentials: "include" })
  window.location.assign("/login")
}

export function displayName(user: AuthUser): string {
  return user.name?.trim() || user.email?.trim() || user.sub
}
