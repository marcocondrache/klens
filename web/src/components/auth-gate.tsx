import type { ReactNode } from "react"
import { Navigate, useLocation } from "react-router"

import { PageLoading } from "@/components/page-loading"
import { useAuth } from "@/hooks/use-auth"

export function AuthGate({ children }: { children: ReactNode }) {
  const { pathname } = useLocation()
  const { data, isPending, isError } = useAuth()
  const onLogin = pathname === "/login"

  if (isPending) {
    return (
      <PageLoading
        title="Starting"
        description="Checking whether sign-in is required."
      />
    )
  }

  if (isError) {
    return children
  }

  const enabled = data?.enabled ?? false
  const user = data?.user ?? null

  if (enabled && !user && !onLogin) {
    return <Navigate to="/login" replace />
  }

  if ((!enabled || user) && onLogin) {
    return <Navigate to="/" replace />
  }

  return children
}
