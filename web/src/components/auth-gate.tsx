import type { ReactNode } from "react"
import { Navigate, useLocation } from "react-router"

import { Spinner } from "@/components/ui/spinner"
import { useAuth } from "@/hooks/use-auth"

export function AuthGate({ children }: { children: ReactNode }) {
  const { pathname } = useLocation()
  const { data, isPending, isError } = useAuth()
  const onLogin = pathname === "/login"

  if (isPending) {
    return (
      <div className="flex min-h-svh items-center justify-center">
        <Spinner className="size-6" />
      </div>
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
