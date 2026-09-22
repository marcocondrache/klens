import { useEffect, type ReactNode } from "react";
import { Navigate, useLocation } from "@tanstack/react-router";

import { PageLoading } from "@/components/page-loading";
import { useAuth } from "@/hooks/use-auth";
import { redirectToSignIn, SIGNED_OUT_PATH } from "@/lib/api/client";

export function AuthGate({ children }: { children: ReactNode }) {
  const { pathname } = useLocation();
  const { data, isPending, isError } = useAuth();
  const onSignedOut = pathname === SIGNED_OUT_PATH;

  const enabled = data?.enabled ?? false;
  const user = data?.user ?? null;
  const signIn = enabled && !user && !onSignedOut;

  useEffect(() => {
    if (signIn) redirectToSignIn();
  }, [signIn]);

  if (isPending) {
    return <PageLoading title="Starting" description="Checking if you need to sign in." />;
  }

  if (isError) {
    return children;
  }

  if (signIn) {
    return <PageLoading title="Signing in" description="Redirecting to your identity provider." />;
  }

  if ((!enabled || user) && onSignedOut) {
    return <Navigate to="/" replace />;
  }

  return children;
}
