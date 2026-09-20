import { useLayoutEffect, type ReactNode } from "react";
import { Navigate, useLocation, useNavigate } from "@tanstack/react-router";

import { PageLoading } from "@/components/page-loading";
import { useAuth } from "@/hooks/use-auth";
import { safeNextPath } from "@/lib/auth";

export function AuthGate({ children }: { children: ReactNode }) {
  const { pathname, searchStr, hash } = useLocation();
  const { data, isPending, isError } = useAuth();
  const onLogin = pathname === "/login";

  if (isPending) {
    return <PageLoading title="Starting" description="Checking whether sign-in is required." />;
  }

  if (isError) {
    return children;
  }

  const enabled = data?.enabled ?? false;
  const user = data?.user ?? null;

  if (enabled && !user && !onLogin) {
    const next = safeNextPath(`${pathname}${searchStr}${hash}`);
    return <Navigate to="/login" search={next ? { next } : {}} replace />;
  }

  if ((!enabled || user) && onLogin) {
    return <RedirectHref href={loginReturnPath(searchStr)} />;
  }

  return children;
}

function loginReturnPath(searchStr: string): string {
  const query = searchStr.startsWith("?") ? searchStr.slice(1) : searchStr;
  return safeNextPath(new URLSearchParams(query).get("next")) ?? "/";
}

function RedirectHref({ href }: { href: string }) {
  const navigate = useNavigate();
  useLayoutEffect(() => {
    void navigate({ href, replace: true });
  }, [href, navigate]);
  return null;
}
