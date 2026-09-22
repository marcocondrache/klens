import type { QueryClient } from "@tanstack/react-query";
import { createRootRouteWithContext, Outlet, redirect } from "@tanstack/react-router";

import { PageLoading } from "@/components/page-loading";
import { authQuery } from "@/hooks/use-auth";
import { signInHref, SIGNED_OUT_PATH } from "@/lib/api/client";
import type { AuthMe } from "@/lib/auth";
import { NotFoundPage } from "@/routes/-not-found";

export const Route = createRootRouteWithContext<{ queryClient: QueryClient }>()({
  beforeLoad: async ({ context, location }) => {
    let auth: AuthMe;
    try {
      auth = await context.queryClient.ensureQueryData(authQuery);
    } catch {
      // Without auth state, let the API decide: a 401 still sends the browser to sign in.
      return;
    }

    const signedIn = !auth.enabled || auth.user != null;
    const onSignedOut = location.pathname === SIGNED_OUT_PATH;

    if (signedIn && onSignedOut) {
      throw redirect({ to: "/", replace: true });
    }
    if (!signedIn && !onSignedOut) {
      throw redirect({ href: signInHref(), reloadDocument: true });
    }
  },
  pendingComponent: () => (
    <PageLoading title="Starting" description="Checking if you need to sign in." />
  ),
  component: Outlet,
  notFoundComponent: NotFoundPage,
});
