import type { QueryClient } from "@tanstack/react-query";
import { createRootRouteWithContext, Outlet, redirect } from "@tanstack/react-router";
import { NuqsAdapter } from "nuqs/adapters/tanstack-router";

import { PageLoading } from "@/components/page-loading";
import { authQuery } from "@/hooks/use-auth";
import { LOGIN_PATH, SIGN_IN_PATH } from "@/lib/api/client";
import type { AuthMe } from "@/lib/auth";
import { NotFoundPage } from "@/routes/-not-found";

export const Route = createRootRouteWithContext<{ queryClient: QueryClient }>()({
  beforeLoad: async ({ context, location }) => {
    let auth: AuthMe;
    try {
      auth = await context.queryClient.ensureQueryData(authQuery);
    } catch {
      return;
    }

    const signedIn = !auth.enabled || auth.user != null;
    const onLogin = location.pathname === LOGIN_PATH;

    if (signedIn && onLogin) {
      throw redirect({ to: "/", replace: true });
    }
    if (!signedIn && !onLogin) {
      throw redirect({ href: SIGN_IN_PATH, reloadDocument: true });
    }
  },
  pendingComponent: () => (
    <PageLoading title="Starting" description="Checking if you need to sign in." />
  ),
  component: RootLayout,
  notFoundComponent: NotFoundPage,
});

function RootLayout() {
  return (
    <NuqsAdapter>
      <Outlet />
    </NuqsAdapter>
  );
}
