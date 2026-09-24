import { createRootRoute, Outlet } from "@tanstack/react-router";

import { AuthGate } from "@/components/auth-gate";
import { NotFoundPage } from "@/routes/-not-found";

export const Route = createRootRoute({
  component: App,
  notFoundComponent: NotFoundPage,
});

function App() {
  return (
    <AuthGate>
      <Outlet />
    </AuthGate>
  );
}
