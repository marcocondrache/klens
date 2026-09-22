import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { TriangleAlertIcon } from "lucide-react";
import { Navigate, Outlet, createFileRoute, useMatch } from "@tanstack/react-router";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { AppHeader } from "@/components/app-header";
import { AppSidebar } from "@/components/app-sidebar";
import { CommandPalette } from "@/components/command-palette";
import { CatalogLoading, ClustersLoading } from "@/components/page-loading";
import { useClusterHealth, useClusterNames } from "@/lib/api/catalog";
import { useUpdates, type Scope } from "@/lib/api/updates";
import { isFirstCatalogPending, useClusterName } from "@/lib/clusters";
import { findSearchHotkeyTarget, isTypingTarget } from "@/lib/keyboard";
import { NotFoundPage } from "@/routes/-not-found";

export const Route = createFileRoute("/cluster/$cluster")({
  component: AppLayout,
  notFoundComponent: NotFoundPage,
});

function AppLayout() {
  const cluster = useClusterName();
  const { data: clusters, isPending } = useClusterNames();
  const { data: health } = useClusterHealth(cluster);
  useUpdates(cluster, useRouteScope());
  const [paletteOpen, setPaletteOpen] = useState(false);
  const known = clusters?.includes(cluster);

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        setPaletteOpen((open) => !open);
        return;
      }

      if (event.key !== "/" || event.metaKey || event.ctrlKey || event.altKey) return;
      if (isTypingTarget(event.target)) return;

      const search = findSearchHotkeyTarget();
      if (search) {
        event.preventDefault();
        search.focus();
        return;
      }

      event.preventDefault();
      setPaletteOpen(true);
    }

    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  if (!isPending && clusters && clusters.length > 0 && !known) {
    return <Navigate to="/cluster/$cluster" params={{ cluster: clusters[0] }} replace />;
  }

  if (isPending) {
    return <ClustersLoading />;
  }

  if (isFirstCatalogPending(health)) {
    return <CatalogLoading />;
  }

  const topology = health?.topology;

  return (
    <SidebarProvider
      defaultOpen={sidebarDefaultOpen()}
      className="h-svh"
      style={
        {
          "--sidebar-width": "calc(var(--spacing) * 72)",
          "--header-height": "calc(var(--spacing) * 12)",
        } as CSSProperties
      }
    >
      <AppSidebar variant="inset" />
      <SidebarInset className="min-w-0 overflow-hidden">
        <AppHeader onSearch={() => setPaletteOpen(true)} />
        <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto p-4 lg:p-6">
          {topology?.lastError ? (
            <Alert variant="destructive">
              <TriangleAlertIcon />
              <AlertTitle>
                {topology.updatedAt == null ? "Cluster unreachable" : "Topology lane failing"}
              </AlertTitle>
              <AlertDescription>{topology.lastError}</AlertDescription>
            </Alert>
          ) : null}
          <Outlet />
        </div>
      </SidebarInset>
      <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} />
    </SidebarProvider>
  );
}

/** Restores the state `SidebarProvider` persists in the `sidebar_state` cookie. */
function sidebarDefaultOpen(): boolean {
  return !document.cookie.split("; ").includes("sidebar_state=false");
}

function useRouteScope(): Scope {
  const topic = useMatch({
    from: "/cluster/$cluster/topics_/$topic",
    shouldThrow: false,
    select: (match) => match.params.topic,
  });
  const group = useMatch({
    from: "/cluster/$cluster/groups_/$group",
    shouldThrow: false,
    select: (match) => match.params.group,
  });

  return useMemo(
    () => ({ ...(topic ? { topic } : {}), ...(group ? { group } : {}) }),
    [topic, group],
  );
}
