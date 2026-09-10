import { useEffect, useState } from "react";
import { TriangleAlertIcon } from "lucide-react";
import { Navigate, Outlet } from "react-router";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { AppHeader } from "@/components/app-header";
import { AppSidebar } from "@/components/app-sidebar";
import { CommandPalette } from "@/components/command-palette";
import { useClusters, useTopicRates } from "@/lib/api/queries";
import { useClusterName } from "@/lib/clusters";
import { findSearchHotkeyTarget, isTypingTarget } from "@/lib/keyboard";

export function AppLayout() {
  const cluster = useClusterName();
  const { data: clusters, isPending } = useClusters();
  useTopicRates(cluster);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const current = clusters?.find((entry) => entry.name === cluster);

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

  const known = clusters?.some((entry) => entry.name === cluster);

  if (!isPending && clusters && clusters.length > 0 && !known) {
    return <Navigate to={`/cluster/${clusters[0].name}`} replace />;
  }

  return (
    <SidebarProvider className="h-svh">
      <AppSidebar />
      <SidebarInset className="min-w-0 overflow-hidden">
        <AppHeader onSearch={() => setPaletteOpen(true)} />
        <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto p-4 md:p-6 md:group-has-data-[collapsible=icon]/sidebar-wrapper:px-8">
          {current?.status === "OFFLINE" ? (
            <Alert variant="destructive">
              <TriangleAlertIcon />
              <AlertTitle>Cluster unreachable</AlertTitle>
              <AlertDescription>
                Metadata for {current.label} could not be fetched. Catalog pages stay empty until
                the brokers respond.
              </AlertDescription>
            </Alert>
          ) : null}
          <Outlet />
        </div>
      </SidebarInset>
      <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} />
    </SidebarProvider>
  );
}
