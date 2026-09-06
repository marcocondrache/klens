import { useEffect, useState } from "react"
import { Navigate, Outlet } from "react-router"

import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar"
import { AppHeader } from "@/components/app-header"
import { AppSidebar } from "@/components/app-sidebar"
import { CommandPalette } from "@/components/command-palette"
import { useClusters } from "@/lib/api/queries"
import { DEFAULT_CLUSTER, useClusterName } from "@/lib/clusters"

export function AppLayout() {
  const cluster = useClusterName()
  const { data: clusters, isPending } = useClusters()
  const [paletteOpen, setPaletteOpen] = useState(false)

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault()
        setPaletteOpen((open) => !open)
      }
    }

    window.addEventListener("keydown", onKeyDown)
    return () => window.removeEventListener("keydown", onKeyDown)
  }, [])

  const known = clusters?.some((entry) => entry.name === cluster)

  if (!isPending && clusters?.length && !known) {
    return <Navigate to={`/cluster/${clusters[0]?.name ?? DEFAULT_CLUSTER}`} replace />
  }

  return (
    <SidebarProvider>
      <AppSidebar />
      <SidebarInset className="min-w-0 overflow-hidden">
        <AppHeader onSearch={() => setPaletteOpen(true)} />
        <div className="flex-1 space-y-5 p-4 md:p-6">
          <Outlet />
        </div>
      </SidebarInset>
      <CommandPalette open={paletteOpen} onOpenChange={setPaletteOpen} />
    </SidebarProvider>
  )
}
