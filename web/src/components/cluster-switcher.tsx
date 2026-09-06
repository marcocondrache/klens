import { CheckIcon, ChevronsUpDownIcon, ServerIcon } from "lucide-react"
import { useLocation, useNavigate } from "react-router"

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { SidebarMenu, SidebarMenuButton, SidebarMenuItem, useSidebar } from "@/components/ui/sidebar"
import { EnvironmentBadge, StatusDot } from "@/components/status"
import { useClusterName } from "@/lib/clusters"
import { useClusters } from "@/lib/api/queries"
import type { ClusterStatus } from "@/lib/api/types"

const SECTIONS = new Set(["topics", "groups", "nodes", "schemas", "acls"])

const STATUS_TONE: Record<ClusterStatus, "ok" | "warn" | "error"> = {
  healthy: "ok",
  degraded: "warn",
  offline: "error",
}

export function ClusterSwitcher() {
  const active = useClusterName()
  const { data: clusters = [] } = useClusters()
  const navigate = useNavigate()
  const location = useLocation()
  const { state } = useSidebar()

  const current = clusters.find((cluster) => cluster.name === active)
  const collapsed = state === "collapsed"

  function switchTo(name: string) {
    const [, , , section] = location.pathname.split("/")
    const target = section && SECTIONS.has(section) ? `/cluster/${name}/${section}` : `/cluster/${name}`
    navigate(target)
  }

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger
            render={
              <SidebarMenuButton
                size="lg"
                tooltip={current?.label ?? active}
                className="data-[popup-open]:bg-sidebar-accent"
              />
            }
          >
            <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-brand/12 text-brand">
              <ServerIcon className="size-4" />
            </span>
            <span className="grid flex-1 text-left leading-tight">
              <span className="truncate text-sm font-medium">{current?.label ?? active}</span>
              <span className="truncate text-xs text-muted-foreground">
                {current ? `${current.brokerCount} brokers · ${current.version}` : "loading…"}
              </span>
            </span>
            {collapsed ? null : <ChevronsUpDownIcon className="ml-auto size-4 opacity-60" />}
          </DropdownMenuTrigger>

          <DropdownMenuContent align="start" side="bottom" className="w-64">
            <DropdownMenuGroup>
              <DropdownMenuLabel className="text-xs text-muted-foreground">
                Clusters
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              {clusters.map((cluster) => (
                <DropdownMenuItem
                  key={cluster.name}
                  onClick={() => switchTo(cluster.name)}
                  className="gap-2"
                >
                  <StatusDot tone={STATUS_TONE[cluster.status]} />
                  <span className="flex-1 truncate">{cluster.label}</span>
                  <EnvironmentBadge environment={cluster.environment} />
                  {cluster.name === active ? <CheckIcon className="size-3.5 text-brand" /> : null}
                </DropdownMenuItem>
              ))}
            </DropdownMenuGroup>
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  )
}
