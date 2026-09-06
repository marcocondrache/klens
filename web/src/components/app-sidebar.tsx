import {
  FileJsonIcon,
  GaugeIcon,
  HardDriveIcon,
  LayersIcon,
  ShieldCheckIcon,
  UsersRoundIcon,
} from "lucide-react"
import { Link, useLocation } from "react-router"

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  SidebarSeparator,
} from "@/components/ui/sidebar"
import { ClusterSwitcher } from "@/components/cluster-switcher"
import { useCluster } from "@/lib/api/queries"
import { clusterPath, useClusterName } from "@/lib/clusters"
import { formatCount } from "@/lib/format"

interface NavItem {
  label: string
  segment: string
  icon: typeof GaugeIcon
  count?: number
}

export function AppSidebar() {
  const cluster = useClusterName()
  const { data } = useCluster(cluster)
  const { pathname } = useLocation()

  const groups: Array<{ label: string; items: NavItem[] }> = [
    {
      label: "Cluster",
      items: [
        { label: "Overview", segment: "", icon: GaugeIcon },
        { label: "Nodes", segment: "nodes", icon: HardDriveIcon, count: data?.brokerCount },
      ],
    },
    {
      label: "Data",
      items: [
        { label: "Topics", segment: "topics", icon: LayersIcon, count: data?.topicCount },
        {
          label: "Consumer groups",
          segment: "groups",
          icon: UsersRoundIcon,
          count: data?.consumerGroupCount,
        },
        { label: "Schema registry", segment: "schemas", icon: FileJsonIcon },
      ],
    },
    {
      label: "Security",
      items: [{ label: "ACLs", segment: "acls", icon: ShieldCheckIcon }],
    },
  ]

  const root = clusterPath(cluster)

  function isActive(segment: string) {
    if (!segment) return pathname === root || pathname === `${root}/`
    return pathname.startsWith(`${root}/${segment}`)
  }

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader>
        <ClusterSwitcher />
      </SidebarHeader>

      <SidebarContent>
        {groups.map((group) => (
          <SidebarGroup key={group.label}>
            <SidebarGroupLabel>{group.label}</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {group.items.map((item) => (
                  <SidebarMenuItem key={item.label}>
                    <SidebarMenuButton
                      isActive={isActive(item.segment)}
                      tooltip={item.label}
                      render={<Link to={clusterPath(cluster, item.segment)} />}
                    >
                      <item.icon />
                      <span>{item.label}</span>
                    </SidebarMenuButton>
                    {item.count === undefined ? null : (
                      <SidebarMenuBadge className="numeric">
                        {formatCount(item.count)}
                      </SidebarMenuBadge>
                    )}
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        ))}
      </SidebarContent>

      <SidebarSeparator />

      <SidebarFooter>
        <div className="flex items-center gap-2 px-2 py-1 group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:px-0">
          <span className="size-1.5 shrink-0 rounded-full bg-brand" />
          <span className="truncate text-xs font-medium tracking-tight group-data-[collapsible=icon]:hidden">
            klens
          </span>
          <span className="numeric ml-auto text-[0.7rem] text-muted-foreground group-data-[collapsible=icon]:hidden">
            v0.1.0
          </span>
        </div>
      </SidebarFooter>

      <SidebarRail />
    </Sidebar>
  )
}
