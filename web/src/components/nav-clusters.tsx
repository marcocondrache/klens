import { Link } from "@tanstack/react-router";

import {
  SidebarGroup,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { StatusDot } from "@/components/status";
import { useClusters } from "@/lib/api/catalog";
import { clusterTone } from "@/lib/clusters";
import { clusterSectionTo, useActiveSection } from "@/lib/sections";

export function NavClusters({ active }: { active: string }) {
  const { setOpenMobile } = useSidebar();
  const { data: clusters = [] } = useClusters();
  const section = useActiveSection();
  const to = section ? clusterSectionTo(section.segment) : "/cluster/$cluster";

  return (
    <SidebarGroup className="group-data-[collapsible=icon]:hidden">
      <SidebarGroupLabel>Clusters</SidebarGroupLabel>
      <SidebarMenu>
        {clusters.map((entry) => (
          <SidebarMenuItem key={entry.cluster}>
            <SidebarMenuButton
              isActive={entry.cluster === active}
              onClick={() => setOpenMobile(false)}
              render={<Link to={to} params={{ cluster: entry.cluster }} />}
            >
              <span className="flex size-4 shrink-0 items-center justify-center">
                <StatusDot tone={clusterTone(entry)} />
              </span>
              <span>{entry.cluster}</span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        ))}
      </SidebarMenu>
    </SidebarGroup>
  );
}
