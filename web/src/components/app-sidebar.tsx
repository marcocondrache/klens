import type { ComponentProps } from "react";
import { Link } from "@tanstack/react-router";

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
} from "@/components/ui/sidebar";
import { BuildLinks } from "@/components/build-links";
import { LogoMark } from "@/components/logo";
import { NavMain } from "@/components/nav-main";
import { NavUser } from "@/components/nav-user";
import { useAccess } from "@/hooks/use-access";
import { useAuth } from "@/hooks/use-auth";
import { useClusterHealth } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { visibleSections } from "@/lib/sections";

export function AppSidebar(props: ComponentProps<typeof Sidebar>) {
  const cluster = useClusterName();
  const { can } = useAccess();
  const { data: auth } = useAuth();
  const { data: health } = useClusterHealth(cluster);
  const user = auth?.enabled ? auth.user : null;

  const topology = health?.topology.updatedAt == null ? undefined : health;
  const subjects = health?.subjects.updatedAt == null ? undefined : health;

  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              size="lg"
              render={<Link to="/cluster/$cluster" params={{ cluster }} />}
            >
              <div className="flex aspect-square size-8 items-center justify-center text-sidebar-accent-foreground">
                <LogoMark className="size-5!" />
              </div>
              <span className="truncate text-base font-semibold tracking-[-0.02em] text-sidebar-accent-foreground [text-box:trim-both_cap_alphabetic]">
                klens
              </span>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        <NavMain
          cluster={cluster}
          sections={visibleSections(can(cluster, "ACLS"))}
          counts={{
            topics: topology?.topicCount,
            groups: topology?.groupCount,
            schemas: subjects?.subjectCount,
            nodes: topology?.brokerCount,
          }}
        />
      </SidebarContent>
      <SidebarFooter>
        {user ? <NavUser user={user} /> : null}
        <BuildLinks className="py-1 group-data-[collapsible=icon]:hidden" />
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  );
}
