import type { ComponentProps } from "react";
import { TagIcon } from "lucide-react";
import { Link } from "@tanstack/react-router";

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar";
import { GithubIcon } from "@/components/icons";
import { NavClusters } from "@/components/nav-clusters";
import { NavMain } from "@/components/nav-main";
import { NavSecondary } from "@/components/nav-secondary";
import { NavUser } from "@/components/nav-user";
import { useAccess } from "@/hooks/use-access";
import { useAuth } from "@/hooks/use-auth";
import { useClusterHealth } from "@/lib/api/catalog";
import { RELEASE_URL, REPO_URL, VERSION } from "@/lib/build";
import { useClusterName } from "@/lib/clusters";
import { visibleSections } from "@/lib/sections";

const NAV_SECONDARY = [
  { title: "Repository", url: REPO_URL, icon: <GithubIcon /> },
  {
    title: <span className="numeric font-mono">v{VERSION}</span>,
    url: RELEASE_URL,
    icon: <TagIcon />,
  },
];

export function AppSidebar(props: ComponentProps<typeof Sidebar>) {
  const cluster = useClusterName();
  const { can } = useAccess();
  const { data: auth } = useAuth();
  const { data: health } = useClusterHealth(cluster);

  const topology = health?.topology.updatedAt == null ? undefined : health;
  const subjects = health?.subjects.updatedAt == null ? undefined : health;

  return (
    <Sidebar collapsible="offcanvas" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton
              className="data-[slot=sidebar-menu-button]:p-1.5!"
              render={<Link to="/cluster/$cluster" params={{ cluster }} />}
            >
              <img src="/favicon.svg" alt="" className="size-5!" />
              <span className="text-base font-semibold">klens</span>
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
        <NavClusters active={cluster} />
        <NavSecondary items={NAV_SECONDARY} className="mt-auto" />
      </SidebarContent>
      {auth?.enabled && auth.user ? (
        <SidebarFooter>
          <NavUser user={auth.user} />
        </SidebarFooter>
      ) : null}
    </Sidebar>
  );
}
