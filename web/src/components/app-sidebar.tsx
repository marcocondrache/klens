import type { ComponentProps } from "react";
import { TagIcon } from "lucide-react";

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarRail,
} from "@/components/ui/sidebar";
import { ClusterSwitcher } from "@/components/cluster-switcher";
import { GithubIcon } from "@/components/icons";
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
    title: "Release notes",
    url: RELEASE_URL,
    icon: <TagIcon />,
    label: <span className="numeric font-mono">v{VERSION}</span>,
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
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader>
        <ClusterSwitcher />
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
        <NavSecondary items={NAV_SECONDARY} className="mt-auto" />
      </SidebarContent>
      {auth?.enabled && auth.user ? (
        <SidebarFooter>
          <NavUser user={auth.user} />
        </SidebarFooter>
      ) : null}
      <SidebarRail />
    </Sidebar>
  );
}
