import { SearchIcon } from "lucide-react";
import { Link, useLocation } from "react-router";

import { Button } from "@/components/ui/button";
import { Kbd } from "@/components/ui/kbd";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  SidebarSeparator,
  useSidebar,
} from "@/components/ui/sidebar";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ClusterSwitcher } from "@/components/cluster-switcher";
import { GithubIcon } from "@/components/icons";
import { useCluster, useSchemaSubjects } from "@/lib/api/queries";
import { RELEASE_URL, REPO_URL, VERSION } from "@/lib/build";
import { clusterPath, useClusterName } from "@/lib/clusters";
import { formatCount } from "@/lib/format";
import { SECTIONS } from "@/lib/sections";
import { cn } from "@/lib/utils";

const NAV_BUTTON =
  "h-9 gap-0 rounded-md p-0 text-sidebar-foreground/80 hover:bg-sidebar-accent hover:text-sidebar-foreground data-active:bg-sidebar-accent data-active:font-medium data-active:text-sidebar-foreground data-active:hover:bg-sidebar-accent group-data-[collapsible=icon]:size-9! group-data-[collapsible=icon]:p-0!";

function SidebarFind({ onSearch }: { onSearch: () => void }) {
  const { state, isMobile } = useSidebar();
  const collapsed = state === "collapsed" && !isMobile;

  if (collapsed) {
    return (
      <Tooltip>
        <TooltipTrigger
          render={
            <Button
              type="button"
              variant="ghost"
              size="icon"
              aria-label="Find"
              onClick={onSearch}
              className="size-9 text-sidebar-foreground/70 hover:bg-sidebar-accent hover:text-sidebar-foreground"
            />
          }
        >
          <SearchIcon className="size-4" />
        </TooltipTrigger>
        <TooltipContent side="right" align="center">
          Find
        </TooltipContent>
      </Tooltip>
    );
  }

  return (
    <button
      type="button"
      onClick={onSearch}
      className="relative flex h-9 w-full items-center rounded-md bg-background outline outline-1 outline-sidebar-border transition-colors hover:bg-sidebar-accent/60 focus-visible:outline-2 focus-visible:outline-ring"
    >
      <span className="grid size-9 shrink-0 place-content-center text-muted-foreground">
        <SearchIcon className="size-4" />
      </span>
      <span className="flex-1 truncate text-left text-sm text-muted-foreground">Find</span>
      <span className="grid size-9 place-content-center">
        <Kbd className="bg-background shadow-[0_0_0_1px_var(--sidebar-border)]">/</Kbd>
      </span>
    </button>
  );
}

function NavLink({
  label,
  icon: Icon,
  count,
  active,
  to,
}: {
  label: string;
  icon: (typeof SECTIONS)[number]["icon"];
  count?: number;
  active: boolean;
  to: string;
}) {
  return (
    <SidebarMenuItem>
      <SidebarMenuButton
        isActive={active}
        tooltip={label}
        className={NAV_BUTTON}
        render={<Link to={to} />}
      >
        <span className="grid size-9 shrink-0 place-content-center">
          <Icon className="size-4" />
        </span>
        <span className="min-w-0 flex-1 truncate pr-2 text-sm">{label}</span>
      </SidebarMenuButton>
      {count === undefined ? null : (
        <SidebarMenuBadge className="numeric right-2 text-muted-foreground peer-data-active/menu-button:text-sidebar-foreground">
          {formatCount(count)}
        </SidebarMenuBadge>
      )}
    </SidebarMenuItem>
  );
}

export function AppSidebar({ onSearch }: { onSearch: () => void }) {
  const cluster = useClusterName();
  const { pathname } = useLocation();

  const { data } = useCluster(cluster);
  const { data: subjects } = useSchemaSubjects(cluster);

  const counts: Record<string, number | undefined> = {
    topics: data?.topicCount,
    groups: data?.consumerGroupCount,
    schemas: subjects?.length,
    nodes: data?.brokerCount,
  };

  const browse = SECTIONS.filter((section) => section.group === "browse");
  const infra = SECTIONS.filter((section) => section.group === "infra");

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="gap-1 pt-1">
        <div className="px-2 pt-1 group-data-[collapsible=icon]:px-1">
          <ClusterSwitcher variant="sidebar" />
        </div>
        <div className="px-2 group-data-[collapsible=icon]:flex group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:px-1">
          <SidebarFind onSearch={onSearch} />
        </div>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup className="px-2 py-0">
          <SidebarGroupContent>
            <SidebarMenu className="gap-px">
              {browse.map((section) => (
                <NavLink
                  key={section.segment}
                  label={section.label}
                  icon={section.icon}
                  count={counts[section.segment]}
                  active={pathname.startsWith(clusterPath(cluster, section.segment))}
                  to={clusterPath(cluster, section.segment)}
                />
              ))}

              <SidebarSeparator className="mx-0 my-1 w-full bg-sidebar-border" />

              {infra.map((section) => (
                <NavLink
                  key={section.segment}
                  label={section.label}
                  icon={section.icon}
                  count={counts[section.segment]}
                  active={pathname.startsWith(clusterPath(cluster, section.segment))}
                  to={clusterPath(cluster, section.segment)}
                />
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="gap-0 p-2 group-data-[collapsible=icon]:items-center">
        <div
          className={cn(
            "flex w-full items-center gap-2 rounded-full px-2.5 py-1.5",
            "group-data-[collapsible=icon]:justify-center group-data-[collapsible=icon]:px-0",
          )}
        >
          <img
            src="/favicon.svg"
            alt=""
            className="size-5 shrink-0 group-data-[collapsible=icon]:size-4"
          />
          <div className="flex min-w-0 flex-1 items-center gap-1.5 group-data-[collapsible=icon]:hidden">
            <Tooltip>
              <TooltipTrigger
                render={
                  <a
                    href={RELEASE_URL}
                    target="_blank"
                    rel="noreferrer"
                    className="min-w-0 truncate text-sm text-sidebar-foreground transition-colors hover:text-foreground"
                  />
                }
              >
                <span className="numeric font-mono text-xs text-muted-foreground">v{VERSION}</span>
              </TooltipTrigger>
              <TooltipContent>GitHub release</TooltipContent>
            </Tooltip>
          </div>
          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon-xs"
                  aria-label="Open repository"
                  className="size-6 rounded-full border border-sidebar-border bg-background text-muted-foreground hover:bg-sidebar-accent hover:text-sidebar-foreground group-data-[collapsible=icon]:hidden"
                  render={<a href={REPO_URL} target="_blank" rel="noreferrer" />}
                />
              }
            >
              <GithubIcon className="size-3" />
            </TooltipTrigger>
            <TooltipContent>Repository</TooltipContent>
          </Tooltip>
        </div>
      </SidebarFooter>

      <SidebarRail />
    </Sidebar>
  );
}
