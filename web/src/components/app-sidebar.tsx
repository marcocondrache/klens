import { Link, useLocation } from "react-router";

import { Button } from "@/components/ui/button";
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
} from "@/components/ui/sidebar";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { GithubIcon } from "@/components/icons";
import { useCluster, useSchemaSubjects } from "@/lib/api/queries";
import { RELEASE_URL, REPO_URL, VERSION } from "@/lib/build";
import { clusterPath, useClusterName } from "@/lib/clusters";
import { formatCount } from "@/lib/format";
import { SECTIONS } from "@/lib/sections";

const ACTIVE_MARKER =
  "relative data-active:before:absolute data-active:before:inset-y-1.5 data-active:before:-left-3 data-active:before:w-0.5 data-active:before:rounded-r-full data-active:before:bg-sidebar-primary";

export function AppSidebar() {
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

  return (
    <Sidebar collapsible="icon">
      <SidebarHeader className="group-data-[collapsible=icon]:hidden">
        <div className="flex h-14 items-center px-3">
          <span className="text-2xl font-semibold tracking-tight">klens</span>
        </div>
      </SidebarHeader>

      <SidebarContent>
        <SidebarGroup className="px-3">
          <SidebarGroupLabel>Cluster</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu className="gap-2">
              {SECTIONS.map((section) => (
                <SidebarMenuItem key={section.segment}>
                  <SidebarMenuButton
                    isActive={pathname.startsWith(clusterPath(cluster, section.segment))}
                    tooltip={section.label}
                    className={ACTIVE_MARKER}
                    render={<Link to={clusterPath(cluster, section.segment)} />}
                  >
                    <section.icon />
                    <span>{section.label}</span>
                  </SidebarMenuButton>
                  {counts[section.segment] === undefined ? null : (
                    <SidebarMenuBadge className="numeric text-muted-foreground">
                      {formatCount(counts[section.segment] ?? 0)}
                    </SidebarMenuBadge>
                  )}
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>

      <SidebarFooter className="px-3 group-data-[collapsible=icon]:hidden">
        <div className="flex items-center justify-between gap-2">
          <Tooltip>
            <TooltipTrigger
              render={
                <a
                  href={RELEASE_URL}
                  target="_blank"
                  rel="noreferrer"
                  className="flex min-w-0 items-center text-xs text-muted-foreground transition-colors hover:text-foreground"
                />
              }
            >
              <span className="numeric truncate font-mono">v{VERSION}</span>
            </TooltipTrigger>
            <TooltipContent>GitHub release</TooltipContent>
          </Tooltip>

          <Tooltip>
            <TooltipTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon-xs"
                  aria-label="Open repository"
                  className="text-muted-foreground"
                  render={<a href={REPO_URL} target="_blank" rel="noreferrer" />}
                />
              }
            >
              <GithubIcon className="size-3.5" />
            </TooltipTrigger>
            <TooltipContent>Repository</TooltipContent>
          </Tooltip>
        </div>
      </SidebarFooter>

      <SidebarRail />
    </Sidebar>
  );
}
