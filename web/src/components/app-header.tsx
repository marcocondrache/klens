import { Fragment } from "react";
import { RefreshCwIcon, SearchIcon } from "lucide-react";
import { Link, useLocation } from "react-router";
import { useIsFetching, useQueryClient } from "@tanstack/react-query";
import { cn } from "@/lib/utils";

import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { Button } from "@/components/ui/button";
import { Kbd } from "@/components/ui/kbd";
import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { ClusterSwitcher } from "@/components/cluster-switcher";
import { ModeToggle } from "@/components/mode-toggle";
import { UserMenu } from "@/components/user-menu";
import { useAuth } from "@/hooks/use-auth";
import { clusterPath, useClusterName } from "@/lib/clusters";
import { formatModK } from "@/lib/keyboard";
import { findSection } from "@/lib/sections";

interface Crumb {
  label: string;
  href?: string;
  icon?: typeof SearchIcon;
  mono?: boolean;
}

export function AppHeader({ onSearch }: { onSearch: () => void }) {
  const cluster = useClusterName();
  const { pathname } = useLocation();
  const queryClient = useQueryClient();
  const fetching = useIsFetching() > 0;
  const { data: auth } = useAuth();

  const [, , , segment, detail] = pathname.split("/");
  const section = findSection(segment);

  const crumbs: Crumb[] = [];

  if (section) {
    crumbs.push({
      label: section.label,
      icon: section.icon,
      href: detail ? clusterPath(cluster, section.segment) : undefined,
    });
  }

  if (detail) {
    crumbs.push({ label: decodeURIComponent(detail), mono: true });
  }

  return (
    <header className="sticky top-0 z-20 flex h-14 shrink-0 items-center gap-2 border-b bg-background/95 px-3 backdrop-blur-md group-has-data-[collapsible=icon]/sidebar-wrapper:px-4">
      <SidebarTrigger className="-ml-1 group-has-data-[collapsible=icon]/sidebar-wrapper:ml-0" />
      <Separator orientation="vertical" className="mx-1 !h-4 my-auto" />

      <ClusterSwitcher />

      {crumbs.length ? <Separator orientation="vertical" className="mx-1 !h-4 my-auto" /> : null}

      <Breadcrumb className="min-w-0">
        <BreadcrumbList className="flex-nowrap">
          {crumbs.map((crumb, index) => {
            const last = index === crumbs.length - 1;

            return (
              <Fragment key={`${crumb.label}-${index}`}>
                <BreadcrumbItem className="min-w-0 gap-1.5">
                  {crumb.icon ? <crumb.icon className="size-3.5 shrink-0" /> : null}
                  {last || !crumb.href ? (
                    <BreadcrumbPage className={cn("truncate", crumb.mono && "font-mono text-sm")}>
                      {crumb.label}
                    </BreadcrumbPage>
                  ) : (
                    <BreadcrumbLink render={<Link to={crumb.href} />} className="truncate">
                      {crumb.label}
                    </BreadcrumbLink>
                  )}
                </BreadcrumbItem>
                {last ? null : <BreadcrumbSeparator />}
              </Fragment>
            );
          })}
        </BreadcrumbList>
      </Breadcrumb>

      <div className="ml-auto flex items-center gap-1.5">
        <Button
          variant="outline"
          size="sm"
          onClick={onSearch}
          className="hidden min-w-56 justify-start gap-2 text-muted-foreground sm:flex"
        >
          <SearchIcon />
          <span>Search</span>
          <Kbd className="ml-auto">{formatModK()}</Kbd>
          <Kbd>/</Kbd>
        </Button>

        <Button
          variant="ghost"
          size="icon-sm"
          onClick={onSearch}
          aria-label="Search"
          className="sm:hidden"
        >
          <SearchIcon />
        </Button>

        <Tooltip>
          <TooltipTrigger
            render={
              <Button
                variant="ghost"
                size="icon-sm"
                aria-label="Refresh"
                onClick={() => queryClient.invalidateQueries()}
              />
            }
          >
            <RefreshCwIcon className={cn(fetching && "animate-spin")} />
          </TooltipTrigger>
          <TooltipContent>Refresh</TooltipContent>
        </Tooltip>

        <ModeToggle />

        {auth?.enabled && auth.user ? <UserMenu user={auth.user} /> : null}
      </div>
    </header>
  );
}
