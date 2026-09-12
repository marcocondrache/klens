import { Fragment } from "react";
import { RefreshCwIcon, SearchIcon } from "lucide-react";
import { Link, useParams } from "@tanstack/react-router";
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
import { useClusterName } from "@/lib/clusters";
import { formatModK } from "@/lib/keyboard";
import { clusterSectionTo, useActiveSection } from "@/lib/sections";

interface Crumb {
  label: string;
  section?: ReturnType<typeof clusterSectionTo>;
  icon?: typeof SearchIcon;
  mono?: boolean;
}

export function AppHeader({ onSearch }: { onSearch: () => void }) {
  const cluster = useClusterName();
  const detail = useParams({
    strict: false,
    shouldThrow: false,
    select: (params) => params?.topic ?? params?.group ?? params?.id,
  });
  const queryClient = useQueryClient();
  const fetching = useIsFetching() > 0;
  const { data: auth } = useAuth();
  const section = useActiveSection();

  const crumbs: Crumb[] = [];

  if (section) {
    crumbs.push({
      label: section.label,
      icon: section.icon,
      section: detail ? clusterSectionTo(section.segment) : undefined,
    });
  }

  if (detail) {
    crumbs.push({ label: detail, mono: true });
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
                  {last || !crumb.section ? (
                    <BreadcrumbPage className={cn("truncate", crumb.mono && "font-mono text-sm")}>
                      {crumb.label}
                    </BreadcrumbPage>
                  ) : (
                    <BreadcrumbLink
                      render={<Link to={crumb.section} params={{ cluster }} />}
                      className="truncate"
                    >
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
