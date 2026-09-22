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
import { useClusterName } from "@/lib/clusters";
import { formatModK } from "@/lib/keyboard";
import { clusterSectionTo, useActiveSection } from "@/lib/sections";

interface Crumb {
  label: string;
  section?: ReturnType<typeof clusterSectionTo>;
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
  const section = useActiveSection();

  const crumbs: Crumb[] = [];

  if (section) {
    crumbs.push({
      label: section.label,
      section: detail ? clusterSectionTo(section.segment) : undefined,
    });
  }

  if (detail) {
    crumbs.push({ label: detail, mono: true });
  }

  return (
    <header className="flex h-16 shrink-0 items-center gap-2 border-b transition-[height] duration-200 ease-out group-has-data-[collapsible=icon]/sidebar-wrapper:h-12">
      <div className="flex w-full min-w-0 items-center gap-2 px-4">
        <SidebarTrigger className="-ml-1" />
        <Separator
          orientation="vertical"
          className="mr-2 data-vertical:h-4 data-vertical:self-auto"
        />

        <Breadcrumb className="min-w-0">
          <BreadcrumbList className="flex-nowrap">
            <BreadcrumbItem className="min-w-0">
              <ClusterSwitcher />
            </BreadcrumbItem>
            {crumbs.length ? <BreadcrumbSeparator /> : null}
            {crumbs.map((crumb, index) => {
              const last = index === crumbs.length - 1;

              return (
                <Fragment key={`${crumb.label}-${index}`}>
                  <BreadcrumbItem className="min-w-0">
                    {last || !crumb.section ? (
                      <BreadcrumbPage
                        className={cn("truncate font-medium", crumb.mono && "font-mono text-sm")}
                      >
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

        <div className="ml-auto flex items-center gap-1">
          <Button
            variant="outline"
            size="sm"
            onClick={onSearch}
            className="hidden min-w-56 justify-start gap-2 text-muted-foreground sm:flex"
          >
            <SearchIcon />
            <span>Search</span>
            <Kbd className="ml-auto -mr-1.5">{formatModK()}</Kbd>
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
        </div>
      </div>
    </header>
  );
}
