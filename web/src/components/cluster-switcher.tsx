import { ChevronsUpDownIcon } from "lucide-react";
import { useLocation, useNavigate } from "react-router";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { StatusDot } from "@/components/status";
import { useSidebar } from "@/components/ui/sidebar";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { useClusterName } from "@/lib/clusters";
import { useClusters } from "@/lib/api/queries";
import { findSection } from "@/lib/sections";
import { cn } from "@/lib/utils";
import type { ClusterStatus } from "@/lib/api/types";

const STATUS_TONE: Record<ClusterStatus, "ok" | "warn" | "error"> = {
  HEALTHY: "ok",
  DEGRADED: "warn",
  OFFLINE: "error",
};

export function ClusterSwitcher({ variant = "header" }: { variant?: "header" | "sidebar" }) {
  const active = useClusterName();
  const { data: clusters = [] } = useClusters();
  const navigate = useNavigate();
  const location = useLocation();
  const { state, isMobile } = useSidebar();

  const current = clusters.find((cluster) => cluster.name === active);
  const collapsed = variant === "sidebar" && state === "collapsed" && !isMobile;
  const label = current?.label ?? active;

  function switchTo(name: string) {
    const [, , , segment] = location.pathname.split("/");
    const section = findSection(segment);
    void navigate(section ? `/cluster/${name}/${section.segment}` : `/cluster/${name}`);
  }

  const trigger =
    variant === "sidebar" ? (
      <button
        type="button"
        className={cn(
          "flex h-10 w-full min-w-0 items-center gap-2 rounded-md px-2.5 text-left text-sm font-medium transition-colors",
          "hover:bg-sidebar-accent focus-visible:bg-sidebar-accent focus-visible:outline-2 focus-visible:outline-ring",
          collapsed && "size-9 justify-center px-0",
        )}
      />
    ) : (
      <Button variant="outline" size="sm" className="max-w-64 font-medium" />
    );

  const switcher = (
    <DropdownMenu>
      <DropdownMenuTrigger render={trigger}>
        <StatusDot tone={current ? STATUS_TONE[current.status] : "idle"} />
        {collapsed ? null : (
          <>
            <span className="min-w-0 flex-1 truncate">{label}</span>
            {variant === "sidebar" ? (
              <span className="inline-flex h-5 shrink-0 items-center rounded-full bg-muted px-2 text-[11px] font-medium tracking-[0.2px] text-muted-foreground">
                Cluster
              </span>
            ) : null}
            <ChevronsUpDownIcon
              className={cn("size-4 shrink-0 opacity-60", variant === "sidebar" && "size-3.5")}
            />
          </>
        )}
      </DropdownMenuTrigger>

      <DropdownMenuContent align="start" side={collapsed ? "right" : "bottom"} className="w-64">
        <DropdownMenuRadioGroup value={active} onValueChange={switchTo}>
          <DropdownMenuLabel className="text-xs text-muted-foreground">Clusters</DropdownMenuLabel>
          {clusters.map((cluster) => (
            <DropdownMenuRadioItem key={cluster.name} value={cluster.name}>
              <StatusDot tone={STATUS_TONE[cluster.status]} />
              <span className="flex-1 truncate">{cluster.label}</span>
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );

  if (!collapsed) return switcher;

  return (
    <Tooltip>
      <TooltipTrigger render={<div className="flex w-full justify-center" />}>
        {switcher}
      </TooltipTrigger>
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}
