import { ChevronsUpDownIcon } from "lucide-react";
import { useNavigate } from "@tanstack/react-router";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "@/components/ui/sidebar";
import { StatusDot } from "@/components/status";
import { useClusters } from "@/lib/api/catalog";
import { clusterTone, useClusterName, type Tone } from "@/lib/clusters";
import { clusterSectionTo, useActiveSection } from "@/lib/sections";

const TONE_LABEL: Record<Tone, string> = {
  ok: "Healthy",
  warn: "Degraded",
  error: "Unreachable",
  idle: "Connecting",
};

export function ClusterSwitcher() {
  const { isMobile, setOpenMobile } = useSidebar();
  const active = useClusterName();
  const { data: clusters = [] } = useClusters();
  const navigate = useNavigate();
  const section = useActiveSection();
  const tone = clusterTone(clusters.find((entry) => entry.cluster === active));

  function switchTo(name: string) {
    setOpenMobile(false);
    void navigate({
      to: section ? clusterSectionTo(section.segment) : "/cluster/$cluster",
      params: { cluster: name },
    });
  }

  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger
            render={
              <SidebarMenuButton
                size="lg"
                className="data-open:bg-sidebar-accent data-open:text-sidebar-accent-foreground"
              />
            }
          >
            <div className="relative flex aspect-square size-8 items-center justify-center rounded-lg border bg-background">
              <img src="/favicon.svg" alt="" className="size-5" />
              <span className="absolute right-0.5 bottom-0.5 flex">
                <StatusDot tone={tone} />
              </span>
            </div>
            <div className="grid flex-1 text-left text-sm leading-tight">
              <span className="truncate font-medium">{active}</span>
              <span className="truncate text-xs text-muted-foreground">{TONE_LABEL[tone]}</span>
            </div>
            <ChevronsUpDownIcon className="ml-auto" />
          </DropdownMenuTrigger>

          <DropdownMenuContent
            className="min-w-56"
            align="start"
            side={isMobile ? "bottom" : "right"}
            sideOffset={4}
          >
            <DropdownMenuRadioGroup value={active} onValueChange={switchTo}>
              <DropdownMenuLabel className="text-xs text-muted-foreground">
                Clusters
              </DropdownMenuLabel>
              {clusters.map((entry) => (
                <DropdownMenuRadioItem key={entry.cluster} value={entry.cluster}>
                  <StatusDot tone={clusterTone(entry)} />
                  <span className="flex-1 truncate">{entry.cluster}</span>
                </DropdownMenuRadioItem>
              ))}
            </DropdownMenuRadioGroup>
          </DropdownMenuContent>
        </DropdownMenu>
      </SidebarMenuItem>
    </SidebarMenu>
  );
}
