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
import { StatusDot } from "@/components/status";
import { useClusters } from "@/lib/api/catalog";
import { clusterTone, useClusterName } from "@/lib/clusters";
import { clusterSectionTo, useActiveSection } from "@/lib/sections";

/** Breadcrumb root that shows the active cluster and switches between clusters. */
export function ClusterSwitcher() {
  const active = useClusterName();
  const { data: clusters = [] } = useClusters();
  const navigate = useNavigate();
  const section = useActiveSection();
  const tone = clusterTone(clusters.find((entry) => entry.cluster === active));

  function switchTo(name: string) {
    void navigate({
      to: section ? clusterSectionTo(section.segment) : "/cluster/$cluster",
      params: { cluster: name },
    });
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={
          <button className="flex min-w-0 items-center gap-1.5 rounded-md font-medium text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring/50" />
        }
      >
        <StatusDot tone={tone} />
        <span className="truncate">{active}</span>
        <ChevronsUpDownIcon data-icon="inline-end" className="size-3.5 text-muted-foreground" />
      </DropdownMenuTrigger>

      <DropdownMenuContent align="start" className="min-w-56">
        <DropdownMenuRadioGroup value={active} onValueChange={switchTo}>
          <DropdownMenuLabel className="text-xs text-muted-foreground">Clusters</DropdownMenuLabel>
          {clusters.map((entry) => (
            <DropdownMenuRadioItem key={entry.cluster} value={entry.cluster}>
              <StatusDot tone={clusterTone(entry)} />
              <span className="flex-1 truncate">{entry.cluster}</span>
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
