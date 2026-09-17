import { ChevronDownIcon } from "lucide-react";
import { useNavigate } from "@tanstack/react-router";

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
import { clusterTone, useClusterName } from "@/lib/clusters";
import { useClusters } from "@/lib/api/catalog";
import { clusterSectionTo, useActiveSection } from "@/lib/sections";

export function ClusterSwitcher() {
  const active = useClusterName();
  const { data: clusters = [] } = useClusters();
  const navigate = useNavigate();
  const section = useActiveSection();
  const health = clusters.find((entry) => entry.cluster === active) ?? null;

  function switchTo(name: string) {
    void navigate({
      to: section ? clusterSectionTo(section.segment) : "/cluster/$cluster",
      params: { cluster: name },
    });
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={<Button variant="outline" size="sm" className="max-w-64 font-medium" />}
      >
        <StatusDot tone={clusterTone(health)} />
        <span className="truncate">{active}</span>
        <ChevronDownIcon className="opacity-60" />
      </DropdownMenuTrigger>

      <DropdownMenuContent align="start" className="w-64">
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
