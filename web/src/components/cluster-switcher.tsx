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
import { catalogTone, useClusterName } from "@/lib/clusters";
import { useCatalogHealth, useClusters } from "@/lib/api/catalog";
import { clusterSectionTo, useActiveSection } from "@/lib/sections";

export function ClusterSwitcher() {
  const active = useClusterName();
  const { data: clusters = [] } = useClusters();
  const { data: health } = useCatalogHealth(active);
  const navigate = useNavigate();
  const section = useActiveSection();

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
        <StatusDot tone={catalogTone(health)} />
        <span className="truncate">{active}</span>
        <ChevronDownIcon className="opacity-60" />
      </DropdownMenuTrigger>

      <DropdownMenuContent align="start" className="w-64">
        <DropdownMenuRadioGroup value={active} onValueChange={switchTo}>
          <DropdownMenuLabel className="text-xs text-muted-foreground">Clusters</DropdownMenuLabel>
          {clusters.map((name) => (
            <DropdownMenuRadioItem key={name} value={name}>
              <StatusDot tone={name === active ? catalogTone(health) : "idle"} />
              <span className="flex-1 truncate">{name}</span>
            </DropdownMenuRadioItem>
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
