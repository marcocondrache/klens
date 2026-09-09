import { ChevronDownIcon } from "lucide-react";
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
import { useClusterName } from "@/lib/clusters";
import { useClusters } from "@/lib/api/queries";
import { findSection } from "@/lib/sections";
import type { ClusterStatus } from "@/lib/api/types";

const STATUS_TONE: Record<ClusterStatus, "ok" | "warn" | "error"> = {
  HEALTHY: "ok",
  DEGRADED: "warn",
  OFFLINE: "error",
};

export function ClusterSwitcher() {
  const active = useClusterName();
  const { data: clusters = [] } = useClusters();
  const navigate = useNavigate();
  const location = useLocation();

  const current = clusters.find((cluster) => cluster.name === active);

  function switchTo(name: string) {
    const [, , , segment] = location.pathname.split("/");
    const section = findSection(segment);
    navigate(section ? `/cluster/${name}/${section.segment}` : `/cluster/${name}`);
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={<Button variant="outline" size="sm" className="max-w-64 font-medium" />}
      >
        <StatusDot tone={current ? STATUS_TONE[current.status] : "idle"} />
        <span className="truncate">{current?.label ?? active}</span>
        <ChevronDownIcon className="opacity-60" />
      </DropdownMenuTrigger>

      <DropdownMenuContent align="start" className="w-64">
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
}
