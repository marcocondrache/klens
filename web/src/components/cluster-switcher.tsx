import { ChevronsUpDownIcon } from "lucide-react";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { StatusDot } from "@/components/status";
import { cn } from "@/lib/utils";
import { useClusters } from "@/lib/api/catalog";
import { clusterStatus, clusterTone, useClusterName } from "@/lib/clusters";
import type { Tone } from "@/lib/tone";
import { useSwitchCluster } from "@/lib/sections";

export function ClusterSwitcher() {
  const active = useClusterName();
  const { data: clusters = [] } = useClusters();
  const switchTo = useSwitchCluster();
  const tone = clusterTone(clusters.find((entry) => entry.cluster === active));

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        render={
          <button className="flex min-w-0 items-center gap-1.5 rounded-md font-medium text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring" />
        }
      >
        <StatusDot tone={tone} label={clusterStatus(tone)} />
        <span className="truncate">{active}</span>
        <ProblemLabel tone={tone} className="hidden sm:inline" />
        <ChevronsUpDownIcon data-icon="inline-end" className="size-3.5 text-muted-foreground" />
      </DropdownMenuTrigger>

      <DropdownMenuContent align="start" className="min-w-56">
        <DropdownMenuRadioGroup value={active} onValueChange={switchTo}>
          <DropdownMenuLabel className="text-xs text-muted-foreground">Clusters</DropdownMenuLabel>
          {clusters.map((entry) => (
            <ClusterItem key={entry.cluster} cluster={entry.cluster} tone={clusterTone(entry)} />
          ))}
        </DropdownMenuRadioGroup>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function ClusterItem({ cluster, tone }: { cluster: string; tone: Tone }) {
  return (
    <DropdownMenuRadioItem value={cluster}>
      <StatusDot tone={tone} label={clusterStatus(tone)} />
      <span className="flex-1 truncate">{cluster}</span>
      <ProblemLabel tone={tone} />
    </DropdownMenuRadioItem>
  );
}

/** Spells out a degraded or unreachable cluster, so the dot's color is not the only cue. */
function ProblemLabel({ tone, className }: { tone: Tone; className?: string }) {
  if (tone !== "warn" && tone !== "error") {
    return null;
  }

  return (
    <span aria-hidden className={cn("text-xs font-normal text-muted-foreground", className)}>
      {clusterStatus(tone)}
    </span>
  );
}
