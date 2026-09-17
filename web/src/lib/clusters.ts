import { useParams } from "@tanstack/react-router";

import type { ClusterHealth, LaneHealth } from "@/lib/api/types";
import { formatRelative } from "@/lib/format";

export function useClusterName() {
  const { cluster } = useParams({ from: "/cluster/$cluster" });
  return cluster;
}

export type Tone = "ok" | "warn" | "error" | "idle";

export function clusterTone(health: ClusterHealth | null | undefined): Tone {
  if (!health) return "idle";
  if (!health.ready) return health.topology.lastError ? "error" : "idle";
  return lanes(health).every((lane) => lane.healthy) ? "ok" : "warn";
}

/** True until topology commits once, unless that first poll already failed. */
export function isFirstCatalogPending(health: ClusterHealth | null | undefined): boolean {
  return health != null && !health.ready && health.topology.lastError == null;
}

export function lanes(health: ClusterHealth): LaneHealth[] {
  return [health.topology, health.watermarks, health.offsets, health.configs, health.subjects];
}

export function laneCaption(lane: LaneHealth | undefined, now?: number): string | undefined {
  if (!lane) return undefined;

  const parts: string[] = [];
  if (lane.updatedAt) {
    parts.push(`Updated ${formatRelative(lane.updatedAt, now)}`);
  }
  if (lane.lastError) {
    parts.push(lane.lastError);
  }
  return parts.length > 0 ? parts.join(" · ") : undefined;
}

/** String href for catalog search hits. Encodes each tail segment. */
export function clusterPath(cluster: string, ...segments: string[]) {
  const tail = segments.filter(Boolean).map(encodeURIComponent).join("/");
  return tail ? `/cluster/${cluster}/${tail}` : `/cluster/${cluster}`;
}
