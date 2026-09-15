import { useParams } from "@tanstack/react-router";

export function useClusterName() {
  const { cluster } = useParams({ from: "/cluster/$cluster" });
  return cluster;
}

export function catalogTone(health?: {
  updatedAt: string | null;
  lastError: string | null;
}): "ok" | "warn" | "error" | "idle" {
  if (health?.lastError && health.updatedAt == null) return "error";
  if (health?.lastError) return "warn";
  if (health?.updatedAt) return "ok";
  return "idle";
}

/** String href for catalog search hits. Encodes each tail segment. */
export function clusterPath(cluster: string, ...segments: string[]) {
  const tail = segments.filter(Boolean).map(encodeURIComponent).join("/");
  return tail ? `/cluster/${cluster}/${tail}` : `/cluster/${cluster}`;
}
