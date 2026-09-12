import { useParams } from "@tanstack/react-router";

export function useClusterName() {
  const { cluster } = useParams({ from: "/cluster/$cluster" });
  return cluster;
}

/** String href for catalog search hits. Encodes each tail segment. */
export function clusterPath(cluster: string, ...segments: string[]) {
  const tail = segments.filter(Boolean).map(encodeURIComponent).join("/");
  return tail ? `/cluster/${cluster}/${tail}` : `/cluster/${cluster}`;
}
