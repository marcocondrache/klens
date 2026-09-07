import { useParams } from "react-router"

export const DEFAULT_CLUSTER = "local"

export function useClusterName() {
  const { cluster } = useParams<{ cluster: string }>()
  return cluster ?? DEFAULT_CLUSTER
}

export function clusterPath(cluster: string, ...segments: string[]) {
  const tail = segments.filter(Boolean).map(encodeURIComponent).join("/")
  return tail ? `/cluster/${cluster}/${tail}` : `/cluster/${cluster}`
}
