import { canAccess, canAccessCluster, type AccessPrivilege } from "@/lib/auth";
import { useAuth } from "@/hooks/use-auth";

export function useAccess() {
  const { data } = useAuth();

  return {
    can: (cluster: string, privilege: AccessPrivilege) => canAccess(data, cluster, privilege),
    canSeeCluster: (cluster: string) => canAccessCluster(data, cluster),
  };
}
