import { useQuery } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";
import { whoamiQuery } from "@/lib/api/documents";
import { keys } from "@/lib/api/keys";
import type { Identity, PrivilegeName, Role } from "@/lib/api/types";

export function useWhoami() {
  return useQuery({
    queryKey: keys.whoami(),
    queryFn: async () => {
      const { whoami } = await execute(whoamiQuery);
      return whoami;
    },
    staleTime: 60_000,
  });
}

export type Access = {
  ready: boolean;
  can: (cluster: string, privilege: PrivilegeName) => boolean;
  canSeeCluster: (cluster: string) => boolean;
  roleFor: (cluster: string) => Role | null;
};

export function useAccess(): Access {
  const { data } = useWhoami();

  return {
    ready: data != null,
    can: (cluster, privilege) => grant(data, cluster)?.privileges.includes(privilege) ?? !data,
    canSeeCluster: (cluster) => (data ? grant(data, cluster) != null : true),
    roleFor: (cluster) => grant(data, cluster)?.role ?? null,
  };
}

function grant(identity: Identity | undefined, cluster: string) {
  return identity?.clusters.find((entry) => entry.cluster === cluster);
}
