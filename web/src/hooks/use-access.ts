import { queryOptions, useQuery } from "@tanstack/react-query";

import { get } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { Identity, PrivilegeName } from "@/lib/api/types";

export const whoamiQuery = queryOptions({
  queryKey: keys.whoami(),
  queryFn: () => get<Identity>("/whoami"),
  staleTime: 60_000,
});

export function useWhoami() {
  return useQuery(whoamiQuery);
}

export type Access = {
  ready: boolean;
  can: (cluster: string, privilege: PrivilegeName) => boolean;
  canSeeCluster: (cluster: string) => boolean;
};

export function useAccess(): Access {
  const { data } = useWhoami();

  return {
    ready: data != null,
    can: (cluster, privilege) => hasPrivilege(data, cluster, privilege),
    canSeeCluster: (cluster) => (data ? grant(data, cluster) != null : true),
  };
}

export function hasPrivilege(
  identity: Identity | undefined,
  cluster: string,
  privilege: PrivilegeName,
) {
  return grant(identity, cluster)?.privileges.includes(privilege) ?? !identity;
}

function grant(identity: Identity | undefined, cluster: string) {
  return identity?.clusters.find((entry) => entry.cluster === cluster);
}
