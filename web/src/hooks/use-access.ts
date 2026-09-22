import { useQuery } from "@tanstack/react-query";

import { get } from "@/lib/api/client";
import { keys } from "@/lib/api/keys";
import type { Identity, PrivilegeName } from "@/lib/api/types";

export function useWhoami() {
  return useQuery({
    queryKey: keys.whoami(),
    queryFn: () => get<Identity>("/whoami"),
    staleTime: 60_000,
  });
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
    can: (cluster, privilege) => grant(data, cluster)?.privileges.includes(privilege) ?? !data,
    canSeeCluster: (cluster) => (data ? grant(data, cluster) != null : true),
  };
}

function grant(identity: Identity | undefined, cluster: string) {
  return identity?.clusters.find((entry) => entry.cluster === cluster);
}
