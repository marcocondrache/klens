import { useQuery, type Query } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";
import { clusterPath } from "@/lib/clusters";

import {
  brokerRowsQuery,
  clustersQuery,
  groupQuery,
  groupRowsQuery,
  searchQuery,
  subjectRowsQuery,
  topicGroupsQuery,
  topicQuery,
  topicRowsQuery,
} from "./documents";
import { keys } from "./keys";
import type { ClusterHealth, SearchHit } from "./types";

function visibleCluster<T>(cluster: T | null): T {
  if (cluster == null) {
    throw new Error("Unknown cluster");
  }
  return cluster;
}

function searchHref(cluster: string, hit: Omit<SearchHit, "href">): string {
  switch (hit.kind) {
    case "TOPIC":
      return clusterPath(cluster, "topics", hit.id);
    case "GROUP":
      return clusterPath(cluster, "groups", hit.id);
    case "NODE":
      return clusterPath(cluster, "nodes", hit.id);
    case "SUBJECT":
      return `${clusterPath(cluster, "schemas")}?q=${encodeURIComponent(hit.id)}`;
  }
}

const clustersOptions = {
  queryKey: keys.clusters(),
  queryFn: async () => {
    const { clusters } = await execute(clustersQuery);
    return clusters.map((entry) => entry.health);
  },
  refetchInterval: (query: Query<ClusterHealth[]>) =>
    query.state.data?.every((cluster) => cluster.ready) === false ? 2000 : false,
};

export function useClusters() {
  return useQuery(clustersOptions);
}

export function useClusterNames() {
  return useQuery({
    ...clustersOptions,
    select: (clusters: ClusterHealth[]) => clusters.map((cluster) => cluster.cluster),
  });
}

export function useClusterHealth(cluster: string) {
  return useQuery({
    ...clustersOptions,
    select: (clusters: ClusterHealth[]) =>
      clusters.find((entry) => entry.cluster === cluster) ?? null,
  });
}

export function useTopicRows(cluster: string) {
  return useQuery({
    queryKey: keys.topicRows(cluster),
    queryFn: async () => {
      const { cluster: node } = await execute(topicRowsQuery, { cluster });
      return visibleCluster(node).topics.rows;
    },
  });
}

export function useTopic(cluster: string, topic: string) {
  return useQuery({
    queryKey: keys.topic(cluster, topic),
    queryFn: async () => {
      const { cluster: node } = await execute(topicQuery, { cluster, name: topic });
      const resolved = visibleCluster(node);
      return {
        detail: resolved.topic,
        row: resolved.topics.rows.find((row) => row.name === topic) ?? null,
      };
    },
  });
}

export function useTopicGroups(cluster: string, topic: string, enabled = true) {
  return useQuery({
    queryKey: keys.topicGroups(cluster, topic),
    queryFn: async () => {
      const { cluster: node } = await execute(topicGroupsQuery, { cluster, topic });
      return visibleCluster(node).topicGroups;
    },
    enabled,
  });
}

export function useGroupRows(cluster: string) {
  return useQuery({
    queryKey: keys.groupRows(cluster),
    queryFn: async () => {
      const { cluster: node } = await execute(groupRowsQuery, { cluster });
      return visibleCluster(node).groups.rows;
    },
  });
}

export function useGroup(cluster: string, group: string) {
  return useQuery({
    queryKey: keys.group(cluster, group),
    queryFn: async () => {
      const { cluster: node } = await execute(groupQuery, { cluster, id: group });
      return visibleCluster(node).group ?? null;
    },
  });
}

export function useBrokerRows(cluster: string) {
  return useQuery({
    queryKey: keys.brokerRows(cluster),
    queryFn: async () => {
      const { cluster: node } = await execute(brokerRowsQuery, { cluster });
      return visibleCluster(node).brokers;
    },
  });
}

export function useBroker(cluster: string, id: number) {
  const { data, ...rest } = useBrokerRows(cluster);
  return {
    ...rest,
    data: data?.find((broker) => broker.id === id) ?? null,
  };
}

export function useSubjectRows(cluster: string) {
  return useQuery({
    queryKey: keys.subjectRows(cluster),
    queryFn: async () => {
      const { cluster: node } = await execute(subjectRowsQuery, { cluster });
      return visibleCluster(node).subjects;
    },
  });
}

export function useSearch(cluster: string, term: string) {
  return useQuery({
    queryKey: keys.search(cluster, term),
    queryFn: async () => {
      const { cluster: node } = await execute(searchQuery, { cluster, term });
      return visibleCluster(node).search.map((hit) => ({
        ...hit,
        href: searchHref(cluster, hit),
      }));
    },
    enabled: term.trim().length > 0,
  });
}
