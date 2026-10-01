import { keepPreviousData, useQuery, type Query } from "@tanstack/react-query";

import type {
  AclListing,
  BrokerRow,
  ClusterHealth,
  GroupDetail,
  GroupRow,
  QuotaListing,
  SubjectRowsResult,
  TopicDetail,
  TopicGroupRow,
  TopicRow,
  Transactions,
} from "@/api/types.gen";
import { clusterPath } from "@/lib/clusters";
import { isLogDirsPending } from "@/lib/storage";

import { clusterPathname, get, getOrNull, resourceId } from "./client";
import { keys } from "./keys";
import type { SearchHit } from "./types";

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

function settled(cluster: ClusterHealth) {
  return cluster.ready && !isLogDirsPending(cluster.logDirs);
}

const clustersOptions = {
  queryKey: keys.clusters(),
  queryFn: () => get<ClusterHealth[]>("/clusters"),
  refetchInterval: (query: Query<ClusterHealth[]>) =>
    query.state.data?.every(settled) === false ? 2000 : false,
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
    queryFn: () => get<TopicRow[]>(clusterPathname(cluster, "topics")),
  });
}

export function useTopic(cluster: string, topic: string) {
  return useQuery({
    queryKey: keys.topic(cluster, topic),
    queryFn: () =>
      getOrNull<TopicDetail>(clusterPathname(cluster, "topics", encodeURIComponent(topic))),
  });
}

export function useTopicGroups(cluster: string, topic: string, enabled = true) {
  return useQuery({
    queryKey: keys.topicGroups(cluster, topic),
    queryFn: () =>
      get<TopicGroupRow[]>(clusterPathname(cluster, "topics", encodeURIComponent(topic), "groups")),
    enabled,
  });
}

export function useGroupRows(cluster: string) {
  return useQuery({
    queryKey: keys.groupRows(cluster),
    queryFn: () => get<GroupRow[]>(clusterPathname(cluster, "groups")),
  });
}

export function useGroup(cluster: string, group: string) {
  return useQuery({
    queryKey: keys.group(cluster, group),
    queryFn: () => getOrNull<GroupDetail>(clusterPathname(cluster, "groups", resourceId(group))),
  });
}

export function useBrokerRows(cluster: string) {
  return useQuery({
    queryKey: keys.brokerRows(cluster),
    queryFn: () => get<BrokerRow[]>(clusterPathname(cluster, "brokers")),
  });
}

export function useBroker(cluster: string, id: number) {
  const { data, ...rest } = useBrokerRows(cluster);
  return {
    ...rest,
    data: data?.find((broker) => broker.id === id) ?? null,
  };
}

export function useTransactions(cluster: string) {
  return useQuery({
    queryKey: keys.transactions(cluster),
    queryFn: () => get<Transactions>(clusterPathname(cluster, "transactions")),
  });
}

export function useSubjectRows(cluster: string) {
  return useQuery({
    queryKey: keys.subjectRows(cluster),
    queryFn: () => get<SubjectRowsResult>(clusterPathname(cluster, "subjects")),
  });
}

export function useAcls(cluster: string, enabled = true) {
  return useQuery({
    queryKey: keys.acls(cluster),
    queryFn: () => get<AclListing>(clusterPathname(cluster, "acls")),
    enabled,
    refetchInterval: (query: Query<AclListing>) => {
      const data = query.state.data;
      return data?.status === "PENDING" && data.sourceHealth.lastError == null ? 2000 : false;
    },
  });
}

export function useQuotas(cluster: string, enabled = true) {
  return useQuery({
    queryKey: keys.quotas(cluster),
    queryFn: () => get<QuotaListing>(clusterPathname(cluster, "quotas")),
    enabled,
    refetchInterval: (query: Query<QuotaListing>) => {
      const data = query.state.data;
      return data?.status === "PENDING" && data.sourceHealth.lastError == null ? 2000 : false;
    },
  });
}

export function useSearch(cluster: string, term: string) {
  return useQuery({
    queryKey: keys.search(cluster, term),
    queryFn: async () => {
      const hits = await get<Omit<SearchHit, "href">[]>(clusterPathname(cluster, "search"), {
        q: term,
      });
      return hits.map((hit) => ({
        ...hit,
        href: searchHref(cluster, hit),
      }));
    },
    enabled: term.trim().length > 0,
    placeholderData: keepPreviousData,
  });
}
