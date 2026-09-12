import { useQuery } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";
import { clusterPath } from "@/lib/clusters";

import {
  brokerQuery,
  brokersQuery,
  catalogHealthQuery,
  clusterQuery,
  clustersQuery,
  consumerGroupQuery,
  consumerGroupsQuery,
  groupsCatalogQuery,
  schemaSubjectsQuery,
  searchQuery,
  topicQuery,
  topicsQuery,
} from "./documents";
import { keys } from "./keys";
import type { GroupList, SearchResult, TopicList } from "./types";

export type TopicsCache = {
  topics: TopicList[];
  updatedAt: string;
};

export type GroupsCache = {
  groups: GroupList[];
  updatedAt: string;
};

function required<T>(value: T | null | undefined, message: string): T {
  if (value == null) {
    throw new Error(message);
  }

  return value;
}

function searchHref(cluster: string, result: Omit<SearchResult, "href">): string {
  switch (result.kind) {
    case "TOPIC":
      return clusterPath(cluster, "topics", result.id);
    case "GROUP":
      return clusterPath(cluster, "groups", result.id);
    case "NODE":
      return clusterPath(cluster, "nodes", result.id);
    case "SUBJECT":
      return `${clusterPath(cluster, "schemas")}?q=${encodeURIComponent(result.id)}`;
  }
}

export function useClusters() {
  return useQuery({
    queryKey: keys.clusters(),
    queryFn: async () => {
      const { clusters } = await execute(clustersQuery);
      return clusters;
    },
  });
}

export function useCluster(cluster: string) {
  return useQuery({
    queryKey: keys.cluster(cluster),
    queryFn: async () => {
      const { cluster: data } = await execute(clusterQuery, { name: cluster });
      return required(data, `unknown cluster '${cluster}'`);
    },
  });
}

export function useCatalogHealth(cluster: string) {
  return useQuery({
    queryKey: keys.catalogHealth(cluster),
    queryFn: async () => {
      const { catalogHealth } = await execute(catalogHealthQuery, { cluster });
      return catalogHealth;
    },
  });
}

export function useBrokers(cluster: string) {
  return useQuery({
    queryKey: keys.brokers(cluster),
    queryFn: async () => {
      const { brokers } = await execute(brokersQuery, { cluster });
      return brokers;
    },
  });
}

export function useBroker(cluster: string, id: number) {
  return useQuery({
    queryKey: keys.broker(cluster, id),
    queryFn: async () => {
      const { broker } = await execute(brokerQuery, { cluster, id });
      return required(broker, `unknown broker '${id}' in cluster '${cluster}'`);
    },
    enabled: Number.isFinite(id),
  });
}

export function useTopics(cluster: string) {
  return useQuery({
    queryKey: keys.topics(cluster),
    queryFn: async () => {
      const { clusterCatalog } = await execute(topicsQuery, { cluster });
      return {
        topics: clusterCatalog.topics,
        updatedAt: clusterCatalog.updatedAt,
      } satisfies TopicsCache;
    },
  });
}

export function useTopic(cluster: string, topic: string) {
  return useQuery({
    queryKey: keys.topic(cluster, topic),
    queryFn: async () => {
      const { topic: data } = await execute(topicQuery, { cluster, name: topic });
      return data ?? null;
    },
  });
}

export function useConsumerGroups(cluster: string) {
  return useQuery({
    queryKey: keys.groups(cluster),
    queryFn: async () => {
      const { clusterCatalog } = await execute(groupsCatalogQuery, { cluster });
      return {
        groups: clusterCatalog.consumerGroups,
        updatedAt: clusterCatalog.updatedAt,
      } satisfies GroupsCache;
    },
  });
}

export function useTopicConsumerGroups(cluster: string, topic: string, enabled = true) {
  return useQuery({
    queryKey: keys.topicGroups(cluster, topic),
    queryFn: async () => {
      const { consumerGroups } = await execute(consumerGroupsQuery, {
        cluster,
        topic,
      });
      return consumerGroups;
    },
    enabled,
  });
}

export function useConsumerGroup(cluster: string, group: string) {
  return useQuery({
    queryKey: keys.group(cluster, group),
    queryFn: async () => {
      const { consumerGroup } = await execute(consumerGroupQuery, { cluster, id: group });
      return consumerGroup ?? null;
    },
  });
}

export function useSchemaSubjects(cluster: string) {
  return useQuery({
    queryKey: keys.subjects(cluster),
    queryFn: async () => {
      const { schemaSubjects } = await execute(schemaSubjectsQuery, { cluster });
      return schemaSubjects;
    },
  });
}

export function useSearch(cluster: string, term: string) {
  return useQuery({
    queryKey: keys.search(cluster, term),
    queryFn: async () => {
      const { search } = await execute(searchQuery, { cluster, term });
      return {
        hits: search.hits.map((result) => ({
          ...result,
          href: searchHref(cluster, result),
        })),
        schemaRegistryError: search.schemaRegistryError,
      };
    },
    enabled: term.trim().length > 0,
  });
}
