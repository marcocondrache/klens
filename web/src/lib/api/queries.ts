import { useEffect } from "react";
import { useInfiniteQuery, useQuery, useQueryClient } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";
import { clusterPath } from "@/lib/clusters";

import {
  brokerConfigsQuery,
  brokerQuery,
  brokersQuery,
  catalogHealthQuery,
  catalogUpdatedSubscription,
  clusterQuery,
  clustersQuery,
  consumerGroupLagSubscription,
  consumerGroupQuery,
  consumerGroupsQuery,
  groupsCatalogQuery,
  groupLagHistoryQuery,
  recordsQuery,
  schemaSubjectsQuery,
  searchQuery,
  topicConfigsQuery,
  topicQuery,
  topicRatesSubscription,
  topicsQuery,
  topicThroughputQuery,
} from "./documents";
import { subscribe } from "./subscribe";
import type {
  ConsumerGroup,
  GroupOffset,
  RecordQuery,
  SearchResult,
  ThroughputPoint,
  GroupList,
  Topic,
  TopicList,
  TopicRate,
} from "./types";

export type RecordsFilter = Omit<RecordQuery, "cursor">;

export const keys = {
  clusters: () => ["clusters"] as const,
  cluster: (cluster: string) => ["cluster", cluster] as const,
  throughput: (cluster: string) => ["cluster", cluster, "throughput"] as const,
  brokers: (cluster: string) => ["cluster", cluster, "brokers"] as const,
  broker: (cluster: string, id: number) => ["cluster", cluster, "brokers", id] as const,
  brokerConfigs: (cluster: string, id: number) =>
    ["cluster", cluster, "brokers", id, "configs"] as const,
  topics: (cluster: string) => ["cluster", cluster, "topics"] as const,
  topic: (cluster: string, topic: string) => ["cluster", cluster, "topics", topic] as const,
  topicConfigs: (cluster: string, topic: string) =>
    ["cluster", cluster, "topics", topic, "configs"] as const,
  topicThroughput: (cluster: string, topic: string) =>
    ["cluster", cluster, "topics", topic, "throughput"] as const,
  records: (query: RecordsFilter) =>
    ["cluster", query.cluster, "topics", query.topic, "records", query] as const,
  groups: (cluster: string) => ["cluster", cluster, "groups"] as const,
  topicGroups: (cluster: string, topic: string) =>
    ["cluster", cluster, "topics", topic, "groups"] as const,
  group: (cluster: string, group: string) => ["cluster", cluster, "groups", group] as const,
  groupLagHistory: (cluster: string, group: string) =>
    ["cluster", cluster, "groups", group, "lag"] as const,
  subjects: (cluster: string) => ["cluster", cluster, "subjects"] as const,
  catalogHealth: (cluster: string) => ["cluster", cluster, "catalogHealth"] as const,
  search: (cluster: string, term: string) => ["cluster", cluster, "search", term] as const,
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

export function useBrokerConfigs(cluster: string, id: number) {
  return useQuery({
    queryKey: keys.brokerConfigs(cluster, id),
    queryFn: async () => {
      const { brokerConfigs } = await execute(brokerConfigsQuery, { cluster, id });
      return brokerConfigs;
    },
    enabled: Number.isFinite(id),
  });
}

export type TopicsCache = {
  topics: TopicList[];
  updatedAt: string;
};

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
      return required(data, `unknown topic '${topic}' in cluster '${cluster}'`);
    },
  });
}

export function useTopicConfigs(cluster: string, topic: string, enabled = true) {
  return useQuery({
    queryKey: keys.topicConfigs(cluster, topic),
    queryFn: async () => {
      const { topicConfigs } = await execute(topicConfigsQuery, { cluster, name: topic });
      return topicConfigs;
    },
    enabled,
  });
}

export function useTopicThroughput(cluster: string, topic: string) {
  return useQuery({
    queryKey: keys.topicThroughput(cluster, topic),
    queryFn: async () => {
      const { topicThroughput } = await execute(topicThroughputQuery, { cluster, topic });
      return topicThroughput;
    },
  });
}

export function useRecords(query: RecordsFilter) {
  return useInfiniteQuery({
    queryKey: keys.records(query),
    queryFn: async ({ pageParam }) => {
      const { records } = await execute(recordsQuery, {
        query: { ...query, cursor: pageParam },
      });
      return records;
    },
    initialPageParam: null as string | null,
    getNextPageParam: (lastPage) => lastPage.nextCursor,
  });
}

export type GroupsCache = {
  groups: GroupList[];
  updatedAt: string;
};

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
      return required(consumerGroup, `unknown consumer group '${group}' in cluster '${cluster}'`);
    },
  });
}

export function useGroupLagHistory(cluster: string, group: string) {
  return useQuery({
    queryKey: keys.groupLagHistory(cluster, group),
    queryFn: async () => {
      const { groupLagHistory } = await execute(groupLagHistoryQuery, {
        cluster,
        id: group,
      });
      return groupLagHistory;
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
      return search.map((result) => ({
        ...result,
        href: searchHref(cluster, result),
      }));
    },
    enabled: term.trim().length > 0,
  });
}

const RATE_HISTORY = 60;

export function useCatalogUpdated(cluster: string) {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!cluster) {
      return;
    }

    return subscribe(catalogUpdatedSubscription, { cluster }, () => {
      void queryClient.invalidateQueries({ queryKey: keys.clusters() });
      void queryClient.invalidateQueries({ queryKey: keys.cluster(cluster), exact: true });
      void queryClient.invalidateQueries({ queryKey: keys.topics(cluster), exact: true });
      void queryClient.invalidateQueries({ queryKey: keys.groups(cluster), exact: true });
      void queryClient.invalidateQueries({ queryKey: keys.brokers(cluster), exact: true });
      void queryClient.invalidateQueries({ queryKey: keys.catalogHealth(cluster) });
      void queryClient.invalidateQueries({
        predicate: (query) => {
          const key = query.queryKey;
          return (
            key[0] === "cluster" &&
            key[1] === cluster &&
            (key[2] === "topics" || key[2] === "groups" || key[2] === "brokers") &&
            key.length === 4
          );
        },
      });
    });
  }, [cluster, queryClient]);
}

export function useTopicRates(cluster: string) {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!cluster) {
      return;
    }

    return subscribe(topicRatesSubscription, { cluster }, (data) => {
      const rates = new Map(data.topicRates.map((rate) => [rate.name, rate]));
      const timestamp = new Date().toISOString();

      queryClient.setQueryData(keys.topics(cluster), (current: TopicsCache | undefined) =>
        current
          ? {
              ...current,
              topics: current.topics.map((topic) => withRate(topic, rates.get(topic.name))),
            }
          : current,
      );

      for (const rate of data.topicRates) {
        queryClient.setQueryData(keys.topic(cluster, rate.name), (topic: Topic | undefined) =>
          topic ? withRate(topic, rate) : topic,
        );
        queryClient.setQueryData(
          keys.topicThroughput(cluster, rate.name),
          (points: ThroughputPoint[] | undefined) =>
            appendThroughput(points, timestamp, rate.messagesPerSec),
        );
      }

      queryClient.setQueryData(keys.throughput(cluster), (points: ThroughputPoint[] | undefined) =>
        appendThroughput(
          points,
          timestamp,
          data.topicRates.reduce((total, rate) => total + rate.messagesPerSec, 0),
        ),
      );
    });
  }, [cluster, queryClient]);
}

export function useConsumerGroupLag(cluster: string, group: string) {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!cluster || !group) {
      return;
    }

    return subscribe(consumerGroupLagSubscription, { cluster, id: group }, (data) => {
      const lag = data.consumerGroupLag;
      const timestamp = new Date().toISOString();
      const topics =
        queryClient.getQueryData<ConsumerGroup>(keys.group(cluster, group))?.topics ?? [];

      queryClient.setQueryData(keys.group(cluster, group), (existing: ConsumerGroup | undefined) =>
        existing ? withLag(existing, lag) : existing,
      );

      queryClient.setQueryData(keys.groups(cluster), (current: GroupsCache | undefined) =>
        current
          ? {
              ...current,
              groups: current.groups.map((entry) =>
                entry.id === lag.id ? { ...entry, lag: lag.lag } : entry,
              ),
            }
          : current,
      );

      queryClient.setQueryData(
        keys.groupLagHistory(cluster, group),
        (points: ThroughputPoint[] | undefined) => appendThroughput(points, timestamp, lag.lag),
      );

      for (const topic of topics) {
        queryClient.setQueryData(
          keys.topicGroups(cluster, topic),
          (groups: ConsumerGroup[] | undefined) =>
            groups?.map((entry) => (entry.id === lag.id ? withLag(entry, lag) : entry)),
        );
      }
    });
  }, [cluster, group, queryClient]);
}

function withLag(
  group: ConsumerGroup,
  lag: { id: string; lag: number; offsets: GroupOffset[] },
): ConsumerGroup {
  return {
    ...group,
    lag: lag.lag,
    offsets: lag.offsets,
  };
}

function withRate<T extends { messagesPerSec: number; bytesInPerSec: number }>(
  topic: T,
  rate: TopicRate | undefined,
): T {
  if (!rate) {
    return topic;
  }

  return {
    ...topic,
    messagesPerSec: rate.messagesPerSec,
    bytesInPerSec: rate.bytesInPerSec,
  };
}

function appendThroughput(
  points: ThroughputPoint[] | undefined,
  timestamp: string,
  messages: number,
): ThroughputPoint[] {
  return [...(points ?? []), { timestamp, bytesIn: 0, bytesOut: 0, messages }].slice(-RATE_HISTORY);
}
