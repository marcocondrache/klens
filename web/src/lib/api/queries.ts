import { useEffect } from "react";
import { useInfiniteQuery, useQuery, useQueryClient } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";
import { clusterPath } from "@/lib/clusters";

import {
  aclsQuery,
  brokerConfigsQuery,
  brokerQuery,
  brokersQuery,
  clusterQuery,
  clustersQuery,
  clusterThroughputQuery,
  consumerGroupQuery,
  consumerGroupsQuery,
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
import type { RecordQuery, SearchResult, ThroughputPoint, Topic, TopicRate } from "./types";

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
  subjects: (cluster: string) => ["cluster", cluster, "subjects"] as const,
  acls: (cluster: string) => ["cluster", cluster, "acls"] as const,
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

export function useClusterThroughput(cluster: string) {
  return useQuery({
    queryKey: keys.throughput(cluster),
    queryFn: async () => {
      const { clusterThroughput } = await execute(clusterThroughputQuery, { cluster });
      return clusterThroughput;
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

export function useTopics(cluster: string) {
  return useQuery({
    queryKey: keys.topics(cluster),
    queryFn: async () => {
      const { topics } = await execute(topicsQuery, { cluster });
      return topics;
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

export function useConsumerGroups(cluster: string, topic?: string, enabled = true) {
  return useQuery({
    queryKey: topic ? keys.topicGroups(cluster, topic) : keys.groups(cluster),
    queryFn: async () => {
      const { consumerGroups } = await execute(consumerGroupsQuery, {
        cluster,
        topic: topic ?? null,
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

export function useSchemaSubjects(cluster: string) {
  return useQuery({
    queryKey: keys.subjects(cluster),
    queryFn: async () => {
      const { schemaSubjects } = await execute(schemaSubjectsQuery, { cluster });
      return schemaSubjects;
    },
  });
}

export function useAcls(cluster: string) {
  return useQuery({
    queryKey: keys.acls(cluster),
    queryFn: async () => {
      const { acls } = await execute(aclsQuery, { cluster });
      return acls;
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

export function useTopicRates(cluster: string) {
  const queryClient = useQueryClient();

  useEffect(() => {
    if (!cluster) {
      return;
    }

    return subscribe(topicRatesSubscription, { cluster }, (data) => {
      const rates = new Map(data.topicRates.map((rate) => [rate.name, rate]));
      const timestamp = new Date().toISOString();

      queryClient.setQueryData(keys.topics(cluster), (topics: Topic[] | undefined) =>
        topics?.map((topic) => withRate(topic, rates.get(topic.name))),
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

function withRate(topic: Topic, rate: TopicRate | undefined): Topic {
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
