import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";

import {
  catalogUpdatedSubscription,
  consumerGroupLagSubscription,
  topicRatesSubscription,
} from "./documents";
import { keys } from "./keys";
import { subscribe } from "./subscribe";
import type { GroupsCache, TopicsCache } from "./catalog";
import type { ConsumerGroup, GroupOffset, ThroughputPoint, Topic, TopicRate } from "./types";

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
