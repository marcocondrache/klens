import { useInfiniteQuery, useQuery } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";

import {
  brokerConfigsQuery,
  groupLagHistoryQuery,
  recordsQuery,
  topicConfigsQuery,
  topicThroughputQuery,
} from "./documents";
import { keys, type RecordsFilter } from "./keys";

export type { RecordsFilter };

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
