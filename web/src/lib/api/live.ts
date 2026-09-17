import { keepPreviousData, useQuery } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";

import {
  aclsQuery,
  brokerConfigsQuery,
  groupLagHistoryQuery,
  recordsQuery,
  subjectQuery,
  topicConfigsQuery,
  topicRateHistoryQuery,
} from "./documents";
import { keys, type RecordsFilter } from "./keys";

export type { RecordsFilter };

export function useAcls(cluster: string, enabled = true) {
  return useQuery({
    queryKey: keys.acls(cluster),
    queryFn: async () => {
      const { acls } = await execute(aclsQuery, { cluster });
      return acls;
    },
    enabled,
  });
}

export function useBrokerConfigs(cluster: string, id: number, enabled = true) {
  return useQuery({
    queryKey: keys.brokerConfigs(cluster, id),
    queryFn: async () => {
      const { brokerConfigs } = await execute(brokerConfigsQuery, { cluster, id });
      return brokerConfigs;
    },
    enabled: enabled && Number.isFinite(id),
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

/** Schema bodies are fetched per subject, never shipped with the listing. */
export function useSubject(
  cluster: string,
  name: string | null,
  version: number | null,
  enabled = true,
) {
  return useQuery({
    queryKey: keys.subject(cluster, name ?? "", version),
    queryFn: async () => {
      const { subject } = await execute(subjectQuery, { cluster, name: name ?? "", version });
      return subject;
    },
    enabled: enabled && name != null,
  });
}

/**
 * Seeds a sparkline from the same ring the subscription streams, with the
 * same server timestamps, so `seed ++ stream` is one continuous series.
 */
export function useTopicRateHistory(cluster: string, topic: string) {
  return useQuery({
    queryKey: keys.topicRateHistory(cluster, topic),
    queryFn: async () => {
      const { topicRateHistory } = await execute(topicRateHistoryQuery, { cluster, topic });
      return topicRateHistory;
    },
  });
}

export function useGroupLagHistory(cluster: string, group: string) {
  return useQuery({
    queryKey: keys.groupLagHistory(cluster, group),
    queryFn: async () => {
      const { groupLagHistory } = await execute(groupLagHistoryQuery, { cluster, group });
      return groupLagHistory;
    },
  });
}

/**
 * One page at a time, in either direction: the server hands back the cursor
 * for both edges of what it scanned, so there is nothing to cache client-side
 * to fake a previous page.
 */
export function useRecords(
  cluster: string,
  query: RecordsFilter,
  cursor: string | null,
  enabled = true,
) {
  return useQuery({
    queryKey: keys.records(cluster, query, cursor),
    queryFn: async () => {
      const { records } = await execute(recordsQuery, {
        cluster,
        query: { ...query, cursor },
      });
      return records;
    },
    enabled,
    placeholderData: keepPreviousData,
  });
}
