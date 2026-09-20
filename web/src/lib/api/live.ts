import { keepPreviousData, useQuery } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";

import {
  aclsQuery,
  brokerConfigsQuery,
  recordsQuery,
  subjectQuery,
  topicConfigsQuery,
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
