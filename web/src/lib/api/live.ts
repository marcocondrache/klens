import { skipToken, useInfiniteQuery, useQuery, type QueryKey } from "@tanstack/react-query";

import type { ConfigEntry, RecordLookup, RecordPage, SubjectDetail } from "@/api/types.gen";

import { apiUrl, clusterPathname, get, resourceId } from "./client";
import { keys, type RecordAddress, type RecordsFilter } from "./keys";

export type { RecordAddress, RecordsFilter };

export function useBrokerConfigs(cluster: string, id: number, enabled = true) {
  return useQuery({
    queryKey: keys.brokerConfigs(cluster, id),
    queryFn: () => get<ConfigEntry[]>(clusterPathname(cluster, "brokers", String(id), "configs")),
    enabled: enabled && Number.isFinite(id),
  });
}

export function useTopicConfigs(cluster: string, topic: string, enabled = true) {
  return useQuery({
    queryKey: keys.topicConfigs(cluster, topic),
    queryFn: () =>
      get<ConfigEntry[]>(clusterPathname(cluster, "topics", encodeURIComponent(topic), "configs")),
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
    queryFn: () =>
      get<SubjectDetail>(clusterPathname(cluster, "subjects", resourceId(name ?? "")), {
        version,
      }),
    enabled: enabled && name != null,
  });
}

const RECORD_BATCH_SIZE = 50;

function sameTopic(previous: QueryKey | undefined, next: QueryKey) {
  return previous != null && previous.slice(0, 4).every((part, index) => part === next[index]);
}

function recordsPath(cluster: string, topic: string, ...rest: string[]) {
  return clusterPathname(cluster, "topics", encodeURIComponent(topic), "records", ...rest);
}

function recordParams(query: RecordsFilter) {
  return {
    partition: query.partitions,
    order: query.order,
    from: query.from,
    to: query.to,
    contains: query.filter?.contains,
    schemaId: query.schemaId,
  };
}

export function recordsExportUrl(cluster: string, query: RecordsFilter) {
  return apiUrl(recordsPath(cluster, query.topic, "export"), recordParams(query));
}

export function useRecords(cluster: string, query: RecordsFilter) {
  const scans = query.partitions?.length !== 0;

  return useInfiniteQuery({
    queryKey: keys.records(cluster, query),
    queryFn: ({ pageParam }) =>
      get<RecordPage>(recordsPath(cluster, query.topic), {
        ...recordParams(query),
        limit: RECORD_BATCH_SIZE,
        cursor: pageParam,
      }),
    initialPageParam: null as string | null,
    getNextPageParam: (page) => page.nextCursor,
    placeholderData: (previous, previousQuery) =>
      scans && sameTopic(previousQuery?.queryKey, keys.records(cluster, query))
        ? previous
        : undefined,
    enabled: scans,
  });
}

export function useRecord(
  cluster: string,
  topic: string,
  address: RecordAddress | null,
  schemaId: number | null,
  enabled = true,
) {
  return useQuery({
    queryKey: keys.record(cluster, topic, address, schemaId),
    queryFn:
      address && enabled
        ? () =>
            get<RecordLookup>(
              recordsPath(cluster, topic, String(address.partition), String(address.offset)),
              { schemaId },
            )
        : skipToken,
    staleTime: Infinity,
  });
}
