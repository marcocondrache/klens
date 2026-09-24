import { useInfiniteQuery, useQuery, type QueryKey } from "@tanstack/react-query";

import type { AclListing, ConfigEntry, RecordPage, SubjectDetail } from "@/api/types.gen";

import { get, resourceId } from "./client";
import { keys, type RecordsFilter } from "./keys";

export type { RecordsFilter };

function clusterPathname(cluster: string, ...rest: string[]) {
  return ["/clusters", encodeURIComponent(cluster), ...rest].join("/");
}

export function useAcls(cluster: string, enabled = true) {
  return useQuery({
    queryKey: keys.acls(cluster),
    queryFn: () => get<AclListing>(clusterPathname(cluster, "acls")),
    enabled,
  });
}

export function useBrokerConfigs(cluster: string, id: number) {
  return useQuery({
    queryKey: keys.brokerConfigs(cluster, id),
    queryFn: () => get<ConfigEntry[]>(clusterPathname(cluster, "brokers", String(id), "configs")),
    enabled: Number.isFinite(id),
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

export function useRecords(cluster: string, query: RecordsFilter, enabled = true) {
  const scans = query.partitions?.length !== 0;

  return useInfiniteQuery({
    queryKey: keys.records(cluster, query),
    queryFn: ({ pageParam }) =>
      get<RecordPage>(
        clusterPathname(cluster, "topics", encodeURIComponent(query.topic), "records"),
        {
          partition: query.partitions,
          order: query.order,
          from: query.from,
          to: query.to,
          limit: RECORD_BATCH_SIZE,
          contains: query.filter?.contains,
          schemaId: query.schemaId,
          cursor: pageParam,
        },
      ),
    initialPageParam: null as string | null,
    getNextPageParam: (page) => page.nextCursor,
    placeholderData: (previous, previousQuery) =>
      scans && sameTopic(previousQuery?.queryKey, keys.records(cluster, query))
        ? previous
        : undefined,
    enabled: enabled && scans,
  });
}
