import {
  keepPreviousData,
  useInfiniteQuery,
  useQueryClient,
  type InfiniteData,
} from "@tanstack/react-query";

import { execute } from "@/graphql/execute";

import type { RecordsQuery } from "@/graphql/graphql";

import { recordsQuery } from "./documents";
import { keys, type RecordsFilter } from "./keys";
import type { KafkaRecord } from "./types";

export type { RecordsFilter };

type RecordPage = NonNullable<RecordsQuery["cluster"]>["records"];

export type WalkPhase = "loading" | "ready" | "refreshing" | "pending";

export type RecordWalk = {
  records: KafkaRecord[];
  complete: boolean;
  obfuscated: boolean;
  pageIndex: number;
  hasNext: boolean;
  hasPrevious: boolean;
  phase: WalkPhase;
  error: unknown;
  stepNext: () => void;
  stepPrev: () => void;
};

function visibleCluster<T>(cluster: T | null): T {
  if (cluster == null) {
    throw new Error("Unknown cluster");
  }
  return cluster;
}

function walkPhase(
  enabled: boolean,
  pageCount: number,
  isPlaceholder: boolean,
  isFetching: boolean,
  isFetchingNextPage: boolean,
): WalkPhase {
  if (!enabled) {
    return "ready";
  }
  if (isPlaceholder || isFetchingNextPage) {
    return "pending";
  }
  if (isFetching && pageCount === 0) {
    return "loading";
  }
  if (isFetching) {
    return "refreshing";
  }
  return "ready";
}

export function useRecords(cluster: string, query: RecordsFilter, enabled = true): RecordWalk {
  const recordsKey = keys.records(cluster, query);
  const queryClient = useQueryClient();

  const result = useInfiniteQuery({
    queryKey: recordsKey,
    initialPageParam: null as string | null,
    placeholderData: keepPreviousData,
    enabled,
    queryFn: async ({ pageParam }) => {
      const { cluster: node } = await execute(recordsQuery, {
        cluster,
        query: { ...query, cursor: pageParam },
      });
      return visibleCluster(node).records;
    },
    getNextPageParam: (page) => page.nextCursor ?? undefined,
  });

  const pages = result.data?.pages ?? [];
  const pageIndex = result.isPlaceholderData ? 0 : Math.max(0, pages.length - 1);
  const page = pages[pageIndex];
  const phase = walkPhase(
    enabled,
    pages.length,
    result.isPlaceholderData,
    result.isFetching,
    result.isFetchingNextPage,
  );
  const pending = phase === "pending";

  if (!enabled) {
    return {
      records: [],
      complete: true,
      obfuscated: false,
      pageIndex: 0,
      hasNext: false,
      hasPrevious: false,
      phase: "ready",
      error: null,
      stepNext() {},
      stepPrev() {},
    };
  }

  return {
    records: page?.records ?? [],
    complete: page?.complete ?? true,
    obfuscated: page?.obfuscated ?? false,
    pageIndex,
    hasNext: !pending && page?.nextCursor != null,
    hasPrevious: !pending && pages.length > 1,
    phase,
    error: result.error ?? null,
    stepNext() {
      if (pending || page?.nextCursor == null) {
        return;
      }
      void result.fetchNextPage({ cancelRefetch: false });
    },
    stepPrev() {
      if (pending || pages.length <= 1) {
        return;
      }
      queryClient.setQueryData<InfiniteData<RecordPage, string | null>>(recordsKey, (data) => {
        if (data == null || data.pages.length <= 1) {
          return data;
        }
        return {
          pages: data.pages.slice(0, -1),
          pageParams: data.pageParams.slice(0, -1),
        };
      });
    },
  };
}
