import { useState } from "react";
import { hashKey, keepPreviousData, useInfiniteQuery } from "@tanstack/react-query";

import { execute } from "@/graphql/execute";

import { recordsQuery } from "./documents";
import { keys, type RecordsFilter } from "./keys";
import type { KafkaRecord } from "./types";

export type { RecordsFilter };

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
  isFetchingPreviousPage: boolean,
): WalkPhase {
  if (!enabled) {
    return "ready";
  }
  if (isPlaceholder || isFetchingNextPage || isFetchingPreviousPage) {
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
  const identity = hashKey(recordsKey);
  const [seen, setSeen] = useState(identity);
  const [pageIndex, setPageIndex] = useState(0);
  const identityChanged = seen !== identity;
  const index = identityChanged ? 0 : pageIndex;

  if (identityChanged) {
    setSeen(identity);
    setPageIndex(0);
  }

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
    getPreviousPageParam: (page) => page.prevCursor ?? undefined,
  });

  const pages = result.data?.pages ?? [];
  const page = pages[index];
  const phase = walkPhase(
    enabled,
    pages.length,
    result.isPlaceholderData,
    result.isFetching,
    result.isFetchingNextPage,
    result.isFetchingPreviousPage,
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
    pageIndex: index,
    hasNext: !pending && (index < pages.length - 1 || page?.nextCursor != null),
    hasPrevious: !pending && (index > 0 || page?.prevCursor != null),
    phase,
    error: result.error ?? null,
    stepNext() {
      if (pending) {
        return;
      }
      if (index < pages.length - 1) {
        setPageIndex(index + 1);
        return;
      }
      if (page?.nextCursor == null) {
        return;
      }
      void result.fetchNextPage({ cancelRefetch: false }).then((next) => {
        if (!next.isError && next.data != null) {
          setPageIndex(next.data.pages.length - 1);
        }
      });
    },
    stepPrev() {
      if (pending) {
        return;
      }
      if (index > 0) {
        setPageIndex(index - 1);
        return;
      }
      if (page?.prevCursor == null) {
        return;
      }
      void result.fetchPreviousPage({ cancelRefetch: false });
    },
  };
}
