import { useRef, useState } from "react";
import {
  hashKey,
  keepPreviousData,
  useInfiniteQuery,
  type InfiniteData,
} from "@tanstack/react-query";

import { execute } from "@/graphql/execute";

import { recordsQuery } from "./documents";
import { keys, type RecordsFilter } from "./keys";
import type { KafkaRecord } from "./types";

export type { RecordsFilter };

declare const walkCursorBrand: unique symbol;

type WalkCursor = string & { readonly [walkCursorBrand]: true };

type WalkPageParam = WalkCursor | null;

type WalkEdges = {
  next: WalkCursor | null;
  prev: WalkCursor | null;
};

type RecordWindow = {
  records: KafkaRecord[];
  complete: boolean;
  obfuscated: boolean;
  edges: WalkEdges;
};

type WalkFocus = {
  window: RecordWindow;
};

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
  rewind: () => void;
};

type WireRecordPage = {
  records: KafkaRecord[];
  complete: boolean;
  obfuscated: boolean;
  nextCursor: string | null;
  prevCursor: string | null;
};

type WalkActions = Pick<RecordWalk, "stepNext" | "stepPrev" | "rewind">;

type WalkChrome = {
  identityHash: string;
  epoch: number;
  pageIndex: number;
  trail: RecordWindow[];
};

function visibleCluster<T>(cluster: T | null): T {
  if (cluster == null) {
    throw new Error("Unknown cluster");
  }
  return cluster;
}

function asWalkCursor(value: string): WalkCursor {
  return value as WalkCursor;
}

function toWindow(page: WireRecordPage): RecordWindow {
  return {
    records: page.records,
    complete: page.complete,
    obfuscated: page.obfuscated,
    edges: {
      next: page.nextCursor ? asWalkCursor(page.nextCursor) : null,
      prev: page.prevCursor ? asWalkCursor(page.prevCursor) : null,
    },
  };
}

function focusOf(
  data: InfiniteData<RecordWindow, WalkPageParam> | undefined,
): WalkFocus | undefined {
  if (data == null || data.pages.length === 0) {
    return undefined;
  }
  if (data.pages.length !== 1) {
    throw new Error("WalkFocus requires exactly one page");
  }
  return { window: data.pages[0] };
}

function walkPhase(input: {
  enabled: boolean;
  hasFocus: boolean;
  isPlaceholder: boolean;
  isFetching: boolean;
  isFetchingNextPage: boolean;
  isFetchingPreviousPage: boolean;
}): WalkPhase {
  if (!input.enabled) {
    return "ready";
  }
  if (input.isPlaceholder || input.isFetchingNextPage || input.isFetchingPreviousPage) {
    return "pending";
  }
  if (input.isFetching && !input.hasFocus) {
    return "loading";
  }
  if (input.isFetching) {
    return "refreshing";
  }
  return "ready";
}

function forwardKind(window: RecordWindow): "resume" | "advance" | "none" {
  if (window.edges.next == null) {
    return "none";
  }
  return window.complete ? "advance" : "resume";
}

function emptyWalk(actions: WalkActions): RecordWalk {
  return {
    records: [],
    complete: true,
    obfuscated: false,
    pageIndex: 0,
    hasNext: false,
    hasPrevious: false,
    phase: "ready",
    error: null,
    ...actions,
  };
}

function mergeWalk(
  window: RecordWindow | undefined,
  pageIndex: number,
  trailLength: number,
  phase: WalkPhase,
  error: unknown,
): Omit<RecordWalk, keyof WalkActions> {
  const stepping = phase === "pending";
  return {
    records: window?.records ?? [],
    complete: window?.complete ?? true,
    obfuscated: window?.obfuscated ?? false,
    pageIndex,
    hasNext: !stepping && (pageIndex < trailLength - 1 || window?.edges.next != null),
    hasPrevious: !stepping && (pageIndex > 0 || window?.edges.prev != null),
    phase,
    error,
  };
}

function emptyChrome(identityHash: string, epoch = 0): WalkChrome {
  return { identityHash, epoch, pageIndex: 0, trail: [] };
}

export function useRecords(cluster: string, query: RecordsFilter, enabled = true): RecordWalk {
  const recordsKey = keys.records(cluster, query);
  const identityHash = hashKey(recordsKey);
  const [chrome, setChrome] = useState<WalkChrome>(() => emptyChrome(identityHash));
  const identityChanged = chrome.identityHash !== identityHash;
  const epoch = identityChanged ? 0 : chrome.epoch;
  const pageIndex = identityChanged ? 0 : chrome.pageIndex;
  const trail = identityChanged ? [] : chrome.trail;
  const inFlight = useRef(false);

  if (identityChanged) {
    setChrome(emptyChrome(identityHash));
  }

  const result = useInfiniteQuery<
    RecordWindow,
    Error,
    InfiniteData<RecordWindow, WalkPageParam>,
    readonly unknown[],
    WalkPageParam
  >({
    queryKey: [...recordsKey, epoch],
    initialPageParam: null,
    maxPages: 1,
    placeholderData: keepPreviousData,
    enabled,
    queryFn: async ({ pageParam }) => {
      const { cluster: node } = await execute(recordsQuery, {
        cluster,
        query: { ...query, cursor: pageParam },
      });
      return toWindow(visibleCluster(node).records);
    },
    getNextPageParam: (window) => window.edges.next ?? undefined,
    getPreviousPageParam: (window) => window.edges.prev ?? undefined,
  });

  const queryFocus = focusOf(result.data);
  const viewingHead = trail.length === 0 || pageIndex === trail.length - 1;

  if (!identityChanged && !result.isPlaceholderData && queryFocus != null) {
    if (trail.length === 0) {
      setChrome((current) =>
        current.identityHash === identityHash &&
        current.epoch === epoch &&
        current.trail.length === 0
          ? { ...current, trail: [queryFocus.window] }
          : current,
      );
    } else if (
      viewingHead &&
      !result.isFetchingNextPage &&
      !result.isFetchingPreviousPage &&
      trail[pageIndex] !== queryFocus.window
    ) {
      setChrome((current) =>
        current.identityHash === identityHash &&
        current.epoch === epoch &&
        current.pageIndex === current.trail.length - 1 &&
        current.trail[current.pageIndex] !== queryFocus.window
          ? {
              ...current,
              trail: [...current.trail.slice(0, -1), queryFocus.window],
            }
          : current,
      );
    }
  }

  const displayed = trail[pageIndex] ?? queryFocus?.window;
  const phase = walkPhase({
    enabled,
    hasFocus: displayed != null,
    isPlaceholder: result.isPlaceholderData && trail.length === 0,
    isFetching: viewingHead && result.isFetching,
    isFetchingNextPage: viewingHead && result.isFetchingNextPage,
    isFetchingPreviousPage: viewingHead && result.isFetchingPreviousPage,
  });

  const idle: WalkActions = {
    stepNext() {},
    stepPrev() {},
    rewind() {},
  };

  if (!enabled) {
    return emptyWalk(idle);
  }

  function sameWalk(
    current: WalkChrome,
    start: Pick<WalkChrome, "identityHash" | "epoch">,
  ): boolean {
    return current.identityHash === start.identityHash && current.epoch === start.epoch;
  }

  function stepNext() {
    if (phase === "pending" || inFlight.current) {
      return;
    }
    if (pageIndex < trail.length - 1) {
      setChrome((current) =>
        sameWalk(current, { identityHash, epoch }) && current.pageIndex < current.trail.length - 1
          ? { ...current, pageIndex: current.pageIndex + 1 }
          : current,
      );
      return;
    }
    if (displayed == null) {
      return;
    }
    const kind = forwardKind(displayed);
    if (kind === "none") {
      return;
    }
    const start = { identityHash, epoch };
    inFlight.current = true;
    void (async () => {
      try {
        const next = await result.fetchNextPage({ cancelRefetch: false });
        if (next.isError) {
          return;
        }
        const fetched = focusOf(next.data)?.window;
        if (fetched == null) {
          return;
        }
        setChrome((current) => {
          if (!sameWalk(current, start)) {
            return current;
          }
          if (kind === "resume") {
            const trail = current.trail.slice();
            trail[current.pageIndex] = fetched;
            return { ...current, trail };
          }
          return {
            ...current,
            trail: [...current.trail, fetched],
            pageIndex: current.pageIndex + 1,
          };
        });
      } finally {
        inFlight.current = false;
      }
    })();
  }

  function stepPrev() {
    if (phase === "pending" || inFlight.current) {
      return;
    }
    if (pageIndex > 0) {
      setChrome((current) =>
        sameWalk(current, { identityHash, epoch }) && current.pageIndex > 0
          ? { ...current, pageIndex: current.pageIndex - 1 }
          : current,
      );
      return;
    }
    if (displayed?.edges.prev == null) {
      return;
    }
    const start = { identityHash, epoch };
    inFlight.current = true;
    void (async () => {
      try {
        const previous = await result.fetchPreviousPage({ cancelRefetch: false });
        if (previous.isError) {
          return;
        }
        const fetched = focusOf(previous.data)?.window;
        if (fetched == null) {
          return;
        }
        setChrome((current) =>
          sameWalk(current, start)
            ? { ...current, trail: [fetched, ...current.trail], pageIndex: 0 }
            : current,
        );
      } finally {
        inFlight.current = false;
      }
    })();
  }

  function rewind() {
    setChrome(emptyChrome(identityHash, epoch + 1));
  }

  return {
    ...mergeWalk(displayed, pageIndex, trail.length, phase, result.error ?? null),
    stepNext,
    stepPrev,
    rewind,
  };
}
