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

function canStepPrev(window: RecordWindow | undefined, pageIndex: number): boolean {
  return window?.edges.prev != null || pageIndex > 0;
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
  focus: WalkFocus | undefined,
  pageIndex: number,
  phase: WalkPhase,
  error: unknown,
): Omit<RecordWalk, keyof WalkActions> {
  const stepping = phase === "pending";
  return {
    records: focus?.window.records ?? [],
    complete: focus?.window.complete ?? true,
    obfuscated: focus?.window.obfuscated ?? false,
    pageIndex,
    hasNext: !stepping && focus?.window.edges.next != null,
    hasPrevious: !stepping && canStepPrev(focus?.window, pageIndex),
    phase,
    error,
  };
}

export function useRecords(cluster: string, query: RecordsFilter, enabled = true): RecordWalk {
  const recordsKey = keys.records(cluster, query);
  const identityHash = hashKey(recordsKey);
  const [chrome, setChrome] = useState<WalkChrome>({ identityHash, epoch: 0, pageIndex: 0 });
  const identityChanged = chrome.identityHash !== identityHash;
  const epoch = identityChanged ? 0 : chrome.epoch;
  const pageIndex = identityChanged ? 0 : chrome.pageIndex;
  const inFlight = useRef(false);

  if (identityChanged) {
    inFlight.current = false;
    setChrome({ identityHash, epoch: 0, pageIndex: 0 });
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
    // first === last === focus, so previous is this window's server prev.
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

  const focus = focusOf(result.data);
  const phase = walkPhase({
    enabled,
    hasFocus: focus != null,
    isPlaceholder: result.isPlaceholderData,
    isFetching: result.isFetching,
    isFetchingNextPage: result.isFetchingNextPage,
    isFetchingPreviousPage: result.isFetchingPreviousPage,
  });

  const idle: WalkActions = {
    stepNext() {},
    stepPrev() {},
    rewind() {},
  };

  if (!enabled) {
    return emptyWalk(idle);
  }

  function sameWalk(current: WalkChrome, start: WalkChrome): boolean {
    return current.identityHash === start.identityHash && current.epoch === start.epoch;
  }

  async function stepNext() {
    if (phase === "pending" || inFlight.current) {
      return;
    }
    if (focus == null) {
      return;
    }
    const kind = forwardKind(focus.window);
    if (kind === "none") {
      return;
    }
    const start = { identityHash, epoch, pageIndex };
    inFlight.current = true;
    try {
      const next = await result.fetchNextPage({ cancelRefetch: false });
      if (next.isError) {
        return;
      }
      if (kind === "advance") {
        setChrome((current) =>
          sameWalk(current, start) ? { ...current, pageIndex: current.pageIndex + 1 } : current,
        );
      }
    } finally {
      inFlight.current = false;
    }
  }

  async function stepPrev() {
    if (phase === "pending" || inFlight.current) {
      return;
    }
    if (focus?.window.edges.prev != null) {
      const start = { identityHash, epoch, pageIndex };
      inFlight.current = true;
      try {
        const previous = await result.fetchPreviousPage({ cancelRefetch: false });
        if (previous.isError) {
          return;
        }
        setChrome((current) =>
          sameWalk(current, start)
            ? { ...current, pageIndex: Math.max(0, current.pageIndex - 1) }
            : current,
        );
      } finally {
        inFlight.current = false;
      }
      return;
    }
    if (pageIndex > 0) {
      rewind();
    }
  }

  function rewind() {
    setChrome({ identityHash, epoch: epoch + 1, pageIndex: 0 });
  }

  return {
    ...mergeWalk(focus, pageIndex, phase, result.error ?? null),
    stepNext,
    stepPrev,
    rewind,
  };
}
