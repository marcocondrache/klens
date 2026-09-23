import { useState } from "react";
import { experimental_streamedQuery, useQuery, useQueryClient } from "@tanstack/react-query";

import type { TailEvent } from "@/api/types.gen";

import { ApiError, events, streamError } from "./client";
import { keys, type TailFilter } from "./keys";
import type { KafkaRecord } from "./types";

export type { TailFilter };

export type TailStatus = "idle" | "connecting" | "live" | "reconnecting" | "error";

export const TAIL_BUFFER = 1000;

const OPEN_DELAY_MS = 300;

type TailChunk = TailEvent | { type: "connecting" };

type Tail = { records: KafkaRecord[]; skipped: number; obfuscated: boolean; ready: boolean };

const EMPTY_TAIL: Tail = { records: [], skipped: 0, obfuscated: false, ready: false };

export function useTail(cluster: string, filter: TailFilter) {
  const queryClient = useQueryClient();
  const [paused, setPaused] = useState(false);
  const queryKey = keys.tail(cluster, filter);

  const query = useQuery({
    queryKey,
    queryFn: experimental_streamedQuery({
      streamFn: ({ signal }) => follow(cluster, filter, signal),
      reducer: apply,
      initialValue: EMPTY_TAIL,
      refetchMode: "append",
    }),
    enabled: !paused,
    staleTime: 0,
    retry: (_, error) => !(error instanceof ApiError),
  });

  return {
    ...(query.data ?? EMPTY_TAIL),
    status: status(query),
    error: query.error,
    paused,
    pause: () => {
      setPaused(true);
      void queryClient.cancelQueries({ queryKey });
    },
    resume: () => setPaused(false),
    retry: () => void query.refetch(),
    clear: () =>
      queryClient.setQueryData(queryKey, (tail) => tail && { ...tail, records: [], skipped: 0 }),
  };
}

async function* follow(
  cluster: string,
  filter: TailFilter,
  signal: AbortSignal,
): AsyncGenerator<TailChunk> {
  yield { type: "connecting" };
  await new Promise((resolve) => setTimeout(resolve, OPEN_DELAY_MS));

  const path = `/clusters/${encodeURIComponent(cluster)}/topics/${encodeURIComponent(filter.topic)}/records/tail`;
  const { partition, contains, schemaId } = filter;
  for await (const frame of events(path, signal, { partition, contains, schemaId })) {
    if (frame.event === "error") throw streamError(frame.data);
    if (frame.event === "ready" || frame.event === "records") {
      yield JSON.parse(frame.data) as TailEvent;
    }
  }
  throw new Error("The live tail connection closed.");
}

function apply(tail: Tail, chunk: TailChunk): Tail {
  switch (chunk.type) {
    case "connecting":
      return { ...tail, ready: false };
    case "ready":
      return { ...tail, ready: true, obfuscated: chunk.obfuscated };
    case "records":
      return {
        ...tail,
        records: [...chunk.records.toReversed(), ...tail.records].slice(0, TAIL_BUFFER),
        skipped: tail.skipped + Number(chunk.skipped),
      };
  }
}

function status(query: {
  fetchStatus: string;
  isError: boolean;
  failureCount: number;
  data?: Tail;
}): TailStatus {
  if (query.fetchStatus === "idle") return query.isError ? "error" : "idle";
  if (query.data?.ready) return "live";
  return query.failureCount > 0 ? "reconnecting" : "connecting";
}
