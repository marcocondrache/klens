import { useEffect, useState } from "react";

import type { TailEvent } from "@/api/types.gen";

import {
  ApiError,
  RETRY_ATTEMPTS,
  eventsUrl,
  openEvents,
  readEvents,
  retryDelay,
  streamError,
  wait,
} from "./client";
import { recordId } from "@/lib/records";

import type { KafkaRecord } from "./types";

export type TailFilter = {
  topic: string;
  partition: number | null;
  contains: string | null;
  schemaId: number | null;
};

export type TailStatus = "idle" | "connecting" | "live" | "reconnecting" | "error";

/** Newest records a tail keeps on screen. Older ones fall off the end. */
export const TAIL_BUFFER = 1000;

/** Typing in the search box reopens a tail, so wait for a pause first. */
const OPEN_DELAY_MS = 300;

type TailHandlers = {
  onStatus: (status: TailStatus) => void;
  onEvent: (event: TailEvent) => void;
  onError: (error: ApiError) => void;
};

type TailState = {
  scope: string;
  enabled: boolean;
  records: KafkaRecord[];
  skipped: number;
  obfuscated: boolean;
  status: TailStatus;
  error: ApiError | null;
};

function initial(scope: string, enabled: boolean): TailState {
  return {
    scope,
    enabled,
    records: [],
    skipped: 0,
    obfuscated: false,
    status: enabled ? "connecting" : "idle",
    error: null,
  };
}

/**
 * Follow a topic from its current end. Records arrive oldest first and are
 * kept newest first, capped at {@link TAIL_BUFFER}. Turning `enabled` off
 * closes the stream but keeps what arrived; a new filter starts over.
 */
export function useTail(cluster: string, filter: TailFilter, enabled = true) {
  const active = enabled && Boolean(cluster);
  const scope = JSON.stringify([cluster, filter]);
  const [state, setState] = useState(() => initial(scope, active));
  const [generation, setGeneration] = useState(0);

  if (state.scope !== scope) {
    setState(initial(scope, active));
  } else if (state.enabled !== active) {
    setState({ ...state, enabled: active, status: active ? "connecting" : "idle", error: null });
  }

  const { topic, partition, contains, schemaId } = filter;

  useEffect(() => {
    if (!active) return;

    const controller = new AbortController();
    const path = `/clusters/${encodeURIComponent(cluster)}/topics/${encodeURIComponent(topic)}/records/tail`;
    const url = eventsUrl(path, { partition, contains, schemaId });

    const timer = setTimeout(() => {
      void follow(url, controller.signal, {
        onStatus: (status) => setState((current) => ({ ...current, status })),
        onEvent: (event) => setState((current) => apply(current, event)),
        onError: (error) => setState((current) => ({ ...current, status: "error", error })),
      });
    }, OPEN_DELAY_MS);

    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [cluster, topic, partition, contains, schemaId, active, generation]);

  return {
    records: state.records,
    skipped: state.skipped,
    obfuscated: state.obfuscated,
    status: state.status,
    error: state.error,
    clear: () => setState((current) => ({ ...current, records: [], skipped: 0 })),
    retry: () => {
      setState((current) => ({ ...current, status: "connecting", error: null }));
      setGeneration((value) => value + 1);
    },
  };
}

function apply(state: TailState, event: TailEvent): TailState {
  switch (event.type) {
    case "ready":
      return { ...state, status: "live", obfuscated: event.obfuscated };

    case "records": {
      const seen = new Set(state.records.map(recordId));
      const fresh: KafkaRecord[] = [];
      for (let index = event.records.length - 1; index >= 0; index -= 1) {
        const record = event.records[index];
        if (!seen.has(recordId(record))) fresh.push(record);
      }
      return {
        ...state,
        records: [...fresh, ...state.records].slice(0, TAIL_BUFFER),
        skipped: state.skipped + Number(event.skipped),
      };
    }
  }
}

/**
 * Keep a tail open. A dropped connection reopens from the new end with
 * backoff. A refused request or an `error` frame ends it for good.
 */
async function follow(url: string, signal: AbortSignal, handlers: TailHandlers) {
  let attempt = 0;
  while (!signal.aborted) {
    handlers.onStatus(attempt === 0 ? "connecting" : "reconnecting");
    let ended = false;
    try {
      const body = await openEvents(url, signal);
      await readEvents(body, signal, (frame) => {
        switch (frame.event) {
          case "ready":
          case "records": {
            const event = parse(frame.data);
            if (event == null) return;
            if (event.type === "ready") attempt = 0;
            handlers.onEvent(event);
            return;
          }
          case "error":
            ended = true;
            handlers.onError(streamError(frame.data));
            return;
        }
      });
    } catch (error) {
      if (signal.aborted) return;
      if (error instanceof ApiError) {
        handlers.onError(error);
        return;
      }
      console.error(error);
    }
    if (ended || signal.aborted) return;

    attempt += 1;
    if (attempt > RETRY_ATTEMPTS) {
      handlers.onError(new ApiError("Lost the connection to the live tail.", 0));
      return;
    }
    handlers.onStatus("reconnecting");
    await wait(retryDelay(attempt), signal);
  }
}

function parse(data: string): TailEvent | null {
  try {
    return JSON.parse(data) as TailEvent;
  } catch {
    return null;
  }
}
