import { useEffect } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";

import type { UpdatesSubscription } from "@/graphql/graphql";

import { updatesSubscription } from "./documents";
import { keys } from "./keys";
import { subscribe } from "./subscribe";
import type { GroupDetail, GroupOffset, GroupRow, Point, TopicGroupRow, TopicRow } from "./types";

/** Matches the server's ring capacity, so seed and stream agree on length. */
const HISTORY_LEN = 60;

type Update = UpdatesSubscription["updates"];

export type Scope = { topic?: string; group?: string };

/**
 * The one subscription. Every lane delta for this page arrives here already
 * narrowed to its scope, and is *applied* to the cache: a catalog tick costs
 * one small event rather than a fan of full-catalog refetches.
 */
export function useUpdates(cluster: string, scope: Scope = {}) {
  const queryClient = useQueryClient();
  const { topic, group } = scope;

  useEffect(() => {
    if (!cluster) {
      return;
    }

    return subscribe(
      updatesSubscription,
      { cluster, scope: { topic: topic ?? null, group: group ?? null } },
      ({ updates }) => apply(queryClient, cluster, updates),
    );
  }, [cluster, topic, group, queryClient]);
}

function apply(queryClient: QueryClient, cluster: string, update: Update): void {
  switch (update.__typename) {
    case "WatermarksTick": {
      const rates = new Map(update.topics.map((entry) => [entry.topic, entry.rate]));

      patchTopicRows(queryClient, cluster, (row) => {
        const rate = rates.get(row.name);
        return rate === undefined ? row : { ...row, rate };
      });

      for (const [name, rate] of rates) {
        patchTopicDetailRate(queryClient, cluster, name, rate);
        appendPoint(queryClient, keys.topicRateHistory(cluster, name), update.at, rate);
      }
      return;
    }

    case "GroupLagUpdate": {
      const lag = Number(update.lag);

      patchGroupRows(queryClient, cluster, update.group, (row) => ({
        ...row,
        totalLag: update.lag,
        lagComplete: update.lagComplete,
      }));

      queryClient.setQueryData(
        keys.group(cluster, update.group),
        (detail: GroupDetail | null | undefined) =>
          detail == null
            ? detail
            : {
                ...detail,
                totalLag: update.lag,
                lagComplete: update.lagComplete,
                // A list-scoped wave carries totals only; keeping the stale
                // per-partition rows beats blanking the offsets table.
                offsets: update.offsets.length > 0 ? update.offsets : detail.offsets,
              },
      );

      appendPoint(queryClient, keys.groupLagHistory(cluster, update.group), update.at, lag);

      for (const [name, lagOnTopic] of lagByTopic(update.offsets)) {
        queryClient.setQueryData(
          keys.topicGroups(cluster, name),
          (rows: TopicGroupRow[] | undefined) =>
            rows?.map((row) =>
              row.id === update.group ? { ...row, lagOnTopic: String(lagOnTopic) } : row,
            ),
        );
      }
      return;
    }

    case "TopologyDelta": {
      // Counts and lane freshness move with topology, and nothing else
      // reports them.
      void queryClient.invalidateQueries({ queryKey: keys.clusters() });

      const topics = [...update.addedTopics, ...update.removedTopics, ...update.changedTopics];
      const groups = [...update.addedGroups, ...update.removedGroups, ...update.changedGroups];

      if (topics.length > 0) {
        void queryClient.invalidateQueries({ queryKey: keys.topicRows(cluster), exact: true });
        for (const name of topics) {
          void queryClient.invalidateQueries({ queryKey: keys.topic(cluster, name), exact: true });
        }
      }

      if (groups.length > 0) {
        void queryClient.invalidateQueries({ queryKey: keys.groupRows(cluster), exact: true });
        for (const id of groups) {
          void queryClient.invalidateQueries({ queryKey: keys.group(cluster, id), exact: true });
        }
        // Membership moved, and the delta names groups rather than the topics
        // they read, so every open topic-groups view is suspect.
        void queryClient.invalidateQueries({
          predicate: (query) => isTopicSubKey(query.queryKey, cluster, "groups"),
        });
      }

      if (update.brokersChanged) {
        void queryClient.invalidateQueries({ queryKey: keys.brokerRows(cluster), exact: true });
      }
      return;
    }

    case "ConfigsChanged": {
      // Retention and cleanup policy are row fields sourced from the config
      // lane, so a config sweep moves the rows too.
      void queryClient.invalidateQueries({ queryKey: keys.topicRows(cluster), exact: true });
      for (const name of update.configTopics) {
        void queryClient.invalidateQueries({
          queryKey: keys.topicConfigs(cluster, name),
          exact: true,
        });
        void queryClient.invalidateQueries({ queryKey: keys.topic(cluster, name), exact: true });
      }
      return;
    }

    case "SubjectsChanged": {
      void queryClient.invalidateQueries({ queryKey: keys.subjectRows(cluster), exact: true });
      for (const name of [...update.changed, ...update.removed]) {
        // Every cached version of the subject, not just the latest one.
        void queryClient.invalidateQueries({ queryKey: keys.subjectVersions(cluster, name) });
      }
      return;
    }

    // The stream outran this client, so the deltas it missed are gone. The
    // connection is still good: refetch the projections and keep following.
    case "Resync": {
      void queryClient.invalidateQueries({ queryKey: keys.clusters() });
      void queryClient.invalidateQueries({ queryKey: keys.cluster(cluster) });
      return;
    }
  }
}

function patchTopicRows(
  queryClient: QueryClient,
  cluster: string,
  patch: (row: TopicRow) => TopicRow,
) {
  queryClient.setQueryData(keys.topicRows(cluster), (rows: TopicRow[] | undefined) =>
    rows?.map(patch),
  );
}

function patchTopicDetailRate(
  queryClient: QueryClient,
  cluster: string,
  topic: string,
  rate: number,
) {
  queryClient.setQueryData(
    keys.topic(cluster, topic),
    (cache: { row: TopicRow | null } | undefined) =>
      cache?.row ? { ...cache, row: { ...cache.row, rate } } : cache,
  );
}

function patchGroupRows(
  queryClient: QueryClient,
  cluster: string,
  group: string,
  patch: (row: GroupRow) => GroupRow,
) {
  queryClient.setQueryData(keys.groupRows(cluster), (rows: GroupRow[] | undefined) =>
    rows?.map((row) => (row.id === group ? patch(row) : row)),
  );
}

/** Appends only where a ring is already cached: an unscoped tick names every
 * topic in the cluster, and none of them need a history nobody is showing. */
function appendPoint(queryClient: QueryClient, key: readonly unknown[], at: string, value: number) {
  queryClient.setQueryData(key, (points: Point[] | undefined) =>
    points ? [...points, { at, value }].slice(-HISTORY_LEN) : points,
  );
}

function lagByTopic(offsets: GroupOffset[]): Map<string, number> {
  const totals = new Map<string, number>();
  for (const offset of offsets) {
    totals.set(offset.topic, (totals.get(offset.topic) ?? 0) + Number(offset.lag));
  }
  return totals;
}

function isTopicSubKey(key: readonly unknown[], cluster: string, leaf: string) {
  return key[0] === "cluster" && key[1] === cluster && key[2] === "topics" && key[4] === leaf;
}
