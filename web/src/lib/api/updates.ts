import { useEffect } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";

import type { UpdatesSubscription } from "@/graphql/graphql";

import { updatesSubscription } from "./documents";
import { keys } from "./keys";
import { subscribe } from "./subscribe";
import type { GroupDetail, GroupOffset, GroupRow, TopicGroupRow, TopicRow } from "./types";

type Update = UpdatesSubscription["updates"];

export type Scope = { topic?: string; group?: string };

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
      }
      return;
    }

    case "GroupLagUpdate": {
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
                offsets: update.offsets.length > 0 ? update.offsets : detail.offsets,
              },
      );

      for (const [name, lagOnTopic] of lagByTopic(update.offsets)) {
        queryClient.setQueryData(
          keys.topicGroups(cluster, name),
          (rows: TopicGroupRow[] | undefined) =>
            rows?.map((row) =>
              row.id === update.group
                ? { ...row, lagOnTopic: lagOnTopic == null ? null : String(lagOnTopic) }
                : row,
            ),
        );
      }
      return;
    }

    case "TopologyDelta": {
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
        void queryClient.invalidateQueries({ queryKey: keys.subjectVersions(cluster, name) });
      }
      return;
    }

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

function lagByTopic(offsets: GroupOffset[]): Map<string, number | null> {
  const totals = new Map<string, { sum: number; known: boolean }>();
  for (const offset of offsets) {
    const entry = totals.get(offset.topic) ?? { sum: 0, known: false };
    if (offset.lag != null) {
      entry.sum += Number(offset.lag);
      entry.known = true;
    }
    totals.set(offset.topic, entry);
  }
  return new Map([...totals].map(([topic, { sum, known }]) => [topic, known ? sum : null]));
}

function isTopicSubKey(key: readonly unknown[], cluster: string, leaf: string) {
  return key[0] === "cluster" && key[1] === cluster && key[2] === "topics" && key[4] === leaf;
}
