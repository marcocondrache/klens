import type { RecordQuery } from "./types";

export type RecordsFilter = Omit<RecordQuery, "cursor">;

export const keys = {
  clusters: () => ["clusters"] as const,
  cluster: (cluster: string) => ["cluster", cluster] as const,
  throughput: (cluster: string) => ["cluster", cluster, "throughput"] as const,
  brokers: (cluster: string) => ["cluster", cluster, "brokers"] as const,
  broker: (cluster: string, id: number) => ["cluster", cluster, "brokers", id] as const,
  brokerConfigs: (cluster: string, id: number) =>
    ["cluster", cluster, "brokers", id, "configs"] as const,
  topics: (cluster: string) => ["cluster", cluster, "topics"] as const,
  topic: (cluster: string, topic: string) => ["cluster", cluster, "topics", topic] as const,
  topicConfigs: (cluster: string, topic: string) =>
    ["cluster", cluster, "topics", topic, "configs"] as const,
  topicThroughput: (cluster: string, topic: string) =>
    ["cluster", cluster, "topics", topic, "throughput"] as const,
  records: (query: RecordsFilter) =>
    ["cluster", query.cluster, "topics", query.topic, "records", query] as const,
  groups: (cluster: string) => ["cluster", cluster, "groups"] as const,
  topicGroups: (cluster: string, topic: string) =>
    ["cluster", cluster, "topics", topic, "groups"] as const,
  group: (cluster: string, group: string) => ["cluster", cluster, "groups", group] as const,
  groupLagHistory: (cluster: string, group: string) =>
    ["cluster", cluster, "groups", group, "lag"] as const,
  subjects: (cluster: string) => ["cluster", cluster, "subjects"] as const,
  catalogHealth: (cluster: string) => ["cluster", cluster, "catalogHealth"] as const,
  search: (cluster: string, term: string) => ["cluster", cluster, "search", term] as const,
};
