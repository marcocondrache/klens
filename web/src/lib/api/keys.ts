import type { RecordOrder } from "./types";

export type RecordsFilter = {
  topic: string;
  /** `null` reads every partition; an empty list reads none. */
  partitions: number[] | null;
  order: RecordOrder | null;
  from: string | null;
  to: string | null;
  filter: { contains: string } | null;
  schemaId: number | null;
};

export type TailFilter = {
  topic: string;
  /** `null` follows every partition; an empty list follows none. */
  partitions: number[] | null;
  contains: string | null;
  schemaId: number | null;
};

export const keys = {
  whoami: () => ["whoami"] as const,
  clusters: () => ["clusters"] as const,
  cluster: (cluster: string) => ["cluster", cluster] as const,

  brokerRows: (cluster: string) => ["cluster", cluster, "brokers"] as const,
  brokerConfigs: (cluster: string, id: number) =>
    ["cluster", cluster, "brokers", id, "configs"] as const,

  topicRows: (cluster: string) => ["cluster", cluster, "topics"] as const,
  topic: (cluster: string, topic: string) => ["cluster", cluster, "topics", topic] as const,
  topicConfigs: (cluster: string, topic: string) =>
    ["cluster", cluster, "topics", topic, "configs"] as const,
  topicGroups: (cluster: string, topic: string) =>
    ["cluster", cluster, "topics", topic, "groups"] as const,
  records: (cluster: string, query: RecordsFilter) =>
    ["cluster", cluster, "topics", query.topic, "records", query] as const,

  groupRows: (cluster: string) => ["cluster", cluster, "groups"] as const,
  group: (cluster: string, group: string) => ["cluster", cluster, "groups", group] as const,

  subjectRows: (cluster: string) => ["cluster", cluster, "subjects"] as const,
  subjectVersions: (cluster: string, name: string) =>
    ["cluster", cluster, "subjects", name] as const,
  subject: (cluster: string, name: string, version: number | null) =>
    ["cluster", cluster, "subjects", name, version] as const,

  acls: (cluster: string) => ["cluster", cluster, "acls"] as const,
  search: (cluster: string, term: string) => ["cluster", cluster, "search", term] as const,

  tail: (cluster: string, filter: TailFilter) => ["tail", cluster, filter] as const,
};
