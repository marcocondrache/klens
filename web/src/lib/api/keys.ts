import type { RecordQueryInput } from "./types";

export type RecordsFilter = Omit<RecordQueryInput, "cursor">;

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
  records: (cluster: string, query: RecordsFilter, cursor: string | null) =>
    ["cluster", cluster, "topics", query.topic, "records", query, cursor] as const,

  groupRows: (cluster: string) => ["cluster", cluster, "groups"] as const,
  group: (cluster: string, group: string) => ["cluster", cluster, "groups", group] as const,

  subjectRows: (cluster: string) => ["cluster", cluster, "subjects"] as const,
  subjectVersions: (cluster: string, name: string) =>
    ["cluster", cluster, "subjects", name] as const,
  subject: (cluster: string, name: string, version: number | null) =>
    ["cluster", cluster, "subjects", name, version] as const,

  acls: (cluster: string) => ["cluster", cluster, "acls"] as const,
  search: (cluster: string, term: string) => ["cluster", cluster, "search", term] as const,
};
