import type { SearchSchemaInput } from "@tanstack/react-router";

import { filterParam } from "@/components/data-table/filters";
import type { GroupState } from "@/lib/api/types";

type RawSearch = Record<string, unknown>;

/**
 * A `validateSearch` that accepts any subset of `T` from links and navigations,
 * leaving the fallbacks to `parse`.
 */
function searchValidator<T>(parse: (search: RawSearch) => T) {
  return (search: Partial<T> & SearchSchemaInput): T => parse(search as RawSearch);
}

function text(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function flag(value: unknown): boolean {
  return value === true || value === "true";
}

function literal<T extends string>(allowed: readonly T[], value: unknown): T | undefined {
  return allowed.find((candidate) => candidate === value);
}

// Filter params: `a,b` matches any of the values, `!a,b` none of them.
export const TOPIC_POLICIES = ["delete", "compact"] as const;
export const TOPIC_HEALTH = ["under-replicated", "in-sync"] as const;
export const TOPIC_ACTIVITY = ["active", "idle"] as const;

export type TopicFilter = "policy" | "health" | "activity";
export type TopicsSearch = { q: string; internal: boolean } & Partial<Record<TopicFilter, string>>;

export const topicsDefaults = { q: "", internal: false };

export const validateTopicsSearch = searchValidator<TopicsSearch>((search) => ({
  q: text(search.q),
  internal: flag(search.internal),
  policy: filterParam(TOPIC_POLICIES, search.policy),
  health: filterParam(TOPIC_HEALTH, search.health),
  activity: filterParam(TOPIC_ACTIVITY, search.activity),
}));

export const GROUP_STATES = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "DEAD",
] as const satisfies readonly GroupState[];

export const GROUP_LAG = ["lagging", "caught-up"] as const;

export type GroupFilter = "state" | "lag";
export type GroupsSearch = { q: string } & Partial<Record<GroupFilter, string>>;

export const groupsDefaults = { q: "" };

export const validateGroupsSearch = searchValidator<GroupsSearch>((search) => ({
  q: text(search.q),
  state: filterParam(GROUP_STATES, search.state),
  lag: filterParam(GROUP_LAG, search.lag),
}));

export type SchemasSearch = { q: string };

export const schemasDefaults = { q: "" };

export const validateSchemasSearch = searchValidator<SchemasSearch>((search) => ({
  q: text(search.q),
}));

export const ACL_RESOURCE_TYPES = [
  "TOPIC",
  "GROUP",
  "CLUSTER",
  "TRANSACTIONAL_ID",
  "DELEGATION_TOKEN",
] as const;

export const ACL_OPERATIONS = [
  "ALL",
  "READ",
  "WRITE",
  "CREATE",
  "DELETE",
  "ALTER",
  "DESCRIBE",
  "CLUSTER_ACTION",
  "DESCRIBE_CONFIGS",
  "ALTER_CONFIGS",
  "IDEMPOTENT_WRITE",
] as const;
export const ACL_PERMISSIONS = ["ALLOW", "DENY"] as const;
export const ACL_PATTERNS = ["LITERAL", "PREFIXED"] as const;

export type AclFilter = "resource" | "operation" | "permission" | "pattern";
export type AclsSearch = { q: string } & Partial<Record<AclFilter, string>>;

export const aclsDefaults = { q: "" };

export const validateAclsSearch = searchValidator<AclsSearch>((search) => ({
  q: text(search.q),
  resource: filterParam(ACL_RESOURCE_TYPES, search.resource),
  operation: filterParam(ACL_OPERATIONS, search.operation),
  permission: filterParam(ACL_PERMISSIONS, search.permission),
  pattern: filterParam(ACL_PATTERNS, search.pattern),
}));

export type LoginSearch = { error?: string; from?: "callback" };

export function validateLoginSearch(search: RawSearch): LoginSearch {
  return {
    error: text(search.error) || undefined,
    from: literal(["callback"], search.from),
  };
}

export const TOPIC_TABS = ["partitions", "groups", "config"] as const;

export type TopicDetailSearch = { tab?: (typeof TOPIC_TABS)[number] };

export function topicTab(value: unknown) {
  return literal(TOPIC_TABS, value);
}

export function validateTopicDetailSearch(search: RawSearch): TopicDetailSearch {
  return { tab: topicTab(search.tab) };
}

export const GROUP_TABS = ["offsets", "members"] as const;

export type GroupDetailSearch = { tab: (typeof GROUP_TABS)[number] };

export const groupDetailDefaults = { tab: "offsets" as const };

export function groupTab(value: unknown) {
  return literal(GROUP_TABS, value) ?? "offsets";
}

export const validateGroupDetailSearch = searchValidator<GroupDetailSearch>((search) => ({
  tab: groupTab(search.tab),
}));

export function parseSearch(searchStr: string): Record<string, string> {
  const query = searchStr.startsWith("?") ? searchStr.slice(1) : searchStr;
  const params = new URLSearchParams(query);
  const out: Record<string, string> = {};
  params.forEach((value, key) => {
    out[key] = value;
  });
  return out;
}

export function stringifySearch(search: Record<string, unknown>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(search)) {
    if (typeof value === "string") {
      if (value !== "") params.set(key, value);
      continue;
    }
    if (typeof value === "number" || typeof value === "boolean") {
      params.set(key, String(value));
    }
  }
  const qs = params.toString();
  return qs ? `?${qs}` : "";
}
