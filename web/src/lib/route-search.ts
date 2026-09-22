import { parseAsBoolean, parseAsString, parseAsStringLiteral } from "nuqs";

import { parseAsFilter } from "@/components/data-table/filters";
import type { GroupState } from "@/lib/api/types";

const term = parseAsString.withDefault("");

// Filter params: `a,b` matches any of the values, `!a,b` none of them.
export const TOPIC_POLICIES = ["delete", "compact"] as const;
export const TOPIC_HEALTH = ["under-replicated", "in-sync"] as const;
export const TOPIC_ACTIVITY = ["active", "idle"] as const;

export const topicsSearch = {
  q: term,
  internal: parseAsBoolean.withDefault(false),
  policy: parseAsFilter(TOPIC_POLICIES),
  health: parseAsFilter(TOPIC_HEALTH),
  activity: parseAsFilter(TOPIC_ACTIVITY),
};

export const GROUP_STATES = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "DEAD",
] as const satisfies readonly GroupState[];

export const GROUP_LAG = ["lagging", "caught-up"] as const;

export const groupsSearch = {
  q: term,
  state: parseAsFilter(GROUP_STATES),
  lag: parseAsFilter(GROUP_LAG),
};

export const schemasSearch = {
  q: term,
};

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

export const aclsSearch = {
  q: term,
  resource: parseAsFilter(ACL_RESOURCE_TYPES),
  operation: parseAsFilter(ACL_OPERATIONS),
  permission: parseAsFilter(ACL_PERMISSIONS),
  pattern: parseAsFilter(ACL_PATTERNS),
};

export const loginSearch = {
  error: parseAsString,
  from: parseAsStringLiteral(["callback"]),
};

export const topicDetailSearch = {
  tab: parseAsStringLiteral(["partitions", "groups", "config"]),
};

export const groupDetailSearch = {
  tab: parseAsStringLiteral(["offsets", "members"]).withDefault("offsets"),
};

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
