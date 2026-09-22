import { parseAsBoolean, parseAsString, parseAsStringLiteral } from "nuqs";

import type { GroupState } from "@/lib/api/types";

const term = parseAsString.withDefault("");

export const topicsSearch = {
  q: term,
  internal: parseAsBoolean.withDefault(false),
  policy: parseAsStringLiteral(["all", "delete", "compact"]).withDefault("all"),
};

export const GROUP_STATES = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "DEAD",
] as const satisfies readonly GroupState[];

export const groupsSearch = {
  q: term,
  state: parseAsStringLiteral(["all", ...GROUP_STATES]).withDefault("all"),
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

export const aclsSearch = {
  q: term,
  resource: parseAsStringLiteral(["all", ...ACL_RESOURCE_TYPES]).withDefault("all"),
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
