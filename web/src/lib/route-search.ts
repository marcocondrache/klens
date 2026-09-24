import * as z from "zod/mini";

import { filterParam } from "@/components/data-table/filters";
import type { GroupState } from "@/lib/api/types";

const term = z.catch(z._default(z.string(), ""), "");

const flag = z.catch(
  z._default(
    z.union([
      z.boolean(),
      z.pipe(
        z.enum(["true", "false"]),
        z.transform((value) => value === "true"),
      ),
    ]),
    false,
  ),
  false,
);

function oneOf<const T extends readonly [string, ...string[]]>(values: T) {
  return z.catch(z.optional(z.enum(values)), undefined);
}

// Filter params: `a,b` matches any of the values, `!a,b` none of them.
function filter(allowed: readonly string[]) {
  const param = z.pipe(
    z.string(),
    z.transform((raw) => filterParam(allowed, raw)),
  );
  return z.catch(z.optional(param), undefined);
}

export const TOPIC_POLICIES = ["delete", "compact"] as const;
export const TOPIC_HEALTH = ["under-replicated", "in-sync"] as const;
export const TOPIC_ACTIVITY = ["active", "idle"] as const;

export const topicsSearch = z.object({
  q: term,
  internal: flag,
  policy: filter(TOPIC_POLICIES),
  health: filter(TOPIC_HEALTH),
  activity: filter(TOPIC_ACTIVITY),
});

export type TopicsSearch = z.output<typeof topicsSearch>;
export type TopicFilter = Exclude<keyof TopicsSearch, "q" | "internal">;

export const GROUP_STATES = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "DEAD",
] as const satisfies readonly GroupState[];

export const GROUP_LAG = ["lagging", "caught-up"] as const;

export const groupsSearch = z.object({
  q: term,
  state: filter(GROUP_STATES),
  lag: filter(GROUP_LAG),
});

export type GroupsSearch = z.output<typeof groupsSearch>;
export type GroupFilter = Exclude<keyof GroupsSearch, "q">;

const version = z.catch(
  z.optional(
    z.union([
      z.int().check(z.positive()),
      z.pipe(
        z.string().check(z.regex(/^[1-9]\d*$/)),
        z.transform((value) => Number(value)),
      ),
    ]),
  ),
  undefined,
);

export const schemasSearch = z.object({
  q: term,
  subject: z.catch(z.optional(z.string()), undefined),
  version,
});

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

export const aclsSearch = z.object({
  q: term,
  resource: filter(ACL_RESOURCE_TYPES),
  operation: filter(ACL_OPERATIONS),
  permission: filter(ACL_PERMISSIONS),
  pattern: filter(ACL_PATTERNS),
});

export type AclsSearch = z.output<typeof aclsSearch>;
export type AclFilter = Exclude<keyof AclsSearch, "q">;

export const loginSearch = z.object({
  error: z.catch(z.optional(z.string()), undefined),
});

export type LoginSearch = z.output<typeof loginSearch>;

const topicTabParam = oneOf(["partitions", "groups", "config"]);

export const topicDetailSearch = z.object({
  tab: topicTabParam,
});

export function topicTab(value: unknown) {
  return z.parse(topicTabParam, value);
}

const groupTabParam = z.catch(z._default(z.enum(["offsets", "members"]), "offsets"), "offsets");

export const groupDetailSearch = z.object({
  tab: groupTabParam,
});

export function groupTab(value: unknown) {
  return z.parse(groupTabParam, value);
}

export function searchDefaults<T extends z.ZodMiniType>(schema: T): z.output<T> {
  return z.parse(schema, {});
}

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
