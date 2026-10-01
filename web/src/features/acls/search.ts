import * as z from "zod/mini";

import { filter, term } from "@/lib/route-search";

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
const ACL_PERMISSIONS = ["ALLOW", "DENY"] as const;
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
