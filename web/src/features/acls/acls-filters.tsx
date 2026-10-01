import { AsteriskIcon, BoxIcon, ShieldIcon, ZapIcon } from "lucide-react";

import type { FilterField } from "@/components/data-table/filters";
import { StatusDot } from "@/components/status";
import type { Acl } from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";

import { ACL_OPERATIONS, ACL_PATTERNS, ACL_RESOURCE_TYPES, type AclFilter } from "./search";

function enumOptions(values: readonly string[]) {
  return values.map((value) => ({ value, label: formatEnumLabel(value) }));
}

export const ACL_FILTERS: Array<FilterField<Acl, AclFilter>> = [
  {
    id: "resource",
    label: "Resource",
    plural: "resources",
    icon: BoxIcon,
    options: enumOptions(ACL_RESOURCE_TYPES),
    accessor: (acl) => acl.resourceType,
  },
  {
    id: "operation",
    label: "Operation",
    plural: "operations",
    icon: ZapIcon,
    options: enumOptions(ACL_OPERATIONS),
    accessor: (acl) => acl.operation,
  },
  {
    id: "permission",
    label: "Permission",
    plural: "permissions",
    icon: ShieldIcon,
    options: [
      { value: "ALLOW", label: "Allow", icon: <StatusDot tone="ok" /> },
      { value: "DENY", label: "Deny", icon: <StatusDot tone="error" /> },
    ],
    accessor: (acl) => acl.permission,
  },
  {
    id: "pattern",
    label: "Pattern",
    plural: "patterns",
    icon: AsteriskIcon,
    options: enumOptions(ACL_PATTERNS),
    accessor: (acl) => acl.patternType,
  },
];

export function aclMatches(acl: Acl, needle: string) {
  return [
    acl.resourceName,
    acl.principal,
    acl.host,
    acl.resourceType,
    acl.patternType,
    acl.operation,
    acl.permission,
  ]
    .join(" ")
    .toLowerCase()
    .includes(needle);
}
