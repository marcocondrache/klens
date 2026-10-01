import { AsteriskIcon, UserRoundIcon } from "lucide-react";

import type { FilterField } from "@/components/data-table/filters";
import type { ClientQuota } from "@/lib/api/types";

import { QUOTA_ENTITY_TYPES, type QuotaFilter } from "./search";
import { ENTITY_LABEL, isDefault } from "./quota-entity";

export const QUOTA_FILTERS: Array<FilterField<ClientQuota, QuotaFilter>> = [
  {
    id: "entity",
    label: "Entity",
    plural: "entities",
    icon: UserRoundIcon,
    options: QUOTA_ENTITY_TYPES.map((value) => ({ value, label: ENTITY_LABEL[value] })),
    accessor: (quota) => quota.entity.map((part) => part.entityType),
  },
  {
    id: "scope",
    label: "Scope",
    plural: "scopes",
    icon: AsteriskIcon,
    options: [
      { value: "named", label: "Named" },
      { value: "default", label: "Default" },
    ],
    accessor: (quota) => (isDefault(quota) ? "default" : "named"),
  },
];

export function quotaMatches(quota: ClientQuota, needle: string) {
  return quota.entity
    .map((part) => `${ENTITY_LABEL[part.entityType]} ${part.name ?? "default"}`)
    .join(" ")
    .toLowerCase()
    .includes(needle);
}
