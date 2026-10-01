import { Fragment } from "react";

import { Pill } from "@/components/status";
import type { ClientQuota, QuotaEntity, QuotaEntityType } from "@/lib/api/types";

export const ENTITY_LABEL: Record<QuotaEntityType, string> = {
  USER: "User",
  CLIENT_ID: "Client ID",
  IP: "IP",
};

export function isDefault(quota: ClientQuota) {
  return quota.entity.some((part) => part.name === null);
}

export function entityKey(quota: ClientQuota) {
  return quota.entity
    .map((part) => (part.name === null ? part.entityType : `${part.entityType}=${part.name}`))
    .join("\u0000");
}

export function EntityCell({ parts }: { parts: QuotaEntity[] }) {
  return (
    <span className="flex min-w-0 items-center gap-2">
      {parts.map((part, index) => (
        <Fragment key={part.entityType}>
          {index > 0 ? <span className="text-muted-foreground/60">+</span> : null}
          <span className="flex min-w-0 items-center gap-1.5">
            <Pill className="shrink-0">{ENTITY_LABEL[part.entityType]}</Pill>
            {part.name === null ? (
              <span className="text-muted-foreground italic">default</span>
            ) : (
              <span className="truncate font-mono">{part.name}</span>
            )}
          </span>
        </Fragment>
      ))}
    </span>
  );
}
