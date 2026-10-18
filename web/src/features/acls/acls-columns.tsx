import { createColumnHelper } from "@tanstack/react-table";
import { Trash2Icon } from "lucide-react";

import type { DataTableFeatures } from "@/components/data-table/features";
import { IconButton } from "@/components/icon-button";
import { Pill, StatusLabel } from "@/components/status";
import type { Acl } from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";

const columnHelper = createColumnHelper<DataTableFeatures, Acl>();

export const aclColumns = columnHelper.columns([
  columnHelper.accessor("resourceType", {
    header: "Resource",
    size: 144,
    cell: ({ getValue }) => <Pill>{formatEnumLabel(getValue())}</Pill>,
  }),
  columnHelper.accessor("resourceName", {
    header: "Name",
    meta: { grow: true },
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("patternType", {
    header: "Pattern",
    size: 96,
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{formatEnumLabel(getValue())}</span>
    ),
  }),
  columnHelper.accessor("principal", {
    header: "Principal",
    meta: { grow: true },
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("host", {
    header: "Host",
    size: 128,
    cell: ({ getValue }) => <span className="font-mono text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("operation", {
    header: "Operation",
    size: 160,
    cell: ({ getValue }) => formatEnumLabel(getValue()),
  }),
  columnHelper.accessor("permission", {
    header: "Permission",
    size: 112,
    cell: ({ getValue }) => (
      <StatusLabel tone={getValue() === "DENY" ? "error" : "ok"}>
        {formatEnumLabel(getValue())}
      </StatusLabel>
    ),
  }),
]);

export function aclDeleteColumn(onDelete: (acl: Acl) => void) {
  return columnHelper.display({
    id: "delete",
    size: 56,
    meta: { align: "right" },
    cell: ({ row }) => {
      const acl = row.original;

      return (
        <IconButton
          label={`Delete ACL that ${acl.permission === "ALLOW" ? "allows" : "denies"} ${acl.principal} ${formatEnumLabel(acl.operation)} on ${acl.resourceName}`}
          tooltip="Delete"
          reveal
          onClick={() => onDelete(acl)}
        >
          <Trash2Icon />
        </IconButton>
      );
    },
  });
}

function lengthPrefixed(parts: string[]): string {
  return parts.map((part) => `${part.length}:${part}`).join("");
}

export function aclRowId(acl: Acl): string {
  return lengthPrefixed([
    acl.resourceType,
    acl.resourceName,
    acl.patternType,
    acl.principal,
    acl.host,
    acl.operation,
    acl.permission,
  ]);
}
