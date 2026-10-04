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
    meta: { width: "9rem" },
    cell: ({ getValue }) => <Pill>{formatEnumLabel(getValue())}</Pill>,
  }),
  columnHelper.accessor("resourceName", {
    header: "Name",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("patternType", {
    header: "Pattern",
    meta: { width: "6rem" },
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{formatEnumLabel(getValue())}</span>
    ),
  }),
  columnHelper.accessor("principal", {
    header: "Principal",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("host", {
    header: "Host",
    meta: { width: "8rem" },
    cell: ({ getValue }) => <span className="font-mono text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("operation", {
    header: "Operation",
    meta: { width: "10rem" },
    cell: ({ getValue }) => formatEnumLabel(getValue()),
  }),
  columnHelper.accessor("permission", {
    header: "Permission",
    meta: { width: "7rem" },
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
    enableResizing: false,
    meta: { align: "right", width: "3.5rem" },
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
