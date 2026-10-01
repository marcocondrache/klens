import { HardDriveIcon } from "lucide-react";
import { createColumnHelper } from "@tanstack/react-table";

import type { DataTableFeatures } from "@/components/data-table/features";
import { Pill, UsedValue } from "@/components/status";
import { formatBytes, formatNumber } from "@/lib/format";
import { usedShare } from "@/lib/storage";
import type { LogDir } from "@/lib/api/types";

const columnHelper = createColumnHelper<DataTableFeatures, LogDir>();

function optionalBytes(bytes: number | null) {
  if (bytes === null) {
    return (
      <span className="text-muted-foreground/60" title="The broker does not report its volume size">
        —
      </span>
    );
  }
  return formatBytes(bytes);
}

export const logDirColumns = columnHelper.columns([
  columnHelper.accessor("path", {
    header: "Path",
    cell: ({ row }) => {
      const dir = row.original;

      return (
        <span className="flex items-center gap-2">
          <span className="truncate font-mono">{dir.path}</span>
          {dir.error ? (
            <Pill tone="error" className="shrink-0" title={dir.error}>
              <HardDriveIcon />
              offline
            </Pill>
          ) : null}
          {dir.cordoned ? (
            <Pill tone="warn" className="shrink-0" title="Takes no new partitions">
              cordoned
            </Pill>
          ) : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("replicaCount", {
    id: "partitions",
    header: "Partitions",
    meta: { align: "right", width: "7rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  columnHelper.accessor("sizeBytes", {
    id: "size",
    header: "Size",
    meta: { align: "right", width: "7rem" },
    cell: ({ getValue }) => formatBytes(getValue()),
  }),
  columnHelper.accessor((dir) => dir.usableBytes ?? -1, {
    id: "free",
    header: "Free",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => optionalBytes(row.original.usableBytes),
  }),
  columnHelper.accessor((dir) => dir.totalBytes ?? -1, {
    id: "capacity",
    header: "Capacity",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => optionalBytes(row.original.totalBytes),
  }),
  columnHelper.accessor((dir) => usedShare(dir) ?? -1, {
    id: "used",
    header: "Used",
    meta: { align: "right", width: "6rem" },
    cell: ({ row }) => <UsedValue used={usedShare(row.original)} />,
  }),
]);
