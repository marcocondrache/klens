import { CrownIcon, HardDriveIcon } from "lucide-react";
import { createColumnHelper } from "@tanstack/react-table";

import { CopyButton } from "@/components/copy-button";
import type { DataTableFeatures } from "@/components/data-table/features";
import { Pill, SizeValue, UsedValue } from "@/components/status";
import { formatBytes, formatNumber } from "@/lib/format";
import { fullestDir } from "@/lib/storage";
import type { BrokerRow } from "@/lib/api/types";

const columnHelper = createColumnHelper<DataTableFeatures, BrokerRow>();

export const brokerColumns = columnHelper.columns([
  columnHelper.accessor("id", {
    header: "ID",
    meta: { width: "10rem" },
    cell: ({ row }) => {
      const broker = row.original;

      return (
        <span className="flex items-center gap-2">
          <span className="numeric font-medium">{broker.id}</span>
          {broker.controller ? (
            <Pill tone="brand">
              <CrownIcon />
              controller
            </Pill>
          ) : null}
          {broker.logDirs.some((dir) => dir.error != null) ? (
            <Pill tone="error">
              <HardDriveIcon />
              log dir offline
            </Pill>
          ) : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("host", {
    header: "Host",
    cell: ({ row }) => {
      const broker = row.original;

      return (
        <span className="flex items-center gap-1">
          <span className="truncate font-mono">
            {broker.host}
            <span className="text-muted-foreground">:{broker.port}</span>
          </span>
          <CopyButton value={`${broker.host}:${broker.port}`} label="Copy address" reveal />
        </span>
      );
    },
  }),
  columnHelper.accessor((broker) => broker.rack ?? "", {
    id: "rack",
    header: "Rack",
    meta: { width: "8rem" },
    cell: ({ row }) =>
      row.original.rack ? (
        <span className="font-mono text-muted-foreground">{row.original.rack}</span>
      ) : (
        <span className="text-muted-foreground/60">—</span>
      ),
  }),
  columnHelper.accessor("partitionCount", {
    id: "partitions",
    header: "Partitions",
    meta: { align: "right", width: "7rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  columnHelper.accessor("leaderCount", {
    id: "leaders",
    header: "Leaders",
    meta: { align: "right", width: "6.5rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  columnHelper.accessor((broker) => broker.sizeBytes ?? -1, {
    id: "size",
    header: "Size",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => <SizeValue bytes={row.original.sizeBytes} />,
  }),
  columnHelper.accessor((broker) => fullestDir(broker.logDirs)?.used ?? -1, {
    id: "disk",
    header: "Disk used",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => {
      if (row.original.logDirs.length === 0) {
        return <SizeValue bytes={null} />;
      }
      const fullest = fullestDir(row.original.logDirs);
      return (
        <UsedValue
          used={fullest?.used ?? null}
          title={
            fullest
              ? `Fullest log dir ${fullest.dir.path}: ${formatBytes(fullest.dir.usableBytes ?? 0)} free of ${formatBytes(fullest.dir.totalBytes ?? 0)}`
              : undefined
          }
        />
      );
    },
  }),
]);
