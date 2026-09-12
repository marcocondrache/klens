import { CrownIcon } from "lucide-react";
import { useNavigate } from "react-router";

import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { useBrokers, useCatalogHealth, useCluster } from "@/lib/api/queries";
import { clusterPath, useClusterName } from "@/lib/clusters";
import { formatBytes, formatNumber, formatRate, formatRelative } from "@/lib/format";
import type { Broker } from "@/lib/api/types";
import { createAppColumnHelper } from "@/lib/table";

const columnHelper = createAppColumnHelper<Broker>();

const columns = columnHelper.columns([
  columnHelper.accessor("id", {
    header: "ID",
    cell: ({ row }) => {
      const broker = row.original;

      return (
        <span className="flex items-center gap-2">
          <span className="numeric font-mono font-medium">{broker.id}</span>
          {broker.controller ? (
            <Pill tone="brand">
              <CrownIcon className="size-3" />
              controller
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
          <span className="font-mono text-sm">
            {broker.host}:{broker.port}
          </span>
          <CopyButton value={`${broker.host}:${broker.port}`} label="Copy address" />
        </span>
      );
    },
  }),
  columnHelper.accessor((broker) => broker.rack ?? "", {
    id: "rack",
    header: "Rack",
    cell: ({ row }) =>
      row.original.rack ? (
        <span className="font-mono text-sm">{row.original.rack}</span>
      ) : (
        <span className="text-muted-foreground">—</span>
      ),
  }),
  columnHelper.accessor("partitionCount", {
    id: "partitions",
    header: "Partitions",
    meta: { align: "right" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  columnHelper.accessor("leaderCount", {
    id: "leaders",
    header: "Leaders",
    meta: { align: "right" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  columnHelper.accessor("logDirSizeBytes", {
    id: "disk",
    header: "Log size",
    meta: { align: "right" },
    cell: ({ getValue }) => formatBytes(getValue()),
  }),
  columnHelper.accessor("bytesInPerSec", {
    id: "in",
    header: "Bytes in",
    meta: { align: "right" },
    cell: ({ getValue }) => formatRate(getValue()),
  }),
  columnHelper.accessor("bytesOutPerSec", {
    id: "out",
    header: "Bytes out",
    meta: { align: "right" },
    cell: ({ getValue }) => formatRate(getValue()),
  }),
]);

export function NodesPage() {
  const cluster = useClusterName();
  const navigate = useNavigate();
  const { data: brokers = [], isPending, isError, error } = useBrokers(cluster);
  const { data: info } = useCluster(cluster);
  const { data: health } = useCatalogHealth(cluster);
  const updatedAt = health?.updatedAt;

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Brokers"
        description={`${brokers.length} brokers · Kafka ${info?.version ?? "—"}${
          updatedAt ? ` · Updated ${formatRelative(updatedAt)}` : ""
        }`}
      />

      <DataTable
        columns={columns}
        data={brokers}
        getRowId={(broker) => String(broker.id)}
        loading={isPending}
        error={
          isError ? (error instanceof Error ? error.message : "Failed to load brokers.") : undefined
        }
        defaultSort={{ id: "id", direction: "asc" }}
        onRowClick={(broker) => navigate(clusterPath(cluster, "nodes", String(broker.id)))}
        fill
      />
    </div>
  );
}
