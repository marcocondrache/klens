import { CrownIcon } from "lucide-react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { CopyButton } from "@/components/copy-button";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { useNow } from "@/hooks/use-now";
import { useBrokerRows, useClusterHealth } from "@/lib/api/catalog";
import { laneCaption, useClusterName } from "@/lib/clusters";
import { formatNumber } from "@/lib/format";
import type { BrokerRow } from "@/lib/api/types";

export const Route = createFileRoute("/cluster/$cluster/nodes")({
  component: NodesPage,
});

const columnHelper = createColumnHelper<DataTableFeatures, BrokerRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("id", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="ID" />,
    meta: { label: "ID" },
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
    header: ({ column }) => <DataTableColumnHeader column={column} title="Host" />,
    meta: { label: "Host" },
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
    header: ({ column }) => <DataTableColumnHeader column={column} title="Rack" />,
    meta: { label: "Rack" },
    cell: ({ row }) =>
      row.original.rack ? (
        <span className="font-mono text-sm">{row.original.rack}</span>
      ) : (
        <span className="text-muted-foreground">—</span>
      ),
  }),
  columnHelper.accessor("partitionCount", {
    id: "partitions",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Partitions" className="justify-end" />
    ),
    meta: { align: "right", label: "Partitions" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  columnHelper.accessor("leaderCount", {
    id: "leaders",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Leaders" className="justify-end" />
    ),
    meta: { align: "right", label: "Leaders" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
]);

function NodesPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { data: brokers = [], isPending, isError, error } = useBrokerRows(cluster);
  const { data: health } = useClusterHealth(cluster);
  const now = useNow();
  const caption = laneCaption(health?.topology, now);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Brokers"
        description={`${brokers.length} brokers${caption ? ` · ${caption}` : ""}`}
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
        onRowClick={(broker) => {
          void navigate({
            to: "/cluster/$cluster/nodes/$id",
            params: { cluster, id: String(broker.id) },
          });
        }}
        fill
      />
    </div>
  );
}
