import { CrownIcon } from "lucide-react"
import { useNavigate } from "react-router"

import { CopyButton } from "@/components/copy-button"
import { DataTable, type Column } from "@/components/data-table"
import { PageHeader } from "@/components/page-header"
import { Pill } from "@/components/status"
import { useBrokers, useCluster } from "@/lib/api/queries"
import { clusterPath, useClusterName } from "@/lib/clusters"
import { formatBytes, formatNumber, formatRate } from "@/lib/format"
import type { Broker } from "@/lib/api/types"

export function NodesPage() {
  const cluster = useClusterName()
  const navigate = useNavigate()
  const { data: brokers = [], isPending } = useBrokers(cluster)
  const { data: info } = useCluster(cluster)

  const columns: Array<Column<Broker>> = [
    {
      id: "id",
      header: "ID",
      sortValue: (broker) => broker.id,
      cell: (broker) => (
        <span className="flex items-center gap-2">
          <span className="numeric font-mono font-medium">{broker.id}</span>
          {broker.controller ? (
            <Pill tone="brand">
              <CrownIcon className="size-3" />
              controller
            </Pill>
          ) : null}
        </span>
      ),
    },
    {
      id: "host",
      header: "Host",
      sortValue: (broker) => broker.host,
      cell: (broker) => (
        <span className="flex items-center gap-1">
          <span className="font-mono text-[0.8rem]">
            {broker.host}:{broker.port}
          </span>
          <CopyButton value={`${broker.host}:${broker.port}`} label="Copy address" />
        </span>
      ),
    },
    {
      id: "rack",
      header: "Rack",
      sortValue: (broker) => broker.rack ?? "",
      cell: (broker) =>
        broker.rack ? (
          <span className="font-mono text-xs text-muted-foreground">{broker.rack}</span>
        ) : (
          <span className="text-muted-foreground">—</span>
        ),
    },
    {
      id: "partitions",
      header: "Partitions",
      align: "right",
      sortValue: (broker) => broker.partitionCount,
      cell: (broker) => formatNumber(broker.partitionCount),
    },
    {
      id: "leaders",
      header: "Leaders",
      align: "right",
      sortValue: (broker) => broker.leaderCount,
      cell: (broker) => formatNumber(broker.leaderCount),
    },
    {
      id: "disk",
      header: "Log size",
      align: "right",
      sortValue: (broker) => broker.logDirSizeBytes,
      cell: (broker) => formatBytes(broker.logDirSizeBytes),
    },
    {
      id: "in",
      header: "Bytes in",
      align: "right",
      sortValue: (broker) => broker.bytesInPerSec,
      cell: (broker) => formatRate(broker.bytesInPerSec),
    },
    {
      id: "out",
      header: "Bytes out",
      align: "right",
      sortValue: (broker) => broker.bytesOutPerSec,
      cell: (broker) => formatRate(broker.bytesOutPerSec),
    },
  ]

  return (
    <div className="space-y-5">
      <PageHeader
        title="Nodes"
        description={`${brokers.length} brokers · Kafka ${info?.version ?? "—"}`}
      />

      <DataTable
        columns={columns}
        rows={brokers}
        rowKey={(broker) => String(broker.id)}
        loading={isPending}
        defaultSort={{ id: "id", direction: "asc" }}
        onRowClick={(broker) => navigate(clusterPath(cluster, "nodes", String(broker.id)))}
      />
    </div>
  )
}
