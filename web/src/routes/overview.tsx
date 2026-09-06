import {
  ActivityIcon,
  AlertTriangleIcon,
  ArrowDownRightIcon,
  ArrowUpRightIcon,
  DatabaseIcon,
  HardDriveIcon,
  LayersIcon,
  NetworkIcon,
} from "lucide-react"
import { Link, useNavigate } from "react-router"

import { Button } from "@/components/ui/button"
import { Skeleton } from "@/components/ui/skeleton"
import { CopyButton } from "@/components/copy-button"
import { DataTable, type Column } from "@/components/data-table"
import { PageHeader, SectionTitle } from "@/components/page-header"
import { Sparkline, ThroughputChart } from "@/components/charts"
import { Stat, StatGrid } from "@/components/stat"
import {
  ClusterStatusBadge,
  EnvironmentBadge,
  GroupStateBadge,
  Pill,
  StatusDot,
} from "@/components/status"
import { lagTone } from "@/lib/tone"
import {
  useBrokers,
  useCluster,
  useClusterThroughput,
  useConsumerGroups,
  useTopics,
} from "@/lib/api/queries"
import { clusterPath, useClusterName } from "@/lib/clusters"
import { formatBytes, formatCount, formatNumber, formatRate } from "@/lib/format"
import type { ConsumerGroup, Topic } from "@/lib/api/types"

export function OverviewPage() {
  const cluster = useClusterName()
  const navigate = useNavigate()

  const { data, isPending } = useCluster(cluster)
  const { data: brokers = [] } = useBrokers(cluster)
  const { data: topics = [] } = useTopics(cluster)
  const { data: groups = [] } = useConsumerGroups(cluster)
  const { data: throughput = [], isPending: throughputPending } = useClusterThroughput(cluster)

  const internalTopics = topics.filter((topic) => topic.internal).length
  const totalLag = groups.reduce((sum, group) => sum + group.lag, 0)

  const busiest = [...topics]
    .filter((topic) => !topic.internal)
    .sort((left, right) => right.messagesPerSec - left.messagesPerSec)
    .slice(0, 8)

  const lagging = [...groups].sort((left, right) => right.lag - left.lag).slice(0, 6)

  const topicColumns: Array<Column<Topic>> = [
    {
      id: "name",
      header: "Topic",
      cell: (topic) => <span className="font-mono text-[0.8rem]">{topic.name}</span>,
    },
    {
      id: "rate",
      header: "Msg/s",
      align: "right",
      cell: (topic) => formatCount(topic.messagesPerSec),
    },
    {
      id: "in",
      header: "Bytes in",
      align: "right",
      cell: (topic) => formatRate(topic.bytesInPerSec),
    },
    {
      id: "size",
      header: "Size",
      align: "right",
      cell: (topic) => formatBytes(topic.sizeBytes),
    },
  ]

  return (
    <div className="space-y-5">
      <PageHeader
        title={data?.label ?? cluster}
        description={
          data ? (
            <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
              <span className="font-mono text-xs">{data.bootstrapServers.join(", ")}</span>
              <CopyButton value={data.bootstrapServers.join(",")} label="Copy bootstrap servers" />
              <span className="text-border">|</span>
              <span className="font-mono text-xs">{data.clusterId}</span>
            </span>
          ) : (
            <Skeleton className="h-4 w-72" />
          )
        }
        badges={
          data ? (
            <>
              <ClusterStatusBadge status={data.status} />
              <EnvironmentBadge environment={data.environment} />
              <Pill>{data.securityProtocol}</Pill>
              <Pill>Kafka {data.version}</Pill>
            </>
          ) : null
        }
      />

      <StatGrid>
        <Stat
          label="Brokers"
          value={data?.brokerCount ?? 0}
          hint={`controller · broker ${brokers.find((broker) => broker.controller)?.id ?? "—"}`}
          icon={<HardDriveIcon />}
          loading={isPending}
        />
        <Stat
          label="Topics"
          value={data?.topicCount ?? 0}
          hint={`${internalTopics} internal`}
          icon={<LayersIcon />}
          loading={isPending}
        />
        <Stat
          label="Partitions"
          value={formatNumber(data?.partitionCount ?? 0)}
          hint={
            data?.underReplicatedPartitions
              ? `${data.underReplicatedPartitions} under-replicated`
              : "all in sync"
          }
          icon={<NetworkIcon />}
          loading={isPending}
          accent={Boolean(data?.underReplicatedPartitions)}
        />
        <Stat
          label="Storage"
          value={formatBytes(data?.sizeBytes ?? 0)}
          hint={`${formatCount(data?.messageCount ?? 0)} messages`}
          icon={<DatabaseIcon />}
          loading={isPending}
        />
      </StatGrid>

      <div className="rounded-xl border bg-card">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b px-4 py-3">
          <SectionTitle title="Throughput" description="Last 48 minutes across all brokers" />
          <div className="flex items-center gap-4 text-xs">
            <span className="flex items-center gap-1.5">
              <span className="size-2 rounded-full bg-chart-1" />
              <ArrowUpRightIcon className="size-3 text-muted-foreground" />
              <span className="numeric font-mono">{formatRate(data?.bytesInPerSec ?? 0)}</span>
            </span>
            <span className="flex items-center gap-1.5">
              <span className="size-2 rounded-full bg-chart-2" />
              <ArrowDownRightIcon className="size-3 text-muted-foreground" />
              <span className="numeric font-mono">{formatRate(data?.bytesOutPerSec ?? 0)}</span>
            </span>
          </div>
        </div>
        <div className="p-2">
          {throughputPending ? (
            <Skeleton className="m-2 h-52" />
          ) : (
            <ThroughputChart data={throughput} />
          )}
        </div>
      </div>

      <div className="grid gap-5 xl:grid-cols-[1.55fr_1fr]">
        <div className="space-y-3">
          <SectionTitle
            title="Busiest topics"
            description="Ranked by produce rate"
            actions={
              <Button variant="ghost" size="xs" render={<Link to={clusterPath(cluster, "topics")} />}>
                View all
              </Button>
            }
          />
          <DataTable
            columns={topicColumns}
            rows={busiest}
            rowKey={(topic) => topic.name}
            loading={isPending}
            pageSize={8}
            onRowClick={(topic) => navigate(clusterPath(cluster, "topics", topic.name))}
          />
        </div>

        <div className="space-y-3">
          <SectionTitle
            title="Consumer lag"
            description={`${formatCount(totalLag)} messages behind in total`}
            actions={
              <Button variant="ghost" size="xs" render={<Link to={clusterPath(cluster, "groups")} />}>
                View all
              </Button>
            }
          />

          <div className="divide-y overflow-hidden rounded-xl border bg-card">
            {isPending ? (
              <div className="space-y-3 p-4">
                {Array.from({ length: 4 }, (_, index) => (
                  <Skeleton key={index} className="h-8" />
                ))}
              </div>
            ) : (
              lagging.map((group: ConsumerGroup) => (
                <Link
                  key={group.id}
                  to={clusterPath(cluster, "groups", group.id)}
                  className="flex items-center justify-between gap-3 px-4 py-2.5 transition-colors hover:bg-muted/40"
                >
                  <div className="min-w-0 space-y-0.5">
                    <p className="truncate font-mono text-[0.8rem]">{group.id}</p>
                    <p className="text-xs text-muted-foreground">
                      {group.members.length} members · {group.topics.length} topics
                    </p>
                  </div>
                  <div className="flex shrink-0 items-center gap-2">
                    <GroupStateBadge state={group.state} />
                    <Pill tone={lagTone(group.lag)} className="numeric font-mono">
                      {formatCount(group.lag)}
                    </Pill>
                  </div>
                </Link>
              ))
            )}
          </div>

          <div className="space-y-3 rounded-xl border bg-card p-4">
            <SectionTitle title="Health" />
            <div className="grid gap-2 text-sm">
              <HealthRow
                label="Under-replicated partitions"
                value={data?.underReplicatedPartitions ?? 0}
                tone={data?.underReplicatedPartitions ? "warn" : "ok"}
              />
              <HealthRow
                label="Offline partitions"
                value={data?.offlinePartitions ?? 0}
                tone={data?.offlinePartitions ? "error" : "ok"}
              />
              <HealthRow
                label="Rebalancing groups"
                value={groups.filter((group) => group.state.includes("Rebalance")).length}
                tone={groups.some((group) => group.state.includes("Rebalance")) ? "warn" : "ok"}
              />
              <HealthRow
                label="Empty groups"
                value={groups.filter((group) => group.state === "Empty").length}
                tone="idle"
              />
            </div>
          </div>

          <div className="space-y-2 rounded-xl border bg-card p-4">
            <SectionTitle title="Cluster ingest" description="Bytes in per broker" />
            <Sparkline data={throughput} className="h-12" />
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span className="flex items-center gap-1.5">
                <ActivityIcon className="size-3" />
                live
              </span>
              <span className="numeric font-mono">
                {formatRate((data?.bytesInPerSec ?? 0) / Math.max(1, data?.brokerCount ?? 1))} avg
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function HealthRow({
  label,
  value,
  tone,
}: {
  label: string
  value: number
  tone: "ok" | "warn" | "error" | "idle"
}) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="flex items-center gap-2 text-muted-foreground">
        {tone === "ok" ? (
          <StatusDot tone="ok" />
        ) : tone === "idle" ? (
          <StatusDot tone="idle" />
        ) : (
          <AlertTriangleIcon
            className={tone === "warn" ? "size-3.5 text-amber-500" : "size-3.5 text-rose-500"}
          />
        )}
        {label}
      </span>
      <span className="numeric font-mono">{formatNumber(value)}</span>
    </div>
  )
}
