import { useMemo } from "react"
import { AlertTriangleIcon, DatabaseIcon, GaugeIcon, NetworkIcon } from "lucide-react"
import { useNavigate, useParams, useSearchParams } from "react-router"

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { ConfigTable } from "@/components/config-table"
import { CopyButton } from "@/components/copy-button"
import { DataTable, type Column } from "@/components/data-table"
import { PageHeader } from "@/components/page-header"
import { RecordBrowser } from "@/components/record-browser"
import { Sparkline } from "@/components/charts"
import { Stat, StatGrid } from "@/components/stat"
import { GroupStateBadge, Pill } from "@/components/status"
import { lagTone } from "@/lib/tone"
import {
  useConsumerGroups,
  useTopic,
  useTopicConfigs,
  useTopicThroughput,
} from "@/lib/api/queries"
import { clusterPath, useClusterName } from "@/lib/clusters"
import {
  formatBytes,
  formatCleanupPolicy,
  formatCount,
  formatDuration,
  formatNumber,
  formatThroughput,
  isCompactCleanup,
} from "@/lib/format"
import type { ConsumerGroup, Partition } from "@/lib/api/types"

const TABS = ["data", "partitions", "groups", "config"]

export function TopicPage() {
  const cluster = useClusterName()
  const navigate = useNavigate()
  const { topic: topicParam } = useParams<{ topic: string }>()
  const topicName = decodeURIComponent(topicParam ?? "")
  const [params, setParams] = useSearchParams()

  const tab = TABS.includes(params.get("tab") ?? "") ? params.get("tab")! : "data"

  const { data: topic, isPending, isError } = useTopic(cluster, topicName)
  const { data: configs = [], isPending: configsPending } = useTopicConfigs(cluster, topicName)
  const { data: throughput = [] } = useTopicThroughput(cluster, topicName)
  const { data: groups = [] } = useConsumerGroups(cluster)

  const consuming = useMemo(
    () => groups.filter((group) => group.topics.includes(topicName)),
    [groups, topicName],
  )

  function selectTab(value: string) {
    const next = new URLSearchParams(params)
    if (value === "data") {
      next.delete("tab")
    } else {
      next.set("tab", value)
    }
    setParams(next, { replace: true })
  }

  if (isError) {
    return (
      <PageHeader
        title={topicName}
        mono
        description="This topic does not exist in the selected cluster."
      />
    )
  }

  const partitionColumns: Array<Column<Partition>> = [
    {
      id: "id",
      header: "Partition",
      align: "right",
      sortValue: (partition) => partition.id,
      cell: (partition) => <span className="numeric font-mono">{partition.id}</span>,
    },
    {
      id: "leader",
      header: "Leader",
      align: "right",
      sortValue: (partition) => partition.leader,
      cell: (partition) => <span className="numeric font-mono">{partition.leader}</span>,
    },
    {
      id: "replicas",
      header: "Replicas",
      cell: (partition) => (
        <span className="flex flex-wrap gap-1">
          {partition.replicas.map((replica) => (
            <Pill
              key={replica}
              tone={partition.isr.includes(replica) ? "idle" : "error"}
              className="numeric font-mono"
            >
              {replica}
            </Pill>
          ))}
        </span>
      ),
    },
    {
      id: "isr",
      header: "In sync",
      align: "right",
      sortValue: (partition) => partition.isr.length,
      cell: (partition) => (
        <span
          className={
            partition.isr.length < partition.replicas.length
              ? "numeric font-mono text-amber-500"
              : "numeric font-mono"
          }
        >
          {partition.isr.length}/{partition.replicas.length}
        </span>
      ),
    },
    {
      id: "low",
      header: "Low offset",
      align: "right",
      sortValue: (partition) => partition.lowWatermark,
      cell: (partition) => formatNumber(partition.lowWatermark),
    },
    {
      id: "high",
      header: "High offset",
      align: "right",
      sortValue: (partition) => partition.highWatermark,
      cell: (partition) => formatNumber(partition.highWatermark),
    },
    {
      id: "messages",
      header: "Messages",
      align: "right",
      sortValue: (partition) => partition.highWatermark - partition.lowWatermark,
      cell: (partition) => formatNumber(partition.highWatermark - partition.lowWatermark),
    },
    {
      id: "size",
      header: "Size",
      align: "right",
      sortValue: (partition) => partition.sizeBytes,
      cell: (partition) => formatBytes(partition.sizeBytes),
    },
  ]

  const groupColumns: Array<Column<ConsumerGroup>> = [
    {
      id: "id",
      header: "Group",
      sortValue: (group) => group.id,
      cell: (group) => <span className="font-mono text-[0.8rem]">{group.id}</span>,
    },
    {
      id: "state",
      header: "State",
      sortValue: (group) => group.state,
      cell: (group) => <GroupStateBadge state={group.state} />,
    },
    {
      id: "members",
      header: "Members",
      align: "right",
      sortValue: (group) => group.members.length,
      cell: (group) => group.members.length,
    },
    {
      id: "lag",
      header: "Lag on this topic",
      align: "right",
      sortValue: (group) =>
        group.offsets
          .filter((offset) => offset.topic === topicName)
          .reduce((sum, offset) => sum + offset.lag, 0),
      cell: (group) => {
        const lag = group.offsets
          .filter((offset) => offset.topic === topicName)
          .reduce((sum, offset) => sum + offset.lag, 0)

        return (
          <Pill tone={lagTone(lag)} className="numeric font-mono">
            {formatNumber(lag)}
          </Pill>
        )
      },
    },
  ]

  return (
    <div className="space-y-5">
      <PageHeader
        title={
          <span className="flex items-center gap-1">
            {topicName}
            <CopyButton value={topicName} label="Copy topic name" size="icon-sm" />
          </span>
        }
        mono
        badges={
          topic ? (
            <>
              {topic.internal ? <Pill>internal</Pill> : null}
              <Pill tone={isCompactCleanup(topic.cleanupPolicy) ? "brand" : "idle"}>
                {formatCleanupPolicy(topic.cleanupPolicy)}
              </Pill>
              <Pill>RF {topic.replicationFactor}</Pill>
              {topic.underReplicated ? (
                <Pill tone="warn">
                  <AlertTriangleIcon className="size-3" />
                  under-replicated
                </Pill>
              ) : null}
            </>
          ) : null
        }
        description={
          topic ? `retention ${formatDuration(topic.retentionMs)} · ${consuming.length} consumer groups` : null
        }
      />

      <StatGrid>
        <Stat
          label="Partitions"
          value={topic?.partitions.length ?? 0}
          hint={`replication factor ${topic?.replicationFactor ?? "—"}`}
          icon={<NetworkIcon />}
          loading={isPending}
        />
        <Stat
          label="Messages"
          value={formatCount(topic?.messageCount ?? 0)}
          hint={formatNumber(topic?.messageCount ?? 0)}
          icon={<DatabaseIcon />}
          loading={isPending}
        />
        <Stat
          label="Size"
          value={formatBytes(topic?.sizeBytes ?? 0)}
          hint="sum of all replicas"
          icon={<DatabaseIcon />}
          loading={isPending}
        />
        <Stat
          label="Produce rate"
          value={`${formatThroughput(topic?.messagesPerSec ?? 0)}/s`}
          icon={<GaugeIcon />}
          loading={isPending}
          accent
        >
          <Sparkline data={throughput} />
        </Stat>
      </StatGrid>

      <Tabs value={tab} onValueChange={(value) => selectTab(String(value))}>
        <TabsList variant="line">
          <TabsTrigger value="data">Data</TabsTrigger>
          <TabsTrigger value="partitions">
            Partitions
            <span className="numeric ml-1.5 text-muted-foreground">
              {topic?.partitions.length ?? 0}
            </span>
          </TabsTrigger>
          <TabsTrigger value="groups">
            Consumer groups
            <span className="numeric ml-1.5 text-muted-foreground">{consuming.length}</span>
          </TabsTrigger>
          <TabsTrigger value="config">Configuration</TabsTrigger>
        </TabsList>

        <TabsContent value="data" className="mt-4">
          {topic ? <RecordBrowser cluster={cluster} topic={topic} /> : null}
        </TabsContent>

        <TabsContent value="partitions" className="mt-4">
          <DataTable
            columns={partitionColumns}
            rows={topic?.partitions ?? []}
            rowKey={(partition) => String(partition.id)}
            loading={isPending}
            pageSize={25}
            defaultSort={{ id: "id", direction: "asc" }}
          />
        </TabsContent>

        <TabsContent value="groups" className="mt-4">
          <DataTable
            columns={groupColumns}
            rows={consuming}
            rowKey={(group) => group.id}
            loading={isPending}
            onRowClick={(group) => navigate(clusterPath(cluster, "groups", group.id))}
            emptyState={
              <p className="py-10 text-center text-sm text-muted-foreground">
                No consumer group is subscribed to this topic.
              </p>
            }
          />
        </TabsContent>

        <TabsContent value="config" className="mt-4">
          <ConfigTable entries={configs} loading={configsPending} />
        </TabsContent>
      </Tabs>
    </div>
  )
}
