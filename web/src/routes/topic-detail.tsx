import { useMemo } from "react";
import { AlertTriangleIcon, DatabaseIcon, GaugeIcon, NetworkIcon } from "lucide-react";
import { useNavigate, useParams, useSearchParams } from "@/lib/navigation";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ConfigTable } from "@/components/config-table";
import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table";
import { PageHeader } from "@/components/page-header";
import { RecordBrowser } from "@/components/records/record-browser";
import { Sparkline } from "@/components/charts";
import { Stat, StatGrid } from "@/components/stat";
import { GroupStateBadge, Pill } from "@/components/status";
import { lagTone } from "@/lib/tone";
import { useTopic, useTopicConsumerGroups } from "@/lib/api/catalog";
import { catalogLookupMessage } from "@/lib/catalog-lookup";
import { useTopicConfigs, useTopicThroughput } from "@/lib/api/live";
import { useTopicRates } from "@/lib/api/subscriptions";
import { clusterPath, useClusterName } from "@/lib/clusters";
import {
  formatCleanupPolicy,
  formatCount,
  formatDuration,
  formatNumber,
  formatThroughput,
  isCompactCleanup,
} from "@/lib/format";
import type { ConsumerGroup, Partition } from "@/lib/api/types";
import { createAppColumnHelper } from "@/lib/table";

const TABS = ["data", "partitions", "groups", "config"];

const partitionColumnHelper = createAppColumnHelper<Partition>();
const groupColumnHelper = createAppColumnHelper<ConsumerGroup>();

const partitionColumns = partitionColumnHelper.columns([
  partitionColumnHelper.accessor("id", {
    header: "Partition",
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("leader", {
    header: "Leader",
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  partitionColumnHelper.display({
    id: "replicas",
    header: "Replicas",
    cell: ({ row }) => (
      <span className="flex flex-wrap gap-1">
        {row.original.replicas.map((replica) => (
          <Pill
            key={replica}
            tone={row.original.isr.includes(replica) ? "idle" : "error"}
            className="numeric font-mono"
          >
            {replica}
          </Pill>
        ))}
      </span>
    ),
  }),
  partitionColumnHelper.accessor((partition) => partition.isr.length, {
    id: "isr",
    header: "In sync",
    meta: { align: "right" },
    cell: ({ row }) => (
      <span
        className={
          row.original.isr.length < row.original.replicas.length
            ? "numeric font-mono text-amber-500"
            : "numeric font-mono"
        }
      >
        {row.original.isr.length}/{row.original.replicas.length}
      </span>
    ),
  }),
  partitionColumnHelper.accessor("lowWatermark", {
    id: "low",
    header: "Low offset",
    meta: { align: "right" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor("highWatermark", {
    id: "high",
    header: "High offset",
    meta: { align: "right" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor((partition) => partition.highWatermark - partition.lowWatermark, {
    id: "messages",
    header: "Messages",
    meta: { align: "right" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
]);

export function TopicPage() {
  const cluster = useClusterName();
  const navigate = useNavigate();
  const { topic: topicParam } = useParams<{ topic: string }>();
  const topicName = decodeURIComponent(topicParam ?? "");
  const [params, setParams] = useSearchParams();

  const tab = TABS.includes(params.get("tab") ?? "") ? params.get("tab")! : "data";

  const { data: topic, isPending, isError, error } = useTopic(cluster, topicName);
  useTopicRates(cluster);
  const { data: configs = [], isPending: configsPending } = useTopicConfigs(
    cluster,
    topicName,
    tab === "config",
  );
  const { data: throughput = [] } = useTopicThroughput(cluster, topicName);
  const { data: groups = [], isPending: groupsPending } = useTopicConsumerGroups(
    cluster,
    topicName,
    tab === "groups",
  );

  const consuming = useMemo(
    () => groups.filter((group) => group.topics.includes(topicName)),
    [groups, topicName],
  );
  const groupCount = topic?.consumerGroups.length ?? consuming.length;

  function selectTab(value: string) {
    const next = new URLSearchParams(params);
    if (value === "data") {
      next.delete("tab");
    } else {
      next.set("tab", value);
    }
    setParams(next, { replace: true });
  }

  const lookup = catalogLookupMessage({
    isPending,
    isError,
    error,
    data: topic,
    missing: "This topic does not exist in the selected cluster.",
    failed: "Failed to load this topic.",
  });
  if (lookup) {
    return <PageHeader title={topicName} mono description={lookup} />;
  }

  const groupColumns = groupColumnHelper.columns([
    groupColumnHelper.accessor("id", {
      header: "Group",
      cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
    }),
    groupColumnHelper.accessor("state", {
      header: "State",
      cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
    }),
    groupColumnHelper.accessor((group) => group.members.length, {
      id: "members",
      header: "Members",
      meta: { align: "right" },
      cell: ({ getValue }) => getValue(),
    }),
    groupColumnHelper.accessor(
      (group) =>
        group.offsets
          .filter((offset) => offset.topic === topicName)
          .reduce((sum, offset) => sum + offset.lag, 0),
      {
        id: "lag",
        header: "Lag on this topic",
        meta: { align: "right" },
        cell: ({ getValue }) => (
          <Pill tone={lagTone(getValue())} className="numeric font-mono">
            {formatNumber(getValue())}
          </Pill>
        ),
      },
    ),
  ]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
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
          topic
            ? `retention ${formatDuration(topic.retentionMs)} · ${groupCount} consumer groups`
            : null
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
          label="Produce rate"
          value={`${formatThroughput(topic?.messagesPerSec ?? 0)}/s`}
          icon={<GaugeIcon />}
          loading={isPending}
          accent
        >
          <Sparkline data={throughput} />
        </Stat>
      </StatGrid>

      <Tabs
        value={tab}
        onValueChange={(value) => selectTab(String(value))}
        className="min-h-0 flex-1"
      >
        <TabsList variant="line" className="shrink-0">
          <TabsTrigger value="data">Data</TabsTrigger>
          <TabsTrigger value="partitions">
            Partitions
            <span className="numeric ml-1.5 text-muted-foreground">
              {topic?.partitions.length ?? 0}
            </span>
          </TabsTrigger>
          <TabsTrigger value="groups">
            Consumer groups
            <span className="numeric ml-1.5 text-muted-foreground">{groupCount}</span>
          </TabsTrigger>
          <TabsTrigger value="config">Configuration</TabsTrigger>
        </TabsList>

        <TabsContent value="data" className="mt-4 flex min-h-0 flex-col">
          {topic ? <RecordBrowser cluster={cluster} topic={topic} /> : null}
        </TabsContent>

        <TabsContent value="partitions" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={partitionColumns}
            data={topic?.partitions ?? []}
            getRowId={(partition) => String(partition.id)}
            loading={isPending}
            pageSize={25}
            defaultSort={{ id: "id", direction: "asc" }}
            fill
          />
        </TabsContent>

        <TabsContent value="groups" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={groupColumns}
            data={consuming}
            getRowId={(group) => group.id}
            loading={groupsPending}
            onRowClick={(group) => navigate(clusterPath(cluster, "groups", group.id))}
            emptyState={
              <p className="py-10 text-center text-sm text-muted-foreground">
                No consumer group is subscribed to this topic.
              </p>
            }
            fill
          />
        </TabsContent>

        <TabsContent value="config" className="mt-4 flex min-h-0 flex-col">
          <ConfigTable entries={configs} loading={configsPending} fill />
        </TabsContent>
      </Tabs>
    </div>
  );
}
