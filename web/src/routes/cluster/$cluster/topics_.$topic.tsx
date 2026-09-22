import { AlertTriangleIcon } from "lucide-react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ConfigTable } from "@/components/config-table";
import { CopyButton } from "@/components/copy-button";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PageHeader } from "@/components/page-header";
import { RecordBrowser } from "@/components/records/record-browser";
import { GroupStateBadge, Pill } from "@/components/status";
import { lagTone } from "@/lib/tone";
import { useTopic, useTopicGroups } from "@/lib/api/catalog";
import { catalogLookupMessage } from "@/lib/catalog-lookup";
import { useTopicConfigs } from "@/lib/api/live";
import { useClusterName } from "@/lib/clusters";
import {
  formatCleanupPolicy,
  formatCount,
  formatDuration,
  formatNumber,
  formatThroughput,
  isCompactCleanup,
  toNumber,
} from "@/lib/format";
import type { PartitionRow, TopicDetail, TopicGroupRow } from "@/lib/api/types";
import { parseTopicDetailSearch } from "@/lib/route-search";
import { useAccess } from "@/hooks/use-access";

export const Route = createFileRoute("/cluster/$cluster/topics_/$topic")({
  validateSearch: parseTopicDetailSearch,
  component: TopicPage,
});

const partitionColumnHelper = createColumnHelper<DataTableFeatures, PartitionRow>();
const groupColumnHelper = createColumnHelper<DataTableFeatures, TopicGroupRow>();

const partitionColumns = partitionColumnHelper.columns([
  partitionColumnHelper.accessor("id", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Partition" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("leader", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Leader" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("replicas", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Replicas" />,
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
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="In sync" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ row }) => (
      <span
        className={
          row.original.isr.length < row.original.replicas.length
            ? "numeric font-mono text-warn"
            : "numeric font-mono"
        }
      >
        {row.original.isr.length}/{row.original.replicas.length}
      </span>
    ),
  }),
  partitionColumnHelper.accessor((partition) => toNumber(partition.lowWatermark), {
    id: "low",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Low offset" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ row }) => formatNumber(row.original.lowWatermark),
  }),
  partitionColumnHelper.accessor((partition) => toNumber(partition.highWatermark), {
    id: "high",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="High offset" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ row }) => formatNumber(row.original.highWatermark),
  }),
  partitionColumnHelper.accessor((partition) => toNumber(partition.retained), {
    id: "messages",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Messages" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ row }) => formatNumber(row.original.retained),
  }),
]);

const groupColumns = groupColumnHelper.columns([
  groupColumnHelper.accessor("id", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Group" />,
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  groupColumnHelper.accessor("state", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="State" />,
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  groupColumnHelper.accessor("memberCount", {
    id: "members",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Members" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  groupColumnHelper.accessor((group) => toNumber(group.lagOnTopic), {
    id: "lag",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Lag on this topic" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ row: groupRow }) => (
      <Pill tone={lagTone(toNumber(groupRow.original.lagOnTopic))} className="numeric font-mono">
        {formatNumber(groupRow.original.lagOnTopic)}
      </Pill>
    ),
  }),
]);

function TopicFacts({ detail }: { detail: TopicDetail }) {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
      <span className="numeric">{detail.partitions.length} partitions</span>
      <span className="numeric">{formatCount(detail.retainedMessages)} msgs</span>
      <span className="numeric">retention {formatDuration(detail.retentionMs)}</span>
      <span className="numeric text-brand">{formatThroughput(detail.rate)}/s</span>
    </div>
  );
}

function TopicPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { topic: topicName } = Route.useParams();
  const { tab: tabParam } = Route.useSearch();
  const { can } = useAccess();
  const canRecords = can(cluster, "RECORDS");
  const canConfigs = can(cluster, "CONFIGS");
  const requested = tabParam ?? (canRecords ? "data" : "partitions");
  const tab =
    (requested === "data" && !canRecords) || (requested === "config" && !canConfigs)
      ? "partitions"
      : requested;

  const { data: detail = null, isPending, isError, error } = useTopic(cluster, topicName);
  const { data: configs = [], isPending: configsPending } = useTopicConfigs(
    cluster,
    topicName,
    tab === "config" && canConfigs,
  );
  const { data: groups = [], isPending: groupsPending } = useTopicGroups(
    cluster,
    topicName,
    tab === "groups",
  );

  const groupCount = detail?.groupCount ?? groups.length;

  function selectTab(value: string) {
    void navigate({
      to: ".",
      search: (prev) => {
        const next = { ...prev };
        if (value === "partitions" || value === "groups" || value === "config") {
          next.tab = value;
        } else {
          delete next.tab;
        }
        return next;
      },
      replace: true,
      resetScroll: false,
    });
  }

  const lookup = catalogLookupMessage({
    isPending,
    isError,
    error,
    data: detail,
    missing: "This topic does not exist in the selected cluster.",
    failed: "Failed to load this topic.",
  });
  if (lookup) {
    return <PageHeader title={topicName} mono description={lookup} />;
  }

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
          detail ? (
            <>
              {detail.internal ? <Pill>internal</Pill> : null}
              <Pill tone={isCompactCleanup(detail.cleanupPolicy) ? "brand" : "idle"}>
                {formatCleanupPolicy(detail.cleanupPolicy)}
              </Pill>
              <Pill>RF {detail.replicationFactor}</Pill>
              {detail.underReplicated ? (
                <Pill tone="warn">
                  <AlertTriangleIcon className="size-3" />
                  under-replicated
                </Pill>
              ) : null}
            </>
          ) : null
        }
        description={detail ? <TopicFacts detail={detail} /> : null}
      />

      <Tabs
        value={tab}
        onValueChange={(value) => selectTab(String(value))}
        className="min-h-0 flex-1"
      >
        <TabsList variant="line" className="shrink-0">
          {canRecords ? <TabsTrigger value="data">Data</TabsTrigger> : null}
          <TabsTrigger value="partitions">
            Partitions
            <span className="numeric ml-1.5 text-muted-foreground">
              {detail?.partitions.length ?? 0}
            </span>
          </TabsTrigger>
          <TabsTrigger value="groups">
            Consumer groups
            <span className="numeric ml-1.5 text-muted-foreground">{groupCount}</span>
          </TabsTrigger>
          {canConfigs ? <TabsTrigger value="config">Configuration</TabsTrigger> : null}
        </TabsList>

        {canRecords ? (
          <TabsContent value="data" className="mt-4 flex min-h-0 flex-col">
            {detail ? <RecordBrowser cluster={cluster} topic={detail} /> : null}
          </TabsContent>
        ) : null}

        <TabsContent value="partitions" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={partitionColumns}
            data={detail?.partitions ?? []}
            getRowId={(partition) => String(partition.id)}
            loading={isPending}
            defaultSort={{ id: "id", direction: "asc" }}
            fill
          />
        </TabsContent>

        <TabsContent value="groups" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={groupColumns}
            data={groups}
            getRowId={(group) => group.id}
            loading={groupsPending}
            onRowClick={(group) => {
              void navigate({
                to: "/cluster/$cluster/groups/$group",
                params: { cluster, group: group.id },
              });
            }}
            emptyState={
              <p className="py-10 text-center text-sm text-muted-foreground">
                No consumer group is subscribed to this topic.
              </p>
            }
            fill
          />
        </TabsContent>

        {canConfigs ? (
          <TabsContent value="config" className="mt-4 flex min-h-0 flex-col">
            <ConfigTable entries={configs} loading={configsPending} fill />
          </TabsContent>
        ) : null}
      </Tabs>
    </div>
  );
}
