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
import { Facts } from "@/components/facts";
import { TabCount } from "@/components/tab-count";
import { GroupStateBadge, PendingValue, Pill, StatusDot, TONE_TEXT } from "@/components/status";
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
} from "@/lib/format";
import type { PartitionRow, TopicDetail, TopicGroupRow } from "@/lib/api/types";
import { topicTab, topicDetailSearch } from "@/lib/route-search";
import { useAccess } from "@/hooks/use-access";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/cluster/$cluster/topics_/$topic")({
  validateSearch: topicDetailSearch,
  component: TopicPage,
});

const partitionColumnHelper = createColumnHelper<DataTableFeatures, PartitionRow>();
const groupColumnHelper = createColumnHelper<DataTableFeatures, TopicGroupRow>();

const partitionColumns = partitionColumnHelper.columns([
  partitionColumnHelper.accessor("id", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Partition" className="justify-end" />
    ),
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("leader", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Leader" className="justify-end" />
    ),
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  partitionColumnHelper.accessor("replicas", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Replicas" />,
    cell: ({ row }) => (
      <span className="flex flex-wrap gap-1">
        {row.original.replicas.map((replica) => (
          <Pill
            key={replica}
            tone={row.original.isr.includes(replica) ? "idle" : "error"}
            className="numeric min-w-5 justify-center"
            title={row.original.isr.includes(replica) ? "In sync" : "Out of sync"}
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
    meta: { align: "right", width: "6rem" },
    cell: ({ row }) => (
      <span
        className={
          row.original.isr.length < row.original.replicas.length
            ? "numeric text-warn"
            : "numeric text-muted-foreground"
        }
      >
        {row.original.isr.length}/{row.original.replicas.length}
      </span>
    ),
  }),
  partitionColumnHelper.accessor("lowWatermark", {
    id: "low",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Low offset" className="justify-end" />
    ),
    meta: { align: "right", width: "9rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor("highWatermark", {
    id: "high",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="High offset" className="justify-end" />
    ),
    meta: { align: "right", width: "9rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
  partitionColumnHelper.accessor("retained", {
    id: "messages",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Messages" className="justify-end" />
    ),
    meta: { align: "right", width: "9rem" },
    cell: ({ getValue }) => formatNumber(getValue()),
  }),
]);

const groupColumns = groupColumnHelper.columns([
  groupColumnHelper.accessor("id", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Group" />,
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  groupColumnHelper.accessor("state", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="State" />,
    meta: { width: "12rem" },
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  groupColumnHelper.accessor("memberCount", {
    id: "members",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Members" className="justify-end" />
    ),
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => getValue(),
  }),
  groupColumnHelper.accessor((group) => group.lagOnTopic ?? -1, {
    id: "lag",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Lag on this topic" className="justify-end" />
    ),
    meta: { align: "right", width: "9rem" },
    cell: ({ row: groupRow }) => {
      if (groupRow.original.lagOnTopic === null) {
        return <PendingValue label="Fetching committed offsets" className="ml-auto block" />;
      }
      const lag = groupRow.original.lagOnTopic;

      return (
        <span
          className={cn("numeric", lag === 0 ? "text-muted-foreground" : TONE_TEXT[lagTone(lag)])}
        >
          {formatNumber(lag)}
        </span>
      );
    },
  }),
]);

function TopicFacts({ detail }: { detail: TopicDetail }) {
  return (
    <Facts>
      <span>{detail.partitions.length} partitions</span>
      <span>{formatCount(detail.retainedMessages)} messages</span>
      {detail.retentionMs === null ? (
        <PendingValue label="Fetching topic configs" />
      ) : (
        <span>{formatDuration(detail.retentionMs)} retention</span>
      )}
      {detail.rate > 0 ? (
        <span className="inline-flex items-center gap-1.5 text-foreground">
          <StatusDot tone="brand" pulse />
          {formatThroughput(detail.rate)} msg/s
        </span>
      ) : (
        <span>idle</span>
      )}
    </Facts>
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
              <Pill>{formatCleanupPolicy(detail.cleanupPolicy)}</Pill>
              <Pill>RF {detail.replicationFactor}</Pill>
              {detail.underReplicated ? (
                <Pill tone="warn">
                  <AlertTriangleIcon />
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
        onValueChange={(value) =>
          void navigate({ search: { tab: topicTab(value) }, replace: true })
        }
        className="min-h-0 flex-1"
      >
        <TabsList
          variant="line"
          className="w-full shrink-0 justify-start gap-3 border-b [&>[data-slot=tabs-trigger]]:flex-none [&>[data-slot=tabs-trigger]]:after:bg-brand"
        >
          {canRecords ? <TabsTrigger value="data">Data</TabsTrigger> : null}
          <TabsTrigger value="partitions">
            Partitions
            <TabCount value={detail?.partitions.length} />
          </TabsTrigger>
          <TabsTrigger value="groups">
            Consumer groups
            <TabCount value={detail?.groupCount} />
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
