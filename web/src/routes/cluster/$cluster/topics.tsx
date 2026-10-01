import { useMemo, type ReactNode } from "react";
import { ActivityIcon, AlertTriangleIcon, HeartPulseIcon, RecycleIcon } from "lucide-react";
import { createFileRoute, stripSearchParams } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { FilterBar } from "@/components/data-table/filter-bar";
import { type FilterField } from "@/components/data-table/filters";
import { useTableSearch } from "@/components/data-table/use-table-search";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { PendingValue, Pill, SizeValue, StatusDot } from "@/components/status";
import { useClusterHealth, useTopicRows } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import {
  formatCleanupPolicy,
  formatDuration,
  formatNumber,
  formatThroughput,
  isCompactCleanup,
} from "@/lib/format";
import type { TopicRow } from "@/lib/api/types";
import {
  searchDefaults,
  topicsSearch,
  type TopicFilter,
  type TopicsSearch,
} from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/topics")({
  validateSearch: topicsSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(topicsSearch))] },
  component: TopicsPage,
});

const FILTERS: Array<FilterField<TopicRow, TopicFilter>> = [
  {
    id: "policy",
    label: "Policy",
    plural: "policies",
    icon: RecycleIcon,
    options: [
      { value: "delete", label: "delete" },
      { value: "compact", label: "compact" },
    ],
    accessor: (topic) => formatCleanupPolicy(topic.cleanupPolicy).split(","),
  },
  {
    id: "health",
    label: "Health",
    plural: "states",
    icon: HeartPulseIcon,
    options: [
      { value: "under-replicated", label: "Under-replicated", icon: <StatusDot tone="warn" /> },
      { value: "in-sync", label: "In sync", icon: <StatusDot tone="ok" /> },
    ],
    accessor: (topic) => (topic.underReplicated ? "under-replicated" : "in-sync"),
  },
  {
    id: "activity",
    label: "Activity",
    plural: "states",
    icon: ActivityIcon,
    options: [
      { value: "active", label: "Producing", icon: <StatusDot tone="ok" /> },
      { value: "idle", label: "Idle", icon: <StatusDot tone="idle" /> },
    ],
    accessor: (topic) => (topic.rate === 0 ? "idle" : "active"),
  },
];

const EMPTY_TOPICS: TopicRow[] = [];

function topicMatches(topic: TopicRow, needle: string) {
  return topic.name.toLowerCase().includes(needle);
}

function emptyMetric(value: number, display: ReactNode) {
  if (value === 0) {
    return <span className="text-muted-foreground/60">—</span>;
  }

  return display;
}

const columnHelper = createColumnHelper<DataTableFeatures, TopicRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("name", {
    header: "Topic",
    cell: ({ row }) => {
      const topic = row.original;

      return (
        <span className="flex items-center gap-2">
          <span className="truncate font-mono">{topic.name}</span>
          {topic.internal ? <Pill className="shrink-0">internal</Pill> : null}
          {topic.underReplicated ? (
            <Pill tone="warn" className="shrink-0">
              <AlertTriangleIcon />
              under-replicated
            </Pill>
          ) : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("partitionCount", {
    id: "partitions",
    header: "Parts",
    meta: { align: "right", width: "5rem" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("replicationFactor", {
    id: "replication",
    header: "RF",
    meta: { align: "right", width: "4rem" },
  }),
  columnHelper.accessor("retainedMessages", {
    id: "messages",
    header: "Messages",
    meta: { align: "right", width: "9rem" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatNumber(getValue())),
  }),
  columnHelper.accessor((topic) => topic.sizeBytes ?? -1, {
    id: "size",
    header: "Size",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => <SizeValue bytes={row.original.sizeBytes} />,
  }),
  columnHelper.accessor("rate", {
    id: "rate",
    header: "Msg/s",
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatThroughput(getValue())),
  }),
  columnHelper.accessor((topic) => topic.retentionMs ?? Number.POSITIVE_INFINITY, {
    id: "retention",
    header: "Retention",
    meta: { align: "right", width: "7rem" },
    cell: ({ row }) => {
      if (row.original.retentionMs === null) {
        return <PendingValue label="Fetching topic configs" className="ml-auto block" />;
      }
      return (
        <span className="text-muted-foreground">{formatDuration(row.original.retentionMs)}</span>
      );
    },
  }),
  columnHelper.accessor("cleanupPolicy", {
    id: "policy",
    header: "Policy",
    meta: { align: "right", width: "8rem" },
    cell: ({ getValue }) => (
      <span className={isCompactCleanup(getValue()) ? "text-foreground" : "text-muted-foreground"}>
        {formatCleanupPolicy(getValue())}
      </span>
    ),
  }),
  columnHelper.accessor("groupCount", {
    id: "groups",
    header: "Groups",
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => emptyMetric(getValue(), getValue()),
  }),
]);

function TopicsPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const search = Route.useSearch();
  const showInternal = search.internal;

  function setSearch(patch: Partial<TopicsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }

  const { data: topics = EMPTY_TOPICS, isPending, isError, error } = useTopicRows(cluster);
  const { data: health } = useClusterHealth(cluster);

  const visible = useMemo(
    () => (showInternal ? topics : topics.filter((topic) => !topic.internal)),
    [topics, showInternal],
  );
  const { searchInput, rows, filterBar } = useTableSearch({
    rows: visible,
    fields: FILTERS,
    search,
    setSearch,
    matches: topicMatches,
  });

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Topics"
        description={
          <>
            {rows.length} of {topics.length} topics
            <LaneCaption lane={health?.topology} />
          </>
        }
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(topic) => topic.name}
        toolbar={
          <>
            <SearchField {...searchInput} placeholder="Search topics…" />

            <FilterBar {...filterBar} />

            <Label className="ml-auto flex items-center gap-2 text-sm font-normal text-muted-foreground">
              <Switch
                size="sm"
                checked={showInternal}
                onCheckedChange={(checked) => setSearch({ internal: checked })}
              />
              Show internal
            </Label>
          </>
        }
        loading={isPending}
        error={isError ? apiErrorMessage(error, "Failed to load topics.") : undefined}
        defaultSort={{ id: "name", direction: "asc" }}
        onRowClick={(topic) => {
          void navigate({
            to: "/cluster/$cluster/topics/$topic",
            params: { cluster, topic: topic.name },
          });
        }}
      />
    </div>
  );
}
