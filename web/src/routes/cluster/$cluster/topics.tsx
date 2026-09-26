import { useMemo, type ReactNode } from "react";
import { ActivityIcon, AlertTriangleIcon, HeartPulseIcon, RecycleIcon } from "lucide-react";
import { createFileRoute, stripSearchParams } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { FilterBar } from "@/components/data-table/filter-bar";
import {
  applyFilters,
  filterParams,
  readFilters,
  type FilterField,
  type FilterRule,
} from "@/components/data-table/filters";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { PendingValue, Pill, StatusDot } from "@/components/status";
import { useSearchDraft } from "@/hooks/use-search-draft";
import { useClusterHealth, useTopicRows } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import {
  formatCleanupPolicy,
  formatDuration,
  formatNumber,
  formatThroughput,
  isCompactCleanup,
  isZero,
  toNumber,
} from "@/lib/format";
import type { Int64 } from "@/lib/format";
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
    accessor: (topic) => (isZero(topic.rate) ? "idle" : "active"),
  },
];

const EMPTY_TOPICS: TopicRow[] = [];

function emptyMetric(value: Int64, display: ReactNode) {
  if (isZero(value)) {
    return <span className="text-muted-foreground/60">—</span>;
  }

  return display;
}

const columnHelper = createColumnHelper<DataTableFeatures, TopicRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("name", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Topic" />,
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
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Parts" className="justify-end" />
    ),
    meta: { align: "right", width: "5rem" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("replicationFactor", {
    id: "replication",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="RF" className="justify-end" />
    ),
    meta: { align: "right", width: "4rem" },
  }),
  columnHelper.accessor((topic) => toNumber(topic.retainedMessages), {
    id: "messages",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Messages" className="justify-end" />
    ),
    meta: { align: "right", width: "9rem" },
    cell: ({ row }) =>
      emptyMetric(row.original.retainedMessages, formatNumber(row.original.retainedMessages)),
  }),
  columnHelper.accessor("rate", {
    id: "rate",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Msg/s" className="justify-end" />
    ),
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatThroughput(getValue())),
  }),
  columnHelper.accessor(
    (topic) => (topic.retentionMs === null ? Number.POSITIVE_INFINITY : toNumber(topic.retentionMs)),
    {
      id: "retention",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Retention" className="justify-end" />
      ),
      meta: { align: "right", width: "7rem" },
      cell: ({ row }) => {
        if (row.original.retentionMs === null) {
          return <PendingValue label="Fetching topic configs" className="ml-auto block" />;
        }
        return (
          <span className="text-muted-foreground">
            {formatDuration(row.original.retentionMs)}
          </span>
        );
      },
    },
  ),
  columnHelper.accessor("cleanupPolicy", {
    id: "policy",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Policy" className="justify-end" />
    ),
    meta: { align: "right", width: "8rem" },
    cell: ({ getValue }) => (
      <span className={isCompactCleanup(getValue()) ? "text-foreground" : "text-muted-foreground"}>
        {formatCleanupPolicy(getValue())}
      </span>
    ),
  }),
  columnHelper.accessor("groupCount", {
    id: "groups",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Groups" className="justify-end" />
    ),
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => emptyMetric(getValue(), getValue()),
  }),
]);

function TopicsPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const search = Route.useSearch();
  const { q: term, internal: showInternal } = search;
  const filters = readFilters(FILTERS, search);

  function setSearch(patch: Partial<TopicsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }
  const searchInput = useSearchDraft(term, (q) => setSearch({ q }));

  const { data: topics = EMPTY_TOPICS, isPending, isError, error } = useTopicRows(cluster);
  const { data: health } = useClusterHealth(cluster);

  function setFilters(rules: FilterRule[]) {
    setSearch(filterParams(FILTERS, rules));
  }

  const searched = useMemo(() => {
    const needle = term.trim().toLowerCase();

    return topics.filter((topic) => {
      if (!showInternal && topic.internal) return false;
      if (needle && !topic.name.toLowerCase().includes(needle)) return false;
      return true;
    });
  }, [topics, term, showInternal]);

  const rows = applyFilters(searched, FILTERS, filters);

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

            <FilterBar fields={FILTERS} rows={searched} value={filters} onChange={setFilters} />

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
        fill
      />
    </div>
  );
}
