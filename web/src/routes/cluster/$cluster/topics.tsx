import { useMemo, type ReactNode } from "react";
import { AlertTriangleIcon } from "lucide-react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useNow } from "@/hooks/use-now";
import { useClusterHealth, useTopicRows } from "@/lib/api/catalog";
import { laneCaption, useClusterName } from "@/lib/clusters";
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
import { parseTopicsSearch } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/topics")({
  validateSearch: parseTopicsSearch,
  component: TopicsPage,
});

const POLICY_ITEMS = [
  { value: "all", label: "All policies" },
  { value: "delete", label: "delete" },
  { value: "compact", label: "compact" },
] as const;

const EMPTY_TOPICS: TopicRow[] = [];

function emptyMetric(value: Int64, display: ReactNode) {
  if (isZero(value)) {
    return <span className="text-muted-foreground">—</span>;
  }

  return display;
}

const columnHelper = createColumnHelper<DataTableFeatures, TopicRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("name", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Topic" />,
    meta: { label: "Topic" },
    cell: ({ row }) => {
      const topic = row.original;

      return (
        <span className="flex items-center gap-2">
          <span className="font-mono text-sm">{topic.name}</span>
          {topic.internal ? <Pill>internal</Pill> : null}
          {topic.underReplicated ? (
            <Pill tone="warn">
              <AlertTriangleIcon className="size-3" />
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
    meta: { align: "right", label: "Parts" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("replicationFactor", {
    id: "replication",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="RF" className="justify-end" />
    ),
    meta: { align: "right", label: "RF" },
  }),
  columnHelper.accessor((topic) => toNumber(topic.retainedMessages), {
    id: "messages",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Messages" className="justify-end" />
    ),
    meta: { align: "right", label: "Messages" },
    cell: ({ row }) =>
      emptyMetric(row.original.retainedMessages, formatNumber(row.original.retainedMessages)),
  }),
  columnHelper.accessor("rate", {
    id: "rate",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Msg/s" className="justify-end" />
    ),
    meta: { align: "right", label: "Msg/s" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatThroughput(getValue())),
  }),
  columnHelper.accessor((topic) => toNumber(topic.retentionMs), {
    id: "retention",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Retention" className="justify-end" />
    ),
    meta: { align: "right", label: "Retention" },
    cell: ({ row }) => <span>{formatDuration(row.original.retentionMs)}</span>,
  }),
  columnHelper.accessor("cleanupPolicy", {
    id: "policy",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Policy" className="justify-end" />
    ),
    meta: { align: "right", label: "Policy" },
    cell: ({ getValue }) => (
      <Pill tone={isCompactCleanup(getValue()) ? "brand" : "idle"}>
        {formatCleanupPolicy(getValue())}
      </Pill>
    ),
  }),
  columnHelper.accessor("groupCount", {
    id: "groups",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Groups" className="justify-end" />
    ),
    meta: { align: "right", label: "Groups" },
    cell: ({ getValue }) => emptyMetric(getValue(), getValue()),
  }),
]);

function TopicsPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { q: term = "", internal, policy = "all" } = Route.useSearch();
  const showInternal = internal === "1";

  const { data: topics = EMPTY_TOPICS, isPending, isError, error } = useTopicRows(cluster);
  const { data: health } = useClusterHealth(cluster);
  const now = useNow();
  const caption = laneCaption(health?.topology, now);

  function update(key: "q" | "internal" | "policy", value: string | null) {
    void navigate({
      to: ".",
      search: (prev) => {
        const next = { ...prev };
        if (value === null || value === "" || value === "all") {
          delete next[key];
        } else if (key === "internal") {
          next.internal = "1";
        } else if (key === "policy") {
          if (value === "delete" || value === "compact") next.policy = value;
        } else {
          next.q = value;
        }
        return next;
      },
      replace: true,
      resetScroll: false,
    });
  }

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();

    return topics.filter((topic) => {
      if (!showInternal && topic.internal) return false;
      if (policy !== "all" && !formatCleanupPolicy(topic.cleanupPolicy).includes(policy))
        return false;
      if (needle && !topic.name.toLowerCase().includes(needle)) return false;
      return true;
    });
  }, [topics, term, showInternal, policy]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Topics"
        description={`${rows.length} of ${topics.length} topics${caption ? ` · ${caption}` : ""}`}
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(topic) => topic.name}
        toolbar={
          <>
            <SearchField
              value={term}
              onChange={(event) => update("q", event.target.value)}
              placeholder="Search topics…"
            />

            <Select
              value={policy}
              items={POLICY_ITEMS}
              onValueChange={(value) => update("policy", String(value))}
            >
              <SelectTrigger className="w-40">
                <SelectValue placeholder="Cleanup policy" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {POLICY_ITEMS.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>

            <Label className="flex items-center gap-2 text-sm text-muted-foreground">
              <Switch
                size="sm"
                checked={showInternal}
                onCheckedChange={(checked) => update("internal", checked ? "1" : null)}
              />
              Show internal
            </Label>
          </>
        }
        loading={isPending}
        error={
          isError ? (error instanceof Error ? error.message : "Failed to load topics.") : undefined
        }
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
