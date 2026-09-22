import { useMemo } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { GroupStateBadge, Pill } from "@/components/status";
import { lagTone } from "@/lib/tone";
import { useNow } from "@/hooks/use-now";
import { useClusterHealth, useGroupRows } from "@/lib/api/catalog";
import { laneCaption, useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { formatCount, formatEnumLabel, formatNumber, toNumber } from "@/lib/format";
import type { GroupRow, GroupState } from "@/lib/api/types";
import { parseGroupsSearch } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/groups")({
  validateSearch: parseGroupsSearch,
  component: ConsumerGroupsPage,
});

const EMPTY_GROUPS: GroupRow[] = [];

const STATES: GroupState[] = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "DEAD",
];

const STATE_ITEMS = [
  { value: "all", label: "All states" },
  ...STATES.map((value) => ({ value, label: formatEnumLabel(value) })),
];

const columnHelper = createColumnHelper<DataTableFeatures, GroupRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("id", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Group" />,
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("state", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="State" />,
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  columnHelper.accessor("memberCount", {
    id: "members",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Members" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor((group) => group.topicNames.length, {
    id: "topics",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Topics" />,
    cell: ({ row }) => (
      <span className="flex flex-wrap gap-1">
        {row.original.topicNames.slice(0, 1).map((topic) => (
          <Pill key={topic} className="font-mono">
            {topic}
          </Pill>
        ))}
        {row.original.topicNames.length > 1 ? (
          <Pill>+{row.original.topicNames.length - 1}</Pill>
        ) : null}
      </span>
    ),
  }),
  columnHelper.accessor((group) => toNumber(group.totalLag), {
    id: "lag",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Lag" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ row }) => <LagPill row={row.original} />,
  }),
  columnHelper.accessor("coordinatorId", {
    id: "coordinator",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Coordinator" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">broker {getValue()}</span>,
  }),
]);

function LagPill({ row }: { row: GroupRow }) {
  return (
    <Pill
      tone={lagTone(toNumber(row.totalLag))}
      className="numeric font-mono"
      title={row.lagComplete ? undefined : "Some partitions have no watermark yet"}
    >
      {row.lagComplete ? "" : "≥ "}
      {formatNumber(row.totalLag)}
    </Pill>
  );
}

function ConsumerGroupsPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { q: term = "", state = "all" } = Route.useSearch();

  const { data: groups = EMPTY_GROUPS, isPending, isError, error } = useGroupRows(cluster);
  const { data: health } = useClusterHealth(cluster);
  const now = useNow();
  const caption = laneCaption(health?.offsets, now);

  function update(key: "q" | "state", value: string | null) {
    void navigate({
      to: ".",
      search: (prev) => {
        const next = { ...prev };
        if (value === null || value === "" || value === "all") {
          delete next[key];
        } else if (key === "q") {
          next.q = value;
        } else if (STATES.includes(value as (typeof STATES)[number])) {
          next.state = value as (typeof STATES)[number];
        }
        return next;
      },
      replace: true,
      resetScroll: false,
    });
  }

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();

    return groups.filter((group) => {
      if (state !== "all" && group.state !== state) return false;
      if (needle && !group.id.toLowerCase().includes(needle)) return false;
      return true;
    });
  }, [groups, term, state]);

  const totalLag = rows.reduce((sum, group) => sum + toNumber(group.totalLag), 0);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Consumer groups"
        description={`${rows.length} groups · ${formatCount(totalLag)} messages of lag${caption ? ` · ${caption}` : ""}`}
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(group) => group.id}
        toolbar={
          <>
            <SearchField
              value={term}
              onChange={(event) => update("q", event.target.value)}
              placeholder="Search consumer groups…"
            />

            <Select
              value={state}
              items={STATE_ITEMS}
              onValueChange={(value) => update("state", String(value))}
            >
              <SelectTrigger className="w-48">
                <SelectValue placeholder="State" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {STATE_ITEMS.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </>
        }
        loading={isPending}
        error={isError ? apiErrorMessage(error, "Failed to load consumer groups.") : undefined}
        defaultSort={{ id: "lag", direction: "desc" }}
        onRowClick={(group) => {
          void navigate({
            to: "/cluster/$cluster/groups/$group",
            params: { cluster, group: group.id },
          });
        }}
        fill
      />
    </div>
  );
}
