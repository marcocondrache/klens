import { useMemo } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";
import { useQueryStates } from "nuqs";
import { CircleDashedIcon, TimerIcon } from "lucide-react";

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
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { GROUP_TONE, GroupStateBadge, Pill, StatusDot, TONE_TEXT } from "@/components/status";
import { lagTone } from "@/lib/tone";
import { useNow } from "@/hooks/use-now";
import { useClusterHealth, useGroupRows } from "@/lib/api/catalog";
import { laneCaption, useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { formatCount, formatEnumLabel, formatNumber, toNumber } from "@/lib/format";
import type { GroupRow } from "@/lib/api/types";
import { cn } from "@/lib/utils";
import { GROUP_STATES, groupsSearch } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/groups")({
  component: ConsumerGroupsPage,
});

const EMPTY_GROUPS: GroupRow[] = [];

const FILTERS: Array<FilterField<GroupRow>> = [
  {
    id: "state",
    label: "State",
    plural: "states",
    icon: CircleDashedIcon,
    options: GROUP_STATES.map((state) => ({
      value: state,
      label: formatEnumLabel(state),
      icon: <StatusDot tone={GROUP_TONE[state]} />,
    })),
    accessor: (group) => group.state,
  },
  {
    id: "lag",
    label: "Lag",
    plural: "states",
    icon: TimerIcon,
    options: [
      { value: "lagging", label: "Lagging", icon: <StatusDot tone="warn" /> },
      { value: "caught-up", label: "Caught up", icon: <StatusDot tone="ok" /> },
    ],
    accessor: (group) => (toNumber(group.totalLag) > 0 ? "lagging" : "caught-up"),
  },
];

const columnHelper = createColumnHelper<DataTableFeatures, GroupRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("id", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Group" />,
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
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
      <span className="flex items-center gap-1.5">
        {row.original.topicNames.slice(0, 1).map((topic) => (
          <span key={topic} className="truncate font-mono text-muted-foreground">
            {topic}
          </span>
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
    cell: ({ row }) => <LagValue row={row.original} />,
  }),
  columnHelper.accessor("coordinatorId", {
    id: "coordinator",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Coordinator" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <span className="numeric text-muted-foreground">Broker {getValue()}</span>
    ),
  }),
]);

function LagValue({ row }: { row: GroupRow }) {
  const lag = toNumber(row.totalLag);

  return (
    <span
      className={cn("numeric", lag === 0 ? "text-muted-foreground" : TONE_TEXT[lagTone(lag)])}
      title={row.lagComplete ? undefined : "Some partitions have no watermark yet"}
    >
      {row.lagComplete ? "" : "≥ "}
      {formatNumber(row.totalLag)}
    </span>
  );
}

function ConsumerGroupsPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const [search, setSearch] = useQueryStates(groupsSearch);
  const { q: term } = search;
  const filters = readFilters(FILTERS, search);

  const { data: groups = EMPTY_GROUPS, isPending, isError, error } = useGroupRows(cluster);
  const { data: health } = useClusterHealth(cluster);
  const now = useNow();
  const caption = laneCaption(health?.offsets, now);

  function setFilters(rules: FilterRule[]) {
    void setSearch(filterParams(FILTERS, rules));
  }

  const searched = useMemo(() => {
    const needle = term.trim().toLowerCase();
    if (!needle) return groups;
    return groups.filter((group) => group.id.toLowerCase().includes(needle));
  }, [groups, term]);

  const rows = applyFilters(searched, FILTERS, filters);

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
              onChange={(event) => void setSearch({ q: event.target.value })}
              placeholder="Search consumer groups…"
            />

            <FilterBar fields={FILTERS} rows={searched} value={filters} onChange={setFilters} />
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
