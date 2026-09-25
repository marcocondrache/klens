import { useMemo } from "react";
import { createFileRoute, stripSearchParams } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";
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
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { GROUP_TONE, GroupStateBadge, Pill, StatusDot, TONE_TEXT } from "@/components/status";
import { lagTone } from "@/lib/tone";
import { useSearchDraft } from "@/hooks/use-search-draft";
import { useClusterHealth, useGroupRows } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { formatCount, formatEnumLabel, formatNumber, toNumber } from "@/lib/format";
import type { GroupRow } from "@/lib/api/types";
import { cn } from "@/lib/utils";
import {
  GROUP_STATES,
  searchDefaults,
  groupsSearch,
  type GroupFilter,
  type GroupsSearch,
} from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/groups")({
  validateSearch: groupsSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(groupsSearch))] },
  component: ConsumerGroupsPage,
});

const EMPTY_GROUPS: GroupRow[] = [];

const FILTERS: Array<FilterField<GroupRow, GroupFilter>> = [
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
    meta: { width: "12rem" },
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  columnHelper.accessor("memberCount", {
    id: "members",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Members" className="justify-end" />
    ),
    meta: { align: "right", width: "6rem" },
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
          <Pill className="shrink-0">+{row.original.topicNames.length - 1}</Pill>
        ) : null}
      </span>
    ),
  }),
  columnHelper.accessor((group) => toNumber(group.totalLag), {
    id: "lag",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Lag" className="justify-end" />
    ),
    meta: { align: "right", width: "9rem" },
    cell: ({ row }) => <LagValue row={row.original} />,
  }),
  columnHelper.accessor("coordinatorId", {
    id: "coordinator",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Coordinator" className="justify-end" />
    ),
    meta: { align: "right", width: "7.5rem" },
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
  const search = Route.useSearch();
  const { q: term } = search;
  const filters = readFilters(FILTERS, search);

  function setSearch(patch: Partial<GroupsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }
  const searchInput = useSearchDraft(term, (q) => setSearch({ q }));

  const { data: groups = EMPTY_GROUPS, isPending, isError, error } = useGroupRows(cluster);
  const { data: health } = useClusterHealth(cluster);

  function setFilters(rules: FilterRule[]) {
    setSearch(filterParams(FILTERS, rules));
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
        description={
          <>
            {rows.length} groups · {formatCount(totalLag)} messages of lag
            <LaneCaption lane={health?.offsets} />
          </>
        }
      />

      <DataTable
        columns={columns}
        storageKey="groups"
        data={rows}
        getRowId={(group) => group.id}
        toolbar={
          <>
            <SearchField {...searchInput} placeholder="Search consumer groups…" />

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
