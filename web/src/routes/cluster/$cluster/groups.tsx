import { createFileRoute, stripSearchParams } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";
import { CircleDashedIcon, TimerIcon } from "lucide-react";

import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { FilterBar } from "@/components/data-table/filter-bar";
import { type FilterField } from "@/components/data-table/filters";
import { useTableSearch } from "@/components/data-table/use-table-search";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { GROUP_TONE, GroupStateBadge, LagValue, Pill, StatusDot } from "@/components/status";
import { useClusterHealth, useGroupRows } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { formatCount, formatEnumLabel } from "@/lib/format";
import type { GroupRow } from "@/lib/api/types";
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

function groupMatches(group: GroupRow, needle: string) {
  return group.id.toLowerCase().includes(needle);
}

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
    accessor: (group) =>
      group.totalLag === null ? [] : group.totalLag > 0 ? "lagging" : "caught-up",
  },
];

const columnHelper = createColumnHelper<DataTableFeatures, GroupRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("id", {
    header: "Group",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("state", {
    header: "State",
    meta: { width: "12rem" },
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  columnHelper.accessor("memberCount", {
    id: "members",
    header: "Members",
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor((group) => group.topicNames.length, {
    id: "topics",
    header: "Topics",
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
  columnHelper.accessor((group) => group.totalLag ?? -1, {
    id: "lag",
    header: "Lag",
    meta: { align: "right", width: "9rem" },
    cell: ({ row }) => <LagValue lag={row.original.totalLag} complete={row.original.lagComplete} />,
  }),
]);

function ConsumerGroupsPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const search = Route.useSearch();

  function setSearch(patch: Partial<GroupsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }

  const { data: groups = EMPTY_GROUPS, isPending, isError, error } = useGroupRows(cluster);
  const { data: health } = useClusterHealth(cluster);

  const { searchInput, rows, filterBar } = useTableSearch({
    rows: groups,
    fields: FILTERS,
    search,
    setSearch,
    matches: groupMatches,
  });

  const lagPending = rows.some((group) => group.totalLag === null);
  const totalLag = rows.reduce((sum, group) => sum + (group.totalLag ?? 0), 0);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Consumer groups"
        description={
          <>
            {rows.length} groups · {lagPending ? "≥ " : ""}
            {formatCount(totalLag)} total lag
            <LaneCaption lane={health?.offsets} />
          </>
        }
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(group) => group.id}
        toolbar={
          <>
            <SearchField {...searchInput} placeholder="Search consumer groups…" />

            <FilterBar {...filterBar} />
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
      />
    </div>
  );
}
