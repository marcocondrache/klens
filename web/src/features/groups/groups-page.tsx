import { getRouteApi } from "@tanstack/react-router";

import { DataTable } from "@/components/data-table/data-table";
import { FilterBar } from "@/components/data-table/filter-bar";
import { useTableSearch } from "@/components/data-table/use-table-search";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { useClusterHealth, useGroupRows } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { formatCount } from "@/lib/format";
import type { GroupRow } from "@/lib/api/types";

import type { GroupsSearch } from "./search";
import { groupColumns } from "./groups-columns";
import { GROUP_FILTERS, groupMatches } from "./groups-filters";

const route = getRouteApi("/cluster/$cluster/groups");

const EMPTY_GROUPS: GroupRow[] = [];

export function ConsumerGroupsPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const search = route.useSearch();

  function setSearch(patch: Partial<GroupsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }

  const { data: groups = EMPTY_GROUPS, isPending, isError, error } = useGroupRows(cluster);
  const { data: health } = useClusterHealth(cluster);

  const { searchInput, rows, filterBar } = useTableSearch({
    rows: groups,
    fields: GROUP_FILTERS,
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
        columns={groupColumns}
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
