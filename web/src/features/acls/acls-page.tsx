import { getRouteApi } from "@tanstack/react-router";

import { DataTable } from "@/components/data-table/data-table";
import { FilterBar } from "@/components/data-table/filter-bar";
import { useTableSearch } from "@/components/data-table/use-table-search";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { useAccess } from "@/hooks/use-access";
import { useAcls } from "@/lib/api/catalog";
import type { Acl } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";

import type { AclsSearch } from "./search";
import { aclColumns, aclRowId } from "./acls-columns";
import { ACL_FILTERS, aclMatches } from "./acls-filters";

const route = getRouteApi("/cluster/$cluster/acls");

const EMPTY_BINDINGS: Acl[] = [];
const DISABLED = "Authorization is disabled on this cluster.";
const DENIED = "klens's Kafka user needs DESCRIBE on the cluster to read ACLs.";

export function AclsPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const search = route.useSearch();

  function setSearch(patch: Partial<AclsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }
  const { can } = useAccess();
  const canAcls = can(cluster, "ACLS");
  const { data, isPending, isError, error } = useAcls(cluster, canAcls);
  const lane = data?.sourceHealth;
  const status = data?.status;
  const notice = status === "DISABLED" ? DISABLED : status === "DENIED" ? DENIED : undefined;
  const bindings = data?.bindings ?? EMPTY_BINDINGS;

  const { searchInput, rows, filterBar } = useTableSearch({
    rows: bindings,
    fields: ACL_FILTERS,
    search,
    setSearch,
    matches: aclMatches,
  });

  if (!canAcls) {
    return <PageHeader title="ACLs" description="Your role cannot view ACL bindings." />;
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="ACLs"
        description={
          <>
            {notice ?? `${rows.length} bindings`}
            <LaneCaption lane={lane} />
          </>
        }
      />

      <DataTable
        columns={aclColumns}
        data={rows}
        getRowId={aclRowId}
        toolbar={
          <>
            <SearchField {...searchInput} placeholder="Search ACLs…" />

            <FilterBar {...filterBar} />
          </>
        }
        loading={isPending || (status === "PENDING" && lane?.lastError == null)}
        error={isError ? apiErrorMessage(error, "Failed to load ACLs.") : undefined}
        emptyState={
          notice ??
          (lane?.lastError ? `ACLs are unavailable: ${lane.lastError}` : "No ACL bindings.")
        }
        defaultSort={{ id: "resourceName", direction: "asc" }}
      />
    </div>
  );
}
