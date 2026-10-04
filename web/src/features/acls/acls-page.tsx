import { useMemo, useState } from "react";
import { getRouteApi } from "@tanstack/react-router";

import { DataTable } from "@/components/data-table/data-table";
import { FilterBar } from "@/components/data-table/filter-bar";
import { useTableSearch } from "@/components/data-table/use-table-search";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { createDialogHandle } from "@/components/ui/dialog";
import { useAccess } from "@/hooks/use-access";
import { useAcls } from "@/lib/api/catalog";
import type { Acl } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";

import type { AclsSearch } from "./search";
import { aclActionColumn, aclColumns, aclRowId } from "./acls-columns";
import { ACL_FILTERS, aclMatches } from "./acls-filters";
import { CreateAclDialog } from "./create-acl";
import { DeleteAclButton, DeleteAclDialog } from "./delete-acl";

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
  const { can, canChange } = useAccess();
  const canAcls = can(cluster, "ACLS");
  const { data, isPending, isError, error } = useAcls(cluster, canAcls);
  const lane = data?.sourceHealth;
  const status = data?.status;
  const canCreate = status === "ENABLED" && canChange(cluster, "CREATE_ACLS");
  const canDelete = status === "ENABLED" && canChange(cluster, "DELETE_ACLS");
  const [deleter] = useState(() => createDialogHandle<Acl>());
  const columns = useMemo(
    () =>
      canDelete
        ? [...aclColumns, aclActionColumn((acl) => <DeleteAclButton handle={deleter} acl={acl} />)]
        : aclColumns,
    [canDelete, deleter],
  );
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
        actions={canCreate ? <CreateAclDialog cluster={cluster} /> : null}
      />

      <DataTable
        columns={columns}
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
      {canDelete ? <DeleteAclDialog cluster={cluster} handle={deleter} /> : null}
    </div>
  );
}
