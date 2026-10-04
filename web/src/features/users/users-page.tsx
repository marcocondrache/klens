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
import { apiErrorMessage } from "@/lib/api/client";
import { useScramUsers } from "@/lib/api/catalog";
import type { ScramUser } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";

import type { UsersSearch } from "./search";
import { DeleteCredentialButton, DeleteCredentialDialog } from "./delete-credential";
import { NewUserDialog, SetPasswordButton, SetPasswordDialog } from "./set-credential";
import { userActionColumn, userColumns } from "./users-columns";
import { USER_FILTERS, userMatches } from "./users-filters";

const route = getRouteApi("/cluster/$cluster/users");

const EMPTY_USERS: ScramUser[] = [];

const DENIED = "klens's Kafka user needs DESCRIBE on the cluster to read SCRAM credentials.";

export function UsersPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const search = route.useSearch();

  function setSearch(patch: Partial<UsersSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }
  const { can, canChange } = useAccess();
  const canAcls = can(cluster, "ACLS");
  const { data, isPending, isError, error } = useScramUsers(cluster, canAcls);
  const lane = data?.sourceHealth;
  const status = data?.status;
  const canSet = status === "DESCRIBED" && canChange(cluster, "SET_SCRAM_CREDENTIALS");
  const canDelete = status === "DESCRIBED" && canChange(cluster, "DELETE_SCRAM_CREDENTIALS");
  const [setter] = useState(() => createDialogHandle<ScramUser>());
  const [deleter] = useState(() => createDialogHandle<ScramUser>());
  const columns = useMemo(
    () =>
      canSet || canDelete
        ? [
            ...userColumns,
            userActionColumn((user) => (
              <>
                {canSet ? <SetPasswordButton handle={setter} user={user} /> : null}
                {canDelete ? <DeleteCredentialButton handle={deleter} user={user} /> : null}
              </>
            )),
          ]
        : userColumns,
    [canSet, canDelete, setter, deleter],
  );
  const denied = status === "DENIED";
  const users = data?.users ?? EMPTY_USERS;

  const { searchInput, rows, filterBar } = useTableSearch({
    rows: users,
    fields: USER_FILTERS,
    search,
    setSearch,
    matches: userMatches,
  });

  if (!canAcls) {
    return <PageHeader title="Users" description="Your role cannot view SCRAM users." />;
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Users"
        description={
          <>
            {denied ? DENIED : `${rows.length} ${rows.length === 1 ? "user" : "users"}`}
            <LaneCaption lane={lane} />
          </>
        }
        actions={canSet ? <NewUserDialog cluster={cluster} /> : null}
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(user) => user.name}
        toolbar={
          <>
            <SearchField {...searchInput} placeholder="Search users…" />

            <FilterBar {...filterBar} />
          </>
        }
        loading={isPending || (status === "PENDING" && lane?.lastError == null)}
        error={isError ? apiErrorMessage(error, "Failed to load users.") : undefined}
        emptyState={
          denied
            ? DENIED
            : lane?.lastError
              ? `Users are unavailable: ${lane.lastError}`
              : "No user has a SCRAM credential."
        }
        defaultSort={{ id: "name", direction: "asc" }}
      />
      {canSet ? <SetPasswordDialog cluster={cluster} handle={setter} /> : null}
      {canDelete ? <DeleteCredentialDialog cluster={cluster} handle={deleter} /> : null}
    </div>
  );
}
