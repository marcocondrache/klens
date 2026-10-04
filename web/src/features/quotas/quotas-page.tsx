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
import { useQuotas } from "@/lib/api/catalog";
import type { ClientQuota } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";

import type { QuotasSearch } from "./search";
import { entityKey } from "./quota-entity";
import { quotaActionColumn, quotaColumns } from "./quotas-columns";
import { QUOTA_FILTERS, quotaMatches } from "./quotas-filters";
import {
  EditQuotaButton,
  EditQuotaDialog,
  NewQuotaDialog,
  RemoveQuotaButton,
  RemoveQuotaDialog,
} from "./set-quota";

const route = getRouteApi("/cluster/$cluster/quotas");

const EMPTY_QUOTAS: ClientQuota[] = [];

const DENIED = "klens's Kafka user needs DESCRIBE_CONFIGS on the cluster to read quotas.";

export function QuotasPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const search = route.useSearch();

  function setSearch(patch: Partial<QuotasSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }
  const { can, canChange } = useAccess();
  const canConfigs = can(cluster, "BROKER_CONFIGS");
  const { data, isPending, isError, error } = useQuotas(cluster, canConfigs);
  const lane = data?.sourceHealth;
  const status = data?.status;
  const canManage = status === "DESCRIBED" && canChange(cluster, "ALTER_QUOTAS");
  const [editor] = useState(() => createDialogHandle<ClientQuota>());
  const [remover] = useState(() => createDialogHandle<ClientQuota>());
  const columns = useMemo(
    () =>
      canManage
        ? [
            ...quotaColumns,
            quotaActionColumn((quota) => (
              <>
                <EditQuotaButton handle={editor} quota={quota} />
                <RemoveQuotaButton handle={remover} quota={quota} />
              </>
            )),
          ]
        : quotaColumns,
    [canManage, editor, remover],
  );
  const denied = status === "DENIED";
  const quotas = data?.quotas ?? EMPTY_QUOTAS;

  const { searchInput, rows, filterBar } = useTableSearch({
    rows: quotas,
    fields: QUOTA_FILTERS,
    search,
    setSearch,
    matches: quotaMatches,
  });

  if (!canConfigs) {
    return <PageHeader title="Quotas" description="Your role cannot view client quotas." />;
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Quotas"
        description={
          <>
            {denied ? DENIED : `${rows.length} ${rows.length === 1 ? "entity" : "entities"}`}
            <LaneCaption lane={lane} />
          </>
        }
        actions={canManage ? <NewQuotaDialog cluster={cluster} /> : null}
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={entityKey}
        toolbar={
          <>
            <SearchField {...searchInput} placeholder="Search users, client IDs and IPs…" />

            <FilterBar {...filterBar} />
          </>
        }
        loading={isPending || (status === "PENDING" && lane?.lastError == null)}
        error={isError ? apiErrorMessage(error, "Failed to load quotas.") : undefined}
        emptyState={
          denied
            ? DENIED
            : lane?.lastError
              ? `Quotas are unavailable: ${lane.lastError}`
              : "No client quotas are set."
        }
        defaultSort={{ id: "entity", direction: "asc" }}
      />
      {canManage ? (
        <>
          <EditQuotaDialog cluster={cluster} handle={editor} />
          <RemoveQuotaDialog cluster={cluster} handle={remover} />
        </>
      ) : null}
    </div>
  );
}
