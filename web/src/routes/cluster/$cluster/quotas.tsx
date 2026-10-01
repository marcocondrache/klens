import { Fragment, useMemo, type ReactNode } from "react";
import { createFileRoute, stripSearchParams } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";
import { AsteriskIcon, UserRoundIcon } from "lucide-react";

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
import { Pill } from "@/components/status";
import { useAccess } from "@/hooks/use-access";
import { useSearchDraft } from "@/hooks/use-search-draft";
import { apiErrorMessage } from "@/lib/api/client";
import { useQuotas } from "@/lib/api/catalog";
import type { ClientQuota, QuotaEntity, QuotaEntityType } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";
import { formatBytes, formatNumber, formatThroughput } from "@/lib/format";
import {
  QUOTA_ENTITY_TYPES,
  quotasSearch,
  searchDefaults,
  type QuotaFilter,
  type QuotasSearch,
} from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/quotas")({
  validateSearch: quotasSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(quotasSearch))] },
  component: QuotasPage,
});

const EMPTY_QUOTAS: ClientQuota[] = [];

const DENIED = "klens's Kafka user needs DESCRIBE_CONFIGS on the cluster to read quotas.";

const ENTITY_LABEL: Record<QuotaEntityType, string> = {
  USER: "User",
  CLIENT_ID: "Client ID",
  IP: "IP",
};

function isDefault(quota: ClientQuota) {
  return quota.entity.some((part) => part.name === null);
}

const FILTERS: Array<FilterField<ClientQuota, QuotaFilter>> = [
  {
    id: "entity",
    label: "Entity",
    plural: "entities",
    icon: UserRoundIcon,
    options: QUOTA_ENTITY_TYPES.map((value) => ({ value, label: ENTITY_LABEL[value] })),
    accessor: (quota) => quota.entity.map((part) => part.entityType),
  },
  {
    id: "scope",
    label: "Scope",
    plural: "scopes",
    icon: AsteriskIcon,
    options: [
      { value: "named", label: "Named" },
      { value: "default", label: "Default" },
    ],
    accessor: (quota) => (isDefault(quota) ? "default" : "named"),
  },
];

function entityKey(quota: ClientQuota) {
  return quota.entity
    .map((part) => (part.name === null ? part.entityType : `${part.entityType}=${part.name}`))
    .join("\u0000");
}

function EntityCell({ parts }: { parts: QuotaEntity[] }) {
  return (
    <span className="flex min-w-0 items-center gap-2">
      {parts.map((part, index) => (
        <Fragment key={part.entityType}>
          {index > 0 ? <span className="text-muted-foreground/60">+</span> : null}
          <span className="flex min-w-0 items-center gap-1.5">
            <Pill className="shrink-0">{ENTITY_LABEL[part.entityType]}</Pill>
            {part.name === null ? (
              <span className="text-muted-foreground italic">default</span>
            ) : (
              <span className="truncate font-mono">{part.name}</span>
            )}
          </span>
        </Fragment>
      ))}
    </span>
  );
}

function quotaValue(
  value: number | null,
  display: (value: number) => string,
  exact: (value: number) => string,
) {
  if (value === null) {
    return <span className="text-muted-foreground/60">—</span>;
  }

  return (
    <span className="numeric" title={exact(value)}>
      {display(value)}
    </span>
  );
}

function byteRate(value: number | null) {
  return quotaValue(
    value,
    (bytes) => `${formatBytes(bytes)}/s`,
    (bytes) => `${formatNumber(bytes)} bytes/s`,
  );
}

function perSecond(value: number | null) {
  return quotaValue(
    value,
    (rate) => `${formatThroughput(rate)}/s`,
    (rate) => `${formatNumber(rate)}/s`,
  );
}

function percent(value: number | null) {
  return quotaValue(
    value,
    (share) => `${formatNumber(share)}%`,
    () => "Of network and I/O thread time",
  );
}

const columnHelper = createColumnHelper<DataTableFeatures, ClientQuota>();

function rateColumn(
  id: string,
  title: string,
  pick: (quota: ClientQuota) => number | null,
  render: (value: number | null) => ReactNode,
) {
  return columnHelper.accessor((quota) => pick(quota) ?? -1, {
    id,
    header: title,
    meta: { align: "right", width: "8rem" },
    cell: ({ row }) => render(pick(row.original)),
  });
}

const columns = columnHelper.columns([
  columnHelper.accessor(entityKey, {
    id: "entity",
    header: "Entity",
    cell: ({ row }) => <EntityCell parts={row.original.entity} />,
  }),
  rateColumn("produce", "Produce", (quota) => quota.producerByteRate, byteRate),
  rateColumn("consume", "Consume", (quota) => quota.consumerByteRate, byteRate),
  rateColumn("request", "Request time", (quota) => quota.requestPercentage, percent),
  rateColumn("mutations", "Mutations", (quota) => quota.controllerMutationRate, perSecond),
  rateColumn("connections", "Connections", (quota) => quota.connectionCreationRate, perSecond),
]);

function QuotasPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const search = Route.useSearch();
  const { q: term } = search;
  const filters = readFilters(FILTERS, search);

  function setSearch(patch: Partial<QuotasSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }), replace: true });
  }
  const searchInput = useSearchDraft(term, (q) => setSearch({ q }));
  const { can } = useAccess();
  const canConfigs = can(cluster, "CONFIGS");
  const { data, isPending, isError, error } = useQuotas(cluster, canConfigs);
  const lane = data?.sourceHealth;
  const status = data?.status;
  const denied = status === "DENIED";
  const quotas = data?.quotas ?? EMPTY_QUOTAS;

  function setFilters(rules: FilterRule[]) {
    setSearch(filterParams(FILTERS, rules));
  }

  const searched = useMemo(() => {
    const needle = term.trim().toLowerCase();
    if (!needle) return quotas;

    return quotas.filter((quota) =>
      quota.entity
        .map((part) => `${ENTITY_LABEL[part.entityType]} ${part.name ?? "default"}`)
        .join(" ")
        .toLowerCase()
        .includes(needle),
    );
  }, [quotas, term]);

  const rows = applyFilters(searched, FILTERS, filters);

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
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={entityKey}
        toolbar={
          <>
            <SearchField {...searchInput} placeholder="Search users, client IDs and IPs…" />

            <FilterBar fields={FILTERS} rows={searched} value={filters} onChange={setFilters} />
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
    </div>
  );
}
