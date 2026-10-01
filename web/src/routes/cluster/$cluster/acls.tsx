import { createFileRoute, stripSearchParams } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";
import { AsteriskIcon, BoxIcon, ShieldIcon, ZapIcon } from "lucide-react";

import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { FilterBar } from "@/components/data-table/filter-bar";
import { type FilterField } from "@/components/data-table/filters";
import { useTableSearch } from "@/components/data-table/use-table-search";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { Pill, StatusDot, StatusLabel } from "@/components/status";
import { useAccess } from "@/hooks/use-access";
import { useAcls } from "@/lib/api/catalog";
import type { Acl } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { formatEnumLabel } from "@/lib/format";
import {
  ACL_OPERATIONS,
  ACL_PATTERNS,
  ACL_RESOURCE_TYPES,
  searchDefaults,
  aclsSearch,
  type AclFilter,
  type AclsSearch,
} from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/acls")({
  validateSearch: aclsSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(aclsSearch))] },
  component: AclsPage,
});

const EMPTY_BINDINGS: Acl[] = [];
const DISABLED = "Authorization is disabled on this cluster.";
const DENIED = "klens's Kafka user needs DESCRIBE on the cluster to read ACLs.";

function enumOptions(values: readonly string[]) {
  return values.map((value) => ({ value, label: formatEnumLabel(value) }));
}

const FILTERS: Array<FilterField<Acl, AclFilter>> = [
  {
    id: "resource",
    label: "Resource",
    plural: "resources",
    icon: BoxIcon,
    options: enumOptions(ACL_RESOURCE_TYPES),
    accessor: (acl) => acl.resourceType,
  },
  {
    id: "operation",
    label: "Operation",
    plural: "operations",
    icon: ZapIcon,
    options: enumOptions(ACL_OPERATIONS),
    accessor: (acl) => acl.operation,
  },
  {
    id: "permission",
    label: "Permission",
    plural: "permissions",
    icon: ShieldIcon,
    options: [
      { value: "ALLOW", label: "Allow", icon: <StatusDot tone="ok" /> },
      { value: "DENY", label: "Deny", icon: <StatusDot tone="error" /> },
    ],
    accessor: (acl) => acl.permission,
  },
  {
    id: "pattern",
    label: "Pattern",
    plural: "patterns",
    icon: AsteriskIcon,
    options: enumOptions(ACL_PATTERNS),
    accessor: (acl) => acl.patternType,
  },
];

const columnHelper = createColumnHelper<DataTableFeatures, Acl>();

const columns = columnHelper.columns([
  columnHelper.accessor("resourceType", {
    header: "Resource",
    meta: { width: "9rem" },
    cell: ({ getValue }) => <Pill>{formatEnumLabel(getValue())}</Pill>,
  }),
  columnHelper.accessor("resourceName", {
    header: "Name",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("patternType", {
    header: "Pattern",
    meta: { width: "6rem" },
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{formatEnumLabel(getValue())}</span>
    ),
  }),
  columnHelper.accessor("principal", {
    header: "Principal",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("host", {
    header: "Host",
    meta: { width: "8rem" },
    cell: ({ getValue }) => <span className="font-mono text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("operation", {
    header: "Operation",
    meta: { width: "10rem" },
    cell: ({ getValue }) => formatEnumLabel(getValue()),
  }),
  columnHelper.accessor("permission", {
    header: "Permission",
    meta: { width: "7rem" },
    cell: ({ getValue }) => (
      <StatusLabel tone={getValue() === "DENY" ? "error" : "ok"}>
        {formatEnumLabel(getValue())}
      </StatusLabel>
    ),
  }),
]);

function lengthPrefixed(parts: string[]): string {
  return parts.map((part) => `${part.length}:${part}`).join("");
}

function aclRowId(acl: Acl): string {
  return lengthPrefixed([
    acl.resourceType,
    acl.resourceName,
    acl.patternType,
    acl.principal,
    acl.host,
    acl.operation,
    acl.permission,
  ]);
}

function aclMatches(acl: Acl, needle: string) {
  return [
    acl.resourceName,
    acl.principal,
    acl.host,
    acl.resourceType,
    acl.patternType,
    acl.operation,
    acl.permission,
  ]
    .join(" ")
    .toLowerCase()
    .includes(needle);
}

function AclsPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const search = Route.useSearch();

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
    fields: FILTERS,
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
    </div>
  );
}
