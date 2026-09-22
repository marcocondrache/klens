import { useMemo } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";
import { useQueryStates } from "nuqs";
import { AsteriskIcon, BoxIcon, ShieldIcon, ZapIcon } from "lucide-react";

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
import { Pill, StatusDot } from "@/components/status";
import { useAccess } from "@/hooks/use-access";
import { useAcls } from "@/lib/api/live";
import type { Acl } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { formatEnumLabel } from "@/lib/format";
import { ACL_OPERATIONS, ACL_PATTERNS, ACL_RESOURCE_TYPES, aclsSearch } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/acls")({
  component: AclsPage,
});

const EMPTY_BINDINGS: Acl[] = [];

function enumOptions(values: readonly string[]) {
  return values.map((value) => ({ value, label: formatEnumLabel(value) }));
}

const FILTERS: Array<FilterField<Acl>> = [
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
      { value: "DENY", label: "Deny", icon: <StatusDot tone="warn" /> },
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
    header: ({ column }) => <DataTableColumnHeader column={column} title="Resource" />,
    cell: ({ getValue }) => <Pill>{formatEnumLabel(getValue())}</Pill>,
  }),
  columnHelper.accessor("resourceName", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Name" />,
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("patternType", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Pattern" />,
    cell: ({ getValue }) => formatEnumLabel(getValue()),
  }),
  columnHelper.accessor("principal", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Principal" />,
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("host", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Host" />,
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("operation", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Operation" />,
    cell: ({ getValue }) => formatEnumLabel(getValue()),
  }),
  columnHelper.accessor("permission", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Permission" />,
    cell: ({ getValue }) => (
      <Pill tone={getValue() === "DENY" ? "warn" : "ok"}>{formatEnumLabel(getValue())}</Pill>
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

function AclsPage() {
  const cluster = useClusterName();
  const [search, setSearch] = useQueryStates(aclsSearch);
  const { q: term } = search;
  const filters = readFilters(FILTERS, search);
  const { can } = useAccess();
  const canAcls = can(cluster, "ACLS");
  const { data, isPending, isError, error } = useAcls(cluster, canAcls);
  const disabled = data?.authorizer === "DISABLED";
  const bindings = data?.bindings ?? EMPTY_BINDINGS;

  function setFilters(rules: FilterRule[]) {
    void setSearch(filterParams(FILTERS, rules));
  }

  const searched = useMemo(() => {
    const needle = term.trim().toLowerCase();
    if (!needle) return bindings;

    return bindings.filter((acl) =>
      [
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
        .includes(needle),
    );
  }, [bindings, term]);

  const rows = applyFilters(searched, FILTERS, filters);

  if (!canAcls) {
    return <PageHeader title="ACLs" description="ACL bindings are not available for your role." />;
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="ACLs"
        description={
          disabled ? "Authorization is disabled on this cluster." : `${rows.length} bindings`
        }
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={aclRowId}
        toolbar={
          <>
            <SearchField
              value={term}
              onChange={(event) => void setSearch({ q: event.target.value })}
              placeholder="Search ACLs…"
            />

            <FilterBar fields={FILTERS} rows={searched} value={filters} onChange={setFilters} />
          </>
        }
        loading={isPending}
        error={isError ? apiErrorMessage(error, "Failed to load ACLs.") : undefined}
        emptyState={disabled ? "Authorization is disabled on this cluster." : "No ACL bindings."}
        defaultSort={{ id: "resourceName", direction: "asc" }}
        fill
      />
    </div>
  );
}
