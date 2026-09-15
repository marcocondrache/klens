import { useMemo } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useAcls } from "@/lib/api/live";
import type { Acl, AclResourceType } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";
import { formatEnumLabel } from "@/lib/format";
import { ACL_RESOURCE_TYPES, parseAclsSearch } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/acls")({
  validateSearch: parseAclsSearch,
  component: AclsPage,
});

const EMPTY_BINDINGS: Acl[] = [];

const RESOURCE_ITEMS = [
  { value: "all", label: "All resources" },
  ...ACL_RESOURCE_TYPES.map((value) => ({ value, label: formatEnumLabel(value) })),
];

const columnHelper = createColumnHelper<DataTableFeatures, Acl>();

const columns = columnHelper.columns([
  columnHelper.accessor("resourceType", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Resource" />,
    meta: { label: "Resource" },
    cell: ({ getValue }) => <Pill>{formatEnumLabel(getValue())}</Pill>,
  }),
  columnHelper.accessor("resourceName", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Name" />,
    meta: { label: "Name" },
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("patternType", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Pattern" />,
    meta: { label: "Pattern" },
    cell: ({ getValue }) => formatEnumLabel(getValue()),
  }),
  columnHelper.accessor("principal", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Principal" />,
    meta: { label: "Principal" },
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("host", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Host" />,
    meta: { label: "Host" },
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("operation", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Operation" />,
    meta: { label: "Operation" },
    cell: ({ getValue }) => formatEnumLabel(getValue()),
  }),
  columnHelper.accessor("permission", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Permission" />,
    meta: { label: "Permission" },
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
  const navigate = Route.useNavigate();
  const { q: term = "", resource = "all" } = Route.useSearch();
  const { data, isPending, isError, error } = useAcls(cluster);
  const disabled = data?.authorizer === "DISABLED";
  const bindings = data?.bindings ?? EMPTY_BINDINGS;

  function update(key: "q" | "resource", value: string | null) {
    void navigate({
      to: ".",
      search: (prev) => {
        const next = { ...prev };
        if (value === null || value === "" || value === "all") {
          delete next[key];
        } else if (key === "q") {
          next.q = value;
        } else if (ACL_RESOURCE_TYPES.includes(value as AclResourceType)) {
          next.resource = value as AclResourceType;
        }
        return next;
      },
      replace: true,
      resetScroll: false,
    });
  }

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();

    return bindings.filter((acl) => {
      if (resource !== "all" && acl.resourceType !== resource) return false;
      if (!needle) return true;
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
    });
  }, [bindings, resource, term]);

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
              onChange={(event) => update("q", event.target.value)}
              placeholder="Search ACLs…"
            />

            <Select
              value={resource}
              items={RESOURCE_ITEMS}
              onValueChange={(value) => update("resource", String(value))}
            >
              <SelectTrigger size="sm" className="w-48">
                <SelectValue placeholder="Resource" />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {RESOURCE_ITEMS.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </>
        }
        loading={isPending}
        error={
          isError ? (error instanceof Error ? error.message : "Failed to load ACLs.") : undefined
        }
        emptyState={disabled ? "Authorization is disabled on this cluster." : "No ACL bindings."}
        defaultSort={{ id: "resourceName", direction: "asc" }}
        fill
      />
    </div>
  );
}
