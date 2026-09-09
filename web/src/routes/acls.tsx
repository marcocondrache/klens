import { useMemo } from "react";
import { SearchIcon } from "lucide-react";
import { useSearchParams } from "react-router";

import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DataTable } from "@/components/data-table";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { useAcls } from "@/lib/api/queries";
import { useClusterName } from "@/lib/clusters";
import type { Acl } from "@/lib/api/types";
import { createAppColumnHelper } from "@/lib/table";

const RESOURCE_TYPES = ["TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID"];

const columnHelper = createAppColumnHelper<Acl>();

const columns = columnHelper.columns([
  columnHelper.accessor("principal", {
    header: "Principal",
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("resourceType", {
    header: "Resource type",
    cell: ({ getValue }) => <Pill>{getValue().toLowerCase()}</Pill>,
  }),
  columnHelper.accessor("resourceName", {
    header: "Resource",
    cell: ({ row }) => (
      <span className="font-mono text-sm">
        {row.original.resourceName}
        {row.original.patternType === "PREFIXED" ? (
          <span className="text-muted-foreground">*</span>
        ) : null}
      </span>
    ),
  }),
  columnHelper.accessor("patternType", {
    id: "pattern",
    header: "Pattern",
    cell: ({ getValue }) => <span>{getValue().toLowerCase()}</span>,
  }),
  columnHelper.accessor("operation", {
    header: "Operation",
  }),
  columnHelper.display({
    id: "host",
    header: "Host",
    meta: { align: "right" },
    cell: ({ row }) => <span className="font-mono text-sm">{row.original.host}</span>,
  }),
  columnHelper.accessor("permission", {
    header: "Permission",
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <Pill tone={getValue() === "ALLOW" ? "ok" : "error"}>{getValue()}</Pill>
    ),
  }),
]);

export function AclsPage() {
  const cluster = useClusterName();
  const [params, setParams] = useSearchParams();

  const term = params.get("q") ?? "";
  const resource = params.get("resource") ?? "all";

  const { data: entries = [], isPending } = useAcls(cluster);

  function update(key: string, value: string | null) {
    const next = new URLSearchParams(params);
    if (value === null || value === "" || value === "all") {
      next.delete(key);
    } else {
      next.set(key, value);
    }
    setParams(next, { replace: true });
  }

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();

    return entries.filter((entry) => {
      if (resource !== "all" && entry.resourceType !== resource) return false;
      if (needle && !`${entry.principal} ${entry.resourceName}`.toLowerCase().includes(needle)) {
        return false;
      }
      return true;
    });
  }, [entries, term, resource]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader title="ACLs" description={`${rows.length} access control entries`} />

      <div className="flex flex-wrap items-center gap-3">
        <InputGroup className="w-full max-w-sm">
          <InputGroupAddon>
            <SearchIcon />
          </InputGroupAddon>
          <InputGroupInput
            value={term}
            onChange={(event) => update("q", event.target.value)}
            placeholder="Search principals and resources…"
          />
        </InputGroup>

        <Select value={resource} onValueChange={(value) => update("resource", String(value))}>
          <SelectTrigger size="sm" className="w-48">
            <SelectValue placeholder="Resource type" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All resources</SelectItem>
            {RESOURCE_TYPES.map((value) => (
              <SelectItem key={value} value={value}>
                {value.toLowerCase()}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(entry) =>
          `${entry.principal}-${entry.resourceType}-${entry.resourceName}-${entry.operation}`
        }
        loading={isPending}
        defaultSort={{ id: "principal", direction: "asc" }}
        pageSize={30}
        fill
      />
    </div>
  );
}
