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
import { DataTable, type Column } from "@/components/data-table";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { useAcls } from "@/lib/api/queries";
import { useClusterName } from "@/lib/clusters";
import type { Acl } from "@/lib/api/types";

const RESOURCE_TYPES = ["TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID"];

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

  const columns: Array<Column<Acl>> = [
    {
      id: "principal",
      header: "Principal",
      sortValue: (entry) => entry.principal,
      cell: (entry) => <span className="font-mono text-sm">{entry.principal}</span>,
    },
    {
      id: "resourceType",
      header: "Resource type",
      sortValue: (entry) => entry.resourceType,
      cell: (entry) => <Pill>{entry.resourceType.toLowerCase()}</Pill>,
    },
    {
      id: "resourceName",
      header: "Resource",
      sortValue: (entry) => entry.resourceName,
      cell: (entry) => (
        <span className="font-mono text-sm">
          {entry.resourceName}
          {entry.patternType === "PREFIXED" ? (
            <span className="text-muted-foreground">*</span>
          ) : null}
        </span>
      ),
    },
    {
      id: "pattern",
      header: "Pattern",
      sortValue: (entry) => entry.patternType,
      cell: (entry) => <span>{entry.patternType.toLowerCase()}</span>,
    },
    {
      id: "operation",
      header: "Operation",
      sortValue: (entry) => entry.operation,
      cell: (entry) => entry.operation,
    },
    {
      id: "host",
      header: "Host",
      align: "right",
      cell: (entry) => <span className="font-mono text-sm">{entry.host}</span>,
    },
    {
      id: "permission",
      header: "Permission",
      align: "right",
      sortValue: (entry) => entry.permission,
      cell: (entry) => (
        <Pill tone={entry.permission === "ALLOW" ? "ok" : "error"}>{entry.permission}</Pill>
      ),
    },
  ];

  return (
    <div className="space-y-5">
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
        rows={rows}
        rowKey={(entry) =>
          `${entry.principal}-${entry.resourceType}-${entry.resourceName}-${entry.operation}`
        }
        loading={isPending}
        defaultSort={{ id: "principal", direction: "asc" }}
        pageSize={30}
      />
    </div>
  );
}
