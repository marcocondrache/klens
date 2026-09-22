import { useMemo, useState } from "react";
import { EyeOffIcon, LockIcon } from "lucide-react";
import { createColumnHelper } from "@tanstack/react-table";

import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CopyButton } from "@/components/copy-button";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import type { ConfigEntry } from "@/lib/api/types";

const SOURCE_LABEL: Record<ConfigEntry["source"], string> = {
  DYNAMIC_TOPIC_CONFIG: "topic override",
  DYNAMIC_BROKER_CONFIG: "broker override",
  STATIC_BROKER_CONFIG: "static",
  DEFAULT_CONFIG: "default",
};

const columnHelper = createColumnHelper<DataTableFeatures, ConfigEntry>();

const columns = columnHelper.columns([
  columnHelper.accessor("name", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Key" />,
    cell: ({ row }) => {
      const entry = row.original;

      return (
        <span className="flex items-center gap-1.5">
          <span className="font-mono text-sm">{entry.name}</span>
          {entry.readOnly ? (
            <Tooltip>
              <TooltipTrigger render={<LockIcon className="size-3 text-muted-foreground" />} />
              <TooltipContent>Read-only</TooltipContent>
            </Tooltip>
          ) : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("value", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Value" />,
    meta: { className: "whitespace-normal" },
    cell: ({ row }) => {
      const entry = row.original;

      return entry.sensitive ? (
        <span className="flex items-center gap-1.5 text-muted-foreground">
          <EyeOffIcon className="size-3" />
          <span className="text-xs">hidden</span>
        </span>
      ) : (
        <span className="flex items-center gap-1">
          <span className="numeric font-mono text-sm break-all">
            {entry.value === "" ? "—" : entry.value}
          </span>
          {entry.value ? <CopyButton value={entry.value} label="Copy value" /> : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("source", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Source" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <Pill tone={getValue() === "DEFAULT_CONFIG" ? "idle" : "brand"}>
        {SOURCE_LABEL[getValue()]}
      </Pill>
    ),
  }),
]);

export function ConfigTable({
  entries,
  loading = false,
  fill = false,
}: {
  entries: ConfigEntry[];
  loading?: boolean;
  fill?: boolean;
}) {
  const [term, setTerm] = useState("");
  const [onlyOverrides, setOnlyOverrides] = useState(false);

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();

    return entries.filter((entry) => {
      if (onlyOverrides && entry.source === "DEFAULT_CONFIG") return false;
      if (!needle) return true;
      return (
        entry.name.toLowerCase().includes(needle) ||
        (entry.value ?? "").toLowerCase().includes(needle)
      );
    });
  }, [entries, term, onlyOverrides]);

  return (
    <DataTable
      columns={columns}
      data={rows}
      getRowId={(entry) => entry.name}
      toolbar={
        <>
          <SearchField
            className="max-w-xs"
            value={term}
            onChange={(event) => setTerm(event.target.value)}
            placeholder="Filter configuration…"
          />

          <Label className="flex items-center gap-2 text-sm text-muted-foreground">
            <Switch
              size="sm"
              checked={onlyOverrides}
              onCheckedChange={(checked) => setOnlyOverrides(checked)}
            />
            Overrides only
          </Label>
        </>
      }
      loading={loading}
      defaultSort={{ id: "name", direction: "asc" }}
      fill={fill}
    />
  );
}
