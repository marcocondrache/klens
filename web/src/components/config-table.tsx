import { useId, useMemo, useState } from "react";
import { EyeOffIcon, LockIcon, PencilIcon } from "lucide-react";
import { createColumnHelper } from "@tanstack/react-table";

import { Field, FieldLabel } from "@/components/ui/field";
import { Switch } from "@/components/ui/switch";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { IconButton } from "@/components/icon-button";
import { SearchField } from "@/components/search-field";
import { StatusLabel } from "@/components/status";
import type { ConfigEntry } from "@/lib/api/types";

const SOURCE_LABEL: Record<ConfigEntry["source"], string> = {
  DYNAMIC_TOPIC_CONFIG: "topic override",
  DYNAMIC_BROKER_CONFIG: "broker override",
  DYNAMIC_DEFAULT_BROKER_CONFIG: "cluster default",
  STATIC_BROKER_CONFIG: "static",
  DEFAULT_CONFIG: "default",
};

const columnHelper = createColumnHelper<DataTableFeatures, ConfigEntry>();

const columns = columnHelper.columns([
  columnHelper.accessor("name", {
    header: "Key",
    cell: ({ row }) => {
      const entry = row.original;

      return (
        <span className="flex items-center gap-1.5">
          <span className="truncate font-mono">{entry.name}</span>
          {entry.readOnly ? (
            <Tooltip>
              <TooltipTrigger
                render={<LockIcon className="size-3 shrink-0 text-muted-foreground" />}
              />
              <TooltipContent>Read-only</TooltipContent>
            </Tooltip>
          ) : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("value", {
    header: "Value",
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
          <span className="font-mono break-all text-muted-foreground">
            {entry.value === "" ? "—" : entry.value}
          </span>
          {entry.value ? <CopyButton value={entry.value} label="Copy value" reveal /> : null}
        </span>
      );
    },
  }),
  columnHelper.accessor("source", {
    header: "Source",
    meta: { align: "right", width: "9rem" },
    cell: ({ getValue }) => {
      const source = getValue();

      if (source !== "STATIC_BROKER_CONFIG" && source !== "DEFAULT_CONFIG") {
        return <StatusLabel tone="brand">{SOURCE_LABEL[source]}</StatusLabel>;
      }

      return (
        <span
          className={
            source === "DEFAULT_CONFIG" ? "text-muted-foreground/60" : "text-muted-foreground"
          }
        >
          {SOURCE_LABEL[source]}
        </span>
      );
    },
  }),
]);

function editColumn(onEdit: (entry: ConfigEntry) => void) {
  return columnHelper.display({
    id: "edit",
    enableResizing: false,
    meta: { align: "right", width: "3.5rem" },
    cell: ({ row }) =>
      row.original.readOnly ? null : (
        <IconButton
          label={`Edit ${row.original.name}`}
          tooltip="Edit"
          reveal
          onClick={(event) => {
            event.stopPropagation();
            onEdit(row.original);
          }}
        >
          <PencilIcon />
        </IconButton>
      ),
  });
}

/** With `onEdit`, a click on a writable row opens it for editing. */
export function ConfigTable({
  entries,
  loading = false,
  onEdit,
}: {
  entries: ConfigEntry[];
  loading?: boolean;
  onEdit?: (entry: ConfigEntry) => void;
}) {
  const [term, setTerm] = useState("");
  const [onlyOverrides, setOnlyOverrides] = useState(false);
  const overridesId = useId();

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

  const tableColumns = useMemo(
    () => (onEdit ? [...columns, editColumn(onEdit)] : columns),
    [onEdit],
  );

  return (
    <DataTable
      columns={tableColumns}
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

          <Field orientation="horizontal" className="w-fit">
            <Switch
              id={overridesId}
              size="sm"
              checked={onlyOverrides}
              onCheckedChange={(checked) => setOnlyOverrides(checked)}
            />
            <FieldLabel htmlFor={overridesId} className="text-sm font-normal">
              Overrides only
            </FieldLabel>
          </Field>
        </>
      }
      loading={loading}
      defaultSort={{ id: "name", direction: "asc" }}
      onRowClick={
        onEdit
          ? (entry) => {
              if (!entry.readOnly) onEdit(entry);
            }
          : undefined
      }
    />
  );
}
