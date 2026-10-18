import { createColumnHelper } from "@tanstack/react-table";

import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { type DataTableFeatures } from "@/components/data-table/features";
import { formatBytes, formatRelative, formatTimestamp } from "@/lib/format";
import type { KafkaRecord } from "@/lib/api/types";
import { cn } from "@/lib/utils";

// The cell truncates long before this, so scanning the whole payload is wasted work.
const PREVIEW_CHARS = 512;

function preview(value: string | null) {
  if (!value) return "—";
  return value.slice(0, PREVIEW_CHARS).replace(/\s+/g, " ").trim();
}

const columnHelper = createColumnHelper<DataTableFeatures, KafkaRecord>();

export const recordColumns = columnHelper.columns([
  columnHelper.accessor("partition", {
    size: 64,
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Part" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("offset", {
    size: 120,
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Offset" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  columnHelper.accessor((record) => record.key ?? "", {
    id: "key",
    size: 192,
    header: ({ column }) => <DataTableColumnHeader column={column} title="Key" />,
    cell: ({ row }) => (
      <span
        className={cn(
          "block max-w-48 truncate font-mono",
          row.original.key == null && "text-muted-foreground/60 italic",
        )}
      >
        {row.original.key ?? "null"}
      </span>
    ),
  }),
  columnHelper.accessor((record) => record.value ?? "", {
    id: "value",
    header: ({ column }) => <DataTableColumnHeader column={column} title="Value" />,
    meta: { grow: true },
    cell: ({ row }) => (
      <span className="block truncate font-mono text-muted-foreground">
        {preview(row.original.value)}
      </span>
    ),
  }),
  columnHelper.accessor("sizeBytes", {
    id: "size",
    size: 96,
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Size" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <span className="numeric text-muted-foreground">{formatBytes(getValue())}</span>
    ),
  }),
  columnHelper.accessor("timestamp", {
    size: 184,
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Timestamp" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <Tooltip>
        <TooltipTrigger
          render={
            <span className="numeric cursor-default whitespace-nowrap text-muted-foreground" />
          }
        >
          {formatTimestamp(getValue())}
        </TooltipTrigger>
        <TooltipContent>{formatRelative(getValue())}</TooltipContent>
      </Tooltip>
    ),
  }),
]);
