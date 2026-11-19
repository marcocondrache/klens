import { createColumnHelper } from "@tanstack/react-table";

import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
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

// Headers stay plain strings so the column menu can reuse them as labels.
export const recordColumns = columnHelper.columns([
  columnHelper.accessor("partition", {
    size: 64,
    header: "Part",
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("offset", {
    size: 120,
    header: "Offset",
    enableHiding: false,
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
  }),
  columnHelper.accessor((record) => record.key ?? "", {
    id: "key",
    size: 192,
    header: "Key",
    cell: ({ row }) => (
      <span
        className={cn(
          "block max-w-48 truncate font-mono",
          row.original.key == null && "text-muted-foreground italic",
        )}
      >
        {row.original.key ?? "null"}
      </span>
    ),
  }),
  columnHelper.accessor((record) => record.value ?? "", {
    id: "value",
    header: "Value",
    enableHiding: false,
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
    header: "Size",
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <span className="numeric text-muted-foreground">{formatBytes(getValue())}</span>
    ),
  }),
  columnHelper.accessor("timestamp", {
    size: 184,
    header: "Timestamp",
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
