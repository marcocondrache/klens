import { createColumnHelper } from "@tanstack/react-table";

import { CopyButton } from "@/components/copy-button";
import type { DataTableFeatures } from "@/components/data-table/features";
import { Pill } from "@/components/status";
import type { GroupMember } from "@/lib/api/types";

const columnHelper = createColumnHelper<DataTableFeatures, GroupMember>();

export const memberColumns = columnHelper.columns([
  columnHelper.accessor("clientId", {
    header: "Client ID",
    meta: { grow: true },
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("id", {
    header: "Member ID",
    size: 320,
    cell: ({ row }) => (
      <span className="flex items-center gap-1">
        <span className="max-w-72 truncate font-mono text-muted-foreground">{row.original.id}</span>
        <CopyButton value={row.original.id} label="Copy member ID" reveal />
      </span>
    ),
  }),
  columnHelper.accessor("host", {
    header: "Host",
    size: 144,
    cell: ({ getValue }) => <span className="font-mono text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("assignments", {
    header: "Assignments",
    meta: { grow: true },
    cell: ({ row }) => (
      <span className="flex flex-wrap gap-1">
        {row.original.assignments.map((assignment) => (
          <Pill key={assignment.topic} className="max-w-full font-mono font-normal text-foreground">
            <span className="truncate">{assignment.topic}</span>
            <span className="shrink-0 text-muted-foreground">×{assignment.partitions.length}</span>
          </Pill>
        ))}
      </span>
    ),
  }),
  columnHelper.accessor(
    (member) =>
      member.assignments.reduce((sum, assignment) => sum + assignment.partitions.length, 0),
    {
      id: "partitions",
      header: "Partitions",
      size: 112,
      meta: { align: "right" },
      cell: ({ getValue }) => getValue(),
    },
  ),
]);
