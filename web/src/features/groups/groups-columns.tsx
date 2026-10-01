import { createColumnHelper } from "@tanstack/react-table";

import type { DataTableFeatures } from "@/components/data-table/features";
import { GroupStateBadge, LagValue, Pill } from "@/components/status";
import type { GroupRow } from "@/lib/api/types";

const columnHelper = createColumnHelper<DataTableFeatures, GroupRow>();

export const groupColumns = columnHelper.columns([
  columnHelper.accessor("id", {
    header: "Group",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("state", {
    header: "State",
    meta: { width: "12rem" },
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  columnHelper.accessor("memberCount", {
    id: "members",
    header: "Members",
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor((group) => group.topicNames.length, {
    id: "topics",
    header: "Topics",
    cell: ({ row }) => (
      <span className="flex items-center gap-1.5">
        {row.original.topicNames.slice(0, 1).map((topic) => (
          <span key={topic} className="truncate font-mono text-muted-foreground">
            {topic}
          </span>
        ))}
        {row.original.topicNames.length > 1 ? (
          <Pill className="shrink-0">+{row.original.topicNames.length - 1}</Pill>
        ) : null}
      </span>
    ),
  }),
  columnHelper.accessor((group) => group.totalLag ?? -1, {
    id: "lag",
    header: "Lag",
    meta: { align: "right", width: "9rem" },
    cell: ({ row }) => <LagValue lag={row.original.totalLag} complete={row.original.lagComplete} />,
  }),
]);
