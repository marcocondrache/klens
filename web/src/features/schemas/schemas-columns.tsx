import { createColumnHelper } from "@tanstack/react-table";

import type { DataTableFeatures } from "@/components/data-table/features";
import { Pill } from "@/components/status";
import type { SubjectRow } from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";

const columnHelper = createColumnHelper<DataTableFeatures, SubjectRow>();

export const subjectColumns = columnHelper.columns([
  columnHelper.accessor("subject", {
    header: "Subject",
    meta: { grow: true },
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("id", {
    header: "Latest ID",
    size: 96,
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("type", {
    header: "Type",
    size: 104,
    cell: ({ getValue }) => <Pill>{formatEnumLabel(getValue())}</Pill>,
  }),
  columnHelper.accessor("latestVersion", {
    id: "version",
    header: "Latest version",
    size: 128,
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric">v{getValue()}</span>,
  }),
  columnHelper.accessor((subject) => subject.versions.length, {
    id: "versions",
    header: "Versions",
    size: 96,
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("compatibility", {
    header: "Compatibility",
    size: 160,
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <span className={getValue() === "NONE" ? "text-warn" : "text-muted-foreground"}>
        {formatEnumLabel(getValue())}
      </span>
    ),
  }),
]);
