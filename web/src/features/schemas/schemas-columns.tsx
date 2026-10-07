import { createColumnHelper } from "@tanstack/react-table";

import type { DataTableFeatures } from "@/components/data-table/features";
import { Pill } from "@/components/status";
import type { SubjectRow } from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";

const columnHelper = createColumnHelper<DataTableFeatures, SubjectRow>();

export const subjectColumns = columnHelper.columns([
  columnHelper.accessor("subject", {
    header: "Subject",
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("id", {
    header: "Latest ID",
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => <span className="numeric text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("type", {
    header: "Type",
    meta: { width: "6.5rem" },
    cell: ({ getValue }) => <Pill>{formatEnumLabel(getValue())}</Pill>,
  }),
  columnHelper.accessor("latestVersion", {
    id: "version",
    header: "Latest version",
    meta: { align: "right", width: "8rem" },
    cell: ({ getValue }) => <span className="numeric">v{getValue()}</span>,
  }),
  columnHelper.accessor((subject) => subject.versions.length, {
    id: "versions",
    header: "Versions",
    meta: { align: "right", width: "6rem" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("compatibility", {
    header: "Compatibility",
    meta: { align: "right", width: "10rem" },
    cell: ({ getValue }) => (
      <span className={getValue() === "NONE" ? "text-warn" : "text-muted-foreground"}>
        {formatEnumLabel(getValue())}
      </span>
    ),
  }),
]);
