import { useState, type ReactNode } from "react";
import { useTable, type ColumnDef, type RowData } from "@tanstack/react-table";

import { features, type DataTableFeatures } from "./features";
import { StaticTable } from "./static-table";
import { VirtualTable } from "./virtual-table";
import { DataTableViewport } from "./viewport";

// Small tables render every row so their columns can size to their content.
const VIRTUALIZE_ABOVE_ROWS = 100;

interface DataTableProps<TData extends RowData> {
  columns: Array<ColumnDef<DataTableFeatures, TData>>;
  data: TData[];
  getRowId: (row: TData) => string;
  toolbar?: ReactNode;
  onRowClick?: (row: TData) => void;
  loading?: boolean;
  error?: ReactNode;
  emptyState?: ReactNode;
  defaultSort?: { id: string; direction: "asc" | "desc" };
}

export function DataTable<TData extends RowData>({
  columns,
  data,
  getRowId,
  toolbar,
  onRowClick,
  loading = false,
  error,
  emptyState,
  defaultSort,
}: DataTableProps<TData>) {
  const table = useTable({
    features,
    data,
    columns,
    getRowId,
    enableMultiSort: false,
    sortDescFirst: false,
    initialState: {
      sorting: defaultSort ? [{ id: defaultSort.id, desc: defaultSort.direction === "desc" }] : [],
    },
  });

  const rows = table.getRowModel().rows;
  const [viewport, setViewport] = useState<HTMLDivElement | null>(null);
  const view = {
    table,
    rows,
    loading,
    placeholder: error ?? emptyState ?? "No results.",
    onRowClick,
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {toolbar ? <div className="flex shrink-0 flex-wrap items-center gap-2">{toolbar}</div> : null}
      <DataTableViewport ref={setViewport} aria-busy={loading || undefined}>
        {rows.length > VIRTUALIZE_ABOVE_ROWS ? (
          <VirtualTable {...view} viewport={viewport} />
        ) : (
          <StaticTable {...view} />
        )}
      </DataTableViewport>
    </div>
  );
}
