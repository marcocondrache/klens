import { createContext, useContext, type CSSProperties, type ReactNode } from "react";
import type { Column, ReactTable, Row, RowData } from "@tanstack/react-table";

import type { DataTableFeatures } from "./features";

export interface TableViewProps<TData extends RowData> {
  table: ReactTable<DataTableFeatures, TData>;
  rows: Array<Row<DataTableFeatures, TData>>;
  loading: boolean;
  placeholder: ReactNode;
  onRowClick?: (row: TData) => void;
  selectedRowId?: string;
  stale?: boolean;
  footer?: ReactNode;
}

export type TableLayout = "static" | "virtual";

export const TableLayoutContext = createContext<TableLayout>("static");

export function useTableLayout() {
  return useContext(TableLayoutContext);
}

export const ROW_LAYOUT: Record<TableLayout, string> = {
  static: "h-10",
  virtual: "flex min-h-10",
};

export function isGrowing<TData extends RowData>(
  column: Column<DataTableFeatures, TData, unknown>,
) {
  return column.columnDef.meta?.grow === true;
}

export function cellLayout<TData extends RowData>(
  column: Column<DataTableFeatures, TData, unknown>,
  layout: TableLayout,
): { className?: string; style?: CSSProperties } {
  const grow = isGrowing(column);

  // min-w-0 keeps a flex cell at its column size when its content is wider.
  if (layout === "virtual") {
    return {
      className: "min-w-0 content-center",
      style: { flex: `${grow ? 1 : 0} 0 ${column.getSize()}px` },
    };
  }

  // max-w-0 stops a growing cell's content from widening its column, so it truncates instead.
  return grow ? { className: "max-w-0", style: { minWidth: column.getSize() } } : {};
}
