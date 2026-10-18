import type { Column, RowData } from "@tanstack/react-table";

import type { DataTableFeatures } from "./features";

const MIN_FLEX_WIDTH = 192;

export function columnWidth<TData extends RowData>(
  column: Column<DataTableFeatures, TData, unknown>,
) {
  return column.columnDef.meta?.grow ? undefined : column.getSize();
}

export function minTableWidth<TData extends RowData>(
  columns: Array<Column<DataTableFeatures, TData, unknown>>,
) {
  return columns.reduce((total, column) => total + (columnWidth(column) ?? MIN_FLEX_WIDTH), 0);
}
