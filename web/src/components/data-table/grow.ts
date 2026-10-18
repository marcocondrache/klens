import type { Column, RowData } from "@tanstack/react-table";

import type { DataTableFeatures } from "./features";

// max-w-0 stops a growing cell's content from widening its column, so it truncates instead.
export const GROW_CELL = "max-w-0 min-w-32";

export function growWidths<TData extends RowData>(
  columns: Array<Column<DataTableFeatures, TData, unknown>>,
) {
  const growing = columns.filter((column) => column.columnDef.meta?.grow).length;
  const share = growing ? `${100 / growing}%` : undefined;

  return columns.map((column) => ({
    id: column.id,
    width: column.columnDef.meta?.grow ? share : undefined,
  }));
}
