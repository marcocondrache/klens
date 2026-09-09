import {
  createPaginatedRowModel,
  createSortedRowModel,
  createTableHook,
  metaHelper,
  rowPaginationFeature,
  rowSortingFeature,
  sortFn_alphanumeric,
  sortFn_datetime,
  sortFn_text,
  tableFeatures,
} from "@tanstack/react-table";
import type { ColumnDef, RowData } from "@tanstack/react-table";

export interface DataTableColumnMeta {
  align?: "left" | "right";
  className?: string;
  headerClassName?: string;
}

export const tableFeatureSet = tableFeatures({
  rowSortingFeature,
  rowPaginationFeature,
  sortedRowModel: createSortedRowModel(),
  paginatedRowModel: createPaginatedRowModel(),
  sortFns: {
    alphanumeric: sortFn_alphanumeric,
    datetime: sortFn_datetime,
    text: sortFn_text,
  },
  columnMeta: metaHelper<DataTableColumnMeta>(),
});

export const { useAppTable, createAppColumnHelper } = createTableHook({
  features: tableFeatureSet,
  enableMultiSort: false,
  sortDescFirst: false,
});

export type AppColumnDef<TData extends RowData> = ColumnDef<typeof tableFeatureSet, TData>;
