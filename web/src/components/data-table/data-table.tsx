import { useState, type ReactNode } from "react";
import {
  useTable,
  type ColumnDef,
  type ColumnFiltersState,
  type ColumnVisibilityState,
  type PaginationState,
  type RowData,
  type SortingState,
} from "@tanstack/react-table";

import { RefreshBar } from "@/components/refresh-bar";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

import { features, type DataTableFeatures } from "./features";
import { DataTablePagination, DEFAULT_PAGE_SIZE } from "./pagination";
import { DataTableViewOptions } from "./view-options";

interface DataTableProps<TData extends RowData> {
  columns: Array<ColumnDef<DataTableFeatures, TData>>;
  data: TData[];
  getRowId: (row: TData) => string;
  toolbar?: ReactNode;
  onRowClick?: (row: TData) => void;
  selectedKey?: string;
  loading?: boolean;
  refreshing?: boolean;
  error?: ReactNode;
  emptyState?: ReactNode;
  defaultSort?: { id: string; direction: "asc" | "desc" };
  pageSize?: number;
  pageIndex?: number;
  hasMore?: boolean;
  canPreviousPage?: boolean;
  onPreviousPage?: () => void;
  onNextPage?: () => void;
  loadingMore?: boolean;
  fill?: boolean;
}

function tablePlaceholder(content: ReactNode) {
  if (typeof content === "string") {
    return <p className="py-10 text-center text-sm text-muted-foreground">{content}</p>;
  }

  return content;
}

export function DataTable<TData extends RowData>({
  columns,
  data,
  getRowId,
  toolbar,
  onRowClick,
  selectedKey,
  loading = false,
  refreshing = false,
  error,
  emptyState,
  defaultSort,
  pageSize = DEFAULT_PAGE_SIZE,
  pageIndex = 0,
  hasMore = false,
  canPreviousPage = false,
  onPreviousPage,
  onNextPage,
  loadingMore = false,
  fill = false,
}: DataTableProps<TData>) {
  const serverPaging = onNextPage != null;
  const [sorting, setSorting] = useState<SortingState>(
    defaultSort ? [{ id: defaultSort.id, desc: defaultSort.direction === "desc" }] : [],
  );
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
  const [columnVisibility, setColumnVisibility] = useState<ColumnVisibilityState>({});
  const [rowSelection, setRowSelection] = useState({});
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize,
  });

  const table = useTable({
    features,
    data,
    columns,
    getRowId,
    enableMultiSort: false,
    sortDescFirst: false,
    enableSorting: !serverPaging,
    manualPagination: serverPaging,
    pageCount: serverPaging ? -1 : undefined,
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onColumnVisibilityChange: setColumnVisibility,
    onRowSelectionChange: setRowSelection,
    onPaginationChange: serverPaging ? undefined : setPagination,
    state: {
      sorting,
      columnFilters,
      columnVisibility,
      rowSelection,
      pagination: serverPaging ? { pageIndex, pageSize } : pagination,
    },
  });

  const rows = table.getRowModel().rows;

  return (
    <div className={cn("flex flex-col gap-4", fill && "min-h-0 flex-1")}>
      <div className={cn("flex flex-wrap items-center gap-3", fill && "shrink-0")}>
        {toolbar}
        <DataTableViewOptions table={table} />
      </div>
      <div
        className={cn(
          "relative overflow-hidden rounded-md border",
          fill && "flex min-h-0 flex-1 flex-col",
        )}
      >
        {refreshing ? <RefreshBar className="absolute inset-x-0 top-0 z-20" /> : null}
        <div className={cn(fill && "min-h-0 flex-1 overflow-auto")}>
          <Table>
            <TableHeader className={cn(fill && "[&_tr]:border-b-0!")}>
              {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id}>
                  {headerGroup.headers.map((header) => {
                    const meta = header.column.columnDef.meta;

                    return (
                      <TableHead
                        key={header.id}
                        className={cn(
                          fill &&
                            "sticky top-0 z-10 bg-background shadow-[inset_0_-1px_0_0_var(--color-border)]",
                          meta?.align === "right" && "text-right",
                          meta?.headerClassName,
                        )}
                      >
                        {header.isPlaceholder ? null : <table.FlexRender header={header} />}
                      </TableHead>
                    );
                  })}
                </TableRow>
              ))}
            </TableHeader>
            <TableBody>
              {loading ? (
                Array.from({ length: 6 }, (_, index) => (
                  <TableRow key={index} className="hover:bg-transparent">
                    {columns.map((_, columnIndex) => (
                      <TableCell key={columnIndex}>
                        <Skeleton className="h-4 w-full max-w-32" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : rows.length === 0 ? (
                <TableRow className="hover:bg-transparent">
                  <TableCell
                    colSpan={table.getVisibleLeafColumns().length || columns.length}
                    className={error || emptyState ? "p-0" : "h-24 text-center"}
                  >
                    {error || emptyState ? tablePlaceholder(error ?? emptyState) : "No results."}
                  </TableCell>
                </TableRow>
              ) : (
                rows.map((row) => {
                  const selected = selectedKey === row.id;

                  return (
                    <TableRow
                      key={row.id}
                      data-state={selected || row.getIsSelected() ? "selected" : undefined}
                      onClick={onRowClick ? () => onRowClick(row.original) : undefined}
                      className={cn(onRowClick && "cursor-pointer")}
                    >
                      {row.getVisibleCells().map((cell) => {
                        const meta = cell.column.columnDef.meta;

                        return (
                          <TableCell
                            key={cell.id}
                            className={cn(
                              meta?.align === "right" && "text-right numeric",
                              meta?.className,
                            )}
                          >
                            <table.FlexRender cell={cell} />
                          </TableCell>
                        );
                      })}
                    </TableRow>
                  );
                })
              )}
            </TableBody>
          </Table>
        </div>
      </div>
      <div className={cn(fill && "shrink-0")}>
        <DataTablePagination
          table={table}
          manual={serverPaging}
          hasMore={hasMore}
          canPreviousPage={canPreviousPage}
          onPreviousPage={onPreviousPage}
          onNextPage={onNextPage}
          loadingMore={loadingMore}
        />
      </div>
    </div>
  );
}
