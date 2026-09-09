import type { ReactNode } from "react";
import { ArrowDownIcon, ArrowUpIcon, ChevronLeftIcon, ChevronRightIcon } from "lucide-react";
import type { RowData } from "@tanstack/react-table";

import { Button } from "@/components/ui/button";
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
import { type AppColumnDef, useAppTable } from "@/lib/table";
import { cn } from "@/lib/utils";

export type { AppColumnDef };

function tablePlaceholder(content: ReactNode) {
  if (typeof content === "string") {
    return <p className="py-10 text-center text-sm text-muted-foreground">{content}</p>;
  }

  return content;
}

interface DataTableProps<TData extends RowData> {
  columns: Array<AppColumnDef<TData>>;
  data: TData[];
  getRowId: (row: TData) => string;
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
  fill?: boolean;
}

export function DataTable<TData extends RowData>({
  columns,
  data,
  getRowId,
  onRowClick,
  selectedKey,
  loading = false,
  refreshing = false,
  error,
  emptyState,
  defaultSort,
  pageSize = 25,
  pageIndex = 0,
  hasMore = false,
  canPreviousPage = false,
  onPreviousPage,
  onNextPage,
  fill = false,
}: DataTableProps<TData>) {
  const serverPaging = onNextPage != null;
  const sorting = defaultSort
    ? [{ id: defaultSort.id, desc: defaultSort.direction === "desc" }]
    : [];

  const table = useAppTable(
    {
      columns,
      data,
      getRowId,
      enableSorting: !serverPaging,
      initialState: {
        sorting,
        ...(serverPaging ? {} : { pagination: { pageIndex: 0, pageSize } }),
      },
      ...(serverPaging
        ? {
            manualPagination: true,
            pageCount: -1,
            state: {
              pagination: { pageIndex, pageSize },
            },
          }
        : {}),
    },
    (state) => ({
      sorting: state.sorting,
      pagination: state.pagination,
    }),
  );

  const rows = table.getRowModel().rows;
  const pagination = table.state.pagination;
  const pageCount = table.getPageCount();
  const showPager = serverPaging ? canPreviousPage || hasMore : pageCount > 1;

  return (
    <div className={cn(fill ? "flex min-h-0 flex-1 flex-col gap-3" : "space-y-3")}>
      <div
        className={cn(
          "relative overflow-hidden rounded-xl border bg-card",
          fill && "flex min-h-0 flex-col",
        )}
      >
        {refreshing ? <RefreshBar className="absolute inset-x-0 top-0 z-20" /> : null}
        <Table containerClassName={cn(fill && "min-h-0 flex-1 overflow-auto")}>
          <TableHeader className={cn(fill && "[&_tr]:border-b-0!")}>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} className="hover:bg-transparent">
                {headerGroup.headers.map((header) => {
                  const meta = header.column.columnDef.meta;
                  const sorted = header.column.getIsSorted();

                  return (
                    <TableHead
                      key={header.id}
                      className={cn(
                        "h-10 bg-muted/40 text-sm font-medium tracking-wide text-muted-foreground",
                        fill &&
                          "sticky top-0 z-10 bg-card bg-linear-to-b from-muted/40 to-muted/40 shadow-[inset_0_-1px_0_0_var(--color-border)]",
                        meta?.align === "right" && "text-right",
                        header.column.getCanSort() &&
                          "cursor-pointer select-none hover:text-foreground",
                        meta?.headerClassName,
                      )}
                      onClick={header.column.getToggleSortingHandler()}
                    >
                      {header.isPlaceholder ? null : (
                        <span
                          className={cn(
                            "inline-flex items-center gap-1",
                            meta?.align === "right" && "flex-row-reverse",
                          )}
                        >
                          <table.FlexRender header={header} />
                          {sorted === "asc" ? (
                            <ArrowUpIcon className="size-3" />
                          ) : sorted === "desc" ? (
                            <ArrowDownIcon className="size-3" />
                          ) : null}
                        </span>
                      )}
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
                    <TableCell key={columnIndex} className="py-2.5">
                      <Skeleton className="h-4 w-full max-w-32" />
                    </TableCell>
                  ))}
                </TableRow>
              ))
            ) : rows.length === 0 ? (
              <TableRow className="hover:bg-transparent">
                <TableCell colSpan={columns.length} className="p-0">
                  {tablePlaceholder(error ?? emptyState ?? "No results.")}
                </TableCell>
              </TableRow>
            ) : (
              rows.map((row, index) => {
                const selected = selectedKey === row.id;

                return (
                  <TableRow
                    key={row.id}
                    data-state={selected ? "selected" : undefined}
                    onClick={onRowClick ? () => onRowClick(row.original) : undefined}
                    className={cn(
                      "border-border",
                      index % 2 === 1 && !selected && "bg-muted/35",
                      onRowClick && "cursor-pointer",
                    )}
                  >
                    {row.getAllCells().map((cell) => {
                      const meta = cell.column.columnDef.meta;

                      return (
                        <TableCell
                          key={cell.id}
                          className={cn(
                            "py-2.5 text-sm",
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

      {showPager ? (
        <div
          className={cn(
            "flex items-center justify-between px-1 text-sm text-muted-foreground",
            fill && "shrink-0",
          )}
        >
          <span className="numeric">
            {pagination.pageIndex * pagination.pageSize + (rows.length > 0 ? 1 : 0)}
            {rows.length > 0 ? `–${pagination.pageIndex * pagination.pageSize + rows.length}` : ""}
            {serverPaging ? (hasMore ? "+" : "") : ` of ${table.getRowCount()}`}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="icon-xs"
              disabled={serverPaging ? !canPreviousPage : !table.getCanPreviousPage()}
              onClick={serverPaging ? onPreviousPage : () => table.previousPage()}
              aria-label="Previous page"
            >
              <ChevronLeftIcon />
            </Button>
            <span className="numeric px-2">
              {serverPaging
                ? `Page ${pageIndex + 1}`
                : `${pagination.pageIndex + 1} / ${pageCount}`}
            </span>
            <Button
              variant="outline"
              size="icon-xs"
              disabled={serverPaging ? !hasMore : !table.getCanNextPage()}
              onClick={serverPaging ? onNextPage : () => table.nextPage()}
              aria-label="Next page"
            >
              <ChevronRightIcon />
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
