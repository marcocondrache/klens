import { useEffect, useRef, useState, type ReactNode } from "react";
import {
  useTable,
  type ColumnDef,
  type ColumnVisibilityState,
  type RowData,
} from "@tanstack/react-table";

import { DataTableViewOptions } from "@/components/data-table/view-options";
import { features, type DataTableFeatures } from "@/components/data-table/features";
import { RefreshBar } from "@/components/refresh-bar";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

interface RecordTableProps<TData extends RowData> {
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
  hasNextPage?: boolean;
  fetchNextPage?: () => void;
  isFetchingNextPage?: boolean;
  isFetchNextPageError?: boolean;
}

function tablePlaceholder(content: ReactNode) {
  if (typeof content === "string") {
    return <p className="py-10 text-center text-sm text-muted-foreground">{content}</p>;
  }

  return content;
}

export function RecordTable<TData extends RowData>({
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
  hasNextPage = false,
  fetchNextPage,
  isFetchingNextPage = false,
  isFetchNextPageError = false,
}: RecordTableProps<TData>) {
  const [columnVisibility, setColumnVisibility] = useState<ColumnVisibilityState>({});
  const sentinelRef = useRef<HTMLTableRowElement>(null);

  const table = useTable({
    features,
    data,
    columns,
    getRowId,
    enableSorting: false,
    onColumnVisibilityChange: setColumnVisibility,
    state: { columnVisibility },
  });

  const rows = table.getRowModel().rows;
  const columnCount = table.getVisibleLeafColumns().length || columns.length;
  const showLoader = hasNextPage || isFetchingNextPage || isFetchNextPageError;

  useEffect(() => {
    const node = sentinelRef.current;
    if (
      node == null ||
      fetchNextPage == null ||
      !hasNextPage ||
      isFetchingNextPage ||
      isFetchNextPageError
    ) {
      return;
    }

    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) fetchNextPage();
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, [fetchNextPage, hasNextPage, isFetchingNextPage, isFetchNextPageError, rows.length]);

  const showSkeleton = loading || (rows.length === 0 && hasNextPage && error == null);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-4">
      <div className="flex shrink-0 flex-wrap items-center gap-3">
        {toolbar}
        <DataTableViewOptions table={table} />
      </div>
      <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border">
        {refreshing ? <RefreshBar className="absolute inset-x-0 top-0 z-20" /> : null}
        <div className="min-h-0 flex-1 overflow-auto">
          <Table className="w-fit">
            <TableHeader className="[&_tr]:border-b-0!">
              {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id}>
                  {headerGroup.headers.map((header) => {
                    const meta = header.column.columnDef.meta;

                    return (
                      <TableHead
                        key={header.id}
                        className={cn(
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
              {showSkeleton ? (
                Array.from({ length: 6 }, (_, index) => (
                  <TableRow key={index} className="hover:bg-transparent">
                    {table.getVisibleLeafColumns().map((column) => (
                      <TableCell key={column.id}>
                        <Skeleton className="h-4 w-16" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : rows.length === 0 ? (
                <TableRow className="hover:bg-transparent">
                  <TableCell
                    colSpan={columnCount}
                    className={error || emptyState ? "p-0" : "h-24 text-center"}
                  >
                    {error || emptyState ? (
                      tablePlaceholder(error ?? emptyState)
                    ) : (
                      <p className="py-10 text-center text-sm text-muted-foreground">No results.</p>
                    )}
                  </TableCell>
                </TableRow>
              ) : (
                rows.map((row) => (
                  <TableRow
                    key={row.id}
                    data-state={selectedKey === row.id ? "selected" : undefined}
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
                ))
              )}
              {showLoader && !showSkeleton ? (
                <TableRow ref={sentinelRef} className="hover:bg-transparent">
                  <TableCell colSpan={columnCount} className="text-center text-muted-foreground">
                    {isFetchNextPageError ? (
                      <button
                        type="button"
                        className="underline-offset-2 hover:underline"
                        onClick={() => fetchNextPage?.()}
                      >
                        Couldn't load more. Retry
                      </button>
                    ) : (
                      <span className="inline-flex items-center gap-2">
                        <Spinner />
                        Loading more…
                      </span>
                    )}
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  );
}
