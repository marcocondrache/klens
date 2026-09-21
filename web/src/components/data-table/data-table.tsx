import { useEffect, useRef, useState, type ReactNode, type RefObject } from "react";
import {
  useTable,
  type ColumnDef,
  type ColumnFiltersState,
  type ColumnVisibilityState,
  type ReactTable,
  type Row,
  type RowData,
  type SortingState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

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

import { features, type DataTableFeatures } from "./features";
import { DataTableViewOptions } from "./view-options";

const ROW_HEIGHT = 40;
const LOAD_MORE_OVERSCAN = 8;

const measureRow =
  typeof navigator === "undefined" || navigator.userAgent.includes("Firefox")
    ? undefined
    : (element: HTMLTableRowElement) => element.getBoundingClientRect().height;

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
  onLoadMore?: () => void;
  hasMore?: boolean;
  loadingMore?: boolean;
  loadMoreError?: boolean;
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
  onLoadMore,
  hasMore = false,
  loadingMore = false,
  loadMoreError = false,
  fill = false,
}: DataTableProps<TData>) {
  const serverRows = onLoadMore != null;
  const scrollRef = useRef<HTMLDivElement>(null);
  const [sorting, setSorting] = useState<SortingState>(
    defaultSort ? [{ id: defaultSort.id, desc: defaultSort.direction === "desc" }] : [],
  );
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([]);
  const [columnVisibility, setColumnVisibility] = useState<ColumnVisibilityState>({});
  const [rowSelection, setRowSelection] = useState({});

  const table = useTable({
    features,
    data,
    columns,
    getRowId,
    enableMultiSort: false,
    sortDescFirst: false,
    enableSorting: !serverRows,
    onSortingChange: setSorting,
    onColumnFiltersChange: setColumnFilters,
    onColumnVisibilityChange: setColumnVisibility,
    onRowSelectionChange: setRowSelection,
    state: {
      sorting,
      columnFilters,
      columnVisibility,
      rowSelection,
    },
  });

  const rows = table.getRowModel().rows;
  const columnCount = table.getVisibleLeafColumns().length || columns.length;
  const scanning = rows.length === 0 && hasMore && !loading && error == null;

  useEffect(() => {
    if (!scanning || loadingMore || onLoadMore == null) return;
    onLoadMore();
  }, [scanning, loadingMore, onLoadMore]);

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
        <div
          ref={scrollRef}
          className={cn("[overflow-anchor:none]", fill && "min-h-0 flex-1 overflow-auto")}
        >
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
            <TableBody className={rows.length > 0 ? "[&_tr:last-child]:border-b" : undefined}>
              {loading || scanning ? (
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
                    colSpan={columnCount}
                    className={error || emptyState ? "p-0" : "h-24 text-center"}
                  >
                    {error || emptyState ? tablePlaceholder(error ?? emptyState) : "No results."}
                  </TableCell>
                </TableRow>
              ) : (
                <DataTableVirtualRows
                  table={table}
                  rows={rows}
                  columnCount={columnCount}
                  scrollRef={scrollRef}
                  selectedKey={selectedKey}
                  onRowClick={onRowClick}
                  onLoadMore={onLoadMore}
                  hasMore={hasMore}
                  loadingMore={loadingMore}
                  loadMoreError={loadMoreError}
                />
              )}
            </TableBody>
          </Table>
          {rows.length > 0 && (loadingMore || loadMoreError) ? (
            <div className="flex items-center justify-center gap-2 py-3 text-sm text-muted-foreground">
              {loadMoreError ? (
                <button
                  type="button"
                  className="underline-offset-2 hover:underline"
                  onClick={() => onLoadMore?.()}
                >
                  Couldn't load more. Retry
                </button>
              ) : (
                <>
                  <Spinner />
                  Loading more…
                </>
              )}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function SpacerRow({ height, columnCount }: { height: number; columnCount: number }) {
  if (height <= 0) return null;

  return (
    <tr aria-hidden className="pointer-events-none">
      {Array.from({ length: columnCount }, (_, index) => (
        <td key={index} className="border-0 p-0" style={{ height }} />
      ))}
    </tr>
  );
}

function DataTableVirtualRows<TData extends RowData>({
  table,
  rows,
  columnCount,
  scrollRef,
  selectedKey,
  onRowClick,
  onLoadMore,
  hasMore,
  loadingMore,
  loadMoreError,
}: {
  table: ReactTable<DataTableFeatures, TData>;
  rows: Array<Row<DataTableFeatures, TData>>;
  columnCount: number;
  scrollRef: RefObject<HTMLDivElement | null>;
  selectedKey?: string;
  onRowClick?: (row: TData) => void;
  onLoadMore?: () => void;
  hasMore: boolean;
  loadingMore: boolean;
  loadMoreError: boolean;
}) {
  const virtualizer = useVirtualizer({
    count: rows.length,
    estimateSize: () => ROW_HEIGHT,
    getScrollElement: () => scrollRef.current,
    getItemKey: (index) => rows[index]?.id ?? index,
    measureElement: measureRow,
    overscan: LOAD_MORE_OVERSCAN,
  });
  const items = virtualizer.getVirtualItems();
  const lastIndex = items[items.length - 1]?.index;
  const paddingTop = items[0]?.start ?? 0;
  const paddingBottom =
    items.length > 0 ? virtualizer.getTotalSize() - (items[items.length - 1]?.end ?? 0) : 0;

  useEffect(() => {
    if (onLoadMore == null || !hasMore || loadingMore || loadMoreError) return;
    if (lastIndex == null) return;
    if (lastIndex >= rows.length - LOAD_MORE_OVERSCAN) onLoadMore();
  }, [lastIndex, rows.length, hasMore, loadingMore, loadMoreError, onLoadMore]);

  return (
    <>
      <SpacerRow height={paddingTop} columnCount={columnCount} />
      {items.map((item) => {
        const row = rows[item.index];
        if (!row) return null;
        const selected = selectedKey === row.id;

        return (
          <TableRow
            key={row.id}
            data-index={item.index}
            ref={virtualizer.measureElement}
            data-state={selected || row.getIsSelected() ? "selected" : undefined}
            onClick={onRowClick ? () => onRowClick(row.original) : undefined}
            className={cn(onRowClick && "cursor-pointer")}
          >
            {row.getVisibleCells().map((cell) => {
              const meta = cell.column.columnDef.meta;

              return (
                <TableCell
                  key={cell.id}
                  className={cn(meta?.align === "right" && "text-right numeric", meta?.className)}
                >
                  <table.FlexRender cell={cell} />
                </TableCell>
              );
            })}
          </TableRow>
        );
      })}
      <SpacerRow height={paddingBottom} columnCount={columnCount} />
    </>
  );
}
