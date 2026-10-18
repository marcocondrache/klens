import { useEffect, useRef, type ReactNode } from "react";
import { useTable, type ColumnDef, type RowData } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

import { features, type DataTableFeatures } from "@/components/data-table/features";
import { GROW_CELL, growWidths } from "@/components/data-table/grow";
import { tablePlaceholder } from "@/components/data-table/placeholder";
import { CLICKABLE_ROW, clickableRowProps } from "@/components/data-table/row-interaction";
import { SkeletonBar, skeletonRowStyle } from "@/components/data-table/skeleton-bar";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

import { RefreshBar } from "./refresh-bar";

const ROW_SIZE = 41;
const LOADER_ROWS = 3;
const SKELETON_ROWS = 14;
const PREFETCH_ROWS = 20;

const CELL = "px-3 first:pl-4 last:pr-4";

interface RecordTableProps<TData extends RowData> {
  columns: Array<ColumnDef<DataTableFeatures, TData>>;
  data: TData[];
  getRowId: (row: TData) => string;
  toolbar?: ReactNode;
  onRowClick?: (row: TData) => void;
  selectedKey?: string;
  loading?: boolean;
  refreshing?: boolean;
  stale?: boolean;
  error?: ReactNode;
  emptyState: ReactNode;
  hasNextPage?: boolean;
  fetchNextPage?: () => void;
  isFetchingNextPage?: boolean;
  isFetchNextPageError?: boolean;
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
  stale = false,
  error,
  emptyState,
  hasNextPage = false,
  fetchNextPage,
  isFetchingNextPage = false,
  isFetchNextPageError = false,
}: RecordTableProps<TData>) {
  const table = useTable({
    features,
    data,
    columns,
    getRowId,
    enableSorting: false,
  });

  const rows = table.getRowModel().rows;
  const leafColumns = table.getAllLeafColumns();
  const awaitingFirstPage = rows.length === 0 && hasNextPage && error == null;
  const showLoader = hasNextPage || isFetchingNextPage || isFetchNextPageError;

  const scrollRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer<HTMLDivElement, HTMLTableRowElement>({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_SIZE,
    getItemKey: (index) => rows[index].id,
    overscan: 6,
  });

  const virtualRows = virtualizer.getVirtualItems();
  const padTop = virtualRows.length ? virtualRows[0].start : 0;
  const padBottom = virtualRows.length
    ? virtualizer.getTotalSize() - virtualRows[virtualRows.length - 1].end
    : 0;
  const endIndex = virtualRows.length ? virtualRows[virtualRows.length - 1].index : -1;
  const nearEnd = hasNextPage && (rows.length === 0 || endIndex >= rows.length - PREFETCH_ROWS);

  useEffect(() => {
    if (fetchNextPage == null || !nearEnd || isFetchingNextPage || isFetchNextPageError) {
      return;
    }
    fetchNextPage();
  }, [fetchNextPage, isFetchNextPageError, isFetchingNextPage, nearEnd]);

  function skeletonRows(count: number, firstRow: number) {
    return Array.from({ length: count }, (_, index) => (
      <TableRow
        key={index}
        aria-hidden
        className="border-border/70 hover:bg-transparent"
        style={skeletonRowStyle(index, count)}
      >
        {leafColumns.map((column, columnIndex) => {
          const meta = column.columnDef.meta;

          return (
            <TableCell key={column.id} className={cn("h-10 py-2", CELL, meta?.grow && GROW_CELL)}>
              <SkeletonBar row={firstRow + index} column={columnIndex} align={meta?.align} />
            </TableCell>
          );
        })}
      </TableRow>
    ));
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {toolbar ? <div className="flex shrink-0 flex-wrap items-center gap-2">{toolbar}</div> : null}
      <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border">
        {refreshing ? <RefreshBar className="absolute inset-x-0 top-0 z-20" /> : null}
        <div ref={scrollRef} className="min-h-0 flex-1 overflow-auto">
          <Table aria-busy={loading || refreshing || isFetchingNextPage || undefined}>
            <colgroup>
              {growWidths(leafColumns).map(({ id, width }) => (
                <col key={id} style={{ width }} />
              ))}
            </colgroup>
            <TableHeader className="[&_tr]:border-b-0!">
              {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id}>
                  {headerGroup.headers.map((header) => (
                    <TableHead
                      key={header.id}
                      className={cn(
                        "sticky top-0 z-10 bg-subtle text-xs text-muted-foreground shadow-[inset_0_-1px_0_0_var(--color-border)]",
                        CELL,
                        header.column.columnDef.meta?.align === "right" && "text-right",
                      )}
                    >
                      {header.isPlaceholder ? null : <table.FlexRender header={header} />}
                    </TableHead>
                  ))}
                </TableRow>
              ))}
            </TableHeader>
            <TableBody>
              {loading || awaitingFirstPage ? (
                skeletonRows(SKELETON_ROWS, 0)
              ) : rows.length === 0 ? (
                <TableRow className="hover:bg-transparent">
                  <TableCell colSpan={leafColumns.length} className="p-0">
                    {tablePlaceholder(error ?? emptyState)}
                  </TableCell>
                </TableRow>
              ) : (
                <>
                  {padTop > 0 ? <tr aria-hidden style={{ height: padTop }} /> : null}
                  {virtualRows.map((item) => {
                    const row = rows[item.index];

                    return (
                      <TableRow
                        key={row.id}
                        data-index={item.index}
                        ref={virtualizer.measureElement}
                        data-state={selectedKey === row.id ? "selected" : undefined}
                        {...(onRowClick ? clickableRowProps(() => onRowClick(row.original)) : {})}
                        className={cn(
                          "group/row border-border/70 transition-[background-color,opacity] duration-75",
                          onRowClick && CLICKABLE_ROW,
                          stale && "opacity-50",
                        )}
                      >
                        {row.getAllCells().map((cell) => {
                          const meta = cell.column.columnDef.meta;

                          return (
                            <TableCell
                              key={cell.id}
                              className={cn(
                                "py-2.5",
                                CELL,
                                meta?.align === "right" && "text-right numeric",
                                meta?.grow && GROW_CELL,
                                meta?.className,
                              )}
                            >
                              <table.FlexRender cell={cell} />
                            </TableCell>
                          );
                        })}
                      </TableRow>
                    );
                  })}
                  {padBottom > 0 ? <tr aria-hidden style={{ height: padBottom }} /> : null}
                  {isFetchNextPageError ? (
                    <TableRow className="border-border/70 hover:bg-transparent">
                      <TableCell colSpan={leafColumns.length} className="h-10 p-0">
                        <div className="flex items-center justify-center gap-2 text-sm text-muted-foreground">
                          Couldn't load more records.
                          <Button variant="outline" size="xs" onClick={() => fetchNextPage?.()}>
                            Retry
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ) : showLoader ? (
                    skeletonRows(LOADER_ROWS, rows.length)
                  ) : null}
                </>
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  );
}
