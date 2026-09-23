import { useCallback, useEffect, useMemo, useRef, type CSSProperties, type ReactNode } from "react";
import {
  useTable,
  type Column,
  type ColumnDef,
  type Header,
  type ReactTable,
  type RowData,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

import { features, type DataTableFeatures } from "@/components/data-table/features";
import { CLICKABLE_ROW, clickableRowProps } from "@/components/data-table/row-interaction";
import { SkeletonBar, skeletonRowStyle } from "@/components/data-table/skeleton-bar";
import { RefreshBar } from "@/components/refresh-bar";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const ROW_SIZE = 40;
const LOAD_MORE_KEY = "load-more";
const LOADER_ROWS = 3;
const SKELETON_ROWS = 14;
const PREFETCH_ROWS = 20;

const COLUMN_TRACK: Record<string, string> = {
  partition: "4rem",
  offset: "7rem",
  key: "minmax(8rem,12rem)",
  value: "minmax(0,1fr)",
  size: "5rem",
  timestamp: "11rem",
};

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
  stale = false,
  error,
  emptyState,
  hasNextPage = false,
  fetchNextPage,
  isFetchingNextPage = false,
  isFetchNextPageError = false,
}: RecordTableProps<TData>) {
  const scrollRef = useRef<HTMLDivElement>(null);

  const table = useTable({
    features,
    data,
    columns,
    getRowId,
    enableSorting: false,
  });

  const rows = table.getRowModel().rows;
  const leafColumns = table.getAllLeafColumns();
  const gridTemplateColumns = useMemo(
    () => leafColumns.map((column) => COLUMN_TRACK[column.id] ?? "minmax(0,1fr)").join(" "),
    [leafColumns],
  );
  const loaderCount = hasNextPage || isFetchingNextPage || isFetchNextPageError ? 1 : 0;
  const count = rows.length + loaderCount;

  const getItemKey = useCallback(
    (index: number) => {
      if (loaderCount && index === rows.length) return LOAD_MORE_KEY;
      return rows[index]?.id ?? index;
    },
    [loaderCount, rows],
  );

  const virtualizer = useVirtualizer<HTMLDivElement, HTMLDivElement>({
    count,
    getScrollElement: () => scrollRef.current,
    estimateSize: (index) =>
      loaderCount && index === rows.length ? ROW_SIZE * LOADER_ROWS : ROW_SIZE,
    getItemKey,
    overscan: 6,
    measureElement: (element) => element.offsetHeight,
  });

  const items = virtualizer.getVirtualItems();
  const endIndex = items.length === 0 ? -1 : items[items.length - 1].index;
  const nearEnd = hasNextPage && (rows.length === 0 || endIndex >= rows.length - PREFETCH_ROWS);

  useEffect(() => {
    if (fetchNextPage == null || !nearEnd || isFetchingNextPage || isFetchNextPageError) {
      return;
    }
    fetchNextPage();
  }, [fetchNextPage, isFetchNextPageError, isFetchingNextPage, nearEnd]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {toolbar ? <div className="flex shrink-0 flex-wrap items-center gap-2">{toolbar}</div> : null}
      <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border">
        {refreshing ? <RefreshBar className="absolute inset-x-0 top-0 z-20" /> : null}
        {loading || (rows.length === 0 && hasNextPage && error == null) ? (
          <div role="table" aria-busy className="min-h-0 flex-1 overflow-hidden text-sm">
            <HeaderRow table={table} gridTemplateColumns={gridTemplateColumns} />
            {Array.from({ length: SKELETON_ROWS }, (_, index) => (
              <SkeletonRow
                key={index}
                row={index}
                columns={leafColumns}
                gridTemplateColumns={gridTemplateColumns}
                style={skeletonRowStyle(index, SKELETON_ROWS)}
              />
            ))}
          </div>
        ) : rows.length === 0 ? (
          <div role="table" className="text-sm">
            <HeaderRow table={table} gridTemplateColumns={gridTemplateColumns} />
            <div className={error || emptyState ? "p-0" : "h-24"}>
              {error || emptyState ? (
                tablePlaceholder(error ?? emptyState)
              ) : (
                <p className="py-10 text-center text-sm text-muted-foreground">No results.</p>
              )}
            </div>
          </div>
        ) : (
          <div
            role="table"
            aria-busy={refreshing || isFetchingNextPage || undefined}
            className="flex min-h-0 flex-1 flex-col text-sm"
          >
            <HeaderRow table={table} gridTemplateColumns={gridTemplateColumns} />
            <div
              ref={scrollRef}
              data-slot="table-container"
              className="min-h-0 flex-1 overflow-auto [scrollbar-gutter:stable]"
            >
              <div
                role="rowgroup"
                data-slot="table-body"
                className="relative w-full"
                style={{ height: virtualizer.getTotalSize() }}
              >
                {items.map((item) => {
                  if (getItemKey(item.index) === LOAD_MORE_KEY) {
                    return (
                      <div
                        key={item.key}
                        data-index={item.index}
                        ref={virtualizer.measureElement}
                        className="absolute top-0 left-0 w-full"
                        style={{ transform: `translateY(${item.start}px)` }}
                      >
                        {isFetchNextPageError ? (
                          <div role="row" className="border-b border-border/70">
                            <div
                              role="cell"
                              className="flex h-10 items-center justify-center gap-2 text-sm text-muted-foreground"
                            >
                              Couldn't load more records.
                              <Button variant="outline" size="xs" onClick={() => fetchNextPage?.()}>
                                Retry
                              </Button>
                            </div>
                          </div>
                        ) : (
                          Array.from({ length: LOADER_ROWS }, (_, index) => (
                            <SkeletonRow
                              key={index}
                              row={rows.length + index}
                              columns={leafColumns}
                              gridTemplateColumns={gridTemplateColumns}
                              style={skeletonRowStyle(index, LOADER_ROWS)}
                            />
                          ))
                        )}
                      </div>
                    );
                  }

                  const row = rows[item.index];

                  return (
                    <div
                      key={item.key}
                      role="row"
                      data-index={item.index}
                      ref={virtualizer.measureElement}
                      data-state={row && selectedKey === row.id ? "selected" : undefined}
                      {...(row && onRowClick
                        ? clickableRowProps(() => onRowClick(row.original))
                        : {})}
                      className={cn(
                        "group/row absolute top-0 left-0 grid w-full border-b border-border/70",
                        row && onRowClick && CLICKABLE_ROW,
                        row &&
                          "transition-[background-color,opacity] duration-75 hover:bg-muted/50 data-[state=selected]:bg-muted",
                        stale && "opacity-50",
                      )}
                      style={{
                        gridTemplateColumns,
                        transform: `translateY(${item.start}px)`,
                      }}
                    >
                      {row
                        ? row.getAllCells().map((cell) => {
                            const meta = cell.column.columnDef.meta;

                            return (
                              <div
                                key={cell.id}
                                role="cell"
                                className={cn(
                                  "px-3 py-2.5 align-middle text-sm whitespace-nowrap first:pl-4 last:pr-4",
                                  meta?.align === "right" && "text-right numeric",
                                  meta?.className,
                                )}
                              >
                                <table.FlexRender cell={cell} />
                              </div>
                            );
                          })
                        : null}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function HeaderRow<TData extends RowData>({
  table,
  gridTemplateColumns,
}: {
  table: ReactTable<DataTableFeatures, TData>;
  gridTemplateColumns: string;
}) {
  const headers = table.getHeaderGroups()[0]?.headers ?? [];

  return (
    <div
      role="row"
      className="grid shrink-0 border-b bg-subtle [scrollbar-gutter:stable]"
      style={{ gridTemplateColumns }}
    >
      {headers.map((header: Header<DataTableFeatures, TData, unknown>) => {
        const column = header.column as Column<DataTableFeatures, TData, unknown>;
        const meta = column.columnDef.meta;

        return (
          <div
            key={header.id}
            role="columnheader"
            className={cn(
              "flex h-10 items-center px-3 text-left text-xs font-medium whitespace-nowrap text-muted-foreground first:pl-4 last:pr-4",
              meta?.align === "right" && "justify-end text-right",
              meta?.headerClassName,
            )}
          >
            {header.isPlaceholder ? null : <table.FlexRender header={header} />}
          </div>
        );
      })}
    </div>
  );
}

function SkeletonRow<TData extends RowData>({
  row,
  columns,
  gridTemplateColumns,
  style,
}: {
  row: number;
  columns: Array<Column<DataTableFeatures, TData, unknown>>;
  gridTemplateColumns: string;
  style?: CSSProperties;
}) {
  return (
    <div
      aria-hidden
      className="grid h-10 items-center border-b border-border/70"
      style={{ gridTemplateColumns, ...style }}
    >
      {columns.map((column, index) => (
        <div key={column.id} className="px-3 first:pl-4 last:pr-4">
          <SkeletonBar row={row} column={index} align={column.columnDef.meta?.align} />
        </div>
      ))}
    </div>
  );
}
