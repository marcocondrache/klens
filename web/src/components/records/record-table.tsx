import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  useTable,
  type Column,
  type ColumnDef,
  type ColumnVisibilityState,
  type Header,
  type ReactTable,
  type RowData,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

import { DataTableViewOptions } from "@/components/data-table/view-options";
import { features, type DataTableFeatures } from "@/components/data-table/features";
import { RefreshBar } from "@/components/refresh-bar";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { cn } from "@/lib/utils";

const ROW_SIZE = 40;
const LOAD_MORE_KEY = "load-more";

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
  const scrollRef = useRef<HTMLDivElement>(null);
  const [columnVisibility, setColumnVisibility] = useState<ColumnVisibilityState>({});

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
  const visible = table.getVisibleLeafColumns();
  const gridTemplateColumns = useMemo(
    () => visible.map((column) => COLUMN_TRACK[column.id] ?? "minmax(0,1fr)").join(" "),
    [visible],
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
    estimateSize: () => ROW_SIZE,
    getItemKey,
    overscan: 6,
    measureElement: (element) => element.offsetHeight,
  });

  const items = virtualizer.getVirtualItems();

  useEffect(() => {
    if (fetchNextPage == null || !hasNextPage || isFetchingNextPage || isFetchNextPageError) return;
    if (rows.length === 0) {
      fetchNextPage();
      return;
    }

    const edge = items[items.length - 1];
    if (edge == null) return;
    if (getItemKey(edge.index) === LOAD_MORE_KEY) fetchNextPage();
  }, [
    fetchNextPage,
    getItemKey,
    hasNextPage,
    isFetchNextPageError,
    isFetchingNextPage,
    items,
    rows.length,
  ]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-4">
      <div className="flex shrink-0 flex-wrap items-center gap-3">
        {toolbar}
        <DataTableViewOptions table={table} />
      </div>
      <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden rounded-md border">
        {refreshing ? <RefreshBar className="absolute inset-x-0 top-0 z-20" /> : null}
        {loading || (rows.length === 0 && hasNextPage && error == null) ? (
          <div role="table" className="text-sm">
            <HeaderRow table={table} gridTemplateColumns={gridTemplateColumns} />
            {Array.from({ length: 6 }, (_, index) => (
              <div
                key={index}
                role="row"
                className="grid border-b px-0"
                style={{ gridTemplateColumns }}
              >
                {visible.map((column) => (
                  <div key={column.id} role="cell" className="px-2 py-2">
                    <Skeleton className="h-4 w-full max-w-32" />
                  </div>
                ))}
              </div>
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
          <div role="table" className="flex min-h-0 flex-1 flex-col text-sm">
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
                  const isLoader = getItemKey(item.index) === LOAD_MORE_KEY;
                  const row = isLoader ? undefined : rows[item.index];

                  return (
                    <div
                      key={item.key}
                      role="row"
                      data-index={item.index}
                      ref={virtualizer.measureElement}
                      data-state={row && selectedKey === row.id ? "selected" : undefined}
                      onClick={row && onRowClick ? () => onRowClick(row.original) : undefined}
                      className={cn(
                        "absolute top-0 left-0 grid w-full border-b",
                        row && onRowClick && "cursor-pointer",
                        row && "transition-colors hover:bg-muted/50 data-[state=selected]:bg-muted",
                      )}
                      style={{
                        gridTemplateColumns,
                        transform: `translateY(${item.start}px)`,
                      }}
                    >
                      {isLoader ? (
                        <div
                          role="cell"
                          className="col-span-full flex items-center justify-center gap-2 py-3 text-sm text-muted-foreground"
                        >
                          {isFetchNextPageError ? (
                            <button
                              type="button"
                              className="underline-offset-2 hover:underline"
                              onClick={() => fetchNextPage?.()}
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
                      ) : row ? (
                        row.getVisibleCells().map((cell) => {
                          const meta = cell.column.columnDef.meta;

                          return (
                            <div
                              key={cell.id}
                              role="cell"
                              className={cn(
                                "px-2 py-2 align-middle text-sm whitespace-nowrap",
                                meta?.align === "right" && "text-right numeric",
                                meta?.className,
                              )}
                            >
                              <table.FlexRender cell={cell} />
                            </div>
                          );
                        })
                      ) : null}
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
      className="grid shrink-0 border-b bg-background [scrollbar-gutter:stable]"
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
              "h-10 px-2 text-left text-sm font-medium whitespace-nowrap text-foreground",
              meta?.align === "right" && "text-right",
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
