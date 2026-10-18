import { useEffect, useState, type ReactNode } from "react";
import { useTable, type ColumnDef, type RowData } from "@tanstack/react-table";

import { features, type DataTableFeatures } from "@/components/data-table/features";
import { DataTableMessageRow, DataTableSkeletonRows } from "@/components/data-table/rows";
import { VirtualTable } from "@/components/data-table/virtual-table";
import { DataTableViewport } from "@/components/data-table/viewport";
import { Button } from "@/components/ui/button";

import { RefreshBar } from "./refresh-bar";

const LOADER_ROWS = 3;
const PREFETCH_DISTANCE = 800;

interface RecordTableProps<TData extends RowData> {
  columns: Array<ColumnDef<DataTableFeatures, TData>>;
  data: TData[];
  getRowId: (row: TData) => string;
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

// Records stream in without bound, so the table always virtualizes rather than
// switching layout midway through a scroll.
export function RecordTable<TData extends RowData>({
  columns,
  data,
  getRowId,
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
  const canFetchMore = hasNextPage && !isFetchingNextPage && !isFetchNextPageError;

  const [viewport, setViewport] = useState<HTMLDivElement | null>(null);

  function fetchMoreNearEnd() {
    if (viewport == null || !canFetchMore) return;

    const remaining = viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight;
    if (remaining < PREFETCH_DISTANCE) fetchNextPage?.();
  }

  // A page that lands without filling the viewport gives no scroll to react to.
  useEffect(fetchMoreNearEnd);

  return (
    <DataTableViewport
      ref={setViewport}
      onScroll={fetchMoreNearEnd}
      overlay={refreshing ? <RefreshBar className="absolute inset-x-0 top-0 z-20" /> : null}
      aria-busy={loading || refreshing || isFetchingNextPage || undefined}
    >
      <VirtualTable
        table={table}
        rows={rows}
        viewport={viewport}
        loading={loading || awaitingFirstPage}
        placeholder={error ?? emptyState}
        onRowClick={onRowClick}
        selectedRowId={selectedKey}
        stale={stale}
        footer={
          rows.length === 0 ? null : isFetchNextPageError ? (
            <DataTableMessageRow columnCount={leafColumns.length}>
              <div className="flex h-10 items-center justify-center gap-2 text-sm text-muted-foreground">
                Couldn't load more records.
                <Button variant="outline" size="xs" onClick={() => fetchNextPage?.()}>
                  Retry
                </Button>
              </div>
            </DataTableMessageRow>
          ) : hasNextPage || isFetchingNextPage ? (
            <DataTableSkeletonRows columns={leafColumns} count={LOADER_ROWS} />
          ) : null
        }
      />
    </DataTableViewport>
  );
}
