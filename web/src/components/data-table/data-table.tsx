import { useRef, useState, type ReactNode } from "react";
import {
  useTable,
  type Column,
  type ColumnDef,
  type ColumnSizingState,
  type RowData,
  type SortingState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

import { RefreshBar } from "@/components/refresh-bar";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

import {
  ColumnResizeHandle,
  MIN_COLUMN_WIDTH,
  RESIZING_CLASS,
  readColumnSizing,
  resizedWidth,
  usePersistColumnSizing,
} from "./column-resize";
import { features, type DataTableFeatures } from "./features";
import { CLICKABLE_ROW, clickableRowProps } from "./row-interaction";
import { SkeletonBar, skeletonRowStyle } from "./skeleton-bar";

const ROW_SIZE = 40;
const MIN_FLEX_WIDTH = "12rem";

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
  fill?: boolean;
  /** Remembers resized column widths under this key. Columns resize only when `fill` is set. */
  storageKey?: string;
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
  fill = false,
  storageKey,
}: DataTableProps<TData>) {
  const [sorting, setSorting] = useState<SortingState>(
    defaultSort ? [{ id: defaultSort.id, desc: defaultSort.direction === "desc" }] : [],
  );

  const [initialColumnSizing] = useState(() => readColumnSizing(storageKey));

  const table = useTable({
    features,
    data,
    columns,
    getRowId,
    enableMultiSort: false,
    sortDescFirst: false,
    onSortingChange: setSorting,
    enableColumnResizing: fill,
    columnResizeMode: "onChange",
    defaultColumn: { minSize: MIN_COLUMN_WIDTH },
    initialState: { columnSizing: initialColumnSizing },
    state: { sorting },
  });

  const rows = table.getRowModel().rows;
  const leafColumns = table.getAllLeafColumns();
  const columnCount = leafColumns.length || columns.length;
  const skeletonRows = fill ? 14 : 6;

  const columnSizing = table.state.columnSizing;
  const resizingColumn = table.state.columnResizing.isResizingColumn;
  usePersistColumnSizing(storageKey, columnSizing, resizingColumn !== false);
  const flexColumnIds = leafColumns
    .filter((column) => column.columnDef.meta?.width == null)
    .map((column) => column.id);
  const fillColumnId = flexColumnIds.at(-1);
  const layout = fill ? columnLayout(leafColumns, columnSizing, fillColumnId) : null;

  const scrollRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer<HTMLDivElement, HTMLTableRowElement>({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_SIZE,
    getItemKey: (index) => rows[index].id,
    overscan: 10,
    enabled: fill,
  });
  const virtualRows = fill ? virtualizer.getVirtualItems() : null;
  const visibleRows = virtualRows ? virtualRows.map((item) => rows[item.index]) : rows;
  const padTop = virtualRows?.length ? virtualRows[0].start : 0;
  const padBottom = virtualRows?.length
    ? virtualizer.getTotalSize() - virtualRows[virtualRows.length - 1].end
    : 0;

  return (
    <div className={cn("flex flex-col gap-3", fill && "min-h-0 flex-1")}>
      {toolbar ? (
        <div className={cn("flex flex-wrap items-center gap-2", fill && "shrink-0")}>{toolbar}</div>
      ) : null}
      <div
        className={cn(
          "relative overflow-hidden rounded-lg border",
          fill && "flex min-h-0 flex-1 flex-col",
        )}
      >
        {refreshing ? <RefreshBar className="absolute inset-x-0 top-0 z-20" /> : null}
        <div
          ref={scrollRef}
          className={cn(fill && "min-h-0 flex-1 overflow-auto", resizingColumn && RESIZING_CLASS)}
        >
          <Table
            aria-busy={loading || refreshing || undefined}
            className={cn(fill && "table-fixed")}
            style={
              layout ? { minWidth: `calc(${layout.map((c) => c.min).join(" + ")})` } : undefined
            }
          >
            {layout ? (
              <colgroup>
                {layout.map((column) => (
                  <col key={column.id} style={{ width: column.width }} />
                ))}
              </colgroup>
            ) : null}
            <TableHeader className={cn(fill && "[&_tr]:border-b-0!")}>
              {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id}>
                  {headerGroup.headers.map((header) => {
                    const meta = header.column.columnDef.meta;

                    return (
                      <TableHead
                        key={header.id}
                        data-column-id={header.column.id}
                        className={cn(
                          "relative h-10 bg-subtle px-3 text-xs font-medium text-muted-foreground first:pl-4 last:pr-4",
                          fill && "sticky top-0 z-10 shadow-[inset_0_-1px_0_0_var(--color-border)]",
                          meta?.align === "right" && "text-right",
                          meta?.headerClassName,
                        )}
                      >
                        {header.isPlaceholder ? null : <table.FlexRender header={header} />}
                        {header.column.getCanResize() ? (
                          <ColumnResizeHandle
                            table={table}
                            header={header}
                            active={resizingColumn === header.column.id}
                            fillColumnId={fillColumnId}
                            flexColumnIds={flexColumnIds}
                          />
                        ) : null}
                      </TableHead>
                    );
                  })}
                </TableRow>
              ))}
            </TableHeader>
            <TableBody>
              {loading ? (
                Array.from({ length: skeletonRows }, (_, index) => (
                  <TableRow
                    key={index}
                    aria-hidden
                    className="border-border/70 hover:bg-transparent"
                    style={skeletonRowStyle(index, skeletonRows)}
                  >
                    {leafColumns.map((column, columnIndex) => (
                      <TableCell key={column.id} className="h-10 px-3 py-2 first:pl-4 last:pr-4">
                        <SkeletonBar
                          row={index}
                          column={columnIndex}
                          align={column.columnDef.meta?.align}
                        />
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
                <>
                  {padTop > 0 ? <tr aria-hidden style={{ height: padTop }} /> : null}
                  {visibleRows.map((row, index) => {
                    const selected = selectedKey === row.id;

                    return (
                      <TableRow
                        key={row.id}
                        data-index={virtualRows?.[index].index}
                        ref={virtualRows ? virtualizer.measureElement : undefined}
                        data-state={selected ? "selected" : undefined}
                        {...(onRowClick ? clickableRowProps(() => onRowClick(row.original)) : {})}
                        className={cn(
                          "group/row border-border/70 transition-colors duration-75",
                          onRowClick && CLICKABLE_ROW,
                        )}
                      >
                        {row.getAllCells().map((cell) => {
                          const meta = cell.column.columnDef.meta;

                          return (
                            <TableCell
                              key={cell.id}
                              className={cn(
                                "h-10 px-3 py-2 first:pl-4 last:pr-4",
                                fill && "overflow-hidden text-ellipsis",
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
                  })}
                  {padBottom > 0 ? <tr aria-hidden style={{ height: padBottom }} /> : null}
                </>
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  );
}

/**
 * Width and minimum width of each column. The fill column takes whatever width is
 * left, so resizing it sets its minimum and scrolls the table once it outgrows the view.
 */
function columnLayout<TData extends RowData>(
  columns: Array<Column<DataTableFeatures, TData, unknown>>,
  sizing: ColumnSizingState,
  fillColumnId: string | undefined,
) {
  return columns.map((column) => {
    const resized = resizedWidth(column, sizing);
    if (column.id === fillColumnId) {
      return { id: column.id, width: undefined, min: resized ?? MIN_FLEX_WIDTH };
    }

    const width = resized ?? column.columnDef.meta?.width;
    return { id: column.id, width, min: width ?? MIN_FLEX_WIDTH };
  });
}
