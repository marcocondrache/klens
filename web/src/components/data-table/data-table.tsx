import { useRef, useState, type ReactNode } from "react";
import {
  FlexRender,
  useTable,
  type ColumnDef,
  type Header,
  type RowData,
  type SortingState,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

import { DataTableColumnHeader } from "./column-header";
import { ColumnResizeHandle, columnLayout, resizeOptions } from "./column-resize";
import { features, type DataTableFeatures } from "./features";
import { tablePlaceholder } from "./placeholder";
import { CLICKABLE_ROW, clickableRowProps } from "./row-interaction";
import { SkeletonBar, skeletonRowStyle } from "./skeleton-bar";

const ROW_SIZE = 40;
const SKELETON_ROWS = 14;

interface DataTableProps<TData extends RowData> {
  columns: Array<ColumnDef<DataTableFeatures, TData>>;
  data: TData[];
  getRowId: (row: TData) => string;
  toolbar?: ReactNode;
  onRowClick?: (row: TData) => void;
  loading?: boolean;
  error?: ReactNode;
  emptyState?: ReactNode;
  defaultSort?: { id: string; direction: "asc" | "desc" };
}

export function DataTable<TData extends RowData>({
  columns,
  data,
  getRowId,
  toolbar,
  onRowClick,
  loading = false,
  error,
  emptyState,
  defaultSort,
}: DataTableProps<TData>) {
  const [sorting, setSorting] = useState<SortingState>(
    defaultSort ? [{ id: defaultSort.id, desc: defaultSort.direction === "desc" }] : [],
  );

  const table = useTable(
    {
      features,
      data,
      columns,
      getRowId,
      enableMultiSort: false,
      sortDescFirst: false,
      onSortingChange: setSorting,
      ...resizeOptions,
      state: { sorting },
    },
    (state) => ({ columnSizing: state.columnSizing }),
  );

  const rows = table.getRowModel().rows;
  const leafColumns = table.getAllLeafColumns();
  const columnCount = leafColumns.length || columns.length;
  const layout = columnLayout(leafColumns, table.state.columnSizing);

  const scrollRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer<HTMLDivElement, HTMLTableRowElement>({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_SIZE,
    getItemKey: (index) => rows[index].id,
    overscan: 10,
  });
  const virtualRows = virtualizer.getVirtualItems();
  const padTop = virtualRows.length ? virtualRows[0].start : 0;
  const padBottom = virtualRows.length
    ? virtualizer.getTotalSize() - virtualRows[virtualRows.length - 1].end
    : 0;

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-3">
      {toolbar ? <div className="flex shrink-0 flex-wrap items-center gap-2">{toolbar}</div> : null}
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border">
        <div ref={scrollRef} data-slot="table-viewport" className="min-h-0 flex-1 overflow-auto">
          <Table
            aria-busy={loading || undefined}
            className="table-fixed"
            style={{ minWidth: layout.minWidth }}
          >
            <colgroup>
              {leafColumns.map((column, index) => (
                <col key={column.id} style={{ width: layout.cols[index] }} />
              ))}
            </colgroup>
            <TableHeader className="sticky top-0 z-10 [&_tr]:border-b-0!">
              {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id} className="group/header">
                  {headerGroup.headers.map((header) => {
                    const meta = header.column.columnDef.meta;

                    return (
                      <TableHead
                        key={header.id}
                        className={cn(
                          "relative h-10 bg-subtle px-3 text-xs font-medium text-muted-foreground shadow-[inset_0_-1px_0_0_var(--color-border)] first:pl-4 last:pr-4",
                          meta?.align === "right" && "text-right",
                        )}
                      >
                        {header.isPlaceholder ? null : <HeaderContent header={header} />}
                        <ColumnResizeHandle header={header} />
                      </TableHead>
                    );
                  })}
                </TableRow>
              ))}
            </TableHeader>
            <TableBody>
              {loading ? (
                Array.from({ length: SKELETON_ROWS }, (_, index) => (
                  <TableRow
                    key={index}
                    aria-hidden
                    className="border-border/70 hover:bg-transparent"
                    style={skeletonRowStyle(index, SKELETON_ROWS)}
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
                  {virtualRows.map((item) => {
                    const row = rows[item.index];

                    return (
                      <TableRow
                        key={row.id}
                        data-index={item.index}
                        ref={virtualizer.measureElement}
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
                                "h-10 overflow-hidden px-3 py-2 text-ellipsis first:pl-4 last:pr-4",
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

function HeaderContent<TData extends RowData>({
  header,
}: {
  header: Header<DataTableFeatures, TData, unknown>;
}) {
  "use no memo";

  const title = header.column.columnDef.header;
  if (typeof title === "string") {
    return <DataTableColumnHeader column={header.column} title={title} />;
  }
  return <FlexRender header={header} />;
}
