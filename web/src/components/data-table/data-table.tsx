import { useState, type ReactNode } from "react";
import { useTable, type ColumnDef, type RowData, type SortingState } from "@tanstack/react-table";

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

import { features, type DataTableFeatures } from "./features";
import { CLICKABLE_ROW, clickableRowProps } from "./row-interaction";
import { SkeletonBar, skeletonRowStyle } from "./skeleton-bar";

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
}: DataTableProps<TData>) {
  const [sorting, setSorting] = useState<SortingState>(
    defaultSort ? [{ id: defaultSort.id, desc: defaultSort.direction === "desc" }] : [],
  );

  const table = useTable({
    features,
    data,
    columns,
    getRowId,
    enableMultiSort: false,
    sortDescFirst: false,
    onSortingChange: setSorting,
    state: { sorting },
  });

  const rows = table.getRowModel().rows;
  const leafColumns = table.getAllLeafColumns();
  const columnCount = leafColumns.length || columns.length;
  const skeletonRows = fill ? 14 : 6;

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
        <div className={cn(fill && "min-h-0 flex-1 overflow-auto")}>
          <Table aria-busy={loading || refreshing || undefined}>
            <TableHeader className={cn(fill && "[&_tr]:border-b-0!")}>
              {table.getHeaderGroups().map((headerGroup) => (
                <TableRow key={headerGroup.id}>
                  {headerGroup.headers.map((header) => {
                    const meta = header.column.columnDef.meta;

                    return (
                      <TableHead
                        key={header.id}
                        className={cn(
                          "h-10 bg-subtle px-3 text-xs font-medium text-muted-foreground first:pl-4 last:pr-4",
                          fill && "sticky top-0 z-10 shadow-[inset_0_-1px_0_0_var(--color-border)]",
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
                rows.map((row) => {
                  const selected = selectedKey === row.id;

                  return (
                    <TableRow
                      key={row.id}
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
    </div>
  );
}
