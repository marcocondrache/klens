import type { ComponentProps, ReactNode } from "react";
import { FlexRender, type Cell, type Column, type Row, type RowData } from "@tanstack/react-table";

import { TableCell, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";

import type { DataTableFeatures } from "./features";
import { tablePlaceholder } from "./placeholder";
import { CLICKABLE_ROW, clickableRowProps } from "./row-interaction";
import { SkeletonBar, skeletonRowStyle } from "./skeleton-bar";
import { cellLayout, ROW_LAYOUT, useTableLayout } from "./view";

const SKELETON_ROWS = 14;

type DataTableColumn<TData extends RowData> = Column<DataTableFeatures, TData, unknown>;

interface DataTableRowProps<TData extends RowData> extends Omit<ComponentProps<"tr">, "children"> {
  row: Row<DataTableFeatures, TData>;
  columns: Array<DataTableColumn<TData>>;
  selected?: boolean;
  onRowClick?: (row: TData) => void;
}

export function DataTableRow<TData extends RowData>({
  row,
  columns,
  selected = false,
  onRowClick,
  className,
  ...props
}: DataTableRowProps<TData>) {
  return (
    <BodyRow
      data-state={selected ? "selected" : undefined}
      {...(onRowClick ? clickableRowProps(() => onRowClick(row.original)) : {})}
      className={cn(
        "group/row transition-colors duration-75",
        onRowClick && CLICKABLE_ROW,
        className,
      )}
      {...props}
    >
      {/* A row object survives a change of columns, so the React Compiler would keep serving
          its old row.getAllCells(). Looking each cell up by column keys the memo on columns. */}
      {columns.map((column) => (
        <DataTableCell key={column.id} cell={row.getAllCellsByColumnId()[column.id]} />
      ))}
    </BodyRow>
  );
}

export function DataTablePlaceholderRows<TData extends RowData>({
  loading,
  columns,
  children,
}: {
  loading: boolean;
  columns: Array<DataTableColumn<TData>>;
  children: ReactNode;
}) {
  if (loading) {
    return <DataTableSkeletonRows columns={columns} />;
  }

  return <DataTableMessageRow columnCount={columns.length}>{children}</DataTableMessageRow>;
}

export function DataTableSkeletonRows<TData extends RowData>({
  columns,
  count = SKELETON_ROWS,
}: {
  columns: Array<DataTableColumn<TData>>;
  count?: number;
}) {
  return Array.from({ length: count }, (_, index) => (
    <BodyRow
      key={index}
      aria-hidden
      className="hover:bg-transparent"
      style={skeletonRowStyle(index, count)}
    >
      {columns.map((column, columnIndex) => (
        <ColumnCell key={column.id} column={column}>
          <SkeletonBar row={index} column={columnIndex} align={column.columnDef.meta?.align} />
        </ColumnCell>
      ))}
    </BodyRow>
  ));
}

export function DataTableMessageRow({
  columnCount,
  children,
}: {
  columnCount: number;
  children: ReactNode;
}) {
  return (
    <BodyRow className="hover:bg-transparent">
      <TableCell colSpan={columnCount} className="flex-1 p-0">
        {tablePlaceholder(children)}
      </TableCell>
    </BodyRow>
  );
}

function BodyRow({ className, ...props }: ComponentProps<"tr">) {
  const layout = useTableLayout();

  return <TableRow className={cn("border-border/70", ROW_LAYOUT[layout], className)} {...props} />;
}

function DataTableCell<TData extends RowData>({
  cell,
}: {
  cell: Cell<DataTableFeatures, TData, unknown>;
}) {
  return (
    <ColumnCell column={cell.column}>
      <FlexRender cell={cell} />
    </ColumnCell>
  );
}

function ColumnCell<TData extends RowData>({
  column,
  children,
}: {
  column: DataTableColumn<TData>;
  children: ReactNode;
}) {
  const layout = useTableLayout();
  const meta = column.columnDef.meta;
  const { className, style } = cellLayout(column, layout);

  return (
    <TableCell
      className={cn(
        "overflow-hidden px-3 py-2 text-ellipsis first:pl-4 last:pr-4",
        meta?.align === "right" && "text-right numeric",
        className,
        meta?.className,
      )}
      style={style}
    >
      {children}
    </TableCell>
  );
}
