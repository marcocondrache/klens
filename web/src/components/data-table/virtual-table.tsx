import type { Column, Row, RowData } from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";

import { Table, TableBody } from "@/components/ui/table";
import { cn } from "@/lib/utils";

import type { DataTableFeatures } from "./features";
import { DataTableHeader } from "./header";
import { DataTablePlaceholderRows, DataTableRow } from "./rows";
import { TableLayoutContext, type TableViewProps } from "./view";

const ROW_HEIGHT = 40;
const OVERSCAN = 10;

interface VirtualTableProps<TData extends RowData> extends TableViewProps<TData> {
  // Held in state rather than a ref: the virtualizer mounts inside the viewport before a
  // ref to it attaches, and only a re-render lets it pick the element up.
  viewport: HTMLDivElement | null;
}

export function VirtualTable<TData extends RowData>({
  table,
  rows,
  viewport,
  loading,
  placeholder,
  onRowClick,
  selectedRowId,
  stale = false,
  footer,
}: VirtualTableProps<TData>) {
  const columns = table.getAllLeafColumns();

  return (
    <TableLayoutContext value="virtual">
      <Table className="grid" style={{ minWidth: table.getTotalSize() }}>
        <DataTableHeader table={table} />
        {loading || rows.length === 0 ? (
          <TableBody className="grid">
            <DataTablePlaceholderRows loading={loading} columns={columns}>
              {placeholder}
            </DataTablePlaceholderRows>
          </TableBody>
        ) : (
          <VirtualTableBody
            rows={rows}
            columns={columns}
            viewport={viewport}
            onRowClick={onRowClick}
            selectedRowId={selectedRowId}
            stale={stale}
          />
        )}
        {footer ? <TableBody className="grid border-t border-border/70">{footer}</TableBody> : null}
      </Table>
    </TableLayoutContext>
  );
}

function VirtualTableBody<TData extends RowData>({
  rows,
  columns,
  viewport,
  onRowClick,
  selectedRowId,
  stale,
}: {
  rows: Array<Row<DataTableFeatures, TData>>;
  columns: Array<Column<DataTableFeatures, TData, unknown>>;
  viewport: HTMLDivElement | null;
  onRowClick?: (row: TData) => void;
  selectedRowId?: string;
  stale: boolean;
}) {
  const virtualizer = useVirtualizer<HTMLDivElement, HTMLTableRowElement>({
    count: rows.length,
    getScrollElement: () => viewport,
    estimateSize: () => ROW_HEIGHT,
    getItemKey: (index) => rows[index].id,
    overscan: OVERSCAN,
  });

  return (
    <TableBody
      className={cn("relative grid transition-opacity duration-75", stale && "opacity-50")}
      style={{ height: virtualizer.getTotalSize() }}
    >
      {virtualizer.getVirtualItems().map((item) => {
        const row = rows[item.index];

        return (
          <DataTableRow
            key={row.id}
            ref={virtualizer.measureElement}
            data-index={item.index}
            row={row}
            columns={columns}
            selected={row.id === selectedRowId}
            onRowClick={onRowClick}
            className="absolute w-full"
            style={{ transform: `translateY(${item.start}px)` }}
          />
        );
      })}
    </TableBody>
  );
}
