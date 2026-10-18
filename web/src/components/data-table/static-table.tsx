import type { RowData } from "@tanstack/react-table";

import { Table, TableBody } from "@/components/ui/table";
import { cn } from "@/lib/utils";

import { DataTableHeader } from "./header";
import { DataTablePlaceholderRows, DataTableRow } from "./rows";
import { isGrowing, TableLayoutContext, type TableViewProps } from "./view";

export function StaticTable<TData extends RowData>({
  table,
  rows,
  loading,
  placeholder,
  onRowClick,
  selectedRowId,
  stale = false,
  footer,
}: TableViewProps<TData>) {
  const columns = table.getAllLeafColumns();
  const growShare = `${100 / columns.filter(isGrowing).length}%`;

  return (
    <TableLayoutContext value="static">
      <Table>
        <colgroup>
          {columns.map((column) => (
            <col key={column.id} style={{ width: isGrowing(column) ? growShare : undefined }} />
          ))}
        </colgroup>
        <DataTableHeader table={table} />
        <TableBody className={cn("transition-opacity duration-75", stale && "opacity-50")}>
          {loading || rows.length === 0 ? (
            <DataTablePlaceholderRows loading={loading} columns={columns}>
              {placeholder}
            </DataTablePlaceholderRows>
          ) : (
            rows.map((row) => (
              <DataTableRow
                key={row.id}
                row={row}
                columns={columns}
                selected={row.id === selectedRowId}
                onRowClick={onRowClick}
              />
            ))
          )}
          {footer}
        </TableBody>
      </Table>
    </TableLayoutContext>
  );
}
