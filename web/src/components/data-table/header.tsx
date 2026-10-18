import { FlexRender, type Header, type ReactTable, type RowData } from "@tanstack/react-table";

import { TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";

import { DataTableColumnHeader } from "./column-header";
import type { DataTableFeatures } from "./features";
import { cellLayout, ROW_LAYOUT, useTableLayout } from "./view";

export function DataTableHeader<TData extends RowData>({
  table,
}: {
  table: ReactTable<DataTableFeatures, TData>;
}) {
  const layout = useTableLayout();

  return (
    <TableHeader
      className={cn("sticky top-0 z-10 [&_tr]:border-b-0!", layout === "virtual" && "grid")}
    >
      {table.getHeaderGroups().map((headerGroup) => (
        <TableRow key={headerGroup.id} className={ROW_LAYOUT[layout]}>
          {headerGroup.headers.map((header) => (
            <DataTableHead key={header.id} header={header} />
          ))}
        </TableRow>
      ))}
    </TableHeader>
  );
}

function DataTableHead<TData extends RowData>({
  header,
}: {
  header: Header<DataTableFeatures, TData, unknown>;
}) {
  const layout = useTableLayout();
  const { className, style } = cellLayout(header.column, layout);
  const title = header.column.columnDef.header;

  return (
    <TableHead
      className={cn(
        "h-10 bg-subtle px-3 text-xs font-medium text-muted-foreground shadow-[inset_0_-1px_0_0_var(--color-border)] first:pl-4 last:pr-4",
        header.column.columnDef.meta?.align === "right" && "text-right",
        className,
      )}
      style={style}
    >
      {header.isPlaceholder ? null : typeof title === "string" ? (
        <DataTableColumnHeader column={header.column} title={title} />
      ) : (
        <FlexRender header={header} />
      )}
    </TableHead>
  );
}
