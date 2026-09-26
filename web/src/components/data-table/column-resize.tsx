import type { MouseEvent, TouchEvent } from "react";
import type { Column, ColumnSizingState, Header, RowData } from "@tanstack/react-table";

import type { DataTableFeatures } from "./features";

export const resizeOptions = {
  columnResizeMode: "onChange",
  defaultColumn: { minSize: 48 },
} as const;

export function resizedWidth<TData extends RowData>(
  column: Column<DataTableFeatures, TData, unknown>,
  sizing: ColumnSizingState,
) {
  return Object.hasOwn(sizing, column.id) ? `${column.getSize()}px` : undefined;
}

export function ColumnResizeHandle<TData extends RowData>({
  header,
}: {
  header: Header<DataTableFeatures, TData, unknown>;
}) {
  "use no memo";

  function start(event: MouseEvent<HTMLElement> | TouchEvent<HTMLElement>) {
    const width = event.currentTarget.parentElement?.getBoundingClientRect().width;
    if (width)
      header.column.table.setColumnSizing((old) => ({ ...old, [header.column.id]: width }));
    header.getResizeHandler()(event);
  }

  return (
    <div
      aria-hidden
      onMouseDown={start}
      onTouchStart={start}
      onDoubleClick={() => header.column.resetSize()}
      className="group/resize absolute inset-y-0 right-0 z-10 flex w-2 cursor-col-resize touch-none justify-end select-none"
    >
      <span className="h-full w-px transition-colors group-hover/resize:bg-muted-foreground/40" />
    </div>
  );
}
