import { useState, type MouseEvent, type TouchEvent } from "react";
import { flushSync } from "react-dom";
import {
  Subscribe,
  type Column,
  type ColumnSizingState,
  type Header,
  type RowData,
} from "@tanstack/react-table";

import type { DataTableFeatures } from "./features";

const MIN_FLEX_WIDTH = "12rem";

export const resizeOptions = {
  columnResizeMode: "onEnd",
  defaultColumn: { minSize: 48 },
} as const;

/** Columns without a width share the slack. Once none is left, the last column takes it. */
export function columnLayout<TData extends RowData>(
  columns: Array<Column<DataTableFeatures, TData, unknown>>,
  sizing: ColumnSizingState,
) {
  const widths = columns.map((column) =>
    Object.hasOwn(sizing, column.id) ? `${column.getSize()}px` : column.columnDef.meta?.width,
  );
  const mins = widths.map(
    (width, index) => width ?? columns[index].columnDef.meta?.minWidth ?? MIN_FLEX_WIDTH,
  );
  if (!widths.includes(undefined)) widths[widths.length - 1] = undefined;

  return {
    cols: widths,
    tracks: mins.map((min, index) => widths[index] ?? `minmax(${min},1fr)`).join(" "),
    minWidth: `calc(${mins.join(" + ")})`,
  };
}

/**
 * The drag moves only a guide and the columns take their widths on release. The tables select
 * only `columnSizing` from the table state, so the rows do not render on every move.
 */
export function ColumnResizeHandle<TData extends RowData>({
  header,
}: {
  header: Header<DataTableFeatures, TData, unknown>;
}) {
  "use no memo";

  const { column } = header;
  const { table } = column;
  const [reach, setReach] = useState<number>();

  if (!column.getCanResize() || header.index === table.getAllLeafColumns().length - 1) return null;

  function start(event: MouseEvent<HTMLElement> | TouchEvent<HTMLElement>) {
    if ("button" in event) {
      if (event.button !== 0) return;
      event.preventDefault();
    }

    const handle = event.currentTarget;
    const cells = handle.closest("tr, [role=row]")?.children ?? [];
    const sizing = table.atoms.columnSizing.get();
    // The dragged column and the flexible ones before it keep their rendered width, so only the
    // columns after the boundary give or take.
    const seed: ColumnSizingState = {};
    const leaves = table.getAllLeafColumns().slice(0, header.index + 1);
    for (const [index, leaf] of leaves.entries()) {
      if (index === header.index || leaf.columnDef.meta?.width == null) {
        seed[leaf.id] = cells[index].getBoundingClientRect().width;
      }
    }
    // The handler reads the start size from the table, so the seed must land first.
    if (Object.entries(seed).some(([id, width]) => Math.abs(width - (sizing[id] ?? 0)) >= 0.5)) {
      flushSync(() => table.setColumnSizing({ ...sizing, ...seed }));
    }
    header.getResizeHandler()(event);
    setReach((handle.closest("[data-slot=table-viewport]") ?? handle).clientHeight);
  }

  function reset() {
    for (const leaf of table.getAllLeafColumns()) {
      if (leaf.id === column.id || leaf.columnDef.meta?.width == null) leaf.resetSize();
    }
  }

  return (
    <Subscribe
      source={table.atoms.columnResizing}
      selector={(resizing) =>
        resizing.isResizingColumn === column.id
          ? Math.max(
              resizing.deltaOffset ?? 0,
              (column.columnDef.minSize ?? 0) - (resizing.startSize ?? 0),
            )
          : null
      }
    >
      {(offset) => (
        <div
          aria-hidden
          data-column-resizing={offset == null ? undefined : ""}
          onMouseDown={start}
          onTouchStart={start}
          onDoubleClick={reset}
          className="group/resize absolute inset-y-0 -right-1.5 z-10 flex w-3 cursor-col-resize touch-none justify-center select-none pointer-coarse:-right-3 pointer-coarse:w-6"
        >
          <span
            className="mt-3 h-4 w-px shrink-0 bg-border opacity-0 transition-[margin,height,background-color,opacity] duration-150 group-hover/header:opacity-100 group-hover/resize:mt-0 group-hover/resize:h-full group-hover/resize:bg-ring group-data-column-resizing/resize:mt-0 group-data-column-resizing/resize:bg-brand group-data-column-resizing/resize:opacity-100"
            style={offset == null ? undefined : { height: reach, translate: `${offset}px` }}
          />
        </div>
      )}
    </Subscribe>
  );
}
