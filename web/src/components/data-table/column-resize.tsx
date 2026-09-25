import { useEffect, type KeyboardEvent, type MouseEvent, type TouchEvent } from "react";
import type { Column, ColumnSizingState, Header, ReactTable, RowData } from "@tanstack/react-table";

import type { DataTableFeatures } from "./features";

export const MIN_COLUMN_WIDTH = 48;

const KEYBOARD_STEP = 16;
const STORAGE_PREFIX = "klens:column-sizing:";

export const RESIZING_CLASS = "cursor-col-resize select-none [&_*]:cursor-col-resize";

/** Column widths the user saved for the table, or none when nothing valid is stored. */
export function readColumnSizing(storageKey: string | undefined): ColumnSizingState {
  if (!storageKey) return {};

  try {
    const stored: unknown = JSON.parse(localStorage.getItem(STORAGE_PREFIX + storageKey) ?? "{}");
    if (typeof stored !== "object" || stored === null) return {};

    return Object.fromEntries(
      Object.entries(stored).filter(
        (entry): entry is [string, number] =>
          typeof entry[1] === "number" && Number.isFinite(entry[1]) && entry[1] >= 0,
      ),
    );
  } catch {
    return {};
  }
}

/** Saves the widths once a drag ends, so a resize writes storage once rather than per frame. */
export function usePersistColumnSizing(
  storageKey: string | undefined,
  sizing: ColumnSizingState,
  resizing: boolean,
) {
  useEffect(() => {
    if (!storageKey || resizing) return;

    try {
      if (Object.keys(sizing).length === 0) {
        localStorage.removeItem(STORAGE_PREFIX + storageKey);
      } else {
        localStorage.setItem(STORAGE_PREFIX + storageKey, JSON.stringify(sizing));
      }
    } catch {
      // Storage can be full or blocked; the widths still apply for this visit.
    }
  }, [storageKey, sizing, resizing]);
}

/** The user's width for a column as a CSS length, or undefined when they never resized it. */
export function resizedWidth<TData extends RowData>(
  column: Column<DataTableFeatures, TData, unknown>,
  sizing: ColumnSizingState,
): string | undefined {
  if (!Object.hasOwn(sizing, column.id)) return undefined;

  return `${Math.max(column.columnDef.minSize ?? MIN_COLUMN_WIDTH, sizing[column.id])}px`;
}

interface ColumnResizeHandleProps<TData extends RowData> {
  table: ReactTable<DataTableFeatures, TData>;
  header: Header<DataTableFeatures, TData, unknown>;
  active: boolean;
  /** Column that takes the leftover width. Its size is a minimum, not a fixed width. */
  fillColumnId?: string;
  /** Columns without a fixed width, which are pinned when the fill column is dragged. */
  flexColumnIds?: string[];
}

/**
 * Drag handle on a header cell's right edge. The cell and its siblings must carry
 * `data-column-id` so the drag can start from the widths the browser actually rendered.
 */
export function ColumnResizeHandle<TData extends RowData>({
  table,
  header,
  active,
  fillColumnId,
  flexColumnIds = [],
}: ColumnResizeHandleProps<TData>) {
  "use no memo";

  const column = header.column;
  const minSize = column.columnDef.minSize ?? MIN_COLUMN_WIDTH;
  const sizing = table.state.columnSizing;

  // Rem widths and flex columns have no pixel size until the first drag, so read them
  // off the DOM. Dragging the fill column also pins the other flex columns, otherwise
  // they would share its growth and the edge would lag behind the pointer.
  function pinRenderedWidths(handle: HTMLElement) {
    const row = handle.closest("[data-column-id]")?.parentElement;
    if (!row) return;

    const pinned =
      column.id === fillColumnId
        ? flexColumnIds.filter((id) => id !== column.id && !Object.hasOwn(sizing, id))
        : [];
    const measured: ColumnSizingState = {};
    for (const id of [column.id, ...pinned]) {
      const cell = row.querySelector(`[data-column-id="${CSS.escape(id)}"]`);
      if (cell) measured[id] = cell.getBoundingClientRect().width;
    }

    table.setColumnSizing((old) => ({ ...old, ...measured }));
  }

  function startDrag(
    event: MouseEvent<HTMLElement> | TouchEvent<HTMLElement>,
    startX: number,
    endEvents: string[],
  ) {
    const before = sizing;
    pinRenderedWidths(event.currentTarget);
    header.getResizeHandler()(event);

    // A click without a drag should leave the layout alone rather than pin widths.
    const restoreIfUnmoved = (end: Event) => {
      for (const type of endEvents) document.removeEventListener(type, restoreIfUnmoved);
      const endX =
        "changedTouches" in end
          ? (end as globalThis.TouchEvent).changedTouches[0]?.clientX
          : (end as globalThis.MouseEvent).clientX;
      if (endX === undefined || endX === startX) table.setColumnSizing(before);
    };
    for (const type of endEvents) document.addEventListener(type, restoreIfUnmoved);
  }

  function onKeyDown(event: KeyboardEvent<HTMLElement>) {
    const step =
      event.key === "ArrowLeft" ? -KEYBOARD_STEP : event.key === "ArrowRight" ? KEYBOARD_STEP : 0;
    if (step === 0) return;

    event.preventDefault();
    pinRenderedWidths(event.currentTarget);
    table.setColumnSizing((old) => ({
      ...old,
      [column.id]: Math.max(minSize, (old[column.id] ?? minSize) + step),
    }));
  }

  return (
    <div
      role="separator"
      aria-orientation="vertical"
      aria-label="Resize column"
      tabIndex={0}
      data-resizing={active || undefined}
      onMouseDown={(event) => {
        if (event.button !== 0) return;
        event.preventDefault();
        startDrag(event, event.clientX, ["mouseup"]);
      }}
      onTouchStart={(event) => {
        if (event.touches.length !== 1) return;
        startDrag(event, event.touches[0].clientX, ["touchend", "touchcancel"]);
      }}
      onDoubleClick={() => column.resetSize()}
      onKeyDown={onKeyDown}
      className="group/resize absolute inset-y-0 right-0 z-10 flex w-2 cursor-col-resize touch-none justify-end outline-none select-none"
    >
      <span
        aria-hidden
        className="h-full w-px transition-colors group-hover/resize:bg-muted-foreground/40 group-focus-visible/resize:bg-ring group-data-resizing/resize:bg-brand"
      />
    </div>
  );
}
