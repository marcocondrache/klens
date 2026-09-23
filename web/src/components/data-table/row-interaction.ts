import type { KeyboardEvent } from "react";

/** Classes for a row that opens something: pointer, and a visible focus mark in the accent. */
export const CLICKABLE_ROW =
  "cursor-pointer outline-none focus-visible:bg-muted/60 focus-visible:shadow-[inset_2px_0_0_var(--color-brand)]";

/** Props that make a row reachable with Tab and openable with Enter or Space. */
export function clickableRowProps(open: () => void) {
  return {
    tabIndex: 0,
    onClick: open,
    onKeyDown: (event: KeyboardEvent<HTMLElement>) => {
      // Keys pressed on a control inside the row belong to that control.
      if (event.target !== event.currentTarget) return;
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      open();
    },
  };
}
