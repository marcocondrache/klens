import type { KeyboardEvent } from "react";

export const CLICKABLE_ROW =
  "cursor-pointer outline-none focus-visible:bg-muted/60 focus-visible:shadow-[inset_2px_0_0_var(--color-brand)]";

export function clickableRowProps(open: () => void) {
  return {
    tabIndex: 0,
    onClick: open,
    onKeyDown: (event: KeyboardEvent<HTMLElement>) => {
      if (event.target !== event.currentTarget) return;
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      open();
    },
  };
}
