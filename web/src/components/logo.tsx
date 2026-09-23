import type { ComponentProps } from "react";

import { cn } from "@/lib/utils";

/** The klens mark: a bold ring, a lens seen face-on. Drawn in the brand color. */
export function LogoMark({ className, ...props }: ComponentProps<"svg">) {
  return (
    <svg
      viewBox="0 0 64 64"
      fill="none"
      aria-hidden
      className={cn("size-5 shrink-0", className)}
      {...props}
    >
      <circle cx="32" cy="32" r="21" stroke="var(--brand)" strokeWidth="14" />
    </svg>
  );
}
