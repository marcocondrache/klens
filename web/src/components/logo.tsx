import type { ComponentProps } from "react";

import { cn } from "@/lib/utils";

/** The klens mark: a message stream entering and leaving a lens. Solid, in the text color. */
export function LogoMark({ className, ...props }: ComponentProps<"svg">) {
  return (
    <svg
      viewBox="0 0 64 64"
      fill="currentColor"
      aria-hidden
      className={cn("size-5 shrink-0", className)}
      {...props}
    >
      <path d="M32 3A37 37 0 0 1 32 61A37 37 0 0 1 32 3Z" />
      <rect x="2" y="29.5" width="14" height="5" rx="2.5" />
      <rect x="48" y="29.5" width="14" height="5" rx="2.5" />
    </svg>
  );
}
