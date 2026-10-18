import type { ComponentProps, ReactNode } from "react";

import { cn } from "@/lib/utils";

export function DataTableViewport({
  overlay,
  className,
  ...props
}: ComponentProps<"div"> & { overlay?: ReactNode }) {
  return (
    <div className="relative flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border">
      {overlay}
      <div className={cn("min-h-0 flex-1 overflow-auto", className)} {...props} />
    </div>
  );
}
