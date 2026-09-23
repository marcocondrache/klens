import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function PageHeader({
  title,
  description,
  badges,
  actions,
  mono = false,
}: {
  title: ReactNode;
  description?: ReactNode;
  badges?: ReactNode;
  actions?: ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="flex shrink-0 flex-wrap items-end justify-between gap-4">
      <div className="min-w-0 space-y-1">
        <div className="flex min-h-7 flex-wrap items-center gap-2">
          <h1
            className={cn(
              "truncate text-xl font-semibold tracking-[-0.015em]",
              mono && "font-mono text-lg tracking-[-0.02em]",
            )}
          >
            {title}
          </h1>
          {badges}
        </div>
        {description ? <div className="text-sm text-muted-foreground">{description}</div> : null}
      </div>

      {actions ? <div className="flex shrink-0 items-center gap-2">{actions}</div> : null}
    </div>
  );
}
