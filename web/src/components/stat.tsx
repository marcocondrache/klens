import type { ReactNode } from "react"
import { cn } from "@/lib/utils"

import { Skeleton } from "@/components/ui/skeleton"

export function StatGrid({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div className={cn("grid gap-3 sm:grid-cols-2 xl:grid-cols-4", className)}>{children}</div>
  )
}

export function Stat({
  label,
  value,
  hint,
  icon,
  accent = false,
  loading = false,
  children,
}: {
  label: string
  value: ReactNode
  hint?: ReactNode
  icon?: ReactNode
  accent?: boolean
  loading?: boolean
  children?: ReactNode
}) {
  return (
    <div
      className={cn(
        "relative overflow-hidden rounded-xl border bg-card p-4",
        accent && "border-brand/25",
      )}
    >
      {accent ? (
        <span className="pointer-events-none absolute inset-x-0 -top-24 h-32 bg-brand/10 blur-2xl" />
      ) : null}

      <div className="relative flex items-start justify-between gap-3">
        <div className="min-w-0 space-y-1.5">
          <p className="text-[0.7rem] font-medium tracking-wider text-muted-foreground uppercase">
            {label}
          </p>
          {loading ? (
            <Skeleton className="h-7 w-20" />
          ) : (
            <p className="numeric truncate text-2xl leading-none font-semibold tracking-tight">
              {value}
            </p>
          )}
          {hint ? <p className="truncate text-xs text-muted-foreground">{hint}</p> : null}
        </div>

        {icon ? (
          <span
            className={cn(
              "flex size-8 shrink-0 items-center justify-center rounded-lg border bg-muted/50 text-muted-foreground [&_svg]:size-4",
              accent && "border-brand/25 bg-brand/10 text-brand",
            )}
          >
            {icon}
          </span>
        ) : null}
      </div>

      {children ? <div className="relative mt-3">{children}</div> : null}
    </div>
  )
}
