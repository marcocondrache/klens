import type { ComponentProps } from "react";
import { cn } from "@/lib/utils";

import type { GroupState } from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";
import type { Tone } from "@/lib/tone";

const TONE_BG: Record<Tone, string> = {
  ok: "bg-ok",
  warn: "bg-warn",
  error: "bg-destructive",
  idle: "bg-muted-foreground/60",
  brand: "bg-brand",
};

const TONE_PILL: Record<Tone, string> = {
  ok: "border-ok/25 bg-ok/10 text-ok",
  warn: "border-warn/25 bg-warn/10 text-warn",
  error: "border-destructive/25 bg-destructive/10 text-destructive",
  idle: "border-border bg-muted text-foreground/80",
  brand: "border-brand/25 bg-brand/10 text-brand",
};

export function StatusDot({ tone, pulse = false }: { tone: Tone; pulse?: boolean }) {
  return (
    <span className="relative inline-flex size-2 shrink-0">
      {pulse ? (
        <span
          className={cn(
            "absolute inset-0 animate-ping rounded-full opacity-60 motion-reduce:hidden",
            TONE_BG[tone],
          )}
        />
      ) : null}
      <span className={cn("relative size-2 rounded-full", TONE_BG[tone])} />
    </span>
  );
}

export function Pill({
  tone = "idle",
  children,
  className,
  ...props
}: ComponentProps<"span"> & { tone?: Tone }) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 overflow-hidden rounded-4xl border px-2 py-0.5 text-sm font-medium whitespace-nowrap",
        TONE_PILL[tone],
        className,
      )}
      {...props}
    >
      {children}
    </span>
  );
}

export const GROUP_TONE: Record<GroupState, Tone> = {
  STABLE: "ok",
  EMPTY: "idle",
  PREPARING_REBALANCE: "warn",
  COMPLETING_REBALANCE: "warn",
  DEAD: "error",
};

export function GroupStateBadge({ state }: { state: GroupState }) {
  return (
    <Pill tone={GROUP_TONE[state]}>
      <StatusDot tone={GROUP_TONE[state]} />
      {formatEnumLabel(state)}
    </Pill>
  );
}
