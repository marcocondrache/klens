import type { ComponentProps, ReactNode } from "react";
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
  ok: "bg-ok/12 text-ok",
  warn: "bg-warn/14 text-warn",
  error: "bg-destructive/12 text-destructive",
  idle: "bg-muted text-muted-foreground shadow-[inset_0_0_0_1px_var(--color-border)]",
  brand: "bg-brand/12 text-brand",
};

export const TONE_TEXT: Record<Tone, string> = {
  ok: "text-foreground",
  warn: "text-warn",
  error: "text-destructive",
  idle: "text-muted-foreground",
  brand: "text-brand",
};

export function StatusDot({ tone, pulse = false }: { tone: Tone; pulse?: boolean }) {
  return (
    <span className="relative inline-flex size-2 shrink-0 items-center justify-center">
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
        "inline-flex h-5 items-center gap-1 overflow-hidden rounded-md px-1.5 text-xs font-medium whitespace-nowrap [&_svg:not([class*='size-'])]:size-3",
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

export function StatusLabel({
  tone,
  pulse = false,
  children,
  className,
}: {
  tone: Tone;
  pulse?: boolean;
  children: ReactNode;
  className?: string;
}) {
  return (
    <span className={cn("inline-flex items-center gap-2 whitespace-nowrap", className)}>
      <StatusDot tone={tone} pulse={pulse} />
      {children}
    </span>
  );
}

export function GroupStateBadge({ state }: { state: GroupState }) {
  return (
    <StatusLabel tone={GROUP_TONE[state]} pulse={GROUP_TONE[state] === "warn"}>
      {formatEnumLabel(state)}
    </StatusLabel>
  );
}

export function PendingValue({ label, className }: { label: string; className?: string }) {
  return (
    <span
      role="status"
      aria-label={label}
      title={label}
      className={cn(
        "inline-block h-3 w-12 animate-pulse rounded-sm bg-muted align-middle",
        className,
      )}
    />
  );
}
