import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

import type { ClusterStatus, ConsumerGroupState } from "@/lib/api/types";
import { formatEnumLabel } from "@/lib/format";
import type { Tone } from "@/lib/tone";

const TONE_DOT: Record<Tone, string> = {
  ok: "bg-emerald-500",
  warn: "bg-amber-500",
  error: "bg-rose-500",
  idle: "bg-muted-foreground/60",
  brand: "bg-brand",
};

const TONE_PILL: Record<Tone, string> = {
  ok: "border-emerald-500/25 bg-emerald-500/10 text-emerald-600 dark:text-emerald-400",
  warn: "border-amber-500/25 bg-amber-500/10 text-amber-600 dark:text-amber-400",
  error: "border-rose-500/25 bg-rose-500/10 text-rose-600 dark:text-rose-400",
  idle: "border-border bg-muted text-foreground/80",
  brand: "border-brand/25 bg-brand/10 text-brand",
};

export function StatusDot({ tone, pulse = false }: { tone: Tone; pulse?: boolean }) {
  return (
    <span className="relative inline-flex size-2 shrink-0">
      {pulse ? (
        <span
          className={cn("absolute inset-0 animate-ping rounded-full opacity-60", TONE_DOT[tone])}
        />
      ) : null}
      <span className={cn("relative size-2 rounded-full", TONE_DOT[tone])} />
    </span>
  );
}

export function Pill({
  tone = "idle",
  children,
  className,
}: {
  tone?: Tone;
  children: ReactNode;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 text-sm font-medium whitespace-nowrap",
        TONE_PILL[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

const CLUSTER_TONE: Record<ClusterStatus, Tone> = {
  HEALTHY: "ok",
  DEGRADED: "warn",
  OFFLINE: "error",
};

export function ClusterStatusBadge({ status }: { status: ClusterStatus }) {
  return (
    <Pill tone={CLUSTER_TONE[status]}>
      <StatusDot tone={CLUSTER_TONE[status]} pulse={status !== "OFFLINE"} />
      {formatEnumLabel(status)}
    </Pill>
  );
}

const GROUP_TONE: Record<ConsumerGroupState, Tone> = {
  STABLE: "ok",
  EMPTY: "idle",
  PREPARING_REBALANCE: "warn",
  COMPLETING_REBALANCE: "warn",
  DEAD: "error",
};

export function GroupStateBadge({ state }: { state: ConsumerGroupState }) {
  return (
    <Pill tone={GROUP_TONE[state]}>
      <StatusDot tone={GROUP_TONE[state]} />
      {formatEnumLabel(state)}
    </Pill>
  );
}
