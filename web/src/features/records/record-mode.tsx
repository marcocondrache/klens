import {
  ArrowDownWideNarrowIcon,
  ArrowUpNarrowWideIcon,
  RadioIcon,
  type LucideIcon,
} from "lucide-react";

import { Button } from "@/components/ui/button";
import { Toggle } from "@/components/ui/toggle";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { StatusDot } from "@/components/status";
import type { TailStatus } from "@/lib/api/tail";
import type { RecordOrder } from "@/lib/api/types";
import type { Tone } from "@/lib/tone";
import { cn } from "@/lib/utils";

export type RecordMode = RecordOrder | "LIVE";

const ORDERS = ["NEWEST", "OLDEST"] as const satisfies readonly RecordOrder[];

const ORDER: Record<RecordOrder, { label: string; icon: LucideIcon }> = {
  NEWEST: { label: "Newest first", icon: ArrowDownWideNarrowIcon },
  OLDEST: { label: "Oldest first", icon: ArrowUpNarrowWideIcon },
};

const TAIL: Record<TailStatus, { label: string; tone: Tone }> = {
  idle: { label: "Paused", tone: "idle" },
  connecting: { label: "Connecting…", tone: "warn" },
  live: { label: "Following new records", tone: "ok" },
  reconnecting: { label: "Reconnecting…", tone: "warn" },
  error: { label: "Stopped", tone: "error" },
};

const QUIET =
  "font-normal text-muted-foreground hover:text-foreground aria-expanded:text-foreground aria-pressed:text-foreground";

export function RecordOrderToggle({
  value,
  onChange,
}: {
  value: RecordMode;
  onChange: (mode: RecordMode) => void;
}) {
  // The tail always shows the newest record first, so the toggle says so and
  // stays locked until live is off.
  const order = value === "LIVE" ? "NEWEST" : value;
  const next = order === "NEWEST" ? "OLDEST" : "NEWEST";
  const Icon = ORDER[order].icon;

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Button
            variant="outline"
            disabled={value === "LIVE"}
            aria-label={`Sort: ${ORDER[order].label}`}
            className={QUIET}
            onClick={() => onChange(next)}
          />
        }
      >
        <Icon className="size-3.5" />
        {/* Every label takes the same cell, so the button keeps the widest
            one's width and the filter beside it holds still on a change. */}
        <span className="grid text-left">
          {ORDERS.map((candidate) => (
            <span
              key={candidate}
              className={cn("col-start-1 row-start-1", candidate !== order && "invisible")}
            >
              {ORDER[candidate].label}
            </span>
          ))}
        </span>
      </TooltipTrigger>
      <TooltipContent>Show {ORDER[next].label.toLowerCase()}</TooltipContent>
    </Tooltip>
  );
}

export function LiveToggle({
  value,
  onChange,
  status,
}: {
  value: RecordMode;
  onChange: (mode: RecordMode) => void;
  status?: TailStatus;
}) {
  const tail = status ? TAIL[status] : null;

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Toggle
            variant="outline"
            pressed={value === "LIVE"}
            onPressedChange={(pressed) => onChange(pressed ? "LIVE" : "NEWEST")}
            aria-label={tail ? `Live: ${tail.label}` : "Live"}
            className={cn("bg-background dark:bg-input/20", QUIET)}
          />
        }
      >
        <span className="flex size-3.5 items-center justify-center">
          {tail ? (
            <StatusDot tone={tail.tone} pulse={status === "live"} />
          ) : (
            <RadioIcon className="size-3.5" />
          )}
        </span>
        Live
      </TooltipTrigger>
      <TooltipContent>{tail ? tail.label : "Follow new records as they arrive"}</TooltipContent>
    </Tooltip>
  );
}
