import { RadioIcon } from "lucide-react";

import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { StatusDot } from "@/components/status";
import type { TailStatus } from "@/lib/api/tail";
import type { RecordOrder } from "@/lib/api/types";
import type { Tone } from "@/lib/tone";

export type RecordMode = RecordOrder | "LIVE";

const TAIL: Record<TailStatus, { label: string; tone: Tone }> = {
  idle: { label: "Paused", tone: "idle" },
  connecting: { label: "Connecting…", tone: "warn" },
  live: { label: "Following new records", tone: "ok" },
  reconnecting: { label: "Reconnecting…", tone: "warn" },
  error: { label: "Stopped", tone: "error" },
};

const ITEM =
  "px-2.5 font-normal text-muted-foreground hover:text-foreground aria-pressed:text-foreground";

export function RecordModeSwitch({
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
    <ToggleGroup
      variant="outline"
      spacing={0}
      value={[value]}
      onValueChange={(next) => {
        const picked = next[0] as RecordMode | undefined;
        if (picked) onChange(picked);
      }}
      aria-label="Records to show"
      className="bg-background dark:bg-input/20"
    >
      <ToggleGroupItem value="NEWEST" className={ITEM}>
        Newest
      </ToggleGroupItem>
      <ToggleGroupItem value="OLDEST" className={ITEM}>
        Oldest
      </ToggleGroupItem>
      <Tooltip>
        <TooltipTrigger
          render={
            <ToggleGroupItem
              value="LIVE"
              aria-label={tail ? `Live: ${tail.label}` : "Live"}
              className={ITEM}
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
    </ToggleGroup>
  );
}
