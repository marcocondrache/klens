import { useId, useState } from "react";
import { ChevronDownIcon, ClockIcon } from "lucide-react";

import { DropdownMenuItem, DropdownMenuSeparator } from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { CHIP_SEGMENT, type CustomFilter } from "@/components/data-table/filter-bar";
import { toDatetimeLocalValue } from "@/lib/format";
import { cn } from "@/lib/utils";

const PRESETS = [
  { label: "Last 15 minutes", since: "15 minutes ago", ms: 15 * 60_000 },
  { label: "Last hour", since: "1 hour ago", ms: 3_600_000 },
  { label: "Last 24 hours", since: "24 hours ago", ms: 86_400_000 },
  { label: "Last 7 days", since: "7 days ago", ms: 7 * 86_400_000 },
] as const;

export type TimestampRange = { from: string; to: string };

type State = TimestampRange & { preset: (typeof PRESETS)[number] | null };

const EMPTY: State = { from: "", to: "", preset: null };

function formatBound(value: string) {
  return new Date(value).toLocaleString("en-GB", {
    day: "numeric",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function describe({ from, to, preset }: State) {
  if (preset) return { operator: "is after", value: preset.since };
  if (from && to)
    return { operator: "is between", value: `${formatBound(from)} – ${formatBound(to)}` };
  if (from) return { operator: "is after", value: formatBound(from) };
  if (to) return { operator: "is before", value: formatBound(to) };
  return { operator: "is", value: "any time" };
}

export function useTimestampFilter(): { range: TimestampRange; filter: CustomFilter } {
  const [state, setState] = useState<State>(EMPTY);
  const [editing, setEditing] = useState(false);
  const id = useId();
  const { from, to } = state;
  const { operator, value } = describe(state);

  function setBound(bound: "from" | "to", next: string) {
    setState((current) => ({ ...current, [bound]: next, preset: null }));
  }

  const chip = (
    <>
      <span className="flex h-full items-center px-2 text-muted-foreground">{operator}</span>

      <Popover
        open={editing}
        onOpenChange={(open) => {
          setEditing(open);
          if (!open && !from && !to) setState(EMPTY);
        }}
      >
        <PopoverTrigger
          aria-label="Timestamp range"
          className={cn(CHIP_SEGMENT, "font-medium whitespace-nowrap")}
        >
          {value}
          <ChevronDownIcon className="size-3.5 text-muted-foreground" />
        </PopoverTrigger>
        <PopoverContent align="start" className="w-auto gap-3 p-3">
          <div className="grid gap-1.5">
            <Label htmlFor={`${id}-from`} className="text-muted-foreground">
              From
            </Label>
            <Input
              id={`${id}-from`}
              type="datetime-local"
              value={from}
              max={to || undefined}
              onChange={(event) => setBound("from", event.target.value)}
            />
          </div>
          <div className="grid gap-1.5">
            <Label htmlFor={`${id}-to`} className="text-muted-foreground">
              To
            </Label>
            <Input
              id={`${id}-to`}
              type="datetime-local"
              value={to}
              min={from || undefined}
              onChange={(event) => setBound("to", event.target.value)}
            />
          </div>
        </PopoverContent>
      </Popover>
    </>
  );

  return {
    range: { from, to },
    filter: {
      id: "timestamp",
      label: "Timestamp",
      icon: ClockIcon,
      menu: (
        <>
          {PRESETS.map((preset) => (
            <DropdownMenuItem
              key={preset.label}
              onClick={() => {
                setState({
                  from: toDatetimeLocalValue(new Date(Date.now() - preset.ms)),
                  to: "",
                  preset,
                });
                setEditing(false);
              }}
            >
              {preset.label}
            </DropdownMenuItem>
          ))}
          <DropdownMenuSeparator />
          <DropdownMenuItem onClick={() => setEditing(true)}>Custom range…</DropdownMenuItem>
        </>
      ),
      chip: from || to || editing ? chip : null,
      onClear: () => {
        setState(EMPTY);
        setEditing(false);
      },
    },
  };
}
