import { useState } from "react";
import type { DateRange } from "react-day-picker";
import { ChevronRightIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
  Command,
  CommandGroup,
  CommandItem,
  CommandList,
  CommandShortcut,
} from "@/components/ui/command";
import { Input } from "@/components/ui/input";
import { Kbd } from "@/components/ui/kbd";
import { EditorSearchHeader, type FilterEditorProps } from "@/components/filters/editors/shared";
import {
  combineDateAndTime,
  DATE_PRESETS,
  describeDateRange,
  formatDayLabel,
  timeInputValue,
} from "@/lib/filters/date-range";

export function DateRangeEditor({
  field,
  value,
  header,
  onChange,
  onClose,
}: FilterEditorProps<"date-range">) {
  const [custom, setCustom] = useState(value.preset === "custom");
  const [range, setRange] = useState<DateRange | undefined>(() => {
    if (value.preset !== "custom") return undefined;
    return {
      from: value.from ? new Date(value.from) : undefined,
      to: value.to ? new Date(value.to) : undefined,
    };
  });
  const [startTime, setStartTime] = useState(timeInputValue(value.from, "00:00"));
  const [endTime, setEndTime] = useState(timeInputValue(value.to, "23:59"));

  const presets = field.presets
    ? DATE_PRESETS.filter((preset) => field.presets?.includes(preset.id))
    : DATE_PRESETS;

  function apply() {
    if (!range?.from) return;
    const from = combineDateAndTime(range.from, startTime);
    const to = combineDateAndTime(range.to ?? range.from, endTime);
    onChange({
      kind: "date-range",
      preset: "custom",
      from: from.toISOString(),
      to: to.toISOString(),
    });
    onClose();
  }

  if (!custom) {
    return (
      <Command loop>
        <EditorSearchHeader header={header} />
        <CommandList>
          <CommandGroup>
            {presets.map((preset) =>
              preset.id === "custom" ? (
                <CommandItem
                  key={preset.id}
                  value={`${preset.label} between dates`}
                  onSelect={() => setCustom(true)}
                >
                  <span className="min-w-0 flex-1 truncate">{preset.label}</span>
                  <CommandShortcut>
                    <ChevronRightIcon className="size-4" />
                  </CommandShortcut>
                </CommandItem>
              ) : (
                <CommandItem
                  key={preset.id}
                  value={preset.label}
                  data-checked={preset.id === value.preset || undefined}
                  onSelect={() => {
                    onChange({ kind: "date-range", preset: preset.id, from: null, to: null });
                    onClose();
                  }}
                >
                  {preset.label}
                </CommandItem>
              ),
            )}
          </CommandGroup>
        </CommandList>
      </Command>
    );
  }

  return (
    <div className="flex flex-col">
      <div className="flex min-w-0 items-center gap-1 p-1">
        {header}
        <span className="min-w-0 flex-1 truncate text-sm text-muted-foreground">
          {range?.from
            ? describeDateRange({
                kind: "date-range",
                preset: "custom",
                from: combineDateAndTime(range.from, startTime).toISOString(),
                to: combineDateAndTime(range.to ?? range.from, endTime).toISOString(),
              })
            : "Pick a range"}
        </span>
      </div>

      <Calendar
        mode="range"
        autoFocus
        selected={range}
        onSelect={setRange}
        defaultMonth={range?.from}
        className="w-full [--cell-size:--spacing(8)]"
      />

      <div className="flex flex-col gap-2 border-t p-2">
        <TimeRow
          label="Start"
          day={formatDayLabel(range?.from)}
          time={startTime}
          onTimeChange={setStartTime}
        />
        <TimeRow
          label="End"
          day={formatDayLabel(range?.to ?? range?.from)}
          time={endTime}
          onTimeChange={setEndTime}
        />
        <Button size="sm" disabled={!range?.from} onClick={apply}>
          Apply
          <Kbd>⏎</Kbd>
        </Button>
      </div>
    </div>
  );
}

function TimeRow({
  label,
  day,
  time,
  onTimeChange,
}: {
  label: string;
  day: string;
  time: string;
  onTimeChange: (value: string) => void;
}) {
  return (
    <div className="flex items-center gap-2">
      <span className="w-10 shrink-0 text-sm text-muted-foreground">{label}</span>
      <span className="min-w-0 flex-1 truncate text-sm">{day}</span>
      <Input
        type="time"
        aria-label={`${label} time`}
        value={time}
        onChange={(event) => onTimeChange(event.target.value)}
        className="h-8 w-28 shrink-0"
      />
    </div>
  );
}
