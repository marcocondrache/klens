import { useState } from "react";
import { CornerDownLeftIcon } from "lucide-react";
import type { DateRange } from "react-day-picker";

import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  endOfLocalDay,
  formatDateShort,
  formatTimeShort,
  localTimezoneLabel,
  startOfLocalDay,
} from "@/lib/format";
import type { DateRangeValue } from "@/lib/record-filters";
import { cn } from "@/lib/utils";

function parseTimeInput(value: string, fallback: Date) {
  const match = value.trim().match(/^(\d{1,2}):(\d{2})\s*(AM|PM)?$/i);
  if (!match) return fallback;

  let hours = Number(match[1]);
  const minutes = Number(match[2]);
  const meridiem = match[3]?.toUpperCase();

  if (meridiem === "PM" && hours < 12) hours += 12;
  if (meridiem === "AM" && hours === 12) hours = 0;
  if (hours > 23 || minutes > 59) return fallback;

  const next = new Date(fallback);
  next.setHours(hours, minutes, meridiem ? 0 : fallback.getSeconds(), 0);
  return next;
}

function combineDateAndTime(day: Date, timeSource: Date) {
  const next = new Date(day);
  next.setHours(
    timeSource.getHours(),
    timeSource.getMinutes(),
    timeSource.getSeconds(),
    timeSource.getMilliseconds(),
  );
  return next;
}

export function DateRangeFilter({
  value,
  onApply,
  className,
}: {
  value?: DateRangeValue | null;
  onApply: (range: DateRangeValue) => void;
  className?: string;
}) {
  const initialFrom = value?.from ?? startOfLocalDay(new Date());
  const initialTo = value?.to ?? endOfLocalDay(new Date());
  const [range, setRange] = useState<DateRange | undefined>({
    from: initialFrom,
    to: initialTo,
  });
  const [from, setFrom] = useState(initialFrom);
  const [to, setTo] = useState(initialTo);
  const [fromDateText, setFromDateText] = useState(formatDateShort(initialFrom));
  const [fromTimeText, setFromTimeText] = useState(formatTimeShort(initialFrom));
  const [toDateText, setToDateText] = useState(formatDateShort(initialTo));
  const [toTimeText, setToTimeText] = useState(formatTimeShort(initialTo));

  function syncFrom(next: Date) {
    setFrom(next);
    setFromDateText(formatDateShort(next));
    setFromTimeText(formatTimeShort(next));
  }

  function syncTo(next: Date) {
    setTo(next);
    setToDateText(formatDateShort(next));
    setToTimeText(formatTimeShort(next));
  }

  function onSelect(next: DateRange | undefined) {
    setRange(next);
    if (!next?.from) return;

    if (!next.to) {
      syncFrom(startOfLocalDay(next.from));
      syncTo(endOfLocalDay(next.from));
      return;
    }

    const fromDayChanged =
      startOfLocalDay(next.from).getTime() !== startOfLocalDay(from).getTime();
    const toDayChanged =
      startOfLocalDay(next.to).getTime() !== startOfLocalDay(to).getTime();

    syncFrom(
      fromDayChanged ? startOfLocalDay(next.from) : combineDateAndTime(next.from, from),
    );
    syncTo(toDayChanged ? endOfLocalDay(next.to) : combineDateAndTime(next.to, to));
  }

  function commitFromDate() {
    const parsed = new Date(fromDateText);
    if (!Number.isFinite(parsed.getTime())) {
      setFromDateText(formatDateShort(from));
      return;
    }
    const next = combineDateAndTime(parsed, from);
    syncFrom(next);
    setRange((current) => ({ from: next, to: current?.to ?? to }));
  }

  function commitToDate() {
    const parsed = new Date(toDateText);
    if (!Number.isFinite(parsed.getTime())) {
      setToDateText(formatDateShort(to));
      return;
    }
    const next = combineDateAndTime(parsed, to);
    syncTo(next);
    setRange((current) => ({ from: current?.from ?? from, to: next }));
  }

  function commitFromTime() {
    const next = parseTimeInput(fromTimeText, from);
    syncFrom(combineDateAndTime(from, next));
  }

  function commitToTime() {
    const next = parseTimeInput(toTimeText, to);
    syncTo(combineDateAndTime(to, next));
  }

  function apply() {
    const start = from.getTime() <= to.getTime() ? from : to;
    const end = from.getTime() <= to.getTime() ? to : from;
    onApply({ from: start, to: end });
  }

  return (
    <div className={cn("flex flex-col gap-3 p-2", className)}>
      <Calendar
        mode="range"
        selected={range}
        onSelect={onSelect}
        defaultMonth={from}
        className="w-full bg-transparent p-0"
        formatters={{
          formatWeekdayName: (date) =>
            date.toLocaleDateString("en-US", { weekday: "narrow" }),
        }}
      />

      <div className="flex flex-col gap-2 px-1">
        <div className="flex items-center gap-2">
          <Label className="w-10 shrink-0 text-muted-foreground">Start</Label>
          <Input
            value={fromDateText}
            onChange={(event) => setFromDateText(event.target.value)}
            onBlur={commitFromDate}
            onKeyDown={(event) => {
              if (event.key === "Enter") commitFromDate();
            }}
            aria-label="Start date"
            className="h-8"
          />
          <Input
            value={fromTimeText}
            onChange={(event) => setFromTimeText(event.target.value)}
            onBlur={commitFromTime}
            onKeyDown={(event) => {
              if (event.key === "Enter") commitFromTime();
            }}
            aria-label="Start time"
            className="h-8 w-[7.5rem] shrink-0"
          />
        </div>
        <div className="flex items-center gap-2">
          <Label className="w-10 shrink-0 text-muted-foreground">End</Label>
          <Input
            value={toDateText}
            onChange={(event) => setToDateText(event.target.value)}
            onBlur={commitToDate}
            onKeyDown={(event) => {
              if (event.key === "Enter") commitToDate();
            }}
            aria-label="End date"
            className="h-8"
          />
          <Input
            value={toTimeText}
            onChange={(event) => setToTimeText(event.target.value)}
            onBlur={commitToTime}
            onKeyDown={(event) => {
              if (event.key === "Enter") commitToTime();
            }}
            aria-label="End time"
            className="h-8 w-[7.5rem] shrink-0"
          />
        </div>
      </div>

      <div className="flex flex-col gap-2 px-1 pb-1">
        <Button size="sm" className="w-full" onClick={apply}>
          Apply
          <CornerDownLeftIcon data-icon="inline-end" />
        </Button>
        <p className="truncate px-1 text-xs text-muted-foreground" title={localTimezoneLabel()}>
          {localTimezoneLabel()}
        </p>
      </div>
    </div>
  );
}
