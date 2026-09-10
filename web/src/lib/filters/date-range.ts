import { format, startOfMonth, subDays, subHours, subMinutes } from "date-fns";

import type { DatePresetId, ValueOf } from "@/lib/filters/types";

export type DateRangeValue = ValueOf<"date-range">;

export type ResolvedRange = {
  from: string | null;
  to: string | null;
};

export const DATE_PRESETS: { id: DatePresetId; label: string }[] = [
  { id: "any", label: "Any time" },
  { id: "last-15-minutes", label: "Last 15 Minutes" },
  { id: "last-hour", label: "Last Hour" },
  { id: "last-24-hours", label: "Last 24 Hours" },
  { id: "last-7-days", label: "Last 7 Days" },
  { id: "last-30-days", label: "Last 30 Days" },
  { id: "this-month", label: "This Month" },
  { id: "custom", label: "Custom Range" },
];

export function datePresetLabel(preset: DatePresetId) {
  return DATE_PRESETS.find((entry) => entry.id === preset)?.label ?? "Any time";
}

/**
 * Turn a stored value into absolute bounds. Relative presets resolve against
 * the current time on every call so they keep sliding as the page refetches.
 */
export function resolveDateRange(value: DateRangeValue): ResolvedRange {
  if (value.preset === "custom") {
    return { from: value.from, to: value.to };
  }

  const now = new Date();
  switch (value.preset) {
    case "last-15-minutes":
      return { from: subMinutes(now, 15).toISOString(), to: null };
    case "last-hour":
      return { from: subHours(now, 1).toISOString(), to: null };
    case "last-24-hours":
      return { from: subHours(now, 24).toISOString(), to: null };
    case "last-7-days":
      return { from: subDays(now, 7).toISOString(), to: null };
    case "last-30-days":
      return { from: subDays(now, 30).toISOString(), to: null };
    case "this-month":
      return { from: startOfMonth(now).toISOString(), to: null };
    default:
      return { from: null, to: null };
  }
}

export function describeDateRange(value: DateRangeValue) {
  if (value.preset !== "custom") {
    return datePresetLabel(value.preset);
  }

  const from = value.from ? new Date(value.from) : null;
  const to = value.to ? new Date(value.to) : null;
  if (from && to) return `${format(from, "MMM d, HH:mm")} – ${format(to, "MMM d, HH:mm")}`;
  if (from) return `After ${format(from, "MMM d, HH:mm")}`;
  if (to) return `Before ${format(to, "MMM d, HH:mm")}`;
  return "Any time";
}

export function formatDayLabel(date: Date | undefined) {
  return date ? format(date, "MMM d, yyyy") : "Pick a date";
}

/** `HH:mm` for a time input, falling back when the instant is unset. */
export function timeInputValue(iso: string | null, fallback: string) {
  if (!iso) return fallback;
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? fallback : format(date, "HH:mm");
}

export function combineDateAndTime(date: Date, time: string) {
  const [hours = "0", minutes = "0"] = time.split(":");
  const combined = new Date(date);
  combined.setHours(Number(hours), Number(minutes), 0, 0);
  return combined;
}
