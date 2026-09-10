import { format, startOfDay, startOfMonth, subHours } from "date-fns";

import type { RecordOrder } from "@/lib/api/types";

export type CreatedPresetId = "1h" | "24h" | "7d" | "30d" | "month";

export type CreatedFilter =
  | { kind: "preset"; id: CreatedPresetId }
  | { kind: "custom"; from: Date; to: Date };

export type RecordFilterState = {
  created: CreatedFilter | null;
  search: string;
  partition: string | null;
  order: RecordOrder | null;
  limit: string | null;
  schemaId: number | null;
};

export type RecordFilterKey = keyof RecordFilterState;

export const CREATED_PRESETS: Array<{ id: CreatedPresetId; label: string }> = [
  { id: "1h", label: "Last Hour" },
  { id: "24h", label: "Last 24 Hours" },
  { id: "7d", label: "Last 7 Days" },
  { id: "30d", label: "Last 30 Days" },
  { id: "month", label: "This Month" },
];

export function resolveCreatedRange(
  created: CreatedFilter | null,
  now = new Date(),
): { from: string | null; to: string | null } {
  if (!created) return { from: null, to: null };
  if (created.kind === "custom") {
    return { from: created.from.toISOString(), to: created.to.toISOString() };
  }

  const to = now;
  const from =
    created.id === "month"
      ? startOfMonth(now)
      : subHours(
          now,
          created.id === "1h" ? 1 : created.id === "24h" ? 24 : created.id === "7d" ? 24 * 7 : 24 * 30,
        );

  return { from: from.toISOString(), to: to.toISOString() };
}

export function createdLabel(created: CreatedFilter) {
  if (created.kind === "preset") {
    return CREATED_PRESETS.find((preset) => preset.id === created.id)?.label ?? created.id;
  }
  return `${format(created.from, "LLL d, y")} – ${format(created.to, "LLL d, y")}`;
}

export function defaultCustomRange(now = new Date()) {
  return { from: startOfDay(now), to: now };
}
