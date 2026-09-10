import {
  formatDateRangeLabel,
  startOfLocalMonth,
} from "@/lib/format";
import type { RecordOrder } from "@/lib/api/types";

export type CreatedFilter =
  | { kind: "preset"; id: CreatedPresetId }
  | { kind: "custom"; from: Date; to: Date };

export type CreatedPresetId = "1h" | "24h" | "7d" | "30d" | "month";

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

export type DateRangeValue = {
  from: Date;
  to: Date;
};

export function createdPresetRange(id: CreatedPresetId, now = new Date()): DateRangeValue {
  const to = new Date(now);
  if (id === "month") {
    return { from: startOfLocalMonth(now), to };
  }

  const hours = id === "1h" ? 1 : id === "24h" ? 24 : id === "7d" ? 24 * 7 : 24 * 30;
  const from = new Date(now.getTime() - hours * 3_600_000);
  return { from, to };
}

export function resolveCreatedRange(
  created: CreatedFilter | null,
  now = new Date(),
): { from: string | null; to: string | null } {
  if (!created) return { from: null, to: null };
  if (created.kind === "custom") {
    return { from: created.from.toISOString(), to: created.to.toISOString() };
  }
  const range = createdPresetRange(created.id, now);
  return { from: range.from.toISOString(), to: range.to.toISOString() };
}

export function createdLabel(created: CreatedFilter) {
  if (created.kind === "preset") {
    return CREATED_PRESETS.find((preset) => preset.id === created.id)?.label ?? created.id;
  }
  return formatDateRangeLabel(created.from, created.to);
}
