import type { LucideIcon } from "lucide-react";

/** One selectable entry in an option based filter editor. */
export type FilterOption = {
  value: string;
  label: string;
  /** Shorter label for filter pills, where the field name gives the context. */
  short?: string;
  /** Muted text shown after the label. */
  hint?: string;
  /** Extra text matched by the editor search box. */
  keywords?: string;
};

export type NumberOp = "gt" | "gte" | "lt" | "lte" | "eq";

export type DatePresetId =
  | "any"
  | "last-15-minutes"
  | "last-hour"
  | "last-24-hours"
  | "last-7-days"
  | "last-30-days"
  | "this-month"
  | "custom";

/**
 * A single applied filter. Values are plain JSON so filter state can be
 * serialized into the URL or storage without reshaping.
 */
export type FilterValue =
  | { kind: "multi-select"; values: string[] }
  | { kind: "choice"; value: string }
  | { kind: "text"; text: string }
  | { kind: "date-range"; preset: DatePresetId; from: string | null; to: string | null }
  | { kind: "number"; op: NumberOp; value: number | null }
  | { kind: "number-range"; from: number | null; to: number | null }
  | { kind: "key-value"; key: string; value: string }
  | { kind: "raw"; expression: string };

export type FilterKind = FilterValue["kind"];

type FieldBase = {
  id: string;
  label: string;
  icon?: LucideIcon;
  /** Extra text matched by the field picker search box. */
  keywords?: string;
};

/** Declarative description of something a page can be filtered by. */
export type FilterFieldDef =
  | (FieldBase & { kind: "multi-select"; options: FilterOption[]; anyLabel?: string })
  | (FieldBase & { kind: "choice"; options: FilterOption[]; anyLabel?: string })
  | (FieldBase & { kind: "text"; placeholder?: string; mono?: boolean })
  | (FieldBase & { kind: "date-range"; presets?: DatePresetId[] })
  | (FieldBase & { kind: "number"; ops?: NumberOp[]; placeholder?: string; unit?: string })
  | (FieldBase & { kind: "number-range"; fromPlaceholder?: string; toPlaceholder?: string })
  | (FieldBase & { kind: "key-value"; keyPlaceholder?: string; valuePlaceholder?: string })
  | (FieldBase & { kind: "raw"; placeholder?: string; hint?: string });

/** Applied filters keyed by field id, in the order they were added. */
export type FilterState = Record<string, FilterValue>;

/** Messages to show inside a field editor, keyed by field id. */
export type FilterErrors = Record<string, string>;

export type FieldOf<TKind extends FilterKind> = Extract<FilterFieldDef, { kind: TKind }>;
export type ValueOf<TKind extends FilterKind> = Extract<FilterValue, { kind: TKind }>;
