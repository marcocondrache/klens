import { describeDateRange } from "@/lib/filters/date-range";
import type {
  FilterFieldDef,
  FilterOption,
  FilterState,
  FilterValue,
  NumberOp,
  ValueOf,
} from "@/lib/filters/types";

export const NUMBER_OPS: { id: NumberOp; symbol: string; label: string }[] = [
  { id: "gt", symbol: ">", label: "greater than" },
  { id: "gte", symbol: "≥", label: "at least" },
  { id: "lt", symbol: "<", label: "less than" },
  { id: "lte", symbol: "≤", label: "at most" },
  { id: "eq", symbol: "=", label: "equal to" },
];

export type ActiveFilter = {
  field: FilterFieldDef;
  value: FilterValue;
};

export function defaultFilterValue(field: FilterFieldDef): FilterValue {
  switch (field.kind) {
    case "multi-select":
      return { kind: "multi-select", values: [] };
    case "choice":
      return { kind: "choice", value: "" };
    case "text":
      return { kind: "text", text: "" };
    case "date-range":
      return { kind: "date-range", preset: "any", from: null, to: null };
    case "number":
      return { kind: "number", op: field.ops?.[0] ?? "gt", value: null };
    case "number-range":
      return { kind: "number-range", from: null, to: null };
    case "key-value":
      return { kind: "key-value", key: "", value: "" };
    case "raw":
      return { kind: "raw", expression: "" };
  }
}

/** Read the stored value for a field, falling back to an empty one. */
export function filterValue<TField extends FilterFieldDef>(
  state: FilterState,
  field: TField,
): ValueOf<TField["kind"]> {
  const stored = state[field.id];
  const value = stored?.kind === field.kind ? stored : defaultFilterValue(field);
  return value as ValueOf<TField["kind"]>;
}

export function setFilter(state: FilterState, id: string, value: FilterValue): FilterState {
  return { ...state, [id]: value };
}

export function removeFilter(state: FilterState, id: string): FilterState {
  const next = { ...state };
  delete next[id];
  return next;
}

export function isEmptyFilterValue(value: FilterValue) {
  switch (value.kind) {
    case "multi-select":
      return value.values.length === 0;
    case "choice":
      return value.value === "";
    case "text":
      return value.text.trim() === "";
    case "date-range":
      return value.preset === "any" || (value.preset === "custom" && !value.from && !value.to);
    case "number":
      return value.value == null;
    case "number-range":
      return value.from == null && value.to == null;
    case "key-value":
      return value.key.trim() === "";
    case "raw":
      return value.expression.trim() === "";
  }
}

/** Applied filters in insertion order, paired with their field definition. */
export function activeFilters(fields: FilterFieldDef[], state: FilterState): ActiveFilter[] {
  return Object.keys(state).flatMap((id) => {
    const field = fields.find((candidate) => candidate.id === id);
    if (!field) return [];
    return [{ field, value: filterValue(state, field) }];
  });
}

export function appliedFilterCount(fields: FilterFieldDef[], state: FilterState) {
  return activeFilters(fields, state).filter((entry) => !isEmptyFilterValue(entry.value)).length;
}

function optionLabel(options: FilterOption[], value: string) {
  const option = options.find((entry) => entry.value === value);
  return option?.short ?? option?.label ?? value;
}

/** Short text shown after the field name on a filter pill. */
export function summarizeFilter(field: FilterFieldDef, value: FilterValue): string {
  if (isEmptyFilterValue(value)) return "";

  switch (value.kind) {
    case "multi-select": {
      const options = field.kind === "multi-select" ? field.options : [];
      const labels = value.values.map((entry) => optionLabel(options, entry));
      if (labels.length <= 2) return labels.join(", ");
      return `${labels[0]} +${labels.length - 1}`;
    }
    case "choice":
      return optionLabel(field.kind === "choice" ? field.options : [], value.value);
    case "text":
      return value.text.trim();
    case "date-range":
      return describeDateRange(value);
    case "number": {
      const symbol = NUMBER_OPS.find((op) => op.id === value.op)?.symbol ?? "=";
      const unit = field.kind === "number" ? (field.unit ?? "") : "";
      return `${symbol} ${value.value}${unit ? ` ${unit}` : ""}`;
    }
    case "number-range": {
      if (value.from != null && value.to != null) return `${value.from} – ${value.to}`;
      if (value.from != null) return `≥ ${value.from}`;
      return `≤ ${value.to}`;
    }
    case "key-value":
      return value.value.trim() ? `${value.key} = ${value.value}` : value.key;
    case "raw":
      return value.expression.trim();
  }
}
