import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";

export interface FilterOption {
  value: string;
  label: string;
  icon?: ReactNode;
  /** Trailing menu text, shown when the bar has no rows to count. */
  hint?: ReactNode;
}

export interface FilterField<TData, TId extends string = string> {
  /** Doubles as the URL search key. */
  id: TId;
  label: string;
  plural: string;
  icon: LucideIcon;
  options: readonly FilterOption[];
  accessor: (row: TData) => string | readonly string[];
}

export interface FilterRule {
  id: string;
  values: string[];
  negate: boolean;
}

export function operatorLabel(rule: FilterRule, negate = rule.negate) {
  if (rule.values.length > 1) return negate ? "is none of" : "is any of";
  return negate ? "is not" : "is";
}

function rowMatches<TData>(row: TData, field: FilterField<TData>, rule: FilterRule) {
  const raw = field.accessor(row);
  const values: readonly string[] = typeof raw === "string" ? [raw] : raw;
  const hit = values.some((value) => rule.values.includes(value));
  return rule.negate ? !hit : hit;
}

/** The options a rule keeps: its values, or every other option when negated. */
export function selectedOptions<TData>(field: FilterField<TData>, rule: FilterRule) {
  return field.options.filter((option) => rule.values.includes(option.value) !== rule.negate);
}

export function applyFilters<TData>(
  rows: TData[],
  fields: ReadonlyArray<FilterField<TData>>,
  rules: FilterRule[],
  skip?: string,
) {
  const active = rules.flatMap((rule) => {
    const field = fields.find((candidate) => candidate.id === rule.id);
    return field && rule.id !== skip ? [{ field, rule }] : [];
  });
  if (active.length === 0) return rows;

  return rows.filter((row) => active.every(({ field, rule }) => rowMatches(row, field, rule)));
}

/** Replaces the values of the `id` rule, adding it or dropping it as needed. */
export function setFilterValues(rules: FilterRule[], id: string, values: string[]): FilterRule[] {
  const existing = rules.find((rule) => rule.id === id);
  if (values.length === 0) return rules.filter((rule) => rule !== existing);
  if (!existing) return [...rules, { id, values, negate: false }];
  return rules.map((rule) => (rule === existing ? { ...rule, values } : rule));
}

/** Option counts for `field`, honouring every other active filter. */
export function facetCounts<TData>(
  rows: TData[],
  fields: ReadonlyArray<FilterField<TData>>,
  rules: FilterRule[],
  field: FilterField<TData>,
) {
  const counts = new Map<string, number>();
  for (const row of applyFilters(rows, fields, rules, field.id)) {
    const raw = field.accessor(row);
    for (const value of typeof raw === "string" ? [raw] : raw) {
      counts.set(value, (counts.get(value) ?? 0) + 1);
    }
  }
  return counts;
}

type FilterParam = Omit<FilterRule, "id">;

function decodeFilter(raw: string): FilterParam {
  const negate = raw.startsWith("!");
  return { values: (negate ? raw.slice(1) : raw).split(","), negate };
}

function encodeFilter({ values, negate }: FilterParam) {
  return `${negate ? "!" : ""}${values.join(",")}`;
}

export function filterParam(allowed: readonly string[], raw: unknown): string | undefined {
  if (typeof raw !== "string") return undefined;
  const { values, negate } = decodeFilter(raw);
  const kept = [...new Set(values)].filter((value) => allowed.includes(value));
  return kept.length > 0 ? encodeFilter({ values: kept, negate }) : undefined;
}

export function readFilters<TData>(
  fields: ReadonlyArray<FilterField<TData>>,
  search: Partial<Record<string, unknown>>,
): FilterRule[] {
  return fields.flatMap((field) => {
    const raw = search[field.id];
    return typeof raw === "string" && raw !== "" ? [{ id: field.id, ...decodeFilter(raw) }] : [];
  });
}

export function filterParams<TData, TId extends string>(
  fields: ReadonlyArray<FilterField<TData, TId>>,
  rules: FilterRule[],
): Record<TId, string | undefined> {
  const params = {} as Record<TId, string | undefined>;
  for (const field of fields) {
    const rule = rules.find((candidate) => candidate.id === field.id);
    params[field.id] = rule ? encodeFilter(rule) : undefined;
  }
  return params;
}
